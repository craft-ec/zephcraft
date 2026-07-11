//! Partial-view membership: HyParView-style active/passive views with
//! SWIM-style probing over the active view.
//!
//! Spec: foundation §3 as amended by §62.1 (partial views from the start).
//! Per-node state is O(log N): a small ACTIVE view (warm links, probed for
//! liveness) and a larger PASSIVE view (backup addresses, refreshed by
//! shuffles). Joins propagate via FORWARD_JOIN random walks.
//!
//! v1 scope notes (recorded in the tracker): direct probing only (no
//! indirect PING-REQ), single-hop shuffle, and deaths are detected by each
//! node's own probing rather than gossiped — sufficient while views are
//! dense; SWIM dissemination arrives with scale hardening.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::seq::SliceRandom;
use tokio::sync::{mpsc, RwLock};
use zeph_core::NodeId;
use zeph_transport::{tag, PeerAddr, Transport};
use zeph_wire as wire;

/// ALPN for membership messages.
pub const ALPN: &[u8] = b"/craftec/member/1";

/// Max membership frame we will read (shuffle samples are small).
const MAX_FRAME: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub active_size: usize,
    pub passive_size: usize,
    /// Active random-walk length for FORWARD_JOIN (foundation ARWL).
    pub arwl: u8,
    /// Walk length at which the origin is ALSO added to passive (PRWL).
    pub prwl: u8,
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    /// Consecutive probe failures before a peer is declared dead.
    pub probe_failures: u32,
    pub shuffle_interval: Duration,
    pub shuffle_sample: usize,
    /// How long dead peers stay visible as tombstones in snapshots before
    /// being forgotten (a rejoining peer clears its tombstone immediately).
    /// Also the age at which a member is fully forgotten from the converged
    /// member set (a live member re-asserts itself every sync round).
    pub dead_retention: Duration,
    /// How long the node tolerates FULL isolation (empty active view with
    /// seed-recovery dials failing) before suspecting a wedged endpoint and
    /// asking the transport to rebind it. See the transport's `rebind` doc.
    pub wedge_rebind: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            active_size: 5,
            passive_size: 30,
            arwl: 6,
            prwl: 3,
            probe_interval: Duration::from_secs(5),
            probe_timeout: Duration::from_secs(3),
            probe_failures: 3,
            shuffle_interval: Duration::from_secs(30),
            shuffle_sample: 8,
            dead_retention: Duration::from_secs(600),
            wedge_rebind: Duration::from_secs(120),
        }
    }
}

/// Window (ms) within which a member counts as alive in the [`Membership::census`].
/// Generous on purpose: the converged set must not falsely drop a peer between
/// anti-entropy rounds — genuine deaths age out after `dead_retention`, and a live
/// peer re-asserts its `last_heard_ms` every sync round (well inside this TTL).
// Gossip now diffuses via the 30s shuffle (folded in) rather than a 10s dedicated round, so a
// member's fresh evidence takes a few shuffle hops to reach every node — the TTL must comfortably
// exceed that diffusion time or transitively-known peers flap in and out of the census.
const CENSUS_TTL_MS: u64 = 120_000;

/// Tighter freshness window for the REAL-TIME liveness census used by content
/// placement/repair (vs the 120s election-consistency census). The wide window
/// keeps registry-writer election consistent, but reusing it as liveness let a
/// SWIM-dead holder's stale piece_count inflate `have` and SUPPRESS repair for
/// up to 120s after a death (review finding). 30s > the 30s shuffle interval's
/// worst-case gossip lag yet ~4x faster death-visibility than the old reuse.
const LIVENESS_TTL_MS: u64 = 30_000;

/// Minimum interval (ms) between isolation-recovery seed dials — keeps recovery gentle so a
/// transient loss doesn't storm a fragile relay every probe round.
const RECOVER_INTERVAL_MS: u64 = 15_000;

/// Live state of one known peer (active view or recently dead).
#[derive(Debug, Clone)]
pub struct PeerState {
    pub addr: PeerAddr,
    pub alive: bool,
    pub rtt_us: Option<u64>,
    pub skew_ms: Option<u64>,
    pub last_seen_unix: Option<u64>,
    pub consecutive_failures: u32,
}

impl PeerState {
    fn new(addr: PeerAddr) -> Self {
        Self {
            addr,
            alive: false,
            rtt_us: None,
            skew_ms: None,
            last_seen_unix: None,
            consecutive_failures: 0,
        }
    }
}

/// One converged-member record: a dialable address plus the last time we had
/// positive evidence the member was alive (ms since the Unix epoch). This map
/// is the CONVERGENCE LAYER that lives BESIDE the HyParView active/passive views
/// — it is not a replacement for them.
#[derive(Debug, Clone)]
struct Member {
    addr: PeerAddr,
    last_heard_ms: u64,
}

#[derive(Default)]
struct Views {
    active: HashMap<NodeId, PeerState>,
    passive: Vec<PeerAddr>,
    /// Recently-dead peers with their time of death — kept as tombstones for
    /// the status table until `dead_retention` elapses.
    dead: HashMap<NodeId, (PeerState, std::time::Instant)>,
    /// The liveness-tracked FULL member set, gossiped by MemberSync anti-entropy.
    /// Every node converges on the same map (union + max-`last_heard_ms` merge),
    /// so an election over the derived census ([`Membership::census`]) is
    /// consistent across nodes — unlike the size-bounded, per-node-divergent
    /// active view. NOTE: full-map gossip is O(N) per round — fine at current
    /// scale; a digest / SWIM-piggybacked delta is needed for very large N.
    members: HashMap<NodeId, Member>,
}

/// Snapshot for the control API / dashboard.
pub struct Snapshot {
    pub active: Vec<(NodeId, PeerState)>,
    pub dead: Vec<(NodeId, PeerState)>,
    pub passive_count: usize,
}

pub struct Membership {
    transport: Arc<Transport>,
    cfg: Config,
    views: RwLock<Views>,
    /// The seed peers (dht_seeds) passed to [`Self::start`], RETAINED so the node can
    /// RE-bootstrap if it ever loses its whole overlay. Without this, a transient network
    /// loss that drains the views leaves a node permanently isolated (shuffle needs a live
    /// active peer; the initial bootstrap runs only once).
    bootstrap: RwLock<Vec<PeerAddr>>,
    /// Last time isolation recovery dialed a seed (ms) — rate-limits it so a hiccup doesn't
    /// storm the relay every probe round.
    last_recover_ms: RwLock<u64>,
    /// When FULL isolation began (ms; 0 = not isolated). Feeds the wedge
    /// watchdog: isolation that outlasts `cfg.wedge_rebind` despite ongoing
    /// seed recovery means the dials themselves are broken — a wedged
    /// endpoint — and the transport is asked to rebind.
    isolated_since_ms: RwLock<u64>,
    /// Last epidemic-shuffle fire (ms) — debounces the new-member fast path.
    last_epidemic_ms: RwLock<u64>,
    /// Wakes the shuffle task for an immediate epidemic round (new members).
    epidemic: tokio::sync::Notify,
}

/// Min interval between epidemic (new-member-triggered) diffusion rounds.
const EPIDEMIC_DEBOUNCE_MS: u64 = 1_000;
/// Active peers a single epidemic round pushes the member map to (gossip
/// fan-out). One target diffused a join wave through the tail in ~40s;
/// fan-out spreads new-member knowledge in log-fanout hops.
const EPIDEMIC_FANOUT: usize = 3;
/// Periodic epidemic SAFETY NET. The new-member cascade only fires while a node
/// keeps LEARNING members, so once its neighbors converge a straggler stops
/// receiving pushes and waits for the 30s shuffle — the measured 3s (cascade
/// reached everyone) vs ~35s (fell back to shuffle) census-convergence bimodal.
/// A light periodic push (member map → EPIDEMIC_FANOUT peers) bounds the tail to
/// ~this interval per hop regardless of cascade luck. Cheaper than the retired
/// 10s dedicated shuffle round (map-only, fire-and-forget); a no-op merge on the
/// receiver when nothing is new.
const EPIDEMIC_PERIODIC: Duration = Duration::from_secs(5);

impl Membership {
    pub fn new(transport: Arc<Transport>, cfg: Config) -> Arc<Self> {
        Arc::new(Self {
            transport,
            cfg,
            views: RwLock::new(Views::default()),
            bootstrap: RwLock::new(Vec::new()),
            last_recover_ms: RwLock::new(0),
            isolated_since_ms: RwLock::new(0),
            last_epidemic_ms: RwLock::new(0),
            epidemic: tokio::sync::Notify::new(),
        })
    }

    fn me(&self) -> wire::PeerInfo {
        wire::PeerInfo {
            addr: self.transport.addr().to_string(),
        }
    }

    fn my_id(&self) -> NodeId {
        self.transport.node_id()
    }

    /// Start all membership tasks: server loop (fed by the transport's ALPN
    /// dispatcher), bootstrap joins, probing, and shuffling.
    pub fn start(
        self: &Arc<Self>,
        bootstrap: Vec<PeerAddr>,
        mut streams: mpsc::Receiver<zeph_transport::TaggedStream>,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            while let Some(stream) = streams.recv().await {
                let this = this.clone();
                tokio::spawn(async move { this.handle_stream(stream).await });
            }
        });

        let this = self.clone();
        tokio::spawn(async move {
            *this.bootstrap.write().await = bootstrap.clone();
            for peer in bootstrap {
                this.join(&peer).await;
            }
        });

        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.cfg.probe_interval);
            loop {
                interval.tick().await;
                this.probe_round().await;
            }
        });

        let this = self.clone();
        tokio::spawn(async move {
            // First shuffle EARLY (after the bootstrap joins land) so a fresh
            // node's view diffuses in seconds, then the steady cadence — with
            // an EPIDEMIC fast path: learning new members wakes an immediate
            // extra round (debounced), so a join wave diffuses in ~seconds-hops
            // instead of 30s cycles.
            tokio::time::sleep(Duration::from_secs(3)).await;
            this.shuffle_round().await;
            let mut interval = tokio::time::interval(this.cfg.shuffle_interval);
            interval.tick().await; // consumes the immediate tick
                                   // Periodic epidemic safety net (see EPIDEMIC_PERIODIC): catches
                                   // stragglers the new-member cascade missed, so census convergence no
                                   // longer falls back to the 30s shuffle.
            let mut epidemic_tick = tokio::time::interval(EPIDEMIC_PERIODIC);
            epidemic_tick.tick().await; // consume the immediate tick
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        this.shuffle_round().await;
                    }
                    _ = epidemic_tick.tick() => {
                        this.epidemic_push().await;
                    }
                    _ = this.epidemic.notified() => {
                        // Epidemic: fan the member map to several active peers
                        // at once (not one shuffle target) so a join wave
                        // diffuses in log-fanout hops, not one-hop-per-30s.
                        this.epidemic_push().await;
                    }
                }
            }
        });
    }

    /// Seed the passive view with candidate peers (in production: the configured
    /// `dht_seeds`, re-seeded periodically by noded) and fill the active view from
    /// them — how a node bootstraps membership without a full peer list.
    pub async fn seed(self: &Arc<Self>, peers: Vec<PeerAddr>) {
        let me = self.my_id();
        for peer in peers {
            if peer.node_id() != me {
                self.add_passive(peer).await;
            }
        }
        self.fill_active().await;
    }

    pub async fn snapshot(&self) -> Snapshot {
        let mut views = self.views.write().await;
        let retention = self.cfg.dead_retention;
        views.dead.retain(|_, (_, died)| died.elapsed() < retention);
        Snapshot {
            active: views.active.iter().map(|(k, v)| (*k, v.clone())).collect(),
            dead: views
                .dead
                .iter()
                .map(|(k, (v, _))| (*k, v.clone()))
                .collect(),
            passive_count: views.passive.len(),
        }
    }

    // ── outbound ──────────────────────────────────────────────────────────

    async fn join(&self, contact: &PeerAddr) {
        tracing::info!(peer = %short(&contact.node_id()), "joining via contact");
        let msg = wire::Message::Join(wire::Join { origin: self.me() });
        if self.send_oneway(contact, &msg).await {
            self.add_active(contact.clone()).await;
            // FAST BOOT: pull the full member map over the fresh link NOW
            // instead of waiting shuffle-hops (30s rounds) — a joiner reached
            // full census in 1-3 MINUTES before, and the readiness gate kept
            // settling on a partial view.
            self.sync_members_with(contact).await;
        } else {
            tracing::warn!(peer = %short(&contact.node_id()), "bootstrap contact unreachable");
        }
    }

    /// Epidemic fan-out: push the converged member map to up to
    /// [`EPIDEMIC_FANOUT`] random active peers concurrently (fire-and-forget).
    /// Triggered by learning NEW members; the receivers merge and re-fire,
    /// giving log-fanout diffusion of a join wave.
    async fn epidemic_push(self: &Arc<Self>) {
        self.refresh_self().await;
        let msg = wire::Message::MemberSync(wire::MemberSync {
            members: self.member_entries().await,
        });
        let mut targets: Vec<PeerAddr> = {
            let views = self.views.read().await;
            views.active.values().map(|s| s.addr.clone()).collect()
        };
        targets.shuffle(&mut rand::thread_rng());
        targets.truncate(EPIDEMIC_FANOUT);
        let sends = targets.into_iter().map(|peer| {
            let this = self.clone();
            let msg = msg.clone();
            async move {
                if let Some(frame) = this.request(&peer, &msg, true).await {
                    if let wire::Message::MemberSync(sync) = frame.message {
                        this.merge_members(&sync.members).await;
                    }
                }
            }
        });
        futures::future::join_all(sends).await;
    }

    /// One immediate MemberSync exchange with `peer`: send our converged map,
    /// merge theirs from the reply. Rides the pooled connection; failures are
    /// benign (the shuffle-piggybacked gossip converges regardless).
    async fn sync_members_with(&self, peer: &PeerAddr) {
        self.refresh_self().await;
        let msg = wire::Message::MemberSync(wire::MemberSync {
            members: self.member_entries().await,
        });
        if let Some(frame) = self.request(peer, &msg, true).await {
            if let wire::Message::MemberSync(sync) = frame.message {
                self.merge_members(&sync.members).await;
            }
        }
    }

    /// Fire-and-forget send. Returns true on success.
    async fn send_oneway(&self, peer: &PeerAddr, msg: &wire::Message) -> bool {
        self.request(peer, msg, false).await.is_some() || matches!(msg, _ if false)
    }

    /// Send a frame; optionally read a single reply frame. Rides the pooled
    /// per-peer connection (streams multiplex; the connection is shared and
    /// must not be closed here). A failed request evicts the pooled entry so
    /// the next one re-dials.
    async fn request(
        &self,
        peer: &PeerAddr,
        msg: &wire::Message,
        expect_reply: bool,
    ) -> Option<wire::Frame> {
        // Muxed request/reply (tag::MEMBER) on the shared per-peer connection.
        // request_tagged writes + finishes + reads the whole reply (waiting for
        // the peer to finish the stream, so a one-way push is delivered) and
        // evicts the mux connection on any stream failure.
        let req = wire::encode(msg, self.transport.clock().now().0);
        let bytes = tokio::time::timeout(
            self.cfg.probe_timeout,
            self.transport
                .request_tagged(peer, tag::MEMBER, &req, MAX_FRAME),
        )
        .await
        .ok()?
        .ok()?;
        if expect_reply {
            wire::decode(&bytes).ok()
        } else {
            // One-way: the round-trip completed (peer received the frame and
            // finished the empty reply stream); report success.
            Some(dummy_ok_frame())
        }
    }

    // ── view management ───────────────────────────────────────────────────

    async fn add_active(&self, peer: PeerAddr) {
        let id = peer.node_id();
        if id == self.my_id() {
            return;
        }
        let evicted = {
            let mut views = self.views.write().await;
            views.dead.remove(&id);
            views.passive.retain(|p| p.node_id() != id);
            if views.active.contains_key(&id) {
                views.active.get_mut(&id).expect("checked").addr = peer;
                return;
            }
            let mut evicted = None;
            if views.active.len() >= self.cfg.active_size {
                let ids: Vec<NodeId> = views.active.keys().copied().collect();
                if let Some(victim) = ids.choose(&mut rand::thread_rng()) {
                    if let Some(state) = views.active.remove(victim) {
                        push_passive(
                            &mut views.passive,
                            state.addr.clone(),
                            self.cfg.passive_size,
                        );
                        evicted = Some(state.addr);
                    }
                }
            }
            views.active.insert(id, PeerState::new(peer));
            evicted
        };
        tracing::info!(peer = %short(&id), "active view: added");
        if let Some(victim) = evicted {
            let _ = self.send_oneway(&victim, &wire::Message::Disconnect).await;
        }
    }

    async fn add_passive(&self, peer: PeerAddr) {
        let id = peer.node_id();
        if id == self.my_id() {
            return;
        }
        let mut views = self.views.write().await;
        if views.active.contains_key(&id) {
            return;
        }
        push_passive(&mut views.passive, peer, self.cfg.passive_size);
    }

    async fn mark_dead(&self, id: NodeId) {
        let promoted = {
            let mut views = self.views.write().await;
            let Some(mut state) = views.active.remove(&id) else {
                return;
            };
            state.alive = false;
            views.dead.insert(id, (state, std::time::Instant::now()));
            // Promote a random passive candidate.
            views.passive.shuffle(&mut rand::thread_rng());
            views.passive.pop()
        };
        tracing::warn!(peer = %short(&id), "peer dead — removed from active view");
        if let Some(candidate) = promoted {
            self.try_promote(candidate).await;
        }
    }

    async fn try_promote(&self, candidate: PeerAddr) -> bool {
        let high = self.views.read().await.active.is_empty();
        let msg = wire::Message::Neighbor(wire::Neighbor {
            origin: self.me(),
            high_priority: high,
        });
        if let Some(frame) = self.request(&candidate, &msg, true).await {
            if let wire::Message::NeighborReply(reply) = frame.message {
                if reply.accepted {
                    self.add_active(candidate.clone()).await;
                    // Fast boot: full member map over the fresh link (see join).
                    self.sync_members_with(&candidate).await;
                    return true;
                }
            }
        }
        false
    }

    /// Re-run the seed bootstrap joins to recover from total isolation (active view empty).
    /// Rate-limited isolation recovery: at most once per [`RECOVER_INTERVAL_MS`], dial ONE random
    /// seed to re-enter the overlay. GENTLE — the old path dialed EVERY seed every probe round,
    /// storming a fragile relay. Runs IN ADDITION to `fill_active` (passive promotion), never
    /// instead of it, so both recovery paths stay live (skipping fill_active left nodes stuck).
    async fn recover_isolated(&self) {
        {
            let now = now_ms();
            let mut last = self.last_recover_ms.write().await;
            if now.saturating_sub(*last) < RECOVER_INTERVAL_MS {
                return;
            }
            *last = now;
        }
        let me = self.my_id();
        let seed = {
            let peers = self.bootstrap.read().await;
            peers
                .iter()
                .filter(|p| p.node_id() != me)
                .cloned()
                .collect::<Vec<_>>()
                .choose(&mut rand::thread_rng())
                .cloned()
        };
        if let Some(seed) = seed {
            tracing::warn!(peer = %short(&seed.node_id()), "isolated — re-bootstrapping (one seed)");
            self.join(&seed).await;
        }
    }

    /// The wedge watchdog (called only while the active view is empty): when
    /// isolation outlasts `cfg.wedge_rebind` even though `recover_isolated`
    /// keeps dialing seeds, the dials themselves are broken — a wedged
    /// endpoint (stale QUIC path state after uplink churn; measured incident:
    /// every dial to known-alive seeds died in 3s for 10+ minutes while ICMP
    /// on the same path was clean, and a fresh endpoint joined in 15s). Ask
    /// the transport to rebind, then re-arm seed recovery so the next probe
    /// round dials immediately from the fresh endpoint.
    async fn maybe_rebind_wedged(&self) {
        // Nobody to dial ⇒ isolation is expected (solo/dev node) and a
        // rebind can't help; don't churn the endpoint.
        if self.bootstrap.read().await.is_empty() {
            return;
        }
        let now = now_ms();
        let mut since = self.isolated_since_ms.write().await;
        if *since == 0 {
            *since = now;
            return;
        }
        let isolated_ms = now.saturating_sub(*since);
        if isolated_ms < self.cfg.wedge_rebind.as_millis() as u64 {
            return;
        }
        // Re-arm a full window either way — a failed rebind (port lag,
        // transport closed) retries on the next window, not every round.
        *since = now;
        tracing::warn!(
            isolated_secs = isolated_ms / 1000,
            "isolated past wedge window — rebinding endpoint"
        );
        match self.transport.rebind().await {
            Ok(()) => {
                tracing::info!("endpoint rebound — re-running seed recovery");
                *self.last_recover_ms.write().await = 0;
            }
            Err(err) => tracing::warn!(%err, "endpoint rebind failed"),
        }
    }

    // ── periodic tasks ────────────────────────────────────────────────────

    async fn probe_round(&self) {
        // ALWAYS top up the active view from passive (cheap; promotions succeed when peers are
        // reachable). This is the PRIMARY recovery path and must never be skipped — skipping it
        // when isolated was a bug that left the node stuck.
        self.fill_active().await;
        // ADDITIONALLY, when fully isolated, gently re-bootstrap from a seed (rate-limited) so a
        // node that has lost its whole overlay can rediscover the network — and if isolation
        // outlasts the wedge window despite those dials, rebind the endpoint itself.
        if self.views.read().await.active.is_empty() {
            self.recover_isolated().await;
            self.maybe_rebind_wedged().await;
        } else {
            *self.isolated_since_ms.write().await = 0;
        }
        let targets: Vec<(NodeId, PeerAddr)> = {
            let views = self.views.read().await;
            views
                .active
                .iter()
                .map(|(id, st)| (*id, st.addr.clone()))
                .collect()
        };
        for (id, addr) in targets {
            // One immediate retry before a failure COUNTS: a fresh QUIC handshake can transiently
            // exceed the timeout under connection churn even on a HEALTHY path (measured on the
            // relay-Mac: membership pings timing out while ICMP on the same path was 0% loss —
            // self-inflicted handshake congestion, not packet loss). The retry rides the path the
            // first attempt warmed, so a handshake hiccup doesn't become a consecutive-failure;
            // a genuinely dead peer just costs one extra timeout per round.
            let outcome = match self.transport.ping(&addr, self.cfg.probe_timeout).await {
                Ok(r) => Ok(r),
                Err(_first) => self.transport.ping(&addr, self.cfg.probe_timeout).await,
            };
            match outcome {
                Ok(report) => {
                    let mut views = self.views.write().await;
                    if let Some(state) = views.active.get_mut(&id) {
                        state.alive = true;
                        state.rtt_us = Some(report.rtt.as_micros() as u64);
                        state.skew_ms = Some(report.peer_skew_ms);
                        state.last_seen_unix = now_unix();
                        state.consecutive_failures = 0;
                    }
                    // Positive liveness evidence → refresh the converged member record.
                    views.members.insert(
                        id,
                        Member {
                            addr: addr.clone(),
                            last_heard_ms: now_ms(),
                        },
                    );
                    tracing::info!(
                        peer = %short(&id),
                        rtt_us = report.rtt.as_micros() as u64,
                        "peer alive"
                    );
                }
                Err(err) => {
                    let dead = {
                        let mut views = self.views.write().await;
                        match views.active.get_mut(&id) {
                            Some(state) => {
                                state.consecutive_failures += 1;
                                state.consecutive_failures >= self.cfg.probe_failures
                            }
                            None => false,
                        }
                    };
                    tracing::warn!(peer = %short(&id), %err, "peer unreachable");
                    if dead {
                        self.mark_dead(id).await;
                    }
                }
            }
        }
    }

    /// Keep the active view full: promote random passive candidates while
    /// there is room (standard HyParView maintenance).
    /// Keep the active view full: promote random passive candidates while there is room. A FAILED
    /// promotion DROPS the candidate (it stays popped) — this SELF-CLEANS the passive view of
    /// unreachable/stale addresses so promotions keep finding reachable peers. An earlier
    /// "self-heal" version capped attempts and re-queued failures; that polluted the passive with
    /// dead addresses and left a flaky node unable to fill its active view at all (a regression).
    /// The full-isolation case that motivated it is handled instead by `recover_isolated`'s seed
    /// dial, so draining is safe.
    async fn fill_active(&self) {
        loop {
            let candidate = {
                let mut views = self.views.write().await;
                if views.active.len() >= self.cfg.active_size || views.passive.is_empty() {
                    return;
                }
                views.passive.shuffle(&mut rand::thread_rng());
                views.passive.pop()
            };
            let Some(candidate) = candidate else { return };
            let _ = self.try_promote(candidate).await;
        }
    }

    async fn shuffle_round(&self) {
        self.refresh_self().await;
        let (target, sample) = {
            let views = self.views.read().await;
            let actives: Vec<PeerAddr> = views.active.values().map(|s| s.addr.clone()).collect();
            // TARGET MIXING (census-tail fix): the active view is a small clique
            // around the bootstrap graph; the full member map rides every
            // shuffle, so one exchange with any well-informed peer completes a
            // node. Every ~3rd shuffle targets a random PASSIVE peer, reaching
            // beyond the clique (active-only left the last nodes' members
            // trickling 40s+; mixing → ~35s).
            let mix_passive =
                !views.passive.is_empty() && rand::Rng::gen_ratio(&mut rand::thread_rng(), 1, 3);
            let target = if mix_passive {
                views.passive.choose(&mut rand::thread_rng()).cloned()
            } else {
                actives.choose(&mut rand::thread_rng()).cloned()
            };
            let Some(target) = target else {
                return;
            };
            let mut pool: Vec<PeerAddr> = actives
                .iter()
                .filter(|a| a.node_id() != target.node_id())
                .cloned()
                .chain(views.passive.iter().cloned())
                .collect();
            pool.shuffle(&mut rand::thread_rng());
            pool.truncate(self.cfg.shuffle_sample);
            let sample = pool
                .into_iter()
                .map(|a| wire::PeerInfo {
                    addr: a.to_string(),
                })
                .collect();
            (target, sample)
        };
        let msg = wire::Message::Shuffle(wire::Shuffle {
            origin: self.me(),
            sample,
            members: self.member_entries().await,
        });
        if let Some(frame) = self.request(&target, &msg, true).await {
            if let wire::Message::ShuffleReply(reply) = frame.message {
                for info in reply.sample {
                    if let Ok(addr) = info.addr.parse::<PeerAddr>() {
                        self.add_passive(addr).await;
                    }
                }
                // Converged-membership gossip rides the shuffle reply — no extra connection.
                self.merge_members(&reply.members).await;
            }
        }
        self.prune_members().await;
    }

    // ── converged membership (anti-entropy layer beside HyParView) ─────────

    /// Keep SELF in the member map with a fresh `last_heard_ms` (called each probe/shuffle
    /// round and before replying to a shuffle) — a live node re-asserts itself so it is never
    /// falsely aged out of any peer's converged set.
    async fn refresh_self(&self) {
        let addr = self.transport.addr();
        let id = self.my_id();
        self.views.write().await.members.insert(
            id,
            Member {
                addr,
                last_heard_ms: now_ms(),
            },
        );
    }

    /// This node's member map serialized as wire entries.
    async fn member_entries(&self) -> Vec<wire::MemberEntry> {
        self.views
            .read()
            .await
            .members
            .iter()
            .map(|(id, m)| wire::MemberEntry {
                id: id.0,
                addr: m.addr.to_string(),
                last_heard_ms: m.last_heard_ms,
            })
            .collect()
    }

    /// Merge an incoming member set into ours: union + max-`last_heard_ms`. Skips
    /// entries about SELF (we manage our own record authoritatively via
    /// [`Self::refresh_self`]). Idempotent + commutative → convergence.
    async fn merge_members(&self, entries: &[wire::MemberEntry]) {
        let me = self.my_id().0;
        let new_members = {
            let mut views = self.views.write().await;
            let before = views.members.len();
            for e in entries {
                if e.id == me {
                    continue;
                }
                merge_one(&mut views.members, e);
            }
            views.members.len() - before
        };
        // EPIDEMIC DIFFUSION (v2 census bar): learning NEW members triggers one
        // immediate extra shuffle toward a random active peer (debounced), so
        // new-member knowledge doubles per ~seconds-hop instead of per 30s
        // cycle — a 20-node join wave converges in ~5 hops. Zero steady-state
        // cost: no new information, no extra shuffle.
        if new_members > 0 {
            let now = now_ms();
            let fire = {
                let mut last = self.last_epidemic_ms.write().await;
                if now.saturating_sub(*last) >= EPIDEMIC_DEBOUNCE_MS {
                    *last = now;
                    true
                } else {
                    false
                }
            };
            if fire {
                self.epidemic.notify_one();
            }
        }
    }

    /// Bump the sender's `last_heard_ms` on positive evidence (an inbound message).
    /// If the sender is not yet a known member, adopt its address from the active
    /// view when available (its full record arrives via MemberSync regardless).
    async fn note_heard(&self, id: NodeId) {
        if id == self.my_id() {
            return;
        }
        let now = now_ms();
        let mut views = self.views.write().await;
        if let Some(m) = views.members.get_mut(&id) {
            m.last_heard_ms = now;
        } else if let Some(addr) = views.active.get(&id).map(|s| s.addr.clone()) {
            views.members.insert(
                id,
                Member {
                    addr,
                    last_heard_ms: now,
                },
            );
        }
    }

    /// Fully forget members whose last positive evidence is older than
    /// `dead_retention` (SELF is always retained).
    async fn prune_members(&self) {
        let now = now_ms();
        let retention = self.cfg.dead_retention.as_millis() as u64;
        let me = self.my_id();
        self.views
            .write()
            .await
            .members
            .retain(|id, m| *id == me || now.saturating_sub(m.last_heard_ms) < retention);
    }

    /// The CONVERGED alive set: every member (incl. SELF) whose `last_heard_ms`
    /// is within [`CENSUS_TTL_MS`]. Because the member map converges across nodes
    /// (union + max-merge) this returns the SAME set on every node, so an election
    /// over it is consistent — the whole point of this layer. Unlike `snapshot().active`
    /// it is the FULL alive membership, not a size-bounded per-node partial view.
    pub async fn census(&self) -> Vec<(NodeId, PeerAddr)> {
        let now = now_ms();
        let me = self.my_id();
        let views = self.views.read().await;
        // SELF is trivially alive — always included, with our current address.
        let mut out = vec![(me, self.transport.addr())];
        for (id, m) in &views.members {
            if *id == me {
                continue;
            }
            if now.saturating_sub(m.last_heard_ms) < CENSUS_TTL_MS {
                out.push((*id, m.addr.clone()));
            }
        }
        out
    }

    /// The REAL-TIME liveness census for content placement/repair: members
    /// heard within [`LIVENESS_TTL_MS`] AND not locally tombstoned-dead. Tighter
    /// than [`Self::census`] so a dead holder stops counting toward durability
    /// quickly; the 120s census stays for registry-writer election consistency.
    pub async fn liveness_census(&self) -> Vec<(NodeId, PeerAddr)> {
        let now = now_ms();
        let me = self.my_id();
        let views = self.views.read().await;
        let mut out = vec![(me, self.transport.addr())];
        for (id, m) in &views.members {
            if *id == me || views.dead.contains_key(id) {
                continue;
            }
            if now.saturating_sub(m.last_heard_ms) < LIVENESS_TTL_MS {
                out.push((*id, m.addr.clone()));
            }
        }
        out
    }

    /// Look up a member's dialable address from the converged set (used by
    /// consumers that elect over the census and must then reach the winner).
    pub async fn member_addr(&self, id: NodeId) -> Option<PeerAddr> {
        if id == self.my_id() {
            return Some(self.transport.addr());
        }
        self.views
            .read()
            .await
            .members
            .get(&id)
            .map(|m| m.addr.clone())
    }

    // ── inbound ───────────────────────────────────────────────────────────

    async fn handle_stream(self: Arc<Self>, stream: zeph_transport::TaggedStream) {
        // The authenticated sender identity (iroh binds the QUIC session to the
        // peer's NodeId), used as positive liveness evidence. Muxed: one tagged
        // stream carries one membership message.
        let sender = stream.remote;
        let (mut send, mut recv) = (stream.send, stream.recv);
        let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
            return;
        };
        let frame = match wire::decode(&bytes) {
            Ok(frame) => frame,
            Err(err) => {
                tracing::warn!(%err, "bad membership frame");
                return;
            }
        };
        // Receiving ANY message is evidence the sender is alive.
        self.note_heard(sender).await;
        let merge = self
            .transport
            .clock()
            .merge(zeph_core::hlc::Timestamp(frame.hlc_ts));
        if merge.clamped {
            tracing::warn!(
                skew_ms = merge.skew_ms,
                "membership peer clock far ahead (clamped)"
            );
        }
        let reply = self.handle_message(frame.message).await;
        if let Some(reply) = reply {
            let _ = send
                .write_all(&wire::encode(&reply, self.transport.clock().now().0))
                .await;
        }
        let _ = send.finish();
    }

    async fn handle_message(&self, msg: wire::Message) -> Option<wire::Message> {
        match msg {
            wire::Message::Join(join) => {
                let origin: PeerAddr = join.origin.addr.parse().ok()?;
                tracing::info!(peer = %short(&origin.node_id()), "join received");
                let forward_targets: Vec<PeerAddr> = {
                    let views = self.views.read().await;
                    views
                        .active
                        .values()
                        .map(|s| s.addr.clone())
                        .filter(|a| a.node_id() != origin.node_id())
                        .collect()
                };
                self.add_active(origin.clone()).await;
                let fwd = wire::Message::ForwardJoin(wire::ForwardJoin {
                    origin: join.origin,
                    ttl: self.cfg.arwl,
                });
                for target in forward_targets {
                    let _ = self.send_oneway(&target, &fwd).await;
                }
                None
            }
            wire::Message::ForwardJoin(fwd) => {
                let origin: PeerAddr = fwd.origin.addr.parse().ok()?;
                if origin.node_id() == self.my_id() {
                    return None;
                }
                let active_len = self.views.read().await.active.len();
                if fwd.ttl == 0 || active_len <= 1 {
                    // Adopt the joiner and tell it about us so the link is mutual.
                    self.add_active(origin.clone()).await;
                    let neighbor = wire::Message::Neighbor(wire::Neighbor {
                        origin: self.me(),
                        high_priority: false,
                    });
                    let _ = self.request(&origin, &neighbor, true).await;
                } else {
                    if fwd.ttl == self.cfg.prwl {
                        self.add_passive(origin.clone()).await;
                    }
                    let next = {
                        let views = self.views.read().await;
                        let candidates: Vec<PeerAddr> = views
                            .active
                            .values()
                            .map(|s| s.addr.clone())
                            .filter(|a| a.node_id() != origin.node_id())
                            .collect();
                        candidates.choose(&mut rand::thread_rng()).cloned()
                    };
                    if let Some(next) = next {
                        let _ = self
                            .send_oneway(
                                &next,
                                &wire::Message::ForwardJoin(wire::ForwardJoin {
                                    origin: fwd.origin,
                                    ttl: fwd.ttl - 1,
                                }),
                            )
                            .await;
                    } else {
                        self.add_active(origin).await;
                    }
                }
                None
            }
            wire::Message::Neighbor(neighbor) => {
                let origin: PeerAddr = neighbor.origin.addr.parse().ok()?;
                let accept = neighbor.high_priority
                    || self.views.read().await.active.len() < self.cfg.active_size;
                if accept {
                    self.add_active(origin).await;
                } else {
                    self.add_passive(origin).await;
                }
                Some(wire::Message::NeighborReply(wire::NeighborReply {
                    accepted: accept,
                }))
            }
            wire::Message::Disconnect => None, // sender demoted us; probing will settle it
            wire::Message::Shuffle(shuffle) => {
                let reply_sample: Vec<wire::PeerInfo> = {
                    let views = self.views.read().await;
                    let mut pool: Vec<PeerAddr> = views.passive.clone();
                    pool.shuffle(&mut rand::thread_rng());
                    pool.truncate(shuffle.sample.len().max(1));
                    pool.into_iter()
                        .map(|a| wire::PeerInfo {
                            addr: a.to_string(),
                        })
                        .collect()
                };
                for info in shuffle.sample {
                    if let Ok(addr) = info.addr.parse::<PeerAddr>() {
                        self.add_passive(addr).await;
                    }
                }
                if let Ok(origin) = shuffle.origin.addr.parse::<PeerAddr>() {
                    self.add_passive(origin).await;
                }
                // Converged-membership gossip rides the shuffle: merge the sender's member set
                // and reply with ours — no separate connection (that congested relay peers).
                self.merge_members(&shuffle.members).await;
                self.refresh_self().await;
                Some(wire::Message::ShuffleReply(wire::ShuffleReply {
                    sample: reply_sample,
                    members: self.member_entries().await,
                }))
            }
            wire::Message::MemberSync(sync) => {
                // Merge the sender's member set (union + max-last_heard), then reply
                // with ours so the exchange is symmetric — both sides converge.
                self.merge_members(&sync.members).await;
                self.refresh_self().await;
                Some(wire::Message::MemberSync(wire::MemberSync {
                    members: self.member_entries().await,
                }))
            }
            other => {
                tracing::warn!(tag = other.type_tag(), "unexpected message on member alpn");
                None
            }
        }
    }
}

fn push_passive(passive: &mut Vec<PeerAddr>, peer: PeerAddr, cap: usize) {
    let id = peer.node_id();
    if passive.iter().any(|p| p.node_id() == id) {
        return;
    }
    if passive.len() >= cap {
        let victim = rand::Rng::gen_range(&mut rand::thread_rng(), 0..passive.len());
        passive.swap_remove(victim);
    }
    passive.push(peer);
}

fn now_unix() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

/// Monotonic-ish millisecond wall clock (ms since the Unix epoch) — the merge key
/// for converged membership.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Merge one wire entry into a member map: union + max-`last_heard_ms` (upserting
/// the address when the entry is fresher). Commutative and idempotent.
fn merge_one(members: &mut HashMap<NodeId, Member>, e: &wire::MemberEntry) {
    let id = NodeId(e.id);
    let Ok(addr) = e.addr.parse::<PeerAddr>() else {
        return;
    };
    match members.get_mut(&id) {
        Some(existing) if e.last_heard_ms <= existing.last_heard_ms => {}
        Some(existing) => {
            existing.last_heard_ms = e.last_heard_ms;
            existing.addr = addr;
        }
        None => {
            members.insert(
                id,
                Member {
                    addr,
                    last_heard_ms: e.last_heard_ms,
                },
            );
        }
    }
}

/// Merge a whole entry set (union + max-last_heard). Used by tests to exercise the
/// convergence property directly.
#[cfg(test)]
fn merge_entries(members: &mut HashMap<NodeId, Member>, entries: &[wire::MemberEntry]) {
    for e in entries {
        merge_one(members, e);
    }
}

fn short(id: &NodeId) -> String {
    id.to_hex()[..12].to_string()
}

/// Placeholder frame for "sent successfully, no reply expected".
fn dummy_ok_frame() -> wire::Frame {
    wire::Frame {
        version: wire::VERSION,
        hlc_ts: 0,
        message: wire::Message::Disconnect,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dead_tombstones_expire_and_rejoin_clears_them() {
        let identity = zeph_crypto_test_identity();
        let transport = Arc::new(
            Transport::bind(identity, zeph_transport::Reach::LocalOnly, vec![], 0)
                .await
                .unwrap(),
        );
        let membership = Membership::new(
            transport,
            Config {
                dead_retention: Duration::from_millis(50),
                ..Default::default()
            },
        );

        // Fabricate a peer, mark it active then dead.
        let other = zeph_crypto_test_identity();
        let other_id = {
            let t = Transport::bind(other, zeph_transport::Reach::LocalOnly, vec![], 0)
                .await
                .unwrap();
            let addr = t.addr();
            membership.add_active(addr.clone()).await;
            t.close().await;
            addr.node_id()
        };
        membership.mark_dead(other_id).await;
        assert_eq!(
            membership.snapshot().await.dead.len(),
            1,
            "tombstone visible"
        );

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            membership.snapshot().await.dead.len(),
            0,
            "tombstone expired after retention"
        );
    }

    /// Wedge watchdog: a node whose seeds are unreachable stays isolated; once
    /// isolation outlasts `wedge_rebind` the transport must be rebound — and a
    /// node with NO seeds must never rebind (isolation is expected there).
    #[tokio::test]
    async fn isolation_watchdog_rebinds_endpoint() {
        let transport = Arc::new(
            Transport::bind(
                zeph_crypto_test_identity(),
                zeph_transport::Reach::LocalOnly,
                vec![],
                0,
            )
            .await
            .unwrap(),
        );
        let cfg = Config {
            probe_timeout: Duration::from_millis(200),
            wedge_rebind: Duration::from_millis(1),
            ..Default::default()
        };
        let membership = Membership::new(transport.clone(), cfg);

        // A dead seed: bind an endpoint for its address, then close it.
        let dead_seed = {
            let t = Transport::bind(
                zeph_crypto_test_identity(),
                zeph_transport::Reach::LocalOnly,
                vec![],
                0,
            )
            .await
            .unwrap();
            let addr = t.addr();
            t.close().await;
            addr
        };

        // No seeds configured → isolated, but must NOT rebind.
        membership.probe_round().await;
        membership.probe_round().await;
        assert_eq!(transport.rebinds(), 0, "no seeds: rebind can't help");

        // With a (dead) seed: first isolated round arms the timer, a later
        // round past the wedge window triggers the rebind.
        *membership.bootstrap.write().await = vec![dead_seed];
        membership.probe_round().await; // arms isolated_since
        tokio::time::sleep(Duration::from_millis(10)).await;
        membership.probe_round().await; // past the window → rebind
        assert_eq!(transport.rebinds(), 1, "wedge window elapsed → rebound");

        // Identity is stable across the rebind.
        assert_eq!(membership.my_id(), transport.node_id());
        transport.close().await;
    }

    fn zeph_crypto_test_identity() -> [u8; 32] {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes
    }

    async fn test_addr() -> PeerAddr {
        Transport::bind(
            zeph_crypto_test_identity(),
            zeph_transport::Reach::LocalOnly,
            vec![],
            0,
        )
        .await
        .unwrap()
        .addr()
    }

    fn entry(addr: &PeerAddr, last_heard_ms: u64) -> wire::MemberEntry {
        wire::MemberEntry {
            id: addr.node_id().0,
            addr: addr.to_string(),
            last_heard_ms,
        }
    }

    fn last_heard(members: &HashMap<NodeId, Member>, id: NodeId) -> Option<u64> {
        members.get(&id).map(|m| m.last_heard_ms)
    }

    #[tokio::test]
    async fn member_merge_is_commutative_and_idempotent() {
        let a = test_addr().await;
        let b = test_addr().await;
        // Two divergent views of the same two members.
        let x_entries = vec![entry(&a, 100), entry(&b, 50)];
        let y_entries = vec![entry(&a, 30), entry(&b, 80)];

        // Converge X then Y.
        let mut xy = HashMap::new();
        merge_entries(&mut xy, &x_entries);
        merge_entries(&mut xy, &y_entries);

        // Converge Y then X (opposite order).
        let mut yx = HashMap::new();
        merge_entries(&mut yx, &y_entries);
        merge_entries(&mut yx, &x_entries);

        // Both reach the UNION with the MAX last_heard for each member.
        for map in [&xy, &yx] {
            assert_eq!(map.len(), 2, "union of both member sets");
            assert_eq!(
                last_heard(map, a.node_id()),
                Some(100),
                "max last_heard for A"
            );
            assert_eq!(
                last_heard(map, b.node_id()),
                Some(80),
                "max last_heard for B"
            );
        }

        // Idempotent: re-merging either set changes nothing.
        merge_entries(&mut xy, &x_entries);
        merge_entries(&mut xy, &y_entries);
        assert_eq!(last_heard(&xy, a.node_id()), Some(100));
        assert_eq!(last_heard(&xy, b.node_id()), Some(80));
    }

    #[tokio::test]
    async fn census_excludes_stale_members_and_includes_self() {
        let transport = Arc::new(
            Transport::bind(
                zeph_crypto_test_identity(),
                zeph_transport::Reach::LocalOnly,
                vec![],
                0,
            )
            .await
            .unwrap(),
        );
        let me = transport.node_id();
        let membership = Membership::new(transport, Config::default());

        let fresh = test_addr().await;
        let stale = test_addr().await;
        {
            let mut views = membership.views.write().await;
            views.members.insert(
                fresh.node_id(),
                Member {
                    addr: fresh.clone(),
                    last_heard_ms: now_ms(),
                },
            );
            // Older than CENSUS_TTL_MS → must be excluded.
            views.members.insert(
                stale.node_id(),
                Member {
                    addr: stale.clone(),
                    last_heard_ms: now_ms().saturating_sub(CENSUS_TTL_MS + 5_000),
                },
            );
        }

        let census = membership.census().await;
        let ids: Vec<NodeId> = census.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&me), "census always includes self");
        assert!(ids.contains(&fresh.node_id()), "fresh member is alive");
        assert!(
            !ids.contains(&stale.node_id()),
            "stale member aged out of the census"
        );
    }

    /// Deploy-gate regression (review finding): the LIVENESS census used for
    /// content placement must drop a SWIM-dead holder AND a member heard only
    /// within the wide 120s election window but past the tight liveness TTL —
    /// otherwise a dead holder's stale piece_count inflates `have` and
    /// suppresses repair for up to 120s after a death.
    #[tokio::test]
    async fn liveness_census_drops_dead_and_beyond_liveness_ttl() {
        let transport = Arc::new(
            Transport::bind(
                zeph_crypto_test_identity(),
                zeph_transport::Reach::LocalOnly,
                vec![],
                0,
            )
            .await
            .unwrap(),
        );
        let me = transport.node_id();
        let membership = Membership::new(transport, Config::default());

        let fresh = test_addr().await;
        let dead = test_addr().await;
        // Heard within the wide 120s census window but PAST the 30s liveness TTL.
        let stale_but_in_census = test_addr().await;
        {
            let mut views = membership.views.write().await;
            for (a, heard) in [
                (&fresh, now_ms()),
                (&dead, now_ms()),
                (
                    &stale_but_in_census,
                    now_ms().saturating_sub(LIVENESS_TTL_MS + 5_000),
                ),
            ] {
                views.members.insert(
                    a.node_id(),
                    Member {
                        addr: a.clone(),
                        last_heard_ms: heard,
                    },
                );
            }
            // `dead` is fresh in members but locally tombstoned-dead.
            views.dead.insert(
                dead.node_id(),
                (PeerState::new(dead.clone()), std::time::Instant::now()),
            );
        }

        let wide: Vec<NodeId> = membership.census().await.iter().map(|(i, _)| *i).collect();
        let live: Vec<NodeId> = membership
            .liveness_census()
            .await
            .iter()
            .map(|(i, _)| *i)
            .collect();

        // The wide census keeps all three (election consistency).
        assert!(wide.contains(&dead.node_id()));
        assert!(wide.contains(&stale_but_in_census.node_id()));
        // The liveness census keeps only self + genuinely-fresh, drops the
        // dead holder and the beyond-TTL one — the durability fix.
        assert!(live.contains(&me) && live.contains(&fresh.node_id()));
        assert!(
            !live.contains(&dead.node_id()),
            "SWIM-dead holder excluded from liveness"
        );
        assert!(
            !live.contains(&stale_but_in_census.node_id()),
            "beyond-liveness-TTL member excluded"
        );
    }
}
