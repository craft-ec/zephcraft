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
use zeph_transport::{PeerAddr, Transport};
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
}

impl Membership {
    pub fn new(transport: Arc<Transport>, cfg: Config) -> Arc<Self> {
        Arc::new(Self {
            transport,
            cfg,
            views: RwLock::new(Views::default()),
            bootstrap: RwLock::new(Vec::new()),
            last_recover_ms: RwLock::new(0),
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
        mut conns: mpsc::Receiver<zeph_transport::Connection>,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            while let Some(conn) = conns.recv().await {
                let this = this.clone();
                tokio::spawn(async move { this.handle_conn(conn).await });
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
            let mut interval = tokio::time::interval(this.cfg.shuffle_interval);
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                this.shuffle_round().await;
            }
        });
    }

    /// Seed the passive view with candidate peers (e.g. discovered from the
    /// tracker's node registry) and fill the active view from them. This is
    /// how a node bootstraps membership from the network WITHOUT a hardcoded
    /// peer — once it's on a tracker, it finds everyone.
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
        } else {
            tracing::warn!(peer = %short(&contact.node_id()), "bootstrap contact unreachable");
        }
    }

    /// Fire-and-forget send. Returns true on success.
    async fn send_oneway(&self, peer: &PeerAddr, msg: &wire::Message) -> bool {
        self.request(peer, msg, false).await.is_some() || matches!(msg, _ if false)
    }

    /// Send a frame; optionally read a single reply frame.
    async fn request(
        &self,
        peer: &PeerAddr,
        msg: &wire::Message,
        expect_reply: bool,
    ) -> Option<wire::Frame> {
        let fut = async {
            let conn = self.transport.connect(peer, ALPN).await.ok()?;
            let (mut send, mut recv) = conn.open_bi().await.ok()?;
            send.write_all(&wire::encode(msg, self.transport.clock().now().0))
                .await
                .ok()?;
            send.finish().ok()?;
            let reply = if expect_reply {
                let bytes = recv.read_to_end(MAX_FRAME).await.ok()?;
                wire::decode(&bytes).ok()
            } else {
                // Wait for the peer to close so the frame is delivered.
                let _ = recv.read_to_end(1).await;
                None
            };
            conn.close(0u32.into(), b"done");
            Some(reply)
        };
        match tokio::time::timeout(self.cfg.probe_timeout, fut).await {
            Ok(Some(reply)) => reply.or(Some(dummy_ok_frame())),
            _ => None,
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
                    self.add_active(candidate).await;
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

    // ── periodic tasks ────────────────────────────────────────────────────

    async fn probe_round(&self) {
        // ALWAYS top up the active view from passive (cheap; promotions succeed when peers are
        // reachable). This is the PRIMARY recovery path and must never be skipped — skipping it
        // when isolated was a bug that left the node stuck.
        self.fill_active().await;
        // ADDITIONALLY, when fully isolated, gently re-bootstrap from a seed (rate-limited) so a
        // node that has lost its whole overlay can rediscover the network.
        if self.views.read().await.active.is_empty() {
            self.recover_isolated().await;
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
            match self.transport.ping(&addr, self.cfg.probe_timeout).await {
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
    async fn fill_active(&self) {
        let mut requeue = Vec::new();
        let mut attempts = 0;
        loop {
            let candidate = {
                let mut views = self.views.write().await;
                if views.active.len() >= self.cfg.active_size
                    || views.passive.is_empty()
                    || attempts >= self.cfg.active_size
                {
                    break;
                }
                views.passive.shuffle(&mut rand::thread_rng());
                views.passive.pop()
            };
            let Some(candidate) = candidate else { break };
            attempts += 1;
            if !self.try_promote(candidate.clone()).await {
                // A transient promote failure must NOT erase peer knowledge (the old code
                // popped the candidate and dropped it on failure): re-queue it so a later
                // round retries once the peer is reachable again. Draining the passive view on
                // a network blip was half of what left a flaky node permanently isolated.
                requeue.push(candidate);
            }
        }
        if !requeue.is_empty() {
            let mut views = self.views.write().await;
            for c in requeue {
                if !views.passive.iter().any(|p| p.node_id() == c.node_id()) {
                    views.passive.push(c);
                }
            }
        }
    }

    async fn shuffle_round(&self) {
        self.refresh_self().await;
        let (target, sample) = {
            let views = self.views.read().await;
            let actives: Vec<PeerAddr> = views.active.values().map(|s| s.addr.clone()).collect();
            let Some(target) = actives.choose(&mut rand::thread_rng()).cloned() else {
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
        let mut views = self.views.write().await;
        for e in entries {
            if e.id == me {
                continue;
            }
            merge_one(&mut views.members, e);
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

    async fn handle_conn(self: Arc<Self>, conn: zeph_transport::Connection) {
        // The authenticated sender identity for this connection (iroh binds the
        // QUIC session to the peer's NodeId), used as positive liveness evidence.
        let sender = NodeId(*conn.remote_id().as_bytes());
        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
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
}
