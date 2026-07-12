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
    /// Consecutive DIRECT probe failures before a peer is put through the SWIM indirect
    /// probe + suspicion path (was: before it was declared dead).
    pub probe_failures: u32,
    /// SWIM indirect probe fan-out: on a direct-probe timeout, ask this many random alive
    /// members to ping the target before suspecting it (rules out a one-hop network blip).
    pub indirect_probes: usize,
    /// How long a member stays Suspect before being promoted to Dead — the refutation window
    /// (the suspected node, hearing the gossiped Suspect, bumps its incarnation + re-asserts
    /// Alive). Must exceed a couple gossip hops so a refutation can arrive.
    pub suspect_timeout: Duration,
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
            // 2 direct failures then the SWIM indirect probe confirms (was 3 direct = death) — the
            // indirect probe is the false-positive guard now, so we can suspect sooner.
            probe_failures: 2,
            indirect_probes: 3,
            // Short refutation window: ~1 gossip round-trip (probe_interval 5s) is enough for a
            // wrongly-suspected node to hear it + re-assert Alive. Keeps total death detection
            // (~2 fails + indirect + this) near the old direct-only latency while adding fast
            // gossip convergence.
            suspect_timeout: Duration::from_secs(6),
            shuffle_interval: Duration::from_secs(30),
            shuffle_sample: 8,
            dead_retention: Duration::from_secs(600),
            wedge_rebind: Duration::from_secs(120),
        }
    }
}

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
/// SWIM liveness state of a converged member. Ordered by "how dead": at equal
/// incarnation the higher-ranked state wins the merge (a Suspect/Dead overrides an
/// Alive), while a higher incarnation always wins (the member refuting a false
/// suspicion by bumping its own incarnation and re-asserting Alive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberState {
    Alive,
    Suspect,
    Dead,
}

impl MemberState {
    fn rank(self) -> u8 {
        match self {
            MemberState::Alive => 0,
            MemberState::Suspect => 1,
            MemberState::Dead => 2,
        }
    }
    fn to_u8(self) -> u8 {
        self.rank()
    }
    fn from_u8(v: u8) -> Self {
        match v {
            1 => MemberState::Suspect,
            2 => MemberState::Dead,
            _ => MemberState::Alive,
        }
    }
}

#[derive(Debug, Clone)]
struct Member {
    addr: PeerAddr,
    last_heard_ms: u64,
    /// SWIM incarnation — only the member itself bumps it (to refute). Higher wins the merge.
    incarnation: u64,
    state: MemberState,
    /// LOCAL ONLY (never gossiped): when THIS node first observed the member as Suspect (ms).
    /// Drives the Suspect→Dead promotion after `cfg.suspect_timeout`. `None` unless Suspect.
    suspect_since_ms: Option<u64>,
}

impl Member {
    /// A freshly-observed ALIVE member at incarnation 0 (the common case: direct
    /// probe success, an inbound message, self-refresh). SWIM transitions
    /// (Suspect/Dead, incarnation bumps) are applied by the merge / lifecycle, not here.
    fn alive(addr: PeerAddr, last_heard_ms: u64) -> Self {
        Self {
            addr,
            last_heard_ms,
            incarnation: 0,
            state: MemberState::Alive,
            suspect_since_ms: None,
        }
    }

    /// Apply a new SWIM `(incarnation, state)`, maintaining the local Suspect clock: entering
    /// Suspect stamps `now`; leaving Suspect clears it. Idempotent for an unchanged Suspect.
    fn set_liveness(&mut self, incarnation: u64, state: MemberState, now: u64) {
        self.incarnation = incarnation;
        self.state = state;
        self.suspect_since_ms = match state {
            MemberState::Suspect => Some(self.suspect_since_ms.unwrap_or(now)),
            _ => None,
        };
    }
}

#[derive(Default)]
struct Views {
    active: HashMap<NodeId, PeerState>,
    passive: Vec<PeerAddr>,
    /// Recently-dead peers with their time of death — kept as tombstones for
    /// the status table until `dead_retention` elapses.
    dead: HashMap<NodeId, (PeerState, std::time::Instant)>,
    /// The liveness-tracked FULL member set. Every node converges on the same map (union + SWIM
    /// merge), so an election over the derived census ([`Membership::census`]) is consistent across
    /// nodes — unlike the size-bounded, per-node-divergent active view. Propagated by DELTA gossip
    /// [S1]: the frequent `epidemic_push` carries only the members in `dirty` (changed since the last
    /// push); the 30s shuffle carries the FULL map as the reconciliation backstop that repairs any
    /// missed delta. Steady state (nothing dirty) ⇒ the frequent path sends nothing.
    members: HashMap<NodeId, Member>,
    /// Delta-gossip retransmission counters: `id → remaining pushes`. A changed member is enqueued at
    /// [`GOSSIP_REPEATS`] and re-sent (decrementing) on each `epidemic_push` until it hits 0 — SWIM-
    /// style limited retransmission so a change SATURATES the cluster (push-once left the last nodes
    /// waiting on the 30s shuffle). A miss only delays to that shuffle (never breaks convergence), so
    /// it need not be exhaustive. Freshness-only (`last_heard`) bumps do NOT enqueue — `last_heard` is
    /// no longer a gossiped liveness signal (see `census`).
    dirty: HashMap<NodeId, u8>,
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
    /// THIS node's SWIM incarnation — bumped only by us to REFUTE a false Suspect/Dead
    /// about ourselves (a higher-incarnation Alive overrides it everywhere). We stamp it
    /// on our own member record ([`Self::refresh_self`] / [`Self::member_entries`]).
    self_incarnation: std::sync::atomic::AtomicU64,
}

/// Min interval between epidemic (new-member-triggered) diffusion rounds.
const EPIDEMIC_DEBOUNCE_MS: u64 = 1_000;
/// Active peers a single epidemic round pushes the member map to (gossip
/// fan-out). One target diffused a join wave through the tail in ~40s;
/// fan-out spreads new-member knowledge in log-fanout hops.
const EPIDEMIC_FANOUT: usize = 3;
/// Times a single delta is re-transmitted (SWIM limited retransmission [S1]): a change is enqueued
/// at this and re-sent on each epidemic round until it hits 0, so it saturates the cluster via
/// fan-out^repeats instead of a single push (which left the tail waiting on the 30s shuffle).
const GOSSIP_REPEATS: u8 = 6;
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
            self_incarnation: std::sync::atomic::AtomicU64::new(0),
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
                        // DIGEST anti-entropy [S1]: the cheap divergence check that replaces the
                        // shuffle's full-map carriage — full sync only on a hash mismatch.
                        this.digest_round().await;
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
        let swim_dead = |id: &NodeId| {
            views
                .members
                .get(id)
                .is_some_and(|m| m.state == MemberState::Dead)
        };
        // Active peers, EXCLUDING any that gossip has since converged as Dead (they belong in `dead`,
        // not shown as a live link even for the round or two before probing removes them).
        let active: Vec<(NodeId, PeerState)> = views
            .active
            .iter()
            .filter(|(id, _)| !swim_dead(id))
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let mut dead: Vec<(NodeId, PeerState)> = views
            .dead
            .iter()
            .map(|(k, (v, _))| (*k, v.clone()))
            .collect();
        // Surface converged SWIM-Dead members not locally tombstoned. The converged Dead state is the
        // authoritative "this peer is down" signal on EVERY node — whether it detected the death itself
        // or learned it via gossip — so a node that never held the dead peer as an active link (and so
        // has no tombstone) still reports it down.
        for (id, m) in &views.members {
            if m.state == MemberState::Dead && !dead.iter().any(|(k, _)| k == id) {
                let mut ps = PeerState::new(m.addr.clone());
                ps.alive = false;
                dead.push((*id, ps));
            }
        }
        Snapshot {
            active,
            dead,
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
    /// A stable hash of the CENSUS set — sorted `(id, incarnation, state)` over non-Dead members
    /// (excludes `last_heard`, which ticks, and Dead, whose prune timing varies). Two nodes with the
    /// same digest hold the identical census ⇒ the same election result. The cheap O(1) divergence
    /// check that replaces shipping the full member map every round [S1].
    async fn members_digest(&self) -> [u8; 32] {
        let views = self.views.read().await;
        let mut rows: Vec<([u8; 32], u64, u8)> = views
            .members
            .iter()
            .filter(|(_, m)| m.state != MemberState::Dead)
            .map(|(id, m)| (id.0, m.incarnation, m.state.to_u8()))
            .collect();
        rows.sort_unstable();
        let mut buf = Vec::with_capacity(rows.len() * 41);
        for (id, inc, state) in rows {
            buf.extend_from_slice(&id);
            buf.extend_from_slice(&inc.to_le_bytes());
            buf.push(state);
        }
        zeph_core::Cid::of(&buf).0
    }

    /// DIGEST anti-entropy [S1]: exchange our census-set hash with a random active peer; only if the
    /// hashes DIFFER do we do a full `sync_members_with` reconcile. Steady state (in sync) ⇒ just two
    /// tiny hashes, no member data — the O(N)/round backstop → O(1). A missed delta is caught here
    /// within one digest interval (same bound the full-map shuffle used to give).
    async fn digest_round(&self) {
        let peer = {
            let views = self.views.read().await;
            let addrs: Vec<PeerAddr> = views.active.values().map(|s| s.addr.clone()).collect();
            addrs.choose(&mut rand::thread_rng()).cloned()
        };
        let Some(peer) = peer else { return };
        let my_hash = self.members_digest().await;
        let msg = wire::Message::Digest(wire::Digest { hash: my_hash });
        if let Some(frame) = self.request(&peer, &msg, true).await {
            if let wire::Message::Digest(d) = frame.message {
                if d.hash != my_hash {
                    self.sync_members_with(&peer).await;
                }
            }
        }
    }

    async fn epidemic_push(self: &Arc<Self>) {
        // Keep our own last_heard fresh locally (the 30s shuffle gossips the full map incl. self).
        self.refresh_self().await;
        // DELTA gossip [S1]: send ONLY the members that changed since the last push. Steady state
        // (nothing dirty) → send nothing — the O(N)/round → O(Δ) win. One-way (fire-and-forget): the
        // 30s shuffle is the bidirectional full-map anti-entropy that repairs any missed delta.
        let delta: Vec<wire::MemberEntry> = {
            let mut views = self.views.write().await;
            if views.dirty.is_empty() {
                return;
            }
            let ids: Vec<NodeId> = views.dirty.keys().copied().collect();
            let entries: Vec<wire::MemberEntry> = ids
                .iter()
                .filter_map(|id| views.members.get(id).map(|m| member_entry(*id, m)))
                .collect();
            // Decrement each retransmission counter; retire a member once it has been re-sent
            // GOSSIP_REPEATS times.
            for id in &ids {
                let done = match views.dirty.get_mut(id) {
                    Some(c) => {
                        *c = c.saturating_sub(1);
                        *c == 0
                    }
                    None => false,
                };
                if done {
                    views.dirty.remove(id);
                }
            }
            entries
        };
        if delta.is_empty() {
            return;
        }
        let msg = wire::Message::MemberSync(wire::MemberSync { members: delta });
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
                this.send_oneway(&peer, &msg).await;
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

    /// SWIM indirect probe: after our DIRECT probe of `target` timed out, ask up to
    /// `cfg.indirect_probes` random ALIVE members (≠ self, ≠ target) to ping it on our behalf,
    /// concurrently. Returns true as soon as ANY helper reports it alive — a first-hand refutation
    /// of our timeout that rules out a one-hop network blip before we suspect the target.
    async fn indirect_probe(&self, target: NodeId, target_addr: &PeerAddr) -> bool {
        let k = self.cfg.indirect_probes;
        if k == 0 {
            return false;
        }
        let me = self.my_id();
        let mut helpers: Vec<PeerAddr> = {
            let views = self.views.read().await;
            views
                .members
                .iter()
                // Pick ALIVE helpers by SWIM state (not a last_heard TTL, which delta gossip [S1]
                // no longer keeps fresh for un-probed members).
                .filter(|(id, m)| **id != me && **id != target && m.state == MemberState::Alive)
                .map(|(_, m)| m.addr.clone())
                .collect()
        };
        if helpers.is_empty() {
            return false;
        }
        helpers.shuffle(&mut rand::thread_rng());
        helpers.truncate(k);
        let req = wire::Message::PingReq(wire::PingReq {
            target_id: target.0,
            target_addr: target_addr.to_string(),
        });
        let acks =
            futures::future::join_all(helpers.iter().map(|h| self.request(h, &req, true))).await;
        acks.into_iter().flatten().any(|frame| {
            matches!(frame.message, wire::Message::PingReqAck(ack) if ack.target_id == target.0 && ack.alive)
        })
    }

    /// Put a member into SWIM Suspect (at its current incarnation) after direct + indirect probes
    /// both failed. Gossips via the member map; escalated to Dead by [`Self::promote_suspects`]
    /// after `cfg.suspect_timeout` unless the member refutes (Alive @ a higher incarnation).
    async fn suspect(&self, id: NodeId) {
        let mut views = self.views.write().await;
        if let Some(m) = views.members.get_mut(&id) {
            // Only a currently-Alive member (don't downgrade a Dead; a re-suspect keeps the original
            // suspect_since so the window doesn't reset).
            if m.state == MemberState::Alive {
                let inc = m.incarnation;
                m.set_liveness(inc, MemberState::Suspect, now_ms());
                views.dirty.insert(id, GOSSIP_REPEATS); // gossip the Suspect as a delta
                tracing::warn!(peer = %short(&id), "peer SUSPECT (direct+indirect probe failed)");
            }
        }
    }

    /// Promote every member Suspect longer than `cfg.suspect_timeout` to Dead. The first node to
    /// promote gossips Dead via the member map (census-excluded immediately); others converge.
    async fn promote_suspects(&self) {
        let now = now_ms();
        let timeout = self.cfg.suspect_timeout.as_millis() as u64;
        let expired: Vec<NodeId> = {
            let views = self.views.read().await;
            views
                .members
                .iter()
                .filter(|(_, m)| {
                    m.state == MemberState::Suspect
                        && m.suspect_since_ms
                            .is_some_and(|s| now.saturating_sub(s) >= timeout)
                })
                .map(|(id, _)| *id)
                .collect()
        };
        for id in expired {
            self.mark_dead(id).await;
        }
    }

    async fn mark_dead(&self, id: NodeId) {
        let promoted = {
            let mut views = self.views.write().await;
            // SWIM: mark the converged record Dead at its CURRENT incarnation so it GOSSIPS + drops
            // from the census immediately (not just TTL-aged). Preserving the incarnation lets the
            // member refute (Alive @ a higher incarnation) if it is actually alive.
            if let Some(m) = views.members.get_mut(&id) {
                let inc = m.incarnation;
                m.set_liveness(inc, MemberState::Dead, now_ms());
                views.dirty.insert(id, GOSSIP_REPEATS); // gossip the Dead as a delta
            }
            // Active-view maintenance: only if it was a warm link (a Suspect promoted by the tick
            // may not be in our active view at all).
            match views.active.remove(&id) {
                Some(mut state) => {
                    state.alive = false;
                    views.dead.insert(id, (state, std::time::Instant::now()));
                    views.passive.shuffle(&mut rand::thread_rng());
                    views.passive.pop()
                }
                None => None,
            }
        };
        tracing::warn!(peer = %short(&id), "peer DEAD");
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
        // SWIM lifecycle: escalate any member Suspect past the timeout to Dead (gossips + drops it
        // from the census). Runs every round so a death converges within a bounded window.
        self.promote_suspects().await;
        // Drop active-view peers already converged Dead (via gossip): no point probing a corpse, it
        // holds an active slot, and it belongs in `dead`. Local + cheap; fill_active backfills.
        {
            let mut views = self.views.write().await;
            let dead_active: Vec<NodeId> = views
                .active
                .keys()
                .copied()
                .filter(|id| {
                    views
                        .members
                        .get(id)
                        .is_some_and(|m| m.state == MemberState::Dead)
                })
                .collect();
            for id in dead_active {
                if let Some(mut ps) = views.active.remove(&id) {
                    ps.alive = false;
                    views.dead.insert(id, (ps, std::time::Instant::now()));
                }
            }
        }
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
                    // DIRECT positive liveness evidence → refresh the converged member record.
                    // Preserve the SWIM incarnation/state (don't clobber a refutation); a first-hand
                    // probe ack also CLEARS our own Suspect (we have proof it's alive), but leaves a
                    // higher-incarnation Dead to the member's own refutation.
                    match views.members.get_mut(&id) {
                        Some(m) => {
                            m.last_heard_ms = now_ms();
                            m.addr = addr.clone();
                            // First-hand proof it is alive clears OUR Suspect — via set_liveness so
                            // the local suspect clock resets too (a raw `state = Alive` would leave a
                            // stale suspect_since that a later re-suspect reuses → premature Dead).
                            if m.state == MemberState::Suspect {
                                let inc = m.incarnation;
                                m.set_liveness(inc, MemberState::Alive, now_ms());
                            }
                        }
                        None => {
                            views
                                .members
                                .insert(id, Member::alive(addr.clone(), now_ms()));
                            views.dirty.insert(id, GOSSIP_REPEATS); // newly-learned member → gossip as a delta
                        }
                    }
                    tracing::info!(
                        peer = %short(&id),
                        rtt_us = report.rtt.as_micros() as u64,
                        "peer alive"
                    );
                }
                Err(err) => {
                    let threshold_hit = {
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
                    if threshold_hit {
                        // SWIM: before declaring the peer gone, ask K members to ping it. A helper's
                        // success means our direct path blipped, not that the peer died — rescue it.
                        if self.indirect_probe(id, &addr).await {
                            tracing::info!(peer = %short(&id), "indirect probe reached target — alive");
                            if let Some(state) = self.views.write().await.active.get_mut(&id) {
                                state.consecutive_failures = 0;
                            }
                            self.note_heard(id).await;
                        } else {
                            // Direct AND indirect probes failed → SUSPECT (a refutation grace window
                            // before Dead). Gossips via the member map; promoted to Dead by the tick.
                            self.suspect(id).await;
                        }
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
            // Member gossip no longer rides the shuffle [S1] — the digest round reconciles instead
            // (full sync only on a hash mismatch). The shuffle is now HyParView passive-view mixing only.
            members: Vec::new(),
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
        let incarnation = self
            .self_incarnation
            .load(std::sync::atomic::Ordering::Relaxed);
        self.views.write().await.members.insert(
            id,
            Member {
                addr,
                last_heard_ms: now_ms(),
                incarnation,
                state: MemberState::Alive,
                suspect_since_ms: None,
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
            .map(|(id, m)| member_entry(*id, m))
            .collect()
    }

    /// Merge an incoming member set into ours with SWIM ordering (see [`merge_one`]). An entry about
    /// SELF that says Suspect/Dead triggers REFUTATION: we bump our incarnation above theirs and
    /// re-assert Alive, so the higher incarnation overrides the false suspicion everywhere.
    async fn merge_members(&self, entries: &[wire::MemberEntry]) {
        let me = self.my_id().0;
        let mut refute = false;
        let (new_members, dirtied) = {
            let mut views = self.views.write().await;
            let before = views.members.len();
            let mut dirtied = false;
            for e in entries {
                if e.id == me {
                    // SWIM refutation of a false Suspect/Dead about ourselves.
                    if MemberState::from_u8(e.state) != MemberState::Alive {
                        use std::sync::atomic::Ordering::Relaxed;
                        // saturating: `e.incarnation` is an unauthenticated wire field — a peer
                        // sending u64::MAX must not overflow (panic in debug / wrap-to-0 in release,
                        // which would leave us unable to refute).
                        let bumped = e
                            .incarnation
                            .max(self.self_incarnation.load(Relaxed))
                            .saturating_add(1);
                        self.self_incarnation.store(bumped, Relaxed);
                        refute = true;
                    }
                    continue;
                }
                // Delta-worthy changes (new member / liveness change) become the next gossip delta.
                if merge_one(&mut views.members, e) {
                    views.dirty.insert(NodeId(e.id), GOSSIP_REPEATS);
                    dirtied = true;
                }
            }
            (views.members.len() - before, dirtied)
        };
        if refute {
            // Stamp the bumped incarnation onto our own record (done outside the lock —
            // refresh_self takes the views lock itself), and gossip the refutation as a delta.
            self.refresh_self().await;
            self.views
                .write()
                .await
                .dirty
                .insert(self.my_id(), GOSSIP_REPEATS);
        }
        // EPIDEMIC DIFFUSION: ANY delta-worthy change (new member OR a liveness change — suspect /
        // dead / refute) wakes an immediate debounced delta push, so it diffuses per-seconds-hop
        // instead of waiting the 5s periodic tick or the 30s shuffle. Steady state (no change) → no
        // wake, no gossip.
        if new_members > 0 || dirtied || refute {
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
            // Positive evidence refreshes freshness but PRESERVES the SWIM incarnation/state
            // (a refuted higher incarnation must not regress; a gossiped Suspect/Dead is only
            // cleared by the member's own refutation, not by us hearing one message).
            m.last_heard_ms = now;
        } else if let Some(addr) = views.active.get(&id).map(|s| s.addr.clone()) {
            views.members.insert(id, Member::alive(addr, now));
            views.dirty.insert(id, GOSSIP_REPEATS); // newly-learned member → gossip as a delta
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

    /// The CONVERGED alive set for the registry writer election: every KNOWN member (incl. SELF)
    /// that is not SWIM-Dead. Because the member map converges across nodes (union + SWIM merge)
    /// this returns the SAME set on every node, so the election is consistent — the point of this layer.
    ///
    /// LIVENESS IS THE SWIM STATE, NOT a freshness TTL [S1]: a member leaves the census only when it
    /// is gossiped `Dead` (active detection, converges in seconds) — NOT when its `last_heard` goes
    /// stale. `last_heard` is only a COARSE backstop (`dead_retention`) for a member that went totally
    /// silent without a `Dead` reaching us (a lost gossip / long partition). Decoupling liveness from
    /// the ever-ticking `last_heard` is what lets gossip become O(Δ) — the freshness clock no longer
    /// has to be refreshed cluster-wide every round to keep a live member in the census.
    pub async fn census(&self) -> Vec<(NodeId, PeerAddr)> {
        let now = now_ms();
        let backstop = self.cfg.dead_retention.as_millis() as u64;
        let me = self.my_id();
        let views = self.views.read().await;
        // SELF is trivially alive — always included, with our current address.
        let mut out = vec![(me, self.transport.addr())];
        for (id, m) in &views.members {
            if *id == me {
                continue;
            }
            // Alive OR Suspect stay (Suspect is still probably alive; only fast-converging Dead is
            // excluded). The `last_heard` check is the coarse silent-member backstop, NOT the primary
            // liveness signal.
            if m.state != MemberState::Dead && now.saturating_sub(m.last_heard_ms) < backstop {
                out.push((*id, m.addr.clone()));
            }
        }
        out
    }

    /// The REAL-TIME liveness census for content placement/repair: members that are SWIM **Alive**
    /// (not Suspect/Dead) and not locally tombstoned. Tighter than [`Self::census`] (which keeps
    /// Suspect) so a holder stops counting toward durability the moment it is suspected → repair
    /// fires fast. State-based, not a `last_heard` TTL (see the body / [`Self::census`]).
    pub async fn liveness_census(&self) -> Vec<(NodeId, PeerAddr)> {
        let now = now_ms();
        let backstop = self.cfg.dead_retention.as_millis() as u64;
        let me = self.my_id();
        let views = self.views.read().await;
        let mut out = vec![(me, self.transport.addr())];
        for (id, m) in &views.members {
            // Live-for-durability = SWIM **Alive** (excludes Suspect AND Dead, so repair reacts to a
            // holder the moment it is suspected) and not locally tombstoned. Uses the CONVERGED state,
            // NOT a `last_heard` freshness TTL: with delta gossip [S1] a node no longer refreshes
            // `last_heard` for members it doesn't directly probe, so a TTL here would falsely drop live
            // holders (only ~active_size are probed) → a repair storm. A coarse backstop still forgets
            // a truly-silent member.
            if *id == me || views.dead.contains_key(id) || m.state != MemberState::Alive {
                continue;
            }
            if now.saturating_sub(m.last_heard_ms) < backstop {
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
                // Merge any members the sender rode along (empty now that the digest replaced the
                // shuffle carriage [S1]; kept for robustness), and reply with only the passive sample.
                self.merge_members(&shuffle.members).await;
                self.refresh_self().await;
                Some(wire::Message::ShuffleReply(wire::ShuffleReply {
                    sample: reply_sample,
                    members: Vec::new(),
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
            wire::Message::Digest(_) => {
                // Reply with our own census-set hash. The requester reconciles (full sync) on a
                // mismatch — which is bidirectional, so we learn from it too; no action needed here.
                Some(wire::Message::Digest(wire::Digest {
                    hash: self.members_digest().await,
                }))
            }
            wire::Message::PingReq(req) => {
                // SWIM indirect probe: a peer whose DIRECT probe of `target` timed out asks us to
                // ping it. A helper's success rules out a one-hop blip before the target is suspected.
                let alive = match req.target_addr.parse::<PeerAddr>() {
                    Ok(addr) => self
                        .transport
                        .ping(&addr, self.cfg.probe_timeout)
                        .await
                        .is_ok(),
                    Err(_) => false,
                };
                Some(wire::Message::PingReqAck(wire::PingReqAck {
                    target_id: req.target_id,
                    alive,
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
/// Merge ONE incoming member entry with SWIM ordering. The liveness winner is decided by
/// `(incarnation, rank(state))`: a strictly higher incarnation wins outright (this is how a member
/// REFUTES a false Suspect/Dead — it bumps its incarnation and re-asserts Alive); at EQUAL
/// incarnation the higher-ranked state wins (Dead > Suspect > Alive), so a suspicion/death
/// propagates. `last_heard_ms` always takes the max (freshness backstop, independent of state).
/// Commutative + idempotent on `(incarnation, rank)` → the map still converges across nodes.
/// Serialize one converged member as a wire entry.
fn member_entry(id: NodeId, m: &Member) -> wire::MemberEntry {
    wire::MemberEntry {
        id: id.0,
        addr: m.addr.to_string(),
        last_heard_ms: m.last_heard_ms,
        incarnation: m.incarnation,
        state: m.state.to_u8(),
    }
}

/// Returns `true` iff this merge made a DELTA-WORTHY change — a new member or a liveness
/// `(incarnation, state)` change — so the caller can mark it `dirty` for delta gossip. A
/// `last_heard`-only advance returns `false` (freshness is not a gossiped liveness signal).
fn merge_one(members: &mut HashMap<NodeId, Member>, e: &wire::MemberEntry) -> bool {
    let id = NodeId(e.id);
    let Ok(addr) = e.addr.parse::<PeerAddr>() else {
        return false;
    };
    let in_state = MemberState::from_u8(e.state);
    match members.get_mut(&id) {
        Some(existing) => {
            // Liveness ((incarnation, rank)) and freshness (last_heard) merge INDEPENDENTLY.
            let incoming_wins = e.incarnation > existing.incarnation
                || (e.incarnation == existing.incarnation
                    && in_state.rank() > existing.state.rank());
            if incoming_wins {
                // set_liveness maintains the local Suspect clock (stamp on enter, clear on leave).
                existing.set_liveness(e.incarnation, in_state, now_ms());
            }
            if e.last_heard_ms > existing.last_heard_ms {
                existing.last_heard_ms = e.last_heard_ms;
                existing.addr = addr;
            }
            incoming_wins
        }
        None => {
            let mut m = Member {
                addr,
                last_heard_ms: e.last_heard_ms,
                incarnation: e.incarnation,
                state: in_state,
                suspect_since_ms: None,
            };
            if in_state == MemberState::Suspect {
                m.suspect_since_ms = Some(now_ms());
            }
            members.insert(id, m);
            true // a newly-learned member is delta-worthy
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
            incarnation: 0,
            state: 0,
        }
    }

    fn swim_entry(
        addr: &PeerAddr,
        last_heard_ms: u64,
        incarnation: u64,
        state: MemberState,
    ) -> wire::MemberEntry {
        wire::MemberEntry {
            id: addr.node_id().0,
            addr: addr.to_string(),
            last_heard_ms,
            incarnation,
            state: state.to_u8(),
        }
    }

    #[test]
    fn member_state_u8_roundtrips_and_ranks() {
        for s in [MemberState::Alive, MemberState::Suspect, MemberState::Dead] {
            assert_eq!(MemberState::from_u8(s.to_u8()), s);
        }
        assert!(MemberState::Alive.rank() < MemberState::Suspect.rank());
        assert!(MemberState::Suspect.rank() < MemberState::Dead.rank());
        assert_eq!(MemberState::from_u8(99), MemberState::Alive); // unknown byte → Alive
    }

    #[tokio::test]
    async fn set_liveness_maintains_the_suspect_clock() {
        let mut m = Member::alive(test_addr().await, 0);
        assert!(m.suspect_since_ms.is_none());
        // Entering Suspect stamps the local clock.
        m.set_liveness(0, MemberState::Suspect, 100);
        assert_eq!(m.suspect_since_ms, Some(100));
        // Re-applying Suspect keeps the ORIGINAL stamp — a re-suspect must not reset the window.
        m.set_liveness(0, MemberState::Suspect, 200);
        assert_eq!(m.suspect_since_ms, Some(100));
        // Leaving Suspect (→Alive) CLEARS the clock; otherwise a later re-suspect would reuse a
        // stale, long-expired window and promote to Dead with ~no grace (the reviewed bug).
        m.set_liveness(1, MemberState::Alive, 300);
        assert_eq!(m.suspect_since_ms, None);
        // A fresh Suspect after recovery stamps a NEW time.
        m.set_liveness(1, MemberState::Suspect, 400);
        assert_eq!(m.suspect_since_ms, Some(400));
        // Dead also clears the clock.
        m.set_liveness(1, MemberState::Dead, 500);
        assert_eq!(m.suspect_since_ms, None);
    }

    #[tokio::test]
    async fn merge_one_swim_liveness_ordering_and_refutation() {
        let a = test_addr().await;
        let id = a.node_id();
        let mut m: HashMap<NodeId, Member> = HashMap::new();

        // First observation: Alive@0.
        merge_one(&mut m, &swim_entry(&a, 100, 0, MemberState::Alive));
        assert_eq!(m[&id].state, MemberState::Alive);

        // Same incarnation, HIGHER rank wins: Suspect@0 overrides Alive@0, and stamps the clock.
        merge_one(&mut m, &swim_entry(&a, 100, 0, MemberState::Suspect));
        assert_eq!(m[&id].state, MemberState::Suspect);
        assert!(
            m[&id].suspect_since_ms.is_some(),
            "entering Suspect stamps the local clock"
        );

        // Same incarnation, LOWER rank does NOT override (an Alive@0 can't clear a Suspect@0).
        merge_one(&mut m, &swim_entry(&a, 100, 0, MemberState::Alive));
        assert_eq!(m[&id].state, MemberState::Suspect);

        // Escalate to Dead@0 (highest rank at the same incarnation); the Suspect clock clears.
        merge_one(&mut m, &swim_entry(&a, 100, 0, MemberState::Dead));
        assert_eq!(m[&id].state, MemberState::Dead);
        assert!(
            m[&id].suspect_since_ms.is_none(),
            "leaving Suspect clears the clock"
        );

        // REFUTATION: Alive at a HIGHER incarnation beats Dead@0 (the member came back / refuted).
        merge_one(&mut m, &swim_entry(&a, 100, 1, MemberState::Alive));
        assert_eq!(m[&id].state, MemberState::Alive);
        assert_eq!(m[&id].incarnation, 1);
    }

    #[tokio::test]
    async fn merge_one_freshness_is_independent_of_liveness() {
        let a = test_addr().await;
        let id = a.node_id();
        let mut m: HashMap<NodeId, Member> = HashMap::new();
        merge_one(&mut m, &swim_entry(&a, 100, 5, MemberState::Alive));

        // A STALER (lower last_heard) but higher-rank entry: liveness merges, freshness does NOT regress.
        merge_one(&mut m, &swim_entry(&a, 50, 5, MemberState::Suspect));
        assert_eq!(m[&id].state, MemberState::Suspect, "liveness merged");
        assert_eq!(
            m[&id].last_heard_ms, 100,
            "freshness kept the max, not regressed"
        );

        // A FRESHER entry whose liveness LOSES: freshness advances, liveness unchanged.
        merge_one(&mut m, &swim_entry(&a, 200, 5, MemberState::Alive));
        assert_eq!(m[&id].last_heard_ms, 200, "freshness advanced");
        assert_eq!(
            m[&id].state,
            MemberState::Suspect,
            "lower rank at equal incarnation did not override"
        );
    }

    fn last_heard(members: &HashMap<NodeId, Member>, id: NodeId) -> Option<u64> {
        members.get(&id).map(|m| m.last_heard_ms)
    }

    // ── SWIM active death detection (P4) ─────────────────────────────────────

    /// A Suspect member is escalated to Dead once its window elapses, and Dead drops it from BOTH
    /// censuses IMMEDIATELY (active detection) rather than waiting out the ~120s/30s TTL.
    #[tokio::test]
    async fn suspect_promotes_to_dead_and_drops_from_census() {
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
        let m = Membership::new(
            transport,
            Config {
                suspect_timeout: Duration::from_millis(50),
                ..Default::default()
            },
        );
        // A freshly-heard alive peer is in the census.
        let peer = test_addr().await;
        let pid = peer.node_id();
        m.merge_members(&[entry(&peer, now_ms())]).await;
        assert!(in_census(&m, pid).await, "alive peer is in the census");

        // Direct+indirect failure ⇒ Suspect. Still counted (only Dead is excluded).
        m.suspect(pid).await;
        assert!(in_census(&m, pid).await, "a Suspect stays in the census");

        // After the window the promotion tick escalates it to Dead → excluded from both censuses.
        tokio::time::sleep(Duration::from_millis(80)).await;
        m.promote_suspects().await;
        assert!(
            !in_census(&m, pid).await,
            "Dead is dropped from the election census"
        );
        assert!(
            !m.liveness_census().await.iter().any(|(id, _)| *id == pid),
            "Dead is dropped from the liveness census"
        );
    }

    /// A node that hears a FALSE Suspect/Dead about ITSELF refutes it: bumps its incarnation above
    /// the accuser's and re-asserts Alive, so the higher incarnation overrides the false state.
    #[tokio::test]
    async fn self_suspicion_is_refuted_by_incarnation_bump() {
        use std::sync::atomic::Ordering::Relaxed;
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
        let m = Membership::new(transport, Config::default());
        let me = m.my_id();
        let my_addr = m.transport.addr();

        // Someone gossips us as Dead @ incarnation 5.
        m.merge_members(&[swim_entry(&my_addr, now_ms(), 5, MemberState::Dead)])
            .await;

        assert_eq!(
            m.self_incarnation.load(Relaxed),
            6,
            "incarnation bumped above the false Dead's"
        );
        // Our own gossiped record is Alive @ the bumped incarnation (this is what refutes it fleet-wide).
        let self_entry = m
            .member_entries()
            .await
            .into_iter()
            .find(|e| e.id == me.0)
            .expect("self in member map");
        assert_eq!(self_entry.state, MemberState::Alive.to_u8());
        assert_eq!(self_entry.incarnation, 6);
        assert!(in_census(&m, me).await, "we keep ourselves in the census");
    }

    /// A RESTARTED node comes up at incarnation 0 while the cluster still holds it Dead@0. It must
    /// refute past its own stale tombstone (Alive@1 > Dead@0) on its first sync, else it can't rejoin.
    #[tokio::test]
    async fn restarted_node_refutes_own_stale_dead_and_rejoins() {
        use std::sync::atomic::Ordering::Relaxed;
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
        let m = Membership::new(transport, Config::default());
        let me = m.my_id();
        let my_addr = m.transport.addr();

        // Fresh node (self_incarnation 0) syncs a peer whose map still says WE are Dead@0.
        m.merge_members(&[swim_entry(&my_addr, now_ms(), 0, MemberState::Dead)])
            .await;

        assert_eq!(m.self_incarnation.load(Relaxed), 1, "bumped from 0 to 1");
        let self_entry = m
            .member_entries()
            .await
            .into_iter()
            .find(|e| e.id == me.0)
            .unwrap();
        assert_eq!(
            (self_entry.state, self_entry.incarnation),
            (MemberState::Alive.to_u8(), 1),
            "re-asserts Alive@1, overriding the stale Dead@0"
        );
    }

    /// The indirect-probe HELPER side: a `PingReq` is answered with a `PingReqAck` carrying the
    /// helper's probe result. An unreachable target ⇒ `alive: false` (exercises the handler wiring;
    /// the alive path needs a live serving target, validated on the fleet at roll).
    #[tokio::test]
    async fn ping_req_handler_replies_with_probe_result() {
        let m = Membership::new(
            Arc::new(
                Transport::bind(
                    zeph_crypto_test_identity(),
                    zeph_transport::Reach::LocalOnly,
                    vec![],
                    0,
                )
                .await
                .unwrap(),
            ),
            Config {
                probe_timeout: Duration::from_millis(200),
                ..Default::default()
            },
        );
        // test_addr binds then drops the transport, so this address is unreachable.
        let dead = test_addr().await;
        let reply = m
            .handle_message(wire::Message::PingReq(wire::PingReq {
                target_id: dead.node_id().0,
                target_addr: dead.to_string(),
            }))
            .await;
        match reply {
            Some(wire::Message::PingReqAck(ack)) => {
                assert_eq!(ack.target_id, dead.node_id().0, "acks the requested target");
                assert!(!ack.alive, "unreachable target reported not alive");
            }
            other => panic!("expected a PingReqAck, got {other:?}"),
        }
    }

    /// Delta gossip [S1]: only real changes (new member / liveness) dirty the delta set; a
    /// `last_heard`-only refresh does NOT — that's what makes steady-state gossip O(1) (the freshness
    /// clock ticks every round but doesn't get gossiped).
    #[tokio::test]
    async fn delta_gossip_dirties_only_real_changes() {
        let m = Membership::new(
            Arc::new(
                Transport::bind(
                    zeph_crypto_test_identity(),
                    zeph_transport::Reach::LocalOnly,
                    vec![],
                    0,
                )
                .await
                .unwrap(),
            ),
            Config::default(),
        );
        let peer = test_addr().await;
        let pid = peer.node_id();

        // Learning a NEW member is delta-worthy.
        m.merge_members(&[swim_entry(&peer, now_ms(), 0, MemberState::Alive)])
            .await;
        assert!(
            m.views.read().await.dirty.contains_key(&pid),
            "a newly-learned member is dirty"
        );
        m.views.write().await.dirty.clear();

        // A pure last_heard advance (same incarnation+state) is NOT delta-worthy.
        m.merge_members(&[swim_entry(&peer, now_ms() + 1000, 0, MemberState::Alive)])
            .await;
        assert!(
            m.views.read().await.dirty.is_empty(),
            "a last_heard-only bump does not dirty — freshness is not gossiped"
        );

        // A liveness change (→Suspect) IS delta-worthy.
        m.merge_members(&[swim_entry(&peer, now_ms() + 2000, 0, MemberState::Suspect)])
            .await;
        assert!(
            m.views.read().await.dirty.contains_key(&pid),
            "a liveness change is dirty"
        );
    }

    /// Digest [S1]: the census-set hash changes on a membership change but NOT on a freshness bump,
    /// and excludes Dead — so two nodes with the same census have the same hash (⇒ no reconcile).
    #[tokio::test]
    async fn members_digest_reflects_census_not_freshness() {
        let m = Membership::new(
            Arc::new(
                Transport::bind(
                    zeph_crypto_test_identity(),
                    zeph_transport::Reach::LocalOnly,
                    vec![],
                    0,
                )
                .await
                .unwrap(),
            ),
            Config::default(),
        );
        let a = test_addr().await;
        let empty = m.members_digest().await;

        m.merge_members(&[swim_entry(&a, now_ms(), 0, MemberState::Alive)])
            .await;
        let h1 = m.members_digest().await;
        assert_ne!(empty, h1, "adding a member changes the digest");

        // A last_heard-only bump does NOT change it (freshness is excluded).
        m.merge_members(&[swim_entry(&a, now_ms() + 9_999, 0, MemberState::Alive)])
            .await;
        assert_eq!(
            h1,
            m.members_digest().await,
            "freshness does not affect the digest"
        );

        // A liveness change DOES change it.
        m.merge_members(&[swim_entry(&a, now_ms(), 0, MemberState::Suspect)])
            .await;
        assert_ne!(
            h1,
            m.members_digest().await,
            "a liveness change changes the digest"
        );

        // Dead is excluded from the census hash → back to the empty-census digest.
        m.merge_members(&[swim_entry(&a, now_ms(), 0, MemberState::Dead)])
            .await;
        assert_eq!(
            empty,
            m.members_digest().await,
            "Dead is excluded — digest returns to the empty-census hash"
        );
    }

    async fn in_census(m: &Membership, id: NodeId) -> bool {
        m.census().await.iter().any(|(i, _)| *i == id)
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

        // Liveness is the SWIM STATE, not a tight freshness TTL [S1]. dead_retention (default 600s)
        // is only the coarse silent-member backstop.
        let backstop = Config::default().dead_retention.as_millis() as u64;
        let fresh = test_addr().await; // Alive, just heard → in
        let old_but_alive = test_addr().await; // Alive, heard 200s ago (past the OLD 120s TTL) → STILL in
        let silent = test_addr().await; // Alive but silent beyond the backstop → out
        let dead = test_addr().await; // freshly heard but SWIM-Dead → out
        {
            let mut views = membership.views.write().await;
            views
                .members
                .insert(fresh.node_id(), Member::alive(fresh.clone(), now_ms()));
            views.members.insert(
                old_but_alive.node_id(),
                Member::alive(old_but_alive.clone(), now_ms().saturating_sub(200_000)),
            );
            views.members.insert(
                silent.node_id(),
                Member::alive(silent.clone(), now_ms().saturating_sub(backstop + 5_000)),
            );
            let mut d = Member::alive(dead.clone(), now_ms());
            d.set_liveness(0, MemberState::Dead, now_ms());
            views.members.insert(dead.node_id(), d);
        }

        let census = membership.census().await;
        let ids: Vec<NodeId> = census.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&me), "census always includes self");
        assert!(ids.contains(&fresh.node_id()), "fresh Alive member is in");
        assert!(
            ids.contains(&old_but_alive.node_id()),
            "an Alive member past the old freshness TTL STAYS in — liveness is SWIM state, not a clock"
        );
        assert!(
            !ids.contains(&silent.node_id()),
            "a member silent beyond the coarse backstop is forgotten"
        );
        assert!(
            !ids.contains(&dead.node_id()),
            "a SWIM-Dead member is excluded regardless of freshness"
        );
    }

    /// Deploy-gate regression (review finding): the LIVENESS census used for
    /// content placement must drop a SWIM-dead holder AND a member heard only
    /// within the wide 120s election window but past the tight liveness TTL —
    /// otherwise a dead holder's stale piece_count inflates `have` and
    /// suppresses repair for up to 120s after a death.
    #[tokio::test]
    async fn liveness_census_is_swim_alive_only() {
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

        let fresh = test_addr().await; // Alive, just heard
        let stale_alive = test_addr().await; // Alive but heard 200s ago (past the old 30s liveness TTL)
        let suspect = test_addr().await; // SWIM Suspect
        let dead_state = test_addr().await; // SWIM Dead
        let tombstoned = test_addr().await; // Alive record but locally tombstoned
        {
            let mut views = membership.views.write().await;
            views
                .members
                .insert(fresh.node_id(), Member::alive(fresh.clone(), now_ms()));
            views.members.insert(
                stale_alive.node_id(),
                Member::alive(stale_alive.clone(), now_ms().saturating_sub(200_000)),
            );
            let mut sus = Member::alive(suspect.clone(), now_ms());
            sus.set_liveness(0, MemberState::Suspect, now_ms());
            views.members.insert(suspect.node_id(), sus);
            let mut d = Member::alive(dead_state.clone(), now_ms());
            d.set_liveness(0, MemberState::Dead, now_ms());
            views.members.insert(dead_state.node_id(), d);
            views.members.insert(
                tombstoned.node_id(),
                Member::alive(tombstoned.clone(), now_ms()),
            );
            views.dead.insert(
                tombstoned.node_id(),
                (
                    PeerState::new(tombstoned.clone()),
                    std::time::Instant::now(),
                ),
            );
        }

        let wide: Vec<NodeId> = membership.census().await.iter().map(|(i, _)| *i).collect();
        let live: Vec<NodeId> = membership
            .liveness_census()
            .await
            .iter()
            .map(|(i, _)| *i)
            .collect();

        // Liveness census = SWIM Alive ONLY (state-based, not a freshness TTL).
        assert!(live.contains(&me) && live.contains(&fresh.node_id()));
        assert!(
            live.contains(&stale_alive.node_id()),
            "an Alive holder past the old 30s TTL STAYS live — state-based (fixes the repair storm)"
        );
        assert!(
            !live.contains(&suspect.node_id()),
            "a Suspect holder is excluded from liveness so repair reacts fast"
        );
        assert!(!live.contains(&dead_state.node_id()), "SWIM-Dead excluded");
        assert!(!live.contains(&tombstoned.node_id()), "tombstoned excluded");
        // The wide election census keeps Alive + Suspect + stale-alive, drops only Dead.
        assert!(
            wide.contains(&suspect.node_id()),
            "Suspect stays in the election census"
        );
        assert!(wide.contains(&stale_alive.node_id()));
        assert!(
            !wide.contains(&dead_state.node_id()),
            "Dead excluded from the election census too"
        );
    }
}
