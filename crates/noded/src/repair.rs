//! Death-driven repair (`docs/DURABILITY_DESIGN.md` P2).
//!
//! Repair as a REACTION to a node dying, not a sweep of the inventory. The periodic scan asks "is every
//! cid still healthy?" on a timer — O(N_cids) forever, detecting loss in 15min–2h, and measured as the
//! largest traffic source on an idle fleet. But pieces are not lost per-cid; they are lost when a NODE
//! dies, and SWIM already reports that in SECONDS. So the work becomes proportional to CHURN instead of
//! to inventory, and detection gets *faster* at the same time — cheaper and more durable, not a trade.
//!
//! **How the fleet divides the work with zero coordination.** Node X dies holding `S_x`; every survivor
//! holds a different set. Each survivor fetches X's manifest ONCE and intersects it with its own set
//! LOCALLY — the differing sets are not a problem to coordinate around, they ARE the partition. Then for
//! each shared cid every survivor independently computes the same rendezvous winner over the same
//! surviving-holder set, so exactly one node repairs it. No messages, no leader, no consensus: the inputs
//! are shared (census + manifests), so the answers agree by construction.
//!
//! **Why leaving the census means Dead.** `census()` retains `Suspect` and drops only gossiped `Dead`, so
//! "left the census" is precisely the converged-Dead signal — and the hysteresis the design demands (never
//! repair a flap; see the connection-churn history) is free rather than another timer to tune.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use zeph_core::Cid;
use zeph_membership::Membership;
use zeph_obj::ObjEngine;

use crate::manifest::{Changes, ManifestStore};

/// How often the census is re-read for departures. A LOCAL O(N_nodes) set comparison — no network, no
/// per-cid work — so it can be brisk: this is the latency between a converged `Dead` and repair starting.
const WATCH_TICK: Duration = Duration::from_secs(5);

/// How often peers' manifest HEADS are checked for change (P3 anti-entropy).
///
/// O(N_nodes) head reads — one tiny DHT record each, no set data — versus the scan's O(N_cids) resolves.
/// A peer that has lost nothing costs one unchanged cid comparison, which is the entire point: steady state
/// must be silent. Slower than [`WATCH_TICK`] because this covers a *rarer* event (a holder noticing it
/// dropped something) than death, which SWIM already reports promptly.
const ANTI_ENTROPY_TICK: Duration = Duration::from_secs(60);

/// The consolidation window (`docs/DURABILITY_DESIGN.md` §5.2). Provider changes accumulate as a per-cid
/// NET over this period; only the net at the window's close triggers a reconcile. The offset is a PERIOD,
/// not a per-event reflex — a leave and a return within one window cancel and fire nothing. Longer offsets
/// more churn but delays repair by up to this long; short enough that erasure margin covers the wait,
/// long enough to swallow a burst (a mass death, a node's whole manifest delta, a quick flap).
const RECONCILE_WINDOW: Duration = Duration::from_secs(30);

/// For each cid WE hold: the peers currently claiming to hold it.
///
/// Bounded by OUR store × replication — NOT by the fleet's inventory. That distinction is the whole point:
/// an earlier version cached every watched peer's FULL set (O(N_nodes × N_cids)), which is the same O(N)
/// mistake the periodic scan makes, just moved into memory. We can only ever repair cids we hold pieces
/// for, so a peer's holdings that we do not share are irrelevant and must not be stored.
///
/// This index also makes a death O(1) LOCAL work: the dead node's share is `{c : holders[c] ∋ dead}` — no
/// manifest fetch, no network, at the moment the fleet is least able to afford either.
type HolderIndex = HashMap<[u8; 32], HashSet<[u8; 32]>>;

/// Per peer: the last `(version, head_cid)` we processed. 40 bytes each — the version is what lets
/// `changes_since` take the O(Δ) fast path instead of re-reading a set.
type PeerHeads = HashMap<[u8; 32], (u64, [u8; 32])>;

/// Fold one provider change into the window's per-cid net map, pruning a cid whose net returns to zero.
///
/// Zero-prune IS the offset: a leave (`-1`) later cancelled by a return (`+1`) leaves NO entry, so the
/// window's close triggers nothing for it — not even a no-op reconcile. Pure so the accumulation is
/// testable without standing up an engine.
fn accrue_into(map: &mut HashMap<[u8; 32], i32>, cid: [u8; 32], delta: i32) {
    let net = map.entry(cid).or_insert(0);
    *net += delta;
    if *net == 0 {
        map.remove(&cid);
    }
}

/// The node that must repair `cid`, elected by rendezvous over the surviving holders.
///
/// Every survivor computes this from the same inputs and therefore agrees without exchanging a message —
/// that is the entire coordination mechanism. Hashing on the cid also spreads a dead node's set uniformly
/// across everyone who overlapped it, in proportion to overlap, so no single peer inherits its whole load.
///
/// Candidates are HOLDERS, never the whole census: a non-holder winner would have to fetch k pieces to
/// regenerate, and — worse — would never look, since only holders compute an intersection containing the
/// cid. Electing outside the holder set silently elects nobody.
fn repairer_for(cid: &[u8; 32], holders: &[[u8; 32]]) -> Option<[u8; 32]> {
    holders
        .iter()
        .copied()
        .min_by_key(|h| Cid::of(&[&cid[..], &h[..]].concat()).0)
}

pub struct DeathRepair {
    me: [u8; 32],
    manifests: Arc<ManifestStore>,
    membership: Arc<Membership>,
    engine: Arc<ObjEngine>,
    /// Last processed `(version, head_cid)` per peer — 40 bytes each, NOT their set. The version is what
    /// lets `changes_since` take the O(Δ) fast path (one small diff) instead of re-reading a whole set.
    heads: tokio::sync::Mutex<PeerHeads>,
    /// Who holds what, restricted to cids WE hold (see [`HolderIndex`]).
    holders: tokio::sync::Mutex<HolderIndex>,
    /// Per-cid NET provider change accumulated over the current [`RECONCILE_WINDOW`]: `+1` when a holder
    /// starts holding a cid we hold, `-1` when one stops (a drop or a death). This is the consolidation the
    /// design turns on: we do NOT reconcile per provider event — a churny fleet would then reconcile
    /// forever. We monitor a window and trigger ONCE per cid at its close, and only if the net is non-zero.
    /// A departure offset by an arrival in the same window nets to 0 and fires NOTHING — not even a no-op
    /// reconcile. The sign is a cheap proxy for direction (net loss → likely repair, net gain → likely
    /// shed); `reconcile_cid` re-derives the true direction from probe-verified pieces at execution.
    pending: tokio::sync::Mutex<HashMap<[u8; 32], i32>>,
}

impl DeathRepair {
    pub fn new(
        me: [u8; 32],
        manifests: Arc<ManifestStore>,
        membership: Arc<Membership>,
        engine: Arc<ObjEngine>,
    ) -> Self {
        Self {
            me,
            manifests,
            membership,
            engine,
            heads: tokio::sync::Mutex::new(HashMap::new()),
            holders: tokio::sync::Mutex::new(HashMap::new()),
            pending: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Accumulate a provider change for `cid` into the current window (`+1` gained, `-1` lost).
    async fn accrue(&self, cid: [u8; 32], delta: i32) {
        let mut pending = self.pending.lock().await;
        accrue_into(&mut pending, cid, delta);
    }

    /// The consolidation loop: every [`RECONCILE_WINDOW`], drain the accumulated per-cid net changes and
    /// enqueue ONE `reconcile:{cid}` for each cid whose net is non-zero. Runs forever.
    ///
    /// This is what makes the offset a PERIOD, not a per-event reflex. Within a window a cid can lose and
    /// regain holders any number of times; only the net at the window's close triggers work, and a net of
    /// zero triggers none. `reconcile_cid` still computes the true net across all providers from
    /// probe-verified pieces — the sign here only decides whether it is worth looking at all.
    pub async fn run_reconcile(self: Arc<Self>) {
        loop {
            tokio::time::sleep(RECONCILE_WINDOW).await;
            let batch: Vec<[u8; 32]> = {
                let mut pending = self.pending.lock().await;
                pending.drain().map(|(c, _)| c).collect()
            };
            if batch.is_empty() {
                continue; // a quiet window — nothing changed net, so nothing triggers
            }
            tracing::info!(
                cids = batch.len(),
                "reconcile window: net changes → reconcile"
            );
            for c in batch {
                self.engine.request_reconcile(Cid(c)).await;
            }
        }
    }

    /// Watch the census; react to departures. Runs forever.
    pub async fn run(self: Arc<Self>) {
        let mut known: HashSet<[u8; 32]> = HashSet::new();
        let mut primed = false;
        loop {
            tokio::time::sleep(WATCH_TICK).await;
            let now: HashSet<[u8; 32]> = self
                .membership
                .census()
                .await
                .into_iter()
                .map(|(n, _)| n.0)
                .collect();
            if now.is_empty() {
                continue; // membership not up yet — an empty census is not a mass death
            }
            if !primed {
                // The FIRST view is a baseline, not an event. Without this, every node treats its own
                // startup as the death of everyone it has not met yet and repairs the entire fleet.
                tracing::info!(census = now.len(), "death watch primed");
                known = now;
                primed = true;
                continue;
            }
            // Report every census CHANGE, not just the deaths. A watcher that only speaks when it decides
            // to repair is indistinguishable from one that is not running at all — which is exactly how a
            // dead death-repair path passed as a healthy quiet fleet. Changes are rare, so this is cheap.
            if now != known {
                let gone: Vec<[u8; 32]> = known.difference(&now).copied().collect();
                let joined = now.difference(&known).count();
                tracing::info!(
                    census = now.len(),
                    gone = gone.len(),
                    joined,
                    departed = ?gone.iter().map(|g| hex::encode(&g[..6])).collect::<Vec<_>>(),
                    "census changed"
                );
                // SPAWN, do not await. `on_death` now only ELECTS (O(1) local) and ENQUEUES a per-cid
                // repair job each (O(elected) cheap submits) — milliseconds, not the 2h36m inline grind it
                // replaced — but spawning keeps the watcher structurally decoupled from on_death's latency
                // (the `liveness_census().await`, the holders lock) so a death can never delay detecting the
                // next one. `known` advances below regardless, so each death is dispatched exactly once, and
                // the scheduler — not a private semaphore — bounds how many repairs actually run at once.
                for g in gone {
                    let this = Arc::clone(&self);
                    tokio::spawn(async move { this.on_death(g).await });
                }
            }
            known = now;
        }
    }

    /// P3 — manifest anti-entropy: watch peers' heads; react when a holder's set SHRINKS.
    ///
    /// Covers loss a holder KNOWS about (eviction, deliberate drops) and death events this node missed. It
    /// does NOT cover unaware loss — a node whose bytes vanished while its index survived publishes an
    /// unchanged manifest, because `store.cids()` enumerates the index, not the bytes. A manifest is a claim
    /// about what a node BELIEVES it holds, and it can believe wrongly. Only asking for the bytes settles
    /// that: K8's `AvailabilityProbe` today (still carried by the periodic scan), PDP (K5) properly. This is
    /// why P3 does not retire the scan — that would trade a real check for a self-report.
    pub async fn run_anti_entropy(self: Arc<Self>) {
        loop {
            tokio::time::sleep(ANTI_ENTROPY_TICK).await;
            let alive = self.membership.liveness_census().await;
            let (mut checked, mut readable) = (0usize, 0usize);
            for (peer, _) in alive {
                if peer.0 == self.me {
                    continue;
                }
                checked += 1;
                if self.check_peer(peer.0).await {
                    readable += 1;
                }
            }
            // A pass that reads NOTHING looks exactly like a healthy quiet fleet from the outside, and an
            // unpopulated index makes death-driven repair a silent no-op. Report the pass so the
            // difference is visible without a redeploy.
            let indexed = self.holders.lock().await.len();
            tracing::info!(
                peers = checked,
                readable,
                indexed_cids = indexed,
                "manifest anti-entropy pass"
            );
        }
    }

    /// One peer: has its holdings changed, and did it LOSE anything we can repair?
    ///
    /// Returns whether its manifest was READABLE — an unreadable peer contributes nothing to the index,
    /// and an empty index makes every death a silent no-op.
    async fn check_peer(&self, peer: [u8; 32]) -> bool {
        let known = self.heads.lock().await.get(&peer).copied();
        let Some(changes) = self.manifests.changes_since(peer, known).await else {
            return false; // no manifest head, or an unusable chain — the scan is the backstop for that
        };
        // First sight is a BASELINE, not a loss event — same reasoning as the census `primed` guard:
        // otherwise a joining node reads every peer's manifest as fresh loss and repairs the fleet.
        let first_sight = known.is_none();

        // The head is whatever `changes_since` actually RESOLVED against. Re-reading it here would be a
        // race: the peer can publish between the two calls, and recording a head we never applied would
        // make the next tick see `kc == head_cid` and skip that version's changes forever.
        let (version, head, lost, gained) = match changes {
            Changes::Delta {
                version,
                head,
                added,
                removed,
            } => {
                if added.is_empty() && removed.is_empty() {
                    self.heads.lock().await.insert(peer, (version, head));
                    return true; // unchanged — the O(1) steady state: nothing fetched, nothing indexed
                }
                let (lost, gained) = self.apply_delta(peer, &added, &removed).await;
                (version, head, lost, gained)
            }
            Changes::Reset { version, head, set } => {
                let (lost, gained) = self.apply_reset(peer, &set).await;
                (version, head, lost, gained)
            }
        };
        self.heads.lock().await.insert(peer, (version, head));
        if first_sight {
            return true; // a baseline: neither a loss nor a surplus event, just learning the world
        }
        // ACCRUE this peer's changes into the current window: a cid it DROPPED is a net loss (-1), one it
        // GAINED is a net gain (+1). The consolidation loop nets these — across peers and across the window
        // — and triggers reconcile only for cids whose net is non-zero at the window's close. Some left,
        // some returned: they cancel, and nothing fires. (lost ∩ gained = ∅ for one peer — a diff never
        // claims a cid on both sides — so we never double-count within this call.)
        for c in lost {
            self.accrue(c, -1).await;
        }
        for c in gained {
            self.accrue(c, 1).await;
        }
        true
    }

    /// Fold a peer's ADDED/REMOVED into the index, keeping only cids we hold. Returns `(lost, gained)`:
    /// cids of ours this peer stopped holding (→ repair) and started holding (→ shed).
    ///
    /// Same filter throughout: we only remember and act on cids we hold, since those are the only ones we
    /// could ever repair or shed.
    async fn apply_delta(
        &self,
        peer: [u8; 32],
        added: &[[u8; 32]],
        removed: &[[u8; 32]],
    ) -> (Vec<[u8; 32]>, Vec<[u8; 32]>) {
        let mine = self.our_cids();
        let mut holders = self.holders.lock().await;
        let mut gained = Vec::new();
        for c in added.iter().filter(|c| mine.contains(*c)) {
            if holders.entry(*c).or_default().insert(peer) {
                gained.push(*c); // a NEW holder for a cid we hold — one more copy than before → surplus?
            }
        }
        let mut lost = Vec::new();
        for c in removed.iter().filter(|c| mine.contains(*c)) {
            if let Some(hs) = holders.get_mut(c) {
                if hs.remove(&peer) {
                    lost.push(*c); // it WAS a holder for us and no longer claims to be
                }
            }
        }
        // The index must not outlive our own store.
        holders.retain(|c, hs| mine.contains(c) && !hs.is_empty());
        (lost, gained)
    }

    /// REPLACE what we believed about a peer with its whole set. Returns `(lost, gained)` as [`apply_delta`].
    ///
    /// A reset carries no `removed` list because there is nobody to diff against — so absence IS the
    /// removal. Merging additively instead would leave every stale belief in place permanently: the peer's
    /// later diffs are computed against its own current set, so a cid it already dropped is never mentioned
    /// again and the phantom holder never clears.
    async fn apply_reset(
        &self,
        peer: [u8; 32],
        set: &[[u8; 32]],
    ) -> (Vec<[u8; 32]>, Vec<[u8; 32]>) {
        let mine = self.our_cids();
        let claimed: HashSet<[u8; 32]> = set.iter().copied().filter(|c| mine.contains(c)).collect();
        let mut holders = self.holders.lock().await;
        let mut gained = Vec::new();
        for c in &claimed {
            if holders.entry(*c).or_default().insert(peer) {
                gained.push(*c); // newly attributed to this peer → one more copy than we knew of
            }
        }
        let mut lost = Vec::new();
        for (c, hs) in holders.iter_mut() {
            // `mine` is re-read each pass and our store shrinks too, so the index can hold a cid we have
            // since dropped. Skipping those keeps the same rule as every other path: we only report a loss
            // for a cid we could actually repair.
            if mine.contains(c) && !claimed.contains(c) && hs.remove(&peer) {
                lost.push(*c); // we thought it held this; its own set says otherwise
            }
        }
        holders.retain(|c, hs| mine.contains(c) && !hs.is_empty());
        (lost, gained)
    }

    /// The cids WE hold. Every index decision is filtered through this: we can only repair what we hold.
    fn our_cids(&self) -> HashSet<[u8; 32]> {
        self.engine
            .store()
            .cids()
            .into_iter()
            .map(|c| c.0)
            .collect()
    }

    /// One node died: repair the share of its holdings that is ours to repair.
    ///
    /// Finding and electing the share is O(1) LOCAL — the index already knows who held what, so DECIDING
    /// what to repair costs no manifest fetch and no DHT lookup. EXECUTING it does not: `repair_our_share`
    /// runs `repair_cid` per elected cid, each a DHT resolve (probe-before-repair), so the execution is
    /// O(elected) network and can run for hours (measured: 2h36m for 1242 cids). That is why the caller
    /// SPAWNS this — a slow execution must not block the census watcher from seeing the next death.
    async fn on_death(&self, dead: [u8; 32]) {
        let shared: Vec<[u8; 32]> = {
            let holders = self.holders.lock().await;
            holders
                .iter()
                .filter(|(_, hs)| hs.contains(&dead))
                .map(|(c, _)| *c)
                .collect()
        };
        if shared.is_empty() {
            // Either it held nothing we hold, or we never saw its manifest — REPORT which. Silently
            // returning made a totally broken index (nothing indexed, ever) look exactly like a healthy
            // "nothing to do", which is precisely how this path shipped dead: the fleet logged nothing on a
            // real death and nothing is what a working quiet fleet logs too.
            let indexed = self.holders.lock().await.len();
            tracing::warn!(
                node = %hex::encode(&dead[..6]),
                indexed_cids = indexed,
                "death: nothing of ours indexed for this peer — no repair (index empty ⇒ we never read its manifest)"
            );
            return;
        }
        // It is gone: stop counting it toward durability before electing, or it could be elected to repair.
        {
            let mut holders = self.holders.lock().await;
            for c in &shared {
                if let Some(hs) = holders.get_mut(c) {
                    hs.remove(&dead);
                }
            }
        }
        self.heads.lock().await.remove(&dead);
        self.repair_our_share(&shared, dead, "death-driven repair")
            .await;
    }

    /// Given cids that just lost a holder (`gone`), repair the subset this node is elected for.
    ///
    /// Shared by both triggers: a death and an anti-entropy shrink are the same problem — a holder
    /// disappeared from some cids — and must not grow two subtly different election paths.
    async fn repair_our_share(&self, cids: &[[u8; 32]], gone: [u8; 32], why: &str) {
        // Candidates come from the INDEX (already maintained), intersected with the ALIVE census so a
        // Suspect or departed peer is never elected to act. No fetches, no DHT: this must stay cheap
        // precisely because it runs when nodes are dying.
        let alive: HashSet<[u8; 32]> = self
            .membership
            .liveness_census()
            .await
            .into_iter()
            .map(|(n, _)| n.0)
            .collect();
        let mut mine: Vec<([u8; 32], usize)> = {
            let holders = self.holders.lock().await;
            cids.iter()
                .filter_map(|c| {
                    let mut cands: Vec<[u8; 32]> = holders
                        .get(c)
                        .map(|hs| {
                            hs.iter()
                                .copied()
                                .filter(|h| *h != gone && alive.contains(h))
                                .collect()
                        })
                        .unwrap_or_default();
                    cands.push(self.me); // we hold it by construction; the index may not list us
                    cands.sort_unstable();
                    cands.dedup();
                    (repairer_for(c, &cands) == Some(self.me)).then_some((*c, cands.len()))
                })
                .collect()
        };

        // P4 — PRIORITY: fewest surviving holders FIRST. Repairing in discovery order spends the budget on
        // comfortable cids while the ones nearest the k floor wait; under a correlated failure that is
        // exactly how data is lost while the fleet looks busy.
        mine.sort_by_key(|(_, holders)| *holders);

        let elected = mine.len();
        let last_holder = mine.iter().filter(|(_, n)| *n <= 1).count();

        // ACCRUE, do not enqueue. Each elected cid records a net LOSS (-1) into the current reconcile
        // window; the consolidation loop triggers ONE reconcile per net-non-zero cid when the window closes.
        // If a holder returns for the same cid before the window closes, the +1 cancels this -1 and nothing
        // fires — the offset. This also replaced the old inline budget-semaphore loop (O(elected) DHT
        // resolves, 2h36m for 1242 cids live); accrual is O(elected) cheap map-inserts, returning in ms.
        for (c, _) in mine {
            self.accrue(c, -1).await;
        }
        tracing::info!(
            node = %hex::encode(&gone[..6]),
            candidates = cids.len(),
            elected,
            last_holder,
            why,
            "reconcile accrued (window)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::accrue_into;

    #[test]
    fn a_leave_offset_by_a_return_in_the_window_triggers_nothing() {
        // The consolidation the design turns on: over one window, net a cid's provider churn and act only
        // on a non-zero net. A departure (-1) cancelled by an arrival (+1) leaves NO pending entry — the
        // window closes and reconcile fires for it zero times, not once-then-no-op.
        let mut w = std::collections::HashMap::new();
        let c = [7u8; 32];
        accrue_into(&mut w, c, -1); // a holder dropped it (or died)
        accrue_into(&mut w, c, 1); //  a holder (re)appeared for it
        assert!(!w.contains_key(&c), "net zero → no trigger for this cid");

        // A net LOSS survives (→ reconcile will repair). Two left, one returned.
        accrue_into(&mut w, c, -1);
        accrue_into(&mut w, c, -1);
        accrue_into(&mut w, c, 1);
        assert_eq!(
            w.get(&c),
            Some(&-1),
            "net loss remains, to be repaired at window close"
        );

        // A net GAIN survives (→ reconcile will consider a shed).
        let d = [8u8; 32];
        accrue_into(&mut w, d, 1);
        accrue_into(&mut w, d, 1);
        assert_eq!(
            w.get(&d),
            Some(&2),
            "net gain remains, to be evaluated for surplus"
        );
    }

    use super::*;

    fn node(n: u8) -> [u8; 32] {
        [n; 32]
    }
    fn cid(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn exactly_one_holder_is_elected_and_every_holder_agrees() {
        // The coordination claim: each survivor computes the winner independently from the same inputs and
        // they agree — so the cid is repaired once, with no messages exchanged.
        let holders = [node(1), node(2), node(3)];
        let winner = repairer_for(&cid(9), &holders).unwrap();
        assert!(holders.contains(&winner));
        // Order must not matter: peers enumerate holders in whatever order their manifests arrived.
        let shuffled = [node(3), node(1), node(2)];
        assert_eq!(repairer_for(&cid(9), &shuffled), Some(winner));
        // Deterministic across calls.
        assert_eq!(repairer_for(&cid(9), &holders), Some(winner));
    }

    #[test]
    fn the_load_spreads_across_holders_rather_than_landing_on_one() {
        // Hashing on the CID (not the node) is what makes a dead node's set fragment across its peers. If
        // every cid elected the same winner, one node would inherit the whole dead node's load.
        let holders: Vec<[u8; 32]> = (1..=4u8).map(node).collect();
        let mut counts: HashMap<[u8; 32], usize> = HashMap::new();
        for c in 0..=255u8 {
            let w = repairer_for(&cid(c), &holders).unwrap();
            *counts.entry(w).or_default() += 1;
        }
        assert_eq!(counts.len(), 4, "every holder should win some share");
        // Roughly even: 256 cids over 4 holders ≈ 64 each. Generous bounds — this is a hash, not a counter.
        for (_, n) in counts {
            assert!(n > 20 && n < 130, "load skewed: {n} of 256");
        }
    }

    #[test]
    fn a_dead_holder_is_never_elected() {
        // The candidate set is the SURVIVING holders. Electing the dead node would mean nobody repairs —
        // the failure this whole path exists to prevent.
        let all = [node(1), node(2), node(3)];
        let dead = repairer_for(&cid(7), &all).unwrap();
        let survivors: Vec<[u8; 32]> = all.into_iter().filter(|h| *h != dead).collect();
        let winner = repairer_for(&cid(7), &survivors).unwrap();
        assert_ne!(winner, dead);
        assert!(survivors.contains(&winner));
    }

    #[test]
    fn the_index_is_bounded_by_our_store_not_the_fleets_inventory() {
        // The limit this replaced: caching every peer's FULL set is O(N_nodes × N_cids) — the periodic
        // scan's O(N) mistake moved into memory. We can only repair what we hold, so a peer's holdings we
        // do not share must never be stored. This models `reindex_peer`'s filter.
        let mine: HashSet<[u8; 32]> = [cid(1), cid(2)].into_iter().collect();
        // A peer holding a huge set, of which we share two.
        let theirs: Vec<[u8; 32]> = (1..=200u8).map(cid).collect();
        let kept: HashSet<[u8; 32]> = theirs
            .iter()
            .copied()
            .filter(|c| mine.contains(c))
            .collect();
        assert_eq!(
            kept.len(),
            2,
            "store only the intersection, never their set"
        );
        assert!(kept.contains(&cid(1)) && kept.contains(&cid(2)));
        // Memory is bounded by OUR store regardless of how much they hold.
        assert!(kept.len() <= mine.len());
    }

    #[test]
    fn a_death_is_answered_from_the_index_with_no_fetch() {
        // The dead node's share is `{c : holders[c] ∋ dead}` — derivable locally. A death must not require
        // fetching anything: it arrives exactly when the fleet is least able to serve a fetch.
        let mut holders: HolderIndex = HashMap::new();
        holders.insert(cid(1), [node(7), node(8)].into_iter().collect());
        holders.insert(cid(2), [node(8)].into_iter().collect());
        holders.insert(cid(3), [node(9)].into_iter().collect());
        let dead = node(8);
        let mut share: Vec<[u8; 32]> = holders
            .iter()
            .filter(|(_, hs)| hs.contains(&dead))
            .map(|(c, _)| *c)
            .collect();
        share.sort_unstable();
        assert_eq!(share, vec![cid(1), cid(2)]);
        assert!(
            !share.contains(&cid(3)),
            "cids it never held are not our concern"
        );
    }

    #[test]
    fn repair_order_is_by_actual_redundancy_not_discovery_order() {
        // P4's priority rule, isolated: the budget must always buy the most durability available. Under a
        // correlated failure the budget is the binding constraint, so spending it in discovery order means
        // comfortable cids get repaired while the ones nearest the k floor wait — that is how data is lost
        // while the fleet looks busy.
        let mut mine: Vec<([u8; 32], usize)> = vec![
            (cid(1), 4), // comfortable
            (cid(2), 1), // LAST holder — most urgent
            (cid(3), 3),
            (cid(4), 2),
        ];
        mine.sort_by_key(|(_, holders)| *holders);
        assert_eq!(
            mine.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "fewest surviving holders must be repaired first"
        );
        assert_eq!(mine[0].0, cid(2), "the last-holder cid goes first");
    }

    #[test]
    fn no_holders_elects_nobody() {
        // A cid whose every holder died is already lost — repair cannot see it (the design's last-holder
        // gap). It must return None rather than pick an arbitrary node that holds nothing.
        assert_eq!(repairer_for(&cid(1), &[]), None);
    }
}
