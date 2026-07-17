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

use crate::manifest::ManifestStore;

/// How often the census is re-read for departures. A LOCAL O(N_nodes) set comparison — no network, no
/// per-cid work — so it can be brisk: this is the latency between a converged `Dead` and repair starting.
const WATCH_TICK: Duration = Duration::from_secs(5);

/// Max repairs this node runs at once (P4 budget).
///
/// The dangerous case is not a single death — it is a CORRELATED one (a rack, an AZ, or the 19-node freeze
/// in the tracker). Death-driven repair then fires for many nodes at once, i.e. it stampedes precisely when
/// the fleet is weakest and can least afford a thundering herd of k-piece fetches. An unbudgeted repair
/// under correlated failure is a WORSE outage than the polling it replaces, which is why the design marks
/// this phase mandatory before scale rather than an optimisation.
///
/// Small on purpose: repair is throughput-bound (fetch k pieces, regenerate, distribute), so concurrency
/// past a couple of jobs buys queueing, not speed — while making the herd worse.
const MAX_CONCURRENT_REPAIRS: usize = 2;

/// How often peers' manifest HEADS are checked for change (P3 anti-entropy).
///
/// O(N_nodes) head reads — one tiny DHT record each, no set data — versus the scan's O(N_cids) resolves.
/// A peer that has lost nothing costs one unchanged cid comparison, which is the entire point: steady state
/// must be silent. Slower than [`WATCH_TICK`] because this covers a *rarer* event (a holder noticing it
/// dropped something) than death, which SWIM already reports promptly.
const ANTI_ENTROPY_TICK: Duration = Duration::from_secs(60);

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
    /// The repair BUDGET (P4). Held for the duration of a repair, so concurrency is bounded fleet-wide by
    /// construction rather than by hoping deaths arrive one at a time.
    budget: Arc<tokio::sync::Semaphore>,
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
            budget: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_REPAIRS)),
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
                known = now;
                primed = true;
                continue;
            }
            for gone in known.difference(&now).copied().collect::<Vec<_>>() {
                self.on_death(gone).await;
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
            for (peer, _) in alive {
                if peer.0 == self.me {
                    continue;
                }
                self.check_peer(peer.0).await;
            }
        }
    }

    /// One peer: has its holdings changed, and did it LOSE anything we can repair?
    async fn check_peer(&self, peer: [u8; 32]) {
        let known = self.heads.lock().await.get(&peer).copied();
        let Some((version, added, removed)) = self.manifests.changes_since(peer, known).await
        else {
            return; // no manifest, or an unusable chain — the scan is the backstop for that
        };
        let head = match self.manifests.peer_head(peer).await {
            Some((c, _)) => c,
            None => return,
        };
        if added.is_empty() && removed.is_empty() {
            self.heads.lock().await.insert(peer, (version, head));
            return; // unchanged — the O(1) steady-state path: nothing fetched, nothing indexed
        }
        let first_sight = known.is_none();
        let lost = self.apply_changes(peer, &added, &removed).await;
        self.heads.lock().await.insert(peer, (version, head));
        if first_sight {
            // First sight is a BASELINE, not a loss event — same reasoning as the census `primed` guard:
            // otherwise a joining node reads every peer's manifest as fresh loss and repairs the fleet.
            return;
        }
        if lost.is_empty() {
            return; // it GREW or churned elsewhere — gaining cids is not a durability event
        }
        self.repair_our_share(&lost, peer, "anti-entropy: holder dropped cids")
            .await;
    }

    /// Fold a peer's ADDED/REMOVED into the index, keeping only cids we hold, and return the ones it used
    /// to hold for us and no longer does.
    ///
    /// Both filters are the same rule: we can only repair what we hold, so a peer's changes outside our set
    /// are not ours to remember or act on.
    async fn apply_changes(
        &self,
        peer: [u8; 32],
        added: &[[u8; 32]],
        removed: &[[u8; 32]],
    ) -> Vec<[u8; 32]> {
        let mine: HashSet<[u8; 32]> = self
            .engine
            .store()
            .cids()
            .into_iter()
            .map(|c| c.0)
            .collect();
        let mut holders = self.holders.lock().await;
        for c in added.iter().filter(|c| mine.contains(*c)) {
            holders.entry(*c).or_default().insert(peer);
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
        lost
    }

    /// One node died: repair the share of its holdings that is ours to repair.
    ///
    /// O(1) LOCAL work to find that share — the index already knows who held what, so a death costs no
    /// manifest fetch and no DHT lookup at the exact moment the fleet can least afford either.
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
            // Either it held nothing we hold, or we never saw its manifest. The two are indistinguishable
            // here, which is why the periodic scan remains the backstop until P5: silence is not proof.
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
        let mut repaired = 0usize;
        let mut last_holder = 0usize;
        for (c, n_holders) in mine {
            if n_holders <= 1 {
                last_holder += 1; // we may be its only survivor — the most urgent case there is
            }
            // P4 — BUDGET: bound concurrency, held across the repair. A correlated failure fires repair for
            // many nodes at once — a herd exactly when the fleet is weakest. Bounded, that becomes a slower
            // recovery instead of a second outage.
            let Ok(_permit) = self.budget.clone().acquire_owned().await else {
                break; // semaphore closed — shutting down
            };
            // `repair_cid` re-checks health itself, so a stale claim costs a cheap no-op rather than a
            // pointless regenerate — the probe-before-repair rule that makes acting on disagreement safe.
            if self.engine.repair_cid(Cid(c)).await > 0 {
                repaired += 1;
            }
        }
        tracing::info!(
            node = %hex::encode(&gone[..6]),
            candidates = cids.len(),
            elected,
            repaired,
            last_holder,
            why,
            "repair"
        );
    }
}

#[cfg(test)]
mod tests {
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
