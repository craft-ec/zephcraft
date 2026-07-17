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

/// What we last saw a peer assert: `(head_cid, holdings)`. The head is the cheap change signal; the set is
/// only needed to diff when the head moves.
type PeerHoldings = HashMap<[u8; 32], ([u8; 32], HashSet<[u8; 32]>)>;

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
    /// Last seen `(head_cid, holdings)` per peer — the anti-entropy baseline (P3).
    ///
    /// The head cid is the cheap signal: unchanged cid ⇒ unchanged holdings ⇒ nothing to do, no set data
    /// moved. The cached set is only needed to DIFF when a head does change.
    ///
    /// KNOWN LIMIT (design's "manifest size" gap): this holds every watched peer's full set, so memory is
    /// O(N_nodes × N_cids). Acceptable at this fleet's scale and the reason a Merkle root + diffs must land
    /// before a large store — the diff should come from the tree, not from a local mirror of the fleet.
    seen: tokio::sync::Mutex<PeerHoldings>,
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
            seen: tokio::sync::Mutex::new(HashMap::new()),
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

    /// One peer: has its holdings set changed, and did it LOSE anything we can repair?
    async fn check_peer(&self, peer: [u8; 32]) {
        let Some((head, _version)) = self.manifests.peer_head(peer).await else {
            return; // no manifest published (yet) — nothing to compare against
        };
        {
            let seen = self.seen.lock().await;
            if let Some((prev_head, _)) = seen.get(&peer) {
                if *prev_head == head {
                    return; // unchanged head ⇒ unchanged holdings. The O(1) steady-state path.
                }
            }
        }
        // The head moved ⇒ the set changed. NOW pay for the set (the only time we do).
        let Some(manifest) = self.manifests.fetch(peer).await else {
            return;
        };
        let now: HashSet<[u8; 32]> = manifest.cids.iter().copied().collect();
        let lost: Vec<[u8; 32]> = {
            let seen = self.seen.lock().await;
            match seen.get(&peer) {
                // First sight is a BASELINE, not a loss event — same reasoning as the census `primed`
                // guard: without it, a node joining would read every peer's manifest as a fresh loss.
                None => Vec::new(),
                Some((_, prev)) => prev.difference(&now).copied().collect(),
            }
        };
        self.seen.lock().await.insert(peer, (head, now));
        if lost.is_empty() {
            return; // it GREW (or churned) — gaining cids is not a durability event
        }
        self.repair_our_share(&lost, peer, "anti-entropy: holder dropped cids")
            .await;
    }

    /// One node died: repair the share of its holdings that is ours to repair.
    async fn on_death(&self, dead: [u8; 32]) {
        // 1. The dead node's holdings — ONE fetch for the whole set, not one lookup per cid. This is the
        //    difference between O(N_cids) and O(1) network calls to learn what was lost.
        let Some(manifest) = self.manifests.fetch(dead).await else {
            // No manifest (never published, expired, or unverifiable) → we cannot know what it held. The
            // periodic scan remains the backstop for exactly this until P3; silence here is not "nothing
            // was lost", it is "we cannot tell".
            tracing::warn!(node = %hex::encode(&dead[..6]), "death: no verifiable manifest — falling back to the scan");
            return;
        };

        // 2. Intersect LOCALLY. We only consider cids we already hold pieces for: nothing to ask anyone,
        //    and the work partitions itself across the survivors by construction.
        let mine: HashSet<[u8; 32]> = self
            .engine
            .store()
            .cids()
            .into_iter()
            .map(|c| c.0)
            .collect();
        let shared: Vec<[u8; 32]> = manifest
            .cids
            .iter()
            .copied()
            .filter(|c| mine.contains(c))
            .collect();
        if shared.is_empty() {
            return; // it held nothing we hold — not our share (or already-lost; see the design's gap)
        }

        // 3+4. Elect + repair our share (shared with the anti-entropy path — the invariants live in ONE
        //       place so the two triggers cannot drift apart and double-repair or skip).
        self.repair_our_share(&shared, dead, "death-driven repair")
            .await;
    }

    /// Given cids that just lost a holder (`gone`), repair the subset this node is elected for.
    ///
    /// Shared by both triggers: a death and an anti-entropy shrink are the same problem — a holder
    /// disappeared from some cids — and must not grow two subtly different election paths.
    async fn repair_our_share(&self, cids: &[[u8; 32]], gone: [u8; 32], why: &str) {
        // Who else survives holding these? From peers' MANIFESTS — O(N_nodes) fetches for the whole event,
        // not O(cids) DHT lookups. `liveness_census` is Alive-only, so a Suspect peer is not elected to act.
        let alive = self.membership.liveness_census().await;
        let want: HashSet<[u8; 32]> = cids.iter().copied().collect();
        let mut holders: HashMap<[u8; 32], Vec<[u8; 32]>> = HashMap::new();
        for c in cids {
            holders.insert(*c, vec![self.me]); // we hold all of these by construction (we intersected)
        }
        for (peer, _) in alive {
            if peer.0 == self.me || peer.0 == gone {
                continue;
            }
            let Some(pm) = self.manifests.fetch(peer.0).await else {
                continue; // no manifest → not a candidate; it cannot be elected to act
            };
            for c in pm.cids.iter().filter(|c| want.contains(*c)) {
                holders.entry(*c).or_default().push(peer.0);
            }
        }
        // Our elected share, carrying each cid's SURVIVING holder count.
        let mut mine: Vec<([u8; 32], usize)> = cids
            .iter()
            .filter_map(|c| {
                let cands = holders.get(c)?;
                (repairer_for(c, cands) == Some(self.me)).then_some((*c, cands.len()))
            })
            .collect();

        // P4 — PRIORITY: fewest surviving holders FIRST. Repairing in discovery order spends the budget on
        // comfortable cids while the ones nearest the k floor wait; under a correlated failure that is
        // exactly how data is lost while the fleet looks busy. Ordering by ACTUAL redundancy means the
        // budget always buys the most durability available.
        mine.sort_by_key(|(_, holders)| *holders);

        let elected = mine.len();
        let mut repaired = 0usize;
        let mut last_holder = 0usize;
        for (c, n_holders) in mine {
            if n_holders <= 1 {
                last_holder += 1; // we may be the only survivor holding it — the most urgent case there is
            }
            // P4 — BUDGET: bound concurrency, held across the repair. A correlated failure (rack/AZ, or the
            // 19-node freeze in the tracker) fires repair for many nodes at once — a herd precisely when the
            // fleet is weakest. Bounding it turns that into a slower recovery instead of a second outage.
            let Ok(_permit) = self.budget.clone().acquire_owned().await else {
                break; // semaphore closed — shutting down
            };
            // `repair_cid` re-checks health itself, so a stale manifest costs a cheap no-op rather than a
            // pointless regenerate — the design's probe-before-repair rule, and what makes acting on a
            // disagreement safe.
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
