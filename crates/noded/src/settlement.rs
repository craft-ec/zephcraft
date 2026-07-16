//! `SettlementStore` — the epoch-close settlement pool (ECONOMIC_LAYER_DESIGN.md §10.1; step 4 phase
//! 4d-3). It holds the **running pool** and drives the pay-into-pool / claim-out model:
//!
//! - **`unallocated`** — payments not yet assigned to any provider (new `Pay` pay-ins + integer dust +
//!   expired forfeits). The *only* thing a reward record distributes.
//! - **records** (`owed`) — per-epoch [`RewardRecord`]s within the claim window; a record's shares are
//!   *reserved* for their providers until claimed or expired.
//!
//! Each epoch, [`settle_epoch`](SettlementStore::settle_epoch) distributes the current `unallocated`
//! to that epoch's contributions by ratio (via the pure [`zeph_reward::compute`], so verifiers
//! reproduce it), moving `unallocated → owed`; dust stays `unallocated` and folds into the next epoch.
//! A provider [`claim`](SettlementStore::claim)s its share (`owed → paid`). Records older than the
//! **[`CLAIM_WINDOW_EPOCHS`]** window expire — their UNCLAIMED shares forfeit back to `unallocated` —
//! which bounds storage to the last N records. Conservation is total: every paid token is `claimed`,
//! `owed` (within window), or `unallocated`; `unallocated + owed = Σ pay-ins − Σ claims ≥ 0`.

use std::collections::{BTreeMap, BTreeSet};

use zeph_reward::{compute, Contribution, RewardInput, RewardRecord};

/// Governed claim window (§10.1): an unclaimed reward share forfeits back to the pool after this many
/// epochs, bounding record storage. A config value later; a sane default for now. Also the startup
/// reconstruction depth (replay this many epochs of durable reports to rebuild state).
pub const CLAIM_WINDOW_EPOCHS: u64 = 8;

#[derive(Default)]
pub struct SettlementStore {
    /// Distributable pool (running): new pay-ins + rolled dust + expired forfeits.
    unallocated: u64,
    /// Published epoch records (the `owed` shares) within the claim window, by epoch.
    records: BTreeMap<u64, RewardRecord>,
    /// `(epoch, provider)` shares already claimed — so they neither re-claim nor forfeit-on-expiry.
    claimed: BTreeSet<(u64, [u8; 32])>,
    /// Per-node CUMULATIVE `Pay` total already folded into the pool. An epoch folds only each node's
    /// `paid_cumulative − watermark` delta, advancing the watermark — so a pay is counted exactly once.
    paid_watermark: BTreeMap<[u8; 32], u64>,
    /// Per-node FIRST-SIGHT `Pay` baseline (§10.1 per-consumer path). A consumer's reward QUOTA is what it
    /// has folded into the pool = `paid_cumulative − paid_baseline` (deltas since first sight), so the
    /// quota can never exceed pool-funded value even for a node that joins with a historical `Pay` total.
    paid_baseline: BTreeMap<[u8; 32], u64>,
    /// Per-`(provider, consumer)` CUMULATIVE served bytes already processed for rewardable allocation
    /// (per-consumer path). Deltas past it are rewardable-eligible; monotonic (cheques are cumulative), so
    /// re-announcing the same cheque earns nothing twice. Safety is the quota cap, not a first-sight baseline.
    served_pair_wm: BTreeMap<([u8; 32], [u8; 32]), u64>,
    /// Per-consumer CUMULATIVE rewardable already allocated — the per-consumer CAP: once a consumer's
    /// allocated reaches its quota, further serving to it is subsidy (unrewarded). Guarantees
    /// `Σ rewarded-for-a-consumer ≤ what that consumer paid` → self-dealing nets ≤ its own payment (zero-sum).
    consumer_allocated: BTreeMap<[u8; 32], u64>,
    /// Per-provider CUMULATIVE rewardable served (Σ allocated to it across consumers) — the observable
    /// "settled" numerator of the dashboard settled/served meter.
    rewardable: BTreeMap<[u8; 32], u64>,
}

impl SettlementStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A consumer paid `amount` into the pool (from a committed `Pay` write) → `unallocated`.
    pub fn pay_in(&mut self, amount: u64) {
        self.unallocated = self.unallocated.saturating_add(amount);
    }

    /// The current distributable pool balance (for observability).
    pub fn unallocated(&self) -> u64 {
        self.unallocated
    }

    /// Close `epoch`: first expire out-of-window records (forfeiting their unclaimed shares back), then
    /// distribute the current `unallocated` to `contributions` by contribution ratio, publishing the
    /// record and moving `unallocated → owed` (Σ shares; the dust remainder stays `unallocated`).
    /// Returns the published record. Re-settling the same epoch is refused (returns the existing one).
    pub fn settle_epoch(&mut self, epoch: u64, contributions: Vec<Contribution>) -> RewardRecord {
        if let Some(existing) = self.records.get(&epoch) {
            return existing.clone(); // idempotent: an epoch settles once
        }
        self.expire(epoch);
        let record = compute(&RewardInput {
            epoch,
            pool: self.unallocated,
            contributions,
        });
        let allocated: u64 = record.shares.iter().map(|s| s.amount).sum();
        // Σ shares ≤ unallocated (guaranteed by `compute`), so this never underflows; dust stays.
        self.unallocated -= allocated;
        self.records.insert(epoch, record.clone());
        record
    }

    /// Cross-node epoch close with PER-CONSUMER FCFS reward capping (§10.1, the full model — supersedes the
    /// aggregate [`settle_epoch_cumulative`] on the production path). Instead of rewarding each node's raw
    /// aggregate served delta, it derives every provider's REWARDABLE-served from the board's cheques grouped
    /// by consumer: a consumer's paid quota is allocated FCFS (by cheque timestamp, provider/consumer
    /// tie-break) across the providers that served it, capped so `Σ rewarded-for-a-consumer ≤ what it paid`.
    /// Then the pool (Σ paid deltas) is distributed pool-average over the resulting rewardable bytes — both
    /// layers. Deterministic (converged board + stable sort) and verifiable (cheques consumer-signed, paid
    /// on-chain). Idempotent per epoch (watermarks advance once).
    ///
    /// - `paid` = each participant's `(node, paid_cumulative)` — funds the pool AND (as deltas past its
    ///   first-sight baseline) sets that node's reward quota when it acts as a consumer.
    /// - `cheques` = every `(provider, consumer, cumulative_bytes, timestamp)` from the board's proofs.
    pub fn settle_epoch_from_cheques(
        &mut self,
        epoch: u64,
        paid: Vec<([u8; 32], u64)>,
        mut cheques: Vec<([u8; 32], [u8; 32], u64, u64)>,
    ) -> RewardRecord {
        if let Some(existing) = self.records.get(&epoch) {
            return existing.clone(); // already settled → don't re-advance watermarks
        }
        // Pool + per-consumer quota. Pool grows by each node's paid delta past its watermark; a node's quota
        // (as a consumer) is what it has folded into the pool = paid_cumulative − first-sight baseline.
        let mut pool_add = 0u64;
        let mut quota: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        for (node, paid_cum) in &paid {
            let baseline = *self.paid_baseline.entry(*node).or_insert(*paid_cum);
            quota.insert(*node, paid_cum.saturating_sub(baseline));
            match self.paid_watermark.get(node).copied() {
                None => {
                    self.paid_watermark.insert(*node, *paid_cum); // baseline on first sight
                }
                Some(w) => {
                    pool_add = pool_add.saturating_add(paid_cum.saturating_sub(w));
                    self.paid_watermark.insert(*node, (*paid_cum).max(w));
                }
            }
        }
        // Allocate rewardable per provider: walk cheque DELTAS in a deterministic FCFS order and cap each
        // consumer's cumulative rewardable at its quota (over-quota serving = subsidy, unrewarded).
        cheques.sort_by(|a, b| (a.3, a.0, a.1).cmp(&(b.3, b.0, b.1)));
        let mut contrib: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        for (provider, consumer, cum, _ts) in cheques {
            let key = (provider, consumer);
            let w = self.served_pair_wm.get(&key).copied().unwrap_or(0);
            let delta = cum.saturating_sub(w);
            self.served_pair_wm.insert(key, cum.max(w));
            if delta == 0 {
                continue;
            }
            let q = quota.get(&consumer).copied().unwrap_or(0);
            let used = self.consumer_allocated.get(&consumer).copied().unwrap_or(0);
            let rewardable = delta.min(q.saturating_sub(used));
            if rewardable == 0 {
                continue;
            }
            *contrib.entry(provider).or_default() += rewardable;
            *self.consumer_allocated.entry(consumer).or_default() += rewardable;
            *self.rewardable.entry(provider).or_default() += rewardable;
        }
        self.pay_in(pool_add);
        let contributions: Vec<Contribution> = contrib
            .into_iter()
            .map(|(provider, bytes)| Contribution { provider, bytes })
            .collect();
        self.settle_epoch(epoch, contributions)
    }

    /// This provider's CUMULATIVE rewardable served bytes (the "settled" numerator of settled/served) —
    /// the portion of its serving that fell within consumers' paid quotas and thus earned reward.
    pub fn rewardable_served(&self, provider: &[u8; 32]) -> u64 {
        self.rewardable.get(provider).copied().unwrap_or(0)
    }

    /// DEV/manual settle from an explicit pool + contributions (bypasses watermarks). The production path
    /// is [`settle_epoch_from_cheques`]. Idempotent per epoch.
    pub fn settle_epoch_with_pool(
        &mut self,
        epoch: u64,
        pool_add: u64,
        contributions: Vec<Contribution>,
    ) -> RewardRecord {
        if let Some(existing) = self.records.get(&epoch) {
            return existing.clone(); // already settled → do NOT re-add the pool
        }
        self.pay_in(pool_add);
        self.settle_epoch(epoch, contributions)
    }

    /// This node's own computed record for `epoch` (from its settle re-execution), for verification
    /// against the canonical committee-attested record. `None` if it hasn't settled that epoch.
    pub fn record(&self, epoch: u64) -> Option<RewardRecord> {
        self.records.get(&epoch).cloned()
    }

    /// Total unclaimed reward this `provider` is owed across ALL in-window records (Σ `share_of`) — the
    /// dashboard "reward earned but not yet claimed" figure. Claimed shares already read 0, so they're
    /// excluded automatically.
    pub fn owed_to(&self, provider: &[u8; 32]) -> u64 {
        self.records
            .keys()
            .map(|e| self.share_of(*e, provider))
            .sum()
    }

    /// The unclaimed share owed to `provider` for `epoch` (0 if absent, already claimed, or expired) —
    /// what a `RewardClaim` resolves + credits.
    pub fn share_of(&self, epoch: u64, provider: &[u8; 32]) -> u64 {
        if self.claimed.contains(&(epoch, *provider)) {
            return 0;
        }
        self.records
            .get(&epoch)
            .map(|r| r.share_of(provider))
            .unwrap_or(0)
    }

    /// Mark `(epoch, provider)`'s share claimed (called after a `RewardClaim` commits), moving it out
    /// of `owed`. Idempotent.
    pub fn claim(&mut self, epoch: u64, provider: [u8; 32]) {
        self.claimed.insert((epoch, provider));
    }

    /// Expire records with `epoch < now − window`: their UNCLAIMED shares forfeit back to
    /// `unallocated` (the claimed ones already left `owed`), and the record + its claimed-markers drop.
    fn expire(&mut self, now_epoch: u64) {
        let cutoff = now_epoch.saturating_sub(CLAIM_WINDOW_EPOCHS);
        let stale: Vec<u64> = self
            .records
            .keys()
            .copied()
            .filter(|e| *e < cutoff)
            .collect();
        for e in stale {
            if let Some(rec) = self.records.remove(&e) {
                for s in &rec.shares {
                    if !self.claimed.contains(&(e, s.provider)) {
                        self.unallocated = self.unallocated.saturating_add(s.amount);
                    }
                }
            }
            self.claimed.retain(|(ep, _)| *ep != e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prov(n: u8) -> [u8; 32] {
        [n; 32]
    }

    fn contrib(p: u8, bytes: u64) -> Contribution {
        Contribution {
            provider: prov(p),
            bytes,
        }
    }

    #[test]
    fn pay_in_settle_and_claim_conserves_the_pool() {
        let mut s = SettlementStore::new();
        s.pay_in(100); // consumers paid 100 into the pool
        assert_eq!(s.unallocated(), 100);
        // Settle epoch 1: two providers 60/40 → shares 60/40; unallocated → 0 (no dust here).
        let rec = s.settle_epoch(1, vec![contrib(1, 60), contrib(2, 40)]);
        assert_eq!(rec.share_of(&prov(1)), 60);
        assert_eq!(s.unallocated(), 0); // all moved to `owed`
        assert_eq!(s.share_of(1, &prov(1)), 60);
        // Provider 1 claims → its owed share is gone; 2 still owed.
        s.claim(1, prov(1));
        assert_eq!(s.share_of(1, &prov(1)), 0);
        assert_eq!(s.share_of(1, &prov(2)), 40);
        // Re-settling epoch 1 is idempotent.
        assert_eq!(
            s.settle_epoch(1, vec![contrib(3, 999)]).share_of(&prov(1)),
            60
        );
    }

    #[test]
    fn dust_stays_unallocated_and_folds_into_the_next_epoch() {
        let mut s = SettlementStore::new();
        s.pay_in(10);
        // 3 equal providers → floor(10/3)=3 each, Σ=9, dust 1 stays unallocated.
        s.settle_epoch(1, vec![contrib(1, 1), contrib(2, 1), contrib(3, 1)]);
        assert_eq!(s.unallocated(), 1);
        // The rolled dust (1) is what the NEXT epoch distributes (no new pay-ins here).
        let rec = s.settle_epoch(2, vec![contrib(1, 5)]);
        assert_eq!(rec.share_of(&prov(1)), 1);
        assert_eq!(s.unallocated(), 0, "the dust was distributed, nothing left");
    }

    #[test]
    fn unclaimed_shares_forfeit_back_after_the_claim_window() {
        let mut s = SettlementStore::new();
        s.pay_in(50);
        s.settle_epoch(1, vec![contrib(1, 1)]); // provider 1 owed 50, never claims
        assert_eq!(s.unallocated(), 0);
        // Advance past the window: settling a far-future epoch expires epoch 1 → its 50 forfeits back.
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![]);
        assert_eq!(s.share_of(1, &prov(1)), 0, "expired record is gone");
        assert_eq!(
            s.unallocated(),
            50,
            "unclaimed 50 forfeited back to the pool"
        );
    }

    #[test]
    fn settle_with_pool_folds_aggregated_pays_and_is_idempotent() {
        let mut s = SettlementStore::new();
        // Cross-node close: feed Σ all nodes' announced pays (say 100) + the epoch's contributions.
        let rec = s.settle_epoch_with_pool(1, 100, vec![contrib(1, 60), contrib(2, 40)]);
        assert_eq!(rec.share_of(&prov(1)), 60);
        assert_eq!(s.unallocated(), 0, "the aggregated 100 was distributed");
        // Re-driving the same epoch must NOT re-add the pool (idempotent) — returns the same record.
        let again = s.settle_epoch_with_pool(1, 100, vec![contrib(1, 60), contrib(2, 40)]);
        assert_eq!(again.share_of(&prov(1)), 60);
        assert_eq!(s.unallocated(), 0, "no double pay-in on re-settle");
    }

    #[test]
    fn owed_to_sums_unclaimed_shares_across_records() {
        let mut s = SettlementStore::new();
        s.pay_in(100);
        s.settle_epoch(1, vec![contrib(1, 60), contrib(2, 40)]); // 1→60, 2→40
        s.pay_in(50);
        s.settle_epoch(2, vec![contrib(1, 1)]); // 1→50
                                                // Provider 1 is owed 60 (epoch 1) + 50 (epoch 2) across records; provider 2 owed 40.
        assert_eq!(s.owed_to(&prov(1)), 110);
        assert_eq!(s.owed_to(&prov(2)), 40);
        // Claiming epoch 1 drops it out of `owed`.
        s.claim(1, prov(1));
        assert_eq!(s.owed_to(&prov(1)), 50);
    }

    #[test]
    fn per_consumer_fcfs_caps_rewardable_at_what_the_consumer_paid() {
        let mut s = SettlementStore::new();
        let c = prov(9);
        // Epoch 1: consumer first seen at paid 0 → baselines (quota 0, pool 0), no serving yet.
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]);
        // Epoch 2: C has paid 100 (Δ100 → pool 100, quota 100). A served 60 @ts1, B served 80 @ts2.
        // FCFS by timestamp: A takes 60, B takes the remaining 40 of the 100 quota; B's other 40 = subsidy.
        let rec = s.settle_epoch_from_cheques(
            2,
            vec![(c, 100)],
            vec![(prov(1), c, 60, 1), (prov(2), c, 80, 2)],
        );
        assert_eq!(rec.share_of(&prov(1)), 60, "A within quota → full 60");
        assert_eq!(
            rec.share_of(&prov(2)),
            40,
            "B gets the remaining 40 of the quota"
        );
        assert_eq!(s.rewardable_served(&prov(1)), 60);
        assert_eq!(
            s.rewardable_served(&prov(2)),
            40,
            "B's other 40 served is subsidy"
        );
    }

    #[test]
    fn self_dealing_nets_at_most_what_was_paid() {
        let mut s = SettlementStore::new();
        let attacker = prov(5);
        s.settle_epoch_from_cheques(1, vec![(attacker, 0)], vec![]); // baseline
                                                                     // Attacker pays 100 and serves ITSELF 1000 bytes (sock-puppet consumer = itself).
        let rec = s.settle_epoch_from_cheques(
            2,
            vec![(attacker, 100)],
            vec![(attacker, attacker, 1000, 1)],
        );
        // Rewardable capped at the 100 it paid → it gets back ≤ what it put in (zero-sum, no profit).
        assert!(rec.share_of(&attacker) <= 100);
        assert_eq!(
            s.rewardable_served(&attacker),
            100,
            "capped at quota, rest subsidy"
        );
    }

    #[test]
    fn replayed_cheques_earn_nothing_twice_per_consumer() {
        let mut s = SettlementStore::new();
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]);
        s.settle_epoch_from_cheques(2, vec![(c, 100)], vec![(prov(1), c, 60, 1)]);
        assert_eq!(s.rewardable_served(&prov(1)), 60);
        // Epoch 3: same cheque (cum 60) + no new pay → zero delta, zero pool → nothing.
        let rec = s.settle_epoch_from_cheques(3, vec![(c, 100)], vec![(prov(1), c, 60, 1)]);
        assert!(
            rec.shares.is_empty(),
            "replayed cheque + no new pay = nothing"
        );
        assert_eq!(s.rewardable_served(&prov(1)), 60, "no double count");
    }

    #[test]
    fn serving_beyond_a_consumers_quota_is_unrewarded_subsidy() {
        let mut s = SettlementStore::new();
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]);
        // C paid 40 but was served 100 by one provider → only 40 rewardable, 60 subsidy.
        let rec = s.settle_epoch_from_cheques(2, vec![(c, 40)], vec![(prov(1), c, 100, 1)]);
        assert_eq!(rec.share_of(&prov(1)), 40);
        assert_eq!(s.rewardable_served(&prov(1)), 40, "capped at the 40 paid");
    }

    #[test]
    fn a_free_consumer_that_never_paid_rewards_nobody() {
        let mut s = SettlementStore::new();
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]);
        // C never paid (quota 0) but was served 500 → all subsidy, no reward, no pool.
        let rec = s.settle_epoch_from_cheques(2, vec![(c, 0)], vec![(prov(1), c, 500, 1)]);
        assert!(rec.shares.is_empty(), "free consumer funds no reward");
        assert_eq!(s.rewardable_served(&prov(1)), 0);
    }

    #[test]
    fn a_joining_nodes_historical_pay_is_baselined_not_dumped() {
        let mut s = SettlementStore::new();
        let c = prov(9);
        // C's FIRST appearance already carries a historical paid total of 1000 → baseline, quota 0, pool 0.
        let rec1 = s.settle_epoch_from_cheques(1, vec![(c, 1000)], vec![(prov(1), c, 500, 1)]);
        assert!(
            rec1.shares.is_empty(),
            "historical pay is baselined, not dumped into one epoch"
        );
        assert_eq!(s.unallocated(), 0);
        // Only NEW pay past the baseline becomes quota + pool.
        let rec2 = s.settle_epoch_from_cheques(2, vec![(c, 1200)], vec![(prov(1), c, 700, 2)]);
        assert_eq!(
            rec2.share_of(&prov(1)),
            200,
            "Δpaid 200 → quota 200 → 200 rewardable"
        );
    }

    #[test]
    fn a_claimed_share_does_not_forfeit_on_expiry() {
        let mut s = SettlementStore::new();
        s.pay_in(50);
        s.settle_epoch(1, vec![contrib(1, 1)]);
        s.claim(1, prov(1)); // provider claimed its 50 (owed → paid, out of the pool)
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![]); // expire epoch 1
        assert_eq!(s.unallocated(), 0, "claimed shares don't double-forfeit");
    }
}
