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
/// epochs, bounding record storage. A config value later; a sane default for now.
const CLAIM_WINDOW_EPOCHS: u64 = 8;

#[derive(Default)]
pub struct SettlementStore {
    /// Distributable pool (running): new pay-ins + rolled dust + expired forfeits.
    unallocated: u64,
    /// Published epoch records (the `owed` shares) within the claim window, by epoch.
    records: BTreeMap<u64, RewardRecord>,
    /// `(epoch, provider)` shares already claimed — so they neither re-claim nor forfeit-on-expiry.
    claimed: BTreeSet<(u64, [u8; 32])>,
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
    fn a_claimed_share_does_not_forfeit_on_expiry() {
        let mut s = SettlementStore::new();
        s.pay_in(50);
        s.settle_epoch(1, vec![contrib(1, 1)]);
        s.claim(1, prov(1)); // provider claimed its 50 (owed → paid, out of the pool)
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![]); // expire epoch 1
        assert_eq!(s.unallocated(), 0, "claimed shares don't double-forfeit");
    }
}
