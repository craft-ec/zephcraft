//! `SettlementStore` — the epoch-close settlement pool (ECONOMIC_LAYER_DESIGN.md §10.1; step 4 phase
//! 4d-3) + the P6 per-consumer SUBSCRIPTION entitlements. It holds the **running pool** and drives the
//! pay-into-pool / claim-out model:
//!
//! - **`unallocated`** — payments not yet assigned to any provider (new `Pay` pay-ins + integer dust +
//!   expired forfeits). The *only* thing a reward record distributes.
//! - **records** (`owed`) — per-epoch [`RewardRecord`]s within the claim window; a record's shares are
//!   *reserved* for their providers until claimed or expired.
//!
//! Each epoch, [`settle_epoch`](SettlementStore::settle_epoch) distributes the current `unallocated`
//! to that epoch's contributions by ratio (via the pure [`zeph_reward::compute`], so verifiers
//! reproduce it), moving `unallocated → owed`; dust stays `unallocated` and folds into the next epoch.
//! One paid delta has TWO effects (P6): it funds the pool AND buys the payer a windowed egress
//! entitlement (`delta × bytes_per_token` bytes, expiring after the governed window) — serving a consumer
//! earns reward only within its unexpired entitlement, and unspent bytes are lost at the edge rather than
//! refunded, because those same tokens already funded providers' reward through the pool.
//!
//! A provider [`claim`](SettlementStore::claim)s its share (`owed → paid`). Records older than the
//! **[`CLAIM_WINDOW_EPOCHS`]** window expire — their UNCLAIMED shares forfeit back to `unallocated` —
//! which bounds storage to the last N records. Conservation is total: every paid token is `claimed`,
//! `owed` (within window), or `unallocated`; `unallocated + owed = Σ pay-ins − Σ claims ≥ 0`.

use std::collections::{BTreeMap, BTreeSet};

use zeph_economy_egress::subscription::{
    SubscriptionLedger, DEFAULT_BYTES_PER_TOKEN, DEFAULT_WINDOW,
};
use zeph_reward::{compute, Contribution, Entitlement, RewardInput, RewardRecord};

/// Governed claim window (§10.1): an unclaimed reward share forfeits back to the pool after this many
/// epochs, bounding record storage. A config value later; a sane default for now. Also the startup
/// reconstruction depth (replay this many epochs of durable reports to rebuild state).
pub const CLAIM_WINDOW_EPOCHS: u64 = 8;

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
    /// Per-`(provider, consumer)` CUMULATIVE served bytes already processed for rewardable allocation
    /// (per-consumer path). Deltas past it are rewardable-eligible; monotonic (cheques are cumulative), so
    /// re-announcing the same cheque earns nothing twice. Safety is the entitlement cap below.
    served_pair_wm: BTreeMap<([u8; 32], [u8; 32]), u64>,
    /// Per-consumer WINDOWED egress entitlement (P6 subscriptions, `zeph_economy_egress`): each epoch's
    /// paid delta buys `delta × bytes_per_token` bytes expiring `window` epochs later, and serving is
    /// rewardable only within it. This IS the per-consumer cap — it guarantees
    /// `Σ rewarded-for-a-consumer ≤ what that consumer's payments entitle` → self-dealing nets ≤ its own
    /// payment. Unused bytes expire (use-it-or-lose-it); the first-sight watermark keeps a joining node's
    /// historical `Pay` total from buying anything (delta from first sight = 0).
    subs: SubscriptionLedger,
    /// GOVERNED egress price: bytes of rewardable serving one token buys (`economy:bytes_per_token`).
    bytes_per_token: u64,
    /// Subscription window in EPOCHS, DERIVED from a duration (governed `economy:subscription_window_secs`,
    /// default 30 days) via `epoch::epochs_in` — so retuning the epoch period re-derives the window rather
    /// than silently rescaling it.
    window_epochs: u64,
    /// Per-provider CUMULATIVE rewardable served (Σ allocated to it across consumers) — the observable
    /// "settled" numerator of the dashboard settled/served meter.
    rewardable: BTreeMap<[u8; 32], u64>,
    /// GOVERNED seed rate in TOKENS PER DAY (`economy:issuance_tokens_per_day`). Kept as the rate in TIME
    /// — the reward program pays it against `epochs_per_day` on an exact schedule, so a sub-epoch rate
    /// (1 token/day is 1/288 at a 5min epoch) still pays exactly rather than flooring to nothing.
    issuance_tokens_per_day: u64,
    /// GOVERNED lifetime ceiling on cumulative fresh issuance, in tokens (`economy:issuance_total_cap`).
    issuance_total_cap: u64,
    /// Cumulative fresh issuance so far. RUNNING state, but this store is entirely in-memory (like
    /// `unallocated` and every watermark here), so it MUST be seeded from the durable records chain at
    /// startup — otherwise a restart would reset the lifetime supply cap and re-open minting. The record
    /// carries `cumulative_issued` precisely so the durable chain, not this field, is the source of truth.
    cumulative_issued: u64,
}

/// Epochs per day at this node's epoch period — the denominator the reward program's exact issuance
/// schedule pays against. Derived (not a constant) so retuning `EPOCH_MILLIS` cannot silently rescale
/// the real daily seed, the same discipline as `window_epochs`.
pub fn epochs_per_day() -> u64 {
    crate::epoch::epochs_in(core::time::Duration::from_secs(24 * 3600)).max(1)
}

impl Default for SettlementStore {
    fn default() -> Self {
        Self {
            unallocated: 0,
            records: BTreeMap::new(),
            claimed: BTreeSet::new(),
            paid_watermark: BTreeMap::new(),
            served_pair_wm: BTreeMap::new(),
            subs: SubscriptionLedger::new(),
            bytes_per_token: DEFAULT_BYTES_PER_TOKEN,
            window_epochs: crate::epoch::epochs_in(DEFAULT_WINDOW),
            rewardable: BTreeMap::new(),
            issuance_tokens_per_day: zeph_reward::DEFAULT_ISSUANCE_TOKENS_PER_DAY,
            issuance_total_cap: zeph_reward::DEFAULT_ISSUANCE_TOTAL_CAP,
            cumulative_issued: 0,
        }
    }
}

impl SettlementStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply the GOVERNED egress price (bytes one token buys). Takes effect for FUTURE purchases only —
    /// entitlements already bought keep the price they were bought at (no retroactive repricing).
    pub fn set_bytes_per_token(&mut self, bytes_per_token: u64) {
        self.bytes_per_token = bytes_per_token;
    }

    /// Apply the GOVERNED subscription window, given as a DURATION and converted to epochs here (the
    /// layer that knows the period). Future purchases only, same reasoning as the price.
    pub fn set_window(&mut self, window: std::time::Duration) {
        self.window_epochs = crate::epoch::epochs_in(window);
    }

    /// `consumer`'s remaining unexpired egress entitlement at `epoch`.
    ///
    /// Retained as the store's observable state + asserted by this module's tests; the DASHBOARD now
    /// reads this figure from the durable records chain instead (`my_view_from_records`), because a
    /// non-settling node has no local state to read.
    #[allow(dead_code)]
    pub fn entitlement(&self, consumer: &[u8; 32], epoch: u64) -> u64 {
        self.subs.available(consumer, epoch)
    }

    /// Apply the GOVERNED DEFAULT TIER: bytes of rewardable-serving entitlement every account holds per
    /// window without paying (0 = off). This is what lets the economy start from all-zero balances.
    pub fn set_default_tier(&mut self, bytes: u64) {
        let window = self.window_epochs;
        self.subs.set_default_tier(bytes, window);
    }

    /// Set the GOVERNED issuance schedule: the seed RATE in tokens per day, and the lifetime cap.
    pub fn set_issuance(&mut self, tokens_per_day: u64, total_cap: u64) {
        self.issuance_tokens_per_day = tokens_per_day;
        self.issuance_total_cap = total_cap;
    }

    /// Seed cumulative issuance from the DURABLE records chain (startup). Monotonic — takes the max, so a
    /// late or out-of-order chain read can only ever raise it, never re-open minting headroom.
    pub fn seed_cumulative_issued(&mut self, from_chain: u64) {
        self.cumulative_issued = self.cumulative_issued.max(from_chain);
    }

    /// Cumulative fresh issuance so far — observable state its own tests assert (same treatment as the
    /// other store observables). Not read by the binary yet; the natural consumer is a dashboard
    /// "issued supply" figure, which is worth surfacing once issuance actually runs on a fleet.
    #[allow(dead_code)]
    pub fn cumulative_issued(&self) -> u64 {
        self.cumulative_issued
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
    ///
    /// `entitlements` = each consumer's spend + resulting balance. Carried into the record so the attested
    /// artifact fully describes the epoch and any node can read its own view off it (see [`RewardRecord`]).
    pub fn settle_epoch(
        &mut self,
        epoch: u64,
        contributions: Vec<Contribution>,
        entitlements: Vec<Entitlement>,
    ) -> RewardRecord {
        if let Some(existing) = self.records.get(&epoch) {
            return existing.clone(); // idempotent: an epoch settles once
        }
        self.expire(epoch);
        let record = compute(&RewardInput {
            epoch,
            pool: self.unallocated,
            contributions,
            entitlements,
            cumulative_issued: self.cumulative_issued,
            issuance: zeph_reward::IssuanceParams {
                tokens_per_day: self.issuance_tokens_per_day,
                epochs_per_day: epochs_per_day(),
                total_cap: self.issuance_total_cap,
            },
        });
        // Fresh issuance (0 in steady state, and 0 whenever nobody contributed) enters the pool here, so
        // the shares below are funded by paid + issued. Taking it from the RECORD, not recomputing it,
        // keeps this node's books identical to what every verifier re-derives.
        self.unallocated = self.unallocated.saturating_add(record.issued);
        self.cumulative_issued = record.cumulative_issued;
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
        // Pool + subscriptions. ONE paid delta has both effects (P6): it funds the pool AND buys the
        // payer's windowed egress entitlement — the same tokens, priced into the pool-average, which is
        // exactly why an unused entitlement is never refunded (the reward it funded is already paid out).
        // First sight sets the watermark only: a node joining with a historical `Pay` total buys nothing.
        let mut pool_add = 0u64;
        for (node, paid_cum) in &paid {
            match self.paid_watermark.get(node).copied() {
                None => {
                    self.paid_watermark.insert(*node, *paid_cum); // first sight → delta 0
                }
                Some(w) => {
                    let delta = paid_cum.saturating_sub(w);
                    pool_add = pool_add.saturating_add(delta);
                    self.subs.purchase(
                        *node,
                        delta,
                        self.bytes_per_token,
                        epoch,
                        self.window_epochs,
                    );
                    self.paid_watermark.insert(*node, (*paid_cum).max(w));
                }
            }
        }
        // Allocate rewardable per provider: walk cheque DELTAS in a deterministic FCFS order, drawing each
        // consumer's serving from its unexpired entitlement, oldest grant first (serving past what the
        // consumer's subscription entitles = subsidy, unrewarded).
        cheques.sort_by(|a, b| (a.3, a.0, a.1).cmp(&(b.3, b.0, b.1)));
        let mut contrib: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        let mut spent: BTreeMap<[u8; 32], u64> = BTreeMap::new();
        for (provider, consumer, cum, _ts) in cheques {
            let key = (provider, consumer);
            let w = self.served_pair_wm.get(&key).copied().unwrap_or(0);
            let delta = cum.saturating_sub(w);
            self.served_pair_wm.insert(key, cum.max(w));
            if delta == 0 {
                continue;
            }
            let rewardable = self.subs.allocate(&consumer, delta, epoch);
            if rewardable == 0 {
                continue;
            }
            *contrib.entry(provider).or_default() += rewardable;
            // The consumer's entitlement that funded it — recorded so the epoch's attested summary says
            // whose subscription paid for the serving, not just who got paid.
            *spent.entry(consumer).or_default() += rewardable;
            *self.rewardable.entry(provider).or_default() += rewardable;
        }
        self.pay_in(pool_add);
        let contributions: Vec<Contribution> = contrib
            .into_iter()
            .map(|(provider, bytes)| Contribution {
                provider,
                bytes,
                // The running total AFTER this epoch's allocation (advanced in the loop above) — the
                // record carries it so a provider reads its settled figure from one row.
                cumulative_bytes: self.rewardable.get(&provider).copied().unwrap_or(0),
            })
            .collect();
        // A row for every consumer that SPENT or still HOLDS entitlement — an idle subscriber's balance
        // must stay visible, so it cannot be built from the spend map alone.
        let mut rows: BTreeMap<[u8; 32], (u64, u64)> = BTreeMap::new();
        for (consumer, bytes) in spent {
            rows.entry(consumer).or_default().0 = bytes;
        }
        for (consumer, remaining) in self.subs.balances(epoch) {
            rows.entry(consumer).or_default().1 = remaining;
        }
        let entitlements: Vec<Entitlement> = rows
            .into_iter()
            .map(|(consumer, (spent, remaining))| Entitlement {
                consumer,
                spent,
                remaining,
            })
            .collect();
        self.settle_epoch(epoch, contributions, entitlements)
    }

    /// This provider's CUMULATIVE rewardable served bytes (the "settled" numerator of settled/served) —
    /// the portion of its serving that fell within consumers' entitlements and thus earned reward. This
    /// is the value carried into each record as `Share::cumulative_bytes`.
    ///
    /// Retained as the store's observable state + asserted by this module's tests; the DASHBOARD reads it
    /// from the durable records chain instead (a non-settling node has no local state).
    #[allow(dead_code)]
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
        // DEV path: injects contributions directly, bypassing the per-consumer allocation, so there are
        // no entitlement spends to record.
        self.settle_epoch(epoch, contributions, Vec::new())
    }

    /// This node's own computed record for `epoch` (from its settle re-execution), for verification
    /// against the canonical committee-attested record. `None` if it hasn't settled that epoch.
    pub fn record(&self, epoch: u64) -> Option<RewardRecord> {
        self.records.get(&epoch).cloned()
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
            cumulative_bytes: 0, // these tests assert the pool/ratio, not the carried state
        }
    }

    #[test]
    fn pay_in_settle_and_claim_conserves_the_pool() {
        let mut s = SettlementStore::new();
        s.pay_in(100); // consumers paid 100 into the pool
        assert_eq!(s.unallocated(), 100);
        // Settle epoch 1: two providers 60/40 → shares 60/40; unallocated → 0 (no dust here).
        let rec = s.settle_epoch(1, vec![contrib(1, 60), contrib(2, 40)], vec![]);
        assert_eq!(rec.share_of(&prov(1)), 60);
        assert_eq!(s.unallocated(), 0); // all moved to `owed`
        assert_eq!(s.share_of(1, &prov(1)), 60);
        // Provider 1 claims → its owed share is gone; 2 still owed.
        s.claim(1, prov(1));
        assert_eq!(s.share_of(1, &prov(1)), 0);
        assert_eq!(s.share_of(1, &prov(2)), 40);
        // Re-settling epoch 1 is idempotent.
        assert_eq!(
            s.settle_epoch(1, vec![contrib(3, 999)], vec![])
                .share_of(&prov(1)),
            60
        );
    }

    /// Store-level integration of the §10.3 seed: scheduled fresh supply enters the pool, the shares
    /// divide paid + issued, and the lifetime counter advances by exactly what was minted — the counter
    /// being the thing that must survive to enforce the cap.
    #[test]
    fn the_seed_enters_the_pool_and_advances_the_counter() {
        let mut s = SettlementStore::new();
        // A rate of one token per epoch (rate = epochs/day) makes the per-epoch arithmetic explicit.
        s.set_issuance(epochs_per_day(), 1_000_000);
        s.pay_in(40); // consumers paid 40
        let rec = s.settle_epoch(1, vec![contrib(1, 1)], vec![]);
        assert_eq!(rec.issued, 1, "the epoch's scheduled seed");
        assert_eq!(
            rec.pool, 41,
            "the sole provider divides paid(40) + issued(1)"
        );
        assert_eq!(rec.share_of(&prov(1)), 41);
        assert_eq!(
            s.cumulative_issued(),
            1,
            "counter advanced by exactly the mint"
        );

        // Seeding from the durable chain is monotonic — it can only ever RAISE the counter, so a stale
        // or out-of-order read can never restore spent minting headroom.
        s.seed_cumulative_issued(0);
        assert_eq!(
            s.cumulative_issued(),
            1,
            "a lower chain read does not lower the counter"
        );
        s.seed_cumulative_issued(500);
        assert_eq!(s.cumulative_issued(), 500, "a higher chain read raises it");
    }

    /// The lifetime cap holds at the STORE level too: once cumulative issuance reaches it, settles stop
    /// minting entirely and the epoch distributes only what was actually paid.
    #[test]
    fn the_lifetime_cap_stops_minting_at_the_store_level() {
        let mut s = SettlementStore::new();
        s.set_issuance(epochs_per_day(), 1); // one token/epoch, lifetime cap of exactly 1
        s.pay_in(10);
        let rec = s.settle_epoch(1, vec![contrib(1, 1)], vec![]);
        assert_eq!(rec.issued, 1, "the cap's entire headroom");
        s.pay_in(10);
        let rec2 = s.settle_epoch(2, vec![contrib(1, 1)], vec![]);
        assert_eq!(rec2.issued, 0, "cap reached → no further minting, ever");
        assert_eq!(rec2.pool, 10, "the epoch distributes only what was paid");
    }

    #[test]
    fn dust_stays_unallocated_and_folds_into_the_next_epoch() {
        let mut s = SettlementStore::new();
        // Isolate REDISTRIBUTION: bootstrap issuance off, so the dust arithmetic under test is not also
        // being topped up by a subsidy. Issuance has its own tests in `zeph-reward`.
        s.set_issuance(0, 0);
        s.pay_in(10);
        // 3 equal providers → floor(10/3)=3 each, Σ=9, dust 1 stays unallocated.
        s.settle_epoch(1, vec![contrib(1, 1), contrib(2, 1), contrib(3, 1)], vec![]);
        assert_eq!(s.unallocated(), 1);
        // The rolled dust (1) is what the NEXT epoch distributes (no new pay-ins here).
        let rec = s.settle_epoch(2, vec![contrib(1, 5)], vec![]);
        assert_eq!(rec.share_of(&prov(1)), 1);
        assert_eq!(s.unallocated(), 0, "the dust was distributed, nothing left");
    }

    #[test]
    fn unclaimed_shares_forfeit_back_after_the_claim_window() {
        let mut s = SettlementStore::new();
        s.pay_in(50);
        s.settle_epoch(1, vec![contrib(1, 1)], vec![]); // provider 1 owed 50, never claims
        assert_eq!(s.unallocated(), 0);
        // Advance past the window: settling a far-future epoch expires epoch 1 → its 50 forfeits back.
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![], vec![]);
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
    fn per_consumer_fcfs_caps_rewardable_at_what_the_consumer_paid() {
        let mut s = SettlementStore::new();
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase free tier is off.
        s.set_default_tier(0);
        s.set_bytes_per_token(1); // unit price: 1 token = 1 byte, so the arithmetic below is the mechanics
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
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase free tier is off.
        s.set_default_tier(0);
        s.set_bytes_per_token(1); // unit price (the price cancels in pool-average — see the price test)
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
        s.set_bytes_per_token(1); // unit price
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
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase free tier is off.
        s.set_default_tier(0);
        s.set_bytes_per_token(1); // unit price
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]);
        // C paid 40 but was served 100 by one provider → only 40 rewardable, 60 subsidy.
        let rec = s.settle_epoch_from_cheques(2, vec![(c, 40)], vec![(prov(1), c, 100, 1)]);
        assert_eq!(rec.share_of(&prov(1)), 40);
        assert_eq!(s.rewardable_served(&prov(1)), 40, "capped at the 40 paid");
    }

    #[test]
    fn a_token_buys_bytes_per_token_bytes_of_rewardable_serving() {
        // P6: the governed price converts paid TOKENS into an egress BYTE budget. Before P6 the cap
        // compared bytes against tokens directly (an implicit 1 token = 1 byte).
        let mut s = SettlementStore::new();
        // STEADY STATE (post-seeding): the default tier is off, so entitlement must be PAID for. This is
        // the real end state — the free tier is a seeding-phase bootstrap that governance switches off.
        s.set_default_tier(0);
        // Isolate the PRICE property: no bootstrap issuance, so "the whole pool" is exactly what was paid.
        s.set_issuance(0, 0);
        s.set_bytes_per_token(1000);
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]); // first sight
                                                              // C pays 2 tokens → 2 × 1000 = 2000 bytes of entitlement.
        assert_eq!(
            s.entitlement(&c, 2),
            0,
            "not bought until the delta settles"
        );
        let rec = s.settle_epoch_from_cheques(2, vec![(c, 2)], vec![(prov(1), c, 1500, 1)]);
        assert_eq!(
            rec.share_of(&prov(1)),
            2,
            "the whole pool (2 paid) to the sole provider"
        );
        assert_eq!(
            s.rewardable_served(&prov(1)),
            1500,
            "1500 served is inside the 2000-byte entitlement → all rewardable"
        );
        assert_eq!(s.entitlement(&c, 2), 500, "500 bytes of subscription left");
        // Serving past the entitlement is subsidy: 900 more, only 500 rewardable.
        s.settle_epoch_from_cheques(3, vec![(c, 2)], vec![(prov(1), c, 2400, 2)]);
        assert_eq!(
            s.rewardable_served(&prov(1)),
            2000,
            "capped at the 2000 bought"
        );
        assert_eq!(s.entitlement(&c, 3), 0);
    }

    #[test]
    fn an_unused_entitlement_expires_and_is_never_refunded() {
        // P6 use-it-or-lose-it: the tokens were folded into the pool and already priced into everyone's
        // pool-average, so an unspent subscription is LOST at the window edge, not refunded.
        let mut s = SettlementStore::new();
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase free tier is off.
        s.set_default_tier(0);
        s.set_bytes_per_token(10);
        s.set_window(std::time::Duration::from_millis(
            crate::epoch::EPOCH_MILLIS * 5,
        )); // 5 epochs
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]); // first sight
        s.settle_epoch_from_cheques(2, vec![(c, 10)], vec![]); // buys 100 B at epoch 2, expires at 7
        assert_eq!(s.entitlement(&c, 6), 100, "alive inside the window");
        assert_eq!(s.entitlement(&c, 7), 0, "dead at the window edge");
        // Serving after expiry earns nothing — the entitlement is gone, not refunded or carried.
        let rec = s.settle_epoch_from_cheques(8, vec![(c, 10)], vec![(prov(1), c, 50, 1)]);
        assert!(
            rec.shares.is_empty(),
            "expired entitlement → no rewardable serving"
        );
        assert_eq!(s.rewardable_served(&prov(1)), 0);
    }

    #[test]
    fn a_free_consumer_that_never_paid_rewards_nobody() {
        let mut s = SettlementStore::new();
        // STEADY STATE (post-seeding): the default tier is off, so entitlement must be PAID for. This is
        // the real end state — the free tier is a seeding-phase bootstrap that governance switches off.
        s.set_default_tier(0);
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
        // STEADY STATE (post-seeding): the default tier is off, so entitlement must be PAID for. This is
        // the real end state — the free tier is a seeding-phase bootstrap that governance switches off.
        s.set_default_tier(0);
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
        s.settle_epoch(1, vec![contrib(1, 1)], vec![]);
        s.claim(1, prov(1)); // provider claimed its 50 (owed → paid, out of the pool)
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![], vec![]); // expire epoch 1
        assert_eq!(s.unallocated(), 0, "claimed shares don't double-forfeit");
    }
}
