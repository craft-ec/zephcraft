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
    /// The protocol's LITERAL holdings: every token the protocol has, plus the supply counter that
    /// gates minting (`zeph_token::ProtocolState`). Replaces a bare `unallocated: u64` that was a
    /// DERIVED figure — under which `Pay` destroyed tokens and `RewardClaim` created them.
    ///
    /// The pool holds settled-but-unclaimed shares too; those are an EARMARK recorded in `records`, not a
    /// separate balance. So the distributable figure is `pool − owed` ([`Self::unallocated`]), and expiry
    /// moves no value at all — it just drops an earmark that was always backed by pool tokens.
    protocol: zeph_token::ProtocolState,
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
    /// Σ shares claimed since the last settle — value that has left the pool but is not yet reflected in
    /// a canonical record. Folded into the NEXT record via `RewardInput::claim_payouts`, which is what
    /// makes the pool a function of the chain rather than of whoever processed the claim.
    pending_payouts: u64,
    /// Pay-ins folded since the last record — the recurrence's income term, carried as
    /// `RewardInput::paid` so the pool stays a function of the chain.
    pending_paid: u64,
    /// The snapshot the most recent epoch record COMMITTED to.
    ///
    /// The live position keeps moving after an epoch closes — a claim moves value out of the pool — so
    /// the live state deliberately does NOT match the record's `state_hash`. Persisting and verifying
    /// against the live state would report a FALSE divergence on any node that has claimed since the
    /// last settle, which is precisely the failure the check exists to detect. So it compares like with
    /// like: the committed point-in-time state.
    committed: zeph_reward::EconomicSnapshot,
    /// GOVERNED seed rate in TOKENS PER DAY (`economy:issuance_tokens_per_day`). Kept as the rate in TIME
    /// — the reward program pays it against `epochs_per_day` on an exact schedule, so a sub-epoch rate
    /// (1 token/day is 1/288 at a 5min epoch) still pays exactly rather than flooring to nothing.
    issuance_tokens_per_day: u64,
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
            committed: zeph_reward::EconomicSnapshot::default(),
            pending_payouts: 0,
            pending_paid: 0,
            protocol: zeph_token::ProtocolState::new(
                zeph_token::SupplyState::new(0, zeph_reward::DEFAULT_ISSUANCE_TOTAL_CAP),
                0,
            ),
            records: BTreeMap::new(),
            claimed: BTreeSet::new(),
            paid_watermark: BTreeMap::new(),
            served_pair_wm: BTreeMap::new(),
            subs: SubscriptionLedger::new(),
            bytes_per_token: DEFAULT_BYTES_PER_TOKEN,
            window_epochs: crate::epoch::epochs_in(DEFAULT_WINDOW),
            rewardable: BTreeMap::new(),
            issuance_tokens_per_day: zeph_reward::DEFAULT_ISSUANCE_TOKENS_PER_DAY,
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

    /// The position the most recent record committed to — what to persist and verify against.
    pub fn committed_snapshot(&self) -> zeph_reward::EconomicSnapshot {
        self.committed.clone()
    }

    /// Capture the COMPLETE economic position — token side and reward side — for the epoch record.
    ///
    /// This is what makes the economy restart-safe. Everything here was previously in-memory only, so a
    /// restart silently handed every account a fresh seeding allowance, forgot every token the pool held,
    /// dropped payments made while the node was down, and could re-debit an already-claimed share.
    pub fn snapshot(&self) -> zeph_reward::EconomicSnapshot {
        zeph_reward::EconomicSnapshot {
            pool: self.protocol.pool(),
            minted: self.protocol.total_supply(),
            paid_watermarks: self.paid_watermark.iter().map(|(k, v)| (*k, *v)).collect(),
            served_watermarks: self.served_pair_wm.iter().map(|(k, v)| (*k, *v)).collect(),
            seeding_next: self.subs.seeding_eligibility(),
            claimed: self.claimed.iter().map(|(e, p)| (*e, *p)).collect(),
        }
    }

    /// Restore the complete economic position from a durable, committee-attested record.
    ///
    /// Supply is restored MONOTONICALLY (it can only rise), so a stale read can never hand back spent
    /// minting headroom. Everything else is authoritative: the record is the agreed state, and a node
    /// rejoining adopts it rather than trusting whatever its own memory happened to hold.
    pub fn restore(&mut self, snap: &zeph_reward::EconomicSnapshot) {
        self.protocol = zeph_token::ProtocolState::new(
            zeph_token::SupplyState::new(
                self.protocol.total_supply().max(snap.minted),
                self.protocol.supply().cap(),
            ),
            snap.pool,
        );
        self.paid_watermark = snap.paid_watermarks.iter().copied().collect();
        self.served_pair_wm = snap.served_watermarks.iter().copied().collect();
        self.subs.restore_seeding_eligibility(&snap.seeding_next);
        self.claimed = snap.claimed.iter().copied().collect();
    }

    /// Apply the GOVERNED DEFAULT TIER: bytes of rewardable-serving entitlement every account holds per
    /// window without paying (0 = off). This is what lets the economy start from all-zero balances.
    pub fn set_seeding_paid_tier(&mut self, bytes: u64) {
        let window = self.window_epochs;
        self.subs.set_seeding_paid_tier(bytes, window);
    }

    /// Set the GOVERNED issuance schedule: the seed RATE in tokens per day, and the lifetime cap.
    pub fn set_issuance(&mut self, tokens_per_day: u64, total_cap: u64) {
        self.issuance_tokens_per_day = tokens_per_day;
        // The CAP belongs to the token, not to this layer — same reasoning as the counter. Keeping a
        // second copy here is how the counter bug happened, and a duplicated cap is worse: the compute
        // check and the mint gate would disagree about how much may exist.
        self.protocol.supply_mut().set_cap(total_cap);
    }


    /// Fold a committed `Pay` write into the pool — the CREDIT half of a transfer whose debit already
    /// happened on the payer's own chain. Supply is unchanged: this MOVES tokens (the old code destroyed
    /// them, since nothing received the debit).
    pub fn pay_in(&mut self, amount: u64) {
        // Accumulate rather than folding into the pool directly: the pool is set from the CANONICAL
        // record at settle, so a local fold here would be double-counted when the recurrence adds
        // `paid` again — and would diverge from nodes that did not see this pay.
        self.pending_paid = self.pending_paid.saturating_add(amount);
    }

    /// Settled-but-unclaimed shares still sitting in the pool — the earmarked portion.
    fn owed_outstanding(&self) -> u64 {
        self.records
            .iter()
            .flat_map(|(e, rec)| {
                rec.shares
                    .iter()
                    .filter(move |s| !self.claimed.contains(&(*e, s.provider)))
            })
            .map(|s| s.amount)
            .fold(0u64, |a, b| a.saturating_add(b))
    }

    /// Total protocol holdings (pool) and total supply — for the conservation check and observability.
    pub fn pool_total(&self) -> u64 {
        self.protocol.pool()
    }

    pub fn total_supply(&self) -> u64 {
        self.protocol.total_supply()
    }

    /// The DISTRIBUTABLE balance: pool tokens not already earmarked as owed to a provider.
    pub fn unallocated(&self) -> u64 {
        self.protocol
            .pool()
            .saturating_add(self.pending_paid)
            .saturating_sub(self.owed_outstanding())
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
            pool: self.unallocated(),
            contributions,
            entitlements,
            cumulative_issued: self.protocol.total_supply(),
            // Payouts since the last record — folded here so the pool recurrence is closed inside the
            // pure function rather than by a local side effect.
            claim_payouts: self.pending_payouts,
            paid: self.pending_paid,
            state: self.snapshot(),
            issuance: zeph_reward::IssuanceParams {
                tokens_per_day: self.issuance_tokens_per_day,
                epochs_per_day: epochs_per_day(),
                total_cap: self.protocol.supply().cap(),
            },
        });
        // MINT the seed into the pool for real, through the token's supply gate — the ONE operation in
        // the system that creates tokens. `mint_into_pool` both advances the token-owned supply counter
        // and credits the pool, so the cap is structural rather than a convention this layer honours.
        // OVERDRAW GUARD. The local per-claim debit is gone (it was wrong under committee-gating), so
        // the protection has to live where the subtraction now happens. Structurally payouts cannot
        // exceed holdings — Σ shares ≤ distributable ≤ pool, and a claim cannot exceed its share — but
        // the recurrence uses `saturating_sub`, which would SILENTLY floor a violation to zero rather
        // than lose money loudly. If this ever fires, claims have been paid that the pool never held.
        let backing = self
            .protocol
            .pool()
            .saturating_add(self.pending_paid)
            .saturating_add(record.issued);
        if self.pending_payouts > backing {
            tracing::error!(
                epoch,
                payouts = self.pending_payouts,
                backing,
                "OVERDRAWN POOL: claims paid out exceed what the pool held — supply accounting is wrong"
            );
        }
        // Advance the SUPPLY counter through the token's gate. Its pool credit is immediately superseded
        // by the canonical figure below — deliberately: minting is the token's business (it is what may
        // create supply and what the cap binds), while the resulting POOL is the chain's business.
        let minted = self.protocol.mint_into_pool(record.issued);
        // The pool is now whatever the RECORD says it is — the canonical recurrence, not this node's
        // accumulation. Adopting the record's figure is what keeps a node that settles only a sample of
        // epochs in agreement with one that settles all of them.
        self.protocol.set_pool(record.state_pool());
        self.pending_payouts = 0;
        self.pending_paid = 0;
        // A REAL check, not a `debug_assert`: this is a money invariant, and a debug assertion is
        // compiled out of release builds — exactly where it would matter. Today it holds by construction
        // (the cap fed to `compute` is read from this same `protocol`, with no await or mutation
        // between), so this costs nothing; it exists so a future refactor that inserts one cannot break
        // it silently. If the gate ever grants less than the record committed to, the record promises
        // shares the pool cannot back — claims would start being refused — so it must be loud.
        if minted != record.issued {
            tracing::error!(
                epoch,
                committed = record.issued,
                minted,
                "ISSUANCE MISMATCH: the pool minted less than the record committed to — shares in this \
                 epoch are not fully backed and claims against them will be refused"
            );
        }
        // Σ shares are NOT deducted from the pool here: they stay in it, EARMARKED as owed via `records`.
        // The tokens only leave the protocol when a provider actually claims (`claim` → debit). That is
        // what makes `Σ balances + pool == total_supply` hold — the old code moved a number between two
        // buckets while the actual value was minted at claim time out of nothing.
        // Capture exactly what this record committed to, BEFORE any later claim moves value. This is
        // what gets persisted and what verification compares — comparing the live state instead would
        // flag every node that has claimed since.
        self.committed = self.snapshot();
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
    /// A provider claims its share for `epoch`: the tokens LEAVE the pool for that provider's balance.
    ///
    /// Returns the amount actually released (0 if there was nothing owed, it was already claimed, or the
    /// pool could not cover it). The debit is the point: under the old model a claim moved no value here
    /// at all — the provider's balance was credited from NOTHING on its own chain — so an over-large or
    /// duplicated share simply created supply. Now a claim can only ever hand out tokens the pool holds.
    pub fn claim(&mut self, epoch: u64, provider: [u8; 32]) -> u64 {
        let Some(rec) = self.records.get(&epoch) else {
            return 0; // no such epoch (never settled, or already expired out of the window)
        };
        let share = rec.share_of(&provider);
        if share == 0 || self.claimed.contains(&(epoch, provider)) {
            return 0; // nothing owed, or single-use already spent
        }
        // Record the payout for the NEXT record to fold; do NOT debit the local pool here.
        //
        // The debit used to happen locally, which was wrong under committee-gated settlement: the CREDIT
        // resolves from the canonical record (any node can see it), while this path only runs where the
        // node had settled the epoch itself — a minority. So the common claim credited without ever
        // debiting, and `Σ balances + pool` drifted above `total_supply` by the share, every time.
        // Accounting for it in the epoch record instead means every node derives the same pool.
        self.pending_payouts = self.pending_payouts.saturating_add(share);
        self.claimed.insert((epoch, provider));
        share
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
                // Unclaimed shares FORFEIT by simply ceasing to be earmarked — the tokens were in the
                // pool the whole time, so nothing moves and `unallocated()` rises on its own once the
                // record is gone. The old code added the amount back to a derived counter, which only
                // worked because that counter was fictional.
                let _ = &rec;
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
        // Conservation of PAID value: no seed, so `unallocated + owed` is exactly what was paid in.
        s.set_issuance(0, 0);
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

    /// THE PROPERTY THIS REWORK EXISTS FOR: a node that did NOT settle an epoch derives the SAME pool as
    /// one that did.
    ///
    /// Under committee-gated settlement (`should_settle` = committee OR a 1-in-4 sample) a node observes
    /// only a sample of epochs, so a locally-accumulated pool is simply wrong — and the review found the
    /// concrete consequence: a claim resolved its CREDIT from the canonical record while its DEBIT only
    /// happened on a node that had settled that epoch locally, so the common claim credited without ever
    /// debiting. The pool is now a function of the chain: `pool(E) = pool(E-1) + paid + issued − payouts`,
    /// every term carried in the record, so any node adopting the record adopts the same pool.
    #[test]
    fn a_node_that_never_settled_derives_the_same_pool_from_the_record() {
        let mut settler = SettlementStore::new();
        settler.set_issuance(1000 * epochs_per_day(), 1_000_000_000);
        settler.pay_in(5_000);
        let rec = settler.settle_epoch(1, vec![contrib(1, 3), contrib(2, 1)], vec![]);

        // A DIFFERENT node that settled nothing — no pay-ins folded, no records of its own.
        let mut observer = SettlementStore::new();
        observer.set_issuance(1000 * epochs_per_day(), 1_000_000_000);
        assert_eq!(observer.pool_total(), 0, "it has accumulated nothing itself");

        // Adopting the canonical record's committed state gives it the settler's pool exactly.
        observer.restore(&zeph_reward::EconomicSnapshot {
            pool: rec.state_pool(),
            minted: rec.cumulative_issued,
            ..Default::default()
        });
        assert_eq!(
            observer.pool_total(),
            settler.pool_total(),
            "a non-settling node derives the SAME pool — which local accumulation could never give it"
        );
        assert_eq!(observer.total_supply(), settler.total_supply(), "and the same supply");
    }

    /// A claim no longer debits the pool locally; it accrues a payout that the NEXT record folds. That is
    /// what makes the debit happen for every node rather than only the one that settled the epoch.
    #[test]
    fn a_claim_accrues_a_payout_that_the_next_record_folds() {
        let mut s = SettlementStore::new();
        s.set_issuance(1000 * epochs_per_day(), 1_000_000_000);
        s.pay_in(5_000);
        let rec1 = s.settle_epoch(1, vec![contrib(1, 3), contrib(2, 1)], vec![]);
        let pool_after_settle = s.pool_total();

        let got = s.claim(1, prov(1));
        assert!(got > 0, "there was a share to claim");
        assert_eq!(
            s.pool_total(),
            pool_after_settle,
            "the claim does NOT debit locally — the pool still reads the last canonical figure"
        );

        // The NEXT record folds the payout, and the pool drops by exactly it.
        let rec2 = s.settle_epoch(2, vec![contrib(1, 1)], vec![]);
        assert_eq!(
            rec2.state_pool(),
            rec1.state_pool() + rec2.issued - got,
            "pool(E) = pool(E-1) + paid(0) + issued - payouts — the recurrence, in the record"
        );
        assert_eq!(s.pool_total(), rec2.state_pool(), "and the node adopts it");
    }

    /// THE end-to-end conservation invariant — now an EPOCH-BOUNDARY property, which is what deriving
    /// the pool from the chain costs and buys.
    ///
    /// A claim no longer debits the pool the instant it happens; it accrues a payout that the NEXT record
    /// folds. So between records the pool is stale by exactly the pending payouts, and conservation reads
    /// `balances + (pool − pending payouts) == supply`. That is not a weakening: the old instant debit
    /// only ran on a node that had settled the epoch locally, so under committee-gating the common claim
    /// credited without ever debiting. Boundary-accurate everywhere beats instant-accurate on a minority
    /// of nodes and silently wrong on the rest.
    #[test]
    fn every_token_is_either_in_a_balance_or_in_the_pool_at_every_record_boundary() {
        let mut s = SettlementStore::new();
        s.set_issuance(1000 * epochs_per_day(), 1_000_000_000);
        let mut balances = 0u64; // value that has left the protocol onto providers' own chains
        // Conservation, accounting for payouts not yet folded into a record.
        let check = |bal: u64, st: &SettlementStore, step: &str| {
            assert_eq!(
                bal + st.pool_total() - st.pending_payouts,
                st.total_supply(),
                "conservation broken at: {step}"
            );
        };
        check(balances, &s, "genesis");

        // SETTLE — the seed is MINTED. The only creation point in the system.
        let rec1 = s.settle_epoch(1, vec![contrib(1, 3), contrib(2, 1)], vec![]);
        assert_eq!(rec1.issued, 1000, "the epoch's scheduled seed");
        assert_eq!(s.total_supply(), 1000, "supply rose by exactly the mint");
        assert_eq!(s.pool_total(), 1000, "and the pool holds it, per the record");
        check(balances, &s, "after the seed mint");

        // CLAIM — accrues a payout; the pool still reads the last canonical figure.
        let got = s.claim(1, prov(1));
        assert_eq!(got, 750, "3:1 ratio over the 1000 distributable");
        balances += got;
        assert_eq!(s.total_supply(), 1000, "claiming MOVES value, never creates it");
        assert_eq!(s.pool_total(), 1000, "not debited locally — that is the whole point");
        check(balances, &s, "after the claim (payout pending)");

        // Double-claim is refused and accrues nothing further.
        assert_eq!(s.claim(1, prov(1)), 0, "single-use");
        check(balances, &s, "after a refused double-claim");

        // NEXT RECORD folds the payout: the pool drops by exactly it, and the boundary is exact.
        let rec2 = s.settle_epoch(2, vec![contrib(1, 1)], vec![]);
        assert_eq!(
            rec2.state_pool(),
            rec1.state_pool() + rec2.issued - got,
            "pool(E) = pool(E-1) + paid + issued - payouts"
        );
        assert_eq!(s.pending_payouts, 0, "folded, so nothing is outstanding");
        check(balances, &s, "after the next record folded the payout");
        assert_eq!(s.total_supply(), 1000 + rec2.issued, "supply only ever rises by mints");
    }

    /// Payouts can never exceed what the pool held. The protection MOVED when the per-claim local debit
    /// was removed: it now lives at the recurrence, because that is where the subtraction happens (with a
    /// loud guard, since `saturating_sub` would otherwise floor a violation to zero silently).
    /// Structurally it cannot be violated — Σ shares ≤ distributable ≤ pool, and a claim cannot exceed
    /// its own share — so this pins the structural fact rather than a rejection path.
    #[test]
    fn payouts_never_exceed_what_the_pool_held() {
        let mut s = SettlementStore::new();
        s.set_issuance(0, 0);
        s.pay_in(1_000);
        let rec = s.settle_epoch(1, vec![contrib(1, 3), contrib(2, 1)], vec![]);
        let total_shares: u64 = rec.shares.iter().map(|x| x.amount).sum();
        let a = s.claim(1, prov(1));
        let b = s.claim(1, prov(2));
        assert_eq!(a + b, total_shares, "claimed exactly what was owed");
        assert!(
            a + b <= rec.state_pool(),
            "and the payouts are covered by the pool the record committed to"
        );
    }

    /// The token scale is mirrored in three crates (`zeph_token` canonical, `zeph_reward` and
    /// `zeph_economy_egress` mirroring it) because the token program and the valuation program are
    /// deliberately separate with no dependency between them. `noded` is the one place that sees all
    /// three, so this is where the duplication is pinned — without it, a change to one would silently
    /// re-denominate half the economy.
    #[test]
    fn the_token_scale_is_identical_everywhere_it_is_mirrored() {
        assert_eq!(
            zeph_token::ONE_TOKEN,
            zeph_reward::ONE_TOKEN,
            "reward's mirrored token scale drifted from the canonical one"
        );
        assert_eq!(
            zeph_token::ONE_TOKEN,
            zeph_economy_egress::ONE_TOKEN,
            "economy-egress's mirrored token scale drifted from the canonical one"
        );
        assert_eq!(
            zeph_token::ONE_TOKEN,
            10u64.pow(zeph_token::DECIMALS as u32),
            "ONE_TOKEN must be exactly 10^DECIMALS or display and arithmetic disagree"
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
            s.total_supply(),
            1,
            "counter advanced by exactly the mint"
        );

        // Seeding from the durable chain is monotonic — it can only ever RAISE the counter, so a stale
        // or out-of-order read can never restore spent minting headroom.
        s.restore(&zeph_reward::EconomicSnapshot {
            minted: 0,
            pool: s.pool_total(),
            ..Default::default()
        });
        assert_eq!(
            s.total_supply(),
            1,
            "a lower chain read does not lower the counter"
        );
        s.restore(&zeph_reward::EconomicSnapshot {
            minted: 500,
            pool: s.pool_total(),
            ..Default::default()
        });
        assert_eq!(s.total_supply(), 500, "a higher chain read raises it");
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
        // Steady state: no seed, so the asserted pool arithmetic is exactly what was paid.
        s.set_issuance(0, 0);
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
        // Steady state: no seed, so the asserted pool arithmetic is exactly what was paid.
        s.set_issuance(0, 0);
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
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase paid-tier subsidy is off.
        s.set_seeding_paid_tier(0);
        // Steady state: no seed, so the asserted shares are exactly the paid pool.
        s.set_issuance(0, 0);
        // UNIT PRICE in base terms: one whole token buys ONE_TOKEN bytes, i.e. 1 BASE UNIT = 1 byte, so
        // the arithmetic below reads as the mechanics rather than the pricing.
        s.set_bytes_per_token(zeph_token::ONE_TOKEN);
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
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase paid-tier subsidy is off.
        s.set_seeding_paid_tier(0);
        // Steady state: no seed. UNIT PRICE in base terms (1 base unit = 1 byte); the price cancels in
        // the pool-average anyway — see the price test.
        s.set_issuance(0, 0);
        s.set_bytes_per_token(zeph_token::ONE_TOKEN);
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
        // Steady state: no seed, so the asserted pool arithmetic is exactly what was paid.
        s.set_issuance(0, 0);
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
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase paid-tier subsidy is off.
        s.set_seeding_paid_tier(0);
        // Steady state: no seed. UNIT PRICE in base terms — 1 base unit = 1 byte.
        s.set_issuance(0, 0);
        s.set_bytes_per_token(zeph_token::ONE_TOKEN);
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
        // the real end state — the seeding-phase PAID-TIER SUBSIDY is what governance switches off (the free
        // tier — reciprocity — is permanent and unrelated).
        s.set_seeding_paid_tier(0);
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
        let paid = 2 * zeph_token::ONE_TOKEN; // amounts are BASE UNITS
        let rec = s.settle_epoch_from_cheques(2, vec![(c, paid)], vec![(prov(1), c, 1500, 1)]);
        assert_eq!(
            rec.share_of(&prov(1)),
            paid,
            "the whole pool (2 tokens paid) to the sole provider"
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
        // Steady state: no seed either, so the asserted pool is exactly what was paid.
        s.set_issuance(0, 0);
        // STEADY STATE (post-seeding): entitlement must be PAID for. The seeding-phase paid-tier subsidy is off.
        s.set_seeding_paid_tier(0);
        s.set_bytes_per_token(10);
        s.set_window(std::time::Duration::from_millis(
            crate::epoch::EPOCH_MILLIS * 5,
        )); // 5 epochs
        let c = prov(9);
        s.settle_epoch_from_cheques(1, vec![(c, 0)], vec![]); // first sight
                                                              // 10 whole tokens (BASE UNITS) × 10 B/token = 100 B, bought at epoch 2, expiring at 7.
        s.settle_epoch_from_cheques(2, vec![(c, 10 * zeph_token::ONE_TOKEN)], vec![]);
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
        // Steady state: no seed, so the asserted pool arithmetic is exactly what was paid.
        s.set_issuance(0, 0);
        // STEADY STATE (post-seeding): the default tier is off, so entitlement must be PAID for. This is
        // the real end state — the seeding-phase PAID-TIER SUBSIDY is what governance switches off (the free
        // tier — reciprocity — is permanent and unrelated).
        s.set_seeding_paid_tier(0);
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
        // Steady state: no seed either, so the asserted pool is exactly what was paid.
        s.set_issuance(0, 0);
        // STEADY STATE (post-seeding): the default tier is off, so entitlement must be PAID for. This is
        // the real end state — the seeding-phase PAID-TIER SUBSIDY is what governance switches off (the free
        // tier — reciprocity — is permanent and unrelated).
        s.set_seeding_paid_tier(0);
        let c = prov(9);
        // Amounts are BASE UNITS. C's FIRST appearance already carries a historical paid total →
        // baseline, quota 0, pool 0.
        let hist = 1000 * zeph_token::ONE_TOKEN;
        let rec1 = s.settle_epoch_from_cheques(1, vec![(c, hist)], vec![(prov(1), c, 500, 1)]);
        assert!(
            rec1.shares.is_empty(),
            "historical pay is baselined, not dumped into one epoch"
        );
        assert_eq!(s.unallocated(), 0);
        // Only NEW pay past the baseline becomes quota + pool.
        let delta = 200 * zeph_token::ONE_TOKEN;
        let rec2 =
            s.settle_epoch_from_cheques(2, vec![(c, hist + delta)], vec![(prov(1), c, 700, 2)]);
        assert_eq!(
            rec2.share_of(&prov(1)),
            delta,
            "Δpaid → quota → all of it rewardable"
        );
    }

    #[test]
    fn a_claimed_share_does_not_forfeit_on_expiry() {
        let mut s = SettlementStore::new();
        // Steady state: no seed, so the asserted pool arithmetic is exactly what was paid.
        s.set_issuance(0, 0);
        s.pay_in(50);
        s.settle_epoch(1, vec![contrib(1, 1)], vec![]);
        s.claim(1, prov(1)); // provider claimed its 50 (owed → paid, out of the pool)
        s.settle_epoch(1 + CLAIM_WINDOW_EPOCHS + 1, vec![], vec![]); // expire epoch 1
        assert_eq!(s.unallocated(), 0, "claimed shares don't double-forfeit");
    }
}
