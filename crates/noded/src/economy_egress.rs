//! `EconomyEgressService` — the node-side integration of the ECONOMY-EGRESS program
//! (`ECONOMY_PROGRAMS_DESIGN.md`). It owns the paid-egress POLICY: the epoch-close settlement pool +
//! per-consumer SUBSCRIPTION entitlements ([`SettlementStore`]) and the committee-attested record
//! finality ([`RecordChain`]) — separate from the TOKEN value ledger ([`crate::ledger::LedgerService`]).
//!
//! **Boundary (one-directional, no cycle).** This service is self-contained POLICY: it never touches the
//! token account-chain and never moves value. `LedgerService` (the value authority) holds an
//! `Arc<EconomyEgressService>` and asks it one question when folding a `RewardClaim` — what SHARE is owed
//! ([`reward_share`](Self::reward_share)) — then credits it and marks the epoch claimed in TOKEN's own
//! state; [`mark_claimed`](Self::mark_claimed) only updates this pool's `owed` accounting, it is not the
//! safety gate. The settlement loop feeds proven cumulatives via
//! [`settle_from_board`](Self::settle_from_board), and governance tunes the egress price + subscription
//! window. First of the `economy-*` family (economy-storage, … reuse the same token program).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use zeph_reward::{Contribution, RewardRecord};

use crate::record_chain::RecordChain;
use crate::settlement::SettlementStore;

pub struct EconomyEgressService {
    /// The epoch-close settlement pool (running `unallocated`/`owed`, §10.1) + per-consumer subscription
    /// entitlements (P6). Fed CROSS-NODE by `settle_from_board` (every node folds the identical proven
    /// cumulatives with the identical governed price), NOT by a local `pay()` — so the epoch record is
    /// deterministic network-wide.
    settlement: tokio::sync::RwLock<SettlementStore>,
    /// Committee-attested record finality (set after construction). When present, a `RewardClaim` resolves
    /// its share from the CANONICAL quorum-signed record (durable, restart-safe), else the local record.
    record_chain: tokio::sync::RwLock<Option<Arc<RecordChain>>>,
    /// Cached records-derived view: `(derived_at, account, view)`. See [`VIEW_TTL`].
    view_cache: tokio::sync::RwLock<Option<(Instant, [u8; 32], MyView)>>,
    /// The ECONOMIC STORE (CraftSQL) — where the O(accounts) state actually lives. Set after
    /// construction, like `record_chain`. The epoch record commits to it by hash only.
    econ_db: tokio::sync::Mutex<Option<zeph_sql::CraftDb>>,
    /// SINGLE-FLIGHT gate for [`derive_view`](Self::derive_view). A TTL cache alone does NOT stop a
    /// stampede: the derivation walks the claim window over the DHT, which on a high-RTT node takes
    /// MINUTES, while the dashboard polls every SECOND — so every poll missed the still-empty cache and
    /// launched another derivation, piling up hundreds of concurrent walks and turning the dashboard into
    /// a self-inflicted DHT lookup storm (measured: ~700 KB/s on a hotspot-attached node, while the
    /// sub-ms-RTT nodes running identical code showed nothing, because their walks finished before the
    /// next poll). One derivation at a time; everyone else serves the previous value.
    derive_lock: tokio::sync::Mutex<()>,
}

impl EconomyEgressService {
    pub fn new() -> Self {
        Self {
            settlement: tokio::sync::RwLock::new(SettlementStore::new()),
            record_chain: tokio::sync::RwLock::new(None),
            view_cache: tokio::sync::RwLock::new(None),
            derive_lock: tokio::sync::Mutex::new(()),
            econ_db: tokio::sync::Mutex::new(None),
        }
    }

    /// Inject the committee-attested record finality (built after the committee/sequencer). Once set,
    /// reward claims resolve against the canonical quorum-signed record.
    pub async fn set_record_chain(&self, records: Arc<RecordChain>) {
        *self.record_chain.write().await = Some(records);
    }

    /// Apply the GOVERNED egress price — how many bytes of rewardable serving ONE token buys (P6
    /// subscriptions, `economy:bytes_per_token`). Every node reads the identical governed value, so
    /// entitlements stay deterministic. Future purchases only (no retroactive repricing).
    pub async fn set_bytes_per_token(&self, bytes_per_token: u64) {
        self.settlement
            .write()
            .await
            .set_bytes_per_token(bytes_per_token);
    }

    /// Total tokens the pool literally holds (earmarked or not).
    pub async fn pool_total(&self) -> u64 {
        self.settlement.read().await.pool_total()
    }

    /// TOTAL SUPPLY in existence — CTS-1 L0. Rises only via the seed mint, capped by governance.
    pub async fn total_supply(&self) -> u64 {
        self.settlement.read().await.total_supply()
    }

    /// Attach the economic store (a CraftSQL DB under [`ECON_NAMESPACE`]).
    pub async fn set_econ_db(&self, db: zeph_sql::CraftDb) {
        *self.econ_db.lock().await = Some(db);
    }

    /// Adopt epoch `epoch`'s CANONICAL economic state, for a node that did not settle it.
    ///
    /// Returns whether a canonical record was available to adopt. This is what keeps every node's
    /// economic position tracking the chain rather than only the sample of epochs it was elected for —
    /// the natural consequence of the pool being a function of the record rather than local
    /// accumulation.
    pub async fn adopt_canonical_state(&self, epoch: u64) -> bool {
        let Some(records) = self.record_chain.read().await.clone() else {
            return false;
        };
        let Some(rec) = records.canonical_record(epoch).await else {
            return false; // not finalized (yet) — nothing canonical to adopt
        };
        self.settlement.write().await.adopt_canonical(&rec);
        true
    }

    /// Persist the current economic position to the store. Called at epoch close, after the record is
    /// computed, so what is stored is exactly what the record committed to.
    pub async fn persist_economic_state(&self) -> anyhow::Result<()> {
        // The COMMITTED snapshot, not the live one: the record hashed the state at epoch close, and
        // claims since then have legitimately moved value.
        let snap = self.settlement.read().await.committed_snapshot();
        if let Some(db) = self.econ_db.lock().await.as_mut() {
            crate::econ_store::persist(db, &snap).await?;
        }
        Ok(())
    }

    /// Load the economic position from the store and adopt it, VERIFYING it against the canonical
    /// record's commitment.
    ///
    /// The hash check is the point of the split: the state lives outside the record, so a node must be
    /// able to tell whether the state it holds is the state the network agreed on. A mismatch means this
    /// node has diverged — it is reported rather than silently adopted, because silently continuing from
    /// a wrong economic position is how a divergence becomes permanent.
    pub async fn load_and_verify_economic_state(&self, expected: [u8; 32]) -> bool {
        let loaded = {
            let mut guard = self.econ_db.lock().await;
            match guard.as_mut() {
                Some(db) => match crate::econ_store::load(db) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "economic store unreadable; keeping current state");
                        return false;
                    }
                },
                None => return false,
            }
        };
        let got = loaded.state_hash();
        if got != expected {
            tracing::error!(
                expected = %hex::encode(&expected[..8]),
                got = %hex::encode(&got[..8]),
                "ECONOMIC STATE DIVERGED from the canonical record — not adopting local state"
            );
            return false;
        }
        self.settlement.write().await.restore(&loaded);
        true
    }

    /// Apply the GOVERNED DEFAULT TIER — the per-window egress allowance every account holds WITHOUT
    /// paying, i.e. everyone on the paid tier by default. 0 turns it off (payment becomes the only route
    /// to entitlement). Governed so every node grants identically; a divergence would change which
    /// serving is rewardable and so break record re-execution.
    pub async fn set_seeding_paid_tier(&self, bytes: u64) {
        self.settlement.write().await.set_seeding_paid_tier(bytes);
    }

    /// Apply the GOVERNED bootstrap ISSUANCE schedule (§10.3 fair launch): a rate in TOKENS PER DAY plus
    /// a lifetime ceiling on fresh supply. The rate is converted to a per-epoch target HERE, where the
    /// epoch period is known, so retuning `EPOCH_MILLIS` re-derives it instead of silently rescaling real
    /// issuance (the same discipline as the subscription window).
    ///
    /// Every node must resolve the identical governed values, because issuance is computed inside the
    /// reward program and verified by re-execution — a node using a different schedule would produce a
    /// record that every verifier rejects.
    pub async fn set_issuance(&self, tokens_per_day: u64, total_cap: u64) {
        // The RATE passes through as-is: the reward program pays it against `epochs_per_day` on an exact
        // schedule, so a sub-epoch rate (1 token/day = 1/288 at a 5min epoch) still pays exactly. The old
        // lossy tokens/day → per-epoch division, which silently floored such rates to zero, is gone.
        self.settlement
            .write()
            .await
            .set_issuance(tokens_per_day, total_cap);
    }

    /// Apply the GOVERNED subscription window as a DURATION (`economy:subscription_window_secs`, default
    /// 30 days). Time, not an epoch count, so retuning the epoch period cannot rescale the promise made
    /// to payers.
    pub async fn set_window(&self, window: std::time::Duration) {
        self.settlement.write().await.set_window(window);
    }

    /// An account's own economy view — settled bytes, subscription balance, pool, and unclaimed reward —
    /// derived from the CANONICAL committee-attested records rather than local settle state, so a node
    /// that never settles (the common case, since settling is committee-gated) still sees its own money
    /// instead of 0.
    ///
    /// ONE walk of the claim window serves all four figures: they read the same records, so walking
    /// twice doubled the cost for nothing. CACHED for [`VIEW_TTL`] — the records advance once per epoch
    /// while the dashboard polls every second, and profiling a polled node showed this derivation at the
    /// top of the stack.
    ///
    /// `claimed` is the account's own on-chain dedup set: TOKEN is the authority on what it has already
    /// taken, and economy must not call token (the one-directional boundary), so the caller — which holds
    /// both handles — passes it in.
    ///
    /// Absence is not zero: `pool`/`settled`/`remaining` are STATE, so the newest record carrying a row
    /// wins (walk newest-first); `owed` is a SUM over every unclaimed in-window epoch.
    pub async fn my_view_from_records(
        &self,
        account: [u8; 32],
        claimed: &BTreeSet<u64>,
        now_epoch: u64,
    ) -> MyView {
        if let Some(view) = self.cached_view(account).await {
            return view;
        }
        // SINGLE-FLIGHT: one derivation at a time (see `derive_lock`). `try_lock` rather than `lock` —
        // a waiter that blocked here would still be an outstanding request piling up behind a walk that
        // can take minutes, which is the stampede itself. Serve the last known value instead; it is at
        // most VIEW_TTL+ stale, against data that only changes once per epoch.
        let Ok(_guard) = self.derive_lock.try_lock() else {
            return self.last_view(account).await.unwrap_or_default();
        };
        // Re-check under the guard: a derivation may have completed while we were acquiring it.
        if let Some(view) = self.cached_view(account).await {
            return view;
        }
        let view = self.derive_view(account, claimed, now_epoch).await;
        *self.view_cache.write().await = Some((Instant::now(), account, view));
        view
    }

    /// The cached view if it is fresh enough to serve.
    async fn cached_view(&self, account: [u8; 32]) -> Option<MyView> {
        match *self.view_cache.read().await {
            Some((at, who, view)) if who == account && at.elapsed() < VIEW_TTL => Some(view),
            _ => None,
        }
    }

    /// The last derived view REGARDLESS of age — served to callers that lost the single-flight race, so a
    /// slow walk degrades freshness rather than queueing another walk behind it.
    async fn last_view(&self, account: [u8; 32]) -> Option<MyView> {
        match *self.view_cache.read().await {
            Some((_, who, view)) if who == account => Some(view),
            _ => None,
        }
    }

    async fn derive_view(
        &self,
        account: [u8; 32],
        claimed: &BTreeSet<u64>,
        now_epoch: u64,
    ) -> MyView {
        let Some(records) = self.record_chain.read().await.clone() else {
            return MyView::default();
        };
        let start = now_epoch.saturating_sub(crate::settlement::CLAIM_WINDOW_EPOCHS);
        // Collect the window NEWEST-FIRST, then read every figure off it — one traversal of the chain
        // for all four, instead of one per figure.
        let mut window = Vec::new();
        for e in (start..=now_epoch).rev() {
            if let Some(rec) = records.canonical_record(e).await {
                window.push(rec);
            }
        }
        MyView {
            // STATE: the newest record carrying a row for this account wins. `find_map` stops at the
            // first row rather than treating an absent row as zero.
            settled_bytes: window
                .iter()
                .find_map(|r| r.cumulative_bytes_of(&account))
                .unwrap_or(0),
            subscription_bytes: window
                .iter()
                .find_map(|r| r.remaining_for(&account))
                .unwrap_or(0),
            pool: window.first().map(|r| r.pool_remaining()).unwrap_or(0),
            // SUM over unclaimed epochs — the same pure, unit-tested helper.
            owed: sum_unclaimed_shares(&account, &window, claimed),
        }
    }

    /// The reward share owed to `account` for `epoch`: the CANONICAL committee-attested record's share if
    /// one is finalized (durable, restart-safe, census-divergence-proof), else the local in-memory record.
    /// Called by the token ledger's balance fold for a `RewardClaim` — token credits this share (and owns
    /// the single-use dedup itself, so this answer is a valuation, not an authorization).
    pub async fn reward_share(&self, epoch: u64, account: &[u8; 32]) -> u64 {
        // CANONICAL ONLY — no local fallback.
        //
        // `balance()` re-derives the account chain from scratch on every call, resolving this live rather
        // than reading a value pinned into the committed write. With a local fallback that made the SAME
        // committed `RewardClaim` fold to DIFFERENT amounts depending on when it was asked: a query
        // before quorum used whatever this node happened to hold (0 if it never settled the epoch, or a
        // pre-convergence value if it did), a query after quorum used the canonical figure. A `Transfer`
        // authored against the higher pre-canonical balance could then silently void on the next replay
        // — value disappearing with no new chain activity — and it broke the premise that a verifier
        // re-running the fold reproduces the node's result, since the input was node-local and
        // time-varying.
        //
        // Canonical records are immutable once finalised, so resolving only from them makes the fold
        // stable forever. Before finality this returns 0, which `apply_token` rejects BEFORE marking the
        // epoch claimed — so a premature claim wastes nothing and simply succeeds once the epoch is
        // final. Claiming an unfinalised epoch was never meaningful anyway.
        let Some(records) = self.record_chain.read().await.clone() else {
            return 0;
        };
        match records.canonical_record(epoch).await {
            Some(record) => record.share_of(account),
            None => 0, // not finalised yet — the claim is refused, not guessed at
        }
    }

    /// Mark `(epoch, node)`'s reward claimed (called by the token ledger after a `RewardClaim` commits),
    /// moving it out of the pool's `owed`. Idempotent.
    pub async fn mark_claimed(&self, epoch: u64, node: [u8; 32]) {
        self.settlement.write().await.claim(epoch, node);
    }

    /// Cross-node epoch close (§10.1, node-orchestrated by the settlement loop). `paid` = each node's
    /// `(node, paid_cumulative)` and `cheques` = every `(provider, consumer, cumulative_bytes, timestamp)`
    /// from the converged, proof-verified board; the store buys each payer's subscription entitlement from
    /// its paid delta, allocates serving against it FCFS (P6), then pool-averages. Idempotent per epoch;
    /// every node passes identical inputs → identical record.
    pub async fn settle_from_board(
        &self,
        epoch: u64,
        paid: Vec<([u8; 32], u64)>,
        cheques: Vec<([u8; 32], [u8; 32], u64, u64)>,
    ) -> RewardRecord {
        self.settlement
            .write()
            .await
            .settle_epoch_from_cheques(epoch, paid, cheques)
    }

    /// DEV/manual override (the `ledger-settle-epoch` RPC): inject `pool` + settle `epoch` with the given
    /// `contributions` directly, bypassing the announcement loop — exercises the settlement math offline.
    pub async fn dev_settle_epoch(
        &self,
        epoch: u64,
        pool: u64,
        contributions: Vec<Contribution>,
    ) -> RewardRecord {
        self.settlement
            .write()
            .await
            .settle_epoch_with_pool(epoch, pool, contributions)
    }

    /// The current distributable (`unallocated`) pool balance — observability.
    pub async fn pool_unallocated(&self) -> u64 {
        self.settlement.read().await.unallocated()
    }

    /// This node's OWN computed record for `epoch` (its settle re-execution) — the verification loop
    /// compares it against the canonical committee-attested record.
    pub async fn local_record(&self, epoch: u64) -> Option<RewardRecord> {
        self.settlement.read().await.record(epoch)
    }
}

/// An account's own economy view, read from the durable records chain rather than local settle state —
/// what a node can report about itself without ever settling (settling is committee-gated).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MyView {
    /// CUMULATIVE rewardable bytes this account has served.
    pub settled_bytes: u64,
    /// Unexpired egress entitlement remaining (its subscription balance).
    pub subscription_bytes: u64,
    /// The distributable pool as of the newest record (`pool − Σ shares`).
    pub pool: u64,
    /// Reward earned but not yet claimed (Σ shares over in-window epochs this account hasn't claimed).
    pub owed: u64,
}

/// How long a derived [`MyView`] is served from cache.
///
/// The records chain only advances ONCE PER EPOCH (5 min), but the dashboard is HTTP-polled every second
/// or so — and each derivation walks the claim window (epochs × committee members → `sequence_of` →
/// DHT). Profiling a polled node put `snapshot → canonical_record → sequence_of → DhtNode::get` at the
/// top, so the derivation is the hot path WHEN POLLED. (An earlier note here claimed it cost ~45pts of
/// CPU; that compared `ps` LIFETIME averages across runs with different poll load and is withdrawn — the
/// justification is the profile, not that number.) Re-deriving data that cannot change faster than an
/// epoch, once per second, is waste at any magnitude; anything well under an epoch is free freshness.
const VIEW_TTL: Duration = Duration::from_secs(20);

/// The pure core of [`EconomyEgressService::owed_from_records`]: sum `account`'s shares across `window`,
/// skipping epochs it has already claimed (token's `claimed_epochs` is the authority on what was taken —
/// a claimed share is in `balance`, not owed). Split out so the sum/dedup logic stays unit-testable
/// without standing up a RecordChain.
fn sum_unclaimed_shares(
    account: &[u8; 32],
    window: &[RewardRecord],
    claimed: &BTreeSet<u64>,
) -> u64 {
    window
        .iter()
        .filter(|r| !claimed.contains(&r.epoch))
        .map(|r| r.share_of(account))
        .fold(0u64, |a, b| a.saturating_add(b))
}

impl Default for EconomyEgressService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_reward::Share;

    fn prov(n: u8) -> [u8; 32] {
        [n; 32]
    }
    fn rec(epoch: u64, shares: &[([u8; 32], u64)]) -> RewardRecord {
        RewardRecord {
            epoch,
            pool: 0, // not under test (owed sum)
            shares: shares
                .iter()
                .map(|(provider, amount)| Share {
                    provider: *provider,
                    amount: *amount,
                    bytes: 0,            // irrelevant to the owed sum under test
                    cumulative_bytes: 0, // ditto
                })
                .collect(),
            entitlements: Vec::new(),
            ..Default::default()
        }
    }

    #[test]
    fn owed_sums_unclaimed_shares_across_records() {
        // Moved from SettlementStore::owed_to, which the canonical-records path replaced: the dashboard
        // must show a node its own money even when it never settles (settling is committee-gated).
        let window = vec![
            rec(1, &[(prov(1), 60), (prov(2), 40)]),
            rec(2, &[(prov(1), 50)]),
        ];
        let none = BTreeSet::new();
        assert_eq!(sum_unclaimed_shares(&prov(1), &window, &none), 110);
        assert_eq!(sum_unclaimed_shares(&prov(2), &window, &none), 40);
        // Claiming an epoch drops it out of `owed` — it is in `balance` now, so counting it twice would
        // overstate what the node can still take.
        let claimed_1: BTreeSet<u64> = [1u64].into_iter().collect();
        assert_eq!(sum_unclaimed_shares(&prov(1), &window, &claimed_1), 50);
        // A provider absent from every record is owed nothing.
        assert_eq!(sum_unclaimed_shares(&prov(9), &window, &none), 0);
        // All claimed → nothing owed.
        let both: BTreeSet<u64> = [1u64, 2].into_iter().collect();
        assert_eq!(sum_unclaimed_shares(&prov(1), &window, &both), 0);
    }
}
