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

use std::sync::Arc;

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
}

impl EconomyEgressService {
    pub fn new() -> Self {
        Self {
            settlement: tokio::sync::RwLock::new(SettlementStore::new()),
            record_chain: tokio::sync::RwLock::new(None),
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

    /// Apply the GOVERNED subscription window in epochs (`economy:subscription_window_epochs`, ≈30 days).
    pub async fn set_window_epochs(&self, window_epochs: u64) {
        self.settlement
            .write()
            .await
            .set_window_epochs(window_epochs);
    }

    /// `consumer`'s remaining unexpired egress entitlement at `epoch` — the dashboard's "subscription
    /// bytes left" (use-it-or-lose-it: it is gone at the window edge, never refunded).
    pub async fn entitlement(&self, consumer: [u8; 32], epoch: u64) -> u64 {
        self.settlement.read().await.entitlement(&consumer, epoch)
    }

    /// The reward share owed to `account` for `epoch`: the CANONICAL committee-attested record's share if
    /// one is finalized (durable, restart-safe, census-divergence-proof), else the local in-memory record.
    /// Called by the token ledger's balance fold for a `RewardClaim` — token credits this share (and owns
    /// the single-use dedup itself, so this answer is a valuation, not an authorization).
    pub async fn reward_share(&self, epoch: u64, account: &[u8; 32]) -> u64 {
        if let Some(records) = self.record_chain.read().await.clone() {
            if let Some(record) = records.canonical_record(epoch).await {
                return record.share_of(account);
            }
        }
        self.settlement.read().await.share_of(epoch, account)
    }

    /// Mark `(epoch, node)`'s reward claimed (called by the token ledger after a `RewardClaim` commits),
    /// moving it out of the pool's `owed`. Idempotent.
    pub async fn mark_claimed(&self, epoch: u64, node: [u8; 32]) {
        self.settlement.write().await.claim(epoch, node);
    }

    /// Total reward `account` is owed but hasn't yet claimed, across all in-window settlement records —
    /// the dashboard "reward earned by serving, awaiting claim" figure (the claimed part is in `balance`).
    pub async fn reward_owed(&self, account: [u8; 32]) -> u64 {
        self.settlement.read().await.owed_to(&account)
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

    /// This provider's cumulative REWARDABLE served bytes (the per-consumer-capped "settled" side of the
    /// dashboard settled/served meter). Gross served is the cheque `total_earned`.
    pub async fn rewardable_served(&self, provider: [u8; 32]) -> u64 {
        self.settlement.read().await.rewardable_served(&provider)
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

impl Default for EconomyEgressService {
    fn default() -> Self {
        Self::new()
    }
}
