//! `EconomyEgressService` — the node-side integration of the ECONOMY-EGRESS policy program
//! (`ECONOMY_PROGRAMS_DESIGN.md`; economy split phase P4). It owns the epoch-close SETTLEMENT policy —
//! the running pool + per-consumer FCFS records ([`SettlementStore`]) and the committee-attested record
//! finality ([`RecordChain`]) — separate from the TOKEN value ledger ([`crate::ledger::LedgerService`]).
//!
//! **Boundary (one-directional, no cycle).** This service is self-contained POLICY: it never touches the
//! token account-chain. `LedgerService` (the value authority) holds an `Arc<EconomyEgressService>` and
//! asks it for a [`reward_share`](Self::reward_share) when it co-folds a `RewardClaim` (token credits the
//! share) and marks the epoch claimed via [`mark_claimed`](Self::mark_claimed) on commit. The settlement
//! loop feeds it proven cumulatives via [`settle_from_board`](Self::settle_from_board). First of the
//! `economy-*` family (economy-storage, … reuse the same token ledger).

use std::sync::Arc;

use zeph_reward::{Contribution, RewardRecord};

use crate::record_chain::RecordChain;
use crate::settlement::SettlementStore;

pub struct EconomyEgressService {
    /// The epoch-close settlement pool (running `unallocated`/`owed`, §10.1) + per-consumer FCFS records.
    /// Fed CROSS-NODE by `settle_from_board` (every node folds the identical proven cumulatives), NOT by a
    /// local `pay()` — so the epoch record is deterministic network-wide.
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

    /// The reward share owed to `account` for `epoch`: the CANONICAL committee-attested record's share if
    /// one is finalized (durable, restart-safe, census-divergence-proof), else the local in-memory record.
    /// Called by the token ledger's balance fold when it co-folds a `RewardClaim` (it credits this share).
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
    /// from the converged, proof-verified board; the store applies the PER-CONSUMER FCFS cap then
    /// pool-average. Idempotent per epoch; every node passes identical inputs → identical record.
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
