//! `LedgerService` — the node-side integration of the token-ledger protocol program
//! (TOKEN_LEDGER_BUILD.md §4; step 4 phase 4b-3). It:
//! - **authors** owner-signed ledger writes into the sequencer, ordered by the **epoch committee**
//!   (the write's owner is the anchor sentinel, so `AnchorAwareQuorumSource` routes to the committee);
//! - **folds** an account's committed sequence into its balance via the shared [`zeph_ledger`] crate —
//!   NATIVELY, identical to a verifier re-running the wasm program by construction (same crate).
//!
//! The ledger *program* is the embedded [`LEDGER_WASM`] (the canonical cid pinned behind the K1
//! `token-ledger` anchor, for verification/governance-swap); the node's own balance computation folds
//! natively. Validity is by re-execution (the fold), not a committee for the fold itself; an invalid
//! write (e.g. an overdraft) folds to a no-op, so it can occupy a nonce but never corrupts the balance.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_ledger::{
    apply, ClaimOp, LedgerBalanceState, LedgerOp, Resolved, ResolvedDebit, TransferOp,
};
use zeph_reward::{Contribution, RewardRecord};

use crate::anchor::AnchorDispatcher;
use crate::sequence::SequenceStore;
use crate::settlement::SettlementStore;

/// The embedded ledger WASM program — the canonical `token-ledger` cid. Built from `apps/ledger-wasm`
/// (a thin wrapper over `zeph-ledger`), so re-running it reproduces the node's native fold.
const LEDGER_WASM: &[u8] = include_bytes!("../ledger.wasm");

/// The canonical token-ledger program cid = the content hash of the embedded wasm.
pub fn ledger_program_cid() -> [u8; 32] {
    Cid::of(LEDGER_WASM).0
}

pub struct LedgerService {
    identity: Arc<NodeIdentity>,
    sequence: Arc<SequenceStore>,
    /// The canonical ledger program cid (the sequencer's `program_cid` for every ledger account).
    cid: [u8; 32],
    /// The deterministic sentinel owner of the anchored ledger — routes ordering to the epoch committee
    /// (a network-owned program has no owner key). One owner, one committee, many accounts.
    owner: [u8; 32],
    /// The epoch-close settlement pool (running `unallocated`/`owed`, §10.1). Providers' reward shares
    /// are resolved from here for a `RewardClaim`. Fed CROSS-NODE by the settlement loop (Σ every node's
    /// announced pays), NOT by the local `pay()` — so every node's pool folds the identical aggregate and
    /// the epoch record is deterministic network-wide.
    settlement: tokio::sync::RwLock<SettlementStore>,
    /// This node's CUMULATIVE `Pay` total (monotonic; incremented on each committed `pay`). The settlement
    /// loop reads its per-epoch DELTA to announce this node's pay-in contribution to the shared pool.
    total_paid: AtomicU64,
}

impl LedgerService {
    pub fn new(identity: Arc<NodeIdentity>, sequence: Arc<SequenceStore>) -> Self {
        let cid = ledger_program_cid();
        let owner = AnchorDispatcher::anchor_owner(&cid);
        Self {
            identity,
            sequence,
            cid,
            owner,
            settlement: tokio::sync::RwLock::new(SettlementStore::new()),
            total_paid: AtomicU64::new(0),
        }
    }

    /// The embedded ledger wasm bytes + its cid — consumed by the genesis step (publish the wasm to
    /// obj so verifiers can fetch it, and pin the `token-ledger` anchor), which is the next follow-on.
    #[allow(dead_code)]
    pub fn wasm() -> &'static [u8] {
        LEDGER_WASM
    }

    #[allow(dead_code)]
    pub fn cid(&self) -> [u8; 32] {
        self.cid
    }

    /// Resolve the sender's ordered debit at `(account, nonce)` → its `TransferOp`, for a claim to
    /// check. `None` if the sequence/entry is missing or the entry isn't a transfer.
    async fn resolve_debit(&self, account: [u8; 32], nonce: u64) -> Option<ResolvedDebit> {
        let seq = self
            .sequence
            .sequence_of(self.owner, self.cid, account)
            .await?;
        let payload = seq.payload_at(nonce)?;
        match postcard::from_bytes::<LedgerOp>(payload).ok()? {
            LedgerOp::Transfer(t) => Some(ResolvedDebit { transfer: t }),
            // The referenced write isn't a transfer → not a claimable debit.
            LedgerOp::Claim(_) | LedgerOp::Pay(_) | LedgerOp::RewardClaim(_) => None,
        }
    }

    /// Fold `account`'s committed sequence into its balance state (native — identical to a wasm
    /// re-run). An invalid write (`apply → None`) is a no-op, leaving the prior state.
    pub async fn balance(&self, account: [u8; 32]) -> LedgerBalanceState {
        let Some(seq) = self
            .sequence
            .sequence_of(self.owner, self.cid, account)
            .await
        else {
            return LedgerBalanceState::default();
        };
        let mut state = LedgerBalanceState::default();
        for nonce in 0..seq.next_nonce() {
            let Some(payload) = seq.payload_at(nonce) else {
                break;
            };
            let Ok(op) = postcard::from_bytes::<LedgerOp>(payload) else {
                continue; // a non-ledger payload at this nonce → skip
            };
            let resolved = match &op {
                LedgerOp::Claim(c) => Resolved {
                    debit: self.resolve_debit(c.debit_account, c.debit_nonce).await,
                    reward_share: None,
                },
                // A RewardClaim's share is resolved from the settlement pool's epoch record (0 if the
                // epoch is unsettled/expired or already claimed → `apply` rejects it).
                LedgerOp::RewardClaim(epoch) => Resolved {
                    debit: None,
                    reward_share: Some(self.settlement.read().await.share_of(*epoch, &account)),
                },
                _ => Resolved::default(),
            };
            if let Some(next) = apply(state.clone(), &op, &account, &resolved) {
                state = next;
            }
            // else: rejected write → no-op (the nonce is spent, the balance is unchanged)
        }
        state
    }

    /// Author + submit a ledger write for THIS node's own account, ordered by the committee. Returns
    /// whether it committed (a quorum of the committee co-signed the ordering).
    async fn submit_own(&self, op: LedgerOp) -> bool {
        let account = self.identity.node_id().0;
        let nonce = self
            .sequence
            .sequence_of(self.owner, self.cid, account)
            .await
            .map(|s| s.next_nonce())
            .unwrap_or(0);
        let Ok(payload) = postcard::to_allocvec(&op) else {
            return false;
        };
        let write = SequencedWrite::author(&self.identity, nonce, payload);
        self.sequence.sequence(self.owner, self.cid, write).await
    }

    /// Transfer `amount` from this node's account to `to` — a DEBIT on this account (the recipient
    /// later CLAIMs it). Rejected (returns false) on overdraft only at fold time; the write still
    /// orders (spending a nonce) but folds to a no-op if it would overdraw.
    pub async fn transfer(&self, to: [u8; 32], amount: u64) -> bool {
        self.submit_own(LedgerOp::Transfer(TransferOp {
            to,
            amount,
            memo: [0u8; 32],
        }))
        .await
    }

    /// Claim a transfer `(debit_account, debit_nonce)` credited to this node's account.
    pub async fn claim(&self, debit_account: [u8; 32], debit_nonce: u64) -> bool {
        self.submit_own(LedgerOp::Claim(ClaimOp {
            debit_account,
            debit_nonce,
        }))
        .await
    }

    /// PAY `amount` of egress cost into the epoch pool — a self-authored debit (§10.1 pay-into-pool).
    /// On commit, we bump this node's cumulative `total_paid`; the settlement loop later announces the
    /// per-epoch DELTA so EVERY node folds the same Σ pays into the pool (the pool is announcement-driven,
    /// not fed locally here — a local pay_in would make each node's pool differ and break determinism).
    pub async fn pay(&self, amount: u64) -> bool {
        let ok = self.submit_own(LedgerOp::Pay(amount)).await;
        if ok {
            self.total_paid.fetch_add(amount, Ordering::Relaxed);
        }
        ok
    }

    /// This node's cumulative committed `Pay` total — the settlement loop deltas it per epoch.
    pub fn total_paid(&self) -> u64 {
        self.total_paid.load(Ordering::Relaxed)
    }

    /// Claim this node's reward share for `epoch` (single-use, §10.1). The share is resolved from the
    /// settlement pool's epoch record; on commit, the pool marks it claimed (out of `owed`).
    pub async fn reward_claim(&self, epoch: u64) -> bool {
        let ok = self.submit_own(LedgerOp::RewardClaim(epoch)).await;
        if ok {
            self.settlement
                .write()
                .await
                .claim(epoch, self.identity.node_id().0);
        }
        ok
    }

    /// Cross-node epoch close (§10.1, node-orchestrated by the settlement loop): fold this epoch's
    /// AGGREGATED pool (`pool_add` = Σ every node's announced pays) into `unallocated`, then distribute to
    /// `contributions` (Σ every node's announced served bytes) by ratio → the epoch RECORD. Idempotent per
    /// epoch. Every node calls this with the identical `(pool_add, contributions)` from the same converged
    /// announcement board, so the record is bit-for-bit identical network-wide.
    pub async fn settle_from_board(
        &self,
        epoch: u64,
        pool_add: u64,
        contributions: Vec<Contribution>,
    ) -> RewardRecord {
        self.settlement
            .write()
            .await
            .settle_epoch_with_pool(epoch, pool_add, contributions)
    }

    /// DEV/manual override (the `ledger-settle-epoch` RPC): inject `pool` + settle `epoch` with the given
    /// `contributions` directly, bypassing the announcement loop — exercises the settlement math offline.
    /// The production path is [`settle_from_board`], driven automatically by the settlement loop.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_cid_is_stable_and_nonzero() {
        // The embedded program has a stable content cid (the anchor referent).
        assert_eq!(ledger_program_cid(), ledger_program_cid());
        assert_ne!(ledger_program_cid(), [0u8; 32]);
        // Sentinel owner is derived from it (routes to the committee) and isn't the cid itself.
        let owner = AnchorDispatcher::anchor_owner(&ledger_program_cid());
        assert_ne!(owner, ledger_program_cid());
    }
}
