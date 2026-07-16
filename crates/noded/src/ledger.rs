//! `LedgerService` — the node-side integration of the TOKEN protocol program (the `token` anchor;
//! ECONOMY_PROGRAMS_DESIGN.md §5, P5 split). It:
//! - **authors** owner-signed ledger writes into the sequencer, ordered by the **epoch committee**
//!   (the write's owner is the anchor sentinel, so `AnchorAwareQuorumSource` routes to the committee);
//! - **folds** an account's committed sequence into its balance via the shared [`zeph_token`] crate —
//!   NATIVELY, identical to a verifier re-running the wasm program by construction (same crate).
//!
//! The token *program* is the embedded [`TOKEN_WASM`] (the canonical cid pinned behind the K1
//! `token` anchor, for verification/governance-swap); the node's own balance computation folds
//! natively. Validity is by re-execution (the fold), not a committee for the fold itself; an invalid
//! write (e.g. an overdraft) folds to a no-op, so it can occupy a nonce but never corrupts the balance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_token::{apply_token, ClaimOp, LedgerOp, Resolved, ResolvedDebit, TokenState, TransferOp};

use crate::anchor::AnchorDispatcher;
use crate::economy_egress::EconomyEgressService;
use crate::sequence::SequenceStore;

/// The embedded TOKEN WASM program — the canonical `token` cid, and the program of every account chain.
/// Built from `apps/token-wasm` (a thin wrapper over `zeph-token`), so re-running it reproduces the
/// node's native fold.
const TOKEN_WASM: &[u8] = include_bytes!("../token.wasm");

/// The canonical token program cid = the content hash of the embedded wasm. This is the account chain's
/// `program_cid`, so it IS the chain's identity: changing the program's bytes starts a fresh chain.
pub fn token_program_cid() -> [u8; 32] {
    Cid::of(TOKEN_WASM).0
}

pub struct LedgerService {
    identity: Arc<NodeIdentity>,
    sequence: Arc<SequenceStore>,
    /// The canonical TOKEN program cid (the sequencer's `program_cid` for every account chain).
    cid: [u8; 32],
    /// The deterministic sentinel owner of the anchored token program — routes ordering to the committee
    /// (a network-owned program has no owner key). One owner, one committee, many accounts.
    owner: [u8; 32],
    /// The ECONOMY-EGRESS policy service (settlement pool + committee-attested records). Token is the
    /// value authority and owns the claim DEDUP; economy answers only "what SHARE is this provider owed
    /// for this epoch" ([`EconomyEgressService::reward_share`]), which token folds as `Resolved`.
    /// One-directional: token → economy, no cycle, no cross-program transaction.
    economy: Arc<EconomyEgressService>,
    /// Scan cache for [`paid_total`](Self::paid_total): `account → (next_nonce, running Pay total)`. The
    /// ledger chain is append-only, so a re-read only sums NEW `Pay` writes instead of the whole chain.
    paid_scan: Mutex<HashMap<[u8; 32], (u64, u64)>>,
}

impl LedgerService {
    pub fn new(
        identity: Arc<NodeIdentity>,
        sequence: Arc<SequenceStore>,
        economy: Arc<EconomyEgressService>,
    ) -> Self {
        let cid = token_program_cid();
        let owner = AnchorDispatcher::anchor_owner(&cid);
        Self {
            identity,
            sequence,
            cid,
            owner,
            economy,
            paid_scan: Mutex::new(HashMap::new()),
        }
    }

    /// The embedded token wasm bytes — consumed by the genesis step (publish the wasm to obj so
    /// verifiers can fetch it, and pin the `token` anchor).
    #[allow(dead_code)]
    pub fn wasm() -> &'static [u8] {
        TOKEN_WASM
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

    /// The account's cumulative committed `Pay` total — the sum of its `Pay` writes on the durable,
    /// committee-ordered ledger chain. This is the DURABLE proof of the paid side of settlement: a node's
    /// reported `paid_cumulative` is cross-checked against (capped at) this, so it can't inflate the pool
    /// beyond what it actually paid.
    pub async fn paid_total(&self, account: [u8; 32]) -> u64 {
        let Some(seq) = self
            .sequence
            .sequence_of(self.owner, self.cid, account)
            .await
        else {
            return self
                .paid_scan
                .lock()
                .unwrap()
                .get(&account)
                .map(|(_, t)| *t)
                .unwrap_or(0);
        };
        let end = seq.next_nonce();
        // Sum only the NEW nonces since the last scan (the chain is append-only, so a committed nonce's
        // payload never changes → resuming from the cached position is identical to a full re-scan). The
        // lock is held only for the sync scan — no await inside.
        let mut cache = self.paid_scan.lock().unwrap();
        let (next, total) = cache.entry(account).or_insert((0, 0));
        while *next < end {
            let Some(payload) = seq.payload_at(*next) else {
                break;
            };
            if let Ok(LedgerOp::Pay(amount)) = postcard::from_bytes::<LedgerOp>(payload) {
                *total = total.saturating_add(amount);
            }
            *next += 1;
        }
        *total
    }

    /// Fold `account`'s committed sequence into its balance state (native — identical to a wasm
    /// re-run). An invalid write (`apply → None`) is a no-op, leaving the prior state.
    pub async fn balance(&self, account: [u8; 32]) -> TokenState {
        let Some(seq) = self
            .sequence
            .sequence_of(self.owner, self.cid, account)
            .await
        else {
            return TokenState::default();
        };
        let mut state = TokenState::default();
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
                // A RewardClaim's share resolves from the CANONICAL committee-attested record if one is
                // finalized (durable + restart-safe), else the local in-memory record. `apply_token`
                // enforces single-use (its own `claimed_epochs`), so a replay credits nothing regardless
                // of the source — the dedup is token's, not economy's.
                LedgerOp::RewardClaim(epoch) => Resolved {
                    debit: None,
                    reward_share: Some(self.economy.reward_share(*epoch, &account).await),
                },
                _ => Resolved::default(),
            };
            if let Some(next) = apply_token(state.clone(), &op, &account, &resolved) {
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

    /// PAY `amount` of egress cost into the epoch pool — a self-authored debit (§10.1 pay-into-pool). The
    /// committed `Pay` write on the durable ledger chain IS the record; the settlement loop reads this
    /// node's cumulative from it via [`paid_total`](Self::paid_total) (no in-memory counter, so it survives
    /// data loss). The pool is fed cross-node from the settlement reports, not locally here.
    pub async fn pay(&self, amount: u64) -> bool {
        self.submit_own(LedgerOp::Pay(amount)).await
    }

    /// Claim this node's reward share for `epoch` (single-use, §10.1). The share is resolved from the
    /// settlement pool's epoch record; on commit, the pool marks it claimed (out of `owed`).
    pub async fn reward_claim(&self, epoch: u64) -> bool {
        let ok = self.submit_own(LedgerOp::RewardClaim(epoch)).await;
        if ok {
            // The authoritative dedup is token's own `claimed_epochs` (folded with the credit). This only
            // moves the epoch out of the pool's `owed` accounting — observability, not a safety gate.
            self.economy
                .mark_claimed(epoch, self.identity.node_id().0)
                .await;
        }
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_cid_is_stable_and_nonzero() {
        // The embedded program has a stable content cid (the anchor referent).
        assert_eq!(token_program_cid(), token_program_cid());
        assert_ne!(token_program_cid(), [0u8; 32]);
        // Sentinel owner is derived from it (routes to the committee) and isn't the cid itself.
        let owner = AnchorDispatcher::anchor_owner(&token_program_cid());
        assert_ne!(owner, token_program_cid());
    }
}
