//! `zeph-ledger` — the COMBINER over the split token/economy protocol programs
//! (`ECONOMY_PROGRAMS_DESIGN.md §5b`). The value authority is [`zeph_token`] ([`TokenState`] +
//! [`apply_token`]); the egress policy authority is [`zeph_economy_egress`] ([`EconomyState`] +
//! [`apply_economy`]). This crate co-folds them over ONE account write into the flat
//! [`LedgerBalanceState`] the node/wasm still commit — a behaviour-preserving seam: the committed state
//! bytes are byte-identical to the pre-split monolith, so the split is invisible on the wire until the
//! deployed program split (P5). `#![no_std]` so the identical crate compiles for wasm and native.
//!
//! **Co-fold order (`RewardClaim`).** Economy folds first (single-use-per-epoch dedup; reject if already
//! claimed) → token credits the resolved share. Both are the effect of one self-authored write on the
//! provider's own chain (single-writer → atomic); the combiner only commits if BOTH slices accept, so a
//! partial mark is discarded on any rejection.

#![no_std]

extern crate alloc;

use alloc::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use zeph_economy_egress::{apply_economy, EconomyState};
use zeph_token::{apply_token, TokenState};

// Re-export the shared vocabulary + context so existing `zeph_ledger::{LedgerOp, ...}` call sites
// (noded LedgerService, apps/ledger-wasm, CLI) keep working unchanged.
pub use zeph_token::{ClaimOp, LedgerInput, LedgerOp, Resolved, ResolvedDebit, TransferOp};

/// An account's ledger state — the COMBINED fold (token value slice + economy policy slice), kept in a
/// FLAT layout byte-identical to the pre-split monolith so committed state is unchanged.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct LedgerBalanceState {
    pub balance: u64,
    /// `(debit_account, debit_nonce)` already claimed (token slice — single-use claim dedup).
    pub processed_claims: BTreeSet<([u8; 32], u64)>,
    /// Reward epochs already claimed (economy slice — single-use-per-epoch reward dedup).
    pub claimed_epochs: BTreeSet<u64>,
}

impl LedgerBalanceState {
    /// Decode from an account's prior state blob (empty = a fresh, zero-balance account).
    pub fn decode(prev: &[u8]) -> Option<Self> {
        if prev.is_empty() {
            Some(Self::default())
        } else {
            postcard::from_bytes(prev).ok()
        }
    }

    fn split(&self) -> (TokenState, EconomyState) {
        (
            TokenState {
                balance: self.balance,
                processed_claims: self.processed_claims.clone(),
            },
            EconomyState {
                claimed_epochs: self.claimed_epochs.clone(),
            },
        )
    }
}

/// Convenience: decode the prior state + a [`LedgerInput`], apply, and return the new state blob to
/// commit (`None` = reject → commit nothing). The whole program body — the wasm wrapper and native node
/// both call it, so their results are identical by construction.
pub fn run_transition(prev_state: &[u8], input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let state = LedgerBalanceState::decode(prev_state)?;
    let inp: LedgerInput = postcard::from_bytes(input).ok()?;
    let next = apply(state, &inp.op, &inp.account, &inp.resolved)?;
    postcard::to_allocvec(&next).ok()
}

/// Apply one ledger op by CO-FOLDING the two slices — pure + deterministic, byte-identical to the
/// pre-split fold. Economy's policy effect first (so a `RewardClaim` dedup rejects the whole write
/// before token credits), then token's value effect. Returns `None` on ANY rejection (nothing commits,
/// so a partial economy mark is discarded).
pub fn apply(
    state: LedgerBalanceState,
    op: &LedgerOp,
    caller: &[u8; 32],
    resolved: &Resolved,
) -> Option<LedgerBalanceState> {
    let (token, economy) = state.split();
    let economy = apply_economy(economy, op)?; // policy (dedup) — rejects before any credit
    let token = apply_token(token, op, caller, resolved)?; // value effect
    Some(LedgerBalanceState {
        balance: token.balance,
        processed_claims: token.processed_claims,
        claimed_epochs: economy.claimed_epochs,
    })
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    fn acct(n: u8) -> [u8; 32] {
        [n; 32]
    }
    fn transfer(to: [u8; 32], amount: u64) -> LedgerOp {
        LedgerOp::Transfer(TransferOp {
            to,
            amount,
            memo: [0u8; 32],
        })
    }

    #[test]
    fn combined_fold_matches_the_pre_split_behaviour() {
        let (alice, bob) = (acct(1), acct(2));
        let st = LedgerBalanceState {
            balance: 100,
            ..Default::default()
        };
        // Transfer debits.
        let st = apply(st, &transfer(bob, 30), &alice, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 70);
        // Pay debits.
        let st = apply(st, &LedgerOp::Pay(20), &alice, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 50);
        // RewardClaim: economy dedups + token credits the resolved share.
        let rctx = Resolved {
            debit: None,
            reward_share: Some(15),
        };
        let st = apply(st, &LedgerOp::RewardClaim(7), &alice, &rctx).unwrap();
        assert_eq!(st.balance, 65);
        assert!(st.claimed_epochs.contains(&7));
        // Replay of the epoch is rejected by the economy slice → whole write rejected, no double credit.
        assert!(apply(st.clone(), &LedgerOp::RewardClaim(7), &alice, &rctx).is_none());
    }

    #[test]
    fn a_rejected_write_discards_partial_state() {
        // RewardClaim with a fresh epoch but NO resolved share: economy would mark the epoch, but token
        // rejects (no share) → the combiner returns None, so the epoch mark is discarded (not committed).
        let p = acct(5);
        let st = LedgerBalanceState {
            balance: 10,
            ..Default::default()
        };
        assert!(apply(
            st.clone(),
            &LedgerOp::RewardClaim(9),
            &p,
            &Resolved::default()
        )
        .is_none());
        // The epoch was NOT marked (state unchanged, since the write was rejected wholesale).
        assert!(!st.claimed_epochs.contains(&9));
    }

    #[test]
    fn state_roundtrips_and_layout_is_flat() {
        let mut st = LedgerBalanceState {
            balance: 4242,
            ..Default::default()
        };
        st.processed_claims.insert((acct(9), 3));
        st.claimed_epochs.insert(7);
        let bytes = postcard::to_allocvec(&st).unwrap();
        assert_eq!(LedgerBalanceState::decode(&bytes).unwrap(), st);
        assert_eq!(
            LedgerBalanceState::decode(&[]).unwrap(),
            LedgerBalanceState::default()
        );
    }

    #[test]
    fn run_transition_is_the_whole_program_body() {
        let (alice, bob) = (acct(1), acct(2));
        let prev = postcard::to_allocvec(&LedgerBalanceState {
            balance: 50,
            ..Default::default()
        })
        .unwrap();
        let input = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 20),
            resolved: Resolved::default(),
        })
        .unwrap();
        let out = run_transition(&prev, &input).expect("valid transfer commits");
        let next: LedgerBalanceState = postcard::from_bytes(&out).unwrap();
        assert_eq!(next.balance, 30);
        let bad = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 999),
            resolved: Resolved::default(),
        })
        .unwrap();
        assert!(run_transition(&prev, &bad).is_none());
    }
}
