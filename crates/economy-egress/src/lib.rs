//! `zeph-economy-egress` — the egress ECONOMY policy program (`ECONOMY_PROGRAMS_DESIGN.md`). It is a
//! separate protocol program from `zeph-token`: token owns *value* (balances), economy-egress owns
//! *policy* — the reward record, the pool (derived, node-side), per-consumer caps, and (future) egress
//! pricing/subscriptions/expiry. This crate is the on-chain POLICY SLICE co-folded with token over one
//! account write: for a `RewardClaim` it owns the single-use-per-epoch dedup ([`EconomyState`]); token
//! then credits the resolved share. First of the `economy-*` family (economy-storage, … reuse token).
//!
//! `#![no_std]`; the reward VALUATION (`pool × bytes / Σ bytes`) is the sibling `zeph-reward` crate,
//! and the running pool + per-consumer FCFS settlement is node-side (`crates/noded/src/settlement.rs`).

#![no_std]

extern crate alloc;

use alloc::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use zeph_token::LedgerOp;

/// An account's ECONOMY-egress state — the policy slice of the account fold. Currently just the
/// reward-epoch dedup; egress subscription quotas (P6) land here without touching `zeph-token`.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct EconomyState {
    /// Reward epochs already claimed — a `RewardClaim` is single-use per epoch (the policy that makes
    /// the token credit safe). Co-folded BEFORE the token credit: if already claimed, the whole op is
    /// rejected and token never credits.
    pub claimed_epochs: BTreeSet<u64>,
}

/// Apply one op's POLICY effect to `caller`'s ECONOMY state — pure + deterministic. Returns `None` on
/// rejection (which, in the co-fold, rejects the whole write so token's value effect is discarded too).
///
/// - **RewardClaim(epoch)**: single-use dedup — reject if this epoch was already claimed, else mark it.
///   (The share amount + credit are token's slice; this slice only enforces once-per-epoch.)
/// - **Transfer / Claim / Pay**: no economy-state change (identity) — the pool is derived by the node
///   summing `Pay` writes, not stored here.
pub fn apply_economy(mut state: EconomyState, op: &LedgerOp) -> Option<EconomyState> {
    match op {
        LedgerOp::RewardClaim(epoch) => {
            if !state.claimed_epochs.insert(*epoch) {
                return None; // this epoch's reward already claimed (single-use)
            }
            Some(state)
        }
        LedgerOp::Transfer(_) | LedgerOp::Claim(_) | LedgerOp::Pay(_) => Some(state),
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use zeph_token::{ClaimOp, TransferOp};

    #[test]
    fn reward_claim_dedups_per_epoch() {
        let st = EconomyState::default();
        let st = apply_economy(st, &LedgerOp::RewardClaim(7)).unwrap();
        assert!(st.claimed_epochs.contains(&7));
        // Replaying the same epoch is rejected (single-use).
        assert!(apply_economy(st.clone(), &LedgerOp::RewardClaim(7)).is_none());
        // A different epoch is fine.
        let st = apply_economy(st, &LedgerOp::RewardClaim(8)).unwrap();
        assert!(st.claimed_epochs.contains(&8));
    }

    #[test]
    fn token_ops_leave_economy_state_unchanged() {
        let st = EconomyState::default();
        for op in [
            LedgerOp::Transfer(TransferOp {
                to: [2u8; 32],
                amount: 5,
                memo: [0u8; 32],
            }),
            LedgerOp::Claim(ClaimOp {
                debit_account: [1u8; 32],
                debit_nonce: 0,
            }),
            LedgerOp::Pay(10),
        ] {
            assert_eq!(
                apply_economy(st.clone(), &op).unwrap(),
                st,
                "token ops don't touch economy state"
            );
        }
    }
}
