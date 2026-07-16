//! `zeph-token` — the minimal, stable TOKEN protocol program: pure money mechanics only
//! (`ECONOMY_PROGRAMS_DESIGN.md`). It owns the account-chain op vocabulary ([`LedgerOp`]) and the
//! VALUE-authority fold: [`TokenState`] (`balance` + claim dedup) advanced by [`apply_token`]. All
//! *policy* (egress pricing, subscriptions, reward records, per-consumer caps) lives in the separate
//! `zeph-economy-egress` program; this crate never learns what a subscription is — it just moves value.
//!
//! `#![no_std]` so the identical crate compiles for the wasm program and native noded/CLI/tests.
//!
//! **Model.** A balance is the fold of an account's OWN quorum-ordered sequence — *validity by
//! re-execution*, no committee for the fold (individual, not global, coordination). Transfer DEBITs the
//! sender; the recipient CLAIMs it onto its own chain (O(1)/account, no global "who-owes-me" scan). The
//! `Pay`/`RewardClaim` VALUE effects (debit into the pool / credit a resolved reward share) live here
//! because value is token's authority; their POLICY (the pool, the record, the per-epoch dedup) is
//! economy-egress's — the two co-fold one write (`ECONOMY_PROGRAMS_DESIGN.md §3/§5b`).

#![no_std]

extern crate alloc;

use alloc::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A transfer: DEBIT the sender (this account) by `amount`, in favour of `to` (who later CLAIMs it).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TransferOp {
    pub to: [u8; 32],
    pub amount: u64,
    /// Opaque application memo (e.g. an invoice id); not interpreted by the token program.
    pub memo: [u8; 32],
}

/// A claim: CREDIT the recipient (this account) with the transfer the sender debited at
/// `(debit_account, debit_nonce)`. Validated against the node-resolved debit (`to == me`, amount) +
/// single-use dedup.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClaimOp {
    pub debit_account: [u8; 32],
    pub debit_nonce: u64,
}

/// One ledger write on an account's sequence. The account-chain op vocabulary — token applies the
/// VALUE effect of each; economy-egress co-folds the POLICY effect of `Pay`/`RewardClaim`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LedgerOp {
    Transfer(TransferOp),
    Claim(ClaimOp),
    /// PAY `amount` into the epoch pool — a self-authored debit (`§10.1` pay-into-pool). Token debits
    /// the balance; economy-egress sums `Pay` writes into the (derived) pool. No escrow, no cross-draw.
    Pay(u64),
    /// A PROVIDER claims its reward for `epoch`. Token credits the node-resolved share; economy-egress
    /// owns the single-use-per-epoch dedup + resolves the share from the verified record.
    RewardClaim(u64),
}

/// An account's TOKEN state — the money slice of the account fold.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenState {
    pub balance: u64,
    /// `(debit_account, debit_nonce)` already claimed — single-use, self-contained (no global spent-set).
    pub processed_claims: BTreeSet<([u8; 32], u64)>,
}

/// The resolved debit a claim references: the node supplies the sender's ORDERED `TransferOp`
/// (validated as a committed entry of `debit_account`'s sequence) so this pure transition can check
/// `to == me` + the amount.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDebit {
    pub transfer: TransferOp,
}

/// The node-supplied context a transition needs beyond its own state: the resolved debit (for a
/// `Claim`) and the resolved reward share (for a `RewardClaim`, from the verified epoch reward record,
/// via economy-egress). Both are node-resolved and re-checked by re-execution.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Resolved {
    #[serde(default)]
    pub debit: Option<ResolvedDebit>,
    #[serde(default)]
    pub reward_share: Option<u64>,
}

/// The full input the node hands the program for one write: the account being advanced (its identity
/// is authenticated by the sequencer's `owner_authentic` gate), the op, and the resolved context.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerInput {
    pub account: [u8; 32],
    pub op: LedgerOp,
    #[serde(default)]
    pub resolved: Resolved,
}

/// Apply one op's VALUE effect to `caller`'s TOKEN state — pure + deterministic. Returns `None` on any
/// rejection (commits nothing). It does NOT touch reward-epoch dedup (that is economy-egress's slice,
/// co-folded); for a `RewardClaim` it credits the resolved `share` and trusts economy to have deduped.
///
/// - **Transfer**: debit `caller` by `amount` iff the balance suffices (`amount > 0`).
/// - **Claim**: credit `caller` by the resolved debit iff `debit.transfer.to == caller` and the
///   `(debit_account, debit_nonce)` was not already claimed.
/// - **Pay**: debit `amount` iff sufficient (`amount > 0`) — the pool total is summed by economy-egress.
/// - **RewardClaim**: credit `caller` by its resolved epoch share iff the share is non-zero.
pub fn apply_token(
    mut state: TokenState,
    op: &LedgerOp,
    caller: &[u8; 32],
    resolved: &Resolved,
) -> Option<TokenState> {
    match op {
        LedgerOp::Transfer(t) => {
            if t.amount == 0 {
                return None; // a zero transfer is a no-op; reject rather than churn a nonce
            }
            state.balance = state.balance.checked_sub(t.amount)?; // insufficient funds → reject
            Some(state)
        }
        LedgerOp::Claim(c) => {
            let d = resolved.debit.as_ref()?; // the node must resolve + validate the ordered debit
            if &d.transfer.to != caller {
                return None; // the debit does not credit me
            }
            let key = (c.debit_account, c.debit_nonce);
            if !state.processed_claims.insert(key) {
                return None; // already claimed (single-use)
            }
            state.balance = state.balance.checked_add(d.transfer.amount)?;
            Some(state)
        }
        LedgerOp::Pay(amount) => {
            if *amount == 0 {
                return None;
            }
            state.balance = state.balance.checked_sub(*amount)?; // insufficient balance → reject
            Some(state)
        }
        LedgerOp::RewardClaim(_epoch) => {
            let share = resolved.reward_share?; // node-resolved from the verified epoch record
            if share == 0 {
                return None; // nothing to claim
            }
            state.balance = state.balance.checked_add(share)?;
            Some(state)
        }
    }
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
    fn dctx(d: &ResolvedDebit) -> Resolved {
        Resolved {
            debit: Some(d.clone()),
            reward_share: None,
        }
    }

    #[test]
    fn transfer_debits_and_rejects_overdraft_and_zero() {
        let (alice, bob) = (acct(1), acct(2));
        let st = TokenState {
            balance: 100,
            ..Default::default()
        };
        let st = apply_token(st, &transfer(bob, 30), &alice, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 70);
        assert!(
            apply_token(st.clone(), &transfer(bob, 71), &alice, &Resolved::default()).is_none()
        );
        assert!(apply_token(st, &transfer(bob, 0), &alice, &Resolved::default()).is_none());
    }

    #[test]
    fn claim_credits_once_and_rejects_wrong_recipient_missing_and_replay() {
        let (alice, bob, carol) = (acct(1), acct(2), acct(3));
        let debit = ResolvedDebit {
            transfer: TransferOp {
                to: bob,
                amount: 40,
                memo: [0u8; 32],
            },
        };
        let claim = LedgerOp::Claim(ClaimOp {
            debit_account: alice,
            debit_nonce: 7,
        });
        assert!(apply_token(TokenState::default(), &claim, &bob, &Resolved::default()).is_none());
        assert!(apply_token(TokenState::default(), &claim, &carol, &dctx(&debit)).is_none());
        let bob_st = apply_token(TokenState::default(), &claim, &bob, &dctx(&debit)).unwrap();
        assert_eq!(bob_st.balance, 40);
        assert!(bob_st.processed_claims.contains(&(alice, 7)));
        assert!(apply_token(bob_st, &claim, &bob, &dctx(&debit)).is_none());
    }

    #[test]
    fn pay_debits_and_reward_claim_credits_the_resolved_share() {
        let p = acct(5);
        let st = TokenState {
            balance: 100,
            ..Default::default()
        };
        let st = apply_token(st, &LedgerOp::Pay(40), &p, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 60);
        assert!(apply_token(st.clone(), &LedgerOp::Pay(61), &p, &Resolved::default()).is_none());
        assert!(apply_token(st.clone(), &LedgerOp::Pay(0), &p, &Resolved::default()).is_none());
        let rctx = Resolved {
            debit: None,
            reward_share: Some(15),
        };
        let st = apply_token(st, &LedgerOp::RewardClaim(7), &p, &rctx).unwrap();
        assert_eq!(
            st.balance, 75,
            "credits the resolved share (dedup is economy's slice)"
        );
        // A missing or zero resolved share is rejected (nothing to claim).
        assert!(apply_token(
            st.clone(),
            &LedgerOp::RewardClaim(8),
            &p,
            &Resolved::default()
        )
        .is_none());
        let zero = Resolved {
            debit: None,
            reward_share: Some(0),
        };
        assert!(apply_token(st, &LedgerOp::RewardClaim(8), &p, &zero).is_none());
    }
}
