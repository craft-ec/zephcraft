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
//! because value is token's authority — as does their DEDUP, because deduping a credit protects the
//! balance. Economy-egress's authority is the *valuation*: what share the record says is owed, which
//! arrives as node-resolved [`Resolved::reward_share`]. One program folds the chain; no cross-program
//! transaction exists or is needed (`ECONOMY_PROGRAMS_DESIGN.md §3`).

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

/// One ledger write on an account's sequence. The account-chain op vocabulary — token folds every op
/// (it is the chain's program); economy-egress supplies the egress VALUATION a `RewardClaim` credits.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LedgerOp {
    Transfer(TransferOp),
    Claim(ClaimOp),
    /// PAY `amount` into the epoch pool — a self-authored debit (`§10.1` pay-into-pool). Token debits
    /// the balance; economy-egress sums `Pay` writes into the (derived) pool. No escrow, no cross-draw.
    Pay(u64),
    /// A PROVIDER claims its reward for `epoch`. Token marks the epoch claimed (single-use) + credits
    /// the node-resolved share, which economy-egress resolved from the committee-attested record.
    RewardClaim(u64),
}

/// An account's TOKEN state — the complete state of the account chain (token IS the chain's program).
///
/// Both dedup sets are TOKEN's, not economy's: deduping a CREDIT is *value safety* (it protects the
/// balance from a double-credit), the same property `processed_claims` gives `Claim`. Keeping them in
/// this state — folded by this program, on the account's own single-writer chain — is what makes a
/// `RewardClaim` atomic: the dedup and the credit are one fold of one write (both or neither), with no
/// cross-program transaction (`ECONOMY_PROGRAMS_DESIGN.md §3`). Economy-egress's authority is the
/// *record* (what share is owed), which arrives as node-resolved [`Resolved::reward_share`].
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenState {
    pub balance: u64,
    /// `(debit_account, debit_nonce)` already claimed — single-use, self-contained (no global spent-set).
    pub processed_claims: BTreeSet<([u8; 32], u64)>,
    /// Reward epochs already claimed — single-use-per-epoch, so a replayed `RewardClaim` credits nothing.
    pub claimed_epochs: BTreeSet<u64>,
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

impl TokenState {
    /// Decode from an account's prior state blob (empty = a fresh, zero-balance account).
    pub fn decode(prev: &[u8]) -> Option<Self> {
        if prev.is_empty() {
            Some(Self::default())
        } else {
            postcard::from_bytes(prev).ok()
        }
    }
}

/// The WHOLE program body: decode the prior state + a [`LedgerInput`], apply, and return the new state
/// blob to commit (`None` = reject → commit nothing). The wasm program (`apps/token-wasm`) and the native
/// node both call it, so a verifier re-running the wasm reproduces the node's fold by construction.
pub fn run_transition(prev_state: &[u8], input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let state = TokenState::decode(prev_state)?;
    let inp: LedgerInput = postcard::from_bytes(input).ok()?;
    let next = apply_token(state, &inp.op, &inp.account, &inp.resolved)?;
    postcard::to_allocvec(&next).ok()
}

/// Apply one op to `caller`'s TOKEN state — pure + deterministic, the WHOLE account-chain transition
/// (token is the chain's program). Returns `None` on any rejection (commits nothing, so a rejected write
/// spends a nonce but leaves the state untouched — dedup marks included).
///
/// - **Transfer**: debit `caller` by `amount` iff the balance suffices (`amount > 0`).
/// - **Claim**: credit `caller` by the resolved debit iff `debit.transfer.to == caller` and the
///   `(debit_account, debit_nonce)` was not already claimed.
/// - **Pay**: debit `amount` iff sufficient (`amount > 0`) — the pool total is summed by economy-egress
///   from the committed `Pay` writes (derived, not held here).
/// - **RewardClaim**: mark `epoch` claimed (reject a replay) and credit `caller` by its node-resolved
///   share. Dedup + credit in ONE fold ⇒ atomic single-use, no cross-program transaction.
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
        LedgerOp::RewardClaim(epoch) => {
            let share = resolved.reward_share?; // node-resolved from the verified epoch record
            if share == 0 {
                return None; // nothing to claim
            }
            if !state.claimed_epochs.insert(*epoch) {
                return None; // already claimed this epoch (single-use) → no double credit
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
    fn reward_claim_is_single_use_per_epoch_and_atomic() {
        // The dedup + credit are ONE fold of ONE write (design §3): token owns both, so a replay can
        // never double-credit, and a REJECTED claim leaves no epoch mark behind (both or neither).
        let p = acct(5);
        let share = Resolved {
            debit: None,
            reward_share: Some(40),
        };
        let st = TokenState::default();
        let st = apply_token(st, &LedgerOp::RewardClaim(3), &p, &share).unwrap();
        assert_eq!(st.balance, 40);
        assert!(st.claimed_epochs.contains(&3));
        // Replay of the same epoch → rejected, no second credit.
        assert!(apply_token(st.clone(), &LedgerOp::RewardClaim(3), &p, &share).is_none());
        // A different epoch still claims.
        let st = apply_token(st, &LedgerOp::RewardClaim(4), &p, &share).unwrap();
        assert_eq!(st.balance, 80);
        // A claim with NO resolved share is rejected — and marks nothing (the returned state is the
        // rejection `None`, so the caller commits the prior state: epoch 9 stays unclaimed).
        assert!(apply_token(
            st.clone(),
            &LedgerOp::RewardClaim(9),
            &p,
            &Resolved::default()
        )
        .is_none());
        assert!(!st.claimed_epochs.contains(&9));
    }

    #[test]
    fn run_transition_is_the_whole_program_body() {
        // The wasm program body == the native fold (same crate) — a verifier re-run reproduces it.
        let (alice, bob) = (acct(1), acct(2));
        let prev = postcard::to_allocvec(&TokenState {
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
        let next: TokenState = postcard::from_bytes(&out).unwrap();
        assert_eq!(next.balance, 30);
        // An empty prior state = a fresh account; an overdraft commits nothing.
        let bad = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 999),
            resolved: Resolved::default(),
        })
        .unwrap();
        assert!(run_transition(&[], &bad).is_none());
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
