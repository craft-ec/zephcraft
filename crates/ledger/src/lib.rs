//! `zeph-ledger` — the token ledger's pure, deterministic transition logic + wire schemas
//! (TOKEN_LEDGER_BUILD.md §4; the canonical token PROTOCOL PROGRAM, not a user app). `#![no_std]` so
//! the identical crate compiles for the `apps/ledger-wasm` governed-WASM program (which the node runs
//! behind the K1 `token-ledger` anchor) and for native noded/CLI/tests.
//!
//! **Model.** A balance is the fold of an account's OWN sequence: each ordered write is one
//! [`apply`] step (like the registry program — `(prev_state, op) → new_state`). Every node, and a
//! verifier re-run, computes the identical state from the same public, quorum-ordered sequence —
//! *validity by re-execution*, no committee for the fold itself.
//!
//! **Recipient credit = CLAIM** (not a global scan). A transfer DEBITS the sender's own chain; the
//! recipient later CLAIMS it onto *its* own chain, referencing the sender's debit. So every account's
//! state stays a pure fold of only its own chain (§6: O(1)/account, no "who-owes-me" index), and
//! "no double-credit" is an ordinary same-chain dedup ([`LedgerBalanceState::processed_claims`]).
//!
//! **Reserved namespace.** Balances are self-custodial account-chains, not PDAs (§3): the owner signs
//! the write (the sequencer's `owner_authentic` gate), but the transition here CONSTRAINS it to a
//! valid step — a modified client can submit garbage, but re-execution of the canonical cid rejects
//! any state that isn't `apply(...)`, so nobody can forge a balance.

#![no_std]

extern crate alloc;

use alloc::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A transfer: DEBIT the sender (this account) by `amount`, in favour of `to` (who later CLAIMs it).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TransferOp {
    pub to: [u8; 32],
    pub amount: u64,
    /// Opaque application memo (e.g. an invoice id); not interpreted by the ledger.
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

/// One ledger write on an account's sequence.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LedgerOp {
    Transfer(TransferOp),
    Claim(ClaimOp),
}

/// An account's ledger state — the fold of its own sequence.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct LedgerBalanceState {
    pub balance: u64,
    /// `(debit_account, debit_nonce)` already claimed — single-use, self-contained (§4b): a duplicate
    /// claim is caught the same way a duplicate nonce is, with no global spent-set.
    pub processed_claims: BTreeSet<([u8; 32], u64)>,
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
}

/// The resolved debit a claim references: the node supplies the sender's ORDERED `TransferOp`
/// (validated as a committed entry of `debit_account`'s sequence) so this pure transition can check
/// `to == me` + the amount. `None` at the call site means the node could not resolve/validate it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDebit {
    pub transfer: TransferOp,
}

/// The full input the node hands the ledger program for one write: the account being advanced (its
/// identity is authenticated by the sequencer's `owner_authentic` gate — the owner signed for it — so
/// the program trusts it), the op (the write's payload), and, for a `Claim`, the node-resolved debit.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerInput {
    pub account: [u8; 32],
    pub op: LedgerOp,
    #[serde(default)]
    pub debit: Option<ResolvedDebit>,
}

/// Convenience: decode the prior state + a [`LedgerInput`], apply, and return the new state blob to
/// commit (`None` = reject → commit nothing). This is the whole program body — the `apps/ledger-wasm`
/// wrapper and the native node both call it, so their results are identical by construction.
pub fn run_transition(prev_state: &[u8], input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let state = LedgerBalanceState::decode(prev_state)?;
    let inp: LedgerInput = postcard::from_bytes(input).ok()?;
    let next = apply(state, &inp.op, &inp.account, inp.debit.as_ref())?;
    postcard::to_allocvec(&next).ok()
}

/// Apply one ledger op to `caller`'s state — pure + deterministic, so every node and every verifier
/// re-run computes the identical next state. Returns `None` on ANY rejection (the program then commits
/// nothing, which the node treats as a rejected write).
///
/// - **Transfer**: debit `caller` by `amount` iff the balance suffices (`amount > 0`). The recipient
///   is credited later, by its own claim.
/// - **Claim**: credit `caller` by the resolved debit's amount iff the debit credits `caller`
///   (`debit.transfer.to == caller`) and `(debit_account, debit_nonce)` was not already claimed.
pub fn apply(
    mut state: LedgerBalanceState,
    op: &LedgerOp,
    caller: &[u8; 32],
    debit: Option<&ResolvedDebit>,
) -> Option<LedgerBalanceState> {
    match op {
        LedgerOp::Transfer(t) => {
            if t.amount == 0 {
                return None; // a zero transfer is a no-op; reject rather than churn a nonce
            }
            state.balance = state.balance.checked_sub(t.amount)?; // insufficient funds → reject
            Some(state)
        }
        LedgerOp::Claim(c) => {
            let d = debit?; // the node must resolve + validate the referenced ordered debit
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

    #[test]
    fn transfer_debits_and_rejects_overdraft_and_zero() {
        let alice = acct(1);
        let bob = acct(2);
        let st = LedgerBalanceState {
            balance: 100,
            ..Default::default()
        };
        // Debit 30 → 70.
        let st = apply(st, &transfer(bob, 30), &alice, None).unwrap();
        assert_eq!(st.balance, 70);
        // Overdraft rejected (state unchanged — caller keeps the prior).
        assert!(apply(st.clone(), &transfer(bob, 71), &alice, None).is_none());
        // Zero rejected.
        assert!(apply(st, &transfer(bob, 0), &alice, None).is_none());
    }

    #[test]
    fn claim_credits_once_and_rejects_wrong_recipient_missing_and_replay() {
        let alice = acct(1);
        let bob = acct(2);
        let carol = acct(3);
        // Alice debited a 40-transfer to Bob at her nonce 7 (resolved by the node).
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

        // Missing/unresolved debit → reject.
        assert!(apply(LedgerBalanceState::default(), &claim, &bob, None).is_none());
        // Wrong recipient (Carol claims Bob's credit) → reject.
        assert!(apply(LedgerBalanceState::default(), &claim, &carol, Some(&debit)).is_none());

        // Bob claims → +40.
        let bob_st = apply(LedgerBalanceState::default(), &claim, &bob, Some(&debit)).unwrap();
        assert_eq!(bob_st.balance, 40);
        assert!(bob_st.processed_claims.contains(&(alice, 7)));
        // Replay of the same debit → reject (single-use), balance unchanged.
        assert!(apply(bob_st, &claim, &bob, Some(&debit)).is_none());
    }

    #[test]
    fn transfer_then_claim_conserves_supply() {
        let alice = acct(1);
        let bob = acct(2);
        let alice_st = LedgerBalanceState {
            balance: 100,
            ..Default::default()
        };
        // Alice debits 25 to Bob at nonce 0.
        let alice_st = apply(alice_st, &transfer(bob, 25), &alice, None).unwrap();
        let debit = ResolvedDebit {
            transfer: TransferOp {
                to: bob,
                amount: 25,
                memo: [0u8; 32],
            },
        };
        let bob_st = apply(
            LedgerBalanceState::default(),
            &LedgerOp::Claim(ClaimOp {
                debit_account: alice,
                debit_nonce: 0,
            }),
            &bob,
            Some(&debit),
        )
        .unwrap();
        // Conservation: Alice 75 + Bob 25 = 100.
        assert_eq!(alice_st.balance + bob_st.balance, 100);
        assert_eq!(alice_st.balance, 75);
        assert_eq!(bob_st.balance, 25);
    }

    #[test]
    fn state_roundtrips_through_postcard() {
        let mut st = LedgerBalanceState {
            balance: 4242,
            ..Default::default()
        };
        st.processed_claims.insert((acct(9), 3));
        let bytes = postcard::to_allocvec(&st).unwrap();
        let back = LedgerBalanceState::decode(&bytes).unwrap();
        assert_eq!(st, back);
        // Empty prior → fresh zero account.
        assert_eq!(
            LedgerBalanceState::decode(&[]).unwrap(),
            LedgerBalanceState::default()
        );
    }

    #[test]
    fn run_transition_is_the_whole_program_body() {
        // The exact path `apps/ledger-wasm` runs: decode prev + LedgerInput → apply → encode.
        let alice = acct(1);
        let bob = acct(2);
        let prev = postcard::to_allocvec(&LedgerBalanceState {
            balance: 50,
            ..Default::default()
        })
        .unwrap();
        let input = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 20),
            debit: None,
        })
        .unwrap();
        let out = run_transition(&prev, &input).expect("valid transfer commits");
        let next: LedgerBalanceState = postcard::from_bytes(&out).unwrap();
        assert_eq!(next.balance, 30);
        // An overdraft input rejects → no commit.
        let bad = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 999),
            debit: None,
        })
        .unwrap();
        assert!(run_transition(&prev, &bad).is_none());
    }
}
