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
    /// Lock `amount` of liquid balance into egress ESCROW (a consumer pre-authorises settlement to
    /// draw up to this for its paid egress; §7). Reversible only by settlement consuming it.
    Escrow(u64),
    /// A PROVIDER claims its reward share for `epoch` from the verified epoch reward RECORD (§10.1) —
    /// single-use per epoch; the node resolves the share from the record.
    RewardClaim(u64),
}

/// An account's ledger state — the fold of its own sequence.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct LedgerBalanceState {
    pub balance: u64,
    /// Tokens LOCKED for egress (already removed from `balance`); settlement draws paid egress from
    /// here, so the escrow lock is the consumer's standing authorisation to be charged (§7/§10.9).
    pub escrowed: u64,
    /// `(debit_account, debit_nonce)` already claimed — single-use, self-contained (§4b): a duplicate
    /// claim is caught the same way a duplicate nonce is, with no global spent-set.
    pub processed_claims: BTreeSet<([u8; 32], u64)>,
    /// Reward epochs already claimed — a `RewardClaim` is single-use per epoch.
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
}

/// The resolved debit a claim references: the node supplies the sender's ORDERED `TransferOp`
/// (validated as a committed entry of `debit_account`'s sequence) so this pure transition can check
/// `to == me` + the amount.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDebit {
    pub transfer: TransferOp,
}

/// The node-supplied context a transition needs beyond its own state: the resolved debit (for a
/// `Claim`) and the resolved reward share (for a `RewardClaim`, from the verified epoch reward
/// record). Both are node-resolved and re-checked by re-execution; a missing one for the op that
/// needs it rejects the write.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Resolved {
    #[serde(default)]
    pub debit: Option<ResolvedDebit>,
    #[serde(default)]
    pub reward_share: Option<u64>,
}

/// The full input the node hands the ledger program for one write: the account being advanced (its
/// identity is authenticated by the sequencer's `owner_authentic` gate — the owner signed for it — so
/// the program trusts it), the op (the write's payload), and the resolved context it needs.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerInput {
    pub account: [u8; 32],
    pub op: LedgerOp,
    #[serde(default)]
    pub resolved: Resolved,
}

/// Convenience: decode the prior state + a [`LedgerInput`], apply, and return the new state blob to
/// commit (`None` = reject → commit nothing). This is the whole program body — the `apps/ledger-wasm`
/// wrapper and the native node both call it, so their results are identical by construction.
pub fn run_transition(prev_state: &[u8], input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let state = LedgerBalanceState::decode(prev_state)?;
    let inp: LedgerInput = postcard::from_bytes(input).ok()?;
    let next = apply(state, &inp.op, &inp.account, &inp.resolved)?;
    postcard::to_allocvec(&next).ok()
}

/// Apply one ledger op to `caller`'s state — pure + deterministic, so every node and every verifier
/// re-run computes the identical next state. Returns `None` on ANY rejection (commits nothing).
///
/// - **Transfer**: debit `caller` by `amount` iff the balance suffices (`amount > 0`).
/// - **Claim**: credit `caller` by the resolved debit iff `debit.transfer.to == caller` and the
///   `(debit_account, debit_nonce)` was not already claimed.
/// - **Escrow**: lock `amount` of `caller`'s balance into `escrowed` (iff sufficient, `amount > 0`).
/// - **RewardClaim**: credit `caller` by its resolved epoch share iff that epoch was not already
///   claimed and the share is non-zero.
pub fn apply(
    mut state: LedgerBalanceState,
    op: &LedgerOp,
    caller: &[u8; 32],
    resolved: &Resolved,
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
        LedgerOp::Escrow(amount) => {
            if *amount == 0 {
                return None;
            }
            state.balance = state.balance.checked_sub(*amount)?; // insufficient balance → reject
            state.escrowed = state.escrowed.checked_add(*amount)?;
            Some(state)
        }
        LedgerOp::RewardClaim(epoch) => {
            let share = resolved.reward_share?; // node-resolved from the verified epoch record
            if share == 0 {
                return None; // nothing to claim
            }
            if !state.claimed_epochs.insert(*epoch) {
                return None; // this epoch's reward already claimed (single-use)
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

    /// A resolved context carrying just a debit (for `Claim` tests).
    fn dctx(d: &ResolvedDebit) -> Resolved {
        Resolved {
            debit: Some(d.clone()),
            reward_share: None,
        }
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
        let st = apply(st, &transfer(bob, 30), &alice, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 70);
        // Overdraft rejected (state unchanged — caller keeps the prior).
        assert!(apply(st.clone(), &transfer(bob, 71), &alice, &Resolved::default()).is_none());
        // Zero rejected.
        assert!(apply(st, &transfer(bob, 0), &alice, &Resolved::default()).is_none());
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
        assert!(apply(
            LedgerBalanceState::default(),
            &claim,
            &bob,
            &Resolved::default()
        )
        .is_none());
        // Wrong recipient (Carol claims Bob's credit) → reject.
        assert!(apply(LedgerBalanceState::default(), &claim, &carol, &dctx(&debit)).is_none());

        // Bob claims → +40.
        let bob_st = apply(LedgerBalanceState::default(), &claim, &bob, &dctx(&debit)).unwrap();
        assert_eq!(bob_st.balance, 40);
        assert!(bob_st.processed_claims.contains(&(alice, 7)));
        // Replay of the same debit → reject (single-use), balance unchanged.
        assert!(apply(bob_st, &claim, &bob, &dctx(&debit)).is_none());
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
        let alice_st = apply(alice_st, &transfer(bob, 25), &alice, &Resolved::default()).unwrap();
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
            &dctx(&debit),
        )
        .unwrap();
        // Conservation: Alice 75 + Bob 25 = 100.
        assert_eq!(alice_st.balance + bob_st.balance, 100);
        assert_eq!(alice_st.balance, 75);
        assert_eq!(bob_st.balance, 25);
    }

    #[test]
    fn escrow_locks_balance_and_reward_claim_credits_once() {
        let p = acct(5);
        let st = LedgerBalanceState {
            balance: 100,
            ..Default::default()
        };
        // Escrow 40 → balance 60, escrowed 40 (tokens leave liquid balance into the lock).
        let st = apply(st, &LedgerOp::Escrow(40), &p, &Resolved::default()).unwrap();
        assert_eq!(st.balance, 60);
        assert_eq!(st.escrowed, 40);
        // Over-escrow (more than the liquid balance) and zero are rejected.
        assert!(apply(st.clone(), &LedgerOp::Escrow(61), &p, &Resolved::default()).is_none());
        assert!(apply(st.clone(), &LedgerOp::Escrow(0), &p, &Resolved::default()).is_none());

        // RewardClaim epoch 7 with a node-resolved share of 15 → +15, epoch marked.
        let rctx = Resolved {
            debit: None,
            reward_share: Some(15),
        };
        let st = apply(st, &LedgerOp::RewardClaim(7), &p, &rctx).unwrap();
        assert_eq!(st.balance, 75);
        assert!(st.claimed_epochs.contains(&7));
        // Replaying the same epoch is rejected (single-use).
        assert!(apply(st.clone(), &LedgerOp::RewardClaim(7), &p, &rctx).is_none());
        // A missing or zero resolved share is rejected (nothing to claim).
        assert!(apply(
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
        assert!(apply(st, &LedgerOp::RewardClaim(8), &p, &zero).is_none());
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
            resolved: Resolved::default(),
        })
        .unwrap();
        let out = run_transition(&prev, &input).expect("valid transfer commits");
        let next: LedgerBalanceState = postcard::from_bytes(&out).unwrap();
        assert_eq!(next.balance, 30);
        // An overdraft input rejects → no commit.
        let bad = postcard::to_allocvec(&LedgerInput {
            account: alice,
            op: transfer(bob, 999),
            resolved: Resolved::default(),
        })
        .unwrap();
        assert!(run_transition(&prev, &bad).is_none());
    }
}
