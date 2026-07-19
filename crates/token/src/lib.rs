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

/// Decimal places for display: the ledger stores integer BASE UNITS, and this says where the point goes.
/// 8 — Bitcoin's convention (a satoshi). Not universal (Ethereum uses 18, USDC 6, SOL 9); what matters is
/// that ONE value is canonical, and this is it.
pub const DECIMALS: u8 = 8;

/// Base units in one whole token — the scale every amount in the economy is denominated in.
///
/// **All balances, pools, shares, seeds and caps are BASE UNITS.** Storing base units and treating
/// decimals as display metadata is the standard ledger discipline: it keeps the arithmetic exact integer
/// maths with no float anywhere near money.
///
/// Two concrete defects this scale removes, beyond convention:
/// - **Dust.** Reward shares are `pool × bytes_i / Σ bytes`, floor-divided. In whole tokens the remainder
///   was a WHOLE token (1 MiB of egress at the default price); in base units it is 1e-8 of one.
/// - **An indivisible seed.** A 1-token/day seed could only land on ONE epoch of 288, so providers in the
///   other 287 earned nothing from it. In base units every epoch carries a divisible share.
///
/// `u64` holds ~1.8e11 whole tokens at this scale — vast headroom over any planned cap.
pub const ONE_TOKEN: u64 = 100_000_000;

/// THE SUPPLY LEDGER — one counter per token, which every path that creates tokens must pass through.
///
/// **This is the token's own state, not the valuation layer's.** Deciding *who gets what share* is the
/// reward program's job; deciding *how much money exists* is the token's, exactly as an ERC-20/SPL
/// contract owns its `total_supply` while nothing else may mint behind its back. The counter previously
/// lived in the reward/settlement layer, which meant a second distribution path (token purchase, grants,
/// anything future) could credit balances without the cap ever seeing it.
///
/// **Contract for anyone adding a new distribution mechanism:** call [`SupplyState::authorize_mint`] and
/// mint only what it grants. It is the single gate; a path that mints without it is not capped by
/// anything, and no test or invariant elsewhere will catch that.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct SupplyState {
    /// Total base units brought into existence so far, across ALL issuance paths.
    minted: u64,
    /// Hard ceiling on `minted`. Governed; 0 means nothing may ever be minted.
    cap: u64,
}

impl SupplyState {
    pub fn new(minted: u64, cap: u64) -> Self {
        Self { minted, cap }
    }

    /// Total supply in existence — the CTS-1 L0 `total_supply` any token must be able to report.
    pub fn total_supply(&self) -> u64 {
        self.minted
    }

    /// The ceiling currently in force.
    pub fn cap(&self) -> u64 {
        self.cap
    }

    /// Base units that may still be minted before the cap binds.
    pub fn headroom(&self) -> u64 {
        self.cap.saturating_sub(self.minted)
    }

    /// Set the GOVERNED cap. Monotonic in the SAFE direction only for lowering is NOT enforced here —
    /// governance may raise or lower it — but `minted` is never rewritten, so lowering the cap below what
    /// already exists simply means zero headroom, never a negative supply.
    pub fn set_cap(&mut self, cap: u64) {
        self.cap = cap;
    }

    /// Raise `minted` to at least `value` — used to restore the counter from durable history. Monotonic,
    /// so a stale or out-of-order read can only ever raise it, never hand back spent headroom.
    pub fn observe_minted(&mut self, value: u64) {
        self.minted = self.minted.max(value);
    }

    /// THE MINT GATE. Grants at most the remaining headroom and advances the counter by exactly what it
    /// granted. Returns the amount actually authorized, which may be less than requested and may be zero.
    ///
    /// Every path that creates tokens goes through here, which is what makes the cap structural rather
    /// than a convention each path is trusted to honour.
    pub fn authorize_mint(&mut self, want: u64) -> u64 {
        let granted = want.min(self.headroom());
        self.minted = self.minted.saturating_add(granted);
        granted
    }
}

/// The protocol's SHARED economic state — the one PDA-analog. Per `ECONOMIC_LAYER_DESIGN.md`, "the
/// subsidy pool, issuance counter, epoch clock" live on a governance-owned chain, touched at EPOCH
/// CADENCE rather than per transfer.
///
/// **The pool is LITERAL: it holds tokens.** It used to be a number derived by summing `Pay` writes,
/// which made the value movement fictional in both directions — `Pay` DESTROYED the payer's tokens (a
/// debit with no credit anywhere) and `RewardClaim` CREATED the provider's (a credit with no debit). The
/// two happened to net out in aggregate, so nothing caught it, but supply was untracked across the whole
/// cycle and the "pool" could never be over-drawn because it did not exist.
///
/// Now value moves for real, and supply changes at exactly ONE point:
///
/// | operation      | movement                | supply    |
/// |----------------|-------------------------|-----------|
/// | `Pay`          | payer  → pool           | conserved |
/// | seed           | **mint** → pool         | +granted  |
/// | `RewardClaim`  | pool   → provider       | conserved |
///
/// The invariant that proves it: `Σ account balances + pool == total_supply` ([`Self::conserves`]).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct ProtocolState {
    supply: SupplyState,
    /// Tokens the protocol HOLDS, awaiting distribution to providers.
    pool: u64,
}

impl ProtocolState {
    pub fn new(supply: SupplyState, pool: u64) -> Self {
        Self { supply, pool }
    }

    /// Tokens currently held by the pool.
    pub fn pool(&self) -> u64 {
        self.pool
    }

    /// Total supply in existence (CTS-1 L0 `total_supply`).
    pub fn total_supply(&self) -> u64 {
        self.supply.total_supply()
    }

    pub fn supply(&self) -> &SupplyState {
        &self.supply
    }

    pub fn supply_mut(&mut self) -> &mut SupplyState {
        &mut self.supply
    }

    /// Fold a payer's `Pay` into the pool — the CREDIT half of a transfer whose debit already happened on
    /// the payer's own chain. Supply is unchanged: this moves tokens, it does not create them.
    ///
    /// Deliberately at epoch cadence rather than per transfer: the pool is shared state on a single
    /// governance-owned chain, so crediting it synchronously with every payer's write would need a
    /// cross-account transaction the account-chain model does not have.
    pub fn fold_pay(&mut self, amount: u64) {
        self.pool = self.pool.saturating_add(amount);
    }

    /// MINT fresh tokens into the pool — the ONLY operation in the system that creates supply, and it is
    /// gated by the cap. Returns what was actually granted, which may be less than asked or zero.
    pub fn mint_into_pool(&mut self, want: u64) -> u64 {
        let granted = self.supply.authorize_mint(want);
        self.pool = self.pool.saturating_add(granted);
        granted
    }

    /// Debit the pool to fund a provider's claim — the DEBIT half of a transfer whose credit lands on the
    /// provider's chain. Refuses if the pool cannot cover it, so claims can never draw money that does not
    /// exist (which the old mint-on-claim path had no way to prevent).
    pub fn debit_for_claim(&mut self, amount: u64) -> bool {
        match self.pool.checked_sub(amount) {
            Some(rest) => {
                self.pool = rest;
                true
            }
            None => false,
        }
    }

    /// Adopt a pool figure derived from the CANONICAL record chain.
    ///
    /// The pool is not this node's to accumulate: under committee-gated settlement a node observes only
    /// a sample of epochs, so a locally-summed pool is simply wrong. The canonical record carries the
    /// pool recurrence, and every node adopts it — which is what makes two nodes agree.
    pub fn set_pool(&mut self, pool: u64) {
        self.pool = pool;
    }

    /// THE conservation invariant: every token that exists is either in someone's balance or in the pool.
    /// Callers supply the summed account balances (the token cannot see other chains itself).
    pub fn conserves(&self, sum_of_balances: u64) -> bool {
        sum_of_balances.saturating_add(self.pool) == self.supply.total_supply()
    }
}

/// Render base units as a decimal token amount (display only — never feed this back into arithmetic).
pub fn format_amount(base_units: u64) -> alloc::string::String {
    let whole = base_units / ONE_TOKEN;
    let frac = base_units % ONE_TOKEN;
    let mut s = alloc::format!("{whole}.{frac:0width$}", width = DECIMALS as usize);
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.push('0');
    }
    s
}

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

    // ── The pool is LITERAL: value moves, and supply changes at exactly one point ──────────────────

    /// THE invariant that distinguishes a real pool from a derived number: every token that exists is
    /// either in an account balance or in the pool. Walked across a full pay → seed → claim cycle.
    #[test]
    fn every_token_is_either_in_a_balance_or_in_the_pool() {
        // Genesis: nothing exists. A cap alone mints nothing.
        let mut p = ProtocolState::new(SupplyState::new(0, 1_000), 0);
        let mut balances = 0u64;
        assert!(p.conserves(balances));
        assert_eq!(p.total_supply(), 0);

        // SEED — the only operation that creates supply. It lands in the pool, not in a balance.
        let minted = p.mint_into_pool(100);
        assert_eq!(minted, 100);
        assert_eq!(p.pool(), 100);
        assert_eq!(
            p.total_supply(),
            100,
            "supply rose by exactly what was minted"
        );
        assert!(
            p.conserves(balances),
            "minted tokens are accounted for in the pool"
        );

        // CLAIM — pool → provider. Supply unchanged; the tokens moved.
        assert!(p.debit_for_claim(60));
        balances += 60; // credited on the provider's own chain
        assert_eq!(p.pool(), 40);
        assert_eq!(
            p.total_supply(),
            100,
            "a claim MOVES tokens, it does not create them"
        );
        assert!(p.conserves(balances));

        // PAY — payer → pool. Supply unchanged again.
        balances -= 25; // debited on the payer's own chain
        p.fold_pay(25);
        assert_eq!(p.pool(), 65);
        assert_eq!(
            p.total_supply(),
            100,
            "a payment MOVES tokens, it does not destroy them"
        );
        assert!(p.conserves(balances), "conserved across the whole cycle");
    }

    /// A claim can never draw money that does not exist. The old mint-on-claim path had no way to refuse
    /// this — it credited the provider from nothing, so an over-large share simply created supply.
    #[test]
    fn a_claim_cannot_overdraw_the_pool() {
        let mut p = ProtocolState::new(SupplyState::new(0, 1_000), 0);
        p.mint_into_pool(50);
        assert!(!p.debit_for_claim(51), "refused: the pool cannot cover it");
        assert_eq!(p.pool(), 50, "and nothing moved");
        assert!(p.debit_for_claim(50), "exactly the balance is fine");
        assert_eq!(p.pool(), 0);
        assert!(!p.debit_for_claim(1), "an empty pool funds nothing");
    }

    /// The cap binds the ONE mint path, and every other path is supply-neutral — so no amount of paying
    /// or claiming can inflate supply past it.
    #[test]
    fn the_cap_binds_minting_and_nothing_else_can_inflate_supply() {
        let mut p = ProtocolState::new(SupplyState::new(0, 100), 0);
        assert_eq!(p.mint_into_pool(70), 70);
        assert_eq!(
            p.mint_into_pool(70),
            30,
            "only the headroom left under the cap"
        );
        assert_eq!(
            p.mint_into_pool(70),
            0,
            "cap reached → nothing further, ever"
        );
        assert_eq!(p.total_supply(), 100);

        // Paying in and claiming out churn the pool without touching supply.
        p.fold_pay(500);
        assert!(p.debit_for_claim(400));
        assert_eq!(
            p.total_supply(),
            100,
            "supply is untouched by value movement"
        );
        assert_eq!(p.supply().headroom(), 0);
    }

    /// A claim with NO resolved share must waste nothing: it is refused BEFORE the epoch is marked
    /// claimed, so the provider can claim again once the epoch is finalised.
    ///
    /// This is what makes "resolve the share only from the canonical record" safe. Before finality the
    /// share resolves to 0 and the claim is simply refused; if the refusal instead burned the single-use
    /// marker, claiming early would forfeit the reward permanently.
    #[test]
    fn a_claim_before_finality_is_refused_without_burning_its_single_use() {
        let p = [1u8; 32];
        let st = TokenState::default();
        // No resolved share (epoch not finalised) → refused.
        let none = Resolved::default();
        assert!(
            apply_token(st.clone(), &LedgerOp::RewardClaim(7), &p, &none).is_none(),
            "refused while unfinalised"
        );
        // The epoch was NOT marked, so the same claim succeeds once the share resolves.
        let resolved = Resolved {
            reward_share: Some(40),
            ..Default::default()
        };
        let after = apply_token(st, &LedgerOp::RewardClaim(7), &p, &resolved)
            .expect("claims once the epoch is finalised");
        assert_eq!(after.balance, 40, "credited the canonical share");
        assert!(
            after.claimed_epochs.contains(&7),
            "and only now is it single-used"
        );
    }

    /// Restoring the counter from durable history is monotonic — a stale read can never hand back spent
    /// minting headroom.
    #[test]
    fn observing_minted_history_can_only_raise_the_counter() {
        let mut s = SupplyState::new(0, 1_000);
        s.authorize_mint(400);
        s.observe_minted(100);
        assert_eq!(
            s.total_supply(),
            400,
            "a lower observation does not lower supply"
        );
        s.observe_minted(900);
        assert_eq!(s.total_supply(), 900, "a higher one raises it");
        assert_eq!(s.headroom(), 100);
    }
}
