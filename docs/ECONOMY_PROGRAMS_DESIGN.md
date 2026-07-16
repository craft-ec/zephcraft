# Economy Programs — Token / economy-* split + Cross-Program Invocation

**Status: DESIGN (2026-07-16).** Refactor the monolithic token-ledger into a minimal **token** program
plus a family of **economy-\*** policy programs (first: **economy-egress**), and add **cross-program
invocation (CPI)** as a general composition primitive. Supersedes the "add `egress_bytes` to the `Pay`
op" approach (which would have mutated the canonical token program) — the whole subscription/pricing/
expiry model now lives in `economy-egress`, and `token` stays a stable money primitive.

## 1. Why

- **Separation of concerns (Solana model):** `token` = SPL-Token-like money primitive (transfer, balance);
  `economy-egress` = a rewards/subscription program built *on top of* tokens. Changing egress pricing,
  quotas, or expiry never touches the canonical token standard (CTS-1).
- **Extensible economy family:** `economy-egress`, `economy-storage`, `economy-…` — one policy program per
  paid resource, each owning its pool + settlement, all denominated in the one `token`.
- **Dissolves the "canonical program churn" problem:** the subscription op (locked `egress_bytes`, 30-day
  expiry, governed rate) is an `economy-egress` op, not a `token` op.

## 2. The seam (from the architecture map)

`LedgerBalanceState.balance` (`crates/ledger/src/lib.rs`) is written by all four ops in one `apply()`:
`Transfer`/`Claim` (token) and `Pay`/`RewardClaim` (economy) all do `balance.checked_add/sub`. The split:

| | **token** (`zeph_token`, slimmed `zeph_ledger`) | **economy-egress** (`zeph_economy_egress`, new) |
|---|---|---|
| State | `{ balance, processed_claims }` | `{ claimed_epochs }` + pool/records (node-side) |
| Ops | `Transfer`, `Claim` | `Pay`/`Subscribe`, `RewardClaim` |
| Knows | money only (CTS-1 reference impl) | egress quota, per-consumer FCFS, 30-day expiry, rate |
| Service | `TokenService` | `EconomyEgressService` (holds `Arc<TokenService>`) |

## 3. How value moves across the boundary

A `Pay`/subscribe = **token `Transfer`(consumer → pool account)** + an **economy-egress record** (credits the
locked egress quota). A reward payout = economy-egress computes the record → a **token credit** to the
provider. The token program never learns what a subscription is; economy-egress never touches raw balances
except *through* token.

**Two integration points, kept consistent (the map's key caveat):**
1. **Native fold (hot path):** the node folds balances in Rust today (`LedgerService::balance`). Post-split,
   `EconomyEgressService` calls `TokenService` **directly in Rust** to move value — trivial, no host fn.
2. **Wasm / verification path:** if a verifier re-runs the economy wasm and it must reflect a token move,
   it needs `invoke_program(token_cid, …)` under a **deterministic** callee grant, or its committed output
   won't reproduce.

## 4. CPI (`invoke_program`) — general primitive, but NOT on the verified settlement path

- **What:** a host fn `invoke_program(anchor_name|cid, func, input) -> output` + `Capability::InvokeProgram`,
  callee run under `CapabilityGrant::deterministic()` (no wall-clock/random/verify/attest/sequence) so the
  caller's own re-execution reproduces the whole call tree. Hooks into `bind_granted` (transition.rs) via a
  new `InvokeProgramBackend` injected into `TransitionCtx`, driven by `InvokeService`/`AnchorDispatcher`.
- **The hazard (why the tracker deferred it):** an in-wasm value-move CPI on a *verified* program means every
  re-run nests a callee execution — and the actual balance fold is **native**, so an in-wasm CPI wouldn't even
  be on the hot path. **Resolution:** the economy→token *value move* stays **node-orchestrated** (Pay = a real
  token `Transfer`; claim = node-authored token credit against the verified record). CPI is built as a
  **general composition primitive** for programs that want synchronous, deterministic-callee composition
  (incl. future economy-\* programs calling `token`), NOT wedged onto the verified settlement fold.
- **Determinism:** callee gets the deterministic capability subset only; `verify_mode` returns inert (no
  nested orchestration); fuel is shared/bounded by the caller's grant.

## 5. Naming

- Anchor names become shared `const`s (today they are re-typed string literals across genesis/control/main):
  `TOKEN_ANCHOR = "token"`, `ECONOMY_EGRESS_ANCHOR = "economy-egress"` (was `"reward"`). Future:
  `ECONOMY_STORAGE_ANCHOR = "economy-storage"`, etc.
- Crates: `zeph_ledger` → `zeph_token` (slimmed); new `zeph_economy_egress` (absorbs `Pay`/`RewardClaim` +
  `zeph_reward::compute`). Wasm artifacts renamed + rebuilt; genesis publishes + pins both anchors.

## 6. Phase plan (each phase: build + test + gate + commit; roll together where consensus-affecting)

1. **P1 — anchor-name constants + rename `reward` → `economy-egress`** (mechanical; re-pin anchor at genesis).
2. **P2 — CPI primitive:** `Capability::InvokeProgram` + `invoke_program` host fn + `InvokeProgramBackend`,
   deterministic-callee grant; tests (a program invokes another, re-execution reproduces). No behavior change
   to existing programs.
3. **P3 — split `zeph_ledger` → `zeph_token` (Transfer/Claim) + `zeph_economy_egress` (Pay/RewardClaim +
   reward compute).** Keep native fold; `EconomyEgressService` holds `Arc<TokenService>`.
4. **P4 — rehome settlement** (SettlementStore/Service, record_chain) under economy-egress; wire pool as a
   token pool account; `reward_claim` two-step (token credit → economy claim mark) across the new boundary.
5. **P5 — genesis + dashboard:** publish `token.wasm` + `economy-egress.wasm`, pin both anchors; dashboard
   anchor list + labels.
6. **P6 — subscriptions in economy-egress:** governed `bytes_per_token`, subscription = locked `egress_bytes`,
   30-day windowed per-consumer quota (build on the per-consumer FCFS already shipped). Use-it-or-lose-it.
7. **P7 — deploy** (wire+consensus → simultaneous fleet roll).

## 7. Open decisions

- **CPI scope (§4):** general composition primitive with the *value move node-orchestrated* (recommended),
  vs. attempting in-wasm CPI value-moves (fights verification, off the native hot path — not recommended).
- Whether `SETTLE_PROGRAM_TAG` / `RECORDS_PROGRAM_TAG` chains re-anchor under `economy-egress` or stay
  independent synthetic programs (lean: stay independent; they're plumbing, not user-facing anchors).
