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

## 3. How value moves across the boundary — atomicity WITHOUT cross-program transactions

**A value move is never a two-op cross-account transaction** (that could half-commit: pay-no-quota = lost
funds, quota-no-pay = a farm). Instead, atomicity is inherent to "one quorum-ordered write, one account
chain, folded by both programs" — the same model the current `Pay`/`RewardClaim` self-ops already use:

1. **A cross-program op is ONE self-authored write on ONE account chain, co-folded.** A subscribe is a single
   write on the *consumer's* chain; the **token** fold debits `balance`, the **economy-egress** fold records
   the locked egress quota. One committed write → both effects or neither. The two programs own disjoint
   *state slices* of the account (token: `balance`, `processed_claims`; economy: `quota`, `claimed_epochs`),
   routed by op type — but the entry is atomic because it's one chain write.
2. **Cross-account value flow is self-authored + record-mediated, never a transfer.** The pool is a *derived*
   aggregate (Σ pay-writes), not an account funds move into. A provider **self-claims** — one write on *its*
   chain: token fold credits `balance` by the record share, economy fold marks the epoch claimed.
   Conservation is the committee-attested **record** (`Σ claims ≤ pool`), not a drained pool account. No
   two-account atomic transfer ever occurs.

So there is **no atomic multi-program transaction primitive to build** — the account-chain model provides
atomicity per write, and value flows across accounts are self-authored against a shared record. (A true
Solana-style atomic multi-account transaction would only be needed for a value move spanning two
uncontrolled accounts in one instruction — which this design deliberately avoids.)

## 4. CPI (`invoke_program`) — a CALCULATION primitive, not a transaction primitive

**Why CPI here is read-only (the load-bearing rationale):** ZephCraft is **single-writer-per-identity** — every
write is self-authored on the writer's own account chain. A cross-program *state change* therefore never means
"program A writes program B's state"; it means **one self-authored write co-folded by both programs** (§3). So
there is no cross-program *write* to atomize, and CPI is needed only for **calculation** — a deterministic read
of another program's committed state/logic (token's claim-fold asking economy for `share_of(epoch)`). This is
the opposite of Solana, whose *multi-writer* model forces CPI to atomically mutate program-owned accounts inside
a global transaction (privilege delegation, reentrancy, all-or-nothing rollback). ZephCraft CPI returns a value,
never mutates a callee, runs the callee under the deterministic capability subset — so it can't reenter or
escalate, and verifier re-execution reproduces it trivially.


- **What:** a host fn `invoke_program(anchor_name|cid, func, input) -> output` + `Capability::InvokeProgram`,
  callee run under `CapabilityGrant::deterministic()` (no wall-clock/random/verify/attest/sequence) so the
  caller's own re-execution reproduces the whole call tree. Hooks into `bind_granted` (transition.rs) via a
  new `InvokeProgramBackend` injected into `TransitionCtx`, driven by `InvokeService`/`AnchorDispatcher`.
- **CPI's actual role here is a deterministic READ, not a value move (§3).** The token program's claim-fold
  needs the reward *share* from the economy record: `invoke_program(economy-egress, "share_of", epoch)` —
  a pure, reproducible cross-program read that replaces today's node-resolved `Resolved.reward_share`. Value
  moves stay single-write self-ops, so CPI carries **no atomicity requirement** and does not nest a value
  mutation.
- **The hazard it avoids:** an in-wasm CPI that *mutated* a callee's state on a verified program would make
  every re-run nest a state-changing callee execution (and the balance fold is native anyway). By keeping CPI
  to deterministic reads/pure-compute, re-execution reproduces trivially. General composition primitive for
  any program (incl. future economy-\* programs reading `token`), never a value-move channel.
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
