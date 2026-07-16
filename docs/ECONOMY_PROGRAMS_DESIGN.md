# Economy Programs — Token / economy-* split + Cross-Program Invocation

**Status: DESIGN (2026-07-16).** Refactor the monolithic token-ledger into a minimal **token** program
plus a family of **economy-\*** policy programs (first: **economy-egress**), and add **cross-program
invocation (CPI)** as a general composition primitive. Supersedes the "add `egress_bytes` to the `Pay`
op" approach (which would have mutated the canonical token program) — the whole subscription/pricing/
expiry model now lives in `economy-egress`, and `token` stays a stable money primitive.

## 0. Root rationale — individual, not global, coordination

Everything below (single-writer, transparent reads, co-fold instead of atomic transactions, CPI as a mere
read) falls out of ONE choice: ZephCraft coordinates trust **individually**, not globally. Global-state
chains (Ethereum, Solana, …) require every node to agree on **one global state under one global order** — that
is what forces atomic global transactions, a consensus/ordering bottleneck, and mediated cross-program access
so all nodes re-derive the same global sequence. (Native code — precompiles, native programs — does not escape
this; it still operates on the globally-agreed state.) ZephCraft instead makes each identity **its own trust
domain**: a writer authors + orders only its own chain (single-writer; the quorum orders that account's writes
for uniqueness, not a global order), and correctness is established **per-writer by re-execution**. There is no
global state to atomically mutate → no global atomic transaction to need, no global order to bottleneck on;
cross-identity effects are self-authored + reconciled. This is the north-star bet — **correctness by
verification, not global lockstep** — and the economy split is a direct consequence of it. (Prior art for the
lineage: Holochain's agent source chains + validation-by-re-execution; Nano's per-account block-lattice.)

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

**Two mechanics the DB model forces (2026-07-16):**
- **CPI is shaped to the token's standard interface, not an opaque invoke.** A caller invokes the **CTS-1 token
  shape** — a defined read surface (`balance_of(account)`, `share_of(epoch, provider)`, …) — so the economy-\*
  family all compose against the same interface. `invoke_program(anchor|cid, func, input)` where `func` is a
  named interface method.
- **The callee runs in ITS OWN reserved DB namespace.** `sql_query`/`sql_execute` are gated to `ctx.app_ns`, so
  `invoke_program` switches `ctx.app_ns` to the **callee's reserved namespace** and grants **`Sql` read only**
  (plus the deterministic subset), never write. The callee queries its own state and `commit`s a value; the
  caller sees only the returned value, never the callee's raw namespace. Value moves remain the self-authored
  co-folded write (§3) — CPI can *compute over* token state but never *mutate* it.

**Reserved namespaces.** Canonical programs (`token`, `economy-egress`, `economy-storage`, …) get
**protocol-reserved** DB namespaces a user app cannot claim. Their state lives there; a co-folded write
materializes into each program's namespace (a subscribe updates token's `balance` row AND economy's `quota`
row, each in its own reserved namespace, single-writer per account); CPI reads them read-only.

**SECURITY INVARIANT — reserved namespaces are RE-EXECUTION-authoritative, NOT owner-signature-authoritative
(2026-07-16).** This is the opposite trust model from a user-app namespace and it MUST NOT be conflated:
- **User-app namespace:** the owner *is* the authority — single-writer, owner-signed head, no re-execution.
  Forging your own app data only fools yourself; nobody else depends on it.
- **Reserved/protocol namespace:** the owner is *NOT* the authority. State is valid ONLY as the canonical
  program's **re-execution over the owner's quorum-ordered op-log**. A malicious node running a custom binary
  can write any forged page (`balance = 1e9`, `quota = ∞`) with the correct shape into its own reserved
  namespace — its own node believes it — but every honest node **re-derives** the state by re-running
  `token::apply`/`economy::apply` over the ordered ops and DISCARDS any claimed page that isn't the fold
  result. Same shape, wrong value → rejected. Two walls: ops are quorum-ordered + `apply`-validated (can't
  author free money); materialized state is a cache, never the authority.
- **The rule P2–P4 must honor:** a CPI read (`balance_of`, `share_of`) and the token/economy folds ALWAYS
  re-derive reserved-namespace state from the ordered op-log — they must NEVER trust a target's self-published
  materialized page. The check fires at every cross-node dependency (Claim, settlement, CPI read, the verify
  loop); forge-and-never-use is harmless, forge-and-use is caught on first re-execution. No new trust
  assumption vs. the existing "correctness by re-execution" model.


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

## 5b. The split seam — token folds the chain, economy values the bytes (settled 2026-07-17, P5)

**Superseded design note.** P3 shipped an interim *co-fold*: `claimed_epochs` sat in a `zeph_economy_egress`
slice and a `zeph-ledger` COMBINER folded both slices into one flat state. P5 removed that combiner. The
co-fold only existed because the reward dedup had been assigned to economy; putting it where it belongs
dissolved the problem. What follows is the built design.

**The cut: value vs. valuation.**
- **`zeph_token` — the account chain's program (the value authority).** `TokenState { balance,
  processed_claims, claimed_epochs }`, advanced by `apply_token` for ALL four ops. Its cid IS the chain's
  identity (`program_cid`), so a verifier re-running `token.wasm` over a chain reproduces the balance.
- **`zeph_economy_egress` — the policy/record program (the valuation authority).** STATELESS: a pure
  function of a node-built `RewardInput` → `RewardRecord` (it is the former `reward` program's bytes,
  absorbed per §5). It never folds a balance and holds no account state. P6 adds subscriptions here.

**Why both dedup sets are token's.** Deduping a credit is *value safety* — it protects the balance, exactly
what `processed_claims` already does for `Claim`. So `claimed_epochs` lives in `TokenState`, folded WITH the
credit in one write on the provider's own single-writer chain ⇒ the dedup and the credit are atomic by
construction (both or neither), with no cross-program transaction and no combiner. Economy says what is
*owed*; token says what is *paid*, exactly once. This is §3's atomicity claim, discharged.

**The share crosses as data, not a call.** A `RewardClaim`'s share is resolved by the node from the
committee-attested record (economy's authority) and handed to token as `Resolved.reward_share` — the same
node-resolved pattern `Claim` already uses for its debit, and re-checked by re-execution (a verifier
re-derives the record from committed inputs). No CPI on this path.

**Why native protocol programs never need CPI internally (2026-07-16).** In this substrate **reads are
transparent** — all committed data is public and re-derivable if you know the DB shape; the `app_ns` gate is
single-writer *write* confinement + a *sandbox* scoping for untrusted WASM, NOT data secrecy. So there are two
read paths: **native protocol programs** (`token`, `economy-egress` — part of the node) **direct-read** any
namespace in Rust (no host-fn, no gate — "we are the protocol"), while **sandboxed WASM** (user apps, any WASM
economy-\* program) must cross the host-fn boundary via **CPI**. Both re-derive from the op-log (re-execution-
authoritative), so no trust difference — just native-direct vs. host-fn-mediated. The token↔economy internal
flow is native, so it direct-reads + passes `share` as resolved data; **CPI's real audience is the WASM
sandbox.** Do not conflate the two in later phases.

## 6. Phase plan (each phase: build + test + gate + commit; roll together where consensus-affecting)

1. **P1 — anchor-name constants + rename `reward` → `economy-egress`** (mechanical; re-pin anchor at genesis).
2. **P2 — CPI primitive:** `Capability::InvokeProgram` + `invoke_program` host fn + `InvokeProgramBackend`,
   deterministic-callee grant; tests (a program invokes another, re-execution reproduces). No behavior change
   to existing programs.
3. **P3 — DONE 2026-07-17.** Split the crates (`zeph-token` + `zeph-economy-egress`), interim combiner —
   superseded by P5 (see §5b).
4. **P4 — DONE 2026-07-17.** Rehomed settlement (SettlementStore + record_chain) under
   `EconomyEgressService`; `LedgerService` holds `Arc<EconomyEgressService>` one-directionally (token →
   economy) for `reward_share`. NOTE: inverted vs. the original sketch — economy is self-contained policy
   with no account-chain access, which is what avoids a service-level cycle.
5. **P5 — DONE 2026-07-17.** Combiner removed; `token.wasm` (from `apps/token-wasm`) is the account chain's
   program and `economy-egress.wasm` (from `apps/economy-egress-wasm`, byte-identical to the retired
   `reward.wasm`) is the valuation program. Anchors re-pinned `token` / `economy-egress`; `zeph-ledger`,
   `apps/ledger-wasm`, `apps/reward-wasm` deleted. **Consensus:** token's cid changed ⇒ account chains
   restart from empty (accepted: dev-testnet balances, no migration written).
6. **P6 — DONE 2026-07-17.** Subscriptions in `zeph_economy_egress::subscription`: a paid delta buys
   `delta × bytes_per_token` egress bytes expiring after a governed window (`economy:bytes_per_token`
   default 1 MiB/token, `economy:subscription_window_epochs` default 86 400 ≈ 30 days at a 30s epoch).
   Serving is drawn FCFS from a consumer's unexpired grants (oldest first); past it = unrewarded subsidy;
   unspent bytes expire unrefunded. **This fixed a real unit bug:** the pre-P6 cap compared served BYTES
   against paid TOKENS directly (an implicit 1 token = 1 byte). **The price is distribution-neutral** —
   in pool-average `pool × (t·p)/Σ(tᵢ·p)` it cancels — so it sets the byte budget, not who earns what;
   that is why self-dealing stays bounded exactly as before. Dashboard: `subscription_bytes`.
7. **P7 — deploy** (wire+consensus → simultaneous fleet roll).

## 7. Open decisions

- **CPI scope (§4):** general composition primitive with the *value move node-orchestrated* (recommended),
  vs. attempting in-wasm CPI value-moves (fights verification, off the native hot path — not recommended).
- Whether `SETTLE_PROGRAM_TAG` / `RECORDS_PROGRAM_TAG` chains re-anchor under `economy-egress` or stay
  independent synthetic programs (lean: stay independent; they're plumbing, not user-facing anchors).
