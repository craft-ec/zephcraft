# Token-Ledger Protocol Program — Build Blueprint (Economic Layer §11 Step 4)

Status: BLUEPRINT (2026-07-16), derived from a code-architect pass over the live tree. The **why** lives in
`ECONOMIC_LAYER_DESIGN.md` (source of truth); this is the **how** — concrete files, seams, and phase sequence.
The token ledger is the first genuinely-governed-WASM protocol program and the first use of K1's anchor-dispatcher.

## 0. Grounding — the machinery is already built, mostly unwired

This is overwhelmingly a **wiring + one-new-crate** task, not a new-primitive task:

- `crates/com/src/transition.rs` (`bind_granted`) already binds `sequence`/`attest`/`verify`/`pre_grant` host fns;
  `SequenceBackend`/`AttestBackend`/`VerifyBackend` traits exist. **Zero new host-fn ABI needed.**
- `crates/com/src/sequencer.rs` + `crates/noded/src/sequence.rs` (`SequenceStore`) give per-account,
  quorum-ordered, owner-authenticated nonce logs, durably published/pulled cross-node.
- `crates/com/src/attestation.rs` + `crates/noded/src/attest.rs` give a program a declared quorum with an
  owner-signed genesis trust root.
- `crates/noded/src/governance.rs` (`GovernanceChainStore`) already folds the governance chain into
  `ProgramRegistryState` (name→cid+version, via `SetProgram`) and `ConfigRegistryState` (key→i64, via `SetConfig`).
  **This *is* K1's plural anchor table** — built, tested, but never wired to an invoke path (`resolve()` is
  `#[allow(dead_code)]`).
- `crates/noded/src/headreg.rs` already has the BLAKE3-rendezvous-rank + converged-census pattern needed for
  §10.5's epoch committee (`replicas`/`effective_epoch`/`eligible`).
- `crates/cheque/src/lib.rs` (`ServingCheque`/`ChequeBook`/`allocate_quota`) + `crates/noded/src/cheque.rs`
  (`ChequeService`, `total_earned`) give the measurement substrate.
- `crates/obj/src/lib.rs` has the exact `OnceLock<Arc<dyn Fn...>>` hook pattern (`shed_gate`/`grant_gate`/
  `byte_meter`) to model the new admission/pin gates on.
- `apps/registry-wasm/src/lib.rs` (no_std + dlmalloc + host imports, state-in/request-in/state-out) is the
  transition-style program template; `crates/noded/src/account.rs` (`ProgramAccountStore`, `pda()`) is the PDA
  substrate for the one shared piece (subsidy pool / issuance counter / epoch anchor).

The only genuinely new mechanism is the §10.5 **rotating epoch committee** (a new quorum *source*, not new plumbing).

## 1. File / crate manifest

**New crates**
- `crates/ledger` (`zeph-ledger`) — shared `#![no_std]` + `alloc` crate: Layer-0/Layer-1 postcard message schemas +
  deterministic fold functions. Consumed by *both* the wasm program and native noded/CLI/tests.
- `apps/ledger-wasm` (`zeph-ledger-wasm`) — the protocol program compiled to `wasm32-unknown-unknown`; thin `run()`
  over the `craftcom` host ABI (mirrors `apps/registry-wasm`), calling into `zeph-ledger`. Own `[workspace]`,
  `dlmalloc`; add to root `Cargo.toml` `exclude`.

**New noded modules**
- `crates/noded/src/anchor.rs` — K1 anchor dispatcher: `resolve(name)→{cid,interface_version}`, `anchor_owner(name)`
  (deterministic sentinel), `invoke_anchor(...)` via the existing `InvokeService`.
- `crates/noded/src/epoch_committee.rs` — §4e: `EpochCommitteeSource` computes a `Quorum` deterministically from
  `Membership::census()` eligible + HLC epoch (BLAKE3 rendezvous); publishes per-epoch snapshots.
- `crates/noded/src/quorum_source.rs` — `QuorumSource` trait (`current_quorum(owner, program_cid)→Option<Quorum>`),
  implemented by the existing `AttestStore` (unchanged) and the new `EpochCommitteeSource`; routed by an
  `AnchorAwareQuorumSource`.
- `crates/noded/src/ledger.rs` — `LedgerService`: balance-fold cache, transfer/claim/mint/settle orchestration, the
  admission-gate + pin-gate closures, the settlement loop.

**Modified**
- root `Cargo.toml` — add `zeph-ledger` to workspace.deps; add `apps/ledger-wasm` to `exclude`.
- `crates/obj/src/lib.rs` — add `admission_gate` + `pin_gate` OnceLock fields + setters (mirror `set_shed_gate`),
  call sites at top of `get()` (~1017) and in `publish_impl()` before the pin branch (~720).
- `crates/noded/src/sequence.rs` — generalize `quorums: Arc<AttestStore>` → `Arc<dyn QuorumSource>` (existing call
  sites pass `attest_store.clone()` unchanged — `AttestStore` implements the trait).
- `crates/noded/src/governance.rs` — un-dead-code `resolve()`; add `resolve_interface_version(name)` reading
  `ConfigRegistryState` key `anchor:<name>:iface` (no wire change — reuses the generic config schema).
- `crates/noded/src/main.rs` — wire dispatcher/committee/quorum-source/ledger; construct `SequenceStore` with the
  composite quorum source; `engine.set_admission_gate`/`set_pin_gate`; CLI `Ledger*`/`AnchorResolve` commands
  (mirror `GovPropose`/`SequenceLog`).
- `crates/noded/src/control.rs` — `rpc_ledger_*`/`rpc_anchor_resolve` handlers (mirror `rpc_invoke`/`rpc_gov_propose`).

**Not changed:** `crates/com/src/{lib,transition,capability,gov,attestation,registry,verification,invoke}.rs` — host
ABI, capabilities, and governance/attestation wire types are complete for this build.

## 2. Decision — shared crate, not a hand-mirrored twin

`apps/registry-wasm` hand-mirrors `crates/com/src/registry.rs` (kept in sync manually, cross-checked by a test) —
tolerable there for historical reasons. The ledger has no native-twin requirement (§5: the token logic *is* a
governed-WASM program) and a materially larger ABI consumed by native code too. So factor schemas + pure fold logic
into `crates/ledger` (compiles for wasm + host) and make `apps/ledger-wasm` a thin shim. This is the one deliberate
departure from the app-crate pattern; the registry precedent was an accident, not a rule.

## 3. Phase 4a — K1 anchor-dispatcher

`GovAction::SetProgram{name,cid}` + `SetConfig{key,value}` (folded into `ProgramRegistryState`/`ConfigRegistryState`)
*are* the anchor table; the write path is unchanged. Build the **read/dispatch** path (`anchor.rs`):
`resolve(name)` = `governance.resolve(name)` (un-dead-coded) + `resolve_interface_version(name)`. Interface version
lives in the existing config registry under `anchor:<name>:iface` — **zero wire change** (rejecting a widened
`ProgramRegistryState` tuple, which would force a simultaneous fleet roll). Dispatch reuses `InvokeService::invoke`
with `InvokeRequest{app_ns:name, wasm_cid:cid, func, input}`.

**The genuine gap (resolved decisively):** a K1-anchored, *network-owned* program has no owner keypair, so nobody can
produce `AttestedChain::new(&owner_identity,...)` for it. Do **not** extend `AttestedChain`'s owner-signature trust
root. Instead the anchor's `program_owner` resolves to a deterministic **sentinel**
`pda(b"craftec/anchor-owner/1", name)`, and the quorum for that `(sentinel, program_cid)` is answered by
`EpochCommitteeSource`/governance, **not** `AttestStore`. This keeps the attestation trust model (owner-signed
genesis) untouched for *user* programs while anchored protocol programs get authority from committee computation —
matching §5's "sequencer quorum membership = binary/rotating epoch committee, agreement machinery, not a program knob."
**Flag for a design-check before shipping 4a** — first time `program_owner` isn't a real registry-resolved keyholder.

## 4. Phase 4b — ledger core (Layer-0 ABI + account-chain model)

Schemas (`crates/ledger`): `TransferOp{to,amount,memo}`, `ClaimOp{debit:SequencedCommitRef,amount}`,
`LedgerBalanceState{balance, processed_claims:BTreeSet<(sender,nonce)>, checkpoint_root}`. `SequencedCommitRef`
carries the full `SequencedCommit` (write + k-of-n sigs) inline → a claim re-runs without a live network round-trip.

- **Balance = fold of the owner's own `AccountSequence`.** A transfer is `SequencedWrite::author(sender, next_nonce,
  postcard(TransferOp))` → the `sequence` host fn → `SequenceStore::sequence` (unchanged), ordered under the sender's
  epoch committee. `fold_account` in `zeph-ledger` replays `payload_at(0..n)` deterministically — every node computes
  the identical balance, no gossip, no committee for the fold itself (like governance/attestation folding). Durability
  rides `SequenceStore`'s existing publish/pull.
- **Recipient credit = CLAIM (not fold) — decisive.** Fold would need a global "who-owes-me" index (violates §6's
  O(1)/account, no-global-scan design). Claim keeps every account a pure fold of *only its own chain*: (1) sender's
  `TransferOp` is a `Debit` at nonce N on the sender's chain; (2) recipient authors `ClaimOp{debit=commit(sender,N)}`
  on **its own** account (`owner_authentic` structurally blocks anyone else); (3) the transition validates
  self-contained — `debit.authorizes(quorum_at_that_epoch)`, `TransferOp.to == me`, `(sender,N) ∉ processed_claims`.
  "No double-credit" becomes an ordinary same-chain duplicate check; zero new storage.
- **Verification = periodic CHECKPOINTING, not per-transfer.** Determinism gives per-transfer validity for free (any
  node re-folds the public quorum-ordered sequence). K6/Board's role: once per epoch (lazily — gap #4), a `verify()`
  asks k nodes to re-fold from the last checkpoint to head + sign off; the verdict becomes the new
  `checkpoint_root`, bounding re-execution cost and letting a wallet/claim trust "balance ≥ X as of checkpoint C"
  without replaying from genesis. Straight reuse of `Board`/`Verifier`, unchanged.
- **Reserved-namespace enforcement is structural:** `owner_authentic` stops anyone ordering a write into an account
  they don't hold; the deterministic transition (re-run by verifiers + any reader) rejects any fold that isn't
  `fold_account(canonical_ledger_cid, sequence)`. Same "verification re-runs the canonical cid" property K6 provides.

## 5. Phase 4c — mint-from-receipts

`ChequeService` (unchanged) accumulates `ServingCheque`s; at epoch close the provider authors `MintOp{epoch, cheques,
claimed_reward}` on its own account. **Reward valuation is INLINED in the ledger program for v1** (not a separate
anchor — there is no program-to-program invoke host fn yet; decomposing is a deferrable pure refactor, still fully
governed via the same `SetProgram` swap). The mint amount = `reward_from_cheques(cheques)` (§10 #1: metered, capped at
paid usage, linear) computed inside the deterministic fold → self-verifying; K6 checkpointing as in 4b.
**Single-use** = a monotonic "already-rewarded-up-to" watermark per consumer in `LedgerBalanceState` (reuses
`ServingCheque`'s monotonic-cumulative invariant), so replayed cheques mint zero.

## 6. Phase 4d — settlement + tiers

**Reciprocity-offset before `allocate_quota`** — compose the unchanged pure fn **twice**:
1. `reciprocity_credit = min(total_earned, gross_consumed_this_epoch)` (from `ChequeService::total_earned`, drop its
   `#[allow(dead_code)]`).
2. Pass 1: `allocate_quota(cheques, reciprocity_credit)` → the reciprocal band (nets to zero, no tokens move).
3. Pass 2: `allocate_quota(remainder, token_quota)` → `(token_paid, subsidy)`.

`token_paid` → a real movement: an `EscrowOp` (consumer pre-reserves tokens; a `SequencedWrite` on its own account)
redeemed by providers via a `SettleClaim` (same shape as `ClaimOp`). `subsidy` → a mint from the governance-owned
subsidy-pool **PDA** (`ProgramAccountStore`/`pda()` — the one PDA-analog §3 calls out), capped by pool health.

**Admission gate** (new obj OnceLock hook, mirrors `shed_gate`): checked at the top of `get()` (~1017) + range
variants; unwired default = permissive. Wired by `LedgerService` to a sync closure reading the in-memory reciprocity
position + cold-start/escrow state — a **paid** user (escrow/balance) always passes (never gated); a **free** user
passes only within reciprocity headroom + cold-start grant.

**Pin/publish gate** (second OnceLock hook): checked in `publish_impl` before `if pin {...}` — on rejection,
**downgrade to a non-pinned consume-only publish** rather than failing the call (§8 free = consume-only,
owner-pays-pin = paid). Distinct code path from the admission gate (store vs. serve).

## 7. Phase 4e — rotating epoch committee (§10.5)

Reuse `headreg.rs`'s `effective_epoch()` (HLC + boundary-race guard), `eligible()` (converged census), and BLAKE3
rendezvous. New `EpochCommitteeSource` computes a `Quorum` directly over `(program_cid, epoch)` — the *full membership*
shifts each epoch (unlike headreg's single-writer-within-a-fixed-set):

```
committee_for(program_cid, epoch, eligible, n, k):
  sort eligible by Cid::of([program_cid, epoch_le, id])
  truncate to n
  Quorum::genesis(ids, k)     # n,k governed (§10 #4, default 4/3)
```

Every node computes the identical committee with no election messages. **Cross-epoch hand-off:** `SequenceStore`
(now `Arc<dyn QuorumSource>`) re-derives the quorum per-write at the active epoch, so an in-flight sequence continues
under the new committee once it rotates; `EpochCommitteeSource` also **publishes a durable per-epoch snapshot** so
checkpoint re-verification can look up "who was the committee at epoch E." **Genesis** = the degenerate epoch-0 case,
computed (not declared) the moment `resolve("token-ledger")` first succeeds — the key simplification vs. AttestStore's
owner-signed genesis. Heaviest sub-phase; its own commit + gate.

## 8. Build sequence (strict sequential; one phase resolved + committed before the next)

- [ ] **4a — K1 anchor dispatcher.** `anchor.rs`; un-dead-code `resolve` + `resolve_interface_version`; `--anchor` on
  `Invoke` + `AnchorResolve`; sentinel anchor-owner in `rpc_invoke`. **Exit:** build/test green; `gov-propose
  --set-program` + `--set-config anchor:foo:iface=1` + `invoke --anchor foo` round-trips on one node; no `--name`/
  `--wasm` regression. *(Design-check the anchor-authority routing first — gap #1.)*
- [ ] **4e — rotating epoch committee.** `quorum_source.rs` (trait + AttestStore impl) + `epoch_committee.rs`;
  generalize `SequenceStore`. **Exit:** identical-committee-across-nodes test; hand-off property test (write started
  under E, gathered after rotation to E+1, still commits); existing Sequence/Attest tests green. *Built before
  4b/4c/4d because they depend on it for real ordering.*
- [ ] **4b — ledger core.** `crates/ledger` + `apps/ledger-wasm`; `TransferOp`/`ClaimOp`/`fold_account`; checkpoint
  `verify()`. **Exit:** wasm-vs-native fold-equivalence test; transfer→claim→balance integration; duplicate-claim
  rejection.
- [ ] **4c — mint-from-receipts.** `MintOp` + `reward_from_cheques`; wire `total_earned`. **Exit:** mint increases
  balance by the deterministic formula; replayed cheques mint zero (monotonic dedup).
- [ ] **4d — settlement + tiers.** `EscrowOp`/`SettleClaim`; admission + pin gates. **Exit:** two-pass
  `allocate_quota` unit-tested (extend `quota_allocation_caps_paid_at_quota_by_timestamp` with a reciprocity case);
  free-tier deficit trips the admission gate; pin-without-balance downgrades.

Each phase runs the full cycle (context-load → design-check → implement → design-check → code-review →
integration-check → commit) and updates `.claude/feature-progress.md`.

## 9. Remaining design gaps (need a call before/at their phase)

1. **Anchor authority routing** — sentinel owner + `EpochCommitteeSource` instead of `AttestedChain`'s owner-sig root.
   First time `program_owner` ≠ a real keyholder → **design-check before 4a**.
2. **Escrow lifecycle** (top-up/close/reclaim/disputes) — deferred in §7/§10.9, but 4d needs *a* minimal answer.
   Recommend: reclaim after N epochs of no matching `SettleClaim` (governed N).
3. **Cold-start grant + identity gate** (§8/§10 #6/#7) — the admission gate needs a Sybil bound. Recommend v1: gate
   the initial grant on registry-registered account age (first `HeadEntry` timestamp); defer stake/invite/PoP.
4. **Checkpoint cadence** — every-account-every-epoch is O(accounts). Recommend lazy: trigger on first claim/read
   referencing an account older than N nonces since its last checkpoint.
5. **Reward-valuation decomposition** — scoped out (inlined v1); revisit if/when a program-to-program invoke host fn
   exists.
