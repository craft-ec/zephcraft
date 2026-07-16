# ECONOMIC LAYER — TOKEN-LEDGER PROTOCOL PROGRAM (§11 step 4) — BLUEPRINT DONE, BUILD NEXT (2026-07-16)

Blueprint: **docs/TOKEN_LEDGER_BUILD.md** (code-architect pass over the live tree). Design/why: docs/ECONOMIC_LAYER_DESIGN.md.
Headline: mostly **wiring + one new crate** (`crates/ledger` + `apps/ledger-wasm`), NOT new primitives — the K1 anchor
table (governance ProgramRegistry/ConfigRegistry), sequencer, cheque substrate, obj gate pattern, and headreg
rendezvous are all already built (much of it unwired). Only genuinely-new mechanism = the §10.5 rotating epoch committee.

Load-bearing decisions (blueprint + user directives 2026-07-16): recipient credit = **CLAIM** (not fold — keeps each
account a pure fold of its own chain); anchored programs use a **sentinel owner + epoch-committee quorum** (= the #5
rotating committee, NOT AttestedChain's owner-sig root — SETTLED, not an open design-check); verification = **always-on
defense-in-depth re-execution** (NOT periodic checkpointing — checkpointing dropped from core); reward = **BOUNDED
POOL-AVERAGE** (pool ÷ Σ min(used,paid-quota), uniform per-byte rate → providers earn the average regardless of which
consumer served; mostly redistribution, bootstrap-issuance top-up; uniform-pricing guardrail) computed by a **SEPARATE
reward-valuation program** via a new **`invoke_program`** host fn (deterministic-callee only); **shared `crates/ledger`**
crate (not a hand-mirrored wasm twin).

Build order (resequenced — 4e before the ledger; invoke_program before 4c):
- [x] **4a — K1 anchor-dispatcher DONE 2026-07-16.** `crates/noded/src/anchor.rs` (`AnchorDispatcher`:
      `resolve(name)→{cid,interface_version}` via governance `resolve` + config key `anchor:<name>:iface`;
      `anchor_owner(cid)` = deterministic `pda(cid,"craftec/anchor-owner/1")` sentinel; `invoke_anchor` →
      `InvokeService::invoke` with the sentinel owner). Un-dead-coded `GovernanceChainStore::resolve`. Wired into
      `control::State`; RPC `anchor_resolve` + `--anchor` branch in `rpc_invoke`; CLI `AnchorResolve` + `invoke
      --anchor`. 1 unit test (sentinel deterministic + cid-bound). Gates: build/test/fmt/clippy green. (Sentinel→
      committee quorum routing is 4e; for now a stateless anchor invoke works, stateful awaits the committee.)
- [ ] **4e — rotating epoch committee** (`quorum_source.rs` trait + `epoch_committee.rs`; generalize SequenceStore to
      `Arc<dyn QuorumSource>`; per-epoch snapshots for historical re-verify).
- [ ] **4a-bis — `invoke_program` primitive** (com host fn + `Capability::InvokeProgram`; deterministic-callee only).
- [ ] **4b — ledger core** (`crates/ledger` + `apps/ledger-wasm`; TransferOp/ClaimOp/fold_account; always-on re-fold
      validity, no checkpoint).
- [ ] **4c — reward = pool-average (separate program)** (reward-valuation program via invoke_program; two-pass
      allocate_quota identifies rewardable bytes; uniform-rate distribution; monotonic `minted_watermark` single-use).
- [ ] **4d — settlement + tiers** (two-pass allocate_quota reciprocity-offset; EscrowOp/SettleClaim; admission + pin
      gates in obj mirroring shed_gate).
Open gaps needing a call at their phase: (1) anchor-authority routing RESOLVED (= committee), (2) escrow reclaim lifecycle [4d],
(3) cold-start grant + identity gate [4d], (4) uniform-pricing floor for the pool-average reward [4c]. (Checkpoint
acceleration + reward-valuation decomposition RESOLVED; see TOKEN_LEDGER_BUILD.md §9.)

---

# ECONOMIC LAYER — SERVING-CHEQUE + MEASUREMENT SUBSTRATE (§11 step 2) — DONE (2026-07-15)

SWAP-style egress cheques (docs/ECONOMIC_LAYER_DESIGN.md §7): a CONSUMER signs a running cumulative of bytes
received from a PROVIDER; the provider accumulates (one per consumer, monotonic); the sum = payment basis +
serving MEASUREMENT (the cheque is payment instrument + fair-exchange proof + measurement evidence — one
artifact, three roles). No dependency on the §10 policy decisions — the buildable-now measurement substrate.

Phases:
- [x] **P1 — Cheque core DONE 2026-07-15** (new crate `crates/cheque`, pure offline; deps zeph-core + zeph-crypto):
      `ServingCheque{server, consumer, cumulative_bytes, timestamp, consumer_sig}` (domain
      `craftec/serving-cheque/1`, cheques are per-`(server,consumer)` pair, CUMULATIVE across all cids —
      content-agnostic — because a node holds few pieces over many cids; `sign`/`verify`), `ChequeIssuer`
      (consumer — `issue` monotonic timestamped cheques per server, `owed_to`), `ChequeBook` (provider —
      `record` iff addressed-to-me + valid-sig + STRICTLY-higher cumulative; `total_earned` = serving
      measurement; `load`/`cheques()`). **Design decision (2026-07-15): NO per-pair cap** — instead
      `allocate_quota(cheques, quota)` splits each provider's owed into (paid, subsidy), the consumer's single
      paid quota allocated FIRST-COME by timestamp; total PAID ≤ quota (= what the consumer paid) → self-dealing
      zero-sum, per-pair cap unneeded; overflow = subsidy. Free quota isn't rewarded (nothing to inflate); the
      cap protects the PAID distribution. Timestamp is signed (integrity) but gaming it can't inflate the total,
      only reorder paid-vs-subsidy. 7 tests: sign/verify + tamper (cumulative/server/**timestamp**/sig);
      non-consumer-signed refused; monotonic + stale refused; wrong-server refused; multi-consumer sum; load
      roundtrip; **quota allocation caps paid at quota by timestamp** (+ quota=0 → all subsidy). Gates: build,
      7 tests, fmt, clippy. (Settlement — calling `allocate_quota` + paying providers from tokens/pool — is step 4.)
- [x] **P2 — Transport hook DONE 2026-07-15** (DECOUPLED model — the piece hot-path stays untouched; cheques ride
      their OWN mux tag). obj gained a `ByteMeter` trait + `set_byte_meter`/`meter_bytes`, fired inline in
      `ObjEngine::get` for each VERIFIED piece (post-vtag `wp.data.len()`), crediting the provider it came from
      (the fetch fanout now carries `p.node_id` out via a `(node_id, result)` tuple). New `noded::cheque::ChequeService`
      (`crates/noded/src/cheque.rs`) `impl ByteMeter`: accumulates per-provider against a CREDIT_BAND (4 MiB); at the
      band it issues a cumulative `ServingCheque` (`ChequeIssuer::issue`, HLC-ms timestamp from `transport.clock()`) and
      ENQUEUES a fire-and-forget push over new `tag::CHEQUE = 10` (non-blocking `try_send` → `run_pusher` resolves the
      provider addr via `membership.member_addr` + `request_tagged`). Provider side: `serve(tag::CHEQUE)` records inbound
      cheques into a `ChequeBook` (→ `total_earned`, the measurement). Wired in main.rs (construct + `set_byte_meter`
      + handler registration + `serve`/`run_pusher` spawns + `set_membership`); `zeph-cheque` added to root
      workspace.deps + noded. Fire-and-forget like `tag::BOARD` (serve finishes the stream, sender ignores the reply).
      2 noded tests (credit-band batches into a cumulative cheque + verifies; provider records inbound → earnings).
      Gates: build (workspace), tests, fmt, clippy all green. (Cross-node push/record is compile-verified +
      fleet-validatable, matching how attest/sequencer shipped — not unit-tested cross-node.)
- [x] **P3 — DISSOLVED into step 4 (2026-07-15).** With §10 resolved, the participation metric is dissolved
      (§10.2: paid demand *is* the metric — no standalone contribution oracle). So P3's "surface the measurement
      for the metric" has no separate consumer: `total_earned`/`allocate_quota` are consumed directly by the
      token-ledger's settlement (step 4). No standalone observability built (would have been a to-be-reshaped
      interface with no consumer — CLAUDE.md "no speculative breadth"). `total_earned` keeps its documented
      `#[allow(dead_code)]` until the ledger reads it.

**§10 RESOLVED (2026-07-15) — economic policy locked; see docs/ECONOMIC_LAYER_DESIGN.md §10 (reconciled in place).**
Decisions: #1 reward ∝ paid demand (DECIDED, the spine); #2 participation metric DISSOLVED; #3 token issuance/genesis
= GOVERNED PARAMETER (native default fair-launch, no premine); #4 finality (n,k) = program param, default n=4/k=3
(f=1); #5 sequencer quorum selection = ROTATING EPOCH COMMITTEE (deterministic per-epoch rendezvous selection from
live membership — the heaviest step-4 sub-build: epoch clock + committee fn + cross-epoch sequence hand-off);
#6–#11 models decided, numeric params governed-at-launch; #12 PDP deferred (cryptographer). Doc header + §3/§4/§5/§11
reconciled to match.

**NEXT BUILD = §11 step 4: the TOKEN-LEDGER APP** (now unblocked). Two balances (tokens + non-transferable credit),
transfer, mint-from-receipts, egress settlement via `allocate_quota`, free-tier credit redemption — on verification
[K6] + the sequencer. Mechanism-first with governed policy (issuance §10.3, quorum (n,k) §10.4, fee/allowance §10.6);
the rotating-epoch-committee selection (§10.5) is its heaviest part, genesis-committee-bootstrapped.

**DESIGN CAPTURE (2026-07-15) — step-4 spec written into docs/ECONOMIC_LAYER_DESIGN.md** (from the free/paid Q&A):
(1) §7 settlement across many providers — the COMPLETE-cheque-set rule (allocate_quota's paid cap is global →
completeness enforced by reconciliation/monotonic-max-per-pair + unilateral provider settlement, not trust);
(2) §8 the FREE/PAID product boundary (scale/reliability/durability/value) + conversion drivers ("free lets you
USE the network; paid lets you BUILD on it"); (3) §8 the ENFORCEMENT-POINT MAP (cheque = tier-blind METER not
enforcer; rules enforced at settlement [authoritative] + fetch-admission gate + pin/publish gate [real-time]) +
the ledger-STATE-vs-ledger-PROGRAM(policy)-vs-MECHANISM frame; (4) §5 the K1 coupling — the canonical-token-program
pin IS a K1 anchor + the governed knobs are K1 config; K1's deferred anchor-dispatcher half is exactly what step 4
(first governed-WASM program) requires. Litmus: hard invariants native, swappable policy governed-WASM behind an anchor.

**DESIGN REVISION (2026-07-16) — reconciled into docs/ECONOMIC_LAYER_DESIGN.md in place (supersedes the "credit token"
model). From the free/paid Q&A:**
- **Free tier is NOT a token/second balance — it is GLOBAL TIT-FOR-TAT reciprocity** (§8 rewritten). Free headroom =
  `total_earned − consumed` (byte reciprocity), derived from accounting already collected; net-positive = free (network
  pays nothing, reciprocal not subsidised), deficit beyond band = pay tokens or throttle. **KILLED the two-balance /
  non-transferable-credit model** → ONE token balance + a derived reciprocity position. §7's tit-for-tat credit band IS
  the free tier now (§7/§8 merged). Subsidy shrinks to a **cold-start grant only**; free-tier farming is now largely
  **intrinsic** (can't consume free without contributing) → §10.6/§10.7 revised.
- **Reciprocity is UNIVERSAL, not free-only (refined 2026-07-16):** the SWAP credit band nets reciprocal byte-exchange
  to zero for EVERYONE (paid or not), so tokens/grant settle only the DEFICIT (`consumed − served`). Paid vs free =
  how the deficit settles: paid deficit → tokens against escrow (retroactive, NO gate); free deficit → bounded
  cold-start grant else throttle (REAL-TIME admission gate — fires only on an UNBACKED deficit). "Pre-funded → check
  late; un-funded → check live." Settlement mechanics (step 4): reciprocity offset applies BEFORE allocate_quota;
  GLOBAL position is authoritative, bilateral netting = trustless fast-path; surplus serving (beyond own consumption)
  earns a token reward — no double-count (reciprocity first, surplus rewarded).
- **Balances = SELF-CUSTODIAL account-chains, NOT PDAs** (§3): balance = fold of the owner's own single-writer chain,
  re-executed for validity; user signs, the token PROGRAM only CONSTRAINS to valid transitions (verification rejects
  else — the reserved-namespace write model). A malicious custom program has ZERO authority over the token namespace
  (canonical cid pinned in the trust root, verification re-runs THAT, never a caller-supplied cid). Shared state (pool/
  issuance/epoch) = a governance-owned chain (the lone PDA-analog). Recipient-credit = claim-vs-fold, pinned at step 4.
- **Token INTERFACE STANDARD (§5 new "Protocol standards"):** the token is a **protocol program, not a user app**;
  publish a versioned ABI, not the impl. **Layer 0** = core fungible interface (transfer/balance/total_supply, every
  asset incl. user-issued); **Layer 1** = protocol privileges (claim/settle) — native/canonical ONLY, gated to the
  canonical cid by the trust root (user assets CAN'T adopt it). Native token = reference impl of L0+L1; user asset = L0
  only. Discovery/versioning via the anchor registry (name → cid + interface_version). Formal "ZIP" process DEFERRED
  (governance + K1 anchors already give binding ratification). Suggested name: CTS-1. This is what step 4 builds against.

---

# ECONOMIC LAYER — ORDERING SEQUENCER (§11 step 1 of docs/ECONOMIC_LAYER_DESIGN.md) — SEQUENCER COMPLETE (P1–P4b-2, 2026-07-15)

Design settled in docs/ECONOMIC_LAYER_DESIGN.md (§4/§5). Building the first no-dependency piece of the economic
layer: finish the deferred K7 attestation auto-signing and extend the attestation substrate into a per-account,
non-equivocating, quorum-intersection-sized ORDERING SEQUENCER — the mechanism the token ledger sits on
(validity = verification [K6]; ORDERING = this sequencer; custody = attestation [K7]). Reuses the attestation
substrate (crates/com/src/attestation.rs): `Quorum`, `count_signers`, `MemberSignature`, the `QuorumChain` fold.

Current state (Explore map 2026-07-15): `QuorumChain`'s strict-seq fold is already a non-equivocating serializer
for the quorum CONFIG (not per-account nonces). Missing pieces: (a) a per-account nonce distinct from quorum-config
seq; (b) an intersection constraint on threshold (`2k>n` — absent; threshold only clamped `1..=n`); (c) a
quorum-gated append-at-nonce WRITE path (the `attest` host fn is read-only `is_authorized`); (d) the structural
non-equivocation invariant + the auto-sign policy hook (Package B — deferred, not in code).

Phases (each: build+test → design-check → review → commit):
- [x] **P1 — Sequencer core DONE 2026-07-15 (commit pending)** (pure offline, `crates/com/src/sequencer.rs`): `SequencedWrite{account,nonce,payload}`,
      `SequencedCommit{write,sigs}`, `AccountSequence` log (sequential-nonce fold), `Quorum::is_intersection_sized`
      (`2k>n`), `SequencerMember` structural non-equivocation (a member signs ≤1 write per `(account,nonce)`;
      idempotent for the identical write). Unit tests: intersection sizing; member refuses conflicting same-nonce;
      two conflicting writes can't BOTH reach k under intersection+honest-refusal; account binding; fold/tamper
      reject; encode/decode roundtrip. NO wire/host/node changes.
      **Landed:** `crates/com/src/sequencer.rs` (new) + `Quorum::is_intersection_sized`/`byzantine_tolerance`
      (attestation.rs, additive) + lib.rs exports. **Design-check fix:** `2k>n` is the intersection FLOOR
      (tolerates `f = 2k−n−1` Byzantine double-signers, `=f` for the classic `n=3f+1`/`k=2f+1`); added
      `byzantine_tolerance()` + a test proving 2-of-3 forks under 1 Byzantine while 3-of-4 does not. **Gates
      all green:** 82 zeph-com tests + 8 new sequencer tests, fmt, clippy `--all-targets`, `cargo build
      --workspace` (noded compiles). Core safety proven: `two_conflicting_writes_cannot_both_commit`.
- [x] **P2 — Backend + host fn DONE 2026-07-15** : `Capability::Sequence`, a `sequence(account_ptr, nonce, payload_ptr, len) -> i32`
      host fn (mirror `attest`), a `SequenceBackend` trait, wired through transition/invoke. Node serializes the
      write through the per-account quorum. owner/quorum server-resolved (like `attest`, never caller-supplied).
      **Landed** (mirrors the `attest` machinery exactly): `Capability::Sequence` (capability.rs, in `full()` +
      `verifier()` inert, NOT `deterministic()`; 3 profile tests updated); `SequenceBackend` trait (lib.rs);
      `sequence(account_ptr,nonce:i64,payload_ptr,len)->i32` host fn (transition.rs — `2` inert / `-1` no-backend
      or no-owner or malformed/neg-nonce / `1` committed / `0` rejected; owner server-resolved so no self-order) +
      `sequence_backend` ctx field + `with_sequence_backend`; `InvokeService` field/param + `.with_sequence_backend`
      in the invoke path; the 3 `InvokeService::new` call sites (noded main + feed/invoke tests) pass the 6th arg
      (noded `None` until P3). **Test:** `sequence_producer_path_returns_the_commit_outcome` proves 1/0/-1/2.
      **Gates:** 84 zeph-com tests + integration tests, fmt, clippy `--all-targets`, `cargo build --workspace`.
- [x] **P3 — Node service + cross-node propagation DONE 2026-07-15 (model 1: leaderless gossip)**: per-account
      sequence chains publish/pull cross-node (mirror `AttestStore` anti-entropy over a reserved DHT name);
      non-equivocation guard generalized per-`(account,nonce)`; any node serves the sequenced order (sync-first).
      **Landed:** `crates/noded/src/sequence.rs` — `SequenceStore` implementing `SequenceBackend`: one
      `AccountSequence` per (owner,program,account), persisted `<dir>/sequence/*.seq`, published as durable
      content + a reserved `\u{1}sequence-<o>-<p>-<a>` DHT head, pulled cross-node (`sync` adopts only a
      strictly-longer sequence that EXTENDS our committed prefix — `extends()` refuses a fork; requires the
      quorum intersection-sized). REUSES the program's attestation quorum (`AttestStore::current_quorum`, new
      method — one program, one quorum for both authority + ordering). `submit(owner,program,commit)` = the
      k-of-n write path (syncs first, then `AccountSequence::append`). `sequence()` backend auto-commits a
      threshold-1 (self) quorum; k>1 → `false` (needs the P4 auto-sign accumulate). Wired into `InvokeService`
      (main.rs, replaces the P2 `None`) + `set_membership`; control-plane read `sequence_log` RPC + `zeph
      sequence-log --program --account [--owner]` CLI (`control.rs`/main.rs) so any node serves the order.
      **Tests (4):** self-quorum commit+read-back; submit k-of-n orders + refuses forks/gaps/sub-threshold;
      non-intersection-sized (2-of-4) refused; persistence reload. **Gates:** full zeph-noded suite (22) + fmt +
      clippy + `cargo build --workspace`. **Deferred to P4:** the leaderless *sig-accumulate gossip* (pending
      proposals gathering k signatures cross-node) pairs with member auto-sign — until then multi-member writes
      use `submit` with explicitly-gathered signatures.
- **P4 — Owner-authenticity gate + multi-member auto-collection (approach (b): PURE ORDERING, decided 2026-07-15).**
  Design settled over a design discussion: the sequencer stays PURE ORDERING; owner-authenticity is a cheap
  owner-SIGNATURE gate (like Ethereum's signed-by-sender — prevents nonce front-running/griefing), NOT a member
  policy-program (the quorum membership IS the policy). App-VALIDITY (e.g. balance) stays in the program's
  transition fn, enforced by verification. The ledger COMPOSES sequence (order) + transition fn (validity) +
  verify (certify) — three substrates, one program (order+validity are separate, like Ethereum base-layer vs EVM).
  - [x] **P4a — Owner-signature gate DONE 2026-07-15.** `SequencedWrite` gains `owner_sig` (account owner's
        ed25519 sig over (account,nonce,payload), domain `craftec/sequencer-owner/1`, distinct from the member
        ORDERING domain); `SequencedWrite::author()` + `owner_authentic()`; `SequencedCommit::authorizes` AND
        `SequencerMember::sign` both REFUSE a non-authentic write; the store's `sequence()` authors for the
        node's OWN account (account==self.me()); multi-member writes ride `submit` with a pre-authored write.
        Tests: forged/impostor/garbage-sig write refused (com `an_unauthenticated_write_is_never_ordered`);
        store tests author writes. Gates: com 85 tests + noded 22, fmt, clippy, `cargo build --workspace`.
  - [x] **P4b-1 — owner_sig ABI DONE 2026-07-15.** `sequence(account_ptr, nonce, payload_ptr, len,
        owner_sig_ptr) -> i32` now takes a PRE-AUTHORED write (the account owner's 64-byte owner_sig read at
        owner_sig_ptr); `SequenceBackend::sequence(owner, program, SequencedWrite)`; the store orders ANY
        owner-authentic write (dropped the account==self restriction) as a quorum member. Test: a node orders
        alice's pre-authored write; a spoofed (wrong-signer) write is refused. Gates: com 9 seq + transition
        producer, noded 5 seq, fmt, clippy, `cargo build --workspace`.
  - [x] **P4b-2 — Multi-member auto-collection DONE 2026-07-15 (leaderless solicit-RPC).** New
        `tag::SIGN_SOLICIT` mux protocol (transport, additive). `SequenceStore` gains: `sign_write_locally`
        (member auto-sign — quorum-member + owner-authentic + non-equivocation via a PERSISTENT signed-set
        `<dir>/sequence/signed.set` for cross-restart safety); `solicit_member` (`request_tagged` to a member's
        `member_addr`); `collect` (self-sign + solicit the rest → k sigs → `submit`; leaderless — any node
        collects); `serve` (inbound solicitation handler). `sequence()` now COLLECTS k (not just k=1). Wired in
        main.rs (`open` takes transport; `serve` spawned on `SIGN_SOLICIT`). Test:
        `member_auto_signs_and_refuses_equivocation_across_restart` (idempotent sign + conflict refusal + survives
        restart); k=1 self path proven via `self_quorum...` (now through `collect`). Gates: noded 6 seq, fmt,
        clippy, `cargo build --workspace`. Cross-node k>1 solicit = compile-verified + fleet-validatable.

**★ ORDERING SEQUENCER (§11 step 1) COMPLETE — P1→P4b-2, 7 commits.** The mechanism the token ledger sits on is
built end-to-end: per-account non-equivocating quorum-ordered writes, owner-authenticated, cross-node propagated
+ served, fork-impossible at commit, automatic multi-member collection. Next in §11: step 2 (serving-cheque +
measurement substrate) / step 3 (participation metric, blocked on §10 policy) / step 4 (token-ledger app).

---

# FILE SEGMENTATION — large-file chunking (design-check DONE 2026-07-15; chosen: Model B) — ACTIVE

**Design-check verdict:** CRAFTOBJ_DESIGN is internally inconsistent — §80–92 (one `content_cid`, segments
internal, pieces self-describe `segment_index`) vs §224 (chunked block-tree + range reads + block-level
dedup). The code keys EVERYTHING by cid alone (one generation per cid; `CodedPiece = {coding_vector,data}`,
no segment fields — erasure docstring says segment identity belongs in the obj wrapper). **CHOSE Model B
(segment = sub-cid, IPFS-UnixFS DAG):** `Manifest::File` lists ordered segment cids, each segment its own
K=32 generation. **Wire-COMPATIBLE** (staggered roll — corrects the earlier "wire-incompatible" assumption),
reuses ALL per-cid repair/health/distribute/store machinery unchanged, gives block-level dedup (§224), and
solves the doc's two unspecified gaps for free (per-segment digest = the segment cid; per-segment vtags =
each segment's own vtags blob). Deviation from §80–92 → reconcile that section in P4.

**Params (from CO §71/§73 + TF §352–359):** segment 8 MiB = 32×256 KiB; files **K=32** → n = k·ceil(2+16/k)
= 96 pieces (3×, 64 parity); piece 256 KiB. SQL stays K=8/16 KB (untouched). Encryption = layer-above /
**encrypt-then-chunk** (segments are opaque ciphertext chunks, CID=BLAKE3(ciphertext), one DEK across a
file's segments — CO §76/§286/§298).

- [x] **P1 core segmentation DONE 2026-07-15.** `FILE_SEGMENT_BYTES=8 MiB` + `FILE_K=32` consts (obj/lib.rs);
      `publish_impl` gained a `k` param (files use K=32, other objects keep `config.k`); `split_sources` takes
      `k`. `Manifest::File.content ([u8;32])` → `segments: Vec<Segment{cid,len}>` (+ `Segment` type,
      `file_segments()`). `publish_file` chunks into ≤8 MiB segments, publishes each as its own generation
      (K=32) → segment cids → manifest; `fetch_file` fetches + concatenates segments IN ORDER (each cid
      self-verifies). `chain_children` walks `File.segments` (pin/want/forget cascade over all segments). New
      `ObjEngine::get_following_manifest(cid,mode)` centralizes "resolve bytes behind a possibly-multi-segment
      file manifest" → the 6 program-fetch callers (com/invoke, noded/account/governance/board/attest/control)
      now use it (correct for multi-segment wasm). DECISION taken: **broke old single-cid file manifests**
      (pre-1.0, no compat variant). Tests (obj green): small file = 1 segment + round-trips (parity); 17 MiB →
      3 ordered segments, byte-identical concat; identical leading 8 MiB segment **dedups to one cid** across
      files. Full workspace builds; obj (6+17+7+1) + com pass; clippy+fmt clean. KNOWN FOLLOW-UP: private files
      (`publish_private` → encrypt-then-publish ONE object) are NOT yet segmented — chunk the ciphertext in a
      follow-up (encrypt-then-chunk, one DEK). Wire-compatible (no piece/wire change; only the manifest shape).
- [x] **P2 range/partial reads DONE 2026-07-15.** `ObjEngine::fetch_file_range(manifest_cid, offset, len)`
      (obj/lib.rs): fetches the manifest, walks segments accumulating byte offsets, and `get`s ONLY the
      segments overlapping `[offset, offset+len)` — slicing each to the requested window + EOF-clamping;
      early-breaks once the range is covered. Tests (obj green): exact-slice correctness across all boundaries
      (whole / inside a segment / spanning the seg0→seg1 boundary / last partial segment / past-EOF-clamped /
      past-EOF-empty / zero-len); and an EFFICIENCY proof — forget the non-covering segments locally, a
      segment-1 range still reads correctly and never fetches seg0/seg2. (Debugging surfaced that the test's
      `i`-indexed data generator aliased at the 8 MiB power-of-two boundary → identical segments — a neat
      accidental proof dedup works; switched to a sequential LCG stream for distinct segments.) Follow-up
      (not P2-blocking): sequential-scan PREFETCH for smooth streaming (fetch the next segment ahead); wiring
      the range read to the CLI/host-fn.
- [x] **P3 lifecycle/repair over segments DONE 2026-07-15.** Confirmed the existing per-cid machinery already
      handles segments independently — `chain_children` walks `File.segments` (P1), each segment is its own
      generation want/pin-marked at publish, so the health-scan repairs each on its own; NO new repair code
      needed. Made `file_segment_bytes`/`file_k` **config fields** (default 8 MiB/32; also better production
      tunability) so tests use small segments; refactored the healthscan test `node`→`node_cfg`. New test
      (`each_file_segment_is_repaired_independently`): a 3-segment file, deficit ONE segment (forget its
      pieces on some holders, keeping rank ≥ k), scan → that segment's pieces are restored INDEPENDENTLY
      while the healthy segments are untouched, and a fresh fetcher reassembles the WHOLE file byte-identical.
      (Asserted relatively — deficit→repair progress + file integrity — not exact-floor convergence, which the
      single-object self-heal test already covers.) 18 healthscan tests pass; full workspace builds; clippy+fmt
      clean. Lightened the test (single-thread, 4 holders) after it flaked a neighbor timing test under cargo's
      parallel harness.
- [x] **P4 gate + staggered roll + doc reconcile DONE 2026-07-15. FILE SEGMENTATION COMPLETE + LIVE.**
      Reconciled CRAFTOBJ_DESIGN (craftec `1ebe4aa`): §80–92 piece struct (dropped in-piece
      `segment_index`/`segment_count` → segment = sub-cid), the Manifest model (`content_cid` → `segments:
      Vec<Segment>`), and marked §224 (chunked block-tree + range reads + block-level dedup) BUILT. Full gate:
      A-G harness + clippy + workspace tests all ✅ (a fmt-only failure on the DST-harness comment alignment,
      fixed `ffa1a86`, re-gated 🟢). Staggered roll of all 4 Hetzner nodes (wire-compatible — only the manifest
      shape changed): each active, NRestarts=0, 3 peers, 0 panics. **Live cross-node validation:** a 20 MB file
      (3 segments) published on node `zeph` → fetched by cid alone on `zeph2` (DHT-resolved, cid-verified) →
      byte-identical. Backup `zeph.bak-20260715-0320`. Remaining follow-ups (non-blocking): private-file
      segmentation — **CHUNK-then-ENCRYPT** (NOT encrypt-then-chunk: a single whole-file AEAD tag would
      block per-segment integrity → no streaming). Chunk the PLAINTEXT into ≤8 MiB segments, seal EACH
      independently (one DEK, per-segment nonce) → each ciphertext segment is independently
      fetchable/decryptable/verifiable → `fetch_file_range` streams over sealed segments; `cid =
      BLAKE3(segment_ciphertext)`; crypto-shred (destroy DEK) still nukes all. Sub-decision (as SQL #3):
      deterministic per-segment nonce (block-dedup within key, leaks equality) vs random (no dedup, max
      privacy) — default deterministic. Plus: sequential-scan prefetch for streaming, CLI/host-fn range-read.
- [x] **All 3 follow-ups DONE 2026-07-15.** (1) **Private-file segmentation (chunk-then-encrypt):**
      `publish_private` chunks the PLAINTEXT into ≤8 MiB segments + seals each independently under one DEK
      (`seal_deterministic` → within-file block dedup) → publishes each ciphertext segment; the envelope
      (`EncryptedEnvelope`) now lists `segments: Vec<Segment>` + sealed `meta` (name/mime stay private) +
      `size` (was `ciphertext_cid`; `PlainFile`→`PlainMeta`). `get_private` reassembles; new
      `get_private_range` streams (fetch+decrypt only covering segments). `chain_children` walks env
      segments. Test: 3 sealed segments, identical blocks dedup to one cid, whole round-trip, boundary-
      spanning range read, foreign-can't-decrypt. (2) **Prefetch:** `fetch_file`/`get_following_manifest`/
      `get_private` fetch segments with bounded read-ahead concurrency (`buffered(SEGMENT_PREFETCH=8)`,
      order-preserving). (3) **CLI range read:** `zeph get <cid> -o <path> --offset N --length M` → control
      RPC → `fetch_file_range`/`get_private_range` (private auto-detected via the envelope). obj tests pass,
      clippy+fmt clean, workspace builds. (4) **WASM host-fn range read DONE:** `AppBackend::obj_get_range`
      (default errors; `CraftBackend` → `fetch_file_range`) + the `obj_get_range(cid, offset, len, out, cap)`
      host fn under `Capability::Obj`. Test: a WASM program reads a file slice `[10,30)` by manifest cid →
      correct bytes. **FILE SEGMENTATION fully complete — all follow-ups closed.**
- [x] **Follow-ups ROLLED + LIVE 2026-07-15.** Full gate 🟢 (A-G + clippy + workspace tests), staggered roll
      of all 4 Hetzner nodes (each active, 0 restarts, 3 peers, 0 panics; backup `zeph.bak-20260715-0505`).
      Live-validated on the fleet: a 20 MB PUBLIC file → cross-node whole fetch (zeph→zeph2) byte-identical +
      `zeph get --offset 10M --length 1000` range read (only covering segments) matches; a 20 MB PRIVATE file
      (chunk-then-encrypt) → owner whole fetch byte-identical + private `--offset/--length` range read matches.
      Commits `184fd6e` + `5281565`.

---

# K3 — SHARING via PROXY RE-ENCRYPTION (task #4) — DONE + LIVE 2026-07-15 (below is the build log)
The encrypted-grants substrate. `cipher` (`crates/cipher/src/lib.rs`) already has the OWNER side: `Dek`
+ `seal`/`open` (XChaCha20), `EncSecretKey`/`EncPublicKey` (PRE keypair, `from_identity_seed`),
`DekCapsule` + `encapsulate` (the DEK encapsulated under the owner's key via `umbral_pre::encrypt`),
self-open via `decrypt_original`. Crypto-shred = destroy the capsule → DEK unrecoverable (single-key,
already the design — confirms K4 not needed for shred). MISSING = the SHARING (re-encryption) ops.

DESIGN (decided 2026-07-14): sharing = **Umbral threshold PRE** — the whole substrate; K4 NOT needed
(Umbral's M-of-N kfrags ARE the threshold secret-sharing, built in). Owner encrypts once; to grant a
recipient, owner issues M-of-N **kfrags** (re-encryption key fragments) to proxy nodes who transform the
capsule WITHOUT seeing plaintext; recipient collects M **cfrags** + decrypts with their own key.
Revoke = stop re-encrypting / rotate. Grants (who may access) = policy on top (app decides).

PHASES (each: build+test):
- [x] P1 cipher PRE sharing ops DONE 2026-07-14 (`crates/cipher/src/lib.rs`): `ReKeyFrag`/`ReCapsuleFrag`
      (serde-serializable wire types); `grant(owner, recipient_pk, threshold, shares) -> Vec<ReKeyFrag>`
      (Umbral `generate_kfrags`, owner PRE key delegates+signs); `reencrypt(owner_pk, recipient_pk, obj,
      kfrag) -> ReCapsuleFrag` (proxy verifies kfrag origin then `umbral_reencrypt`; no plaintext);
      `decrypt_granted(recipient, owner_pk, obj, cfrags)` (verify cfrags → `decrypt_reencrypted` → DEK →
      open). Additive — no change to SealedObject/DekCapsule; owner self-decrypt unaffected. 2 tests
      (8 total pass): 2-of-3 grant→2 proxies reencrypt→Bob decrypts, <threshold fails, non-recipient
      fails, owner still self-decrypts; kfrag/cfrag postcard roundtrip. clippy clean. Added postcard
      dev-dep to cipher. NOTE confirmed: crypto-shred (destroy capsule) already in cipher — single-key,
      K4 not needed.
**DESIGN CORRECTION (2026-07-14, before P2) — checked ENCRYPTION_DESIGN §9b/§13 + the user's steer, which
overturned the original P2 plan (`pre_rekey`+`pre_reencrypt` raw-key host fns):**
- §9b: the re-encryption transform is **pure WASM (`umbral-pre`), NEEDS NO HOST FN**; its threshold trust
  "maps directly onto CraftCOM's attestation" (k-of-n proxies = a quorum, already built) → **`pre_reencrypt`
  DROPPED.**
- §13: **"the app never sees raw keys — the runtime mediates"** → a raw-key `pre_rekey` is wrong; the ONE
  key-touching op (`generate_kfrags`) must be **runtime/backend-mediated**.
- Grant RECORD = a row in the **OWNER's own CraftSQL DB** (existing `sql_execute`), NOT a new KIND_ record and
  NOT the registry single-writer path: a grant is owner-authored + bilateral, no contention → no consensus.
  The owner writes their own record (sidecar-authoritative); the DB head rides `RT_DBROOT` (owner-signed,
  background-published, no CAS) only for offline-owner DISCOVERY, never on R's critical path. `KIND_GRANT=8`
  is reserved in routing but subsumed by owner-DB rows.
- [x] P2 host ABI DONE 2026-07-15 — `Capability::Pre` + the runtime-mediated `pre_grant` host fn (the ONLY
      new host fn K3 needs). `crates/com/src/capability.rs`: `Pre` in `full()` (app-profile: non-det
      `generate_kfrags`) + `verifier()` INERT (single-module link), NOT `deterministic()`. `lib.rs`:
      `AppBackend::pre_rekey(recipient_pk: Vec<u8>, threshold, shares) -> Result<Option<Vec<u8>>>` default
      `Ok(None)` (noded overrides in P3; mocks/CraftBackend compile unchanged). `transition.rs`:
      `pre_grant(recipient_ptr,len, threshold, shares, out,cap) -> i32` — `2` INERT on verify-mode, `-1`
      UNAVAILABLE (no backend / `Ok(None)` / bad recipient / small buf), else the serialized `Vec<ReKeyFrag>`
      length. Delegates with the RUNNING identity's OWN key (backend-derived) → no self-authorize risk, no
      `program_owner` check (unlike attest). Recipient key is the raw 33-byte compressed PRE pubkey (NOT a
      32-byte NodeId — caught + fixed a `[u8;32]` ABI bug via a failing test). Tests (75 com pass):
      end-to-end `pre_grant` → real `cipher::grant` fragments → proxy reencrypt → recipient decrypts (owner
      key never leaves the backend), below-threshold fails; + the Pre capability GATE (deterministic profile →
      `pre_grant` unbound → fails to instantiate). clippy+fmt clean, full workspace builds.
- [x] P3 backend glue DONE 2026-07-15 — `CraftBackend::pre_rekey` (`crates/com/src/craft.rs`): the backend
      already holds this identity's `EncKeypair` (new field, derived in main.rs from `identity.secret_key_bytes()`
      like the obj/sql encryption key) → `EncPublicKey::from_bytes(recipient)` (validates) → `cipher::grant` →
      `postcard::to_allocvec`. Delegates with the node's OWN key (a program can only share ITS OWN data — no
      cross-owner escalation). `zeph-cipher` moved to com's main deps; all 4 `CraftBackend::new` call sites
      updated (main.rs prod + 3 integration tests). Live-path GATE test (`craft_backend.rs`): a WASM program
      calls `pre_grant` against a REAL CraftBackend (not a mock) → real 2-of-3 fragments → proxy reencrypt →
      recipient decrypts a sealed object under the same owner key (owner secret never leaves the backend).
      Full workspace builds, com tests pass, clippy+fmt clean. Grant-row storage + kfrag distribution stay
      app-orchestrated (existing `sql_execute` + kfrags-as-data); reencrypt is pure WASM. (Note: hit + cleared
      a host disk-full/APFS-snapshot-pinned blocker mid-build; user freed space.)
- [x] P4 gate + roll DONE 2026-07-15 — `deploy/gate.sh --quick` 🟢 PASSED (fmt + clippy -D + full workspace
      tests; A-G skipped, sanctioned for local-logic). STAGGERED roll of all 4 Hetzner nodes (additive/local,
      wire-unchanged → mixed-version-safe): rsync `crates/` → box build (1m24s, clean) → install → restart
      zeph2→zeph3→zeph4→zeph one-at-a-time. Every node came back active, NRestarts=0, reconverged to 3 distinct
      alive peers (the current baseline — Mac node stopped, so 3 not the stale note's "4"), 0 panics. Backup
      `zeph.bak-20260714-2304`. Commits `54ec26f`/`454752c`/`671d88a` pushed to origin. Mac node skipped (stopped
      per [[zeph-fleet-deploy]]). No cross-node behavior to live-validate (pre_grant is a local host fn); the
      real grant→reencrypt→decrypt path is covered by the P3 live-CraftBackend integration test.

**K3 COMPLETE (2026-07-15).** Sharing-via-proxy-re-encryption substrate is built, tested, and live. Mechanism
in the kernel (cipher PRE ops + `pre_grant` host fn + owner-DB grant model); products (file-share w/ access
control, followers-only feeds, shared encrypted DBs) are the app layer on top. Deferred follow-ons: a demo
app for a live cross-node grant (optional — integration test covers the path); ENCRYPTION_DESIGN §9b prose
reconcile ("grant record = owner-DB row, not a registry record / no single-writer").
NOTE: it's "add the re-encryption ops" not merely "expose" — cipher has encrypt/self-open but not the
kfrag/reencrypt side yet (both are in the `umbral_pre` dep, just unwired). Still cheap.

# PDP — PROOF OF DATA POSSESSION (K5 / task #3) — vtags approach SHELVED; lattice-LHS = future milestone (2026-07-14)

**OUTCOME: the vtags-based P1-P3 (below) is SHELVED to `git stash` ("K5 PDP (vtags-based) — SHELVED..."),
NOT committed/rolled.** Adversarial review found it UNSOUND: verification collapses to `vtags::verify`,
which is only ~8 fixed PUBLIC linear equations → forgeable (all-zero piece, or Gaussian-solve for any
chosen coding vector) → a lying holder farms receipts storing nothing. vtags is a corruption detector,
NOT an adversarial possession proof (its own docs say so).

**DESIGN CONCLUSION (worked through with the user):** a sound PDP here must be simultaneously
(1) sound vs a lying holder, (2) repair-compatible = HOMOMORPHIC (pieces regenerate via recode — so
ANY publish-time static commitment [Merkle, per-piece tag] is dead; the tag must recode WITH the piece),
(3) PUBLICLY verifiable / no single-point-of-failure (the protocol deliberately has no privileged/
required-online verifier — a symmetric homomorphic MAC is REJECTED because its designated-verifier key =
a SPOF + owner-online dependency), (4) field-compatible (GF(2⁸), no erasure-engine rewrite). Those jointly
force an ASYMMETRIC homomorphic signature. Two options: pairing (BFKW, mature crypto but needs F_p → the
GF(2⁸) rewrite, because pairing scalars are prime-order and GF(2⁸) is char 2 — no homomorphism) vs LATTICE
(Boneh–Freeman "Linearly Homomorphic Signatures over Binary Fields" 2011 — literally for network coding
over binary fields, public + no field change + post-quantum). **USER CHOSE LATTICE** (future-proof + zero
change to the mature erasure core; accepted tradeoff = exotic-crypto implementation risk, managed by
design-first + isolation + adversarial forgery tests + external cryptographer review).

**LIBRARY REALITY (2026-07-14):** `lattirust/lattirust` is NOT usable — it's a ZK-proof/ring-arithmetic
lib (LaBRADOR/Lova), does NOT implement LHS; its own README: "not audited nor fit for real-world
deployment. Always consult your trusted cryptographer." No production Rust binary-field-LHS lib exists →
the lattice route means implementing the scheme (highest crypto risk) → **needs a real cryptographer**,
not just engineering. **SEPARATE REPO CREATED: `../binfield-lhs`** (sibling of zephcraft; own git repo,
initial commit d15a66b) — standalone/publishable crypto lib; `docs/DESIGN.md` = the design pass, `src/lib.rs`
= interface-only skeleton (unimplemented). NEXT STEP = resolve the design-pass open questions with a
cryptographer: unbounded-recode/noise ceiling
(make-or-break — does the sig survive arbitrarily many recodes?), signature SIZE/overhead at piece scale,
SIS params, library/impl plan, integration (per-piece sig field + recode carries it). Reusable from the
shelved work: the wire challenge/response shape, the rendezvous+jitter auditor loop, `StorageReceipt` —
only the vtags verification CORE is replaced.

--- SHELVED vtags approach (git stash@ K5 PDP; reference only) ---
Storage proofs → StorageReceipts (reputation currency). Foundation §PDP: "coefficient vector
cross-checking" (GF(2⁸) linear algebra), verify possession WITHOUT downloading the full piece, →
signed StorageReceipt. Reuses RLNC `recode` + `vtags::verify`.

DESIGN (locked with user 2026-07-14):
- PROOF = coefficient cross-check. Challenge = (cid, nonce/seed). Prover derives GF(2⁸) coeffs from
  the seed (BLAKE3-keyed PRNG) and `recode`s its held pieces with those coeffs → ONE combined piece
  + its claimed coding vectors. Verifier: (a) claimed cvs DISTINCT, (b) combined_cv == Σ cᵢ·cvᵢ,
  (c) `vtags::verify(cid_vtags, combined_piece)`. Sound (a holder missing a challenged piece can't
  produce valid combined_data — the null-space check fails); cheap (~1 piece, no source).
- CHALLENGER = rendezvous-elected per (cid, epoch) (reuse `rendezvous_score`), NOT the healthscan
  (user's K8 boundary: PDP is a SEPARATE fair auditor; healthscan is durability). Fired at a
  deterministic RANDOM OFFSET within the epoch — `hash(node‖cid‖epoch) mod epoch_ms` — so
  challengers don't all storm at the epoch boundary (user's explicit refinement).
- OUTPUT = `StorageReceipt{content_id, storage_node(prover), challenger, segment_index, piece_id,
  timestamp, nonce, signature}` (foundation shape; challenger-signed; prover signs its proof).
  Scope = the RECEIPT MECHANISM only. Reputation SCORING is policy "above" it (roadmap) — deferred
  (thin tally or governed-WASM later). Distinct from K6 verification (compute consistency) + K8
  availability probe (self-report durability; PDP is the CRYPTOGRAPHIC upgrade of that path).

PHASES (each: build+test):
- [x] P1 pure proof DONE 2026-07-14 (obj/src/pdp.rs, `pub mod pdp`): `challenge_coeffs(seed,m)`
      (blake3 keyed XOF, forced non-zero), `prove(seed, held)` → (cvs, combined piece via gf::axpy),
      `verify(vtags, seed, cvs, combined)` (distinct cvs + combined_cv==Σcᵢcvᵢ + vtags::verify),
      `StorageReceipt{...}` (foundation shape; sig=Vec<u8> for serde; issue/verify_sig). Added blake3
      + zeph-crypto to obj deps. 4 tests pass: honest+wrong-seed; missing-piece (partial-combine AND
      forged-cv both fail); duplicate-cv rejected; receipt sig roundtrip+tamper. clippy clean.
- [x] P2 wire + responder DONE 2026-07-14. wire: `PdpChallenge{cid,nonce,segment_index}` +
      `PdpProof{cid,nonce,prover,coding_vectors,piece}` (tags 0x0044/0x0045, append-only; NO prover
      sig — obj has no identity, the QUIC peer authenticates the prover). obj responder (handle arm):
      `serve_pieces` → `pdp::prove(nonce, held)` → proof (empty = holds nothing). Challenger-side obj
      methods `pdp_challenge(cid,addr,nonce)` (round-trip) + `verify_possession(cid, proof)` (loads
      vtags from the held Generation — challengers are cid holders → have vtags). Round-trip test
      (`pdp_challenge_proves_then_fails_after_eviction`): honest holder proves + verifies; evicted
      (alive) holder → empty proof → verify false. clippy clean.
- [x] P3 challenger loop + receipt DONE 2026-07-14. obj helpers `is_pdp_challenger(cid,epoch,holders)`
      (rendezvous over holders∪self; must hold the generation→vtags) + `pdp_challenge_offset(cid,epoch,
      epoch_ms)` (jitter = rendezvous_score[0..8] mod epoch). noded `crates/noded/src/pdp.rs`
      `PdpAuditor`: periodic tick (every 10s) → per held cid, if past its jittered offset this epoch
      (PDP_EPOCH_MS=60s) + not done + elected → resolve holders, challenge each (fresh random nonce),
      verify_possession, issue+persist StorageReceipt to `<data>/pdp/receipts.jsonl`, log. done-set
      pruned per epoch. Wired in main.rs (spawned loop; logs running total). Builds + clippy clean.
- [ ] P4 gate + roll + live-validate — **BLOCKED: CRITICAL SOUNDNESS BREAK found by review 2026-07-14.**
      The coefficient-cross-check verification relies on `vtags::verify`, which only checks ~8 FIXED
      PUBLIC linear equations — a CORRUPTION detector (random bit-rot), NOT an adversarial possession
      proof. An adaptive holder forges a passing proof with ZERO real data: trivially all-zeros
      (coding_vector=[0;k], data=[0], expected=Σcᵢ·0=0, vtags dot(0,·)=0 → passes), OR generally via
      Gaussian elimination solving the 8 equations for any chosen coding_vector. vtags.rs's OWN docs
      admit this ("an adaptive attacker who solves the published linear constraints can craft pieces
      that pass vtags") and name PDP as the intended defense — circular, since our PDP RELIES on vtags.
      So a dishonest holder could farm valid StorageReceipts storing nothing → poisons the reputation
      currency. This is a DESIGN gap (foundation's "coefficient cross-checking via vtags" is
      cryptographically insufficient), not just impl. Also IMPORTANT: the auditor tick loop is an
      unbounded O(held-cids) DHT-resolve storm every 10s (resolve re-runs for non-won cids every tick;
      done-set only dedups winners; no pacing) — same class as [[zeph-dht-cutover-thrash]].
      P1-P3 CODE STANDS (mechanism: challenge/response/receipt/loop is fine; the VERIFICATION is the
      unsound part). NOT committed, NOT rolled. Needs a DESIGN DECISION on the sound path:
      (B) homomorphic authenticators (Shacham-Waters PoR / per-piece tag at publish; sound + no-download;
      heavy new crypto + format change — the vtags-doc upgrade path); (C) Merkle commitment per object +
      spot-check (sound; needs root committed at publish + format change); (2) REFRAME as a possession
      LIVENESS check (catches honest eviction/corruption like a stronger availability probe, NOT
      adversarial) + defer true PDP; or pause K5 until the authenticator infra exists.

# ATTESTATION SUBSTRATE (K7 / task #9) — building (2026-07-14)
User-defined quorum AUTHORITY per VERIFICATION_ATTESTATION_MODEL.md + ATTESTATION_DESIGN.md: "do the
specific parties I chose authorize this?" A program declares a named quorum (member pubkeys + k-of-n)
and triggers it to authorize a statement. Distinct from verification (consistency, #8 DONE) — they
compose in the program. It is `gov.rs` GENERALIZED from the single network anchor to an app-declarable
one: GovernanceSet→Quorum, GovAction→(opaque app Statement + self-amendment), GovernanceApproval→
Attestation, GovernanceChain→QuorumChain. Network governance is the genesis instance of the same
substrate. Reconfiguration = governance's self-amending apply() (already solves the "hard part").

Phases (each: build+test+gate+commit):
- [x] P1 core types (com) DONE 2026-07-14 — new `crates/com/src/attestation.rs`, near-verbatim generalization
      of gov.rs: `Quorum{members,threshold,seq}` (genesis/is_member/quorum(distinct valid sigs)/verify
      (seq+1 & ≥threshold)/apply(self-amend)); `AttestAction{Statement(Vec<u8>) | AddMember |
      RemoveMember | SetThreshold}`; `AttestProposal{action,seq}` (+sign, domain b"craftec/attest/1");
      `MemberSignature{member,signature}`; `Attestation{proposal,signatures}`; `QuorumChain{genesis,
      attestations}` (current fold + append + `is_authorized(statement)` = replay Statement actions).
      Pure/offline; tests mirror gov.rs. UNIFIED 2026-07-14 (user chose "unify now"): governance is
      now a LITERAL instance — the shared quorum mechanics live on `Quorum` (`count_signers`, `advance`
      + `MemberChange`), each approval type (`Attestation`, `GovernanceApproval`) carries its own
      `authorizes`/`apply_to`; `GovernanceSet = Quorum`, `GovSignature = MemberSignature` (type
      aliases). WIRE-IDENTICAL (postcard positional — `{governors,threshold,seq}` == `{members,…}`);
      pinned by a `governance_set_wire_layout_is_unchanged` guard test (35-byte layout). No governance
      wire/behavior change → no dedicated roll needed (ships mixed-version-safe with the attestation
      roll). Fixed 5 external sites (governance.rs is_governor→is_member/.governor→.member/.governors
      →.members, control.rs .governors→.members). com 70, noded 11, integration 4.
- [x] P2 the `attest` host ABI + AttestBackend (com) DONE 2026-07-14 — mirrors verify()/VerifyBackend:
      `Capability::Attest` (app full() + verifier() re-run grant, NOT deterministic); `attest(stmt_ptr,
      stmt_len)->i32` host fn (`2`=INERT on a verifier re-run — attestation is non-deterministic;
      `-1`=no backend; `1`/`0`=authorized/rejected from the backend); `AttestBackend` trait
      (`attest(program_cid, statement)->bool`); `TransitionCtx.attest_backend` + `with_attest_backend`;
      invoke.rs threads it (main + 2 com test callers pass None for now). Test: mock backend → 1/0/-1/2.
      NOTE: quorum bootstrap/lookup + solicitation deferred to P3 (the backend is where the program's
      QuorumChain lives + members are solicited). com 71; clippy/fmt clean.
- MEMBER-SIGNING POLICY DECIDED 2026-07-14 (Package A): governance-style — a statement is proposed,
  the NAMED members cosign MANUALLY (human judgment, like gov_sign), k-of-n appended to the program's
  QuorumChain; `attest()` = a CHECK ("is my statement authorized?"), decoupling the sync host call
  from async human signing. Automated *discrete* signing (a member-side policy program) = deferred
  attestation add-on. AGGREGATION oracle (feeds→median+freshness) = a SEPARATE future substrate, NOT
  attestation (boundary recorded in VERIFICATION_ATTESTATION_MODEL.md §5.3).
- [x] P3-1 local store + AttestBackend DONE 2026-07-14. `crates/noded/src/attest.rs` `AttestStore`:
      per-program QuorumChains keyed by program_cid, persisted to `<data>/attest/<cid>.chain`,
      loaded on open (only chains that fold validly). `bootstrap` (genesis quorum), `propose`/`cosign`
      (this node's sig), `submit` (append k-of-n + persist), `is_authorized`. `impl AttestBackend`:
      attest = is_authorized (the check). Wired in main.rs as the com attest backend. 2 tests: k-of-n
      authorizes → attest true; sub-threshold rejected + persists nothing (reopen confirms). noded 13,
      workspace builds, clippy/fmt clean. (propose/cosign/bootstrap/submit are the P3-2 control-plane
      API — #[allow(dead_code)] in the bin until the CLI wires them.)
- [x] P3-2 control plane DONE 2026-07-14. The cross-node COLLECTION is manual hex-passing (like
      governance's multi-governor flow), so NO new wire/gossip needed for the basic path — the PRODUCER
      collects: bootstrap the quorum on their node → propose (→ attestation hex) → each member runs
      attest-cosign on THEIR node (adds their sig to the hex) → producer attest-submit the k-of-n hex
      to their local chain → invoke the app → attest() checks the LOCAL chain → authorized. control.rs
      RPCs (attest_bootstrap/propose/cosign/submit/status) + State.attest + main.rs CLI (5 attest-*
      verbs via cmd_attest/query_unix_params) + AttestStore in State. Workspace builds; noded 13;
      clippy/fmt clean; CLI verbs present. DEFERRED (P3-2b, optional): tag::ATTEST chain GOSSIP so a
      NON-collector node also sees is_authorized (not needed when the producer is the collector).
- [x] P4 ROLL + LIVE SMOKE DONE 2026-07-14. Gate 🟢 (A-G 8/8, 814s). STAGGERED roll (additive — gov
      unification wire-identical, tag::ATTEST gossip deferred) of all 4 Hetzner nodes: active,
      NRestarts=0, 4-node census, 0 panics. GOVERNANCE UNIFICATION VALIDATED LIVE: `zeph gov` folds
      the deployed chain (1-of-1 seq 6) correctly under the refactored Quorum code. LIVE ATTESTATION
      SMOKE: published an attest()-calling demo app on zeph (cid efb1d194…), bootstrapped a 2-of-2
      quorum {zeph, zeph2}, proposed "authorized" on zeph → cosigned on zeph2 (CROSS-NODE) → submitted
      2-of-2 → attest-status authorized:true → invoked the app with "authorized" → committed [01]
      (attest=1); with an unauthorized statement → committed [00] (attest=0). **ATTESTATION (#9)
      COMPLETE + LIVE.** Deferred enhancements: tag::ATTEST chain gossip (non-collector nodes);
      member-side policy-program auto-signing (the discrete-fact automated niche).
- [x] P3-2b GLOBAL propagation DONE + LIVE 2026-07-14 (user: "it should be global attestation.
      governance already is, if it is not, it is a downgrade and governance is broken"). Made
      attestation cross-node like governance — NOT via tag::ATTEST gossip but via the SAME publish/pull
      anti-entropy `GovernanceChainStore` uses, per program: `AttestStore` gained `obj`+`routing`+
      `membership`; `publish(program_cid)` = `obj.publish_system(chain)` + `announce_app("\u{1}attest-
      chain-<hex cid>", content_cid, seq+1)` (called after bootstrap/submit); `sync(program_cid)` pulls
      each census peer via `resolve_app`+`obj.get`+File-deref+`QuorumChain::decode`, adopting the longest
      VALID chain that shares the genesis; `is_authorized`/`attest()` sync-FIRST → any node answers.
      `open()` +obj+routing; `set_membership` wired in main.rs. Review (feature-dev:code-reviewer)
      CAUGHT a real bug pre-roll: `local_genesis` snapshotted once before the peer loop (gov-safe since
      its genesis is fixed at open(); UNSAFE here — per-program genesis starts None) → a later higher-seq
      peer with a DIFFERENT genesis could overwrite an in-loop adoption; FIXED by re-deriving genesis per
      iteration. Gate --quick 🟢 (fmt+clippy -D+workspace tests; additive → no A-G needed). STAGGERED
      roll of all 4 Hetzner nodes: active, NRestarts=0, 0 panics. LIVE GLOBAL SMOKE: collected a 2-of-2
      {zeph,zeph2} "authorized" attestation ENTIRELY on zeph (submit→publish), then attest-status on
      NON-member/NON-collector zeph3 AND zeph4 → authorized:true (~6s, they pulled zeph's chain);
      unauthorized statement → false. Deferred: owner-signature genesis authentication (trust-on-fetch
      today, honest-fleet-correct; hardening mirrors registry read verification).
- [x] P3-2c OWNER-SIGNED GENESIS DONE + LIVE 2026-07-14 (user: "yes, build it now" — no downgrade vs gov).
      Close the trust-on-fetch genesis hole so attestation's genesis trust root = as strong as
      governance's (gov pins genesis from local CONFIG; attestation pins it to the program's REGISTERED
      OWNER, reusing the owner-sig-verified registry). DESIGN: the genesis quorum is signed by the app
      OWNER bound to program_cid (`Quorum::owner_signing_bytes(program_cid)`, domain
      b"craftec/attest-genesis/1"); propagate an `AttestedChain{owner, owner_sig, chain}` envelope keyed
      by (owner, program_cid); a fetching node adopts only if `owner_sig` verifies AND owner == the
      registry-resolved program owner. The owner fed to `attest()` is SERVER-resolved from the
      authenticated registry (rpc_invoke by-name → publisher = owner), NEVER caller-supplied (else an
      invoker self-authorizes); raw --wasm invoke or remote invoke → owner None → attest UNAVAILABLE (safe).
      BATCHES: (1) com/attestation.rs AttestedChain+owner_signing_bytes+verify; (2) com/transition.rs
      program_owner + AttestBackend::attest(owner,program,stmt) + host fn -1 when owner None; (3)
      com/invoke.rs invoke(req,caller,owner)+with_program_owner; (4) noded/attest.rs (owner,program) key
      + envelope + owner-signed genesis + verify-on-adopt; (5) control rpc_invoke threads publisher-owner,
      attest-status --owner, bootstrap owner=self; (6) gate+review+roll+smoke (real deployed owned app +
      FORGERY negative: a peer publishing a fake genesis under a wrong owner is rejected).
      OUTCOME: all 6 batches built; com 76 tests + noded attest 3 tests green (added owner_signed_genesis
      trust-root test + the invoker-no-owner→UNAVAILABLE case + a persist-reload test). Gate --quick 🟢.
      SECURITY REVIEW (feature-dev:code-reviewer, adversarial): NO bypass found (≥80) — domain-separated
      (attest-genesis vs attest), program-cid-bound (no cross-program replay), verify checks owner-sig
      AND fold, owner server-resolved never caller-supplied, sync rejects wrong-owner/forged, no path
      traversal; traced the residual assumption (registry.resolve re-verifies owner sig bound to the
      queried owner) into headreg.rs and CONFIRMED it holds at local + remote boundaries. STAGGERED roll
      of all 4 nodes (wire-incompatible with P3-2b attest — new head-name + AttestedChain payload — but
      attestation is ISOLATED/non-load-bearing + governance untouched → staggered safe; old attest chains
      just fail to load): active, 0 restarts, 0 panics. LIVE SMOKES (all pass): (C) owner-signed GLOBAL —
      collected a 2-of-2 authorization on zeph, non-collector zeph3 AND zeph4 pulled zeph's OWNER-SIGNED
      chain, verified the sig, returned authorized:true (false for unauth). (E) anti-forgery owner-binding
      — zeph2 (a different owner) published a valid self-signed quorum for program Q; on zeph3, Q under
      owner=zeph → FALSE (no cross-owner forgery), under owner=zeph2 → TRUE. (D) full invoke-by-name on
      NON-collector zeph3: `<zeph2>/attestapp` input "authorized" → committed 01, "nope" → 00, and RAW
      --wasm (no registry-resolved owner) → committed ff (UNAVAILABLE) proving an invoker can't
      self-authorize. **ATTESTATION GENESIS TRUST ROOT = as strong as governance's now.** ORTHOGONAL
      finding (NOT this change): `deploy` on zeph fails "table heads has no column named sig" while zeph2
      succeeds → a per-shard registry sig-column migration GAP (registry read-verification, 07-12), not
      attestation; flagged in STATE_AND_ROADMAP §3.2. Nothing left deferred for attestation.
OPEN Qs (from the design): what members attest to = arbitrary app statement (the one genuinely new
piece over gov.rs, DONE in P1's AttestAction::Statement); liveness policy for a closed quorum
(timeout/fallback); the member-signing policy (P3).

# VERIFICATION PRIMITIVE (K6 / task #8) — building (2026-07-13)
Build the automated-consistency verification primitive per VERIFICATION_DESIGN.md (re-cut to
consistency-only): "is this the correct output of this deterministic program?" answered by ANY node
re-running the pure function and comparing; app-declared threshold k (1/2/3); interchangeable open
verifiers. Distinct from attestation (authority, task #9). Rides the BUILT runtime: `run_program`
(returns the committed output), `CapabilityGrant::deterministic()` (fail-safe, reproducible), the
`Random`-template for a new reserved host fn.

Phases (VERIFICATION_DESIGN §9 build order; each: build+test+gate+commit):
- [x] P1 Verdict + local re-verify (offline core) DONE 2026-07-13. New `crates/com/src/verification.rs`:
      `VerifyRequest { program_cid, prev_state, request, now, claimed_output }` (+ `request_hash`);
      `Verdict { verifier, program_cid, request_hash, output_hash, agree, signature }` (+ signing
      bytes / sign / verify, mirroring registry's HeadSubmission pattern); `verify_locally(runtime,
      identity, req, wasm, fuel) -> Verdict` re-runs under the DETERMINISTIC grant + same `now`
      (reproducible) and signs agree = (rerun_output == claimed_output). No board, no host-fn, no
      capability change yet. Tests: honest→agree, tampered→disagree, trap→disagree, sig verifies,
      cross-node determinism (two identities, same request → same agree).
- [x] P2 `Verify` capability + host ABI DONE 2026-07-13. `Capability::Verify` (app `full()` profile
      + `CapabilityGrant::verifier()` re-run grant); `TransitionCtx.verify_mode` + `in_verify_mode()`;
      `verify(func,in,claimed)->i32` host fn: `2`=INERT (verify_mode, the recursion guard), `-1`=
      UNAVAILABLE (no board yet / malformed), `1`/`0` reserved for the wired board. verify_locally
      now re-runs under `verifier()`+verify_mode so a single-module program (pure f + verify-importing
      orchestration) re-runs without recursion. KEY RULE: the pure `f` must NEVER call verify (only
      orchestration does, and orchestration is not re-run) — else producer (real verify) and verifier
      (inert) outputs diverge. 4 P2 tests: link-gate (verify import fails under deterministic, links
      under verifier), pure-f-of-a-verify-importing-module still verifies, inert-on-rerun vs
      unavailable-to-producer, verifier() grant membership.
- [x] P3 The request board DONE 2026-07-13 (LOCAL semantics; gossip wiring deferred to P5 integ).
      `VerifierSet{Open|Whitelist}` + `VerifyPolicy{k,set}` + `PostedRequest{producer,req,policy}` +
      `Board` (append-only dedup'd HashMaps: request_hash→request, request_hash→verifier→verdict).
      `post_request` (idempotent), `post_verdict` (dedup by (rh,verifier)), `grabbable_by(node)`
      (eligible + not-producer + not-already-verified + not-satisfied), `satisfied`/`valid_agreements`
      (k DISTINCT verdicts each: valid sig + matches (rh,oh) + agree + eligible + NOT producer). Board
      is DUMB — accepts anything on post, ALL correctness paid back by readers, so a gossiped/merged
      board (a union) is safe. 6 tests: collect-to-k, dedup+ignore-disagree, reject self-verify +
      invalid-on-read, whitelist-only, grabbable exclusions, idempotent post.
- [x] P4 Cooldown scheduler + collection certificate DONE 2026-07-13. `Verifier{node,cooldown_ms,
      last_verified_ms}`: `ready(now)`, `select(board,now)` (None if on cooldown; else the grabbable
      request minimising rendezvous `blake3(node‖request_hash)` — spreads load + a producer can't
      steer which node grabs, since the pick is keyed on the verifier's OWN id), `mark_verified(now)`.
      `Board::collected(posted) -> Option<Vec<Verdict>>` = the ≥k valid-verdict certificate (refactored
      valid_agreements onto a shared `valid_verdicts`). Cooldown does 3 jobs: spread load, force k
      DISTINCT verifiers, disrupt collusion. 4 tests: cooldown/readiness gating, single-verifier can't
      meet k=3 (dedup), 5-node convergence to k distinct + certificate, collected-only-when-satisfied.
      (Policy schema `VerifyPolicy{k,set}` landed in P3.)
- [x] P5a First consumer + local end-to-end DONE 2026-07-13. `produce()` (producer side: run the
      pure f under verifier()+verify_mode → package a VerifyRequest byte-identical to what verifiers
      reproduce). COUNTER_WAT (consistency-critical shared counter, pure f=state+input). E2E test:
      produce → post k=3 open → 5 cooldown-scheduled verifiers verify_locally (real re-run) → collect
      the k=3 certificate; + a forged transition (claims 9, f yields 8) never collects. Ties P1+P3+P4.
- [x] P5b-1 Board CRDT (gossip payload) DONE 2026-07-13. `BoardSnapshot{requests,verdicts}` +
      `Board::snapshot()` + `Board::merge(snap)` (CRDT UNION via the idempotent post_* — commutative,
      idempotent, convergent). Malicious-snapshot-safe (readers re-check → can't fabricate a cert).
      4 tests: postcard round-trip, idempotent union, commutative, forged-snapshot-can't-fabricate.
- [ ] P5b-2 noded transport wiring. DESIGN (grounded in the noded patterns brief 2026-07-13):
      the Board is a union-merge CRDT (built), so distribution = the MEMBERSHIP epidemic-gossip
      pattern + the REGISTRY serve/mux shape. Components:
      1. WIRE: `transport::tag::BOARD = 8` (append-only tag — an old node without the handler drops
         board msgs → ADDITIVE, so a normal STAGGERED roll, NOT wire-incompatible). Payload =
         `BoardSnapshot` postcard, fire-and-forget push (like membership epidemic_push).
      2. noded `BoardService`: `Arc<RwLock<com::Board>>` + membership handle (set_membership setter,
         copy governance) + identity + TransitionRuntime + obj (fetch wasm by program_cid).
         `serve(mpsc::Receiver<TaggedStream>)` → decode snapshot → `board.merge()` (copy headreg
         serve). Gossip loop (interval ~5s): snapshot → push to a census() fanout. Verifier loop:
         grabbable (Verifier scheduler + cooldown) → obj.get(program_cid) → verify_locally →
         post_verdict → gossip. Wire in main.rs (tag channel + serve spawn + set_membership).
      3. `VerifyBackend` trait (com, beside AppBackend) + `TransitionCtx.verify_backend` field; the
         `verify()` host fn (currently -1) posts a PostedRequest + awaits `Board::collected` w/
         timeout → 1 verified / 0 rejected. Inject exactly like AppBackend (InvokeService + main.rs).
      ROLL: additive → staggered (not simultaneous). Key refs: registry_net.rs, headreg.rs:1535
      serve, governance.rs:251 tick + :83 set_membership, membership epidemic_push, transport::tag,
      invoke.rs:74 backend injection, main.rs:1266/1520.
  - [x] P5b-2a wire tag + BoardService DONE 2026-07-13. `transport::tag::BOARD=8` (additive). New
        `crates/noded/src/board.rs` `BoardService`: `serve()` (read snapshot → `Board::merge`, per-
        stream spawn, mirrors registry serve), gossip loop (5s, fire-and-forget push to a census()
        FANOUT=3), verifier loop (2s: `Verifier::select` off cooldown → `fetch_program` (obj.get,
        deref File manifest) → `verify_locally` → `post_verdict` → gossip), `set_membership`,
        `post_request` (producer entry, wired in 2b). Wired in main.rs (channel+handler before
        server.serve, serve spawn, set_membership). 1 integration test: publish counter → post
        (different producer) → `verify_once` fetches+re-runs+posts a valid verdict → k=1 satisfied.
        noded builds + 10 unit + 4 integration tests pass; clippy/fmt clean. NOTE: cross-node GOSSIP
        over real transport is by-construction (mirrors registry_net request_tagged+serve) + wired;
        it'll be LIVE-smoke-tested after the roll (a 2-node transport harness wasn't stood up in-unit).
  - [x] P5b-2b VerifyBackend + verify() producer path DONE 2026-07-13. com: `VerifyBackend` trait
        (beside AppBackend); `TransitionCtx.{program_cid, verify_backend}` + `with_program`/
        `with_verify_backend` builders; the `verify()` host fn now (not verify_mode, backend present)
        builds a VerifyRequest {program_cid=ctx.program_cid, func/request/claimed from guest,
        prev_state/now from ctx} and returns `i32::from(vb.verify(req).await)` (1/0), still -1 w/o a
        backend; invoke.rs threads program_cid=Cid::of(wasm) + the backend into the ctx. noded:
        `impl VerifyBackend for BoardService` = post (k=1 Open, this node producer) + gossip + poll
        `collected` w/ 30s timeout; main.rs constructs board_service BEFORE com_service + passes
        Some(board) as the verify backend. Tests: com producer-path (mock backend → 1/0/-1), noded
        VerifyBackend collects another node's pre-injected verdict → true. workspace builds; com 64 +
        noded 11 + integration pass; clippy/fmt clean.
  - [x] P5 ROLL + LIVE SMOKE DONE 2026-07-13. Gate 🟢 (A-G 8/8, 769s). STAGGERED roll (additive
        tag::BOARD) of all 4 Hetzner nodes — each active, NRestarts=0, 4-node census, 0 panics.
        LIVE cross-node smoke: published a verify()-calling demo app on zeph (cid 00209e60…),
        invoked it → the app's verify("f",[x],[x*2]) posted to the board → gossiped → ANOTHER node
        re-ran f + posted a verdict → zeph collected the cert → verify()→1 → app committed [01]
        (VERIFIED). Repeatable (2nd invoke also 01). Since a producer can't self-verify, [01] proves
        a different node confirmed it. **VERIFICATION (#8) COMPLETE + LIVE.**

NOTE (design): SYBIL is the honest ceiling (per-node cooldown binds one node, not a fleet) — name it,
don't claim to defend it (stake/reputation weighting is deferred). NO self-verification (a DIFFERENT
node must re-run). Determinism boundary: the re-run reads only explicit inputs (prev_state, request,
now) — `now` must be carried in the request, never host wall-time.

---

# REGISTRY READ VERIFICATION — P1–P4 DONE + review-fixed; P5 (roll) PENDING USER GO-AHEAD (2026-07-12)
Closed the last registry correctness/security gap: reads WERE trust-on-announce. The write path validated
the owner sig (RegistryState::apply → sub.verify()) then DISCARDED it — HeadEntry had NO signature, so
replication-merge + resolve accepted whatever they were given (a malicious writer/replica could inject a
FORGED head and it propagated + was trusted). Fix: the owner sig TRAVELS with the head and is re-verified
at every trust boundary.

Design (built): HeadEntry gained `signature: Vec<u8>` (owner ed25519 over
head_signing_bytes(owner,name,cid,version) — the same bytes HeadSubmission signs; `HeadEntry::verify()`
re-checks it). apply() stores sub.signature. The SQL heads table gained a `sig BLOB` col (stored on write,
returned on read). Native + WASM fixture HeadEntry both carry sig (registry.wasm rebuilt → wasm_registry_
matches_native still byte-identical).

** REVIEW (xhigh) CAUGHT TWO GAPS that defeated the feature — now FIXED + tested: **
1. CRITICAL: verify() was added to `RegistryState::merge()` (com), but the node's REAL replication path is
   `sql_merge` (headreg) → `sql_upsert`, which BYPASSED it. A malicious PushState with a high-version
   forged entry would version-guard-OVERWRITE the honest head, then fail read-verify → the name is ERASED
   (remote DoS). FIX: `sql_merge` verifies each entry at ingress (drops forged/unsigned). Test:
   `sql_merge_drops_a_forged_remote_head_and_preserves_the_honest_one` (injects via decode, the real wire
   vector).
2. IMPORTANT: the cross-node resolve RPC `RegistryResp::Resolved(Option<(cid,version)>)` carried NO sig →
   a resolver trusted a remote replica's answer unverified (the common non-holder read path). FIX:
   `Resolved(Option<HeadEntry>)` carries the signed entry; the asking node `entry.verify()`s before
   trusting and falls through to the next replica on failure. (sql_resolve/resolve_local now return
   HeadEntry.)

** MIGRATION — DECIDED: idempotent ALTER, no manual wipe. ** `ADD_SIG_COLUMN` = `ALTER TABLE heads ADD
COLUMN sig BLOB NOT NULL DEFAULT X''`, run after CREATE_HEADS in `shard_db` (write path); duplicate-column
error on fresh DBs is swallowed. Legacy rows get a 0-byte sig → fail verify() → unresolvable until the
owner re-registers (correct: an unsigned legacy head can't be trusted). Lets the fleet upgrade WITHOUT
wiping regshards. Test: `add_sig_column_migrates_a_legacy_shard_db` (validates the ALTER on real CraftSql).
Known minor (accepted): read-only replicas of a legacy shard open via `shard_db_existing` (no migration) →
SELECT sig errors → return None — impact nil (legacy heads can't verify anyway; a write/push migrates it).

Phases:
- [x] P1 com/registry: HeadEntry+signature; HeadEntry::verify(); apply() stores sig; merge()/merge_entries()
      reject unverifiable; forged+unsigned merge-rejection tests. WASM fixture synced + rebuilt.
- [x] P2 SQL persistence (headreg): heads `sig` col + idempotent ALTER migration; sql_upsert stores it;
      sql_resolve/sql_state carry it; advance_local persists sub.signature.
- [x] P3 verify on READ: sql_resolve re-verifies (local); sql_merge re-verifies (replication ingress);
      resolve RPC caller re-verifies the returned HeadEntry (cross-node). Three trust boundaries closed.
- [x] P4 tests: resolve_drops_a_row (local read), sql_merge_drops_a_forged_remote_head (replication),
      add_sig_column_migrates_a_legacy_shard_db (migration). All green; fmt+clippy clean; workspace builds.
- [x] P5 gate GREEN (fmt+clippy+workspace tests + A-H harness 8/8, 761s) → SIMULTANEOUS wire-incompatible
      roll DONE + LIVE-VALIDATED 2026-07-12 ~14:04. Wire-incompatible (RegistryState.HeadEntry gained a
      field AND RegistryResp::Resolved changed shape → old binaries can't decode) → stop-all-4/start-all-4.
      Reconverged in ~80s: 4-node census (each node distinct_alive_peers=3), sub-ms SWIM RTT, 0 panics, 0
      wire-decode errors, NRestarts=0. END-TO-END LIVE CHECK: wrote a fresh DB-root head on `zeph`
      (sql-exec ns=rollcheck, valid owner sig) → cross-node read from `zeph2` (--owner zeph) returned
      `post-roll-readverify` — proves the new resolve RPC (Resolved carries the signed HeadEntry) + verify
      path serves a validly-signed head across nodes. No regshards wipe (ALTER migrated legacy rows in
      place). Efficiency nits deferred (acceptable): sql_state + advance_local each re-verify already-trusted
      rows (belt-and-suspenders; source-filters legacy rows).
      **CAVEAT:** the Mac governor node still runs the OLD (pre-read-verify) binary — it's stopped/disabled,
      so no interop today, but it MUST get the new binary (build release locally → install ~/.zeph/zeph)
      before it's next spun up for a governance op, else it's wire-incompatible with the fleet.

# DIGEST/SAMPLED GOSSIP [S1] — PLAN (2026-07-12)
Make membership gossip O(Δ), not O(N)/round — the last remaining membership scale ceiling (roadmap;
pairs with the now-done SWIM). Today `epidemic_push` sends the FULL member map (with ever-fresh
last_heard) to 3 peers every 5s → O(N) per node/round, O(N²)/round cluster-wide. Can't just "send only
changes" because last_heard bumps every round for every member.

KEY MOVE (leverages the now-live SWIM): DECOUPLE LIVENESS FROM last_heard. A member is Alive unless a
Suspect/Dead gossip says otherwise. So census liveness = SWIM STATE (in census iff state != Dead), NOT
the last_heard-within-CENSUS_TTL check. last_heard demoted to a coarse backstop (forget after
dead_retention). Then gossip only carries CHANGES — join / addr / incarnation-bump(refute) / Suspect /
Dead — which are RARE.

MECHANISM (Scuttlebutt-style digest + delta):
- Steady state: peers exchange a compact DIGEST = hash of the sorted (id, incarnation, state) set
  (EXCLUDES last_heard → stable when membership is stable). Hashes match ⇒ in sync, done (O(1) msg).
- A change ⇒ gossip the DELTA (changed entries) eagerly to N peers (like today's epidemic, delta-only).
- Digest MISMATCH (missed a delta) ⇒ reconcile: exchange per-member versions, send only the entries the
  peer is behind on — O(divergence), not O(N)/round.
Result: steady-state O(1), churn O(Δ), full O(N) reconcile ONLY on real divergence.

WIRE: new Digest{hash}/DigestReq + a MemberDelta (or reuse MemberSync for the reconcile payload).
Positional postcard ⇒ wire-incompatible ⇒ SIMULTANEOUS roll (like SWIM/mux).

** KEY DECISION TO CONFIRM (P1 is the crux): census liveness moves from last_heard-TTL to SWIM-state. **
Sound because SWIM active detection is LIVE (Dead converges via delta gossip + TTL backstop), but it IS a
semantic change to the consistency-critical census (the registry election runs over it). The digest
reconciliation is the eventual-consistency backstop.

Phases (each: build+test+commit):
- [x] P1 census liveness from SWIM state DONE (user green-lit the crux decision). `census()` (the
      registry-election census) is now `state != Dead && last_heard < cfg.dead_retention` (was
      `!= Dead && within CENSUS_TTL 120s`) — a member leaves ONLY when gossiped Dead; last_heard is a
      COARSE silent-member backstop (dead_retention, default 600s). Removed the `CENSUS_TTL_MS` const.
      Reworked `census_excludes_stale...` test → proves an Alive member 200s stale (past the old 120s
      TTL) STAYS in, a member silent beyond the backstop is forgotten, a fresh-but-Dead member is out.
      membership 13/13, clippy -D clean, workspace builds. BEHAVIOURALLY INERT under current gossip
      (last_heard stays < 600s, so the census result is unchanged) — no regression, and NO scaling win
      YET: this only ENABLES O(Δ). `liveness_census()` (repair) left on its 30s TTL for now — coupled to
      the gossip change, revisit in P2. The O(Δ) win materialises in P2 (delta) + P3 (digest).
- [~] P2 delta gossip — IMPLEMENTED, re-validating. `Views.dirty: HashMap<NodeId,u8>` (id→remaining
      re-pushes); marked at every delta-worthy site (merge_one returns changed→dirty; suspect; mark_dead;
      refute; new-member inserts). `epidemic_push` now sends ONLY dirty members, one-way, skips when
      empty (the O(N)/round→O(Δ) win); the 30s shuffle stays full-map = the reconciliation backstop.
      merge_one returns bool (delta-worthy = new/liveness change; last_heard-only bump = false, so
      freshness ticks DON'T gossip). Unit test delta_gossip_dirties_only_real_changes. census/liveness
      unchanged (P1).
      REGRESSION FOUND (scenario B): join-wave census-20 hit 35.5s > 30s bar (push-once + one-way lost
      the old full-map flood; tail fell back to the 30s shuffle). Scenario H (death) was FINE (46s<90s,
      no false-pos, no storm). FIX: SWIM limited retransmission — GOSSIP_REPEATS=6, dirty is now a
      per-member repeat COUNT re-sent each round until 0 (saturates via fanout^repeats + the cascade).
      membership 14/14, clippy clean. FIXED + VALIDATED: scenario B census-20 back to 16.4s (was 35.5s
      regressed; bar 30s). Scenario H death still fine. P2 DONE — the 5s hot-path gossip is now O(Δ)
      (nothing sent in steady state); the 30s shuffle is still the O(N) full-map backstop (P3 kills that).
      NOTE: re-confirm scenario H with the final re-push code at the P5 gate (it re-runs the full A-H suite).
- [~] P3 digest hash + reconciliation — IMPLEMENTED, re-validating. wire: Digest{hash:[u8;32]} msg
      (tag 0x010A) — WIRE-INCOMPATIBLE. members_digest() = blake3 of sorted (id,inc,state) over NON-Dead
      members (excludes last_heard + Dead → stable when the census is stable). digest_round() (folded
      into the 30s shuffle tick): send my hash to a random active peer; on MISMATCH → sync_members_with
      (full reconcile, bidirectional). Digest handler replies own hash. Shuffle + ShuffleReply stop
      carrying the full member map (members: vec![]) → the shuffle is now HyParView passive-view only;
      the digest is the O(1) backstop (full sync only on mismatch). Unit test members_digest_reflects_
      census_not_freshness.
      Scenario B census-20 = 13.4s (FASTER than P2 — the redundant shuffle carriage gone) BUT drained=FALSE
      (repair storm): ROOT CAUSE = the P1-flagged liveness_census coupling. It still used last_heard<30s,
      but P2/P3 stopped refreshing last_heard for un-probed members (only ~active_size=5 of 20 probed) →
      it dropped LIVE holders → over-repair → queues churned. FIX: liveness_census + indirect_probe helper
      filter are now SWIM-state-based (live = state==Alive; excludes Suspect too so repair reacts fast),
      NOT last_heard. Removed LIVENESS_TTL_MS. Reworked its test (liveness_census_is_swim_alive_only).
      membership 15/15, clippy clean, workspace builds. FIXED + VALIDATED: scenario B census-20 = 8.3s,
      DRAINED=TRUE, max_job=141ms, at_risk healthy — the repair storm is gone. P3 DONE. The full gossip
      is now O(1) steady-state (5s epidemic sends nothing when nothing changed; 30s digest = two hashes
      when in sync), O(Δ) under churn, O(N) reconcile only on a real digest mismatch. Confirming scenario
      H (mass death) with the P3 liveness change, then P4/P5. Scenario H CONFIRMED: death census 50s,
      no false-pos, no storm, drained=true. === P1+P2+P3 = the full O(N)→O(Δ)/O(1) gossip mechanism BUILT
      + harness-validated (B + H). Remaining: P4 a dedicated missed-delta→digest-reconcile integration
      test (nice-to-have; the unit tests + B/H cover the core), P5 full gate → SIMULTANEOUS wire-
      incompatible roll (Digest msg). ===
- [x] P4 tests — covered by the unit tests (delta-tracking invariant, digest census-not-freshness,
      state-based liveness) + the A-H harness (B mass-rejoin 8.3s drained, H mass-death 50s drained). A
      dedicated missed-delta→reconcile integration test remains a nice-to-have, not a gap.
- [x] P5 SIMULTANEOUS ROLL DONE + LIVE-VALIDATED (2026-07-12 ~07:26, user go). Full deploy/gate.sh =
      🟢 (fmt+clippy+workspace 0-fail + A-H 8/8 incl. scenario_h_mass_death). Backup predigest-20260712-0724
      → rsync (wire+membership) → build 1m17s → install → SIMULTANEOUS flip (stop all 4/start all 4 @
      07:26:51, wire-incompatible: Digest msg + reshaped shuffle). Reconverged <60s: all active, NR=0,
      peers=3, ZERO wire-decode errors (no version split), eligible=4. LIVE: killed zeph4 → 3 survivors
      logged DEAD in ~35s, eligible 4→3; restarted → rejoined, eligible→4. 0 panics/wire-errors across
      the roll. Rollback = zeph.bak-predigest-20260712-0724.
=== DIGEST/SAMPLED GOSSIP [S1] COMPLETE + LIVE. Membership gossip is now O(1) steady-state / O(Δ) churn /
O(N) reconcile-only-on-mismatch — the last membership scale ceiling is gone. ===

# SWIM ACTIVE DEATH DETECTION [K10] — PLAN (2026-07-12)
Roadmap's one real robustness gap. Today: direct probe fails 3× → `mark_dead` LOCAL only → the dead
node ages out of everyone's census by TTL (~30–120s). Add ACTIVE death detection: SWIM Suspect/Dead
states + incarnation numbers, indirect PING-REQ (rule out local blips), epidemic dissemination of death
→ deaths converge in ~1–2s.

KEY DESIGN: `MemberSync` ALREADY gossips the full member map with a union-merge (`merge_members`/
`merge_one`). Put `incarnation`+`state` INTO the member map + make the merge SWIM-ordered → Suspect/Dead
ride the EXISTING epidemic diffusion (~seconds/hop). No separate death-gossip channel.
- State: per member `incarnation:u64` (only the member bumps it, to refute) + `state∈{Alive,Suspect,Dead}`.
- Merge: incoming wins iff inc>existing.inc OR (inc==existing.inc AND rank(state)>rank(existing)), where
  Alive=0<Suspect=1<Dead=2. Higher-incarnation Alive REFUTES a Suspect/Dead. last_heard_ms = max (backstop).
DETECTION: direct ping fail (2 tries) → indirect PING-REQ to K=3 alive members → all fail → SUSPECT
@current inc + local deadline. Deadline passes unrefuted → DEAD (gossips). Refute: a node seeing
Suspect/Dead about ITSELF bumps inc to max(seen,own)+1 + re-asserts Alive.
census(): exclude Dead immediately; keep Alive+Suspect (Suspect stays for election-consistency; only Dead,
which converges fast, is removed). Keep CENSUS_TTL_MS backstop.
WIRE COMPAT: MemberEntry gains fields + new PingReq/PingReqAck → postcard positional → OLD/NEW can't
decode each other → SIMULTANEOUS roll (the `zeph-fleet-deploy` mux-migration mode). Gate 🟢 + user go.

Phases (each: build+test+commit before next):
- [x] P1 wire+state foundation DONE. wire: MemberEntry{+incarnation:u64,+state:u8}; PingReq{target_id,
      target_addr}/PingReqAck{target_id,alive} msgs (tags 0x0108/0x0109) + enum/type_tag/encode/decode.
      membership: MemberState{Alive,Suspect,Dead}+rank/to_u8/from_u8; Member{+incarnation,+state}+alive()
      ctor; Membership.self_incarnation:AtomicU64(0); SWIM merge_one ((inc,rank) liveness merge, higher
      inc refutes, last_heard max independent); census + liveness_census EXCLUDE state==Dead; probe-success
      + note_heard PRESERVE inc/state (no clobber; direct ack clears own Suspect); refresh_self stamps
      self_incarnation. wire 5/5 + membership 9/9 green, workspace builds, clippy -D clean. NO behaviour
      change yet (all states Alive@0 until P2/P3 wire transitions).
- [x] P2 indirect PING-REQ DONE (mechanism). Config +indirect_probes(3) +suspect_timeout(15s). handle_message
      serves PingReq (ping target, reply PingReqAck{alive}). indirect_probe(target,addr): K random alive
      members (via converged map, LIVENESS_TTL, ≠self/target), CONCURRENT join_all, true on first alive.
      Death path: direct threshold_hit → indirect_probe → alive⇒rescue (reset failures + note_heard) else
      → mark_dead. Compiles workspace + clippy -D clean. Full multi-node rescue test → P4.
- [x] P3 suspect→dead lifecycle + refutation DONE. Member +suspect_since_ms (LOCAL, not gossiped) +
      set_liveness() (maintains the Suspect clock). Death path: direct+indirect fail → suspect(id)
      (state=Suspect@cur inc, only from Alive). promote_suspects() (called each probe_round) escalates
      Suspect past cfg.suspect_timeout → mark_dead. mark_dead now sets the converged record state=Dead
      @cur inc (GOSSIPS + census-excluded immediately, works even if not in active view) + keeps the
      dashboard tombstone/promotion. REFUTATION: merge_members, a self-entry that's Suspect/Dead →
      bump self_incarnation to max(seen,own)+1 + refresh_self (Alive@higher overrides everywhere) — this
      also handles a RESTARTED node rejoining past its own stale Dead@0 (it refutes on first MemberSync).
      merge_one uses set_liveness. 3 new unit tests (merge SWIM ordering+refutation, freshness⊥liveness,
      state u8 roundtrip/rank) — membership 8/8 green, wire 5/5, workspace builds, fmt+clippy -D clean.
      Timing: 3 direct fails (~15s) → indirect (~3s) → Suspect → suspect_timeout (15s) → Dead, then Dead
      GOSSIPS in ~seconds + excludes immediately (vs old ~30-120s TTL aging). Tuning knob for faster
      detection: lower probe_failures→1 + shorter suspect_timeout (indirect probe guards false positives).
- [x] P4 tests DONE (deterministic logic). 4 new membership tests (12/12 total): (1)
      suspect_promotes_to_dead_and_drops_from_census — Suspect stays counted, promote_suspects after
      window → Dead → excluded from BOTH censuses immediately; (2) self_suspicion_is_refuted_by_
      incarnation_bump — self Dead@5 gossiped → self_incarnation→6 + own record Alive@6; (3)
      restarted_node_refutes_own_stale_dead_and_rejoins — fresh node (inc0) past its own Dead@0 →
      Alive@1; (4) ping_req_handler_replies_with_probe_result — PingReq → PingReqAck{alive:false} for an
      unreachable target (handler wiring). fmt+clippy -D clean. DEFERRED to P5 live validation (too
      flaky/needs partition control for a unit test): indirect-probe RESCUE (target reachable by helper
      not directly) + full-cluster Dead convergence over real sockets — validated by killing a fleet node.
- [x] P5 SIMULTANEOUS ROLL DONE + LIVE-VALIDATED (2026-07-12 ~02:21, user "p5"). Full deploy/gate.sh =
      🟢 GATE PASSED (fmt+clippy+workspace 0-fail — obj flake didn't recur — + A-G 7/7 incl. C kill-holder
      & D/F restart-rejoin which exercise death detection). Backup zeph.bak-swim-20260712-0217 → rsync
      crates/ (wire+membership+headreg bench) → build on box (release 1m18s, 0 err) → install → SIMULTANEOUS
      flip (stop all 4, start all 4 @ 02:21:29, ~instant). Reconverged <60s: all 4 active, NR=0, peers=3,
      ZERO wire-decode errors (no bad-frame/UnknownType → all on the new MemberEntry, no version split).
      LIVE SWIM VALIDATION: killed zeph4 → all 3 survivors logged SUSPECT then DEAD within ~35s, alive_peers
      2, census eligible 4→3 (Dead excluded immediately, not TTL-aged). Restarted zeph4 → rejoined in 30s,
      all peers=3, eligible back to 4 (refutation past its stale Dead works live). 0 panics/wire-errors/
      corruption across the whole roll. Rollback = zeph.bak-swim-* (box). Mac node stays stopped (governor,
      on-demand). === SWIM ACTIVE DEATH DETECTION [K10] COMPLETE + LIVE ON THE FLEET. ===
Notes: suspect_timeout ~probe_interval×3 (~15s), must exceed a couple gossip hops so a refute can arrive.
Decide in P2: keep probe_failures as the direct threshold before indirect, or drop to 1 + rely on indirect.

P4 FOLLOW-UPS (workspace test + adversarial review):
- Full-workspace test caught five_workers_membership_join_and_death (two_workers.rs): my defaults made
  detection SLOWER (probe_failures=3 → indirect → 15s suspect ≈ 33s+) so it blew the 60s bar under
  cross-binary contention. Also found a GAP: status "down" = local `dead` tombstone only, but a survivor
  that learns C is Dead via GOSSIP set member state=Dead without a tombstone → never showed "down". FIXED:
  (a) faster defaults probe_failures 3→2, suspect_timeout 15s→6s (indirect probe is the false-positive
  guard, so suspect sooner; total ~21s ≈ old direct-only, + fast gossip convergence); (b) merge_members
  now mark_dead's any peer gossip-reports Dead that we still hold as an ACTIVE link (Box::pin — breaks the
  static async cycle merge→mark_dead→try_promote→add_active→sync→merge; idempotent since mark_dead drops
  it from active). census/liveness already exclude Dead.
- Adversarial review (feature-dev:code-reviewer): CLEARED merge convergence/lattice, refutation lock-order,
  rejoin, indirect_probe, census, wire round-trip, and ALL lock/await sites (no held-lock-across-await).
  Found + FIXED 2 real bugs I introduced: (1) CRIT — probe-success recovery from Suspect did `m.state=Alive`
  raw, bypassing set_liveness → stale suspect_since → a later re-suspect promoted to Dead with ~0 grace;
  now uses set_liveness (clears the clock). Regression test set_liveness_maintains_the_suspect_clock added.
  (2) IMPORTANT — self-incarnation bump `+1` on an UNAUTH wire field overflows on u64::MAX (panic debug /
  wrap-0 release → can't refute); now saturating_add(1). Regression test set_liveness_maintains_the_suspect_clock.
- five_workers STILL failed after the timing fix (89s, "survivor 2 never marked C down") — ROOT CAUSE: a
  survivor that only knows C via the converged member map (NOT its active view) learns C is Dead via GOSSIP
  but never creates the local `dead` tombstone that status "down" reads from → never shows down. The
  active-only fix was insufficient. TRUE FIX: `snapshot()` now surfaces converged SWIM-Dead members as
  "down" (the Dead state is the authoritative per-node signal, detected OR gossip-learned) + excludes
  Dead from the active list; probe_round also drops already-Dead peers from the active view (hygiene, no
  network → no async cycle). Reverted the active-only merge_members mark_dead (+ its Box::pin cycle).
  five_workers now PASSES in 30.15s (was 89.97s fail; 60s bar → 2x margin). membership 13/13, fmt+clippy clean.
- FULL WORKSPACE re-run: 15/16 bins green incl. five_workers + two_workers UNDER cross-binary contention.
  1 fail = unwanted_content_fades_then_want_resumes_repair (crates/obj/tests/healthscan.rs) — the KNOWN
  pre-existing obj repair-convergence flake (no node death in it → my membership/SWIM change is inert;
  passed 3/3 in isolation 12/23/24s). NOT a regression. P4 = DONE.

# WIRE ROLL — Elements 1+3 (mux + offer/grant) — PLAN (2026-07-11)
The last STRUCTURAL Transfer Plane v2 pieces. ONE wire migration → version-consistent SIMULTANEOUS
fleet roll (wire incompatible with old binary; NOT staggered). Design: docs/TRANSFER_PLANE_V2.md §1,§3.
CURRENT STATE (surveyed by 2 Explore agents):
- Transport all in crates/transport/src/lib.rs (iroh 1.0.1). Pool keyed (NodeId,ALPN); evict_peer
  already peer-only. Streams EOF-delimited (open_bi→write→finish→read_to_end), NOT length-prefixed.
- Dispatch: Transport::serve(handlers: Vec<(ALPN, mpsc::Sender<Connection>)>) routes WHOLE conns by
  conn.alpn(). 7 live ALPNs → tags: ping, member, piece(obj), sqlpage, invoke, registry, dht.
  (tracker ALPN is DEAD; control.rs unix-socket + dashboard HTTP are NOT ALPN, out of scope.)
- Wire framing MIXED: ping/member/piece use zeph_wire::Message; sqlpage(raw 32B cid+bytes),
  invoke(postcard), registry(postcard RegistryReq/Resp), dht(postcard DhtMessage) are bespoke.
  Tag byte sits ABOVE all of them (strip before each decoder).
- ONLY /craftec/piece/1 (PiecePush/PieceRequest) is the offer/grant path. sqlpage is a separate
  bulk object-fetch (not piece admission). Ping has a reserved dial-slot semaphore that mux collapses
  (one conn/peer → one dial). AvailabilityProbe/Ack wire msgs exist but have NO live client (dormant).
PHASES (each passes the offline harness before the next; harness FIRST):
[x] P0 HARNESS (a): mux connection-count bar added to scenario A (Transport::connection_count) —
    BASELINE CONFIRMED failing: worst 24 conns/node vs mux bar <=7 (commit dd0b7d1). (b) capped-
    receiver scenario deferred to P3 (per doc build order — developed WITH offer/grant).
[x] P1 MUX CORE (transport, commit 3535f63): MUX_ALPN /craftec/mux/1 + `tag` module (1-byte tags) +
    TaggedStream{remote,send,recv}; per-peer mux_pool ([u8;32] key) + mux_conn/open_tagged/evict_mux;
    request_tagged/send_tagged client helpers; serve(conn_handlers, stream_handlers) demuxes MUX
    connections by tag (bounded MUX_PIPELINE_STREAMS). Old per-ALPN path COEXISTS (endpoint
    advertises MUX + remaining legacy ALPNs) so protocols migrate one at a time. VALIDATED: routing
    dht-over-mux test passes.
[x] P2 MIGRATE 7 PROTOCOLS — ALL DONE + VALIDATED. dht (1468576), registry (dd19faa), sqlpage+invoke
    (63d5c7e), com test wiring (a04b3e8), member (fde4715), obj/piece (d397043), ping (last commit).
    Only MUX_ALPN advertised now; every protocol is a per-stream tag. PAYOFF: scenario A conn-count =
    [7,7,7,7,7,7,7,7] EXACTLY (one mux conn per peer, total 56) — DOWN from baseline worst 24 / total
    135; the mux bar (per-node <=7) PASSES. settled 12s, scan p99 360ms. Element 1 = DONE.
    Per-protocol pattern: client→request_tagged (req/reply) or send_tagged (one-way) / mux_conn
    (reachability); server→consume TaggedStream; every serve-wiring (main.rs + TestNode + crate tests:
    com invoke/feed/craft_backend, obj publish_fetch/encrypted/healthscan, craftsql_dst, dht node.rs
    test) moved with it. Piece-push TIMEOUT no longer evicts (shared conn — a slow push must not tear
    down other protocols' streams to that peer); only genuine stream failures evict_mux.
    LEGACY per-ALPN path (connect/connect_fresh/serve conn_handlers/pool/dials/ping_dial_permits)
    remains as harmless dead code (conn_handlers always empty now) — remove in a cleanup pass.
[x] FULL SUITE A-F over mux — ALL 6 GREEN (626s): A conns [7×8]; B census-20 17.8s drained (was the
    census-flake scenario under load — mux's 7-vs-24 conn count eased the churn, incidental fix); C
    recovered=true; D fair; E resolves 1.1x; F rejoin. NO regression from the mux migration.
[x] P3 OFFER/GRANT implemented (commit d540134): wire Offer{class,cid,items,bytes}/
    Grant{accept,retry_after_ms} (tag 0x0014/0x0015, on the muxed tag::PIECE stream). Receiver
    obj::grant() sizes accept from a graded grant_gate (critical→0, high→1 for CLASS_CRITICAL else
    0, else min(items,4)); tombstoned/cooldown→0. Sender repair_one offers ONE critical push/piece
    and on grant-0 (or push fail) redirects to the next candidate via a shared cursor over
    REDIRECT_MARGIN=8 spare recruits. repair_cid→repair_one so ALL repair traffic is admission-gated
    (publish-distribute + demand-scale paths still push un-offered — repair is the durability-
    critical path + backstop). noded wires grant_gate→ResourceGauge.
    Adversarial review (feature-dev:code-reviewer): NO logic bugs in grant/offer/repair_one cursor.
    Only finding = distribute-path scope gap (HIGH-band accepts non-critical intake).
    TRIED to close it (offers on distribute_initial/distribute_pending/rebalance_cid/scale_one,
    CLASS_NORMAL) → MEASURED REGRESSION: per-piece offers doubled the seed's piece-path RTTs during
    scenario B's 100-object publish burst (node0 ran 1096 jobs), pushing census-20 to 35.4s > 30s
    bar (drained=false). Everything still CONVERGED (all nodes census=20, queues drained, 0 failed)
    — pure timing. REVERTED the distribute offers; kept repair-only (proven 7/7 green, census 16.9s).
    Extracted a free offer() fn (used by the &self method; ready for the follow-up). CLASS_NORMAL
    removed. DEFERRED distribute admission to a no-extra-RTT design: carry push class on PiecePush,
    gate graded at ingest via grant_gate(class) — no offer round-trip, so no burst contention.
    Gap meanwhile covered by jemalloc (RSS bounded) + critical shed_gate backstop (95%).
[~] Scenario G (capped-receiver) added + RUNNING: node1 grants 0 + sheds+counts all ingest; kill a
    healthy holder → assert (1) recovered (redirect restores floor around the capped node) AND
    (2) repair-window ingest arrivals at capped == 0 (offer/grant saved the payload vs shed-at-
    ingest). Validating alone before the full A-G regression.
[x] P4 FULL HARNESS GREEN (A-G, 7/7, 646s, commit 8a895f8): A conns [7x8]; B census 16.4s
    drained=true (regression from distribute-offers gone after revert); C recovered; D fair;
    E resolves 1.4x; F rejoin; G recovered + capped repair-window arrivals=0. Wire roll (mux +
    offer/grant on repair) fully built + reviewed (no logic bugs) + validated.
[x] P5 SIMULTANEOUS fleet roll DONE + VERIFIED (2026-07-11 ~01:00, user-authorized "Roll now").
    Backed up binary → rsync crates/ → build on box (release, 1m18s) → install → stop all 4 →
    start all 4 (wire-incompatible flip, ~5s full outage). Verified: all 4 active, NRestarts=0,
    peers=4, live SWIM keepalives to all 3 peers @ sub-ms RTT, health scan running, ZERO panics.
    Only log noise = transient bootstrap-unreachable / isolated-rebootstrap + one iroh path-abandon
    in the 00:59:01-06 flip window (expected — all 4 down together); clean after.

=== WIRE ROLL COMPLETE: elements 1 (mux, conns 24→7) + 3 (offer/grant on repair) LIVE on the fleet.

[x] P6 NO-RTT CLASS ADMISSION (commit 191d83c) — closes the deferred distribute-path gap.
    Add class:u8 to PiecePush (repair→CLASS_CRITICAL, distribute/scale/rebalance→CLASS_NORMAL).
    ingest() consults grant_gate(class,1): under HIGH pressure admit CRITICAL, reject NORMAL — no
    offer RTT (so no scenario-B census regression; the ingest check is a no-op when grant_gate is
    unwired, i.e. every A-F node). Repair still negotiates offer/grant for bandwidth+redirect.
    New deterministic 2-node test high_band_gate_denies_normal_admits_critical_repair PASSES:
    NORMAL denied at ingest (holds 0), CRITICAL repair admitted (accumulates). Full A-G re-run =
    7/7 GREEN (651s) — scenario B census 17.4s drained=true (NO regression; the offer-RTT approach
    hit 35s, this doesn't). Adversarial review (feature-dev:code-reviewer): CLEAN, no findings ≥bar
    (one below-bar cosmetic: reason-string when pressure+tombstone coincide). Clippy clean.
    DEPLOYED + VERIFIED (2026-07-11 ~01:47, user-authorized "Roll it now"): simultaneous 4-node
    flip (backup → rsync → build 1m17s → install → stop all 4 → start all 4). All 4 active,
    NRestarts=0, peers=4, live SWIM keepalives @ sub-ms RTT, ZERO panics/ALPN errors. Memory
    bounded (seed 189MB as hub, others 84-97MB; jemalloc holding). Whole transfer-plane piece path
    is now admission-controlled on the live fleet.
[x] DEAD-CODE CLEANUP (commit 599f9b5) — removed the legacy per-ALPN transport path now that every
    protocol rides the mux: connect/connect_fresh/evict, pool/dials + PoolKey, ping_dial_permits +
    MAX_CONCURRENT_PING_DIALS, mod alpn (alpn::PING), and the never-wired open_tagged/send_tagged.
    connection_count/evict_peer/rebind/close are mux_pool-only; serve() dropped its always-empty
    conn_handlers param + legacy ALPN branch (12 call sites updated). transport -225 net lines.
    SURFACED + FIXED 2 latent breakages the ping->mux migration (e2a1292) left OUTSIDE the A-G gate:
    transport ping unit tests bound the retired alpn::PING (rewritten to the mux API), and
    two_workers_exchange_heartbeats asserted a dropped "ping served" log (restored in
    handle_ping_stream). Full workspace 46/46 green, clippy clean, scenario A [7x8] intact.
    NOT a wire change → no fleet roll needed (removed code was already dead on the running binary).
    LESSON: the transport unit tests + noded subprocess tests are NOT in the acceptance gate — a
    future wire change should run `cargo test --workspace` (non-ignored) too, not just A-G.
[x] DEPLOY GATE HARDENED (commit 24eed4d) — deploy/gate.sh: mandatory pre-roll gate running the
    COMPLETE surface (fmt + clippy -D warnings --all-targets + `cargo test --workspace` NON-ignored
    + A-G harness); exits non-zero on any failure; --quick skips A-G for local-logic-only changes.
    Closes the gap that let the ping→mux migration leave transport unit + two_workers subprocess
    tests red for a whole migration (they live outside the A-G harness). Documented in
    deploy/README.md + wired as step 0 in [[zeph-fleet-deploy]]. Validated: --quick green; full
    end-to-end run (incl A-G) = validating.
Follow-up (deferred, not blocking): reassign governance governor to a Hetzner node (Mac offline).

=== ELEMENT 2 — BOUNDED ACTIVE SET (choke model) — IN PROGRESS (2026-07-11) ===
Last unbuilt TPv2 structural element (1/3/4/5 all shipped). Sender-side, NO wire change → local-
logic, STAGGERED roll (not simultaneous). Spec: docs/TRANSFER_PLANE_V2.md §2 — transfer WORK with
at most K peers at a time (K=4 default), other peers are cheap candidates; active set rotates as a
peer's work drains or it misbehaves (busy/slow).
Design: ActiveSet choke gate — K permits; enter(peer)->guard; a peer already active bumps a refcount
(free — one conn/peer), a NEW peer waits for a permit; guard drop dec refcount, at 0 frees the permit
→ next candidate enters. Lives in obj (transfer plane); ping/census excluded by construction (they're
transport/membership). Composes with offer/grant: grant-0/timeout releases the slot + redirect brings
the next candidate in. K governable later (minimal-kernel: mechanism native, policy swappable).
[x] P1 ActiveSet core + 3 unit tests (commit 8bf7bc2) — K permits, refcount, Drop frees slot.
[x] P2 wired into all push paths via the free push_piece (commit c7b63c3); active_set_k=4 default,
    0 disables. A+B validated live: A settles 14s (budget 120s), B census 3.35s drained — NO
    regression (choke REDUCES load, unlike the offer-RTT; risk retired).
[x] P3 peak high-water-mark + scenario A assertion (commit 6bb13d2): seed peaks EXACTLY at K=4 under
    a 200-object distribute (bound held precisely), holders at 0 (no steady-state push) — proves the
    choke is on the push path + bounds real traffic. FULL A-G 7/7 GREEN (605s) with choke active.
[x] P4 adversarial review (feature-dev:code-reviewer): ActiveSet primitive SOUND (no deadlock/leak/
    refcount bug, 6 concerns cleared). One finding fixed (commit bdbd3ed): scale_one/distribute used
    REQUEST_TIMEOUT (30s) → a slow peer hogged a shared choke slot 10x the PUSH_TIMEOUT (3s) intent,
    able to starve repair's CLASS_CRITICAL pushes; switched both to PUSH_TIMEOUT.
[~] P5 STAGGERED roll ATTEMPTED — the full gate.sh CAUGHT scenario B (6/7, census 50s>30s) and
    BLOCKED the roll (gate working as designed). Diagnosed: NOT an element-2 regression.
    - Refined the choke to ongoing-transfer-only (commit a5c6fc6) while chasing it, then
    - Ran a choke-OFF baseline: B census 3.4/17/35.8/3.3s across 4 runs → 1 FAIL even with NO choke.
    PROOF: scenario B's census-20 bar (30s) is INHERENTLY FLAKY (natural variance 3-36s under 20-node
    mass-rejoin). Element 2 is EXONERATED — it's sound (scenario C: choke peaks [4,1,1,1,4,4,4],
    bounded + exercised by repair; adversarial review clean). Box binary built (choke, K=4) but NOT
    installed; fleet untouched.
    BLOCKER for the roll is the FLAKY B BAR, not element 2 → makes the whole A-G gate ~25% false-fail.
[x] FLAKY GATE — ROOT-CAUSED + census FIXED (commit 3e4dcf4): the epidemic member-map cascade only
    fired on learning NEW members, so a straggler waited for the 30s shuffle → census-20 was bimodal
    3s/35s. Added a periodic epidemic safety net (member map → 3 peers every 5s). Measured: census-20
    now 3.34-3.79s across 9 runs (5 choke-on + 4 choke-off) — variance GONE. Real fleet win (fast
    census recovery after restart waves); mixed-version-safe local-logic change → staggered roll.
[!] RESIDUAL: a SECOND scenario-B flaky bar surfaced — the JobCoordinator queue-DRAIN bar. Isolated:
    choke OFF → B 4/4 drained=true; choke ON → 1/5 drained=false (repair still churning, high
    at_risk on the seed). Implicates ELEMENT 2's repair choke (K=4 serializes repair pushes → slower
    repair drain under mass-rejoin). Element 2's value is marginal anyway (acute issues already
    solved by mux+jemalloc+offer/grant).
[x] CHOKE PROPERLY FIXED (commit 5b3dd9b): ActiveSet::try_enter — NON-BLOCKING. A choked push is
    DEFERRED (bail → caller redirects/retries) instead of blocking + holding a JobCoordinator slot.
    Validated: B 5/5 census-fast + drained=true (drain flake GONE); C recovered (choke peaks [4,..,4]
    exercised+bounded); G recovered arrivals=0. Element 2 now works without destabilizing drain.
[!] FULL GATE still flaky on scenario B — but NOW on a THIRD, different bar: max-job wall-clock (a
    15.6s SCAN job > 10s bar) in the FULL-suite run, while B in ISOLATION is rock-solid (max-job
    106-737ms across 5 runs). So it's a full-suite-context / machine-contention artifact of a harsh
    20-node mass-rejoin stress test with several tight bars — NOT a code regression (census + drain
    fixes hold; B solo is clean). A 15.6s scan = slow DHT resolve under 20-node churn; irrelevant to
    the real 4-node fleet. Box binary (census+choke fixes) BUILT, NOT installed; fleet untouched.
[x] ROLLED + VERIFIED (2026-07-11 ~09:45, user "roll it if the gate passes"). Full gate RETRY =
    7/7 GREEN (the 15.6s scan was a one-off full-suite contention artifact; no parallel build → B
    clean). STAGGERED roll (mixed-version-safe, no wire change): install → restart zeph2→zeph3→zeph4
    →zeph one at a time (09:41/09:43/09:44/09:45), verify each before the next. All 4: active,
    NRestarts=0, full 4-node mesh (each sees 3 peers), 0 panics, memory bounded (seed 531MB fresh,
    others ~103MB). NOTE: first roll command truncated after zeph2; continued the remaining 3.
    LIVE ON THE FLEET: census-convergence fix (periodic epidemic) + element 2 non-blocking choke.

=== ALL 5 TPv2 STRUCTURAL ELEMENTS BUILT + LIVE (1 mux, 2 choke, 3 offer/grant, 4 elected-scan,
5 fair-sched) + P6 no-RTT class admission + census-convergence fix. Fleet: 4 Hetzner nodes, healthy.
[x] GATE HARDENED (commits f7311bb, dc53eec) — full deploy/gate.sh now reliably GREEN. Two contention
    flake sources closed (both TEST-ONLY, no fleet impact):
    1. Scenario B max-job bar: was any-job>10s → false-positived on a 15.6s SCAN under full-suite
       machine contention (B solo clean, max-job <1s). Reshaped: FAIL only on a REPAIR job >10s
       (durability path) or ANY job >30s hang ceiling; slow non-durability jobs 10-30s logged not
       failed (system converges; 60s no-progress bar catches true wedges).
    2. two_workers subprocess tests: five_workers flaked (death detect >60s) in the parallel gate but
       3/3 clean solo (~24s) — libtest ran the 4 tests' ~10+ processes concurrently. Serialized with
       a poison-recovering lock → 4/4 in 25s.
    Full gate now green across fmt+clippy+workspace-tests+A-G (7/7). Node-level census+drain fixes
    already live on the fleet; these harness changes need no roll.

Open follow-ups: NONE blocking. (Governor reassignment DISMISSED 2026-07-11 — spin up the Mac node
on demand for governance ops; see [[zeph-fleet-deploy]].) Transfer plane structural work COMPLETE. ===
=== ALL 5 TPv2 STRUCTURAL ELEMENTS BUILT (1 mux, 2 choke, 3 offer/grant, 4 elected-scan, 5 fair-sched);
elements 1+3+6 LIVE on the fleet; element 2 built+validated, awaiting staggered roll. ===

# SEED-NODE MEMORY: glibc-arena bloat → jemalloc (2026-07-10, ultracode)
Post-deploy soak surfaced the seed node ('zeph', primary DHT hub) at ~8GB RSS (OOM-killed a few
times, auto-recovered, no data loss) while identical-workload peers held <1GB — SAME ~2700 cids,
SAME 130-220MB on disk, SAME FDs/threads/sockets. NOT a data/connection difference; the 8GB was pure
heap (VmData). ROOT-CAUSED (not guessed): a 14-agent adversarial workflow (wf_2dc6416d) audited every
seed-amplified in-memory structure (dht tombstones/failures/record-store/table, transport pool/dials,
obj scan_snapshot/cid_health/last_served/announced_at/node_liveness, membership members) and REFUTED
all 10 — each bounded by peer-identity or held-cid cardinality, none can reach GB. => not a code leak.
The binary used the SYSTEM (glibc) allocator; the seed does the most bursty serve+mint allocation
across 17 threads; glibc retains freed memory in up to 8xncpu(=128) per-thread arenas instead of
returning it. CONFIRMED by controlled experiment: a MALLOC_ARENA_MAX=2 + MALLOC_TRIM_THRESHOLD_
systemd drop-in on zeph → RSS went FLAT at ~550MB (dips to 125 as glibc trims), vs ~7GB at the same
36-min uptime without it (~13x). Also: the actual OOM churner was cadvisor (Coolify metrics agent),
56 of 61 post-deploy OOMs; zeph was collateral because its fat glibc heap made it the kill target.
OOM rate DROPPED ~10x after the transfer-plane deploy (6008 pre / 102 post) — deploy did NOT cause it.
DURABILITY through all of it: CONVERGED — at-risk roughly HALVED across the fleet (zeph 1959->935),
repair kept pace, zero data loss. So the transfer-plane deploy itself is a clean success.
FIX (jemalloc) — took TWO commits because DEFAULT jemalloc is NOT enough:
  0ed4082: tikv-jemallocator as #[global_allocator] in crates/noded (cfg(not msvc); Linux fleet +
    macOS). BUT the soak showed DEFAULT jemalloc STILL CLIMBS (zeph 829->4407MB in 6min) — default
    jemalloc runs NO purge thread and decays dirty pages lazily (10s), so under seed churn RSS grows.
  be9588b: BAKE jemalloc runtime config via the `_rjem_malloc_conf` symbol (read at allocator init):
    Linux = `background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0`, macOS = decay-only
    (background_thread is pthread/Linux-only — jemalloc warns+ignores on macOS, so gated by
    target_os to avoid the startup warning on the low-churn Mac node). Verified jemalloc READS the
    symbol: `_RJEM_MALLOC_CONF=stats_print:true ./zeph` showed opt.dirty_decay_ms:1000 (non-default).
  ENV-CONFIG TEST (proof the values work) on the live seed: `_RJEM_MALLOC_CONF=background_thread:
    true,dirty_decay_ms:1000,muzzy_decay_ms:0` drop-in → RSS FLAT ~530-577MB (dips to 179 as the
    purge thread runs). Matches the glibc MALLOC_ARENA_MAX result.
  THIRD GOTCHA (commit 9779416): baked-config binary ALONE (no env) STILL climbed (zeph 93->3018MB
    in 4min) even though stats confirmed the symbol was read (opt.dirty_decay_ms:1000). Cause:
    setting background_thread:true in malloc_conf/the symbol does NOT actually START the purge
    thread. FIX: start it at RUNTIME — `tikv_jemalloc_ctl::background_thread::write(true)` at top of
    main() (dep tikv-jemalloc-ctl, Linux-gated). CONFIRMED: zeph on baked-config + runtime-enable,
    NO env, held FLAT ~540MB (dips to ~110 as the purge thread runs) across an 11-min soak.
✅ FIX COMPLETE + DEPLOYED to all 5 nodes (runtime-bg-thread binary, commit 9779416): seed zeph
~8GB -> 539MB, zeph2/3/4 94-101MB, Mac 136MB, all nr=0 full mesh. Env-free (no drop-ins). Rollback =
zeph.bak-prejemalloc-1549 (box, glibc) / -2355 (Mac). See [[zeph-seed-memory-glibc-arenas]].
KEY LESSON: the memory fix is THREE pieces, all required — jemalloc allocator + baked short-decay
symbol + RUNTIME background_thread::write(true). "use jemalloc" alone climbs; symbol-set
background_thread does not start the thread. Open infra item (NOT ours): cadvisor OOM kill-loop +
box over-subscription — a Coolify/box-capacity fix (limit cadvisor / MemoryMax / bigger box).

# TRANSFER PLANE V2 — DEPLOY GATE ✅ PASSED + FLEET ROLLED (2026-07-10, ultracode)
Deployed the scenario-C durability fix (commits 6aa5812 + e456b3f) to the LIVE fleet. GATE: full
suite green (A/C/D/E/F + B isolated), C 12/12 flake-free, E 1.1x resolves; deploy-gate adversarial
review (agent) = NO deploy-blocking findings (converges + stops, no panic/wedge/overflow, wire/DHT/
ALPN compatible with old binary for a mixed-version roll, scheduler bounded). Rejoin: scenario F +
persistent /var/lib/zeph stores → restart=full-store rejoin; every node came back to peers=4.
FLEET = 5 nodes: zeph/zeph2/zeph3/zeph4 (co-located on Hetzner ubuntu-32gb-fsn1-1 @46.224.172.252,
build tree /opt/zeph-src/zephcraft-standalone — NOT the doc's zephcraft/; rsync target, binary
byte-identical to /usr/local/bin/zeph) + the Mac launchd node (~/.zeph, ec.craft.zeph, governance
governor, behind NAT/relayed). zeph5/zeph8 stay failed/stopped (per cluster memory).
PROCEDURE (done): backup binary (zeph.bak-20260710-0536 on box, zeph.bak-20260710-1345 on Mac) →
rsync crates/ → build on box (nice -19 -j4, detached, 1m27s, load 21→23.6 fleet survived) →
install → STAGGERED restart zeph2→zeph3→zeph4→zeph→Mac, verifying each (active, NRestarts=0,
peers=4, 0 panic/error) before the next. FINAL: all 5 on new binary, full mesh (every node sees 4
peers incl. cross-checked Mac↔fleet), 0 panics, 0 repair storm (steady-state content >=floor so the
below-2k rescue correctly idle), box load back to 17. Rollback = cp zeph.bak-* → restart.
NOTE for future deploys: build tree is zephcraft-standalone (rsync from local crates/), NOT git;
grep the logs with ANSI stripped (sed 's/\x1b\[[0-9;]*m//g') — tokens render as peer<esc>=<esc>val.

# TRANSFER PLANE V2 — P3 SCENARIO C ✅ GREEN (2026-07-10, ultracode)
DURABILITY-UNDER-LOSS GAP (scenario_c_kill_holder_repairs) — FIXED. Publish 30 PINNED cids from
node0, quiesce, KILL the top non-seed holder permanently; survivors must restore every cid to >=k=8
pieces (cluster-wide piece_count sum) within 200s. NOW: recovered=true, 0/30 below-k, converges in
~47s. ROOT CAUSE (RLNC): a below-k cid's repair election was won by a surviving PIECE holder that
recodes within its <k-dimensional subspace — adds NO independent rank, so recoverability never
returns. Only a whole-content holder can mint the missing rank (node0, the publisher).
THE REAL BUG was in the WIRED repair path: the test/prod coordinator routes EngineWork::Repair →
`repair_cid` (obj/lib.rs ~2187), NOT the inline health_scan_chunk path. repair_cid had its OWN
election `if winner != Some(&me) { return 0 }` that discarded node0's repair when a piece-holder won
the rendezvous. Under elected scan (element 4) ONLY node0 (content holder, always-scan) scans these
cids and enqueues repair — but repair_cid then deferred to n1 (6-piece holder), which never scanned
them itself and cannot add rank: a DETECT-BUT-DEFER DEADLOCK (measured: 5/30 stuck at 6 pieces on
one holder, content holder idle — confirmed by a per-cid diagnostic showing [n0=0+C n1=6 rest=0]).
FIXES (crates/obj/src/lib.rs, all committed together):
  (a) should_scan ~1342: whole-content holders ALWAYS scan (never lose the scan election) — content
      holder must DETECT below-k to enqueue a LOCAL repair (repair runs on the scanning node).
  (b) health_scan_chunk repair path ~1669: content_restorer = (have < gen.k && has_content) repairs
      unconditionally, bypassing the rendezvous (inline path, used when work_trigger unset).
  (c) repair_cid ~2230: SAME content_restorer bypass — THIS is the one that closed scenario C
      (the wired path). Fade gate (~2223 !is_alive→return 0) runs BEFORE it, so unwanted retained
      copies still never repair (no minting storm); only fires below k so healthy cids untouched.
FULL SUITE (A/B/C/D/E/F): A,C,D,E,F GREEN in the back-to-back suite; C 0/30 below-k @47s; E still
EXACTLY 1.0x resolves/cid + 100/100 recoverable (no double-scan / durability regression). B FAILED
in the suite ONLY on census-20 (35.4s > 30s bar) — but B ISOLATED = 15.36s census (within the
tracked 7-21s), below_k=0. => the census miss is the KNOWN CPU-STARVATION FLAKE (B ran last after
570s of prior heavy tests), NOT a regression; content-holder-always-scan did not slow membership.
Do NOT gate on a full back-to-back --ignored suite for the census bar; run heavy scenarios spaced.
ADVERSARIAL REVIEW (2 agents, correctness + regression) — one REAL defect found + fixed:
  [IMPORTANT, FIXED] mint_piece's has_content branch returned a REPEATED stored piece: serve_pieces
    returns STORED pieces before fresh encodes, so a content holder that had ALSO ingested coded
    pieces (ingest has no has_content guard) would push 8 DUPLICATES per repair — no rank added,
    below-k deadlock RETURNS, and the inflated piece COUNT masks the cid as recovered while still
    undecodable (silent durability loss). Green scenario C only hid it because node0 is a PURE
    publisher (piece_count=0). FIX: new Store::mint_from_content(cid) always encodes fresh from the
    k sources (never serve_pieces); mint_piece routes content holders through it. Regression guard:
    store test mint_from_content_is_independent_even_with_stored_pieces (pin + 3 ingested pieces →
    K minted pieces are distinct AND decode to content). All 8 store tests green.
  Regression review CLEARED the fix: the O(cids) resolve property comes from the noded PROVIDER-AWARE
    BACKOFF (recheck_min*holders cadence), NOT the scan election — so content-holder-always-scan
    changes the scan-vs-skip DECISION at a due-event, not the due-RATE. No O(cids*replication)
    amplification; S5 preserved; content_restorer confined to below-k so healthy cids untouched.
  WATCH-ITEM (F2, not a blocker): a node pinning a VERY LARGE object set now resolves ALL of it each
    due-cycle (up to holders× more absolute resolve/CPU than pre-fix elected behavior, still O(held)).
    Future optimization: content holders could scan their content on a slower dedicated cadence, or
    skip locally-faded/unwanted retained copies (should_scan currently always-true for any held
    content, incl. faded — wasted resolve that no-ops at the fade gate; bounded, correctness-neutral).
MARGINAL FLAKE — ROOT-CAUSED + FIXED (was ~1/8, now 0/12+0/12 after fix). Reproduced by looping
scenario C with the committed per-cid content-holder-verdict diagnostic (caught on run 8/20). The
verdict was DECISIVE: stuck cid total=7 [n0=0+C n1=3 n2=4], but the content holder's health record
read effective=9, live_providers=3 — a PHANTOM 3rd provider (the killed node, still counted alive via
HyParView gossip refreshing its last_heard past the 30s liveness TTL, before SWIM tombstones it)
inflated `have` from the true 7 to 9. have=9 >= k=8 suppressed the `have < k` content_restorer, so
node0 deferred to the election → a rank-incapable piece-holder (n1=3/n2=4, recode in <k subspace) or
the dead phantom → detect-but-defer deadlock RETURNS. THE FIX (supersedes the have<k trigger in both
paths): content_restorer now fires on a phantom-PROOF capability check — a whole-content holder
repairs a RANK-FRAGILE cid (have < 2k) iff NO LIVE holder has >= k pieces (`!any_live_holder_has_k`).
A stale phantom holds < k pieces per cid (killed node's pieces were spread thin), so it can neither
fake a >=k holder NOR lift a true-below-k cid past the 2k ceiling. The 2k (not floor) bound confines
the bypass to fragile cids: cids at 2k..floor are safely recoverable and left to the normal election,
so elected-scan efficiency is preserved (floor bound gave E resolves 2.0x; 2k keeps it ~1.x). VERIFY:
C 12/12 green (2k) [and 12/12 at the floor bound before tightening]; E resolves back toward 1.x,
100/100 recoverable; A green. Content-holder-always-scan (should_scan) unchanged; mint_from_content
unchanged. This REPLACES the earlier have<k content_restorer (commit 6aa5812) — amend/new commit.

# TRANSFER PLANE V2 — P1+P2 DONE (2026-07-10, ultracode)
ELEMENT 5 (class-fair scheduling, e9c4270) + REVIEW FIXES (e8c6cf6): JobClass per-key-prefix,
per-class in-flight caps. Adversarial review (workflow wh3rxdfbf, 7 confirmed) found + fixed a
CRITICAL: factory-panic leaked the class slot → permanent per-class starvation → whole-scheduler
wedge. Now: SlotGuard RAII release (all counters + coalescing-map + wake on any exit incl panic);
in_flight reserved in dispatcher (fixes active_cap skew); O(1) HIGH gate (no full-heap drain);
class_queued eligibility precheck; CAPS ARE CONTENTION BOUNDS + work-conserving (lone class fills
8, caps bind only when another under-cap class waits). Tests: per_class_cap_prevents_starvation
(scan+publish both held at 4 under mutual flood) + caps_are_work_conserving + classifier map.
ELEMENT 4 (elected healthscan, 1d32881): per-cid scan_snapshot (capable holders) + should_scan
rendezvous-elects ONE scanner per (cid,epoch) over {cached capable ∩ alive} ∪ self; worker skips
non-winners (no resolve/slot). 3 safety guards (self always candidate, dead-winner filter,
staleness ceiling → no cid unscanned). PROOF (scenario_e, CountingRouting): resolves = exactly
1.0x cids/interval (was O(cids×replication)); 100/100 recoverable (>=k); piece totals IDENTICAL
to elected-scan-OFF baseline → durability-NEUTRAL, efficiency-positive. Scenario A 17ms unchanged.
KEY FINDINGS from the scenario-E investigation (record for P3):
  - Elected scan makes the PER-NODE at_risk metric STALE (a non-winner never re-scans a cid, so
    its at_risk_ids never clears) → scenario B's "at-risk drains to 0" bar is now permanently
    red for a BENIGN reason. P3 MUST reshape it to measure CLUSTER durability (piece totals /
    recoverability), not per-node at_risk_ids.
  - Publisher-only-wanted content plateaus at ~12-22 pieces (below the n=32 margin) in 70s — a
    repair-RATE + Fade-propagation observation, NOT a bug (recoverable at k=8). Full n-margin
    convergence is slow; scenario A/B's settle covers the realistic (steady-state) case.
CENSUS-20 now ~7-21s (membership fixes compounded). max_job ~1s. Scenario A/D/E green; B red only
on the stale at-risk bar. Fleet still on old quiet-config binary — nothing deployed since the P0
set; DEPLOY GATE still pending P3 (kill-holder scenario C, restart-rejoin D, rejoin memory check).
WORKSPACE: 171/0 clean in isolation. KNOWN FLAKE (pre-existing, not P1/P2): a 60s+ timing test
(self-heal / DST class) fails ~1/run ONLY under concurrent build+harness CPU starvation; passes
isolated. Track for a deadline-loosening / #[ignore]-under-load follow-up; not a regression.
P3 (NEXT, blueprints in tasks/w8tyvqane.output): (1) reshape scenario B at-risk bar → CLUSTER
recoverability trend (per-node at_risk_ids is stale under elected scan — element-4 finding);
(2) in-flight-jobs visibility (dedup set → HashMap<key,Option<Instant>>, in_flight_jobs() with
elapsed, surface in status/CLI/webui); (3) restart-rejoin scenario D (TestNode persist store across
respawn → full-store rejoin, the real production failure mode) + scenario C (kill live holder →
repair fires). THEN deploy gate (fix findings + rejoin memory check) → wire roll (offer/grant+mux)
+ scenario C-capped → scale S4/S1/S2.
DO-NOT-SCALE gate still open: S5 O(census)-per-item in scale_one/repair_one recruit needs a bounded
K-subset accessor before any scale-out past ~tens of nodes (benign now, hard ceiling at 1000s).

# TRANSFER PLANE V2 — SCOREBOARD (updated 2026-07-10, ultracode)
DEPLOY-GATE ADVERSARIAL REVIEW (workflow wf_92b52e18, 24 agents, 11 confirmed findings) found the
GREEN HARNESS WAS MASKING 3 REAL DEFECTS (no scenario held passive under-replicated whole content
or killed a live holder). All 3 FIXED + gated (commit 536d90a):
  1. [CRIT] sole-capable repair fallback was DEAD CODE (election over empty `capable` → None →
     skipped enqueue; the e61f252 deadlock fix never ran). Fix: push self in fallback. Gated by
     obj test VERIFIED to fail pre-fix (got Err(Empty)).
  2. [DURABILITY] census-as-liveness counted SWIM-dead holders alive 120s → repair suppressed ~2min
     post-death. New liveness_census() (excl. dead, 30s TTL); 120s census kept for registry election.
  3. [LIVE] rebalance stalled forever on a dead least-full target. Skip-set + no-loss-on-fail.
  + S5 interim: hoisted liveness fetch out of the per-cid rebalance loop.
POST-FIX HARNESS: A PASS (17ms scans); B census-20 30→21s, distribution 17s, max_job 1.2s — all
IMPROVED. Only red = at-risk bar (heal-rate-bound SHAPE issue, P3 reshape).
STILL REQUIRED BEFORE FLEET DEPLOY (P3): scenario C (kill live multi-cid holder → repair fires
within a small multiple of scan interval) + scenario D (restart-rejoin, FULL store) + rejoin
memory check. Do-NOT-SCALE gate: S5 O(census)-per-item in scale_one/repair_one recruit still open
(benign N=20, hard ceiling at 1000s — needs bounded K-subset accessor before any scale-out).
PRIORITIZED PLAN (from workflow synthesis): P1 element5 class-fairness (sched per-class caps) ·
P2 element4 elected-scan · P3 restart-rejoin harness + in-flight-jobs visibility + at-risk bar
reshape · THEN deploy gate · THEN wire roll (offer/grant+mux) + scenario C-capped · THEN scale
S4/S1/S2. Blueprints for all in workflow result (tasks/w8tyvqane.output).

# TRANSFER PLANE V2 — SCOREBOARD (updated end of 2026-07-09)
Harness-gated progress (tests/tests/transfer_plane.rs; run: cargo test -p zeph-tests --test
transfer_plane -- --ignored --test-threads=1 --nocapture):
- Scenario A (steady state): PASS since clean baseline — scan p50 12-15ms, full convergence ~80s.
- Scenario B (5→20 mass rejoin), baseline → now:
    census-20: 105s → 45s (bar 30s — NEAR; tail = last nodes' final members)
    max_job: 43.8s distribute → 22s reannounce (distribute CLASS DELETED by S3)
    at-risk drain: 78-100 all nodes → 3-13 on 19/20 nodes
    queues: plateau → drained
- SHIPPED (committed, NOT yet deployed to fleet): e501d17 S3 lazy rebalance (sweep deleted,
  rebalance_cid rides the scan, fired_for_digest bug retired) · 1df361e epidemic census diffusion
  (new-member merge wakes an immediate debounced shuffle) + TestNode synced to post-S3 wiring
  (harness fidelity MUST track cmd_run — its stale driver contaminated one run).
- SEED ANOMALY RESOLVED (e61f252, dug 2026-07-10): TWO stacked causes + one non-bug.
  (a) MembershipPeers returned the size-5 ACTIVE VIEW not the census (doc violation; same class
  as the old registry election ceiling) → scan liveness filtered 14/19 providers as dead AND
  placement round-robined over 5 targets (scenario A skew). Census-backed now → scenario A
  distribution 78s→22s, scenario B initial 46s→14s. (b) REPAIR DEADLOCK: 1-piece holders can't
  mint independent RLNC pieces, so thin spreading left NO capable repairer while the publisher's
  wanted whole content sat passive by rule → sole-capable fallback (content-holder repairs iff
  no piece-capable live holder; need/batch/election contain minting). (c) Residual high seed
  count = VISIBILITY not sickness: it scans all 100 held cids mid-heal; joiners hold few.
- REANNOUNCE CHUNKED (7baef74): due list → ~25-cid coordinator batch jobs; max_job 22s → 1.0s —
  the >10s JOB BAR IS GREEN, no O(held) single jobs remain anywhere.
- SCOREBOARD vs baseline: max_job 43.8s→1.0s ✓ · initial distribution 46s→12s · scenario A
  distribution 78s→19s, scans 17ms ✓ · census-20 105s→~45s (bar 30s — LAST structural red) ·
  at-risk stuck-forever→heal-rate-bound mid-drain.
- CENSUS TAIL CLOSED (d4fa9d1): epidemic FAN-OUT (push member map to 3 active peers per round,
  not 1) + passive-mixed shuffle targets → census-20 105s→30.4s (at the bar; ~5s run noise).
- SCOREBOARD vs baseline: census-20 105→30s ✓ · max_job 43.8→1-2s ✓ · distribution A 78→17-19s,
  B 46→12s · scenario A PASS (17ms scans) · at-risk = only remaining red (heal-rate-bound; a
  bar-SHAPE question — reshape to trend+rate-floor, per the design workflow).
- UNDEPLOYED v2 diff = a70204c..HEAD (5 crates). Fleet still on old quiet-config binary.
- IN FLIGHT (ultracode workflow wf_92b52e18): deploy-gate adversarial review of the undeployed
  diff (4 dims × verify) + design blueprints for element 5 (class fairness), element 4 (elected
  scan), restart-rejoin harness scenario, at-risk bar reshape + in-flight-jobs visibility →
  synthesized deploy-gate verdict + prioritized plan. Implement sequentially through the harness
  after; DEPLOY only after findings fixed + a rejoin memory check.
- Fleet: extras zeph5-19 STOPPED+DISABLED (production was a permanent scenario-B loop; caps can't
  cure the balloon — 3.1G kill on a 3G cap). Core-4 quiet config running binary f81fbe66. Deploy
  of S3+epidemic happens only after the remaining scenario-B bars are green or user green-lights.

# TRANSFER PLANE V2 (2026-07-09, ACTIVE — supersedes further v1 patching)
User verdict after the patch-regress cycle: the problem is STRUCTURAL — stop building around the
current design. docs/TRANSFER_PLANE_V2.md (commit 9b3e7c9) is the spec: 5 structural elements
(mux single-conn/peer · choke active-set · offer/grant admission with redirect/requeue · elected
healthscan (scanner=actor, electorate=last-known capable holders) · class-fair scheduling) + 5
SCALE elements (S1 digest membership O(Δ) · S2 DHT-native registry placement (removes the census
ceiling — sharded SQL was built on an O(N²) substrate) · S3 lazy-only convergence, reactive sweeps
DELETED · S4 version beacons on gossip · S5 invariant: per-node work = O(held+active_set), never
O(census)). Build order in the doc; NOTHING deploys without passing the offline acceptance harness
(tests/tests/transfer_plane.rs: scenario A steady-state scan p50<250ms; B mass-rejoin census 20<30s,
no job >10s, queue drains; C capped receiver sheds via grants). Harness construction delegated
(subagent, tests/ crate only) — baseline numbers against current code expected to FAIL = the
reproduction. Live cluster: STABLE (census 20, no OOM) but slow; last deployed binary 82d22719
(DHT serve pipelining — fixes the pooling-induced per-peer serialization convoy). NO further live
patching; v1 patch train ends here.

# SCALE CONVERGENCE: CONN POOL + JOB COORDINATOR EXTENSION + RESOURCE MANAGER (2026-07-09, in progress)
Root cause chain proven by the capped 20-node redo + single/5-node rejoin experiments:
conn-per-request architecture → under concurrency handshakes stack (each holds MBs of QUIC state)
→ RSS balloons (zeph5: flat 240MB alone; 965MB with 4 co-rejoiners, −800MB freed in ONE 5s sample
when attempts aborted = pending-conn state, not data) → OOM cap kills → deaths re-trigger dials →
thrash. Churn↔death correlation: zeph9 3432 churn lines/8min → 3 kills; zeph6 2190 → 3; zeph7 766 → 2.
Same root as Mac flapping + noq PTO wedges. DHT already has the right pattern (conn_for cache).
User also directed: (a) resource manager to supplement the job coordinator, (b) extend coordinator
to cover ALL node jobs (today only distribute ×2 + healthscan go through it; repair runs INSIDE the
scan job at HealthScan priority — the Repair tier is unused!).

- [x] Phase 1: per-peer connection pool in Transport — DONE. Pool keyed (peer, ALPN) with
      close_reason validity + stable_id-checked evict + connect_fresh + evict_peer; cleared on
      rebind/close. All 6 request paths converted (per-request conn.close removed); DHT's private
      conns cache DELETED (delegates to the pool; attempt-1 = connect_fresh). Review found 2 real
      issues, both fixed: (1) external tokio::timeout wrapping push_piece dropped the future
      before its internal evict ran → stuck-but-open conn pooled forever; fix = timeout param
      runs INSIDE request(), evicts on timeout too (data-plane contract; pings still tolerate
      timeouts); (2) membership oneway branch swallowed the delivery-read error → now fails the
      request and evicts. headreg 3s-drain site documented as self-healing. Gates: clippy 0,
      164/164 tests. NOTE for reviewers: never wrap pooled-conn requests in external timeouts.
- [x] Phase 2: coordinator extension — DONE (commit 17723c8). Audit found: only distribute×2 +
      healthscan went through the coordinator; repair ran INSIDE scan jobs (Repair tier unused);
      publish distribution = raw spawn per publish; distribute_pending = inline loop; headreg
      replicate = spawn per write. All routed: EngineWork trigger → Encoding publish:{cid} /
      Repair repair:{cid} jobs; distribute_pending deduped Distribution job; pushstate:{shard}
      full-state-at-run-time + per-shard dirty counter (review fix: mid-push write was dropped);
      repair_cid re-checks floor + Fade gate at exec time (review fix: TOCTOU minted surplus).
      Stays direct (deliberate): membership probe/shuffle, gov tick, migrate/reshard rounds.
- [x] Phase 3: resource manager — DONE pending review. sched::ResourceGauge (budget from own
      cgroup memory.max, RSS sampler 5s): >85% only Repair dispatches, >95% nothing + inbound
      sheds (obj ingest + headreg PushState answer "busy"; senders' next pass retries). deferred
      + mem_load_pct in JobStats. Gauge off when no cgroup limit / non-Linux (Mac). Gated
      dispatch re-checks on 500ms tick. Test: gauge_gates_routine_work_but_not_repair.
- [ ] Phase 5 — TRANSFER PLANE v2 (LOCKED by user 2026-07-09): stability achieved (run 2),
      throughput nowhere near hardware — queue ~2200 flat, jobs timeout-bound. ONE wire roll:
      (a) PUSH ADMISSION NEGOTIATION — receivers advertise free intake slots; senders offer
          BEFORE shipping bytes (PushOffer{cid,pieces,bytes} → PushGrant{accept,retry_after_ms},
          grants from gauge state); on busy/partial: REDIRECT remainder to another candidate
          when target-fungible (coded pieces), REQUEUE with backoff when target-fixed (registry
          shard replicas). Kills timeout-as-failure-signal (3-8s → ~1 RTT busy answer).
      (b) SINGLE-CONN-PER-PEER MULTIPLEXING — one connection per peer, protocol tag per stream
          (~190 conns → ~19 per node); makes per-peer accounting natural.
      Companions (no wire change, ship alongside or before):
      (c) bounded per-connection stream PIPELINING (~8 concurrent streams served per conn,
          replacing serial accept_bi handling) — kills the per-peer-pair serialization;
      (d) gauge-modulated JOB CONCURRENCY (8 under pressure → 16-32 healthy);
      (e) per-peer ACTIVE-SET budget (BitTorrent choke model): actively transfer with K peers,
          queue the rest as cheap candidates.
      (f) HOLDER-ELECTED HEALTHSCAN (user design: elect FIRST, only the elected node scans,
          then it directly repairs/degrades — scanner = actor by construction). Electorate =
          last-known CAPABLE holder set from the previous scan's provider records (ids+counts
          must be stored per cid, ~1.5MB) ∪ self; rendezvous per (cid, epoch); non-winners just
          reschedule locally (zero network). NO wire change needed (supersedes the RepairHint
          idea). ~20x aggregate scan-traffic cut at 20 nodes AND faster healing (today repair
          waits for the winner to coincidentally be the scanner). Divergent views → occasional
          benign double-scan; dead winner ages out via membership-filtered records; fresh
          holder bootstraps with one unconditional scan. Composes with provider-aware backoff.
          Implement as its own reviewed change AFTER the batch-repair batch lands.
      (g) FAST BOOT (user report: boot still slow): census diffusion is shuffle-paced (30s
          rounds, first tick skipped → 1-3 min to learn all members) and the readiness gates'
          stability clock resets on each arrival → rides the 90s cap. Fix: send a full
          MemberSync IMMEDIATELY on join/neighbor connection establishment (message already
          exists — no wire change) + fire the first shuffle right after bootstrap join. Census
          completes in ~1 RTT; ready gates settle at the 10s floor. Boot-to-ready ~2-3min → ~15-20s.
      Shipped early (no wire change, commit pending review): REPAIR BATCHING — repair_one mints
      min(deficit, 8) pieces per pass to distinct targets concurrently (was 1/pass; ~2,100
      debris cids × ~90 missing pieces healed at ~30 pieces/min cluster-wide = days; batching
      ≈8×) + PIPELINING (obj/registry/sql serve 8 concurrent streams per conn).
- [x] Phase 4 ATTEMPT 2 (binary ec4356b8, dial dedup+caps): STABILITY PASS — census 20 EVERY
      minute for 13+ min, extras restarts 0 (was 32/5min), OOM kills 0 (was +17), churn 2-4
      lines/min (was ~1500), Mac 5/5 solid, node1 19-42% of 12G budget. The thrash class is
      closed. Throughput bar NOT met (queue ~2200 static, timeout-bound drain) → Phase 5.
- [ ] Phase 4 (acceptance): deploy fleet, rerun 20-node rejoin — PASS = census 20 converges, no
      OOM kills, churn lines near-zero, deploys fast. THEN the original stress measurements
      (writer spread, held-DB counts, remote resolve latency, reshard 8→9 under load).
      ATTEMPT 1 (binary 5fd0fbab): FAILED, slower thrash — cores stable (node1 gauge armed 6G,
      went critical at a transient 5.9G spike that self-deflated to 118MB; raised to 12G), Mac
      solid, but extras still OOM-looped (+17 kills/5min, census collapsed to 8). Learned: (a)
      gauge deferral can freeze the job queue (UI "held jobs") without recovering memory when
      RSS is serve-side, not job-side; (b) remaining balloon driver = UNBOUNDED CONCURRENT DIAL
      ATTEMPTS to dead peers (probe+DHT+pushstate all dial the same dead peer in parallel, each
      holding handshake state 3-8s, never pooled, retried forever). FIX (pending review): per-key
      dial dedup (losers adopt the winner's fresh conn) + global 16-dial semaphore in Transport.

# ISOLATION WATCHDOG: ENDPOINT REBIND (2026-07-09, commit 29f9ce1) — DEPLOYED to all 5 nodes
Fleet roll (binary b9f74279, watchdog string verified in binary on both server + Mac): staggered
restart zeph..zeph4, Mac binary swap + launchd bounce (transient bootstrap IO-error-5, retry OK).
Post-roll: Mac 4/4 active, census eligible=5, shards=256. Census-overview UI (e183de4) shipped in
the same roll (dashboard is include_str-embedded). Gotcha for next deploy: the release binary is
`target/release/zeph` (NOT zeph-noded — [[bin]] name); an install of the wrong path no-ops silently.
Review verdict: design sound; 1 CRITICAL found+fixed — close()/rebind() race (SIGTERM during a
wedge-recovery rebind could install a fresh open endpoint AFTER close() returned, orphaned forever);
fix = re-check `closed` before installing, close the just-built endpoint and bail. Reviewer caveat
(accepted, below threshold): dht/main cache transport.addr() once at startup — only matters on
port=0 nodes, and the Mac (the only port-0 node) is relay-dialed so its usable addr survives rebinds.
Incident (during the 19-node stress test + box freeze): the Mac's long-lived iroh endpoint WEDGED —
after the all-peers outage every recovery dial to known-alive seeds died in 3s for 10+ min while ICMP
on the same path was clean; `noq` errors `MultipathNotNegotiated` + `PTO expired while unset` (all ~5
conns died in the SAME millisecond = local/uplink path event, e.g. hotspot NAT churn). Process restart
reconnected in 15-20s, three times. Membership-level recovery can't fix it (dials go THROUGH the
wedged endpoint). FIX: (1) transport — endpoint behind RwLock + saved BindCfg; `rebind()` closes old
FIRST (frees fixed port), rebuilds identical (identity/port/relays/ALPNs), 10×500ms retries; `serve()`
re-attaches via epoch counter, exits only on `close()`; removed dead `endpoint()` accessor. (2)
membership — `wedge_rebind` (default 120s) + `isolated_since_ms`; when active view empty AND bootstrap
seeds exist AND isolation outlasts the window → transport.rebind() + re-arm seed recovery; full window
between attempts. Solo nodes (no seeds) never rebind. Also fixed pre-existing broken wire test
(Shuffle/ShuffleReply `members` field missing in roundtrip initializers). Gates: fmt/build/clippy(0)/
workspace tests green (transport 5/5 incl rebind roundtrip; membership 4/4 incl watchdog test;
healthscan 15/15 on rerun — earlier fail was parallel-load flake). Docs: ZEPHCRAFT.md §3.4+§4.1.
Memory: zeph-iroh-endpoint-wedge. NEXT: commit, then fleet roll (4 Hetzner + Mac) together with the
census-UI commit e183de4 (still undeployed). Edge accepted: a seed node with no peers of its own never
arms the watchdog (nothing to dial; wedge only ever observed on churn-prone uplinks).

# BACKGROUND-LOOP AUDIT + COMMENT-HYGIENE SWEEP (2026-07-09, commits f7e2a28 + d420794) — DONE
Follow-up to the churn incident: audited ALL 13 periodic loops for unconditional per-tick network
work. 12 clean (TTL-gated / change-gated / local-only / event-drained / bounded+cached / by-design
liveness / steady-state-empty). ONE offender: `distribute()` — an unconditional O(held) concurrent
DHT-lookup sweep every 30s (hundreds of lookups/tick on a loaded node). FIX (f7e2a28): census-gated
via the migrate_round pattern — fires once the census digest is stable 2 ticks after a change (never
during a join storm) + a ~10min heartbeat; scale()/enforce_quota() stay per-tick (no-ops when idle).
COMMENT SWEEP (d420794): purged every verifier-flagged stale comment — headreg module/field docs
(deleted shard_seed fn, WASM-validator prose → SQL/native reality), registry_net blob-era seed
formula, dead REGISTRY_SEED const REMOVED (com), sql KIND_ROOT/tracker/CAS prose, obj publish
`durable` overclaim, dht Phase-1/2 framing + tracker census claim, membership tracker-registry
docstring, noded routing_dht comment / CLI "via tracker" / "Poll the tracker" / committee mentions,
account.rs as-built note, gov.rs committee analogy. No behavior change except the distribute gate.
Deployed to all 5 nodes. Tier-0 comment debt: CLOSED.

# PEER-FLAPPING ROOT CAUSE: SELF-INFLICTED CONNECTION CHURN (2026-07-09, commit a846723) — FIXED + MEASURED
User reported consistent peer disconnect/reconnect on the Mac and (correctly, again) rejected my
packet-loss theory. CONTROLLED TEST proved it: ICMP to Hetzner = 0% loss/≤380ms WITH the node running,
while zeph pings on the same path timed out at 3s (2,860/2,915 failures = timeout; 18× "server refused
to accept a new connection" = connection pressure). Cause: our own QUIC-handshake churn — the
GOVERNANCE TICK did resolve_app (DHT lookup) + 1-2 obj.get(Drop) content fetches for EVERY census peer
EVERY 5s (Drop retains nothing → refetch forever) + unconditional publish/announce per tick; plus
fresh-connection-per-ping probes. Hetzner LAN hides it; the Mac's 260ms RTT amplifies handshakes ~100×
(the canary, not the cause — 3rd incident of this class after member-sync 10s and DHT per-op conns).
FIX: fetch_if_newer (version-gated: fetch only if announced version > local seq+1 → steady-state ticks
do ZERO content fetches), publish_if_due (announce on seq change + 10min heartbeat), tick 5s→30s,
DRAIN_TICKS 6→18 (~180s, matches slower propagation), membership ping retry-once before a failure
counts. MEASURED on the fleet (12-min window): Mac unreachable 31-64 → **3** (−95%), mark-dead → **0**
(the user's symptom eliminated), node1→Mac 23 → **1**, governance intact (seq 6).
Memory: zeph-connection-churn-flapping (the ICMP-vs-app-ping diagnostic + gate-per-tick-loops rule).

# DOCS CONSOLIDATION + PUBLIC SURFACES (2026-07-09) — DONE
One consolidated design doc + website + docs-site, all shipped:
- **`docs/ZEPHCRAFT.md`** (commit 65fcdd6): THE single reconciled design & state document (16 parts,
  ~430 lines dense) consolidating all ~20 design docs against code. Produced by a 9-domain parallel
  extraction workflow (~1M tokens read over docs+crates), synthesized, then ADVERSARIALLY VERIFIED by
  3 independent reviewer agents (numbers/mechanisms/status lenses) — verdicts "very hard to refute";
  all 7 findings fixed (job-priority order Repair>Encoding>Distribution>HealthScan>Eviction;
  NativeProgram exists-but-uncalled; readiness gate bounded-20s-not-absolute; TRACKER_* tags are
  fossils not "carried over DHT"; app-path clock = local time; delete propagation REJECTED-not-
  deferred; 17-crate inventory incl. cipher/events/sched). Part 14 = per-doc supersession map;
  maintenance rule: fix ZEPHCRAFT.md in the same change that lands code.
- **Public stats endpoint** (commit 5895253) + LIVE CUTOVER: api.zeph.craft.ec/stats was the RETIRED
  tracker serving all zeros (nodes stopped announcing; source deleted) — the website's "live network"
  section showed a dead network. Node now serves token-free CORS-open GET /stats on
  --public-stats-port (census-based node count + local store/DHT counts, tracker-compatible schema).
  Deployed: zombie zeph-tracker stopped+disabled, node1 runs --public-stats-port 9947 (Traefik yaml
  untouched), all 5 nodes rolled. LIVE: nodes 5, cids ~905, pieces ~4k, providers ~5.4k — real numbers.
- **Website zeph.craft.ec** (same commit, DEPLOYED via vercel): stats copy no longer credits the
  tracker; new STACK section (the full vertical, all live); nav/footer → docs.craft.ec/zeph; erasure
  floor corrected to as-built (4× survives 75% piece loss); lede/meta mention databases+compute.
- **docs-site docs.craft.ec** (docs-site repo commit 7309bd2, deployed): new /zeph section — index,
  architecture, storage, database, compute, registry-governance, run-a-node, faq (honest not-built
  list); root index reframed (Craftec = infrastructure + apps). Builds clean (29 pages).
- Mac launchd note: `launchctl bootstrap` can fail transiently ("Input/output error 5") right after
  bootout — wait ~5s and retry; ALWAYS check `launchctl list` after (the silent-fail lesson).

# Feature: Kademlia DHT for content routing

Replace tracker-based **content routing** with a Kademlia DHT behind the existing
`ContentRouting` trait. Per foundation §62 + user direction:

- **DHT = all content routing**: provider records (cid→holders), want records, and
  owner-keyed heads (DB root / app / manifest / meta) as **highest-seq-wins signed
  records** (no strict CAS — a DHT has no single authority).
- **Tracker, slimmed = node/relay census + DHT bootstrap** only.
- **No global content enumeration** — `content()` is DROPPED entirely. The dashboard's
  "serving N cids" already counts OUR OWN held pieces (local), not network enumeration.
  Node census stays (DHT routing table + tracker).
- **Fade** uses per-cid want lookups, not global `wanted_cids()` enumeration.

Kademlia params (foundation §3): 256 k-buckets, k=20, α=3, XOR distance on 32-byte keys,
provider records keep `addr` inline (dialable), TTL 48h / republish 22h. Reuse the existing
`SignedRecord` + `records::sign/verify` verbatim. New crate `zeph-dht`, ALPN `/craftec/dht/1`.

## Phases

- [x] **P1 — Overlay core.** DONE. `zeph-dht` crate: k-bucket table (table.rs), DHT protocol
      (proto.rs, own ALPN `/craftec/dht/1`, postcard), `DhtNode` (node.rs) with serve +
      iterative α=3 lookup + bootstrap. 9 tests green incl. a live 5-node overlay test
      (bootstrap + lookup locates a peer known only via the seed). clippy 0.
- [x] **P2 — Record store.** DONE. `StoredRecord` (generic signed key-value envelope,
      Ed25519, verified on store + return, highest-seq-per-publisher, many publishers coexist)
      + `RecordStore` (TTL, expire). `Store`/`StoreAck`/`FindValue`/`Value` messages; node
      `put` (sign → lookup K-closest → Store) + `get` (iterative FIND_VALUE, verify, merge).
      14 tests incl. cross-overlay PUT/GET (node 1 publishes, node 4 fetches). Republish is
      routing-layer policy (re-put every 22h), wired in P3/P4. clippy 0.
- [x] **P3 — `DhtRouting` impl of `ContentRouting`.** DONE (crates/routing/src/dht_routing.rs).
      provider/want/meta keyed by CID (namespaced per kind), many-coexist, monotonic-seq
      re-announce, empty-tombstone withdraw. root/app/manifest owner-keyed, highest-seq/
      version-wins, reads filtered to the owner's signature. census/enumeration return empty
      (tracker serves them in the composite). Test: providers announce/resolve/withdraw +
      coexist, head highest-seq-wins — all over a live 3-node overlay. Routing suite green,
      clippy 0.
## RETIRE THE TRACKER (re-planned 2026-07-05)

Decision: retire the tracker service AND `TrackerRouting` entirely. `ContentRouting` becomes
pure-content, `DhtRouting` its ONLY impl. `CompositeRouting` deleted (nothing to compose).
- **Content** (provider/want/meta/root/app/manifest) → DHT.
- **Census / liveness** → SWIM membership (real-time, in-network; NOT the governance chain).
- **Bootstrap** → seed peer addresses in config.
- **Relays** → relay URLs in config (already mostly there); drop the dynamic relay registry.
- **Fade** → per-cid `is_wanted(cid)` replaces `wanted_cids()` enumeration.
- **content()** → gone (dashboard is local).

- [x] **P4a — Composite.** (superseded — CompositeRouting will be DELETED, not used.)
- [x] **P4b — Trim the trait + membership census.** ContentRouting → content-only
      (drop nodes/relays/announce_node_registry/announce_relay_registry/content/wanted_cids;
      add `is_wanted(cid)`). DhtRouting: add is_wanted. Rewire census callers (obj candidate
      peers, dashboard) to membership. Fade → per-cid is_wanted. Delete CompositeRouting.
      DONE — confirmed by reconciliation 2026-07-08 (trait is 16 content-only methods, is_wanted
      required, DhtRouting sole impl, CompositeRouting deleted).
- [x] **P4c — Wire DhtRouting into noded + seed bootstrap.** DONE (flag-gated). routing_dht +
      dht_seeds config (OFF by default); DhtNode construct/serve, DHT ALPN, bootstrap from
      seeds, routing=DhtRouting, MembershipPeers (peers.rs) as the PeerSource. Republish rides
      the re-announce loop; hourly expire. VERIFIED on the Mac: flag-off identical; flag-on the
      overlay bootstraps + publish/get/health-scan work over the DHT. Reverted Mac to flag-off.
      (noq PoisonError on abrupt-shutdown is a pre-existing dependency issue, not P4c.)
- [x] **P5a-c — Migrate the cluster to the DHT.** DONE. 5-node cluster resolves + repairs
      entirely over the DHT; no tracker in the routing path. Stability hardening done (unified
      job manager, hysteresis band, record-store persistence). Tracker still CONSTRUCTED as a
      fallback + all tracker code still present.

## RETIRE THE TRACKER — code deletion (2026-07-06)

Surface map (agent): NO CompositeRouting, NO content() (both already gone). Two impls only:
TrackerRouting (delete) + DhtRouting (keep). Trait census methods `nodes/relays/
announce_node_registry/announce_relay_registry/wanted_cids` are REQUIRED (no default);
`is_wanted` has an enumerate-default that DhtRouting already overrides. Census callers:
obj RoutingPeerSource.nodes (→ MembershipPeers, already the DHT-path source), noded seed loop
(dead on DHT), sql net.rs owner_addr (needs membership+resolve), ObjEngine announce_node/relay
(drop). ~13 test files build TrackerRouting as shared routing + peer census.

- [x] **P5d-1 — Test double.** DONE (commit test:...). MemNet/MemRouting/MemPeers in zeph-testkit; 13 harnesses migrated; tracker.rs deleted; healthscan 15/0, com 55/0.
- [~] **P5d-1 (orig text) — Test double.** MemRouting (shared in-mem ContentRouting) gated
      `#[cfg(any(test, feature="test-support"))]` in zeph-routing; MemPeers (shared PeerSource)
      same in zeph-obj. Migrate the ~13 harnesses off TrackerRouting → MemRouting + MemPeers
      (ObjEngine::with_peer_source). DELETE routing/tests/tracker.rs (the only real tracker test).
      Exit: all suites green with zero TrackerRouting refs in tests.
- [x] **P5d-2 — Production rewiring.** DONE (b82c6b8). DhtRouting+MembershipPeers unconditional; owner_addr→PeerSource (remote-fetch fix); tracker construction/seed/announce removed. obj: MembershipPeers unconditional, delete
      RoutingPeerSource. noded: DhtRouting unconditional, remove tracker construction + seed loop
      + announce_node/relay calls. sql net.rs owner_addr → membership snapshot + resolve fallback.
      Exit: build green, cluster redeploy stays healthy.
- [x] **P5d-3/4/5 — DONE (aa1da52).** Deleted TrackerRouting+server.rs+registry.rs+apps/tracker; trait trimmed to 16 content-only methods (is_wanted required); DhtRouting sole impl; dead record kinds + noded --tracker/trackers removed. (dead after -1/-2).
- [x] **Restart overlay gate (e25ed94).** First scan waits for the Kademlia routing table to
      settle (not just membership) — flattened core-restart at_risk transient 182→30 peak, 7x
      less false repair. FEATURE COMPLETE: tracker fully retired, cluster DHT-only.

## Notes / decisions
- Provider records carry `addr` inline → resolve returns dialable providers (no separate
  NodeId→addr discovery needed for providers).
- DHT routing-table contacts carry `PeerAddr` → dialable during lookups.
- Heads: highest seq/version wins; single-writer-per-identity makes same-seq races rare.
- 22h republish (foundation), NOT the old 6s reannounce.


---

# Feature: Open owner-signed registry + verification substrate (updated 2026-07-07)

Two SEPARATE tracks, settled through a long design pass. **Design docs are the source of truth:**
`docs/VERIFICATION_DESIGN.md` (new), `docs/ATTESTATION_DESIGN.md` (revision banner 2026-07-07),
`docs/REGISTRY_DESIGN.md` §2.1 (patched 2026-07-07). Memory: `zeph-attested-registry-notes`.

## Settled facts (do NOT re-litigate)
- **No incident.** app-registry v2 (char-limit) is LIVE — deploying a >32-char name is rejected;
  governance is durable, the v2 SetProgram is intact. The earlier "revert" was my misread of a
  program-registry version field. Verify behaviour empirically (deploy test) before diagnosing.
- **Attestation is CONSISTENCY-only** — not authority (owner signature), not arbitration
  (governance), not durability (erasure-coded storage).
- **Open registries do NOT use attestation.** app / DB-root / manifest / meta are all owner-signed
  CRDTs (partition-by-owner, last-writer-wins per key) — they converge by construction, nothing to
  verify. Attestation is only for consistency-critical state (shared counter/quota/balance).

## Terminology convention (2026-07-07) — everything is a "program", drop "app"
Applied as a TARGETED rename sweep AFTER the attestation removal (they touch the same files; do not
run concurrently). Not a blind `s/app/program/` (that mauls `append`/`apply`/`happen`).
- **Everything the network runs is a PROGRAM** (WASM). "app" is retired.
- **Protocol Program Registry** — governance-controlled: which WASM is canonical for each protocol
  program / anchor. Old `program_registry()` (gov.rs) → `protocol_program_registry()`.
- **User Program Registry** — owner-deployed `(owner, name) → cid`, owner-signed CRDT. Old
  app-registry / `AppRegistry` / `appreg.rs`.
- **Runtime namespaces:** `protocol_program.<ns>` / `user_program.<ns>` (replacing `app.<ns>`).
- Identifier renames: `AppRegistry`→`UserProgramRegistry`, `appreg.rs`→`user_program_registry.rs`,
  `program_registry()`→`protocol_program_registry()`, `KIND_APP`→`KIND_USER_PROGRAM`,
  `announce_app`/`resolve_app`→`announce_program`/`resolve_program`, "deploy a … app"→"deploy a
  program", webui "user apps"→"user programs". Docs to sweep: CRAFTCOM, REGISTRY, VERIFICATION,
  MINIMAL_KERNEL, ATTESTATION, CLAUDE.md, webui.

## Directive (2026-07-07) — remove attestation entirely, build the anchor, rework the app
Per user: **TOTALLY REMOVE** the k-of-n committee attestation — it needs a ground-up rework as the
open-board verification substrate (`VERIFICATION_DESIGN.md`), a **replacement not an evolution**.
Rework the app-registry to the open model behind the anchor; the new validation WASM redeploys via
governance later. Keep the binary MINIMAL (`MINIMAL_KERNEL_DESIGN.md`); set up the WASM-integration
**anchor**.

**KEEP / REMOVE boundary:**
- **KEEP:** the deterministic WASM program runtime (run a program on `(prev,request)→new_state`) —
  reframed as the **anchor runtime**; the registry program *logic* (`RegistryState`/validation);
  `pda`/`registry_program_cid`/`REGISTRY_SEED`/`HeadSubmission`.
- **REMOVE:** `select_committee`, `verify_quorum`, `attest_transition`, `AttestRequest`/
  `request_attestation`, `AttestedCommit`, `CommitteeChain`, `AttestService` committee-orchestration,
  `ATTEST_ALPN` + handler, attested accounts (`noded/account.rs`), `noded/committee.rs`,
  `control.rs` `api_attestation` + committee status, appreg `coord`/`committee_status`/`mode`/
  `set_coordinator`, the attestation tests.

### Phase 1 — Rip out the attestation subsystem (keep the WASM runtime)
- [x] **Drop attestation from the app-registry WRITE path.** DONE 2026-07-07 — `register()` runs the
      program locally (no committee), `try_committee` + dead imports removed, char-limit still fires,
      build/clippy clean.
- [x] **Remove the com attestation machinery** — `attest.rs` committee/quorum/chain + `coordinate.rs`
      committee orchestration; **split out and keep** the WASM runtime + `NativeProgram` +
      `run_transition`.
- [x] **Remove the noded wiring** — `noded/committee.rs`, `noded/account.rs` (attested accounts),
      the `ATTEST_ALPN` handler (`main.rs`), `control.rs` `api_attestation`, the webui committee panel.
- [x] **Remove/rework the attestation tests** (`com/tests/coordinate.rs`, `registry_live.rs`).
- [x] **Clean appreg vestiges** (`coord`/`committee_status`/`mode`/`set_coordinator`) — the
      membership handle moves to the sync path (phase 3).
- *Gate:* MET 2026-07-07 — 5 files deleted (coordinate/account/committee + 2 tests), attest.rs split
  (runtime + pda kept, NativeProgram relocated to registry.rs). `cargo build/clippy --workspace` clean,
  27 zeph-com tests pass, char-limit `rejects_an_overlong_name` passes, 0 residual attestation symbols.
  REMAINING: webui still shows the dead `/api/attestation` panel (5 refs) — folded into the rename sweep.

### Phase 2 — The anchor (minimal kernel, the WASM-integration point)
- [ ] **Generalize `run_program` into a named ANCHOR:** the kernel resolves the anchor's program cid
      via the governance program registry and runs it (fuel-bounded) with a **native-default**
      fallback. One generic primitive (`MINIMAL_KERNEL_DESIGN §3, §6–7`) — sane default + anti-brick
      + per-epoch decision cache.
- *Gate:* an anchor resolves to its native default at genesis; a governance `SetProgram` swaps a
  WASM program; a missing/failed/fuel-exhausted program falls back to the default (never bricks).

### Phase 3 — Rework the app-registry as the first anchor consumer (open CRDT)
- [ ] **Open-registry MECHANISM in the kernel:** owner-signed rows (carry the sig), anti-entropy
      UNION-merge / LWW-per-`(owner,name)`, resolve LOCALLY. Drop `announce_app`/`resolve_app` (owner
      pointer); coalesce-to-latest; per-row storage (not the O(N) blob) at scale.
- [ ] **Validation via the ANCHOR** (governance program, native default). The real validation WASM
      (char-limit) redeploys via governance later — "the new app".
- *Gate:* resolve an app AND a DB root with the owner node OFFLINE; validation runs via the anchor.
- Extends to DB root / manifest / meta — same substrate.

## Program-account substrate — the fresh design (2026-07-07, user-confirmed)

REVAMP: not registry-specific. Build the GENERIC substrate; the registry is one consumer.
`account = pda(program_cid, seed)` → a single-writer account. **THE PROGRAM IS THE WRITER** —
its deterministic execution IS the write authority (validates the request, decides new state).
NO owner key, NO committee, NO attestation, NO gossip. Durability = CraftSQL/CraftOBJ (content is
erasure-coded — the DB *is* the durable layer, so no replication). Multi-account by seed (as many
as you want, any purpose). Writes to SQL + object. Reads direct (derive address → read).
Aligns with MINIMAL_KERNEL: kernel = the account mechanism; each use = a program on top.

The build is a SUBTRACTION from the deleted `account.rs` (recovered from 634ee25^) — strip the
committee, leaving a pure program-executed writer.

- [x] **Step 1 — the substrate + RPC/CLI.** DONE 2026-07-07. `crates/noded/src/account.rs` = `ProgramAccountStore`
      (WRITTEN): `open(obj,data_dir)`, `advance(program_cid,seed,request)` (run program → persist →
      publish durable content), `resolve`. No identity/routing/committee. Wiring (mod + construct +
      `program-advance`/`program-resolve` RPC/CLI) in progress. Gate MET: `program-advance`/`program-resolve` RPC+CLI wired (control.rs, main.rs); build + clippy clean, 0 warnings; char-limit test passes. appreg untouched.
- [x] **Step 2 — registry as consumer.** DONE 2026-07-07. Migrate the registry to `store.advance(REGISTRY namespace,
      seed, submission)`. NOTE the account address must derive from a STABLE program-namespace id,
      not the governance-upgradeable cid (else an upgrade orphans the state) — resolve the executing
      program separately from the address. State moved to accounts/<pda>.state (fresh on redeploy). `appreg`→`programreg`,
      `AppRegistry`→`ProgramRegistry`; thin store consumer; store `advance(program_id, code_cid, ...)`
      splits stable address from executing code; deploy path fully off-DHT (version via
      `current_version`, announce dropped). Invoke cross-node keeps a KIND_APP fallback until 4b.
      Build/clippy/27 com tests clean.
- [ ] **Step 3 — SQL-backed account state** (CraftSQL DB per account, `SELECT` resolve) — replaces
      the state blob; the query surface + per-row scaling.
- [x] **Step 4 (4b) — non-DHT cross-node resolution. DONE 2026-07-07.**** Governance/config
      `registry_writer` (default None → self-writer). One authoritative writer holds the global
      registry account; non-writers forward Submit + query Resolve over a new REGISTRY_ALPN
      (/craftec/registry/1), mirroring the removed committee ALPN request/serve pattern. Closes the offline-owner gap. registry_net.rs (ALPN + client), programreg serve/
      writer-dispatch, main.rs wiring. Build/clippy/27 com tests clean. FOLLOW-UPS: (1) resolve has
      no cache — queries the writer each time; (2) current_version is still LOCAL, so a NON-writer
      RE-deploy computes a stale version (first deploys fine); make it cross-node or deploy on the
      writer. Original note: — how a reader gets another account's latest root
      without an owner-announced DHT pointer (the one genuinely open piece). Today: local resolve +
      durable publish; cross-node deferred.
- [x] **Step 4 (4c) — DETERMINISTIC PER-EPOCH WRITER ELECTION (rotating writer). DONE 2026-07-07.**
      Replaced the fixed `registry_writer` config with a computed rotation: `writer(epoch)` = the
      eligible member (self + membership.active) with the smallest `blake3(epoch_le ‖ node_id)`;
      `epoch = clock.now().millis() / EPOCH_MILLIS` (30s). `is_writer`/`writer_addr`/`current_writer`
      are computed, not configured. HANDOFF: on becoming a NEW epoch's writer, `ensure_current()`
      fetches the previous writer's full state via `RegistryReq::GetState`→`RegistryResp::State`
      (new `ProgramAccountStore::put_state` adopts it) before advancing/resolving. Removed the
      `registry_writer` config field + `writer` struct field; clock passed into `open`. Election in
      `programreg::elect`/`current_writer`; handoff in `programreg::ensure_current` (called at top of
      `advance_local`/`resolve_local`). Build/clippy/com tests clean.
      EDGE CASES (accepted, not over-engineered — also inline code comments): (a) clock-skew races at
      epoch boundaries can briefly yield two writers → a write may be lost in that window; (b) if the
      previous writer is unreachable at handoff, keep local/last-known state (best-effort); (c) the
      FULL state is transferred each rotation — fine while small; later hand off the cid + fetch lazily.

## Track B — Verification substrate (the attestation REWORK, deferred)
Fresh ground-up per `VERIFICATION_DESIGN.md`: open request board + cooldown-rotated verifiers;
`verify` (k=1) / `attestation` (k-of-n / whitelist / open); pure-function boundary; no
self-verification. **Nothing of the removed committee code is reused except the WASM runtime.**
Build only when a consistency-critical app needs it. Deferred layers: Sybil-weighting, credit economy.

## Non-issues (do NOT re-open)
- Committee-chain re-genesis: **MOOT** — the committee is being removed.
- Persisting `AttestedCommit` / verify-against-chain: N/A — removed.
- A "durable DHT head record": **rejected** — heads live in the open registry, not a pinned DHT record.

## Registry follow-ups (2026-07-07) — the distributed registry is BUILT + committed
Full design as-built: `docs/REGISTRY_DESIGN.md §0`. Substrate + sharded rotating-writer registry +
cross-node resolution shipped (zephcraft commits: substrate, rework, rotating writer, sharding).
- [x] Non-writer re-deploy version — DONE (current_version routes to the shard writer).
- [x] Boundary-race — grace window (2s) shipped; deterministic boundary while skew < grace.
- [x] **Read-caching** — DONE (commit 3e2683a). `ResolveCache` TTL's (RESOLVE_CACHE_TTL_MS=3s) the
      resolved `(rtype,owner,name)→(cid,version)` for NON-replica reads (a replica reads authoritative
      local state); `register()` invalidates the key (read-your-writes). Extracted w/ injected clock,
      unit-tested (TTL, key-isolation, invalidate). Takes a hot shard's writer tens→thousands of readers.
- [ ] **Live cluster test** — redeploy the new binary; deploy an app on node A; resolve it from node
      B with A OFFLINE; confirm it resolves via the shard's rotating writer (no DHT). The real proof.
- [ ] **SQL-backed per-shard state** — today each shard's state is a postcard blob; move to a
      CraftSQL DB per shard (SELECT resolve, page-level durability, per-row scale).
- [~] **Dynamic re-sharding** — the one hard bit: changing the shard count on a live network without
      dropping keys, via power-of-two split/merge (bits→bits±1). Phased:
      - [x] **B1 routing foundation** (commit 6c316a8) — SHARD_COUNT const → runtime `shard_bits` field +
            `shard_count()`; `shard_of(owner,name,bits)` routes to the LOW `bits` of the key hash
            (bits=8 == old %256, behavior-preserving, NO cutover). Low-bit routing makes split LOCAL:
            shard s's keys go only to children s and s|(1<<bits). Live count in RegistryStatus + dashboard.
            Unit-tested prefix-stability invariant. All ShardKey sites still fixed at shard_bits=8.
      - [x] **B2 cluster agreement on `bits`** — DONE. `shard_bits` is now a GOVERNED value, agreed
            cluster-wide via the governance chain (minimal-kernel: policy in governance). Built on the
            pre-existing inert `GovAction::SetConfig{key,value}` stub: added `ConfigRegistryState`
            (com/registry.rs, mirrors ProgramRegistryState — i64, monotonic-version upsert),
            `GovernanceChain::config_registry()` fold (gov.rs), `GovernanceChainStore::resolve_config()`
            (governance.rs), and a `set_config` arm to `parse_gov_action` (control.rs `gov-propose`).
            `HeadRegistry::shard_bits` is now an async governance read (fallback DEFAULT_SHARD_BITS=8,
            clamped to [1, MAX_SHARD_BITS=12] so a bad value can't blow up the O(2^bits) loops); read
            ONCE per op and threaded into `shard_of`. Transition window: the key-routed wire requests
            (Submit/Resolve/CurrentVersion) now CARRY the submitter's `bits`, and the writer routes with
            the SUBMITTER's bits (not its own), so a `shard_bits` change in flight can't split-route a
            key. Behavior-preserving at bits=8 (governance unset → default 8 → identical routing). Unit
            tests: config-registry upsert/resolve/stale + chain SetConfig fold. WIRE CHANGE → roll ALL
            nodes version-consistent before deploy. NOTE: at a FIXED bits this is fully correct; it does
            NOT yet migrate state on a bits change — that's B3 (state doesn't follow until then).
      - [x] **B3/B4 ONLINE RESHARD — BUILT (2026-07-08).** (Superseded the brief wipe-and-restart
            close-out from commit b58f9c9: the user first said "wipe is fine" — I over-narrowed to the
            cutover question — then clarified they DO want live online resharding. Built it.) A live
            cluster now changes `shard_bits` via governance with NO wipe; keys migrate. Three tested
            batches (one commit):
            - **A — addressing:** `ShardKey` carries the shard-count GENERATION (`bits`); `shard_seed`
              folds it, so `(rtype,8,5)` and `(rtype,9,5)` are DISTINCT accounts (a reshard reads the old
              generation and writes the new without clobbering). Election (`replicas`) deliberately
              ignores `bits`, so a shard number keeps a stable replica set across generations (parent `s`
              and child-0 `s` share replicas → migration locality). `GetState`/`PushState` wire carry
              `bits`. Behavior-preserving at fixed bits.
            - **B — split/merge:** `reshard_round` (new anti-entropy job in the 10s serve loop, gated on
              a persisted per-node generation marker `GEN_MARKER_SEED` so it's a no-op while the count is
              stable) re-buckets every head this node holds at the OLD generation into the NEW
              generation's accounts (pure `rebucket_entries` + `RegistryState::merge_entries` in com) and
              pushes to the new owners. Merge-forward (old generation left intact → both resolve during
              the window), idempotent, at-least-once (marker saved after a full pass). Handles
              grow/shrink/multi-step uniformly (re-routes each key at the target count).
            - **C — transition reads:** `resolve_entry` refactored to a per-generation `resolve_at_bits`;
              on a miss at the current generation it reads through to the ADJACENT generation (bits±1), so
              a resolve survives the in-flight migration window.
            Tests: `shard_seed_is_distinct_per_generation`, `rebucket_routes_every_entry_and_splits_parent_into_two_children`
            (+ the earlier routing/clamp tests). Build+clippy+com(35)+workspace green. WIRE CHANGE (bits on
            GetState/PushState) → roll all nodes version-consistent.
            KNOWN WINDOW (documented, accepted pre-prod): a write landing on the OLD generation AFTER a node
            has migrated isn't swept forward again — visible only to old-count readers until its writer
            moves to the new count (bounded by governance-propagation seconds; softened by the read-through).
            NOT YET (future, if needed): continuous re-bucketing until the old generation quiesces; old-gen
            account GC after a reshard settles; a live-cluster grow-then-shrink integration test on hardware.
      - [x] **B3/B4 PROVEN ON THE LIVE CLUSTER (2026-07-08).** Deployed all 5 nodes (4 Hetzner + Mac) on
            the reshard binary + added `gov-propose --set-config key=value` CLI (commit ffabafe). Ran the
            grow: `gov-propose --set-config shard_bits=9` on the Mac governor → governance propagated to
            all 4 Hetzner nodes (gov seq 0→1) → each node's reshard_round split 8→9 → node1 shard_count
            256→512 cluster-wide (~50s) → the pre-registered `reshardtest` head (cid 0623371b) STILL
            resolved cross-node from node2/3/4 AND appeared in entries_global at bits=9 (i.e. physically
            re-bucketed into its gen-9 account, not just read-through). No wipe, no downtime for the key.
            NOTE the binary upgrade itself performed the one-time seed-format cutover (old no-bits accounts
            orphaned → the pre-existing 49 heads went unresolvable; expected, user pre-approved wipe). Fresh
            deploys on the new binary migrate cleanly.
      - [x] **SHRINK (merge) ALSO PROVEN LIVE (2026-07-08).** Grow-then-shrink now fully validated.
            Deployed `mergetest` at bits=9 (exists ONLY in a gen-9 account), then `set-config
            shard_bits=8`: governance propagated to all 4 Hetzner nodes in ~10s (the seq 1→2 transition —
            fast, since the announce-version fix lets it supersede), shard_count 512→256, and `mergetest`
            (a) resolved cross-node AND (b) appeared in entries_global at bits=8 — proving it was physically
            MERGED from its gen-9 account down into a gen-8 account, not merely read-through. `reshardtest`
            regression-clean. So both directions work: split (parent→2 children) and merge (2 children→parent).
      - [x] **GOVERNANCE PROPAGATION BUG found + fixed during the live test (commit b14461d).** The live
            test EXPOSED a deterministic (NOT network) bug: `governance publish()` announced the chain head
            at `seq.max(1)`, flooring both genesis (seq 0) and the first change (seq 1) to DHT record
            version 1. The DHT record store rejects an equal seq (record.rs: `existing.seq >= rec.seq`), so
            the seq-1 record never superseded the genesis record → peers forever resolved genesis → the
            FIRST governance change (0→1) never propagated. (I initially misattributed this to the Mac's
            relay flakiness; user correctly rejected that — it was code. See memory
            attribute-failures-to-code-not-environment.) Fix: announce at `seq + 1` (monotonic, never 0).
            After redeploying the fixed binary to the Mac (sole seq>0 publisher), all 4 Hetzner nodes
            adopted seq 1 within ~50s and the reshard fired. Higher transitions (1→2, …) were never
            affected; only the 0→1 step.
- [ ] **Fuller boundary hardening** — replace the grace heuristic with a short writer lease if
      clock-skew guarantees prove insufficient in practice.
- [ ] **rows()/summary()** are now per-node partial views (only shards this node writes) — a proper
      network-wide snapshot would query across shard writers (UI concern, low priority).

## Cluster test PASSED (2026-07-07) — writer-offline gap CLOSED
Live 5-node cluster (4 Hetzner + Mac governor), all on the new binary. Validated end-to-end:
- [x] **Cross-node resolve** — deploy on node1, resolve from node2/3/4 (baseline). Works.
- [x] **Offline-owner resolve** — stop node1 (owner+writer), resolve node1's program from node2/3/4 → all return the correct cid. THE GAP IS CLOSED. (First cluster run returned "not found"; fixed by K-successor replication + resolve fallback.)
- [x] **Replication confirmed** — a deploy's state lands on K=3 nodes (verified via accounts/ state files).
What made it work (all committed): native default (fresh net self-starts) + type-in-seed + K-successor replication (writer rotates among a stable K, push-on-write, merge-on-takeover) + resolve robustness (3s request timeout + self→writer→replica fallback).
### Findings from the live run (real, worth remembering)
- **Heterogeneous binaries break the registry**: the Mac node, left on the OLD binary, stayed in membership, got elected registry writer for a shard, and its incompatible ALPN + missing state made ALL resolves for that shard fail. Fix = keep the cluster on one binary (updated the Mac, kept its governor identity). Rollout lesson: registry participants must be version-consistent.
- **node4 transiently "not found"** right after the kill = membership convergence lag; resolved once SWIM dropped node1 (the fallback then reaches a live replica). Expected.
### Still open (NOT the registry's job / minor)
- [ ] **Content durability with < 8 nodes**: the 16KB program WASM was below the 8-peer erasure floor on a 4-node cluster, so it lived only on the owner → `invoke` (which fetches content) can't run it offline. Registry resolve is fine (that's why we test with the resolve-only CLI). A real network (≥8 nodes) replicates content durably. Separate from the pointer work.
- [ ] Mac node is one commit behind (d4be8de vs 84d17d6) — ALPN-compatible, functional; update when convenient.
- [ ] Read-caching still deferred (resolve now reads locally when self is a replica — partial).

## Compute execution — unified runtime (2026-07-07, DESIGN pinned)
Design doc written: `docs/COMPUTE_EXECUTION_DESIGN.md`. Settled (with the user) that the two runtimes
(transition `AttestedRuntime` + capability `Runtime`) are an ACCIDENTAL split, not two program
classes — the registry (a protocol program) legitimately wants CraftSQL, and SQL is deterministic,
so the real boundary is `clock`/randomness, not `sql`/`obj`. TARGET: ONE runtime + per-program
capability grant; consensus programs get the deterministic subset (no wall-clock/random); apps get
the full set. Industry-standard (WASI/wasmtime, CosmWasm, Substrate, EVM — one VM, determinism by
denying non-determinism, consensus/block clock). Phased migration in the doc §11:
- [x] **Phase 0 (cosmetic, do first):** rename `AttestedRuntime`→`TransitionRuntime` (attest.rs→transition.rs, AttestCtx, ATTEST_MODULE); scrub stale "committee/attested" comments in registry.rs + apps/registry-wasm; DELETE orphaned `apps/counter-wasm` demo. No behavior change. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 1: unified host surface + link-time capability binding (default = deterministic profile). DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 2: absorb capability runtime's sql/obj/clock/caller as grantable; reconcile guest ABI. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 3: re-point `zeph invoke` (read) + substrate `advance` (write) onto one runtime; retire capability Runtime. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 4: consensus clock (time from request HLC for deterministic profile). DONE — confirmed by reconciliation 2026-07-08.
Note: the counter deploy timeout I hit was ALSO fixed (request timeout 3s->8s, commit 73eda29) but
the Hetzner cluster still runs the pre-that binary; redeploy when convenient.

### Phase 2 DONE (2026-07-07) — runtime merged into one grant-gated async runtime
- 2a (d7dc10a): transition runtime -> async + TransitionCtx extended (caller/app_ns/backend). Behavior-preserving.
- 2b (bdeb2e9): ported sql/obj/caller/clock as GRANT-GATED host fns; clock=WallClock (per-node HLC, non-det), deterministic() dropped Clock; backend=Option (None->-1, no panic); run_program(ctx) core + run_transition convenience; ABI = run()->()+commit (2c satisfied). Capability Runtime + invoke.rs UNTOUCHED.
- The transition runtime is now THE unified runtime (10 host fns, grant-gated). 38 com tests pass incl. new capability gate tests.
- [x] Phase 3 DONE (181bae7): migrate `zeph invoke` (InvokeService) + substrate onto the unified runtime; DELETE the capability Runtime + its bind_host_functions; invoke reads committed output (not i64). This is where the old runtime finally goes.
- [x] Phase 4: consensus clock (deterministic Clock from request HLC) + gate WallClock to full profile. DONE — confirmed by reconciliation 2026-07-08.

### Phase 3 DONE — ONE runtime. Capability Runtime deleted (-418 net lines). invoke returns committed bytes (run()->()+commit); 3 integration tests migrated to the new ABI + pass. Only Phase 4 (consensus clock) remains.

### COMPUTE_EXECUTION DESIGN COMPLETE (2026-07-07) — phases 0-4 all built + committed
One WASM runtime; per-program capability grant; deterministic subset (clock=consensus/ctx.now, wall_clock=app-only); capability Runtime deleted; invoke returns committed bytes. Commits: 1e9a9ba(0) 76dabef(1) d7dc10a(2a) bdeb2e9(2b) 181bae7(3) 43d2ebf(4). All com tests green. Remaining future work (separate): verifier re-run reproducibility (persist now in request), a real capability-app demo, and the invoke ABI is now committed-bytes (any old app must use commit).

### Deploy/write speed FIXED (2026-07-07): 10s -> ~40ms, all 5 nodes updated
Root cause (found by INSTRUMENTING, after several wrong registry guesses): CraftOBJ publish awaited join_all of every piece-push, so any publish (deploy wasm, registry state, CraftSQL page-commit) blocked on the slowest peer — the Mac on a hotspot relay stalled it ~10s. Chain of fixes (all committed + deployed to all 5 nodes):
- 4a90ca5 fire-and-forget replica pushes; 7a548e0 PUSH_TIMEOUT 10s->3s + async registry publish; 8ee918c deploy wasm publish backgrounded; 9fef5df exclude slow (rtt>150ms) peers from writer/replica election; b749c49 fire-and-forget apps_add (the app-index CraftSQL write = the real 2.4s bottleneck).
- fcc07e7 THE ROOT FIX: CraftOBJ publish retains locally SYNC then SPAWNS distribution + returns the cid immediately (cid is BLAKE3, retain is local, distribution is only for durability = async). push_piece/request became free fns; 14 tests adapted with bounded polls. Now EVERY write is fast, not just deploy-path callers.
Note: the per-caller fire-and-forget spawns (deploy wasm, account.rs publish, apps_add) are now redundant given fcc07e7 but harmless; could be simplified later. Also: DEPLOY_TIMING instrumentation was removed. Pre-existing noq QUIC teardown SIGABRT under parallel obj tests -> use --test-threads=1 (already the project rule).

### DB roots + manifests on the registry substrate — DONE + PROVEN (2026-07-07), all 5 nodes
Decision settled: foundation §62 A3 (DB roots off the registry) SUPERSEDED by REGISTRY_DESIGN §2.1 — its objection assumed the ATTESTED registry's quorum bottleneck, which is void now the registry is a sharded rotating-writer CRDT (per-owner keys never contend). Commit 081a272.
Phase 1 (57574f0): CraftSQL root+manifest heads publish/resolve through the HeadRegistry (RT_DBROOT/RT_MANIFEST) instead of the DHT KIND_ROOT/KIND_MANIFEST path. programreg register/current_version/resolve_entry take an rtype; resolve surfaces (cid, version); resolve(owner,name) kept as an RT_PROGRAM shim. New registry_heads.rs: RegistryRootStore/RegistryManifestStore impl zeph_sql's RootStore/ManifestStore over the registry (stale-version->Conflict; single-writer LWW-by-seq, prev ignored — the DHT backend already ignored prev). main.rs builds the registry before CraftSQL, drops the redundant reannounce_heads.
Rename (72508cd): ProgramRegistry->HeadRegistry, programreg.rs->headreg.rs (holds programs+roots+manifests now).
PROOF on the live cluster: a fresh guestbook2 DB counted 1,2,3 across invokes, then RESTARTED node1 (clears the in-memory root cache) and the next invoke returned 4 — the DB reopened by resolving its root through the registry, not the DHT.
- [x] REMAINING — phase 2 (cleanup, not wired to anything live): delete the now-dead DHT publish_root/resolve_root/withdraw_root/publish_manifest/resolve_manifest (ContentRouting trait + dht_routing impl), RootRecord/ManifestRecord/RootPayload/ManifestPayload/KIND_ROOT/KIND_MANIFEST, RoutingRootStore/RoutingManifestStore in sql, CraftSql::reannounce_heads; adapt testkit mock + obj/tests/healthscan.rs (DHT root-CAS test). DONE — confirmed by reconciliation 2026-07-08 (dead root/manifest DHT funcs, records, payloads, KIND consts, and Routing*Store all removed; only historical doc mentions remain).

## SHARD-PAGE ERASURE DURABILITY RESTORED (2026-07-09, commit 2942cf3)
Dropping ObjDurable from the shard engine (during the SQL-registry build) was a durability REGRESSION
(the old blob registry erasure-coded every state via publish_system) AND a band-aid over the real bug:
the shard-DB namespace `reg/<rtype>/<bits>/<shard>` contained SLASHES, so CraftSQL's per-DB durability
sidecar path `store_dir/<owner16>_<ns>.gens` became a NESTED path whose parent dirs don't exist →
save_manifest failed 'No such file' → the durability sweep failed → CraftDb::write propagated it (`?`)
and failed the write. ObjDurable-off masked it (sweep returns early). User DBs use slash-free namespaces,
so never hit it. FIX: ns_of slash-free (`reg_<rtype>_<bits>_<shard>`) → flat `.gens` sidecar → re-added
with_durable(ObjDurable). Shard pages now get default erasure durability (k=8/n=32 changed-page coding +
distribute + repair) on top of K-replica row-push. Proven single-node: deploy succeeds w/ durability,
`.gens` sidecars written, resolve works. Namespace change = a cutover (wipe, accepted).
FOLLOW-UP BUG found while verifying erasure (commit 1a55f00): `shard_db` created a DB + published a
root on ANY access including READS, so read paths (resolve/sql_state/serve handlers) created empty DBs
+ roots for every accessed shard; the held-index backfill then counted those roots and snowballed
`held` toward all 2^bits shards each restart (observed ~768 DBs, writer_shards≈full) — DEFEATING the
O(held) loops. Fix: `shard_db_existing` opens only if a root exists (no create); all READS use it,
only sql_upsert (a real write) creates + held_add. CLUSTER (wiped fresh, all 5 rolled): 0 DBs before
any deploy, 1 DB per replica after one deploy (not ~768), resolve works, `.gens` written (erasure
active). O(held) is now genuinely effective + erasure durability restored. DONE.

## REGISTRY READINESS GATE — post-restart resolve/register transient (2026-07-09, commit 402f26d)
MEASURED first (user's instinct confirmed): in steady state deploy→resolve is instantly consistent from
every node; the "not found" transient occurs ONLY in the post-restart convergence window — a freshly
(re)started node's census is still growing, so its writer election differs from the settled cluster →
it routes resolves to the wrong node (miss) or lands registers on the wrong writer. FIX (mirrors the
health-scan restart gate the user pointed to): a one-way `ready` latch flips once the census member
count has been UNCHANGED for READY_STABLE_SECS(10) (bounded by READY_MAX_SECS(90)); register/resolve/
current_version `wait_ready()` first (bounded READY_WAIT_SECS(20); a no-op once ready). PROVEN LIVE:
restart node2 → immediate resolve WAITED 16s then returned the CORRECT cid (was: instant wrong
"not found"); once converged, resolve is instant again (0.022s, no steady-state regression). eligible=5.

## O(shards)→O(held) REGISTRY LOOPS (2026-07-09, commits f4db195 + 7e3e247)
Lifted the last scaling ceiling: status/migrate_round/sweep_generation/gc_generation/rows/summary/
local_head_rows scanned all 2^bits shards (is_writer/is_replica per shard) → O(2^bits). Now a
PERSISTENT held-shards index (the (rtype,bits,shard) set this node actually has a DB for) drives
them → O(held). Empty shards have no state, so skipping them is correct. Persisted IMMEDIATELY on
first-write-per-shard (once per shard, not per write — hot path unaffected) + on GC removal, under
HELD_MARKER_SEED; lazily loaded once. `writer_of()` hoists eligible() out of the loops.
`backfill_held_if_needed()` (7e3e247): one-time O(2^bits) probe of shard-ROOT pointers on first boot
after the upgrade (or a fresh node) so existing shard DBs from the prior binary aren't dropped from
the dashboard — uses shard_roots.resolve (account read, no DB creation), persists so it never repeats.
PROVEN single-node: status head-count correct, survives restart (held loaded from disk), reshard 8→9
sweeps held shards to gen-9 + status still counts them. CLUSTER DEPLOY + VERIFIED (all 5 rolled): the
backfill repopulated held from existing shard DBs — `sqltest` (deployed pre-upgrade) is back in the
global entries AND resolves from all 4 nodes; status program_heads correct. DONE.

## SQL-BACKED REGISTRY (2026-07-08, building) — docs/SQL_REGISTRY_DESIGN.md
Replace the per-shard `RegistryState` postcard blob with a per-shard CraftSQL DB, so registry
write/resolve/replicate/durability scale O(1)/O(changed) not O(rows-in-shard). Motivated by the
target topology (thousands of nodes, ~80% NAT readers, ~20% writer backbone) where blob
write-amplification + whole-shard replication flood the scarce writer tier. Decisions settled this
session: granularity = **DB-per-shard** (Option 1 — preserves the sharding/election/reshard model;
fine at scale where each writer holds bounded substantial DBs; only wasteful at tiny scale, accepted);
validation = **native** (drop the governed-WASM validator — mechanism not policy, memory
[[registry-native-validation-not-wasm-hook]]); recursion broken by a **blob-backed RootStore** (shard-DB
root cid stored in the ProgramAccountStore account, pages in CraftOBJ). Full design in the doc.
- [x] **P1+P2+P3 — DONE (commit 376daab), done coherently in one pass since storage + replication +
      reshard are tightly coupled.** shard_root.rs (blob-backed RootStore breaking the recursion); a
      dedicated CraftSql engine (ns `reg/<rtype>/<bits>/<shard>`) + per-shard DB cache; register =
      version-guarded upsert, resolve = indexed SELECT, current_version/status/rows/entries = SELECT,
      GetState = SELECT*-as-RegistryState (wire DTO unchanged), PushState = row upsert, ensure_current
      takeover = GetState→upsert. Validation NATIVE (sig + name char-limit). Row-level replication: a
      write pushes a 1-row RegistryState (scale win). No ObjDurable on the shard engine (durability =
      K-replica row-push; write path never blocks on/fails from sync erasure — this fixed a single-node
      deploy failure the sync sweep caused). Blob persistence + shard_seed + WASM advance path removed.
      PROVEN single-node: deploy, v1→v2 upsert, resolve, restart persistence, online reshard 8→9 (rows
      swept gen-8→gen-9, regshards 12→19). Wire unchanged (no version-consistency break).
- [x] **P4 — cluster deploy + live re-test. DONE + PROVEN.** All 5 nodes rolled to the SQL binary
      (cutover = wipe, old blobs ignored; program_heads 0 fresh). Cross-node: deploy `sqltest` on node1
      → resolves from all 4 nodes. Offline-owner: node2 served `sqltest` with node1 DOWN → the row was
      replicated via row-push. Cluster reshard 9→8 (seq 6, shard_count 512→256 in ~20s) → `sqltest`
      resolves from all nodes after the SQL sweep (rows moved gen-9→gen-8 shard DBs cross-node). Cluster
      rests at bits=8/256, gov seq 6. FEATURE COMPLETE.
      FOLLOW-UPS (not blocking): erasure-durability for shard pages as a best-effort background layer;
      the user-DB app-index (`apps`) durability warns on single-isolated nodes (pre-existing, unrelated).

## RESHARD ROBUSTNESS — drain + GC (2026-07-08, commit 4abf6a5, PROVEN LIVE)
Closed the two deferred reshard gaps. `reshard_round` no longer does a single merge-forward pass;
after a generation change it DRAINS the old generation: keeps re-sweeping old→current for
`DRAIN_TICKS` (6 ≈ 60s, >> the ~20s governance-propagation window) so a write that lands on the old
generation from a straggler still on the old count is carried forward (closes the "late write" gap),
THEN GC's the old generation via a new `ProgramAccountStore::clear` (deletes the local account state
files) so old generations don't accumulate on disk (closes the "GC" gap). Drain state is in-memory
`(old_gen, ticks)`; a restart mid-drain just leaves the old gen un-GC'd (harmless — reads resolve at
the current generation). `sweep_generation` extracted from the old inline body (idempotent LWW merge).
PROVEN LIVE on the 5-node cluster (deployed all 5): a reshard 8→9 with a file-set diff of node1's
`accounts/` showed 4 pre-existing gen-8 account files DELETED after the drain window + 1 new gen-9
added (63→60 files), while `mergetest` resolved throughout. Count now trends DOWN across reshards
(64→63→60) instead of accumulating. Registry holds few non-empty accounts (2 programs), so the
magnitude is small; the mechanism is confirmed.

## GOVERNANCE PROPAGATION HARDENED — census-based tick (2026-07-08, commit 7679b68)
Root-cause fix behind the seq 0→1 propagation bug (whose proximate symptom was the announce-version
floor, commit b14461d). `GovernanceChainStore::tick()` pulled peer chains only from `snapshot().active`
— the bounded (~5), per-node-divergent HyParView active view — so a governor absent from a node's
active view was never pulled and its change never reached that node. Same active-view limitation class
already fixed for registry election. Fix: pull from `census()` (the converged, union-merged member set)
∪ the current governors (the SOURCE of every change; a flaky/relay-only governor can drop out of the
census at the TTL edge, so include the ids explicitly — `fetch` resolves a peer head via the DHT, no
direct peering needed). PROVEN LIVE: after rolling all 5 nodes, a `set-config shard_bits=9` (seq 2→3)
propagated to all 4 Hetzner nodes + resharded 256→512 in ~20s. O(targets) fetches/tick (fine at
10s–100s nodes; digest/sampling is the scale follow-up). Cluster rests at bits=9.

## CONVERGED MEMBERSHIP + registry election fix + dynamic-sharding groundwork (2026-07-08)
Root cause (19-node live scaling test): registry `eligible()` elects over the size-5 HyParView ACTIVE view (partial + per-node-divergent) -> caps at ~6 writers + INCONSISTENT shard->writer assignment above ~6 nodes (split-brain, not a throughput cap). Fix = elect over a CONVERGED member set. See docs/STATE_AND_ROADMAP.md §5 + memory zeph-registry-active-view-election-cap.
- [ ] Phase 1 — converged membership: add a `members` map (node_id -> {addr,last_heard}) to the membership crate; anti-entropy it via a new `MemberSync` gossip round (union + max last_heard); each node re-asserts self each round; `census()` = members alive within TTL. Deaths propagate by aging out (no SWIM suspect/incarnation yet — acceptable; slower death detection). NOTE: full-map gossip is O(N) — fine for 10s-100s of nodes, needs digest/SWIM-piggyback for 1M (future).
- [ ] Phase 2 — election over census: headreg `eligible()` uses `membership.census()` not `snapshot().active` (writer + replica election both). DROP the rtt-exclusion (local rtt breaks election consistency; slow-writer handled by resolve fallback + the tail fixes; a converged health signal is future). Verify on cluster: eligible grows to full N.
- [ ] Phase 3 (groundwork) — dynamic sharding: make SHARD_COUNT a governed/converged value (needs K1 config registry) so all nodes agree; design consistent-hashing split/merge + rebalance. Full auto-resharding = later.

### Phase 1+2 LANDED + PROVEN (2026-07-08, commit 50f34ea)
19-node re-run: eligible 6 -> 19 (census-based election spans the cluster), writer_shards 41 -> 15 (shards spread across all 19, not ~6), active view stayed 5 (census is decoupled). Election consistent across nodes (both agreed). Base cluster healed post-teardown (eligible back to 5, resolve returns correct cid — NO data loss).
### NEW GAP EXPOSED -> Phase 2.5 (state migration on re-election)
Making the election correctly span N re-elects shards to new writers on membership growth, but the registry does NOT migrate state to the new replica set — so existing heads were orphaned on the old holders (still durable in CraftOBJ, but not routed-to) and transiently unresolvable while the cluster was grown. Healed on teardown (election reverted). NOT data loss; a routing/migration gap.
- [ ] Phase 2.5: state migration on replica-set change — re-push / anti-entropy state to the current replica set when membership changes (not only on write), OR reconstruct-from-durable-CraftOBJ on takeover, OR broaden resolve-miss to query old holders. Prerequisite met (consistent election); this completes elastic membership. NOW ahead of Phase 3 (dynamic sharding).

### Phase 2.5 DONE + PROVEN (2026-07-08, commits a0d83f5 + 769cf93)
State migration on membership change. First attempt (a0d83f5) fired migration on EVERY census change -> during a join storm the census changes every gossip tick -> the 768-scan+push stormed and STALLED convergence (19->12). Fix (769cf93): debounce -> migrate once after the census is unchanged for MIGRATE_STABLE_TICKS (3 ticks, ~30s). LESSON (memory attribute-failures-to-code-not-environment): I wrongly blamed box load; the variable was my code. Re-run at 19 nodes: eligible converged to 18 (no storm), and node1/guestbook2 resolved to the SAME correct cid (807888d6) from BOTH an old node and a new node -> state followed the election, consistent. Elastic membership now works end-to-end: grow -> consistent census election + state migration -> resolves work.

### Relay-peer (Mac governor) stability saga — 2026-07-08
Long investigation into the Mac (phone hotspot, relay, ~600ms rtt) misbehaving. Findings + fixes:
- REAL regression #1 (fixed b5cc1f4): converged-membership added a SEPARATE member-sync connection every 10s that congested the fragile relay. Fix: fold member gossip into the existing 30s shuffle (no new connection).
- Confounder: the hotspot is genuinely flaky (~70% probe success). CONTROLLED test — full pre-membership cluster (f0554a8 everywhere) fluctuated IDENTICALLY (71%). So probe-% instability is the NETWORK, not code. Lesson: probe-% is the wrong metric.
- REAL bug #2 (fixed 1288873): the right metric is RECONNECT-vs-STUCK. Reconnection test proved the node STUCK at eligible=1 (t0=t1=t2=1) — dropped out and never climbed back. Cause: membership.start got only cfg.peers (EMPTY when a node configures only dht_seeds), so recover_isolated had NO seed to dial. Fix: seed membership bootstrap from dht_seeds + gentle rate-limited (15s, one seed) recovery ALONGSIDE fill_active (an earlier attempt ddf6c03 skipped fill_active -> itself caused stuck; reverted). Post-fix: t0=5 (converges), recovery fires (~15s spacing, not a storm), eligible climbs 1->2 (reconnects).
- My-error commits: ddf6c03 (skipped fill_active, reverted 5d095e2). Chased several wrong hypotheses (re-bootstrap storm [no-op], registry write-path [gated, still unstable]) before the controlled test + the reconnect-vs-stuck reframe pinned it.

### ROOT CAUSE of Mac "no active connection" regression — fill_active (fixed c3f99c9)
Found by DIFFING old-vs-new functions (user correctly rejected my live tests: full-pre-membership was window-confounded; old-Mac-vs-new-Hetzner is mixed-version = invalid). add_active/mark_dead UNCHANGED. The regression is my fill_active self-heal (2a99780): f0554a8 loops ALL passive candidates and DROPS a failed promotion (self-cleans passive of unreachable/stale addrs -> promotions keep finding reachable peers). Mine CAPPED at active_size attempts + RE-QUEUED failures -> on a passive polluted with dead addrs (accumulated via shuffle), a random few attempts hit only dead entries -> active view never fills -> ZERO active connections, can't refill, and re-queue prevents self-cleaning. Reverted to draining; recover_isolated's seed dial covers the full-isolation case that motivated the self-heal. This is the REAL regression; the member-sync fold (b5cc1f4) + empty-bootstrap (1288873) were also real but secondary.
