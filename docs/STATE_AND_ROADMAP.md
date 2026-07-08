# ZephCraft — State, Reconciliation & Roadmap

**Date:** 2026-07-07 · **Updated:** 2026-07-09
**Purpose:** the single consolidated view of where the node actually stands — what is built and validated, where the code and the design docs disagree (a 7-domain spec-vs-code reconciliation), what is deferred by choice, the architectural constraints we know of, and the prioritized plan. This supersedes ad-hoc status in conversation and complements `.claude/feature-progress.md` (the working phase tracker).

---

## 0. 2026-07-09 UPDATE — Tier 1 (registry scaling) essentially complete; where §§ below are superseded

Since the 07-07 audit, the registry control-plane — the doc's #1 scaling concern — was rebuilt and proven on the live 5-node cluster. Read this section first; it supersedes the stale specifics in §2 (registry row), §5 (the ceiling), §6 (K1/K9/K10 status), and §7 (Tier 1).

**Shipped + proven live (07-08/07-09):**
- **Census-based writer election** (`50f34ea`) — election runs over the CONVERGED census, not the size-5 HyParView active view. The ~6-writer ceiling in §5 is GONE (`eligible` grew 6→19 in the live test). Plus **state migration on membership change** (`a0d83f5`+`769cf93`, debounced) so heads follow the election.
- **Governed dynamic shard count = K9** (`9d29713`, `9b5d538`, `4abf6a5`) — `shard_bits` is a governed `SetConfig` value (so **the config registry, K1's config half, is now CONSUMED**); the count changes on a LIVE cluster via power-of-two split/merge with a drain window + old-generation GC. Grow (8→9) AND shrink (9→8) proven cross-node.
- **SQL-backed registry** (`376daab`) — each shard's heads are a per-shard **CraftSQL DB** (`heads(owner,name,cid,version)`), not a re-encoded-per-write postcard blob. Register = version-guarded upsert, resolve = indexed SELECT, replication = row-level push (a 1-row state, not the whole shard). Erasure-coded shard-page durability restored (`2942cf3`).
- **O(held) enumeration** (`f4db195`, `1a55f00`) — status/migrate/reshard/GC iterate only the shards a node HOLDS (a persistent held-index), not all `2^bits`. Reads never create empty DBs.
- **Governance propagation hardened** — the first change (seq 0→1) now propagates (`b14461d`, announce version was floored) and `tick()` pulls over the census, not the active view (`7679b68`).
- **Registry native validation** — the write validator (owner sig + name char-limit) is NATIVE mechanism, NOT a governed-WASM program (the `app-registry` anchor seed was dropped). This changes K1's "anchor dispatcher" framing.
- **Readiness gate** (`402f26d`) — a node waits for its census to settle before routing registry ops, so a freshly-restarted node never registers/resolves against an unconverged election (was a transient "not found").

**Still open (accurate below):** K10 SWIM Suspect/Dead dissemination (the converged *census* is built; proper epidemic death-detection is not) · K2 `random` host fn (unbound) · the doc-reconciliation sweep itself (REGISTRY_DESIGN / foundation §62·Part F / GOVERNANCE §4·§6 still describe attestation/committee/postcard-blob/256-fixed-shard models) · the Tier-4 trust primitives (K3–K7) · encryption sharing (phase 5).

**Design docs added:** `SQL_REGISTRY_DESIGN.md`.

---

## 1. Executive summary

The full vertical is standing and validated on a live 5-node cluster (4 Hetzner + 1 Mac governor): **storage → database → compute → registry coordination**, with a real SQL app deployed, invoked, persisting, and surviving a node restart.

A parallel **spec-vs-code reconciliation** (7 domains, every concrete claim classified with file:line evidence) reached a clear verdict:

- **The code is honest.** Every high-value "done" claim — compute phases 0–4, DB-roots-on-registry, publish fire-and-forget, registry sharding/K-replication/writer-offline, the tail-latency fix, and end-to-end encryption — is **confirmed in code + commits**. The progress file, if anything, *understates* (several done items left unchecked). There is **no overstatement anywhere**.
- **The debt is in the docs.** The largest finding is stale documentation: `REGISTRY_DESIGN`, foundation §62, `GOVERNANCE §4/§6`, and Part F still describe **attestation / committee / quorum / Pkarr / tracker** models the code has since replaced with an owner-signed CRDT + registry + DHT. Anyone reading those docs today builds the wrong mental model.
- **A short list of genuine gaps** remains, mostly deferred by design and honestly flagged in their own docs — plus one real robustness item (membership) worth an explicit decision.

**Highest-value single fix:** the registry docs actively describe a *k-of-n attestation* security model that does not exist in code. That misdescribes how authority works and should be corrected first.

---

## 2. Current state — built & validated

| Layer | Status | Notes |
|---|---|---|
| **Storage (CraftOBJ)** | BUILT | Content-addressed (BLAKE3), RLNC erasure (k=8/n=32) + null-space vtags, health-scan repair/degrade/offload/fade, system objects (WANT-pinned), DHT-only routing. Publish retains locally + distributes in the background. ~52 MB/s local ingest. |
| **Database (CraftSQL)** | BUILT | SQLite VFS over content-addressed pages, single-writer-per-identity, generation-based durability (incremental diff per commit) + manifest, network recovery from `(owner, ns)`, compaction. |
| **Compute** | BUILT | ONE unified WASM runtime; per-program capability grant (deterministic subset vs full); determinism boundary = clock/random, not sql/obj; consensus clock; invoke returns committed bytes. The accidental two-runtime split is gone (capability Runtime deleted). |
| **Coordination (HeadRegistry)** | BUILT (rebuilt 07-08/09, §0) | **`2^shard_bits` GOVERNED, dynamically-resizable** shards (online split/merge, drain+GC), each a **per-shard CraftSQL DB** (`heads` table; not postcard blobs), type-in-seed (RT_PROGRAM/RT_DBROOT/RT_MANIFEST), K=3 **row-level** replication (push-on-write, merge-on-takeover), owner-signed LWW CRDT — **no attestation**, **native** write validation. Writer election over the **converged census**; enumeration loops **O(held)**; readiness-gated. Writer-offline gap closed (validated). DB roots + manifests ride this substrate; DHT KIND_ROOT/KIND_MANIFEST retired. |
| **Governance** | BUILT | Governor multisig (k-of-n), governance chain head, registry's executing program cid resolved *through* governance (upgradeable), real native default that self-starts a fresh network. |
| **Encryption** | BUILT (phases 1–4) | `cipher` crate (XChaCha20-Poly1305 + Umbral PRE), private files (`publish_private`/`get_private`), private DBs (`open_private`), owner-only decryption proven by tests. Sharing (phase 5) is design-only. |
| **Performance** | FIXED | Deploy 10s→40ms; invoke tail 26s→<250ms (fire-and-forget writes + sidecar-first opens). ~11 commits/sec/DB sequential. |
| **Dashboard** | BUILT | Registry/Governance tabs, network-wide registry entries browser (global / this-node toggle). |

Validated on hardware: cross-node resolve, offline-owner resolve, DB-root-on-registry (survives restart), fast deploys, no invoke tail across epoch boundaries.

---

## 3. Reconciliation matrix

### 3.1 Confirmed built (high-value claims, verified in code + commits)
Compute 0–4 (`1e9a9ba 76dabef d7dc10a bdeb2e9 181bae7 43d2ebf`); DB-roots-on-registry (`57574f0`) + DHT retirement (`af5958e`); publish fire-and-forget (`fcc07e7`); registry sharding/K-replication/writer-offline (`21e8c34 d4be8de 84d17d6`); tail-latency fix (`4e3b794`); encryption end-to-end. All CONFIRMED.

### 3.2 Real gaps — specced, not (fully) built

**Deferred by design, honestly flagged in their own docs (awareness only):**
- PDP / storage receipts (M3).
- Open computation verification — board/verdict/grab (VERIFICATION_DESIGN, deferred).
- Crypto-shred Tier-0 (guaranteed) — only best-effort fade shipped; capsule-destroy exists as a unit test.
- ~~Config registry — `GovAction::SetConfig` present but unconsumed.~~ **DONE 07-08 (§0)** — consumed; drives the registry `shard_bits`.
- General anchor dispatcher — no governed-WASM program today (the `app-registry` anchor was dropped; registry validation went native, §0), so no plural anchor table needed yet.
- File segmentation / K=32 — publish does a single whole-object generation at k=8.
- SIMD GF(2⁸) — scalar table-driven today.
- Ephemeral ConsumeMode — aliased to Drop.

**Worth an explicit decision (not clearly in the plan):**
- **Membership: no SWIM dissemination** (PARTIAL). HyParView views + RTT probing are built, but no Suspect/Dead epidemic gossip or indirect PING-REQ — deaths are self-detected per-node. Failure detection weakens as N grows; ties directly to the scaling story.
- **`random` capability declared but no host fn bound** — bind it or drop the cap.
- **`SIGNED_WRITE` / RPC-write CAS / delegation** absent from the sql crate (may be a node-layer concern).

### 3.3 Genuine drifts — code diverged from spec detail (decide: fix doc or code)
- **Writer election**: spec *and the code's own docstring* say `min hash(shard‖epoch‖id)`; the actual impl is a stable K-replica set (`min hash(rtype‖shard‖id)`, no epoch) + `replicas[epoch % K]` rotation. The code misdescribes its own live mechanism — fix the docstring at minimum (`headreg.rs`).
- **SQL live pages are not individually erasure-coded** (separate local store; only *generations* are) — contra foundation §392.
- **SQL index nodes + root are plaintext** (only page contents encrypted → structure exposed) — contra ENCRYPTION §7.
- **HealthScan reads provider records + liveness filter, not live availability probes** — contra foundation §62.1 (deliberate).

### 3.4 Doc contradictions — the fix worklist (highest leverage, low risk)
| Doc / location | Claims | Reality (code) |
|---|---|---|
| **REGISTRY_DESIGN** banner, §Status, §2, §4, §5, §6, §9 | attested / committee / quorum / PDA-CraftSQL-DB, "no publisher online needed" | owner-signed CRDT, `2^shard_bits` **governed/dynamic** shards each a **per-shard CraftSQL DB** (07-09, §0), **no attestation**, native validation; resolve needs a live writer/replica *(REGISTRY_DESIGN updated 07-09; see it directly)* |
| **Foundation §62 A1/A2/A3-bullet1** | attestation-quorum authority, two-typed owner-or-attestation heads, app names → attested CraftSQL DB | none built (A3-bullet2 — DB roots on registry — IS correct) |
| **Foundation Part F** §408/§411 (sync `expected_root_cid` CAS / `WRITE_CONFLICT`), §34/§37/§397 (Pkarr/DHT/SWIM publication) | synchronous CAS-on-write, Pkarr names | fire-and-forget + LWW-by-seq; publish via RootStore/registry |
| **CRAFTCOM_DESIGN** §5/§10 | `craft_*` host-fn names, single `craft_clock` | unprefixed names (`sql_execute`, `clock`, …), clock/wall_clock split |
| **ENCRYPTION §8** | guaranteed self-executing crypto-shred | best-effort fade only |
| **GOVERNANCE §4/§6** | PDA-registry program verifies approval + committee attests | governance chain-fold + open CRDT, no committee |
| **routing crate** (code prose) | tracker-as-backend: ALPN `/craftec/tracker/1`, `NoTracker` errors, "tracker now, iroh DHT later" | DHT (`zeph-dht`) is the sole backend; tracker retired |
| **obj/lib.rs** module docstring | publish blocks until ≥K distinct peer acks | fire-and-forget; returns cid immediately with `durable:false` |
| **Internal comments** | headreg 3s timeout (actual 8s); `crate::Runtime` (deleted type); `app/<name>` (actual `app.<name>`); ATTESTATION `attest.rs` paths (no such file); gov.rs "committee attests" | — |

### 3.5 Progress file — honest, understates
Tick the done boxes: compute phases 0–4, the P4b routing-trait trim, the DHT phase-2 cleanup note. **`apps/guestbook-wasm/` is untracked** — the real-app demo exists but was never committed. Known gaps still true: content durability <8 nodes, read-caching deferred, per-node registry views.

---

## 4. On hold — deferred by design
- The verification layer (Track B / VERIFICATION_DESIGN); verifier re-run reproducibility (persist `now` in the request).
- CraftCOM future: app versioning (name→CID head) + app-store-as-a-catalog-app — kept out of the node.
- Dynamic sharding / split-merge; writer leases; read-caching; auto-recovery/compaction triggers; the `releasing` churn-cleanup loop.
- Sharing via proxy re-encryption (ENCRYPTION phase 5); crypto-shred Tier-0.

---

## 5. Known architectural constraints
- **Registry control-plane ceiling — MEASURED 2026-07-08, and worse than theorized.** The writer/replica election runs over `self + membership.snapshot().active`, and the HyParView **active view is a fixed `active_size = 5`** (`crates/membership/src/lib.rs:52`; `headreg.rs:214`). So the 256 shards spread across **only ~6 elected writers regardless of cluster size** — adding nodes past ~6 does **not** add registry write capacity. A 19-node live test confirmed it: node1 knew all 19 (5 active + 14 passive) yet `eligible` stayed pinned at 6. **The real ceiling is the `active_size` constant (~6 writers), not the 256-shard / ~256k-writer figure previously estimated — that estimate was wrong.** Worse: above ~6 nodes each node's active view diverges (HyParView shuffles), so elections diverge across nodes → registry writes/resolves can become inconsistent (mechanism-implied; corroborates the earlier "stale node elected writer breaks resolves" observation). **Fix ordering:** (1) build a globally-consistent census (all known-alive) via membership dissemination [K10] so every node elects over the same set; (2) point the registry election at that census, not the size-5 active view; (3) *then* dynamic sharding [K9] + coalescing matter. Until (1)+(2), the registry does not scale past ~6 nodes and risks divergence. The **data plane** (DB pages, content) remains genuinely share-nothing; this is purely a control-plane bug + limit.
  - **UPDATE 2026-07-08 — election fix LANDED + PROVEN** (converged-membership census, commit `50f34ea`). A re-run at 19 nodes: `eligible` grew **6 → 19**, shards spread (`writer_shards` **41 → 15 of 256**), the active view stayed 5 (census is decoupled), and the election was consistent across nodes. Base cluster healed on teardown (no data loss). **The re-run exposed the next gap: registry state does NOT migrate when the election changes.** Growing the cluster re-elected shards to new writers that lacked the state (orphaned on the old holders + still durable in CraftOBJ, but not routed-to), so existing heads were *transiently unresolvable while grown* — healed when membership reverted. It's a routing/migration gap, not data loss. **State migration — DONE + PROVEN 2026-07-08** (commits `a0d83f5` + `769cf93`): an event-driven anti-entropy loop re-replicates held shards to their current replica set once the census settles (debounced ~30s so it never storms during convergence). Re-run at 19 nodes: eligible converged to 18 and `node1/guestbook2` resolved to the same cid from BOTH an old and a new node — state follows the election. **Elastic membership now works end-to-end.** Remaining scale work: churn damping (hysteresis) for sustained heavy churn, O(N) gossip -> digest sync, then dynamic sharding [K9].
  - **UPDATE 2026-07-09 — CEILING RESOLVED.** The three remaining control-plane items all landed + proven live: **[K9] dynamic sharding** (governed `shard_bits`, online split/merge with drain+GC; grow AND shrink proven), the **SQL-backed registry** (per-shard CraftSQL DBs; per-row upsert/indexed-resolve/row-level replication instead of whole-shard blobs — the write/resolve/replication amplification that made the blob model costly at scale is gone), and the **O(held)** enumeration loops (a node touches only the shards it holds, not all `2^bits`). With election-over-census + these, the registry control plane scales with the writer set and the shard count is elastic. Left: SWIM death-detection [K10] and, at very large scale, digest/sampled gossip. See §0.
- **Failure detection.** No SWIM dissemination (see 3.2) — self-detected deaths only; weakens as N grows.
- **Per-DB single-writer.** One DB is capped at its writer (~11/s here); scale is across many DBs, never within one.
- **Throughput is unmeasured in aggregate.** All figures are sequential single-DB latency, not a scaling curve.

---

## 6. Missing kernel primitives — the bounded backlog

The minimal-kernel bet is largely won: the substrate (account model + unified runtime + storage + governance) is complete, and most "features" reduce to **governed protocol programs** — reputation, tracking, catalogs, app store, versioning, and the *coordination* logic of verification are all policy-over-existing-primitives, buildable with **no kernel change**.

What remains is a **bounded, enumerable set of primitives** the kernel must expose before those programs can be written. *A protocol program cannot grant itself a capability the kernel lacks.*

**Litmus test:** new policy over existing primitives → protocol program (kernel is done for it); needs a new capability, wire protocol, or substrate mechanic → kernel first.

| # | Primitive | Kind | Unlocks (policy/feature) | Status / note |
|---|---|---|---|---|
| **K1** | **General anchor dispatcher + config registry** | substrate | *every* new protocol program behind a governed anchor; governed config values | **Config registry: DONE (07-08).** `GovAction::SetConfig` is now consumed — `config_registry()` fold + `resolve_config` + `set_config` CLI; drives the registry `shard_bits`. Anchor-dispatcher half: **reframed** — the registry validator went NATIVE (the `app-registry` anchor seed was dropped; validating an owner's own submission is mechanism, not governed policy), so a plural anchor table is only needed when a genuinely governed-WASM protocol program appears. |
| **K2** | **`random` host fn** | capability | any full-profile app needing randomness | Declared in `CapabilityGrant::full()`, no host fn bound. Trivial — bind or drop. |
| **K3** | **Proxy re-encryption ops** (PRE rekey + reencrypt) | capability | **sharing / grants** — encrypted data to other recipients | `cipher` already has Umbral PRE; expose rekeygen + reencrypt as host fns. Grant records are policy on top. |
| **K4** | **Threshold secret-sharing ops** (split / combine / destroy shares) | capability | **key-share crypto-shred**; any k-of-n secret | The only path to a *probabilistic* deletion guarantee — and even then a trust, not a proof (deletion is unverifiable; see §3.4 ENCRYPTION §8). |
| **K5** | **PDP challenge/response** (+ holder proof over pieces) | wire + storage | **storage proofs → reputation**; adaptive-pollution defense (M3) | Holder computes a possession proof (relates to vtags); challenger verifies. Reputation/tracking are policy *above* it — the kernel provides the evidence. |
| **K6** | **Cross-node re-execution + signed verdict** | wire | **compute verification** | The runtime re-executes deterministically already; missing is the request-elsewhere + signed-verdict protocol. |
| **K7** | **Attestation gather** (solicit + collect k-of-n signed statements over a fact) | wire | verification, shred, reputation evidence — any "quorum attests a fact" | The general primitive under K4/K5/K6 policies. The old attestation coordinator was removed; this is its clean, general, consistency-only replacement. |
| **K8** | **Issue `AvailabilityProbe`s from the health scan** | wire (half-built) | verified availability counts (vs record + liveness today) | Messages exist and are answered; the prober is never *issued* (foundation §62.1). |
| **K9** | **Dynamic sharding of the account substrate** (split / merge, rebalance) | substrate | breaks the fixed-shard registry ceiling → elastic scale | **DONE + PROVEN LIVE (07-08/09).** Governed `shard_bits`; low-bit routing makes split/merge LOCAL (parent→two children); online reshard with a drain window (catches in-flight writes) + old-generation GC; grow (8→9) and shrink (9→8) both proven cross-node. Not consistent-hashing — power-of-two split/merge on the governed count. See §0. |
| **K10** | **SWIM dissemination** (Suspect/Dead gossip + indirect PING-REQ) | substrate | robust failure detection at scale | **PARTIAL (07-08).** The CONVERGED census (union + max-last_heard gossip, folded into the existing shuffle) was built — this is what the registry election + governance tick now run over. Still missing: Suspect/Dead epidemic gossip + indirect PING-REQ, so deaths still age out by TTL rather than fast active detection. |

**Reading the list by cost:**
- **Capabilities (K2–K4)** — cheapest: host fns over crypto/PRE already in-tree.
- **Wire protocols (K5–K8)** — medium: node-to-node message + verify logic; **K7 generalizes K5/K6**.
- **Substrate (K1, K9, K10)** — deepest: they change the kernel's own mechanics. **K1 is the single highest-leverage item** — it turns "add a feature" into "deploy a protocol program."

Once **K1** lands, reputation / tracking / catalog / app-store / versioning are pure protocol-program work. **K3–K7** turn sharing, verification, and best-achievable shred into programs-over-primitives. **K9–K10** are substrate hardening for scale and robustness. *(Minor/optional, not enabling primitives: delegated-write authority / `SIGNED_WRITE` (§408-409, absent from the sql crate); multi-segment/K=32 large-file storage — both improvements to existing mechanisms.)*

---

## 7. Roadmap

Tiers cross-reference the kernel primitives in §6 as **[Kn]**.

**Tier 0 — reconciliation & hygiene (now; cheap, low-risk):**
- Doc-reconciliation sweep (§3.4) — purge attested/committee/tracker/Pkarr framing; fix ENCRYPTION §8's physically-impossible "guaranteed shred" claim; fix the writer-election docstring.
- Hygiene: bind-or-drop `random` **[K2]**; commit `guestbook-wasm`; tick the done boxes.

**Tier 1 — fix the registry scaling ceiling — ✅ ESSENTIALLY COMPLETE (07-08/09, see §0):**
- ✅ Scaling benchmark (19 nodes) found the real cap: the `active_size=5` election, not 256 shards.
- ✅ **Registry election over a consistent census** — LANDED (`50f34ea`) + state migration on membership change; `eligible` 6→19.
- ✅ **Dynamic sharding [K9]** — LANDED + proven (governed count, online split/merge, drain/GC).
- ✅ **SQL-backed registry + O(held) loops** — LANDED; per-row upsert / indexed resolve / row-level replication replaced whole-shard blobs (the amplification that made blobs costly at scale), and enumeration is O(held) not O(2^bits).
- ⏳ Coalesce head publishes (latest-per-DB per interval) — still useful at very high write rates; secondary. Digest/sampled gossip for O(N)→sub-linear membership at 1000s of nodes — future.

**Tier 2 — extensibility & robustness:**
- **General anchor dispatcher + config registry [K1]** — the enabler that turns every future feature into a protocol-program deploy. Highest structural leverage of anything on this list.
- **SWIM dissemination [K10]**; app-name resolve cache.
- Wire **`AvailabilityProbe` issuance [K8]** for verified availability.

**Tier 3 — prove the product thesis:**
- Federated app demo (multi-owner, enumerated via the registry directory) + the cross-node app-DB read path.
- **Sharing [K3]** makes it collaborative (encrypted grants to recipients).

**Tier 4 — trust primitives (the deferred layer):**
- **PDP [K5] + reputation** (evidence primitive + scoring policy).
- **Cross-node verify [K6] + attestation gather [K7]** → the verification layer.
- **Threshold shares [K4]** → best-achievable crypto-shred (a trust guarantee, not a proof).

---

## 8. Recommended next step (revised 2026-07-09)

Tier 1 (the registry scaling ceiling) is essentially done and proven live (§0), so the plan has moved on. In priority order:

1. **Finish the Tier-0 doc-reconciliation sweep (in progress).** `STATE_AND_ROADMAP.md` (this file) and `REGISTRY_DESIGN.md` are updated; still stale: **foundation §62 + Part F**, **GOVERNANCE §4/§6**, **CRAFTCOM_DESIGN §5/§10**, **ENCRYPTION §8**, and the routing/obj/internal-comment drifts in §3.4. They still describe attestation/committee/postcard-blob/Pkarr/tracker/256-fixed-shard models the code has replaced. Low-risk, high-leverage.
2. **Prove the scaling win with numbers** — deploy hundreds/thousands of heads and measure register/resolve latency vs row count (the per-row SQL win) and held/DB growth (the O(held) win). Converts "should scale" into a curve.
3. **Robustness: SWIM Suspect/Dead dissemination [K10]** — the converged census is built; fast active death-detection is not.
4. Then the deferred layers: extensibility (K1 anchor dispatcher only if a governed-WASM program appears), trust primitives (K3–K7), sharing (K3), verification (Track B).
