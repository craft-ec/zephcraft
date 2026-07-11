# ZephCraft — State, Reconciliation & Roadmap

**Date:** 2026-07-07 · **Updated:** 2026-07-11
**Purpose:** the single consolidated view of where the node actually stands — what is built and validated, where the code and the *other* design docs disagree (a spec-vs-code reconciliation), what is deferred by choice, the known architectural constraints, and the prioritized plan. This is the current picture, not a changelog: it is kept edited-in-place. It supersedes ad-hoc status in conversation and complements `.claude/feature-progress.md` (the working phase tracker). For deep architecture, `ZEPHCRAFT.md` is the consolidated design doc.

---

## 1. Executive summary

The full vertical is standing and validated on live Hetzner hardware: **transport substrate → storage → database → compute → registry coordination**, with a real SQL app deployed, invoked, persisting, and surviving a node restart. The registry control-plane scaling ceiling (the previous #1 concern) is **resolved**, and the transport plane was rebuilt to a structural design and rolled to the fleet.

A spec-vs-code reconciliation (every concrete claim classified with file:line/commit evidence) reached a clear verdict that still holds:

- **The code is honest.** Every high-value "done" claim is confirmed in code + commits; the progress file, if anything, *understates*. No overstatement.
- **The debt is in the docs.** The largest remaining finding is stale documentation: `REGISTRY_DESIGN` (partially fixed), foundation §62/Part F, `GOVERNANCE §4/§6`, `CRAFTCOM_DESIGN`, and `ENCRYPTION §8` still describe **attestation / committee / quorum / Pkarr / tracker / fixed-shard** models the code replaced with an owner-signed CRDT + census-elected sharded registry + DHT. Anyone reading those builds the wrong mental model — this is the open Tier-0 sweep (§7).
- **A short list of genuine gaps** remains, mostly deferred by design and flagged in their own docs — plus the one real robustness item, SWIM death-detection (§5).

**Highest-value single fix:** the doc-reconciliation sweep of the stale design docs above — chiefly the registry docs' non-existent *k-of-n attestation* security model.

---

## 2. Current state — built & validated

| Layer | Status | Notes |
|---|---|---|
| **Transport substrate (Transfer Plane v2)** | BUILT + LIVE | One QUIC connection per peer, each protocol a bi-stream with a 1-byte tag (mux; conns/node 24→7 at 8 nodes). Sender-side **bounded active set** (choke, K=4 distinct active-push peers, non-blocking) + **offer/grant admission** with class carried on every push and graded receiver gating from a `ResourceGauge`; repair redirects around a pressured peer. Elected healthscan (one scanner per cid per epoch) + class-fair `JobCoordinator`. Gated by an offline acceptance harness (`tests/tests/transfer_plane.rs` A–G) — nothing rolls without it (`deploy/gate.sh`). Spec: `docs/TRANSFER_PLANE_V2.md`. |
| **Storage (CraftOBJ)** | BUILT | Content-addressed (BLAKE3), RLNC erasure (k=8/n=32) + null-space vtags, health-scan repair/degrade/offload/fade, system objects (WANT-pinned), DHT-only routing. Publish retains locally + distributes in the background (fire-and-forget). ~52 MB/s local ingest. |
| **Database (CraftSQL)** | BUILT | SQLite VFS over content-addressed pages, single-writer-per-identity, generation-based durability (incremental diff per commit) + manifest, network recovery from `(owner, ns)`, compaction. Backs the registry (per-shard DBs). |
| **Compute** | BUILT | ONE unified WASM runtime; per-program capability grant (deterministic subset vs full); determinism boundary = clock/random, not sql/obj; consensus clock; invoke returns committed bytes. |
| **Coordination (HeadRegistry)** | BUILT | **`2^shard_bits` GOVERNED, dynamically-resizable** shards (online power-of-two split/merge, drain+GC), each a **per-shard CraftSQL DB** (`heads` table), type-in-seed, K=3 **row-level** replication (push-on-write, merge-on-takeover), owner-signed LWW CRDT — **no attestation**, **native** write validation. Writer election over the **converged census** (scales with the writer set, not the size-5 active view); state migrates to follow the election on membership change; enumeration loops **O(held)**; readiness-gated. DB roots + manifests ride this; DHT KIND_ROOT/KIND_MANIFEST retired. |
| **Governance** | BUILT | Governor multisig (k-of-n), governance chain head, registry's executing program cid resolved *through* governance (upgradeable), native default that self-starts a fresh network. Governed `SetConfig` drives the registry `shard_bits`. |
| **Encryption** | BUILT (phases 1–4) | `cipher` crate (XChaCha20-Poly1305 + Umbral PRE), private files + private DBs, owner-only decryption proven by tests. Sharing (phase 5) is design-only. |
| **Performance / memory** | STABLE | Deploy 10s→40ms; invoke tail 26s→<250ms (fire-and-forget writes + sidecar-first opens); ~11 commits/sec/DB sequential. Hub-node RSS bounded by jemalloc (glibc-arena bloat fixed, ~13x cut; ~100–530 MB/node on the fleet). Census convergence reliably fast (~3.3s; periodic-epidemic safety net). |
| **Dashboard** | BUILT | Registry/Governance tabs, network-wide registry entries browser. |

**Fleet:** 4 Hetzner nodes (`zeph`/`zeph2`/`zeph3`/`zeph4` on one box), all rolled + healthy. The Mac node (sole governance governor) is stopped by choice — spin it up on demand for governance ops; do not reassign the governor. Validated on hardware: cross-node resolve, offline-owner resolve, DB-root-on-registry (survives restart), fast deploys, census election consistent 6→19 nodes with state following the election.

**Key commit evidence:** transport substrate — mux `dd0b7d1`/`3535f63`…`e2a1292` (cleanup `599f9b5`), choke `8bf7bc2`/`c7b63c3`/`5b3dd9b`, offer/grant `d540134`/`191d83c`; registry — census election `50f34ea`, state migration `a0d83f5`/`769cf93`, dynamic shards `9d29713`/`9b5d538`/`4abf6a5`, SQL-backed `376daab`, O(held) `f4db195`/`1a55f00`, readiness `402f26d`; memory `0ed4082`/`be9588b`/`9779416`; census hardening `3e4dcf4`; deploy gate `24eed4d`; compute 0–4 `1e9a9ba`…`43d2ebf`; encryption end-to-end.

---

## 3. Reconciliation matrix (spec-vs-code)

### 3.1 Confirmed built (verified in code + commits)
Transport substrate (mux/choke/offer-grant, §2); compute 0–4; DB-roots-on-registry (`57574f0`) + DHT retirement (`af5958e`); publish fire-and-forget (`fcc07e7`); registry census-election/dynamic-sharding/SQL-backed/O(held)/state-migration (§2 commits); tail-latency fix (`4e3b794`); config registry (`GovAction::SetConfig` consumed, drives `shard_bits`); encryption end-to-end. All CONFIRMED.

### 3.2 Real gaps — specced, not (fully) built (deferred by design unless noted)
- **Membership death-detection [K10]** — the converged census + fast convergence is built (§2), but there is still **no SWIM Suspect/Dead epidemic gossip or indirect PING-REQ**: deaths are self-detected per-node and age out by TTL (~30s) rather than fast active detection. The one real robustness item worth an explicit decision; weakens as N grows.
- PDP / storage receipts (M3) [K5]. Open computation verification (VERIFICATION_DESIGN) [K6/K7]. Crypto-shred Tier-0 [K4] — only best-effort fade shipped.
- `random` capability declared but no host fn bound [K2] — bind or drop.
- File segmentation / K=32 (publish is a single whole-object generation at k=8); SIMD GF(2⁸) (scalar today); ephemeral ConsumeMode (aliased to Drop); `SIGNED_WRITE`/RPC-write CAS/delegation absent from the sql crate.
- General anchor dispatcher [K1] — no governed-WASM program exists today (registry validation went native), so no plural anchor table is needed yet.

### 3.3 Genuine drifts — code diverged from spec detail (decide: fix doc or code)
- **Writer election docstring**: spec + the code's own docstring say `min hash(shard‖epoch‖id)`; the impl is a stable K-replica set (`min hash(rtype‖shard‖id)`, no epoch) + `replicas[epoch % K]` rotation. Fix the docstring (`headreg.rs`).
- **SQL live pages are not individually erasure-coded** (only *generations* are) — contra foundation §392.
- **SQL index nodes + root are plaintext** (only page contents encrypted) — contra ENCRYPTION §7.
- **HealthScan reads provider records + liveness filter, not live availability probes** — contra foundation §62.1 (deliberate; the AvailabilityProbe issuer is [K8]).

### 3.4 Doc contradictions — the fix worklist (the open Tier-0 sweep; highest leverage, low risk)
These *other* docs still describe models the code replaced:
| Doc / location | Claims | Reality (code) |
|---|---|---|
| **REGISTRY_DESIGN** (partially updated 07-09; recheck) | attested / committee / quorum / PDA-CraftSQL-DB, "no publisher online needed" | owner-signed CRDT, governed/dynamic per-shard CraftSQL DBs, no attestation, native validation; resolve needs a live writer/replica |
| **Foundation §62 A1/A2/A3-b1** | attestation-quorum authority, owner-or-attestation heads, app names → attested CraftSQL DB | none built (A3-b2, DB roots on registry, IS correct) |
| **Foundation Part F** §408/§411, §34/§37/§397 | synchronous `expected_root_cid` CAS / `WRITE_CONFLICT`, Pkarr/DHT/SWIM publication | fire-and-forget + LWW-by-seq; publish via RootStore/registry |
| **CRAFTCOM_DESIGN** §5/§10 | `craft_*` host-fn names, single `craft_clock` | unprefixed names (`sql_execute`, `clock`, …), clock/wall_clock split |
| **ENCRYPTION §8** | guaranteed self-executing crypto-shred | best-effort fade only (physically impossible claim) |
| **GOVERNANCE §4/§6** | PDA-registry program verifies approval + committee attests | governance chain-fold + open CRDT, no committee |
| **routing crate prose** | tracker-as-backend (`/craftec/tracker/1`, `NoTracker`) | DHT (`zeph-dht`) is the sole backend; tracker retired |
| **obj/lib.rs module docstring** | publish blocks until ≥K distinct peer acks | fire-and-forget; returns cid immediately with `durable:false` |
| **Internal comments** | headreg 3s timeout (actual 8s); `crate::Runtime` (deleted); `app/<name>` (actual `app.<name>`); ATTESTATION `attest.rs` (no such file) | — |

### 3.5 Progress-file hygiene
Tick the done boxes (compute 0–4, routing-trait trim, DHT phase-2 cleanup). **`apps/guestbook-wasm/` is untracked** — the real-app demo exists but was never committed. Still-true gaps: content durability <8 nodes, read-caching deferred, per-node registry views.

---

## 4. On hold — deferred by design
- The verification layer (Track B / VERIFICATION_DESIGN); verifier re-run reproducibility (persist `now` in the request).
- CraftCOM future: app versioning (name→CID head) + app-store-as-a-catalog-app — kept out of the node.
- Writer leases; read-caching; auto-recovery/compaction triggers; the `releasing` churn-cleanup loop.
- Sharing via proxy re-encryption (ENCRYPTION phase 5); crypto-shred Tier-0.

---

## 5. Known architectural constraints
- **Registry control-plane scaling — RESOLVED.** The historical ceiling (election over the fixed size-5 HyParView active view → ~6 writers regardless of cluster size, and divergence past ~6 nodes) is gone: election runs over the converged census (`eligible` grew 6→19 in a live test, consistent across nodes), state migrates to follow the election on membership change, the shard count is governed/elastic [K9], and the registry is per-shard SQL with O(held) loops. The control plane now scales with the writer set. **Remaining scale work** (not a ceiling): SWIM death-detection [K10]; digest/sampled gossip to keep membership sub-linear at 1000s of nodes (the census gossip is O(N)/round today); churn damping (hysteresis) under sustained heavy churn; coalescing head publishes at very high write rates. The data plane (DB pages, content) is genuinely share-nothing.
- **Failure detection** — no SWIM Suspect/Dead dissemination yet (§3.2); self-detected deaths, weakens as N grows.
- **Per-DB single-writer** — one DB is capped at its writer (~11/s here); scale is across many DBs, never within one.
- **Aggregate throughput is unmeasured** — all figures are sequential single-DB latency, not a scaling curve.

---

## 6. Missing kernel primitives — the bounded backlog

The minimal-kernel bet is largely won: the substrate (account model + unified runtime + storage + governance + transport) is complete, and most "features" reduce to **governed protocol programs** (reputation, tracking, catalogs, app store, versioning, verification *coordination*) — no kernel change. What remains is a bounded set of primitives the kernel must expose first. **Litmus:** new policy over existing primitives → protocol program; needs a new capability/wire/substrate mechanic → kernel first.

| # | Primitive | Kind | Unlocks | Status |
|---|---|---|---|---|
| **K1** | Anchor dispatcher + config registry | substrate | governed protocol programs; governed config | **Config half DONE** (`SetConfig` consumed → `shard_bits`). Anchor half **reframed** — registry validation went native; a plural anchor table is only needed when a genuinely governed-WASM program appears. |
| **K2** | `random` host fn | capability | full-profile apps needing randomness | Declared, unbound. Trivial — bind or drop. |
| **K3** | Proxy re-encryption ops (PRE rekey + reencrypt) | capability | **sharing / grants** | `cipher` has Umbral PRE; expose as host fns. Grants are policy on top. |
| **K4** | Threshold secret-sharing (split/combine/destroy) | capability | key-share crypto-shred; k-of-n secrets | Only path to *probabilistic* deletion (a trust, not a proof). |
| **K5** | PDP challenge/response (+ holder proof over pieces) | wire+storage | storage proofs → reputation; M3 | Holder proof relates to vtags; reputation is policy above it. |
| **K6** | Cross-node re-execution + signed verdict | wire | compute verification | Runtime re-executes deterministically already; missing the request+verdict protocol. |
| **K7** | Attestation gather (solicit + collect k-of-n signed statements) | wire | verification, shred, reputation evidence | The general primitive under K4/K5/K6. Consistency-only replacement for the removed attestation coordinator. |
| **K8** | Issue `AvailabilityProbe`s from the health scan | wire (half-built) | verified availability counts | Messages exist + are answered; the prober is never *issued* (foundation §62.1). |
| **K9** | Dynamic sharding (split/merge, rebalance) | substrate | elastic registry scale | **DONE + PROVEN LIVE.** Governed `shard_bits`; low-bit routing makes split/merge LOCAL; online reshard with drain window + old-generation GC; grow (8→9) and shrink (9→8) proven cross-node. |
| **K10** | SWIM dissemination (Suspect/Dead gossip + indirect PING-REQ) | substrate | robust failure detection at scale | **PARTIAL.** The converged census is built AND reliably fast-converging (`3e4dcf4`, ~3.3s) — the set the registry election + governance tick run over. Missing the DEATH half: Suspect/Dead epidemic gossip + indirect PING-REQ (deaths age out by TTL ~30s). The census gossip is O(N)/round → pairs with the digest-gossip scale item. |

**By cost:** capabilities K2–K4 cheapest (host fns over in-tree crypto); wire K5–K8 medium (K7 generalizes K5/K6); substrate K1/K9/K10 deepest. K9 done; K1 config-half done. Once the K1 anchor half lands (if a governed-WASM program appears), reputation/tracking/catalog/app-store are pure protocol-program work.

---

## 7. Roadmap

Cross-references kernel primitives as **[Kn]**.

**Tier 0 — reconciliation & hygiene (now; cheap, low-risk):**
- Doc-reconciliation sweep of the stale *design* docs in §3.4 — purge attested/committee/tracker/Pkarr/fixed-shard framing; fix ENCRYPTION §8's impossible "guaranteed shred"; fix the writer-election docstring.
- Hygiene: bind-or-drop `random` [K2]; commit `guestbook-wasm`; tick the done boxes.

**Tier 1 — registry scaling ceiling — ✅ COMPLETE:** census election + state migration, dynamic sharding [K9], SQL-backed registry + O(held) loops all landed and proven live (§2, §5). Residual (secondary): coalesce head publishes at very high write rates; digest/sampled gossip for sub-linear membership at 1000s of nodes.

**Tier 2 — extensibility & robustness:**
- **SWIM Suspect/Dead dissemination [K10]** — the census (JOIN) half is done + hardened; the DEATH half is the natural next code item.
- Anchor dispatcher [K1] — only when a governed-WASM protocol program appears.
- App-name resolve cache; `AvailabilityProbe` issuance [K8].

**Tier 3 — prove the product thesis:**
- Federated app demo (multi-owner, enumerated via the registry directory) + cross-node app-DB read path.
- **Sharing [K3]** — encrypted grants to recipients.
- **Prove the scaling win with numbers** — deploy hundreds/thousands of heads; measure register/resolve latency vs row count (the per-row SQL win) and held/DB growth (the O(held) win). Converts "should scale" into a curve.

**Tier 4 — trust primitives (deferred):**
- PDP [K5] + reputation; cross-node verify [K6] + attestation gather [K7] → verification layer; threshold shares [K4] → best-achievable crypto-shred.

---

## 8. Recommended next step

The registry scaling ceiling and the transport substrate are done and live, so the highest-value items are:

1. **Finish the Tier-0 doc-reconciliation sweep of the stale design docs (§3.4)** — foundation §62/Part F, GOVERNANCE §4/§6, CRAFTCOM_DESIGN §5/§10, ENCRYPTION §8, and the routing/obj/internal-comment drifts still describe attestation/committee/tracker/Pkarr/fixed-shard models the code replaced. Low-risk, high-leverage; misleads anyone building a mental model.
2. **SWIM Suspect/Dead dissemination [K10]** — the one real robustness gap; the census (JOIN) half is built + hardened, fast active DEATH detection is not.
3. **Prove the scaling win with numbers** (Tier 3) — turn "should scale" into a measured curve.
4. Then the deferred layers: sharing [K3], trust primitives [K4–K7], verification (Track B), the K1 anchor half if/when a governed-WASM program appears.
