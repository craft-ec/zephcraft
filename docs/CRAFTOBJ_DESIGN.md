# CraftOBJ Design Document

**Version 2.0 — July 2026 (greenfield rebuild)**

> **Source of truth**: This document derives from `craftec_technical_foundation.md` v3.6/v3.7 (Parts A, E, J) plus the Decision Record of 2026-07-02 (`.claude/design-reviews/2026-07-02T000000Z.md`). It supersedes the v1 CraftOBJ design (libp2p, pin/unpin, SHA-256, subscription economics), which describes the archived implementation (`craft-ec/craftobj-archive`).

## Overview

CraftOBJ is the storage layer of the Craftec node: a decentralized, content-addressed object store over a P2P network. Store bytes by content, retrieve bytes by CID, keep them alive through churn. RLNC erasure coding provides redundancy; a protocol-driven lifecycle (no pin/unpin) maintains it autonomously.

Storage nodes are dumb piece-holders: they receive coded pieces, store them, serve them, and never decode. All reconstruction intelligence is client-side.

**What changed from v1:**

| v1 (archived) | v2 (this doc) |
|---|---|
| libp2p (TCP, request_response, gossipsub removed late) | iroh (QUIC), ALPN protocol composition |
| SHA-256 CIDs | BLAKE3 CIDs |
| pin/unpin as survival mechanism + GC | protocol-driven lifecycle (Push/Distribution/Repair/Scaling/Degradation); pin re-added 2026-07-03 as an OPT-IN durability anchor, not a survival requirement (see Pinning) |
| 10 MiB segments, segment-dependent k (1–40) | 8 MiB segments, K=32 (files); K=8 generations (16 KB SQL pages) |
| PRNG-derived vtags (L=4) | public null-space vtags (L=8), 2⁻⁶⁴ forgery bound |
| piece_id = SHA-256(coefficients) | piece_id = BLAKE3(coding_vector ‖ data) |
| DHT provider counts as health truth | live batched AVAILABILITY_PROBE; DHT supplies candidates only |
| top-provider-by-piece-count repair election | rendezvous hashing election |
| subscription/creator-pool economics in core spec | deferred to a future ECONOMICS design; receipts retained |

---

## Position in the Stack

```
Layer 3+  CraftSQL / CraftVFS / CraftCOM      (future, on top of CraftOBJ)
Layer 2   CraftOBJ                            ← this document
Layer 1   transport (iroh), membership        (zephcraft/crates/transport, membership)
Layer 0   identity (Ed25519 + Pkarr), crypto  (zephcraft/crates/crypto)
```

Dependency rule: each layer depends only on layers below it.

### Repository Layout (greenfield: `craftec/zephcraft/` — **ZephCraft**, the Craftec node)

```
zephcraft/
├── Cargo.toml            workspace
├── crates/
│   ├── core/             shared types: Cid, NodeId, errors, config, HLC
│   ├── crypto/           BLAKE3 helpers, Ed25519 identity, keystore (zeroize)
│   ├── wire/             postcard frames, message enums, version negotiation
│   ├── erasure/          GF(2⁸) SIMD tables, RLNC encode/recode/decode, null-space vtags
│   ├── transport/        iroh endpoint, ALPN registry, connection pool, admission
│   ├── membership/       partial-view membership (HyParView-style) + SWIM-style probing
│   ├── routing/          ContentRouting trait + tracker impl (+ iroh-DHT impl later)
│   ├── store/            local CAS: sharded dirs, bloom filter, LRU, watermarks
│   ├── obj/              CraftOBJ engine: publish, distribution, health, repair, fetch
│   └── noded/            node binary: kernel assembly, lifecycle, signal handling
├── apps/
│   └── tracker/          simple tracker service (ContentRouting impl #1)
├── webui/                dashboard assets, embedded into the zeph binary (M-UI)
├── deploy/               headless deployment (systemd unit, Hetzner notes)
└── tests/                DST harness, property tests, multi-node integration
```

**Deployment model — one node implementation:** `zeph` (crates/noded) is the single headless daemon for every platform (macOS arm64, Linux x86_64/Hetzner). The UI is a **web dashboard served by the daemon itself** — static assets embedded in the binary, bound to `127.0.0.1` only, talking token-authed JSON over localhost HTTP (JSON-RPC over Unix socket for the CLI; WebSocket deferred until live events need it). Identical on every OS; remote daemons are reached via SSH tunnel, never exposed publicly. There is no separate native app and never a second node implementation.

---

## Content Model

- `Cid = BLAKE3(content)` — 32 bytes. Immutable, self-verifying, deduplicating.
- Content is **always** stored as RLNC-coded pieces (Decision C2: no replication tier).
- Files: 8 MiB segments → K=32 source pieces of 256 KiB each.
- CraftSQL pages: 16 KB pages grouped into K=8 generations (128 KB).
- Redundancy: `redundancy(k) = 2.0 + 16/k`; target `n = k × ceil(redundancy(k))`.
  - K=32 → 96 pieces (3×). K=16 → 48. K=8 → 32.
  - The same formula is used by the encoder and the health scanner — never duplicate it.
- Encryption is a layer-above concern (Decision R10): CraftOBJ stores opaque bytes. Encrypted content arrives as ciphertext; CID = BLAKE3(ciphertext). Nodes never see plaintext or keys.

### Self-Describing Pieces

Every piece carries its own metadata — interpretable without any manifest:

```rust
CodedPiece {
    piece_id:      [u8; 32],   // BLAKE3(coding_vector ‖ data)
    cid:           [u8; 32],   // BLAKE3 of original content
    segment_index: u32,
    segment_count: u32,
    k:             u32,        // generation size, carried explicitly
    coding_vector: Vec<u8>,    // k GF(2⁸) coefficients
    data:          Vec<u8>,    // 256 KiB (files) / 16 KB (pages)
}
```

`ContentRecord { content_id, total_size, vtags_cid }` is a cached convenience summary (stored in routing layer, 1 h TTL) — never a reconstruction dependency.

---

## Verification Tags — Public Null-Space Vtags (Decision R1)

Pollution defense. Any node can verify any coded piece is a valid linear combination of the source generation — at ingest, at repair, at fetch — with no secrets.

**Publish time** (per segment/generation):
1. Form the augmented source matrix: row `i` = `(e_i ‖ source_piece_i)` — unit coding vector plus data, over GF(2⁸).
2. Sample **L = 8** random vectors from the null space of the row span (vectors `v` with `row · v = 0` for every source row).
3. Store per tag only `(seed, v_c)`: the data part `v_d` is derived from the 32-byte seed via BLAKE3 XOF (identity `v_c[i] = sᵢ·v_d`), so the blob is ~L×(32+K) bytes (<1 KiB) instead of full-length vectors → store as a CraftOBJ object → `vtags_cid = BLAKE3(vtags_blob)`. [Implementation refinement 2026-07-03]
4. `vtags_cid` travels in the ContentRecord and is pushed alongside pieces (like v1's manifest-before-pieces rule).

**Verification of piece `(c, d)`**: valid iff `(c ‖ d) · v_l == 0` for all 8 vectors. Cost: 8 GF(2⁸) dot products (SIMD).

**Properties:**
- Forgery probability for a piece outside the row space: `(1/256)⁸ = 2⁻⁶⁴` — **against corruption crafted independently of the tags** (bit rot, transmission faults, blind garbage: the overwhelmingly common case). Because tags are public, an ADAPTIVE attacker can solve the published constraints and craft passing pollution; defense in depth: PDP coefficient cross-checks by challengers holding unpredictable pieces (M3) + whole-content BLAKE3 verify at decode with parole/ban. Documented upgrade path if adaptive pollution is observed: pairing-based homomorphic signatures (computationally sound public verification). [Implementation note 2026-07-03, M1.6]
- Recoded pieces (repair) remain in the row space → verify with the same vtags. No key handoff.
- The vtags blob is itself content-addressed — it cannot be tampered with (its CID is its integrity proof). A malicious *publisher* can only sabotage their own content.
- Storage nodes **reject pieces at ingest** if vtag verification fails → pollution never propagates.
- Bootstrapping: a node that receives pieces before the vtags blob stores them as *unverified* (not served, not counted, not repair-eligible) until vtags arrive; unverified pieces older than 10 minutes are dropped.

---

## Storage Lifecycle (protocol-driven; pinning is an opt-in overlay)

| Function | Trigger | Action |
|----------|---------|--------|
| **Push** | Publisher publishes | Encode n pieces; round-robin ~2 pieces per node across ≥K distinct peers |
| **Distribution** | Node holds > 2 pieces per CID | Pass excess to peers without the CID (piece *moves*: sender deletes on ack) |
| **Repair** | HealthScan verified count < n | Rendezvous-elected nodes recode 1 piece each per cycle |
| **Scaling** | High local fetch demand | Providers push pieces to best non-provider |
| **Degradation** | Count > n and no demand | Excess pieces shed, 1 per node per cycle |

Distribution priority: (1) peers with 0 pieces, (2) peers with exactly 1 (to make them repair-eligible at ≥2). The **repair** path inverts this: 1-piece holders first, then non-holders. Both orderings are intentional (§Foundation 29/55).

**Event → behavior mapping (clarified 2026-07-03).** The two count-changing behaviors are driven by opposite membership events and use opposite mechanics — keep them distinct:
- **Node spin-up (join) → Distribution.** New capacity appears; over-full holders *MOVE* excess pieces to peers *without* the CID (the newcomer). Piece moves (sender deletes on ack) — spreads existing redundancy across more distinct nodes (better geometry), no net new pieces. This is what populates a freshly-joined node; Repair never does.
- **Node exit (death) → Repair.** Redundancy is lost; a rendezvous-elected holder *CREATES* fresh pieces by recode to replace what the departed node held — net new pieces, restoring count to the floor.
Repair is poll-driven by HealthScan (30s) today; an exit-event fast-path off membership's SWIM death-detection is a latency enhancement (same "repair on exit", faster). Repair recruiting a brand-new holder only happens in the sole-survivor case (no other holder to recode onto); routine population of new nodes is Distribution's job.

**Asymmetric up/down — fast scale-out, slow scale-in (Decision 2026-07-03, user).** Scaling and Degradation are deliberately NOT symmetric. Scaling is **un-elected and parallel**: every provider over its local demand threshold recruits independently, so the aggregate rate is proportional to how hot the CID is (5 hot providers → +5/cycle), and with read-spreading the hot-provider count can itself grow — a viral CID fans out multiplicatively until the demand equilibrium or the n/2 cap. Degradation is **rendezvous-elected**: one shedder per cycle, a constant linear trickle down to the floor regardless of surplus. Both still obey bounded-per-node-per-cycle (Scaling's speed is parallelism, not any node bursting). Rationale: content popularity spikes fast and decays slow; under-serving hot content is expensive while holding brief surplus is cheap; and shedding is the destructive/irreversible direction (demand may return mid-lull) so the RISKY direction is serialized+slow and the SAFE direction is parallel+fast. Same principle as cloud autoscalers (scale out fast, scale in slow).

**Periodic + bounded, NOT batch (Decision 2026-07-03, user).** All four count-changing behaviors — Repair, Distribution, Scaling, Degradation — move a **small bounded amount per cycle** (today: 1 piece per CID per 30s scan), never a one-shot jump to the target. This is deliberate, to *avoid major upset*: no bandwidth bursts (a lopsided CID balances over minutes, not a single 2 MB shove), no thundering herd (nodes settle gradually instead of colliding on a synchronized rebalance), self-correcting mid-flight (each cycle re-reads live state — joins/deaths/demand shifts — so the loop never over-commits to a snapshot that's already stale), and fail-soft (a dropped move is one piece, retried next cycle — never a half-finished migration). Convergence is intentionally slow-and-smooth; the per-cycle step and interval are tunable but the loop is always incremental. Verified live: an over-full holder shed to a newcomer 1 piece/cycle (29/3 → 25/7 → 23/9, total conserved), converging to balance without a lurch. Do NOT replace with batch-to-target.

**Unified control model (Decision 2026-07-03) — one loop, not five behaviors.** Every row above is the *same* controller reconciling observed **HAVE** (supply = verified provider count) against a **target** set by **WANT** (demand) + PIN + the durability floor:

```
alive  = pinned OR want > 0
target = alive ? floor + headroom(fetch_rate) : 0      # floor = n(k); headroom = bandwidth
HAVE < target  → Repair (heal to floor) / Scaling (add above floor for demand)   [ACTIVE]
HAVE > target  → Degradation (shed surplus toward floor, 1/node/cycle)            [ACTIVE, stops at floor]
target = 0     → stop repairing → churn attrition → Fade (death below floor)      [PASSIVE, no active delete]
```

Two demand timescales feed it: **persistent WANT/PIN** sets `alive` (slow keep-alive intent → floor or fade); **observed fetch_rate** sets `headroom` (fast bandwidth scaling). Keep the two target terms distinct — **floor = durability** (survive churn), **headroom = bandwidth** (serve concurrent fetches).

**Degradation ≠ Fade (they are NOT the same shed).** Degradation is an *active* trim of surplus that **stops at the floor** — content stays alive and durable, only waste is removed; it never goes below the floor. Fade is the *passive* decay *below* the floor toward zero, reachable only when `target = 0` (nothing wants it) — it is the **absence of repair**, not an active delete. Passive on purpose: it is fail-safe and reversible (a late WANT mid-fade resumes repair and recovers the content), and space is reclaimed by each node's local eviction anyway — no coordinated deletion needed. ("Descale" is not a separate behavior — it is just Degradation triggered by demand falling rather than by over-replication.) The four behaviors are Repair, Scaling, Degradation, Fade. HAVE and WANT are observed from the signed signal sets in §Deletion (the open HAVE/WANT market); this loop turns those signals into behavior.

**Durability acknowledgment (added 2026-07-02):** `publish()` MUST NOT report content as durable when local encoding completes — only once coded pieces have been accepted (`PIECE_PUSH_ACK: Ok`) by **≥ K distinct peers, excluding the publisher**. Until that threshold, publish reports `LocalOnly` / distribution-in-progress. Rationale: free-tier durability comes entirely from network-side self-repair, which requires the piece spread to exist before the publisher disconnects; a casual publisher who goes offline right after a local-encode "success" would otherwise hold the only copies. Clients that must disconnect early keep the content queued for push on reconnect (local-first).

### Pinning — Intentional Durability Anchors (Decision 2026-07-03)

Pinning is an **opt-in local-policy overlay on eviction**, not a survival requirement. This is deliberately *different* from the archived v1 pin (where unpinned content was garbage-collected): here the protocol lifecycle keeps data alive by default, and a pin is an *additional* guarantee that a node volunteers.

**Definition.** To pin a CID is to commit to holding the **whole content** (~1× original size) and serving it indefinitely, exempt from eviction and disk-watermark shedding. A pinner stores the decoded content and **encodes coded pieces on demand** — so it can mint unlimited fresh, independent pieces (a perfect repair source) rather than being limited to recoding a fixed set.

**Three roles:**
- **Uploader — pinned by default.** The publisher already holds the file from encoding it; pinning is free and gives every published CID a guaranteed reconstruction anchor from moment zero. Opt out with `publish(..., pin=false)`.
- **Consumer pin.** A node that fetched and decoded a CID to consume it holds the whole content; it may `pin(cid)` to keep serving it. Default off (consumers evict normally).
- **Normal (unpinned) nodes** hold ~2 transient coded pieces, subject to Distribution/Repair/Eviction exactly as the lifecycle table specifies.

**Pinning complements distribution — it does not replace it.** Pin = "available while this node is up"; network distribution = "available when it is down." The Durability acknowledgment rule stands unchanged: `publish()` still spreads pieces to ≥K distinct peers before reporting durable, *even though the uploader pins*. A pin-only file (never distributed) dies with its lone pinner. Real durability = anchor (pin) + spread (distribution + repair).

**Interactions:**
- **HealthScan** maintains the distributed floor REGARDLESS of pins (Decision 2026-07-03, user — supersedes Review R4's pinner-as-full-availability). A pin is NOT a substitute for spread: even under a live pin, if the distributed coded-piece count < n, Repair fires. A pinner PARTICIPATES in the loop as a mint source (it holds the whole content → can recode unlimited fresh pieces) and in Distribution, but is EXCLUDED from Degradation — a node with pinned content only creates + distributes to prevent loss, and never sheds. Rationale: belt-and-suspenders durability — pin (anchor) AND ≥n distributed pieces (survives the pinner dying). Degradation shed_one only ever removes coded pieces (`pieces/<id>`), never the whole-content copy.
- **Eviction** never touches pinned content. Pins count toward disk usage; if pins alone exceed the operator's disk, further `pin()` calls fail (local intent, local cost — the operator's problem, not the network's).
- **Serving.** A pinner answers PIECE_REQUEST by encoding pieces on the fly (excludes honored); it also answers whole-content reads directly.
- **Announce.** Pinners announce as providers (with a `pinned` capability bit) so fetchers and repair prefer them — a pinner never runs dry.
- `unpin(cid)` reverts the CID to normal lifecycle (evictable); it does not delete immediately.

Pinning is free-tier by nature (you spend your own disk to guarantee availability). The future paid tier is the orthogonal "pay others to guarantee it" — both coexist.

### Participation Modes — User Flows (Decision 2026-07-03, BitTorrent-modeled)

The swarm experience mirrors BitTorrent (leech/seed), with RLNC removing BitTorrent's rare-piece and last-piece-stall problems: any K independent pieces reconstruct, providers mint fresh pieces on demand, and a decoded consumer is a perfect seed (can generate unlimited unique pieces).

**Download-for-consumption (leech).** `get(cid)`: resolve providers (tracker) → fetch K independent pieces in parallel → vtag-verify each → decode → whole-content BLAKE3 verify → consume. During the fetch, already-collected coded pieces are servable to co-swarmers (tit-for-tat, feeds the reciprocity ledger that governs the node's own admission).

**Post-consumption default — seed-while-online.** After decoding, the node holds the whole file and by default becomes a **transient provider** (announces, serves pieces by encoding on demand) until it goes offline or eviction reclaims the space under disk pressure. This is the Scaling function: popular content gains providers automatically. No durability commitment — a laptop may close freely; distributed copies + repair + pins carry the content. `pin` promotes a transient provider to a permanent one.

**Configurable per download.** The post-consumption behavior is a node-wide config default (`consume_mode = seed`) overridable per `get`:
- `seed` (default) — transient provider while online (above).
- `drop` — contribute pieces *during* download (reciprocity stays healthy) but discard everything once decoded; no lingering provider record. For consume-and-leave without being a visible long-term holder.
- `ephemeral` — fetch straight to output, serve nothing even during download, hold nothing; pure client for laptops/mobile. Leans on reciprocity credit or accepts deprioritized admission.
CLI: `zeph get <cid> [--seed|--drop|--ephemeral]`; `pin` always available to make it permanent.

**Uploader who keeps serving (seed).** `publish` pins by default → holds the whole file, spreads to ≥K peers (durability), and serves piece requests by encoding on demand for as long as the node is up — never runs dry, no rare-piece problem. "Keep uploading" = stay online with the pin. Stop = `unpin` or go offline; the ≥K distributed copies keep the file alive without the uploader.

**Privacy note.** In any swarm, serving what you consumed reveals you hold it (provider records / piece serving are observable) — same property as BitTorrent. The `drop`/`ephemeral` modes exist precisely for consumers who don't want to be long-term visible holders; app-layer encryption (VISION) hides *content* but not the fact of holding a CID. A future CraftNET tunnel overlay can hide the network identity of a consumer.

### Provider granularity for CraftSQL DB pages: announce per-DB, serve per-page (Decision 2026-07-03)

Question: how does a node holding a CraftSQL DB advertise so readers can fetch its pages? **Call: announce per-DB (one record per holder per DB, keyed by `(owner_identity, namespace)`), and serve any page by CID on request. NEVER mint a provider record per page-generation.**

Three reasons — the third is decisive:
1. **Record count.** Per-page-generation records scale with `distinct_live_pages × holders` (page-CID dedup helps — unchanged pages across commits reuse CIDs — but a large/busy DB is still thousands of generations). Per-DB records scale with `DBs × holders`. Orders of magnitude smaller; friendly to BOTH the interim single tracker AND the DHT.
2. **No write churn.** Every commit mints new page CIDs; per-page records would churn constantly. "I serve DB X" is stable across writes.
3. **DB integrity (decisive).** A DB is a COHERENT unit. If pages were independently replicated (page A on {1,2,3}, page B on {4,5,6}), DB survival = Π(per-page survival) — catastrophic for many-page DBs (0.9999^8000 ≈ 45%). **Durability must ride on WHOLE-DB holders**, not independent per-page survival. The single-writer owner is *always* a whole-DB holder (local-first); replicas hold whole DBs too. So the durability unit is the DB.

**Consistent with §33's "many providers per page":** those providers are the many WHOLE-DB holders, each of which serves *any* page CID on request — so a page still has many providers, without a per-page announce. Announce granularity (per-DB) ≠ serve granularity (per-page-by-CID).

- **v1**: a reader fetches pages directly from the OWNER — `resolve_root` gives the owner identity → node registry gives the address → request page CIDs → owner serves from its store. No new provider record needed at all for the single-owner case.
- **Later**: a `KIND_DBPROVIDER`-style record keyed by `(owner, namespace)` lists whole-DB replicas, so the DB survives the owner going offline.

**Corollary (CraftSQL-layer, flagged not decided here):** DB page objects are a DISTINCT class from lifecycle-managed content — kept alive by *reachability from a live DB root*, not by content-demand. They must be EXCLUDED from HealthScan's demand-driven scale/fade (a separate store namespace or object-class flag), and superseded pages become GC-reclaimable. (Also resolves the "k=0 generation breaks target_pieces" edge.)

### Storage granularity: SQL pages and file pieces stay SEPARATE (Decision 2026-07-03, user)

CraftSQL page storage and file/content storage use **distinct mechanisms** on CraftOBJ — NOT a single "convergent block-tree." Per the foundation (§5, §33, §28): **files** are chunked at **256 KiB pieces** (manifest tree); **SQL databases** are **16 KB pages** in K=8 page generations with a dedicated **CID-VFS + page index** and a single-writer **root CID**.

A convergent "everything is one Merkle block-tree" design was **considered and rejected**, because the access patterns diverge:
- **SQL** = random access to *small* units (SQLite reads a few 16 KB B-tree pages per query).
- **Files** = sequential streaming of *large* content (big runs).

One block size pessimizes one side: 16 KB blocks → a 10 GB file becomes ~650k objects (index bloat, per-object overhead, terrible streaming); 256 KiB blocks → **16× read amplification** per SQL page (fetch 256 KiB to use 16 KB). Variable block sizes would just re-introduce the separation inside one layer. Also: SQLite is *already* a B-tree of pages, so the CID-VFS maps **page → CID directly** (via the page index); a generic block-tree would be a redundant tree-on-tree with extra indirection. And the separation keeps CraftSQL's dependency graph clean — it needs only CraftOBJ `put/get` + its own page index, not the file-manifest machinery.

**Consequences:**
1. CraftSQL builds **directly** on the existing CraftOBJ — CID-VFS (§33: xRead/xWrite/xSync) + page index + root-CID + `SIGNED_WRITE` optimistic-concurrency (CAS on `expected_root_cid`). **No file-manifest overhaul is a prerequisite.**
2. Large-file **chunked block-trees + partial/range reads** (streaming/seek, block-level dedup) remain a valuable but **separate** content-track item at 256 KiB — NOT shared with SQL.
3. CraftSQL is a **convenience metadata index** (Path 2: search → CIDs → resolve via DHT), **NOT a routing layer** (foundation §5).

### Metadata Envelope — KIND_META (Decision 2026-07-03, user)

BitTorrent-style separation of immutable identity from editable metadata. The **manifest is the info-hash analog** (content-addressed, deterministic → dedup-safe); **`KIND_META` is the `.torrent`-envelope analog** — signed, per-publisher metadata that *references* a manifest CID without ever perturbing it.

- **Record**: `MetaPayload { cid, published_at, comment }`, signed (KIND_META=5). Registry table keyed by cid→node_id; query_kind 6 = all_metas. Same announce/withdraw/supersede-by-HLC path as WANT.
- **Decoupled, not wrapped**: unlike BitTorrent's single `.torrent` file, the manifest is a standalone object (content resolves without any envelope) and `KIND_META` is a separate signed tracker record pointing at it. Optional + additive.
- **Two read paths**: (1) *default view* — collapse N envelopes to one; **objective fields (`published_at`) CRDT-merge by min** (first-published, converges with no coordinator), subjective fields (comment) pick-one-by-policy. (2) *full query* — `metas(cid)` returns all publishers' envelopes.
- **Authority**: user-owned, single-writer-per-identity. `set_meta` edits (preserves `published_at`), `del_meta` = signed withdrawal (only that publisher's claim). The manifest/content stays network-managed and immutable. Editing/deleting metadata never changes the manifest CID → dedup airtight through all of it.
- `publish_file`/`publish_dir` auto-announce `published_at=now`. Dashboard shows "published Xago · by · 💬 comment" (default view) + a `note` button.

Three deletes, three authorities: delete-my-meta (signed withdrawal) · delete-my-copy (tombstone) · delete-content-network-wide (nobody — censorship-resistant).

### Eviction Cooldown — TTL'd Soft-Tombstone (Decision 2026-07-03, user)

When content is EVICTED (disk-watermark pressure) or actively removed after Fade, the node records the CID on an **eviction list with a 30-day TTL** (configurable) — NOT a permanent tombstone, and NOT nothing. Three-way distinction:
- **Delete tombstone** — permanent (until manual unban); explicit refusal.
- **Eviction cooldown** — 30-day TTL, then PURGED (forgotten); auto, from eviction.
- **Passive fade** — no record; stop-repairing, attrition removes.

Purpose:
1. **Allow future re-upload.** A permanent tombstone would block the CID from ever being re-distributed; the TTL means after 30 days the record is forgotten, so a legitimate re-publish distributes normally. Eviction is "not now," not "never."
2. **Prevent too-fast refill.** While in cooldown, repair/distribution/ingest will NOT re-acquire the CID — so a node that just evicted content doesn't immediately refill it before the network truly sheds it (anti-thrash).
3. **Want/pin override.** A manual want or pin removes the CID from the eviction list immediately — intent beats the cooldown.

Build unit (pairs with the not-yet-wired disk-watermark eviction trigger): disk-pressure eviction → cooldown list (30d TTL, persisted) → refill-prevention checks in ingest/repair/distribution → want/pin override → purge-after-TTL. The cooldown is the *record*; the eviction trigger is the *event* that populates it — build together.

### Manifests — Names, Sizes, Folders (Decision 2026-07-03)

A CID is `BLAKE3(bytes)` — content *identity*. A filename is NOT part of it (identical bytes → identical CID → dedup). Names, sizes, MIME types, and folder structure are *metadata about* content, held in a **manifest**: a small content-addressed object.

**Model (ZephCraft's "git tree object"):**
```rust
enum Manifest {
    File { name: String, size: u64, mime: Option<String>, content_cid: Cid },
    Dir  { name: String, entries: Vec<Entry> },   // entry.cid → a file or dir manifest
}
struct Entry { path: String, size: u64, cid: Cid }
```
The manifest is postcard-serialized, stored as a CraftOBJ object → `manifest_cid`. Sharing the manifest CID conveys names + sizes + structure + the content CIDs. This is exactly BitTorrent's infohash-of-a-`.torrent` / IPFS UnixFS-DAG / git tree — the immutable, content-addressed directory representation. Each file is independently content-addressed, so the **same file in two folders is stored once** (dedup across folders) and every file is erasure-coded + self-healing.

**Relationship to CraftVFS (foundation Part F/§ CraftVFS) — complementary, NOT redundant:**
- **Manifest** = immutable, self-contained, lightweight, no SQL needed → the *share/snapshot* format (git tree object; BitTorrent .torrent).
- **CraftVFS** = mutable personal filesystem as SQL inode/dirent tables on CraftSQL → the *working-filesystem* view on top.
- They share one schema (`names → CIDs + sizes`). CraftVFS SITS ON manifests: its dirents point at content CIDs; snapshotting a subtree *produces* a manifest; fetching a manifest *materializes* into CraftVFS. Round-trippable by design (same fields). The manifest is the base layer, available now without the CraftSQL stack; CraftVFS is a later, heavier mutable layer over it.

**CLI:** `zeph publish <file>` → file manifest → returns manifest CID; `zeph publish <dir>` → recursive dir manifest. `zeph get <manifest_cid> -o <path>` → fetch manifest → restore file (with name) or recreate the tree. Content metadata (name/size/mime) is also announced lightweight to the tracker so dashboards show it without a manifest fetch.

### Deletion — Signed Tombstone + Crypto-Shred (Decision 2026-07-03)

**The fundamental ceiling (stated honestly):** you cannot *force* erasure of public bytes from a node that deliberately keeps them — content lives on others' disks, there is no authority to command deletion, and anyone with the bytes can re-publish them (same CID). This is true of BitTorrent, IPFS, and the web. "Permanently remove" therefore means best-effort purge from cooperating nodes, **not** guaranteed erasure from adversarial ones — except via cryptography (below).

Two mechanisms:

**1. Signed delete tombstone (best-effort, effective on the honest majority).** The publisher issues a `DELETE(cid)` record signed by the publishing identity, propagated via the tracker/gossip. Honest nodes that receive it:
- drop their pieces for the CID and stop announcing as provider;
- add the CID to a **local tombstone set** so the self-healing machinery cannot resurrect it — HealthScan skips it, Distribution won't forward it, ingest refuses new pushes for it, repair won't recode it.

The tombstone is kept locally (small: CID + signature); it persists rather than expiring, because a TTL'd tombstone would let repair resurrect deleted content. The CID fades from discovery and stops healing everywhere except on nodes that deliberately retain it.

**2. Cryptographic deletion (guaranteed — for encrypted content).** For content encrypted at the app layer (VISION), the network only ever held ciphertext. **Destroying the key renders every copy permanently unrecoverable, regardless of piece persistence** — crypto-shredding, the standard answer to erasing data you cannot physically reach. This is a real guarantee, not best-effort, and it comes for free once app-layer encryption lands. Posture: encrypt-by-default for anything that may need true deletion.

**Authority is publisher-scoped (hard rule, anti-griefing).** A delete is honored only for content a node holds *on that publisher's behalf* (pieces were pushed under that identity's authenticated session / ContentRecord). A stranger's `DELETE` can never purge content a node independently holds or wants — otherwise deletion is a censorship weapon. Consequence of content-addressing: your delete covers *your* upload; the same bytes independently published by someone else are a separate provenance and unaffected.

**Survival is an open market of HAVE and WANT signals — NOT per-publisher accounting (Decision 2026-07-03, refined from the multi-uploader + heartbeat questions; supersedes an earlier per-publisher-refcount sketch).** Two flaws kill per-publisher refcounting: (a) the publisher may hold nothing — holding was never the basis of authority (for encrypted content authority is the *key*; for public content it is essentially nil); (b) provider records carry no publisher provenance — a record says "N holds X", signed by N, not "on behalf of P" — so a per-publisher count can't even be reconstructed. Survival is instead governed by **two independent, open, signed, TTL-leased signal sets — anyone may emit either, neither tied to the other:**
- **HAVE** (provider record) = "I hold pieces of X." *Supply* — makes content servable. Signed by the holder.
- **WANT** (interest record) = "I want X kept alive." *Demand* — directs the network's active maintenance (repair/scaling). Signed by the wanter, **no holding required.**

Content lives while supply serves it and demand keeps the network investing; it fades when **both go quiet** ("when no one cares, it dies naturally"). WANT-without-HAVE is deliberate: it signals content still matters after holders drift off, and it is the trigger that *pulls supply back* (demand → repair/re-host). It is a **vote, not a guarantee** — many WANT + none HOLD ⇒ content is honestly lost despite interest. A **PIN** is the guarantee (WANT backed by your own disk, eviction-exempt); WANT is the cheap open interest signal. **Spam-proof:** WANT obliges no one to store, and repair only recodes among *existing* holders, so WANT for content nobody holds does nothing — supply stays voluntary, demand only prioritizes among existing supply.

**Consistency — no global counter, a CRDT of signed leases.** A shared mutable count would need consensus (rejected). Each HAVE/WANT/PIN and each withdrawal is a signed record with exactly one writer (single-writer-per-identity ⇒ no conflicts ⇒ CRDT, per-emitter LWW by HLC). Observable via the tracker registry keyed by CID (self-verifying — the tracker aggregates, can't forge; DHT in M3, no single index); each node is authoritative for its own disk. Signals are **leases** kept alive by the re-announce loop: explicit `DELETE` withdraws immediately; silence (leave / crash / *lost* delete) decays by TTL — the sets self-clean without any decrement message arriving. GC is **local + fail-safe**: drop pieces only when nothing local holds AND no supply/demand is observed; a stale view only *delays* the fade, never wrongly erases. The CID graph is acyclic (CID = hash of contents) ⇒ a **DAG, no cycles** ⇒ the easy case for GC.

**Deletion under this model:** withdraw only your *own* signals; no one can force-withdraw another's (censorship-resistant). The only *hard* erasure is **crypto-shred** (encrypted content, key = authority — needs no provenance). A publisher's `DELETE` of public content is a best-effort request honored where a node *locally* recorded "I got this from P" (effective for fresh, directly-pushed content; naturally ineffective once spread wide — you cannot unpublish the public). Encrypted content never dedups across authorities (per-key CID), giving it a clean sole owner.

**Store gap (build item):** `pin` is a boolean today; the store needs to track its HAVE set and observed WANT to drive eviction/maintenance. Lands with deletion + lifecycle (M2/M3).

**`unpin` vs `delete`:** `unpin` (local) stops *you* guaranteeing/serving it — the network keeps it alive via distributed copies + repair. `delete` (network, signed) asks the whole cooperating network to purge it *and* blocks resurrection. Going offline is neither — the network carries on.

**Sequencing:** the tombstone *set* is designed into the M2 lifecycle from day one (repair/distribution/ingest must consult it, or they resurrect deleted content); full signed-delete propagation + the crypto-shred guarantee land across M2/M3.

### Admission — Quotas + Reciprocity (Decision R5)

Nodes are not obligated to accept pushed pieces. v1 policy:

- **Per-publisher quota**: default 1 GB accepted bytes per publisher NodeId (config: `admission.per_publisher_quota`). Pieces beyond quota → `Ack { status: QuotaExceeded }`.
- **Reciprocity ledger**: local given/taken byte counters per peer. Under disk pressure (>80%), pieces from peers with better reciprocity ratios win admission; strangers are best-effort.
- Publisher identity = the NodeId that signs the push session (transport-authenticated via QUIC/iroh — no extra signature needed).
- Spam economics: filling the network costs the spammer their quota at every victim independently.

### Eviction & Disk Watermarks

Eviction is a purely local decision (disk pressure, LRU, local policy). No network notification message — but evictions become visible to the network within one HealthScan cycle via live probes (see below), not after a 48 h TTL.

| Disk usage | Action |
|---|---|
| < 90% | normal |
| 90–95% | stop accepting remote pieces; run eviction |
| 95–99% | evict LRU non-critical pieces; stop inbound transfers |
| > 99% | refuse all writes except critical DB ops |

Two-phase mark-and-sweep with 5.5-minute safety window; first run deferred 10 min post-startup.

---

## Content Routing — the `ContentRouting` Trait (Decision R7)

The iroh Kademlia DHT is experimental; routing is therefore a swappable trait, not a hard dependency:

```rust
#[async_trait]
pub trait ContentRouting: Send + Sync {
    /// Announce this node as a provider for `cid` (piece_count is advisory).
    async fn announce(&self, cid: Cid, piece_count: u32) -> Result<()>;
    /// Candidate providers for `cid`. ADVISORY ONLY — never health truth.
    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>>;
    /// Best-effort withdrawal (graceful shutdown path).
    async fn withdraw(&self, cid: Cid) -> Result<()>;
    /// Store/fetch small metadata blobs (ContentRecord, 1h TTL).
    async fn put_record(&self, key: [u8; 32], value: Vec<u8>) -> Result<()>;
    async fn get_record(&self, key: [u8; 32]) -> Result<Option<Vec<u8>>>;
}
```

- **Impl #1 — tracker** (`apps/tracker`): trivially simple announce/resolve service over iroh ALPN `/craftec/tracker/1`. Used for dev, tests, and the early network. Multiple trackers can be configured; announcements go to all.
- **Impl #2 — iroh Kademlia DHT**: standard provider records (48 h TTL, 22 h re-announce), one small record per provider, adopted behind the same trait when the iroh DHT matures.
- Nodes may run both simultaneously (`routing.backends = ["tracker", "dht"]`); resolve = union.

Provider records are **candidate lists**, never availability truth — HealthScan verifies live (below).

---

## HealthScan — Live Probes + PDP-Weighted Counts (Decisions R3, R4)

KERNEL-LEVEL. Runs every 5 minutes; each node scans 1% of the CIDs for which it holds ≥2 pieces (full pass ≈ 8 h). Initial scan deferred 5 min post-startup.

**Per cycle:**

1. **Gather candidates**: for each scanned CID, `routing.resolve(cid)` → candidate provider list.
2. **Batch by provider**: group all scanned CIDs by candidate provider — one `AVAILABILITY_PROBE` per provider per cycle, listing every scanned CID that provider is a candidate for.
3. **Probe**: provider answers `AVAILABILITY_ACK` with, per CID: held piece count + coding-vector list (lightweight — vectors are k bytes each, not piece data).
4. **PDP piggyback**: the probe embeds a PDP challenge for one randomly selected (CID, piece) pair per provider (coefficient cross-check, below). Failure ⇒ that provider's claims are **discounted for all CIDs this cycle**, reputation penalty applied, parole mode entered.
5. **Count**: `verified_count(cid)` = Σ pieces from providers that (a) answered the probe live, (b) passed any PDP challenge issued to them. Silent evictions are visible immediately: an evicted provider simply reports 0 pieces.
6. **Rank-aware health** (carried over from v1 — better than counting): with coding vectors in hand, compute the coefficient-matrix rank per segment. `rank < K` ⇒ DATA AT RISK (highest repair priority); `verified_count < n` ⇒ under-replicated.

Traffic cost: one probe message per known provider per cycle (~O(providers)), instead of per-CID-per-provider. DHT/tracker queries are only for candidate discovery.

### Repair — Rendezvous Election (Decision R2)

No coordinator, no election messages, no local-score dependence:

1. Candidates = probe-confirmed providers holding **≥2 verified pieces** for the CID, filtered by membership-alive.
2. Rank every candidate by `BLAKE3(node_id ‖ cid ‖ epoch)` where `epoch = floor(HLC_wall / 1h)`.
3. deficit `N = n − verified_count` (or `K − rank` escalated priority). The top-N ranked candidates each recode **1 new piece** this cycle (fresh random GF(2⁸) coefficients — no decode, no network fetch).
4. New piece is vtag-verified locally, then distributed: 1-piece holders first, then non-holders.
5. Failed/offline elected nodes are simply absent next cycle's candidate list; the ranking re-computes. No synchronization.

Deterministic from public inputs → every node computes the identical top-N. Epoch rotation spreads repair load; per-CID hashing spreads it across the network.

### PDP — Coefficient Vector Cross-Check (carried over from v1)

The challenger holds pieces of the same segment and uses GF(2⁸) linear algebra:

1. Challenger sends nonce + random byte positions for a claimed piece.
2. Prover returns the bytes at those positions + its coding vector, signed over the nonce.
3. Challenger expresses the prover's coding vector as a combination of its own pieces' vectors and computes the expected bytes; mismatch = fraud.
4. Pass ⇒ `StorageReceipt` issued (point-in-time possession proof). Fail ⇒ discounted from health counts + reputation penalty. Receipts feed reputation now; settlement/economics is a deferred design (see below).

```rust
StorageReceipt {
    content_id: [u8; 32],
    storage_node: [u8; 32],  // Ed25519 pubkey = iroh NodeId
    challenger: [u8; 32],
    segment_index: u32,
    piece_id: [u8; 32],
    timestamp: u64,          // HLC
    nonce: [u8; 32],
    signature: [u8; 64],     // challenger Ed25519
}
```

---

## Fetch — Client-Side Intelligence (carried over from v1, transport updated)

Storage nodes are simple piece servers. All strategy lives in the client:

1. `routing.resolve(cid)` → candidates; prefer low-latency; open a capped pool (20–50 QUIC streams via the single iroh connection per peer).
2. Request **"any piece for CID X segment Y, excluding these piece_ids"** (exclude-list, Bitswap-style). Provider returns a random held piece + coding vector.
3. Client vtag-verifies, checks linear independence (Gaussian elimination, progressive); dependent pieces are discarded (~1/256 probability — cheap).
4. K independent pieces per segment → decode → verify `BLAKE3(segment bytes)` against the content record; final content verified against CID.
5. Singleflight dedup of concurrent fetches for the same CID; slow connections dropped and replaced; sequential-scan prefetch for streaming.
6. Streaming: segments decode independently and in order for media; in parallel for downloads; range requests fetch only covering segments.

---

## Wire Protocol Additions

All messages use the foundation §23 frame (postcard, 17-byte v1 header, HLC timestamp). CraftOBJ ALPN: `/craftobj/1`. New/updated message types beyond foundation §23:

| Type Tag | Message | Direction | Description |
|----------|---------|-----------|-------------|
| 0x0010 | PIECE_REQUEST | client→provider | CID, segment, exclude-list of held piece_ids, max_pieces |
| 0x0011 | PIECE_RESPONSE | provider→client | batch of CodedPiece (streamed) |
| 0x0012 | PIECE_PUSH | distributor→peer | one CodedPiece; receiver vtag-verifies before ack |
| 0x0013 | PIECE_PUSH_ACK | peer→distributor | Ok / QuotaExceeded / StorageFull / VtagInvalid |
| 0x0041 | AVAILABILITY_PROBE | scanner→provider | list of CIDs + optional embedded PDP challenge |
| 0x0042 | AVAILABILITY_ACK | provider→scanner | per CID: piece count + coding vectors; PDP response if challenged |

Clock-skew policy (Decision R9): these messages are **warn-and-accept** on >500 ms HLC skew (clamped HLC merge + metric). Strict rejection applies only to attestation and SIGNED_WRITE paths.

---

## Security Summary

| Threat | Defense |
|---|---|
| Pollution (garbage pieces) | Null-space vtags verified at ingest, repair, and fetch (2⁻⁶⁴) |
| Fake availability (suppressed repair) | Live probes + PDP discount — self-asserted records never counted |
| Storage spam | Per-publisher quotas + reciprocity ledger |
| Sybil provider inflation | PDP cross-check; failed provers discounted + paroled + banned (§Foundation 19–20 subnet diversity) |
| Content tampering | CID = BLAKE3(content); piece_id binds vector+data |
| Replay | HLC nonce in PDP; strict skew window on signed paths |
| Snooping storage nodes | Encryption above CraftOBJ; nodes store opaque bytes |

## Deferred (explicitly out of v2 scope)

- **Economics/settlement** (creator pools, subscriptions, tiers, on-chain claims): future `ECONOMICS_DESIGN.md`. StorageReceipts are collected from day one so the data exists when settlement lands.
- **Proxy re-encryption / key sharing**: application layer, future doc.
- **Mutable pointers** (`identity → CID`): specified at CraftSQL layer (root CID publication, foundation §37).
- **Mobile bindings**: after M2. The daemon **control API** (JSON-RPC over Unix socket + localhost WebSocket) and the Tauri desktop shell are scheduled — see feature tracker milestone M-UI; the old craftec-ipc conventions serve as design reference only (greenfield code).

## Testing Requirements (per foundation §46)

- **Property tests**: `decode(any K independent pieces) == source`; `vtag_verify(recode(p1, p2)) == true`; `vtag_verify(corrupted) == false`; CAS `get(put(x)) == x`.
- **DST**: seeded single-threaded simulation of churn — validate healing rate vs churn rate (Review item R4) before tuning constants.
- **Integration**: 5-node cluster + nemesis (kill nodes, drop pieces, lie in probes) — data must survive and repair.

## Milestones

- **M1 — Foundation**: crates `core, crypto, wire, erasure, transport, membership`. Exit: two nodes connect (iroh), exchange pieces, decode; vtags verify; property tests green.
- **M2 — Storage network**: crates `routing (tracker), store, obj, noded` + `apps/tracker`. Exit: N-node network stores/fetches under churn with repair; DST harness validates healing > churn.
- **M3 — Hardening**: PDP receipts, reputation/parole, admission quotas live, disk watermarks, iroh-DHT routing impl behind the trait.
