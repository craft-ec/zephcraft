# ZephCraft — The Consolidated Design & State

**Date:** 2026-07-09 · **Status:** AUTHORITATIVE — the single reconciled description of the system as built
**Scope:** consolidates and reconciles every design document (`docs/*.md`, `zephcraft/docs/*.md`) against the code. Where an older document disagrees with this one, **this document wins**; Part 14 lists every superseded claim per document. The code remains the ultimate source of truth; every section below was verified against the crates at the date above.

---

## Part 0 — How to read this document

- **What this replaces.** Design knowledge was spread across ~20 documents written between 2026-02 and 2026-07, several of which describe models the code has since replaced (k-of-n attestation, the tracker service, Pkarr, fixed 256 shards, synchronous CAS writes, per-shard postcard blobs). This document is the consolidation the Tier-0 reconciliation called for: one detailed, current description.
- **Precedence order:** (1) the code, (2) this document, (3) `STATE_AND_ROADMAP.md` (the working reconciliation ledger), (4) `craftec_technical_foundation.md` v3.7 **`docs/` copy** (§62 amendments win over body; several sections carry STALE banners), (5) everything else. The `zephcraft/docs/` copies of the foundation and ZEPHCRAFT_NETWORK are **outdated snapshots** — cite the root `docs/` copies.
- **Retired models** you may still find described elsewhere, none of which exist in the running system: the tracker census/routing service; k-of-n attestation / committees / quorums as registry write authority; Pkarr DNS publication; DHT-published DB roots (`KIND_ROOT`/`KIND_MANIFEST`); synchronous `expected_root_cid` CAS with `WRITE_CONFLICT`; a fixed 256-shard registry; per-shard postcard state blobs; a governed-WASM registry validator.
- **Naming.** **Craftec** is the ecosystem (the node + a family of product apps). **ZephCraft** is the node — the `zephcraft/` Rust workspace, the `zeph` daemon, the `zeph-*` crates. The product apps (mindcraft, handcraft, salamcraft, flowcraft, …) are independent Solana/web apps and are **not on the node stack yet**.

---

## Part 1 — Vision & positioning

### 1.1 The thesis

> **The world's shared storage grid — every device contributes spare disk into one content-addressed pool that replicates only what matters.**

The internet already has the hardware: billions of devices with idle disk. ZephCraft pools it into a single elastic utility — you draw from it, you contribute to it, and the network keeps alive exactly what is needed, no more. The mental model is the electricity grid: nobody owns a generator sized to their peak; you draw from a grid sized to aggregate demand.

The critical scaling identity — the anti-blockchain property — is **distribution, not replication**: more devices joining means **more capacity, not more redundancy**. A blockchain's every-node-stores-everything model makes growth O(N) waste; the cloud's rented data centers make durability a bill. ZephCraft's redundancy is *engineered to need* from three independent knobs:

```
replication ≈ durability floor (erasure) + live demand (scaling) + intentional pins
```

- **The floor — erasure coding.** RLNC coded pieces at `n = k·ceil(2 + 16/k)` (k=8 → n=32 today; the ~3× floor quoted for K=32 segmentation is a design target, §12). Survives losing the majority of holders at a fraction of full-copy replication's cost.
- **Demand — scaling.** Providers are recruited where there is measured traffic (served piece-pulls, not interest declarations) and shed when demand fades. Temporary and bandwidth-driven.
- **Intent — pins and wants.** A pin is a guaranteed whole-copy anchor; a want is a keep-alive intent without holding. BitTorrent's "seed forever," done deliberately.
- **The fade.** Content nothing pins, wants, or fetches is *allowed to die*. This is a feature: the network never pays to keep garbage alive, and it is what makes the other three knobs meaningful.

### 1.2 The contrasts

| system | how it replicates | the catch |
|---|---|---|
| BitTorrent | Accidental — a side-effect of who's online. Popular = 1000× copies; unpopular = 1× then 0. | Wasteful **and** fragile at once: whole-file copies, no erasure, no dedup, no repair — dies when the last seed leaves. |
| The cloud | Hidden in a rented data center at "11 nines" you cannot verify. | You don't own it, you pay forever, one company can revoke it. |
| Blockchains | Total — every node stores everything. | O(N) waste; more nodes ≠ more capacity. |
| **ZephCraft** | Engineered to need — floor + demand + pins, deduplicated, self-healing. | Leaner *and* more durable at once. Distribution, not replication. |

### 1.3 Why it's possible now

BLAKE3 content addressing (dedup + integrity are free — the name is the checksum); RLNC erasure with **repair-without-decode** (any holder of ≥2 pieces mints fresh valid pieces locally — no rare-piece problem, no coordinator); a protocol-driven, demand-proportional lifecycle; and **single-writer-per-identity** which eliminates consensus entirely — no blockchain, no gas, no global agreement, linear scaling.

### 1.4 No token, no privileged roles

There is no token: the free tier is reciprocity/altruism; a paid tier, when it exists, is USDC. There are no privileged roles: a node is an Ed25519 keypair running the `zeph` daemon; every "global" facility — content DHT, relays, bootstrap seeds, governance — is a service anyone can run. Founder-proofing is a designed ladder (M-OPEN): if the founders vanish, running nodes keep storing, serving, and healing indefinitely; only upgrade *coordination* is lost, and governance itself is built to hand off (Part 10).

### 1.5 Positioning decisions (settled)

- **Storage-grid-first** go-to-market (supersedes VISION.md's unmarked "financial network first" section). The grid claim is demonstrable from the running network.
- **"Engineered to need"** is the differentiator, not raw decentralization — one claim beats BitTorrent, cloud, and blockchain simultaneously.
- **No overclaiming.** The public page shows the real (small) live numbers; claims ship as measurements, not arguments.

---

## Part 2 — System overview

### 2.1 The vertical

ZephCraft is a full data-infrastructure vertical, all layers **built and validated on a live 5-node cluster** (4 Hetzner instances + 1 NAT'd Mac governor):

```
   apps / product layer (future)
        │  invoke / deploy / SQL
   ┌────▼─────────────────────────────────────────────┐
   │ COMPUTE   one WASM runtime, per-program           │
   │           capability grants, deterministic profile │
   ├───────────────────────────────────────────────────┤
   │ COORDINATION  head registry: owner-signed LWW     │
   │           CRDT, governed dynamic shards, per-shard │
   │           CraftSQL state, rotating writers, K=3    │
   ├───────────────────────────────────────────────────┤
   │ DATABASE  CraftSQL: SQLite VFS over content-       │
   │           addressed pages, single-writer, durable  │
   │           generations, network recovery            │
   ├───────────────────────────────────────────────────┤
   │ STORAGE   CraftOBJ: BLAKE3 CIDs, RLNC erasure,     │
   │           health-scan lifecycle, want/pin/fade     │
   ├───────────────────────────────────────────────────┤
   │ ROUTING   Kademlia DHT (zeph-dht) — sole backend   │
   │ MEMBERSHIP HyParView views + converged census      │
   │ TRANSPORT iroh QUIC, relays, ALPN composition      │
   │ IDENTITY  Ed25519 keypair = NodeId = QUIC key      │
   └───────────────────────────────────────────────────┘
```

Governance (a k-of-n governor multisig over a self-verifying chain) and encryption (XChaCha20-Poly1305 + Umbral PRE, private files and private databases) cut across the stack.

### 2.2 What "validated" means

Proven on real hardware, not in simulation: cross-node and **offline-owner** name resolution; DB roots riding the registry and surviving node restarts; deploys in ~40 ms; a 19-node scaling run (census election: eligible 6→19); **online resharding** grow 8→9 and shrink 9→8 bits with keys migrated live; state migration on membership change; encryption round-trips; a real SQL app (guestbook) deployed, invoked, persisting, surviving restart.

### 2.3 The crates

`zephcraft/crates/` (17): `core` (Cid, NodeId, HLC), `crypto` (NodeIdentity keystore), `cipher` (XChaCha20-Poly1305 + Umbral PRE), `wire` (postcard frames), `erasure` (RLNC + vtags), `transport` (iroh), `membership` (views + census), `dht` (Kademlia), `routing` (ContentRouting over the DHT), `store` (piece store), `obj` (CraftOBJ engine), `sql` (CraftSQL), `com` (WASM runtime, governance types, invoke), `events` (node event bus), `sched` (prioritized job coordinator), `noded` (the `zeph` daemon: registry, governance store, accounts, control/dashboard), `testkit`. Strict downward-only dependencies; strictly sequential build discipline (one work item at a time, walking-skeleton order).

---

## Part 3 — Identity, wire & transport

### 3.1 Identity

`NodeId = [u8;32]` is the Ed25519 **public key verbatim** (iroh convention, no hashing) — and the node's Ed25519 key *is* its iroh QUIC secret key, so `zeph NodeId == iroh EndpointId` and **every connection is mutually authenticated by construction**. `NodeIdentity` (crates/crypto): ed25519-dalek with `verify_strict` (rejects malleable signatures); keystore `<dir>/node.key` (raw 32 B, 0600 enforced, atomic temp→fsync→rename writes, key written last, zeroized on drop). Sign ~7 µs, verify ~2.3 µs. There is **no Pkarr** and no DID layer — discovery is iroh presets + configured seeds.

### 3.2 Content addressing & time

- `Cid([u8;32]) = BLAKE3(bytes)` — immutable, self-verifying (`Cid::verifies`), dedup automatic. Everything is a Cid: files, DB pages, vtag blobs, WASM programs, manifests, governance chains. Raw 32 B (no multihash; migration path documented).
- **HLC**: 64-bit packed `(48-bit wall-ms << 16) | 16-bit logical`; `now()` strictly monotonic (CAS); `merge()` clamps remotes > `MAX_SKEW_MS=500` ahead. Skew policy: **warn-and-accept** for ordinary messages (a broken clock must not partition a node off the storage plane, but must not drag others' HLC forward), strict rejection reserved for signed-write paths.

### 3.3 Wire protocol

Frame v1 (big-endian): `[type_tag:u32 | version:u8=1 | hlc_ts:u64 | payload_len:u32 | payload]`, header 17 B, max message 4 MiB, payload = postcard. The **as-built tag table** (`wire/src/lib.rs`) is authoritative (foundation §23's table is stale): `0x0001/0x0002 PING/PONG`; membership `0x0100–0x0107` (JOIN, FORWARD_JOIN, NEIGHBOR, NEIGHBOR_REPLY, DISCONNECT, SHUFFLE, SHUFFLE_REPLY, MEMBER_SYNC); pieces `0x0010–0x0013` (PIECE_REQUEST/RESPONSE/PUSH/PUSH_ACK); routing-record tags `0x0200–0x0204` (TRACKER_* frame types — **wire-compat fossils, no longer sent**; the `SignedRecord` shape they carried now travels inside the DHT's own postcard protocol on `/craftec/dht/1`); `0x0041/0x0042` AVAILABILITY_PROBE/ACK; `0x0043` RELEASE_SYSTEM. `SignedRecord{kind, node_id, payload, hlc_ts, sig}` — Ed25519 over `kind‖node_id‖payload‖hlc_ts`, re-verified by every consumer, never trusted on relay.

### 3.4 Transport — one iroh endpoint, ALPN-composed

A single iroh (QUIC) endpoint carries every protocol via ALPN; no per-protocol sockets. Reach modes: `local` (direct sockets only; tests/LAN) and `relayed` (discovery + relays; production). Relay composition: configured `relay_urls` first (default `https://relay1.zeph.craft.ec`), n0's four public relays appended as lowest-priority fallback unless `fallback_relays=false`. Peer addresses are `<node_id_hex>@<ip:port>[,…]` (relay URLs accepted inline).

**Endpoint lifecycle:** the endpoint is *rebindable* — `Transport::rebind()` tears it down and rebuilds it from the saved bind config (same identity, port, relays, ALPNs); accept loops re-attach via an epoch counter and upper layers never hold the raw endpoint. Exists because a long-lived endpoint can wedge after uplink path churn (stale QUIC path state; dials fail forever while the network is fine) — membership's isolation watchdog (§4.1) is the caller.

**ALPN inventory (complete, exact strings):**

| ALPN | Purpose |
|---|---|
| `/craftec/ping/1` | RTT/liveness ping-pong (probe path; HLC-derived nonce) |
| `/craftec/member/1` | HyParView joins/shuffles + census gossip |
| `/craftec/piece/1` | CraftOBJ piece push/request, availability probes |
| `/craftec/sqlpage/1` | cross-node CraftSQL page fetch |
| `/craftec/invoke/1` | remote CraftCOM invocation |
| `/craftec/registry/1` | head-registry writer/replica RPC |
| `/craftec/dht/1` | Kademlia content-routing DHT |
| `/craftec/tracker/1` | **wire-compat fossil** — defined, never dialed or served |

**Relay reality:** `relay1.zeph.craft.ec` is iroh-relay behind Coolify Traefik (TLS at the proxy, plain HTTP :3340). Known limits: no QUIC address discovery behind the proxy (mixed relay maps prefer n0 as home relay), and public nodes must open their UDP listen port (Hetzner 9944/54/64/74) or peers stay relay-only.

---

## Part 4 — Membership & the converged census

Two layers ride `/craftec/member/1`: bounded **HyParView partial views** for gossip topology, and a **converged census** for anything requiring cluster-wide agreement.

### 4.1 HyParView views + probing

Defaults: `active_size=5`, `passive_size=30`, ARWL=6, PRWL=3, probe 5 s interval / 3 s timeout / 3 consecutive failures → dead, shuffle every 30 s (sample 8), dead-tombstone retention 600 s. Joins random-walk through the active mesh; a full active view evicts a random member to passive (standard HyParView). Probing is SWIM-*style* — direct pings over the transport's ping ALPN; **no indirect PING-REQ, no Suspect/Dead epidemic gossip** (deaths self-detected per node; K10 open).

Hard-won operational semantics (each a fixed regression):
- **`fill_active` drains**: a failed promotion **drops** the candidate — self-cleaning the passive view of dead addresses. The cap+re-queue "self-heal" variant polluted passive and broke active-view fill (commit c3f99c9).
- **Isolation recovery is gentle and additive**: an isolated node dials ONE random retained bootstrap seed at most every 15 s, *alongside* `fill_active` (skipping fill_active while isolated was itself a bug). Membership bootstrap = `cfg.peers ∪ cfg.dht_seeds` — a dht_seeds-only config can still re-bootstrap.
- **Wedge watchdog — isolation that outlasts `wedge_rebind` (120 s) triggers a transport rebind**: a long-lived iroh endpoint can wedge after uplink path churn (stale QUIC path state — every dial to known-alive seeds dies in seconds while ICMP on the same path is clean; measured on the hotspot Mac, where a process restart reconnected in 15 s). Seed re-dials can't fix this (they dial *through* the wedged endpoint), so membership asks the transport to tear down and rebuild the endpoint with identical config (same identity/port/relays/ALPNs; serve loops re-attach via an epoch counter), then re-arms seed recovery. Guarded: never fires on a node with no bootstrap seeds (solo nodes are expectedly isolated), and re-arms a full window between attempts.

### 4.2 The converged census (the election substrate)

`members: NodeId → {addr, last_heard_ms}` — a full member map converging on every node via **union + max-last_heard merge** (commutative, idempotent — a CRDT join, property-tested). Gossip is **piggybacked on the 30 s shuffle** (`Shuffle.members`/`ShuffleReply.members`), not a dedicated round — a dedicated 10 s member-sync connection collapsed fragile relay-only peers. Every node re-asserts itself each round; any authenticated inbound message bumps the sender. `census()` = self + every member heard within **`CENSUS_TTL_MS = 120 s`** (generous, sized to multi-hop diffusion at the 30 s cadence); pruned after 600 s of silence.

**Why it exists:** the size-5 active view diverges per node, which capped registry writer elections at ~6 writers regardless of cluster size and made them inconsistent (measured live at 19 nodes). Election-over-census fixed it (eligible 6→19, proven). The census is now the substrate for: registry writer/replica election, governance anti-entropy pulls, the health scan's liveness filter, and the registry readiness gate.

**Known limits (accepted):** full-map gossip is O(N) per shuffle (digest sync is the very-large-N follow-up); census death-detection is TTL aging (K10 SWIM dissemination open).

---

## Part 5 — Content routing: the Kademlia DHT

`zeph-dht` is the **sole** content-routing backend (the tracker service is retired; the `ContentRouting` trait remains swappable).

- **Overlay**: 256 buckets over the 256-bit XOR keyspace, **K=20**, **α=3**; iterative parallel lookups; contacts carry dialable addresses. Bucket policy: refresh-known-to-back, append-if-room, drop-newcomer-if-full (incumbent stability; ping-evict is a recorded refinement).
- **Dead-node eviction**: 3 consecutive failed requests → evict + drop cached connection + **10-min tombstone** (during which `learn()` refuses re-insertion — breaks the evict→re-teach→re-dial storm); **seeds are never evicted** (bootstrap protection). Outbound connections are cached (fresh bi-stream per request, one forced-reconnect retry) — per-op connect/close churn stormed the network during cutover.
- **Records**: `StoredRecord{key, publisher, seq, value, sig}`, Ed25519 over `key‖publisher‖seq‖value`, verified on store, on return, and on snapshot load. Per `(key, publisher)` highest-seq wins; different publishers **coexist** under one key (many providers per CID). TTL 48 h; republish 22 h (obj layer). `put` = sign → store local → lookup → Store to K closest; `get` = iterative α-parallel FIND_VALUE merging highest-seq-per-publisher.
- **Persistence**: records + routing table saved to `dht_records.bin` / `dht_table.bin` — loaded on boot (expired dropped, every signature re-verified), checkpointed every 120 s, saved on shutdown. A fixed-identity infra node restarts with its overlay intact instead of manufacturing a false-at-risk repair storm.
- **Routing layer semantics** (`DhtRouting`): record kinds `KIND_PROVIDER=1, KIND_WANT=4, KIND_META=5, KIND_GRANT=8 (reserved), KIND_APP=9`. Keys: per-cid kinds → `BLAKE3(kind‖cid)`; owner-keyed heads → `BLAKE3(kind‖owner‖name)` with reads filtered to the owner's signature, **highest-version-wins, no CAS** (a DHT has no arbiter). Withdraw = empty-value tombstone at a fresh seq; TTL reclaims. Provider records are **candidate lists, never availability truth**. There are **no** `KIND_ROOT`/`KIND_MANIFEST` — DB roots and manifests ride the registry (Part 9).

---

## Part 6 — Storage: CraftOBJ

### 6.1 Erasure math

RLNC over GF(2⁸) (poly 0x11D, compile-time tables, scalar ops — SIMD deferred). Floor shared by encoder and health scan: **`target_pieces(k) = k × ceil(2 + 16/k)`** → k=8→**32**, k=16→48, k=32→96. As built, publish is a **single whole-object generation at k=8** (n=32; K=32 8-MiB file segmentation is deferred). `piece_id = BLAKE3(coding_vector‖data)`. **Recode** mints fresh valid pieces from ≥2 held pieces with *no decode* — the churn-repair property; recodes-of-recodes are first-class. Decode is progressive Gaussian elimination.

**Vtags** (pollution defense): L=8 public null-space tags (`scheme=SCHEME_NULL_SPACE_V1`, upgrade byte reserved); any node verifies any piece — including recoded repair pieces — **at ingest, before storing**; 2⁻⁶⁴ forgery bound vs non-adaptive corruption (adaptive-attacker upgrade path: pairing-based schemes; defense-in-depth: PDP, deferred). Vtags ride inside `Generation` on every push/response (no separate vtags object, no quarantine window).

### 6.2 The store

256-way sharded CAS: `cid/<hex2>/<hex>/{meta, content, pieces/}` + marker dirs (`tombstones/ wanted/ distributed/ evicted/`); atomic writes; index rebuilt on open. Two holding classes: **pieces** (coded, evictable) and **pin** (whole content, eviction-exempt — and a pinner **encodes fresh pieces on demand**, so it never runs dry). Flags per CID: `pinned`, `system` (CraftSQL generations — full lifecycle, excluded from user ops and eviction), `wanted`, `distributed`, tombstone (banned; blocks resurrection), eviction-cooldown (30 d anti-thrash; cleared by manual want/pin). Quota: single threshold, LRU `evict_to(90%)`.

### 6.3 Publish — fire-and-forget

`publish` returns the cid **immediately** (`durable:false`): encode + vtags → mark class (system/pin/want) → **retain whole content synchronously** (the publisher's copy is the guaranteed replica) → background: push n pieces round-robin over live membership candidates, each under **PUSH_TIMEOUT=3 s**; mark `distributed` at `pushed≥n` or `distinct≥min(durability_threshold, candidates)`. A re-publish of a `distributed` cid **must not** re-encode (fresh random pieces would grow the cluster's piece count without bound) — it refreshes the announce and returns. `distribute_pending` (12 s loop) completes the erasure spread for retained content; the health scan confirms durability after the fact. Rationale: the old blocking ≥K-acks publish let one slow relay-only peer hold a deploy hostage for ~10 s.

### 6.4 Get

Local whole-content shortcut → decode from local pieces (fetch only the deficit) → resolve providers → rounds of 1-piece requests to ≤16 providers concurrently with exclude-lists (≤64 rounds); **every piece vtag-verified** (pollution = hard error); final whole-content BLAKE3 check. `ConsumeMode::Seed` keeps generation+content and announces as a transient provider; `Drop` holds nothing (`Ephemeral` currently aliases Drop).

### 6.5 The health lifecycle — five behaviors, one loop

Scheduling is a **per-CID delay queue**, not an O(N) sweep: at-risk/converging cids re-check at 30 s; healthy cids back off ×2 to 32 min; discovery feeds new cids every 10 s; work runs through a deduped, prioritized job coordinator (Repair > Encoding > Distribution > HealthScan > Eviction), paced in chunks of 5. The **first** scan waits for the restart settle gate: membership > 0 AND DHT table > 0, both stable 10 s (≤90 s grace) — scanning a half-formed overlay manufactures false repair storms.

**All heavy work goes THROUGH the coordinator** (2026-07-09; before this, publish distribution was a raw spawn per publish, `distribute_pending` an inline loop, registry replication a spawn per write, and repair executed *inside* the scan job at HealthScan priority — together the sender-side engines of the mass-rejoin storm). The engine detects and sends a work trigger; the node maps it to a job: publish distribution = Encoding (`publish:{cid}`), repair = Repair (`repair:{cid}`, need + election re-checked at execution time so a recovered/faded cid is left alone), pending-distribution = Distribution (deduped per tick), registry shard replication = Distribution (`pushstate:{shard}`, pushes the FULL current shard state at run time with a per-shard dirty counter so dedup-coalesced writes are never lost). Protocol liveness (membership probe/shuffle, governance tick, census-gated migrate/reshard) deliberately stays outside the coordinator — queuing it behind repair would break the census the coordinator depends on.

**Resource manager** (supplements the coordinator): the node reads its own cgroup-v2 `memory.max` as its budget (systemd `MemoryMax=`; no limit or non-Linux → gauge off) and samples RSS every 5 s. Above **85%** the dispatcher defers everything below Repair; above **95%** nothing new starts and inbound intake sheds — piece pushes and registry PushState answer "busy" (senders' next pass retries). Deferrals and `mem_load_pct` are visible in job stats. Born from the 20-node rejoin thrash: unbounded intake + spawn storms ballooned rejoining nodes past their caps into OOM kill-loops.

Availability is measured from **provider records filtered by membership liveness** (dead holders' records are ignored) — deliberately *not* per-cid live probes (probing was O(cids×providers×2 s) and hung nodes; a repair push verifies reachability at the moment it matters). `AvailabilityProbe` messages exist and are answered but never issued (K8 half-built by choice).

- **REPAIR** (below the floor, wanted): rendezvous election — `max BLAKE3(node_id‖cid‖epoch)`, epoch = 30 s — picks exactly **one** capable holder (≥2 pieces, or pinned content), which mints **one** fresh piece (recode; no decode, no fetch) and pushes it to the fewest-piece live holder; sole-survivor fallback recruits a fresh node. Passive retained copies never repair (unbounded-mint guard).
- **DEGRADE/shed** (cold surplus): rendezvous-elected shedder drops one piece per pass toward the floor; warm surplus (pulls ≥ 5) is kept for bandwidth. **Hysteresis**: Schmitt band `Δ = max(floor/8, 2)` — repair *to* the floor, shed *to* the floor, hold inside the band (kills the ±1 repair/shed flap).
- **OFFLOAD** (the durability gate): a retained-not-pinned-not-wanted whole copy is dropped only once **other** live nodes hold the **full erasure set (n pieces, not merely k)** — k is zero-margin; n leaves n−k slack so survivors can repair among themselves.
- **FADE**: content that is not pinned/system/wanted (locally or via a per-cid DHT `is_wanted` lookup, checked last) and unserved within the 24 h grace simply isn't repaired — passive death by churn. A later want/pin resumes repair.
- **SCALE** (demand): the serve path fires the *instant* a cid's served pulls cross 20/cycle (decoupled from scan backoff); one new provider is recruited per hot cid, ceiling n/2 providers. Scaling is instant and parallel; shedding is elected and serialized — **the destructive direction is slow, the safe direction is fast**.
- **DISTRIBUTION** (join events): holders with >2 pieces *move* (never copy) pieces to the least-full live peer — belief-map guided so stale records can't pile pieces on one host, ack-before-delete, always keeping ≥2 locally (repair-eligible).

Re-announce is TTL-aware: provider records refresh only at 22 h of their 48 h TTL, trickled; ingest announces real growing piece counts (debounced 2 s — undercounted records once made `effective` stick below floor → perpetual repair minting).

### 6.6 Want / pin / delete semantics

**Pin** = local whole copy + eviction exemption (+`pinned` announce); pin ≠ spread — the coded floor is maintained regardless. **Want** = keep-alive intent *without* holding — persisted + announced as a signed `KIND_WANT` record; gates Fade network-wide. All lifecycle ops cascade over the manifest graph (`File→content`, `Dir→entries`, `envelope→ciphertext`) so a file's content can't fade while its manifest survives. `forget_local` = soft drop (re-fetchable); `delete_local` = local tombstone + provider withdraw (blocks resurrection via repair/ingest); **signed network-wide delete propagation was REJECTED, not deferred** — public deletion decays via withdraw + TTL + fade (DELETE_PROPAGATION_DESIGN records the rejected alternative; see Part 11.3).

**System objects** (CraftSQL generations): published via `publish_system` — full lifecycle (repair, distribute, scale, degrade), never pinned, kept alive by a `system` marker carried in every push (no per-piece network WANT); `release_system` (compaction) unmarks locally and idempotently notifies every provider.

---

## Part 7 — Database: CraftSQL

### 7.1 Architecture

A SQLite database decomposed into **16 KB content-addressed pages** behind a custom VFS. Pages are indexed by a **fanout-256 radix tree** (sparse `BTreeMap<u8, cid>` nodes, postcard); the tree's root plus `page_count`/`depth`/`wrapped_dek` form the **`RootIndex`**, whose CID *is* the DB root — the head that gets published. Commits (`xSync`) flush dirty pages and rewrite **only the dirty tree path** — unchanged subtrees keep their CIDs (structural dedup, O(changed·depth) index cost, inherent snapshot isolation). Journal is RAM-only and WAL is disabled: the root-CID swap *is* atomicity, so a hot journal never needs crash recovery.

**Identity & the single-writer rule.** A DB is `(owner NodeId, namespace)`; local key `<owner_hex[..16]>_<ns>`. Only the owner writes (`open`/`open_private`); anyone reads (`open_reader`). This is the consensus-eliminating primitive of the whole system: every "shared database" decomposes into per-identity DBs plus aggregating readers. Measured: ~11 commits/s per DB sequentially — scale is across many DBs, never within one.

### 7.2 Commit → publish (fire-and-forget)

`write(sql)`: execute (VFS commits → new root) → bump seq → **background-publish** the head via `RootStore` (the registry, RT_DBROOT) → sweep durability inline. There is deliberately **no synchronous CAS / `WRITE_CONFLICT`** (the retired Part F design): under single-writer there is no concurrent writer to conflict with, and a synchronous registry round-trip could stall a write for seconds behind writer rotation. Own-DB opens are **sidecar-first**: the local `.gens` sidecar is authoritative and the registry resolve is skipped (this fixed multi-second open stalls); readers always resolve via the registry.

### 7.3 Durability — generations

Each commit's diff (`reachable(new_root) − reachable(last_root)` = changed pages + rewritten index path) is packed into **one generation blob** and published as a CraftOBJ **system object** (erasure-coded k=8/n=32, distributed, health-scan-repaired) — one coding per commit, O(changed). The generation list is itself a durable object whose CID publishes via `ManifestStore` (RT_MANIFEST). At 16 generations (or manually), **compaction** folds history into one base snapshot, republishes a single-entry manifest, drops old generations in background spawns, and GCs local unreachable objects. **Recovery**: from `(owner, namespace)` alone, any node resolves the manifest → fetches each generation (erasure-reconstructed from any k pieces, every object CID-verified) → rebuilds the DB — a live node can resurrect a dead owner's database.

Live pages are held in a **separate plain local store** (not individually erasure-coded — a deliberate divergence from foundation §392: the durable copy is the generation set; per-page coding would multiply coding and record load).

### 7.4 Readers — lazy page fetch

A reader with a `PageSource` syncs only the root + index nodes at open, then pulls page contents **on demand** over `/craftec/sqlpage/1` (request = 32-byte CID, reply = object bytes, ≤16 MiB) directly from the owner; a point query fetches a strict subset of the DB. The sync VFS is bridged to async by a per-DB fetcher task; SQLite opens run on a blocking thread.

### 7.5 Private databases

A new private DB generates a DEK, wraps it under the owner's PRE public key, and stores the capsule in `RootIndex.wrapped_dek` (plaintext root); pages are encrypted with `seal_deterministic` (deterministic → identical pages still dedup). Foreign readers get ciphertext (SQLite errors); only the owner's key unwraps. **Known accepted gap:** index nodes + root are plaintext — structure (page count, tree shape) is exposed, content is not (drift vs ENCRYPTION §7). Sharing to other recipients (PRE re-encapsulation) is phase-5 design-only.

---

## Part 8 — Compute: one WASM runtime, program accounts

### 8.1 The unified runtime

`TransitionRuntime` is the node's **single** WASM runtime (wasmtime, fuel-metered `DEFAULT_FUEL=10M`, async). The two-runtime split (transition vs capability) was collapsed after reclassifying the determinism axis: **determinism is about clock/random, not I/O** — content-addressed SQL/OBJ reads are deterministic (same state + same query → same rows everywhere), so the deterministic profile includes them.

**Capabilities, enforced at link time** — a program importing a non-granted host fn fails to *instantiate*:

- `CapabilityGrant::deterministic()` = {Input, Caller, State, Commit, Crypto, Sql, Obj, **Clock**} — the native default; what consensus-critical programs get.
- `CapabilityGrant::full()` = deterministic + **WallClock** — userspace apps.
- `Random` is a reserved variant with **no bound host fn** (K2 open — randomness must be request-seeded to stay re-verifiable).

**Host surface** (module `craftcom`, unprefixed): `input, commit, state, caller, ed25519_verify, sql_execute, sql_query, obj_put, obj_get, clock, wall_clock`. `clock` returns `ctx.now` — the writer's HLC (consensus/block-time) for program-account transitions; for **app invocations** `ctx.now` is deliberately the invoking node's local time (a one-off app run has no agreed timestamp — same source as `wall_clock`). `wall_clock` is per-node real time, app-only. Backend-absent calls return `-1`, never panic. **Guest ABI**: export `memory`, entry `run() -> ()`, output = the bytes passed to `commit(ptr,len)`.

**The namespace gate is structural**: `sql_execute` carries no namespace argument — the ns comes from `ctx.app_ns` (`app.<name>`), so a program can only write its *own* `(identity, app.<ns>)` and read others' *same* app namespace. Cross-app escape is unexpressable, not just forbidden.

### 8.2 Invocation

`InvokeRequest{app_ns, wasm_cid, func, input}` over `/craftec/invoke/1`; the remote **caller identity is free** — the QUIC-authenticated peer NodeId is `ctx.caller` (no auth layer). WASM is loaded by CID from CraftOBJ (following a `File` manifest); apps run under `full()` with empty `prev_state` (app state lives in their `app.<ns>` database, not an account blob); the result is the committed bytes. Local invocation resolves the WASM by name through the head registry. Federated reads are proven end-to-end (two sovereign writers + a third node aggregating cross-node with no shared writer).

### 8.3 Program accounts — "the program is the writer"

`pda(program_cid, seed) = Cid::of("craftec/pda/v1"‖program_cid‖seed)` — an address with **no private key**; the program's deterministic execution is the write authority (no keyholder, no committee, no attestation; request-level authority, e.g. an owner signature, is validated *inside* the program). `ProgramAccountStore::advance(program_id, code_cid, seed, request)`: the account **address** derives from the stable `program_id` while the **executing WASM** is `code_cid` — governance can swap the code behind an account without moving it. State = a local blob file + background `publish_system` (fire-and-forget; a write never blocks on distribution). Empty committed output = rejection. `put_state` adopts already-validated bytes (epoch handoff); `clear` deletes local state (reshard GC).

**Native programs**: the `NativeProgram` trait (with `RegistryProgram` wrapping `RegistryState::apply`) exists as the designed shape for logic identical on every node — but note that today **nothing executes through it**: the live registry write path validates inline and writes SQL directly (Part 9.1), and the `SetProgram`→WASM swap path is unwired, deferred with the K1 dispatcher (Part 10.3).

---

## Part 9 — Coordination: the head registry

The registry is the system's coordination layer: a **durable, owner-signed map of heads** — program names (`RT_PROGRAM=0`), CraftSQL DB roots (`RT_DBROOT=1`), and durability manifests (`RT_MANIFEST=2`) — `(owner, name) → (cid, version)`. It exists to close the offline-owner gap: resolution works with the owner offline, without a DHT record, and without any committee.

### 9.1 Authority model (settled)

An **open, owner-signed CRDT**: partition-by-owner, last-writer-wins per `(owner, name)` with strictly-monotonic versions. The owner's Ed25519 signature is the *sole* write authority for their own keys. There is **no attestation, no committee, no quorum** — open registries converge by construction (a stale or forged entry is impossible without the owner's key; concurrent entries from one owner are ordered by version). Write validation is **native mechanism** (signature + 32-char name cap), *not* a governed-WASM program: validating an owner's own submission is a hard invariant, not swappable policy — the `app-registry` governed anchor was deliberately dropped.

### 9.2 Sharding & election

The keyspace splits into **`2^shard_bits` shards** — `shard_bits` is a **governed value** (`SetConfig{"shard_bits"}` on the governance chain; default 8 → 256, clamped [1,12]) so every node agrees on the count. Routing: `shard = low bits of BLAKE3(owner‖name)` — low-bit routing makes a split **local** (shard *s*'s keys go only to children *s* and *s|2^k*).

Per shard, election is **two-stage and computed identically on every node** from the converged census + HLC:
1. **Stable replica set** = the K=3 eligible nodes with the lowest `BLAKE3(rtype‖shard‖node_id)` — *no epoch term*, so the set shifts only on membership change; a fixed group holds each shard's state as warm followers.
2. **Writer** = `replicas[effective_epoch % K]` — the role rotates every 30 s epoch (2 s grace window at boundaries kills the clock-skew dual-writer race). Non-writers forward `Submit`/`Resolve`/`CurrentVersion` to the current writer over `/craftec/registry/1`; the key-routed wire requests **carry the submitter's `bits`** so a count change in flight can never split-route a key.

Election runs over the **converged census** (not the size-5 active view — which capped writers at ~6 and diverged per node; fixed and proven live at 19 nodes, eligible 6→19). On membership change, an event-driven **migration loop** (debounced ~30 s of census stability so join-storms never trigger scan storms) re-replicates held shards to their new replica sets — state follows the election; elastic membership is proven end-to-end.

### 9.3 Storage — per-shard CraftSQL

Each shard's state is a **CraftSQL database** (`heads(owner, name, cid, version, PK(owner,name))`, namespace `reg_<rtype>_<bits>_<shard>` — slash-free because the durability sidecar is a filesystem path). Register = version-guarded upsert; resolve = indexed SELECT; replication is **row-level** — a write pushes a 1-row state to the K replicas, not the whole shard (the main write-amplification win for the target topology: thousands of nodes, ~80% NAT readers, ~20% writer backbone). `GetState` (takeover merge, rare) ships the full shard as the wire DTO. Shard pages get the default erasure durability (`ObjDurable`) on top of K-replica push. The recursion (a registry that stores DB roots, whose shards are themselves DBs) is broken by a **blob-backed `ShardRootStore`**: shard-DB roots live in 40-byte program-account blobs, never routing back through the registry.

Enumeration (status/migrate/reshard/GC/dashboard) iterates a persistent **held-shards index** — **O(held), not O(2^bits)** — and reads never create a shard DB (a read that opened-to-create would publish an empty root and snowball the index toward all shards).

### 9.4 Online resharding

Changing `shard_bits` on a **live cluster** with no wipe: governance propagates the new value; each node's `reshard_round` detects its persisted generation ≠ governed value, **sweeps** every held old-generation shard's rows into the new generation's DBs (idempotent LWW re-bucketing; a parent's keys land only in its two children), then **drains** the old generation for ~60 s (re-sweeping to catch writes from stragglers still on the old count) and finally **GCs** it (drops the old shard DBs and their root pointers — old generations do not accumulate). Reads fall through to the adjacent generation (`bits±1`) during the window, so nothing is unresolvable mid-reshard. **Proven live in both directions**: grow 8→9 (512 shards; keys re-bucketed, resolvable from all nodes) and shrink 9→8 (a key born at bits=9 physically merged into its gen-8 parent).

### 9.5 Fault tolerance & readiness

Resolution is tolerant of a briefly-unreachable writer: try own copy (if replica) → the current writer → each other replica, every remote call 8 s-bounded. Non-replica reads are **cached** (3 s TTL) with read-your-writes invalidation. A **takeover merge** runs once per epoch when a node becomes a shard's writer (fetch + LWW-merge the other replicas' state). A **readiness latch** (mirroring the health scan's settle gate) flips once the node's census has been stable 10 s (≤90 s grace) after (re)start; register/resolve/current_version wait on it **up to 20 s and then proceed best-effort** — bounded, not absolute, but it eliminated the post-restart "not found" transient in practice (a restarted node waited 16 s then answered correctly where it previously insta-missed). Steady-state ops never wait. Offline-owner resolution is validated live: a program deployed by node A resolves from node B with A down.

### 9.6 Public stats

A token-free, CORS-open `GET /stats` (`--public-stats-port`, 0.0.0.0) feeds the website's live-network section: `nodes` from the converged census (a real network-wide count), provider records from the DHT store, content/storage from the node's local view. It replaces the retired tracker's stats port (which served zeros once nodes stopped announcing to it).

---

## Part 10 — Governance & the minimal-kernel principle

### 10.1 The governance chain

Protocol-level change is a **k-of-n governor multisig** over a **self-verifying, totally-ordered chain**. `GovAction ∈ {SetProgram, SetConfig, AddGovernor, RemoveGovernor, SetThreshold}`; a `GovernanceProposal{action, seq}` (domain `craftec/gov/1`) must target `current seq + 1` (replay-proof); an approval carries ≥threshold distinct governor signatures. **Chain-fold verification**: every node folds `apply` from genesis — a chain carrying *one* invalid approval verifies to nothing and is wholly rejected; adoption is **longest valid chain sharing my genesis**. Genesis defaults to 1-of-1 with the local key (a fresh node self-starts); multi-governor sets come from config and must match across nodes.

**Derived registries are pure replays of the chain** (no separate state, nothing to gossip): `program_registry()` folds `SetProgram` (empty at genesis — deliberately **no** `app-registry` seed, see §10.3) and `config_registry()` folds `SetConfig`. The config registry is **consumed**: `shard_bits` (Part 9) is a governed value, clamped at the consumer ([1,12], default 8) so a hostile value can't brick the node.

**Publication & propagation**: the chain publishes as durable content under the reserved name `"\u{1}governance-chain"` (control char — can never collide with a user app), announced at version **`seq + 1`** — strictly increasing, because the DHT store rejects equal seqs; the earlier `seq.max(1)` floored both seq 0 and seq 1 to the same version, so the *first* governance change never propagated (found and fixed live). An anti-entropy tick (5 s) pulls peers' chains and adopts longer valid ones — pulling over the **census ∪ governors** (not the size-5 active view, which could strand propagation; governors added explicitly since they source every change and can transiently age out of the census).

**Governance ≠ verification**: governance converts *subjective judgment* into signatures once (small, stable, accountable, human); everything downstream is deterministic replay.

### 10.2 The minimal-kernel principle

The governing rule for all new work: **mechanism in the binary, policy in governed WASM behind stable anchors.** Decision test: *"would we ever change this without shipping a new binary?"* — yes → policy behind an anchor (with a native default; the network must never brick on a missing/failed program); no → kernel mechanism. First realization: the program-account substrate (`pda` accounts, single writer, run-persist-publish) with the head registry as consumer #1.

### 10.3 The 07-09 recalibration: hard invariants are mechanism

Registry write validation (owner signature, monotonic version, 32-char name cap) went **native**: these are hard invariants, not swappable policy, and a governed-WASM hook on the hot write path to enforce an invariant is kernel bloat, not minimal-kernel alignment. Consequences: the `app-registry` genesis anchor seed was dropped (the dashboard's protocol-programs list correctly reads "none" — the network runs no governed WASM program yet), and the K1 "general anchor dispatcher" is deferred until a genuinely governed-WASM protocol program exists. Governed WASM remains reserved for genuine swappable policy: program cids, the governor set, config values like `shard_bits`.

### 10.4 Verification (designed, NOT built)

`VERIFICATION_DESIGN` specifies the future consistency layer: **independent cross-node re-execution** confirming a transition's output is valid per its program — consistency *only* (never durability, authority, or arbitration). It applies solely to **consistency-critical shared state** (counters, quotas, balances); **open registries never use it** — they converge by construction, which is precisely what justified deleting the old k-of-n committee subsystem outright (−2,519 lines) rather than keeping it dormant. The design: apps declare `{quorum k, set: open|whitelist}`; an open, append-only request board; **cooldown-rotated verifiers** (the cooldown simultaneously spreads load, forces verdict diversity, and disrupts collusion); `verify` = attestation with k=1; Sybil is named honestly as the ceiling. What survives from the removed attestation work: the PDA concept, the deterministic transition runtime, the request/serve wire shape.

---

## Part 11 — Encryption & deletion

### 11.1 Primitives (`zeph-cipher`)

Bulk AEAD = **XChaCha20-Poly1305** with a random per-object 32-byte DEK (zeroized on drop); `seal_deterministic` (keyed-BLAKE3 nonce over the plaintext) gives same-input→same-ciphertext for CraftSQL pages so content-addressed dedup survives encryption (equality leak accepted for sole-owner DBs). Key wrap = **Umbral PRE** capsules from day one — self-access is plain `decrypt_original`, and future sharing (kfrags) becomes purely additive rather than a migration. The PRE keypair derives deterministically from the Ed25519 identity seed (domain-separated), re-derived on boot, never stored.

### 11.2 Private files & databases (built, phases 1–4)

- **Private file** = two published objects: the ciphertext (rides the normal lifecycle — the network stores, codes, verifies, and repairs *ciphertext only*; `CID = BLAKE3(ciphertext)`) and an `EncryptedEnvelope{capsule, ciphertext_cid, owner, recipients:[]}` (magic `ZENVELP1`). Name/mime hide inside the ciphertext. Lifecycle cascades treat envelope→ciphertext as parent→child. No metadata record is announced for private objects.
- **Private DB**: per-DB DEK wrapped into `RootIndex.wrapped_dek`; pages sealed deterministically; readers holding the key decrypt transparently (cross-node lazy fetch pulls ciphertext, decrypts locally); foreign readers fail at the SQL layer. The node's own drive/app indexes are private by default. Known accepted gap: index nodes + root are plaintext (structure, not content).
- Proven by gate tests: sole-owner decryption on a 6-node net; owner reads back byte-identical; a second identity holding the same objects cannot decrypt.

### 11.3 Deletion — honest semantics

- **`delete`** = soft: forget the whole chain locally (content, pieces, generation — no tombstone) + withdraw provider records; re-publishable, re-fetchable. **`ban`** = tombstone: refuse-to-host + block resurrection via repair/ingest (moderation). System objects are guarded from both.
- **Network-wide delete propagation was REJECTED** (DELETE_PROPAGATION's authored KIND_DELETE/provenance machinery — it buys speed, not a stronger guarantee): public deletion = stop re-announcing → records decay at the 48 h TTL → fade + churn + eviction reclaim. Deliberately censorship-resistant: you cannot unpublish what others want.
- **Crypto-shred, honestly tiered**: what ships is **Tier 2 best-effort** — deleting a private file drops your capsule copy and lets the unwanted envelope+ciphertext fade. A *guaranteed* shred of a published object is impossible as originally claimed (capsule copies sit on uncontrolled holders; the PRE key re-derives from the identity seed forever). Tier 1 (local-only key) is permanently rejected as a durability trap. **Tier 0** (DEK shares held by k-of-n agents; shred = share deletion — a trust guarantee, not a proof) is the only real mechanism and is deferred with the verification layer (K4/K7).

---

## Part 12 — Deferred work: the bounded backlog

The minimal-kernel bet is largely won: most future "features" are policy over existing primitives. What remains is a bounded set of kernel primitives (K-numbers from STATE_AND_ROADMAP §6) plus per-domain deferrals:

**Kernel primitives still open:**

| # | Primitive | Status |
|---|---|---|
| K1 | Anchor dispatcher (+config registry) | Config half **DONE** (drives `shard_bits`); dispatcher deferred until a genuinely governed-WASM program exists |
| K2 | `random` host fn | Reserved variant, unbound; blocked on request-seeded reproducibility |
| K3 | PRE re-encryption ops (sharing) | Umbral in-tree; kfrag wire shape reserved (`recipients[]`, `KIND_GRANT=8`); phase-5 design-only |
| K4 | Threshold secret-sharing (Tier-0 shred) | Design-only; the only real shred guarantee |
| K5 | PDP challenge/response (+ receipts, reputation) | Not built; also the defense-in-depth vs adaptive vtag forgery |
| K6/K7 | Cross-node re-execution + attestation gather | The verification layer (Track B); designed, not built |
| K8 | AvailabilityProbe issuance | Half-built by choice — answered, never sent |
| K9 | Dynamic sharding | **DONE + proven live** (governed count, online split/merge, drain/GC) |
| K10 | SWIM Suspect/Dead dissemination | Census built; epidemic death-detection not (deaths TTL-age) |

**Per-domain deferrals (all recorded in their sections above):** K=32 file segmentation / 8 MiB segments (the 3×-floor arithmetic awaits it; publish is whole-object k=8 today) · SIMD GF(2⁸) · true `Ephemeral` consume mode · admission quotas / reciprocity ledger · signed network-wide delete propagation (rejected, not deferred) · k-bucket ping-evict · SQL page-cache layers, batch writer, offline-page K-provider fetch, `SIGNED_WRITE`/delegation · the `releasing` churn-cleanup loop · auto-recovery/compaction triggers · WASM memory/recursion limits (fuel is the only bound) · per-program grant declaration (hardcoded call-site policy today) · SQL determinism enforcement (banned-builtin list design-only) · app versioning + app-store-as-catalog-app (kept out of the node) · governance-tick digest sync at 1000s of nodes · registry head-publish coalescing · relay QAD behind the proxy · CraftStudio / product apps on the node stack · paid tier (PDP + receipts) · the live network map (MU.4).

---

## Part 13 — Known constraints & scaling posture

- **Per-DB single-writer cap** (~11 commits/s sequential here): by design — scale is across many DBs, never within one. Every "shared table" decomposes into per-identity DBs + aggregating readers.
- **Registry control plane**: the ~6-writer election ceiling is **resolved** (census election + dynamic sharding + SQL backing + O(held) loops, all proven live). Remaining: SWIM death-detection (K10), census/governance gossip is O(N) per round (digest sync at very large N), head-publish coalescing at very high write rates.
- **Erasure floor is 4× today** (k=8/n=32 whole-object), not the 3× quoted for K=32 segmentation — the positioning math is a design target until segmentation lands.
- **Content durability below 8 nodes**: a small object on a small cluster can sit below the 8-distinct-peer erasure spread (the publisher's retained copy covers it; a real network ≥8 nodes replicates durably).
- **Vtag ceiling**: 2⁻⁶⁴ vs *non-adaptive* corruption; the adaptive-attacker answer (pairing-based tags via the scheme byte, plus PDP cross-checks) is deferred.
- **Throughput is unmeasured in aggregate** — figures are sequential single-DB latencies, not scaling curves; a heads-at-scale stress run is the natural next measurement.
- **Topology reality**: ~80 % of a volunteer network sits behind NAT; the design leans on hole-punching + relays, row-level replication (writer-backbone bandwidth is the scarce resource), and voluntary census visibility.

---

## Part 14 — Doc reconciliation map (what this document supersedes, per source)

Read older docs only through this table. "Stale" = do not trust those sections; the body above is current.

| Document | Still good for | Stale / superseded |
|---|---|---|
| `STATE_AND_ROADMAP.md` | The working ledger; §0 + inline UPDATEs authoritative | Its §5 body pre-update text (ceiling resolved); §3.4 rows already fixed in code (obj docstring, CRAFTCOM prefixes, ENCRYPTION §8 — since rewritten) |
| `craftec_technical_foundation.md` (**root copy only**) | Core concepts: identity, CIDs, HLC, RLNC rationale, lifecycle philosophy, §62 amendments | §62-A1/A2 + §7/§41/§56 (attestation quorum — never built); Part F §408/§411 (sync CAS/`WRITE_CONFLICT`); §34/§37/§397 (Pkarr, DHT root publication); §23 message table; §10 ALPN table; §3 full-SWIM; §62.1 probe-counting + 1 h epochs; §5 "pages erasure-coded per write"; §2/§48 program-scheduler/whitelist. `zephcraft/docs/` copy is an outdated snapshot — retire it |
| `REGISTRY_DESIGN.md` | §0/§2.1 + the 07-09 banner | Entire attested/committee body below the banners; "256 shards"; "postcard blob" state; `programreg.rs` naming |
| `SQL_REGISTRY_DESIGN.md` | Current (07-09 amendments included) | §3/§6-P4 pre-amendment text (see its header banner) |
| `CRAFTOBJ_DESIGN.md` v2.0 | Concepts: piece/holder model, provider granularity, lifecycle behaviors | Tracker-era routing; §Content Model segmentation/`vtags_cid`; §HealthScan (5-min sweeps, probes, PDP, rank); §Repair top-N/1 h; §Wire type-tags; §Admission/watermarks; "blocks until ≥K acks" |
| `COMPUTE_EXECUTION_DESIGN.md` | Current — phases 0–4 all built | §1's two-runtime framing is historical; §4/§5 "plus random" overstates `full()` |
| `CRAFTCOM_DESIGN.md` | App model, `app.<ns>`, invoke flow | Status banner ("BUILDING" — it's built); §6 memory/recursion limits (not implemented); §3 "SWIM" |
| `GOVERNANCE_DESIGN.md` | Reconciled (07-08 banner); chain-fold + open CRDT | §3 "registries independently verify" (they're pure replays) |
| `MINIMAL_KERNEL_DESIGN.md` | The principle, anchors, safety rails | §2's "char-limit ✓ as governed policy" (validation went native); §0.1 "no bespoke registry code in kernel" (headreg is substantial native code) |
| `VERIFICATION_DESIGN.md` | The design (unbuilt) | Header's "current com attestation" framing (committee already deleted) |
| `ATTESTATION_DESIGN.md` | PDA concept, determinism boundary (behind its banner) | Whole committee/attested-authority body |
| `ENCRYPTION_DESIGN.md` | Phases 1–4 (built), phase 5 design | Status banner; §6 `alg` field; §7 encrypted index nodes; §10 envelope-under-signed-record |
| `CRYPTO_SHRED_DESIGN.md` | The honest tier analysis | §7's `zeph shred` command naming (shipped as `delete`, forget-not-tombstone) |
| `DELETE_PROPAGATION_DESIGN.md` | The decision (rejected) + fade rationale | §3–§7 machinery (KIND_DELETE, provenance) — rejected, not pending; "guaranteed shred" claims |
| `VISION.md` | "Dumb infrastructure" thesis, single-writer decomposition, kernel/agent split, everything-is-CIDs | Its three [SUPERSEDED] sections; the *unmarked* "financial network first" GTM (storage-first won); "Ed25519/Pkarr" in the supersession banner itself |
| `ZEPH_POSITIONING.md` | The landing thesis, contrasts, guardrails | "n=96/K=32 3× floor" (as-built 4× at k=8); "self-sizing NOT yet proven" (now proven — stale in the other direction) |
| `ZEPHCRAFT_NETWORK.md` (**root copy**) | Layer stack, three flows, open-network ladder | "durable ONLY at ≥K acks"; probe-based repair; "Live today" table (wholly outdated); membership "millions" (K10 open). `zephcraft/docs/` copy is pre-tracker-retirement — retire it |
| Root `CLAUDE.md` | Build discipline, layering | Ecosystem table rows (CraftSQL "designed" — built; CraftCOM "k-of-n attestation" — retired); "+ Pkarr" in the stack |
| `CRAFTEC_ECOSYSTEM.md` | Nothing (archived generation, in full) | Everything, incl. its CraftSEC meaning |

---

## Part 15 — Legacy appendix (the archived generation)

Six documents describe the **libp2p generation archived Feb 2026** (`craft-ec/*-archive`) and have **no code presence** in zephcraft (verified by sweep): `SETTLEMENT_DESIGN` + `SETTLEMENT_PROGRAM_DESIGN` (shared Solana Anchor settlement: pools, channels, Merkle payouts), `CRAFTNET_DESIGN` (P2P VPN: SOCKS5, onion routing, ForwardReceipts), `CRAFTSTUDIO_DESIGN` (Tauri client + JSON-RPC daemon), `PROVER_DESIGN` (SP1 zkVM receipt compression — "ZK for compression, not security"), `AGGREGATOR_DESIGN` (receipt aggregation; its aspirational WASM-agent-over-SQL shape loosely prefigures today's compute model). **Five of the six carry no status banner** — without this note they read as live specs. Treat all six as design history. The Solana stack survives only in the independent product apps (flowcraft, handcraft, cloakcraft), which do not use these designs. No settlement/economics layer exists or is currently designed for zephcraft.

---

*End of consolidated document. Maintenance rule: when code and this document disagree, fix this document in the same change that lands the code — it replaces the per-doc drift that motivated it.*
