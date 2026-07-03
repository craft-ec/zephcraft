# Craftec Technical Foundation

**Complete Architecture & Integration Specification**

Version 3.7 — July 2026 (v3.6 baseline + Amendments, see §62)
Author: Perplexity Computer (v3.6); Amendments: Craftec design review 2026-07-02
Classification: Internal / Technical

> **v3.7 note:** This document is the single source of truth for the Craftec node. Where §62 (Amendments) conflicts with the v3.6 body text, §62 wins. The old `CRAFTOBJ_DESIGN.md` v1 and any libp2p-era references elsewhere are superseded.

---

## Table of Contents

### [PART A: ARCHITECTURE & DESIGN](#part-a-architecture--design)
- [1. Executive Summary](#1-executive-summary)
- [2. Architecture Overview](#2-architecture-overview)
- [3. P2P Networking](#3-p2p-networking)
- [4. Erasure Coding & Storage Redundancy](#4-erasure-coding--storage-redundancy)
- [5. Distributed Database (CraftSQL)](#5-distributed-database-craftsql)
- [6. CraftOBJ: Content-Addressed Storage Layer](#6-craftobj-content-addressed-storage-layer)
- [7. Cryptography & Attestation](#7-cryptography--attestation)
- [8. Language & Runtime Stack](#8-language--runtime-stack)
- [9. Client Frameworks (CraftStudio)](#9-client-frameworks-craftstudio)
- [10. Application-Layer Protocol Composition](#10-application-layer-protocol-composition)

### [PART B: NODE LIFECYCLE](#part-b-node-lifecycle)
- [11. First-Run Initialization](#11-first-run-initialization)
- [12. Startup Sequence](#12-startup-sequence)
- [13. Configuration Management](#13-configuration-management)
- [14. Shutdown Sequence](#14-shutdown-sequence)
- [15. Crash Recovery](#15-crash-recovery)
- [16. Upgrade & Migration](#16-upgrade--migration)

### [PART C: IDENTITY & TRUST](#part-c-identity--trust)
- [17. Node Identity](#17-node-identity)
- [18. Bootstrap & Discovery](#18-bootstrap--discovery)
- [19. Peer Reputation & Trust](#19-peer-reputation--trust)
- [20. Network Admission](#20-network-admission)

### [PART D: NETWORKING](#part-d-networking)
- [21. Connection Lifecycle](#21-connection-lifecycle)
- [22. Protocol Negotiation](#22-protocol-negotiation)
- [23. Wire Protocol](#23-wire-protocol)
- [24. Backpressure & Flow Control](#24-backpressure--flow-control)
- [25. Connection Pool Management](#25-connection-pool-management)
- [26. NAT Traversal](#26-nat-traversal)

### [PART E: STORAGE — CraftOBJ](#part-e-storage--craftobj)
- [27. Content-Addressed Store](#27-content-addressed-store)
- [28. RLNC Erasure Coding](#28-rlnc-erasure-coding)
- [29. Piece Distribution](#29-piece-distribution)
- [30. Health Scanning & Repair](#30-health-scanning--repair)
- [31. Local Eviction Policy](#31-local-eviction-policy)
- [32. Disk Space Management](#32-disk-space-management)

### [PART F: DATABASE — CraftSQL](#part-f-database--craftsql)
- [33. CID-VFS Implementation](#33-cid-vfs-implementation)
- [34. Commit Flow](#34-commit-flow)
- [35. WAL Elimination / MVCC](#35-wal-elimination--mvcc)
- [36. Page Cache](#36-page-cache)
- [37. Root CID Publication](#37-root-cid-publication)

### [PART G: AGENT RUNTIME — CraftCOM](#part-g-agent-runtime--craftcom)
- [38. Distributed Compute Engine](#38-distributed-compute-engine)
- [39. Agent Lifecycle](#39-agent-lifecycle)
- [40. Host Functions](#40-host-functions)
- [41. Attestation Flow](#41-attestation-flow)

### [PART H: CROSS-CUTTING CONCERNS](#part-h-cross-cutting-concerns)
- [42. Clock & Time (HLC)](#42-clock--time-hlc)
- [43. Observability & Metrics](#43-observability--metrics)
- [44. Resource Management](#44-resource-management)
- [45. Security Hardening](#45-security-hardening)
- [46. Testing Strategy](#46-testing-strategy)
- [47. Error Classification & Handling](#47-error-classification--handling)

### [PART I: COORDINATION & SCHEDULING](#part-i-coordination--scheduling)
- [48. Task Scheduler & Program Lifecycle](#48-task-scheduler--program-lifecycle)
- [49. Request Coalescing / Singleflight](#49-request-coalescing--singleflight)
- [50. Batch Writer](#50-batch-writer)
- [51. Background Job Coordinator](#51-background-job-coordinator)
- [52. Event Bus](#52-event-bus)

### [PART J: END-TO-END DATA FLOWS](#part-j-end-to-end-data-flows)
- [53. Write Path](#53-write-path)
- [54. Read Path](#54-read-path)
- [55. Repair Path](#55-repair-path)
- [56. Attestation Path](#56-attestation-path)
- [57. Join Path](#57-join-path)

### [APPENDICES](#appendices)
- [58. Recommended Technology Stack](#58-recommended-technology-stack)
- [59. Implementation Roadmap](#59-implementation-roadmap)
- [60. Key Risks & Mitigations](#60-key-risks--mitigations)
- [61. Sources](#61-sources)
- [62. Amendments — v3.7 Decision Record (2026-07-02)](#62-amendments--v37-decision-record-2026-07-02)

### [Changelog: v3.5 → v3.6](#changelog-v35--v36)
### [Changelog: v3.4 → v3.5](#changelog-v34--v35)
### [Changelog: v3.3 → v3.4](#changelog-v33--v34)

---

## PART A: ARCHITECTURE & DESIGN

### 1. Executive Summary

Craftec is building the P2P equivalent of what Google, Microsoft, and AWS built in their data centers — distributed storage, distributed compute, distributed database — but on a peer-to-peer network where nobody owns the infrastructure. It eliminates centralized servers by distributing data across a global peer-to-peer network, using erasure coding for redundancy, content-addressed storage for integrity, and a distributed SQL database for structured metadata. This document is the single source of truth for the Craftec project — consolidating the original technical foundation with all validated design work and the complete integration specification.

#### Hyperscaler Comparison

| Hyperscaler | Craftec | Function |
|-------------|---------|----------|
| GFS / S3 / Azure Blob | CraftOBJ | Distributed storage |
| BigQuery / Spanner / CosmosDB | CraftSQL | Distributed database |
| Lambda / Cloud Functions / GCE | CraftCOM | Distributed compute (CPU/GPU) |
| VPC / Cloud Networking | CraftNet | P2P networking / VPN |

#### Global Distributed Runtime, Not a Blockchain

Craftec achieves the trust and coordination properties of a blockchain — distributed consensus, cryptographic identity, immutable content addressing, zero central authority — but with a fundamentally different runtime model. Blockchains are a single logical runtime replicated everywhere: every node executes the same thing, stores the same state, reaches the same conclusion. Craftec is a global distributed runtime where different nodes perform different roles — storage, compute, routing, serving — coordinated by the protocol, not by global consensus.

| Dimension | Blockchain | Craftec |
|-----------|-----------|---------|
| Runtime model | Single replicated VM — every node executes the same thing | Global distributed runtime — different nodes do different work |
| Data model | Full replication (every node stores everything) | Distributed erasure coding (no node has everything) |
| Storage cost | O(N) — linear with replication | Sub-linear — n = k × ceil(2.0 + 16/k) coded pieces (3× for K=32) |
| Database writes | Global consensus on every transaction | Single-writer-per-identity — no consensus needed |
| Runtime | Single deterministic VM, all nodes execute same thing | Distributed compute agents — different nodes run different workloads |
| Throughput | Bottlenecked by slowest validator | Parallel — storage, compute, and queries are independent |
| Coordination | Global consensus on every transaction | Local-first + attestation quorum only when needed |

The system is composed of four principal layers: the P2P Networking layer (iroh, QUIC, SWIM), the CraftOBJ content-addressed object store (BLAKE3, RLNC erasure coding, DHT routing), the CraftSQL distributed database (SQLite-compatible, single-writer-per-identity), and the CraftStudio client framework (cross-platform, local-first applications). A CraftCOM general-purpose compute layer provides distributed agent execution (CPU/GPU) across the network — attestation (k-of-n consensus) is one of many possible workloads, achieving blockchain-like coordination without blockchain overhead. Craftec is not a blockchain — it is a global distributed runtime where different nodes perform different roles (storage, compute, routing), rather than every node replicating the same state.

Beyond persistent storage, the Craftec platform supports application-layer protocol composition: multiple services share a single iroh Endpoint via ALPN negotiation. CraftOBJ provides persistent distributed storage, while iroh's ecosystem protocols (iroh-roq for video streaming, iroh-gossip + iroh-blobs for ephemeral coordination) enable real-time applications without reinventing transport. CraftNet, a validated design direction for decentralized VPN, is migrating from libp2p to the iroh stack to share the same P2P layer.

#### Key Decisions

| Area | Decision | Rationale |
|------|----------|-----------|
| Transport | iroh (Rust, QUIC-based) | NAT traversal, relay fallback, connection multiplexing |
| Membership | SWIM protocol | O(log N) failure detection, protocol piggybacking |
| Identity | Pkarr + Ed25519 | Self-sovereign identity, DNS-compatible publishing |
| Serialization | postcard (serde) | Compact binary, zero-copy deserialization, no-std |
| Erasure Coding | RLNC over GF(2⁸) | Rateless repair, progressive decoding, bandwidth efficiency |
| Database | CraftSQL (SQLite-derived) | SQL compatibility, single-writer-per-identity, local-first |
| Attestation | CraftCOM (WASM agents, CPU/GPU) | General-purpose distributed compute; attestation is one use case |
| Content Hashing | BLAKE3 | All CIDs, piece IDs, Merkle trees — fast, parallel, 32-byte output |
| Content Routing | iroh Kademlia DHT | Provider records for CID→node mapping |
| Protocol Composition | iroh Endpoint + ALPN | Multiple services on one P2P layer |

The architecture is designed around four principles: content integrity (every piece of data is cryptographically verifiable), redundancy by default (erasure coding ensures survival of data despite node churn), local-first operation (clients operate independently, syncing when connectivity permits), and zero central dependencies (no single point of failure in the network).

The node architecture follows a kernel/program split analogous to operating systems: kernel-level subsystems (HealthScan, RLNC engine, piece distribution, DHT provider records, SWIM membership, CID-VFS, program scheduler) are compiled into the node binary for reliability, while network-owned programs (local eviction policy, reputation scoring, load balancing) run as upgradeable WASM agents managed by the program scheduler — upgradeable without binary changes.

#### Node Roles

A database is not hosted on any specific node — it is CID-addressed 16 KB pages scattered across the network via erasure coding. All nodes provide storage (and relay if publicly reachable). A single binary supports all roles.

| Role | Responsibility | RLNC Decode? |
|------|---------------|--------------|
| Storage node | Receives coded pieces, stores locally, serves on request. Never decodes, never reconstructs pages, never sees database content. | No |
| Client node | End-user device. Fetches pieces, decodes (RLNC), reconstructs pages, runs CraftSQL queries locally. | Yes (client-side) |
| RPC node | Accepts signed SIGNED_WRITE instructions from remote identities. Reconstructs affected pages (fetch + decode), executes SQL mutation, produces new pages, encodes, distributes. Heavier resource profile — needs page cache and concurrent reconstruction limits. | Yes (for execution) |

---

### 2. Architecture Overview

Craftec employs a layered architecture where each layer has a single responsibility and well-defined interfaces to adjacent layers. This separation ensures that changes in one layer do not cascade into others.

#### Layer Architecture

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| 5 – Application | CraftStudio | Client frameworks, UI rendering, application logic |
| 4 – Compute | CraftCOM | Distributed WASM agents, general compute (CPU/GPU), attestation |
| 3 – Database | CraftSQL | Distributed SQL, single-writer-per-identity, schema management |
| 2 – Object Store | CraftOBJ | Content-addressed storage, erasure coding, DHT routing, PDP |
| 1 – Network | iroh + SWIM | Peer connections, membership, failure detection, NAT traversal |
| 0 – Identity | Pkarr + Ed25519 | Cryptographic identity, key management, DNS publishing |

Data flows upward: the identity layer provides keys, the network layer establishes connections, CraftOBJ stores and retrieves content-addressed objects, CraftSQL adds structured queries on top, and CraftStudio presents the experience to end users.

#### Settled Design Decisions

The following design decisions have been validated through analysis and are considered settled.

- **RLNC over GF(2⁸):** Rateless repair, progressive decoding, bandwidth-optimal reconstruction. SIMD vectorization via single-byte GF(2⁸) arithmetic.
- **BLAKE3 for all content hashing:** 32-byte output, parallelizable Merkle tree internals, 2–4x faster than SHA-256.
- **postcard for wire serialization:** Compact binary, serde integration, zero-copy, deterministic encoding.
- **iroh for transport:** QUIC connections with NAT traversal, relay fallback. NodeId = Ed25519 public key.
- **SWIM for cluster membership:** O(log N) failure detection with protocol piggybacking.
- **Content routing via DHT:** Kademlia DHT at CraftOBJ layer for CID→node resolution. Only scalable mechanism.
- **Protocol-driven storage lifecycle:** No pin/unpin. Five distribution functions maintain redundancy autonomously.

#### Kernel vs. Network-Owned Programs

Craftec node architecture follows the principle from operating systems: kernel provides mechanism, programs provide policy. The Linux kernel does not encode scheduling policy — it provides the scheduler. Similarly, the Craftec node binary provides reliable mechanisms, while network-owned programs define upgradeable policy — without requiring any binary update.

**Kernel-Level Components** — compiled into the node binary. If any fails, the whole system breaks. These cannot be WASM because WASM depends on them to run:

| Component | Role | Why Kernel-Level |
|-----------|------|-----------------|
| HealthScan | CID health scanning and repair coordination | The HEART of the system. Without CID health, every piece rots and the entire ecosystem breaks. No WASM program can substitute for this foundation. |
| RLNC Engine | Erasure encode, decode, and recode over GF(2⁸) | All storage durability depends on this. Must be available before any WASM loads. Cannot tolerate WASM sandbox overhead on the hot path. |
| Piece Distribution | Moving coded pieces to the right nodes | Fundamental to the storage lifecycle. Failure stops new data from becoming durable. |
| DHT Provider Records | Content routing: CID → provider node mapping | The only scalable content discovery mechanism. Every read and repair depends on it. |
| SWIM Membership | Peer liveness detection and failure marking | All connection and repair logic depends on knowing which peers are alive. Cannot be delegated. |
| CID-VFS I/O | SQLite virtual filesystem backed by CraftOBJ | The storage substrate for CraftSQL. CraftSQL pages cannot be read without it. |
| Program Scheduler | Load WASM by CID, run, monitor, restart on failure, enforce resource limits | Like the Linux kernel scheduler. Minimal — does not make policy, just keeps network-owned programs alive. Network-owned programs cannot run without it. |

**Network-Owned Programs** — policy, not mechanism. Run as whitelisted WASM agents managed by the Program Scheduler. Upgradeable by publishing a new WASM CID to the whitelist; no node binary change required:

| Program | Policy Responsibility | Upgrade Path |
|---------|----------------------|--------------|
| Agent Load Balancer | Which node runs which compute task; workload placement heuristics | Publish new WASM CID to whitelist |
| Reputation Scorer | How to evaluate and score peer behavior; thresholds for trust | Publish new WASM CID to whitelist |
| Eviction Policy Agent | Which local CIDs to evict first; priority ordering; safety windows | Publish new WASM CID to whitelist |
| Degradation Policy Agent | When to shed excess pieces; criteria for which pieces to drop | Publish new WASM CID to whitelist |
| Schema Migration Coordinator | CraftSQL schema migration sequencing across replicas | Publish new WASM CID to whitelist |
| Future Maintenance Routines | Any future network-wide coordination logic | Publish new WASM CID to whitelist |

The analogy is exact: the Linux kernel provides the sched subsystem and syscall interface. Userspace utilities (cron, systemd services, daemons) provide upgradeable policy. Replacing a cron job does not require recompiling the kernel. Replacing the eviction policy agent does not require a node binary upgrade — it requires publishing a new WASM CID.

> **Key principle:** A network-owned program upgrade is a governance action (updating the canonical WASM CID), not an engineering action (redeploying node software). This is the separation that gives the network long-term adaptability without sacrificing the reliability of its core mechanisms.

---

### 3. P2P Networking

The networking layer provides the substrate for all communication in Craftec. It handles peer discovery, connection establishment, NAT traversal, failure detection, and cluster membership.

#### iroh Transport & Connections

iroh is an open-source Rust library that provides encrypted, authenticated peer-to-peer connections. Each node is identified by its NodeId — a 32-byte Ed25519 public key. iroh manages NAT traversal via STUN/TURN-like relay servers, hole punching for direct connections, and seamless fallback to relay when direct paths fail.

Key properties of iroh connections:

- **Authenticated:** Every connection is mutually authenticated via the NodeId (Ed25519 key pair)
- **Encrypted:** All traffic uses QUIC TLS 1.3 encryption — no plaintext on the wire
- **Multiplexed:** Multiple logical streams over a single QUIC connection (no head-of-line blocking)
- **NAT-friendly:** Automatic hole punching with relay fallback for symmetric NATs
- **Connection deduplication:** iroh maintains at most one connection per peer

iroh connections are established via the Endpoint API. Protocols register with ALPN identifiers, enabling multiple application protocols to share a single iroh endpoint. Craftec registers separate ALPNs for CraftOBJ transfer, CraftSQL sync, and SWIM membership.

#### QUIC Transport

QUIC (RFC 9000) is the underlying transport for all iroh connections. It provides:

- **0-RTT connection establishment:** Returning connections can send data immediately
- **Stream multiplexing:** Independent bidirectional streams without head-of-line blocking
- **Built-in congestion control:** BBR or Cubic, per-stream flow control
- **Connection migration:** Connections survive IP address changes (mobile/WiFi transitions)
- **TLS 1.3 integrated:** Encryption is mandatory, not optional

iroh uses the quinn Rust crate as its QUIC backend. The choice of QUIC over TCP is fundamental: TCP head-of-line blocking is catastrophic for multiplexed protocols like piece transfer.

#### Cluster Membership: SWIM

SWIM (Scalable Weakly-consistent Infection-style Membership) provides decentralized cluster membership and failure detection. It has two integrated components: Failure Detection and Dissemination.

**Failure Detection**

Each round, a node probes the next peer from a stable shuffled ring (every alive member probed exactly once per full pass, bounding worst-case detection latency), expecting an ACK.

- **Direct probe:** PING → expect ACK within timeout.
- **Indirect probe:** No ACK? Ask K=3 random nodes to send PING-REQ to the suspect (indirect probe). This distinguishes 'dead' from 'I can't reach it'.
- **Suspected:** Still no ACK from indirect probes? Mark as 'suspected'. Grace period before declaring dead.
- **Confirmed dead:** After suspicion timeout (10 rounds = 5s), mark 'confirmed dead.'

**Dissemination**

Membership *state changes* (join/alive/suspect/dead) enter a gossip buffer and are piggybacked on PING messages — no extra messages required. Each update is piggybacked λ × ceil(log₂ n) times (λ=3) before being dropped; a newer change about the same node replaces the older entry. Selection prioritizes urgent updates (Suspect/Dead before Alive), then fewest transmits. Spreads like an epidemic: each node tells a few others who tell a few more.

**Push-Pull Anti-Entropy Sync**

As a backstop for updates that epidemic gossip missed — and as the join bootstrap path — every 30s each node exchanges membership snapshots with one random alive peer (`SwimSync` → `SwimSyncAck`). Snapshots are capped at 512 entries (randomly sampled when the table is larger; repeated rounds converge) to stay under the 64 KB SWIM stream read limit. The reply to `SwimJoin` is a full snapshot, so a joiner converges to the cluster view in one round-trip instead of waiting on piggyback gossip.

**SWIM Configuration**

| Parameter | Value |
|-----------|-------|
| Round interval | 500ms |
| Indirect probe fanout (K) | 3 |
| Suspicion timeout | 10 rounds (5s) |
| Gossip piggyback per PING | max(4, ceil(log₂ n)) updates |
| Gossip transmit budget | λ × ceil(log₂ n) per update, λ=3 |
| Push-pull sync interval | 30s, 1 random peer, ≤512 entries/snapshot |
| Convergence | O(log N) rounds. At 1M nodes: ~20 rounds = ~10s for single membership change. |
| Staleness | Acceptable — SWIM provides advisory liveness data for DHT lookup filtering, not authoritative routing. |

SWIM is responsible only for membership and failure detection. It does not carry content routing information — that belongs to the Kademlia DHT.

#### Identity: Pkarr & Ed25519

Every Craftec node has a persistent identity based on an Ed25519 key pair. The NodeId is the 32-byte public key. Pkarr (Public Key Addressable Resource Records) provides a bridge between Ed25519 identities and the global DNS system.

- Relay server URL (for NAT traversal)
- Direct addresses (IP:port pairs, if publicly reachable)
- Protocol capabilities (advertised ALPNs)
- Custom metadata (zone name, node role, etc.)

Key rotation is handled by publishing a new Pkarr record signed with the old key, containing the new public key.

#### Serialization: postcard

All wire protocols use postcard, a compact binary serialization format built on Rust serde. Selected for compactness, zero-copy deserialization, no-std compatibility, deterministic encoding, and native serde integration.

#### Content Routing: iroh Kademlia DHT

iroh is building a native Kademlia DHT (iroh-dht-experiment) for content discovery. CraftOBJ uses this DHT for CID→provider node resolution.

**Key Properties**

- 32-byte keyspace: Matches BLAKE3 hash output
- XOR metric routing: Standard Kademlia distance function
- 256 k-buckets, max 20 nodes per bucket: 5,120 routing table entries maximum
- Provider records: `Value::Blake3Provider` stores NodeId of content holders
- 1 KiB value size limit: Sufficient for provider records (postcard-encoded)
- Built on iroh QUIC connections: Inherits all iroh security properties

#### Discovery Stack

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| 1 | iroh mDNS | LAN peer discovery (automatic, zero-config) |
| 2 | Pkarr + DNS | Global node identity (NodeId → relay URL + addresses) |
| 3 | SWIM | Cluster membership + failure detection |
| 4 | Kademlia DHT | Content routing (CID → provider NodeIds) |

---

### 4. Erasure Coding & Storage Redundancy

Craftec uses Random Linear Network Coding (RLNC) over GF(2⁸) as its erasure coding scheme, selected over Reed-Solomon and fountain codes for its properties in a decentralized, churn-heavy environment.

#### RLNC Overview

In RLNC, original data is divided into K source pieces. Each coded piece is a random linear combination of the K source pieces over GF(2⁸). Any K linearly independent pieces suffice to recover the original data.

- **Rateless:** Any node can generate a new coded piece without coordination
- **Progressive decoding:** Gaussian elimination proceeds as pieces arrive
- **Bandwidth optimal:** Every linearly independent piece contributes to decoding
- **Repair without coordination:** A repair node generates pieces from local pieces with fresh random coefficients
- **GF(2⁸) efficiency:** Single-byte operations, enabling SIMD vectorization

#### RLNC Parameters

- Piece size: 256 KiB (for file storage; 16 KB for SQL pages)
- Segment size: 8 MiB = 32 × 256 KiB
- K = 32 (default, per foundation recommendation)
- Initial encode: the full target count is produced at publish — no separate "initial parity" tier
- Redundancy factor: `redundancy(k) = 2.0 + 16/k`
- Target piece count: `n = k × ceil(redundancy(k))` — the canonical formula shared by encoder and health scanner

For K=32: redundancy factor = 2.5 → n = 32 × ceil(2.5) = 96 total pieces for 32 source pieces (64 parity pieces). For K=8: n = 32. For K=16: n = 48.

#### Self-Describing Piece Headers

Every piece carries its own metadata — no external manifest dependency required for interpretation.

```rust
CodedPiece {
    piece_id:      [u8; 32],   // BLAKE3(coding_vector ‖ data)
    cid:           [u8; 32],   // BLAKE3 of original content
    k:             u32,        // generation size — carried explicitly, not inferred
    coding_vector: [u8; k],    // GF(2⁸) RLNC coefficients
    data:          [u8],       // coded payload
    hommac_tag:    [u8; 32],   // homomorphic MAC for recoding integrity
}
// piece_id = BLAKE3(coding_vector ‖ data) — coefficients alone do not identify a piece
```

#### Verification Tags (vtags)

HomMAC + LHS verification tags allow any node to verify that a coded piece is a valid linear combination of the original source data without decoding. Primary defense against pollution attacks. False positive rate: (1/256)⁸ = 2⁻⁶⁴ ≈ 5.4×10⁻²⁰ (L=8 independent tags, consistent with §28).

---

### 5. Distributed Database (CraftSQL)

CraftSQL provides SQLite-compatible SQL queries over identity-owned databases stored in CraftOBJ. It sits at Layer 3, above CraftOBJ and below CraftStudio.

#### Design Principles

- **SQLite compatibility:** Speaks SQLite wire format and SQL dialect.
- **Single-writer-per-identity:** Each database has exactly one owner (Ed25519 identity) who writes. All other participants are readers. No merge conflicts, no coordination on writes.
- **Local-first operation:** The owner writes locally; readers sync by following the owner's published root CID.
- **Page-level storage:** 16 KB pages stored as CraftOBJ objects with erasure coding.
- **Nodes are intermediaries:** Nodes store pieces and serve reads — they do not own databases. Any user with an Ed25519 key can create and write their own database from any node.

#### Ownership & Replication Model

Single-writer model. Each database is owned by an Ed25519 identity — the owner's public key (or a derivation of it) identifies the database. Only the holder of the private key can write. Readers discover the owner's latest root CID via DHT/Pkarr and fetch pages on demand. Multi-writer coordination, if needed, is an application-layer concern — Craftec does not prescribe it, just as iroh provides transport without prescribing content routing.

This eliminates the need for distributed consensus on database state, CRDT merge semantics, or conflict resolution. The owner's latest root CID IS the truth. Snapshot isolation is trivially correct: readers pin a root CID, all referenced pages are immutable CIDs.

#### Write Access Patterns

The identity holder can write from anywhere — directly on a local node, or via RPC to any remote node. This mirrors how blockchain transactions work: you sign the instruction with your private key, send it to any node, the node verifies the signature and executes. The user does not need to run a node or remain online after submitting.

| Pattern | Description | Use Case |
|---------|-------------|----------|
| Direct write | Identity holder writes on a node they run locally. xSync commits immediately. | Desktop/server node operators |
| RPC write | Identity holder signs a write instruction (SIGNED_WRITE) including SQL mutation, expected_root_cid, and Ed25519 signature. Node verifies: (a) signature valid, (b) expected_root_cid matches current root CID. Executes if valid; returns WRITE_CONFLICT if root CID is stale (optimistic concurrency control). Client retries: fetch current root CID, re-sign, re-submit. | Mobile apps, lightweight clients, CLI tools |
| Delegated write | Identity holder authorizes another Ed25519 key to write on their behalf (capability delegation). The delegate signs with their own key; the node verifies the delegation chain. | Applications acting on behalf of users |

RPC write uses optimistic concurrency control — identical to how database transactions work. Since all writes are single-writer-per-identity, conflicts only occur when the same identity submits concurrent RPCs to different nodes (e.g., mobile app and desktop simultaneously). The compare-and-swap on `expected_root_cid` ensures linearizability: the first write wins, and the second must retry with the updated root CID.

In all cases, the write authorization is cryptographic — the node verifies an Ed25519 signature, not a session or connection identity. Nodes are infrastructure; they execute on behalf of identity holders. This decouples identity from presence: a user's database is accessible from any node in the network.

#### Storage Integration

CraftSQL physical storage is handled entirely by CraftOBJ. Database pages are content-addressed objects, erasure-coded in K=8 page generations (8 × 16 KB pages per generation, matching §28). Target piece count per generation: n = 8 × ceil(2.0 + 16/8) = 32 coded pieces.

#### Schema Migration

Under the single-writer model, schema migration is straightforward. There is no network-wide coordination — the owner controls the schema.

- **Owner-controlled:** The owner runs ALTER TABLE / CREATE TABLE on their database. It is their single-writer DB — no coordination needed.
- **New root CID:** Schema changes produce a new root CID with the updated schema, like any other write.
- **Reader discovery:** Readers discover schema changes via a `schema_version` field in the page index.
- **Backward compatibility:** Readers on older schemas can still read columns they know about; unknown columns are ignored.
- **No network coordination:** Single-writer controls the schema. Other nodes hold pieces — they do not interpret schema.
- **Network-owned databases:** For databases owned by network programs (not a single user identity), the Schema Migration Coordinator WASM program handles versioned upgrades via expand-and-contract pattern.

---

### 6. CraftOBJ: Content-Addressed Storage Layer

CraftOBJ is the content-addressed object store. It provides a simple interface: store data by content (receive a CID), retrieve data by CID. Internally it handles RLNC erasure coding, DHT-based content routing, piece distribution, health monitoring, and repair.

#### Content Model

- All content identified by BLAKE3 hash: `ContentId([u8; 32])`
- Content is immutable — same bytes always produce the same CID
- Deduplication is automatic
- Content is always stored as erasure-coded pieces
- Piece granularity: 256 KiB for files, 16 KB for database pages

#### Storage Lifecycle

The storage lifecycle is protocol-driven — no pin/unpin abstraction. Content enters the network via Publish. Five distribution functions maintain pieces automatically:

- **Push:** Initial distribution from creator — round-robin, 2 pieces per node
- **Distribution:** Rebalancing — nodes with >2 pieces per CID pass excess to nodes without
- **Repair:** HealthScan detects piece count below redundancy target
- **Scaling:** High demand triggers providers to push pieces to highest-rated non-provider
- **Degradation:** Excess pieces beyond minimum redundancy target shed naturally

#### DHT Provider Records

Provider records follow the standard Kademlia provider record design: each provider publishes its own record independently. There is no single giant record listing all providers.

- **Self-advertisement:** Each node independently publishes its own provider record: 'I hold pieces for CID X.'
- **Record content:** A single provider record contains: NodeID (32 bytes), piece count, capabilities — well within 1 KiB.
- **Many small records:** The DHT stores MANY small records per CID key, not one giant record listing all providers.
- **Discovery:** Requester queries DHT for CID X → receives provider records from the K-closest DHT nodes → contacts providers in parallel.
- **TTL:** Provider records have 48-hour TTL. Nodes re-announce every 22 hours. When a node drops pieces, the record expires naturally.

| Key Pattern | Purpose | TTL |
|------------|---------|-----|
| BLAKE3(cid) | Content provider records (one per provider node) | 48 hours |
| BLAKE3("manifest:" + cid) | Cached ContentManifest | 1 hour |

```rust
ContentRecord {
    content_id: [u8; 32],    // BLAKE3
    total_size: u64,
    vtags_cid: Option<[u8; 32]>,
}
```

#### DHT Lookup Latency

With extreme distribution, most content has many providers. Popular content is found in very few hops; cold content requires more hops.

- **Parallel queries:** DHT lookups run in parallel across alpha=3 concurrent paths (standard Kademlia). Multiple paths reduce latency.
- **Expected latency:** For widely-distributed content: 1–3 hops × 50ms per hop = 50–150ms total.
- **Cold/rare content:** May require more hops — acceptable tradeoff for rarely-accessed data.
- **Parallel piece fetch:** Once providers are found, K pieces are fetched simultaneously from different providers. Piece fetch and DHT lookup overlap.

#### Proof of Data Possession (PDP)

CraftOBJ uses coefficient vector cross-checking for PDP. A challenger uses GF(2⁸) linear algebra to verify prover data consistency without downloading the full piece.

```rust
StorageReceipt {
    content_id: [u8; 32],
    storage_node: [u8; 32],   // Ed25519 pubkey (iroh NodeId)
    challenger: [u8; 32],
    segment_index: u32,
    piece_id: [u8; 32],       // BLAKE3(coding_vector ‖ piece_data)
    timestamp: u64,
    nonce: [u8; 32],
    signature: [u8; 64],      // Ed25519 signature
}
```

#### HealthScan — Periodic Repair

HealthScan runs every 5 minutes in two phases: Phase 0 (all nodes) performs DHT count checks (~200 bytes per query). Phase 1 (leader only, 1% rotation) does full vtag spot-checks (~512 KiB). Deterministic repair creates orthogonal pieces with no coordination signals needed.

#### Distribution Model

| Function | Trigger | Action |
|---------|---------|--------|
| Push | Creator publishes | Round-robin, 2 pieces per node |
| Distribution | Node holds > 2 pieces per CID | Pass excess to nodes without CID |
| Repair | HealthScan detects deficit | Deterministic assignment, orthogonal piece creation |
| Scaling | High local demand | Push pieces to highest-rated non-provider |
| Degradation | Demand drops | Excess pieces shed naturally |

#### Node-Level Storage Management

Each node manages its own local storage constraints independently. Configurable storage limits, admission policies, and eviction policies (funded pieces > critical CIDs > recently served > own content).

#### Fetch Strategy

Fetch is client-side intelligence, dumb storage nodes. Pipeline: resolve providers via DHT, select by latency, send "any piece for segment Y" requests, check linear independence, decode via Gaussian elimination, verify BLAKE3.

#### Content Discovery Model

Two paths: Path 1 (DHT) — client has CID, resolves directly via Kademlia DHT. Path 2 (CraftSQL) — metadata search returns CIDs, which are then resolved via Path 1. CraftSQL is a convenience index, NOT a routing layer.

#### Economic Model: Free + Funded Tiers

Free tier: Voluntary participation, like BitTorrent. Funded tier: Subscription revenue flows to creator pools. StorageReceipts earned via PDP verification. The protocol mechanics are identical for both tiers.

---

### 7. Cryptography & Attestation

Craftec's security model is built on a small set of well-understood cryptographic primitives. Minimalism: fewer primitives mean a smaller attack surface.

#### Cryptographic Primitives

| Primitive | Algorithm | Usage |
|-----------|-----------|-------|
| Hash | BLAKE3 (32-byte) | CIDs, piece IDs, Merkle trees, challenge vectors |
| Signature | Ed25519 | Node identity, message signing, StorageReceipts |
| Key Agreement | X25519 (via QUIC/TLS 1.3) | Connection encryption |
| Symmetric | ChaCha20-Poly1305 (via TLS 1.3) | Stream encryption |
| PRNG | BLAKE3 in keyed mode | Deterministic challenge vector generation for vtags |

#### CraftCOM Attestation

CraftCOM is a general-purpose distributed compute runtime. One key use case is k-of-n attestation: a quorum of agents must agree before economic actions are executed. Other use cases include data validation, ML inference, content indexing, and custom business logic — any workload that can compile to WASM.

---

### 8. Language & Runtime Stack

Craftec is implemented primarily in Rust. Memory safety without GC, zero-cost abstractions, fearless concurrency, ecosystem alignment with iroh/quinn/blake3/postcard. Async runtime: Tokio. GF(2⁸) arithmetic uses SIMD-vectorized precomputed lookup tables.

---

### 9. Client Frameworks (CraftStudio)

CraftStudio is the application-layer framework. Local-first programming model: applications always work, even offline, and synchronize when connectivity is available.

#### Platform Targets

| Platform | Language | Integration |
|----------|----------|-------------|
| Desktop (Linux, macOS, Windows) | Rust + Tauri | Native webview, system tray, filesystem access |
| iOS | Rust + Swift (via UniFFI) | Native UIKit/SwiftUI, background fetch |
| Android | Rust + Kotlin (via UniFFI) | Native Jetpack Compose, WorkManager |
| Web | Rust + WASM | Browser runtime, IndexedDB, WebRTC for P2P |

#### Developer API

- `craftql(sql)`: Execute SQL queries against local CraftSQL replica
- `store(bytes) → CID`: Store arbitrary bytes, returns content ID
- `fetch(cid) → bytes`: Retrieve content by CID (handles DHT lookup, piece fetch, decoding)
- `subscribe(query) → Stream`: Live query that emits results on data changes
- `sync()`: Force immediate synchronization with peers
- `rpc_write(node_id, sql, keypair) → RootCID`: Sign and send a write instruction to a remote node for execution
- `delegate(child_keypair, permissions) → DelegationCert`: Authorize another key to write on behalf of this identity

---

### 10. Application-Layer Protocol Composition

Craftec's iroh Endpoint supports multiple concurrent services via ALPN. Each service registers a unique ALPN identifier. iroh maintains at most one QUIC connection per peer — all services multiplex over that connection.

#### Registered ALPN Services

| Service | ALPN Identifier | Purpose | Persistence |
|---------|----------------|---------|-------------|
| CraftOBJ | /craftobj/1 | Persistent distributed storage | Persistent (DHT + RLNC) |
| CraftSQL | /craftsql/1 | Database sync (root CID follow) | Persistent (CraftOBJ-backed) |
| CraftNet | /craftnet/1 | Decentralized VPN | Ephemeral (transit only) |
| SWIM | /craftec/swim/1 | Cluster membership | Ephemeral (in-memory) |
| iroh-gossip | /iroh-gossip/0 | Pub-sub coordination | Ephemeral |
| iroh-roq | /iroh-roq/0 | Real-time video/audio streaming | Ephemeral (no storage) |
| iroh-blobs | /iroh-blobs/4 | Large ephemeral data transfers | Ephemeral (temp cache) |

#### Video Streaming (iroh-roq / iroh-live)

Real-time video/audio streaming uses iroh-roq (QUIC-based, moq-rs) and iroh-live. Low-latency, unidirectional media streams over the same iroh Endpoint.

#### AI/ML Training Coordination

Decentralized AI/ML training (Nous Research) coordinates gradient exchange via iroh-gossip and iroh-blobs. Training data stored persistently in CraftOBJ; coordination is ephemeral.

#### CraftNet: Decentralized VPN

CraftNet provides privacy-preserving network transit via onion routing, erasure-coded shard forwarding, and decentralized exit nodes. Migrating from libp2p to iroh stack.

#### Precedent: iroh Ecosystem Projects

| Project | iroh Protocols Used | Data Model | What Craftec Adds |
|---------|-------------------|-----------|------------------|
| Nous Research (AI) | iroh-gossip + iroh-blobs | Ephemeral | Persistent training data via CraftOBJ |
| iroh-live (video) | iroh-roq (moq-rs/QUIC) | Ephemeral | Recorded content stored in CraftOBJ |
| Delta Chat | iroh-gossip (webxdc) | Ephemeral; email as store | Persistent P2P storage |

---

## PART B: NODE LIFECYCLE

### 11. First-Run Initialization

| | |
|--|--|
| **What it does** | Generates Ed25519 keypair, creates directories, initializes SQLite schema, writes completion marker. Runs exactly once; idempotent if marker present. |
| **Depends on** | Filesystem, OS entropy, SQLite library |
| **Depended by** | Startup Sequence (#12), Node Identity (#17), CraftSQL (#33) |

#### Initialization Sequence

1. Check for `~/.craftec/init.done` marker file. If present, skip.
2. Create directory tree: `~/.craftec/{keys/, data/craftobj/, db/, logs/, tmp/}`.
3. Generate Ed25519 keypair using OS CSPRNG. Write private key to `keys/node.key` (mode 0600). Write public key to `keys/node.pub`.
4. Initialize peer database at `db/peers.db` with schema: `peers(node_id, addr, last_seen, reliability_score, ban_score, parole_mode)`.
5. Initialize metrics database at `db/metrics.db`.
6. Write NodeID = Ed25519 public key (32 bytes) to `keys/node_id`. (This is the iroh NodeId convention — the public key IS the identity, no SHA-256 hashing.)
7. Raise file descriptor limit: `setrlimit(RLIMIT_NOFILE, 65535)`. Log result.
8. Write completion marker `~/.craftec/init.done`. This is the LAST step — makes initialization atomic.

#### Error Handling & Recovery

- **Key generation fails:** OS entropy unavailable. Fatal error — abort.
- **Directory creation fails:** Disk full or permissions. Fatal — clean up partial dirs, abort.
- **Database schema fails:** Delete partial database, abort.
- **Partial init detected** (dirs exist, no init.done): Delete all created files, restart from step 1.
- **setrlimit fails:** Non-fatal. Log warning. Continue with default FD limit.

#### Edge Cases

- **Two processes simultaneously:** Use O_EXCL flag on init.done to ensure only one completes.
- **Disk fills between keypair write and database init:** triggers full re-init on next start.
- **Cross-filesystem move of `~/.craftec`:** All paths must be anchored to config root, not CWD.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| FD limit target | 65535 |
| Key file permissions | 0600 |
| NodeID size | 32 bytes (Ed25519 public key) |
| Completion marker | init.done (written last) |

---

### 12. Startup Sequence

| | |
|--|--|
| **What it does** | Orchestrates ordered boot of all subsystems. Detects dirty shutdown via sentinel file, runs integrity checks, starts subsystems in dependency order. |
| **Depends on** | First-Run Init (#11), OS signals |
| **Depended by** | All other subsystems |

#### Initialization Sequence

1. Validate `init.done` exists. If not, run First-Run Init (#11).
2. Check sentinel file `~/.craftec/node.lock`. If present: dirty shutdown. Set `dirty_shutdown=true`, trigger Crash Recovery (#15).
3. Write sentinel file `node.lock` with PID and timestamp.
4. Load and validate configuration (#13). If config invalid, abort.
5. Raise FD limit to 65535 via `setrlimit`.
6. Boot subsystems in dependency order:
   - 6a. Clock/HLC (#42) — no dependencies
   - 6b. Event Bus (#52) — no dependencies
   - 6c. Task Scheduler (#48) — depends on Event Bus
   - 6d. Node Identity (#17) — depends on keypair files
   - 6e. CraftOBJ store (#27) — depends on filesystem
   - 6f. CraftSQL VFS (#33) — depends on CraftOBJ
   - 6g. iroh Endpoint init (#21) — depends on Identity (iroh manages transport-level connections)
   - 6h. Bootstrap & Discovery (#18) — depends on iroh Endpoint
   - 6i. CraftCOM / Wasmtime (#38) — depends on CraftSQL, CraftOBJ
   - 6j. Health Scanner (#30) — depends on CraftOBJ, Networking
   - 6k. Background Job Coordinator (#51) — depends on all storage/net subsystems
7. Register signal handlers: SIGINT/SIGTERM → graceful shutdown, SIGHUP → config hot-reload.
8. Emit `node.started` event. Log startup complete with NodeID and address.

#### Error Handling & Recovery

- **Subsystem fails to start:** If critical (networking, storage), abort. If non-critical (metrics), log warning and continue.
- **FD limit raise fails:** Non-fatal warning.
- **Signal handler registration fails:** Fatal — cannot guarantee clean shutdown.

#### Edge Cases

- **Node started while already running:** PID in `node.lock` alive — refuse to start.
- **Stale lock from killed process:** Treat as dirty shutdown, run crash recovery.
- **Subsystem boot timeout (>30s):** Treat as failure.

---

### 13. Configuration Management

| | |
|--|--|
| **What it does** | Loads, validates, and hot-reloads node configuration from a TOML file. |
| **Depends on** | Filesystem, Event Bus (#52) |
| **Depended by** | All subsystems with tunable parameters |

#### Initialization Sequence

1. Determine config path: CLI flag > env var `CRAFTEC_CONFIG` > default `~/.craftec/config.toml`.
2. If config file does not exist, write defaults to disk.
3. Parse TOML. If parse fails → abort with error pointing to line/col.
4. Validate all fields against schema. Collect all errors before failing.
5. Populate Config struct, falling back to defaults for unset fields.
6. Store config in `Arc<RwLock>` for concurrent read access.

#### Runtime Behavior

SIGHUP triggers hot-reload. If valid, atomically swap into Arc. Emit `config.reloaded` event with diff. If invalid, keep old config. Connection limits, timeout values, and bandwidth caps can be hot-reloaded. Keypair/database paths require restart.

#### Key Parameters

| Config Key | Default | Hot-Reload? |
|-----------|---------|-------------|
| max_connections | 256 | Yes |
| peer_timeout_secs | 120 | Yes |
| handshake_timeout_secs | 10 | Yes |
| disk_watermark_warn_pct | 90 | Yes |
| disk_watermark_critical_pct | 95 | Yes |
| rlnc_k | 32 | No (requires restart) |
| listen_addr | 0.0.0.0:0 | No (requires restart) |
| data_dir | ~/.craftec/data | No (requires restart) |

---

### 14. Shutdown Sequence

| | |
|--|--|
| **What it does** | Orchestrates ordered, graceful teardown. Storage flushed before network closed. Target: <10 seconds. |
| **Depends on** | All subsystems (reverse order), Event Bus |
| **Depended by** | Nothing — terminal operation |

#### Shutdown Steps

1. Set `is_shutting_down=true`. Reject new incoming connections.
2. Emit `shutdown.started` event. All subsystems begin draining.
3. Stop accepting new P2P connections. Broadcast DISCONNECT.
4. Cancel non-critical background jobs (RLNC encoding, eviction, health scan). Allow active writes to complete (30s timeout).
5. Drain write buffers. Flush Batch Writer (#50).
6. Flush CraftSQL — ensure all committed transactions have durable CIDs.
7. Close all open CraftSQL database handles.
8. Close all P2P connections (graceful QUIC CLOSE frame).
9. Flush Prometheus metrics to disk.
10. Close CraftOBJ store (sync filesystem).
11. Shutdown Task Scheduler — drain remaining queue.
12. Shutdown Event Bus — drain pending events.
13. Shutdown Wasmtime engine — terminate running agents.
14. Delete `node.lock` sentinel file. This is the last write.
15. Exit process with code 0.

---

### 15. Crash Recovery

| | |
|--|--|
| **What it does** | Runs when dirty shutdown detected. Verifies CraftOBJ CAS integrity, checks CraftSQL consistency, identifies orphans. |
| **Depends on** | Startup (#12), CraftOBJ (#27), CraftSQL (#33), Local Eviction (#31) |
| **Depended by** | Startup Sequence — must complete before other subsystems boot |

#### Recovery Steps

1. Log "dirty shutdown detected". Emit `recovery.started` event.
2. Open CraftOBJ read-only. Scan all CIDs. Verify `BLAKE3(hash(content)) == CID`.
3. Delete corrupt CIDs from local store (re-fetched from peers later).
4. Open CraftSQL peer database. Run `PRAGMA integrity_check`. If failures, delete and let First-Run Init recreate.
5. Identify orphaned CIDs (not referenced by any live root). Safe to evict immediately.
6. Run local eviction pass on orphaned CIDs (two-phase mark-and-sweep with 5.5-minute safety window, matching normal eviction §31).
7. Verify root CID consistency: load latest root, verify all referenced CIDs are present.
8. Write recovery summary to log. Emit `recovery.complete` event.

---

### 16. Upgrade & Migration

| | |
|--|--|
| **What it does** | Manages protocol versioning, database schema migration, wire format evolution. Minimum 90-day deprecation period. |
| **Depends on** | Configuration (#13), CraftSQL (#33), Protocol Negotiation (#22) |
| **Depended by** | All subsystems with versioned state |

Schema migrations are additive only: new columns, new tables. Never delete or rename. Wire format versioning: all messages include version field (u8) + 4-byte type tag. ALPN negotiation handles version selection. Opportunistic upgrade: always speak the minimum version the peer supports.

| Parameter | Value |
|-----------|-------|
| Deprecation window | ≥90 days after major version bump |
| Max simultaneous protocol versions | 2 (current + current-1 major) |
| Schema version field | u32, stored in db/schema.db |

---

## PART C: IDENTITY & TRUST

### 17. Node Identity

| | |
|--|--|
| **What it does** | Manages Ed25519 keypair, derives canonical NodeID, provides signing/verification. Private key never leaves key storage. |
| **Depends on** | First-Run Init (#11), Filesystem |
| **Depended by** | Wire Protocol (#23), Bootstrap (#18), Attestation (#41) |

#### Initialization Sequence

1. Load `keys/node.key` (raw Ed25519 private key, 32 bytes). Verify permissions are 0600.
2. Derive public key from private key. Compare against `keys/node.pub` — mismatch indicates corruption, abort.
3. Derive NodeID = Ed25519 public key (32 bytes). This is the iroh NodeId convention — the public key IS the identifier.
4. Initialize signing context. Expose `sign(msg) → Signature` and `verify(pk, msg, sig) → bool`.
5. Log NodeID (hex-encoded) at INFO level.

#### Runtime Behavior

Signing is synchronous (~7µs for Ed25519). Key rotation: future feature — generate new keypair, sign new public key with old private key, broadcast rotation announcement. NodeID changes on rotation.

#### Shutdown Sequence

Zeroize private key bytes in memory on shutdown (use `zeroize` crate).

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Key algorithm | Ed25519 (ed25519-dalek) |
| Private/Public key size | 32 bytes each |
| Signature size | 64 bytes |
| NodeID size | 32 bytes (= Ed25519 public key) |
| Sign latency | ~7µs |
| Verify latency | ~2.3µs |

---

### 18. Bootstrap & Discovery

| | |
|--|--|
| **What it does** | Provides initial peer set. Uses layered discovery: iroh relay as primary bootstrap, DNS seeds, Pkarr, SWIM, and DHT for content routing. |
| **Depends on** | Node Identity (#17), iroh Endpoint (#21), Configuration (#13) |
| **Depended by** | Connection Pool (#25), SWIM membership |

#### Initialization Sequence

1. Load peer database from `db/peers.db`. If non-empty, add to candidate set.
2. Connect to iroh relay server (primary bootstrap path). Relay provides immediate connectivity.
3. Resolve DNS seeds in parallel (6–8 hostnames). Collect all returned addresses.
4. If DNS resolution fails, fall back to hardcoded IP/port list (≥6 addresses).
5. If `--addpeer` CLI flags provided, add to front of candidate list.
6. Attempt connections to top 8 candidates in parallel (timeout: 10s).
7. Once ≥3 connections established, begin SWIM join protocol.
8. Resolve Pkarr name records for configured named nodes.
9. Initialize DHT routing table for content routing (CID→provider resolution).
10. Emit `bootstrap.complete` event when ≥3 peers connected.

#### Runtime Behavior

SWIM membership runs continuously: gossip-based failure detection every 500ms. DHT provider re-announce every 22h; records expire at 48h. Pkarr DNS-over-HTTPS for named node lookup with TTL=3600s.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| DNS seed count | 6–8 hostnames |
| Bootstrap parallel attempts | 8 |
| Bootstrap success threshold | ≥3 connections |
| SWIM tick interval | 500ms |
| SWIM suspicion timeout | 5 seconds |
| DHT re-announce interval | 22 hours |
| DHT record TTL | 48 hours |
| Pkarr cache TTL | 3600 seconds |

---

### 19. Peer Reputation & Trust

| | |
|--|--|
| **What it does** | Per-peer reliability and ban scores. Parole mode for corrupt data without immediate banning. Score decay over time. |
| **Depends on** | Node Identity (#17), Event Bus (#52) |
| **Depended by** | Connection Pool (#25), Bootstrap (#18) |

#### Runtime Behavior

**Reliability Score (0.0–1.0):** Incremented on success, decremented on timeout/failure. New peers start at 0.5.

**Ban Score (0–100):** +50 for corrupt data, +25 for protocol violation, +5 for DONT_HAVE spam. Score ≥100 → ban.

**Parole Mode:** When a verified piece fails BLAKE3/HomMAC, all contributing peers enter parole. Isolated testing identifies the offending peer.

**Score Decay:** Reliability decays toward 0.5 at 1%/hr; ban score decays toward 0 at 2%/hr.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Initial reliability score | 0.5 (neutral) |
| Ban threshold | 100 |
| Corrupt data penalty | +50 ban score |
| Protocol violation penalty | +25 ban score |
| Parole failure limit | 3 consecutive → ban |

---

### 20. Network Admission

| | |
|--|--|
| **What it does** | Controls node admission. Enforces Sybil resistance via subnet diversity, eclipse prevention, connection limits. |
| **Depends on** | Node Identity (#17), Bootstrap (#18), Reputation (#19) |
| **Depended by** | Connection Pool (#25) |

#### Runtime Behavior

**Subnet Diversity:** Max 2 nodes from same /24 IPv4 (or /48 IPv6).

**Eclipse Prevention:** ≥30% of peers from different discovery sources.

**Slow Loris Defense:** Max 50 HANDSHAKE_IN_PROGRESS, 15s hard deadline.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Max connections (global) | 256 |
| Max handshakes in progress | 50 |
| Handshake deadline | 15 seconds |
| Max peers per /24 subnet | 2 |
| Source diversity minimum | 30% |
| Peer rotation interval | 300 seconds |

---

## PART D: NETWORKING

### 21. Connection Lifecycle

| | |
|--|--|
| **What it does** | Manages full lifecycle of P2P connections: establishment via relay, hole-punch upgrade, keepalive, migration, teardown. iroh handles transport-level connection management; application-level HELLO/capability exchange sits on top. |
| **Depends on** | Node Identity (#17), Network Admission (#20), NAT Traversal (#26) |
| **Depended by** | Wire Protocol (#23), Connection Pool (#25) |

#### Connection Flow

1. Initialize iroh Endpoint with node keypair and relay list.
2. Start relay connection: all connections start via relay immediately.
3. Attempt hole-punch via iroh DCUtR. 70% success rate; 97.6% succeed on first attempt.
4. If hole-punch succeeds: migrate to direct QUIC. Relay remains 30s as fallback.
5. Connection established. Send HELLO message (NodeID + version + capability bits).
6. Start keepalive timer: 60s interval. Peer timeout: 120s.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Connection start | Via relay immediately |
| Hole-punch success rate | 70% ± 7.1% |
| First-attempt success | 97.6% |
| Symmetric NAT users | ~30% (relay permanently) |
| Keepalive interval | 60 seconds |
| Peer timeout | 120 seconds |
| Reconnect backoff | 60s × failcount (max 180s) |

---

### 22. Protocol Negotiation

| | |
|--|--|
| **What it does** | Negotiates protocol version and capability bits at connection time using ALPN over TLS 1.3. Implements opportunistic upgrade. |
| **Depends on** | Connection Lifecycle (#21) |
| **Depended by** | Wire Protocol (#23) |

Initiator sends ALPN list: `["/craftec/2.0", "/craftec/1.0"]`. Responder selects highest mutual version. After ALPN selection, exchange capability bitfields in HELLO message. Both sides compute capability intersection. 90-day deprecation window for old versions.

---

### 23. Wire Protocol

| | |
|--|--|
| **What it does** | Binary message format for all P2P communication. postcard serialization. Per-message Ed25519 signatures are removed (redundant with QUIC TLS 1.3). Only attestation messages and DHT records are signed. |
| **Depends on** | Node Identity (#17), Protocol Negotiation (#22) |
| **Depended by** | All networking subsystems |

#### Message Frame Layout

```
v1: [ type_tag: u32 | version: u8 (=1) | hlc_ts: u64 | payload_len: u32 | payload: [u8] ]
v0: [ type_tag: u32 | version: u8 (=0) | payload_len: u32 | payload: [u8] ]   (legacy)
```

Total fixed header: 17 bytes per message (v1). All integers big-endian. The `hlc_ts` field carries the sender's HLC timestamp (§42) — it is the basis for the ±30s replay window and ±500ms skew rejection. The 9-byte v0 header is retained only for backward compatibility during upgrade.

> **Note:** QUIC TLS 1.3 provides authentication and encryption at the transport layer, making per-message Ed25519 signatures redundant. Only attestation messages (ATTEST_BROADCAST) and DHT records carry Ed25519 signatures.

#### Message Types

| Type Tag | Message | Direction | Description |
|----------|---------|-----------|-------------|
| 0x0001 | PING | both | HLC timestamp, nonce |
| 0x0002 | PONG | both | Echo nonce, HLC timestamp |
| 0x0010 | PIECE_REQUEST | requester→provider | CID, piece request (any piece for CID/segment) |
| 0x0011 | PIECE_RESPONSE | provider→requester | Coded piece bytes + coding vector + HomMAC tag |
| 0x0020 | PROVIDER_ANNOUNCE | both | "I hold pieces for CID X" announcement |
| 0x0030 | SIGNED_WRITE | requester→node | Signed SQL mutation: database_id, expected_root_cid, SQL, Ed25519 signature, public key |
| 0x0040 | HEALTH_REPORT | both | Piece availability report for health scanning |
| 0x0100 | SWIM_JOIN | both | Membership join |
| 0x0101 | SWIM_ALIVE | both | Membership alive gossip |
| 0x0102 | SWIM_SUSPECT | both | Membership suspicion gossip |
| 0x0103 | SWIM_DEAD | both | Membership dead gossip |
| 0x0104 | SWIM_PING | both | Membership probe |
| 0x0105 | SWIM_PING_ACK | both | Membership probe response |
| 0x0106 | SWIM_PING_REQ | both | Indirect probe request |
| 0x0107 | SWIM_SYNC | both | Push-pull anti-entropy sync request (membership snapshot) |
| 0x0108 | SWIM_SYNC_ACK | both | Push-pull sync response; also the reply to SWIM_JOIN |
| 0x0200 | RPC_REQUEST | requester→node | RPC framing (carries SIGNED_WRITE et al.) |
| 0x0201 | RPC_RESPONSE | node→requester | RPC result: new root CID, row count, or WRITE_CONFLICT if expected_root_cid is stale |

> **Planned, not yet implemented:** HELLO (capability exchange, §21–22), DISCONNECT (graceful close reason), and ATTEST_BROADCAST (CraftCOM attestation, §41) are specified but do not yet have wire-level message types.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Serialization | postcard (serde) |
| Encode latency | ~60ns |
| Decode latency | ~180ns |
| Max message size | 4 MB |
| Header size | 17 bytes (v1); 9 bytes (v0 legacy) |
| Replay window | ±30 seconds (HLC) |

---

### 24. Backpressure & Flow Control

| | |
|--|--|
| **What it does** | Ensures no subsystem can overwhelm another. QUIC native stream flow control, bounded async channels, semaphores. |
| **Depends on** | Connection Lifecycle (#21), all pipeline stages |
| **Depended by** | All subsystems with async data pipelines |

QUIC Flow Control: Stopping to read causes `write_all().await` to block. Window: 1 MB/stream, 10 MB/connection. Every `mpsc::channel` has explicit capacity bound. `Semaphore(256)` for connections, `Semaphore(16)` for concurrent CID fetches per-peer, `Semaphore(8)` for concurrent RLNC decodes.

| Pipeline Stage | Channel | Capacity |
|---------------|---------|----------|
| Incoming connections | accept → handler | 64 |
| Incoming messages | network → dispatch | 256 |
| CID fetch requests | VFS → object store | 128 |
| RLNC encoding jobs | commit → encoder | 64 |
| Distribution jobs | encoder → distributor | 32 |
| Eviction candidates | scanner → eviction | 512 |
| Metrics events | all → reporter | 1024 (lossy OK) |

---

### 25. Connection Pool Management

| | |
|--|--|
| **What it does** | Maintains active P2P connections. Enforces limits. Scores connections and implements turnover policy. |
| **Depends on** | Connection Lifecycle (#21), Admission (#20), Reputation (#19) |
| **Depended by** | All subsystems needing to send to a peer |

Global max 256 connections (20 reserved for inbound). Turnover: every 300s, if >90% full, disconnect bottom 4% by reliability score. Connection score = 0.6×reliability + 0.2×uptime + 0.2×(1 - latency). Outbound rate: max 30 new/second.

#### SWIM-Dead Filter on DHT Lookups

Before connecting to a DHT-returned provider, each node checks its local SWIM-dead set to skip known-dead peers. This is purely local filtering — no broadcast needed.

- **Dead set:** Each node maintains a local SWIM-dead set from SWIM failure detection.
- **Pre-connection check:** Before connecting to a DHT-returned provider, check whether the provider is in the dead set.
- **Skip dead, try alternatives:** Skip dead providers and try alternatives first.
- **Fallback to suspected:** If no alternatives exist, try suspected-dead providers as fallback (SWIM may have false positives, especially on the suspicion boundary).
- **Dead set expiry:** Dead set entries expire after 2× SWIM suspicion timeout to avoid stale avoidance of recovered peers.

---

### 26. NAT Traversal

| | |
|--|--|
| **What it does** | Connectivity for nodes behind NAT using iroh relay infrastructure and hole-punching (DCUtR). Every publicly-reachable node serves as a relay — the relay mesh is as distributed as the storage mesh. |
| **Depends on** | Connection Lifecycle (#21), Configuration (#13) |
| **Depended by** | Connection Lifecycle (#21) |

iroh relay servers handle 1M+ concurrent connections/server. Hole-punch success: open cone ~95%, address-restricted ~85%, port-restricted ~70%, symmetric ~20%. For symmetric NAT: use relay permanently. IPv6 eliminates most NAT issues.

#### Distributed Relay Mesh

Every node with a publicly reachable IP serves as a relay for nodes behind NAT — the same principle as "every node provides storage." Relay capability is a default network contribution from publicly-reachable nodes.

- **iroh relay binary:** Open source, can be embedded in the node binary. No separate deployment needed.
- **Relay capability advertisement:** Advertised via a capability bit in the HELLO message. Nodes discover available relays via DHT/Pkarr.
- **No single point of failure:** No single relay failure can partition the network. The relay mesh is as distributed as the storage mesh.
- **Relay discovery for symmetric NAT:** Nodes behind symmetric NAT connect to the nearest available relay from the mesh, discovered via DHT/Pkarr.
- **Relay vs direct:** All connections start via relay; iroh attempts direct hole-punch upgrade. Relay remains as permanent fallback for symmetric NAT (~30%).

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Relay capacity per node | 1M+ connections/server |
| Overall hole-punch success | 70% ± 7.1% |
| Symmetric NAT prevalence | ~20-30% of internet users |
| Hole-punch timeout | 2 seconds |

---

## PART E: STORAGE — CraftOBJ

### 27. Content-Addressed Store

| | |
|--|--|
| **What it does** | Local, append-only, content-addressed object store. Every blob stored under its BLAKE3 hash (CID). Implicit integrity verification, natural deduplication, immutable storage. |
| **Depends on** | Filesystem, BLAKE3 library |
| **Depended by** | CraftSQL (#33–37), RLNC (#28), GC (#31) |

#### Operations

**Put(data):** Compute CID = BLAKE3(data). Check bloom filter. Write to temp file, fsync, atomic rename. ~2µs for 16KB page hash.

**Get(cid):** Check hot LRU cache. Check bloom filter. Read file. Verify `BLAKE3(content) == CID`. If mismatch: delete corrupt file, emit alert.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Hash function | BLAKE3 |
| CID size | 32 bytes |
| Directory sharding | 256 subdirs (first byte of CID) |
| Bloom filter FP rate | 1% |
| Default LRU cache | 256 MB |
| Write pattern | temp → fsync → atomic rename |

---

### 28. RLNC Erasure Coding

| | |
|--|--|
| **What it does** | Implements RLNC over GF(2⁸) for storage redundancy. Encodes, recodes without decode, verifies via HomMAC. K=32 for large objects, K=4–8 for SQLite pages. |
| **Depends on** | CraftOBJ (#27), Task Scheduler (#48) |
| **Depended by** | Piece Distribution (#29), Health Scanning (#30) |

**Encode:** Generate n = k × ceil(redundancy(k)) coded pieces with random GF(2⁸) vectors.

**Recode:** Combine coded pieces with fresh coefficients — no decode needed.

**Decode:** Gaussian elimination with ≥K linearly independent pieces.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Field | GF(2⁸) (K≤100), GF(2¹⁶) (K>100) |
| K (SQLite pages) | 8 (8-page generations) |
| K (large files) | 32 |
| Redundancy factor | redundancy(k) = 2.0 + 16/k |
| Target piece count | n = k × ceil(redundancy(k)) — K=32→96, K=16→48, K=8→32 |
| HomMAC security | 2⁻⁶⁴ (L=8 independent tags) |
| Encode throughput (K=32) | ~75 Gbps single core |
| Decode throughput (K=32) | ~48 Gbps (4× slower than encode) |

#### RLNC Decode: Client-Side Only

A critical architectural point: storage-serving nodes never decode. RLNC decoding is exclusively a client-side operation.

- **Storage nodes:** Receive coded pieces, store them locally, serve them on request. Never decode, never reconstruct original data. They are piece-holders only.
- **Client nodes:** Fetch K coded pieces from multiple providers, run Gaussian elimination to decode, reconstruct original pages or files.
- **Semaphore(8):** The existing Semaphore(8) for concurrent RLNC decodes is sufficient for client-side resource management.
- **Memory budget:** 8 concurrent segment decodes × 32 pieces × 256 KiB = 64 MB — well within client memory budgets.
- **Risk level:** LOW. Previously rated HIGH was based on an incorrect assumption that serving nodes decode. They do not.

---

### 29. Piece Distribution

| | |
|--|--|
| **What it does** | Distributes coded pieces to peers. Distributes to non-holders first (peers with 0 pieces), then under-holders (peers with 1 piece to bring them to ≥2 for future repair eligibility). Retry with backoff. |
| **Depends on** | RLNC (#28), Connection Pool (#25), Reputation (#19) |
| **Depended by** | Health Scanning (#30) |

Target: distribute n coded pieces across ≥K distinct peers. Distribute n = k × ceil(2 + 16/k) total pieces across ≥K distinct peers. Target: each peer receives at least 1 piece, with enough diversity for any K peers to reconstruct. 10s timeout per peer. Live want limit: 32 outstanding per session.

> **Note (v3.4):** The distribution priority is determined by peer need, not piece rarity. RLNC coded pieces are all unique (each carries a distinct random GF(2⁸) coefficient vector) — the concept of "rarest piece" does not apply. Distribution priority: (1) peers holding 0 pieces (non-holders), (2) peers holding exactly 1 piece (under-holders, to bring them to ≥2 for future repair eligibility). Note that the *repair* path (§55) inverts this order — a newly recoded repair piece goes to 1-piece holders first to restore repair eligibility, then to non-holders. Both orderings are intentional and serve different goals: distribution maximizes spread; repair maximizes the pool of repair-eligible nodes.

---

### 30. Health Scanning & Repair

| | |
|--|--|
| **What it does** | KERNEL-LEVEL — compiled into node binary, not WASM. CID health is the foundational building block of the entire ecosystem. Periodically scans all stored CIDs for piece availability. Detects under-redundancy, triggers repair by recoding. Without HealthScan, CIDs rot silently and the entire system loses durability. Low-priority background job but highest-priority subsystem in terms of system correctness. |
| **Depends on** | CraftOBJ (#27), RLNC (#28), Distribution (#29), Connection Pool (#25), DHT (#3) |
| **Depended by** | Background Job Coordinator (#51), Program Scheduler (#48) |

**Kernel classification rationale:** HealthScan cannot be a network-owned WASM program because WASM programs themselves depend on HealthScan to keep their CIDs healthy. A WASM-based HealthScan would be a circular dependency: the program that maintains durability would itself become undurable without an operational HealthScan to watch it. This is why HealthScan is compiled into the node binary — it is the foundation everything else stands on.

#### Scan Scope and Rate

Each node only scans CIDs for which it holds ≥2 pieces (locally-held CIDs). It never scans CIDs it does not hold — it has no responsibility for those.

- **Scope:** Locally-held CIDs only (this node holds ≥2 pieces for each scanned CID — the minimum required to recode).
- **Per-cycle scan rate:** 1% of held CIDs per cycle.
- **CID ordering:** Sorted by CID value, then by last-health-check timestamp (oldest first).
- **Full coverage:** 100 cycles × 1% = 100% of held CIDs covered per full pass.
- **Cycle interval:** 5 minutes. Full coverage every ~8 hours (100 cycles × 5 min).
- **No query storms:** All nodes run async on independent schedules. Aggregate DHT query load is evenly distributed across all cycles.
- **Example:** A node holding 1,000 CIDs scans 10 per cycle = 10 DHT queries × 200 bytes = 2 KB per cycle.

For each CID in scope: query peers for piece availability. If `available_count < target n`: trigger repair via Natural Selection Coordinator. Repair: recode from ≥2 locally-held coded pieces with fresh GF(2⁸) coefficients (no decode needed), distribute 1 new piece to a peer lacking pieces. Initial scan deferred 5 minutes post-startup.

#### Natural Selection Coordinator

For any CID, multiple nodes hold pieces — any of them could coordinate repair. Craftec uses a natural selection approach: the best-qualified nodes automatically become coordinators for that CID in the current cycle, with no explicit election or network-wide synchronization.

- **Candidate pool:** All provider nodes holding ≥2 pieces for the CID form the candidate repair pool.
- **Ranking:** Candidates ranked by (1) uptime, (2) reputation score, (3) NodeID as tiebreaker. Deterministic ranking — all nodes compute the same order independently.
- **Top-N nodes act:** When deficit = N pieces needed, the top-N ranked nodes each independently produce 1 new coded piece per cycle. Each node deterministically computes the same ranking and knows if it's in the top-N. No explicit election or coordination needed.
- **1 piece per elected node per CID per cycle:** Each of the top-N elected nodes produces 1 piece per CID per cycle. Total repair rate = min(N, deficit) pieces per cycle. Prevents repair storms while still converging quickly.
- **Failover:** If the top-ranked node is down (detected via SWIM), the next-ranked node picks up in the next cycle. No synchronization needed.
- **Async execution:** Nodes run HealthScan on independent schedules. No synchronization between nodes. Aggregate DHT query load is evenly distributed.
- **NOT XOR-based:** Pieces are randomly distributed by RLNC — not stored by key-space proximity. Coordinator selection is by quality (uptime/reputation), not by DHT distance.

---

### 31. Local Eviction Policy

| | |
|--|--|
| **What it does** | Reclaims local storage by dropping pieces a node no longer wants to hold. Eviction is a purely local decision — not a network-wide process. The network is not coordinated; nodes independently decide what to keep. |
| **Depends on** | CraftOBJ (#27), CraftSQL page index (#33), Config (#13) |
| **Depended by** | Disk Space Management (#32) |

Eviction is orthogonal to the storage lifecycle. The storage lifecycle (Push → Distribution → Repair → Scaling → Degradation) manages how pieces are distributed across the network. Eviction is a separate, local decision: a node decides to drop pieces it no longer wants to hold — due to disk pressure, low reputation score for the CID, or other local policy. The network is not notified; the node simply stops announcing the provider record, and the DHT record expires naturally (48h TTL).

- **Drop trigger:** Disk pressure, low reputation score for the CID, age of last access, or local policy.
- **Network notification:** None required. The node stops announcing the DHT provider record. The DHT record expires naturally after 48h TTL.
- **Reference counting:** A local bookkeeping mechanism to identify orphaned local data. Not a network-wide process.
- **Daily mark-and-sweep:** A local correctness backstop that finds orphaned local CIDs not reachable from any live root. Runs once per day; not network-coordinated.
- **Storage lifecycle independence:** Eviction does not conflict with repair. If a node drops pieces and the network falls below redundancy target, HealthScan on other nodes detects the deficit and repairs in the next cycle.
- **Phase 1 (Mark):** Enumerate all live CIDs from root CIDs held locally. **Phase 2 (Sweep):** Delete CIDs not in live set AND older than safety window (5.5 min). First eviction run deferred 10 minutes post-startup.
- **Eviction Policy Agent:** A network-owned WASM program that defines which local CIDs to evict first — priority ordering, safety windows. Upgradeable by governance without binary changes.

---

### 32. Disk Space Management

| | |
|--|--|
| **What it does** | Monitors disk usage against watermarks. 90%: stop accepting remote data. 95%: begin evicting cached content. 99%: refuse all writes. |
| **Depends on** | CraftOBJ (#27), GC (#31), Config (#13) |
| **Depended by** | CraftOBJ write path |

| Usage Level | Threshold | Action |
|------------|-----------|--------|
| Normal | < 90% | Accept all data. GC on normal schedule. |
| Warning | 90–95% | Stop accepting remote pieces. Trigger GC immediately. |
| Critical | 95–99% | Evict LRU non-authoritative pieces. Stop all inbound transfers. |
| Emergency | > 99% | Refuse all writes except critical DB operations. |

---

## PART F: DATABASE — CraftSQL

### 33. CID-VFS Implementation

| | |
|--|--|
| **What it does** | Custom SQLite VFS mapping page I/O to CID lookups in CraftOBJ. xRead/xWrite/xSync intercepted. xSync is the commit trigger. Journal ops are no-ops. |
| **Depends on** | CraftOBJ (#27), SQLite/libSQL library |
| **Depended by** | All CraftSQL operations (#34–37) |

#### Runtime Behavior

**xRead(page_num):** Translate offset to page number. Check hot LRU cache. Look up CID in page index. Fetch from CraftOBJ. Zero-fill on SHORT_READ (critical per SQLite spec).

**xWrite(page_num, data):** Add to `dirty_pages` set. Buffer in memory. Return `SQLITE_OK` immediately.

**xSync(db_file)** — the commit trigger: Hash all dirty pages (BLAKE3). Write to CraftOBJ. Create new page index. Compute root CID. Atomically update root CID record. Clear `dirty_pages`.

#### Key Parameters

| Parameter | Value |
|-----------|-------|
| Page size | 16 KB (PRAGMA page_size = 16384) |
| Mode | Rollback journal (not WAL) |
| Journal handling | No-op (return SQLITE_OK) |
| Commit trigger | xSync on db file |
| BLAKE3 time per page | ~2µs for 16KB |
| B-tree depth (1M rows) | ~4 levels → 4 CIDs per commit |

#### Offline Page Handling

With extreme distribution, most pages have many providers. Parallel fetch to K providers makes single-node failure irrelevant. The CID-VFS handles the case where fewer than K pieces arrive within the timeout.

- **Parallel fetch:** xRead fetches K pieces in parallel from multiple providers simultaneously.
- **Timeout:** 10 seconds per page fetch. After timeout, evaluate how many pieces arrived.
- **Insufficient pieces:** If fewer than K pieces arrive within timeout, return `SQLITE_IOERR_READ` with extended error code `CRAFTEC_CONTENT_UNAVAILABLE`.
- **Application handling:** CraftStudio surfaces this as a 'content offline' error. Applications are expected to handle this gracefully — the local-first model means offline reads are a normal operating condition.
- **Local page cache:** For critical databases, local page cache ensures reads succeed even when fully offline. This is the local-first principle: data already fetched is always available.

---

### 34. Commit Flow

| | |
|--|--|
| **What it does** | End-to-end transaction commit. Commit point = atomic swap of root CID. RLNC encoding/distribution are off critical path. |
| **Depends on** | CID-VFS (#33), CraftOBJ (#27), RLNC (#28), Batch Writer (#50) |
| **Depended by** | All user-visible write operations |

**Critical Path:** BLAKE3 hash dirty pages → Write to CraftOBJ → Atomically update root CID → Return SQLITE_OK.

**Off Critical Path:** Enqueue RLNC encoding → Enqueue root CID to Pkarr publication → Update metrics.

Batch Writer coalesces small writes (count=256, size=4MB, timer=50ms — see §50).

---

### 35. WAL Elimination / MVCC

| | |
|--|--|
| **What it does** | WAL incompatible with distributed storage. MVCC implemented in CID-VFS: each transaction produces new immutable CID pages. Snapshot isolation is inherent in the CID model — not a separate mechanism. |
| **Depends on** | CID-VFS (#33), Page Cache (#36) |
| **Depended by** | All readers/writers of CraftSQL |

#### Snapshot Isolation via CID Immutability

Snapshot isolation is inherent in the CID model — it requires no additional mechanism.

- **Every commit = new root CID:** Each commit produces a new root CID atomically (Section 34). The root CID IS the database version.
- **Reader pins root CID:** A reader pins the root CID at query start. All page CIDs referenced by that root are immutable content-addressed objects.
- **Writer cannot affect reader:** Even if the writer publishes a new root CID mid-query, the reader continues reading from its pinned root. The old CIDs are not modified — they are immutable.
- **No partial reads:** The root CID IS the snapshot. Either all pages from that root are available, or none are (GC safety window ensures pinned snapshots remain available).
- **No WAL needed:** CID immutability provides the same guarantee as a WAL. There is nothing to roll back — old CIDs remain valid until evicted.

GC safety window ensures snapshot validity for `max_transaction_duration`. A reader's pinned root CID will not be evicted until the safety window expires.

---

### 36. Page Cache

| | |
|--|--|
| **What it does** | Multi-layer caching: hot page LRU (in-process), CID content cache (local disk), network fetch (remote). Bloom filter prevents unnecessary network round trips. |
| **Depends on** | CID-VFS (#33), CraftOBJ (#27) |
| **Depended by** | CID-VFS (#33) read path |

| Layer | Scope | Size | Hit Latency |
|-------|-------|------|-------------|
| Hot LRU | In-process (memory) | 256 MB | ~50ns |
| CID content cache | Local disk (CraftOBJ) | Disk capacity | ~10µs |
| Network fetch | Remote peers | Unlimited | ~10–100ms |

Prefetch: On sequential read patterns, prefetch pages N+1 through N+8 asynchronously.

---

### 37. Root CID Publication

| | |
|--|--|
| **What it does** | Broadcasts new root CID after each commit so readers can discover the latest database version. Uses DHT provider records (per v2.1) + Pkarr for names + SWIM gossip for immediate notification. The database is identified by the owner's Ed25519 identity — only the owner can publish new root CIDs. |
| **Depends on** | Commit Flow (#34), Node Identity (#17), Networking (#21) |
| **Depended by** | Any node reading a remote user's CraftSQL database |

#### Publication Flow

1. Receive `(database_id, new_root_cid)` from commit flow.
2. Write DHT provider record for root CID (48h TTL). This is the primary content-routing mechanism.
3. Build Pkarr record: `{name: "db.{node_id}.craftec", value: root_cid_hex, ttl: 3600}`. Sign with Ed25519.
4. Publish via Pkarr DNS-over-HTTPS.
5. Gossip new root CID to connected peers via SWIM piggybacking (immediate notification).
6. Dedup: if root CID unchanged from last publish, skip.

Publish at most once per 5 seconds for a given database. On reconnect after offline, publish latest root CID only.

---

## PART G: AGENT RUNTIME — CraftCOM

### 38. Distributed Compute Engine

| | |
|--|--|
| **What it does** | General-purpose distributed compute runtime. Embeds Wasmtime (WASI 0.2) for arbitrary workloads: validation, data processing, ML inference, indexing, transformation, attestation. Strict resource limits per agent. |
| **Depends on** | CraftSQL (#33), CraftOBJ (#27), Node Identity (#17), Program Scheduler (#48) |
| **Depended by** | Agent Lifecycle (#39), Host Functions (#40), Attestation (#41) |

**Resource metering (decided):** Wasmtime fuel metering is the implemented mechanism — every agent invocation runs in a fresh `Store` with a fuel budget (default 10 million units, roughly proportional to WASM instruction count). A runaway loop exhausts its fuel and traps deterministically.

**Planned refinement:** a startup calibration benchmark measuring fuel-per-wall-second on the actual host, so fuel limits can be specified as wall-clock budgets (e.g., '500ms CPU budget'). Wasmtime epoch interruption may additionally be layered on later for hard wall-clock deadlines; fuel remains the primary mechanism.

#### Per-Program Memory Limits

Memory limits are defined in whitelist metadata per program, not as a single global cap. These are initial estimates and should be profiled under realistic inputs before launch.

| Parameter | Value |
|-----------|-------|
| Reputation scorer | 64 MB |
| Eviction policy agent | 32 MB |
| Agent load balancer | 16 MB |
| Degradation policy agent | 32 MB |
| Schema migration coordinator | 64 MB |
| Unknown / third-party programs | 256 MB hard ceiling |

#### Relationship to Program Scheduler

CraftCOM provides the WASM runtime substrate (Wasmtime engine, host function bindings, fuel metering, memory sandboxing). The Program Scheduler (kernel-level, Section 48) sits above CraftCOM and manages the lifecycle of long-running network-owned programs that execute on CraftCOM.

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| Kernel | Program Scheduler (#48) | Whitelist management, process keepalive, restart-on-failure, resource enforcement for network-owned programs |
| Runtime | CraftCOM / Wasmtime | WASM compilation, execution sandbox, host function dispatch, fuel accounting |
| Programs | Network-Owned WASM Agents | Policy logic (eviction, reputation, load balancing) — loaded by CID, managed by scheduler, executed by CraftCOM |

The distinction: CraftCOM does not know or care whether a WASM invocation is ephemeral (a one-shot compute task) or persistent (a network-owned program managed by the Program Scheduler). The scheduler is what makes the difference — it is the layer that keeps network-owned programs running continuously, detects crashes, and enforces restart policies.

---

### 39. Agent Lifecycle

| | |
|--|--|
| **What it does** | Complete lifecycle for general-purpose compute tasks: load binary from CraftOBJ by CID, instantiate Wasmtime, execute any workload, collect results, terminate. Agents are ephemeral. |
| **Depends on** | Wasmtime (#38), CraftOBJ (#27), CraftSQL (#33) |
| **Depended by** | Attestation (#41), Any application compute |

**Invocation:** receive request → load/compile WASM → create fresh Store → instantiate → call function → collect return value → terminate → reclaim memory. Recursive invocation allowed up to depth 4.

---

### 40. Host Functions

| | |
|--|--|
| **What it does** | Functions exposed to WASM agents via WIT interfaces: CraftSQL queries, CraftOBJ get/put, Ed25519 signing, HLC clock. All capability-gated. |
| **Depends on** | CraftSQL, CraftOBJ, Node Identity, Clock |
| **Depended by** | Agent execution (#39), Attestation (#41) |

| Host Function | Description | Rate Limit |
|---------------|-------------|------------|
| craft_sql_query | Read-only SQL query, max 10,000 rows | 1000 calls/invocation |
| craft_sql_execute | Write SQL, transactional | 1000 calls/invocation |
| craft_obj_get | Fetch content by CID | 100 calls/invocation |
| craft_obj_put | Store content, max 64 MB per put | 10 calls/invocation |
| craft_sign | Ed25519 sign, returns 64-byte signature | 10 calls/invocation |
| craft_clock | Returns current HLC timestamp | Unlimited |

---

### 41. Attestation Flow

| | |
|--|--|
| **What it does** | k-of-n attestation — one use case of CraftCOM general compute. Two modes: (1) internal attestation uses k independent Ed25519 signatures — no shared secrets, no DKG, no MPC; (2) chain-boundary signing (Solana/Ethereum) uses FROST(Ed25519) with DKG for the boundary signer group only. |
| **Depends on** | Agent Lifecycle (#39), Host Functions (#40), Node Identity (#17), Wire Protocol (#23) |
| **Depended by** | Any subsystem requiring trust validation |

**Internal attestation flow:** Event triggers request → broadcast to n peers → each peer loads agent, validates, signs with its own Ed25519 key → coordinator collects k signatures → verify each (2.3µs) → broadcast combined attestation. No shared secrets, no DKG, no MPC.

**Chain-boundary signing:** FROST(Ed25519) 7-of-10 with DKG for the boundary signer group (~0.9ms per signature). DKG is scoped exclusively to chain-boundary operations — internal attestation never uses it.

---

## PART H: CROSS-CUTTING CONCERNS

### 42. Clock & Time (HLC)

| | |
|--|--|
| **What it does** | Hybrid Logical Clocks for distributed event ordering. Always monotonically increasing. Bounded near NTP time. Rejects >500ms clock skew. |
| **Depends on** | OS system clock, NTP (ambient) |
| **Depended by** | CraftSQL commit ordering, Wire Protocol timestamps, Attestation |

HLC = (physical time, logical counter). Send: `physical = max(wall_clock, HLC.physical)`. Receive: `update physical = max(wall, local, msg)`. Skew rejection: >500ms → reject message. Persist to disk every 100ms.

| Parameter | Value |
|-----------|-------|
| HLC format | 64-bit: 48-bit ms timestamp + 16-bit logical |
| Max clock skew | 500ms (reject above) |
| Persistence interval | 100ms |

---

### 43. Observability & Metrics

| | |
|--|--|
| **What it does** | Prometheus-compatible metrics, structured JSON logging, OpenTelemetry tracing. Every subsystem emits metrics. |
| **Depends on** | Clock (#42), Event Bus (#52) |
| **Depended by** | All subsystems |

| Metric Name | Type | Subsystem |
|-------------|------|-----------|
| craftec_connections_total | Gauge | Connection Pool |
| craftec_pieces_stored_total | Counter | CraftOBJ |
| craftec_disk_usage_pct | Gauge | Disk Mgmt |
| craftec_commit_latency_ms | Histogram | CraftSQL |
| craftec_page_cache_hit_rate | Gauge | Page Cache |
| craftec_rlnc_encode_latency_ms | Histogram | RLNC |
| craftec_agent_invocations_total | Counter | CraftCOM |
| craftec_open_file_descriptors | Gauge | Resource Mgmt |
| craftec_corruption_detected_total | Counter | CraftOBJ |
| craftec_peer_ban_events_total | Counter | Reputation |

---

### 44. Resource Management

| | |
|--|--|
| **What it does** | Memory budgets per subsystem, file descriptor limits, CPU scheduling between foreground and background work. |
| **Depends on** | OS (setrlimit, /proc/self/fd), Config (#13) |
| **Depended by** | All subsystems |

| Subsystem | Memory Budget |
|-----------|--------------|
| Hot page LRU (CraftSQL) | 256 MB (16,384 × 16 KB pages) |
| CraftOBJ bloom filter + metadata | 64 MB |
| RLNC encoding buffers | 256 MB |
| Wasmtime agents | 256 MB × count (max 4) = 1,024 MB |
| Connection state (256 conns) | ~128 MB |
| OS + Rust runtime + misc | ~512 MB |
| Reserved headroom | ~512 MB |
| **Total (worst case)** | **~2.75 GB** |

---

### 45. Security Hardening

| | |
|--|--|
| **What it does** | Input validation on all messages, size limits, per-peer rate limiting, Slow Loris defense, amplification prevention, replay protection. |
| **Depends on** | Wire Protocol (#23), Admission (#20), Reputation (#19) |
| **Depended by** | All subsystems receiving external input |

| Message Type | Rate Limit |
|-------------|------------|
| WANT_CID | 100/second per peer |
| SWIM_PING | 10/second per peer |
| ATTEST_BROADCAST | 5/second per peer |
| Any message (total) | 1000/second per peer |
| New connections | 30/second (global outbound) |
| Handshakes in progress | 50 simultaneously |

---

### 46. Testing Strategy

| | |
|--|--|
| **What it does** | Three-layer testing: Deterministic Simulation Testing (DST), property-based testing, integration tests. DST is the highest-value investment. |
| **Depends on** | Dependency injection of all I/O |
| **Depended by** | Entire codebase |

**DST:** Single-threaded pseudo-concurrency, injected I/O, seeded PRNG, injectable time. Target: 2 millennia of simulated time per CPU-day.

**Property Tests:** CAS invariant (`get(put(data)) == data`), RLNC invariant (any K pieces decode), HLC monotonicity.

**Integration:** 5-node cluster + 1 nemesis (Jepsen-style).

---

### 47. Error Classification & Handling

| | |
|--|--|
| **What it does** | Classifies errors into transient (retry) vs permanent (fail). Per-peer circuit breakers. Dead-letter queues. |
| **Depends on** | All subsystems, Reputation (#19) |
| **Depended by** | All subsystems |

| Error Class | Examples | Retry? | Policy |
|-------------|----------|--------|--------|
| Transient Network | Timeout, EAGAIN, connection reset | Yes | Exponential backoff, max 5 attempts |
| Transient Storage | ENOENT (racing with GC) | Yes | Linear backoff, max 3 attempts |
| Permanent Network | Protocol version mismatch, ban | No | Fail immediately, try alternate peer |
| Permanent Storage | Corrupt CID (hash mismatch) | No | Delete local copy, re-fetch from network |
| Application Error | SQL constraint violation | No | Return to caller |
| Resource Exhaustion | Disk full, OOM | Yes (after GC) | Trigger resource cleanup, retry once |

**Circuit breakers:** CLOSED → OPEN (after 5 failures in 30s) → HALF_OPEN (after 60s) → CLOSED (1 success) or OPEN (1 failure).

---

## PART I: COORDINATION & SCHEDULING

### 48. Task Scheduler & Program Lifecycle

| | |
|--|--|
| **What it does** | KERNEL-LEVEL (Program Lifecycle portion). Two distinct responsibilities: (1) Task Scheduler — priority queue with EDF scheduling for background jobs. (2) Program Scheduler — maintains whitelist of network-owned program CIDs, keeps them running, restarts on failure, enforces resource limits. Like the Linux kernel scheduler: does not make policy, just keeps programs alive. |
| **Depends on** | Event Bus (#52), Tokio runtime, CraftCOM (#38), CraftOBJ (#27) |
| **Depended by** | Background Job Coordinator (#51), GC, Health Scanner, RLNC, Network-Owned Programs |

#### Task Scheduler

Priority queue with Earliest-Deadline-First (EDF) scheduling for background infrastructure tasks:

| Priority | Tasks | Max Wait |
|----------|-------|---------|
| HIGH | User SQL queries, CID fetches, SWIM pings | Never promoted |
| NORMAL | RLNC encoding, piece distribution | — |
| LOW | GC, health scan, recode background | 60s → promoted to NORMAL |

#### Program Scheduler (Kernel-Level Program Lifecycle)

The Program Scheduler is the kernel-level component responsible for the lifecycle of all network-owned programs. It is minimal by design: it does not implement policy, it enforces that policy programs stay running.

- **Whitelist management:** Maintains a set of trusted WASM CIDs representing current network-owned program versions. Whitelist updates are a governance action.
- **Keepalive:** Monitors each network-owned program. If a program exits (success or crash), restarts it with exponential backoff (base 1s, max 60s).
- **Restart-on-failure:** Crash detection via Wasmtime trap. Log crash reason, increment failure counter, apply backoff, reload WASM from CraftOBJ by CID.
- **Resource limits:** Enforces per-program fuel budget, memory cap (see Section 38 for per-program limits), and wall-clock time limits. A misbehaving program cannot starve the node.
- **CID-based identity:** Each network-owned program is identified by its WASM CID. Upgrade = update canonical CID in whitelist. Old program gracefully terminated, new CID loaded.
- **Startup ordering:** Program Scheduler starts after CraftCOM is ready. Network-owned programs start after the DHT and SWIM are healthy, ensuring their host function calls succeed.
- **Crash loop quarantine:** After 10 consecutive crash restarts within a window, the program enters QUARANTINED state. Restart attempts stop. Node falls back to hardcoded safe defaults for that function (reputation: neutral 0.5; eviction: oldest-first FIFO; load balancing: round-robin). Quarantine clears automatically when a new WASM CID is published to the whitelist (governance action). Node emits metric: `craftec_program_quarantined{program="..."} = 1`.

| Concern | Handled By | Mechanism |
|---------|-----------|-----------|
| Program crash | Program Scheduler | Trap detection → restart with backoff |
| Resource exhaustion | Program Scheduler + CraftCOM | Fuel limit → graceful trap; memory cap → WASM OOM |
| Program upgrade | Governance → Program Scheduler | Update whitelist CID → scheduler hot-swaps |
| Infinite restart loop | Program Scheduler | Max restart counter (default 10) → quarantine, alert |
| Node restart | Startup sequence | Program Scheduler reloads whitelist from CraftOBJ |

#### Governance Model

The whitelist of trusted WASM program CIDs is governed by a two-phase model, analogous to the Bitcoin and Solana upgrade paths.

- **Phase 1 (Launch):** Central authority — Craftec maintainer Ed25519 key signs whitelist updates. Nodes only accept whitelist updates bearing a valid governance signature. The whitelist itself is stored as a CraftOBJ CID, announced via DHT.
- **Phase 2 (Mature):** k-of-n multisig — a set of trusted maintainers (e.g., 5-of-7) must co-sign whitelist updates. Increases decentralization and resistance to single-key compromise.
- **Phase transition:** The transition from Phase 1 to Phase 2 is itself a governance action signed by the Phase 1 authority. No node binary change required.
- **Whitelist storage:** The whitelist is a CraftOBJ CID (content-addressed). Nodes discover the latest whitelist version via DHT. The governance signature authenticates the whitelist content.

---

### 49. Request Coalescing / Singleflight

| | |
|--|--|
| **What it does** | Deduplicates concurrent CID fetches. 100 peers requesting same CID → one network fetch, all receive result. |
| **Depends on** | Connection Lifecycle (#21), CAS (#27) |
| **Depended by** | Read Path (#54), Page Cache (#36) |

`DashMap<CID, Weak<broadcast::Sender>>`. First caller executes fetch; subscribers await via broadcast channel. Cache checked before entering singleflight.

---

### 50. Batch Writer

| | |
|--|--|
| **What it does** | Coalesces multiple small writes. Three triggers: count (256 items), size (4 MB), timer (50ms). Whichever fires first. |
| **Depends on** | CAS (#27), Task Scheduler (#48) |
| **Depended by** | Commit Flow (#34) |

Write coalescing: multiple writes to same CID within batch window deduplicate (last write wins). Nagle window for network messages: max 5ms, max 16 messages. Input channel bounded at 512.

---

### 51. Background Job Coordinator

| | |
|--|--|
| **What it does** | Coordinates RLNC encoding, piece distribution, local eviction, and health scanning. Enforces prioritization so they do not compete destructively. |
| **Depends on** | Task Scheduler (#48), Health Scanner (#30), Local Eviction (#31), RLNC (#28), Distribution (#29) |
| **Depended by** | Node-level resource management |

| Priority | Job | Rationale |
|----------|-----|-----------|
| 1 (Highest) | Repair / Recode | Data durability at risk |
| 2 | RLNC Encoding | New data needs redundancy quickly |
| 3 | Piece Distribution | Encoded data must reach peers |
| 4 | Health Scanning | Periodic — can tolerate delay |
| 5 (Lowest) | Local Eviction | No urgency |

---

### 52. Event Bus

| | |
|--|--|
| **What it does** | Internal pub/sub for cross-subsystem communication. All channels are bounded. broadcast for fan-out, mpsc for directed events. |
| **Depends on** | Tokio runtime |
| **Depended by** | Background Job Coordinator, Task Scheduler, Observability |

| Event | Type | Capacity | Producers → Consumers |
|-------|------|----------|----------------------|
| CID_WRITTEN | broadcast | 256 | CAS → RLNC, GC, Distribution |
| PAGE_COMMITTED | broadcast | 256 | Commit → Pkarr, Observability |
| PEER_CONNECTED | broadcast | 64 | Connection → SWIM, Reputation |
| PEER_DISCONNECTED | broadcast | 64 | Connection → SWIM, Pool |
| REPAIR_NEEDED | mpsc | 128 | Health Scanner → Job Coordinator |
| DISK_WATERMARK_HIT | broadcast | 8 | Disk Mgr → GC, Job Coordinator |
| SHUTDOWN_SIGNAL | broadcast | 1 | Process signal → all subsystems |

---

## PART J: END-TO-END DATA FLOWS

### 53. Write Path

**User Write → CraftSQL → CID-VFS → CraftOBJ → RLNC → Distribute → DHT Announce**

| Step | Operation | Mode |
|------|-----------|------|
| 1 | User issues SQL statement (INSERT/UPDATE/DELETE). CraftSQL begins transaction. | Sync |
| 2 | SQLite modifies pages. CID-VFS accumulates dirty page map. | Sync |
| 3 | xSync triggers commit. | Sync |
| 4 | BLAKE3 hash all dirty pages (~2µs per 16KB page). | Sync |
| 5 | Write pages to CraftOBJ CAS (local store first, no network). | Sync |
| 6 | Atomically update page index root CID (commit point). | Sync |
| 7 | Return SQLITE_OK to SQLite. User call returns. | Sync |
| 8 | Enqueue RLNC encoding via Event Bus. | Async |
| 9 | RLNC encode each generation. n = k × ceil(2.0 + 16/k) coded pieces. | Async |
| 10 | Distribute coded pieces to peers via QUIC streams. | Async |
| 11 | Announce new root CID via DHT provider records + Pkarr + SWIM gossip. | Async |

---

### 54. Read Path

**User Query → CraftSQL → CID-VFS → Cache → DHT Lookup → Peer Fetch → Verify → Return**

| Step | Operation |
|------|-----------|
| 1 | User issues SQL query. Snapshot isolation: pin current root CID. |
| 2 | SQLite requests page N via xRead. |
| 3 | Check hot page LRU cache (~0.013ms p50). HIT → return immediately. |
| 4 | Resolve page CID from page index. |
| 5 | Check local CID content cache (CraftOBJ). HIT → return. |
| 6 | Check Bloom filter before network fetch. |
| 7 | Singleflight: check if fetch already in flight for this CID. |
| 8 | Query DHT with `get_providers(cid)` for content discovery. Also check SWIM-gossipped provider hints. |
| 9 | Select provider peer (high reliability, low latency). Fetch via QUIC stream (10s timeout). |
| 10 | Verify `BLAKE3(received_data) == CID`. Mismatch → `ban_score++`. |
| 11 | Cache and return. Prefetch adjacent pages if sequential scan detected. |

---

### 55. Repair Path

**Health Scan → Detect Under-Replication → Local Recode → Distribute**

The repair path runs entirely without a coordinator and without decoding to original data — the key RLNC advantage. A node holding 2+ coded pieces can produce new valid coded pieces by recoding. Repair bandwidth: only local pieces used per repair (vs Reed-Solomon requiring K pieces fetched from the network).

| Step | Operation |
|------|-----------|
| 1 | Health scanner runs (every 300s / 5 minutes, or on PEER_DISCONNECTED). |
| 2 | Query piece availability for each CID. |
| 3 | Compute available piece count. If < K → DATA AT RISK. If < n → under-replicated. |
| 4 | Verify this node holds ≥2 local coded pieces. Recode from local pieces (no network fetch). Determine if this node is in the top-N elected repairers (where N = deficit). If not elected, skip. |
| 5 | Verify HomMAC tags on local pieces. |
| 6 | Recode: fresh random GF(2⁸) coefficients. NO DECODE REQUIRED. |
| 7 | Distribute new coded piece with priority: (1) peers holding exactly 1 piece (so they reach ≥2 and become repair-eligible), (2) peers holding 0 pieces. |
| 8 | Update piece availability map. Emit REPAIR_COMPLETE if target met. |

---

### 56. Attestation Path

**Event → Agent Load → Validate → Sign → Broadcast → k-of-n Collect**

| Step | Operation |
|------|-----------|
| 1 | Event written to CraftSQL as pending attestation record. |
| 2 | Select n CraftCOM agents (independent keys, different peers, subnet diversity). |
| 3 | Each agent loads WASM bytecode from CraftOBJ. Wasmtime instantiates. |
| 4 | Agent reads CraftSQL snapshot (snapshot isolation ensures same data for all agents). |
| 5 | Agent validates event. Reaches VALID or INVALID decision. |
| 6 | Agent calls `craft_sign(hash(event_id || decision || snapshot_cid))`. |
| 7 | Signature broadcast via ATTEST_BROADCAST message. |
| 8 | Coordinator collects k distinct valid signatures. Ed25519 verify: ~2.3µs each. |
| 9 | Write finalized attestation to CraftSQL. |
| 10 | Optional: chain boundary via FROST (Solana) or CGGMP24 (Ethereum). |

---

### 57. Join Path

**New Node → Bootstrap → Discover → SWIM Join → Announce → Participate**

| Step | Operation |
|------|-----------|
| 1 | Binary executes. First-run check: generate Ed25519 keypair, NodeID = Ed25519 public key (iroh convention), create directories, init schema. |
| 2 | Write `node.lock` sentinel file. If already present: dirty shutdown → integrity check. |
| 3 | Load config, validate, raise FD limit to 65535. |
| 4 | Initialize subsystems in dependency order (CraftOBJ, CraftSQL, RLNC, Wasmtime, iroh, SWIM, Event Bus, Scheduler). |
| 5 | Bootstrap: connect to iroh relay server (primary). Query DNS seeds as fallback. Hardcoded IPs as last resort. Connect to 8–20 peers. |
| 6 | TLS 1.3 handshakes via QUIC, ALPN negotiation, capability exchange. |
| 7 | SWIM JOIN message to known peers. Membership table updated across network. |
| 8 | Admission checks: verify NodeID = Ed25519 pubkey, subnet diversity, ban list. |
| 9 | Announce identity via Pkarr DNS record. Re-announce every 22 hours. |
| 10 | Request peer lists. Connect to best 20 by diversity/latency/reliability. |
| 11 | Storage bootstrap: check CID availability, distribute under-replicated pieces. |
| 12 | Full participation: accepting connections, serving CIDs, hosting pieces, running agents. |

---

## APPENDICES

### 58. Recommended Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Rust | All core components — memory safety, performance, cross-compilation |
| Async Runtime | Tokio | All async I/O — aligned with iroh |
| Transport | iroh (QUIC/quinn) | Peer connections, NAT traversal, relay fallback |
| Membership | SWIM | Cluster membership, failure detection |
| Identity | Ed25519 + Pkarr | Self-sovereign identity, DNS-compatible discovery |
| Serialization | postcard (serde) | All wire protocols — compact, deterministic, zero-copy |
| Hashing | BLAKE3 | CIDs, piece IDs, Merkle trees, vtag challenge vectors |
| Erasure Coding | RLNC over GF(2⁸) | Rateless repair, progressive decoding |
| Database | CraftSQL (SQLite-derived) | Local-first SQL, single-writer-per-identity |
| Client Framework | CraftStudio | Cross-platform: Tauri, UniFFI, WASM |
| Distributed Compute | CraftCOM | General-purpose WASM agents (CPU/GPU); attestation, inference, validation |
| Content Routing | iroh Kademlia DHT | CID→provider resolution via DHT provider records |
| Protocol Composition | iroh Endpoint + ALPN | Multiple services on one P2P layer |
| Video Streaming | iroh-roq / iroh-live | Real-time media transport |
| Pub-Sub | iroh-gossip | Coordination, app events, ML training sync |
| Ephemeral Transfers | iroh-blobs | Large one-off data transfers |

---

### 59. Implementation Roadmap

The implementation is organized into four phases, each producing a working system.

#### Phase 1: Foundation (Months 1–3)

- iroh endpoint setup, ALPN registration, relay configuration
- Ed25519 key generation and Pkarr identity publishing
- SWIM membership protocol
- BLAKE3 hashing integration
- postcard serialization
- Basic CraftOBJ: local store/retrieve with RLNC coding
- GF(2⁸) arithmetic with SIMD optimizations

#### Phase 2: Distribution (Months 4–6)

- iroh DHT integration: provider records, content discovery
- CraftOBJ distribution engine: Push, Distribution, Repair, Scaling
- HealthScan: Phase 0 + Phase 1
- PDP challenge/response with StorageReceipts
- Multi-node fetch with progressive decoding
- Verification tags: HomMAC + LHS

#### Phase 3: Database (Months 7–9)

- CraftSQL engine: SQLite-compatible query execution
- Single-writer replication: root CID publication, reader sync
- Schema management: schema_version field, additive migrations (no version vectors — ordering via HLC)
- Page-level storage integration with CraftOBJ
- Sync engine: background replication
- Live queries: `subscribe(query) → Stream`

#### Phase 4: Client & Attestation (Months 10–12)

- CraftStudio framework: high-level API
- Tauri (desktop), UniFFI (mobile), WASM (web)
- CraftCOM attestation: agent selection, k-of-n quorum
- End-to-end integration testing

---

### 60. Key Risks & Mitigations

| Risk | Impact | Mitigation | Residual |
|------|--------|-----------|---------|
| RLNC decoding at scale | Low | Decoding is client-side only — storage nodes never decode. SIMD GF(2⁸); K=32; progressive decoding on client. | Low |
| iroh library maturity | Medium | Active development; modular architecture | Low |
| CraftSQL single-writer sync | Low | Single-writer model eliminates merge conflicts; trivial consistency | Low |
| Sybil attacks on storage | High | HealthScan Phase 1 vtag; PDP; CraftCOM penalties | Medium |
| NAT traversal reliability | Low | Distributed relay mesh — every publicly-reachable node is a relay; no single relay failure can partition the network | Low |
| Cross-platform consistency | Medium | Single Rust codebase; platform-specific UI only | Low |
| DHT maturity | Medium | Experimental (Sep 2025); tracker-based fallback | Low |
| Hash function migration | Low | BLAKE3 is current default (raw 32-byte hashes). Future: adopt multihash format ([hash_code][length][hash], +2 bytes) for clean migration path. Not adopted in v3.4 to keep implementation simple. | Low |

#### Network Partition Recovery

Craftec's extreme distribution model provides inherent partition tolerance. With 3× effective erasure redundancy (K=32, n = 32 × ceil(2.5) = 96 total pieces) distributed globally:

- **50/50 split survivability:** A 50/50 network partition leaves ~48 pieces per side — well above K=32. Both halves can independently reconstruct any CID.
- **Independent repair:** Both network halves repair independently using top-N natural selection (each of the top-N elected nodes produces 1 piece per CID per cycle, where N = deficit). No cross-partition coordination needed.
- **Partition rejoin:** On rejoin, Degradation naturally sheds excess pieces — again 1 per node per cycle. No storms, no coordination.
- **Guarantee:** The combination of extreme distribution + natural selection coordinator + async 1-piece-per-elected-node-per-cycle guarantees graceful partition handling with no manual intervention.

---

### 61. Sources

The following sources informed the design decisions documented in this foundation.

| Resource | URL | Notes |
|----------|-----|-------|
| iroh | https://iroh.computer/ | P2P networking library |
| iroh documentation | https://docs.iroh.computer/ | API reference and protocol docs |
| iroh DHT blog | https://www.iroh.computer/blog/lets-write-a-dht-1 | Kademlia DHT design |
| iroh content discovery | https://www.iroh.computer/blog/iroh-content-discovery | Content discovery mechanisms |
| iroh-live | https://github.com/n0-computer/iroh-live | Video/audio streaming over iroh |
| iroh-tor | https://github.com/n0-computer/iroh-tor | Onion routing experiment |
| iroh-blobs | https://docs.iroh.computer/protocols/blobs | Content-addressed blob transfers |
| Nous Research + iroh | https://www.iroh.computer/solutions/nous | Decentralized AI training |
| Delta Chat + iroh | https://delta.chat/en/2024-11-20-webxdc-realtime | P2P messaging |
| QUIC RFC 9000 | https://www.rfc-editor.org/rfc/rfc9000 | QUIC transport specification |
| SWIM paper | https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf | Membership protocol |
| Pkarr | https://pkarr.org/ | Public Key Addressable Resource Records |
| BLAKE3 | https://github.com/BLAKE3-team/BLAKE3 | Cryptographic hash function |
| postcard | https://docs.rs/postcard/latest/postcard/ | Binary serialization for serde |
| RLNC | https://en.wikipedia.org/wiki/Linear_network_coding | Erasure coding theory |
| SQLite | https://www.sqlite.org/ | Database engine |
| Tauri | https://tauri.app/ | Desktop application framework |
| UniFFI | https://mozilla.github.io/uniffi-rs/ | Rust FFI bindings for mobile |
| Tokio | https://tokio.rs/ | Async runtime for Rust |
| quinn | https://github.com/quinn-rs/quinn | Rust QUIC implementation |

---

### 62. Amendments — v3.7 Decision Record (2026-07-02)

Resolved in the pre-build design review (`.claude/design-reviews/2026-07-02T000000Z.md`). Where these amendments conflict with v3.6 body text, the amendments win. Implementation detail lives in `CRAFTOBJ_DESIGN.md` v2.0.

#### 62.1 Overrides to v3.6 body text

| Section affected | v3.6 said | v3.7 amendment |
|---|---|---|
| §4, §28 (vtags) | "HomMAC + LHS tags" with unspecified key management | **Public null-space vtags**: publisher samples L=8 random null-space vectors of each source generation, published as a content-addressed vtags CID. Any node verifies any piece (including recoded repair pieces) with 8 GF(2⁸) dot products. No secrets, ingest-time rejection, 2⁻⁶⁴ forgery bound. The `hommac_tag` field in CodedPiece is replaced by verification against the vtags blob. |
| §30 (Natural Selection Coordinator) | Rank by uptime, reputation, NodeID | **Rendezvous hashing**: rank = BLAKE3(NodeID ‖ CID ‖ epoch), epoch = 1h of HLC wall time; candidates = probe-verified holders of ≥2 pieces, membership-alive. Uptime/reputation ranking is void — reputation is a local score (§19) and can never produce identical rankings across nodes. |
| §30, §55 (HealthScan counting) | Phase 0 counts DHT provider records | **Live batched probes**: routing records supply the candidate list only. Each cycle the scanner sends one AVAILABILITY_PROBE per candidate provider (batching all scanned CIDs), receives held piece counts + coding vectors, and computes verified counts. Eliminates the 48h eviction blind spot. New wire messages: AVAILABILITY_PROBE (0x0041), AVAILABILITY_ACK (0x0042). |
| §30, §6 (PDP) | PDP specified but not wired into repair decisions | **PDP-weighted counts**: each probe embeds a PDP coefficient cross-check challenge for one random (CID, piece) per provider. Providers failing a challenge are discounted from all availability counts that cycle, penalized in reputation, and paroled. Repair triggers on verified counts only. |
| §29 (Piece Distribution) | Peers implicitly accept pushed pieces | **Admission = quotas + reciprocity**: per-publisher accepted-bytes quota (default 1 GB) + local give/take ledger; reciprocal peers win under disk pressure. New ack statuses: QuotaExceeded. |
| §3 (SWIM) | Full-membership SWIM, claims 1M-node scale | **Partial-view membership from the start** (HyParView-style active/passive views, ~log N peers); SWIM-style probing and dissemination run over the active view. Removes the full-table scale ceiling; failure detection guarantees are per-view. |
| §6, §60 (DHT) | iroh DHT with undesigned "tracker fallback" | **ContentRouting trait** (announce/resolve/withdraw/records) with two impls: a simple tracker service (built first; dev/test/early-net) and the iroh Kademlia DHT (adopted behind the same trait when mature). Nodes may run both; resolve = union. |
| §23, §42 (clock skew) | Reject any message with >500ms HLC skew | **Warn-and-accept** for ordinary messages (clamp HLC merge, emit metric); strict 500ms rejection retained for attestation and SIGNED_WRITE only. |

#### 62.2 Confirmations (v3.6 stands, conflicts elsewhere resolved in its favor)

- **Erasure-code everything** — no replication tier for small objects. VISION.md's "replicate small objects / SQLite pages" guidance is superseded. K=8 generations for 16 KB pages, K=32 for files; revisit K=4 for hot small DBs only if Phase 3 measurements demand it.
- **Trust model** — k-of-n independent Ed25519 signatures for internal attestation (§41 as written); FROST DKG only at chain boundaries. VISION.md's CraftSEC MPC/PDK model is superseded.
- **Protocol-driven storage lifecycle** — no pin/unpin (confirmed vs old CraftOBJ v1 design).

#### 62.3 Repository decision

Greenfield implementation lives in `craftec/zephcraft/` (**ZephCraft**, renamed from `node/` 2026-07-03; crates use the `zeph-` prefix, node binary is `zeph`) (plain workspace directory, not submodules). Crate layout and milestones: see `CRAFTOBJ_DESIGN.md` v2.0. The archived libp2p implementation (`craft-ec/*-archive`) is reference-only; per the 2026-07-02 decision the rebuild is 100% greenfield (no code ported).

---

## Changelog: v3.5 → v3.6

**Version 3.6 — June 2026**

This version fixes the SWIM dissemination layer (§3, §23) after a scalability analysis showed Suspect/Dead updates could be starved out of piggyback gossip in clusters larger than ~16 alive nodes, degrading failure detection from O(log n) rounds to O(n) direct-probe discovery.

### Changes

- **S1 (§3)** — Gossip dissemination rewritten from "slice of the full member table, Alive-sorted first" to a proper epidemic gossip buffer: every membership state change is enqueued as an update with a transmit counter; a newer change for the same node replaces the older entry and resets its counter. Each update is piggybacked λ × ceil(log₂ n) times (λ=3) before being dropped from the buffer.
- **S2 (§3)** — Piggyback selection priority: Suspect/Dead updates are always selected before Alive updates, then fewest-transmits-first. This guarantees failure evidence propagates epidemically regardless of cluster size.
- **S3 (§3, §23)** — Push-pull anti-entropy sync added: every 30s each node exchanges a capped membership snapshot (own Alive entry first, ≤512 entries, random-sampled if larger) with one random alive peer via the new `SWIM_SYNC` (0x0107) / `SWIM_SYNC_ACK` (0x0108) messages. Merges follow incarnation-domination rules; resulting state changes re-enter the gossip buffer. This bounds worst-case convergence even under message loss.
- **S4 (§3)** — `SWIM_JOIN` now replies with a full capped membership snapshot (`SWIM_SYNC_ACK`), giving joiners one-round-trip convergence instead of learning the cluster incrementally via piggyback.
- **S5 (§3)** — Probe target selection changed from random-each-tick to a stable shuffled probe ring: every alive member is probed exactly once per pass, then the ring is reshuffled. This bounds worst-case time-to-first-probe for any member (the SWIM paper's round-robin refinement).
- **S6 (§23)** — Message table: added 0x0107 SWIM_SYNC and 0x0108 SWIM_SYNC_ACK (appended at the end of the wire enum to preserve postcard variant-index compatibility).

Spec updates: `domain_networking.toml` (gossip_retransmit_factor=3, push_pull_sync_interval_secs=30, max_sync_members=512, formula gossip_transmit_budget), `actions.toml` (push_pull_sync), `feature_membership.toml` (gossip_disseminate flow rewritten, push_pull_sync flow added).

---

## Changelog: v3.4 → v3.5

**Version 3.5 — June 2026**

This version aligns the foundation doc with the canonical `.specs/` TOML system and the implemented code, fixing stale constants, two internal contradictions, and one formula error identified in the June 2026 doc-vs-implementation audit (`.claude/audit-gaps.md`). No design decisions were changed except where the doc had drifted from the already-canonical spec values.

### Corrections

- **C1 (§1, §4, §28, §53, §60)** — Redundancy formula corrected throughout: target piece count is `n = k × ceil(2.0 + 16/k)` (K=32→96, K=16→48, K=8→32), matching `.specs/domain_rlnc.toml` and the health scanner. The old prose used the bare factor (`2.0 + 16/k` → "80 pieces for K=32") and §60's partition math was computed from it (~40/side → corrected to ~48/side).
- **C2 (§4)** — Removed the self-contradictory "Initial parity ~1.25x (40 total)" tier; the encoder produces the full target count n at publish.
- **C3 (§3)** — SWIM suspicion timeout corrected from 1.5s (3 rounds) to 5s (10 rounds), matching §18 and `DEFAULT_SUSPECT_TIMEOUT`.
- **C4 (§23)** — Wire frame layout corrected from the 9-byte `[type|version|len]` header to the implemented 17-byte v1 header `[type:4 | version:1 | hlc:8 | len:4]`. The HLC field the old layout omitted is what makes the ±30s replay window possible.
- **C5 (§23)** — Message type table replaced with the implemented tag set (0x0001 PING … 0x0201 RPC_RESPONSE). HELLO, DISCONNECT, and ATTEST_BROADCAST marked as planned/not yet implemented.
- **C6 (§34)** — Batch writer constants corrected to count=256 / size=4MB, matching §50 and code.
- **C7 (§44)** — Hot page LRU corrected to 256 MB (16,384 × 16 KB pages), matching §27, §36, and code; total row added (~2.75 GB worst case).
- **C8 (§13, §20, §24, §25, §44)** — Max connections corrected from 200 to 256 everywhere, matching `DEFAULT_MAX_CONNECTIONS`.
- **C9 (§4)** — vtag false-positive rate corrected from (1/256)⁴ ≈ 4×10⁻¹⁰ to (1/256)⁸ = 2⁻⁶⁴, consistent with §28's L=8 tags and `.specs` `hommac_security_bits = 64`.
- **C10 (§5)** — SQL page coding corrected from "k=1 (single page), 18x" to K=8 page generations (n=32 pieces per generation), matching §28 and `.specs` `k_sql_pages = 8`.
- **C11 (§4)** — `PieceHeader` pseudo-struct replaced with the implemented `CodedPiece` (piece_id, cid, k, coding_vector, data, hommac_tag). `piece_id = BLAKE3(coding_vector ‖ data)` — not BLAKE3(coefficients) — here and in the §6 StorageReceipt.
- **C12 (§27)** — Bloom filter FP rate corrected from 0.1% to 1% (`DEFAULT_FP_RATE = 0.01`).
- **C13 (§38)** — WASM resource metering decided: fuel metering (implemented, default 10M units) is primary; wall-clock calibration and epoch interruption noted as planned refinements. Removes the previously undecided "Alternative:".
- **C14 (§29)** — Added clarifying note: distribution priority (0-piece holders first) and repair-path priority (1-piece holders first, §55) are intentionally different orderings.
- **C15 (§59)** — Roadmap "version vectors" leftover replaced with schema_version/HLC wording.

---

## Changelog: v3.3 → v3.4

**Version 3.4 — March 2026**

This version corrects seven Reed-Solomon / BitTorrent concepts that were incorrectly applied to the RLNC-based design. No design decisions were changed; only erroneous terminology and incorrect algorithmic descriptions were fixed to accurately reflect the RLNC properties the system already relies on.

### Summary of Incorrect Concepts Removed

| Concept | Origin | RLNC Reality |
|---------|--------|--------------|
| Rarest-first | BitTorrent | Every RLNC piece is unique — no rarity concept |
| End-game mode | BitTorrent | Any new piece helps any peer — no "last pieces" problem |
| Piece index/rank | Reed-Solomon | RLNC pieces have coefficient vectors, not indices |
| Batch N pieces per peer | RS distribution | Distribute n = k × ceil(2+16/k) total, ≥1 per peer |
| Fetch pieces for repair | RS repair | Recode from ≥2 LOCAL pieces, no network fetch |
| Single coordinator | Centralised repair | Top-N parallel repair (N = deficit) |
| ≥1 piece eligibility | — | ≥2 pieces required to recode |

### Applied Errata

#### §29 Piece Distribution

- **E1** — Replaced "Rarest-first selection" with RLNC-correct distribution priority: non-holders first (peers with 0 pieces), then under-holders (peers with 1 piece, to bring them to ≥2 for future repair eligibility). Rationale: RLNC coded pieces are ALL unique — each has a random GF(2⁸) coefficient vector. There is no concept of "rare" vs "common" pieces.
- **E2** — Removed "End-game mode" entirely. Rationale: End-game mode optimises the "last pieces" problem in RS/BitTorrent where specific missing pieces must be found. In RLNC, any new coded piece is useful to any peer — there are no "last 2" special pieces.
- **E3** — Replaced "Batch 8 pieces per peer per round" with "Distribute n coded pieces across ≥K distinct peers. Target: each peer receives at least 1 piece, with enough diversity for any K peers to reconstruct. 10s timeout per peer."

#### §30 Health Scanning & Repair

- **E4** — Replaced `piece_rank` with `available_count` throughout. Rationale: RLNC pieces don't have ranks or indices; the scanner checks the count of available pieces.
- **E5** — Changed "fetch ≥2 coded pieces" to "recode from ≥2 locally-held coded pieces with fresh GF(2⁸) coefficients". Rationale: A repair node already holds ≥2 pieces locally (eligibility requirement). Recoding happens from local pieces only — no network fetch needed.
- **E6** — Changed scan scope from "this node holds ≥1 piece" to "this node holds ≥2 pieces — the minimum required to recode". Rationale: Nodes with only 1 piece cannot participate in repair; scanning those CIDs wastes cycles.
- **E7** — Changed candidate pool from "nodes holding ≥1 piece" to "nodes holding ≥2 pieces". Rationale: A node with only 1 piece cannot recode and therefore cannot be a repair coordinator.
- **E8** — Changed "Top-ranked node acts as repair coordinator" to "Top-N nodes act: When deficit = N pieces needed, the top-N ranked nodes each independently produce 1 new coded piece per cycle." Rationale: With a single coordinator, repair of large deficits takes N cycles instead of 1.
- **E9** — Clarified "1 piece per CID per cycle" to "1 piece per elected node per CID per cycle: Each of the top-N elected nodes produces 1 piece per CID per cycle. Total repair rate = min(N, deficit) pieces per cycle."

#### §55 Repair Path

- **E10** — Changed flow header from "Health Scan → Detect Under-Replication → Fetch Pieces → Recode → Distribute" to "Health Scan → Detect Under-Replication → Local Recode → Distribute".
- **E11** — Changed "Compute piece rank" to "Compute available piece count" in step 3.
- **E12** — Replaced step 4 "Select 2+ peers holding coded pieces. Fetch via QUIC." with "Verify this node holds ≥2 local coded pieces. Recode from local pieces (no network fetch)." Added: "Determine if this node is in the top-N elected repairers (where N = deficit). If not elected, skip."
- **E13** — Added distribution priority to step 7: "Distribute new coded piece with priority: (1) peers holding exactly 1 piece (so they reach ≥2 and become repair-eligible), (2) peers holding 0 pieces."

#### §60 Network Partition Recovery

- **E14** — Changed "1 piece per node per cycle" to "each of the top-N elected nodes produces 1 piece per CID per cycle, where N = deficit" to accurately describe the parallel top-N repair election applied during partition recovery.
