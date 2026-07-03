# Craftec Vision — Complete Architecture

> **Status (2026-07-02):** This is a directional vision document. The engineering source of truth is `craftec_technical_foundation.md` v3.7. Sections below marked **[SUPERSEDED]** were overridden by the 2026-07-02 design review (`.claude/design-reviews/2026-07-02T000000Z.md`) — do not build against them.

## What Craftec Is

Craftec is dumb data infrastructure. Store bytes. Retrieve bytes. Keep them alive. That's the entire product.

No opinions about consensus. No opinions about privacy. No opinions about finance. No opinions about anything.

The less the infrastructure assumes, the more applications it supports. TCP/IP won because it just moves packets. Craftec is TCP/IP for storage.

---

## The Stack

```
┌─────────────────────────────────────────────┐
│               Applications                  │
│        Any app that reads/writes data       │
├─────────────────────────────────────────────┤
│               CraftVFS                      │
│     Distributed filesystem (ZFS-like)       │
│  Inode table + directory entries on CraftSQL│
├─────────────────────────────────────────────┤
│               CraftSQL                      │
│     SQLite VFS backed by CraftOBJ           │
│     Standard SQL. Portable. Pluggable.      │
├─────────────────────────────────────────────┤
│               CraftOBJ                      │
│     Distributed object storage              │
│     RLNC erasure + P2P + self-healing       │
├─────────────────────────────────────────────┤
│            craftec-core                      │
│        Shared infrastructure                │
│  Identity, crypto, erasure, P2P transport   │
│  DHT + PEX + wire protocol + NAT relay      │
└─────────────────────────────────────────────┘

Products:
  CraftOBJ — distributed object storage
  CraftSQL — SQLite VFS on CraftOBJ
  CraftVFS — distributed filesystem on CraftSQL
  CraftNET — decentralized VPN (formerly CraftNet)
  CraftSEC — on-demand MPC functions (transaction attestation)
  CraftCOM — persistent compute agents (CPU + GPU unified)
```

Each product is independently useful. All share craftec-core for networking, identity, and crypto.

The infrastructure never executes content — it only stores and retrieves.

CraftSEC is the trust layer. Every financial write goes through CraftSEC's MPC threshold signing — the program validates the transaction and multiple nodes co-sign. Without CraftSEC, users could write anything to their own chains. With CraftSEC, every entry is program-attested and cryptographically uncontestable.

---

## The Kernel and the Agent Layer

Craftec has a sharp architectural boundary: the **kernel** (compiled Rust, rarely changes) and the **agent layer** (WASM CIDs on SQL, hot-swappable).

### The Kernel (~10K lines of Rust)

The kernel is the minimum viable node — what must be compiled in for the network to function at all:

```
CraftNET  — connect to peers, send/receive bytes
CraftOBJ  — store CID → bytes, retrieve CID → bytes
            Distribution (batched parallel push to storage nodes)
            Health scanning (periodic piece rank assessment)
            Repair (RLNC recombination for missing pieces)
            PDP (challenge/response proof of possession)
CraftSQL  — SQLite VFS on CraftOBJ
```

These are dumb pipes with self-healing. No opinions, no policies, no application logic. But the self-healing IS in the kernel — because the data layer must be stable before anything can run on top of it. You can't rely on a WASM agent to repair the data layer that hosts the WASM agents. Chicken-and-egg.

### Why Repair Lives in the Kernel

Health scanning, repair, and distribution are not "application logic that accidentally got compiled in." They're fundamental to a storage node being a storage node. Without them:
- Distribution: data never reaches the network
- Health scan: nobody detects missing pieces
- Repair: RLNC recombination can't trigger
- PDP: no proof that nodes actually hold data

A node that can't do these things isn't a node. It's a paperweight.

### Two Tiers of Concern

The kernel and agent layer handle fundamentally different problems:

```
Kernel (immediate, per-node):
  HealthScan detects missing pieces    → repair NOW (30s worst-case)
  PDP challenge fails                  → fix NOW
  Node dies                            → RLNC self-healing kicks in automatically
  Distribution fails                   → retry with backoff
  This is the immune system. Fast. Automatic. No coordination needed.

Agent layer (long-term, network-wide):
  "Region X has 30% fewer nodes"       → rebalancing optimization
  "Content ABC over-replicated 3 months, zero demand" → trend to note
  "Average repair latency up 15% this quarter"        → investigate
  "50 nodes consistently underperform"                → pattern analysis
  This is the nervous system. Observability. Optimization. Coordination.
```

The kernel handles urgency. The agent layer handles wisdom. Agent-level metrics don't need real-time freshness — staleness of minutes or hours is fine for trend analysis and optimization. The kernel already ensures nothing breaks in the meantime.

### The Agent Layer (WASM CIDs on CraftSQL)

Everything above the kernel is agents — WASM binaries identified by CIDs, reading and writing SQL tables:

```
Tier 1: Network agents (whitelisted — the "OS")
  registrar          — node/agent discovery
  health-aggregator  — network-wide observability
  repair-monitor     — long-term repair optimization
  fn-manager         — CraftSEC group maintenance
  reaper             — dead node cleanup
  routing            — user→node routing

Tier 2: Application agents (permissionless)
  chain-watcher-eth  — monitors deposits
  withdrawal-processor — threshold-signed releases
  rebalancer         — CCTP treasury management
  auditor            — balance verification
  trading bots, personal AI, custom logic
```

ALL are just WASM CIDs reading/writing SQL tables. ALL are replaceable independently. ALL are versionable (new CID = new version).

### Network Agent Whitelist

Not every agent should be trusted by the network. Nodes must know which program CIDs are legitimate network agents.

```
Bootstrap config (ships with node binary):
{
  network_id: "craftec-mainnet",
  governance: "Qm_governance_v1",    // THE trust root — one hardcoded CID
  bootstrap_peers: ["1.2.3.4:9000"]  // first contact (IP addresses)
}
```

Node startup:
1. Connect to bootstrap peers
2. Find governance agent instances (verify they're running `Qm_governance_v1`)
3. Read governance agent's database → get current whitelist:
   ```
   registrar:          Qm_registrar_v4
   health_aggregator:  Qm_health_agg_v2
   repair_monitor:     Qm_repair_v2
   fn_manager:         Qm_fnmgr_v1
   reaper:             Qm_reaper_v1
   ```
4. Find and connect to whitelisted agents
5. Register with registrars, join network

**Governance updates the whitelist** — no node binary update needed:
```
Operator proposes: "upgrade registrar to v5"
3-of-5 operators sign → governance agent records:
  UPDATE network_whitelist SET cid='Qm_registrar_v5' WHERE type='registrar'
Nodes periodically re-read governance DB → start trusting v5 → v4 winds down
```

The minimal trust root:
- **Hardcoded** (requires binary update): `network_id`, `bootstrap_peers`, `governance` CID
- **Governed** (hot-swappable): all network agent CIDs
- **Permissionless** (no approval needed): all application and user agents

Anyone can RUN a network agent (the CID is public). Nodes only TRUST instances running whitelisted CIDs. Multiple instances = redundancy, not centralization. Program CID = identity.

### SQL Tables ARE the Coordination Layer

No RPC. No message bus. No coordination service. Agents read tables. Agents write tables. Tables are the coordination.

```sql
-- The "control plane" is just tables:
nodes           → what exists
page_locations  → where data lives
fn_groups       → who signs what
user_routing    → where users live
agent_deployments → what's running where
alerts          → agents tell each other about problems
```

Want a new infrastructure capability? Design a table, write an agent, publish as CID, deploy with one INSERT. No rebuild, no redeploy of anything else.

### Upgrades = CID Swaps

```sql
-- Upgrade repair monitor to v2 (geographic redundancy)
UPDATE agent_deployments SET cid = 'Qm_repair_v2' WHERE cid = 'Qm_repair_v1';

-- Don't like v2? Instant rollback:
UPDATE agent_deployments SET cid = 'Qm_repair_v1' WHERE cid = 'Qm_repair_v2';
```

Old agents wind down. New agents start. Same SQL tables. Same data. Better logic. CIDs are immutable — every version exists forever.

### Self-Modifying System

A governance agent can deploy other agents:

```
governance-agent:
  loop {
    proposals = query("SELECT * FROM proposals WHERE status='approved' AND type='deploy_agent'");
    for p in proposals {
      execute("INSERT INTO agent_deployments (cid, node_count) VALUES (?, ?)", p.agent_cid, p.node_count);
      execute("UPDATE proposals SET status='deployed' WHERE id=?", p.id);
    }
    sleep(30_000);
  }
```

A DAO votes to upgrade the repair strategy. Vote passes → proposal approved → governance agent deploys new CID. The network upgraded itself. No human touched a server.

### The Transition: Kernel → Agent

The kernel provides baked-in self-healing as the foundation and permanent fallback. Over time, as the network matures:

- **Early network**: kernel does everything, agents don't exist yet
- **Stable network**: agents take over coordination (smarter repair strategies, network-wide monitoring), kernel still runs as fallback
- **Mature network**: agents handle 99% of decisions, kernel only kicks in if agent layer is down

Like Linux's kernel OOM killer — crude, last resort — while userspace has systemd, cgroups, and orchestrators making smarter decisions. If userspace breaks, the kernel keeps the machine alive.

### Bootstrap Sequence

```
1. Start 3-5 nodes manually (kernel only — they can already store, distribute, repair)
2. Deploy heartbeat-agent on each (self-register into nodes table)
3. Deploy reaper-agent on 2 nodes (marks dead nodes)
4. Deploy repair-monitor on 2 nodes (network-wide repair coordination)
5. Deploy fn-manager-agent on 2 nodes (CraftSEC group health)
6. System is now self-maintaining
7. Deploy CraftSEC threshold groups
8. Deploy gateway agents (chain watchers, withdrawal processor)
9. Open for users

Steps 1-6: the "operating system" boots
Steps 7-8: the "application" starts
Step 9: business
```

### Comparison

```
Linux:    Kernel (C, compiled) → systemd → services → apps
          Updating systemd = rebuild, reboot, pray

Craftec:  Kernel (Rust, compiled) → OS agents (WASM CIDs) → service agents → user agents
          Updating anything above kernel = publish new CID, update one SQL row. No restart.
```

The kernel never changes. Everything intelligent is composable agents reading and writing SQL. The network can evolve, upgrade, and fix itself without touching the binary that runs on nodes.

---

## craftec-core — Shared Infrastructure

> **[SUPERSEDED — stack]:** The transport stack below is libp2p-era (DCUtR, PEX, DIDs). The current stack is **iroh (QUIC) + partial-view membership + Ed25519/Pkarr + BLAKE3 + postcard** — see foundation §3 and §62. The RLNC erasure direction stands.

P2P connectivity and crypto primitives shared by all products. Same model as BitTorrent.

- **DHT (Kademlia)**: peer routing, key-value records, mutable signed records
- **PEX (Peer Exchange)**: peers share known peer lists
- **P2P request/response**: direct node-to-node communication
- **NAT traversal**: relay nodes + hole punching via DCUtR
- **Identity**: DIDs, signing keys, capability announcements
- **Erasure coding**: RLNC over GF(2^8), homomorphic hashing
- Zero gossipsub — all communication is pull-based or direct
- Proven at 10M+ concurrent nodes (BitTorrent)

---

## CraftOBJ — Distributed Object Storage

Store and retrieve content-addressed objects across a volunteer P2P network.

### Two-Tier Model

**Free tier (BitTorrent-style altruism):**
- You store pieces for others, others store pieces for you
- Tit-for-tat reciprocity, no tokens, no financial overhead
- Best-effort persistence — data survives if enough nodes stay
- Self-healing via HealthScan detects and repairs missing pieces

**Paid tier (guaranteed erasure minimum):**
- Pay to ensure N pieces always exist
- PDP (Proof of Data Possession) verifies nodes hold pieces
- Solana settlement enforces contracts
- For data that must survive regardless of altruism

### Storage Strategy

> **[SUPERSEDED — decision C2, 2026-07-02]:** There is **no replication tier**. Everything is RLNC erasure-coded: 16 KB SQLite pages in K=8 generations (128 KB), files in K=32 segments (8 MiB). See foundation §28 and §62.2.

- ~~**Small objects (< 1MB)**: replicate N copies~~ (superseded)
- **Large objects**: RLNC erasure coding with 256KB pieces (storage efficient for files, video)

### The Core Problem: Churn

Everything else — consensus, ordering, verification, integrity — is solved by the architecture. The one real problem: if pieces disappear faster than healing can replace them, data dies.

```
Data alive: healing rate > churn rate
Data dead:  churn rate > healing rate
```

RLNC's advantage: any surviving node can generate a new unique piece without needing the original data. This dramatically increases healing rate compared to traditional erasure coding.

### Scale Comparison (Bitcoin-equivalent transaction volume)

```
Bitcoin:  600GB × 20,000 nodes = 12,000 TB network-wide (every node stores everything)
Craftec: 100GB × 30 replicas = 3 TB total (distributed across all nodes)
         With 20,000 nodes: each stores ~150MB
         That's less than a phone app. Anyone can participate.
```

---

## Layer 3: CraftSQL — SQLite VFS on CraftOBJ

Not a new database engine. A **SQLite Virtual File System** that maps page I/O to CraftOBJ.

### Why SQLite VFS

- Battle-tested over 20+ years, billions of deployments
- Full SQL, query planner, indexes, transactions — all free
- Massive ecosystem (every language has bindings)
- Public domain — no licensing issues
- Precedent exists: sql.js, cr-sqlite, Litestream, LiteFS

### How It Works

```
SQLite internally:
  xRead(page 42)  → VFS fetches CID from CraftOBJ
  xWrite(page 42) → VFS encodes new CID, stores in CraftOBJ
  xSync()         → VFS updates page table CID, publishes to DHT
```

The application writes SQL. SQLite handles query planning, indexing, transactions. The VFS handles CID mapping. CraftOBJ handles storage.

### Page Table

A page table is just a mapping: page number → CID.

| Database size | Pages | Page table size |
|---|---|---|
| 1 MB | 256 | ~10 KB |
| 10 MB | 2,560 | ~100 KB |
| 100 MB | 25,600 | ~1 MB |

For personal data use cases, the page table is tiny — fits in a single CID fetch.

### Tracking the Page Table

One DHT signed record per database. That's the only mutable thing.

```
DID:alice:mydb → CID_of_page_table (latest version)

DHT signed record (mutable, one per database)
└→ Page table CID (immutable)
   ├→ Page 0 CID (immutable)
   ├→ Page 1 CID (immutable)
   └→ ...
```

Every write creates new immutable CIDs and updates one mutable pointer. Old CIDs persist in the network — that's free version history. A snapshot is just saving the old page table CID.

### Latency — The Real Challenge

SQLite assumes disk (microsecond reads). CraftOBJ responds in milliseconds to seconds.

Solution: local-first, sync later.

```
App → SQLite (local, fast, microseconds)
      ↓ background sync
      CraftOBJ (network, slow, durability + sharing)
```

- Write to local SQLite immediately
- Background sync to CraftOBJ
- Aggressive local caching of pages
- Merkle diffs to keep cache fresh
- User never waits for the network

### Pluggable PageStore

CraftSQL's real value: the PageStore interface is swappable.

```rust
trait PageStore {
    fn get(&self, cid: &Cid) -> Result<Page>;
    fn put(&self, page: &Page) -> Result<Cid>;
    fn update_root(&self, new_root: Cid) -> Result<()>;
}
```

- CraftOBJ implements it (distributed P2P)
- Local disk implements it (offline/single-machine)
- S3/R2 implements it (traditional infra)
- Anyone can implement their own

Develop and test against local PageStore, plug in CraftOBJ when ready.

### CraftVFS uses a Database interface (not PageStore)

```rust
trait Database {
    fn query(&self, sql: &str, params: &[Value]) -> Result<Rows>;
    fn execute(&self, sql: &str, params: &[Value]) -> Result<()>;
}
```

CraftSQL, SQLite, PostgreSQL, DuckDB — any can implement it. Two plugin points at two different abstraction levels.

---

## Layer 4: CraftVFS — Distributed Filesystem

A filesystem built as CraftSQL tables. Uses inode-style design for O(1) renames.

### Schema

```sql
CREATE TABLE inodes (
    id INTEGER PRIMARY KEY,
    cid TEXT,
    type TEXT,  -- 'file' | 'dir'
    size INTEGER,
    owner TEXT  -- DID
);

CREATE TABLE dirents (
    parent_id INTEGER,
    name TEXT,
    inode_id INTEGER,
    PRIMARY KEY (parent_id, name)
);
```

Rename a directory = update one row in dirents. O(1).

### ZFS Features — For Free

| Feature | How |
|---|---|
| Copy-on-write | Every mutation creates new CID pages (inherent) |
| Snapshots | Bookmark current root page CID. Instant, zero cost. |
| Checksums | CID IS the checksum |
| Self-healing | CraftOBJ HealthScan + RLNC repair |
| RAID-Z | RLNC erasure across network nodes |
| Deduplication | Same content = same CID. Automatic. |
| Clones | New entry pointing to same CID. Zero cost. |

### Beyond ZFS

Survives entire machine loss, geographic redundancy, shareable via DID, unlimited snapshots, global namespace.

---

## Single-Owner Writes — The Core Insight

Every user owns their own database. Only the owner writes to it. This eliminates the need for consensus.

### Why Consensus Exists

Blockchain complexity comes from ONE decision: "multiple untrusted parties write to shared state."

That requires consensus protocols, global ordering, state machine replication, gas metering, VM sandboxing, fork choice rules, finality gadgets, sybil resistance, slashing.

Craftec removes that decision. Single owner writes. The entire tower of complexity vanishes.

### Everything Decomposes to Single-Owner

```
Messaging: each person writes to their own DB, app reads from both
Social media: your posts live in your DB, feed = reading followed users
Marketplace: your listings in your DB, search = query across sellers
Email: already works this way — outbox is yours, inbox is theirs
```

Even "multi-writer" problems decompose:

```
Inventory: seller owns inventory DB, seller decides who gets last item
Bank transfer: Alice writes "send $100" (signed), Bob reads and verifies
Game state: players write inputs, host writes resolved state
Stock exchange: each trader writes orders, exchange is single-writer matcher
```

Every "consensus" problem decomposes into individual writes + a coordinator that is itself a single writer.

### Financial Chain Security

Single-owner writes make forking structurally impossible. CraftSEC MPC attestation makes fraud cryptographically impossible.

```
One DID → one DHT entry → one page table CID → one database → one transaction history
```

Alice can't fork because:
- She can't create two different versions of the same CID (content-addressed)
- She can't rewrite history (hash-linked chain)
- Everyone reads the same DHT key (public)
- She can't write invalid entries (CraftSEC threshold nodes must co-sign)

Alice can't lie about her balance because:
- Every transaction is attested by the program's MPC signature
- The program reads her actual balance before signing
- Invalid balance = program refuses to sign = no valid entry

SQL queries verify everything:

```sql
-- Check balance
SELECT SUM(CASE
    WHEN recipient = 'alice' THEN amount
    WHEN sender = 'alice' THEN -amount
END) as balance
FROM transactions
WHERE sender = 'alice' OR recipient = 'alice';

-- Verify program attestation
SELECT * FROM transactions
WHERE program_sig IS NULL
   OR program_cid NOT IN (SELECT cid FROM trusted_programs);
-- Should return zero rows. Any result = invalid entry.
```

No blocks. No mining. No staking. No consensus. Just cryptography, MPC attestation, and a public log.

---

## Privacy — Encryption at the VFS Boundary

Privacy is a flag, not a feature.

```
// Public database
let db = sqlite3_open("craftobj://alice/photos");

// Private database
let db = sqlite3_open("craftobj://alice/photos?encrypt=true");
```

### Three Levels

```
Level 1 — Public:     anyone reads, anyone verifies
Level 2 — Encrypted:  nobody reads, nobody verifies
Level 3 — ZK private: nobody reads, anyone can verify (app-level ZK proofs)
```

### How Encryption Works

```
Write: SQLite page → encrypt with user's key → CID of ciphertext → CraftOBJ
Read:  Fetch CID → get ciphertext → decrypt → SQLite page
```

Nodes store encrypted blobs. They never see the data. The VFS handles this transparently.

### ZK for Private Verifiable Chains

ZK is application-level logic, not infrastructure. The VFS encrypts. The application generates proofs and stores them as regular data in a public table.

```
Alice's DB (encrypted): transactions table
Alice's DB (public):    proofs table

Bob verifying: fetch proof → verify math → accept payment
               Never sees Alice's balance or history.
```

Every "private blockchain" (Zcash, Monero, Aztec, Railgun) embeds privacy into the protocol. Craftec keeps it in the application layer — simpler, flexible, each app picks its own ZK scheme.

### Encryption vs Dedup Tradeoff

Encrypted data can't deduplicate across users (different keys = different CIDs). Accept this: dedup works within a user's namespace only. Public data deduplicates. Private data doesn't. User decides.

---

## CraftSEC — On-Demand MPC Functions (The Trust Layer)

> **[SUPERSEDED — decision C3, 2026-07-02]:** The MPC/DKG/PDK trust model in this section is superseded. Internal attestation uses **k-of-n independent Ed25519 signatures** (no shared secrets, no DKG, no MPC); FROST DKG is used only at chain boundaries. See foundation §41 and §62.2. This section is retained as design history.

Users own their chains and can write anything. Without attestation, a user could post a fake transaction claiming they have funds they don't.

Optimistic verification ("someone will check later") fails in practice — nobody has incentive to run verification nodes for millions of small transactions. Fraud goes unchecked.

CraftSEC solves this with MPC threshold signatures. Every financial write must be validated and co-signed by a program running on threshold nodes. No valid signature = no valid transaction. Not optimistic. Deterministic.

### How It Works

```
Alice wants to send $50 to Bob:

1. Alice submits request to CraftSEC:
   {fn: Qm_transfer_abc, args: {to: bob, amount: 50}}

2. Request goes to 3 threshold nodes (randomly selected)

3. Each node independently:
   - Loads function from CID (cached after first load)
   - Reads Alice's balance from CraftSQL
   - Validates balance >= 50
   - Computes output transaction
   - Produces signature SHARE
   Time: <50ms each (it's just SQL queries)

4. Alice collects 2-of-3 shares → combines → full signature

5. Entry written to Alice's chain:
   {
     sender: alice,
     recipient: bob,
     amount: 50,
     user_sig: <Alice's signature>,          ← proves intent
     program_cid: Qm_transfer_abc,           ← which code ran
     program_sig: <MPC threshold signature>  ← proves code validated it
   }

Total added latency: ~100ms
Total added complexity for user: zero (SDK handles it)
```

### Why MPC Over Optimistic

```
Optimistic:
  "Trust but verify"
  → Assumes someone will check
  → In practice, nobody checks small transactions
  → Fraud goes undetected until damage is done
  → Challenge periods add latency (7 days on L1 rollups)

MPC:
  "Can't write without proof"
  → Valid signature = valid transaction. Period.
  → No challenge window. No watchers needed.
  → Fraud is structurally impossible, not economically unlikely.
  → Not a spectrum. A binary.
```

### Program Derived Keys (PDK)

Programs own keys. No human has access. Same concept as Solana PDAs, but enforced by threshold cryptography instead of a VM runtime.

```
Deploy a program:
1. Developer publishes code → gets CID (Qm_transfer_abc)
2. Network generates a keypair FOR that CID:
   - Distributed Key Generation (DKG) across threshold nodes
   - Each node gets a SHARD of the private key
   - Full private key never exists anywhere
   - Public key = the program's identity
3. Program is now deployed:
   Program CID: Qm_transfer_abc
   Program key:  craft1_xyz... (public, verifiable)
   Key shards:  held by N threshold nodes
```

```
Solana PDA:
  Key derived from: program_id + seeds
  Signing enforced by: Solana runtime (VM)
  Trust assumption: validators run honest VM

Craftec PDK:
  Key derived from: program CID + seeds
  Signing enforced by: threshold nodes (DKG)
  Trust assumption: majority of threshold nodes honest

Same concept. The PROGRAM owns the key. No human has access.
Only correct execution produces a valid signature.
```

### Programs Can Hold Assets

A program key can own balances — just like a Solana PDA holds tokens:

```
Program: Qm_swap_program
Program key: craft1_swap...

This key can OWN assets:
  In CraftSQL: balance entries where owner = craft1_swap...
  On-chain escrows: USDC held by program's derived key

Only the swap program (verified by CID) can move these funds.
No human can access them.

→ This IS a smart contract wallet
→ This IS a DEX liquidity pool
→ This IS a DAO treasury
→ This IS an escrow
```

### Multiple Keys Per Program

Programs can derive multiple keys for different purposes (like Solana PDAs with different seeds):

```
transfer_program_key     = DKG(Qm_transfer + "main")
escrow_key_alice_bob     = DKG(Qm_transfer + "escrow:alice:bob")
treasury_key             = DKG(Qm_transfer + "treasury")

Each key is independently threshold-managed.
Each requires correct program execution to sign.
Program logic decides WHICH key signs WHEN.
```

### Key Rotation

Proactive Secret Sharing allows key shard rotation without changing the public key:

```
Periodically:
- Re-share key shards to new set of nodes
- Old shards become useless
- Public key stays the same
- Even if attacker stole old shards → can't use them

Adds time dimension to security.
Attacker must compromise threshold nodes simultaneously.
```

### Why Not Other Approaches

```
TEE (Trusted Execution Environments):
  Hardware enclaves (Intel SGX) attest code execution.
  Fast, simple, privacy built in.
  BUT: trusts hardware manufacturer, side-channel attacks, vendor lock-in.
  Against Craftec's "no trust in hardware."

Optimistic / Fraud Proofs:
  Assume valid, challenge if wrong. Cheapest and simplest.
  BUT: nobody checks, challenge periods add latency, no enforcement mechanism without bonds/slashing.

ZK Validity Proofs:
  Mathematically prove execution was correct. Strongest guarantee, instant finality.
  BUT: heavy computation for proof generation.
  Used at APPLICATION level (CloakCraft) for privacy, not at infrastructure level for every transaction.

MPC Threshold Signatures:
  ✓ No hardware trust (pure cryptography)
  ✓ No challenge period (instant finality)
  ✓ No heavy computation (SQL queries + signing)
  ✓ Proven technology (TSS libraries exist)
  ✓ Uncontestable (signature valid or not)
```

### Trust Model

```
Every financial transaction requires:
  User signature   → proves intent (Alice authorized this)
  Program MPC sig  → proves correctness (code validated it)

Both required → neither alone is sufficient
User can't bypass program → doesn't have program key
Program can't act alone → needs user's authorization
Threshold nodes can't steal → need 2-of-3 minimum
Single node can't forge → one shard is useless

Three independent guarantees. All cryptographic. None optimistic.
```

### Transaction Schema

```sql
CREATE TABLE transactions (
    seq INTEGER PRIMARY KEY,
    prev_hash TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    amount REAL NOT NULL,
    timestamp INTEGER NOT NULL,

    -- User authorization
    user_sig TEXT NOT NULL,

    -- Program attestation (MPC)
    program_cid TEXT NOT NULL,   -- which code ran (immutable)
    program_sig TEXT NOT NULL,   -- threshold signature (unforgeable)

    -- Optional: multiple attestors for high-value tx
    attestors JSON,             -- [{cid, sig}, {cid, sig}, ...]

    hash TEXT NOT NULL           -- hash of everything above
);
```

### What CraftSEC Nodes Actually Do

```
CraftSEC node is lightweight:
- No persistent state
- No event loop
- No database

Per request:
1. Receive function call + args
2. Load function from CID (cached)
3. Read required state from CraftSQL (network call)
4. Execute function (SQL queries, ~10ms)
5. Produce signature shard
6. Return shard to caller
7. Forget everything

That's it. Stateless. On-demand. Milliseconds.
```

### Compute Taxonomy

```
CraftSEC — On-demand MPC functions
           Stateless. Per-request. Milliseconds.
           THE trust layer for all financial writes.
           Every transaction attestation goes through CraftSEC.

CraftCOM — Persistent compute agents (CPU + GPU unified)
           Stateful. Always-on. Long-running.
           GPU is a feature flag, not a separate system.
           One framework, two execution profiles.
           CPU: gateway watchers, bots, services.
           GPU: inference, ZK proofs, transcoding.
```

---

## Web Hosting — No Servers Needed

```
npm run build → /dist folder → upload to CraftOBJ → done
```

Every modern frontend framework produces static files. CraftOBJ serves them. Popular content caches on more nodes automatically (same as BitTorrent).

```
Vercel charges for:          Craftec equivalent:
CDN bandwidth                CraftOBJ P2P (free)
Static hosting               CraftOBJ CIDs (free)
Build minutes                Your own machine (free)
Serverless functions         Browser + CraftSQL (mostly free)
Database                     CraftSQL (user-owned)
```

SSR (server-side rendering) is mostly eliminated:
- SEO → pre-render at build time (SSG)
- Fast first paint → static HTML shell + client-side hydration
- Dynamic content → client fetches from CraftSQL directly
- Edge cases → ISR (render once, cache as CID forever)

The entire web hosting industry is: "I have files, serve them to browsers." CraftOBJ does exactly that.

---

## CraftCOM — Persistent Compute Agents

One framework, two execution profiles. GPU is a feature flag, not a separate system.

Not distributed batch compute. Persistent autonomous agents that run continuously, coordinate through SQL, and survive host failure.

### Agents, Not Jobs

```
Batch mindset (traditional compute networks):
  Submit job → split across nodes → collect results → done
  One-shot. Request/response. Nodes idle between jobs.

Agent mindset (CraftCOM):
  Agent runs continuously
  Watches CraftSQL for work
  Processes requests as they arrive
  Writes results back to CraftSQL
  Never stops. Always warm.
```

### Why Agents Need Hosting

```
AI agent with MCP, trading bot, IoT edge processor, indexer,
notification service, orchestration agent — all need:
  ✓ Always running (can't close laptop)
  ✓ Stateful (remembers context)
  ✓ Network-connected
  ✓ Autonomous
```

### Agent Model on Craftec

```
Agent state = CraftSQL database (CIDs)
Agent code  = WASM binary (CID)
Agent I/O   = reads/writes CraftOBJ

If host node dies:
  State is already in CraftOBJ
  New node loads code CID + state CID → resumes
```

### CPU Agents

Logic, I/O, coordination — lightweight, runs anywhere:

```
Agent: chain-watcher-ethereum
  Loop: Read latest ETH block
        Check escrow address for incoming USDC
        INSERT INTO deposits (did, amount, chain, tx_hash, confirmations) ...

Agent: withdrawal-processor
  Loop: SELECT * FROM withdrawals WHERE status = 'pending'
        Generate signature share (threshold sig)
        When threshold met → broadcast tx to target chain

Agent: rebalancer
  Loop: Check escrow balances across chains
        Trigger CCTP transfer if imbalanced

Agent: auditor
  Loop: Compare CraftSQL balances vs actual on-chain balances
        Flag any discrepancy
```

### GPU Agents (feature-gated)

Same framework, GPU hardware. Model weights stored as CIDs in CraftOBJ:

```
Agent: inference-llama-70b
  Watches: inference_requests table
  Writes:  inference_results table
  GPU: model loaded, runs forward pass per request

Agent: zk-prover
  Watches: proof_requests table
  GPU: generates ZK proofs continuously

Agent: transcoder
  Watches: transcode_queue table
  GPU: video encoding/decoding
```

### Coded Computation for Redundancy

RLNC coded computation — the same math that protects storage — applies to agent availability and verification:

```
3 agents serve the same model (redundancy)
Any one can handle requests
If one dies → other 2 continue, new agent spawns
Coded computation verifies results mathematically
```

For large jobs needing parallelism, coding eliminates stragglers:

```
Normal: 10 GPUs process parts → wait for ALL 10 (stragglers block)
Coded:  15 GPUs process coded parts → ANY 10 finish → decode
```

Research-proven for linear operations: matrix multiply, convolutions, gradient descent, ZK proof generation. Non-linear operations (sorting, graph traversal) run locally.

### Parallelism Through Decomposition

One agent is sequential. But workflows decompose into multiple agents:

```
Agent A: watches market → writes signals to its DB
Agent B: watches news  → writes summaries to its DB
Agent C: reads A + B   → makes decisions → writes trades
Agent D: reads C       → sends notifications
```

No message queues. No API calls. Agents read each other's databases. SQL is the coordination layer.

### Primary Use Case: Financial Gateway

The multichain gateway is a fleet of CraftCOM agents. All coordinated through SQL — no message queues, no RPC calls, no orchestration framework.

Trust model: 5 independent agents run withdrawal-processor. Each produces a signature share. 3-of-5 required to release funds. No single agent can steal. All signatures recorded in CraftSQL — public proof.

If an agent is compromised: can't steal (needs 3-of-5), can't hide (all actions in public DB), can be replaced (new agent loads same code CID).

### Why It's Novel

Existing compute networks (Render, Akash, io.net) are centralized marketplaces with batch job queues — AWS with extra steps and a token.

CraftCOM: persistent agents (not jobs), no platform, no token, data already on CraftOBJ (data locality), coded computation for verification + straggler elimination. Nobody ships persistent compute agents on a P2P network in production.

---

## Single-Writer Agent Architecture

The single-owner write principle applies to infrastructure too — not just user data. Every database has exactly one writer. Period.

### The Problem: Multi-Writer Infrastructure

A naive infrastructure design uses shared tables:

```
❌ WRONG: One shared nodes table
heartbeat-agent-1 → INSERT INTO nodes ...
heartbeat-agent-2 → INSERT INTO nodes ...
heartbeat-agent-3 → INSERT INTO nodes ...
Three writers. One table. Need coordination. We just reinvented consensus.
```

### The Solution: Single-Owner Decomposition

Each agent writes to ITS OWN database. Other agents READ across databases.

```
✅ RIGHT: Each node writes to its own database
did:node:abc/status → node abc writes its own heartbeat, capacity
did:node:def/status → node def writes its own heartbeat, capacity
did:node:ghi/status → node ghi writes its own heartbeat, capacity

Reaper agent READS ACROSS all node databases:
  for node in known_nodes {
    status = read(node.did + "/status", "SELECT * FROM my_status LIMIT 1");
    if status.timestamp < now() - 30_seconds { /* suspect */ }
  }

Reaper writes verdicts to ITS OWN database:
  did:agent:reaper-1/verdicts → INSERT INTO my_verdicts ...
```

Every infrastructure concern decomposes this way:

| Concern | Writers | Readers |
|---|---|---|
| Node health | Each node → own `status` DB | Health monitor reads all |
| Page locations | Each node → own `my_pages` DB | Repair agent reads all |
| Chain deposits | Each watcher → own `observations` DB | Deposit processor reads all |
| CraftSEC groups | Each fn-node → own `my_shards` DB | FN manager reads all |
| Repair tasks | Repair agent → own `tasks` DB | Nodes read their assignments |

No shared writes. Ever. Reads are free — no coordination needed.

### Namespace: DID as Database Address

Every database is namespaced by its owner's DID. No collision possible.

```
Full namespace hierarchy:
  did:user:alice/financial      ← Alice's financial DB
  did:user:alice/photos         ← Alice's photos (CraftVFS)
  did:node:abc/status           ← Node abc's health
  did:node:abc/my_pages         ← Node abc's inventory
  did:agent:repair-v1/tasks     ← Repair agent's work queue
  did:agent:watcher-eth-7/obs   ← Chain watcher's observations
  did:program:Qm_swap/pool      ← Swap program's state (PDK-owned)
```

Identity types:
- `did:user:*` — human users
- `did:node:*` — infrastructure nodes
- `did:agent:*` — running agent instances
- `did:program:*` — program-derived keys (PDK)

DHT enforces ownership: only the DID owner can sign updates to their namespace. Math, not convention.

Same table names in different databases don't collide — they're separate SQLite instances, like separate Postgres servers. `transactions` in `did:user:alice/financial` and `transactions` in `did:user:bob/financial` are completely independent.

### Discovery: Registrars, Not DHT Enumeration

Kademlia DHT supports point lookups (`GET(key)`), not enumeration (`LIST_ALL()`). You can find a key if you KNOW the key. You cannot discover keys you don't know about.

So `dht.scan_prefix("craftec:agents:")` **doesn't exist**. Agents must ANNOUNCE, not be discovered. Discovery is PUSH, not PULL.

**Registrar agents** (whitelisted network agents) solve this:

```
Node joins → sends registration MESSAGE to registrars:
  craftnet.send(registrar, {type: "register", node_did, address, capabilities, region})

Registrar receives → writes to ITS OWN database (single-writer preserved):
  INSERT OR REPLACE INTO registered_nodes VALUES (?, ?, ?, ?, now())

Agent starts → announces to registrars:
  craftnet.send(registrar, {type: "register_agent", agent_did, agent_type, program_cid, tags})
```

Multiple registrars run the same whitelisted CID. Node announces to ALL of them. If one is down, others still work. They're interchangeable instances of the same program.

**How does a node find registrars?** Bootstrap peers. Same as DNS root servers, Bitcoin seed nodes, BitTorrent bootstrap. Some things MUST be well-known.

**Gossip as backup**: Nodes also exchange peer lists with neighbors (SWIM/Serf-style). If registrars are unreachable, gossip still provides local awareness. Registrars bridge partitions. Gossip provides resilience.

DHT is still used for:
- **Point lookups**: `did:user:alice:financial → CID` (data routing)
- **Signed records**: mutable pointers to latest database version

DHT is NOT used for:
- Enumeration ("list all nodes")
- Discovery ("find all agents of type X")
- These go through registrars

### Cross-Database Queries and Aggregation

Agent-level observability is for trends and optimization, not liveness. Staleness of minutes is fine — the kernel handles immediate repair. This means aggregation can be infrequent and the cross-database read cost is amortized.

**Phase 1 (<1,000 nodes): Direct aggregation**

```
observability-agent:
  loop {
    // get node/agent list from registrar (one read, not N)
    all_agents = read("did:agent:registrar-1/registry",
                      "SELECT * FROM registered_agents");
    for agent in all_agents {
      health = read(agent.did + "/db", "SELECT * FROM _health LIMIT 1");
      db.execute("INSERT OR REPLACE INTO agent_summary VALUES (?, ?, ?)",
                  agent.did, health.status, health.last_action);
    }
    sleep(15 * 60_000); // every 15 minutes — not seconds
  }
```

One agent reads all, writes ONE summary. Everyone else reads that summary. Single-writer preserved.

**Phase 2 (1K-100K nodes): Regional aggregation**

```
Level 0: Each node/agent writes own status
Level 1: Regional aggregators (5-10 agents, each reads ~10K databases)
Level 2: Global aggregator (reads 5-10 regional summaries)
```

No single agent reads more than ~10K databases. Scales indefinitely by adding regions.

**Phase 3 (100K+ nodes): Hierarchical + gossip**

Regional and global aggregators provide the authoritative long-term view. Local gossip (each node exchanges peer lists with ~20 neighbors) provides fast approximate local awareness. Information propagates in ~3 hops.

### Agent Observability Contract

Every CraftCOM agent gets standard tables automatically (framework-injected, developer never sees them). These are for long-term metrics and trend analysis, not real-time liveness — the kernel handles immediate concerns:

```sql
CREATE TABLE _health (
    timestamp TIMESTAMP,
    status TEXT,        -- 'running', 'error', 'stalled'
    last_action TIMESTAMP,
    error TEXT
);

CREATE TABLE _metrics (
    timestamp TIMESTAMP,
    metric TEXT,        -- 'rows_processed', 'latency_ms', etc
    value REAL
);

CREATE TABLE _log (
    timestamp TIMESTAMP,
    level TEXT,         -- 'info', 'warn', 'error'
    message TEXT
);
```

Like `/metrics` in Kubernetes or `/health` in microservices — but as SQL tables. Queryable, joinable, indexable.

The framework wraps every agent's main loop:

```rust
// developer writes:
fn main(db: Database) {
    loop { process_block(); sleep(1000); }
}

// framework wraps with:
fn __runtime_main(db: Database) {
    // auto-register with registrars
    for reg in config.registrars {
        craftnet.send(reg, {type: "register_agent", my_did, program_cid, tags, node_id});
    }
    // auto-update _health on each iteration
    // auto-record _metrics (iteration_ms, error counts)
    // auto-deregister on shutdown
}
```

### Agent Discovery

Every agent auto-registers with registrars on startup (not DHT — DHT doesn't support enumeration):

```
Agent startup → announces to registrars:
  craftnet.send(registrar, {
    type: "register_agent",
    agent_did: "did:agent:watcher-eth-7",
    agent_type: "chain-watcher",
    program_cid: "Qm_watcher_v4",
    database: "observations",
    node_id: "did:node:abc",
    tags: ["ethereum", "mainnet"]
  });

Registrar writes to ITS OWN database:
  INSERT INTO registered_agents VALUES (...)

Framework auto-deregisters on shutdown. Reaper removes stale entries.
```

Discovery is registrar queries (normal SQL against registrar's DB):
- "Find all chain watchers" → `SELECT * FROM registered_agents WHERE type='chain-watcher'`
- "Find Ethereum watchers" → `WHERE tags LIKE '%ethereum%'`
- "Find agents running program Qm_watcher_v4" → `WHERE program_cid='Qm_watcher_v4'`

### The Full Observability Stack

All single-owner. All composable. All replaceable via CID swap.

```
Layer 0: Every agent (automatic)
  Writes: _health, _metrics, _log to own database
  Registers: in DHT with type, tags, capabilities

Layer 1: Agent aggregator
  did:agent:observability-{region}/dashboard
  Reads: all agents' _health tables
  Writes: own agent_summary, metrics_history

Layer 2: Service aggregator
  did:agent:service-monitor/dashboard
  Groups by service type:
    "Gateway": watchers + withdrawal processors + rebalancer
    "Storage": repair agents + GC agents
    "Attestation": CraftSEC fn-groups

Layer 3: Alert agent
  did:agent:alerter/alerts
  Reads: service-monitor summary
  Rules: stalled watcher → alert, degraded fn-group → critical, etc.
```

### The Tradeoff

```
Shared table:   fast reads, need consensus for writes
Single-owner:   no consensus, expensive cross-reads
Aggregators:    absorb the read cost once, everyone else reads one summary
```

Consensus eliminated. Read cost absorbed by aggregators. Net win — reads are cheap, consensus is hard.

---

## What Doesn't Exist in Craftec

| Thing | Why Not |
|---|---|
| Consensus layer | Single-owner writes eliminate the need. No multi-writer = no consensus. |
| Smart contract VM | Programs are functions (any language) validated by CraftSEC MPC. No VM, no gas, no bytecode. |
| Token/coin | Free tier is altruism. Paid tier uses USDC via multichain escrows. |
| Optimistic verification | MPC threshold signatures are deterministic. Valid signature or not. No challenge periods, no watchers. |
| CraftSYS (OS) | Running untrusted code is a fundamentally different problem than storing data. |
| Batch compute | CraftSEC is on-demand, CraftCOM agents are persistent. None are batch job queues. |

---

## Compared to Everything Else

### vs Blockchain

```
Blockchain: every node processes every transaction (replication)
Craftec:    every node stores a fraction (distribution)

More nodes on blockchain = same capacity
More nodes on Craftec    = more capacity
```

```
Trust model:
  Blockchain: consensus (everyone runs the code) → VM enforces
  Craftec:    MPC threshold (3 nodes run the code) → cryptography enforces

Same guarantee: trusted code ran. 1000x less redundancy.
```

```
"On-chain" = committed + ordered + available + tamper-evident

Blockchain: hash-linked blocks, consensus-ordered
Craftec:    hash-linked CIDs, single-owner ordered, MPC-attested

Both are "on-chain." Different topology, same properties.
```

### vs Existing Systems

| System | Has | Missing |
|---|---|---|
| IPFS | Content-addressed P2P | No erasure coding, no DB, no FS |
| Filecoin | P2P + erasure coding | Economics-first, no database, no FS |
| Storj | P2P + erasure coding | Managed satellites, no DB |
| Holochain | Agent-centric + DHT | Custom framework, no SQL, no erasure coding |
| BitTorrent | P2P file sharing | No persistence guarantees, no DB |
| ZFS | COW filesystem | Local only |
| CockroachDB | Distributed SQL | Managed infra, replication not erasure |

### vs Holochain (Closest Relative)

Same philosophy (agent-centric, personal chains, no global consensus). Different execution:

| | Holochain | Craftec |
|---|---|---|
| Data model | Custom entries + links | Standard SQL |
| Developer experience | Learn Holochain Rust framework | Write SQL |
| Storage | DHT replication | RLNC erasure coding |
| Adoption barrier | High (custom everything) | Low (it's just SQLite) |

"They built a framework. We built a database."

---

## Related Protocols

Protocols that won by being dumb:

```
TCP/IP     → move packets (no opinion on content)
DNS        → resolve names (no opinion on targets)
HTTP       → request/response (no opinion on payload)
SMTP       → deliver messages (no opinion on text)
BitTorrent → share files (no opinion on files)
Craftec    → store/retrieve data (no opinion on what it is)
```

Every protocol that won was the dumbest one in its category.

---

## Go-to-Market: Financial Network First

"Store your files on P2P" is abstract. "Send and receive money with no middleman" is instantly understood.

```
1. Financial network → proves CraftOBJ works, solves churn, builds node base
2. File storage      → CraftVFS on top of the same network
3. Developer platform → CraftSQL opens up to developers
4. Full ecosystem    → grows naturally
```

Bitcoin did exactly this. Started as money. Data protocols came later.

The financial use case bootstraps the network. Financial nodes stay online because their money depends on it. That's stronger motivation than altruism.

---

## Multichain Architecture — Chains Are Just I/O Ports

### The Insight

On-chain escrow contracts do almost nothing: receive USDC, emit event, release USDC. That's it. The actual business logic — balances, authorization, withdrawal limits, rebalancing — all lives in CraftSQL.

Blockchains are dumb wallets with events. CraftSQL is the computer.

```
Traditional:
  Logic lives ON each chain (smart contracts)
  → deploy on every chain
  → learn Solidity, Rust, Move, etc.
  → audit each separately
  → limited by each chain's VM

Craftec:
  Logic lives in CraftSQL (one place)
  → chains are just deposit/withdrawal addresses
  → one codebase, one audit
  → unlimited by any chain's constraints
```

### How It Works

Each supported chain has a multisig wallet (not even a smart contract in most cases) operated by gateway agents (CraftCPU). All logic runs in CraftSQL.

```
User deposits USDC on any chain
↓
chain-watcher agent sees it
↓
INSERT INTO deposits (did, amount, chain, tx_hash, confirmations)
VALUES ('did:alice', 100, 'ethereum', '0xabc...', 12);
↓
UPDATE balances SET amount = amount + 100 WHERE did = 'did:alice';
↓
Alice transfers to Bob (pure CraftSQL, instant, free)
↓
Bob requests withdrawal on Base
↓
INSERT INTO withdrawals (did, amount, chain, destination_addr, status)
VALUES ('did:bob', 50, 'base', '0xdef...', 'pending');
↓
withdrawal-processor agents read pending withdrawals
3-of-5 threshold signature → release from Base multisig
↓
Bob receives USDC on Base
```

### What the User Sees

```
Deposit:  "Send USDC to this address on [any chain]" → balance appears
Transfer: Pure CraftSQL. No chain involved. Instant. Free.
Withdraw: "Send my USDC to [any chain]" → arrives
```

Once money is inside Craftec, it's chain-agnostic. The CraftSQL balance doesn't know or care where the USDC came from.

### Per-Chain Deployment

| Chain | What's Deployed | Why First |
|---|---|---|
| Solana | Escrow program | Cheapest, fastest, existing expertise |
| Base | Multisig wallet | Largest L2 user base |
| Ethereum | Multisig wallet | Institutional money |
| Arbitrum | Multisig wallet | DeFi-heavy users |

Start with Solana. Add chains as demand appears. Each chain is just a new watched address + gateway agent. No new contract development needed.

### CCTP for Rebalancing

CCTP (Circle's Cross-Chain Transfer Protocol) becomes an internal treasury tool, not user-facing plumbing.

```
Problem: ETH escrow has $1M, Base escrow has $100
         User wants to withdraw $500 on Base

Solution: Rebalancer agent triggers CCTP
          ETH escrow → CCTP burn → mint on Base → Base escrow
          Now Base has $600, withdrawal succeeds
```

CCTP uses burn-and-mint (no wrapped tokens, no liquidity pools). Circle attests each transfer. Native USDC on both sides. Supports 15+ chains.

Craftec's rebalancer agent automates this entirely.

### Beyond USDC

The architecture isn't limited to USDC. Any asset, any chain, same SQL:

```sql
-- Gateway watches Bitcoin address
INSERT INTO deposits (did, amount, asset, chain, tx_hash)
VALUES ('did:alice', 0.5, 'BTC', 'bitcoin', 'abc...');

-- Gateway watches USDT on Tron
INSERT INTO deposits (did, amount, asset, chain, tx_hash)
VALUES ('did:bob', 200, 'USDT', 'tron', 'def...');
```

Any chain that can hold tokens and confirm transactions can be an I/O port. The gateway just needs to watch addresses and verify finality.

### Auditability

Every deposit and withdrawal logged in CraftSQL — public, verifiable. Mismatch between on-chain balance and CraftSQL balance is instantly detectable:

```sql
SELECT
  (SELECT SUM(amount) FROM deposits WHERE chain='ethereum')
  - (SELECT SUM(amount) FROM withdrawals WHERE chain='ethereum' AND status='complete')
  AS expected_onchain_balance;

-- Compare with actual on-chain balance
-- Any discrepancy = proof of fraud
```

This is the world's most transparent exchange: order book, balances, and audit trail are all public CraftSQL databases — with no company, no server, no single point of failure.

### The Architecture

```
Layer 1: Blockchains (ETH, Solana, Base, Arb, BTC...) = parking lots (hold assets)
Layer 2: Escrow wallets on each chain                  = entrance/exit gates
Layer 3: CraftCPU gateway agents                       = watchers, signers, rebalancers, auditors
Layer 4: Craftec financial chain (CraftSQL)             = the actual city where everything happens
Layer 5: CCTP                                          = highway between parking lots (rebalancing)
```

The multichain answer isn't making Craftec run on multiple chains. It's making multiple chains deposit into one Craftec.

---

## Data Growth

Storage grows linearly, not exponentially. A write only creates new CIDs for changed pages.

```
1 user, 10 transactions/day: ~1.5MB/year
1 million users: ~1.5TB/year
With 30x replication: ~45TB/year across the whole network
```

Natural pruning through churn: old unreferenced CIDs fade on the free tier. Active data heals. Forgotten data dies.

Users choose their own retention policy.

---

## The Principle

Everything is CIDs. Everything is pages. One protocol.

```
A file             = a CID
A database page    = a CID
A directory listing = a query on CIDs
A snapshot         = a saved CID
A filesystem       = a table of CIDs
An agent's state   = a CID
An agent's code    = a CID
A program          = a CID (with a derived key)
A ZK proof         = a CID
A website          = a folder of CIDs
```

The products organize CIDs into increasingly useful structures:

1. **craftec-core**: P2P connectivity, identity, crypto, erasure — the foundation
2. **CraftOBJ**: raw CIDs — store, retrieve, heal
3. **CraftSQL**: CIDs as B-tree pages — query, index, transact
4. **CraftVFS**: CraftSQL tables with path semantics — browse, mount, snapshot
5. **CraftSEC**: programs as CIDs with MPC-derived keys — validate, attest, sign
6. **CraftNET**: encrypted tunnels over the P2P network — VPN

Each product is a thin abstraction. The power comes from composition, not complexity.

---

## Priority Order

```
1. CraftOBJ — reliable store and retrieve (foundation)
2. CraftSQL — SQLite VFS on CraftOBJ (makes it useful)
3. CraftSEC — MPC threshold functions (the trust layer)
4. CraftCOM — persistent compute agents (gateway for financial layer)
5. CraftVFS — filesystem on CraftSQL (makes it accessible)
6. CraftNET — decentralized VPN (when network is mature)
```

CraftSEC is #3 because the financial network — the first product — requires MPC attestation for every transaction. Without CraftSEC, users can write anything to their chains. With it, every entry is cryptographically validated.

CraftCPU follows immediately because gateway agents depend on CraftSEC for threshold signing of cross-chain withdrawals.

---

## Repos

```
craftec/craftec-core  ← shared infrastructure (identity, crypto, erasure, P2P transport)
craftec/craftobj      ← object storage (depends on craftec-core)
craftec/craftsql      ← SQLite VFS (depends on craftobj)
craftec/craftsec      ← MPC threshold functions (depends on craftsql)
craftec/craftcom      ← persistent compute agents, CPU + GPU (depends on craftsql, craftsec)
craftec/craftvfs      ← filesystem (depends on craftsql)
craftec/craftnet      ← decentralized VPN (depends on craftec-core)
```

One org. One repo per component. Separate repos enforce clean dependency boundaries.
