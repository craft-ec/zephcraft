# SQL-backed Registry — Design (2026-07-08)

> **STATUS: DESIGN → building.** Replaces the per-shard `RegistryState` postcard blob with a
> per-shard **CraftSQL database**, so registry writes/resolves/replication/durability scale as
> O(1)/O(changed) instead of O(rows-in-shard). Motivated by the target topology (thousands of
> nodes, ~80% NAT readers, ~20% reachable writer/replica backbone) where blob write-amplification
> and whole-shard replication flood the scarce writer tier. See the session discussion in
> `.claude/feature-progress.md`.

## 1. Granularity — one CraftDb per `(rtype, bits, shard)`

One CraftSQL DB per shard account, 1:1 with today's `ProgramAccountStore` account. This PRESERVES
the sharding model (per-shard election, K-replication, rotating writer, generation/reshard,
drain/GC) — sharding stays *physical* (per account), not a logical column. At scale each writer
holds a **bounded** number of substantial DBs (e.g. 4096 shards ×3 / hundreds of writers ≈ tens),
so the per-DB overhead amortizes; it is only wasteful at tiny scale (few writers, near-empty
shards), which is an accepted transitional cost.

DB namespace (per node, `CraftSql.owner = self`): `reg/<rtype>/<bits>/<shard>` (a stable string).
The engine's single writer identity is this node; the namespace distinguishes shards.

## 2. Breaking the recursion — blob-backed RootStore

The registry stores user DB *roots* (RT_DBROOT). If a registry shard IS a DB, its root must NOT
route back through the registry. So the shard-state `CraftSql` engine uses a **blob-backed
`RootStore`** that stores each shard-DB's root cid in that shard's `ProgramAccountStore` account
(`pda(registry_program_cid(), shard_seed(sk))`), NOT via `HeadRegistry`:

- `RootStore::resolve(self, ns)` → read the ~40-byte `(root_cid, seq)` out of the account blob.
- `RootStore::publish(ns, root, _prev, seq)` → write `(root_cid, seq)` back into the account blob.

The DB *pages* live in CraftOBJ (via `ObjDurable` + a `PageSource`). The account holds only the
pointer. `zeph-sql` sits below `noded`, so `noded` may depend on it (no cycle).

## 3. Two engines (wiring order)

1. `ProgramAccountStore::open(...)` (first).
2. **`ShardRootStore`** (blob-backed, over the account store) → **shard-state `CraftSql` engine**
   (`with_durable(ObjDurable)`, `with_source(page source)`), built BEFORE the registry.
3. `HeadRegistry::open(store, shard_sql_engine, ...)` — now holds the shard engine.
4. User-facing `CraftSql` (RegistryRootStore over HeadRegistry) — built AFTER, unchanged.

Two engines, both `owner = self`, distinct VFS (`VFS_COUNTER` supports it).

## 4. Schema (per shard DB)

```sql
CREATE TABLE IF NOT EXISTS heads (
  owner   BLOB    NOT NULL,   -- [u8;32]
  name    TEXT    NOT NULL,
  cid     BLOB    NOT NULL,   -- [u8;32]
  version INTEGER NOT NULL,
  PRIMARY KEY (owner, name)
);
```

`(owner, name)` PK gives the indexed resolve. No shard column needed (the DB *is* the shard).

## 5. Validation — NATIVE (no WASM hook)

Per the decision (memory `registry-native-validation-not-wasm-hook`): validate each submission in
native Rust — `HeadSubmission::verify()` (owner sig) + name length ≤ 32 (the old char-limit v2,
inline) — then a version-guarded upsert. The governed-WASM validator is DROPPED (mechanism, not
swappable policy). The `ProgramAccountStore::advance` WASM path is no longer used by the registry.

## 6. Operation mapping

- **register / advance_local(sub):** `verify()` + char-limit → `INSERT INTO heads … ON CONFLICT
  (owner,name) DO UPDATE SET cid=…, version=… WHERE excluded.version > heads.version`. Writes go
  through `CraftDb::write(sql)` (string API — the only path that commits + sweeps durability).
  Row values are hex-literal BLOBs (`X'…'`) and quote-escaped TEXT (`'` → `''`) — names are
  untrusted, so escaping is mandatory (SQL-injection guard).
- **resolve / resolve_local(owner,name):** `SELECT cid,version FROM heads WHERE owner=X'…' AND
  name='…'` via `CraftDb::conn()` (reads, parameterized).
- **current_version:** `SELECT version …`.
- **status / rows / local_head_rows / entries:** `SELECT [*|COUNT(*)] FROM heads`.
- **Replication (the big scale win): row-level push.** `PushState` carries ONE submission
  (`Vec<u8>`), the replica upserts it — NOT the whole shard. Kills the O(rows) network
  amplification. `GetState` (takeover merge, rare) returns `SELECT *` serialized (still whole-shard,
  but only on writer rotation).
- **reshard sweep(from→to):** `SELECT *` each held `from` shard DB → rebucket by `to` → upsert
  rows into the `to` shard DBs (+ row-push to their replicas).
- **GC(bits):** drop each held shard DB at `bits` — delete its local pages/sidecar, clear the
  account root, evict it from the DB cache.

## 7. DB cache

Opening a `CraftDb` per op is expensive (blocking + head resolve + sync). Cache open handles:
`HashMap<namespace, Arc<Mutex<CraftDb>>>`, get-or-open per shard. `write` needs `&mut` (behind the
Mutex); reads use `conn()` under the lock. One writer per shard, so per-shard serialization is
free. GC evicts the entry.

## 8. Cutover

The account blobs currently hold `RegistryState` (postcard). The SQL binary reads them as DB roots
→ garbage → effectively a fresh registry. Per the established wipe-and-restart posture (no live
data to preserve), this is an accepted one-time cutover: deploy → old blobs ignored → fresh shard
DBs → re-deploy programs.

## 9. Phases

- **P1 — storage swap (single-node correctness):** ShardRootStore + shard engine + DB cache +
  schema; rewrite register/resolve/current_version/status/rows/entries to SQL. Native validation.
  Blob path deleted. Unit + single-node proof.
- **P2 — cross-node:** row-level `PushState` (one submission) + `GetState` (`SELECT *`) takeover
  merge; the Submit/Resolve/CurrentVersion forwarding unchanged. Cluster resolve proof.
- **P3 — reshard on SQL:** sweep = SELECT+upsert; GC = drop DB. Grow/shrink proof on the cluster.
- **P4 — cleanup:** remove dead blob/`RegistryState`-persistence code; keep `RegistryState`/
  `HeadEntry` only as the wire/merge DTO for GetState. Deploy + full live re-test.
