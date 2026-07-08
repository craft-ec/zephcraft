//! The node's durable owner-signed HEAD registry — program names, CraftSQL DB roots, and
//! durability manifests (RT_PROGRAM / RT_DBROOT / RT_MANIFEST) — a thin consumer of the
//! [`ProgramAccountStore`] substrate. `deploy` registers a signed head by advancing a
//! per-shard registry account (`pda(registry_program_cid(), shard_seed(shard))`); resolution
//! reads that account's state. The account state itself is persisted + published durably by
//! the store — this type holds no state of its own.
//!
//! Authority: the registry is an **open, owner-signed CRDT** (partition-by-owner,
//! last-writer-wins per `(owner, name)`) — it converges by construction, so writes need
//! NO attestation / committee. The store runs the governance-canonical registry program
//! LOCALLY to validate an owner-signed submission, then merges. See
//! `docs/VERIFICATION_DESIGN.md` §2 and `docs/REGISTRY_DESIGN.md` §2.1.
//!
//! Sharding: the keyspace is split into `2^shard_bits` shards. `shard_bits` is a GOVERNED value
//! (a `SetConfig` on the governance chain), so every node agrees on the count. Every `(owner,
//! name)` key routes to exactly ONE shard via [`shard_of`] (the low `bits` of the key hash); each
//! `(rtype, generation, shard)` is its own account (seeded by [`shard_seed`]) with its own
//! independent rotating-writer election, so different shards may be written by different nodes and
//! the write load spreads across the membership.
//!
//! Online resharding: because `shard_seed` encodes the shard-count GENERATION (`bits`), the count
//! can change on a LIVE cluster with no wipe — [`HeadRegistry::reshard_round`] split/merges each
//! held shard's heads from the old generation into the new one, and reads fall through to the
//! adjacent generation during the migration window. Low-bit routing keeps a split LOCAL (a parent
//! shard's keys go only to its two children).

use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use zeph_com::{registry_program_cid, HeadEntry, HeadSubmission, RegistryState, REGISTRY_SEED};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::account::ProgramAccountStore;
use crate::registry_net::{request_registry, HeadRowWire, RegistryReq, RegistryResp};

/// Max size of a registry request/response frame served over `REGISTRY_ALPN`.
const MAX_FRAME: usize = 256 * 1024;

/// Length of a registry writer-election epoch, in milliseconds. Short cycle (fast rotation),
/// tunable. `epoch = clock.now().millis() / EPOCH_MILLIS`.
const EPOCH_MILLIS: u64 = 30_000;

/// Default shard-count exponent: the keyspace starts as `2^DEFAULT_SHARD_BITS` shards. Applied
/// when the governed `shard_bits` config value is unset (or before governance is wired), so a
/// fresh network self-starts. `DEFAULT_SHARD_BITS = 8` → 256 shards, matching the prior fixed
/// count exactly. The live value is read from governance (see [`HeadRegistry::shard_bits`]).
const DEFAULT_SHARD_BITS: u32 = 8;

/// Upper bound on the governed `shard_bits`. The registry's status / migrate / enumeration loops
/// are O(2^bits), so an out-of-range governance value is clamped here to keep them bounded.
/// 12 → up to 4096 shards, ample headroom over the default 256; the O(shards) loops remain the
/// real scaling ceiling to lift later (see `.claude/feature-progress.md`).
const MAX_SHARD_BITS: u32 = 12;

/// Consecutive migration-loop ticks the census must be UNCHANGED before state migration runs.
/// The loop ticks every 10s, so this debounces migration to ~30s of stable membership — during
/// convergence/churn the census changes every tick, which must NOT trigger the scan+push storm.
const MIGRATE_STABLE_TICKS: u32 = 3;

/// After a reshard, how many loop ticks to keep DRAINING the old generation (re-sweeping it
/// forward to catch writes that landed on it during the governance-propagation window) before
/// GC-ing its accounts. The loop ticks every 10s and propagation is ~20s, so 6 (~60s) generously
/// covers the window in which a straggler node still writes the old count.
const DRAIN_TICKS: u32 = 6;

/// Registry KIND tags — each kind is a SEPARATE account per shard (the tag is folded into
/// [`shard_seed`], so `(rtype, shard)` addresses distinct state). Lets program heads, database
/// roots, and manifests share one substrate without colliding.
pub const RT_PROGRAM: u8 = 0;
/// CraftSQL DB roots (`KIND_ROOT`) and durability manifests (`KIND_MANIFEST`) now ride this same
/// substrate (type-in-seed): each `(rtype, shard)` is a distinct account, so program heads, DB
/// roots, and manifests never collide. See `registry_heads.rs`.
pub const RT_DBROOT: u8 = 1;
pub const RT_MANIFEST: u8 = 2;

/// How many replicas hold each shard's state. The writer ROTATES among this stable set and
/// pushes every write to the others, so if the writer dies a warm successor already holds the
/// state (the offline-writer fault the single-writer model could not survive).
const REPLICATION_FACTOR: usize = 3;

/// Resolve-cache TTL. A NON-replica node caches a remote resolve for this long so repeated reads
/// of the same key skip the 8s-bounded network round-trip — this is what takes a hot shard's
/// writer from "tens → thousands" of readers. Short enough that another node's write becomes
/// visible within it; this node's own `register()` invalidates the entry immediately
/// (read-your-writes). Replicas never use it — they read authoritative local state.
const RESOLVE_CACHE_TTL_MS: u64 = 3_000;

/// Identifies one registry account = one `(kind, generation, shard)`. `bits` is the shard-count
/// GENERATION the account belongs to, so `(rtype, 8, 5)` and `(rtype, 9, 5)` are DISTINCT
/// accounts — the split/merge reshard reads the old generation and writes the new one without the
/// two colliding. NOTE the writer election ([`HeadRegistry::replicas`]) deliberately ignores
/// `bits` and keys only on `(rtype, shard)`, so a shard number maps to a STABLE replica set across
/// generations (parent shard `s` and child-0 `s` share replicas → migration locality).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ShardKey {
    rtype: u8,
    bits: u32,
    shard: u64,
}

/// Boundary-race grace window, in milliseconds. During the first `GRACE_MILLIS` of a new epoch
/// the PREVIOUS epoch's writer stays authoritative (see [`HeadRegistry::effective_epoch`]),
/// so a bounded clock skew (< grace) can't produce two live writers at a boundary.
const GRACE_MILLIS: u64 = 2_000;

/// Route a `(owner, name)` key to its shard = the low `bits` of the key hash (so `bits = 8`
/// reproduces the prior `% 256`). Low-bit routing makes split/merge LOCAL: doubling the count
/// (`bits → bits+1`) sends shard `s`'s keys to children `s` and `s | (1 << bits)` by one new bit,
/// so only that shard's keys move and only to its two children. Register and resolve of the same
/// key MUST use the same `bits`, so this ONE function is the only router.
fn shard_of(owner: &[u8; 32], name: &str, bits: u32) -> u64 {
    let h = Cid::of(&[owner.as_slice(), name.as_bytes()].concat()).0;
    let mask = (1u64 << bits) - 1;
    u64::from_le_bytes(h[..8].try_into().unwrap()) & mask
}

/// Map the governed `shard_bits` config value to the live exponent: the value if set, else the
/// built-in [`DEFAULT_SHARD_BITS`] (unset, or governance not yet wired), clamped to
/// `[1, MAX_SHARD_BITS]` so a bad/hostile governance value can't drive the O(2^bits) shard loops
/// out of bounds. Pure (no I/O) so the fallback + clamp is unit-testable in isolation.
///
/// OPERATOR NOTE — changing the count is an ONLINE reshard, no wipe needed. A governance
/// `SetConfig{"shard_bits", n}` is picked up by every node ([`HeadRegistry::shard_bits`]); each
/// node's [`HeadRegistry::reshard_round`] then split/merges its held shards from the old
/// generation into the new one (`shard_seed` encodes the generation, so the two never collide),
/// and [`HeadRegistry::resolve_entry`] reads through to the adjacent generation during the window.
/// Change one bit at a time (±1) and let the cluster settle between steps.
fn resolve_shard_bits(governed: Option<i64>) -> u32 {
    governed
        .map(|v| v.clamp(1, MAX_SHARD_BITS as i64) as u32)
        .unwrap_or(DEFAULT_SHARD_BITS)
}

/// The per-account seed — replaces the bare `REGISTRY_SEED` in every account op so each
/// `(rtype, generation, shard)` gets a distinct `pda(registry_program_cid(), shard_seed(sk))`.
/// The KIND tag is folded in FIRST (a program head and a DB root at the same shard live in
/// separate accounts — type-in-seed), then the shard-count GENERATION `bits`, so the same shard
/// number at two counts addresses two accounts — which is what lets an online reshard read the
/// old generation and write the new one without clobbering it.
fn shard_seed(sk: ShardKey) -> Vec<u8> {
    [
        REGISTRY_SEED,
        &[sk.rtype],
        sk.bits.to_le_bytes().as_slice(),
        sk.shard.to_le_bytes().as_slice(),
    ]
    .concat()
}

/// Seed of the per-node GENERATION MARKER account — records the shard-count `bits` this node
/// last resharded to, so on the next tick it can detect a governed `shard_bits` change and run the
/// split/merge exactly once. Deliberately NOT a [`shard_seed`] output (own prefix), so it never
/// collides with a real `(rtype, bits, shard)` account. Persisted like any account (survives
/// restart), so a node that was down during a reshard catches up when it returns.
const GEN_MARKER_SEED: &[u8] = b"craftec/registry/shard-generation/1";

/// Re-bucket a batch of heads to their shards at `new_bits` — the pure core of the online reshard.
/// Every entry is routed by [`shard_of`] at the NEW count and grouped by its new shard, so the
/// caller can write each group into that new-generation account. Handles grow (a parent's keys
/// fan out to two children), shrink (two children funnel into a parent), and any multi-step jump
/// uniformly, because it simply re-routes each key at the target count.
fn rebucket_entries(
    entries: &[HeadEntry],
    new_bits: u32,
) -> std::collections::HashMap<u64, Vec<HeadEntry>> {
    let mut grouped: std::collections::HashMap<u64, Vec<HeadEntry>> =
        std::collections::HashMap::new();
    for e in entries {
        let shard = shard_of(&e.owner, &e.name, new_bits);
        grouped.entry(shard).or_default().push(e.clone());
    }
    grouped
}

/// Resolve-cache backing map: `(rtype, owner, name)` → `((cid, version), expiry_ms)`.
type ResolveCacheMap = std::collections::HashMap<(u8, [u8; 32], String), (([u8; 32], u64), u64)>;

/// A TTL'd cache of `(rtype, owner, name)` → `(cid, version)` resolves. The clock is injected
/// (`now` is passed in) so the cache is unit-testable without a live registry. Consulted only for
/// NON-replica reads — a replica reads authoritative local state (see [`HeadRegistry`]).
#[derive(Default)]
struct ResolveCache {
    map: RwLock<ResolveCacheMap>,
}

impl ResolveCache {
    /// The cached `(cid, version)` for the key, or `None` if absent or expired at `now`.
    async fn get(
        &self,
        rtype: u8,
        owner: &[u8; 32],
        name: &str,
        now: u64,
    ) -> Option<([u8; 32], u64)> {
        match self
            .map
            .read()
            .await
            .get(&(rtype, *owner, name.to_string()))
        {
            Some((entry, expiry)) if *expiry > now => Some(*entry),
            _ => None,
        }
    }

    /// Cache `entry` for the key until `now + RESOLVE_CACHE_TTL_MS`.
    async fn put(&self, rtype: u8, owner: [u8; 32], name: &str, entry: ([u8; 32], u64), now: u64) {
        self.map.write().await.insert(
            (rtype, owner, name.to_string()),
            (entry, now + RESOLVE_CACHE_TTL_MS),
        );
    }

    /// Drop any cached entry for the key (after this node writes it).
    async fn invalidate(&self, rtype: u8, owner: &[u8; 32], name: &str) {
        self.map
            .write()
            .await
            .remove(&(rtype, *owner, name.to_string()));
    }
}

/// The node's durable owner-signed HEAD registry — a thin consumer of [`ProgramAccountStore`].
///
/// Cross-node model: writer election is TWO-STAGE, computed identically on every node from its
/// membership view + HLC clock, and independent per shard. (1) A STABLE K-replica set = the
/// [`REPLICATION_FACTOR`] eligible members (self + active membership) with the lowest
/// `blake3(rtype ‖ shard_le ‖ node_id)` — NOTE: NO epoch term, so this set shifts only on
/// membership change and a fixed group holds each account's state. (2) The writer for an epoch
/// is `replicas[effective_epoch % replicas.len()]` — the role ROTATES through that stable set,
/// while the other replicas stay warm followers already carrying the state. If this node is a
/// shard's current writer it advances and resolves that shard locally; otherwise it forwards
/// registrations and queries to the shard's current writer over `REGISTRY_ALPN`. As the epoch
/// rotates the writer role moves to the next replica, which already holds the state (see
/// [`Self::replicas`] / [`Self::current_writer`]).
pub struct HeadRegistry {
    identity: Arc<NodeIdentity>,
    /// Shared generic program-account store — each shard's state lives in the account
    /// `pda(registry_program_cid(), shard_seed(shard))` here.
    store: Arc<ProgramAccountStore>,
    /// Governance chain — resolves the EXECUTING registry program cid (upgradeable).
    programs: RwLock<Option<Arc<crate::governance::GovernanceChainStore>>>,
    /// HLC clock — drives the epoch (`now().millis() / EPOCH_MILLIS`) that elects the writer.
    clock: Arc<zeph_core::hlc::Clock>,
    /// Transport for forwarding to a shard's current writer (non-writer nodes only).
    transport: Arc<Transport>,
    /// Membership — the active set feeds the election AND locates a writer's dialable
    /// [`PeerAddr`]. Wired after open.
    membership: RwLock<Option<Arc<Membership>>>,
    /// This node's own id.
    self_id: [u8; 32],
    /// Per `(rtype, shard)`: the last effective epoch for which this node (as that account's
    /// writer) has performed the takeover merge. Guards [`Self::ensure_current`] so the
    /// merge-from-replicas runs once per epoch per account.
    last_epoch: RwLock<std::collections::HashMap<ShardKey, u64>>,
    /// `(digest of the census, consecutive ticks it has been UNCHANGED)`. The migration loop
    /// re-replicates held shards to their new replica set ONLY after the census has been STABLE
    /// for [`MIGRATE_STABLE_TICKS`] ticks — never on every change. Critical: during a join storm
    /// the census changes on EVERY gossip tick, so firing per-change would storm the registry and
    /// contend with the gossip, stalling the very convergence it depends on. Debounced → migration
    /// runs once per settled membership. `None` until the first observation.
    last_census: RwLock<Option<([u8; 32], u32)>>,
    /// Read cache for NON-replica resolves: `(rtype, owner, name)` → `((cid, version), expiry_ms)`.
    /// A replica reads its shard locally and never consults this; a non-replica otherwise pays a
    /// network round-trip per resolve. TTL'd ([`RESOLVE_CACHE_TTL_MS`]) for other-node writes;
    /// [`Self::register`] invalidates the key so this node's own writes are read-your-writes.
    resolve_cache: ResolveCache,
    /// Reshard drain state: `Some((old_gen, ticks))` while an OLD shard-count generation is being
    /// drained after a reshard — [`Self::reshard_round`] keeps re-sweeping `old_gen` forward for
    /// [`DRAIN_TICKS`] ticks (catching writes that landed there during the propagation window),
    /// then GC's its accounts and clears this back to `None`. In-memory (a restart just leaves the
    /// old generation un-GC'd, which is harmless — reads resolve at the current generation).
    draining: RwLock<Option<(u32, u32)>>,
}

/// One head row for the dashboard — hex-encoded `(owner, cid)` + `name` + `version`.
pub struct HeadRow {
    pub owner: String,
    pub name: String,
    pub cid: String,
    pub version: u64,
}

/// Registry heads grouped by registry type (program heads / DB roots / manifests). From
/// [`HeadRegistry::entries`] this is a per-node partial view (only this node's shards); from
/// [`HeadRegistry::entries_global`] it is the network-wide union gathered across members.
#[derive(Default)]
pub struct RegistryEntries {
    pub programs: Vec<HeadRow>,
    pub dbroots: Vec<HeadRow>,
    pub manifests: Vec<HeadRow>,
    /// How many nodes contributed to this view (1 = node-local; N = self + peers that answered
    /// the global gather in time). Surfaced to the UI as "gathered across N nodes".
    pub contributors: usize,
}

/// Registry status for the dashboard. `writer_shards` = how many RT_PROGRAM shards this node
/// currently writes (of the live `2^shard_bits`); the per-type counts are the `(owner, name)` rows
/// across exactly the shards this node writes for each type — a per-node partial view.
pub struct RegistryStatus {
    pub epoch: u64,
    pub eligible: usize,
    pub writer_shards: usize,
    /// The live shard count (`2^shard_bits`) — dynamic, so reported rather than assumed 256.
    pub shard_count: u64,
    pub program_heads: usize,
    pub dbroots: usize,
    pub manifests: usize,
}

impl HeadRegistry {
    /// Open the registry over a shared program-account store. The `clock` drives the per-epoch
    /// per-shard writer election — the writer is computed, not configured.
    pub fn open(
        identity: Arc<NodeIdentity>,
        store: Arc<ProgramAccountStore>,
        clock: Arc<zeph_core::hlc::Clock>,
        transport: Arc<Transport>,
    ) -> Self {
        let self_id = identity.node_id().0;
        Self {
            identity,
            store,
            programs: RwLock::new(None),
            clock,
            transport,
            membership: RwLock::new(None),
            self_id,
            last_epoch: RwLock::new(std::collections::HashMap::new()),
            last_census: RwLock::new(None),
            resolve_cache: ResolveCache::default(),
            draining: RwLock::new(None),
        }
    }

    /// The live shard-count exponent = the governed `shard_bits` config value, agreed
    /// cluster-wide via the governance chain (a `SetConfig` approval), so every node routes a
    /// key to the SAME shard. Falls back to [`DEFAULT_SHARD_BITS`] when the value is unset or
    /// before governance is wired, and is clamped to `[1, MAX_SHARD_BITS]` so a bad governance
    /// value can't blow up the O(2^bits) shard loops. Read ONCE per operation (register /
    /// resolve / status / migrate) and threaded into [`shard_of`], so a single op stays
    /// internally consistent even if governance changes mid-op.
    async fn shard_bits(&self) -> u32 {
        let governed = match self.programs.read().await.as_ref() {
            Some(p) => p.resolve_config("shard_bits").await,
            None => None,
        };
        resolve_shard_bits(governed)
    }

    /// State-migration anti-entropy: when the census changes, the stable replica set of each
    /// shard shifts, so re-replicate every shard THIS node holds to its CURRENT replica set —
    /// newly-elected replicas receive the state, and a node that is no longer a replica hands its
    /// (orphaned) copy off to the current set. This is what makes membership ELASTIC: state
    /// follows the election on join/leave. EVENT-DRIVEN — it only does work when the census
    /// digest changes, so it adds NO registry traffic while membership is stable (avoiding the
    /// per-cycle reannounce storm class of bug). Pushes are fire-and-forget via `replicate`.
    async fn migrate_round(&self) {
        let mut ids = self.eligible().await;
        ids.sort();
        let digest = Cid::of(&ids.concat()).0;
        // Debounce: migrate only once the census has been UNCHANGED for MIGRATE_STABLE_TICKS ticks.
        // During convergence/churn the census changes every tick — firing the scan+push then would
        // storm the registry and stall the very gossip convergence it depends on.
        let should_migrate = {
            let mut g = self.last_census.write().await;
            match g.as_mut() {
                Some((d, ticks)) if *d == digest => {
                    if *ticks < MIGRATE_STABLE_TICKS {
                        *ticks += 1;
                        *ticks == MIGRATE_STABLE_TICKS // fire exactly once, on reaching the threshold
                    } else {
                        false // already migrated for this settled census
                    }
                }
                _ => {
                    *g = Some((digest, 0)); // census changed (or first) — reset stability, don't migrate
                    false
                }
            }
        };
        if !should_migrate {
            return;
        }
        // Census has SETTLED: re-replicate every shard we hold local state for to its new replica
        // set. Operates at the CURRENT generation (`bits`) — the reshard loop handles generation
        // change separately.
        let bits = self.shard_bits().await;
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..(1u64 << bits) {
                let sk = ShardKey { rtype, bits, shard };
                let state = self
                    .store
                    .resolve(registry_program_cid(), &shard_seed(sk))
                    .await;
                if !state.is_empty() {
                    self.replicate(sk, &state).await;
                }
            }
        }
    }

    /// This node's last-resharded generation (the `shard_bits` it last migrated to), or `None`
    /// before it has ever recorded one.
    async fn load_gen(&self) -> Option<u32> {
        let raw = self
            .store
            .resolve(registry_program_cid(), GEN_MARKER_SEED)
            .await;
        (raw.len() == 4).then(|| u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    /// Record `bits` as this node's current generation (persisted, so a restart resumes correctly).
    async fn save_gen(&self, bits: u32) {
        let _ = self
            .store
            .put_state(registry_program_cid(), GEN_MARKER_SEED, &bits.to_le_bytes())
            .await;
    }

    /// ONLINE RESHARD — the split/merge that lets a live cluster change its shard count without a
    /// wipe. Detects that the governed `shard_bits` differs from the generation this node last
    /// migrated to, sweeps every head from the OLD generation into the NEW one, then DRAINS the old
    /// generation (keeps re-sweeping it to catch late writes) and finally GC's it. Gated so the
    /// common (stable, not-draining) path is one governance read + one marker read — no traffic
    /// while the count is unchanged, mirroring `migrate_round`'s event-driven discipline.
    ///
    /// Correctness / convergence:
    /// - MERGE-FORWARD, never overwrite: [`Self::sweep_generation`] LWW-merges into the new
    ///   accounts, so nothing is lost and the sweep is idempotent (safe to repeat during the drain).
    /// - DRAIN window closes the "late write" gap: after the switch, some peers are briefly still on
    ///   the old count and write to the old generation. We keep sweeping the old generation forward
    ///   for [`DRAIN_TICKS`] ticks (>> the governance-propagation window), so those late writes are
    ///   carried to the new generation rather than stranded. Reads also fall through to the adjacent
    ///   generation ([`Self::resolve_entry`]) meanwhile, so nothing is unresolvable in the interim.
    /// - GC: once drained, [`Self::gc_generation`] deletes this node's old-generation account files
    ///   (each replica GC's its own copy on the same schedule) — the old generation is gone for good.
    /// - Crash safety: `save_gen` marks the new generation before draining; a restart mid-drain just
    ///   leaves the old generation un-GC'd (harmless — reads resolve at the current generation).
    async fn reshard_round(&self) {
        let current = self.shard_bits().await;
        let last = match self.load_gen().await {
            Some(b) => b,
            // First run on this node: adopt the current generation without migrating (nothing to
            // move — a fresh account set is already at `current`).
            None => {
                self.save_gen(current).await;
                return;
            }
        };
        if last != current {
            // Generation just changed: sweep old→new, mark the new generation, and begin draining
            // the old one (re-swept each tick below until DRAIN_TICKS, then GC'd).
            self.sweep_generation(last, current).await;
            self.save_gen(current).await;
            *self.draining.write().await = Some((last, 0));
            return;
        }
        // Stable generation. If an old generation is still draining, keep sweeping it forward to
        // catch writes that landed on it during the propagation window, then GC once fully drained.
        let drain = *self.draining.read().await;
        if let Some((old, ticks)) = drain {
            if old == current {
                *self.draining.write().await = None; // safety: never drain the live generation
                return;
            }
            self.sweep_generation(old, current).await;
            if ticks + 1 >= DRAIN_TICKS {
                self.gc_generation(old).await;
                *self.draining.write().await = None;
            } else {
                *self.draining.write().await = Some((old, ticks + 1));
            }
        }
    }

    /// Re-bucket every head this node holds at generation `from` into the accounts of generation
    /// `to`, LWW-merging each group into the target account and pushing it to that account's replica
    /// set. The core migration step of a reshard; idempotent, so it is safe to run repeatedly during
    /// the drain window. Only sweeps shards this node replicates at `from` (election is
    /// bits-independent, so that set matches its current replica assignment).
    async fn sweep_generation(&self, from: u32, to: u32) {
        let elig = self.eligible().await;
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            let mut entries: Vec<HeadEntry> = Vec::new();
            for shard in 0..(1u64 << from) {
                let sk_old = ShardKey {
                    rtype,
                    bits: from,
                    shard,
                };
                if !Self::replicas(sk_old, &elig).contains(&self.self_id) {
                    continue;
                }
                let raw = self
                    .store
                    .resolve(registry_program_cid(), &shard_seed(sk_old))
                    .await;
                if let Some(s) = RegistryState::decode(&raw) {
                    entries.extend(s.entries().iter().cloned());
                }
            }
            if entries.is_empty() {
                continue;
            }
            for (new_shard, group) in rebucket_entries(&entries, to) {
                let sk_new = ShardKey {
                    rtype,
                    bits: to,
                    shard: new_shard,
                };
                let seed = shard_seed(sk_new);
                let mut local =
                    RegistryState::decode(&self.store.resolve(registry_program_cid(), &seed).await)
                        .unwrap_or_default();
                local.merge_entries(group);
                let bytes = local.encode();
                let _ = self
                    .store
                    .put_state(registry_program_cid(), &seed, &bytes)
                    .await;
                self.replicate(sk_new, &bytes).await;
            }
        }
    }

    /// GC a fully-drained generation: delete this node's local account state for every shard it
    /// holds at generation `bits` (all rtypes). Called only after [`DRAIN_TICKS`] of sweeping, by
    /// which point the old generation has been carried forward and no peer still writes it, so the
    /// files are dead weight. Local-only; other replicas GC their own copies on the same schedule.
    async fn gc_generation(&self, bits: u32) {
        let elig = self.eligible().await;
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..(1u64 << bits) {
                let sk = ShardKey { rtype, bits, shard };
                if !Self::replicas(sk, &elig).contains(&self.self_id) {
                    continue;
                }
                self.store
                    .clear(registry_program_cid(), &shard_seed(sk))
                    .await;
            }
        }
    }

    /// Wire the governance chain so the app-registry program cid is resolved THROUGH it
    /// (upgradeable) rather than hardcoded.
    pub async fn set_programs(&self, programs: Arc<crate::governance::GovernanceChainStore>) {
        *self.programs.write().await = Some(programs);
    }

    /// Wire membership (built after the registry) so a non-writer can locate a shard's writer.
    pub async fn set_membership(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }

    /// The current registry epoch = `clock.now().millis() / EPOCH_MILLIS`, adjusted for the
    /// boundary-race grace window: during the first `GRACE_MILLIS` of an epoch the PREVIOUS
    /// epoch is returned, so the previous writer stays authoritative across the boundary. This
    /// is a deterministic rule — every node derives the same effective epoch from its HLC, so
    /// the elected writer rotates in lockstep with no dual-writer while clock skew < grace.
    fn effective_epoch(&self) -> u64 {
        let now = self.clock.now().millis();
        let e = now / EPOCH_MILLIS;
        if now % EPOCH_MILLIS < GRACE_MILLIS && e > 0 {
            e - 1
        } else {
            e
        }
    }

    /// The set eligible to be elected writer: the CONVERGED alive membership (self + every
    /// live member from [`Membership::census`]). The census is a converged set — union +
    /// max-last_heard merged across the whole network — so every node computes the election
    /// over the SAME view. This replaces the size-5, per-node-divergent HyParView active view,
    /// which above ~6 nodes produced inconsistent shard→writer assignment (split-brain).
    ///
    /// NO rtt-based exclusion: rtt is a per-observer LOCAL measurement, so filtering by it makes
    /// the eligible set differ per node and re-breaks election consistency — the exact bug this
    /// change fixes. Slow-writer handling now relies on the resolve fallback + the
    /// fire-and-forget/sidecar-first tail fixes; a CONVERGED health/reachability signal for
    /// excluding slow writers is future work.
    async fn eligible(&self) -> Vec<[u8; 32]> {
        let mut ids = vec![self.self_id];
        if let Some(m) = self.membership.read().await.as_ref() {
            for (n, _addr) in m.census().await {
                if n.0 == self.self_id {
                    continue;
                }
                ids.push(n.0);
            }
        }
        ids
    }

    /// The STABLE replica set for `sk`: the eligible ids sorted ASC by
    /// `blake3(rtype ‖ shard_le ‖ node_id)`, truncated to [`REPLICATION_FACTOR`]. The hash has
    /// NO epoch term on purpose — this set shifts ONLY on membership change, so a fixed group
    /// of nodes holds each account's state. The writer rotates AMONG these; the others are warm
    /// followers that already carry the state, so a writer failure has a ready successor.
    fn replicas(sk: ShardKey, eligible: &[[u8; 32]]) -> Vec<[u8; 32]> {
        let mut ids: Vec<[u8; 32]> = eligible.to_vec();
        ids.sort_by_key(|id| {
            Cid::of(
                &[
                    &[sk.rtype][..],
                    sk.shard.to_le_bytes().as_slice(),
                    id.as_slice(),
                ]
                .concat(),
            )
            .0
        });
        ids.truncate(REPLICATION_FACTOR.min(ids.len()));
        ids
    }

    /// The writer for `sk` in the CURRENT (effective) epoch. The writer is ALWAYS a replica:
    /// the role rotates through the stable replica set by epoch, so every node computes the
    /// same winner and the others stay warm followers. Empty eligibility (membership not yet
    /// wired) → self, so a fresh/single node still writes every account.
    async fn current_writer(&self, sk: ShardKey) -> Option<[u8; 32]> {
        let elig = self.eligible().await;
        let reps = Self::replicas(sk, &elig);
        if reps.is_empty() {
            Some(self.self_id)
        } else {
            Some(reps[(self.effective_epoch() as usize) % reps.len()])
        }
    }

    /// True if THIS node is `sk`'s current (effective-epoch) writer.
    async fn is_writer(&self, sk: ShardKey) -> bool {
        self.current_writer(sk).await == Some(self.self_id)
    }

    /// True if THIS node is in `sk`'s stable replica set (holds its state as a warm follower
    /// even when it is not the current writer).
    #[allow(dead_code)]
    async fn is_replica(&self, sk: ShardKey) -> bool {
        let elig = self.eligible().await;
        Self::replicas(sk, &elig).contains(&self.self_id)
    }

    /// Look up `id`'s dialable address. Consults the CONVERGED member set first (the same set
    /// the election runs over, so any elected writer/replica is resolvable even when it is not
    /// in this node's bounded active view), then falls back to the active/dead snapshot.
    async fn addr_of(&self, id: [u8; 32]) -> Option<PeerAddr> {
        let guard = self.membership.read().await;
        let m = guard.as_ref()?;
        if let Some(addr) = m.member_addr(zeph_core::NodeId(id)).await {
            return Some(addr);
        }
        let snap = m.snapshot().await;
        snap.active
            .iter()
            .chain(snap.dead.iter())
            .find(|(n, _)| n.0 == id)
            .map(|(_, ps)| ps.addr.clone())
    }

    /// `sk`'s current writer's dialable address. Errors if this node IS the writer (caller
    /// should act locally) or the writer is not in the membership view.
    async fn writer_addr(&self, sk: ShardKey) -> anyhow::Result<PeerAddr> {
        let writer = self
            .current_writer(sk)
            .await
            .ok_or_else(|| anyhow::anyhow!("no registry writer for current epoch"))?;
        if writer == self.self_id {
            anyhow::bail!("this node IS the current registry writer");
        }
        self.addr_of(writer)
            .await
            .ok_or_else(|| anyhow::anyhow!("registry writer not in membership view"))
    }

    /// TAKEOVER MERGE: when this node becomes `sk`'s writer for a NEW effective epoch, MERGE
    /// the OTHER live replicas' state into its own before serving, so a freshly-promoted
    /// replica catches up on anything it missed while a peer was writing. Merge (LWW), not
    /// overwrite — so no replica's newer entries are lost. Best-effort and idempotent per epoch
    /// per account (guarded by `last_epoch`).
    ///
    /// Edge cases (see also `.claude/feature-progress.md`):
    /// (a) the grace window ([`Self::effective_epoch`]) removes the clock-skew dual-writer race
    ///     at epoch boundaries while skew < grace;
    /// (b) if a replica is unreachable, we merge whoever answered + keep local state;
    /// (c) the FULL account state is transferred — fine while an account is small; later
    ///     exchange the cid and fetch content lazily.
    async fn ensure_current(&self, sk: ShardKey) {
        if !self.is_writer(sk).await {
            return;
        }
        let eff = self.effective_epoch();
        if eff <= self.last_epoch.read().await.get(&sk).copied().unwrap_or(0) {
            return;
        }
        let elig = self.eligible().await;
        let mut local = RegistryState::decode(
            &self
                .store
                .resolve(registry_program_cid(), &shard_seed(sk))
                .await,
        )
        .unwrap_or_default();
        for id in Self::replicas(sk, &elig) {
            if id == self.self_id {
                continue;
            }
            if let Some(addr) = self.addr_of(id).await {
                if let Ok(RegistryResp::State(bytes)) = request_registry(
                    &self.transport,
                    &addr,
                    &RegistryReq::GetState {
                        rtype: sk.rtype,
                        bits: sk.bits,
                        shard: sk.shard,
                    },
                )
                .await
                {
                    if let Some(other) = RegistryState::decode(&bytes) {
                        local.merge(&other);
                    }
                }
            }
        }
        let _ = self
            .store
            .put_state(registry_program_cid(), &shard_seed(sk), &local.encode())
            .await;
        // Best-effort: mark this epoch handled regardless — on failure we keep local state.
        self.last_epoch.write().await.insert(sk, eff);
    }

    /// Advance `sk`'s registry account from an owner-signed submission's raw bytes.
    /// Runs the governance-resolved registry program on the writer's own store — the same
    /// advance logic as the local writer path, but on already-signed bytes (no re-sign).
    /// After persisting, PUSHES the new state to the other replicas so the K-replica set stays
    /// warm (best-effort — a push failure never fails the write).
    async fn advance_local(&self, sk: ShardKey, sub_bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
        // Merge the other replicas' state if we've just become the writer, before advancing.
        self.ensure_current(sk).await;
        let code = self.program_cid().await;
        let seed = shard_seed(sk);
        // NATIVE DEFAULT (the built-in local registry program). When governance has NOT set a WASM
        // registry program, run `RegistryState::apply` directly — so a FRESH network self-starts
        // with no publish/governance bootstrap. A governance `SetProgram` swaps in the WASM (e.g.
        // the char-limit v2) as the upgrade on top. (MINIMAL_KERNEL: every anchor has a default.)
        let (new_state_bytes, root) = if code == registry_program_cid() {
            let prev =
                RegistryState::decode(&self.store.resolve(registry_program_cid(), &seed).await)
                    .unwrap_or_default();
            let sub = HeadSubmission::decode(sub_bytes)
                .ok_or_else(|| anyhow::anyhow!("bad head submission"))?;
            let next = prev
                .apply(&sub)
                .map_err(|e| anyhow::anyhow!("registry rejected the submission: {e}"))?;
            let bytes = next.encode();
            self.store
                .put_state(registry_program_cid(), &seed, &bytes)
                .await?;
            (bytes, next.root())
        } else {
            let r = self
                .store
                .advance(registry_program_cid(), code, &seed, sub_bytes)
                .await?;
            let bytes = self.store.resolve(registry_program_cid(), &seed).await;
            (bytes, r.new_root)
        };
        // Push-on-write: keep the replica set warm. Best-effort; never fails the write.
        self.replicate(sk, &new_state_bytes).await;
        Ok(root)
    }

    /// Push `sk`'s freshly-written state to every OTHER replica, each of which MERGES it (LWW).
    /// FIRE-AND-FORGET: we resolve the replica addresses (local, instant) and then send the pushes
    /// in a BACKGROUND task — the write must NOT block on the network. The writer already holds the
    /// state (`put_state` ran before this), so it can serve immediately; replicas catch up
    /// asynchronously, and takeover-merge covers any that miss a push. Awaiting the sends here (the
    /// old behaviour) made every write as slow as the slowest replica — a relay-only peer could
    /// stall a register for seconds. Best-effort: push errors are swallowed, never failing the write.
    async fn replicate(&self, sk: ShardKey, state: &[u8]) {
        let elig = self.eligible().await;
        let mut targets = Vec::new();
        for id in Self::replicas(sk, &elig) {
            if id == self.self_id {
                continue;
            }
            if let Some(addr) = self.addr_of(id).await {
                targets.push(addr);
            }
        }
        if targets.is_empty() {
            return;
        }
        let transport = self.transport.clone();
        let state = state.to_vec();
        let (rtype, bits, shard) = (sk.rtype, sk.bits, sk.shard);
        tokio::spawn(async move {
            for addr in targets {
                let _ = request_registry(
                    &transport,
                    &addr,
                    &RegistryReq::PushState {
                        rtype,
                        bits,
                        shard,
                        state: state.clone(),
                    },
                )
                .await;
            }
        });
    }

    /// Local resolve against this node's own copy of `sk`'s registry account. Returns the head
    /// `(cid, version)` so a version-aware caller gets the seq.
    async fn resolve_local(
        &self,
        sk: ShardKey,
        owner: [u8; 32],
        name: &str,
    ) -> Option<([u8; 32], u64)> {
        // Merge the other replicas' state if we've just become the writer, before resolving.
        self.ensure_current(sk).await;
        let raw = self
            .store
            .resolve(registry_program_cid(), &shard_seed(sk))
            .await;
        RegistryState::decode(&raw)?
            .resolve(&owner, name)
            .map(|e| (e.cid, e.version))
    }

    /// The canonical app-registry program cid — resolved via the governance program store
    /// (governance-upgradeable), falling back to the native cid.
    async fn program_cid(&self) -> [u8; 32] {
        match self.programs.read().await.as_ref() {
            Some(p) => p
                .resolve("app-registry")
                .await
                .unwrap_or_else(registry_program_cid),
            None => registry_program_cid(),
        }
    }

    /// Register (or advance) an app head under THIS node's identity. Routes the key to its
    /// shard; advances that shard's account over its STABLE address (`registry_program_cid()`)
    /// while executing the governance-resolved registry program (`program_cid()`) — so an
    /// upgraded WASM program is authoritative without moving the account. The store validates
    /// the owner-signed submission (open CRDT — no committee), persists + publishes the new
    /// state. Returns the new root.
    pub async fn register(
        &self,
        rtype: u8,
        name: &str,
        cid: [u8; 32],
        version: u64,
        _now_millis: u64,
    ) -> anyhow::Result<[u8; 32]> {
        // The OWNER (this node's identity) signs the submission either way.
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        // Route with this node's live (governed) bits; the writer is stamped the same `bits` so
        // it routes identically even if governance changed the count in flight.
        let bits = self.shard_bits().await;
        let sk = ShardKey {
            rtype,
            bits,
            shard: shard_of(&self.self_id, name, bits),
        };
        let root = if self.is_writer(sk).await {
            self.advance_local(sk, &sub.encode()).await?
        } else {
            // Non-writer: forward the signed submission to the shard's writer, which advances the
            // shard account and returns the new root.
            let addr = self.writer_addr(sk).await?;
            match request_registry(
                &self.transport,
                &addr,
                &RegistryReq::Submit {
                    rtype,
                    bits,
                    sub: sub.encode(),
                },
            )
            .await?
            {
                RegistryResp::SubmitAck(root) => root,
                RegistryResp::Err(e) => {
                    return Err(anyhow::anyhow!("registry writer rejected submit: {e}"))
                }
                other => return Err(anyhow::anyhow!("unexpected registry response: {other:?}")),
            }
        };
        // This node just wrote the key under its own identity — drop any stale cached resolve so
        // the write is immediately visible to this node's reads (read-your-writes).
        self.resolve_cache
            .invalidate(rtype, &self.self_id, name)
            .await;
        Ok(root)
    }

    /// Resolve a name published by `owner` to its current cid, TOLERANT of a briefly-unreachable
    /// writer. Routes the key to its shard, then tries in order, returning the first hit:
    ///   (a) THIS node's own copy, if it is in the shard's stable replica set (reads are
    ///       best-effort across replicas — no `ensure_current`; the writer's own takeover merge
    ///       keeps state fresh);
    ///   (b) the current-epoch writer (if remote);
    ///   (c) each OTHER replica (remote).
    /// Every remote call is bounded by the 8s [`request_registry`] timeout, so a dead-but-not-
    /// yet-dropped writer fails fast and the next replica is tried. Returns `None` only if all
    /// candidates miss. Targets are deduped (the writer is one of the replicas) and self is
    /// skipped in the remote loop.
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        self.resolve_entry(RT_PROGRAM, owner, name)
            .await
            .map(|(cid, _)| cid)
    }

    /// Resolve `(rtype, owner, name)` to its current head `(cid, version)`, surfacing the seq
    /// (the DB-root `RootStore` needs it). Same fault-tolerant candidate order as [`Self::resolve`]:
    ///   (a) THIS node's own copy, if it is in the shard's stable replica set;
    ///   (b) the current-epoch writer (if remote);
    ///   (c) each OTHER replica (remote).
    /// Returns `None` only if all candidates miss.
    pub async fn resolve_entry(
        &self,
        rtype: u8,
        owner: [u8; 32],
        name: &str,
    ) -> Option<([u8; 32], u64)> {
        // Resolve at this node's live (governed) generation first.
        let bits = self.shard_bits().await;
        let sk = ShardKey {
            rtype,
            bits,
            shard: shard_of(&owner, name, bits),
        };
        // NON-replica fast path: a fresh cached resolve at the CURRENT generation skips the network
        // round-trip. A replica of the current shard reads authoritative local state (inside
        // `resolve_at_bits`) and never consults the cache.
        let is_replica_now = Self::replicas(sk, &self.eligible().await).contains(&self.self_id);
        if !is_replica_now {
            if let Some(entry) = self
                .resolve_cache
                .get(rtype, &owner, name, self.clock.now().millis())
                .await
            {
                return Some(entry);
            }
        }
        if let Some(entry) = self.resolve_at_bits(rtype, bits, owner, name).await {
            if !is_replica_now {
                self.resolve_cache
                    .put(rtype, owner, name, entry, self.clock.now().millis())
                    .await;
            }
            return Some(entry);
        }
        // TRANSITION FALLBACK: during an in-flight ±1 online reshard the key may still live at an
        // ADJACENT generation — on a grow, at `bits-1` until the split lands; on a shrink, at
        // `bits+1` until the merge lands. Try each adjacent generation once so a resolve doesn't
        // fail in the migration window. NOT cached — the adjacent generation is transient.
        for alt in [bits.wrapping_sub(1), bits + 1] {
            if !(1..=MAX_SHARD_BITS).contains(&alt) || alt == bits {
                continue;
            }
            if let Some(entry) = self.resolve_at_bits(rtype, alt, owner, name).await {
                return Some(entry);
            }
        }
        None
    }

    /// Resolve `(rtype, owner, name)` against ONE specific shard-count generation (`bits`), trying
    /// in order: this node's own copy if it replicates that generation's shard, then the shard's
    /// current-epoch writer, then its other replicas (each remote call 8s-bounded). No cache — the
    /// caller owns caching for the current generation. Returns the first hit, else `None`. This is
    /// the per-generation core shared by the current-generation read and the transition fallback.
    async fn resolve_at_bits(
        &self,
        rtype: u8,
        bits: u32,
        owner: [u8; 32],
        name: &str,
    ) -> Option<([u8; 32], u64)> {
        let sk = ShardKey {
            rtype,
            bits,
            shard: shard_of(&owner, name, bits),
        };
        let elig = self.eligible().await;
        let reps = Self::replicas(sk, &elig);
        // (a) Local read if this node holds the shard's state as a replica.
        if reps.contains(&self.self_id) {
            if let Some(entry) = RegistryState::decode(
                &self
                    .store
                    .resolve(registry_program_cid(), &shard_seed(sk))
                    .await,
            )
            .and_then(|s| s.resolve(&owner, name).map(|e| (e.cid, e.version)))
            {
                return Some(entry);
            }
        }
        // (b/c) Ordered remote targets: current writer first, then the other replicas. Deduped,
        // and self is skipped (its copy was already consulted in (a)).
        let mut targets: Vec<[u8; 32]> = Vec::new();
        if let Some(w) = self.current_writer(sk).await {
            if w != self.self_id {
                targets.push(w);
            }
        }
        for id in reps {
            if id != self.self_id && !targets.contains(&id) {
                targets.push(id);
            }
        }
        for t in targets {
            let Some(addr) = self.addr_of(t).await else {
                continue;
            };
            if let Ok(RegistryResp::Resolved(Some(entry))) = request_registry(
                &self.transport,
                &addr,
                &RegistryReq::Resolve {
                    rtype,
                    bits,
                    owner,
                    name: name.to_string(),
                },
            )
            .await
            {
                return Some(entry);
            }
        }
        None
    }

    /// Serve `REGISTRY_ALPN` requests: as a shard's writer, advance the shard account on
    /// `Submit`, resolve on `Resolve`, hand off state on `GetState`, report a key's version on
    /// `CurrentVersion`. The shard is derived from the request's key so a key always lands on
    /// the SAME shard as the registering node computed.
    pub async fn serve(self: Arc<Self>, mut conns: mpsc::Receiver<Connection>) {
        // Anti-entropy loop — two event-driven jobs, each gated so it adds NO registry traffic
        // while nothing has changed: `reshard_round` (governed shard-count change → split/merge to
        // the new generation) then `migrate_round` (census change → re-replicate held shards to
        // their new replica set). Reshard first, so a just-migrated new-generation account is then
        // replicated to its set in the same tick.
        {
            let this = self.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tick.tick().await;
                    this.reshard_round().await;
                    this.migrate_round().await;
                }
            });
        }
        while let Some(conn) = conns.recv().await {
            let this = self.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                        break;
                    };
                    let resp = match postcard::from_bytes::<RegistryReq>(&bytes) {
                        // Route with the SUBMITTER's `bits` (from the wire), not this node's, so a
                        // `shard_bits` change in flight can't split-route the key.
                        Ok(RegistryReq::Submit { rtype, bits, sub }) => {
                            match HeadSubmission::decode(&sub) {
                                Some(s) => {
                                    let sk = ShardKey {
                                        rtype,
                                        bits,
                                        shard: shard_of(&s.owner, &s.name, bits),
                                    };
                                    match this.advance_local(sk, &sub).await {
                                        Ok(root) => RegistryResp::SubmitAck(root),
                                        Err(e) => RegistryResp::Err(e.to_string()),
                                    }
                                }
                                None => RegistryResp::Err("bad submission".into()),
                            }
                        }
                        // Route with the querier's `bits` (from the wire).
                        Ok(RegistryReq::Resolve {
                            rtype,
                            bits,
                            owner,
                            name,
                        }) => {
                            let sk = ShardKey {
                                rtype,
                                bits,
                                shard: shard_of(&owner, &name, bits),
                            };
                            RegistryResp::Resolved(this.resolve_local(sk, owner, &name).await)
                        }
                        // Serve the full account state (at the requested generation) for the merge.
                        Ok(RegistryReq::GetState { rtype, bits, shard }) => RegistryResp::State(
                            this.store
                                .resolve(
                                    registry_program_cid(),
                                    &shard_seed(ShardKey { rtype, bits, shard }),
                                )
                                .await,
                        ),
                        // Report the current version of a key from its account (0 if none).
                        // Route with the querier's `bits` (from the wire).
                        Ok(RegistryReq::CurrentVersion {
                            rtype,
                            bits,
                            owner,
                            name,
                        }) => {
                            let sk = ShardKey {
                                rtype,
                                bits,
                                shard: shard_of(&owner, &name, bits),
                            };
                            let raw = this
                                .store
                                .resolve(registry_program_cid(), &shard_seed(sk))
                                .await;
                            let v = RegistryState::decode(&raw)
                                .and_then(|s| s.resolve(&owner, &name).map(|e| e.version))
                                .unwrap_or(0);
                            RegistryResp::Version(v)
                        }
                        // A pushed replica state (at its generation) — MERGE (LWW), don't overwrite.
                        Ok(RegistryReq::PushState {
                            rtype,
                            bits,
                            shard,
                            state,
                        }) => {
                            let seed = shard_seed(ShardKey { rtype, bits, shard });
                            let mut local = RegistryState::decode(
                                &this.store.resolve(registry_program_cid(), &seed).await,
                            )
                            .unwrap_or_default();
                            if let Some(pushed) = RegistryState::decode(&state) {
                                local.merge(&pushed);
                            }
                            let _ = this
                                .store
                                .put_state(registry_program_cid(), &seed, &local.encode())
                                .await;
                            RegistryResp::Ack
                        }
                        // Return ALL of this node's local heads for the global dashboard union.
                        Ok(RegistryReq::ListEntries) => {
                            RegistryResp::Entries(this.local_head_rows().await)
                        }
                        Err(e) => RegistryResp::Err(format!("bad registry request: {e}")),
                    };
                    let out = postcard::to_allocvec(&resp).unwrap_or_default();
                    let _ = send.write_all(&out).await;
                    let _ = send.finish();
                }
            });
        }
    }

    /// The current version of `(owner, name)` (0 if unregistered), so a deploy advances to
    /// `prev + 1` from the registry itself — no DHT lookup. Routes the key to its shard: the
    /// shard's writer reads it locally, a non-writer queries the writer over `REGISTRY_ALPN`.
    pub async fn current_version(&self, rtype: u8, owner: [u8; 32], name: &str) -> u64 {
        let bits = self.shard_bits().await;
        let sk = ShardKey {
            rtype,
            bits,
            shard: shard_of(&owner, name, bits),
        };
        if self.is_writer(sk).await {
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(sk))
                .await;
            return RegistryState::decode(&raw)
                .and_then(|s| s.resolve(&owner, name).map(|e| e.version))
                .unwrap_or(0);
        }
        let Ok(addr) = self.writer_addr(sk).await else {
            return 0;
        };
        match request_registry(
            &self.transport,
            &addr,
            &RegistryReq::CurrentVersion {
                rtype,
                bits,
                owner,
                name: name.to_string(),
            },
        )
        .await
        {
            Ok(RegistryResp::Version(v)) => v,
            _ => 0,
        }
    }

    /// Snapshot the registry as `(owner_hex, name, cid_hex, version)` rows for the UI.
    /// per-node partial view now (sharded) — aggregates only the shards THIS node currently
    /// writes.
    #[allow(dead_code)]
    pub async fn rows(&self) -> Vec<(String, String, String, u64)> {
        let mut out = Vec::new();
        let bits = self.shard_bits().await;
        for shard in 0..(1u64 << bits) {
            let sk = ShardKey {
                rtype: RT_PROGRAM,
                bits,
                shard,
            };
            if !self.is_writer(sk).await {
                continue;
            }
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(sk))
                .await;
            for e in RegistryState::decode(&raw).unwrap_or_default().entries() {
                out.push((
                    hex::encode(e.owner),
                    e.name.clone(),
                    hex::encode(e.cid),
                    e.version,
                ));
            }
        }
        out
    }

    /// The number of registered heads + a combined root (for status).
    /// per-node partial view now (sharded) — counts only the shards THIS node currently writes.
    #[allow(dead_code)]
    pub async fn summary(&self) -> (usize, String) {
        let mut count = 0;
        let mut combined: Vec<u8> = Vec::new();
        let bits = self.shard_bits().await;
        for shard in 0..(1u64 << bits) {
            let sk = ShardKey {
                rtype: RT_PROGRAM,
                bits,
                shard,
            };
            if !self.is_writer(sk).await {
                continue;
            }
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(sk))
                .await;
            let s = RegistryState::decode(&raw).unwrap_or_default();
            count += s.len();
            combined.extend_from_slice(&s.root());
        }
        (count, hex::encode(Cid::of(&combined).0))
    }

    /// Registry status for the dashboard. `writer_shards` = the RT_PROGRAM shards THIS node
    /// currently writes; `program_heads` / `dbroots` / `manifests` = the total `(owner, name)`
    /// rows across exactly the shards this node writes for each registry type (a per-node
    /// partial view — the registry is sharded, so no single node sees every shard).
    pub async fn status(&self) -> RegistryStatus {
        let epoch = self.effective_epoch();
        let eligible = self.eligible().await.len();
        let mut writer_shards = 0usize;
        let (mut program_heads, mut dbroots, mut manifests) = (0usize, 0usize, 0usize);
        let bits = self.shard_bits().await;
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..(1u64 << bits) {
                let sk = ShardKey { rtype, bits, shard };
                if !self.is_writer(sk).await {
                    continue;
                }
                if rtype == RT_PROGRAM {
                    writer_shards += 1;
                }
                let raw = self
                    .store
                    .resolve(registry_program_cid(), &shard_seed(sk))
                    .await;
                let n = RegistryState::decode(&raw).map(|s| s.len()).unwrap_or(0);
                match rtype {
                    RT_PROGRAM => program_heads += n,
                    RT_DBROOT => dbroots += n,
                    _ => manifests += n,
                }
            }
        }
        RegistryStatus {
            epoch,
            eligible,
            writer_shards,
            shard_count: 1u64 << bits,
            program_heads,
            dbroots,
            manifests,
        }
    }

    /// Enumerate THIS node's local registry heads — every rtype, every shard it currently
    /// writes (the same all-rtypes/held-shards iteration [`Self::entries`] uses) — as raw-byte
    /// wire rows. Shared by the node-local [`Self::entries`], the `ListEntries` serve handler,
    /// and the global gather in [`Self::entries_global`].
    async fn local_head_rows(&self) -> Vec<HeadRowWire> {
        let mut out = Vec::new();
        let bits = self.shard_bits().await;
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..(1u64 << bits) {
                let sk = ShardKey { rtype, bits, shard };
                if !self.is_writer(sk).await {
                    continue;
                }
                let raw = self
                    .store
                    .resolve(registry_program_cid(), &shard_seed(sk))
                    .await;
                for e in RegistryState::decode(&raw).unwrap_or_default().entries() {
                    out.push(HeadRowWire {
                        rtype,
                        owner: e.owner,
                        name: e.name.clone(),
                        cid: e.cid,
                        version: e.version,
                    });
                }
            }
        }
        out
    }

    /// Snapshot every head this node holds, grouped by registry type, for the dashboard.
    /// A per-node partial view (sharded): for each of RT_PROGRAM / RT_DBROOT / RT_MANIFEST it
    /// iterates the shards THIS node currently writes (same shard-iteration as [`Self::status`])
    /// and collects each account's heads as hex-encoded rows. Retained for callers that want a
    /// cheap node-local view; the dashboard uses [`Self::entries_global`].
    #[allow(dead_code)]
    pub async fn entries(&self) -> RegistryEntries {
        Self::group_rows(self.local_head_rows().await, 1)
    }

    /// GLOBAL registry view: gather every member's local heads and merge them, so the dashboard
    /// shows the COMPLETE registry rather than only this node's shards. Since each shard is
    /// K-replicated across the members, the UNION of all members' local views is the whole
    /// registry.
    ///
    /// - Starts with this node's own local heads.
    /// - Dials every OTHER active member and asks for its local heads (`ListEntries`) over
    ///   `REGISTRY_ALPN`. All peer queries run CONCURRENTLY; each tolerates failure (a dead or
    ///   slow peer contributes nothing — its error is swallowed, never propagated).
    /// - The whole gather is bounded by a short deadline (~3s): results are absorbed as they
    ///   arrive, and whatever responded in time is used. A single laggard (e.g. a relay-only
    ///   peer) can't stall the UI — K-replication means its shards are still covered by other
    ///   replicas that did answer.
    /// - Rows are MERGED into a map keyed by `(rtype, owner, name)` keeping the MAX version
    ///   (last-writer-wins), which dedups the replica overlap.
    pub async fn entries_global(&self) -> RegistryEntries {
        use futures::stream::{FuturesUnordered, StreamExt};
        use std::collections::HashMap;

        // (rtype, owner, name) -> row at its highest version (LWW dedup of replica overlap).
        let mut merged: HashMap<(u8, [u8; 32], String), HeadRowWire> = HashMap::new();
        fn absorb(
            merged: &mut HashMap<(u8, [u8; 32], String), HeadRowWire>,
            rows: Vec<HeadRowWire>,
        ) {
            for r in rows {
                let key = (r.rtype, r.owner, r.name.clone());
                match merged.get(&key) {
                    Some(ex) if ex.version >= r.version => {}
                    _ => {
                        merged.insert(key, r);
                    }
                }
            }
        }

        // Own local heads first (always a contributor).
        absorb(&mut merged, self.local_head_rows().await);
        let mut contributors = 1usize;

        // Other active members' dialable addresses.
        let mut peers: Vec<PeerAddr> = Vec::new();
        if let Some(m) = self.membership.read().await.as_ref() {
            for (n, ps) in m.snapshot().await.active {
                if n.0 == self.self_id {
                    continue;
                }
                peers.push(ps.addr.clone());
            }
        }

        // Query every peer CONCURRENTLY. Each future returns `Some(rows)` on a successful
        // ListEntries reply (a peer with no heads still yields `Some(vec![])` and counts as a
        // contributor) or `None` on any failure. Drained against a single overall deadline so
        // partial results already in hand survive a laggard.
        let mut futs = FuturesUnordered::new();
        for addr in peers {
            let transport = self.transport.clone();
            futs.push(async move {
                match request_registry(&transport, &addr, &RegistryReq::ListEntries).await {
                    Ok(RegistryResp::Entries(rows)) => Some(rows),
                    _ => None,
                }
            });
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while let Ok(Some(res)) = tokio::time::timeout_at(deadline, futs.next()).await {
            if let Some(rows) = res {
                contributors += 1;
                absorb(&mut merged, rows);
            }
        }

        Self::group_rows(merged.into_values().collect(), contributors)
    }

    /// Group raw wire rows into a hex-encoded [`RegistryEntries`] by registry type.
    fn group_rows(rows: Vec<HeadRowWire>, contributors: usize) -> RegistryEntries {
        let mut out = RegistryEntries {
            contributors,
            ..Default::default()
        };
        for r in rows {
            let bucket = match r.rtype {
                RT_PROGRAM => &mut out.programs,
                RT_DBROOT => &mut out.dbroots,
                _ => &mut out.manifests,
            };
            bucket.push(HeadRow {
                owner: hex::encode(r.owner),
                name: r.name,
                cid: hex::encode(r.cid),
                version: r.version,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_of_is_prefix_stable_under_growth() {
        let owner = [7u8; 32];
        // bits = 8 reproduces the prior `% 256` exactly (low 8 bits of the LE key hash).
        let h = Cid::of(&[owner.as_slice(), b"myapp".as_slice()].concat()).0;
        let full = u64::from_le_bytes(h[..8].try_into().unwrap());
        assert_eq!(shard_of(&owner, "myapp", 8), full % 256);

        // The split invariant: growing the count by one bit preserves the low-k prefix and only
        // appends the next bit — a key's shard at k+1 is either s or s | (1<<k), where s is its
        // shard at k. This is what makes split/merge a LOCAL migration (parent -> two children).
        for k in 4..20u32 {
            let s_k = shard_of(&owner, "myapp", k);
            let s_k1 = shard_of(&owner, "myapp", k + 1);
            assert_eq!(
                s_k1 & ((1u64 << k) - 1),
                s_k,
                "the low-k prefix must be preserved across a split"
            );
            assert!(
                s_k1 == s_k || s_k1 == s_k | (1u64 << k),
                "a child shard is its parent or parent | (1<<k)"
            );
        }
    }

    #[test]
    fn shard_seed_is_distinct_per_generation() {
        // The whole online-reshard cutover rests on this: the SAME (rtype, shard) at two different
        // generations (bits) must address two DIFFERENT accounts, so a reshard can read the old
        // generation and write the new one without one clobbering the other.
        let a = shard_seed(ShardKey {
            rtype: RT_PROGRAM,
            bits: 8,
            shard: 5,
        });
        let b = shard_seed(ShardKey {
            rtype: RT_PROGRAM,
            bits: 9,
            shard: 5,
        });
        assert_ne!(a, b, "same shard at bits 8 vs 9 must be distinct accounts");
        // rtype and shard still discriminate within a generation
        assert_ne!(
            shard_seed(ShardKey {
                rtype: RT_PROGRAM,
                bits: 8,
                shard: 5
            }),
            shard_seed(ShardKey {
                rtype: RT_DBROOT,
                bits: 8,
                shard: 5
            })
        );
        assert_ne!(
            shard_seed(ShardKey {
                rtype: RT_PROGRAM,
                bits: 8,
                shard: 5
            }),
            shard_seed(ShardKey {
                rtype: RT_PROGRAM,
                bits: 8,
                shard: 6
            })
        );
    }

    #[test]
    fn rebucket_routes_every_entry_and_splits_parent_into_two_children() {
        let entries: Vec<HeadEntry> = (0..300)
            .map(|i| HeadEntry {
                owner: [1u8; 32],
                name: format!("app{i}"),
                cid: [0u8; 32],
                version: 1,
            })
            .collect();
        let grouped = rebucket_entries(&entries, 9);
        // no entry lost: the groups partition the input
        let total: usize = grouped.values().map(|v| v.len()).sum();
        assert_eq!(total, entries.len(), "every entry re-bucketed exactly once");
        for (shard, group) in &grouped {
            for e in group {
                // each entry lands under its shard at the NEW count...
                assert_eq!(shard_of(&e.owner, &e.name, 9), *shard);
                // ...and a bits-8 parent's keys only fan out to children `p` and `p | 256`
                let parent = shard_of(&e.owner, &e.name, 8);
                assert!(*shard == parent || *shard == parent | 256);
            }
        }
        // a real split actually populates the new high shards (>= 256), not just relabels
        assert!(
            grouped.keys().any(|s| *s >= 256),
            "split populates shards >= 256"
        );
    }

    #[test]
    fn resolve_shard_bits_falls_back_and_clamps() {
        // unset (or governance not wired) -> the built-in default
        assert_eq!(resolve_shard_bits(None), DEFAULT_SHARD_BITS);
        // a governed value in range is honored
        assert_eq!(resolve_shard_bits(Some(9)), 9);
        assert_eq!(
            resolve_shard_bits(Some(MAX_SHARD_BITS as i64)),
            MAX_SHARD_BITS
        );
        // out-of-range / hostile values are clamped, never able to drive the O(2^bits) loops away
        assert_eq!(resolve_shard_bits(Some(0)), 1);
        assert_eq!(resolve_shard_bits(Some(-5)), 1);
        assert_eq!(resolve_shard_bits(Some(1_000)), MAX_SHARD_BITS);
    }

    #[test]
    fn routing_honors_a_non_default_count() {
        // At a governed count other than 256, every key must land within [0, 2^bits) and route by
        // the low `bits` of the hash — this is what a governed shard_bits actually buys.
        let owner = [3u8; 32];
        for bits in [1u32, 9, MAX_SHARD_BITS] {
            let count = 1u64 << bits;
            for name in ["a", "app", "guestbook2", "x-y-z", "verylongprogramname"] {
                let s = shard_of(&owner, name, bits);
                assert!(s < count, "shard {s} must be < {count} at bits={bits}");
                let h = Cid::of(&[owner.as_slice(), name.as_bytes()].concat()).0;
                let full = u64::from_le_bytes(h[..8].try_into().unwrap());
                assert_eq!(s, full & (count - 1), "route by the low {bits} bits");
            }
        }
        // bits=9 (512 shards) genuinely uses shards >= 256 that bits=8 never could — proof the
        // count actually grew, not just re-labeled the same 256.
        let uses_high = (0..2000).any(|i| shard_of(&[9u8; 32], &format!("k{i}"), 9) >= 256);
        assert!(uses_high, "a 512-shard count must populate shards >= 256");
    }

    #[tokio::test]
    async fn resolve_cache_serves_fresh_expires_isolates_and_invalidates() {
        let c = ResolveCache::default();
        let owner = [1u8; 32];
        let entry = ([9u8; 32], 7u64);

        c.put(RT_PROGRAM, owner, "app", entry, 1_000).await;
        // fresh within the TTL window
        assert_eq!(c.get(RT_PROGRAM, &owner, "app", 1_500).await, Some(entry));
        // at/after expiry it is stale (expiry is exclusive)
        let expiry = 1_000 + RESOLVE_CACHE_TTL_MS;
        assert_eq!(c.get(RT_PROGRAM, &owner, "app", expiry).await, None);

        // key isolation: a different name, rtype, or owner is a miss
        c.put(RT_PROGRAM, owner, "app", entry, 1_000).await;
        assert_eq!(c.get(RT_PROGRAM, &owner, "other", 1_100).await, None);
        assert_eq!(c.get(RT_DBROOT, &owner, "app", 1_100).await, None);
        assert_eq!(c.get(RT_PROGRAM, &[2u8; 32], "app", 1_100).await, None);

        // invalidate drops it immediately (read-your-writes after register())
        c.invalidate(RT_PROGRAM, &owner, "app").await;
        assert_eq!(c.get(RT_PROGRAM, &owner, "app", 1_100).await, None);
    }
}
