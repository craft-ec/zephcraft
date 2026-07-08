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
//! Sharding: the keyspace is split into [`SHARD_COUNT`] shards. Every `(owner, name)` key
//! routes to exactly ONE shard via [`shard_of`]; each shard is its own account (seeded by
//! [`shard_seed`]) with its own independent rotating-writer election. So at one moment
//! different shards may be written by different nodes, and the write load spreads across the
//! membership.

use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use zeph_com::{registry_program_cid, HeadSubmission, RegistryState, REGISTRY_SEED};
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

/// Number of registry shards. Each `(owner, name)` key routes to one shard (see [`shard_of`]);
/// each shard is an independent account with its own rotating-writer election.
const SHARD_COUNT: u64 = 256;

/// Consecutive migration-loop ticks the census must be UNCHANGED before state migration runs.
/// The loop ticks every 10s, so this debounces migration to ~30s of stable membership — during
/// convergence/churn the census changes every tick, which must NOT trigger the scan+push storm.
const MIGRATE_STABLE_TICKS: u32 = 3;

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

/// Identifies one registry account = one `(kind, shard)`. Threaded everywhere a shard used to
/// be, so each kind gets its own independent per-shard account + writer set.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ShardKey {
    rtype: u8,
    shard: u64,
}

/// Boundary-race grace window, in milliseconds. During the first `GRACE_MILLIS` of a new epoch
/// the PREVIOUS epoch's writer stays authoritative (see [`HeadRegistry::effective_epoch`]),
/// so a bounded clock skew (< grace) can't produce two live writers at a boundary.
const GRACE_MILLIS: u64 = 2_000;

/// Route a `(owner, name)` key to its shard. Register and resolve of the same key MUST agree,
/// so this ONE function is used everywhere a shard is derived.
fn shard_of(owner: &[u8; 32], name: &str) -> u64 {
    let h = Cid::of(&[owner.as_slice(), name.as_bytes()].concat()).0;
    u64::from_le_bytes(h[..8].try_into().unwrap()) % SHARD_COUNT
}

/// The per-account seed — replaces the bare `REGISTRY_SEED` in every account op so each
/// `(rtype, shard)` gets a distinct `pda(registry_program_cid(), shard_seed(sk))`. The KIND
/// tag is folded in FIRST, so a program head and a database root at the same shard live in
/// separate accounts (type-in-seed).
fn shard_seed(sk: ShardKey) -> Vec<u8> {
    [
        REGISTRY_SEED,
        &[sk.rtype],
        sk.shard.to_le_bytes().as_slice(),
    ]
    .concat()
}

/// A TTL'd cache of `(rtype, owner, name)` → `(cid, version)` resolves. The clock is injected
/// (`now` is passed in) so the cache is unit-testable without a live registry. Consulted only for
/// NON-replica reads — a replica reads authoritative local state (see [`HeadRegistry`]).
#[derive(Default)]
struct ResolveCache {
    map: RwLock<std::collections::HashMap<(u8, [u8; 32], String), (([u8; 32], u64), u64)>>,
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
/// currently writes (of [`SHARD_COUNT`]); the per-type counts are the `(owner, name)` rows
/// across exactly the shards this node writes for each type — a per-node partial view.
pub struct RegistryStatus {
    pub epoch: u64,
    pub eligible: usize,
    pub writer_shards: usize,
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
        }
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
        // Census has SETTLED: re-replicate every shard we hold local state for to its new replica set.
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..SHARD_COUNT {
                let sk = ShardKey { rtype, shard };
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
        let (rtype, shard) = (sk.rtype, sk.shard);
        tokio::spawn(async move {
            for addr in targets {
                let _ = request_registry(
                    &transport,
                    &addr,
                    &RegistryReq::PushState {
                        rtype,
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
        let sk = ShardKey {
            rtype,
            shard: shard_of(&self.self_id, name),
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
        let sk = ShardKey {
            rtype,
            shard: shard_of(&owner, name),
        };
        let elig = self.eligible().await;
        let reps = Self::replicas(sk, &elig);
        let is_replica = reps.contains(&self.self_id);
        // (a) Local read if this node holds the shard's state as a replica.
        if is_replica {
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
        // (a′) NON-replica: a fresh cached resolve avoids the network round-trip below. Replicas
        // skip the cache — their authoritative local read above is the source of truth.
        if !is_replica {
            if let Some(entry) = self
                .resolve_cache
                .get(rtype, &owner, name, self.clock.now().millis())
                .await
            {
                return Some(entry);
            }
        }
        // Ordered remote targets: current writer first, then the other replicas. Deduped, and
        // self is skipped (its copy was already consulted in (a)).
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
                    rtype: sk.rtype,
                    owner,
                    name: name.to_string(),
                },
            )
            .await
            {
                if !is_replica {
                    self.resolve_cache
                        .put(rtype, owner, name, entry, self.clock.now().millis())
                        .await;
                }
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
        // State-migration anti-entropy loop — re-replicate held shards to their current replica
        // set whenever the census changes (see `migrate_round`). Event-driven, so it adds no
        // registry traffic while membership is stable.
        {
            let this = self.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tick.tick().await;
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
                        Ok(RegistryReq::Submit { rtype, sub }) => {
                            match HeadSubmission::decode(&sub) {
                                Some(s) => {
                                    let sk = ShardKey {
                                        rtype,
                                        shard: shard_of(&s.owner, &s.name),
                                    };
                                    match this.advance_local(sk, &sub).await {
                                        Ok(root) => RegistryResp::SubmitAck(root),
                                        Err(e) => RegistryResp::Err(e.to_string()),
                                    }
                                }
                                None => RegistryResp::Err("bad submission".into()),
                            }
                        }
                        Ok(RegistryReq::Resolve { rtype, owner, name }) => {
                            let sk = ShardKey {
                                rtype,
                                shard: shard_of(&owner, &name),
                            };
                            RegistryResp::Resolved(this.resolve_local(sk, owner, &name).await)
                        }
                        // Serve the full current account state for the takeover merge.
                        Ok(RegistryReq::GetState { rtype, shard }) => RegistryResp::State(
                            this.store
                                .resolve(
                                    registry_program_cid(),
                                    &shard_seed(ShardKey { rtype, shard }),
                                )
                                .await,
                        ),
                        // Report the current version of a key from its account (0 if none).
                        Ok(RegistryReq::CurrentVersion { rtype, owner, name }) => {
                            let sk = ShardKey {
                                rtype,
                                shard: shard_of(&owner, &name),
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
                        // A pushed replica state — MERGE (LWW) into our own copy, don't overwrite.
                        Ok(RegistryReq::PushState {
                            rtype,
                            shard,
                            state,
                        }) => {
                            let seed = shard_seed(ShardKey { rtype, shard });
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
        let sk = ShardKey {
            rtype,
            shard: shard_of(&owner, name),
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
        for shard in 0..SHARD_COUNT {
            let sk = ShardKey {
                rtype: RT_PROGRAM,
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
        for shard in 0..SHARD_COUNT {
            let sk = ShardKey {
                rtype: RT_PROGRAM,
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
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..SHARD_COUNT {
                let sk = ShardKey { rtype, shard };
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
        for &rtype in &[RT_PROGRAM, RT_DBROOT, RT_MANIFEST] {
            for shard in 0..SHARD_COUNT {
                let sk = ShardKey { rtype, shard };
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
