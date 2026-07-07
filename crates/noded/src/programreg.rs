//! Phase 4c/4d — the live program-name registry, a THIN consumer of the generic
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
use crate::registry_net::{request_registry, RegistryReq, RegistryResp};

/// Max size of a registry request/response frame served over `REGISTRY_ALPN`.
const MAX_FRAME: usize = 256 * 1024;

/// Length of a registry writer-election epoch, in milliseconds. Short cycle (fast rotation),
/// tunable. `epoch = clock.now().millis() / EPOCH_MILLIS`.
const EPOCH_MILLIS: u64 = 30_000;

/// Number of registry shards. Each `(owner, name)` key routes to one shard (see [`shard_of`]);
/// each shard is an independent account with its own rotating-writer election.
const SHARD_COUNT: u64 = 256;

/// Registry KIND tags — each kind is a SEPARATE account per shard (the tag is folded into
/// [`shard_seed`], so `(rtype, shard)` addresses distinct state). Lets program heads, database
/// roots, and manifests share one substrate without colliding.
pub const RT_PROGRAM: u8 = 0;
/// Reserved kinds — the substrate already routes them (type-in-seed); the DB-root and manifest
/// registries that will use them are not wired yet, so tolerate the unused tags for now.
#[allow(dead_code)]
pub const RT_DBROOT: u8 = 1;
#[allow(dead_code)]
pub const RT_MANIFEST: u8 = 2;

/// How many replicas hold each shard's state. The writer ROTATES among this stable set and
/// pushes every write to the others, so if the writer dies a warm successor already holds the
/// state (the offline-writer fault the single-writer model could not survive).
const REPLICATION_FACTOR: usize = 3;

/// Identifies one registry account = one `(kind, shard)`. Threaded everywhere a shard used to
/// be, so each kind gets its own independent per-shard account + writer set.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ShardKey {
    rtype: u8,
    shard: u64,
}

/// Boundary-race grace window, in milliseconds. During the first `GRACE_MILLIS` of a new epoch
/// the PREVIOUS epoch's writer stays authoritative (see [`ProgramRegistry::effective_epoch`]),
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

/// The node's durable program-name registry — a thin consumer of [`ProgramAccountStore`].
///
/// Cross-node model: for each shard the writer for an epoch is ELECTED DETERMINISTICALLY (a
/// rotating writer), so the write duty circulates among active nodes independently per shard.
/// `writer(shard, epoch)` = the eligible member (self + active membership) with the smallest
/// `blake3(shard_le ‖ epoch_le ‖ node_id)`. Every node computes the same winner from its
/// membership view + HLC clock. If this node is a shard's current writer it advances and
/// resolves that shard locally; otherwise it forwards registrations and queries to the shard's
/// current writer over `REGISTRY_ALPN`. As the epoch rotates the writer is recomputed, and the
/// new writer adopts the previous writer's state (see [`Self::ensure_current`]).
pub struct ProgramRegistry {
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
}

impl ProgramRegistry {
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

    /// The set eligible to be elected writer: self + the membership's ACTIVE peers. Every node
    /// computes the election over this same view.
    async fn eligible(&self) -> Vec<[u8; 32]> {
        let mut ids = vec![self.self_id];
        if let Some(m) = self.membership.read().await.as_ref() {
            for (n, _) in m.snapshot().await.active {
                if n.0 != self.self_id {
                    ids.push(n.0);
                }
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

    /// Look up `id`'s dialable address from the membership view (active or dead).
    async fn addr_of(&self, id: [u8; 32]) -> Option<PeerAddr> {
        let guard = self.membership.read().await;
        let snap = guard.as_ref()?.snapshot().await;
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

    /// Push `sk`'s freshly-written state to every OTHER replica present in the eligible set,
    /// each of which MERGES it (LWW). Best-effort: the ≤ `REPLICATION_FACTOR - 1` sends are
    /// awaited sequentially, and any error is swallowed — a push failure NEVER fails the write.
    async fn replicate(&self, sk: ShardKey, state: &[u8]) {
        let elig = self.eligible().await;
        for id in Self::replicas(sk, &elig) {
            if id == self.self_id {
                continue;
            }
            if let Some(addr) = self.addr_of(id).await {
                let _ = request_registry(
                    &self.transport,
                    &addr,
                    &RegistryReq::PushState {
                        rtype: sk.rtype,
                        shard: sk.shard,
                        state: state.to_vec(),
                    },
                )
                .await;
            }
        }
    }

    /// Local resolve against this node's own copy of `sk`'s registry account.
    async fn resolve_local(&self, sk: ShardKey, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        // Merge the other replicas' state if we've just become the writer, before resolving.
        self.ensure_current(sk).await;
        let raw = self
            .store
            .resolve(registry_program_cid(), &shard_seed(sk))
            .await;
        RegistryState::decode(&raw)?
            .resolve(&owner, name)
            .map(|e| e.cid)
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
        name: &str,
        cid: [u8; 32],
        version: u64,
        _now_millis: u64,
    ) -> anyhow::Result<[u8; 32]> {
        // The OWNER (this node's identity) signs the submission either way.
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        let sk = ShardKey {
            rtype: RT_PROGRAM,
            shard: shard_of(&self.self_id, name),
        };
        if self.is_writer(sk).await {
            return self.advance_local(sk, &sub.encode()).await;
        }
        // Non-writer: forward the signed submission to the shard's writer, which advances the
        // shard account and returns the new root.
        let addr = self.writer_addr(sk).await?;
        match request_registry(
            &self.transport,
            &addr,
            &RegistryReq::Submit {
                rtype: RT_PROGRAM,
                sub: sub.encode(),
            },
        )
        .await?
        {
            RegistryResp::SubmitAck(root) => Ok(root),
            RegistryResp::Err(e) => Err(anyhow::anyhow!("registry writer rejected submit: {e}")),
            other => Err(anyhow::anyhow!("unexpected registry response: {other:?}")),
        }
    }

    /// Resolve a name published by `owner` to its current cid. Routes the key to its shard;
    /// the shard's writer resolves locally, a non-writer queries that writer over
    /// `REGISTRY_ALPN` (no caching yet — see follow-up).
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        let sk = ShardKey {
            rtype: RT_PROGRAM,
            shard: shard_of(&owner, name),
        };
        if self.is_writer(sk).await {
            return self.resolve_local(sk, owner, name).await;
        }
        let addr = self.writer_addr(sk).await.ok()?;
        match request_registry(
            &self.transport,
            &addr,
            &RegistryReq::Resolve {
                rtype: RT_PROGRAM,
                owner,
                name: name.to_string(),
            },
        )
        .await
        .ok()?
        {
            RegistryResp::Resolved(cid) => cid,
            _ => None,
        }
    }

    /// Serve `REGISTRY_ALPN` requests: as a shard's writer, advance the shard account on
    /// `Submit`, resolve on `Resolve`, hand off state on `GetState`, report a key's version on
    /// `CurrentVersion`. The shard is derived from the request's key so a key always lands on
    /// the SAME shard as the registering node computed.
    pub async fn serve(self: Arc<Self>, mut conns: mpsc::Receiver<Connection>) {
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
    pub async fn current_version(&self, owner: [u8; 32], name: &str) -> u64 {
        let sk = ShardKey {
            rtype: RT_PROGRAM,
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
                rtype: RT_PROGRAM,
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

    /// Registry status for the dashboard: `(current_epoch, eligible_count, writer_shard_count,
    /// entries)`. `writer_shard_count` = the shards THIS node currently writes; `entries` = the
    /// total `(owner, name)` rows across exactly those shards (a per-node partial view — the
    /// registry is sharded, so no single node sees every shard).
    pub async fn status(&self) -> (u64, usize, usize, usize) {
        let epoch = self.effective_epoch();
        let eligible = self.eligible().await.len();
        let mut writer_shards = 0usize;
        let mut entries = 0usize;
        for shard in 0..SHARD_COUNT {
            let sk = ShardKey {
                rtype: RT_PROGRAM,
                shard,
            };
            if !self.is_writer(sk).await {
                continue;
            }
            writer_shards += 1;
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(sk))
                .await;
            entries += RegistryState::decode(&raw).map(|s| s.len()).unwrap_or(0);
        }
        (epoch, eligible, writer_shards, entries)
    }
}
