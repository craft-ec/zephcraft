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

/// The per-shard account seed — replaces the bare `REGISTRY_SEED` in every account op so each
/// shard gets a distinct `pda(registry_program_cid(), shard_seed(shard))`.
fn shard_seed(shard: u64) -> Vec<u8> {
    [REGISTRY_SEED, shard.to_le_bytes().as_slice()].concat()
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
    /// Per-shard: the last effective epoch for which this node (as that shard's writer) has
    /// performed the state handoff. Guards [`Self::ensure_current`] so the previous-writer
    /// fetch runs once per epoch per shard.
    last_epoch: RwLock<std::collections::HashMap<u64, u64>>,
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

    /// Deterministic per-shard per-epoch election: the eligible id with the SMALLEST
    /// `blake3(shard_le ‖ epoch_le ‖ node_id)`. Because every node computes this over the same
    /// shard + epoch + membership view, the write duty rotates without any coordination, and
    /// different shards can elect different nodes at the same moment.
    fn elect(shard: u64, epoch: u64, eligible: &[[u8; 32]]) -> Option<[u8; 32]> {
        eligible.iter().copied().min_by_key(|id| {
            Cid::of(
                &[
                    shard.to_le_bytes().as_slice(),
                    epoch.to_le_bytes().as_slice(),
                    id.as_slice(),
                ]
                .concat(),
            )
            .0
        })
    }

    /// The writer for `shard` in the CURRENT (effective) epoch. Empty eligibility (membership
    /// not yet wired) → self, so a fresh/single node still writes every shard.
    async fn current_writer(&self, shard: u64) -> Option<[u8; 32]> {
        let elig = self.eligible().await;
        if elig.is_empty() {
            return Some(self.self_id);
        }
        Self::elect(shard, self.effective_epoch(), &elig)
    }

    /// True if THIS node is `shard`'s current (effective-epoch) elected writer.
    async fn is_writer(&self, shard: u64) -> bool {
        self.current_writer(shard).await == Some(self.self_id)
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

    /// `shard`'s current writer's dialable address. Errors if this node IS the writer (caller
    /// should act locally) or the writer is not in the membership view.
    async fn writer_addr(&self, shard: u64) -> anyhow::Result<PeerAddr> {
        let writer = self
            .current_writer(shard)
            .await
            .ok_or_else(|| anyhow::anyhow!("no registry writer for current epoch"))?;
        if writer == self.self_id {
            anyhow::bail!("this node IS the current registry writer");
        }
        self.addr_of(writer)
            .await
            .ok_or_else(|| anyhow::anyhow!("registry writer not in membership view"))
    }

    /// STATE HANDOFF: when this node becomes `shard`'s writer for a NEW effective epoch, adopt
    /// the PREVIOUS epoch's writer's shard state before serving, so registrations survive
    /// rotation. Best-effort and idempotent per epoch per shard (guarded by `last_epoch`).
    ///
    /// Edge cases (see also `.claude/feature-progress.md`):
    /// (a) the grace window ([`Self::effective_epoch`]) removes the clock-skew dual-writer race
    ///     at epoch boundaries while skew < grace;
    /// (b) if the previous writer is unreachable at handoff, we keep local/last-known state;
    /// (c) the FULL shard state is transferred each rotation — fine while a shard is small;
    ///     later hand off the cid and fetch the content lazily.
    async fn ensure_current(&self, shard: u64) {
        if !self.is_writer(shard).await {
            return;
        }
        let eff = self.effective_epoch();
        if eff
            <= self
                .last_epoch
                .read()
                .await
                .get(&shard)
                .copied()
                .unwrap_or(0)
        {
            return;
        }
        let elig = self.eligible().await;
        if let Some(prev) = Self::elect(shard, eff - 1, &elig) {
            if prev != self.self_id {
                if let Some(addr) = self.addr_of(prev).await {
                    if let Ok(RegistryResp::State(bytes)) =
                        request_registry(&self.transport, &addr, &RegistryReq::GetState { shard })
                            .await
                    {
                        if !bytes.is_empty() {
                            let _ = self
                                .store
                                .put_state(registry_program_cid(), &shard_seed(shard), &bytes)
                                .await;
                        }
                    }
                }
            }
        }
        // Best-effort: mark this epoch handled regardless — on failure we keep local state.
        self.last_epoch.write().await.insert(shard, eff);
    }

    /// Advance `shard`'s registry account from an owner-signed submission's raw bytes.
    /// Runs the governance-resolved registry program on the writer's own store — the same
    /// advance logic as the local writer path, but on already-signed bytes (no re-sign).
    async fn advance_local(&self, shard: u64, sub_bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
        // Adopt the previous epoch's state if we've just become the writer, before advancing.
        self.ensure_current(shard).await;
        let code = self.program_cid().await;
        // NATIVE DEFAULT (the built-in local registry program). When governance has NOT set a WASM
        // registry program, run `RegistryState::apply` directly — so a FRESH network self-starts
        // with no publish/governance bootstrap. A governance `SetProgram` swaps in the WASM (e.g.
        // the char-limit v2) as the upgrade on top. (MINIMAL_KERNEL: every anchor has a default.)
        if code == registry_program_cid() {
            let seed = shard_seed(shard);
            let prev =
                RegistryState::decode(&self.store.resolve(registry_program_cid(), &seed).await)
                    .unwrap_or_default();
            let sub = HeadSubmission::decode(sub_bytes)
                .ok_or_else(|| anyhow::anyhow!("bad head submission"))?;
            let next = prev
                .apply(&sub)
                .map_err(|e| anyhow::anyhow!("registry rejected the submission: {e}"))?;
            self.store
                .put_state(registry_program_cid(), &seed, &next.encode())
                .await?;
            return Ok(next.root());
        }
        let r = self
            .store
            .advance(registry_program_cid(), code, &shard_seed(shard), sub_bytes)
            .await?;
        Ok(r.new_root)
    }

    /// Local resolve against this node's own copy of `shard`'s registry account.
    async fn resolve_local(&self, shard: u64, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        // Adopt the previous epoch's state if we've just become the writer, before resolving.
        self.ensure_current(shard).await;
        let raw = self
            .store
            .resolve(registry_program_cid(), &shard_seed(shard))
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
        let shard = shard_of(&self.self_id, name);
        if self.is_writer(shard).await {
            return self.advance_local(shard, &sub.encode()).await;
        }
        // Non-writer: forward the signed submission to the shard's writer, which advances the
        // shard account and returns the new root.
        let addr = self.writer_addr(shard).await?;
        match request_registry(&self.transport, &addr, &RegistryReq::Submit(sub.encode())).await? {
            RegistryResp::SubmitAck(root) => Ok(root),
            RegistryResp::Err(e) => Err(anyhow::anyhow!("registry writer rejected submit: {e}")),
            other => Err(anyhow::anyhow!("unexpected registry response: {other:?}")),
        }
    }

    /// Resolve a name published by `owner` to its current cid. Routes the key to its shard;
    /// the shard's writer resolves locally, a non-writer queries that writer over
    /// `REGISTRY_ALPN` (no caching yet — see follow-up).
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        let shard = shard_of(&owner, name);
        if self.is_writer(shard).await {
            return self.resolve_local(shard, owner, name).await;
        }
        let addr = self.writer_addr(shard).await.ok()?;
        match request_registry(
            &self.transport,
            &addr,
            &RegistryReq::Resolve {
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
                        Ok(RegistryReq::Submit(sub)) => match HeadSubmission::decode(&sub) {
                            Some(s) => {
                                let shard = shard_of(&s.owner, &s.name);
                                match this.advance_local(shard, &sub).await {
                                    Ok(root) => RegistryResp::SubmitAck(root),
                                    Err(e) => RegistryResp::Err(e.to_string()),
                                }
                            }
                            None => RegistryResp::Err("bad submission".into()),
                        },
                        Ok(RegistryReq::Resolve { owner, name }) => {
                            let shard = shard_of(&owner, &name);
                            RegistryResp::Resolved(this.resolve_local(shard, owner, &name).await)
                        }
                        // Serve the full current shard state for the epoch handoff.
                        Ok(RegistryReq::GetState { shard }) => RegistryResp::State(
                            this.store
                                .resolve(registry_program_cid(), &shard_seed(shard))
                                .await,
                        ),
                        // Report the current version of a key from its shard (0 if none).
                        Ok(RegistryReq::CurrentVersion { owner, name }) => {
                            let shard = shard_of(&owner, &name);
                            let raw = this
                                .store
                                .resolve(registry_program_cid(), &shard_seed(shard))
                                .await;
                            let v = RegistryState::decode(&raw)
                                .and_then(|s| s.resolve(&owner, &name).map(|e| e.version))
                                .unwrap_or(0);
                            RegistryResp::Version(v)
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
        let shard = shard_of(&owner, name);
        if self.is_writer(shard).await {
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(shard))
                .await;
            return RegistryState::decode(&raw)
                .and_then(|s| s.resolve(&owner, name).map(|e| e.version))
                .unwrap_or(0);
        }
        let Ok(addr) = self.writer_addr(shard).await else {
            return 0;
        };
        match request_registry(
            &self.transport,
            &addr,
            &RegistryReq::CurrentVersion {
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
            if !self.is_writer(shard).await {
                continue;
            }
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(shard))
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
            if !self.is_writer(shard).await {
                continue;
            }
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(shard))
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
            if !self.is_writer(shard).await {
                continue;
            }
            writer_shards += 1;
            let raw = self
                .store
                .resolve(registry_program_cid(), &shard_seed(shard))
                .await;
            entries += RegistryState::decode(&raw).map(|s| s.len()).unwrap_or(0);
        }
        (epoch, eligible, writer_shards, entries)
    }
}
