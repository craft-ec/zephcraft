//! Phase 4c/4d — the live program-name registry, a THIN consumer of the generic
//! [`ProgramAccountStore`] substrate. `deploy` registers a signed head by advancing the
//! registry account (`pda(registry_program_cid(), REGISTRY_SEED)`); resolution reads that
//! account's state. The account state itself is persisted + published durably by the store —
//! this type holds no state of its own.
//!
//! Authority: the registry is an **open, owner-signed CRDT** (partition-by-owner,
//! last-writer-wins per `(owner, name)`) — it converges by construction, so writes need
//! NO attestation / committee. The store runs the governance-canonical registry program
//! LOCALLY to validate an owner-signed submission, then merges. See
//! `docs/VERIFICATION_DESIGN.md` §2 and `docs/REGISTRY_DESIGN.md` §2.1.

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

/// The node's durable program-name registry — a thin consumer of [`ProgramAccountStore`].
///
/// Cross-node model: the writer for an epoch is ELECTED DETERMINISTICALLY (a rotating writer),
/// so the write duty circulates among active nodes. `writer(epoch)` = the eligible member
/// (self + active membership) with the smallest `blake3(epoch_le ‖ node_id)`. Every node
/// computes the same winner from its membership view + HLC clock. If this node is the current
/// epoch's writer it advances and resolves locally; otherwise it forwards registrations and
/// queries to the current writer over `REGISTRY_ALPN`. As the epoch rotates the writer is
/// recomputed, and the new writer adopts the previous writer's state (see [`Self::ensure_current`]).
pub struct ProgramRegistry {
    identity: Arc<NodeIdentity>,
    /// Shared generic program-account store — the registry's state lives in the account
    /// `pda(registry_program_cid(), REGISTRY_SEED)` here.
    store: Arc<ProgramAccountStore>,
    /// Governance chain — resolves the EXECUTING registry program cid (upgradeable).
    programs: RwLock<Option<Arc<crate::governance::GovernanceChainStore>>>,
    /// HLC clock — drives the epoch (`now().millis() / EPOCH_MILLIS`) that elects the writer.
    clock: Arc<zeph_core::hlc::Clock>,
    /// Transport for forwarding to the current writer (non-writer nodes only).
    transport: Arc<Transport>,
    /// Membership — the active set feeds the election AND locates the writer's dialable
    /// [`PeerAddr`]. Wired after open.
    membership: RwLock<Option<Arc<Membership>>>,
    /// This node's own id.
    self_id: [u8; 32],
    /// The last epoch for which this node (as writer) has performed the state handoff. Guards
    /// [`Self::ensure_current`] so the previous-writer fetch runs once per epoch.
    last_epoch: RwLock<u64>,
}

impl ProgramRegistry {
    /// Open the registry over a shared program-account store. The `clock` drives the per-epoch
    /// writer election — the writer is computed, not configured.
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
            last_epoch: RwLock::new(0),
        }
    }

    /// Wire the governance chain so the app-registry program cid is resolved THROUGH it
    /// (upgradeable) rather than hardcoded.
    pub async fn set_programs(&self, programs: Arc<crate::governance::GovernanceChainStore>) {
        *self.programs.write().await = Some(programs);
    }

    /// Wire membership (built after the registry) so a non-writer can locate the writer.
    pub async fn set_membership(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }

    /// The current registry epoch = `clock.now().millis() / EPOCH_MILLIS`. Every node derives
    /// the same epoch from its HLC, so the elected writer rotates in lockstep.
    fn current_epoch(&self) -> u64 {
        self.clock.now().millis() / EPOCH_MILLIS
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

    /// Deterministic per-epoch election: the eligible id with the SMALLEST
    /// `blake3(epoch_le ‖ node_id)`. Because every node computes this over the same epoch +
    /// membership view, the write duty rotates without any coordination.
    fn elect(epoch: u64, eligible: &[[u8; 32]]) -> Option<[u8; 32]> {
        eligible
            .iter()
            .copied()
            .min_by_key(|id| Cid::of(&[epoch.to_le_bytes().as_slice(), id.as_slice()].concat()).0)
    }

    /// The writer for the CURRENT epoch (recomputed as the epoch rotates). Empty eligibility
    /// (membership not yet wired) → self, so a fresh/single node still writes.
    async fn current_writer(&self) -> Option<[u8; 32]> {
        let elig = self.eligible().await;
        if elig.is_empty() {
            return Some(self.self_id);
        }
        Self::elect(self.current_epoch(), &elig)
    }

    /// True if THIS node is the current epoch's elected registry writer.
    async fn is_writer(&self) -> bool {
        self.current_writer().await == Some(self.self_id)
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

    /// The current writer's dialable address. Errors if this node IS the writer (caller should
    /// act locally) or the writer is not in the membership view.
    async fn writer_addr(&self) -> anyhow::Result<PeerAddr> {
        let writer = self
            .current_writer()
            .await
            .ok_or_else(|| anyhow::anyhow!("no registry writer for current epoch"))?;
        if writer == self.self_id {
            anyhow::bail!("this node IS the current registry writer");
        }
        self.addr_of(writer)
            .await
            .ok_or_else(|| anyhow::anyhow!("registry writer not in membership view"))
    }

    /// STATE HANDOFF: when this node becomes writer for a NEW epoch, adopt the PREVIOUS epoch's
    /// writer's registry state before serving, so registrations survive rotation. Best-effort
    /// and idempotent per epoch (guarded by `last_epoch`).
    ///
    /// Edge cases (see also `.claude/feature-progress.md`):
    /// (a) clock-skew races at epoch boundaries can briefly yield two writers — a write made to
    ///     the "wrong" writer in that window may be lost;
    /// (b) if the previous writer is unreachable at handoff, we keep local/last-known state;
    /// (c) the FULL state is transferred each rotation — fine while the registry is small; later
    ///     hand off the cid and fetch the content lazily.
    async fn ensure_current(&self) {
        if !self.is_writer().await {
            return;
        }
        let epoch = self.current_epoch();
        if epoch <= *self.last_epoch.read().await {
            return;
        }
        let elig = self.eligible().await;
        if let Some(prev) = Self::elect(epoch - 1, &elig) {
            if prev != self.self_id {
                if let Some(addr) = self.addr_of(prev).await {
                    if let Ok(RegistryResp::State(bytes)) =
                        request_registry(&self.transport, &addr, &RegistryReq::GetState).await
                    {
                        if !bytes.is_empty() {
                            let _ = self
                                .store
                                .put_state(registry_program_cid(), REGISTRY_SEED, &bytes)
                                .await;
                        }
                    }
                }
            }
        }
        // Best-effort: mark this epoch handled regardless — on failure we keep local state.
        *self.last_epoch.write().await = epoch;
    }

    /// Advance the global registry account from an owner-signed submission's raw bytes.
    /// Runs the governance-resolved registry program on the writer's own store — the same
    /// advance logic as the local writer path, but on already-signed bytes (no re-sign).
    async fn advance_local(&self, sub_bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
        // Adopt the previous epoch's state if we've just become the writer, before advancing.
        self.ensure_current().await;
        let code = self.program_cid().await;
        let r = self
            .store
            .advance(registry_program_cid(), code, REGISTRY_SEED, sub_bytes)
            .await?;
        Ok(r.new_root)
    }

    /// Local resolve against this node's own copy of the registry account.
    async fn resolve_local(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        // Adopt the previous epoch's state if we've just become the writer, before resolving.
        self.ensure_current().await;
        let raw = self
            .store
            .resolve(registry_program_cid(), REGISTRY_SEED)
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

    /// Register (or advance) an app head under THIS node's identity. Advances the registry
    /// account over its STABLE address (`registry_program_cid()`) while executing the
    /// governance-resolved registry program (`program_cid()`) — so an upgraded WASM program
    /// is authoritative without moving the account. The store validates the owner-signed
    /// submission (open CRDT — no committee), persists + publishes the new state. Returns
    /// the new root.
    pub async fn register(
        &self,
        name: &str,
        cid: [u8; 32],
        version: u64,
        _now_millis: u64,
    ) -> anyhow::Result<[u8; 32]> {
        // The OWNER (this node's identity) signs the submission either way.
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        if self.is_writer().await {
            return self.advance_local(&sub.encode()).await;
        }
        // Non-writer: forward the signed submission to the writer, which advances the
        // global account and returns the new root.
        let addr = self.writer_addr().await?;
        match request_registry(&self.transport, &addr, &RegistryReq::Submit(sub.encode())).await? {
            RegistryResp::SubmitAck(root) => Ok(root),
            RegistryResp::Err(e) => Err(anyhow::anyhow!("registry writer rejected submit: {e}")),
            other => Err(anyhow::anyhow!("unexpected registry response: {other:?}")),
        }
    }

    /// Resolve a name published by `owner` to its current cid. The writer resolves locally;
    /// a non-writer queries the writer over `REGISTRY_ALPN` (no caching yet — see follow-up).
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        if self.is_writer().await {
            return self.resolve_local(owner, name).await;
        }
        let addr = self.writer_addr().await.ok()?;
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

    /// Serve `REGISTRY_ALPN` requests: as the writer, advance the global account on `Submit`
    /// and resolve on `Resolve`. Mirrors the removed `serve_attestations` accept/decode/reply
    /// shape.
    pub async fn serve(self: Arc<Self>, mut conns: mpsc::Receiver<Connection>) {
        while let Some(conn) = conns.recv().await {
            let this = self.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                        break;
                    };
                    let resp = match postcard::from_bytes::<RegistryReq>(&bytes) {
                        Ok(RegistryReq::Submit(sub)) => match this.advance_local(&sub).await {
                            Ok(root) => RegistryResp::SubmitAck(root),
                            Err(e) => RegistryResp::Err(e.to_string()),
                        },
                        Ok(RegistryReq::Resolve { owner, name }) => {
                            RegistryResp::Resolved(this.resolve_local(owner, &name).await)
                        }
                        // Serve the full current registry state for the epoch handoff.
                        Ok(RegistryReq::GetState) => RegistryResp::State(
                            this.store
                                .resolve(registry_program_cid(), REGISTRY_SEED)
                                .await,
                        ),
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
    /// `prev + 1` from the registry itself — no DHT lookup.
    pub async fn current_version(&self, owner: [u8; 32], name: &str) -> u64 {
        let raw = self
            .store
            .resolve(registry_program_cid(), REGISTRY_SEED)
            .await;
        RegistryState::decode(&raw)
            .and_then(|s| s.resolve(&owner, name).map(|e| e.version))
            .unwrap_or(0)
    }

    /// Snapshot the registry as `(owner_hex, name, cid_hex, version)` rows for the UI.
    #[allow(dead_code)]
    pub async fn rows(&self) -> Vec<(String, String, String, u64)> {
        let raw = self
            .store
            .resolve(registry_program_cid(), REGISTRY_SEED)
            .await;
        RegistryState::decode(&raw)
            .unwrap_or_default()
            .entries()
            .iter()
            .map(|e| {
                (
                    hex::encode(e.owner),
                    e.name.clone(),
                    hex::encode(e.cid),
                    e.version,
                )
            })
            .collect()
    }

    /// The number of registered heads + the current root (for status).
    #[allow(dead_code)]
    pub async fn summary(&self) -> (usize, String) {
        let raw = self
            .store
            .resolve(registry_program_cid(), REGISTRY_SEED)
            .await;
        let s = RegistryState::decode(&raw).unwrap_or_default();
        (s.len(), hex::encode(s.root()))
    }
}
