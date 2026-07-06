//! Phase 4c/4d — the live app-name registry backing on the node. `deploy` registers a
//! signed head into a durable [`RegistryState`]; resolution reads it. The state
//! persists to `<data_dir>/registry.state` and its encoded blob is published as a
//! durable system object.
//!
//! Authority: the registry is an **open, owner-signed CRDT** (partition-by-owner,
//! last-writer-wins per `(owner, name)`) — it converges by construction, so writes need
//! NO attestation / committee. Each node runs the governance-canonical registry program
//! LOCALLY to validate an owner-signed submission, then merges. See
//! `docs/VERIFICATION_DESIGN.md` §2 and `docs/REGISTRY_DESIGN.md` §2.1.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    pda, registry_program_cid, AttestedRuntime, HeadSubmission, RegistryState, DEFAULT_FUEL,
    REGISTRY_SEED,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;

/// Reserved app-name under which a node announces its registry HEAD pointer
/// (owner, this) -> current registry-state cid. Contains a control char so it can
/// never collide with a user app name (deploy rejects control chars).
pub const REGISTRY_HEAD_NAME: &str = "\u{1}registry-head";

/// The node's durable app-name registry.
pub struct AppRegistry {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    state: RwLock<RegistryState>,
    path: PathBuf,
    programs: RwLock<Option<Arc<crate::governance::GovernanceChainStore>>>,
    routing: Arc<dyn ContentRouting>,
    /// Deterministic runtime for running the RESOLVED registry program (native or WASM),
    /// so a governance-upgraded WASM program is authoritative on the coordinator too.
    runtime: AttestedRuntime,
}

impl AppRegistry {
    /// Open the registry, loading any persisted state from `<data_dir>/registry.state`.
    pub fn open(
        identity: Arc<NodeIdentity>,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
        data_dir: &Path,
    ) -> Self {
        let path = data_dir.join("registry.state");
        let state = std::fs::read(&path)
            .ok()
            .and_then(|b| RegistryState::decode(&b))
            .unwrap_or_default();
        Self {
            identity,
            obj,
            state: RwLock::new(state),
            path,
            programs: RwLock::new(None),
            routing,
            runtime: AttestedRuntime::new().expect("attested runtime"),
        }
    }

    /// Fetch a program's WASM bytes by cid (following a File manifest to its content).
    async fn fetch_program(&self, cid: [u8; 32]) -> Option<Vec<u8>> {
        let raw = self.obj.get(Cid(cid), ConsumeMode::Drop).await.ok()?;
        match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()
            }
            _ => Some(raw),
        }
    }

    /// Compute the registry transition by running the RESOLVED program. When the program
    /// is the native genesis cid, that's `RegistryState::apply`; when governance has
    /// upgraded it to WASM, the coordinator runs THAT wasm — so the upgraded logic is
    /// authoritative and not silently shadowed by native `apply`. `None` = the program
    /// rejected the submission (empty output) or the wasm is unavailable.
    async fn run_program(
        &self,
        prev: &RegistryState,
        sub: &HeadSubmission,
    ) -> Option<RegistryState> {
        let program = self.program_cid().await;
        if program == registry_program_cid() {
            return prev.apply(sub).ok(); // native genesis program
        }
        let wasm = self.fetch_program(program).await?;
        let out = self
            .runtime
            .run_transition(&wasm, "run", &prev.encode(), &sub.encode(), DEFAULT_FUEL)
            .ok()?;
        (!out.is_empty())
            .then(|| RegistryState::decode(&out))
            .flatten()
    }

    /// Wire the program registry so the app-registry program cid is resolved THROUGH it
    /// (upgradeable) rather than hardcoded.
    pub async fn set_programs(&self, programs: Arc<crate::governance::GovernanceChainStore>) {
        *self.programs.write().await = Some(programs);
    }

    /// The canonical app-registry program cid — resolved via the program registry
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

    /// The registry PDA account (program-derived; no keyholder). Retained for the
    /// coming anti-entropy sync + status surface (Track A step 3).
    #[allow(dead_code)]
    pub fn account(&self) -> NodeId {
        pda(&registry_program_cid(), REGISTRY_SEED)
    }

    /// Re-announce the current registry head — TTL keep-alive + backend migration
    /// (tracker→DHT). No-op if this node holds no registrations.
    pub async fn republish(&self, now_millis: u64) {
        let encoded = {
            let state = self.state.read().await;
            if state.entries().is_empty() {
                return;
            }
            state.encode()
        };
        if let Ok(head_cid) = self.obj.publish_system(&encoded).await {
            let _ = self
                .routing
                .announce_app(REGISTRY_HEAD_NAME, head_cid, now_millis)
                .await;
        }
    }

    /// Register (or advance) an app head under THIS node's identity. Runs the
    /// governance-canonical registry program LOCALLY to validate the owner-signed
    /// submission (open CRDT — no committee), persists the new state locally + publishes
    /// it as a durable object. Returns the new root.
    pub async fn register(
        &self,
        name: &str,
        cid: [u8; 32],
        version: u64,
        now_millis: u64,
    ) -> anyhow::Result<[u8; 32]> {
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        let mut guard = self.state.write().await;
        let prev = guard.clone();
        // Open registry — NO attestation. The registry is an owner-signed CRDT
        // (partition-by-owner, last-writer-wins), so it converges by construction and needs no
        // committee. Run the governance-canonical program LOCALLY to validate the owner-signed
        // submission, then merge. See docs/VERIFICATION_DESIGN.md §2 / docs/REGISTRY_DESIGN.md §2.1.
        let next = self
            .run_program(&prev, &sub)
            .await
            .ok_or_else(|| anyhow::anyhow!("registry program rejected the submission"))?;
        let encoded = next.encode();
        std::fs::write(&self.path, &encoded)?;
        // Publish the state as durable content, then announce our registry-head pointer
        // (owner, REGISTRY_HEAD_NAME) -> state cid, so other nodes resolve it. Version =
        // HLC millis (monotonic) satisfies the announce CAS.
        if let Ok(head_cid) = self.obj.publish_system(&encoded).await {
            let _ = self
                .routing
                .announce_app(REGISTRY_HEAD_NAME, head_cid, now_millis)
                .await;
        }
        let root = next.root();
        *guard = next;
        tracing::info!(
            name,
            version,
            "registry head advanced (open, no attestation)"
        );
        Ok(root)
    }

    /// Cross-node resolve: fetch `owner`'s announced (committee-attested) registry
    /// state from the network and resolve `name` within it — durable, no need for the
    /// owner to be online. Returns None if the owner has no registry head or `name`.
    pub async fn resolve_cross(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        let rec = self
            .routing
            .resolve_app(NodeId(owner), REGISTRY_HEAD_NAME)
            .await
            .ok()??;
        let raw = self.obj.get(rec.wasm_cid, ConsumeMode::Drop).await.ok()?;
        // publish_system wraps content in a File manifest — follow it to the bytes.
        let bytes = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()?
            }
            _ => raw,
        };
        RegistryState::decode(&bytes)?
            .resolve(&owner, name)
            .map(|e| e.cid)
    }

    /// Resolve a name published by `owner` to its current cid.
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        self.state.read().await.resolve(&owner, name).map(|e| e.cid)
    }

    /// Snapshot the registry as `(owner_hex, name, cid_hex, version)` rows for the UI.
    /// Retained for the status surface reworked in Track A step 3.
    #[allow(dead_code)]
    pub async fn rows(&self) -> Vec<(String, String, String, u64)> {
        self.state
            .read()
            .await
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
    /// Retained for the status surface reworked in Track A step 3.
    #[allow(dead_code)]
    pub async fn summary(&self) -> (usize, String) {
        let s = self.state.read().await;
        (s.len(), hex::encode(s.root()))
    }
}
