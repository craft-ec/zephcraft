//! Phase 4c — the live app-name registry backing on the node. `deploy` registers a
//! signed head into a durable [`RegistryState`]; resolution reads it. The state
//! persists to `<data_dir>/registry.state` across restart, and its network durability
//! rides CraftOBJ (the encoded state is published as a system object).
//!
//! This runs ALONGSIDE the `KIND_APP` tracker path (network discovery) during
//! migration — the registry is the durable, program-owned backing; the tracker head is
//! still what other nodes resolve until the committee head-store lands (phase 4d).
//!
//! v1 ramp: the node self-attests its own registry transition (`n = 1`). The exact
//! same call becomes a committee `collect_commit` when wired to the membership-derived
//! committee — the records and validation are identical, only the quorum grows.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    attest_transition, pda, registry_program_cid, HeadSubmission, RegistryState, REGISTRY_SEED,
};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_obj::ObjEngine;

/// The node's durable app-name registry.
pub struct AppRegistry {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    state: RwLock<RegistryState>,
    path: PathBuf,
}

impl AppRegistry {
    /// Open the registry, loading any persisted state from `<data_dir>/registry.state`.
    pub fn open(identity: Arc<NodeIdentity>, obj: Arc<ObjEngine>, data_dir: &Path) -> Self {
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
        }
    }

    /// The registry PDA account (program-derived; no keyholder).
    pub fn account(&self) -> NodeId {
        pda(&registry_program_cid(), REGISTRY_SEED)
    }

    /// Register (or advance) an app head under THIS node's identity: sign the head, run
    /// the deterministic registry transition, self-attest (v1 ramp), persist locally,
    /// and publish the encoded state as a durable system object. Returns the new root.
    pub async fn register(
        &self,
        name: &str,
        cid: [u8; 32],
        version: u64,
    ) -> anyhow::Result<[u8; 32]> {
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        let mut guard = self.state.write().await;
        let prev_root = guard.root();
        let next = guard
            .apply(&sub)
            .map_err(|e| anyhow::anyhow!("registry: {e}"))?;
        // v1 ramp: the node attests its own deterministic transition (n=1). With a
        // committee this becomes collect_commit over the members — same transition.
        let _att = attest_transition(
            &self.identity,
            registry_program_cid(),
            prev_root,
            &sub.encode(),
            &next.encode(),
        );
        let encoded = next.encode();
        std::fs::write(&self.path, &encoded)?;
        // Network durability: the state blob is content (erasure-coded like any object).
        let _ = self.obj.publish_system(&encoded).await;
        let root = next.root();
        *guard = next;
        Ok(root)
    }

    /// Resolve a name published by `owner` to its current cid.
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        self.state.read().await.resolve(&owner, name).map(|e| e.cid)
    }

    /// Snapshot the registry as `(owner_hex, name, cid_hex, version)` rows for the UI.
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
    pub async fn summary(&self) -> (usize, String) {
        let s = self.state.read().await;
        (s.len(), hex::encode(s.root()))
    }
}
