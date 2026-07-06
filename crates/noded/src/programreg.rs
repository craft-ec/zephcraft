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

use tokio::sync::RwLock;
use zeph_com::{registry_program_cid, HeadSubmission, RegistryState, REGISTRY_SEED};
use zeph_crypto::NodeIdentity;

use crate::account::ProgramAccountStore;

/// The node's durable program-name registry — a thin consumer of [`ProgramAccountStore`].
pub struct ProgramRegistry {
    identity: Arc<NodeIdentity>,
    /// Shared generic program-account store — the registry's state lives in the account
    /// `pda(registry_program_cid(), REGISTRY_SEED)` here.
    store: Arc<ProgramAccountStore>,
    /// Governance chain — resolves the EXECUTING registry program cid (upgradeable).
    programs: RwLock<Option<Arc<crate::governance::GovernanceChainStore>>>,
}

impl ProgramRegistry {
    /// Open the registry over a shared program-account store.
    pub fn open(identity: Arc<NodeIdentity>, store: Arc<ProgramAccountStore>) -> Self {
        Self {
            identity,
            store,
            programs: RwLock::new(None),
        }
    }

    /// Wire the governance chain so the app-registry program cid is resolved THROUGH it
    /// (upgradeable) rather than hardcoded.
    pub async fn set_programs(&self, programs: Arc<crate::governance::GovernanceChainStore>) {
        *self.programs.write().await = Some(programs);
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
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        let code = self.program_cid().await;
        let r = self
            .store
            .advance(registry_program_cid(), code, REGISTRY_SEED, &sub.encode())
            .await?;
        Ok(r.new_root)
    }

    /// Resolve a name published by `owner` to its current cid.
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        let raw = self
            .store
            .resolve(registry_program_cid(), REGISTRY_SEED)
            .await;
        RegistryState::decode(&raw)?
            .resolve(&owner, name)
            .map(|e| e.cid)
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
