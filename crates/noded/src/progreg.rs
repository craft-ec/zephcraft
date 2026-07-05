//! The program registry store — the native bootstrap map `network-program name →
//! canonical wasm cid`. It is **seeded at genesis with the app-registry program** at its
//! native cid, so name resolution already routes THROUGH the registry (indirection) —
//! which is what makes network-owned programs upgradeable: a governance `SetProgram`
//! approval repoints a program at new WASM, no binary release.
//!
//! Writes are governance-authorized (the daemon records a `SetProgram` only after the
//! governance store has verified + applied the approval). Persists to
//! `<data_dir>/programs.state`.

use std::path::{Path, PathBuf};

use tokio::sync::RwLock;
use zeph_com::{registry_program_cid, ProgramRegistryState};

pub struct ProgramRegistryStore {
    state: RwLock<ProgramRegistryState>,
    path: PathBuf,
}

impl ProgramRegistryStore {
    /// Open the store, loading persisted state or seeding genesis. Genesis always
    /// contains `app-registry → registry_program_cid()` (the native cid) so resolution
    /// goes through here from the first boot.
    pub fn open(data_dir: &Path) -> Self {
        let path = data_dir.join("programs.state");
        let mut state = std::fs::read(&path)
            .ok()
            .and_then(|b| ProgramRegistryState::decode(&b))
            .unwrap_or_default();
        if state.resolve("app-registry").is_none() {
            if let Ok(seeded) = state.set("app-registry", registry_program_cid(), 0) {
                state = seeded;
            }
        }
        Self {
            state: RwLock::new(state),
            path,
        }
    }

    /// Record a governance-approved program cid (version = the governance seq, monotonic).
    pub async fn record(&self, name: &str, cid: [u8; 32], version: u64) -> anyhow::Result<()> {
        let mut guard = self.state.write().await;
        let next = guard
            .set(name, cid, version)
            .map_err(|e| anyhow::anyhow!("program registry: {e}"))?;
        std::fs::write(&self.path, next.encode())?;
        tracing::info!(name, version, "program registry updated (governance)");
        *guard = next;
        Ok(())
    }

    /// Resolve a network-owned program's canonical cid (falls back to the native cid for
    /// the app-registry program if somehow unset).
    pub async fn resolve(&self, name: &str) -> Option<[u8; 32]> {
        self.state
            .read()
            .await
            .resolve(name)
            .or_else(|| (name == "app-registry").then(registry_program_cid))
    }

    /// `(name, cid_hex, version)` rows for the dashboard.
    pub async fn rows(&self) -> Vec<(String, String, u64)> {
        self.state
            .read()
            .await
            .entries()
            .iter()
            .map(|(n, c, v)| (n.clone(), hex::encode(c), *v))
            .collect()
    }
}
