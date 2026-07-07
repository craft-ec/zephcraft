//! Registry-backed CraftSQL head stores. CraftSQL's DB roots (`KIND_ROOT`) and durability
//! manifests (`KIND_MANIFEST`) used to be announced over the DHT (`RoutingRootStore` /
//! `RoutingManifestStore`); these route them through the SAME owner-signed registry substrate
//! that carries program heads instead — persisted + replicated by the program-account store, so
//! no separate DHT announce is needed. Each kind rides its own account tag ([`RT_DBROOT`] /
//! [`RT_MANIFEST`]), so DB roots and manifests never collide with program heads.
//!
//! Owner is IMPLICIT: the registry signs every submission as this node's identity, so a
//! [`zeph_sql::RootStore::publish`] publishes MY head. Resolution is keyed by `(owner, ns)`.

use std::sync::Arc;

use zeph_core::hlc::Clock;
use zeph_core::{Cid, NodeId};
use zeph_sql::{ManifestStore, Result, RootStore, SqlError};

use crate::programreg::{ProgramRegistry, RT_DBROOT, RT_MANIFEST};

/// [`RootStore`] backed by the owner-signed registry ([`RT_DBROOT`]). Single-writer per DB, so
/// `prev` (the CAS expectation) is intentionally ignored — the registry is LWW-by-seq and the
/// prior DHT backend already ignored `prev`, so no compare-and-swap is lost.
pub struct RegistryRootStore {
    reg: Arc<ProgramRegistry>,
    clock: Arc<Clock>,
}

impl RegistryRootStore {
    pub fn new(reg: Arc<ProgramRegistry>, clock: Arc<Clock>) -> Self {
        Self { reg, clock }
    }
}

#[async_trait::async_trait]
impl RootStore for RegistryRootStore {
    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>> {
        Ok(self
            .reg
            .resolve_entry(RT_DBROOT, owner.0, namespace)
            .await
            .map(|(c, v)| (Cid(c), v)))
    }

    async fn publish(
        &self,
        namespace: &str,
        root: Cid,
        _prev: Option<Cid>,
        seq: u64,
    ) -> Result<()> {
        match self
            .reg
            .register(RT_DBROOT, namespace, root.0, seq, self.clock.now().millis())
            .await
        {
            Ok(_) => Ok(()),
            // The registry rejects a non-advancing version with a "stale-version" error — that is
            // the head moving under us (another/racing write), so surface it as a CAS Conflict.
            Err(e) if e.to_string().contains("stale") => Err(SqlError::Conflict),
            Err(e) => Err(SqlError::Sqlite(e.to_string())),
        }
    }
}

/// [`ManifestStore`] backed by the owner-signed registry ([`RT_MANIFEST`]). Manifests carry no
/// CAS — a publish is a plain LWW-by-seq advance, so any error maps to [`SqlError::Sqlite`].
pub struct RegistryManifestStore {
    reg: Arc<ProgramRegistry>,
    clock: Arc<Clock>,
}

impl RegistryManifestStore {
    pub fn new(reg: Arc<ProgramRegistry>, clock: Arc<Clock>) -> Self {
        Self { reg, clock }
    }
}

#[async_trait::async_trait]
impl ManifestStore for RegistryManifestStore {
    async fn publish(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()> {
        self.reg
            .register(
                RT_MANIFEST,
                namespace,
                manifest_cid.0,
                seq,
                self.clock.now().millis(),
            )
            .await
            .map(|_| ())
            .map_err(|e| SqlError::Sqlite(e.to_string()))
    }

    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>> {
        Ok(self
            .reg
            .resolve_entry(RT_MANIFEST, owner.0, namespace)
            .await
            .map(|(c, v)| (Cid(c), v)))
    }
}
