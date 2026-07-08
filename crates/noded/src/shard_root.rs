//! Blob-backed [`RootStore`] for the registry's per-shard CraftSQL databases.
//!
//! The head registry stores USER database roots (RT_DBROOT). If a registry SHARD is itself a
//! CraftSQL database, that shard-DB's root must NOT publish back through the registry — that would
//! be the shard depending on the very registry it is. This store breaks the recursion: it keeps
//! each shard-DB's `(root_cid, seq)` pointer in a plain [`ProgramAccountStore`] account (a ~40-byte
//! blob at `pda(registry_program_cid(), "regshardroot/<ns>")`), while the DB PAGES live in CraftOBJ
//! via the engine's durable store. So the shard-DB root routes to a plain account blob, never back
//! through `HeadRegistry`. See `docs/SQL_REGISTRY_DESIGN.md` §2.
//!
//! All shard DBs are owned by THIS node (the engine's single writer identity); the `namespace`
//! (`reg/<rtype>/<bits>/<shard>`) distinguishes them, so `owner` is ignored here.

use std::sync::Arc;

use zeph_com::registry_program_cid;
use zeph_core::{Cid, NodeId};
use zeph_sql::{Result as SqlResult, RootStore, SqlError};

use crate::account::ProgramAccountStore;

pub struct ShardRootStore {
    store: Arc<ProgramAccountStore>,
}

impl ShardRootStore {
    pub fn new(store: Arc<ProgramAccountStore>) -> Self {
        Self { store }
    }

    /// Account seed holding the `(root_cid, seq)` pointer for shard-DB `ns`. Own prefix so it never
    /// collides with a shard's old `RegistryState` blob account or the generation marker.
    fn seed(ns: &str) -> Vec<u8> {
        [b"regshardroot/".as_slice(), ns.as_bytes()].concat()
    }

    /// Drop the stored root pointer for `ns` (on shard-DB GC).
    pub async fn clear(&self, ns: &str) {
        self.store
            .clear(registry_program_cid(), &Self::seed(ns))
            .await;
    }
}

#[async_trait::async_trait]
impl RootStore for ShardRootStore {
    async fn resolve(&self, _owner: NodeId, namespace: &str) -> SqlResult<Option<(Cid, u64)>> {
        let raw = self
            .store
            .resolve(registry_program_cid(), &Self::seed(namespace))
            .await;
        if raw.len() == 40 {
            let root = Cid(raw[..32].try_into().expect("32 bytes"));
            let seq = u64::from_le_bytes(raw[32..40].try_into().expect("8 bytes"));
            Ok(Some((root, seq)))
        } else {
            Ok(None)
        }
    }

    async fn publish(
        &self,
        namespace: &str,
        root: Cid,
        _prev: Option<Cid>,
        seq: u64,
    ) -> SqlResult<()> {
        // Single-writer per shard, so CAS on `prev` is unnecessary — LWW-by-seq, mirroring the
        // registry's other head stores. Persist the 40-byte `(root, seq)` pointer.
        let mut buf = Vec::with_capacity(40);
        buf.extend_from_slice(&root.0);
        buf.extend_from_slice(&seq.to_le_bytes());
        self.store
            .put_state(registry_program_cid(), &Self::seed(namespace), &buf)
            .await
            .map_err(|e| SqlError::Sqlite(format!("shard root publish: {e}")))
    }
}
