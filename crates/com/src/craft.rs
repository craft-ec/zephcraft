//! [`CraftBackend`] — the real substrate behind [`AppBackend`]: an agent's host
//! functions routed to the node's CraftSQL + CraftOBJ engines (phase 3).
//!
//! User-level app data lives under the `app.<name>` CraftSQL namespace, owned by
//! the invoking user's identity — separate from personal namespaces (e.g. the
//! drive's `owned`), consistent with the userspace model. `sql_execute` writes the
//! OWN `(own, app.ns)`; `sql_query` reads own or another participant's `(·, app.ns)`
//! — the same-namespace confinement the structural gate already guarantees.

use std::sync::Arc;

use async_trait::async_trait;
use zeph_cipher::{grant, EncKeypair, EncPublicKey};
use zeph_core::{hlc::Clock, Cid, NodeId};
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_sql::CraftSql;

use crate::AppBackend;

/// Map an app name to its CraftSQL namespace. Keeps app state out of personal
/// namespaces: app "feed" → `app.feed`. (A `.` separator, not `/` — the namespace
/// is used as a VFS db name, so path separators would break file resolution.)
fn ns_of(name: &str) -> String {
    format!("app.{name}")
}

/// [`AppBackend`] backed by the node's CraftSQL + CraftOBJ. Bound to ONE identity
/// (the node's own): `sql_execute` always writes `(own, app.<ns>)` — the agent never
/// chooses the writer.
pub struct CraftBackend {
    sql: Arc<CraftSql>,
    obj: Arc<ObjEngine>,
    clock: Arc<Clock>,
    /// This node's PRE keypair (derived from the identity seed, same as the obj/sql
    /// encryption key). Used by [`pre_rekey`](AppBackend::pre_rekey) to delegate the
    /// OWNER's (this identity's) data to a recipient — the secret never leaves here.
    enc: EncKeypair,
}

impl CraftBackend {
    pub fn new(
        sql: Arc<CraftSql>,
        obj: Arc<ObjEngine>,
        clock: Arc<Clock>,
        enc: EncKeypair,
    ) -> Self {
        Self {
            sql,
            obj,
            clock,
            enc,
        }
    }
}

#[async_trait]
impl AppBackend for CraftBackend {
    async fn sql_execute(&self, ns: &str, sql: &str) -> anyhow::Result<u64> {
        let mut db = self.sql.open(&ns_of(ns)).await?;
        db.write(sql).await?;
        Ok(db.conn().changes())
    }

    async fn sql_query(
        &self,
        owner: Option<[u8; 32]>,
        ns: &str,
        sql: &str,
    ) -> anyhow::Result<String> {
        let db = match owner {
            None => self.sql.open(&ns_of(ns)).await?,
            Some(o) => self.sql.open_reader(NodeId(o), &ns_of(ns)).await?,
        };
        Ok(db.query(sql)?.to_string())
    }

    async fn obj_put(&self, data: &[u8]) -> anyhow::Result<[u8; 32]> {
        Ok(self.obj.publish(data, true).await?.cid.0)
    }

    async fn obj_get(&self, cid: [u8; 32]) -> anyhow::Result<Vec<u8>> {
        self.obj.get(Cid(cid), ConsumeMode::Drop).await
    }

    fn now_millis(&self) -> u64 {
        self.clock.now().millis()
    }

    /// Runtime-mediated PRE delegation (K3 sharing): derive nothing new — this backend already
    /// holds the OWNER's (this node identity's) PRE keypair — and produce the blind Umbral
    /// re-encryption fragments delegating to `recipient_pk` (`threshold`-of-`shares`). The secret
    /// key never leaves the backend; the caller (`pre_grant` host fn) receives only the serialized
    /// fragments. Delegating uses THIS identity's own key, so a program can only ever share ITS
    /// OWN data — there is no cross-owner escalation. `recipient_pk` is the recipient's raw
    /// compressed PRE public key (validated here); an invalid key or threshold is a hard error the
    /// host fn maps to UNAVAILABLE.
    async fn pre_rekey(
        &self,
        recipient_pk: Vec<u8>,
        threshold: u32,
        shares: u32,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let recipient = EncPublicKey::from_bytes(&recipient_pk)
            .map_err(|e| anyhow::anyhow!("recipient PRE public key: {e}"))?;
        if threshold < 1 || shares < 1 || threshold > shares {
            anyhow::bail!("invalid PRE threshold {threshold}-of-{shares}");
        }
        let kfrags = grant(&self.enc, &recipient, threshold as usize, shares as usize);
        Ok(Some(postcard::to_allocvec(&kfrags)?))
    }
}
