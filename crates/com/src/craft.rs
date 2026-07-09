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
}

impl CraftBackend {
    pub fn new(sql: Arc<CraftSql>, obj: Arc<ObjEngine>, clock: Arc<Clock>) -> Self {
        Self { sql, obj, clock }
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
}
