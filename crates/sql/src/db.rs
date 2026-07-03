//! The CraftSQL database handle: a single-writer SQLite DB whose head is a
//! signed `KIND_ROOT` record.
//!
//! The SQLite VFS is synchronous; publishing/resolving the head is async — so
//! the split is: the VFS commits locally (yielding a root CID in the in-memory
//! roots map), while this layer RESOLVES the head before opening a connection
//! and PUBLISHES the new head (compare-and-swap) after each write. The in-memory
//! roots map is just a per-process cache; the RootStore is the source of truth.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zeph_core::{Cid, NodeId};

use crate::{CraftVfs, ObjectStore, Result, Roots, SqlError};

/// The DB head: `(owner, namespace) → (root_cid, seq)`. Abstracts `KIND_ROOT`
/// so CraftSQL can be tested without a live tracker.
#[async_trait::async_trait]
pub trait RootStore: Send + Sync {
    /// Current head for `owner`'s DB (None if never published).
    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>>;
    /// Publish MY new head via compare-and-swap: `prev` must be the current root
    /// (None = expect none) and `seq` must strictly advance, else `Conflict`.
    async fn publish(&self, namespace: &str, root: Cid, prev: Option<Cid>, seq: u64) -> Result<()>;
}

/// Fetches a DB's page objects by CID from the network (from the owner / a
/// per-DB provider). A reader syncs a DB by pulling its root then each page.
/// Objects are self-verifying: `cid == BLAKE3(bytes)` is checked on receipt.
#[async_trait::async_trait]
pub trait PageSource: Send + Sync {
    async fn fetch(&self, owner: NodeId, cid: Cid) -> Result<Option<Vec<u8>>>;
}

/// Adapter binding `RootStore` to the real signed `KIND_ROOT` records over
/// `ContentRouting` (tracker now, DHT later). Publishes sign with this node's
/// identity (implicit owner = self).
pub struct RoutingRootStore {
    routing: Arc<dyn zeph_routing::ContentRouting>,
}

impl RoutingRootStore {
    pub fn new(routing: Arc<dyn zeph_routing::ContentRouting>) -> Self {
        Self { routing }
    }
}

#[async_trait::async_trait]
impl RootStore for RoutingRootStore {
    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>> {
        match self.routing.resolve_root(owner, namespace).await {
            Ok(Some(r)) => Ok(Some((r.root_cid, r.seq))),
            Ok(None) => Ok(None),
            Err(e) => Err(SqlError::Sqlite(e.to_string())),
        }
    }

    async fn publish(&self, namespace: &str, root: Cid, prev: Option<Cid>, seq: u64) -> Result<()> {
        match self.routing.publish_root(namespace, root, prev, seq).await {
            Ok(()) => Ok(()),
            Err(zeph_routing::RoutingError::Conflict(_)) => Err(SqlError::Conflict),
            Err(e) => Err(SqlError::Sqlite(e.to_string())),
        }
    }
}

static VFS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A CraftSQL engine bound to a page store, a head store, and this node's
/// writer identity. Registers one SQLite VFS.
pub struct CraftSql {
    vfs_name: String,
    roots: Roots,
    heads: Arc<dyn RootStore>,
    owner: NodeId,
    store_dir: PathBuf,
    source: Option<Arc<dyn PageSource>>,
}

impl CraftSql {
    /// Register a VFS over `store_dir`, backed by `heads`, writing as `owner`.
    pub fn register(
        store_dir: impl Into<PathBuf>,
        heads: Arc<dyn RootStore>,
        owner: NodeId,
    ) -> Result<Self> {
        let n = VFS_COUNTER.fetch_add(1, Ordering::Relaxed);
        let vfs_name = format!("craftsql-{n}");
        let store_dir = store_dir.into();
        let vfs = CraftVfs::new(store_dir.clone());
        let roots = vfs.roots();
        sqlite_vfs::register(&vfs_name, vfs, false)
            .map_err(|e| SqlError::Sqlite(format!("vfs register: {e:?}")))?;
        Ok(Self {
            vfs_name,
            roots,
            heads,
            owner,
            store_dir,
            source: None,
        })
    }

    /// Attach a network page source so readers can sync DB pages they don't hold
    /// locally (from the owner). Without one, opens are local-only.
    pub fn with_source(mut self, source: Arc<dyn PageSource>) -> Self {
        self.source = Some(source);
        self
    }

    /// Pull everything reachable from `root` — the root header, every index-tree
    /// node, and every page object — into the local store (each verified by CID),
    /// so the sync VFS can then read locally. Walks the tree so only the DB's
    /// live pages are fetched.
    async fn sync_pages(&self, owner: NodeId, root: Cid) -> Result<()> {
        let Some(source) = &self.source else {
            return Ok(());
        };
        let store = ObjectStore::open(&self.store_dir)?;
        let src = source.as_ref();
        let root_bytes = ensure(&store, src, owner, root).await?;
        let ri = crate::pager::decode_root(&root_bytes)?;
        if ri.depth == 0 {
            return Ok(());
        }
        // Breadth-first over the index tree: fetch each node, then its children
        // (child nodes above the leaves, page objects at the leaves).
        let mut frontier = vec![(ri.depth - 1, Cid(ri.root_cid))];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for (level, node_cid) in frontier {
                let node_bytes = ensure(&store, src, owner, node_cid).await?;
                for child in crate::pager::decode_node(&node_bytes)?.into_values() {
                    if level == 0 {
                        ensure(&store, src, owner, Cid(child)).await?;
                    } else {
                        next.push((level - 1, Cid(child)));
                    }
                }
            }
            frontier = next;
        }
        Ok(())
    }

    /// Open this node's own DB `namespace` for reading and writing.
    pub async fn open(&self, namespace: &str) -> Result<CraftDb> {
        self.open_as(self.owner, namespace, true).await
    }

    /// Open another identity's DB `namespace` read-only (a reader/replica).
    pub async fn open_reader(&self, owner: NodeId, namespace: &str) -> Result<CraftDb> {
        self.open_as(owner, namespace, false).await
    }

    async fn open_as(&self, owner: NodeId, namespace: &str, writable: bool) -> Result<CraftDb> {
        let key = format!("{}_{}", &owner.to_hex()[..16], namespace);
        // Resolve the authoritative head and seed the VFS cache for this key.
        let seq = match self.heads.resolve(owner, namespace).await? {
            Some((root, seq)) => {
                // Pull the pages we don't hold from the network before opening.
                self.sync_pages(owner, root).await?;
                self.roots
                    .lock()
                    .expect("roots")
                    .insert(key.clone(), root.0);
                seq
            }
            None => {
                self.roots.lock().expect("roots").remove(&key);
                0
            }
        };
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            key.as_str(),
            rusqlite::OpenFlags::default(),
            &self.vfs_name,
        )
        .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        conn.execute_batch("PRAGMA page_size=16384; PRAGMA synchronous=FULL;")
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        Ok(CraftDb {
            conn,
            roots: self.roots.clone(),
            heads: writable.then(|| self.heads.clone()),
            key,
            namespace: namespace.to_string(),
            seq,
        })
    }
}

/// An open CraftSQL database.
pub struct CraftDb {
    conn: rusqlite::Connection,
    roots: Roots,
    heads: Option<Arc<dyn RootStore>>,
    key: String,
    namespace: String,
    seq: u64,
}

impl CraftDb {
    /// The underlying connection — for reads (`query_row`, `prepare`, …).
    pub fn conn(&self) -> &rusqlite::Connection {
        &self.conn
    }

    /// Current root CID (the head this handle last committed/opened at).
    pub fn root(&self) -> Option<Cid> {
        self.roots
            .lock()
            .expect("roots")
            .get(&self.key)
            .copied()
            .map(Cid)
    }

    /// Run write SQL, then publish the new root as the signed head via CAS.
    /// Returns `Conflict` if another writer moved the head first (retry).
    pub async fn write(&mut self, sql: &str) -> Result<()> {
        let heads = self.heads.clone().ok_or(SqlError::ReadOnly)?;
        let prev = self.roots.lock().expect("roots").get(&self.key).copied();
        self.conn
            .execute_batch(sql)
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        let new = self.roots.lock().expect("roots").get(&self.key).copied();
        if let Some(root) = new {
            if Some(root) != prev {
                heads
                    .publish(&self.namespace, Cid(root), prev.map(Cid), self.seq + 1)
                    .await?;
                self.seq += 1;
            }
        }
        Ok(())
    }

    /// Run a read query, returning `{ "columns": [...], "rows": [[...], ...] }`.
    pub fn query(&self, sql: &str) -> Result<serde_json::Value> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        let cols: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
        let ncol = cols.len();
        let rows = stmt
            .query_map([], |row| {
                let mut out = Vec::with_capacity(ncol);
                for i in 0..ncol {
                    let v: rusqlite::types::Value = row.get(i)?;
                    out.push(cell_to_json(v));
                }
                Ok(serde_json::Value::Array(out))
            })
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        let rows: Vec<serde_json::Value> = rows.filter_map(std::result::Result::ok).collect();
        Ok(serde_json::json!({ "columns": cols, "rows": rows }))
    }
}

/// Ensure `cid` is present in `store`, fetching + verifying it from the network
/// source if missing. Returns its bytes.
async fn ensure(
    store: &ObjectStore,
    source: &dyn PageSource,
    owner: NodeId,
    cid: Cid,
) -> Result<Vec<u8>> {
    if let Some(b) = store.get(&cid) {
        return Ok(b);
    }
    let bytes = source
        .fetch(owner, cid)
        .await?
        .ok_or_else(|| SqlError::CorruptIndex(format!("missing object {}", cid.to_hex())))?;
    if store.put(&bytes)? != cid {
        return Err(SqlError::CorruptIndex(format!(
            "object hash mismatch {}",
            cid.to_hex()
        )));
    }
    Ok(bytes)
}

fn cell_to_json(v: rusqlite::types::Value) -> serde_json::Value {
    use rusqlite::types::Value;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Integer(i) => serde_json::json!(i),
        Value::Real(f) => serde_json::json!(f),
        Value::Text(s) => serde_json::json!(s),
        Value::Blob(b) => serde_json::json!(hex::encode(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::tempdir;

    type MockMap = Mutex<HashMap<(NodeId, String), ([u8; 32], u64)>>;

    /// In-memory head store with the same CAS semantics as KIND_ROOT.
    struct MockHeads {
        owner: NodeId,
        map: MockMap,
    }
    impl MockHeads {
        fn new(owner: NodeId) -> Arc<Self> {
            Arc::new(Self {
                owner,
                map: Mutex::new(HashMap::new()),
            })
        }
    }
    #[async_trait::async_trait]
    impl RootStore for MockHeads {
        async fn resolve(&self, owner: NodeId, ns: &str) -> Result<Option<(Cid, u64)>> {
            Ok(self
                .map
                .lock()
                .unwrap()
                .get(&(owner, ns.to_string()))
                .map(|(r, s)| (Cid(*r), *s)))
        }
        async fn publish(&self, ns: &str, root: Cid, prev: Option<Cid>, seq: u64) -> Result<()> {
            let mut m = self.map.lock().unwrap();
            let key = (self.owner, ns.to_string());
            match m.get(&key) {
                None if prev.is_some() => return Err(SqlError::Conflict),
                None => {}
                Some((cur, cur_seq)) => {
                    if prev.map(|c| c.0) != Some(*cur) || seq <= *cur_seq {
                        return Err(SqlError::Conflict);
                    }
                }
            }
            m.insert(key, (root.0, seq));
            Ok(())
        }
    }

    #[tokio::test]
    async fn head_persists_via_rootstore_and_reopens_elsewhere() {
        let dir = tempdir().unwrap();
        let owner = NodeId([7u8; 32]);
        let heads = MockHeads::new(owner);

        // Writer engine.
        let w = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let mut db = w.open("app").await.unwrap();
        db.write(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'hello');",
        )
        .await
        .unwrap();
        let root_after = db.root().expect("committed a root");

        // The head is now in the store (seq 1).
        let (head, seq) = heads.resolve(owner, "app").await.unwrap().unwrap();
        assert_eq!(head, root_after);
        assert_eq!(seq, 1);

        // A SEPARATE engine (fresh in-memory cache, same page dir + head store)
        // resolves the head from the store and reads the data.
        let other = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let db2 = other.open_reader(owner, "app").await.unwrap();
        let v: String = db2
            .conn()
            .query_row("SELECT v FROM t WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "hello", "reopened from the KIND_ROOT head via RootStore");
    }

    /// A network page source that serves objects from the owner's local store —
    /// stands in for fetching pages from the owner over the wire.
    struct MockSource {
        owner_store: crate::ObjectStore,
    }
    #[async_trait::async_trait]
    impl PageSource for MockSource {
        async fn fetch(&self, _owner: NodeId, cid: Cid) -> Result<Option<Vec<u8>>> {
            Ok(self.owner_store.get(&cid))
        }
    }

    #[tokio::test]
    async fn reader_syncs_pages_from_owner_over_network() {
        let owner_dir = tempdir().unwrap();
        let reader_dir = tempdir().unwrap();
        let owner_id = NodeId([3u8; 32]);
        let heads = MockHeads::new(owner_id);

        // Owner writes a DB (pages land in owner_dir).
        let w = CraftSql::register(owner_dir.path(), heads.clone(), owner_id).unwrap();
        let mut db = w.open("mail").await.unwrap();
        db.write("CREATE TABLE msg(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO msg VALUES (1,'hi'),(2,'yo');")
            .await
            .unwrap();

        // Reader has an EMPTY store + a page source pointing at the owner's store.
        let source = Arc::new(MockSource {
            owner_store: crate::ObjectStore::open(owner_dir.path()).unwrap(),
        });
        let r = CraftSql::register(reader_dir.path(), heads.clone(), owner_id)
            .unwrap()
            .with_source(source);

        // open_reader resolves the head, SYNCS the pages, then reads locally.
        let db2 = r.open_reader(owner_id, "mail").await.unwrap();
        let body: String = db2
            .conn()
            .query_row("SELECT body FROM msg WHERE id=2", [], |x| x.get(0))
            .unwrap();
        assert_eq!(
            body, "yo",
            "reader pulled pages from the owner and read them"
        );

        // The pages really were absent then fetched: the reader store now holds them.
        let reader_store = crate::ObjectStore::open(reader_dir.path()).unwrap();
        let head = db2.root().unwrap();
        assert!(
            reader_store.has(&head),
            "root object synced into the reader's store"
        );
    }

    #[tokio::test]
    async fn stale_write_conflicts() {
        let dir = tempdir().unwrap();
        let owner = NodeId([9u8; 32]);
        let heads = MockHeads::new(owner);

        // Two engines, both open the (empty) DB → both think seq 0, prev None.
        let e1 = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let e2 = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let mut w1 = e1.open("db").await.unwrap();
        let mut w2 = e2.open("db").await.unwrap();

        // w1 commits first → head at seq 1.
        w1.write("CREATE TABLE a(x); INSERT INTO a VALUES (1);")
            .await
            .unwrap();
        // w2's write publishes prev=None seq 1 → CAS fails (head already moved).
        let conflict = w2
            .write("CREATE TABLE b(y); INSERT INTO b VALUES (2);")
            .await;
        assert!(
            matches!(conflict, Err(SqlError::Conflict)),
            "stale writer is rejected"
        );
    }
}
