//! The CraftSQL database handle: a single-writer SQLite DB whose head is a
//! signed `KIND_ROOT` record.
//!
//! The SQLite VFS is synchronous; publishing/resolving the head is async — so
//! the split is: the VFS commits locally (yielding a root CID in the in-memory
//! roots map), while this layer RESOLVES the head before opening a connection
//! and PUBLISHES the new head (compare-and-swap) after each write. The in-memory
//! roots map is just a per-process cache; the RootStore is the source of truth.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zeph_core::{Cid, NodeId};

use crate::gen::{self, DurableStore};
use crate::{CraftVfs, ObjectStore, Result, Roots, SqlError};

/// Per-DB durability manifest: the generation CIDs published so far, and the
/// root they were last swept at (to diff for the next generation). Persisted as
/// a sidecar file so a generation is published only for genuinely new objects.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct GenManifest {
    /// The DB namespace (so a re-announce knows what to publish).
    namespace: String,
    last_root: Option<[u8; 32]>,
    /// Head seq at `last_root` (for re-announce + local-head fallback).
    seq: u64,
    generations: Vec<[u8; 32]>,
    /// CID of the published generation-list object (`KIND_MANIFEST` target).
    manifest_cid: Option<[u8; 32]>,
    /// Monotonic publish counter for the manifest (independent of generation
    /// count, which resets on compaction) — the `KIND_MANIFEST` seq.
    manifest_seq: u64,
    /// Superseded generations still being released — holders remain that haven't
    /// dropped the system marker (e.g. offline during the initial release). The
    /// re-announce loop re-sends the release until this drains (churn safety).
    releasing: Vec<[u8; 32]>,
}

/// Compact after this many generations accumulate, to bound storage growth.
const COMPACT_THRESHOLD: usize = 16;

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

/// Publishes/resolves a DB's durability-manifest pointer (`KIND_MANIFEST`) — the
/// CID of the object listing its generations. Lets any node discover, by
/// `(owner, namespace)` alone, how to reconstruct a DB from its pieces.
#[async_trait::async_trait]
pub trait ManifestStore: Send + Sync {
    async fn publish(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()>;
    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>>;
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

/// Adapter binding `ManifestStore` to `KIND_MANIFEST` over `ContentRouting`.
pub struct RoutingManifestStore {
    routing: Arc<dyn zeph_routing::ContentRouting>,
}

impl RoutingManifestStore {
    pub fn new(routing: Arc<dyn zeph_routing::ContentRouting>) -> Self {
        Self { routing }
    }
}

#[async_trait::async_trait]
impl ManifestStore for RoutingManifestStore {
    async fn publish(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()> {
        self.routing
            .publish_manifest(namespace, manifest_cid, seq)
            .await
            .map_err(|e| SqlError::Sqlite(e.to_string()))
    }

    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>> {
        Ok(self
            .routing
            .resolve_manifest(owner, namespace)
            .await
            .map_err(|e| SqlError::Sqlite(e.to_string()))?
            .map(|r| (r.manifest_cid, r.seq)))
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
    durable: Option<Arc<dyn DurableStore>>,
    manifests: Option<Arc<dyn ManifestStore>>,
    fetchers: crate::vfs::Fetchers,
    /// This node's encryption keypair — unwraps private-DB keys / wraps new ones.
    enc: Option<Arc<zeph_cipher::EncKeypair>>,
    /// Per-db page ciphers (private dbs), shared with the VFS.
    ciphers: crate::vfs::Ciphers,
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
        let fetchers = vfs.fetchers();
        let ciphers = vfs.ciphers();
        sqlite_vfs::register(&vfs_name, vfs, false)
            .map_err(|e| SqlError::Sqlite(format!("vfs register: {e:?}")))?;
        Ok(Self {
            vfs_name,
            roots,
            heads,
            owner,
            store_dir,
            source: None,
            durable: None,
            manifests: None,
            fetchers,
            enc: None,
            ciphers,
        })
    }

    /// Attach this node's encryption keypair — enables PRIVATE databases (encrypted
    /// pages): `open_private` for owners, auto-decrypt for readers with the key.
    pub fn with_enc_keypair(mut self, kp: Arc<zeph_cipher::EncKeypair>) -> Self {
        self.enc = Some(kp);
        self
    }

    /// Attach a network page source so readers can sync DB pages they don't hold
    /// locally (from the owner). Without one, opens are local-only.
    pub fn with_source(mut self, source: Arc<dyn PageSource>) -> Self {
        self.source = Some(source);
        self
    }

    /// Attach a durable store so each commit's new objects are erasure-coded
    /// (k=8/n=32) + distributed + repaired — the DB survives owner/holder loss.
    pub fn with_durable(mut self, durable: Arc<dyn DurableStore>) -> Self {
        self.durable = Some(durable);
        self
    }

    /// Attach a manifest store so each DB's generation list is published network-
    /// wide (`KIND_MANIFEST`) — any node can then rebuild the DB from `(owner,
    /// namespace)` alone, even after the owner is gone.
    pub fn with_manifests(mut self, manifests: Arc<dyn ManifestStore>) -> Self {
        self.manifests = Some(manifests);
        self
    }

    /// Pull everything reachable from `root` — the root header, every index-tree
    /// node, and every page object — into the local store (each verified by CID),
    /// so the sync VFS can then read locally. Walks the tree so only the DB's
    /// live pages are fetched.
    async fn sync_reachable(&self, owner: NodeId, root: Cid, fetch_pages: bool) -> Result<()> {
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
                        // Leaf children are page objects — fetch them only in
                        // eager mode; a lazy reader pulls them per-read.
                        if fetch_pages {
                            ensure(&store, src, owner, Cid(child)).await?;
                        }
                    } else {
                        next.push((level - 1, Cid(child)));
                    }
                }
            }
            frontier = next;
        }
        Ok(())
    }

    /// Spawn a background task that services lazy page fetches from the network
    /// source, returning a sync handle the VFS blocks on.
    fn spawn_fetcher(&self, owner: NodeId) -> crate::fetch::Fetcher {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<crate::fetch::FetchRequest>();
        let source = self.source.clone().expect("lazy reader needs a source");
        tokio::spawn(async move {
            while let Some((cid, resp)) = rx.recv().await {
                let bytes = source.fetch(owner, cid).await.ok().flatten();
                let _ = resp.send(bytes);
            }
        });
        crate::fetch::Fetcher::new(tx)
    }

    /// Re-announce every locally-owned DB head + durability manifest to the
    /// tracker. Heads/manifests are otherwise published only on write, so a
    /// tracker restart would lose them (leaving even the owner unable to open its
    /// own DB). Reads the persisted sidecars; restore-tolerant, so it is a no-op
    /// when the records are already current. Returns the count re-announced.
    pub async fn reannounce_heads(&self) -> usize {
        let mut count = 0;
        let Ok(entries) = std::fs::read_dir(&self.store_dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("gens") {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let mut m: GenManifest = match postcard::from_bytes(&bytes) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let (Some(root), false) = (m.last_root, m.namespace.is_empty()) else {
                continue;
            };
            // prev=None → restore semantics (accepted when the tracker lost it).
            let _ = self
                .heads
                .publish(&m.namespace, Cid(root), None, m.seq)
                .await;
            if let (Some(mstore), Some(mc)) = (&self.manifests, m.manifest_cid) {
                let _ = mstore.publish(&m.namespace, Cid(mc), m.manifest_seq).await;
            }
            // Re-release superseded generations that still have holders — this is
            // what reaches a node that churned (was offline) during the initial
            // release. Drops from the list once no providers remain.
            if !m.releasing.is_empty() {
                if let Some(durable) = &self.durable {
                    let pending = std::mem::take(&mut m.releasing);
                    for g in pending {
                        if durable.drop_generation(Cid(g)).await.unwrap_or(0) > 0 {
                            m.releasing.push(g);
                        }
                    }
                    if let Ok(blob) = postcard::to_allocvec(&m) {
                        let _ = std::fs::write(&path, blob);
                    }
                }
            }
            count += 1;
        }
        count
    }

    /// Open this node's own DB `namespace` for reading and writing.
    pub async fn open(&self, namespace: &str) -> Result<CraftDb> {
        self.open_as(self.owner, namespace, true, false).await
    }

    /// Open (or create) one of your own databases as PRIVATE — pages encrypted
    /// under your key. Existing dbs keep their public/private nature; the flag
    /// only decides a *new* db.
    pub async fn open_private(&self, namespace: &str) -> Result<CraftDb> {
        self.open_as(self.owner, namespace, true, true).await
    }

    /// Open another identity's DB `namespace` read-only (a reader/replica).
    /// Auto-detects a private db (decrypts if we hold the key).
    pub async fn open_reader(&self, owner: NodeId, namespace: &str) -> Result<CraftDb> {
        self.open_as(owner, namespace, false, false).await
    }

    /// For an EXISTING db: if its root carries a wrapped DEK (private) and we can
    /// unwrap it with our key, register the page cipher. A foreign private db we
    /// can't unwrap registers nothing → its pages read as ciphertext (open fails).
    fn register_cipher_existing(&self, key: &str, root: Cid) -> Result<()> {
        let Some(kp) = &self.enc else { return Ok(()) };
        let store = ObjectStore::open(&self.store_dir)?;
        let Some(bytes) = store.get(&root) else {
            return Ok(());
        };
        let ri = crate::pager::decode_root(&bytes)?;
        if ri.wrapped_dek.is_empty() {
            return Ok(()); // public db
        }
        let capsule: zeph_cipher::DekCapsule =
            postcard::from_bytes(&ri.wrapped_dek).map_err(|e| SqlError::Serde(e.to_string()))?;
        if let Ok(dek) = zeph_cipher::open_capsule(kp, &capsule) {
            self.ciphers
                .lock()
                .expect("ciphers")
                .insert(key.to_string(), (dek, ri.wrapped_dek));
        }
        Ok(())
    }

    /// For a NEW private db: generate a DEK, wrap it under our key, register both
    /// (the wrapped DEK is written into the root on the first commit).
    fn register_cipher_new(&self, key: &str) -> Result<()> {
        let Some(kp) = &self.enc else { return Ok(()) };
        let dek = zeph_cipher::Dek::generate();
        let capsule = zeph_cipher::encapsulate(&kp.public(), &dek);
        let wrapped =
            postcard::to_allocvec(&capsule).map_err(|e| SqlError::Serde(e.to_string()))?;
        self.ciphers
            .lock()
            .expect("ciphers")
            .insert(key.to_string(), (dek, wrapped));
        Ok(())
    }

    async fn open_as(
        &self,
        owner: NodeId,
        namespace: &str,
        writable: bool,
        private_hint: bool,
    ) -> Result<CraftDb> {
        let key = format!("{}_{}", &owner.to_hex()[..16], namespace);
        // Resolve the authoritative head. If the tracker lost it (restart) and
        // this is our own DB, recover it from the local sidecar so the owner is
        // never locked out of its own database by tracker liveness.
        let head = match self.heads.resolve(owner, namespace).await? {
            Some(rs) => Some(rs),
            None if writable => {
                let m = load_manifest(&self.store_dir, &key);
                m.last_root.map(|r| (Cid(r), m.seq))
            }
            None => None,
        };
        let seq = match head {
            Some((root, seq)) => {
                if !writable && self.source.is_some() {
                    // Lazy reader: sync only the (tiny) index; pull page contents
                    // on demand as the query touches them.
                    self.sync_reachable(owner, root, false).await?;
                    let fetcher = self.spawn_fetcher(owner);
                    self.fetchers
                        .lock()
                        .expect("fetchers")
                        .insert(key.clone(), fetcher);
                } else {
                    // Writer (all local) or source-less: ensure everything present.
                    self.sync_reachable(owner, root, true).await?;
                }
                self.roots
                    .lock()
                    .expect("roots")
                    .insert(key.clone(), root.0);
                self.register_cipher_existing(&key, root)?;
                seq
            }
            None => {
                self.roots.lock().expect("roots").remove(&key);
                if private_hint {
                    self.register_cipher_new(&key)?;
                }
                0
            }
        };
        // Opening (and its PRAGMA read) can trigger lazy page fetches that block
        // on the sync→async bridge, so run the blocking SQLite work on a blocking
        // thread — never a runtime worker (else the fetcher task can't progress).
        let vfs_name = self.vfs_name.clone();
        let key_c = key.clone();
        let conn = tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open_with_flags_and_vfs(
                key_c.as_str(),
                rusqlite::OpenFlags::default(),
                &vfs_name,
            )?;
            conn.execute_batch("PRAGMA page_size=16384; PRAGMA synchronous=FULL;")?;
            Ok::<_, rusqlite::Error>(conn)
        })
        .await
        .map_err(|e| SqlError::Sqlite(format!("open task: {e}")))?
        .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        Ok(CraftDb {
            conn,
            roots: self.roots.clone(),
            heads: writable.then(|| self.heads.clone()),
            durable: if writable { self.durable.clone() } else { None },
            manifests: if writable {
                self.manifests.clone()
            } else {
                None
            },
            store_dir: self.store_dir.clone(),
            key,
            namespace: namespace.to_string(),
            seq,
        })
    }

    /// Rebuild a DB's local object store from its durable generations — for
    /// recovery after page-store loss/corruption. Decodes each generation
    /// (verifying every object by CID) and re-puts it, then the DB opens
    /// normally at its head. Reads the sidecar manifest for the generation set.
    pub async fn recover(&self, namespace: &str) -> Result<usize> {
        self.recover_owner(self.owner, namespace).await
    }

    /// Rebuild `owner`'s DB `namespace` from its durable generations — discovering
    /// the generation list from the network manifest (`KIND_MANIFEST`) when a
    /// manifest store is set, else the local sidecar. Works for ANY owner from
    /// (owner, namespace) alone, so a live node can resurrect a dead owner's DB.
    pub async fn recover_owner(&self, owner: NodeId, namespace: &str) -> Result<usize> {
        let durable = self
            .durable
            .as_ref()
            .ok_or_else(|| SqlError::Sqlite("no durable store configured".into()))?;
        let store = ObjectStore::open(&self.store_dir)?;
        let key = format!("{}_{}", &owner.to_hex()[..16], namespace);
        let gens: Vec<[u8; 32]> = match &self.manifests {
            Some(mstore) => match mstore.resolve(owner, namespace).await? {
                Some((manifest_cid, _)) => {
                    let blob = durable.get_generation(manifest_cid).await?.ok_or_else(|| {
                        SqlError::CorruptIndex("manifest object unrecoverable".into())
                    })?;
                    postcard::from_bytes(&blob)
                        .map_err(|e| SqlError::CorruptIndex(e.to_string()))?
                }
                None => load_manifest(&self.store_dir, &key).generations,
            },
            None => load_manifest(&self.store_dir, &key).generations,
        };
        let mut restored = 0;
        for gcid in gens {
            let blob = durable.get_generation(Cid(gcid)).await?.ok_or_else(|| {
                SqlError::CorruptIndex(format!("lost generation {}", Cid(gcid).to_hex()))
            })?;
            for (cid, data) in gen::unpack(&blob)? {
                if !store.has(&cid) {
                    if store.put(&data)? != cid {
                        return Err(SqlError::CorruptIndex(
                            "recovered object hash mismatch".into(),
                        ));
                    }
                    restored += 1;
                }
            }
        }
        Ok(restored)
    }

    /// Compact this owned DB now: re-snapshot the live object set into a single
    /// base generation, drop the accumulated old generations, and GC superseded
    /// page objects locally. Returns the count of local objects reclaimed.
    pub async fn compact(&self, namespace: &str) -> Result<usize> {
        let durable = self
            .durable
            .as_ref()
            .ok_or_else(|| SqlError::Sqlite("no durable store".into()))?;
        let key = format!("{}_{}", &self.owner.to_hex()[..16], namespace);
        let root = match self.heads.resolve(self.owner, namespace).await? {
            Some((r, _)) => r,
            None => load_manifest(&self.store_dir, &key)
                .last_root
                .map(Cid)
                .ok_or_else(|| SqlError::Sqlite("no head to compact".into()))?,
        };
        run_compaction(
            &self.store_dir,
            &key,
            namespace,
            root,
            durable,
            &self.manifests,
        )
        .await
    }
}

/// An open CraftSQL database.
pub struct CraftDb {
    conn: rusqlite::Connection,
    roots: Roots,
    heads: Option<Arc<dyn RootStore>>,
    durable: Option<Arc<dyn DurableStore>>,
    manifests: Option<Arc<dyn ManifestStore>>,
    store_dir: PathBuf,
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
                self.sweep_durability(Cid(root)).await?;
            }
        }
        Ok(())
    }

    /// Publish this commit's NEW objects (changed pages + the rewritten index
    /// path) as one durable generation. Diffs the objects reachable from the new
    /// root against the last swept root, so only genuinely new objects are coded
    /// — the generation stays O(changed), like the commit that produced it.
    async fn sweep_durability(&mut self, root: Cid) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        let store = ObjectStore::open(&self.store_dir)?;
        let mut manifest = load_manifest(&self.store_dir, &self.key);
        if manifest.last_root == Some(root.0) {
            return Ok(());
        }
        let prev: std::collections::HashSet<Cid> = match manifest.last_root {
            Some(r) => crate::pager::reachable(&store, Cid(r))?
                .into_iter()
                .collect(),
            None => std::collections::HashSet::new(),
        };
        let mut fresh = Vec::new();
        for cid in crate::pager::reachable(&store, root)? {
            if !prev.contains(&cid) {
                if let Some(bytes) = store.get(&cid) {
                    fresh.push((cid, bytes));
                }
            }
        }
        let mut changed = false;
        if !fresh.is_empty() {
            let blob = gen::pack(&fresh)?;
            let gcid = durable.put_generation(blob).await?;
            manifest.generations.push(gcid.0);
            changed = true;
        }
        manifest.namespace = self.namespace.clone();
        manifest.last_root = Some(root.0);
        manifest.seq = self.seq;
        // Publish the generation list as its own durable object and announce its
        // CID network-wide, so any node can recover the DB from (owner, ns) alone.
        if changed {
            if let Some(mstore) = &self.manifests {
                let list = postcard::to_allocvec(&manifest.generations)
                    .map_err(|e| SqlError::Serde(e.to_string()))?;
                let manifest_cid = durable.put_generation(list).await?;
                manifest.manifest_cid = Some(manifest_cid.0);
                manifest.manifest_seq += 1;
                mstore
                    .publish(&self.namespace, manifest_cid, manifest.manifest_seq)
                    .await?;
            }
        }
        save_manifest(&self.store_dir, &self.key, &manifest)?;
        // Bound storage: once generations pile up, fold them into one base
        // snapshot and reclaim superseded objects.
        if manifest.generations.len() >= COMPACT_THRESHOLD {
            run_compaction(
                &self.store_dir,
                &self.key,
                &self.namespace,
                root,
                durable,
                &self.manifests,
            )
            .await?;
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

fn manifest_path(store_dir: &Path, key: &str) -> PathBuf {
    store_dir.join(format!("{key}.gens"))
}

fn load_manifest(store_dir: &Path, key: &str) -> GenManifest {
    std::fs::read(manifest_path(store_dir, key))
        .ok()
        .and_then(|b| postcard::from_bytes(&b).ok())
        .unwrap_or_default()
}

fn save_manifest(store_dir: &Path, key: &str, m: &GenManifest) -> Result<()> {
    let blob = postcard::to_allocvec(m).map_err(|e| SqlError::Serde(e.to_string()))?;
    std::fs::write(manifest_path(store_dir, key), blob)?;
    Ok(())
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

/// Fold a DB's accumulated generations into one base generation covering only
/// the objects reachable from `root`, drop the old generations (unpin → fade),
/// and GC superseded page objects from the local store. Bounds storage to ~the
/// live DB size instead of growing with write history.
async fn run_compaction(
    store_dir: &Path,
    key: &str,
    namespace: &str,
    root: Cid,
    durable: &Arc<dyn DurableStore>,
    manifests: &Option<Arc<dyn ManifestStore>>,
) -> Result<usize> {
    let store = ObjectStore::open(store_dir)?;
    let mut manifest = load_manifest(store_dir, key);
    if manifest.generations.len() <= 1 {
        return Ok(0);
    }
    let live: std::collections::HashSet<Cid> =
        crate::pager::reachable(&store, root)?.into_iter().collect();
    // Pack the live object set into one fresh base generation.
    let live_objs: Vec<(Cid, Vec<u8>)> = live
        .iter()
        .filter_map(|c| store.get(c).map(|d| (*c, d)))
        .collect();
    let base = durable.put_generation(gen::pack(&live_objs)?).await?;
    let old = std::mem::replace(&mut manifest.generations, vec![base.0]);
    // Republish the now single-entry manifest (monotonic seq).
    if let Some(mstore) = manifests {
        let list = postcard::to_allocvec(&manifest.generations)
            .map_err(|e| SqlError::Serde(e.to_string()))?;
        let mc = durable.put_generation(list).await?;
        manifest.manifest_cid = Some(mc.0);
        manifest.manifest_seq += 1;
        let _ = mstore.publish(namespace, mc, manifest.manifest_seq).await;
    }
    // Drop superseded generations (base is durable first, so this is safe).
    // A generation with holders still providing it stays in `releasing` so the
    // re-announce loop keeps re-sending the release — this is what reaches a
    // holder that was offline (churned) during the initial release.
    for g in old {
        if g != base.0 {
            let remaining = durable.drop_generation(Cid(g)).await.unwrap_or(0);
            if remaining > 0 {
                manifest.releasing.push(g);
            }
        }
    }
    save_manifest(store_dir, key, &manifest)?;
    // GC page objects no longer reachable from the current root.
    let mut reclaimed = 0;
    if let Ok(all) = store.list() {
        for cid in all {
            if !live.contains(&cid) {
                let _ = store.delete(&cid);
                reclaimed += 1;
            }
        }
    }
    Ok(reclaimed)
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
    async fn private_db_encrypts_pages_and_only_the_owner_reads() {
        let dir = tempdir().unwrap();
        let owner = NodeId([9u8; 32]);
        let heads = MockHeads::new(owner);
        let kp_a = Arc::new(zeph_cipher::EncKeypair::from_identity_seed(&[1u8; 32]));

        // Owner writes a PRIVATE db.
        {
            let w = CraftSql::register(dir.path(), heads.clone(), owner)
                .unwrap()
                .with_enc_keypair(kp_a.clone());
            let mut db = w.open_private("vault").await.unwrap();
            db.write("CREATE TABLE s(id INTEGER PRIMARY KEY, secret TEXT); INSERT INTO s VALUES (1,'TOPSECRET');")
                .await
                .unwrap();
        }

        // Owner reopens (fresh engine, same key) → auto-detects private, reads back.
        {
            let w2 = CraftSql::register(dir.path(), heads.clone(), owner)
                .unwrap()
                .with_enc_keypair(kp_a.clone());
            let db = w2.open("vault").await.unwrap();
            let rows = db.query("SELECT secret FROM s WHERE id=1").unwrap();
            assert!(
                rows.to_string().contains("TOPSECRET"),
                "owner reads the private db"
            );
        }

        // A DIFFERENT identity cannot read it (its key can't unwrap the DB key).
        {
            let kp_b = Arc::new(zeph_cipher::EncKeypair::from_identity_seed(&[2u8; 32]));
            let f = CraftSql::register(dir.path(), heads.clone(), owner)
                .unwrap()
                .with_enc_keypair(kp_b);
            let readable = matches!(f.open_reader(owner, "vault").await, Ok(db)
                if db.query("SELECT secret FROM s").map(|v| v.to_string().contains("TOPSECRET")).unwrap_or(false));
            assert!(
                !readable,
                "a different identity must not read the private db"
            );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    /// In-memory durable store: a generation goes in, comes back by CID —
    /// stands in for erasure-coded + distributed + repaired storage.
    #[derive(Default)]
    struct MockDurable {
        gens: Mutex<HashMap<[u8; 32], Vec<u8>>>,
    }
    #[async_trait::async_trait]
    impl DurableStore for MockDurable {
        async fn put_generation(&self, blob: Vec<u8>) -> Result<Cid> {
            let cid = Cid::of(&blob);
            self.gens.lock().unwrap().insert(cid.0, blob);
            Ok(cid)
        }
        async fn get_generation(&self, cid: Cid) -> Result<Option<Vec<u8>>> {
            Ok(self.gens.lock().unwrap().get(&cid.0).cloned())
        }
        async fn drop_generation(&self, cid: Cid) -> Result<usize> {
            self.gens.lock().unwrap().remove(&cid.0);
            Ok(0)
        }
    }

    /// Delete every object shard (keep the manifest sidecar) — simulates total
    /// local page-store loss/corruption on the owner.
    fn wipe_objects(dir: &std::path::Path) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                std::fs::remove_dir_all(&p).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn generations_recover_db_after_object_store_loss() {
        let dir = tempdir().unwrap();
        let owner = NodeId([5u8; 32]);
        let heads = MockHeads::new(owner);
        let durable = Arc::new(MockDurable::default());
        let sql = CraftSql::register(dir.path(), heads.clone(), owner)
            .unwrap()
            .with_durable(durable.clone());

        let mut db = sql.open("app").await.unwrap();
        db.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'durable'),(2,'via erasure');")
            .await
            .unwrap();
        drop(db);
        assert!(
            !durable.gens.lock().unwrap().is_empty(),
            "a generation was published"
        );

        // Catastrophe: the owner's entire local object store is lost.
        wipe_objects(dir.path());
        assert!(
            sql.open_reader(owner, "app").await.is_err(),
            "DB is unreadable with objects gone"
        );

        // Reconstruct from the durable generations, then reopen + query.
        let restored = sql.recover("app").await.unwrap();
        assert!(restored > 0, "objects restored from generations");
        let db2 = sql.open_reader(owner, "app").await.unwrap();
        let v: String = db2
            .conn()
            .query_row("SELECT v FROM t WHERE id=2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            v, "via erasure",
            "DB reconstructed from erasure generations"
        );
    }

    /// In-memory manifest store (owner implicit, like KIND_MANIFEST's signer).
    struct MockManifest {
        owner: NodeId,
        map: MockMap,
    }
    impl MockManifest {
        fn new(owner: NodeId) -> Arc<Self> {
            Arc::new(Self {
                owner,
                map: Mutex::new(HashMap::new()),
            })
        }
    }
    #[async_trait::async_trait]
    impl ManifestStore for MockManifest {
        async fn publish(&self, ns: &str, cid: Cid, seq: u64) -> Result<()> {
            self.map
                .lock()
                .unwrap()
                .insert((self.owner, ns.to_string()), (cid.0, seq));
            Ok(())
        }
        async fn resolve(&self, owner: NodeId, ns: &str) -> Result<Option<(Cid, u64)>> {
            Ok(self
                .map
                .lock()
                .unwrap()
                .get(&(owner, ns.to_string()))
                .map(|(c, s)| (Cid(*c), *s)))
        }
    }

    #[tokio::test]
    async fn network_manifest_recovers_db_on_another_node() {
        let dir1 = tempdir().unwrap(); // owner node
        let dir2 = tempdir().unwrap(); // a different, live node
        let owner = NodeId([6u8; 32]);
        let heads = MockHeads::new(owner);
        let durable = Arc::new(MockDurable::default()); // the shared "network"
        let manifests = MockManifest::new(owner);

        // Owner writes on node 1 (generation + manifest published network-wide).
        let sql1 = CraftSql::register(dir1.path(), heads.clone(), owner)
            .unwrap()
            .with_durable(durable.clone())
            .with_manifests(manifests.clone());
        let mut db = sql1.open("app").await.unwrap();
        db.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'resurrected');")
            .await
            .unwrap();
        drop(db);

        // Node 2 has an EMPTY store and NO local sidecar — it only knows
        // (owner, namespace). It resolves the manifest, reconstructs, and reads.
        let sql2 = CraftSql::register(dir2.path(), heads.clone(), owner)
            .unwrap()
            .with_durable(durable.clone())
            .with_manifests(manifests.clone());
        assert!(sql2.open_reader(owner, "app").await.is_err(), "no data yet");
        let restored = sql2.recover_owner(owner, "app").await.unwrap();
        assert!(restored > 0, "reconstructed objects from the network");
        let db2 = sql2.open_reader(owner, "app").await.unwrap();
        let v: String = db2
            .conn()
            .query_row("SELECT v FROM t WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            v, "resurrected",
            "a different node rebuilt the DB from the network manifest"
        );
    }

    /// A page source that counts fetches and serves from the owner's store.
    struct CountingSource {
        owner_store: crate::ObjectStore,
        count: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl PageSource for CountingSource {
        async fn fetch(&self, _owner: NodeId, cid: Cid) -> Result<Option<Vec<u8>>> {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self.owner_store.get(&cid))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lazy_reader_fetches_only_touched_pages() {
        use std::sync::atomic::Ordering::Relaxed;
        let owner_dir = tempdir().unwrap();
        let reader_dir = tempdir().unwrap();
        let owner_id = NodeId([8u8; 32]);
        let heads = MockHeads::new(owner_id);

        // Owner writes a multi-page DB (100 fat rows → many pages).
        let w = CraftSql::register(owner_dir.path(), heads.clone(), owner_id).unwrap();
        let mut db = w.open("big").await.unwrap();
        let mut sql = String::from("CREATE TABLE t(id INTEGER PRIMARY KEY, blob TEXT);");
        for i in 0..100 {
            sql += &format!("INSERT INTO t VALUES ({i}, '{}');", "x".repeat(3000));
        }
        db.write(&sql).await.unwrap();
        drop(db);

        // Total objects reachable from the DB root.
        let (root, _) = heads.resolve(owner_id, "big").await.unwrap().unwrap();
        let owner_store = crate::ObjectStore::open(owner_dir.path()).unwrap();
        let total = crate::pager::reachable(&owner_store, root).unwrap().len();
        assert!(total > 10, "DB should span many objects, got {total}");

        // Lazy reader: a point query should fetch far fewer than every object.
        let src = Arc::new(CountingSource {
            owner_store: crate::ObjectStore::open(owner_dir.path()).unwrap(),
            count: std::sync::atomic::AtomicUsize::new(0),
        });
        let r = CraftSql::register(reader_dir.path(), heads.clone(), owner_id)
            .unwrap()
            .with_source(src.clone());
        let db2 = r.open_reader(owner_id, "big").await.unwrap();
        let blob: String = db2
            .conn()
            .query_row("SELECT blob FROM t WHERE id=5", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blob.len(), 3000, "correct row read");
        let fetched = src.count.load(Relaxed);
        assert!(
            fetched < total,
            "lazy read fetched {fetched} of {total} objects (should be a subset)"
        );
    }

    #[tokio::test]
    async fn compaction_bounds_storage_and_preserves_db() {
        let dir = tempdir().unwrap();
        let owner = NodeId([11u8; 32]);
        let heads = MockHeads::new(owner);
        let durable = Arc::new(MockDurable::default());
        let manifests = MockManifest::new(owner);
        let sql = CraftSql::register(dir.path(), heads.clone(), owner)
            .unwrap()
            .with_durable(durable.clone())
            .with_manifests(manifests.clone());

        // Many writes accumulate many generations + superseded page objects.
        let mut db = sql.open("app").await.unwrap();
        db.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        for i in 0..6 {
            db.write(&format!("INSERT INTO t VALUES ({i},'row{i}');"))
                .await
                .unwrap();
        }
        drop(db);
        let objs_before = crate::ObjectStore::open(dir.path())
            .unwrap()
            .list()
            .unwrap()
            .len();

        // Compact: fold into one base generation + GC superseded objects.
        let reclaimed = sql.compact("app").await.unwrap();
        assert!(reclaimed > 0, "compaction reclaimed superseded objects");
        let objs_after = crate::ObjectStore::open(dir.path())
            .unwrap()
            .list()
            .unwrap()
            .len();
        assert!(
            objs_after < objs_before,
            "local store shrank: {objs_after} < {objs_before}"
        );

        // The DB still reads correctly after compaction.
        let db2 = sql.open("app").await.unwrap();
        let v: String = db2
            .conn()
            .query_row("SELECT v FROM t WHERE id=3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "row3");
        drop(db2);

        // And it recovers from the single compacted base generation alone.
        wipe_objects(dir.path());
        let restored = sql.recover("app").await.unwrap();
        assert!(restored > 0, "recovered from the compacted base generation");
        let db3 = sql.open("app").await.unwrap();
        let v: String = db3
            .conn()
            .query_row("SELECT v FROM t WHERE id=5", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "row5", "compacted DB still fully recoverable");
    }
}
