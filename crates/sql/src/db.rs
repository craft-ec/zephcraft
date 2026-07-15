//! The CraftSQL database handle: a single-writer SQLite DB whose head `(root, seq)` is
//! published through a [`RootStore`] (in production: the owner-signed head registry).
//!
//! The SQLite VFS is synchronous; publishing/resolving the head is async — so the split is:
//! the VFS commits locally (yielding a root CID in the in-memory roots map), while this layer
//! resolves the head before opening a connection and publishes the new head FIRE-AND-FORGET
//! after each write (LWW-by-seq — safe under single-writer; see `write()`). For a WRITABLE db
//! the local `.gens` sidecar is authoritative; the RootStore serves readers and cold starts.

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

/// The DB head: `(owner, namespace) → (root_cid, seq)`. Abstracts the head store (in
/// production the owner-signed registry) so CraftSQL is testable in-memory.
#[async_trait::async_trait]
pub trait RootStore: Send + Sync {
    /// Current head for `owner`'s DB (None if never published).
    async fn resolve(&self, owner: NodeId, namespace: &str) -> Result<Option<(Cid, u64)>>;
    /// Publish MY new head. `seq` must strictly advance (last-writer-wins by seq — the
    /// registry has no synchronous arbiter). `prev` is retained for causal ordering only;
    /// the production impl does NOT enforce it as a CAS gate (there is no synchronous
    /// `Conflict` under single-writer — the head publish is fire-and-forget, LWW-by-seq).
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
    async fn sync_reachable(
        &self,
        owner: NodeId,
        key: &str,
        root: Cid,
        fetch_pages: bool,
    ) -> Result<()> {
        let Some(source) = &self.source else {
            return Ok(());
        };
        // Write fetched pages into THIS db's per-key store so the VFS reads them locally.
        let store = open_db_store(&self.store_dir, key)?;
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
        let store = open_db_store(&self.store_dir, key)?;
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
        // Reject a namespace that isn't a single safe path component BEFORE it is used to build a
        // key / on-disk directory — it flows in unsanitized from app callers, and the key becomes a
        // `store_dir/<key>/` root (traversal guard; also enforced at the store boundary).
        if !crate::store::is_safe_component(namespace) {
            return Err(SqlError::Sqlite(format!(
                "unsafe db namespace: {namespace:?}"
            )));
        }
        let key = format!("{}_{}", &owner.to_hex()[..16], namespace);
        // Resolve the authoritative head — sidecar-first for our own DB, so the owner is never
        // locked out of its own database by head-store (registry) liveness.
        let head = if writable {
            // The owner is the single writer of its own DB, so the LOCAL sidecar is authoritative:
            // prefer it and SKIP the registry resolve, which would forward to a possibly-rotating
            // shard writer and stall the open for seconds (the write path publishes the head to the
            // registry in the background for OTHER nodes to resolve). Fall back to the registry only
            // when there is no local state yet — a cold start or a recovery onto a fresh store.
            let m = load_manifest(&self.store_dir, &key);
            match m.last_root {
                Some(r) => Some((Cid(r), m.seq)),
                None => self.heads.resolve(owner, namespace).await?,
            }
        } else {
            // Reader of (possibly another owner's) DB: resolve the authoritative head from the
            // registry — the local store may be empty or stale.
            self.heads.resolve(owner, namespace).await?
        };
        let seq = match head {
            Some((root, seq)) => {
                if !writable && self.source.is_some() {
                    // Lazy reader: sync only the (tiny) index; pull page contents
                    // on demand as the query touches them.
                    self.sync_reachable(owner, &key, root, false).await?;
                    let fetcher = self.spawn_fetcher(owner);
                    self.fetchers
                        .lock()
                        .expect("fetchers")
                        .insert(key.clone(), fetcher);
                } else {
                    // Writer (all local) or source-less: ensure everything present.
                    self.sync_reachable(owner, &key, root, true).await?;
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
        let key = format!("{}_{}", &owner.to_hex()[..16], namespace);
        let store = open_db_store(&self.store_dir, &key)?;
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

    /// Run write SQL and commit locally, then publish the new root head to the registry in the
    /// BACKGROUND (fire-and-forget). Returns as soon as the local commit is durable (pages +
    /// in-memory root map + the `.gens` sidecar); the head becomes network-resolvable
    /// asynchronously, where the registry reconciles by LWW-by-seq. Under single-writer-per-
    /// identity this is safe: `seq` advances monotonically and there is no concurrent writer to
    /// conflict with — so the caller never blocks on the registry round-trip (which can hit the
    /// writer-rotation timeout and spike write latency to seconds).
    pub async fn write(&mut self, sql: &str) -> Result<()> {
        let heads = self.heads.clone().ok_or(SqlError::ReadOnly)?;
        let prev = self.roots.lock().expect("roots").get(&self.key).copied();
        self.conn
            .execute_batch(sql)
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        let new = self.roots.lock().expect("roots").get(&self.key).copied();
        if let Some(root) = new {
            if Some(root) != prev {
                self.seq += 1;
                let ns = self.namespace.clone();
                let seq = self.seq;
                let prev_cid = prev.map(Cid);
                tokio::spawn(async move {
                    let _ = heads.publish(&ns, Cid(root), prev_cid, seq).await;
                });
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
        let store = open_db_store(&self.store_dir, &self.key)?;
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
                // Background-publish the manifest head (same reasoning as the root in `write`):
                // the `.gens` sidecar holds the authoritative generation list locally; the
                // registry catches up async, so the commit never blocks on the round-trip.
                let mstore = mstore.clone();
                let ns = self.namespace.clone();
                let mseq = manifest.manifest_seq;
                tokio::spawn(async move {
                    let _ = mstore.publish(&ns, manifest_cid, mseq).await;
                });
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

/// Open the per-DB page store rooted at `store_dir/<key>/`, with reads falling back to the legacy
/// flat `store_dir` root (objects written before per-DB isolation). Writes and GC stay in the
/// per-key root, so one DB's compaction can never delete another DB's dedup'd objects. Every
/// place that opens a store for a SPECIFIC db (`key`) must go through this — never the flat root.
fn open_db_store(store_dir: &Path, key: &str) -> Result<ObjectStore> {
    // `open_db_component` validates `key` is a single safe path component (traversal guard).
    Ok(ObjectStore::open_db_component(store_dir, key, store_dir)?)
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
    let store = open_db_store(store_dir, key)?;
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
        // Background-publish (see `write`/`sweep_durability`): don't block compaction on the
        // registry round-trip; the sidecar is authoritative and the registry reconciles by seq.
        let mstore = mstore.clone();
        let ns = namespace.to_string();
        let mseq = manifest.manifest_seq;
        tokio::spawn(async move {
            let _ = mstore.publish(&ns, mc, mseq).await;
        });
    }
    // Drop superseded generations in the BACKGROUND (the base is durable first, so this is
    // safe). Each drop is a network unpin, and doing ~COMPACT_THRESHOLD of them inline is what
    // spiked the triggering write to seconds. They are pure cleanup — the old pieces just fade —
    // so fire-and-forget them; the write returns without waiting on the unpins.
    for g in old {
        if g != base.0 {
            let durable = durable.clone();
            tokio::spawn(async move {
                let _ = durable.drop_generation(Cid(g)).await;
            });
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
            tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the async head publish land
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the async head publish land
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
        assert_eq!(v, "hello", "reopened from the signed head via RootStore");
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the async head publish land

        // Reader has an EMPTY store + a page source pointing at the owner's per-key store.
        let mail_key = format!("{}_{}", &owner_id.to_hex()[..16], "mail");
        let source = Arc::new(MockSource {
            owner_store: open_db_store(owner_dir.path(), &mail_key).unwrap(),
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
        let reader_store = open_db_store(reader_dir.path(), &mail_key).unwrap();
        let head = db2.root().unwrap();
        assert!(
            reader_store.has(&head),
            "root object synced into the reader's store"
        );
    }

    #[tokio::test]
    async fn writes_commit_locally_publish_async() {
        let dir = tempdir().unwrap();
        let owner = NodeId([9u8; 32]);
        let heads = MockHeads::new(owner);

        // Two engines, both open the (empty) DB → both think seq 0, prev None.
        let e1 = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let e2 = CraftSql::register(dir.path(), heads.clone(), owner).unwrap();
        let mut w1 = e1.open("db").await.unwrap();
        let mut w2 = e2.open("db").await.unwrap();

        // w1 commits first (locally).
        w1.write("CREATE TABLE a(x); INSERT INTO a VALUES (1);")
            .await
            .unwrap();
        // Root/manifest publishes are now fire-and-forget: w2 commits LOCALLY and returns Ok —
        // there is no synchronous `Conflict`. The head publishes to the registry in the
        // background, where LWW-by-seq reconciles. Under single-writer-per-identity a concurrent
        // stale writer does not occur; removing the sync CAS from write() is what eliminates the
        // writer-rotation tail latency (a registry round-trip could hit the 8s timeout).
        let w2_res = w2
            .write("CREATE TABLE b(y); INSERT INTO b VALUES (2);")
            .await;
        assert!(w2_res.is_ok(), "write commits locally + publishes async");
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the async head publish land
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
        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the async head + manifest publishes land
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

        // Total objects reachable from the DB root. Pages live in the per-DB store `<dir>/<key>/`.
        let (root, _) = heads.resolve(owner_id, "big").await.unwrap().unwrap();
        let big_key = format!("{}_{}", &owner_id.to_hex()[..16], "big");
        let owner_store = open_db_store(owner_dir.path(), &big_key).unwrap();
        let total = crate::pager::reachable(&owner_store, root).unwrap().len();
        assert!(total > 10, "DB should span many objects, got {total}");

        // Lazy reader: a point query should fetch far fewer than every object.
        let src = Arc::new(CountingSource {
            owner_store: open_db_store(owner_dir.path(), &big_key).unwrap(),
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
        // Count objects in the DB's OWN per-key store (`<dir>/<key>/`).
        let app_key = format!("{}_{}", &owner.to_hex()[..16], "app");
        let objs_before = open_db_store(dir.path(), &app_key)
            .unwrap()
            .list()
            .unwrap()
            .len();

        // Compact: fold into one base generation + GC superseded objects.
        let reclaimed = sql.compact("app").await.unwrap();
        assert!(reclaimed > 0, "compaction reclaimed superseded objects");
        let objs_after = open_db_store(dir.path(), &app_key)
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

    #[tokio::test]
    async fn one_dbs_compaction_never_touches_a_sibling_db() {
        // Two same-schema DBs share one CraftSql store. Their identical initial pages dedup to one
        // object, and a per-DB GC over a SHARED store used to delete the sibling's pages — compacting
        // one shard corrupted all the others ("file is not a database"). Per-DB store isolation must
        // keep DB "a"'s compaction from deleting any of DB "b"'s objects. (Regression for the
        // ObjDurable shard-page corruption.)
        let dir = tempdir().unwrap();
        let owner = NodeId([12u8; 32]);
        let heads = MockHeads::new(owner);
        let durable = Arc::new(MockDurable::default());
        let manifests = MockManifest::new(owner);
        let sql = CraftSql::register(dir.path(), heads.clone(), owner)
            .unwrap()
            .with_durable(durable.clone())
            .with_manifests(manifests.clone());

        // Sibling DB "b": one row we re-read at the end.
        let mut b = sql.open("b").await.unwrap();
        b.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'keep');")
            .await
            .unwrap();
        drop(b);

        // DB "a": many writes accumulate superseded pages + generations, then compact (the GC step
        // that used to delete across the whole shared store).
        let mut a = sql.open("a").await.unwrap();
        a.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
            .await
            .unwrap();
        for i in 0..8 {
            a.write(&format!("INSERT INTO t VALUES ({i},'row{i}');"))
                .await
                .unwrap();
        }
        drop(a);
        let reclaimed = sql.compact("a").await.unwrap();
        assert!(
            reclaimed > 0,
            "a's compaction reclaimed its own superseded pages"
        );

        // The sibling DB "b" must STILL open and read — its pages were not GC'd by a's compaction.
        let b2 = sql.open("b").await.unwrap();
        let v: String = b2
            .conn()
            .query_row("SELECT v FROM t WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            v, "keep",
            "sibling DB survived another DB's compaction (per-DB store isolation)"
        );
    }

    #[tokio::test]
    async fn traversal_namespace_is_rejected() {
        // The namespace becomes a `store_dir/<key>/` directory, and it flows in unsanitized from
        // app callers — so a name with path separators / `..` must be rejected, not turned into a
        // create_dir_all outside the store dir (path-traversal guard).
        let base = tempdir().unwrap();
        let store_dir = base.path().join("store");
        let owner = NodeId([13u8; 32]);
        let heads = MockHeads::new(owner);
        let sql = CraftSql::register(&store_dir, heads, owner).unwrap();
        for ns in ["../evil", "a/../../evil", "a/b", "/abs", "..", "."] {
            assert!(
                sql.open(ns).await.is_err(),
                "traversal namespace {ns:?} must be rejected"
            );
        }
        // The guard itself: legit registry/app namespaces pass; separators/dot-dirs don't.
        assert!(crate::store::is_safe_component("reg_0_8_42"));
        assert!(crate::store::is_safe_component("app.mydb"));
        assert!(crate::store::is_safe_component("weird..name")); // dots INSIDE a component are fine
        assert!(!crate::store::is_safe_component("../x"));
        assert!(!crate::store::is_safe_component("a/b"));
        assert!(!crate::store::is_safe_component(".."));
        assert!(!crate::store::is_safe_component(""));
    }
}
