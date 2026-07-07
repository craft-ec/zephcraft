//! Test doubles for content routing — a lightweight, in-memory `ContentRouting`
//! (`MemRouting`) and `PeerSource` (`MemPeers`) that let multi-node tests run
//! without standing up a real DHT.
//!
//! All state lives in one shared `MemNet` (an `Arc<Mutex<..>>`), so every test
//! node's `MemRouting` clone reads and writes the SAME network view — announces
//! by one node are resolvable by another, exactly like a real DHT.
//!
//! Fidelity notes (semantics cross-checked against `zeph_routing::DhtRouting`):
//!  - **providers / wants / metas** — per-CID, keyed by the announcing NodeId;
//!    many coexist; re-announce replaces, withdraw removes this node's record.
//!  - **app** — publisher+name-keyed head, monotonic version (equal accepted;
//!    lower rejected).
//!  - **census** — populated by the inherent `MemRouting::announce_node`; `MemPeers`
//!    is a VIEW over the same census, so "who is a candidate peer" == "who called
//!    announce_node". (`ContentRouting` no longer exposes a `nodes()` enumeration.)

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::PeerSource;
use zeph_routing::{AppRecord, ContentRouting, MetaRecord, ProviderRecord, Result, RoutingError};
use zeph_transport::PeerAddr;

// ── shared inner state ─────────────────────────────────────────────────────

struct ProviderEntry {
    addr: PeerAddr,
    piece_count: u32,
    pinned: bool,
}

struct MetaEntry {
    published_at: u64,
    comment: Option<String>,
}

struct AppEntry {
    wasm_cid: [u8; 32],
    version: u64,
}

struct CensusEntry {
    addr: PeerAddr,
}

#[derive(Default)]
struct Inner {
    /// cid -> node_id -> provider
    providers: HashMap<[u8; 32], HashMap<[u8; 32], ProviderEntry>>,
    /// cid -> set of interested node_ids (WANT signals)
    wants: HashMap<[u8; 32], HashSet<[u8; 32]>>,
    /// cid -> node_id -> editable metadata envelope
    metas: HashMap<[u8; 32], HashMap<[u8; 32], MetaEntry>>,
    /// (publisher, name) -> app head (highest version)
    apps: HashMap<([u8; 32], String), AppEntry>,
    /// node_id -> census entry (map/peer registry)
    census: HashMap<[u8; 32], CensusEntry>,
}

/// The shared, in-memory network view. Cheaply cloned; all clones share one store.
#[derive(Clone, Default)]
pub struct MemNet {
    inner: Arc<Mutex<Inner>>,
}

impl MemNet {
    pub fn new() -> Self {
        Self::default()
    }

    /// A per-node routing client bound to this shared network. Captures the
    /// node's (identity, self-addr): the identity keys this node's records,
    /// `addr` is what resolvers dial.
    pub fn routing(&self, identity: Arc<NodeIdentity>, addr: PeerAddr) -> Arc<MemRouting> {
        Arc::new(MemRouting {
            inner: self.inner.clone(),
            identity,
            addr,
        })
    }

    /// A `PeerSource` view over this network's census (the set of nodes that
    /// called `announce_node`). Clone-shareable; pass into `ObjEngine::with_peer_source`.
    pub fn peers(&self) -> MemPeers {
        MemPeers {
            inner: self.inner.clone(),
        }
    }
}

// ── MemPeers: a PeerSource over the shared census ──────────────────────────

/// In-memory [`PeerSource`] backed by the shared census. Registration mirrors
/// the tracker's node registry: a node becomes a candidate peer when it
/// announces itself (`MemRouting::announce_node`),
/// or explicitly via [`MemPeers::register`].
#[derive(Clone)]
pub struct MemPeers {
    inner: Arc<Mutex<Inner>>,
}

impl MemPeers {
    /// Register (or refresh the address of) `node_id` as a live candidate peer.
    pub fn register(&self, node_id: NodeId, addr: PeerAddr) {
        let mut g = self.inner.lock().expect("memnet lock");
        g.census.insert(node_id.0, CensusEntry { addr });
    }

    /// The registered candidate peers (id + dial addr).
    pub fn peers_now(&self) -> Vec<(NodeId, PeerAddr)> {
        let g = self.inner.lock().expect("memnet lock");
        g.census
            .iter()
            .map(|(id, e)| (NodeId(*id), e.addr.clone()))
            .collect()
    }
}

#[async_trait]
impl PeerSource for MemPeers {
    async fn peers(&self) -> Vec<(NodeId, PeerAddr)> {
        self.peers_now()
    }
}

// ── MemRouting: an in-memory ContentRouting ────────────────────────────────

/// In-memory content-routing client for one test node. Clones share the same
/// `MemNet` inner store; each carries its own identity + dial address.
pub struct MemRouting {
    inner: Arc<Mutex<Inner>>,
    identity: Arc<NodeIdentity>,
    addr: PeerAddr,
}

impl MemRouting {
    fn me(&self) -> [u8; 32] {
        self.identity.node_id().0
    }

    /// Announce this node into the census (candidate-peer registry). The
    /// storage-usage / capacity args are accepted for call-site compatibility
    /// but unused — the census only tracks the dialable address. Inherent
    /// method that tests call directly to register a node as a candidate peer.
    pub async fn announce_node(&self, _used_bytes: u64, _capacity_bytes: u64) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        g.census.insert(
            self.me(),
            CensusEntry {
                addr: self.addr.clone(),
            },
        );
        Ok(())
    }
}

#[async_trait]
impl ContentRouting for MemRouting {
    // ---- provider records ----------------------------------------------------

    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        g.providers.entry(cid.0).or_default().insert(
            self.me(),
            ProviderEntry {
                addr: self.addr.clone(),
                piece_count,
                pinned,
            },
        );
        Ok(())
    }

    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>> {
        let g = self.inner.lock().expect("memnet lock");
        Ok(g.providers
            .get(&cid.0)
            .map(|by_node| {
                by_node
                    .iter()
                    .map(|(id, e)| ProviderRecord {
                        node_id: NodeId(*id),
                        addr: e.addr.clone(),
                        piece_count: e.piece_count,
                        pinned: e.pinned,
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn withdraw(&self, cid: Cid) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        if let Some(by_node) = g.providers.get_mut(&cid.0) {
            by_node.remove(&self.me());
            if by_node.is_empty() {
                g.providers.remove(&cid.0);
            }
        }
        Ok(())
    }

    // ---- want signals --------------------------------------------------------

    async fn announce_want(&self, cid: Cid) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        g.wants.entry(cid.0).or_default().insert(self.me());
        Ok(())
    }

    async fn withdraw_want(&self, cid: Cid) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        if let Some(by_node) = g.wants.get_mut(&cid.0) {
            by_node.remove(&self.me());
            if by_node.is_empty() {
                g.wants.remove(&cid.0);
            }
        }
        Ok(())
    }

    async fn is_wanted(&self, cid: Cid) -> Result<bool> {
        let g = self.inner.lock().expect("memnet lock");
        Ok(g.wants.get(&cid.0).is_some_and(|s| !s.is_empty()))
    }

    // ---- editable metadata ---------------------------------------------------

    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        g.metas.entry(cid.0).or_default().insert(
            self.me(),
            MetaEntry {
                published_at,
                comment,
            },
        );
        Ok(())
    }

    async fn withdraw_meta(&self, cid: Cid) -> Result<()> {
        let mut g = self.inner.lock().expect("memnet lock");
        if let Some(by_node) = g.metas.get_mut(&cid.0) {
            by_node.remove(&self.me());
            if by_node.is_empty() {
                g.metas.remove(&cid.0);
            }
        }
        Ok(())
    }

    async fn metas(&self, cid: Cid) -> Result<Vec<MetaRecord>> {
        let g = self.inner.lock().expect("memnet lock");
        Ok(g.metas
            .get(&cid.0)
            .map(|by_node| {
                by_node
                    .iter()
                    .map(|(id, e)| MetaRecord {
                        publisher: NodeId(*id),
                        published_at: e.published_at,
                        comment: e.comment.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    // ---- app head (publisher+name keyed, monotonic version) ------------------

    async fn announce_app(&self, name: &str, wasm_cid: Cid, version: u64) -> Result<()> {
        let key = (self.me(), name.to_string());
        let mut g = self.inner.lock().expect("memnet lock");
        if let Some(cur) = g.apps.get(&key) {
            if version < cur.version {
                return Err(RoutingError::Conflict("app-stale".into()));
            }
        }
        g.apps.insert(
            key,
            AppEntry {
                wasm_cid: wasm_cid.0,
                version,
            },
        );
        Ok(())
    }

    async fn resolve_app(&self, publisher: NodeId, name: &str) -> Result<Option<AppRecord>> {
        let g = self.inner.lock().expect("memnet lock");
        Ok(g.apps
            .get(&(publisher.0, name.to_string()))
            .map(|e| AppRecord {
                publisher,
                name: name.to_string(),
                wasm_cid: Cid(e.wasm_cid),
                version: e.version,
            }))
    }
}

// ── MemHeads: shared in-memory DB-head + manifest store ─────────────────────

/// Shared in-memory CraftSQL head (`RootStore`) + durability-manifest
/// (`ManifestStore`) store for multi-node tests. It stands in for the owner-signed
/// head registry, which lives in `noded` (above the crates these tests exercise) and
/// so isn't reachable here. One `MemHeads` is the shared "network": every node gets a
/// view via [`MemHeads::root_store`] / [`MemHeads::manifest_store`] that publishes as
/// that node (implicit owner) and resolves any owner — so a head/manifest one node
/// publishes is recoverable by another (cross-node reads + rebuild-from-name).
/// Shared `(owner, namespace) -> (cid, seq)` table backing a [`MemHeads`].
type HeadTable = Arc<Mutex<HashMap<(NodeId, String), ([u8; 32], u64)>>>;

#[derive(Clone, Default)]
pub struct MemHeads {
    roots: HeadTable,
    manifests: HeadTable,
}

impl MemHeads {
    pub fn new() -> Self {
        Self::default()
    }

    /// A [`RootStore`] view that publishes `owner`'s DB heads into the shared map.
    pub fn root_store(&self, owner: NodeId) -> Arc<MemRootStore> {
        Arc::new(MemRootStore {
            owner,
            roots: self.roots.clone(),
        })
    }

    /// A [`ManifestStore`] view that publishes `owner`'s durability manifests.
    pub fn manifest_store(&self, owner: NodeId) -> Arc<MemManifestStore> {
        Arc::new(MemManifestStore {
            owner,
            manifests: self.manifests.clone(),
        })
    }
}

/// Per-node [`RootStore`] view over a shared [`MemHeads`]. Same compare-and-swap
/// semantics as the production head store: `prev` must match the current root and
/// `seq` must strictly advance, else [`SqlError::Conflict`].
pub struct MemRootStore {
    owner: NodeId,
    roots: HeadTable,
}

#[async_trait]
impl zeph_sql::RootStore for MemRootStore {
    async fn resolve(
        &self,
        owner: NodeId,
        namespace: &str,
    ) -> zeph_sql::Result<Option<(Cid, u64)>> {
        Ok(self
            .roots
            .lock()
            .expect("memheads lock")
            .get(&(owner, namespace.to_string()))
            .map(|(r, s)| (Cid(*r), *s)))
    }

    async fn publish(
        &self,
        namespace: &str,
        root: Cid,
        prev: Option<Cid>,
        seq: u64,
    ) -> zeph_sql::Result<()> {
        let mut m = self.roots.lock().expect("memheads lock");
        let key = (self.owner, namespace.to_string());
        match m.get(&key) {
            None if prev.is_some() => return Err(zeph_sql::SqlError::Conflict),
            None => {}
            Some((cur, cur_seq)) => {
                if prev.map(|c| c.0) != Some(*cur) || seq <= *cur_seq {
                    return Err(zeph_sql::SqlError::Conflict);
                }
            }
        }
        m.insert(key, (root.0, seq));
        Ok(())
    }
}

/// Per-node [`ManifestStore`] view over a shared [`MemHeads`]. Owner-keyed,
/// last-writer-wins by strictly-advancing `seq`.
pub struct MemManifestStore {
    owner: NodeId,
    manifests: HeadTable,
}

#[async_trait]
impl zeph_sql::ManifestStore for MemManifestStore {
    async fn publish(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> zeph_sql::Result<()> {
        self.manifests
            .lock()
            .expect("memheads lock")
            .insert((self.owner, namespace.to_string()), (manifest_cid.0, seq));
        Ok(())
    }

    async fn resolve(
        &self,
        owner: NodeId,
        namespace: &str,
    ) -> zeph_sql::Result<Option<(Cid, u64)>> {
        Ok(self
            .manifests
            .lock()
            .expect("memheads lock")
            .get(&(owner, namespace.to_string()))
            .map(|(c, s)| (Cid(*c), *s)))
    }
}
