//! Content routing: the `ContentRouting` trait, its tracker backend, and the
//! tracker service (three registries: content providers, nodes, relays).
//!
//! Decision R7: routing is a swappable trait so the iroh Kademlia DHT can
//! replace the tracker for content lookup later without touching callers.
//! The node + relay registries are PERMANENT (DHTs cannot enumerate) — see
//! CRAFTOBJ_DESIGN §Content Routing and the M2 routing tracker item.
//!
//! Provider records are CANDIDATE LISTS ONLY, never availability truth —
//! HealthScan verifies live (foundation §62.1).

mod dht_routing;
pub mod records;
pub use dht_routing::DhtRouting;
pub mod registry;
pub mod server;

use std::sync::Arc;

use async_trait::async_trait;
use zeph_core::{Cid, NodeId};
use zeph_transport::{PeerAddr, Transport};

pub use records::{NodePayload, ProviderPayload, RelayPayload};
pub use registry::{Registry, RegistryConfig};
pub use server::serve;

/// ALPN for the tracker protocol.
pub const ALPN: &[u8] = b"/craftec/tracker/1";

/// A content entry for network-wide content listing: a CID and how many
/// providers hold it (and how many pin it).
#[derive(Debug, Clone)]
pub struct ContentEntry {
    pub cid: Cid,
    pub providers: usize,
    pub pinned: usize,
    /// Sum of advisory piece_counts across providers (≈ network HAVE).
    pub pieces: usize,
    /// Number of WANT interest signals for this CID.
    pub wants: usize,
    /// Editable metadata envelopes (one per publisher). Empty if none.
    pub metas: Vec<MetaRecord>,
}

/// A resolved metadata envelope (`KIND_META`) — one publisher's editable claim
/// about a manifest CID. The default view takes `min(published_at)` across
/// these; the full set is the "who published what" expansion.
#[derive(Debug, Clone)]
pub struct MetaRecord {
    pub publisher: NodeId,
    pub published_at: u64,
    pub comment: Option<String>,
}

/// A resolved single-writer DB root pointer (`KIND_ROOT`) — the CraftSQL head.
#[derive(Debug, Clone)]
pub struct RootRecord {
    pub owner: NodeId,
    pub namespace: String,
    pub root_cid: Cid,
    pub seq: u64,
}

/// A resolved CraftCOM app head (`KIND_APP`) — `(publisher, name) → wasm_cid` at a
/// version. Makes an app resolvable + invocable BY NAME network-wide.
#[derive(Debug, Clone)]
pub struct AppRecord {
    pub publisher: NodeId,
    pub name: String,
    pub wasm_cid: Cid,
    pub version: u64,
}

/// A resolved DB durability-manifest pointer (`KIND_MANIFEST`) — the CID of the
/// object listing a DB's erasure-coded generations, for network recovery.
#[derive(Debug, Clone)]
pub struct ManifestRecord {
    pub owner: NodeId,
    pub namespace: String,
    pub manifest_cid: Cid,
    pub seq: u64,
}

/// A resolved provider: who holds `cid`, where to dial them, advisory count.
#[derive(Debug, Clone)]
pub struct ProviderRecord {
    pub node_id: NodeId,
    pub addr: PeerAddr,
    pub piece_count: u32,
    pub pinned: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("no tracker reachable")]
    NoTracker,
    #[error("tracker error: {0}")]
    Tracker(String),
    /// A compare-and-swap root update lost the race (stale prev_cid/seq).
    #[error("root conflict: {0}")]
    Conflict(String),
}

pub type Result<T> = std::result::Result<T, RoutingError>;

/// Swappable content-routing backend (tracker now, iroh DHT later).
#[async_trait]
pub trait ContentRouting: Send + Sync {
    /// Announce this node as a provider for `cid`.
    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()>;
    /// Candidate providers for `cid`. ADVISORY — never health truth.
    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>>;
    /// Best-effort withdrawal (graceful departure).
    async fn withdraw(&self, cid: Cid) -> Result<()>;
    /// Live node registry (for the map / census).
    async fn nodes(&self) -> Result<Vec<(NodeId, NodePayload)>>;
    /// Live relay registry (for dynamic relay discovery).
    async fn relays(&self) -> Result<Vec<RelayPayload>>;
    /// Announce this node into the node registry (map/census) with its
    /// storage usage + offered capacity.
    async fn announce_node_registry(&self, used_bytes: u64, capacity_bytes: u64) -> Result<()>;
    /// Announce a relay this node operates into the relay registry (§26).
    async fn announce_relay_registry(&self, relay_url: String) -> Result<()>;
    /// Enumerate the network's content (all CIDs the tracker knows), grouped
    /// with provider + pinned counts. Empty on backends that can't enumerate.
    async fn content(&self) -> Result<Vec<ContentEntry>>;
    /// Announce a WANT interest signal for `cid` (keep-alive intent; no holding).
    async fn announce_want(&self, cid: Cid) -> Result<()>;
    /// Withdraw this node's WANT for `cid`.
    async fn withdraw_want(&self, cid: Cid) -> Result<()>;
    /// CIDs the network currently WANTs (has ≥1 want record). Used by Fade to
    /// decide which content to keep repairing.
    async fn wanted_cids(&self) -> Result<Vec<Cid>>;
    /// Announce/replace this node's editable metadata envelope for `cid`
    /// (`published_at` preserved by the caller across edits; superseded by HLC).
    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> Result<()>;
    /// Withdraw this node's metadata envelope for `cid`.
    async fn withdraw_meta(&self, cid: Cid) -> Result<()>;
    /// All metadata envelopes for `cid`, across publishers (the full-set view).
    async fn metas(&self, cid: Cid) -> Result<Vec<MetaRecord>>;
    /// Publish this identity's DB root for `namespace` via compare-and-swap:
    /// `prev_cid` must match the current root (None = expect no prior root),
    /// and `seq` must strictly advance. Returns `Conflict` if the CAS loses.
    async fn publish_root(
        &self,
        namespace: &str,
        root_cid: Cid,
        prev_cid: Option<Cid>,
        seq: u64,
    ) -> Result<()>;
    /// Resolve `owner`'s current DB root for `namespace` (None if none/withdrawn).
    async fn resolve_root(&self, owner: NodeId, namespace: &str) -> Result<Option<RootRecord>>;

    /// Publish this node's app head `(self, name) → (wasm_cid, version)`, signed.
    /// Default: unsupported (only tracker/DHT routing implements it).
    async fn announce_app(&self, _name: &str, _wasm_cid: Cid, _version: u64) -> Result<()> {
        Err(RoutingError::NoTracker)
    }
    /// Resolve `publisher`'s app `name` to its current head. Default: none.
    async fn resolve_app(&self, _publisher: NodeId, _name: &str) -> Result<Option<AppRecord>> {
        Ok(None)
    }
    /// Withdraw this identity's DB root for `namespace`.
    async fn withdraw_root(&self, namespace: &str) -> Result<()>;
    /// Publish this identity's DB durability-manifest pointer for `namespace`
    /// (highest `seq` wins). Lets any node later recover the DB from its pieces.
    async fn publish_manifest(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()>;
    /// Resolve `owner`'s current durability-manifest pointer for `namespace`.
    async fn resolve_manifest(
        &self,
        owner: NodeId,
        namespace: &str,
    ) -> Result<Option<ManifestRecord>>;
}

/// Tracker-backed routing client: announces to and resolves from a set of
/// configured trackers. Every record is re-verified locally.
pub struct TrackerRouting {
    transport: Arc<Transport>,
    identity: Arc<zeph_crypto::NodeIdentity>,
    trackers: Vec<PeerAddr>,
    self_addr: String,
    version: String,
}

impl TrackerRouting {
    pub fn new(
        transport: Arc<Transport>,
        identity: Arc<zeph_crypto::NodeIdentity>,
        trackers: Vec<PeerAddr>,
        version: String,
    ) -> Self {
        let self_addr = transport.addr().to_string();
        Self {
            transport,
            identity,
            trackers,
            self_addr,
            version,
        }
    }

    fn now_hlc(&self) -> u64 {
        self.transport.clock().now().0
    }

    async fn broadcast(&self, msg: &zeph_wire::Message) -> Result<()> {
        let mut any = false;
        for tracker in &self.trackers {
            if server::request(&self.transport, tracker, msg).await.is_ok() {
                any = true;
            }
        }
        if any {
            Ok(())
        } else {
            Err(RoutingError::NoTracker)
        }
    }

    /// Like `broadcast`, but returns the tracker's ACK so compare-and-swap
    /// callers can distinguish accept from conflict. CAS needs a single
    /// authority: today the first reachable tracker is authoritative (a DHT
    /// root under quorum replaces this later).
    async fn announce_cas(&self, rec: zeph_wire::SignedRecord) -> Result<()> {
        for tracker in &self.trackers {
            let msg = zeph_wire::Message::TrackerAnnounce(rec.clone());
            if let Ok(zeph_wire::Message::TrackerAnnounceAck(ack)) =
                server::request(&self.transport, tracker, &msg).await
            {
                return if ack.ok {
                    Ok(())
                } else {
                    Err(RoutingError::Conflict(ack.reason))
                };
            }
        }
        Err(RoutingError::NoTracker)
    }

    /// Announce this node's existence into the node registry, with storage
    /// usage (bytes stored) and offered capacity (quota).
    pub async fn announce_node(&self, used_bytes: u64, capacity_bytes: u64) -> Result<()> {
        let payload = NodePayload {
            addr: self.self_addr.clone(),
            version: self.version.clone(),
            used_bytes,
            capacity_bytes,
        };
        let rec = records::sign(&self.identity, records::KIND_NODE, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerAnnounce(rec))
            .await
    }

    /// Announce this node as a relay operator.
    pub async fn announce_relay(&self, relay_url: String) -> Result<()> {
        let payload = RelayPayload { relay_url };
        let rec = records::sign(
            &self.identity,
            records::KIND_RELAY,
            &payload,
            self.now_hlc(),
        );
        self.broadcast(&zeph_wire::Message::TrackerAnnounce(rec))
            .await
    }

    async fn query(
        &self,
        query: zeph_wire::TrackerResolve,
    ) -> Result<Vec<zeph_wire::SignedRecord>> {
        for tracker in &self.trackers {
            let msg = zeph_wire::Message::TrackerResolve(query.clone());
            if let Ok(zeph_wire::Message::TrackerResolveReply(reply)) =
                server::request(&self.transport, tracker, &msg).await
            {
                // Re-verify every record; drop anything that fails.
                return Ok(reply.records.into_iter().filter(records::verify).collect());
            }
        }
        Err(RoutingError::NoTracker)
    }
}

#[async_trait]
impl ContentRouting for TrackerRouting {
    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()> {
        let payload = ProviderPayload {
            cid: cid.0,
            piece_count,
            addr: self.self_addr.clone(),
            pinned,
        };
        let rec = records::sign(
            &self.identity,
            records::KIND_PROVIDER,
            &payload,
            self.now_hlc(),
        );
        self.broadcast(&zeph_wire::Message::TrackerAnnounce(rec))
            .await
    }

    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>> {
        let records = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 1,
                cid: cid.0,
                max: 64,
            })
            .await?;
        Ok(records
            .iter()
            .filter_map(|r| {
                let p = records::provider(r)?;
                let addr: PeerAddr = p.addr.parse().ok()?;
                Some(ProviderRecord {
                    node_id: NodeId(r.node_id),
                    addr,
                    piece_count: p.piece_count,
                    pinned: p.pinned,
                })
            })
            .collect())
    }

    async fn withdraw(&self, cid: Cid) -> Result<()> {
        let payload = ProviderPayload {
            cid: cid.0,
            piece_count: 0,
            addr: self.self_addr.clone(),
            pinned: false,
        };
        let rec = records::sign(
            &self.identity,
            records::KIND_PROVIDER,
            &payload,
            self.now_hlc(),
        );
        self.broadcast(&zeph_wire::Message::TrackerWithdraw(rec))
            .await
    }

    async fn nodes(&self) -> Result<Vec<(NodeId, NodePayload)>> {
        let records = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 2,
                cid: [0; 32],
                max: 4096,
            })
            .await?;
        Ok(records
            .iter()
            .filter_map(|r| Some((NodeId(r.node_id), records::node(r)?)))
            .collect())
    }

    async fn relays(&self) -> Result<Vec<RelayPayload>> {
        let records = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 3,
                cid: [0; 32],
                max: 256,
            })
            .await?;
        Ok(records.iter().filter_map(records::relay).collect())
    }

    async fn content(&self) -> Result<Vec<ContentEntry>> {
        let records = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 4,
                cid: [0; 32],
                max: 4096,
            })
            .await?;
        // Group provider records by CID → (providers, pinned, pieces).
        let mut by_cid: std::collections::HashMap<[u8; 32], (usize, usize, usize)> =
            std::collections::HashMap::new();
        for r in &records {
            if let Some(p) = records::provider(r) {
                let e = by_cid.entry(p.cid).or_insert((0, 0, 0));
                e.0 += 1;
                if p.pinned {
                    e.1 += 1;
                }
                e.2 += p.piece_count as usize;
            }
        }
        // Merge WANT counts (kind 5).
        let mut wants: std::collections::HashMap<[u8; 32], usize> =
            std::collections::HashMap::new();
        if let Ok(wrecs) = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 5,
                cid: [0; 32],
                max: 4096,
            })
            .await
        {
            for r in &wrecs {
                if let Some(w) = records::want(r) {
                    *wants.entry(w.cid).or_insert(0) += 1;
                    by_cid.entry(w.cid).or_insert((0, 0, 0));
                }
            }
        }
        // Merge metadata envelopes (kind 6).
        let mut metas: std::collections::HashMap<[u8; 32], Vec<MetaRecord>> =
            std::collections::HashMap::new();
        if let Ok(mrecs) = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 6,
                cid: [0; 32],
                max: 4096,
            })
            .await
        {
            for r in &mrecs {
                if let Some(m) = records::meta(r) {
                    metas.entry(m.cid).or_default().push(MetaRecord {
                        publisher: NodeId(r.node_id),
                        published_at: m.published_at,
                        comment: m.comment,
                    });
                    by_cid.entry(m.cid).or_insert((0, 0, 0));
                }
            }
        }
        let mut out: Vec<ContentEntry> = by_cid
            .into_iter()
            .map(|(cid, (providers, pinned, pieces))| ContentEntry {
                cid: Cid(cid),
                providers,
                pinned,
                pieces,
                wants: wants.get(&cid).copied().unwrap_or(0),
                metas: metas.remove(&cid).unwrap_or_default(),
            })
            .collect();
        out.sort_by(|a, b| b.providers.cmp(&a.providers));
        Ok(out)
    }

    async fn announce_node_registry(&self, used_bytes: u64, capacity_bytes: u64) -> Result<()> {
        self.announce_node(used_bytes, capacity_bytes).await
    }

    async fn announce_relay_registry(&self, relay_url: String) -> Result<()> {
        self.announce_relay(relay_url).await
    }

    async fn announce_want(&self, cid: Cid) -> Result<()> {
        let payload = records::WantPayload { cid: cid.0 };
        let rec = records::sign(&self.identity, records::KIND_WANT, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerAnnounce(rec))
            .await
    }

    async fn withdraw_want(&self, cid: Cid) -> Result<()> {
        let payload = records::WantPayload { cid: cid.0 };
        let rec = records::sign(&self.identity, records::KIND_WANT, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerWithdraw(rec))
            .await
    }

    async fn wanted_cids(&self) -> Result<Vec<Cid>> {
        let recs = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 5,
                cid: [0; 32],
                max: 4096,
            })
            .await?;
        Ok(recs
            .iter()
            .filter_map(|r| records::want(r).map(|w| Cid(w.cid)))
            .collect())
    }

    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> Result<()> {
        let payload = records::MetaPayload {
            cid: cid.0,
            published_at,
            comment,
        };
        let rec = records::sign(&self.identity, records::KIND_META, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerAnnounce(rec))
            .await
    }

    async fn withdraw_meta(&self, cid: Cid) -> Result<()> {
        let payload = records::MetaPayload {
            cid: cid.0,
            published_at: 0,
            comment: None,
        };
        let rec = records::sign(&self.identity, records::KIND_META, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerWithdraw(rec))
            .await
    }

    async fn metas(&self, cid: Cid) -> Result<Vec<MetaRecord>> {
        let recs = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 6,
                cid: [0; 32],
                max: 4096,
            })
            .await?;
        Ok(recs
            .iter()
            .filter_map(|r| {
                let m = records::meta(r)?;
                if m.cid != cid.0 {
                    return None;
                }
                Some(MetaRecord {
                    publisher: NodeId(r.node_id),
                    published_at: m.published_at,
                    comment: m.comment,
                })
            })
            .collect())
    }

    async fn publish_root(
        &self,
        namespace: &str,
        root_cid: Cid,
        prev_cid: Option<Cid>,
        seq: u64,
    ) -> Result<()> {
        let payload = records::RootPayload {
            namespace: namespace.to_string(),
            root_cid: root_cid.0,
            prev_cid: prev_cid.map(|c| c.0).unwrap_or([0u8; 32]),
            seq,
        };
        let rec = records::sign(&self.identity, records::KIND_ROOT, &payload, self.now_hlc());
        self.announce_cas(rec).await
    }

    async fn resolve_root(&self, owner: NodeId, namespace: &str) -> Result<Option<RootRecord>> {
        let recs = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 7,
                cid: owner.0,
                max: 64,
            })
            .await?;
        Ok(recs
            .iter()
            .filter_map(|r| {
                let p = records::root(r)?;
                if r.node_id != owner.0 || p.namespace != namespace {
                    return None;
                }
                Some(RootRecord {
                    owner,
                    namespace: p.namespace,
                    root_cid: Cid(p.root_cid),
                    seq: p.seq,
                })
            })
            .max_by_key(|rr| rr.seq))
    }

    async fn announce_app(&self, name: &str, wasm_cid: Cid, version: u64) -> Result<()> {
        let payload = records::AppPayload {
            name: name.to_string(),
            wasm_cid: wasm_cid.0,
            version,
        };
        let rec = records::sign(&self.identity, records::KIND_APP, &payload, self.now_hlc());
        self.announce_cas(rec).await
    }

    async fn resolve_app(&self, publisher: NodeId, name: &str) -> Result<Option<AppRecord>> {
        let recs = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 9,
                cid: publisher.0,
                max: 64,
            })
            .await?;
        Ok(recs
            .iter()
            .filter_map(|r| {
                let p = records::app(r)?;
                if r.node_id != publisher.0 || p.name != name {
                    return None;
                }
                Some(AppRecord {
                    publisher,
                    name: p.name,
                    wasm_cid: Cid(p.wasm_cid),
                    version: p.version,
                })
            })
            .max_by_key(|a| a.version))
    }

    async fn publish_manifest(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()> {
        let payload = records::ManifestPayload {
            namespace: namespace.to_string(),
            manifest_cid: manifest_cid.0,
            seq,
        };
        let rec = records::sign(
            &self.identity,
            records::KIND_MANIFEST,
            &payload,
            self.now_hlc(),
        );
        self.announce_cas(rec).await
    }

    async fn resolve_manifest(
        &self,
        owner: NodeId,
        namespace: &str,
    ) -> Result<Option<ManifestRecord>> {
        let recs = self
            .query(zeph_wire::TrackerResolve {
                query_kind: 8,
                cid: owner.0,
                max: 64,
            })
            .await?;
        Ok(recs
            .iter()
            .filter_map(|r| {
                let p = records::manifest(r)?;
                if r.node_id != owner.0 || p.namespace != namespace {
                    return None;
                }
                Some(ManifestRecord {
                    owner,
                    namespace: p.namespace,
                    manifest_cid: Cid(p.manifest_cid),
                    seq: p.seq,
                })
            })
            .max_by_key(|rr| rr.seq))
    }

    async fn withdraw_root(&self, namespace: &str) -> Result<()> {
        let payload = records::RootPayload {
            namespace: namespace.to_string(),
            root_cid: [0u8; 32],
            prev_cid: [0u8; 32],
            seq: 0,
        };
        let rec = records::sign(&self.identity, records::KIND_ROOT, &payload, self.now_hlc());
        self.broadcast(&zeph_wire::Message::TrackerWithdraw(rec))
            .await
    }
}
