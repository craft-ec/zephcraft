//! Content routing: the `ContentRouting` trait and its Kademlia-DHT backend
//! (`DhtRouting`). Content lookup (providers, wants, metas, and owner-keyed
//! heads) is keyed and served over the DHT; there is no central tracker.
//!
//! Decision R7: routing is a swappable trait so the backend can change without
//! touching callers.
//!
//! Provider records are CANDIDATE LISTS ONLY, never availability truth —
//! HealthScan verifies live (foundation §62.1).

mod dht_routing;
pub mod records;
pub use dht_routing::DhtRouting;

use async_trait::async_trait;
use zeph_core::{Cid, NodeId};
use zeph_transport::PeerAddr;

pub use records::ProviderPayload;

/// ALPN for the content-routing protocol. The DHT is the sole backend; the `tracker` token
/// in the wire name is retained ONLY for wire-compatibility with deployed peers — do not
/// change this string.
pub const ALPN: &[u8] = b"/craftec/tracker/1";

/// A resolved metadata envelope (`KIND_META`) — one publisher's editable claim
/// about a manifest CID. The default view takes `min(published_at)` across
/// these; the full set is the "who published what" expansion.
#[derive(Debug, Clone)]
pub struct MetaRecord {
    pub publisher: NodeId,
    pub published_at: u64,
    pub comment: Option<String>,
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
    // NOTE: the `NoTracker` / `Tracker` variant names (and their message strings) are
    // legacy from the retired tracker backend; the DHT is now the sole backend. Renaming
    // them is a follow-up identifier refactor (tech-debt), deliberately out of scope here.
    #[error("no tracker reachable")]
    NoTracker,
    #[error("tracker error: {0}")]
    Tracker(String),
    /// An owner-keyed head update lost the race (a non-advancing/stale version).
    #[error("head conflict: {0}")]
    Conflict(String),
}

pub type Result<T> = std::result::Result<T, RoutingError>;

/// Swappable content-routing backend. The Kademlia DHT (`DhtRouting`) is the sole backend;
/// the trait stays swappable (decision R7) so the backend can change without touching callers.
#[async_trait]
pub trait ContentRouting: Send + Sync {
    /// Announce this node as a provider for `cid`.
    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()>;
    /// Candidate providers for `cid`. ADVISORY — never health truth.
    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>>;
    /// Best-effort withdrawal (graceful departure).
    async fn withdraw(&self, cid: Cid) -> Result<()>;
    /// Announce a WANT interest signal for `cid` (keep-alive intent; no holding).
    async fn announce_want(&self, cid: Cid) -> Result<()>;
    /// Withdraw this node's WANT for `cid`.
    async fn withdraw_want(&self, cid: Cid) -> Result<()>;
    /// Is `cid` wanted by anyone? A DHT cannot enumerate all wants, so Fade asks
    /// per held CID with a direct keyed lookup.
    async fn is_wanted(&self, cid: Cid) -> Result<bool>;
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

    /// Publish this node's app head `(self, name) → (wasm_cid, version)`, signed.
    /// Default: unsupported (only the DHT routing backend implements it).
    async fn announce_app(&self, _name: &str, _wasm_cid: Cid, _version: u64) -> Result<()> {
        Err(RoutingError::NoTracker)
    }
    /// Resolve `publisher`'s app `name` to its current head. Default: none.
    async fn resolve_app(&self, _publisher: NodeId, _name: &str) -> Result<Option<AppRecord>> {
        Ok(None)
    }
}
