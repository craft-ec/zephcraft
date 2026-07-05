//! `CompositeRouting` — the transition wiring: **content routing on the DHT, census on the
//! tracker**. Per foundation §62 + the chosen split:
//!
//! - **DHT** serves the scalable, keyed content records: provider records (the heavy one),
//!   editable metadata, and owner-keyed heads (DB root / app / manifest).
//! - **Tracker** serves what a DHT cannot enumerate: the node/relay census, and WANT signals
//!   (census-scale — one small interest record per node, and Fade still enumerates them until
//!   it moves to per-CID lookups in P5).
//! - `content()` (the network-wide CID list) is dropped — the dashboard is local now.
//!
//! It is a thin delegator: every method forwards to whichever backend owns that record kind.

use async_trait::async_trait;
use std::sync::Arc;
use zeph_core::{Cid, NodeId};

use crate::{
    AppRecord, ContentEntry, ContentRouting, DhtRouting, ManifestRecord, MetaRecord, NodePayload,
    ProviderRecord, RelayPayload, Result, RootRecord,
};

pub struct CompositeRouting {
    /// Content records (provider / meta / root / app / manifest).
    dht: DhtRouting,
    /// Node/relay census + WANT signals + bootstrap.
    census: Arc<dyn ContentRouting>,
}

impl CompositeRouting {
    pub fn new(dht: DhtRouting, census: Arc<dyn ContentRouting>) -> Self {
        Self { dht, census }
    }
}

#[async_trait]
impl ContentRouting for CompositeRouting {
    // ---- content → DHT -----------------------------------------------------------

    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()> {
        self.dht.announce(cid, piece_count, pinned).await
    }
    async fn resolve(&self, cid: Cid) -> Result<Vec<ProviderRecord>> {
        self.dht.resolve(cid).await
    }
    async fn withdraw(&self, cid: Cid) -> Result<()> {
        self.dht.withdraw(cid).await
    }
    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> Result<()> {
        self.dht.announce_meta(cid, published_at, comment).await
    }
    async fn withdraw_meta(&self, cid: Cid) -> Result<()> {
        self.dht.withdraw_meta(cid).await
    }
    async fn metas(&self, cid: Cid) -> Result<Vec<MetaRecord>> {
        self.dht.metas(cid).await
    }
    async fn publish_root(
        &self,
        namespace: &str,
        root_cid: Cid,
        prev_cid: Option<Cid>,
        seq: u64,
    ) -> Result<()> {
        self.dht
            .publish_root(namespace, root_cid, prev_cid, seq)
            .await
    }
    async fn resolve_root(&self, owner: NodeId, namespace: &str) -> Result<Option<RootRecord>> {
        self.dht.resolve_root(owner, namespace).await
    }
    async fn withdraw_root(&self, namespace: &str) -> Result<()> {
        self.dht.withdraw_root(namespace).await
    }
    async fn announce_app(&self, name: &str, wasm_cid: Cid, version: u64) -> Result<()> {
        self.dht.announce_app(name, wasm_cid, version).await
    }
    async fn resolve_app(&self, publisher: NodeId, name: &str) -> Result<Option<AppRecord>> {
        self.dht.resolve_app(publisher, name).await
    }
    async fn publish_manifest(&self, namespace: &str, manifest_cid: Cid, seq: u64) -> Result<()> {
        self.dht
            .publish_manifest(namespace, manifest_cid, seq)
            .await
    }
    async fn resolve_manifest(
        &self,
        owner: NodeId,
        namespace: &str,
    ) -> Result<Option<ManifestRecord>> {
        self.dht.resolve_manifest(owner, namespace).await
    }

    // ---- census + wants → tracker ------------------------------------------------

    async fn nodes(&self) -> Result<Vec<(NodeId, NodePayload)>> {
        self.census.nodes().await
    }
    async fn relays(&self) -> Result<Vec<RelayPayload>> {
        self.census.relays().await
    }
    async fn announce_node_registry(&self, used_bytes: u64, capacity_bytes: u64) -> Result<()> {
        self.census
            .announce_node_registry(used_bytes, capacity_bytes)
            .await
    }
    async fn announce_relay_registry(&self, relay_url: String) -> Result<()> {
        self.census.announce_relay_registry(relay_url).await
    }
    async fn announce_want(&self, cid: Cid) -> Result<()> {
        self.census.announce_want(cid).await
    }
    async fn withdraw_want(&self, cid: Cid) -> Result<()> {
        self.census.withdraw_want(cid).await
    }
    async fn wanted_cids(&self) -> Result<Vec<Cid>> {
        self.census.wanted_cids().await
    }

    // ---- dropped -----------------------------------------------------------------

    /// No network-wide content enumeration under a DHT — the dashboard is local now.
    async fn content(&self) -> Result<Vec<ContentEntry>> {
        Ok(Vec::new())
    }
}
