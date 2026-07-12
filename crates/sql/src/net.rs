//! Transport-backed CraftSQL page transfer: a reader fetches a DB's page
//! objects (by CID) from the owner over iroh, and the owner serves them from
//! its local page store. This is the network realization of `PageSource` — the
//! `RootStore`'s companion, the actual cross-node page fetch.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_core::{Cid, NodeId};
use zeph_obj::{ConsumeMode, ObjEngine, PeerSource};
use zeph_transport::{tag, PeerAddr, TaggedStream, Transport};

use crate::{DurableStore, PageSource, Result, SqlError};

/// ALPN for CraftSQL page-object fetch.
pub const ALPN: &[u8] = b"/craftec/sqlpage/1";

/// Generous cap for one object (a 16 KB page, or a root index over many pages).
const MAX_OBJECT: usize = 16 * 1024 * 1024;

/// Serve CraftSQL page objects from `store_dir` to requesters. Protocol: read a
/// 32-byte CID, reply with the object bytes (empty if not held). Streams on a
/// peer's pooled connection are served with bounded pipelining — a recovering
/// DB fetches many pages back-to-back and serial handling made that N round
/// trips instead of a pipeline.
pub async fn serve_pages(store_dir: PathBuf, mut streams: mpsc::Receiver<TaggedStream>) {
    // Muxed: one tagged stream per page fetch. The fetch protocol is CID-only (it can't name the
    // owning DB), and pages now live in per-DB subdirs (`store_dir/<key>/`), so serve by SEARCHING
    // the store tree for the content-addressed object — see `find_object`.
    let store_dir = std::sync::Arc::new(store_dir);
    while let Some(TaggedStream {
        mut send, mut recv, ..
    }) = streams.recv().await
    {
        let store_dir = store_dir.clone();
        tokio::spawn(async move {
            let Ok(req) = recv.read_to_end(64).await else {
                return;
            };
            if req.len() == 32 {
                let mut cid = [0u8; 32];
                cid.copy_from_slice(&req);
                let data = find_object(&store_dir, &Cid(cid)).unwrap_or_default();
                let _ = send.write_all(&data).await;
            }
            let _ = send.finish();
        });
    }
}

/// Find a page object by CID anywhere in the store tree: the legacy flat root first, then each
/// per-DB subdir (`store_dir/<key>/`). The object is content-addressed, so any copy is
/// authoritative; a CID-only fetch can't name the owning DB, so the server searches. Cheap — one
/// stat on the flat path, then a bounded scan of the per-DB subdirs (one per hosted DB).
fn find_object(store_dir: &std::path::Path, cid: &Cid) -> Option<Vec<u8>> {
    let hex = cid.to_hex();
    let rel = std::path::Path::new(&hex[0..2]).join(&hex);
    // Legacy flat root first (`store_dir/<2hex>/<hex>`).
    if let Ok(b) = std::fs::read(store_dir.join(&rel)) {
        return Some(b);
    }
    // Then each per-DB subdir. Keys are `<owner16>_<ns>` (len >= 18), so skip the 2-char legacy
    // object-shard dirs already covered by the flat read above.
    let rd = std::fs::read_dir(store_dir).ok()?;
    for e in rd.flatten() {
        let name = e.file_name();
        if name.to_string_lossy().len() == 2 {
            continue;
        }
        let p = e.path();
        if p.is_dir() {
            if let Ok(b) = std::fs::read(p.join(&rel)) {
                return Some(b);
            }
        }
    }
    None
}

/// Fetches CraftSQL page objects from a DB owner over the transport (resolving
/// the owner's dial address from the live peer source).
pub struct TransportPageSource {
    transport: Arc<Transport>,
    peers: Arc<dyn PeerSource>,
}

impl TransportPageSource {
    pub fn new(transport: Arc<Transport>, peers: Arc<dyn PeerSource>) -> Self {
        Self { transport, peers }
    }

    async fn owner_addr(&self, owner: NodeId) -> Option<PeerAddr> {
        self.peers
            .peers()
            .await
            .into_iter()
            .find(|(id, _)| *id == owner)
            .map(|(_, addr)| addr)
    }
}

#[async_trait::async_trait]
impl PageSource for TransportPageSource {
    async fn fetch(&self, owner: NodeId, cid: Cid) -> Result<Option<Vec<u8>>> {
        let addr = self.owner_addr(owner).await.ok_or_else(|| {
            SqlError::Sqlite(format!("owner {} not among live peers", owner.to_hex()))
        })?;
        // Muxed page fetch (tag::SQLPAGE): request is the bare 32-byte CID, the
        // reply is the object bytes (empty = not held). request_tagged evicts
        // the mux connection on a stream failure so the next fetch re-dials.
        let data = self
            .transport
            .request_tagged(&addr, tag::SQLPAGE, &cid.0, MAX_OBJECT)
            .await
            .map_err(|e| SqlError::Sqlite(format!("sqlpage fetch: {e}")))?;
        Ok(if data.is_empty() { None } else { Some(data) })
    }
}

/// `DurableStore` over the CraftOBJ engine: a generation is published as content
/// (erasure-coded k=8/n=32, distributed, HealthScan-repaired) and reconstructed
/// from any k pieces — DB pages get exactly the durability files have.
pub struct ObjDurable {
    engine: std::sync::Arc<ObjEngine>,
}

impl ObjDurable {
    pub fn new(engine: std::sync::Arc<ObjEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl DurableStore for ObjDurable {
    async fn put_generation(&self, blob: Vec<u8>) -> Result<Cid> {
        self.engine
            .publish_system(&blob)
            .await
            .map_err(|e| SqlError::Sqlite(format!("publish generation: {e}")))
    }

    async fn get_generation(&self, cid: Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.engine.get(cid, ConsumeMode::Seed).await.ok())
    }

    async fn drop_generation(&self, cid: Cid) -> Result<usize> {
        self.engine
            .release_system(cid)
            .await
            .map_err(|e| SqlError::Sqlite(format!("release generation: {e}")))
    }
}
