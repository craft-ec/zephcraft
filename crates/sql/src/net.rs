//! Transport-backed CraftSQL page transfer: a reader fetches a DB's page
//! objects (by CID) from the owner over iroh, and the owner serves them from
//! its local page store. This is the network realization of `PageSource` +
//! `RoutingRootStore`'s companion — the actual cross-node page fetch.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_core::{Cid, NodeId};
use zeph_obj::{ConsumeMode, ObjEngine, PeerSource};
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::{DurableStore, ObjectStore, PageSource, Result, SqlError};

/// ALPN for CraftSQL page-object fetch.
pub const ALPN: &[u8] = b"/craftec/sqlpage/1";

/// Generous cap for one object (a 16 KB page, or a root index over many pages).
const MAX_OBJECT: usize = 16 * 1024 * 1024;

/// Serve CraftSQL page objects from `store_dir` to requesters. Protocol: read a
/// 32-byte CID, reply with the object bytes (empty if not held).
pub async fn serve_pages(store_dir: PathBuf, mut conns: mpsc::Receiver<Connection>) {
    while let Some(conn) = conns.recv().await {
        let dir = store_dir.clone();
        tokio::spawn(async move {
            let Ok(store) = ObjectStore::open(&dir) else {
                return;
            };
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(req) = recv.read_to_end(64).await else {
                    return;
                };
                if req.len() == 32 {
                    let mut cid = [0u8; 32];
                    cid.copy_from_slice(&req);
                    let data = store.get(&Cid(cid)).unwrap_or_default();
                    let _ = send.write_all(&data).await;
                }
                let _ = send.finish();
            }
        });
    }
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
        let conn = self
            .transport
            .connect(&addr, ALPN)
            .await
            .map_err(|e| SqlError::Sqlite(format!("connect: {e}")))?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| SqlError::Sqlite(format!("open_bi: {e}")))?;
        send.write_all(&cid.0)
            .await
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        send.finish().map_err(|e| SqlError::Sqlite(e.to_string()))?;
        let data = recv
            .read_to_end(MAX_OBJECT)
            .await
            .map_err(|e| SqlError::Sqlite(e.to_string()))?;
        conn.close(0u32.into(), b"done");
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
