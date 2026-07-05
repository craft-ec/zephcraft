//! `MembershipPeers` — a `PeerSource` backed by SWIM membership. When content routing moves
//! to the DHT, candidate peers for piece placement come from real-time in-network liveness
//! (the membership active view) instead of the tracker's node registry. The membership
//! handle is injected after construction, since membership is built later than the obj engine.

use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_core::NodeId;
use zeph_membership::Membership;
use zeph_obj::PeerSource;
use zeph_transport::PeerAddr;

pub struct MembershipPeers {
    membership: RwLock<Option<Arc<Membership>>>,
}

impl MembershipPeers {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            membership: RwLock::new(None),
        })
    }

    /// Inject the membership handle once it exists.
    pub async fn set(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }
}

#[async_trait::async_trait]
impl PeerSource for MembershipPeers {
    async fn peers(&self) -> Vec<(NodeId, PeerAddr)> {
        match self.membership.read().await.as_ref() {
            Some(m) => m
                .snapshot()
                .await
                .active
                .into_iter()
                .filter(|(_, ps)| ps.alive)
                .map(|(id, ps)| (id, ps.addr))
                .collect(),
            None => Vec::new(),
        }
    }
}
