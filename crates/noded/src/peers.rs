//! `MembershipPeers` — a `PeerSource` backed by membership's LIVENESS census
//! (converged members heard within a tight window, minus local dead — NOT the
//! size-5 active view, and NOT the wide 120s election census which would count
//! SWIM-dead holders alive and suppress repair). The census is the designed
//! substrate for the health scan's liveness filter and placement (ZEPHCRAFT §4.2);
//! using the active view here capped both at ~5 peers — providers outside the
//! view were filtered as "dead" (the seed read at_risk=100 for every cid while
//! its peers read ~0, because its true low local count wasn't padded by its own
//! stale high record the way others saw it) and publish/rebalance round-robined
//! over 5 targets in a 20-node cluster (the node-holds-nothing placement skew).
//! Same defect class as the registry's old active-view election ceiling.
//! The membership handle is injected after construction.

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
            Some(m) => m.liveness_census().await,
            None => Vec::new(),
        }
    }
}
