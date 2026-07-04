//! The node's internal event bus (foundation §52).
//!
//! A bounded broadcast pub/sub for cross-subsystem coordination: producers
//! `publish` typed [`Event`]s, consumers `subscribe` and filter. It decouples
//! subsystems — publish emits `CidWritten` rather than reaching into an indexer,
//! HealthScan emits `RepairNeeded` rather than calling a coordinator — and it is
//! the substrate a reactive app layer builds on (surface it over the control API
//! and external apps can react without being wired into the kernel).
//!
//! Channels are BOUNDED by design: a slow consumer lags and resumes
//! (`RecvError::Lagged`); it never blocks producers. This is Part I
//! (Coordination), independent of CraftCOM (Part G) — nothing here runs code.

use tokio::sync::broadcast;
use zeph_core::{Cid, NodeId};

/// A node event. Fan-out (broadcast) — every subscriber sees every event and
/// filters for what it cares about.
#[derive(Debug, Clone)]
pub enum Event {
    /// Content was published/written locally (`CID_WRITTEN`).
    CidWritten {
        cid: Cid,
        name: Option<String>,
        pinned: bool,
    },
    /// A CraftSQL commit produced a new root (`PAGE_COMMITTED`).
    PageCommitted { namespace: String, root: Cid },
    /// A peer entered the active view (`PEER_CONNECTED`).
    PeerConnected(NodeId),
    /// A peer left the active view (`PEER_DISCONNECTED`).
    PeerDisconnected(NodeId),
    /// HealthScan found a CID below its durability floor (`REPAIR_NEEDED`).
    RepairNeeded(Cid),
    /// Disk usage crossed the capacity watermark (`DISK_WATERMARK_HIT`).
    DiskWatermarkHit { used: u64, cap: u64 },
    /// The node is shutting down (`SHUTDOWN_SIGNAL`).
    Shutdown,
}

impl Event {
    /// Short category tag — for filtering and the activity feed.
    pub fn tag(&self) -> &'static str {
        match self {
            Event::CidWritten { .. } => "publish",
            Event::PageCommitted { .. } => "commit",
            Event::PeerConnected(_) | Event::PeerDisconnected(_) => "peer",
            Event::RepairNeeded(_) => "repair",
            Event::DiskWatermarkHit { .. } => "disk",
            Event::Shutdown => "shutdown",
        }
    }

    /// The CID this event concerns, hex-encoded — for structured consumers
    /// (e.g. the SSE stream). `None` for events without a CID (peer, disk, …).
    pub fn cid_hex(&self) -> Option<String> {
        match self {
            Event::CidWritten { cid, .. }
            | Event::PageCommitted { root: cid, .. }
            | Event::RepairNeeded(cid) => Some(cid.to_hex()),
            _ => None,
        }
    }

    /// A one-line human-readable description — for logs and the activity feed.
    pub fn describe(&self) -> String {
        let short = |c: &Cid| c.to_hex()[..12].to_string();
        match self {
            Event::CidWritten { cid, name, .. } => {
                format!(
                    "published {} · {}",
                    name.as_deref().unwrap_or("content"),
                    short(cid)
                )
            }
            Event::PageCommitted { namespace, root } => {
                format!("db commit · {namespace} · {}", &root.to_hex()[..12])
            }
            Event::PeerConnected(n) => format!("peer connected · {}", &n.to_hex()[..12]),
            Event::PeerDisconnected(n) => format!("peer disconnected · {}", &n.to_hex()[..12]),
            Event::RepairNeeded(cid) => format!("repair needed · {}", short(cid)),
            Event::DiskWatermarkHit { used, cap } => format!("disk watermark · {used}/{cap}"),
            Event::Shutdown => "shutting down".to_string(),
        }
    }
}

/// Bounded broadcast event bus. Cheaply cloneable; publish never blocks.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<Event>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(256)
    }
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Publish to all current subscribers. No-op if there are none; a full
    /// subscriber queue simply drops the oldest for that subscriber (bounded).
    pub fn publish(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    /// Subscribe. A consumer more than `capacity` events behind gets
    /// `RecvError::Lagged(n)` and resumes at the newest — never blocks producers.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_reaches_subscribers_and_is_bounded() {
        let bus = EventBus::new(4);
        let mut a = bus.subscribe();
        let mut b = bus.subscribe();
        bus.publish(Event::CidWritten {
            cid: Cid::of(b"x"),
            name: Some("x".into()),
            pinned: true,
        });
        // Both subscribers see it (fan-out).
        assert_eq!(a.recv().await.unwrap().tag(), "publish");
        assert_eq!(b.recv().await.unwrap().tag(), "publish");
        // Publishing with no subscribers is a harmless no-op.
        drop(a);
        drop(b);
        bus.publish(Event::Shutdown);
    }
}
