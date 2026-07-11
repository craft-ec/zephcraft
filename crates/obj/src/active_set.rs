//! Element 2 (Transfer Plane v2 §2) — the CHOKE: bound active transfer WORK to
//! at most K distinct peers at a time (BitTorrent choke model). Every other
//! known peer is a cheap candidate (an address in membership, zero live cost);
//! the active set rotates as a peer's in-flight work drains or it misbehaves
//! (busy/slow → its caller releases the slot and redirects to the next
//! candidate). Liveness probes and census gossip are NOT gated through this —
//! they live in transport/membership, so they never touch the choke.
//!
//! A peer ALREADY in the active set admits more work for free: one connection
//! per peer (element 1) means extra concurrent streams to it cost nothing new,
//! so only a transfer to a NEW peer waits for a slot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zeph_core::NodeId;

/// A choke gate: at most K DISTINCT peers hold a transfer slot concurrently.
#[derive(Clone)]
pub struct ActiveSet {
    /// One permit per distinct active peer.
    slots: Arc<Semaphore>,
    /// Active peers → their held permit + in-flight refcount. The permit is
    /// retained for as long as the peer has any in-flight transfer work.
    active: Arc<Mutex<HashMap<[u8; 32], Entry>>>,
    /// High-water mark of concurrent DISTINCT active peers (observability: the
    /// harness asserts this stays within K, proving the choke actually bounds
    /// real push traffic and is wired into the path).
    peak: Arc<AtomicUsize>,
}

struct Entry {
    /// Held for the peer's lifetime in the active set; dropped (→ slot freed)
    /// when the last guard for the peer drops.
    _permit: OwnedSemaphorePermit,
    refs: usize,
}

/// Admission guard. Dropping it decrements the peer's in-flight count; the last
/// guard for a peer removes it from the active set and frees its slot so a
/// queued candidate can enter.
pub struct ActiveGuard {
    active: Arc<Mutex<HashMap<[u8; 32], Entry>>>,
    peer: [u8; 32],
}

impl ActiveSet {
    /// A choke bounding active transfer to `k` distinct peers (k is clamped to
    /// at least 1 — a zero cap would deadlock all transfer).
    pub fn new(k: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(k.max(1))),
            active: Arc::new(Mutex::new(HashMap::new())),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Enter the active set for `peer`. Returns immediately if `peer` is already
    /// active (refcount bump — no new slot); otherwise waits until one of the K
    /// slots is free. The returned guard must be held for the duration of the
    /// transfer work and dropped when it completes (or is abandoned).
    pub async fn enter(&self, peer: NodeId) -> ActiveGuard {
        let id = peer.0;
        // Fast path: already active → free admission (no await, no slot).
        {
            let mut active = self.active.lock().expect("active set poisoned");
            if let Some(e) = active.get_mut(&id) {
                e.refs += 1;
                return self.guard(id);
            }
        }
        // New peer: wait for a slot (blocks only if K OTHER peers are active).
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("choke semaphore never closed");
        let mut active = self.active.lock().expect("active set poisoned");
        match active.get_mut(&id) {
            // A concurrent enter() for the SAME peer won the race and already
            // holds the slot — bump its refcount and return ours (dropped below).
            Some(e) => e.refs += 1,
            None => {
                active.insert(
                    id,
                    Entry {
                        _permit: permit,
                        refs: 1,
                    },
                );
                self.peak.fetch_max(active.len(), Ordering::Relaxed);
                return self.guard(id);
            }
        }
        drop(permit); // race case only: return the redundant slot
        self.guard(id)
    }

    /// NON-BLOCKING admission: return a guard if `peer` is already active (free)
    /// or a slot is immediately available; otherwise `None` (the K slots are
    /// full). Callers on coordinator-driven paths (repair/scale/rebalance) use
    /// this so a choked push is DEFERRED (skip this peer, retry next pass) rather
    /// than BLOCKING — a blocked push holds its JobCoordinator slot and starves
    /// the queue (measured: blocking enter() intermittently broke scenario B's
    /// drain bar under mass-rejoin). A deferred peer is just a candidate waiting
    /// its turn, which is exactly the choke model.
    pub fn try_enter(&self, peer: NodeId) -> Option<ActiveGuard> {
        let id = peer.0;
        let mut active = self.active.lock().expect("active set poisoned");
        if let Some(e) = active.get_mut(&id) {
            e.refs += 1; // already active — free
            return Some(self.guard(id));
        }
        match self.slots.clone().try_acquire_owned() {
            Ok(permit) => {
                active.insert(
                    id,
                    Entry {
                        _permit: permit,
                        refs: 1,
                    },
                );
                self.peak.fetch_max(active.len(), Ordering::Relaxed);
                Some(self.guard(id))
            }
            Err(_) => None, // K distinct peers already active → defer
        }
    }

    fn guard(&self, peer: [u8; 32]) -> ActiveGuard {
        ActiveGuard {
            active: self.active.clone(),
            peer,
        }
    }

    /// Distinct peers currently in the active set (observability / tests).
    pub fn active_len(&self) -> usize {
        self.active.lock().expect("active set poisoned").len()
    }

    /// High-water mark of concurrent distinct active peers since construction —
    /// the harness asserts this stayed within K (choke bound held under load)
    /// AND was non-zero (the choke is actually on the push path).
    pub fn peak_active(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut active = self.active.lock().expect("active set poisoned");
        if let Some(e) = active.get_mut(&self.peer) {
            e.refs -= 1;
            if e.refs == 0 {
                active.remove(&self.peer); // drops the permit → frees a slot
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn peer(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    #[tokio::test]
    async fn try_enter_defers_when_full_and_frees_on_drop() {
        let cs = ActiveSet::new(2);
        let a = cs.try_enter(peer(1)).expect("first peer admitted");
        let _b = cs.try_enter(peer(2)).expect("second peer admitted");
        assert_eq!(cs.active_len(), 2);
        // Third distinct peer is DEFERRED (non-blocking None), not queued.
        assert!(cs.try_enter(peer(3)).is_none(), "K=2 full → defer");
        // The SAME peer is always free (refcount bump, no new slot).
        let a2 = cs
            .try_enter(peer(1))
            .expect("already-active peer admitted free");
        assert_eq!(cs.active_len(), 2, "same peer did not consume a new slot");
        // Dropping ONE of peer(1)'s guards keeps it active (refcount 2 → 1).
        drop(a);
        assert!(
            cs.try_enter(peer(3)).is_none(),
            "peer(1) still active, no slot"
        );
        // Dropping its LAST guard frees the slot → a new peer enters.
        drop(a2);
        assert!(
            cs.try_enter(peer(3)).is_some(),
            "slot freed → peer(3) enters"
        );
        assert_eq!(cs.peak_active(), 2, "high-water mark never exceeded K");
    }

    #[tokio::test]
    async fn admits_up_to_k_distinct_peers_then_blocks() {
        let cs = ActiveSet::new(2);
        let _a = cs.enter(peer(1)).await;
        let _b = cs.enter(peer(2)).await;
        assert_eq!(cs.active_len(), 2, "two distinct peers active");

        // A third DISTINCT peer must block while K=2 are active.
        let blocked = tokio::time::timeout(Duration::from_millis(100), cs.enter(peer(3))).await;
        assert!(
            blocked.is_err(),
            "third distinct peer blocks at the K=2 cap"
        );

        // Free a slot → the third peer gets in.
        drop(_a);
        let _c = tokio::time::timeout(Duration::from_millis(500), cs.enter(peer(3)))
            .await
            .expect("third peer enters after a slot frees");
        assert_eq!(cs.active_len(), 2, "still two distinct peers (b + c)");
    }

    #[tokio::test]
    async fn same_peer_is_free_and_refcounted() {
        let cs = ActiveSet::new(1); // one slot — but the same peer is free
        let g1 = cs.enter(peer(1)).await;
        let g2 = tokio::time::timeout(Duration::from_millis(100), cs.enter(peer(1)))
            .await
            .expect("second entry for the SAME peer does not block");
        assert_eq!(cs.active_len(), 1, "same peer occupies one slot");

        drop(g1);
        assert_eq!(
            cs.active_len(),
            1,
            "peer stays active while a guard remains"
        );
        drop(g2);
        assert_eq!(cs.active_len(), 0, "peer leaves when its last guard drops");
    }

    #[tokio::test]
    async fn slot_frees_for_the_next_candidate_on_drain() {
        let cs = ActiveSet::new(1);
        let a = cs.enter(peer(1)).await;
        // B is a candidate waiting behind the single active peer.
        let waiter = {
            let cs = cs.clone();
            tokio::spawn(async move {
                let _b = cs.enter(peer(2)).await;
                cs.active_len()
            })
        };
        // Still blocked.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "candidate waits while the slot is taken"
        );
        // A's work drains → B rotates in.
        drop(a);
        let len = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("candidate rotates in")
            .expect("task ok");
        assert_eq!(len, 1, "exactly one peer active after rotation");
    }
}
