//! `SettlementService` — the **real cross-node epoch-close loop** (ECONOMIC_LAYER_DESIGN.md §10.1;
//! TOKEN_LEDGER_BUILD.md §4d, the production follow-on to the single-node demo). It makes the epoch
//! reward RECORD deterministic network-wide by gossiping a converging per-epoch settlement board:
//!
//! 1. **Announce** — at each epoch boundary, every node signs a `{epoch, paid, served}` summary of its
//!    OWN per-epoch deltas (`paid` = its committed `Pay` total this epoch; `served` = its cheque-proven
//!    bytes served this epoch) and fire-and-forgets it over `tag::SETTLE`, self-including it.
//! 2. **Collect** — [`serve`](SettlementService::serve) verifies each inbound announcement's signature
//!    and folds it into a per-epoch board (`epoch → node → announcement`), which CONVERGES by gossip
//!    exactly like the verification board / membership census.
//! 3. **Settle** — after a grace window (so the board converges), every node deterministically settles
//!    the epoch from the SAME collected set: pool = `Σ paid`, contributions = `{(node, served)}`, fed to
//!    [`LedgerService::settle_from_board`]. Same inputs on every node ⇒ bit-identical record ⇒ a
//!    provider's `RewardClaim` resolves the same share everywhere, and verification re-runs it.
//!
//! **MVP scope (honest):** `served` is self-reported (authenticated, but the counterparty-signed cheque
//! PROOF that backs it isn't yet attached/verified — a node could over-report served bytes); and a node
//! that misses an epoch's announcements can't reproduce that epoch's record (no anti-entropy pull yet).
//! Both are the immediate hardening slices; the loop, its determinism, and its wiring are real here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use zeph_core::hlc::Clock;
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_reward::Contribution;
use zeph_transport::{tag, PeerAddr, TaggedStream, Transport};

use crate::cheque::ChequeService;
use crate::ledger::LedgerService;

/// Epoch length (ms) — MUST match `epoch_committee::EPOCH_MILLIS` so settlement epochs align with the
/// committee that orders the ledger writes.
const EPOCH_MILLIS: u64 = 30_000;
/// How many epochs to wait past an epoch's close before settling it, letting announcements converge by
/// gossip. Epoch `E` settles when the clock reaches `E + 1 + SETTLE_GRACE_EPOCHS`.
const SETTLE_GRACE_EPOCHS: u64 = 1;
/// Loop cadence — sub-epoch so boundary crossings are caught promptly (idempotent within an epoch).
const TICK: Duration = Duration::from_secs(5);
/// A settlement announcement is tiny (fixed fields + a 64-byte sig).
const MAX_SETTLE_FRAME: usize = 4096;
/// Reject inbound announcements older than this many epochs behind `now` (anti-bloat / anti-replay).
const ACCEPT_WINDOW_EPOCHS: u64 = 64;
/// Signing domain — binds a signature to this message kind so it can't be replayed as another.
const SETTLE_DOMAIN: &[u8] = b"craftec/settle/1";

/// One node's signed per-epoch settlement summary. `paid`/`served` are the node's DELTAS over `epoch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementAnnouncement {
    pub epoch: u64,
    pub node: [u8; 32],
    /// This node's committed `Pay` total during `epoch` — its contribution to the shared pool.
    pub paid: u64,
    /// This node's cheque-proven bytes served during `epoch` — its reward-weight contribution.
    pub served: u64,
    /// The signing node's ed25519 signature over [`signing_bytes`] (a 64-byte sig; `Vec` for serde, as
    /// `ServingCheque` does — serde has no built-in `[u8; 64]` impl).
    pub sig: Vec<u8>,
}

/// The bytes an announcement's signature covers.
fn signing_bytes(epoch: u64, node: &[u8; 32], paid: u64, served: u64) -> Vec<u8> {
    let mut m = Vec::with_capacity(SETTLE_DOMAIN.len() + 8 + 32 + 8 + 8);
    m.extend_from_slice(SETTLE_DOMAIN);
    m.extend_from_slice(&epoch.to_le_bytes());
    m.extend_from_slice(node);
    m.extend_from_slice(&paid.to_le_bytes());
    m.extend_from_slice(&served.to_le_bytes());
    m
}

impl SettlementAnnouncement {
    fn verify(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.sig.as_slice()) else {
            return false; // wrong-length signature
        };
        let msg = signing_bytes(self.epoch, &self.node, self.paid, self.served);
        NodeIdentity::verify(&NodeId(self.node), &msg, &sig)
    }
}

/// Aggregate an epoch's collected announcements into the deterministic settlement inputs: the total pool
/// (`Σ paid`) and the reward contributions (`{(node, served)}` for serving nodes). Pure + order-free
/// (`reward::compute` canonicalizes by provider), so every node derives the identical inputs.
fn aggregate(anns: &BTreeMap<[u8; 32], SettlementAnnouncement>) -> (u64, Vec<Contribution>) {
    let pool = anns.values().map(|a| a.paid).sum();
    let contributions = anns
        .values()
        .filter(|a| a.served > 0)
        .map(|a| Contribution {
            provider: a.node,
            bytes: a.served,
        })
        .collect();
    (pool, contributions)
}

pub struct SettlementService {
    identity: Arc<NodeIdentity>,
    clock: Arc<Clock>,
    transport: Arc<Transport>,
    membership: RwLock<Option<Arc<Membership>>>,
    /// Serving measurement source — cumulative cheque-proven bytes served (`total_earned`).
    cheques: Arc<ChequeService>,
    /// Pool + settle sink — cumulative `Pay` total (`total_paid`) and the epoch-close settle.
    ledger: Arc<LedgerService>,
    /// The converging per-epoch board: `epoch → node → announcement`. Written by `serve`/`announce`,
    /// read at settle time. `BTreeMap` inner so aggregation order is stable.
    board: RwLock<BTreeMap<u64, BTreeMap<[u8; 32], SettlementAnnouncement>>>,
}

impl SettlementService {
    pub fn new(
        identity: Arc<NodeIdentity>,
        clock: Arc<Clock>,
        transport: Arc<Transport>,
        cheques: Arc<ChequeService>,
        ledger: Arc<LedgerService>,
    ) -> Arc<Self> {
        Arc::new(Self {
            identity,
            clock,
            transport,
            membership: RwLock::new(None),
            cheques,
            ledger,
            board: RwLock::new(BTreeMap::new()),
        })
    }

    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// The current epoch index = `now / EPOCH_MILLIS` (identical derivation to the epoch committee).
    fn epoch(&self) -> u64 {
        self.clock.now().millis() / EPOCH_MILLIS
    }

    /// Serve inbound announcements (`tag::SETTLE`): verify + fold each into the board. Fire-and-forget
    /// (the sender ignores the reply), mirroring the cheque/board handlers.
    pub async fn serve(self: Arc<Self>, mut streams: mpsc::Receiver<TaggedStream>) {
        while let Some(TaggedStream {
            mut send, mut recv, ..
        }) = streams.recv().await
        {
            let me = self.clone();
            tokio::spawn(async move {
                if let Ok(bytes) = recv.read_to_end(MAX_SETTLE_FRAME).await {
                    if let Ok(ann) = postcard::from_bytes::<SettlementAnnouncement>(&bytes) {
                        me.accept(ann).await;
                    }
                }
                let _ = send.finish();
            });
        }
    }

    /// Record a verified announcement within the acceptance window into the board.
    async fn accept(&self, ann: SettlementAnnouncement) {
        if !ann.verify() {
            return; // forged / corrupt — the sig must match the named node
        }
        let now = self.epoch();
        if ann.epoch + ACCEPT_WINDOW_EPOCHS < now || ann.epoch > now + 2 {
            return; // stale or implausibly future → drop (bounds board growth)
        }
        self.board
            .write()
            .await
            .entry(ann.epoch)
            .or_default()
            .insert(ann.node, ann);
    }

    /// Sign this node's `epoch` summary, self-include it, and gossip it to every census peer.
    async fn announce(&self, epoch: u64, paid: u64, served: u64) {
        let node = self.identity.node_id().0;
        let sig = self
            .identity
            .sign(&signing_bytes(epoch, &node, paid, served))
            .to_vec();
        let ann = SettlementAnnouncement {
            epoch,
            node,
            paid,
            served,
            sig,
        };
        // Self-include so a single node still settles and we always count our own contribution.
        self.board
            .write()
            .await
            .entry(epoch)
            .or_default()
            .insert(node, ann.clone());

        let me = self.identity.node_id();
        let Some(m) = self.membership.read().await.clone() else {
            return;
        };
        let peers: Vec<(NodeId, PeerAddr)> = m
            .census()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .collect();
        let Ok(bytes) = postcard::to_allocvec(&ann) else {
            return;
        };
        let bytes = Arc::new(bytes);
        for (_id, addr) in peers {
            let (t, b) = (self.transport.clone(), bytes.clone());
            tokio::spawn(async move {
                let _ = t
                    .request_tagged(&addr, tag::SETTLE, &b, MAX_SETTLE_FRAME)
                    .await;
            });
        }
    }

    /// Settle one epoch deterministically from its converged board and feed the record to the ledger.
    async fn settle(&self, epoch: u64) {
        let (pool, contributions) = {
            let board = self.board.read().await;
            match board.get(&epoch) {
                Some(anns) => aggregate(anns),
                None => (0, Vec::new()),
            }
        };
        self.ledger
            .settle_from_board(epoch, pool, contributions)
            .await;
    }

    /// Drop board epochs well below the last settled one — settled epochs never re-settle, so a small
    /// keep-window suffices for late arrivals.
    async fn prune_below(&self, target: u64) {
        let cutoff = target.saturating_sub(2);
        self.board.write().await.retain(|&e, _| e >= cutoff);
    }

    /// The epoch-close loop: announce this node's just-closed epoch, then settle every epoch that has
    /// passed its grace window (in order). All loop-local state — no shared mutation but the board.
    pub async fn run(self: Arc<Self>) {
        let mut boundary_served = 0u64;
        let mut boundary_paid = 0u64;
        let mut last_announced: Option<u64> = None;
        let mut last_settled: Option<u64> = None;
        let mut initialized = false;
        let mut ticker = tokio::time::interval(TICK);

        loop {
            ticker.tick().await;
            let now = self.epoch();
            if now == 0 {
                continue;
            }
            // First tick: baseline to the current cumulative + closed epoch so we don't backfill all of
            // history into epoch 0's announcement or re-settle the past. We start fresh next boundary.
            if !initialized {
                boundary_served = self.cheques.total_earned();
                boundary_paid = self.ledger.total_paid();
                last_announced = Some(now.saturating_sub(1));
                last_settled = Some(now.saturating_sub(1 + SETTLE_GRACE_EPOCHS));
                initialized = true;
                continue;
            }

            // ANNOUNCE the just-closed epoch's deltas, once.
            let closed = now - 1;
            if last_announced.is_none_or(|e| e < closed) {
                let served_now = self.cheques.total_earned();
                let paid_now = self.ledger.total_paid();
                let d_served = served_now.saturating_sub(boundary_served);
                let d_paid = paid_now.saturating_sub(boundary_paid);
                boundary_served = served_now;
                boundary_paid = paid_now;
                if d_served > 0 || d_paid > 0 {
                    self.announce(closed, d_paid, d_served).await;
                }
                last_announced = Some(closed);
            }

            // SETTLE every epoch through its grace window, in order (catch up if we fell behind).
            if now > SETTLE_GRACE_EPOCHS + 1 {
                let target = now - 1 - SETTLE_GRACE_EPOCHS;
                let start = last_settled.map_or(target, |e| e + 1);
                for s in start..=target {
                    self.settle(s).await;
                }
                if last_settled.is_none_or(|e| e < target) {
                    self.prune_below(target).await;
                }
                last_settled = Some(target);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ann(node: u8, epoch: u64, paid: u64, served: u64) -> SettlementAnnouncement {
        // Signed by a throwaway identity is unnecessary for the pure-aggregation tests; build directly.
        SettlementAnnouncement {
            epoch,
            node: [node; 32],
            paid,
            served,
            sig: vec![0u8; 64],
        }
    }

    #[test]
    fn aggregate_sums_pool_and_collects_serving_contributions() {
        let mut board = BTreeMap::new();
        board.insert([1u8; 32], ann(1, 5, 30, 100)); // pays 30, serves 100
        board.insert([2u8; 32], ann(2, 5, 70, 0)); // pays 70, serves nothing (pure consumer)
        board.insert([3u8; 32], ann(3, 5, 0, 50)); // pays 0, serves 50 (pure provider)
        let (pool, contribs) = aggregate(&board);
        assert_eq!(pool, 100, "pool = Σ paid = 30 + 70 + 0");
        // Only serving nodes are contributions; the pure consumer is excluded.
        assert_eq!(contribs.len(), 2);
        assert!(contribs
            .iter()
            .any(|c| c.provider == [1u8; 32] && c.bytes == 100));
        assert!(contribs
            .iter()
            .any(|c| c.provider == [3u8; 32] && c.bytes == 50));
        assert!(!contribs.iter().any(|c| c.provider == [2u8; 32]));
    }

    #[test]
    fn announcement_sign_verify_roundtrips_and_rejects_tampering() {
        let id = NodeIdentity::generate();
        let node = id.node_id().0;
        let sig = id.sign(&signing_bytes(7, &node, 42, 900)).to_vec();
        let good = SettlementAnnouncement {
            epoch: 7,
            node,
            paid: 42,
            served: 900,
            sig,
        };
        assert!(good.verify(), "a correctly-signed announcement verifies");
        // Tampering with any signed field breaks verification (can't inflate served after signing).
        let mut forged = good.clone();
        forged.served = 999_999;
        assert!(!forged.verify(), "an inflated served count fails the sig");
        let mut wrong_node = good.clone();
        wrong_node.node = [9u8; 32];
        assert!(!wrong_node.verify(), "a mismatched node fails the sig");
    }
}
