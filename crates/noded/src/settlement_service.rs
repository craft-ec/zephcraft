//! `SettlementService` — the **real cross-node epoch-close loop** (ECONOMIC_LAYER_DESIGN.md §10.1;
//! TOKEN_LEDGER_BUILD.md §4d, the production follow-on to the single-node demo). It makes the epoch
//! reward RECORD deterministic network-wide by gossiping a converging per-epoch settlement board:
//!
//! 1. **Announce** — at each epoch boundary, every node signs a `{epoch, paid_cumulative,
//!    served_cumulative, proof}` summary of its OWN lifetime totals, where `proof` is the set of
//!    counterparty-signed cheques (one per consumer) that BACKS `served_cumulative`, and fire-and-forgets
//!    it over `tag::SETTLE`, self-including it.
//! 2. **Collect** — [`serve`](SettlementService::serve) VERIFIES each inbound announcement — the node's
//!    signature AND its cheque proof (every cheque consumer-signed and naming this node as server, summing
//!    to the claimed `served_cumulative`) — then folds it into a per-epoch board (`epoch → node → ann`),
//!    which CONVERGES by gossip exactly like the verification board / membership census.
//! 3. **Settle** — after a grace window (so the board converges), every node deterministically settles the
//!    epoch from the SAME collected set. The store folds each node's WATERMARK DELTA — `paid_cumulative −
//!    paid_watermark` into the pool, `served_cumulative − served_watermark` as the reward weight — so a
//!    node can't farm by inflating or replaying cheques. Same inputs on every node ⇒ bit-identical record
//!    ⇒ a provider's `RewardClaim` resolves the same share everywhere, and verification re-runs it.
//!
//! **MVP scope (honest):** `served` is now cheque-PROVEN (this file), but `paid` is still self-reported
//! (its proof is the committee-ordered `Pay` ledger writes — a cross-check that's a separate slice); a
//! node that misses an epoch's gossip can't reproduce that epoch's record (no anti-entropy pull yet); and
//! the proof is the full per-consumer cheque set, so very large networks want proof COMPACTION later.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use zeph_cheque::ServingCheque;
use zeph_core::hlc::Clock;
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
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
/// Max announcement frame. Carries the cheque proof (one ~150-byte cheque per consumer), so it must be
/// generous; 256 KiB fits ~1700 consumers. Very large networks want proof compaction (a follow-on).
const MAX_SETTLE_FRAME: usize = 256 * 1024;
/// Reject inbound announcements older than this many epochs behind `now` (anti-bloat / anti-replay).
const ACCEPT_WINDOW_EPOCHS: u64 = 64;
/// Signing domain — binds a signature to this message kind so it can't be replayed as another.
const SETTLE_DOMAIN: &[u8] = b"craftec/settle/1";

/// One node's signed per-epoch settlement summary carrying its LIFETIME cumulatives (the network folds
/// per-node deltas at settle time) plus the cheque `proof` that backs `served_cumulative`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementAnnouncement {
    pub epoch: u64,
    pub node: [u8; 32],
    /// This node's cumulative committed `Pay` total — its contribution to the shared pool (delta-folded).
    pub paid_cumulative: u64,
    /// This node's cumulative cheque-proven bytes served — must equal `Σ proof` (delta-folded as reward
    /// weight). Un-farmable: backed by `proof` and only the monotonic delta earns.
    pub served_cumulative: u64,
    /// The counterparty-signed cheques (latest per consumer) that PROVE `served_cumulative`.
    pub proof: Vec<ServingCheque>,
    /// The signing node's ed25519 signature over [`signing_bytes`] (a 64-byte sig; `Vec` for serde, as
    /// `ServingCheque` does — serde has no built-in `[u8; 64]` impl).
    pub sig: Vec<u8>,
}

/// The bytes an announcement's signature covers (binds the node to its claimed cumulatives).
fn signing_bytes(
    epoch: u64,
    node: &[u8; 32],
    paid_cumulative: u64,
    served_cumulative: u64,
) -> Vec<u8> {
    let mut m = Vec::with_capacity(SETTLE_DOMAIN.len() + 8 + 32 + 8 + 8);
    m.extend_from_slice(SETTLE_DOMAIN);
    m.extend_from_slice(&epoch.to_le_bytes());
    m.extend_from_slice(node);
    m.extend_from_slice(&paid_cumulative.to_le_bytes());
    m.extend_from_slice(&served_cumulative.to_le_bytes());
    m
}

/// The cheque-proven cumulative bytes `node` has served: each cheque must be consumer-signed AND name
/// `node` as its `server` (so a node can't claim another's earnings); the total is `Σ` the latest
/// cumulative per consumer. `None` if any cheque is invalid or names a different server. This is the
/// anti-farming check — the reward weight can't exceed what counterparties actually signed for.
fn proven_cumulative(node: &[u8; 32], proof: &[ServingCheque]) -> Option<u64> {
    let mut per_consumer: BTreeMap<[u8; 32], u64> = BTreeMap::new();
    for c in proof {
        if &c.server != node || !c.verify() {
            return None; // not this node's earnings, or a forged/invalid cheque
        }
        let e = per_consumer.entry(c.consumer).or_default();
        *e = (*e).max(c.cumulative_bytes); // latest (highest) per consumer
    }
    Some(per_consumer.values().sum())
}

impl SettlementAnnouncement {
    /// Verify the node signature AND that the cheque proof backs exactly the claimed `served_cumulative`.
    fn verify(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.sig.as_slice()) else {
            return false; // wrong-length signature
        };
        let msg = signing_bytes(
            self.epoch,
            &self.node,
            self.paid_cumulative,
            self.served_cumulative,
        );
        if !NodeIdentity::verify(&NodeId(self.node), &msg, &sig) {
            return false;
        }
        // The proof must justify the claimed served total (anti-farming). An empty proof proves 0 served.
        proven_cumulative(&self.node, &self.proof) == Some(self.served_cumulative)
    }
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

    /// Build this node's `epoch` announcement from its current cumulatives + cheque proof, self-include
    /// it, and gossip it to every census peer. Returns the `(paid_cumulative, served_cumulative)` sent.
    async fn announce(&self, epoch: u64) -> (u64, u64) {
        let node = self.identity.node_id().0;
        let paid_cumulative = self.ledger.total_paid();
        let proof = self.cheques.serving_proof();
        // Our own cheques are always valid → the proof justifies our served total by construction.
        let served_cumulative = proven_cumulative(&node, &proof).unwrap_or(0);
        let sig = self
            .identity
            .sign(&signing_bytes(
                epoch,
                &node,
                paid_cumulative,
                served_cumulative,
            ))
            .to_vec();
        let ann = SettlementAnnouncement {
            epoch,
            node,
            paid_cumulative,
            served_cumulative,
            proof,
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
            return (paid_cumulative, served_cumulative);
        };
        let peers: Vec<(NodeId, PeerAddr)> = m
            .census()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .collect();
        let Ok(bytes) = postcard::to_allocvec(&ann) else {
            return (paid_cumulative, served_cumulative);
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
        (paid_cumulative, served_cumulative)
    }

    /// Settle one epoch deterministically from its converged board and feed the ledger the per-node
    /// cumulatives (`node, paid_cumulative, served_cumulative`) — it folds each node's watermark delta.
    async fn settle(&self, epoch: u64) {
        let entries: Vec<([u8; 32], u64, u64)> = {
            let board = self.board.read().await;
            match board.get(&epoch) {
                Some(anns) => anns
                    .values()
                    .map(|a| (a.node, a.paid_cumulative, a.served_cumulative))
                    .collect(),
                None => Vec::new(),
            }
        };
        self.ledger.settle_from_board(epoch, entries).await;
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
        let mut last_announced: Option<u64> = None;
        let mut last_settled: Option<u64> = None;
        // The cumulatives we last announced — skip re-announcing an epoch when nothing changed.
        let mut sent_paid = 0u64;
        let mut sent_served = 0u64;
        let mut initialized = false;
        let mut ticker = tokio::time::interval(TICK);

        loop {
            ticker.tick().await;
            let now = self.epoch();
            if now == 0 {
                continue;
            }
            // First tick: baseline the closed/settled epochs so we don't re-announce or re-settle the
            // past. Cumulatives are read fresh at each announce (no boundary bookkeeping needed).
            if !initialized {
                last_announced = Some(now.saturating_sub(1));
                last_settled = Some(now.saturating_sub(1 + SETTLE_GRACE_EPOCHS));
                sent_paid = self.ledger.total_paid();
                sent_served = self.cheques.total_earned();
                initialized = true;
                continue;
            }

            // ANNOUNCE this node's current cumulatives (+ cheque proof) for the just-closed epoch, once —
            // but only if a cumulative actually grew (an unchanged announcement folds a zero delta anyway).
            let closed = now - 1;
            if last_announced.is_none_or(|e| e < closed) {
                let paid_now = self.ledger.total_paid();
                let served_now = self.cheques.total_earned();
                if paid_now > sent_paid || served_now > sent_served {
                    let (p, s) = self.announce(closed).await;
                    sent_paid = p;
                    sent_served = s;
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

    /// A cheque from `consumer` acknowledging that `server` served it `cumulative` bytes.
    fn cheque(consumer: &NodeIdentity, server: [u8; 32], cumulative: u64) -> ServingCheque {
        ServingCheque::sign(consumer, server, cumulative, 1)
    }

    #[test]
    fn proven_cumulative_sums_valid_cheques_and_rejects_theft() {
        let server = NodeIdentity::generate();
        let s = server.node_id().0;
        let alice = NodeIdentity::generate();
        let bob = NodeIdentity::generate();
        // Two consumers each acknowledge the server → proven = 100 + 250.
        let proof = vec![cheque(&alice, s, 100), cheque(&bob, s, 250)];
        assert_eq!(proven_cumulative(&s, &proof), Some(350));
        // A cheque naming a DIFFERENT server can't be claimed as this node's earnings.
        let other = [9u8; 32];
        assert_eq!(proven_cumulative(&s, &[cheque(&alice, other, 100)]), None);
        // An empty proof proves exactly zero served.
        assert_eq!(proven_cumulative(&s, &[]), Some(0));
    }

    #[test]
    fn announcement_verifies_with_matching_proof_and_rejects_farming() {
        let node_ident = NodeIdentity::generate();
        let node = node_ident.node_id().0;
        let alice = NodeIdentity::generate();
        let proof = vec![cheque(&alice, node, 900)];
        let (paid, served) = (42u64, 900u64);
        let sig = node_ident
            .sign(&signing_bytes(7, &node, paid, served))
            .to_vec();
        let good = SettlementAnnouncement {
            epoch: 7,
            node,
            paid_cumulative: paid,
            served_cumulative: served,
            proof: proof.clone(),
            sig,
        };
        assert!(
            good.verify(),
            "a signed announcement whose proof backs served verifies"
        );

        // Tampering served after signing breaks the node sig.
        let mut tampered = good.clone();
        tampered.served_cumulative = 999_999;
        assert!(!tampered.verify(), "post-sign inflation fails the sig");

        // THE ANTI-FARM CASE: correctly SIGN an inflated served, but the (unchanged) proof only sums to
        // 900 → the proof doesn't justify the claim, so it's rejected despite a valid signature.
        let sig_big = node_ident
            .sign(&signing_bytes(7, &node, paid, 999_999))
            .to_vec();
        let farmed = SettlementAnnouncement {
            epoch: 7,
            node,
            paid_cumulative: paid,
            served_cumulative: 999_999,
            proof,
            sig: sig_big,
        };
        assert!(
            !farmed.verify(),
            "a validly-signed but cheque-unbacked served is rejected (anti-farming)"
        );
    }
}
