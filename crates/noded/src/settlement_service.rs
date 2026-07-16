//! `SettlementService` — the **real cross-node epoch-close loop, on the durable sequencer** (not gossip).
//! (ECONOMIC_LAYER_DESIGN.md §10.1; TOKEN_LEDGER_BUILD.md §4d.) It makes the epoch reward RECORD
//! deterministic AND durable by riding the same committee-ordered account-chain substrate the ledger
//! rides — so a record can be re-derived (and verification re-run) from committed state, which an
//! ephemeral gossip board could never support:
//!
//! 1. **Report** — at each epoch boundary, every node authors a `SettleReport { epoch, paid_cumulative,
//!    served_cumulative, proof }` as a COMMITTEE-ORDERED write on ITS OWN settlement chain (owned by the
//!    anchor sentinel → routed to the epoch committee, exactly like a ledger write). `proof` is the
//!    counterparty-signed cheques (one per consumer) that BACK `served_cumulative`. Durable via obj.
//! 2. **Read** — to settle epoch `E`, a node folds each participant's settlement chain to its latest
//!    report with `epoch ≤ E`, VERIFYING the cheque proof (every cheque consumer-signed and naming that
//!    node as server, summing to the claimed `served_cumulative`); an unbacked report is skipped.
//! 3. **Settle** — feed the per-node cumulatives to [`LedgerService::settle_from_board`], which folds each
//!    node's WATERMARK DELTA — `paid_cumulative − paid_watermark` into the pool, `served_cumulative −
//!    served_watermark` as the reward weight — so inflating or replaying cheques earns nothing. Every node
//!    reads the SAME committed reports ⇒ bit-identical record ⇒ a `RewardClaim` resolves the same share
//!    everywhere, and a verifier re-reads the chains to re-run it.
//!
//! **MVP scope (honest):** the participant SET is the converged census, so a momentary census difference
//! can differ the record until it converges (the full-finality version has the committee COMMIT the
//! canonical record as its own write — a follow-on); the proof is carried INLINE (fits the 64 KiB write
//! frame for small networks; large ones want an obj-cid + fetch, or proof compaction); a settle re-scans
//! each chain (a scan-cache is a follow-on); and watermarks are in-memory (persisting them avoids losing
//! one epoch's baseline per restart).

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use zeph_cheque::ServingCheque;
use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::hlc::Clock;
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;

use crate::anchor::AnchorDispatcher;
use crate::cheque::ChequeService;
use crate::ledger::LedgerService;
use crate::sequence::SequenceStore;

/// Epoch length (ms) — MUST match `epoch_committee::EPOCH_MILLIS` so settlement epochs align with the
/// committee that orders the reports.
const EPOCH_MILLIS: u64 = 30_000;
/// How many epochs to wait past an epoch's close before settling it, letting reports commit + propagate.
const SETTLE_GRACE_EPOCHS: u64 = 1;
/// Loop cadence — sub-epoch so boundary crossings are caught promptly (idempotent within an epoch).
const TICK: Duration = Duration::from_secs(5);
/// The synthetic program id of the settlement chain. Its anchor-sentinel owner routes writes to the epoch
/// committee (a network-owned chain, no owner key — same pattern as the ledger).
const SETTLE_PROGRAM_TAG: &[u8] = b"craftec/settlement-chain/1";

/// A node's per-epoch settlement report — authored as a committee-ordered write on its settlement chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleReport {
    pub epoch: u64,
    /// This node's cumulative committed `Pay` total — its contribution to the shared pool (delta-folded).
    pub paid_cumulative: u64,
    /// This node's cumulative cheque-proven bytes served — must equal `Σ proof` (delta-folded as weight).
    pub served_cumulative: u64,
    /// The counterparty-signed cheques (latest per consumer) that PROVE `served_cumulative`.
    pub proof: Vec<ServingCheque>,
}

/// The cheque-proven cumulative bytes `node` has served: each cheque must be consumer-signed AND name
/// `node` as its `server` (so a node can't claim another's earnings); the total is `Σ` the latest
/// cumulative per consumer. `None` if any cheque is invalid or names a different server. This is the
/// anti-farming check — the reward weight can't exceed what counterparties actually signed for.
fn proven_cumulative(node: &[u8; 32], proof: &[ServingCheque]) -> Option<u64> {
    let mut per_consumer: std::collections::BTreeMap<[u8; 32], u64> =
        std::collections::BTreeMap::new();
    for c in proof {
        if &c.server != node || !c.verify() {
            return None; // not this node's earnings, or a forged/invalid cheque
        }
        let e = per_consumer.entry(c.consumer).or_default();
        *e = (*e).max(c.cumulative_bytes); // latest (highest) per consumer
    }
    Some(per_consumer.values().sum())
}

impl SettleReport {
    /// The proof-verified served cumulative for `node`: `Some(served)` iff the cheque proof is valid and
    /// sums to exactly the claimed `served_cumulative` (anti-farming); `None` otherwise.
    fn verified_served(&self, node: &[u8; 32]) -> Option<u64> {
        match proven_cumulative(node, &self.proof) {
            Some(c) if c == self.served_cumulative => Some(c),
            _ => None,
        }
    }
}

pub struct SettlementService {
    identity: Arc<NodeIdentity>,
    clock: Arc<Clock>,
    membership: RwLock<Option<Arc<Membership>>>,
    /// The durable, committee-ordered substrate — authors + reads settlement reports (like the ledger).
    sequence: Arc<SequenceStore>,
    /// Serving measurement + its cheque proof (`total_earned` / `serving_proof`).
    cheques: Arc<ChequeService>,
    /// Pool + settle sink — cumulative `Pay` total (`total_paid`) and the epoch-close settle.
    ledger: Arc<LedgerService>,
    /// The settlement chain's program cid + its anchor-sentinel owner (routes ordering to the committee).
    settle_cid: [u8; 32],
    settle_owner: [u8; 32],
}

impl SettlementService {
    pub fn new(
        identity: Arc<NodeIdentity>,
        clock: Arc<Clock>,
        sequence: Arc<SequenceStore>,
        cheques: Arc<ChequeService>,
        ledger: Arc<LedgerService>,
    ) -> Arc<Self> {
        let settle_cid = Cid::of(SETTLE_PROGRAM_TAG).0;
        let settle_owner = AnchorDispatcher::anchor_owner(&settle_cid);
        Arc::new(Self {
            identity,
            clock,
            membership: RwLock::new(None),
            sequence,
            cheques,
            ledger,
            settle_cid,
            settle_owner,
        })
    }

    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// The current epoch index = `now / EPOCH_MILLIS` (identical derivation to the epoch committee).
    fn epoch(&self) -> u64 {
        self.clock.now().millis() / EPOCH_MILLIS
    }

    /// Author this node's settlement report for `epoch` as a committee-ordered write on its settlement
    /// chain. Returns whether it committed (a quorum of the committee co-signed the ordering).
    async fn report(&self, epoch: u64) -> bool {
        let account = self.identity.node_id().0;
        let paid_cumulative = self.ledger.total_paid();
        let proof = self.cheques.serving_proof();
        // Our own cheques are always valid → the proof justifies our served total by construction.
        let served_cumulative = proven_cumulative(&account, &proof).unwrap_or(0);
        let report = SettleReport {
            epoch,
            paid_cumulative,
            served_cumulative,
            proof,
        };
        let Ok(payload) = postcard::to_allocvec(&report) else {
            return false;
        };
        let nonce = self
            .sequence
            .sequence_of(self.settle_owner, self.settle_cid, account)
            .await
            .map(|s| s.next_nonce())
            .unwrap_or(0);
        let write = SequencedWrite::author(&self.identity, nonce, payload);
        self.sequence
            .sequence(self.settle_owner, self.settle_cid, write)
            .await
    }

    /// Read `account`'s committed cumulatives as of `epoch` — the latest report with `report.epoch ≤
    /// epoch`, with its cheque proof VERIFIED. `None` if the chain is missing, has no in-range report, or
    /// the proof doesn't back the claimed served (an unbacked report earns nothing).
    async fn cumulatives_of(&self, account: [u8; 32], epoch: u64) -> Option<(u64, u64)> {
        let seq = self
            .sequence
            .sequence_of(self.settle_owner, self.settle_cid, account)
            .await?;
        let mut best: Option<SettleReport> = None;
        for nonce in 0..seq.next_nonce() {
            let Some(payload) = seq.payload_at(nonce) else {
                break;
            };
            let Ok(r) = postcard::from_bytes::<SettleReport>(payload) else {
                continue; // a non-report payload → skip
            };
            if r.epoch <= epoch && best.as_ref().is_none_or(|b| r.epoch >= b.epoch) {
                best = Some(r); // latest report at or before E
            }
        }
        let report = best?;
        let served = report.verified_served(&account)?; // proof must back the claim
        Some((report.paid_cumulative, served))
    }

    /// The deterministic participant set: self + every census member (self-included so a single node
    /// settles). Every node with the same converged census derives the same set.
    async fn participants(&self) -> Vec<[u8; 32]> {
        let me = self.identity.node_id().0;
        let mut ids = vec![me];
        if let Some(m) = self.membership.read().await.clone() {
            for (n, _addr) in m.census().await {
                if n.0 != me {
                    ids.push(n.0);
                }
            }
        }
        ids.sort();
        ids
    }

    /// Settle epoch `E` from the durable chains: read each participant's cumulatives as of `E` (proof-
    /// verified) and feed them to the ledger, which folds each node's watermark delta into the record.
    async fn settle(&self, epoch: u64) {
        let mut entries: Vec<([u8; 32], u64, u64)> = Vec::new();
        for account in self.participants().await {
            if let Some((paid_cum, served_cum)) = self.cumulatives_of(account, epoch).await {
                entries.push((account, paid_cum, served_cum));
            }
        }
        self.ledger.settle_from_board(epoch, entries).await;
    }

    /// The epoch-close loop: author this node's report for the just-closed epoch, then settle every epoch
    /// that has passed its grace window (in order) by reading the durable settlement chains.
    pub async fn run(self: Arc<Self>) {
        let mut last_reported: Option<u64> = None;
        let mut last_settled: Option<u64> = None;
        // The cumulatives we last reported — skip re-writing an epoch when nothing changed.
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
            // First tick: baseline the closed/settled epochs so we don't re-report or re-settle the past.
            if !initialized {
                last_reported = Some(now.saturating_sub(1));
                last_settled = Some(now.saturating_sub(1 + SETTLE_GRACE_EPOCHS));
                sent_paid = self.ledger.total_paid();
                sent_served = self.cheques.total_earned();
                initialized = true;
                continue;
            }

            // REPORT this node's cumulatives for the just-closed epoch, once, if a cumulative grew.
            let closed = now - 1;
            if last_reported.is_none_or(|e| e < closed) {
                let paid_now = self.ledger.total_paid();
                let served_now = self.cheques.total_earned();
                if (paid_now > sent_paid || served_now > sent_served) && self.report(closed).await {
                    sent_paid = paid_now;
                    sent_served = served_now;
                }
                last_reported = Some(closed);
            }

            // SETTLE every epoch through its grace window, in order (catch up if we fell behind).
            if now > SETTLE_GRACE_EPOCHS + 1 {
                let target = now - 1 - SETTLE_GRACE_EPOCHS;
                let start = last_settled.map_or(target, |e| e + 1);
                for s in start..=target {
                    self.settle(s).await;
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
        assert_eq!(
            proven_cumulative(&s, &[cheque(&alice, [9u8; 32], 100)]),
            None
        );
        // An empty proof proves exactly zero served.
        assert_eq!(proven_cumulative(&s, &[]), Some(0));
    }

    #[test]
    fn report_verified_served_backs_the_claim_and_rejects_farming() {
        let node = NodeIdentity::generate();
        let n = node.node_id().0;
        let alice = NodeIdentity::generate();
        let proof = vec![cheque(&alice, n, 900)];
        // Honest report: served matches the proof sum.
        let honest = SettleReport {
            epoch: 5,
            paid_cumulative: 42,
            served_cumulative: 900,
            proof: proof.clone(),
        };
        assert_eq!(honest.verified_served(&n), Some(900));
        // THE ANTI-FARM CASE: a report claiming more served than its cheques prove is rejected — even
        // though the write itself is validly authored, the proof (sums to 900) doesn't back 999_999.
        let farmed = SettleReport {
            epoch: 5,
            paid_cumulative: 42,
            served_cumulative: 999_999,
            proof,
        };
        assert_eq!(farmed.verified_served(&n), None);
    }
}
