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
//!    node as server, summing to the claimed `served_cumulative`); an unbacked report is skipped. The
//!    reported `paid_cumulative` is likewise CAPPED at the node's actual committee-ordered `Pay` total on
//!    its ledger chain, so neither side of the settlement can be inflated.
//! 3. **Settle** — feed each participant's `(paid_cumulative)` AND its verified per-consumer cheques to
//!    [`EconomyEgressService::settle_from_board`], which applies the PER-CONSUMER SUBSCRIPTION cap
//!    (§10.1 with P6): each epoch a node's `paid_cumulative` delta both funds the pool AND buys it
//!    `delta × bytes_per_token` bytes of egress ENTITLEMENT expiring one governed window (≈30 days) later.
//!    Serving is allocated first-come (by cheque timestamp) against the consumer's unexpired entitlement,
//!    oldest grant first, so `Σ rewarded-for-a-consumer ≤ what its payments entitle` (self-dealing nets ≤
//!    its own payment — the price cancels in pool-average); serving past it is unrewarded subsidy, and
//!    unspent entitlement expires (use-it-or-lose-it, never refunded — those tokens already funded reward).
//!    The pool (Σ paid deltas) is then distributed pool-average over the resulting rewardable bytes.
//!    Per-(pair) served watermarks make replaying cheques earn nothing. Every node reads the SAME committed
//!    reports + cheques and prices with the SAME governed params ⇒ bit-identical record ⇒ a `RewardClaim`
//!    resolves the same share everywhere, and a verifier re-reads the chains to re-run it.
//!
//! **Committee-settles [2026-07-17].** Settling is O(participants × census) chain reads; every node used
//! to pay that EVERY epoch, which is why an IDLE fleet burned the most CPU (an epoch where nothing
//! happened costs the same as a busy one). Now only the epoch's COMMITTEE settles — it must, since it
//! attests the canonical record — plus a deterministic 1-in-[`VERIFY_SAMPLE`] elected sample of everyone
//! else, because re-execution is the ONLY check on a lying committee and a committee verifying its own
//! record proves nothing. Claims are unaffected: `reward_share` resolves from the canonical attested
//! record, which any node reads cheaply.
//!
//! **KNOWN CONSEQUENCE (observability, not correctness):** a node that neither sits on the committee nor
//! is elected for an epoch never builds local settlement state, so its dashboard reads 0 for `reward_owed`
//! / `reward_settled` / `pool` / `subscription_bytes`. Harmless at the current fleet size (committee 4 of
//! 5) but wrong at scale (~12 of 20 nodes). `reward_owed` is derivable from the canonical records; the
//! running `pool` and a consumer's remaining `subscription_bytes` are NOT (they are settle-derived), so
//! surfacing those on a non-settling node needs the record to carry them. Follow-on, tracked.
//!
//! **MVP scope (honest):** the participant SET is the converged census, so a momentary census difference
//! can differ the record until it converges — resolved at finalization by the committee-attested record
//! ([`crate::record_chain`]); and watermarks are in-memory (persisting them avoids losing one epoch's
//! baseline per restart). The cheque proof scales to any network size (INLINE when small, else a durable
//! obj object by cid), and per-settle chain reads are INCREMENTAL (the append-only scan resumes from the
//! last nonce, verifying each proof once — see [`ReportCache`] and `LedgerService::paid_total`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use zeph_cheque::ServingCheque;
use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::hlc::Clock;
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_reward::RewardRecord;

use crate::anchor::AnchorDispatcher;
use crate::cheque::ChequeService;
use crate::economy_egress::EconomyEgressService;
use crate::ledger::LedgerService;
use crate::record_chain::RecordChain;
use crate::sequence::SequenceStore;
use crate::settlement::CLAIM_WINDOW_EPOCHS;

// Settlement epochs come from THE shared definition (`crate::epoch`), so they cannot drift from the
// committee that orders + attests the reports — previously this was a copy of the constant kept in sync
// only by a comment.
use crate::epoch::EPOCH_MILLIS;
/// How many epochs to wait past an epoch's close before settling it, letting reports commit + propagate.
const SETTLE_GRACE_EPOCHS: u64 = 1;
/// Verification SAMPLING: a NON-committee node re-executes 1-in-this-many epochs [2026-07-17].
///
/// Settling is O(participants × census) chain reads, and EVERY node used to pay it EVERY epoch — the bulk
/// of an idle fleet's CPU, since an epoch in which nothing happened costs the same as a busy one. The
/// committee must settle (it attests the canonical record); everyone else settles only to CHECK it.
/// That check must stay INDEPENDENT — a committee verifying its own record proves nothing — but it does
/// not have to be every node every epoch: sampling keeps a lying committee caught with high probability
/// (each epoch still draws several independent re-executions across the fleet) at a fraction of the cost.
const VERIFY_SAMPLE: u64 = 4;
/// Loop cadence — sub-epoch so boundary crossings are caught promptly (idempotent within an epoch).
const TICK: Duration = Duration::from_secs(5);
/// The synthetic program id of the settlement chain. Its anchor-sentinel owner routes writes to the epoch
/// committee (a network-owned chain, no owner key — same pattern as the ledger).
const SETTLE_PROGRAM_TAG: &[u8] = b"craftec/settlement-chain/1";
/// A serialized cheque proof larger than this is published to obj and referenced by cid instead of
/// carried inline — so the settlement write stays well under the 64 KiB sequencer frame regardless of how
/// many consumers a node serves. Small proofs stay inline (no fetch). ~100 cheques.
const INLINE_PROOF_MAX: usize = 16 * 1024;

/// The cheque proof backing a report's `served_cumulative`: carried INLINE when small, or REFERENCED by an
/// obj cid when large (the proof bytes are published as a durable content-addressed object; a verifier
/// fetches by cid — content-addressing binds the exact bytes to the cid the signed report commits to).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServedProof {
    Inline(Vec<ServingCheque>),
    Ref([u8; 32]),
}

/// A node's per-epoch settlement report — authored as a committee-ordered write on its settlement chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleReport {
    pub epoch: u64,
    /// This node's cumulative committed `Pay` total — its contribution to the shared pool (delta-folded).
    pub paid_cumulative: u64,
    /// This node's cumulative cheque-proven bytes served — must equal `Σ proof` (delta-folded as weight).
    pub served_cumulative: u64,
    /// The counterparty-signed cheques (latest per consumer) that PROVE `served_cumulative` — inline or
    /// an obj cid (see [`ServedProof`]).
    pub proof: ServedProof,
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

/// Is `node` ELECTED to independently re-execute `epoch` (1-in-[`VERIFY_SAMPLE`])? Deterministic per
/// (node, epoch) and unpredictable-but-stable — no coordination needed, and it spreads which nodes check
/// which epoch, so every epoch draws several independent verifiers while no node checks them all.
///
/// This gates a SAFETY property (re-execution is the only check on a lying committee), so it is a free
/// function purely to keep it unit-testable: an election that silently never fires would disable
/// verification fleet-wide without failing anything.
fn elected_to_verify(node: &[u8; 32], epoch: u64) -> bool {
    let mut buf = [0u8; 40];
    buf[..32].copy_from_slice(node);
    buf[32..].copy_from_slice(&epoch.to_le_bytes());
    let h = Cid::of(&buf).0;
    u64::from_le_bytes(h[..8].try_into().unwrap()) % VERIFY_SAMPLE == 0
}

/// Scan cache for one account's settlement chain: the last nonce scanned + the PROOF-VERIFIED reports
/// parsed so far (`(report.epoch, paid_cumulative, verified_served, cheques)`), so a settle only processes
/// — and re-fetches/re-verifies the proof of — NEW reports rather than re-scanning the whole chain each
/// epoch. The cheques (this account's serving proof, latest per consumer) feed the per-consumer settle.
#[derive(Default)]
struct ReportCache {
    next_nonce: u64,
    verified: Vec<(u64, u64, u64, Vec<ServingCheque>)>,
}

pub struct SettlementService {
    identity: Arc<NodeIdentity>,
    clock: Arc<Clock>,
    membership: RwLock<Option<Arc<Membership>>>,
    /// The durable, committee-ordered substrate — authors + reads settlement reports (like the ledger).
    sequence: Arc<SequenceStore>,
    /// Serving measurement + its cheque proof (`total_earned` / `serving_proof`).
    cheques: Arc<ChequeService>,
    /// Token ledger — the durable `Pay` total (`paid_total`, read from the ledger chain).
    ledger: Arc<LedgerService>,
    /// Economy-egress policy — the settle sink (`settle_from_board`) + this node's own `local_record`
    /// (P4 split: settlement lives under economy, not the token ledger).
    economy: Arc<EconomyEgressService>,
    /// Committee-attested record finality — this node attests each epoch's record here if it's on the
    /// committee, and claims resolve against the canonical (quorum-signed) record.
    records: Arc<RecordChain>,
    /// Content-addressed store — publishes a LARGE cheque proof as a durable object and fetches a
    /// participant's proof by cid at settle time (small proofs stay inline in the report).
    obj: Arc<ObjEngine>,
    /// Per-account scan cache of proof-verified reports (see [`ReportCache`]) — avoids re-fetching and
    /// re-verifying every participant's whole proof history on every settle.
    report_scan: Mutex<HashMap<[u8; 32], ReportCache>>,
    /// Verification tally: epochs whose CANONICAL committee-attested record matched this node's own
    /// independent re-execution (`verified`) vs diverged (`mismatched`) — the correctness-by-re-execution
    /// audit, observable via [`verification_stats`](Self::verification_stats).
    verified: AtomicU64,
    mismatched: AtomicU64,
    /// The settlement chain's program cid + its anchor-sentinel owner (routes ordering to the committee).
    settle_cid: [u8; 32],
    settle_owner: [u8; 32],
}

impl SettlementService {
    #[allow(clippy::too_many_arguments)] // a settlement loop with many node-service deps
    pub fn new(
        identity: Arc<NodeIdentity>,
        clock: Arc<Clock>,
        sequence: Arc<SequenceStore>,
        cheques: Arc<ChequeService>,
        ledger: Arc<LedgerService>,
        economy: Arc<EconomyEgressService>,
        records: Arc<RecordChain>,
        obj: Arc<ObjEngine>,
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
            economy,
            records,
            obj,
            report_scan: Mutex::new(HashMap::new()),
            verified: AtomicU64::new(0),
            mismatched: AtomicU64::new(0),
            settle_cid,
            settle_owner,
        })
    }

    /// `(verified, mismatched)` epoch counts — the running result of the re-execution verification loop.
    pub fn verification_stats(&self) -> (u64, u64) {
        (
            self.verified.load(Ordering::Relaxed),
            self.mismatched.load(Ordering::Relaxed),
        )
    }

    /// VERIFY epoch `E` by re-execution: compare this node's OWN computed record (its independent settle)
    /// against the CANONICAL committee-attested record. A match = the finalized record is confirmed by an
    /// independent re-run (open verification, esp. from non-committee nodes); a mismatch is flagged. No-op
    /// until the epoch is finalized (canonical available) and this node has computed its own record.
    async fn verify_epoch(&self, epoch: u64) {
        let Some(canonical) = self.records.canonical_record(epoch).await else {
            return; // not finalized yet — nothing to verify against
        };
        let Some(local) = self.economy.local_record(epoch).await else {
            return; // this node didn't settle E locally (e.g., joined late)
        };
        if local == canonical {
            self.verified.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(epoch, "settlement record verified (matches canonical)");
        } else {
            self.mismatched.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                epoch,
                local_shares = local.shares.len(),
                canonical_shares = canonical.shares.len(),
                "settlement record MISMATCH: local re-execution diverges from the canonical record"
            );
        }
    }

    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// Is this node ELECTED to independently re-execute `epoch`? See [`elected_to_verify`].
    fn elected_to_verify(&self, epoch: u64) -> bool {
        elected_to_verify(&self.identity.node_id().0, epoch)
    }

    /// Should this node settle `epoch` at all? The committee MUST (it attests the canonical record);
    /// everyone else settles only the epochs they are elected to verify — that is the only reason a
    /// non-committee node needs the record, and it is what makes the check independent.
    async fn should_settle(&self, epoch: u64) -> bool {
        self.records.is_committee_for(epoch).await || self.elected_to_verify(epoch)
    }

    /// The current epoch index = `now / EPOCH_MILLIS` (identical derivation to the epoch committee).
    pub fn epoch(&self) -> u64 {
        self.clock.now().millis() / EPOCH_MILLIS
    }

    /// Author this node's settlement report for `epoch` as a committee-ordered write on its settlement
    /// chain. Returns whether it committed (a quorum of the committee co-signed the ordering).
    async fn report(&self, epoch: u64) -> bool {
        let account = self.identity.node_id().0;
        // Paid from the DURABLE ledger `Pay` chain (not the in-memory counter), so a reconstructed node
        // reports its true cumulative rather than 0 after data loss.
        let paid_cumulative = self.ledger.paid_total(account).await;
        let cheques = self.cheques.serving_proof();
        // Our own cheques are always valid → the proof justifies our served total by construction.
        let served_cumulative = proven_cumulative(&account, &cheques).unwrap_or(0);
        // Carry the proof inline if small; otherwise publish it as a durable content-addressed object and
        // reference it by cid — so a node serving many consumers doesn't overflow the sequencer frame.
        let proof = match postcard::to_allocvec(&cheques) {
            Ok(bytes) if bytes.len() > INLINE_PROOF_MAX => {
                match self.obj.publish_system(&bytes).await {
                    Ok(cid) => ServedProof::Ref(cid.0),
                    Err(_) => ServedProof::Inline(cheques), // publish failed → try inline (may reject if huge)
                }
            }
            _ => ServedProof::Inline(cheques),
        };
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

    /// Read `account`'s committed cumulatives as of `epoch` — the latest PROOF-VERIFIED report with
    /// `report.epoch ≤ epoch`. Uses the scan cache: only reports newer than the last scan are parsed and
    /// their proofs fetched/verified (each proof exactly once); a transient proof-fetch failure stops the
    /// scan so that report is retried next settle rather than permanently skipped. `None` if the chain is
    /// missing or no verified report is at/before `epoch`.
    async fn cumulatives_of(
        &self,
        account: [u8; 32],
        epoch: u64,
    ) -> Option<(u64, Vec<ServingCheque>)> {
        let seq = self
            .sequence
            .sequence_of(self.settle_owner, self.settle_cid, account)
            .await?;
        let end = seq.next_nonce();
        let start = self
            .report_scan
            .lock()
            .unwrap()
            .get(&account)
            .map_or(0, |c| c.next_nonce);

        // Scan the NEW nonces, resolving + verifying each report's proof once (no lock held over the fetch).
        let mut fresh: Vec<(u64, u64, u64, Vec<ServingCheque>)> = Vec::new();
        let mut advanced = start;
        let mut n = start;
        while n < end {
            let Some(payload) = seq.payload_at(n) else {
                break;
            };
            let Ok(report) = postcard::from_bytes::<SettleReport>(payload) else {
                n += 1;
                advanced = n; // a non-report payload → skip permanently
                continue;
            };
            // Ok(Some) = cheques; Ok(None) = permanently invalid proof; Err = TRANSIENT fetch failure.
            let resolved: Result<Option<Vec<ServingCheque>>, ()> = match &report.proof {
                ServedProof::Inline(c) => Ok(Some(c.clone())),
                ServedProof::Ref(cid) => match self.obj.get(Cid(*cid), ConsumeMode::Drop).await {
                    Ok(bytes) => Ok(postcard::from_bytes::<Vec<ServingCheque>>(&bytes).ok()),
                    Err(_) => Err(()),
                },
            };
            match resolved {
                Err(()) => break, // don't advance past a transient failure — retry it next settle
                Ok(maybe) => {
                    if let Some(cheques) = maybe {
                        if proven_cumulative(&account, &cheques) == Some(report.served_cumulative) {
                            fresh.push((
                                report.epoch,
                                report.paid_cumulative,
                                report.served_cumulative,
                                cheques,
                            ));
                        }
                        // else: proof doesn't back the claim → permanently invalid, don't cache (anti-farming)
                    }
                    n += 1;
                    advanced = n;
                }
            }
        }

        // Merge fresh results into the cache (guard against a concurrent advance; settle is sequential).
        let (paid_cum, cheques) = {
            let mut cache = self.report_scan.lock().unwrap();
            let entry = cache.entry(account).or_default();
            if entry.next_nonce == start {
                entry.verified.extend(fresh);
                entry.next_nonce = advanced;
            }
            entry
                .verified
                .iter()
                .filter(|(ep, _, _, _)| *ep <= epoch)
                .max_by_key(|(ep, _, _, _)| *ep)
                .map(|(_, p, _, ch)| (*p, ch.clone()))?
        };
        // PAID proof: cap the reported paid at the node's ACTUAL committee-ordered `Pay` total (durable on
        // its ledger chain), so it can't inflate the pool beyond what it really paid.
        let paid = paid_cum.min(self.ledger.paid_total(account).await);
        Some((paid, cheques))
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
    /// verified), feed them to the ledger (which folds each node's watermark delta into the record), then
    /// — if this node is on the committee — ATTEST the resulting record to the records chain, so a quorum
    /// of matching signatures finalizes the canonical record other nodes resolve claims against.
    async fn settle(&self, epoch: u64) {
        let record = self.settle_epoch_state(epoch).await;
        self.records.attest(epoch, &record).await;
    }

    /// Fold epoch `E`'s durable reports into the settlement store (the state half of `settle`, WITHOUT
    /// attesting). Used both by the live loop and by startup RECONSTRUCTION — replaying the durable chains
    /// to rebuild the in-memory watermarks/pool/records after a restart (even total local data loss),
    /// since the store is a deterministic function of the committed reports. Returns the epoch record.
    async fn settle_epoch_state(&self, epoch: u64) -> RewardRecord {
        let mut paid: Vec<([u8; 32], u64)> = Vec::new();
        let mut cheques: Vec<([u8; 32], [u8; 32], u64, u64)> = Vec::new();
        for account in self.participants().await {
            if let Some((paid_cum, chs)) = self.cumulatives_of(account, epoch).await {
                paid.push((account, paid_cum));
                for c in chs {
                    cheques.push((c.server, c.consumer, c.cumulative_bytes, c.timestamp));
                }
            }
        }
        self.economy.settle_from_board(epoch, paid, cheques).await
    }

    /// Reconstruct the in-memory settlement state on startup by replaying the last `CLAIM_WINDOW_EPOCHS`
    /// of durable reports up to `through` (inclusive) — folding state only, NOT re-attesting or re-writing
    /// (the reports + canonical records already exist on the chains). Bounded by the claim window: older
    /// records have forfeited, so genesis replay is never needed.
    async fn reconstruct_through(&self, through: u64) {
        let start = through.saturating_sub(CLAIM_WINDOW_EPOCHS);
        // Seed cumulative issuance from the epoch immediately BEFORE the replay window, so the seed and
        // the replay are contiguous and never overlap.
        //
        // Seeding from the most RECENT record instead would double-count: `cumulative_issued` includes
        // that epoch's own issuance, and the window below almost always contains that same epoch, so the
        // replay would mint it a second time on top of a seed that already had it. That inflates the
        // counter (supply-safe) but makes this node compute a DIFFERENT record from every node that did
        // not just restart — breaking the re-execution equality that record verification depends on, and
        // risking a stalled epoch if several committee members restart together. Deriving the seed from
        // the same `start` the loop uses makes the two boundaries correct by construction.
        if start > 0 {
            if let Some(prev) = self.records.canonical_record(start - 1).await {
                // Restore the COMPLETE economic position — token side and reward side — from the durable,
                // committee-attested record, not merely the issuance counter. Everything in that snapshot
                // was previously in-memory only, so a restart silently handed every account a fresh
                // seeding allowance, forgot every token the pool held, dropped payments made while the
                // node was down, and could re-debit an already-claimed share.
                // The state lives in the economic STORE now, not in the record — the record carries
                // only a commitment. So load locally and CHECK it against that commitment: matching
                // means this node holds the state the network agreed on; mismatching means it diverged,
                // which must surface rather than be silently adopted.
                if !self
                    .economy
                    .load_and_verify_economic_state(prev.state_hash)
                    .await
                {
                    tracing::warn!(
                        epoch = start - 1,
                        "could not adopt a verified economic state; continuing from current"
                    );
                }
            }
        }
        for e in start..=through {
            self.settle_epoch_state(e).await;
        }
    }

    /// Rebuild this node's cheque book from its OWN latest durable settlement report's proof — the cheque
    /// set the node published (inline or as a durable obj object) is its serving evidence, so a node that
    /// lost all local data recovers its earnings from the network instead of from local disk. `record`
    /// merges (keeps the highest per consumer), so this is safe even if a few fresh cheques already arrived.
    async fn reconstruct_cheque_book(&self) {
        let account = self.identity.node_id().0;
        let Some(seq) = self
            .sequence
            .sequence_of(self.settle_owner, self.settle_cid, account)
            .await
        else {
            return;
        };
        // The latest report (highest nonce) carries the most-complete cheque set.
        let mut latest: Option<SettleReport> = None;
        for nonce in 0..seq.next_nonce() {
            if let Some(payload) = seq.payload_at(nonce) {
                if let Ok(r) = postcard::from_bytes::<SettleReport>(payload) {
                    latest = Some(r);
                }
            }
        }
        let Some(report) = latest else {
            return; // never reported → nothing to recover
        };
        let cheques = match &report.proof {
            ServedProof::Inline(c) => c.clone(),
            ServedProof::Ref(cid) => match self.obj.get(Cid(*cid), ConsumeMode::Drop).await {
                Ok(bytes) => postcard::from_bytes::<Vec<ServingCheque>>(&bytes).unwrap_or_default(),
                Err(_) => return, // proof not fetchable → skip (consumers re-send cumulative cheques)
            },
        };
        self.cheques.load_cheques(cheques);
    }

    /// The epoch-close loop: author this node's report for the just-closed epoch, then settle every epoch
    /// that has passed its grace window (in order) by reading the durable settlement chains.
    pub async fn run(self: Arc<Self>) {
        let mut last_reported: Option<u64> = None;
        let mut last_settled: Option<u64> = None;
        let mut last_verified: Option<u64> = None;
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
            // First tick: RECONSTRUCT the in-memory settlement state by replaying the claim-window of
            // durable reports — so a node that restarted (even one that lost all local data) resumes with
            // the correct watermarks/pool/records rather than re-baselining from empty. Then baseline the
            // closed/settled cursors past the replayed window.
            if !initialized {
                // Recover this node's cheque book (earnings evidence) from its own durable report, then
                // rebuild the settlement state — both from the network, so total data loss is survivable.
                self.reconstruct_cheque_book().await;
                let settled_through = now.saturating_sub(1 + SETTLE_GRACE_EPOCHS);
                self.reconstruct_through(settled_through).await;
                last_reported = Some(now.saturating_sub(1));
                last_settled = Some(settled_through);
                last_verified = Some(settled_through);
                // Our own reported cumulatives come from durable sources (the ledger `Pay` chain; served
                // from the cheque book, made durable separately) so they're correct after data loss.
                sent_paid = self.ledger.paid_total(self.identity.node_id().0).await;
                sent_served = self.cheques.total_earned();
                initialized = true;
                continue;
            }

            // REPORT this node's cumulatives for the just-closed epoch, once, if a cumulative grew. Paid is
            // sourced from the durable ledger chain (survives data loss); served from the cheque book.
            let closed = now - 1;
            if last_reported.is_none_or(|e| e < closed) {
                let paid_now = self.ledger.paid_total(self.identity.node_id().0).await;
                let served_now = self.cheques.total_earned();
                if (paid_now > sent_paid || served_now > sent_served) && self.report(closed).await {
                    sent_paid = paid_now;
                    sent_served = served_now;
                }
                last_reported = Some(closed);
            }

            // SETTLE every epoch through its grace window, in order (catch up if we fell behind) — but
            // only the epochs THIS node has a reason to compute: it is on the committee (must attest), or
            // it is elected to independently verify. Before this gate every node re-derived every epoch,
            // which is what made an IDLE fleet expensive (see `should_settle` / `VERIFY_SAMPLE`).
            if now > SETTLE_GRACE_EPOCHS + 1 {
                let target = now - 1 - SETTLE_GRACE_EPOCHS;
                let start = last_settled.map_or(target, |e| e + 1);
                let mut advanced = false;
                for s in start..=target {
                    if self.should_settle(s).await {
                        self.settle(s).await;
                        advanced = true;
                    } else if self.economy.adopt_canonical_state(s).await {
                        // NOT elected for this epoch — so ADOPT its canonical state rather than skipping
                        // it. Now that the pool derives from the chain, a node has no business computing
                        // its own: every node tracks the same state whether it settled or merely read.
                        //
                        // Skipping used to leave a non-settling node's economic position frozen at its
                        // last elected epoch, which then failed the restart hash-check against a newer
                        // canonical record and fell back to a ZERO baseline — wiping subsidy eligibility
                        // and resurrecting "restart refreshes your subsidy" for the third time.
                        advanced = true;
                    }
                }
                // PERSIST whenever the position advanced — by settling OR by adopting. Gating this on
                // settling alone is what made most nodes' on-disk state stale and unverifiable.
                if advanced {
                    if let Err(e) = self.economy.persist_economic_state().await {
                        tracing::warn!(error = %e, "failed to persist economic state");
                    }
                }
                last_settled = Some(target);
            }

            // VERIFY: one epoch behind the settle target (giving the committee time to finalize the
            // canonical record), re-execution-check our own record against the canonical one. A no-op
            // unless we actually settled that epoch above (`verify_epoch` needs a local record), so this
            // naturally runs on exactly the elected sample.
            if now > SETTLE_GRACE_EPOCHS + 2 {
                let vtarget = now - 2 - SETTLE_GRACE_EPOCHS;
                let start = last_verified.map_or(vtarget, |e| e + 1);
                for e in start..=vtarget {
                    self.verify_epoch(e).await;
                }
                last_verified = Some(vtarget);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_election_is_deterministic_and_samples_at_the_expected_rate() {
        let node = [7u8; 32];
        // Deterministic: the same (node, epoch) always elects the same way — a verifier can't flip-flop.
        for e in 0..50u64 {
            assert_eq!(elected_to_verify(&node, e), elected_to_verify(&node, e));
        }
        // It FIRES: an election that never fires would silently disable verification fleet-wide.
        let hits = (0..4000u64)
            .filter(|e| elected_to_verify(&node, *e))
            .count();
        assert!(hits > 0, "election never fires → nothing would ever verify");
        // ~1-in-VERIFY_SAMPLE (generous bounds — this is a hash, not a counter).
        let expected = 4000 / VERIFY_SAMPLE as usize;
        assert!(
            hits > expected / 2 && hits < expected * 2,
            "sampling rate off: {hits} hits vs ~{expected} expected"
        );
    }

    #[test]
    fn every_epoch_draws_verifiers_and_no_node_verifies_them_all() {
        // Across a fleet, each epoch must draw independent re-executions (else a lying committee goes
        // unchecked), while no single node carries the whole cost.
        let fleet: Vec<[u8; 32]> = (0..20u8).map(|i| [i; 32]).collect();
        let mut unverified = 0;
        for epoch in 0..200u64 {
            let verifiers = fleet.iter().filter(|n| elected_to_verify(n, epoch)).count();
            if verifiers == 0 {
                unverified += 1;
            }
        }
        // With 20 nodes at 1-in-4, an epoch drawing ZERO verifiers should be vanishingly rare.
        assert!(
            unverified < 5,
            "{unverified}/200 epochs had no verifier — the committee would be unchecked"
        );
        // And no node is elected for everything (the cost is spread).
        for n in &fleet {
            let mine = (0..200u64).filter(|e| elected_to_verify(n, *e)).count();
            assert!(
                mine < 200,
                "a node elected for every epoch defeats the sampling"
            );
        }
    }

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
    fn proof_must_back_the_claimed_served_regardless_of_inline_or_ref() {
        // `cumulatives_of` accepts a report's served ONLY if the resolved cheques prove exactly it; this
        // is that check (the proof source — inline vs a fetched obj-ref — resolves to the same cheque set).
        let node = NodeIdentity::generate();
        let n = node.node_id().0;
        let alice = NodeIdentity::generate();
        let cheques = vec![cheque(&alice, n, 900)];
        // Honest: the proof sums to exactly the claimed served.
        assert_eq!(proven_cumulative(&n, &cheques), Some(900));
        // THE ANTI-FARM CASE: claiming 999_999 with a proof that sums to 900 fails the `== served` check.
        let claimed = 999_999u64;
        assert_ne!(proven_cumulative(&n, &cheques), Some(claimed));
        // A large proof round-trips through the Ref variant's serialization the same as inline.
        let inline = ServedProof::Inline(cheques.clone());
        let bytes = postcard::to_allocvec(&inline).unwrap();
        assert!(matches!(
            postcard::from_bytes::<ServedProof>(&bytes).unwrap(),
            ServedProof::Inline(_)
        ));
        let refv = ServedProof::Ref([7u8; 32]);
        let rb = postcard::to_allocvec(&refv).unwrap();
        assert!(matches!(
            postcard::from_bytes::<ServedProof>(&rb).unwrap(),
            ServedProof::Ref(c) if c == [7u8; 32]
        ));
    }
}
