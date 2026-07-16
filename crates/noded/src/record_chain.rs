//! `RecordChain` — the durable transport for committee-attested epoch records (the finality wiring for
//! [`crate::record_attest`]). Each committee member for epoch `E` writes the record IT computed, signed,
//! to its own records chain (a second anchored, committee-ordered chain alongside the settlement chain);
//! any node aggregates the members' shares and, once a quorum signed the SAME record, treats it as the
//! CANONICAL finalized record. Reading it lets a node resolve a reward claim without recomputing pool
//! history (restart-safe) and without trusting its own possibly-divergent census (the committee pins it).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_reward::RewardRecord;

use crate::anchor::AnchorDispatcher;
use crate::epoch_committee::EpochCommitteeSource;
use crate::record_attest::{sign_share, RecordAttestation, SignedRecord};
use crate::sequence::SequenceStore;
use crate::settlement::CLAIM_WINDOW_EPOCHS;

/// The records chain's program id; its anchor-sentinel owner routes ordering to the epoch committee.
const RECORDS_PROGRAM_TAG: &[u8] = b"craftec/settlement-records/1";

/// Scan cache for one member's records chain: the last nonce scanned + its signed records indexed by
/// epoch, so `canonical_record` doesn't re-scan each member's whole chain on every claim resolution.
#[derive(Default)]
struct MemberRecordCache {
    next_nonce: u64,
    by_epoch: HashMap<u64, SignedRecord>,
}

pub struct RecordChain {
    identity: Arc<NodeIdentity>,
    sequence: Arc<SequenceStore>,
    committee: Arc<EpochCommitteeSource>,
    records_cid: [u8; 32],
    records_owner: [u8; 32],
    /// Per-member scan cache of signed records (append-only chain → resume from the last nonce).
    record_scan: Mutex<HashMap<[u8; 32], MemberRecordCache>>,
}

impl RecordChain {
    pub fn new(
        identity: Arc<NodeIdentity>,
        sequence: Arc<SequenceStore>,
        committee: Arc<EpochCommitteeSource>,
    ) -> Arc<Self> {
        let records_cid = Cid::of(RECORDS_PROGRAM_TAG).0;
        let records_owner = AnchorDispatcher::anchor_owner(&records_cid);
        Arc::new(Self {
            identity,
            sequence,
            committee,
            records_cid,
            records_owner,
            record_scan: Mutex::new(HashMap::new()),
        })
    }

    /// If this node is on the committee for `epoch`, sign `record` and author it to this node's records
    /// chain (committee-ordered). A no-op for non-committee nodes or an empty record. Returns whether a
    /// share was written.
    pub async fn attest(&self, epoch: u64, record: &RewardRecord) -> bool {
        if record.shares.is_empty() {
            return false; // nothing to finalize / claim for an empty epoch
        }
        let me = self.identity.node_id().0;
        let Some(committee) = self
            .committee
            .committee_for_epoch(&self.records_cid, epoch)
            .await
        else {
            return false;
        };
        if !committee.is_member(&me) {
            return false; // only committee members attest
        }
        let signed = SignedRecord {
            record: record.clone(),
            sig: sign_share(epoch, record, &self.identity),
        };
        let Ok(payload) = postcard::to_allocvec(&signed) else {
            return false;
        };
        let nonce = self
            .sequence
            .sequence_of(self.records_owner, self.records_cid, me)
            .await
            .map(|s| s.next_nonce())
            .unwrap_or(0);
        let write = SequencedWrite::author(&self.identity, nonce, payload);
        self.sequence
            .sequence(self.records_owner, self.records_cid, write)
            .await
    }

    /// This member's latest signed record for `epoch` on its records chain (`None` if it hasn't attested).
    /// Uses the per-member scan cache: only NEW nonces are parsed (append-only chain), indexed by epoch,
    /// and old epochs beyond the claim window are pruned to bound memory.
    async fn share_of_member(&self, member: [u8; 32], epoch: u64) -> Option<SignedRecord> {
        let seq = self
            .sequence
            .sequence_of(self.records_owner, self.records_cid, member)
            .await?;
        let end = seq.next_nonce();
        // Sync scan of only the NEW nonces under the lock (no await held; committed nonces are immutable).
        let mut cache = self.record_scan.lock().unwrap();
        let entry = cache.entry(member).or_default();
        while entry.next_nonce < end {
            if let Some(payload) = seq.payload_at(entry.next_nonce) {
                if let Ok(sr) = postcard::from_bytes::<SignedRecord>(payload) {
                    entry.by_epoch.insert(sr.record.epoch, sr); // latest attestation for an epoch wins
                }
                entry.next_nonce += 1;
            } else {
                break;
            }
        }
        // Prune epochs older than the claim window — a claim can't resolve against a forfeited record.
        if let Some(&max_ep) = entry.by_epoch.keys().max() {
            let cutoff = max_ep.saturating_sub(CLAIM_WINDOW_EPOCHS + 2);
            entry.by_epoch.retain(|&e, _| e >= cutoff);
        }
        entry.by_epoch.get(&epoch).cloned()
    }

    /// The CANONICAL record for `epoch`: gather each committee member's signed share, GROUP by the record
    /// they signed, and return the record a quorum agreed on. `None` if no committee or no record reached
    /// the threshold (not yet finalized). Robust to a minority signing a divergent record.
    pub async fn canonical_record(&self, epoch: u64) -> Option<RewardRecord> {
        let committee = self
            .committee
            .committee_for_epoch(&self.records_cid, epoch)
            .await?;
        let mut groups: HashMap<[u8; 32], RecordAttestation> = HashMap::new();
        for member in &committee.members {
            let Some(sr) = self.share_of_member(*member, epoch).await else {
                continue;
            };
            let key = match postcard::to_allocvec(&sr.record) {
                Ok(b) => Cid::of(&b).0, // group members by the exact record they signed
                Err(_) => continue,
            };
            match groups.get_mut(&key) {
                Some(att) => {
                    att.add_sig(sr.sig);
                }
                None => {
                    groups.insert(key, RecordAttestation::new(epoch, sr.record, sr.sig));
                }
            }
        }
        groups
            .into_values()
            .find(|att| att.is_canonical(&committee))
            .map(|att| att.record)
    }
}
