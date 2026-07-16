//! `RecordChain` — the durable transport for committee-attested epoch records (the finality wiring for
//! [`crate::record_attest`]). Each committee member for epoch `E` writes the record IT computed, signed,
//! to its own records chain (a second anchored, committee-ordered chain alongside the settlement chain);
//! any node aggregates the members' shares and, once a quorum signed the SAME record, treats it as the
//! CANONICAL finalized record. Reading it lets a node resolve a reward claim without recomputing pool
//! history (restart-safe) and without trusting its own possibly-divergent census (the committee pins it).

use std::collections::HashMap;
use std::sync::Arc;

use zeph_com::{SequenceBackend, SequencedWrite};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_reward::RewardRecord;

use crate::anchor::AnchorDispatcher;
use crate::epoch_committee::EpochCommitteeSource;
use crate::record_attest::{sign_share, RecordAttestation, SignedRecord};
use crate::sequence::SequenceStore;

/// The records chain's program id; its anchor-sentinel owner routes ordering to the epoch committee.
const RECORDS_PROGRAM_TAG: &[u8] = b"craftec/settlement-records/1";

pub struct RecordChain {
    identity: Arc<NodeIdentity>,
    sequence: Arc<SequenceStore>,
    committee: Arc<EpochCommitteeSource>,
    records_cid: [u8; 32],
    records_owner: [u8; 32],
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
    async fn share_of_member(&self, member: [u8; 32], epoch: u64) -> Option<SignedRecord> {
        let seq = self
            .sequence
            .sequence_of(self.records_owner, self.records_cid, member)
            .await?;
        let mut found = None;
        for nonce in 0..seq.next_nonce() {
            let Some(payload) = seq.payload_at(nonce) else {
                break;
            };
            if let Ok(sr) = postcard::from_bytes::<SignedRecord>(payload) {
                if sr.record.epoch == epoch {
                    found = Some(sr); // latest attestation for this epoch wins
                }
            }
        }
        found
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
