//! Committee-attested epoch records — the canonical FINALITY layer for settlement
//! (ECONOMIC_LAYER_DESIGN.md §10.1; the `committee-attests-record` follow-on). The epoch reward record
//! for `E` is DETERMINISTIC given the durable settlement reports, so the epoch committee doesn't need
//! consensus to agree a value — each committee member computes the record independently and SIGNS it, and
//! `k` matching signatures make it CANONICAL. A node then trusts the attested record without recomputing:
//!
//! - **Durable + restart-safe** — the record travels with its committee signatures; a node that lost its
//!   in-memory pool state (or joined late) reads the attested record instead of replaying pool history.
//! - **Canonical / census-divergence-proof** — the committee pins the ONE record, so two nodes whose
//!   censuses momentarily differ can't disagree on the finalized record (they check it against the same
//!   deterministic committee for `E`).
//!
//! This reuses the [`Quorum`] primitive (the epoch committee IS a `Quorum`) and mirrors the k-of-n
//! signature semantics of the attestation substrate, but over a COMPUTED committee rather than a declared
//! quorum — so it can't ride the declared-quorum `AttestStore` directly.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use zeph_com::Quorum;
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_reward::RewardRecord;

/// Signing domain — binds a committee signature to this message kind (a record attestation).
const RECORD_DOMAIN: &[u8] = b"craftec/epoch-record/1";

/// One committee member's signature attesting an epoch record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeSig {
    pub member: [u8; 32],
    /// ed25519 signature over [`record_signing_bytes`] (`Vec` for serde — no built-in `[u8; 64]` impl).
    pub sig: Vec<u8>,
}

/// A canonical epoch record plus the committee signatures attesting it — the durable finality artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordAttestation {
    pub epoch: u64,
    pub record: RewardRecord,
    pub sigs: Vec<CommitteeSig>,
}

/// One committee member's contribution written to the records chain: the record it computed + its own
/// signature. Readers aggregate a quorum of these (by matching record) into a canonical [`RecordAttestation`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedRecord {
    pub record: RewardRecord,
    pub sig: CommitteeSig,
}

/// The bytes a committee member signs to attest `record` for `epoch`: `domain ‖ epoch ‖ hash(record)`.
/// Hashing the canonical postcard encoding binds the EXACT shares, so a signature covers one record only.
fn record_signing_bytes(epoch: u64, record: &RewardRecord) -> Vec<u8> {
    let record_hash = postcard::to_allocvec(record)
        .map(|b| Cid::of(&b).0)
        .unwrap_or([0u8; 32]);
    let mut m = Vec::with_capacity(RECORD_DOMAIN.len() + 8 + 32);
    m.extend_from_slice(RECORD_DOMAIN);
    m.extend_from_slice(&epoch.to_le_bytes());
    m.extend_from_slice(&record_hash);
    m
}

/// This committee member's signature attesting `record` for `epoch` (it signs only a record it computed
/// itself, so a signature is a genuine independent agreement).
pub fn sign_share(epoch: u64, record: &RewardRecord, identity: &NodeIdentity) -> CommitteeSig {
    CommitteeSig {
        member: identity.node_id().0,
        sig: identity.sign(&record_signing_bytes(epoch, record)).to_vec(),
    }
}

impl RecordAttestation {
    /// Start an attestation from one member's signed share (the proposer's own).
    pub fn new(epoch: u64, record: RewardRecord, first: CommitteeSig) -> Self {
        Self {
            epoch,
            record,
            sigs: vec![first],
        }
    }

    /// Is `s` a valid signature over THIS attestation's exact `(epoch, record)`?
    fn sig_valid(&self, s: &CommitteeSig) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(s.sig.as_slice()) else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(s.member),
            &record_signing_bytes(self.epoch, &self.record),
            &sig,
        )
    }

    /// Add another member's share (dedup by member; rejects an invalid or wrong-record sig). Returns
    /// whether it was added.
    pub fn add_sig(&mut self, s: CommitteeSig) -> bool {
        if self.sigs.iter().any(|x| x.member == s.member) || !self.sig_valid(&s) {
            return false;
        }
        self.sigs.push(s);
        true
    }

    /// CANONICAL iff at least `committee.threshold` DISTINCT `committee` members validly signed this exact
    /// `(epoch, record)`. Signatures from non-members or over a different record don't count.
    pub fn is_canonical(&self, committee: &Quorum) -> bool {
        let mut signers: BTreeSet<[u8; 32]> = BTreeSet::new();
        for s in &self.sigs {
            if committee.is_member(&s.member) && self.sig_valid(s) {
                signers.insert(s.member);
            }
        }
        signers.len() >= committee.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_reward::Share;

    fn record(epoch: u64, provider: u8, amount: u64) -> RewardRecord {
        RewardRecord {
            epoch,
            shares: vec![Share {
                provider: [provider; 32],
                amount,
            }],
        }
    }

    #[test]
    fn threshold_distinct_committee_sigs_make_a_record_canonical() {
        let members: Vec<NodeIdentity> = (0..4).map(|_| NodeIdentity::generate()).collect();
        let ids: Vec<[u8; 32]> = members.iter().map(|m| m.node_id().0).collect();
        let committee = Quorum::genesis(ids.clone(), 3); // 3-of-4
        let rec = record(7, 1, 500);

        // Two sigs → below threshold.
        let mut att = RecordAttestation::new(7, rec.clone(), sign_share(7, &rec, &members[0]));
        assert!(att.add_sig(sign_share(7, &rec, &members[1])));
        assert!(!att.is_canonical(&committee), "2 of 3 is not canonical yet");

        // Third distinct member → canonical.
        assert!(att.add_sig(sign_share(7, &rec, &members[2])));
        assert!(
            att.is_canonical(&committee),
            "3 distinct committee sigs finalize it"
        );
    }

    #[test]
    fn non_members_and_duplicates_and_wrong_records_do_not_count() {
        let members: Vec<NodeIdentity> = (0..4).map(|_| NodeIdentity::generate()).collect();
        let ids: Vec<[u8; 32]> = members.iter().map(|m| m.node_id().0).collect();
        let committee = Quorum::genesis(ids, 3);
        let rec = record(7, 1, 500);

        let mut att = RecordAttestation::new(7, rec.clone(), sign_share(7, &rec, &members[0]));
        // A duplicate from the same member is rejected (no double-count toward threshold).
        assert!(!att.add_sig(sign_share(7, &rec, &members[0])));
        // An outsider's signature is valid crypto but not a committee member → ignored by is_canonical.
        let outsider = NodeIdentity::generate();
        assert!(att.add_sig(sign_share(7, &rec, &outsider)));
        assert!(att.add_sig(sign_share(7, &rec, &members[1])));
        assert!(
            !att.is_canonical(&committee),
            "outsider doesn't count → only 2 committee signers"
        );

        // A signature over a DIFFERENT record can't be smuggled onto this attestation.
        let other = record(7, 9, 999);
        let wrong = sign_share(7, &other, &members[2]);
        assert!(
            !att.add_sig(wrong),
            "a sig over another record fails validation"
        );
    }
}
