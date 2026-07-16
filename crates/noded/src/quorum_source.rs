//! `QuorumSource` — the sequencer's pluggable quorum provider (TOKEN_LEDGER_BUILD.md §7;
//! ECONOMIC_LAYER_DESIGN.md §10.5). The sequencer needs a k-of-n [`Quorum`] per `(owner, program)`;
//! this abstracts WHERE it comes from — the third extension axis of the attestation substrate (the
//! same `Quorum` primitive, a new *provenance*):
//!
//! - **User programs → their DECLARED, owner-signed quorum** ([`AttestStore`], unchanged).
//! - **Anchored (network-owned) programs → a COMPUTED, rotating epoch committee**
//!   ([`EpochCommitteeSource`]) — because a network-owned program's *sentinel* owner has no key to
//!   sign a declared quorum, so it can't use the attestation path at all.
//!
//! [`AnchorAwareQuorumSource`] routes by the deterministic sentinel: if `owner` is the anchor sentinel
//! of `program_cid` (see [`AnchorDispatcher::anchor_owner`]), the program is anchored → committee;
//! otherwise → its declared quorum. The method is named `quorum_for` (not `current_quorum`) to avoid
//! clashing with `AttestStore`'s inherent method of that name.

use std::sync::Arc;

use async_trait::async_trait;
use zeph_com::Quorum;

use crate::anchor::AnchorDispatcher;
use crate::attest::AttestStore;
use crate::epoch_committee::EpochCommitteeSource;

/// Provides the k-of-n [`Quorum`] the sequencer orders an anchored/owned program's writes under.
#[async_trait]
pub trait QuorumSource: Send + Sync {
    async fn quorum_for(&self, owner: &[u8; 32], program_cid: &[u8; 32]) -> Option<Quorum>;
}

/// A user program's declared, owner-signed quorum (the existing attestation path — unchanged).
#[async_trait]
impl QuorumSource for AttestStore {
    async fn quorum_for(&self, owner: &[u8; 32], program_cid: &[u8; 32]) -> Option<Quorum> {
        self.current_quorum(owner, program_cid).await
    }
}

/// Routes each `(owner, program_cid)` to the right quorum provenance: the epoch committee for anchored
/// (sentinel-owned) programs, the declared quorum otherwise.
pub struct AnchorAwareQuorumSource {
    attest: Arc<AttestStore>,
    committee: Arc<EpochCommitteeSource>,
}

impl AnchorAwareQuorumSource {
    pub fn new(attest: Arc<AttestStore>, committee: Arc<EpochCommitteeSource>) -> Self {
        Self { attest, committee }
    }
}

#[async_trait]
impl QuorumSource for AnchorAwareQuorumSource {
    async fn quorum_for(&self, owner: &[u8; 32], program_cid: &[u8; 32]) -> Option<Quorum> {
        // Anchored (network-owned): the owner is the deterministic sentinel of the cid → epoch committee.
        if *owner == AnchorDispatcher::anchor_owner(program_cid) {
            self.committee.quorum_for(owner, program_cid).await
        } else {
            // User program: its declared, owner-signed quorum.
            self.attest.quorum_for(owner, program_cid).await
        }
    }
}
