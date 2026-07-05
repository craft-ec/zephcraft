//! Governance — the manual-approval layer for PROTOCOL-level (network-owned) changes.
//!
//! User-space apps publish freely (owner-authorized; no governance). This layer covers
//! only the network's OWN policy: which WASM is canonical for a network-owned program,
//! protocol config, and the governor set itself. A change is a [`GovernanceProposal`]
//! approved by a **k-of-n multisig of governors** — that signature is the human
//! *judgment*. The deterministic layer (the program/config registry) then verifies the
//! approval and records it, and the attestation committee attests that recording. So:
//! **governance decides (manual), everything downstream is automated** (foundation
//! mechanism/policy split). Governance is separate from the attestation committee — the
//! committee proves facts, governance makes decisions.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;

const GOV_DOMAIN: &[u8] = b"craftec/gov/1";

/// A protocol-level change requiring governance approval.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GovAction {
    /// Set the canonical WASM cid for a network-owned program (the program registry).
    SetProgram { name: String, cid: [u8; 32] },
    /// Set a protocol config value (the config registry).
    SetConfig { key: String, value: i64 },
    /// Add a governor to the set.
    AddGovernor { governor: [u8; 32] },
    /// Remove a governor from the set.
    RemoveGovernor { governor: [u8; 32] },
    /// Change the approval threshold.
    SetThreshold { threshold: u64 },
}

/// A proposal: an action at a specific governance sequence number (which must be the
/// current seq + 1 — this orders proposals and prevents replay).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceProposal {
    pub action: GovAction,
    pub seq: u64,
}

impl GovernanceProposal {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(GOV_DOMAIN.len() + 64);
        b.extend_from_slice(GOV_DOMAIN);
        b.extend_from_slice(&postcard::to_allocvec(self).unwrap_or_default());
        b
    }

    /// A governor signs this proposal.
    pub fn sign(&self, identity: &NodeIdentity) -> GovSignature {
        GovSignature {
            governor: identity.node_id().0,
            signature: identity.sign(&self.signing_bytes()).to_vec(),
        }
    }
}

/// One governor's signature over a proposal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovSignature {
    pub governor: [u8; 32],
    pub signature: Vec<u8>,
}

/// A proposal plus the collected governor signatures.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceApproval {
    pub proposal: GovernanceProposal,
    pub signatures: Vec<GovSignature>,
}

/// The governor set + threshold + monotonic seq. It is itself amendable — via approved
/// `AddGovernor`/`RemoveGovernor`/`SetThreshold` proposals — so governance can evolve
/// its own membership without a binary release.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct GovernanceSet {
    /// Governor public keys, canonical (sorted) order.
    pub governors: Vec<[u8; 32]>,
    pub threshold: usize,
    pub seq: u64,
}

impl GovernanceSet {
    /// The bootstrap governance set (genesis governors + threshold).
    pub fn genesis(governors: Vec<[u8; 32]>, threshold: usize) -> Self {
        let mut g = governors;
        g.sort();
        g.dedup();
        let threshold = threshold.clamp(1, g.len().max(1));
        Self {
            governors: g,
            threshold,
            seq: 0,
        }
    }

    pub fn is_governor(&self, id: &[u8; 32]) -> bool {
        self.governors.binary_search(id).is_ok()
    }

    /// Distinct valid governor signatures over the approval's proposal.
    fn quorum(&self, approval: &GovernanceApproval) -> usize {
        let msg = approval.proposal.signing_bytes();
        let mut set = HashSet::new();
        for s in &approval.signatures {
            if !self.is_governor(&s.governor) {
                continue;
            }
            if let Ok(sig) = <[u8; 64]>::try_from(s.signature.as_slice()) {
                if NodeIdentity::verify(&NodeId(s.governor), &msg, &sig) {
                    set.insert(s.governor);
                }
            }
        }
        set.len()
    }

    /// Valid iff the proposal targets the next seq AND ≥ threshold distinct governors
    /// signed it. This is what the program/config registry checks before recording.
    pub fn verify(&self, approval: &GovernanceApproval) -> bool {
        approval.proposal.seq == self.seq + 1 && self.quorum(approval) >= self.threshold
    }

    /// Apply an approved proposal, returning the advanced set (seq + 1, with any
    /// governor/threshold change applied). `SetProgram`/`SetConfig` only advance the seq
    /// here — their effect lands in the program/config registries. None if invalid.
    pub fn apply(&self, approval: &GovernanceApproval) -> Option<GovernanceSet> {
        if !self.verify(approval) {
            return None;
        }
        let mut next = self.clone();
        next.seq += 1;
        match &approval.proposal.action {
            GovAction::AddGovernor { governor } => {
                if next.governors.binary_search(governor).is_err() {
                    next.governors.push(*governor);
                    next.governors.sort();
                }
            }
            GovAction::RemoveGovernor { governor } => {
                next.governors.retain(|g| g != governor);
                next.threshold = next.threshold.min(next.governors.len().max(1));
            }
            GovAction::SetThreshold { threshold } => {
                next.threshold = (*threshold as usize).clamp(1, next.governors.len().max(1));
            }
            GovAction::SetProgram { .. } | GovAction::SetConfig { .. } => {}
        }
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn govs(n: usize) -> Vec<NodeIdentity> {
        (0..n).map(|_| NodeIdentity::generate()).collect()
    }
    fn set(ids: &[NodeIdentity], k: usize) -> GovernanceSet {
        GovernanceSet::genesis(ids.iter().map(|i| i.node_id().0).collect(), k)
    }
    fn approve(
        ids: &[NodeIdentity],
        signers: usize,
        action: GovAction,
        seq: u64,
    ) -> GovernanceApproval {
        let proposal = GovernanceProposal { action, seq };
        GovernanceApproval {
            signatures: ids
                .iter()
                .take(signers)
                .map(|id| proposal.sign(id))
                .collect(),
            proposal,
        }
    }

    #[test]
    fn quorum_of_governors_approves() {
        let g = govs(3);
        let gs = set(&g, 2);
        let a = approve(&g, 2, GovAction::SetThreshold { threshold: 3 }, 1);
        assert!(gs.verify(&a));
        // one signature is below threshold
        let a1 = approve(&g, 1, GovAction::SetThreshold { threshold: 3 }, 1);
        assert!(!gs.verify(&a1));
    }

    #[test]
    fn wrong_seq_is_rejected() {
        let g = govs(3);
        let gs = set(&g, 2);
        // gs.seq is 0, so seq must be 1
        let a = approve(&g, 2, GovAction::SetThreshold { threshold: 3 }, 5);
        assert!(!gs.verify(&a), "a proposal must target the next seq");
    }

    #[test]
    fn non_governor_signatures_do_not_count() {
        let g = govs(2);
        let gs = set(&g, 2);
        let outsiders = govs(3);
        let a = approve(&outsiders, 3, GovAction::SetThreshold { threshold: 1 }, 1);
        assert!(
            !gs.verify(&a),
            "only governor signatures count toward the quorum"
        );
    }

    #[test]
    fn add_governor_amends_the_set() {
        let g = govs(3);
        let gs = set(&g, 2);
        let newcomer = NodeIdentity::generate();
        let a = approve(
            &g,
            2,
            GovAction::AddGovernor {
                governor: newcomer.node_id().0,
            },
            1,
        );
        let next = gs.apply(&a).expect("valid approval amends the set");
        assert_eq!(next.seq, 1);
        assert!(next.is_governor(&newcomer.node_id().0));
        assert_eq!(next.governors.len(), 4);
        // the same approval can't be replayed (seq is now 1, needs 2)
        assert!(next.apply(&a).is_none());
    }

    #[test]
    fn set_program_advances_seq_but_not_the_governors() {
        let g = govs(3);
        let gs = set(&g, 2);
        let a = approve(
            &g,
            2,
            GovAction::SetProgram {
                name: "registry".into(),
                cid: [7u8; 32],
            },
            1,
        );
        let next = gs.apply(&a).expect("valid");
        assert_eq!(next.seq, 1);
        assert_eq!(
            next.governors, gs.governors,
            "program change doesn't touch governors"
        );
    }
}
