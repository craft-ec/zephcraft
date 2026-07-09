//! Governance — the manual-approval layer for PROTOCOL-level (network-owned) changes.
//!
//! User-space apps publish freely (owner-authorized; no governance). This layer covers
//! only the network's OWN policy: which WASM is canonical for a network-owned program,
//! protocol config, and the governor set itself. A change is a [`GovernanceProposal`]
//! approved by a **k-of-n multisig of governors** — that signature is the human
//! *judgment*. The approval is then appended to a durable, self-verifying **governance
//! chain**: every node independently FOLDS that chain to derive the identical current
//! governor set + program/config registry (see [`GovernanceChain`]), so the recording is
//! reproduced deterministically cross-node with NO gossip and NO attestation committee.
//! So: **governance decides (manual), everything downstream is automated** (foundation
//! mechanism/policy split) — the multisig quorum makes the decision, the chain-fold makes
//! it durable and self-verifying.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

use crate::registry::{ConfigRegistryState, ProgramRegistryState};

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

/// A durable, **self-verifying** chain of governance approvals from a genesis governor
/// set. Every node derives the SAME current governor set + program registry by folding
/// the approvals from genesis — so governance state is content-addressed and resolvable
/// cross-node, needing NO gossip. Approvals are seq-ordered, so the chain is totally
/// ordered (no forks) and the **longest valid chain wins** (durable state, pulled
/// on demand — no push protocol).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceChain {
    pub genesis: GovernanceSet,
    pub approvals: Vec<GovernanceApproval>,
}

impl GovernanceChain {
    pub fn new(genesis: GovernanceSet) -> Self {
        Self {
            genesis,
            approvals: Vec::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
    pub fn root(&self) -> [u8; 32] {
        Cid::of(&self.encode()).0
    }

    /// Current governance seq = genesis seq + one per approval.
    pub fn seq(&self) -> u64 {
        self.genesis.seq + self.approvals.len() as u64
    }

    /// Fold `apply` from genesis. `Some(final_set)` iff EVERY approval validly extends the
    /// chain; `None` if any approval is invalid (bad quorum / wrong seq) — the whole chain
    /// is then rejected, so a tampered chain can't be adopted.
    pub fn current(&self) -> Option<GovernanceSet> {
        let mut set = self.genesis.clone();
        for a in &self.approvals {
            set = set.apply(a)?;
        }
        Some(set)
    }

    /// Append an approval iff it validly extends the current chain (returns success).
    pub fn append(&mut self, approval: GovernanceApproval) -> bool {
        match self.current() {
            Some(cur) if cur.apply(&approval).is_some() => {
                self.approvals.push(approval);
                true
            }
            _ => false,
        }
    }

    /// The program registry derived from the chain: replay every `SetProgram` approval over an
    /// EMPTY map (version = its seq). Every node computes the identical result from the same chain.
    ///
    /// No `app-registry` seed: the head registry validates writes NATIVELY (owner sig + name
    /// limit) — it is NOT a governed-WASM protocol program (memory
    /// `registry-native-validation-not-wasm-hook`), so seeding it here would misrepresent it as a
    /// governed anchor in the dashboard. The registry starts empty until a real `SetProgram` lists
    /// a network-owned WASM program.
    pub fn program_registry(&self) -> ProgramRegistryState {
        let mut prg = ProgramRegistryState::default();
        let mut seq = self.genesis.seq;
        for a in &self.approvals {
            seq += 1;
            if let GovAction::SetProgram { name, cid } = &a.proposal.action {
                if let Ok(next) = prg.set(name, *cid, seq) {
                    prg = next;
                }
            }
        }
        prg
    }

    /// The config registry derived from the chain: replay every `SetConfig` approval
    /// (version = its seq) over an empty map. Mirrors [`Self::program_registry`] — every node
    /// computes the identical result from the same chain, so a protocol config value (e.g. the
    /// registry `shard_bits`) is cluster-agreed with no gossip. Unlike the program registry
    /// there is NO genesis seed: an unset key resolves to `None`, and the consumer applies its
    /// built-in default.
    pub fn config_registry(&self) -> ConfigRegistryState {
        let mut cfg = ConfigRegistryState::default();
        let mut seq = self.genesis.seq;
        for a in &self.approvals {
            seq += 1;
            if let GovAction::SetConfig { key, value } = &a.proposal.action {
                if let Ok(next) = cfg.set(key, *value, seq) {
                    cfg = next;
                }
            }
        }
        cfg
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
    fn chain_folds_verifies_and_derives_program_registry() {
        let g = govs(1);
        let genesis = set(&g, 1);
        let mut chain = GovernanceChain::new(genesis.clone());
        assert_eq!(chain.seq(), 0);
        assert_eq!(chain.current(), Some(genesis));
        // genesis program registry is EMPTY — no app-registry seed (the registry validates
        // natively, it is not a governed-WASM program).
        assert_eq!(chain.program_registry().resolve("app-registry"), None);
        assert!(chain.program_registry().entries().is_empty());

        // append a SetProgram approval -> derived program registry updates
        let a = approve(
            &g,
            1,
            GovAction::SetProgram {
                name: "some-program".into(),
                cid: [7u8; 32],
            },
            1,
        );
        assert!(chain.append(a), "valid approval appends");
        assert_eq!(chain.seq(), 1);
        assert!(chain.current().is_some());
        assert_eq!(
            chain.program_registry().resolve("some-program"),
            Some([7u8; 32]),
            "SetProgram is reflected in the derived registry"
        );
        // a wrong-seq approval does not append
        let bad = approve(&g, 1, GovAction::SetThreshold { threshold: 1 }, 5);
        assert!(!chain.append(bad));
        // encode/decode roundtrip
        assert_eq!(GovernanceChain::decode(&chain.encode()), Some(chain));
    }

    #[test]
    fn chain_derives_config_registry_from_set_config() {
        let g = govs(1);
        let mut chain = GovernanceChain::new(set(&g, 1));
        // empty chain -> no config values (consumer falls back to its default)
        assert_eq!(chain.config_registry().resolve("shard_bits"), None);

        // a SetConfig approval lands in the derived config registry at version = its seq
        let a = approve(
            &g,
            1,
            GovAction::SetConfig {
                key: "shard_bits".into(),
                value: 9,
            },
            1,
        );
        assert!(chain.append(a), "valid SetConfig appends");
        assert_eq!(chain.config_registry().resolve("shard_bits"), Some(9));

        // a later SetConfig upserts (version = new seq, strictly greater)
        let a2 = approve(
            &g,
            1,
            GovAction::SetConfig {
                key: "shard_bits".into(),
                value: 10,
            },
            2,
        );
        assert!(chain.append(a2));
        assert_eq!(chain.config_registry().resolve("shard_bits"), Some(10));
        // SetConfig does not disturb the (empty) program registry
        assert!(chain.program_registry().entries().is_empty());
    }

    #[test]
    fn chain_rejects_a_tampered_approval() {
        let g = govs(3);
        let mut chain = GovernanceChain::new(set(&g, 2));
        // only 1 of 3 signs (threshold 2) -> invalid; a chain carrying it verifies to None
        let weak = approve(&g, 1, GovAction::SetThreshold { threshold: 1 }, 1);
        chain.approvals.push(weak); // force it in, bypassing append
        assert!(
            chain.current().is_none(),
            "a chain with a sub-quorum approval is wholly rejected"
        );
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
