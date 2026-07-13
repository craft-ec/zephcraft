//! Attestation — **user-defined quorum authority** (`ATTESTATION_DESIGN.md`,
//! `VERIFICATION_ATTESTATION_MODEL.md`). A program declares a **named quorum** (member pubkeys +
//! `k-of-n`) and triggers it to **authorize** a statement: "do the specific parties I chose approve
//! this?" It is a DISTINCT primitive from verification (consistency — "is the output correct?"):
//! attestation is *authority* (the identity of the quorum IS the authority; the members are chosen,
//! not interchangeable). The two compose in the program, not the kernel.
//!
//! This is [`crate::gov`] **generalized** from the single network governance set to an
//! app-declarable one: [`Quorum`] = `GovernanceSet`, [`Attestation`] = `GovernanceApproval`,
//! [`QuorumChain`] = `GovernanceChain`. The one genuinely new piece is [`AttestAction::Statement`] —
//! an OPAQUE, app-defined statement the quorum authorizes (governance signs a fixed `GovAction`; an
//! app signs its own bytes). Reconfiguration reuses governance's **self-amending** `apply()`
//! (add/remove a member, change the threshold through the same k-of-n path). Network governance is
//! the genesis instance of this same substrate.
//!
//! This module (P1) is the pure, offline substrate — the types + the k-of-n check + the durable
//! fold. Declaring a quorum, soliciting member sign-offs over the network, and the `attest` host fn
//! ride on top in later phases.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

/// Domain tag separating an attestation signature from every other ed25519 use (incl. governance).
const ATTEST_DOMAIN: &[u8] = b"craftec/attest/1";

/// What a quorum authorizes at a given seq. Generalizes `GovAction`: the payload case is an
/// **opaque, app-defined statement** (the app interprets it); the rest are self-amendment of the
/// quorum itself (the reconfiguration path, identical to governance).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttestAction {
    /// Authorize an opaque, app-defined statement. The kernel treats it as bytes; the program
    /// decides its meaning and gates a transition on its authorization.
    Statement(Vec<u8>),
    /// Self-amendment: add a member to the quorum.
    AddMember { member: [u8; 32] },
    /// Self-amendment: remove a member.
    RemoveMember { member: [u8; 32] },
    /// Self-amendment: change the `k`-of-n threshold.
    SetThreshold { threshold: u64 },
}

/// A proposed action at a specific quorum seq (which must be the current seq + 1 — this orders
/// proposals and prevents replay). Generalizes `GovernanceProposal`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttestProposal {
    pub action: AttestAction,
    pub seq: u64,
}

impl AttestProposal {
    fn signing_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(ATTEST_DOMAIN.len() + 64);
        b.extend_from_slice(ATTEST_DOMAIN);
        b.extend_from_slice(&postcard::to_allocvec(self).unwrap_or_default());
        b
    }

    /// A quorum member signs this proposal.
    pub fn sign(&self, identity: &NodeIdentity) -> MemberSignature {
        MemberSignature {
            member: identity.node_id().0,
            signature: identity.sign(&self.signing_bytes()).to_vec(),
        }
    }
}

/// One member's signature over a proposal. Governance's `GovSignature` is an alias of this.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberSignature {
    pub member: [u8; 32],
    pub signature: Vec<u8>,
}

/// A membership self-amendment — the reconfiguration path shared by governance + attestation. A
/// payload action (an app `Statement`, or governance's `SetProgram`/`SetConfig`) maps to `None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberChange {
    Add([u8; 32]),
    Remove([u8; 32]),
    SetThreshold(u64),
}

/// A proposal plus the collected member signatures — the k-of-n unit. Generalizes
/// `GovernanceApproval`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attestation {
    pub proposal: AttestProposal,
    pub signatures: Vec<MemberSignature>,
}

/// The named quorum + threshold + monotonic seq. Self-amendable via `AddMember` / `RemoveMember` /
/// `SetThreshold` — so the quorum evolves its own membership through the same k-of-n path (the safe
/// reconfiguration rule, inherited from governance). Generalizes `GovernanceSet`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Quorum {
    /// Member public keys, canonical (sorted) order.
    pub members: Vec<[u8; 32]>,
    pub threshold: usize,
    pub seq: u64,
}

impl Quorum {
    /// The bootstrap quorum (the app owner installs this genesis set + threshold).
    pub fn genesis(members: Vec<[u8; 32]>, threshold: usize) -> Self {
        let mut m = members;
        m.sort();
        m.dedup();
        let threshold = threshold.clamp(1, m.len().max(1));
        Self {
            members: m,
            threshold,
            seq: 0,
        }
    }

    pub fn is_member(&self, id: &[u8; 32]) -> bool {
        self.members.binary_search(id).is_ok()
    }

    /// **Shared substrate** (governance + attestation): count DISTINCT valid member signatures over
    /// `msg` (a non-member's signature is ignored). This is where "authority = who signed" is
    /// enforced — the one k-of-n crypto count for both consumers, each passing its own domain-tagged
    /// proposal bytes.
    pub fn count_signers(&self, msg: &[u8], sigs: &[MemberSignature]) -> usize {
        let mut set = HashSet::new();
        for s in sigs {
            if !self.is_member(&s.member) {
                continue;
            }
            if let Ok(sig) = <[u8; 64]>::try_from(s.signature.as_slice()) {
                if NodeIdentity::verify(&NodeId(s.member), msg, &sig) {
                    set.insert(s.member);
                }
            }
        }
        set.len()
    }

    /// **Shared substrate**: advance the quorum by one seq, applying an optional membership change.
    /// A payload action (a `Statement`, or governance's `SetProgram`/`SetConfig`) passes `None` — it
    /// only bumps the seq; its authority is consumed by the app/registry, not by the quorum.
    pub fn advance(&self, change: Option<MemberChange>) -> Quorum {
        let mut next = self.clone();
        next.seq += 1;
        match change {
            Some(MemberChange::Add(member)) => {
                if next.members.binary_search(&member).is_err() {
                    next.members.push(member);
                    next.members.sort();
                }
            }
            Some(MemberChange::Remove(member)) => {
                next.members.retain(|m| *m != member);
                next.threshold = next.threshold.min(next.members.len().max(1));
            }
            Some(MemberChange::SetThreshold(threshold)) => {
                next.threshold = (threshold as usize).clamp(1, next.members.len().max(1));
            }
            None => {}
        }
        next
    }
}

impl AttestAction {
    /// The membership change this action makes (self-amendment), or `None` for a `Statement`.
    fn member_change(&self) -> Option<MemberChange> {
        match self {
            AttestAction::AddMember { member } => Some(MemberChange::Add(*member)),
            AttestAction::RemoveMember { member } => Some(MemberChange::Remove(*member)),
            AttestAction::SetThreshold { threshold } => {
                Some(MemberChange::SetThreshold(*threshold))
            }
            AttestAction::Statement(_) => None,
        }
    }
}

impl Attestation {
    /// Authorized against `q` iff it targets the next seq AND ≥ `q.threshold` distinct members
    /// signed it.
    pub fn authorizes(&self, q: &Quorum) -> bool {
        self.proposal.seq == q.seq + 1
            && q.count_signers(&self.proposal.signing_bytes(), &self.signatures) >= q.threshold
    }

    /// Apply to `q` → the advanced quorum (seq + 1, with any membership change), or `None` if not
    /// authorized.
    pub fn apply_to(&self, q: &Quorum) -> Option<Quorum> {
        self.authorizes(q)
            .then(|| q.advance(self.proposal.action.member_change()))
    }
}

/// A durable, **self-verifying** chain of attestations from a genesis quorum — the authorization
/// record. Every node folds it to the identical current quorum + set of authorized statements, so
/// authority is content-addressed and reproducible cross-node with no gossip. Generalizes
/// `GovernanceChain`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuorumChain {
    pub genesis: Quorum,
    pub attestations: Vec<Attestation>,
}

impl QuorumChain {
    pub fn new(genesis: Quorum) -> Self {
        Self {
            genesis,
            attestations: Vec::new(),
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

    /// Current seq = genesis seq + one per attestation.
    pub fn seq(&self) -> u64 {
        self.genesis.seq + self.attestations.len() as u64
    }

    /// Fold `apply` from genesis. `Some(final_quorum)` iff EVERY attestation validly extends the
    /// chain; `None` if any is invalid (bad quorum / wrong seq) — the whole chain is rejected, so a
    /// tampered chain can't be adopted.
    pub fn current(&self) -> Option<Quorum> {
        let mut q = self.genesis.clone();
        for a in &self.attestations {
            q = a.apply_to(&q)?;
        }
        Some(q)
    }

    /// Append an attestation iff it validly extends the current chain (returns success).
    pub fn append(&mut self, att: Attestation) -> bool {
        match self.current() {
            Some(cur) if att.apply_to(&cur).is_some() => {
                self.attestations.push(att);
                true
            }
            _ => false,
        }
    }

    /// Every statement the quorum has authorized: replay the chain and collect the payload of each
    /// valid `Statement` attestation. The program's authority query is `is_authorized`.
    pub fn authorized_statements(&self) -> Vec<Vec<u8>> {
        let mut q = self.genesis.clone();
        let mut out = Vec::new();
        for a in &self.attestations {
            let Some(next) = a.apply_to(&q) else {
                return out; // a break in the chain — nothing beyond is authorized
            };
            if let AttestAction::Statement(s) = &a.proposal.action {
                out.push(s.clone());
            }
            q = next;
        }
        out
    }

    /// Whether the quorum has authorized `statement` — the gate a program checks before treating a
    /// transition as authorized.
    pub fn is_authorized(&self, statement: &[u8]) -> bool {
        self.authorized_statements().iter().any(|s| s == statement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(n: usize) -> Vec<NodeIdentity> {
        (0..n).map(|_| NodeIdentity::generate()).collect()
    }
    fn quorum(ids: &[NodeIdentity], k: usize) -> Quorum {
        Quorum::genesis(ids.iter().map(|i| i.node_id().0).collect(), k)
    }
    /// A k-of-n attestation: `signers` of `ids` sign the proposal.
    fn attest(action: AttestAction, seq: u64, signers: &[&NodeIdentity]) -> Attestation {
        let proposal = AttestProposal { action, seq };
        Attestation {
            signatures: signers.iter().map(|s| proposal.sign(s)).collect(),
            proposal,
        }
    }

    #[test]
    fn k_of_n_counts_only_distinct_valid_members() {
        let ids = members(3);
        let q = quorum(&ids, 2);
        let stmt = AttestAction::Statement(b"authorize the thing".to_vec());

        // 1 signer < k=2 → not enough
        assert!(!attest(stmt.clone(), 1, &[&ids[0]]).authorizes(&q));
        // 2 distinct signers → authorizes
        assert!(attest(stmt.clone(), 1, &[&ids[0], &ids[1]]).authorizes(&q));
        // the same member twice still counts once → not enough
        assert!(!attest(stmt.clone(), 1, &[&ids[0], &ids[0]]).authorizes(&q));
        // an outsider's signature doesn't count
        let outsider = NodeIdentity::generate();
        assert!(!attest(stmt.clone(), 1, &[&ids[0], &outsider]).authorizes(&q));
    }

    #[test]
    fn verify_requires_the_next_seq() {
        let ids = members(2);
        let q = quorum(&ids, 2);
        let a = AttestAction::Statement(b"x".to_vec());
        assert!(
            attest(a.clone(), 1, &[&ids[0], &ids[1]]).authorizes(&q),
            "seq must be current+1 = 1"
        );
        assert!(
            !attest(a.clone(), 2, &[&ids[0], &ids[1]]).authorizes(&q),
            "seq 2 is wrong at seq 0"
        );
        assert!(
            !attest(a, 0, &[&ids[0], &ids[1]]).authorizes(&q),
            "replay of seq 0 rejected"
        );
    }

    #[test]
    fn quorum_self_amends_membership_and_threshold() {
        let ids = members(3);
        let mut q = quorum(&ids, 2);
        let newcomer = NodeIdentity::generate();

        // add a 4th member (2-of-3 authorizes it)
        q = attest(
            AttestAction::AddMember {
                member: newcomer.node_id().0,
            },
            1,
            &[&ids[0], &ids[1]],
        )
        .apply_to(&q)
        .expect("add authorized");
        assert!(q.is_member(&newcomer.node_id().0) && q.seq == 1);

        // raise the threshold to 3 (2-of-4 still authorizes this transition)
        q = attest(
            AttestAction::SetThreshold { threshold: 3 },
            2,
            &[&ids[0], &ids[1]],
        )
        .apply_to(&q)
        .expect("threshold change authorized");
        assert_eq!(q.threshold, 3);
        // now 2 signers is no longer enough
        assert!(!attest(
            AttestAction::Statement(b"z".to_vec()),
            3,
            &[&ids[0], &ids[1]]
        )
        .authorizes(&q));
    }

    #[test]
    fn chain_folds_authorizes_statements_and_rejects_tampering() {
        let ids = members(3);
        let mut chain = QuorumChain::new(quorum(&ids, 2));
        assert!(chain.append(attest(
            AttestAction::Statement(b"deploy v2".to_vec()),
            1,
            &[&ids[0], &ids[1]]
        )));
        assert!(chain.append(attest(
            AttestAction::Statement(b"raise quota".to_vec()),
            2,
            &[&ids[1], &ids[2]]
        )));
        assert_eq!(chain.seq(), 2);
        assert!(chain.is_authorized(b"deploy v2") && chain.is_authorized(b"raise quota"));
        assert!(!chain.is_authorized(b"never approved"));
        assert!(chain.current().is_some(), "a valid chain folds");

        // an appended attestation with only 1 signer is rejected (doesn't extend the chain)
        assert!(!chain.append(attest(
            AttestAction::Statement(b"sneaky".to_vec()),
            3,
            &[&ids[0]]
        )));
        assert!(!chain.is_authorized(b"sneaky"));

        // tamper the stored chain: flip a signature → the whole fold rejects
        let mut tampered = chain.clone();
        tampered.attestations[0].signatures[0].signature[0] ^= 0xFF;
        assert!(
            tampered.current().is_none(),
            "a tampered chain can't be adopted"
        );
        assert!(!tampered.is_authorized(b"deploy v2"));
    }

    #[test]
    fn chain_encode_decode_roundtrips() {
        let ids = members(2);
        let mut chain = QuorumChain::new(quorum(&ids, 1));
        chain.append(attest(
            AttestAction::Statement(b"ok".to_vec()),
            1,
            &[&ids[0]],
        ));
        let back = QuorumChain::decode(&chain.encode()).unwrap();
        assert_eq!(back, chain);
        assert!(back.is_authorized(b"ok"));
    }
}
