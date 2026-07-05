//! Phase 4 — the app-name registry: the first attested PDA registry (foundation §62
//! A3, `REGISTRY_DESIGN.md`). A publisher submits a SIGNED head `(owner, name) →
//! (cid, version)`; the registry *transition* validates it (owner signature +
//! monotonic version) and upserts it into a canonical state. The state's root advances
//! by attestation (the committee runs this transition and attests the new root).
//!
//! This module is the registry's **logic** — a pure, deterministic state machine, so
//! every committee agent computes the identical result. Two guards (REGISTRY_DESIGN
//! §4): the *submission* is owner-signed (no one forges your mapping), and the
//! *transition* is attested (the append is agreed) — one is here, the other is the
//! committee wrapping `apply`.
//!
//! Wiring it live — packaging this transition as the network-owned WASM program, running
//! it through the committee (phase 3b), and storing/resolving the registry head — is
//! the follow-up; this is the tested core it stands on.

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

/// Domain tag for a head submission signature.
const HEAD_DOMAIN: &[u8] = b"craftec/head/1";

/// A publisher's signed registration of one head. The signature binds
/// `(owner, name, cid, version)`, so only the owner can register or advance their key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeadSubmission {
    pub owner: [u8; 32],
    pub name: String,
    pub cid: [u8; 32],
    pub version: u64,
    pub signature: Vec<u8>,
}

fn head_signing_bytes(owner: &[u8; 32], name: &str, cid: &[u8; 32], version: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(HEAD_DOMAIN.len() + 76 + name.len());
    b.extend_from_slice(HEAD_DOMAIN);
    b.extend_from_slice(owner);
    b.extend_from_slice(&(name.len() as u32).to_be_bytes());
    b.extend_from_slice(name.as_bytes());
    b.extend_from_slice(cid);
    b.extend_from_slice(&version.to_be_bytes());
    b
}

impl HeadSubmission {
    /// Sign a head with the owner's identity (owner = the signer's NodeId).
    pub fn sign(identity: &NodeIdentity, name: &str, cid: [u8; 32], version: u64) -> Self {
        let owner = identity.node_id().0;
        let signature = identity
            .sign(&head_signing_bytes(&owner, name, &cid, version))
            .to_vec();
        Self {
            owner,
            name: name.to_string(),
            cid,
            version,
            signature,
        }
    }

    /// Canonical wire encoding of this submission (the registry request bytes).
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    /// Decode a submission from its wire bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }

    /// Verify the owner's signature over this submission.
    pub fn verify(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            return false;
        };
        let msg = head_signing_bytes(&self.owner, &self.name, &self.cid, self.version);
        NodeIdentity::verify(&NodeId(self.owner), &msg, &sig)
    }
}

/// One registry row — the current head for a `(owner, name)` key.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeadEntry {
    pub owner: [u8; 32],
    pub name: String,
    pub cid: [u8; 32],
    pub version: u64,
}

/// The registry state: the canonical (sorted by `(owner, name)`) set of current heads —
/// one row per key, latest version. Content-hashed to a `root`; the root is what the
/// committee attests as the registry advances.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryState {
    entries: Vec<HeadEntry>,
}

impl RegistryState {
    /// Decode a state blob (empty bytes → empty state).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return Some(Self::default());
        }
        postcard::from_bytes(bytes).ok()
    }

    /// Canonical encoding of the state.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    /// The state root — content hash of the canonical encoding. Two states with the
    /// same entries hash identically (the entries are kept sorted), so every agent
    /// computes the same root.
    pub fn root(&self) -> [u8; 32] {
        Cid::of(&self.encode()).0
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn find(&self, owner: &[u8; 32], name: &str) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|e| e.owner.cmp(owner).then_with(|| e.name.as_str().cmp(name)))
    }

    /// All current entries (canonical order) — for enumeration / the dashboard.
    pub fn entries(&self) -> &[HeadEntry] {
        &self.entries
    }

    /// Resolve the current head for `(owner, name)`.
    pub fn resolve(&self, owner: &[u8; 32], name: &str) -> Option<&HeadEntry> {
        self.find(owner, name).ok().map(|i| &self.entries[i])
    }

    /// Apply a signed submission — the registry TRANSITION. Validates the owner
    /// signature and a strictly-monotonic version, then upserts. Deterministic: every
    /// agent computes the identical new state (and thus the identical [`root`]) from the
    /// same `(state, submission)`.
    pub fn apply(&self, sub: &HeadSubmission) -> Result<RegistryState, &'static str> {
        if !sub.verify() {
            return Err("bad-signature");
        }
        let mut next = self.clone();
        let entry = HeadEntry {
            owner: sub.owner,
            name: sub.name.clone(),
            cid: sub.cid,
            version: sub.version,
        };
        match next.find(&sub.owner, &sub.name) {
            Ok(i) => {
                if sub.version <= next.entries[i].version {
                    return Err("stale-version");
                }
                next.entries[i] = entry;
            }
            Err(i) => next.entries.insert(i, entry),
        }
        Ok(next)
    }
}

/// The well-known program CID of the app-name registry — a NATIVE network-owned
/// program (its logic is [`RegistryState::apply`]).
pub fn registry_program_cid() -> [u8; 32] {
    Cid::of(b"craftec/program/registry/1").0
}

/// The app-name registry compiled to WASM (governance-upgradeable). Governance sets
/// `app-registry` to [`registry_wasm_cid`] to run THIS instead of the native program.
pub const REGISTRY_WASM: &[u8] = include_bytes!("../registry.wasm");

/// Content cid of [`REGISTRY_WASM`].
pub fn registry_wasm_cid() -> [u8; 32] {
    Cid::of(REGISTRY_WASM).0
}

/// The seed for the registry PDA account (so `pda(registry_program_cid(), REGISTRY_SEED)`
/// is the account whose head advances as the registry).
pub const REGISTRY_SEED: &[u8] = b"apps";

/// The registry as a [`crate::NativeProgram`] the attestation committee runs: decode the
/// prior state, apply the signed submission, re-encode. Deterministic, so every agent
/// computes the identical new state.
pub struct RegistryProgram;

impl crate::NativeProgram for RegistryProgram {
    fn program_cid(&self) -> [u8; 32] {
        registry_program_cid()
    }
    fn run(&self, prev_state: &[u8], request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let state = RegistryState::decode(prev_state)
            .ok_or_else(|| anyhow::anyhow!("undecodable registry state"))?;
        let sub =
            HeadSubmission::decode(request).ok_or_else(|| anyhow::anyhow!("bad submission"))?;
        let next = state.apply(&sub).map_err(|e| anyhow::anyhow!(e))?;
        Ok(next.encode())
    }
}

/// The **program registry** state: `network-program name → (canonical wasm cid,
/// version)`. This is the native bootstrap map that makes every OTHER network-owned
/// program upgradeable — its writes are authorized by governance (a `SetProgram`
/// approval), and nodes resolve a program's canonical cid here instead of hardcoding it.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgramRegistryState {
    /// (name, cid, version) sorted by name — one entry per program.
    programs: Vec<(String, [u8; 32], u64)>,
}

impl ProgramRegistryState {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return Some(Self::default());
        }
        postcard::from_bytes(bytes).ok()
    }
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }
    pub fn root(&self) -> [u8; 32] {
        Cid::of(&self.encode()).0
    }
    pub fn entries(&self) -> &[(String, [u8; 32], u64)] {
        &self.programs
    }

    /// The canonical wasm cid for a network-owned program.
    pub fn resolve(&self, name: &str) -> Option<[u8; 32]> {
        self.programs
            .binary_search_by(|(n, _, _)| n.as_str().cmp(name))
            .ok()
            .map(|i| self.programs[i].1)
    }

    /// Set a program's canonical cid (upsert). Version must strictly increase for an
    /// existing program. Returns the new state, or an error.
    pub fn set(
        &self,
        name: &str,
        cid: [u8; 32],
        version: u64,
    ) -> Result<ProgramRegistryState, &'static str> {
        let mut next = self.clone();
        match next
            .programs
            .binary_search_by(|(n, _, _)| n.as_str().cmp(name))
        {
            Ok(i) => {
                if version <= next.programs[i].2 {
                    return Err("stale-version");
                }
                next.programs[i] = (name.to_string(), cid, version);
            }
            Err(i) => next.programs.insert(i, (name.to_string(), cid, version)),
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> NodeIdentity {
        NodeIdentity::generate()
    }

    #[test]
    fn submission_signs_and_verifies() {
        let a = id();
        let s = HeadSubmission::sign(&a, "feed", [7u8; 32], 1);
        assert!(s.verify());
        let mut tampered = s.clone();
        tampered.cid = [8u8; 32];
        assert!(
            !tampered.verify(),
            "a changed cid invalidates the signature"
        );
    }

    #[test]
    fn apply_upserts_and_resolves() {
        let a = id();
        let state = RegistryState::default();
        let sub = HeadSubmission::sign(&a, "feed", [1u8; 32], 1);
        let next = state.apply(&sub).unwrap();
        let got = next.resolve(&a.node_id().0, "feed").unwrap();
        assert_eq!(got.cid, [1u8; 32]);
        assert_eq!(got.version, 1);
        assert_eq!(next.len(), 1);
    }

    #[test]
    fn apply_rejects_a_forged_submission() {
        let a = id();
        let mut sub = HeadSubmission::sign(&a, "feed", [1u8; 32], 1);
        sub.cid = [9u8; 32]; // signature no longer matches
        assert_eq!(RegistryState::default().apply(&sub), Err("bad-signature"));
    }

    #[test]
    fn apply_rejects_a_stale_version() {
        let a = id();
        let state = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 5))
            .unwrap();
        // same version — not strictly greater
        assert_eq!(
            state.apply(&HeadSubmission::sign(&a, "feed", [2u8; 32], 5)),
            Err("stale-version")
        );
        // older version
        assert_eq!(
            state.apply(&HeadSubmission::sign(&a, "feed", [2u8; 32], 3)),
            Err("stale-version")
        );
    }

    #[test]
    fn advancing_the_version_replaces_the_head() {
        let a = id();
        let s1 = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 1))
            .unwrap();
        let s2 = s1
            .apply(&HeadSubmission::sign(&a, "feed", [2u8; 32], 2))
            .unwrap();
        let head = s2.resolve(&a.node_id().0, "feed").unwrap();
        assert_eq!(head.cid, [2u8; 32]);
        assert_eq!(head.version, 2);
        assert_eq!(s2.len(), 1, "same key upserts, not duplicates");
        assert_ne!(s1.root(), s2.root(), "the state root advances (history)");
    }

    #[test]
    fn partitioned_by_owner_is_conflict_free_and_canonical() {
        // Two publishers register the same name; both coexist, and the state root is
        // independent of the order they were applied (canonical, CRDT-like).
        let a = id();
        let b = id();
        let sa = HeadSubmission::sign(&a, "feed", [1u8; 32], 1);
        let sb = HeadSubmission::sign(&b, "feed", [2u8; 32], 1);
        let ab = RegistryState::default()
            .apply(&sa)
            .unwrap()
            .apply(&sb)
            .unwrap();
        let ba = RegistryState::default()
            .apply(&sb)
            .unwrap()
            .apply(&sa)
            .unwrap();
        assert_eq!(ab.root(), ba.root(), "order-independent → conflict-free");
        assert_eq!(ab.len(), 2);
        assert_eq!(ab.resolve(&a.node_id().0, "feed").unwrap().cid, [1u8; 32]);
        assert_eq!(ab.resolve(&b.node_id().0, "feed").unwrap().cid, [2u8; 32]);
    }

    #[test]
    fn decode_roundtrips_and_empty_is_empty() {
        assert!(RegistryState::decode(&[]).unwrap().is_empty());
        let a = id();
        let s = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "x", [1u8; 32], 1))
            .unwrap();
        assert_eq!(RegistryState::decode(&s.encode()).unwrap(), s);
    }

    #[test]
    fn registry_advances_under_a_committee_attestation() {
        use crate::{attest_transition, pda, select_committee, AttestedCommit};

        // The registry program (native network-owned) — identified by a well-known CID.
        let program_cid = Cid::of(b"craftec/program/registry/1").0;
        // A rotating committee of 3, quorum k=2.
        let agents: Vec<NodeIdentity> = (0..3).map(|_| NodeIdentity::generate()).collect();
        let eligible: Vec<[u8; 32]> = agents.iter().map(|a| a.node_id().0).collect();
        let committee = select_committee(&eligible, 1, 3, 2);

        // A publisher submits a signed head.
        let publisher = id();
        let sub = HeadSubmission::sign(&publisher, "feed", [1u8; 32], 1);
        let request = postcard::to_allocvec(&sub).unwrap();

        // Each committee agent runs the registry transition (deterministic) and attests
        // the (prev_root -> new_root) advance over the encoded new state.
        let prev = RegistryState::default();
        let prev_root = prev.root();
        let new_state = prev.apply(&sub).unwrap();
        let attestations = agents
            .iter()
            .map(|a| attest_transition(a, program_cid, prev_root, &request, &new_state.encode()))
            .collect();
        let commit = AttestedCommit {
            program_cid,
            seed: b"apps".to_vec(),
            prev_root,
            request,
            new_root: new_state.root(),
            attestations,
        };

        // The committee quorum advances the registry PDA's head to the new state...
        let adv = committee
            .verify_commit(&commit)
            .expect("the committee attested the registry advance");
        assert_eq!(
            adv.account,
            pda(&program_cid, b"apps"),
            "the registry PDA account"
        );
        assert_eq!(adv.new_root, new_state.root());
        assert_eq!(adv.prev_root, prev_root);
        // ...and that new state resolves the registered name.
        assert_eq!(
            new_state
                .resolve(&publisher.node_id().0, "feed")
                .unwrap()
                .cid,
            [1u8; 32]
        );
    }

    #[test]
    fn program_registry_sets_and_resolves() {
        let s = ProgramRegistryState::default();
        let s = s.set("app-registry", [1u8; 32], 1).unwrap();
        let s = s.set("reputation", [2u8; 32], 1).unwrap();
        assert_eq!(s.resolve("app-registry"), Some([1u8; 32]));
        assert_eq!(s.resolve("reputation"), Some([2u8; 32]));
        assert_eq!(s.resolve("nope"), None);
        // upgrade the app-registry program (version must advance)
        assert!(
            s.set("app-registry", [9u8; 32], 1).is_err(),
            "stale version rejected"
        );
        let s2 = s.set("app-registry", [9u8; 32], 2).unwrap();
        assert_eq!(s2.resolve("app-registry"), Some([9u8; 32]));
        assert_ne!(s.root(), s2.root());
    }

    #[test]
    fn wasm_registry_matches_native() {
        use crate::AttestedRuntime;
        let rt = AttestedRuntime::new().unwrap();
        let publisher = id();
        let sub = HeadSubmission::sign(&publisher, "feed", [1u8; 32], 1);
        let prev = RegistryState::default();
        // Run the WASM registry over (prev_state, submission).
        let out = rt
            .run_transition(
                REGISTRY_WASM,
                "run",
                &prev.encode(),
                &sub.encode(),
                crate::DEFAULT_FUEL,
            )
            .expect("wasm runs");
        let wasm_state = RegistryState::decode(&out).expect("wasm output decodes as RegistryState");
        // It must equal the NATIVE transition, byte for byte.
        let native = prev.apply(&sub).unwrap();
        assert_eq!(wasm_state, native, "wasm registry == native registry");
        assert_eq!(
            wasm_state
                .resolve(&publisher.node_id().0, "feed")
                .unwrap()
                .cid,
            [1u8; 32]
        );
    }

    #[test]
    fn wasm_registry_rejects_a_forged_submission() {
        use crate::AttestedRuntime;
        let rt = AttestedRuntime::new().unwrap();
        let mut sub = HeadSubmission::sign(&id(), "feed", [1u8; 32], 1);
        sub.cid = [9u8; 32]; // break the signature
        let out = rt
            .run_transition(
                REGISTRY_WASM,
                "run",
                &[],
                &sub.encode(),
                crate::DEFAULT_FUEL,
            )
            .unwrap();
        assert!(out.is_empty(), "a bad signature commits nothing");
    }
}
