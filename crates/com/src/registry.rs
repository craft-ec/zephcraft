//! The app-name registry: an OPEN, owner-signed head registry on the program-account
//! substrate (`REGISTRY_DESIGN.md` §0). A publisher submits a SIGNED head `(owner, name) →
//! (cid, version)`; the registry *transition* validates it (owner signature + monotonic
//! version) and upserts it into a canonical state. The state advances by running the
//! transition deterministically on `(prev_state, submission)` — no committee, no
//! attestation: the owner's signature is the sole write authority for their own key, and
//! determinism makes every node compute the identical new root.
//!
//! This module is the registry's **logic** — a pure, deterministic state machine, so
//! every node computes the identical result. It runs either as the native
//! [`RegistryState::apply`] (the genesis default) or as a governance-set WASM program with
//! the same semantics. The single guard is the owner signature on the submission (no one
//! forges your mapping); the append is agreed simply because the transition is
//! deterministic.

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

/// One registry row — the current head for a `(owner, name)` key, CARRYING the owner's signature so
/// it can be re-verified on merge (a forged head can't propagate through replication) and on read
/// (a resolver never trusts an unverified head — closes the trust-on-announce gap).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeadEntry {
    pub owner: [u8; 32],
    pub name: String,
    pub cid: [u8; 32],
    pub version: u64,
    /// The owner's ed25519 signature over `head_signing_bytes(owner, name, cid, version)` — the
    /// SAME bytes [`HeadSubmission`] signs, so an entry is a submission minus the redundant fields.
    pub signature: Vec<u8>,
}

impl HeadEntry {
    /// Verify the owner actually signed this `(owner, name, cid, version)`. The sole read/merge
    /// authority check — `owner` is the pubkey, so a valid signature proves the owner authored it.
    pub fn verify(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            return false;
        };
        let msg = head_signing_bytes(&self.owner, &self.name, &self.cid, self.version);
        NodeIdentity::verify(&NodeId(self.owner), &msg, &sig)
    }
}

/// The registry state: the canonical (sorted by `(owner, name)`) set of current heads —
/// one row per key, latest version. Content-hashed to a `root`; the root is the account's
/// state identity, recomputed deterministically as the registry advances.
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
            // Persist the (already-verified) signature so every downstream merge + read can re-verify.
            signature: sub.signature.clone(),
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

    /// Merge `other` into self — last-writer-wins per `(owner, name)`: for each entry in
    /// `other`, if self lacks that key OR `other`'s version is strictly greater, take
    /// `other`'s entry. The result is the union of keys, each held at its highest version.
    /// Order-independent (a CRDT join), so replicas converge no matter the merge order —
    /// this is what lets K replicas hold the same shard state and a promoted replica catch
    /// up on takeover.
    pub fn merge(&mut self, other: &RegistryState) {
        for e in &other.entries {
            // VERIFY before accepting: a replica/writer cannot inject a forged or unsigned head into
            // the shard state — the owner signature is checked at every merge, not just the origin
            // write. This is what makes replication trustworthy (closes the trust-on-announce gap).
            if !e.verify() {
                continue;
            }
            match self.find(&e.owner, &e.name) {
                Ok(i) => {
                    if e.version > self.entries[i].version {
                        self.entries[i] = e.clone();
                    }
                }
                Err(i) => self.entries.insert(i, e.clone()),
            }
        }
    }

    /// Merge a batch of raw entries (same LWW-per-`(owner, name)` join as [`Self::merge`]) —
    /// used by the registry's online reshard to move heads from an OLD shard-count generation's
    /// accounts into a NEW generation's account. Idempotent, so re-running the reshard converges.
    pub fn merge_entries(&mut self, entries: impl IntoIterator<Item = HeadEntry>) {
        for e in entries {
            // Verify (as in `merge`) — a reshard must not launder an unsigned/forged head.
            if !e.verify() {
                continue;
            }
            match self.find(&e.owner, &e.name) {
                Ok(i) => {
                    if e.version > self.entries[i].version {
                        self.entries[i] = e;
                    }
                }
                Err(i) => self.entries.insert(i, e),
            }
        }
    }
}

/// The well-known program CID of the app-name registry — a NATIVE network-owned
/// program (its logic is [`RegistryState::apply`]).
pub fn registry_program_cid() -> [u8; 32] {
    Cid::of(b"craftec/program/registry/1").0
}

/// The app-name registry compiled to WASM, kept as a **test fixture only** — the release
/// binary no longer embeds it. Genesis is the native `RegistryProgram`; upgrades are
/// published WASM chosen by governance (`publish-program` + `SetProgram`). Mirrors the
/// source in `apps/registry-wasm`.
#[cfg(test)]
const REGISTRY_WASM: &[u8] = include_bytes!("../registry.wasm");

/// A NATIVE network-owned program run deterministically: `(prev_state, request) →
/// new_state`. Its code is the node's own (identical on every node), so it needn't run
/// through the WASM sandbox — the anchor runtime runs it directly (foundation
/// mechanism/policy split).
pub trait NativeProgram: Send + Sync {
    fn program_cid(&self) -> [u8; 32];
    fn run(&self, prev_state: &[u8], request: &[u8]) -> anyhow::Result<Vec<u8>>;
}

/// The registry as a [`NativeProgram`] each node runs LOCALLY: decode the prior state,
/// apply the signed submission, re-encode. Deterministic, so every node computes the
/// identical new state.
pub struct RegistryProgram;

impl NativeProgram for RegistryProgram {
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

/// The **config registry** state: `protocol config key → (value, version)`. Mirrors
/// [`ProgramRegistryState`] but for governed INTEGER config (e.g. the registry's shard-count
/// exponent `shard_bits`). Its writes are authorized by governance (a `SetConfig` approval);
/// every node folds the same governance chain to derive the identical map, so a config value is
/// cluster-agreed with no gossip. Empty by default — an unset key resolves to `None` so the
/// consumer applies its built-in default.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigRegistryState {
    /// (key, value, version) sorted by key — one entry per config key.
    values: Vec<(String, i64, u64)>,
}

impl ConfigRegistryState {
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
    pub fn entries(&self) -> &[(String, i64, u64)] {
        &self.values
    }

    /// The current value for a protocol config key (`None` if unset).
    pub fn resolve(&self, key: &str) -> Option<i64> {
        self.values
            .binary_search_by(|(k, _, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.values[i].1)
    }

    /// Set a config value (upsert). Version must strictly increase for an existing key.
    /// Returns the new state, or an error.
    pub fn set(
        &self,
        key: &str,
        value: i64,
        version: u64,
    ) -> Result<ConfigRegistryState, &'static str> {
        let mut next = self.clone();
        match next
            .values
            .binary_search_by(|(k, _, _)| k.as_str().cmp(key))
        {
            Ok(i) => {
                if version <= next.values[i].2 {
                    return Err("stale-version");
                }
                next.values[i] = (key.to_string(), value, version);
            }
            Err(i) => next.values.insert(i, (key.to_string(), value, version)),
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
    fn config_registry_upserts_resolves_and_rejects_stale() {
        let s = ConfigRegistryState::default();
        assert_eq!(s.resolve("shard_bits"), None, "unset key resolves to None");
        let s = s.set("shard_bits", 8, 1).unwrap();
        assert_eq!(s.resolve("shard_bits"), Some(8));
        // strictly-increasing version upserts
        let s = s.set("shard_bits", 9, 2).unwrap();
        assert_eq!(s.resolve("shard_bits"), Some(9));
        // same/older version is rejected (monotonic, like the program registry)
        assert_eq!(s.set("shard_bits", 10, 2), Err("stale-version"));
        assert_eq!(s.set("shard_bits", 10, 1), Err("stale-version"));
        // keys are isolated + sorted
        let s = s.set("another", 3, 1).unwrap();
        assert_eq!(s.resolve("another"), Some(3));
        assert_eq!(s.resolve("shard_bits"), Some(9));
        assert_eq!(ConfigRegistryState::decode(&s.encode()), Some(s));
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
    fn merge_keeps_higher_version_and_unions_keys() {
        let a = id();
        let b = id();
        // self holds `a/feed@1` and `a/blog@2`.
        let mut lhs = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 1))
            .unwrap()
            .apply(&HeadSubmission::sign(&a, "blog", [3u8; 32], 2))
            .unwrap();
        // other holds a NEWER `a/feed@5`, an OLDER `a/blog@1`, and a key self lacks `b/feed@1`.
        let rhs = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [9u8; 32], 5))
            .unwrap()
            .apply(&HeadSubmission::sign(&a, "blog", [8u8; 32], 1))
            .unwrap()
            .apply(&HeadSubmission::sign(&b, "feed", [2u8; 32], 1))
            .unwrap();
        lhs.merge(&rhs);
        // higher version wins per key
        let feed = lhs.resolve(&a.node_id().0, "feed").unwrap();
        assert_eq!((feed.cid, feed.version), ([9u8; 32], 5), "newer feed wins");
        // older push does NOT clobber the higher local version
        let blog = lhs.resolve(&a.node_id().0, "blog").unwrap();
        assert_eq!((blog.cid, blog.version), ([3u8; 32], 2), "higher blog kept");
        // union of keys: the key only other had is now present
        assert_eq!(lhs.resolve(&b.node_id().0, "feed").unwrap().cid, [2u8; 32]);
        assert_eq!(lhs.len(), 3, "union of keys");

        // merge is order-independent → both replicas converge to the same root
        let mut rhs2 = rhs.clone();
        rhs2.merge(
            &RegistryState::default()
                .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 1))
                .unwrap()
                .apply(&HeadSubmission::sign(&a, "blog", [3u8; 32], 2))
                .unwrap(),
        );
        assert_eq!(
            lhs.root(),
            rhs2.root(),
            "merge converges regardless of order"
        );
    }

    #[test]
    fn merge_rejects_a_forged_or_unsigned_entry() {
        let a = id();
        // An honest, validly-signed head.
        let honest = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 1))
            .unwrap();

        // A FORGED entry a malicious replica pushes: the owner's name but a cid they never signed,
        // at a higher version so it WOULD win LWW — the signature still binds the OLD cid, so it fails.
        let mut forged = honest.resolve(&a.node_id().0, "feed").unwrap().clone();
        forged.cid = [9u8; 32];
        forged.version = 99;
        let forged_state = RegistryState {
            entries: vec![forged],
        };
        let mut victim = honest.clone();
        victim.merge(&forged_state);
        let head = victim.resolve(&a.node_id().0, "feed").unwrap();
        assert_eq!(
            (head.cid, head.version),
            ([1u8; 32], 1),
            "forged head rejected at merge — the honest head stands"
        );

        // An UNSIGNED entry (empty signature) is also rejected — nothing to trust.
        let unsigned = RegistryState {
            entries: vec![HeadEntry {
                owner: a.node_id().0,
                name: "x".into(),
                cid: [5u8; 32],
                version: 1,
                signature: vec![],
            }],
        };
        let mut v2 = RegistryState::default();
        v2.merge(&unsigned);
        assert!(v2.is_empty(), "unsigned entry rejected at merge");
    }

    #[test]
    fn stored_entry_carries_a_verifiable_signature() {
        let a = id();
        let state = RegistryState::default()
            .apply(&HeadSubmission::sign(&a, "feed", [1u8; 32], 1))
            .unwrap();
        let e = state.resolve(&a.node_id().0, "feed").unwrap();
        assert!(e.verify(), "a stored head re-verifies against the owner");
        let mut tampered = e.clone();
        tampered.cid = [2u8; 32];
        assert!(
            !tampered.verify(),
            "tampering the cid breaks the stored signature"
        );
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

    #[tokio::test]
    async fn wasm_registry_matches_native() {
        use crate::TransitionRuntime;
        let rt = TransitionRuntime::new().unwrap();
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
                &crate::CapabilityGrant::deterministic(),
            )
            .await
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

    #[tokio::test]
    async fn wasm_registry_v2_rejects_an_overlong_name() {
        use crate::TransitionRuntime;
        let rt = TransitionRuntime::new().unwrap();
        let long = "x".repeat(40); // > 32 bytes
        let sub = HeadSubmission::sign(&id(), &long, [1u8; 32], 1);
        let out = rt
            .run_transition(
                REGISTRY_WASM,
                "run",
                &[],
                &sub.encode(),
                crate::DEFAULT_FUEL,
                &crate::CapabilityGrant::deterministic(),
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "v2 rejects a name longer than 32 bytes");
    }

    // Phase 1 capability grant (COMPUTE_EXECUTION_DESIGN §5). registry-wasm imports
    // input/state/commit/ed25519_verify — exactly the deterministic profile → it
    // instantiates and runs (no behavior change).
    #[tokio::test]
    async fn wasm_registry_runs_under_the_deterministic_grant() {
        use crate::{CapabilityGrant, TransitionRuntime};
        let rt = TransitionRuntime::new().unwrap();
        let sub = HeadSubmission::sign(&id(), "feed", [1u8; 32], 1);
        let out = rt
            .run_transition(
                REGISTRY_WASM,
                "run",
                &[],
                &sub.encode(),
                crate::DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .expect("deterministic grant binds the imports registry-wasm needs");
        assert!(!out.is_empty(), "a valid submission commits a new state");
    }

    // THE GATE: drop `Commit` from the grant → the host fn is NOT bound, so registry-wasm's
    // `commit` import is unresolved and it FAILS to instantiate. Proves link-time gating: a
    // non-granted capability cannot be reached.
    #[tokio::test]
    async fn wasm_registry_without_commit_grant_fails_to_instantiate() {
        use crate::{Capability, CapabilityGrant, TransitionRuntime};
        let rt = TransitionRuntime::new().unwrap();
        let sub = HeadSubmission::sign(&id(), "feed", [1u8; 32], 1);
        assert!(
            rt.run_transition(
                REGISTRY_WASM,
                "run",
                &[],
                &sub.encode(),
                crate::DEFAULT_FUEL,
                &CapabilityGrant::deterministic().without(Capability::Commit),
            )
            .await
            .is_err(),
            "an unbound `commit` import must fail instantiation"
        );
    }

    #[tokio::test]
    async fn wasm_registry_rejects_a_forged_submission() {
        use crate::TransitionRuntime;
        let rt = TransitionRuntime::new().unwrap();
        let mut sub = HeadSubmission::sign(&id(), "feed", [1u8; 32], 1);
        sub.cid = [9u8; 32]; // break the signature
        let out = rt
            .run_transition(
                REGISTRY_WASM,
                "run",
                &[],
                &sub.encode(),
                crate::DEFAULT_FUEL,
                &crate::CapabilityGrant::deterministic(),
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "a bad signature commits nothing");
    }
}
