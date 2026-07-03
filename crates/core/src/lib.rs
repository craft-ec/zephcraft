//! Shared types for the Craftec node: content IDs, node IDs, core errors.
//!
//! Spec: `docs/craftec_technical_foundation.md` v3.7 (§17, §27),
//! `docs/CRAFTOBJ_DESIGN.md` v2.0 (§Content Model).

pub mod hlc;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Content identifier: the BLAKE3 hash of the content bytes (foundation §27).
///
/// Immutable, self-verifying, deduplicating. Everything stored in CraftOBJ —
/// files, database pages, vtags blobs, WASM programs — is addressed by a `Cid`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Cid(pub [u8; 32]);

impl Cid {
    /// Hash `bytes` into their content identifier.
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Verify that `bytes` hash to this CID.
    pub fn verifies(&self, bytes: &[u8]) -> bool {
        Self::of(bytes) == *self
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cid({}…)", &self.to_hex()[..12])
    }
}

impl fmt::Display for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Node identifier: the node's Ed25519 public key, verbatim (iroh convention,
/// foundation §17 — no hashing, the public key IS the identity).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl NodeId {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({}…)", &self.to_hex()[..12])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cid_is_deterministic() {
        assert_eq!(Cid::of(b"craftec"), Cid::of(b"craftec"));
    }

    #[test]
    fn cid_differs_by_content() {
        assert_ne!(Cid::of(b"craftec"), Cid::of(b"craftec!"));
    }

    #[test]
    fn cid_verifies_content() {
        let cid = Cid::of(b"store bytes, retrieve bytes");
        assert!(cid.verifies(b"store bytes, retrieve bytes"));
        assert!(!cid.verifies(b"tampered"));
    }
}
