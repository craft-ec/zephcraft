//! Encrypted objects (ENCRYPTION_DESIGN.md phase 2).
//!
//! A private file is published as two objects: the **ciphertext** (opaque content,
//! erasure-coded like anything else) and a small **envelope** that carries the DEK
//! capsule + a pointer to the ciphertext. The network sees only these; only a key
//! holder decrypts. The envelope is what a reader resolves (the "manifest" of a
//! private object).

use serde::{Deserialize, Serialize};
use zeph_cipher::DekCapsule;

/// Magic prefix identifying an encrypted envelope (distinct from `MANIFEST_MAGIC`).
pub const ENVELOPE_MAGIC: &[u8] = b"ZENVELP1";

/// A sharing grant to a recipient — RESERVED (empty in v1). Enforcement (proxy
/// re-encryption) is a CraftCOM concern; this only fixes the on-wire shape so
/// sharing needs no format change later.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recipient {
    /// Recipient's PRE public key (compressed).
    pub recipient: Vec<u8>,
    /// Re-encryption key fragments (added by the sharing app). Empty until then.
    pub kfrags: Vec<u8>,
}

/// The public envelope for a private object.
#[derive(Clone, Serialize, Deserialize)]
pub struct EncryptedEnvelope {
    /// DEK encapsulated under the owner's PRE key.
    pub capsule: DekCapsule,
    /// CID of the ciphertext content object.
    pub ciphertext_cid: [u8; 32],
    /// Owner identity (capsule resolution / sharing).
    pub owner: [u8; 32],
    /// Sharing grants — empty in v1 (reserved).
    #[serde(default)]
    pub recipients: Vec<Recipient>,
}

impl EncryptedEnvelope {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = ENVELOPE_MAGIC.to_vec();
        out.extend_from_slice(&postcard::to_allocvec(self).expect("serialize envelope"));
        out
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes.strip_prefix(ENVELOPE_MAGIC)?).ok()
    }
    pub fn is_envelope(bytes: &[u8]) -> bool {
        bytes.starts_with(ENVELOPE_MAGIC)
    }
}

/// The plaintext payload of a private file (encrypted as one blob — name/mime are
/// hidden inside the ciphertext, not the envelope).
#[derive(Clone, Serialize, Deserialize)]
pub struct PlainFile {
    pub name: String,
    pub mime: String,
    pub content: Vec<u8>,
}

impl PlainFile {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialize plainfile")
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}
