//! Encrypted objects (ENCRYPTION_DESIGN.md phase 2).
//!
//! A private file is **chunk-then-encrypt**: the PLAINTEXT is split into ≤8 MiB
//! segments and EACH is sealed independently under one file DEK (per-segment AEAD),
//! then published as an opaque ciphertext object (erasure-coded like anything else).
//! A small **envelope** carries the DEK capsule + the ordered ciphertext-segment
//! list + the sealed name/mime. Because each segment is independently decryptable +
//! verifiable, a private file is **streamable/seekable** (fetch + decrypt only the
//! covering segments) — which a single whole-file AEAD tag (encrypt-then-chunk) would
//! preclude. The envelope is what a reader resolves (the "manifest" of a private
//! object); only a key holder decrypts. Crypto-shred = destroy the DEK (one per file
//! → every segment unreadable).

use serde::{Deserialize, Serialize};
use zeph_cipher::DekCapsule;

use crate::manifest::Segment;

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
    /// DEK encapsulated under the owner's PRE key (one DEK for the whole file).
    pub capsule: DekCapsule,
    /// Ordered ciphertext SEGMENTS: `cid` = BLAKE3(sealed segment), `len` = the
    /// segment's PLAINTEXT length. The file is the concatenation of the decrypted
    /// segments; identical plaintext segments dedup (deterministic per-segment seal).
    pub segments: Vec<Segment>,
    /// Sealed `PlainMeta {name, mime}` (under the DEK) — name/mime stay private, as
    /// they were inside the ciphertext before segmentation.
    pub meta: Vec<u8>,
    /// Total plaintext size (== sum of segment `len`s).
    pub size: u64,
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

/// The private name/mime of a file — sealed under the file DEK and stored in the
/// envelope's `meta`, so they stay hidden like the content (the content itself now
/// rides the separately-sealed segments, not this struct).
#[derive(Clone, Serialize, Deserialize)]
pub struct PlainMeta {
    pub name: String,
    pub mime: String,
}

impl PlainMeta {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("serialize plainmeta")
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}
