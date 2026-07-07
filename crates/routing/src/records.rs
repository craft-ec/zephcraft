//! Signed routing records (provider, want, meta, app).
//!
//! All records share one shape — a typed payload wrapped in a wire
//! `SignedRecord` (Ed25519 over `kind ‖ node_id ‖ payload ‖ hlc_ts`). Records
//! are re-verified by whoever consumes them, never trusted on relay.

use serde::{Deserialize, Serialize};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_wire::SignedRecord;

pub const KIND_PROVIDER: u8 = 1;
pub const KIND_WANT: u8 = 4;
pub const KIND_META: u8 = 5;
/// RESERVED (ENCRYPTION_DESIGN.md §sharing): a signed grant — "owner grants
/// recipient access to CID". The on-wire shape is fixed now so sharing (a CraftCOM
/// app doing proxy re-encryption) needs no routing rework. NOT yet enforced or
/// wired into the registry — enforcement is a CraftCOM concern, not the tracker's.
pub const KIND_GRANT: u8 = 8;
pub const KIND_APP: u8 = 9;

/// Payload of a KIND_GRANT record (RESERVED — see `KIND_GRANT`). Not yet enforced.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GrantPayload {
    /// The content this grant is for.
    pub cid: [u8; 32],
    /// Recipient's PRE public key (compressed).
    pub recipient: Vec<u8>,
    /// Re-encryption key fragments, added by the sharing app (empty = reserved).
    pub kfrags: Vec<u8>,
    /// Advancing sequence for revoke / supersede.
    pub seq: u64,
}

/// "I hold pieces for `cid`" — advisory piece_count, dialable `addr`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPayload {
    pub cid: [u8; 32],
    pub piece_count: u32,
    pub addr: String,
    /// This provider serves the whole CID from a pin (repair/fetch prefer it).
    pub pinned: bool,
}

/// "I want `cid` kept alive" — the WANT interest signal (no holding implied).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WantPayload {
    pub cid: [u8; 32],
}

/// Editable metadata envelope for a manifest CID — the `.torrent`-envelope
/// analog. Signed per-publisher; the manifest itself stays immutable, so this
/// never perturbs the CID (dedup-safe). `published_at` is set once and
/// preserved across edits; `comment` is freely editable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaPayload {
    pub cid: [u8; 32],
    /// Publisher's original publish time (unix millis), preserved across edits.
    pub published_at: u64,
    /// Free-text label/comment (editable; None = cleared).
    pub comment: Option<String>,
}

/// Bytes covered by the signature: kind ‖ node_id ‖ payload ‖ hlc_ts.
fn signing_bytes(kind: u8, node_id: &[u8; 32], payload: &[u8], hlc_ts: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 32 + payload.len() + 8);
    buf.push(kind);
    buf.extend_from_slice(node_id);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&hlc_ts.to_be_bytes());
    buf
}

/// Build a signed record from a typed payload.
pub fn sign<P: Serialize>(
    identity: &NodeIdentity,
    kind: u8,
    payload: &P,
    hlc_ts: u64,
) -> SignedRecord {
    let node_id = identity.node_id().0;
    let payload = postcard::to_allocvec(payload).expect("record payload serializes");
    let sig = identity.sign(&signing_bytes(kind, &node_id, &payload, hlc_ts));
    SignedRecord {
        kind,
        node_id,
        payload,
        hlc_ts,
        signature: sig.to_vec(),
    }
}

/// Verify a record's signature against its claimed node_id.
pub fn verify(record: &SignedRecord) -> bool {
    let Ok(sig): Result<[u8; 64], _> = record.signature.as_slice().try_into() else {
        return false;
    };
    let bytes = signing_bytes(record.kind, &record.node_id, &record.payload, record.hlc_ts);
    NodeIdentity::verify(&NodeId(record.node_id), &bytes, &sig)
}

/// Decode a provider payload from a verified record.
pub fn provider(record: &SignedRecord) -> Option<ProviderPayload> {
    (record.kind == KIND_PROVIDER)
        .then(|| postcard::from_bytes(&record.payload).ok())
        .flatten()
}

pub fn want(record: &SignedRecord) -> Option<WantPayload> {
    (record.kind == KIND_WANT)
        .then(|| postcard::from_bytes(&record.payload).ok())
        .flatten()
}

pub fn meta(record: &SignedRecord) -> Option<MetaPayload> {
    (record.kind == KIND_META)
        .then(|| postcard::from_bytes(&record.payload).ok())
        .flatten()
}

/// A CraftCOM app head: `(publisher, name) → (wasm_cid, version)`. Signed by the
/// publisher; highest `version` wins. This is what makes an app NAME resolvable
/// network-wide (and versioned), vs sharing a bare cid (CRAFTCOM_DESIGN §13).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppPayload {
    pub name: String,
    pub wasm_cid: [u8; 32],
    pub version: u64,
}

pub fn app(record: &SignedRecord) -> Option<AppPayload> {
    (record.kind == KIND_APP)
        .then(|| postcard::from_bytes(&record.payload).ok())
        .flatten()
}
