//! Cross-node head-registry protocol (`REGISTRY_ALPN`). Closes the offline-owner gap:
//! the keyspace is split into `2^shard_bits` shards (a governed, cluster-agreed count — the
//! key-routed requests carry the submitter's `bits`), each backed by a per-shard CraftSQL DB
//! (namespace `reg_<rtype>_<bits>_<shard>`), and each shard's writer ROTATES among its stable
//! replica set. Non-writer nodes forward registrations and resolution queries to the shard's
//! current writer over this ALPN; replication is row-level (`PushState` normally carries a
//! 1-row state). The client opens a stream on the ALPN, postcard-encodes the request, and
//! reads back a postcard-encoded response.

use serde::{Deserialize, Serialize};
use zeph_transport::{tag, PeerAddr, Transport};

/// Max size of a registry request/response frame.
const MAX_FRAME: usize = 256 * 1024;

/// A registry request forwarded to the writer. Every variant carries an `rtype` (the
/// registry KIND — [`zeph_com`]-style `RT_PROGRAM`/`RT_DBROOT`/`RT_MANIFEST`) so a request
/// routes to the account for that type: each `(rtype, shard)` is a distinct account.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryReq {
    /// Forward an owner-signed [`zeph_com::HeadSubmission`] (its `encode()` bytes) to the
    /// writer, which advances the `(rtype, shard)` registry account and returns the new root.
    /// `bits` is the SUBMITTER's live shard-count exponent: the writer routes the key with the
    /// submitter's `bits`, not its own, so a `shard_bits` change in flight can't split-route a
    /// key (the writer and submitter always agree on the shard for one request).
    Submit { rtype: u8, bits: u32, sub: Vec<u8> },
    /// Ask the writer to resolve `(owner, name)` to its current head cid, under `rtype`. `bits`
    /// is the querier's live shard-count exponent (routed with, not the writer's — see `Submit`).
    Resolve {
        rtype: u8,
        bits: u32,
        owner: [u8; 32],
        name: String,
    },
    /// Ask a replica for the FULL current registry-account state bytes of `(rtype, bits, shard)`.
    /// `bits` is the shard-count generation, so the request addresses the right generation's
    /// account. Used for the takeover MERGE: a node becoming the new epoch's writer fetches the
    /// other replicas' state and merges it before serving so registrations survive rotation.
    GetState { rtype: u8, bits: u32, shard: u64 },
    /// Ask the writer for the current version of `(owner, name)` under `rtype` (0 if
    /// unregistered) — so a non-writer can compute `prev + 1` without holding the shard. `bits`
    /// is the querier's live shard-count exponent (routed with — see `Submit`).
    CurrentVersion {
        rtype: u8,
        bits: u32,
        owner: [u8; 32],
        name: String,
    },
    /// Push the writer's FULL `(rtype, bits, shard)` state to a replica, which MERGES it (LWW)
    /// into its own copy. `bits` is the shard-count generation the state belongs to. Sent on
    /// every write so the K-replica set stays warm.
    PushState {
        rtype: u8,
        bits: u32,
        shard: u64,
        state: Vec<u8>,
    },
    /// Return ALL of this node's local registry heads — every rtype, every shard it holds. Used
    /// to build the GLOBAL dashboard view: since each shard is K-replicated across the members,
    /// the union of every member's local heads is the complete registry. No fields.
    ListEntries,
}

/// One local head row on the wire — raw bytes (hex-encoded later, in control.rs). Carries its
/// `rtype` so the receiver can group it into programs / DB roots / manifests.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeadRowWire {
    pub rtype: u8,
    pub owner: [u8; 32],
    pub name: String,
    pub cid: [u8; 32],
    pub version: u64,
}

/// The writer's response.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryResp {
    /// A `Submit` was applied; the new registry-account root.
    SubmitAck([u8; 32]),
    /// A `Resolve` result: the current head `(cid, version)` (`None` = not registered). The
    /// version is surfaced so a version-aware caller (e.g. a `RootStore`) gets the head seq.
    Resolved(Option<([u8; 32], u64)>),
    /// The full registry-account state bytes (empty = no state yet) — reply to `GetState`,
    /// used for the takeover merge.
    State(Vec<u8>),
    /// The current version of a `(owner, name)` key (0 if unregistered) — reply to
    /// `CurrentVersion`.
    Version(u64),
    /// A `PushState` was merged.
    Ack,
    /// Every local registry head this node holds (reply to `ListEntries`) — raw-byte rows.
    Entries(Vec<HeadRowWire>),
    /// The writer rejected/failed the request.
    Err(String),
}

/// Overall deadline for a single registry round-trip. Bounds the connect+request+read so an
/// unreachable peer (e.g. a dead-but-not-yet-dropped writer/replica) fails and lets the caller
/// fall back to another replica, instead of hanging forever. Must be generous enough for a
/// SLOW-but-alive writer: a relay-only peer (behind NAT) needs QUIC-over-relay setup + a round
/// trip, which can exceed a few seconds — too tight a bound spuriously fails writes to it (a
/// register has no replica fallback). 8s tolerates relay latency while still bounding a dead
/// peer. (Deeper fix: prefer directly-reachable peers in the writer election — see progress.)
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

/// Send a registry request to the writer at `addr` and read its response. Mirrors the
/// removed `request_attestation` client shape. The whole network round-trip is bounded by
/// [`REQUEST_TIMEOUT`]: on elapse it returns an `Err` rather than hanging, so a briefly
/// unreachable peer never blocks a resolve/register/version query indefinitely.
pub async fn request_registry(
    transport: &Transport,
    addr: &PeerAddr,
    req: &RegistryReq,
) -> anyhow::Result<RegistryResp> {
    // Muxed request/reply (tag::REGISTRY) on the shared per-peer connection.
    // request_tagged evicts the mux connection on a stream failure; the whole
    // round-trip is bounded by REQUEST_TIMEOUT so a briefly unreachable peer
    // never blocks a resolve/register/version query indefinitely.
    let req_bytes = postcard::to_allocvec(req)?;
    let resp = tokio::time::timeout(
        REQUEST_TIMEOUT,
        transport.request_tagged(addr, tag::REGISTRY, &req_bytes, MAX_FRAME),
    )
    .await
    .map_err(|_| anyhow::anyhow!("registry request timed out after {REQUEST_TIMEOUT:?}"))?
    .map_err(|e| anyhow::anyhow!("registry request failed: {e}"))?;
    Ok(postcard::from_bytes::<RegistryResp>(&resp)?)
}
