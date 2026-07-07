//! Cross-node program-registry protocol (`REGISTRY_ALPN`). Closes the offline-owner gap:
//! ONE designated writer holds the global registry account
//! (`pda(registry_program_cid(), REGISTRY_SEED)`); non-writer nodes forward registrations
//! and resolution queries to it over this ALPN. Mirrors the request/serve shape of the
//! (removed) attestation coordinator: the client opens a stream on the ALPN, postcard-encodes
//! the request, and reads back a postcard-encoded response.

use serde::{Deserialize, Serialize};
use zeph_transport::{PeerAddr, Transport};

/// ALPN for cross-node registry requests (forward-to-writer + query-writer).
pub const REGISTRY_ALPN: &[u8] = b"/craftec/registry/1";

/// Max size of a registry request/response frame.
const MAX_FRAME: usize = 256 * 1024;

/// A registry request forwarded to the writer. Every variant carries an `rtype` (the
/// registry KIND — [`zeph_com`]-style `RT_PROGRAM`/`RT_DBROOT`/`RT_MANIFEST`) so a request
/// routes to the account for that type: each `(rtype, shard)` is a distinct account.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryReq {
    /// Forward an owner-signed [`zeph_com::HeadSubmission`] (its `encode()` bytes) to the
    /// writer, which advances the `(rtype, shard)` registry account and returns the new root.
    Submit { rtype: u8, sub: Vec<u8> },
    /// Ask the writer to resolve `(owner, name)` to its current head cid, under `rtype`.
    Resolve {
        rtype: u8,
        owner: [u8; 32],
        name: String,
    },
    /// Ask a replica for the FULL current registry-account state bytes of `(rtype, shard)`.
    /// Used for the takeover MERGE: a node becoming the new epoch's writer fetches the other
    /// replicas' state and merges it before serving so registrations survive rotation.
    GetState { rtype: u8, shard: u64 },
    /// Ask the writer for the current version of `(owner, name)` under `rtype` (0 if
    /// unregistered) — so a non-writer can compute `prev + 1` without holding the shard.
    CurrentVersion {
        rtype: u8,
        owner: [u8; 32],
        name: String,
    },
    /// Push the writer's FULL `(rtype, shard)` state to a replica, which MERGES it (LWW) into
    /// its own copy. Sent on every write so the K-replica set stays warm.
    PushState {
        rtype: u8,
        shard: u64,
        state: Vec<u8>,
    },
}

/// The writer's response.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryResp {
    /// A `Submit` was applied; the new registry-account root.
    SubmitAck([u8; 32]),
    /// A `Resolve` result (`None` = not registered).
    Resolved(Option<[u8; 32]>),
    /// The full registry-account state bytes (empty = no state yet) — reply to `GetState`,
    /// used for the takeover merge.
    State(Vec<u8>),
    /// The current version of a `(owner, name)` key (0 if unregistered) — reply to
    /// `CurrentVersion`.
    Version(u64),
    /// A `PushState` was merged.
    Ack,
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
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let conn = transport.connect(addr, REGISTRY_ALPN).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&postcard::to_allocvec(req)?).await?;
        send.finish()?;
        let resp = recv.read_to_end(MAX_FRAME).await?;
        conn.close(0u32.into(), b"done");
        Ok(postcard::from_bytes(&resp)?)
    })
    .await
    .map_err(|_| anyhow::anyhow!("registry request timed out after {REQUEST_TIMEOUT:?}"))?
}
