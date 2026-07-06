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

/// A registry request forwarded to the writer.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryReq {
    /// Forward an owner-signed [`zeph_com::HeadSubmission`] (its `encode()` bytes) to the
    /// writer, which advances the global registry account and returns the new root.
    Submit(Vec<u8>),
    /// Ask the writer to resolve `(owner, name)` to its current head cid.
    Resolve { owner: [u8; 32], name: String },
    /// Ask a writer for the FULL current registry-account state bytes. Used for the
    /// per-epoch state handoff: a node becoming the new epoch's writer fetches the
    /// previous writer's state before serving so registrations survive rotation.
    GetState,
}

/// The writer's response.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegistryResp {
    /// A `Submit` was applied; the new registry-account root.
    SubmitAck([u8; 32]),
    /// A `Resolve` result (`None` = not registered).
    Resolved(Option<[u8; 32]>),
    /// The full registry-account state bytes (empty = no state yet) — reply to `GetState`,
    /// used for the per-epoch state handoff.
    State(Vec<u8>),
    /// The writer rejected/failed the request.
    Err(String),
}

/// Send a registry request to the writer at `addr` and read its response. Mirrors the
/// removed `request_attestation` client shape.
pub async fn request_registry(
    transport: &Transport,
    addr: &PeerAddr,
    req: &RegistryReq,
) -> anyhow::Result<RegistryResp> {
    let conn = transport.connect(addr, REGISTRY_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_allocvec(req)?).await?;
    send.finish()?;
    let resp = recv.read_to_end(MAX_FRAME).await?;
    conn.close(0u32.into(), b"done");
    Ok(postcard::from_bytes(&resp)?)
}
