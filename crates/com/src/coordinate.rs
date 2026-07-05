//! Phase 3b — attestation coordination over the wire (`ATTEST_ALPN`). This is the
//! foundation §41 flow made live (the deferred `ATTEST_BROADCAST`, §1042): a
//! coordinator asks each committee member to attest a deterministic program run, the
//! members return their own signed [`Attestation`], and the coordinator collects a
//! k-of-n quorum into an [`AttestedCommit`].
//!
//! The coordinator is **untrusted** — it only gathers independently-signed
//! attestations; it cannot forge one, and it cannot advance state without a real
//! quorum of committee members agreeing on the same deterministic output.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::{
    attest_transition, verify_quorum, Attestation, AttestedCommit, AttestedRuntime, Committee,
    DEFAULT_FUEL,
};

/// ALPN for attestation requests.
pub const ATTEST_ALPN: &[u8] = b"/craftec/attest/1";

/// ALPN for committee-chain endorsement requests (epoch rollover).
pub const ENDORSE_ALPN: &[u8] = b"/craftec/endorse/1";

/// A proposal to endorse the next epoch's committee: recompute it from your own
/// membership and, if it matches, sign the hand-off.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct EndorseRequest {
    pub epoch: u64,
    pub committee_members: Vec<[u8; 32]>,
    pub k: usize,
    pub prev_hash: [u8; 32],
}

/// Ask ONE member of the outgoing committee to endorse the proposed next committee.
pub async fn request_endorsement(
    transport: &Transport,
    addr: &PeerAddr,
    req: &EndorseRequest,
) -> anyhow::Result<crate::Endorsement> {
    let conn = transport.connect(addr, ENDORSE_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_allocvec(req)?).await?;
    send.finish()?;
    let resp = recv.read_to_end(64 * 1024).await?;
    conn.close(0u32.into(), b"done");
    let reply: Option<crate::Endorsement> = postcard::from_bytes(&resp)?;
    reply.ok_or_else(|| anyhow::anyhow!("member declined to endorse"))
}

/// A request to attest a deterministic program run: which program (by CID), which
/// export, the prior state root it builds on, and the request bytes.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct AttestRequest {
    pub program_cid: [u8; 32],
    pub prev_root: [u8; 32],
    pub func: String,
    #[serde(default)]
    pub request: Vec<u8>,
    /// For NATIVE programs: the prior state the transition runs on. Its hash must equal
    /// `prev_root` (checked by the agent). WASM programs ignore this — their prior state
    /// is bound only through `prev_root`.
    #[serde(default)]
    pub prev_state: Vec<u8>,
}

/// A NATIVE network-owned program the attestation service runs deterministically:
/// `(prev_state, request) → new_state`. Its code is the node's own (identical on every
/// agent), so it needn't run through the WASM sandbox (foundation mechanism/policy split).
pub trait NativeProgram: Send + Sync {
    fn program_cid(&self) -> [u8; 32];
    fn run(&self, prev_state: &[u8], request: &[u8]) -> anyhow::Result<Vec<u8>>;
}

/// Attests deterministic program runs on THIS node: loads the program by CID, runs it
/// under the restricted deterministic ABI, and returns this node's signed
/// [`Attestation`]. The attestor is this node's own identity (it signs) — so, unlike
/// invocation, there is no caller identity.
pub struct AttestService {
    runtime: AttestedRuntime,
    obj: Arc<ObjEngine>,
    identity: Arc<NodeIdentity>,
    native: Vec<Arc<dyn NativeProgram>>,
}

impl AttestService {
    pub fn new(runtime: AttestedRuntime, obj: Arc<ObjEngine>, identity: Arc<NodeIdentity>) -> Self {
        Self {
            runtime,
            obj,
            identity,
            native: Vec::new(),
        }
    }

    /// Register a native network-owned program (e.g. the registry) this node will attest.
    pub fn with_native(mut self, program: Arc<dyn NativeProgram>) -> Self {
        self.native.push(program);
        self
    }

    /// Load the program by CID (following a File manifest to its content, like invoke)
    /// and produce this node's attestation of the deterministic run.
    pub async fn attest(&self, req: &AttestRequest) -> anyhow::Result<Attestation> {
        // Native network-owned program (e.g. the registry): run the transition on the
        // supplied prior state (whose hash must match prev_root) — no WASM sandbox.
        if let Some(program) = self
            .native
            .iter()
            .find(|p| p.program_cid() == req.program_cid)
        {
            anyhow::ensure!(
                Cid::of(&req.prev_state).0 == req.prev_root,
                "prev_state does not hash to prev_root"
            );
            let new_state = program.run(&req.prev_state, &req.request)?;
            return Ok(attest_transition(
                &self.identity,
                req.program_cid,
                req.prev_root,
                &req.request,
                &new_state,
            ));
        }
        let raw = self
            .obj
            .get(Cid(req.program_cid), ConsumeMode::Drop)
            .await?;
        let wasm = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await?
            }
            _ => raw,
        };
        anyhow::ensure!(
            req.prev_state.is_empty() || Cid::of(&req.prev_state).0 == req.prev_root,
            "prev_state does not hash to prev_root"
        );
        let output = self.runtime.run_transition(
            &wasm,
            &req.func,
            &req.prev_state,
            &req.request,
            DEFAULT_FUEL,
        )?;
        Ok(attest_transition(
            &self.identity,
            req.program_cid,
            req.prev_root,
            &req.request,
            &output,
        ))
    }
}

/// Serve attestation requests. Each reply is this node's signed attestation (or `None`
/// if it declines / errors), postcard-encoded.
pub async fn serve_attestations(
    mut conns: mpsc::Receiver<Connection>,
    service: Arc<AttestService>,
) {
    while let Some(conn) = conns.recv().await {
        let service = service.clone();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(bytes) = recv.read_to_end(64 * 1024).await else {
                    break;
                };
                let reply: Option<Attestation> = match postcard::from_bytes::<AttestRequest>(&bytes)
                {
                    Ok(req) => service.attest(&req).await.ok(),
                    Err(_) => None,
                };
                let out = postcard::to_allocvec(&reply).unwrap_or_default();
                let _ = send.write_all(&out).await;
                let _ = send.finish();
            }
        });
    }
}

/// Ask ONE committee member to attest the run.
pub async fn request_attestation(
    transport: &Transport,
    addr: &PeerAddr,
    req: &AttestRequest,
) -> anyhow::Result<Attestation> {
    let conn = transport.connect(addr, ATTEST_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_allocvec(req)?).await?;
    send.finish()?;
    let resp = recv.read_to_end(64 * 1024).await?;
    conn.close(0u32.into(), b"done");
    let reply: Option<Attestation> = postcard::from_bytes(&resp)?;
    reply.ok_or_else(|| anyhow::anyhow!("member declined to attest"))
}

/// Coordinate a commit: fan out the run to the committee's members (concurrently),
/// collect their attestations, and bundle a k-of-n quorum into an [`AttestedCommit`].
/// Returns `None` if the quorum is not reached. `members[i]` is the address of a
/// committee member; verification is against `committee` (its member set + `k`), so an
/// address that isn't a committee member simply doesn't count toward the quorum.
#[allow(clippy::too_many_arguments)]
pub async fn collect_commit(
    transport: &Arc<Transport>,
    members: &[PeerAddr],
    committee: &Committee,
    program_cid: [u8; 32],
    seed: Vec<u8>,
    prev_root: [u8; 32],
    func: &str,
    request: Vec<u8>,
    prev_state: Vec<u8>,
) -> Option<AttestedCommit> {
    let req = AttestRequest {
        program_cid,
        prev_root,
        func: func.to_string(),
        request: request.clone(),
        prev_state,
    };
    let mut set = tokio::task::JoinSet::new();
    for addr in members {
        let t = transport.clone();
        let a = addr.clone();
        let r = req.clone();
        set.spawn(async move { request_attestation(&t, &a, &r).await.ok() });
    }
    let mut attestations = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(Some(att)) = res {
            attestations.push(att);
        }
    }
    let request_hash = Cid::of(&request).0;
    let new_root = verify_quorum(
        &attestations,
        &program_cid,
        &prev_root,
        &request_hash,
        &committee.members,
        committee.k,
    )?;
    Some(AttestedCommit {
        program_cid,
        seed,
        prev_root,
        request,
        new_root,
        attestations,
    })
}
