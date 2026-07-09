//! Phase 4 — invocation. Load a WASM app by CID from CraftOBJ and run it with a
//! caller identity against the node's [`CraftBackend`]. Two entry points:
//! - **local**: the node owner runs an app on their own space (caller = own).
//! - **remote**: over the [`INVOKE_ALPN`], a peer invokes an app on this node; the
//!   caller is the QUIC-authenticated peer NodeId — no separate auth needed.
//!
//! The agent always runs against THIS node's identity-bound backend (it writes this
//! node's `(own, app.ns)`), but it knows WHO called via the `caller` host function —
//! the federated request pattern.
//!
//! Apps run on the unified [`TransitionRuntime`] under [`CapabilityGrant::full`] (they get
//! sql/obj/clock/caller): the app exports `run()` (no result) and declares its output via
//! the `commit` host function. The invocation result is those COMMITTED bytes.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use zeph_core::Cid;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::{AppBackend, CapabilityGrant, TransitionCtx, TransitionRuntime, DEFAULT_FUEL};

/// ALPN for remote app invocation.
pub const INVOKE_ALPN: &[u8] = b"/craftec/invoke/1";

/// An invocation request: which app namespace, which WASM (by CID), which export.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct InvokeRequest {
    pub app_ns: String,
    pub wasm_cid: [u8; 32],
    pub func: String,
    /// Opaque input bytes passed to the agent (via the `input` host fn).
    #[serde(default)]
    pub input: Vec<u8>,
}

/// Runs app invocations on THIS node: loads the WASM by CID from CraftOBJ and runs
/// it with a caller identity against the node's (identity-bound) backend.
pub struct InvokeService {
    runtime: TransitionRuntime,
    obj: Arc<ObjEngine>,
    backend: Arc<dyn AppBackend>,
}

impl InvokeService {
    pub fn new(
        runtime: TransitionRuntime,
        obj: Arc<ObjEngine>,
        backend: Arc<dyn AppBackend>,
    ) -> Self {
        Self {
            runtime,
            obj,
            backend,
        }
    }

    /// Load the app's WASM by CID and run `func` with the given `caller`, returning the
    /// bytes the app COMMITTED. The CID may be a raw-content CID OR a file manifest CID
    /// (what `zeph publish` prints) — a File manifest is followed to its content, so
    /// publishing an `app.wasm` just works. Apps run under [`CapabilityGrant::full`]
    /// (sql/obj/clock/caller) against this node's identity-bound backend; they have no
    /// account blob, so `prev_state` is empty.
    pub async fn invoke(&self, req: &InvokeRequest, caller: [u8; 32]) -> anyhow::Result<Vec<u8>> {
        let raw = self.obj.get(Cid(req.wasm_cid), ConsumeMode::Drop).await?;
        let wasm = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await?
            }
            _ => raw,
        };
        let ctx = TransitionCtx::new(
            Vec::new(), // apps have no account blob
            req.input.clone(),
            caller,
            req.app_ns.clone(),
            // Apps are non-consensus: `clock` reads the invoking node's own time (same source
            // as `wall_clock`). There is no agreed consensus timestamp for a one-off app run.
            self.backend.now_millis(),
            Some(self.backend.clone()),
        );
        self.runtime
            .run_program(
                &wasm,
                &req.func,
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full(),
            )
            .await
    }
}

/// Serve remote invocations. Each connection's caller is its QUIC-authenticated
/// NodeId — the identity is verified by the transport, so the agent's `caller` host
/// function is trustworthy with no extra auth layer.
///
/// Wire framing (response): a 1-byte status — `0x01` followed by the app's committed
/// output bytes on success, or a single `0x00` on error. The status byte makes an
/// empty-output success distinguishable from a failure.
pub async fn serve_invocations(mut conns: mpsc::Receiver<Connection>, service: Arc<InvokeService>) {
    while let Some(conn) = conns.recv().await {
        // The caller's identity, QUIC-authenticated by the transport (iroh
        // EndpointId == zeph NodeId — the same 32 bytes).
        let caller = *conn.remote_id().as_bytes();
        let service = service.clone();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(bytes) = recv.read_to_end(64 * 1024).await else {
                    break;
                };
                let mut resp = Vec::new();
                match postcard::from_bytes::<InvokeRequest>(&bytes) {
                    Ok(req) => match service.invoke(&req, caller).await {
                        Ok(out) => {
                            resp.push(0x01);
                            resp.extend_from_slice(&out);
                        }
                        Err(_) => resp.push(0x00),
                    },
                    Err(_) => resp.push(0x00),
                }
                let _ = send.write_all(&resp).await;
                let _ = send.finish();
            }
        });
    }
}

/// Invoke an app on a remote node; returns the bytes the app COMMITTED on success. The
/// response is framed as a 1-byte status (`0x01` = success, else error) followed by the
/// committed output.
pub async fn invoke_remote(
    transport: &Transport,
    addr: &PeerAddr,
    req: &InvokeRequest,
) -> anyhow::Result<Vec<u8>> {
    // Pooled connection: shared, never closed here; evict on failure so the
    // next invocation re-dials.
    let conn = transport.connect(addr, INVOKE_ALPN).await?;
    let fut = async {
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&postcard::to_allocvec(req)?).await?;
        send.finish()?;
        anyhow::Ok(recv.read_to_end(64 * 1024).await?)
    };
    let resp = match fut.await {
        Ok(resp) => resp,
        Err(err) => {
            transport.evict(addr, INVOKE_ALPN, &conn);
            return Err(err);
        }
    };
    match resp.split_first() {
        Some((0x01, out)) => Ok(out.to_vec()),
        _ => anyhow::bail!("remote invocation failed"),
    }
}
