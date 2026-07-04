//! Phase 4 — invocation. Load a WASM app by CID from CraftOBJ and run it with a
//! caller identity against the node's [`CraftBackend`]. Two entry points:
//! - **local**: the node owner runs an app on their own space (caller = own).
//! - **remote**: over the [`INVOKE_ALPN`], a peer invokes an app on this node; the
//!   caller is the QUIC-authenticated peer NodeId — no separate auth needed.
//!
//! The agent always runs against THIS node's identity-bound backend (it writes this
//! node's `(own, app.ns)`), but it knows WHO called via the `caller` host function —
//! the federated request pattern.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use zeph_core::Cid;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::{AppBackend, HostCtx, Outcome, Runtime, DEFAULT_FUEL};

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
    runtime: Runtime,
    obj: Arc<ObjEngine>,
    backend: Arc<dyn AppBackend>,
}

impl InvokeService {
    pub fn new(runtime: Runtime, obj: Arc<ObjEngine>, backend: Arc<dyn AppBackend>) -> Self {
        Self {
            runtime,
            obj,
            backend,
        }
    }

    /// Load the app's WASM by CID and run `func` with the given `caller`. The CID
    /// may be a raw-content CID OR a file manifest CID (what `zeph publish` prints)
    /// — a File manifest is followed to its content, so publishing an `app.wasm`
    /// just works.
    pub async fn invoke(&self, req: &InvokeRequest, caller: [u8; 32]) -> anyhow::Result<Outcome> {
        let raw = self.obj.get(Cid(req.wasm_cid), ConsumeMode::Drop).await?;
        let wasm = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await?
            }
            _ => raw,
        };
        let ctx = HostCtx {
            caller,
            app_ns: req.app_ns.clone(),
            backend: self.backend.clone(),
            input: req.input.clone(),
        };
        self.runtime
            .invoke(&wasm, &req.func, ctx, DEFAULT_FUEL)
            .await
    }
}

/// Serve remote invocations. Each connection's caller is its QUIC-authenticated
/// NodeId — the identity is verified by the transport, so the agent's `caller` host
/// function is trustworthy with no extra auth layer.
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
                let value = match postcard::from_bytes::<InvokeRequest>(&bytes) {
                    Ok(req) => service
                        .invoke(&req, caller)
                        .await
                        .map(|o| o.value)
                        .unwrap_or(-1),
                    Err(_) => -1,
                };
                let _ = send.write_all(&value.to_le_bytes()).await;
                let _ = send.finish();
            }
        });
    }
}

/// Invoke an app on a remote node; returns the agent's `i64` result.
pub async fn invoke_remote(
    transport: &Transport,
    addr: &PeerAddr,
    req: &InvokeRequest,
) -> anyhow::Result<i64> {
    let conn = transport.connect(addr, INVOKE_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_allocvec(req)?).await?;
    send.finish()?;
    let resp = recv.read_to_end(64).await?;
    conn.close(0u32.into(), b"done");
    anyhow::ensure!(resp.len() == 8, "malformed invoke response");
    Ok(i64::from_le_bytes(resp.try_into().unwrap()))
}
