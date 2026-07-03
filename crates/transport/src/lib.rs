//! Transport: iroh (QUIC) endpoint wrapper, ALPN registry, connect-by-NodeId,
//! and the ping/pong protocol used by the worker heartbeat.
//!
//! Spec: foundation §3 (iroh transport), §21–22 (connection lifecycle, ALPN).
//! iroh version pinned at 1.x (decision: pin at M1.2 integration).
//!
//! The node's Ed25519 identity IS the iroh secret key, so the zeph NodeId and
//! the iroh EndpointId are the same 32 bytes.

use std::time::Duration;

pub use iroh::endpoint::Connection;

use iroh::endpoint::presets;
use iroh::endpoint::RelayMode;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMap, RelayUrl, SecretKey, TransportAddr};
use zeph_core::hlc;
use zeph_core::{Cid, NodeId};
use zeph_wire as wire;

/// ALPN identifiers for zeph protocols (foundation §10, §22).
pub mod alpn {
    /// Heartbeat ping/pong (M1); superseded by wire frames in M1.4.
    pub const PING: &[u8] = b"/craftec/ping/1";
}

/// Maximum ping/pong frame size we will read (sanity bound).
const MAX_PING_FRAME: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("bind failed: {0}")]
    Bind(String),
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("ping timeout after {0:?}")]
    Timeout(Duration),
    #[error("peer echoed wrong nonce")]
    BadEcho,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("invalid peer address `{0}`: expected <node_id_hex>@<ip:port>[,<ip:port>...]")]
    InvalidPeerAddr(String),
}

pub type Result<T> = std::result::Result<T, TransportError>;

/// How this endpoint reaches the wider network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// Direct sockets only — no relays, no discovery. For tests and LAN.
    LocalOnly,
    /// Default iroh relay infrastructure + discovery (production).
    Relayed,
}

/// A peer's dialable address: identity plus optional direct socket addresses.
#[derive(Debug, Clone)]
pub struct PeerAddr(pub EndpointAddr);

impl PeerAddr {
    pub fn node_id(&self) -> NodeId {
        NodeId(*self.0.id.as_bytes())
    }
}

/// Text form: `<node_id_hex>@<ip:port>[,<ip:port>...]` — what `zeph` prints
/// on startup and what `--peer` / config files accept.
impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@", self.node_id().to_hex())?;
        let mut first = true;
        for addr in &self.0.addrs {
            let rendered = match addr {
                TransportAddr::Ip(sock) => sock.to_string(),
                TransportAddr::Relay(url) => url.to_string(),
                _ => continue,
            };
            if !first {
                write!(f, ",")?;
            }
            write!(f, "{rendered}")?;
            first = false;
        }
        Ok(())
    }
}

impl std::str::FromStr for PeerAddr {
    type Err = TransportError;

    fn from_str(s: &str) -> Result<Self> {
        let err = || TransportError::InvalidPeerAddr(s.to_string());
        let (id_hex, socks) = s.split_once('@').ok_or_else(err)?;
        let id_bytes: [u8; 32] = hex::decode(id_hex)
            .map_err(|_| err())?
            .try_into()
            .map_err(|_| err())?;
        let id = EndpointId::from_bytes(&id_bytes).map_err(|_| err())?;
        let mut addr = EndpointAddr::new(id);
        let mut any = false;
        for part in socks.split(',').filter(|p| !p.is_empty()) {
            if part.contains("://") {
                let url: iroh::RelayUrl = part.parse().map_err(|_| err())?;
                addr = addr.with_relay_url(url);
            } else {
                let sock: std::net::SocketAddr = part.parse().map_err(|_| err())?;
                addr = addr.with_ip_addr(sock);
            }
            any = true;
        }
        if !any {
            return Err(err());
        }
        Ok(PeerAddr(addr))
    }
}

/// Result of a ping round-trip: latency plus the peer's measured clock skew
/// (absolute ms difference between HLC wall parts; None if unmeasurable).
#[derive(Debug, Clone, Copy)]
pub struct PingReport {
    pub rtt: Duration,
    pub peer_skew_ms: u64,
}

/// The zeph transport: one iroh endpoint carrying all protocols via ALPN.
pub struct Transport {
    endpoint: Endpoint,
    clock: std::sync::Arc<hlc::Clock>,
}

impl Transport {
    /// Bind an endpoint using the node's Ed25519 identity as the QUIC secret
    /// key. `alpns` lists the protocols this node will ACCEPT. `port` fixes
    /// the UDP listen port (0 = OS-assigned) — servers behind a firewall need
    /// a fixed port to allow.
    ///
    /// Convenience wrapper over [`Self::bind_with_relays`] using default
    /// relay selection (n0 for `Reach::Relayed`).
    pub async fn bind(
        secret: [u8; 32],
        reach: Reach,
        alpns: Vec<Vec<u8>>,
        port: u16,
    ) -> Result<Self> {
        Self::bind_with_relays(secret, reach, alpns, port, Vec::new(), true).await
    }

    /// Bind with a custom relay list (decision M1.8, foundation §26): our
    /// own relay mesh first, n0's public relays appended as lowest-priority
    /// fallback when `fallback_relays` is true. Empty `relay_urls` +
    /// fallback = plain n0 defaults.
    pub async fn bind_with_relays(
        secret: [u8; 32],
        reach: Reach,
        alpns: Vec<Vec<u8>>,
        port: u16,
        relay_urls: Vec<RelayUrl>,
        fallback_relays: bool,
    ) -> Result<Self> {
        let secret_key = SecretKey::from_bytes(&secret);
        let mut builder = match reach {
            // Minimal: local sockets only — no relays, no address lookup.
            Reach::LocalOnly => Endpoint::builder(presets::Minimal),
            // N0 preset (discovery etc.); relay map possibly overridden below.
            Reach::Relayed => {
                let builder = Endpoint::builder(presets::N0);
                if relay_urls.is_empty() {
                    builder
                } else {
                    let mut urls = relay_urls;
                    if fallback_relays {
                        for host in [
                            iroh::defaults::prod::NA_EAST_RELAY_HOSTNAME,
                            iroh::defaults::prod::NA_WEST_RELAY_HOSTNAME,
                            iroh::defaults::prod::EU_RELAY_HOSTNAME,
                            iroh::defaults::prod::AP_RELAY_HOSTNAME,
                        ] {
                            if let Ok(url) = format!("https://{host}").parse::<RelayUrl>() {
                                if !urls.contains(&url) {
                                    urls.push(url);
                                }
                            }
                        }
                    }
                    builder.relay_mode(RelayMode::Custom(RelayMap::from_iter(urls)))
                }
            }
        }
        .secret_key(secret_key)
        .alpns(alpns);
        if port != 0 {
            builder = builder
                .bind_addr(std::net::SocketAddr::from(([0, 0, 0, 0], port)))
                .map_err(|e| TransportError::Bind(e.to_string()))?;
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| TransportError::Bind(e.to_string()))?;
        Ok(Self {
            endpoint,
            clock: std::sync::Arc::new(hlc::Clock::new()),
        })
    }

    /// The node's hybrid logical clock (shared across protocols).
    pub fn clock(&self) -> std::sync::Arc<hlc::Clock> {
        self.clock.clone()
    }

    /// This node's identity on the wire.
    pub fn node_id(&self) -> NodeId {
        NodeId(*self.endpoint.id().as_bytes())
    }

    /// This endpoint's current dialable address (direct socket addresses are
    /// available immediately after bind; relay info arrives asynchronously).
    pub fn addr(&self) -> PeerAddr {
        PeerAddr(self.endpoint.addr())
    }

    /// Access the underlying iroh endpoint (used by upper layers to register
    /// more protocols / accept loops).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Connect to a peer for a given ALPN.
    pub async fn connect(&self, peer: &PeerAddr, alpn: &[u8]) -> Result<Connection> {
        self.endpoint
            .connect(peer.0.clone(), alpn)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))
    }

    /// Round-trip a ping to `peer` over wire frames (M1.4); returns RTT + skew.
    pub async fn ping(&self, peer: &PeerAddr, timeout: Duration) -> Result<PingReport> {
        let fut = async {
            let conn = self.connect(peer, alpn::PING).await?;
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;

            let local_hlc = self.clock.now().0;
            // Fresh nonce per ping, derived without an RNG dependency.
            let nonce = Cid::of(&[&self.node_id().0[..], &local_hlc.to_be_bytes()[..]].concat()).0;
            let frame = wire::encode(&wire::Message::Ping(wire::Ping { nonce }), local_hlc);

            let started = std::time::Instant::now();
            send.write_all(&frame)
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            send.finish()
                .map_err(|e| TransportError::Stream(e.to_string()))?;

            let reply = recv
                .read_to_end(MAX_PING_FRAME)
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            let frame =
                wire::decode(&reply).map_err(|e| TransportError::Protocol(e.to_string()))?;
            let wire::Message::Pong(pong) = frame.message else {
                return Err(TransportError::Protocol("expected PONG".into()));
            };
            if pong.nonce != nonce {
                return Err(TransportError::BadEcho);
            }
            let merge = self.clock.merge(hlc::Timestamp(frame.hlc_ts));
            let peer_skew_ms = merge.skew_ms;
            if merge.clamped {
                tracing::warn!(skew_ms = merge.skew_ms, "peer clock far ahead (clamped)");
            }
            let rtt = started.elapsed();
            conn.close(0u32.into(), b"done");
            Ok(PingReport { rtt, peer_skew_ms })
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| TransportError::Timeout(timeout))?
    }

    /// Accept loop: route incoming connections to per-ALPN handlers.
    /// Connections with an unregistered ALPN are closed. Runs until the
    /// endpoint closes. Spawn this on the runtime.
    pub async fn serve(&self, handlers: Vec<(Vec<u8>, tokio::sync::mpsc::Sender<Connection>)>) {
        let handlers = std::sync::Arc::new(handlers);
        while let Some(incoming) = self.endpoint.accept().await {
            let handlers = handlers.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let alpn = conn.alpn().to_vec();
                match handlers.iter().find(|(a, _)| *a == alpn) {
                    Some((_, tx)) => {
                        if tx.send(conn).await.is_err() {
                            // handler gone; nothing to do
                        }
                    }
                    None => conn.close(1u32.into(), b"unknown alpn"),
                }
            });
        }
    }

    /// Serve ping/pong only (convenience for tests and minimal nodes).
    pub async fn serve_ping(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let clock = self.clock();
        let serve = self.serve(vec![(alpn::PING.to_vec(), tx)]);
        let dispatch = async move {
            while let Some(conn) = rx.recv().await {
                tokio::spawn(Self::handle_ping_conn(clock.clone(), conn));
            }
        };
        tokio::join!(serve, dispatch);
    }

    /// Handle one inbound ping connection: echo framed pings until it closes.
    pub async fn handle_ping_conn(clock: std::sync::Arc<hlc::Clock>, conn: Connection) {
        {
            let peer = NodeId(*conn.remote_id().as_bytes()).to_hex();
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(bytes) = recv.read_to_end(MAX_PING_FRAME).await else {
                    return;
                };
                let frame = match wire::decode(&bytes) {
                    Ok(frame) => frame,
                    Err(err) => {
                        tracing::warn!(peer = %&peer[..12], %err, "bad ping frame");
                        return;
                    }
                };
                let wire::Message::Ping(ping) = frame.message else {
                    tracing::warn!(peer = %&peer[..12], "unexpected message on ping alpn");
                    return;
                };
                let merge = clock.merge(hlc::Timestamp(frame.hlc_ts));
                if merge.clamped {
                    tracing::warn!(peer = %&peer[..12], skew_ms = merge.skew_ms, "peer clock far ahead (clamped)");
                }
                let reply = wire::encode(
                    &wire::Message::Pong(wire::Pong { nonce: ping.nonce }),
                    clock.now().0,
                );
                if send.write_all(&reply).await.is_err() {
                    return;
                }
                let _ = send.finish();
                tracing::info!(peer = %&peer[..12], "ping served");
            }
        }
    }

    /// Gracefully close all connections.
    pub async fn close(&self) {
        self.endpoint.close().await;
    }
}

/// Convert a zeph NodeId to an iroh EndpointId (for dialing by id alone once
/// discovery exists).
pub fn endpoint_id(node_id: &NodeId) -> Option<EndpointId> {
    EndpointId::from_bytes(&node_id.0).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_crypto::NodeIdentity;

    #[test]
    fn peer_addr_parse_display_roundtrip() {
        let id = NodeIdentity::generate();
        let text = format!("{}@127.0.0.1:4433,192.168.1.7:9000", id.node_id().to_hex());
        let parsed: PeerAddr = text.parse().unwrap();
        assert_eq!(parsed.node_id(), id.node_id());
        let rendered = parsed.to_string();
        let reparsed: PeerAddr = rendered.parse().unwrap();
        assert_eq!(reparsed.node_id(), id.node_id());
        assert_eq!(reparsed.0.addrs, parsed.0.addrs);
    }

    #[test]
    fn peer_addr_rejects_garbage() {
        for bad in ["", "nothex@1.2.3.4:1", "aabb@", "@1.2.3.4:1", "deadbeef"] {
            assert!(bad.parse::<PeerAddr>().is_err(), "should reject {bad}");
        }
    }

    /// M1.4: a malformed frame is rejected server-side, and a well-formed
    /// ping still succeeds on the same connection setup afterwards.
    #[tokio::test]
    async fn ping_rejects_garbage_then_still_serves() {
        let server_id = NodeIdentity::generate();
        let server = Transport::bind(
            server_id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![alpn::PING.to_vec()],
            0,
        )
        .await
        .unwrap();
        let server_addr = server.addr();
        let server = std::sync::Arc::new(server);
        let serve = {
            let server = server.clone();
            tokio::spawn(async move { server.serve_ping().await })
        };

        let client_id = NodeIdentity::generate();
        let client = Transport::bind(client_id.secret_key_bytes(), Reach::LocalOnly, vec![], 0)
            .await
            .unwrap();

        // Hand-roll a garbage payload on the ping ALPN: no PONG must come back.
        let conn = client.connect(&server_addr, alpn::PING).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"not a frame").await.unwrap();
        send.finish().unwrap();
        let reply = recv.read_to_end(MAX_PING_FRAME).await.unwrap_or_default();
        assert!(reply.is_empty(), "server must not answer garbage");
        conn.close(0u32.into(), b"done");

        // A proper framed ping still works.
        let report = client
            .ping(&server_addr, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(report.rtt > Duration::ZERO);

        client.close().await;
        server.close().await;
        serve.abort();
    }

    /// M1.2 GATE: two endpoints connect and round-trip a ping.
    #[tokio::test]
    async fn two_endpoints_ping_pong() {
        let server_id = NodeIdentity::generate();
        let client_id = NodeIdentity::generate();

        let server = Transport::bind(
            server_id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![alpn::PING.to_vec()],
            0,
        )
        .await
        .unwrap();
        // Identity flows through: iroh EndpointId == zeph NodeId.
        assert_eq!(server.node_id(), server_id.node_id());

        let server_addr = server.addr();
        let server = std::sync::Arc::new(server);
        let serve = {
            let server = server.clone();
            tokio::spawn(async move { server.serve_ping().await })
        };

        let client = Transport::bind(client_id.secret_key_bytes(), Reach::LocalOnly, vec![], 0)
            .await
            .unwrap();

        let report = client
            .ping(&server_addr, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(report.rtt > Duration::ZERO);
        assert!(report.peer_skew_ms < 5_000, "local clocks agree");

        // Second ping over a fresh connection also works.
        let report2 = client
            .ping(&server_addr, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(report2.rtt > Duration::ZERO);

        client.close().await;
        server.close().await;
        serve.abort();
    }
}
