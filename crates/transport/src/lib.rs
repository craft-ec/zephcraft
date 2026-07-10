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

/// Everything needed to rebuild the endpoint identically on [`Transport::rebind`].
struct BindCfg {
    secret: [u8; 32],
    reach: Reach,
    alpns: Vec<Vec<u8>>,
    port: u16,
    relay_urls: Vec<RelayUrl>,
    fallback_relays: bool,
}

/// The zeph transport: one iroh endpoint carrying all protocols via ALPN.
///
/// The endpoint handle lives behind a lock so [`Self::rebind`] can swap in a
/// fresh one: a long-lived endpoint can wedge after uplink path churn (stale
/// QUIC path state — every dial fails while the raw network is fine), and the
/// only recovery is a rebuild. Methods clone the handle out of the lock and
/// never hold it across an await.
pub struct Transport {
    cfg: BindCfg,
    endpoint: std::sync::RwLock<Endpoint>,
    /// Bumped on every successful rebind — [`Self::serve`] loops watch it to
    /// re-attach their accept loop to the new endpoint.
    epoch: std::sync::atomic::AtomicU64,
    /// Set by [`Self::close`] so serve loops exit instead of awaiting a swap.
    closed: std::sync::atomic::AtomicBool,
    /// Serializes rebinds; a concurrent caller waits, then finds a fresh epoch.
    rebind_lock: tokio::sync::Mutex<()>,
    /// Pooled connections: ONE live QUIC connection per (peer, ALPN), shared
    /// by every caller — streams multiplex over it. Reuse keeps handshake
    /// volume and connection-state memory bounded by PEER COUNT instead of
    /// request rate: conn-per-request ballooned rejoining nodes to their OOM
    /// cap under churn (~1 GB of pending-connection state on one node, freed
    /// in a single instant when the stacked attempts aborted — measured).
    /// Closed entries re-dial lazily; cleared on rebind.
    pool: std::sync::Mutex<std::collections::HashMap<PoolKey, Connection>>,
    /// Per-key dial locks: concurrent dials to the SAME (peer, ALPN) collapse
    /// into one attempt (the winners re-check the pool). During churn every
    /// subsystem dials the same dead peer at once — probe, DHT, replication —
    /// and each parallel attempt held its own handshake state for the full
    /// timeout.
    dials: std::sync::Mutex<
        std::collections::HashMap<PoolKey, std::sync::Arc<tokio::sync::Mutex<()>>>,
    >,
    /// Global bound on concurrent OUTBOUND bulk dial attempts. In-flight QUIC
    /// handshake state is what ballooned nodes to their OOM caps during the
    /// rejoin storms (dead peers: every attempt holds state for its 3-8s
    /// timeout, is never pooled, and is retried forever). Excess dialers wait
    /// here as cheap futures instead.
    dial_permits: std::sync::Arc<tokio::sync::Semaphore>,
    /// RESERVED dial slots for the liveness ping ALPN: probes must never
    /// queue behind bulk dials, or a storm's dial backlog starves probe
    /// rounds past their timeout and healthy peers get falsely marked dead —
    /// churn feeding churn (review finding).
    ping_dial_permits: std::sync::Arc<tokio::sync::Semaphore>,
    clock: std::sync::Arc<hlc::Clock>,
}

/// Max concurrent outbound bulk dial attempts (handshakes in flight). Sized
/// for throughput under legitimate concurrency (pipelined ingest announces,
/// batched repair pushes, post-roll pool warmup: 16 was a measured choke —
/// DHT lookups queued minutes behind it and scan jobs ballooned to 90s) while
/// staying a hard memory bound: 48 in-flight handshakes is ~10MB, vs the
/// unbounded thousands that OOMed nodes before the cap existed.
const MAX_CONCURRENT_DIALS: usize = 48;
/// Reserved concurrent dial slots for liveness pings.
const MAX_CONCURRENT_PING_DIALS: usize = 4;

/// Pool key: remote node + ALPN (one connection per protocol per peer).
type PoolKey = ([u8; 32], Vec<u8>);

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
        let cfg = BindCfg {
            secret,
            reach,
            alpns,
            port,
            relay_urls,
            fallback_relays,
        };
        let endpoint = Self::build_endpoint(&cfg).await?;
        Ok(Self {
            cfg,
            endpoint: std::sync::RwLock::new(endpoint),
            epoch: std::sync::atomic::AtomicU64::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
            rebind_lock: tokio::sync::Mutex::new(()),
            pool: std::sync::Mutex::new(std::collections::HashMap::new()),
            dials: std::sync::Mutex::new(std::collections::HashMap::new()),
            dial_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DIALS)),
            ping_dial_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_PING_DIALS,
            )),
            clock: std::sync::Arc::new(hlc::Clock::new()),
        })
    }

    /// Construct and bind one endpoint from a saved config (initial bind and
    /// every rebind go through here so they are guaranteed identical).
    async fn build_endpoint(cfg: &BindCfg) -> Result<Endpoint> {
        let secret_key = SecretKey::from_bytes(&cfg.secret);
        let mut builder = match cfg.reach {
            // Minimal: local sockets only — no relays, no address lookup.
            Reach::LocalOnly => Endpoint::builder(presets::Minimal),
            // N0 preset (discovery etc.); relay map possibly overridden below.
            Reach::Relayed => {
                let builder = Endpoint::builder(presets::N0);
                if cfg.relay_urls.is_empty() {
                    builder
                } else {
                    let mut urls = cfg.relay_urls.clone();
                    if cfg.fallback_relays {
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
        .alpns(cfg.alpns.clone());
        if cfg.port != 0 {
            builder = builder
                .bind_addr(std::net::SocketAddr::from(([0, 0, 0, 0], cfg.port)))
                .map_err(|e| TransportError::Bind(e.to_string()))?;
        }
        builder
            .bind()
            .await
            .map_err(|e| TransportError::Bind(e.to_string()))
    }

    /// Clone the live endpoint handle out of the lock (cheap: `Endpoint` is an
    /// `Arc` handle). Never hold the lock itself across an await.
    fn current(&self) -> Endpoint {
        self.endpoint.read().expect("endpoint lock").clone()
    }

    /// How many times the endpoint has been rebuilt (0 = the original bind).
    pub fn rebinds(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Number of live pooled connections. Today the pool is keyed by
    /// (peer, ALPN), so this counts one entry per (peer, protocol) pair — the
    /// metric the mux migration (element 1) collapses to one per peer. The
    /// acceptance harness asserts the reduction.
    pub fn connection_count(&self) -> usize {
        self.pool.lock().expect("conn pool").len()
    }

    /// Tear the endpoint down and bind a fresh one with the identical config
    /// (same identity, port, relays, ALPNs). All existing connections die;
    /// [`Self::serve`] accept loops re-attach automatically; peers see the
    /// same NodeId. This is the recovery for a WEDGED endpoint: after uplink
    /// path churn the old endpoint's QUIC path state can go permanently stale
    /// — every dial fails in seconds while ICMP on the same path is clean —
    /// and only a rebuild (identical to a process restart, minus the process)
    /// gets it dialing again.
    pub async fn rebind(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        let _guard = self.rebind_lock.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Bind("transport closed".into()));
        }
        // Close the old endpoint FIRST: with a fixed listen port the fresh
        // socket can only bind once the old one is released. Cap the graceful
        // close — a wedged endpoint may not close cleanly. Every pooled
        // connection belongs to the old endpoint, so flush the pool with it.
        let old = self.current();
        self.pool.lock().expect("conn pool").clear();
        let _ = tokio::time::timeout(Duration::from_secs(5), old.close()).await;
        let mut last_err = String::new();
        for _ in 0..10 {
            match Self::build_endpoint(&self.cfg).await {
                Ok(endpoint) => {
                    // close() may have run while we were building: it closed
                    // the OLD endpoint and returned, and the serve loops have
                    // exited — installing now would leave a live endpoint
                    // nobody owns. Close the fresh one and bail instead.
                    if self.closed.load(Ordering::Acquire) {
                        let _ =
                            tokio::time::timeout(Duration::from_secs(5), endpoint.close()).await;
                        return Err(TransportError::Bind(
                            "transport closed during rebind".into(),
                        ));
                    }
                    *self.endpoint.write().expect("endpoint lock") = endpoint;
                    self.epoch.fetch_add(1, Ordering::AcqRel);
                    return Ok(());
                }
                Err(err) => {
                    // Likely the freed port lagging; brief backoff and retry.
                    last_err = err.to_string();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        // The old endpoint is closed and no new one bound: the node stays
        // dark until the caller (the isolation watchdog) retries.
        Err(TransportError::Bind(format!(
            "rebind failed after retries: {last_err}"
        )))
    }

    /// The node's hybrid logical clock (shared across protocols).
    pub fn clock(&self) -> std::sync::Arc<hlc::Clock> {
        self.clock.clone()
    }

    /// This node's identity on the wire (stable across rebinds — the secret
    /// key is part of the saved bind config).
    pub fn node_id(&self) -> NodeId {
        NodeId(*self.current().id().as_bytes())
    }

    /// This endpoint's current dialable address (direct socket addresses are
    /// available immediately after bind; relay info arrives asynchronously).
    pub fn addr(&self) -> PeerAddr {
        PeerAddr(self.current().addr())
    }

    /// A live connection to `peer` for `alpn` — pooled, or freshly dialed and
    /// pooled. Callers open streams on it (`open_bi`) and MUST NOT close it:
    /// the connection is shared and long-lived; iroh's idle timeout reclaims
    /// unused ones (a closed entry re-dials lazily on the next call). After a
    /// request on it fails, call [`Self::evict`] so the next request re-dials.
    pub async fn connect(&self, peer: &PeerAddr, alpn: &[u8]) -> Result<Connection> {
        let key = (peer.node_id().0, alpn.to_vec());
        if let Some(conn) = self.pool.lock().expect("conn pool").get(&key) {
            // Closed (idle-timeout, peer restart, error) → fall through and re-dial.
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        self.connect_fresh(peer, alpn).await
    }

    /// Dial a NEW connection and pool it, replacing any existing entry —
    /// the recovery path when a pooled connection is broken in a way
    /// `close_reason` cannot see yet (callers evict the broken entry first).
    /// A replaced connection is only dropped from the pool, never closed
    /// here: a concurrent caller may still be mid-request on it, and iroh's
    /// idle timeout reclaims it shortly after.
    ///
    /// Two bounds keep dial-attempt state from ballooning during churn (the
    /// measured OOM driver once per-request conns were pooled): concurrent
    /// dials to the SAME (peer, ALPN) serialize on a per-key lock — losers
    /// re-check the pool the winner filled instead of dialing again — and a
    /// global semaphore caps handshakes in flight; excess dialers wait as
    /// cheap futures (cancellation-safe: a caller's timeout dropping the
    /// future releases both without leaking).
    pub async fn connect_fresh(&self, peer: &PeerAddr, alpn: &[u8]) -> Result<Connection> {
        let key = (peer.node_id().0, alpn.to_vec());
        let dial_lock = {
            let mut dials = self.dials.lock().expect("dials");
            dials
                .entry(key.clone())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                .clone()
            // Entries are tiny and bounded by peers×ALPNs; never pruned.
        };
        let _dial_guard = dial_lock.lock().await;
        // A concurrent dialer may have just filled the pool while we waited —
        // its connection is as fresh as one we would dial now.
        if let Some(conn) = self.pool.lock().expect("conn pool").get(&key) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        // Liveness pings dial through their own reserved lane.
        let sem = if alpn == alpn::PING {
            &self.ping_dial_permits
        } else {
            &self.dial_permits
        };
        let _permit = sem
            .acquire()
            .await
            .map_err(|_| TransportError::Connect("dial semaphore closed".into()))?;
        let conn = self
            .current()
            .connect(peer.0.clone(), alpn)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        self.pool
            .lock()
            .expect("conn pool")
            .insert(key, conn.clone());
        Ok(conn)
    }

    /// Drop the pooled connection for (peer, alpn) if it is still `failed` —
    /// callers report a request failure here so the next request re-dials.
    /// The identity check keeps a stale caller from evicting a healthy
    /// replacement that another task already dialed.
    pub fn evict(&self, peer: &PeerAddr, alpn: &[u8], failed: &Connection) {
        let key = (peer.node_id().0, alpn.to_vec());
        let mut pool = self.pool.lock().expect("conn pool");
        if pool
            .get(&key)
            .is_some_and(|c| c.stable_id() == failed.stable_id())
        {
            pool.remove(&key);
        }
    }

    /// Drop every pooled connection to `peer` (all ALPNs) — for callers that
    /// declared the peer dead and want no lingering entries for it.
    pub fn evict_peer(&self, peer: &NodeId) {
        self.pool
            .lock()
            .expect("conn pool")
            .retain(|(id, _), _| id != &peer.0);
    }

    /// Round-trip a ping to `peer` over wire frames (M1.4); returns RTT + skew.
    /// Rides the POOLED connection (no handshake in the steady state, so the
    /// RTT measures the path, not connection setup). A stream-level failure
    /// evicts the pooled connection so the next ping re-dials; a TIMEOUT does
    /// not — a slow peer's connection is likely fine, and QUIC's own loss
    /// detection closes a truly dead one (which `connect` then replaces).
    pub async fn ping(&self, peer: &PeerAddr, timeout: Duration) -> Result<PingReport> {
        let conn = tokio::time::timeout(timeout, self.connect(peer, alpn::PING))
            .await
            .map_err(|_| TransportError::Timeout(timeout))??;
        let fut = async {
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
            Ok(PingReport { rtt, peer_skew_ms })
        };
        let out = tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| TransportError::Timeout(timeout))?;
        if matches!(out, Err(TransportError::Stream(_))) {
            self.evict(peer, alpn::PING, &conn);
        }
        out
    }

    /// Accept loop: route incoming connections to per-ALPN handlers.
    /// Connections with an unregistered ALPN are closed. Survives
    /// [`Self::rebind`] by re-attaching to the fresh endpoint; runs until
    /// [`Self::close`]. Spawn this on the runtime.
    pub async fn serve(&self, handlers: Vec<(Vec<u8>, tokio::sync::mpsc::Sender<Connection>)>) {
        use std::sync::atomic::Ordering;
        let handlers = std::sync::Arc::new(handlers);
        loop {
            let epoch = self.epoch.load(Ordering::Acquire);
            let endpoint = self.current();
            while let Some(incoming) = endpoint.accept().await {
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
            // accept() drained: the endpoint closed — final shutdown, or a
            // rebind swapping in a replacement. Wait out the swap window and
            // re-attach; exit only on a real close.
            while self.epoch.load(Ordering::Acquire) == epoch
                && !self.closed.load(Ordering::Acquire)
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if self.closed.load(Ordering::Acquire) {
                return;
            }
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

    /// Gracefully close all connections and end the serve loops.
    pub async fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.pool.lock().expect("conn pool").clear();
        self.current().close().await;
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

    /// Isolation-watchdog support: after a rebind the identity is unchanged,
    /// the serve loop re-attaches to the fresh endpoint, and both inbound
    /// (ping to the rebound server) and outbound (ping from a rebound client)
    /// traffic work again.
    #[tokio::test]
    async fn rebind_preserves_identity_and_keeps_serving() {
        let server_id = NodeIdentity::generate();
        let server = std::sync::Arc::new(
            Transport::bind(
                server_id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![alpn::PING.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let serve = {
            let server = server.clone();
            tokio::spawn(async move { server.serve_ping().await })
        };

        let client_id = NodeIdentity::generate();
        let client = Transport::bind(client_id.secret_key_bytes(), Reach::LocalOnly, vec![], 0)
            .await
            .unwrap();

        // Healthy before the rebind.
        client
            .ping(&server.addr(), Duration::from_secs(10))
            .await
            .unwrap();

        // Server rebinds: same NodeId, new socket (port 0 → fresh OS port).
        server.rebind().await.unwrap();
        assert_eq!(server.rebinds(), 1);
        assert_eq!(server.node_id(), server_id.node_id(), "identity survives");

        // The serve loop must have re-attached: a ping to the NEW address works.
        client
            .ping(&server.addr(), Duration::from_secs(10))
            .await
            .unwrap();

        // Client-side rebind: outbound dialing works from a fresh endpoint too.
        client.rebind().await.unwrap();
        client
            .ping(&server.addr(), Duration::from_secs(10))
            .await
            .unwrap();

        client.close().await;
        server.close().await;
        serve.abort();
    }

    /// Connection pool: repeated connects to the same (peer, ALPN) return the
    /// SAME connection; evict forces a re-dial; rebind clears the pool.
    #[tokio::test]
    async fn pool_reuses_evicts_and_clears_on_rebind() {
        let server_id = NodeIdentity::generate();
        let server = std::sync::Arc::new(
            Transport::bind(
                server_id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![alpn::PING.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let serve = {
            let server = server.clone();
            tokio::spawn(async move { server.serve_ping().await })
        };

        let client_id = NodeIdentity::generate();
        let client = Transport::bind(client_id.secret_key_bytes(), Reach::LocalOnly, vec![], 0)
            .await
            .unwrap();
        let addr = server.addr();

        // Reuse: same underlying connection both times.
        let c1 = client.connect(&addr, alpn::PING).await.unwrap();
        let c2 = client.connect(&addr, alpn::PING).await.unwrap();
        assert_eq!(c1.stable_id(), c2.stable_id(), "pooled connection reused");

        // Two pings ride the pool (no per-request close teardown).
        client.ping(&addr, Duration::from_secs(10)).await.unwrap();
        client.ping(&addr, Duration::from_secs(10)).await.unwrap();

        // Evict: next connect dials a NEW connection.
        client.evict(&addr, alpn::PING, &c2);
        let c3 = client.connect(&addr, alpn::PING).await.unwrap();
        assert_ne!(c2.stable_id(), c3.stable_id(), "evicted → re-dialed");

        // A stale evict (old connection) must NOT drop the current entry.
        client.evict(&addr, alpn::PING, &c2);
        let c4 = client.connect(&addr, alpn::PING).await.unwrap();
        assert_eq!(c3.stable_id(), c4.stable_id(), "stale evict ignored");

        // Rebind clears the pool; connects and pings work from the fresh endpoint.
        client.rebind().await.unwrap();
        let c5 = client.connect(&addr, alpn::PING).await.unwrap();
        assert_ne!(c4.stable_id(), c5.stable_id(), "pool cleared on rebind");
        client.ping(&addr, Duration::from_secs(10)).await.unwrap();

        client.close().await;
        server.close().await;
        serve.abort();
    }

    /// Dial dedup: concurrent first connects to the same (peer, ALPN) must
    /// collapse into ONE dial — every caller gets the same connection.
    #[tokio::test]
    async fn concurrent_connects_dedup_to_one_dial() {
        let server_id = NodeIdentity::generate();
        let server = std::sync::Arc::new(
            Transport::bind(
                server_id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![alpn::PING.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let serve = {
            let server = server.clone();
            tokio::spawn(async move { server.serve_ping().await })
        };
        let client = std::sync::Arc::new(
            Transport::bind(
                NodeIdentity::generate().secret_key_bytes(),
                Reach::LocalOnly,
                vec![],
                0,
            )
            .await
            .unwrap(),
        );
        let addr = server.addr();

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (c, a) = (client.clone(), addr.clone());
            tasks.push(tokio::spawn(async move {
                c.connect(&a, alpn::PING).await.unwrap().stable_id()
            }));
        }
        let mut ids = std::collections::HashSet::new();
        for t in tasks {
            ids.insert(t.await.unwrap());
        }
        assert_eq!(ids.len(), 1, "8 concurrent connects → 1 dialed connection");

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
