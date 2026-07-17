//! Transport: iroh (QUIC) endpoint wrapper, ALPN registry, connect-by-NodeId,
//! and the ping/pong protocol used by the worker heartbeat.
//!
//! Spec: foundation §3 (iroh transport), §21–22 (connection lifecycle, ALPN).
//! iroh version pinned at 1.x (decision: pin at M1.2 integration).
//!
//! The node's Ed25519 identity IS the iroh secret key, so the zeph NodeId and
//! the iroh EndpointId are the same 32 bytes.

use std::time::Duration;

pub use iroh::endpoint::{Connection, RecvStream, SendStream};

use iroh::endpoint::presets;
use iroh::endpoint::RelayMode;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMap, RelayUrl, SecretKey, TransportAddr};
use zeph_core::hlc;
use zeph_core::{Cid, NodeId};
use zeph_wire as wire;

/// Transfer Plane v2 element 1 — MUX: one QUIC connection per peer carries
/// EVERY protocol; each bi-stream begins with a 1-byte protocol tag instead of
/// negotiating a per-protocol ALPN at handshake. Collapses ~O(peers×protocols)
/// connections to O(peers). All muxed peers dial/advertise this single ALPN.
pub const MUX_ALPN: &[u8] = b"/craftec/mux/1";

/// One-byte protocol tags written as the first byte of every muxed bi-stream;
/// the accept side reads this byte and routes the stream to the matching
/// handler (replacing the old per-connection ALPN dispatch). Stable on the
/// wire — never renumber; only append.
/// Inbound stream counters, indexed by tag byte (see [`tag`]). Read via
/// `Transport::tag_stream_counts`; index 0 and 11..15 are unused padding so a tag byte can index
/// directly without a bounds dance.
static TAG_STREAMS: [std::sync::atomic::AtomicU64; 16] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

pub mod tag {
    pub const PING: u8 = 1;
    pub const MEMBER: u8 = 2;
    pub const PIECE: u8 = 3;
    pub const SQLPAGE: u8 = 4;
    pub const INVOKE: u8 = 5;
    pub const REGISTRY: u8 = 6;
    pub const DHT: u8 = 7;
    /// Verification board gossip (a `BoardSnapshot` push). Additive: a node without this handler
    /// drops the stream, so board gossip is mixed-version-safe (a staggered roll, not simultaneous).
    pub const BOARD: u8 = 8;
    /// Ordering-sequencer sign solicitation (a collector asks a quorum member to sign a `SequencedWrite`;
    /// the member auto-signs if owner-authentic + non-equivocating). Additive → mixed-version-safe.
    pub const SIGN_SOLICIT: u8 = 9;
    /// Serving-cheque push (a consumer fire-and-forgets a cumulative `ServingCheque` to a provider it
    /// fetched from; the provider records it). Fire-and-forget like `BOARD`. Additive → mixed-version-safe.
    pub const CHEQUE: u8 = 10;
    // NOTE: tag 11 was briefly a settlement-announcement gossip; settlement moved to durable
    // committee-ordered writes on the sequencer (see `noded::settlement_service`), so no tag is used. 11
    // stays RESERVED (never reuse a retired wire tag) — the next protocol appends at 12.
}

/// An inbound muxed bi-stream, already tag-dispatched: the remote's NodeId
/// (QUIC-authenticated) plus the send/recv halves with the tag byte already
/// consumed, so a handler reads/writes its payload exactly as it did on a
/// per-ALPN connection's `accept_bi()` stream.
pub struct TaggedStream {
    pub remote: NodeId,
    pub send: SendStream,
    pub recv: RecvStream,
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
    /// MUX pool (element 1): ONE live QUIC connection per PEER (keyed by NodeId
    /// only, ALPN-free), shared by every protocol — streams carry a 1-byte tag.
    /// Reuse keeps handshake volume and connection-state memory bounded by PEER
    /// COUNT instead of request rate (conn-per-request ballooned rejoining nodes
    /// to their OOM cap under churn — measured). Closed entries re-dial lazily;
    /// cleared on rebind.
    mux_pool: std::sync::Mutex<std::collections::HashMap<[u8; 32], Connection>>,
    /// ACCEPTED (inbound) connections, for statistics only.
    ///
    /// `mux_pool` holds only connections WE dialed, so any stats read from it describe half the fleet's
    /// traffic. That is not a theoretical gap: it made a measured 153 KB/s look authoritative against an
    /// observed ~5 MB/s, i.e. the instrument under-reported by ~30x while appearing precise. A peer that
    /// dials us carries its bytes on a connection we never recorded. Weak refs would be tidier; a plain
    /// map plus prune-on-read keeps this a pure observability addition with no lifecycle coupling.
    in_conns: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<[u8; 32], Connection>>>,
    /// Per-peer dial-collapse locks for the mux connection (mirrors `dials`).
    mux_dials: std::sync::Mutex<
        std::collections::HashMap<[u8; 32], std::sync::Arc<tokio::sync::Mutex<()>>>,
    >,
    /// Global bound on concurrent OUTBOUND bulk dial attempts. In-flight QUIC
    /// handshake state is what ballooned nodes to their OOM caps during the
    /// rejoin storms (dead peers: every attempt holds state for its 3-8s
    /// timeout, is never pooled, and is retried forever). Excess dialers wait
    /// here as cheap futures instead.
    dial_permits: std::sync::Arc<tokio::sync::Semaphore>,
    clock: std::sync::Arc<hlc::Clock>,
}

/// Max concurrent outbound bulk dial attempts (handshakes in flight). Sized
/// for throughput under legitimate concurrency (pipelined ingest announces,
/// batched repair pushes, post-roll pool warmup: 16 was a measured choke —
/// DHT lookups queued minutes behind it and scan jobs ballooned to 90s) while
/// staying a hard memory bound: 48 in-flight handshakes is ~10MB, vs the
/// unbounded thousands that OOMed nodes before the cap existed.
const MAX_CONCURRENT_DIALS: usize = 48;

/// Max concurrently-dispatching inbound streams per MUX connection (all
/// protocols share one connection now, so this bounds a single peer's in-flight
/// requests across every protocol; the permit is held only through the tag read
/// + hand-off, so slow request HANDLING backpressures via the handler channels).
const MUX_PIPELINE_STREAMS: usize = 64;

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
            mux_pool: std::sync::Mutex::new(std::collections::HashMap::new()),
            in_conns: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            mux_dials: std::sync::Mutex::new(std::collections::HashMap::new()),
            dial_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DIALS)),
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

    /// Number of live pooled connections — one per peer in the mux pool
    /// (O(peers), not O(peers×protocols)); the acceptance harness asserts this
    /// per-peer ceiling.
    pub fn connection_count(&self) -> usize {
        self.mux_pool.lock().expect("mux pool").len()
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
        self.mux_pool.lock().expect("mux pool").clear();
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

    /// Drop the pooled mux connection to `peer` — for callers that declared the
    /// peer dead and want no lingering entry for it.
    pub fn evict_peer(&self, peer: &NodeId) {
        self.mux_pool.lock().expect("mux pool").remove(&peer.0);
    }

    // ── MUX (element 1): one connection per peer, protocol chosen per-stream ──

    /// A live MUX connection to `peer` (pooled per-peer, or freshly dialed and
    /// pooled). Every protocol shares it; most callers go through
    /// [`Self::request_tagged`] rather than dialing this directly. Keyed by
    /// NodeId only, dialing the single [`MUX_ALPN`]; concurrent first-dials to a
    /// peer collapse to one handshake and a global semaphore bounds handshakes
    /// in flight.
    pub async fn mux_conn(&self, peer: &PeerAddr) -> Result<Connection> {
        let id = peer.node_id().0;
        if let Some(conn) = self.mux_pool.lock().expect("mux pool").get(&id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        // Collapse concurrent first-dials to this peer into one handshake.
        let dial_lock = {
            let mut dials = self.mux_dials.lock().expect("mux dials");
            dials
                .entry(id)
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = dial_lock.lock().await;
        if let Some(conn) = self.mux_pool.lock().expect("mux pool").get(&id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        let _permit = self
            .dial_permits
            .acquire()
            .await
            .map_err(|_| TransportError::Connect("dial semaphore closed".into()))?;
        let conn = self
            .current()
            .connect(peer.0.clone(), MUX_ALPN)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        self.mux_pool
            .lock()
            .expect("mux pool")
            .insert(id, conn.clone());
        Ok(conn)
    }

    /// Drop the pooled mux connection for `peer` if it is still `failed`
    /// (identity-checked so a stale caller can't evict a healthy replacement).
    pub fn evict_mux(&self, peer: &PeerAddr, failed: &Connection) {
        let id = peer.node_id().0;
        let mut pool = self.mux_pool.lock().expect("mux pool");
        if pool
            .get(&id)
            .is_some_and(|c| c.stable_id() == failed.stable_id())
        {
            pool.remove(&id);
        }
    }

    /// One muxed request/reply round-trip: open a `tag` stream, write `req` +
    /// finish, read the whole reply (≤ `max_reply`). Evicts the mux connection
    /// on any stream failure so the next call re-dials. This is the client half
    /// every request/reply protocol uses in place of its old
    /// `connect(peer, ALPN) → open_bi → write → read_to_end`.
    pub async fn request_tagged(
        &self,
        peer: &PeerAddr,
        tag: u8,
        req: &[u8],
        max_reply: usize,
    ) -> Result<Vec<u8>> {
        let conn = self.mux_conn(peer).await?;
        let round = async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            send.write_all(&[tag])
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            send.write_all(req)
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            send.finish()
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            recv.read_to_end(max_reply)
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))
        }
        .await;
        if round.is_err() {
            self.evict_mux(peer, &conn);
        }
        round
    }

    /// Round-trip a ping to `peer` over wire frames (M1.4); returns RTT + skew.
    /// Rides the POOLED connection (no handshake in the steady state, so the
    /// RTT measures the path, not connection setup). A stream-level failure
    /// evicts the pooled connection so the next ping re-dials; a TIMEOUT does
    /// not — a slow peer's connection is likely fine, and QUIC's own loss
    /// detection closes a truly dead one (which `connect` then replaces).
    pub async fn ping(&self, peer: &PeerAddr, timeout: Duration) -> Result<PingReport> {
        // Muxed ping: rides the shared per-peer connection as a tag::PING stream
        // (the reserved per-ALPN dial lane collapses — there is one connection
        // per peer now). Timing starts AFTER the connection exists so the RTT
        // measures the path, not a handshake.
        let conn = tokio::time::timeout(timeout, self.mux_conn(peer))
            .await
            .map_err(|_| TransportError::Timeout(timeout))??;
        let fut = async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| TransportError::Stream(e.to_string()))?;
            send.write_all(&[tag::PING])
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
            self.evict_mux(peer, &conn);
        }
        out
    }

    /// Accept loop: every inbound connection is a MUX_ALPN connection,
    /// demultiplexed per-stream by its 1-byte tag into `stream_handlers`; a
    /// connection on any other ALPN is closed. Survives [`Self::rebind`] by
    /// re-attaching to the fresh endpoint; runs until [`Self::close`]. Spawn
    /// this on the runtime.
    pub async fn serve(&self, stream_handlers: Vec<(u8, tokio::sync::mpsc::Sender<TaggedStream>)>) {
        use std::sync::atomic::Ordering;
        let stream_handlers = std::sync::Arc::new(stream_handlers);
        loop {
            let epoch = self.epoch.load(Ordering::Acquire);
            let endpoint = self.current();
            while let Some(incoming) = endpoint.accept().await {
                let stream_handlers = stream_handlers.clone();
                let in_conns = self.in_conns.clone();
                tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    if conn.alpn() == MUX_ALPN {
                        // Record it for stats BEFORE serving: an inbound connection's bytes are
                        // otherwise invisible (see `in_conns`).
                        in_conns
                            .lock()
                            .expect("in conns")
                            .insert(*conn.remote_id().as_bytes(), conn.clone());
                        Self::demux_conn(conn, stream_handlers).await;
                    } else {
                        conn.close(1u32.into(), b"unknown alpn");
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

    /// Demultiplex one inbound MUX connection: for each accepted bi-stream read
    /// the leading protocol tag and hand the stream to the matching handler.
    /// Bounded pipelining (a per-connection permit held through the tag read and
    /// hand-off) backpressures a peer that opens streams faster than handlers
    /// drain them; an unknown tag drops the stream.
    /// EXACT per-peer QUIC statistics — the COMPLETE `ConnectionStats`, not a chosen subset.
    ///
    /// Returns `(peer, debug_dump)`. Deliberately the whole struct: udp_tx/udp_rx (bytes, datagrams,
    /// ios), the full frame_tx/frame_rx breakdown by QUIC frame type (STREAM vs ACK vs CRYPTO vs PING vs
    /// MAX_DATA vs PATH_CHALLENGE …), and lost_packets/lost_bytes.
    ///
    /// Capturing everything is the point. This exists because an idle-bandwidth investigation produced
    /// six confident diagnoses (dashboard, health scan, reannounce refresh, relay, poll stampede, scan
    /// floor) and all six were wrong — each one a hypothesis that picked its own evidence. A subset of
    /// counters chosen to test a theory can only ever confirm or deny THAT theory; the frame breakdown
    /// says what is on the wire regardless of what anyone expected. Read it, then form the hypothesis.
    pub fn peer_stats_dump(&self) -> Vec<(String, [u8; 32], String)> {
        let mut out = Vec::new();
        for (id, c) in self.mux_pool.lock().expect("mux pool").iter() {
            out.push(("dialed".to_string(), *id, format!("{:?}", c.stats())));
        }
        // BOTH directions, labelled. Reading only the dialed side under-reported ~30x.
        let mut dead = Vec::new();
        for (id, c) in self.in_conns.lock().expect("in conns").iter() {
            if c.close_reason().is_some() {
                dead.push(*id);
                continue;
            }
            out.push(("accepted".to_string(), *id, format!("{:?}", c.stats())));
        }
        if !dead.is_empty() {
            let mut m = self.in_conns.lock().expect("in conns");
            for id in dead {
                m.remove(&id);
            }
        }
        out
    }

    /// INBOUND stream count per tag — the only direct answer to "what is this node's traffic?".
    ///
    /// Added 2026-07-17 after a long idle-bandwidth investigation in which six successive hypotheses
    /// (dashboard, health scan, reannounce refresh, relay, poll stampede, scan floor) were each argued
    /// from CPU profiles and process-level byte counters, and each was wrong. Neither instrument can name
    /// a protocol: a CPU profile misses I/O-bound work, and `nettop` sees one process, not ten tags. This
    /// counts what actually arrives, by tag, so the question is answered by measurement rather than
    /// inference. Cheap: one relaxed add per inbound stream.
    pub fn tag_stream_counts() -> [u64; 16] {
        let mut out = [0u64; 16];
        for (i, c) in TAG_STREAMS.iter().enumerate() {
            out[i] = c.load(std::sync::atomic::Ordering::Relaxed);
        }
        out
    }

    async fn demux_conn(
        conn: Connection,
        handlers: std::sync::Arc<Vec<(u8, tokio::sync::mpsc::Sender<TaggedStream>)>>,
    ) {
        let remote = NodeId(*conn.remote_id().as_bytes());
        let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(MUX_PIPELINE_STREAMS));
        while let Ok((send, mut recv)) = conn.accept_bi().await {
            let Ok(permit) = permits.clone().acquire_owned().await else {
                break;
            };
            let handlers = handlers.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let mut tag = [0u8; 1];
                if recv.read_exact(&mut tag).await.is_err() {
                    return;
                }
                if let Some(c) = TAG_STREAMS.get(tag[0] as usize) {
                    c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if let Some((_, tx)) = handlers.iter().find(|(t, _)| *t == tag[0]) {
                    let _ = tx.send(TaggedStream { remote, send, recv }).await;
                }
            });
        }
    }

    /// Serve ping/pong only (convenience for tests and minimal nodes). Muxed:
    /// ping is a tag::PING stream on the shared connection.
    pub async fn serve_ping(&self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let clock = self.clock();
        let serve = self.serve(vec![(tag::PING, tx)]);
        let dispatch = async move {
            while let Some(stream) = rx.recv().await {
                tokio::spawn(Self::handle_ping_stream(clock.clone(), stream));
            }
        };
        tokio::join!(serve, dispatch);
    }

    /// Handle one inbound ping stream: echo a Pong for the Ping, merging the
    /// peer's clock (receiving it is liveness evidence for the caller's probe).
    pub async fn handle_ping_stream(clock: std::sync::Arc<hlc::Clock>, stream: TaggedStream) {
        let peer = stream.remote.to_hex();
        let (mut send, mut recv) = (stream.send, stream.recv);
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
            tracing::warn!(peer = %&peer[..12], "unexpected message on ping stream");
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
        let _ = send.write_all(&reply).await;
        let _ = send.finish();
        tracing::info!(peer = %&peer[..12], "ping served");
    }

    /// Gracefully close all connections and end the serve loops.
    pub async fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.mux_pool.lock().expect("mux pool").clear();
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
            vec![MUX_ALPN.to_vec()],
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

        // Hand-roll a garbage payload on a tag::PING mux stream: no PONG back.
        // (Shared mux connection — don't close it; the ping below reuses it.)
        let conn = client.mux_conn(&server_addr).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(&[tag::PING]).await.unwrap();
        send.write_all(b"not a frame").await.unwrap();
        send.finish().unwrap();
        let reply = recv.read_to_end(MAX_PING_FRAME).await.unwrap_or_default();
        assert!(reply.is_empty(), "server must not answer garbage");

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
                vec![MUX_ALPN.to_vec()],
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

    /// MUX pool: repeated mux_conn to a peer returns the SAME connection (one per
    /// peer); evict_peer drops it so the next call re-dials; a stale evict_mux is
    /// ignored; rebind clears the pool.
    #[tokio::test]
    async fn mux_pool_reuses_evicts_and_clears_on_rebind() {
        let server_id = NodeIdentity::generate();
        let server = std::sync::Arc::new(
            Transport::bind(
                server_id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![MUX_ALPN.to_vec()],
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

        // Reuse: one connection per peer, shared by every caller.
        let c1 = client.mux_conn(&addr).await.unwrap();
        let c2 = client.mux_conn(&addr).await.unwrap();
        assert_eq!(
            c1.stable_id(),
            c2.stable_id(),
            "pooled mux connection reused"
        );
        client.ping(&addr, Duration::from_secs(10)).await.unwrap();
        assert_eq!(
            client.connection_count(),
            1,
            "still one connection after a ping"
        );

        // Evict the whole peer → the next mux_conn dials fresh.
        client.evict_peer(&addr.node_id());
        assert_eq!(client.connection_count(), 0, "evict_peer cleared the entry");
        let c3 = client.mux_conn(&addr).await.unwrap();
        assert_ne!(c1.stable_id(), c3.stable_id(), "evicted → re-dialed");

        // A stale evict (the OLD connection) must NOT drop the healthy current one.
        client.evict_mux(&addr, &c1);
        let c4 = client.mux_conn(&addr).await.unwrap();
        assert_eq!(c3.stable_id(), c4.stable_id(), "stale evict ignored");

        // Rebind clears the pool; a fresh connection dials from the new endpoint.
        client.rebind().await.unwrap();
        assert_eq!(client.connection_count(), 0, "rebind cleared the pool");
        let c5 = client.mux_conn(&addr).await.unwrap();
        assert_ne!(c4.stable_id(), c5.stable_id(), "pool cleared on rebind");
        client.ping(&addr, Duration::from_secs(10)).await.unwrap();

        client.close().await;
        server.close().await;
        serve.abort();
    }

    /// MUX dial dedup: concurrent first mux_conn to the same peer must collapse
    /// into ONE dial — every caller gets the same connection.
    #[tokio::test]
    async fn concurrent_mux_conn_dedup_to_one_dial() {
        let server_id = NodeIdentity::generate();
        let server = std::sync::Arc::new(
            Transport::bind(
                server_id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![MUX_ALPN.to_vec()],
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
                c.mux_conn(&a).await.unwrap().stable_id()
            }));
        }
        let mut ids = std::collections::HashSet::new();
        for t in tasks {
            ids.insert(t.await.unwrap());
        }
        assert_eq!(ids.len(), 1, "8 concurrent mux_conn → 1 dialed connection");
        assert_eq!(client.connection_count(), 1, "one pooled connection");

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
            vec![MUX_ALPN.to_vec()],
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
