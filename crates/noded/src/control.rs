//! Control API: live node status over a Unix socket (JSON-RPC 2.0, for the
//! CLI) and localhost HTTP (for the web dashboard, MU.2).
//!
//! The Unix socket lives at `<data_dir>/zeph.sock` — filesystem permissions
//! are the auth boundary. The HTTP server binds 127.0.0.1 only and requires
//! the per-datadir token (`control.token`, 0600); remote access is via SSH
//! tunnel, never public exposure.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerStatus {
    pub id: String,
    pub addrs: String,
    pub alive: bool,
    pub rtt_us: Option<u64>,
    pub skew_ms: Option<u64>,
    pub last_seen_unix: Option<u64>,
    pub consecutive_failures: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ContentInfo {
    pub cid: String,
    /// Network counts (from the tracker).
    pub providers: usize,
    pub pinned: usize,
    /// Advisory network HAVE (sum of provider piece counts).
    pub pieces: usize,
    /// WANT interest signals across the network.
    pub wants: usize,
    /// Generation size k (decode threshold; 0 if this node doesn't hold it).
    pub k: usize,
    /// Durability floor n for this content (0 if this node doesn't hold it).
    pub floor: usize,
    /// THIS node's relationship to the content.
    pub local_pieces: usize,
    pub local_pinned: bool,
    pub local_wanted: bool,
    /// This node has locally BANNED (tombstoned) the CID — tracked by the
    /// network but never hosted here.
    pub local_tombstoned: bool,
    /// Manifest metadata, when this node holds the object and it decodes as a
    /// manifest: the file/folder name, total size, and whether it's a folder.
    /// `None` name = raw content (or a manifest this node doesn't hold).
    pub name: Option<String>,
    pub size: u64,
    pub is_dir: bool,
    /// MIME type (files only; None for folders/raw), for drive-parity display.
    pub mime: Option<String>,
    /// Metadata envelope (default view = earliest publisher): first-published
    /// unix millis, that publisher's short id, and their comment.
    pub published_at: Option<u64>,
    pub publisher: Option<String>,
    pub comment: Option<String>,
}

/// Node configuration for the Settings view (read-only; edit config.toml +
/// restart to change).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeSettings {
    // ── network ──
    pub reach: String,
    pub listen_port: u16,
    pub dashboard_port: u16,
    pub heartbeat_secs: u64,
    pub fallback_relays: bool,
    pub probe_timeout_secs: u64,
    pub relay_urls: Vec<String>,
    pub relay_operator_urls: Vec<String>,
    pub peers: Vec<String>,
    // ── storage & erasure ──
    pub storage_quota_gib: f64,
    pub erasure_k: usize,
    pub durability_threshold: usize,
    // ── lifecycle tunables (obj engine) ──
    pub scale_threshold: u32,
    pub degrade_threshold: u32,
    pub fade_grace_secs: u64,
    pub eviction_cooldown_secs: u64,
    // ── background intervals ──
    pub health_scan_secs: u64,
    pub reannounce_secs: u64,
    // ── paths ──
    pub data_dir: String,
}

/// Event-bus activity: totals + per-type breakdown + live subscriber count.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct EventStats {
    pub total: u64,
    pub by_tag: std::collections::BTreeMap<String, u64>,
    pub subscribers: usize,
    pub capacity: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub node_id: String,
    /// Init-sequence stage: "booting" -> "census-settling" -> "lifecycle-running".
    #[serde(default)]
    pub boot_stage: String,
    pub reach: String,
    pub relays: String,
    pub listen: String,
    pub uptime_secs: u64,
    pub wire_version: u8,
    /// Erasure capability advertisement (scheme + default parameters).
    pub erasure: String,
    /// Current HLC reading: wall millis + logical counter (foundation §42).
    pub hlc_ms: u64,
    pub hlc_logical: u16,
    pub passive_peers: u32,
    /// CONVERGED census size — every member of the network heard (directly or via gossip)
    /// within the census TTL. Network-wide and consistent across nodes, unlike the size-bounded
    /// active view; this is the set elections run over.
    pub census: u32,
    pub store_cids: u64,
    pub store_pieces: u64,
    pub store_pinned: u64,
    pub store_bytes: u64,
    pub providing: u64,
    /// CIDs hosted for the network (provided; not user-curated).
    pub hosting_cids: u64,
    pub content: Vec<ContentInfo>,
    /// HealthScan: last-pass scanned + at-risk CIDs, cumulative pieces repaired.
    pub health_scanned: usize,
    pub health_at_risk: usize,
    pub health_repaired: u64,
    pub health_distributed: u64,
    pub health_scaled: u64,
    pub health_degraded: u64,
    pub health_fading: u64,
    pub health_offloaded: u64,
    pub health_surplus: usize,
    /// INBOUND streams by transport tag since boot — `{"ping": n, "dht": n, "piece": n, ...}`.
    /// Measured, not inferred: this exists because six successive bandwidth hypotheses were argued from
    /// CPU profiles and process byte-counters, which cannot name a protocol, and all six were wrong.
    pub tag_streams: std::collections::BTreeMap<String, u64>,
    /// The COMPLETE per-peer QUIC `ConnectionStats` dump: `"<peer12>" -> "<debug>"`. Everything —
    /// udp bytes/datagrams, the full frame-type breakdown (STREAM/ACK/CRYPTO/PING/…), lost packets and
    /// bytes. Not a chosen subset: a subset can only confirm the theory it was chosen for, and six
    /// theories about this node's traffic have already been wrong.
    pub peer_stats: std::collections::BTreeMap<String, String>,
    pub scan_queue: usize,
    pub scan_due: usize,
    pub peers: Vec<PeerStatus>,
    /// Recent node events (activity feed), newest first.
    pub recent_events: Vec<String>,
    /// Background job coordinator activity (foundation §51).
    pub jobs: zeph_sched::JobStats,
    /// Event-bus activity (foundation §52) — totals + per-type + subscribers.
    pub event_stats: EventStats,
    /// Most recent finished jobs (newest first).
    pub recent_jobs: Vec<zeph_sched::JobRecord>,
    /// Jobs running right now with elapsed time (longest-first) — "stuck on what".
    #[serde(default)]
    pub in_flight_jobs: Vec<zeph_sched::InFlightJob>,
    /// Node configuration (read-only Settings view).
    pub settings: NodeSettings,
    /// Economic layer — ledger balance, pool, settlement verification, reciprocity standing, anchors.
    #[serde(default)]
    pub economy: Economy,
}

/// How often the economy view is re-derived off the request path (see `State::economy_refresh_loop`).
/// Well under the 5-min epoch that gates how fast the underlying records can change.
const ECONOMY_REFRESH: std::time::Duration = std::time::Duration::from_secs(15);

/// The economy view (§11 step 4): the token ledger + settlement + reciprocity state for the dashboard.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct Economy {
    /// This node's reward-ledger balance (folded from its committed account chain; reward reconciled
    /// after claim).
    pub balance: u64,
    /// Reward this node has earned by serving but not yet claimed (Σ owed shares across in-window records).
    pub reward_owed: u64,
    /// Cumulative REWARDABLE served bytes — the per-consumer-capped "settled" numerator of the settled/served
    /// meter (gross served is `reciprocity.earned`). What fell within consumers' paid quotas.
    pub reward_settled: u64,
    /// The distributable settlement pool — pool tokens not already earmarked as owed.
    pub pool: u64,
    /// TOTAL protocol holdings: every token the pool literally holds, earmarked or not. `pool_total −
    /// pool` is what is owed to providers but unclaimed.
    pub pool_total: u64,
    /// TOTAL SUPPLY in existence (CTS-1 L0). Every token ever minted, wherever it now sits. Minting is
    /// the seed alone, so this rises only by the schedule and never past the governed cap — and
    /// `Σ balances + pool_total` must equal it.
    pub total_supply: u64,
    /// P6 SUBSCRIPTION: this node's remaining unexpired egress entitlement in bytes — what its payments
    /// still entitle it to have served (use-it-or-lose-it: unspent bytes expire at the window edge).
    pub subscription_bytes: u64,
    /// Settlement re-execution verification tally — epochs whose canonical record matched this node's own.
    pub verified: u64,
    /// Epochs whose canonical record DIVERGED from this node's re-execution (should stay 0).
    pub mismatched: u64,
    /// Reciprocity standing (earned/consumed/paid + the governed grants).
    pub reciprocity: Option<crate::cheque::Reciprocity>,
    /// The pinned canonical-program anchors (`name → cid @ interface_version`).
    pub anchors: Vec<AnchorRow>,
}

/// One governance-pinned canonical program anchor.
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct AnchorRow {
    pub name: String,
    pub cid: String,
    pub interface_version: i64,
}

/// Health counters: scanned, at_risk, repaired, moved, scaled, degraded, fading, offloaded, surplus.
type HealthCounters = (usize, usize, u64, u64, u64, u64, u64, u64, usize);

pub struct State {
    pub clock: std::sync::Arc<zeph_core::hlc::Clock>,
    /// Init-sequence stage (status page): booting -> census-settling -> lifecycle-running.
    pub boot_stage: tokio::sync::RwLock<String>,
    pub node_id: String,
    pub reach: String,
    pub relays: String,
    pub listen: String,
    pub started: Instant,
    pub engine: Arc<zeph_obj::ObjEngine>,
    pub peers: RwLock<Vec<PeerStatus>>,
    pub passive_peers: std::sync::atomic::AtomicU32,
    /// Converged census size (see `Status::census`), synced by the 1s membership loop.
    pub census: std::sync::atomic::AtomicU32,
    pub storage: RwLock<(u64, u64, u64, u64)>, // (cids, pieces, pinned, bytes)
    pub providing: std::sync::atomic::AtomicU64,
    pub content: RwLock<Vec<ContentInfo>>,
    /// Per-cid health rows (all held cids) for the dashboard health view, pre-built by noded.
    pub cid_health: RwLock<Vec<serde_json::Value>>,
    pub health: RwLock<HealthCounters>,
    /// Health-scan delay-queue depth (cids scheduled) + due-now backlog — the scanner is its own
    /// queue now, NOT a coordinator job, so it is surfaced separately.
    pub scan_queue: std::sync::atomic::AtomicUsize,
    pub scan_due: std::sync::atomic::AtomicUsize,
    pub craftsql: std::sync::Arc<zeph_sql::CraftSql>,
    /// The node event bus (foundation §52) — producers publish, apps subscribe.
    pub events: zeph_events::EventBus,
    /// Recent event descriptions for the dashboard activity feed (newest last).
    pub recent_events: RwLock<std::collections::VecDeque<String>>,
    /// Background job coordinator (foundation §51) — prioritized, deduped,
    /// bounded-concurrency scheduler the lifecycle + reannounce run through.
    pub jobs: zeph_sched::JobCoordinator,
    /// Per-type event counts for the event-bus status view (tag → count).
    pub event_counts: RwLock<std::collections::BTreeMap<String, u64>>,
    /// Count of CIDs this node HOSTS for the network (holds pieces of, but the
    /// user did not pin/want/ban) — the "provided" set, managed separately from
    /// the curated content list.
    pub hosting_cids: std::sync::atomic::AtomicU64,
    /// Node configuration snapshot (Settings view).
    pub settings: NodeSettings,
    /// CraftCOM invocation service — run user-level app WASM (local invoke).
    pub com: std::sync::Arc<zeph_com::InvokeService>,
    /// Durable owner-signed HEAD registry (open CRDT) — program names, DB roots, and
    /// manifests — a thin consumer of the program-account store.
    pub registry: std::sync::Arc<crate::headreg::HeadRegistry>,
    /// Governance: one durable chain deriving both the governor set and program registry.
    pub gov: std::sync::Arc<crate::governance::GovernanceChainStore>,
    /// K1 anchor dispatcher — resolve a canonical name → its governance-pinned program cid, and dispatch.
    pub anchor: std::sync::Arc<crate::anchor::AnchorDispatcher>,
    /// Token-ledger service — author ledger writes (committee-ordered) + fold account balances.
    pub ledger: std::sync::Arc<crate::ledger::LedgerService>,
    /// Economy-egress policy service (settlement pool + records) — P4 split from the token ledger.
    pub economy: std::sync::Arc<crate::economy_egress::EconomyEgressService>,
    /// The economy view, refreshed off-request by [`State::economy_refresh_loop`]. `snapshot` READS this
    /// and never derives — deriving touches the network (chain syncs + DHT lookups), and this handler is
    /// polled ~1/s, so on a high-RTT node the derivations outran the polls and piled up into a lookup
    /// storm. `None` until the first refresh completes (the dashboard shows zeros briefly at boot rather
    /// than blocking the whole status endpoint on the network).
    pub economy_cache: tokio::sync::RwLock<Option<Economy>>,
    /// Settlement service — the epoch-close loop + its re-execution verification tally.
    pub settlement: std::sync::Arc<crate::settlement_service::SettlementService>,
    /// Serving-cheque service — reciprocity standing (earned/consumed/paid + grants) for the dashboard.
    pub cheque: std::sync::Arc<crate::cheque::ChequeService>,
    /// For [`Status::peer_stats`] — QUIC's own per-connection counters, the one instrument that can say
    /// what this node's traffic actually is rather than what someone inferred it to be.
    pub transport: std::sync::Arc<zeph_transport::Transport>,
    /// Generic program accounts — any program's single-writer state (the program is the writer).
    pub accounts: std::sync::Arc<crate::account::ProgramAccountStore>,
    /// Per-program attestation quorum chains (the `attest` host fn's backend).
    pub attest: std::sync::Arc<crate::attest::AttestStore>,
    /// Rotating epoch committee — the computed k-of-n quorum for anchored programs (automated attestation).
    pub epoch_committee: std::sync::Arc<crate::epoch_committee::EpochCommitteeSource>,
    /// Per-(owner,program,account) ordered-write logs (the `sequence` host fn's backend).
    pub sequence: std::sync::Arc<crate::sequence::SequenceStore>,
    /// Verification board — the open re-execution consistency primitive (§ VERIFICATION_DESIGN),
    /// surfaced on the consensus dashboard as the "verification board" card.
    pub board: std::sync::Arc<crate::board::BoardService>,
}

impl State {
    /// Advance the init-sequence stage shown on the status page.
    pub async fn set_boot_stage(&self, stage: &str) {
        *self.boot_stage.write().await = stage.to_string();
    }

    pub async fn snapshot(&self) -> Status {
        let hlc = self.clock.now();
        // Economy view: served from the BACKGROUND-REFRESHED cache — never derived here.
        //
        // This handler is polled ~1/s by the dashboard, and deriving the economy touches the NETWORK:
        // `balance()` syncs the account chain (and resolves a debit per `Claim` op), and the records
        // view walks the claim window across committee members' chains. Each of those is an iterative
        // DHT lookup, which on a high-RTT node (relay-only / NAT / mobile) takes seconds — so a snapshot
        // could take MINUTES while polls kept arriving every second, piling up unboundedly into a
        // self-inflicted DHT lookup storm (measured ~700 KB/s on a hotspot-attached node; sub-ms-RTT
        // nodes running the identical binary showed nothing, because their walks finished before the
        // next poll — the bug NEEDED latency to appear).
        //
        // Predates the economy split: `balance()` was on this path from the start. Caching or
        // single-flighting the derivation only rate-limits the damage; the fix is that a request handler
        // must not do network I/O at all. `economy_refresh_loop` owns the derivation (a loop is
        // single-flight by construction) and this reads its last result.
        let economy = self.economy_cache.read().await.clone().unwrap_or_default();

        Status {
            economy,
            hlc_ms: hlc.millis(),
            hlc_logical: hlc.logical(),
            node_id: self.node_id.clone(),
            reach: self.reach.clone(),
            relays: self.relays.clone(),
            listen: self.listen.clone(),
            boot_stage: self.boot_stage.read().await.clone(),
            uptime_secs: self.started.elapsed().as_secs(),
            wire_version: zeph_wire::VERSION,
            erasure: format!(
                "rlnc-gf256 k=32 n={} · vtags null-space v{}",
                zeph_erasure::target_pieces(32),
                zeph_erasure::vtags::SCHEME_NULL_SPACE_V1,
            ),
            passive_peers: self
                .passive_peers
                .load(std::sync::atomic::Ordering::Relaxed),
            census: self.census.load(std::sync::atomic::Ordering::Relaxed),
            store_cids: self.storage.read().await.0,
            store_pieces: self.storage.read().await.1,
            store_pinned: self.storage.read().await.2,
            store_bytes: self.storage.read().await.3,
            providing: self.providing.load(std::sync::atomic::Ordering::Relaxed),
            hosting_cids: self.hosting_cids.load(std::sync::atomic::Ordering::Relaxed),
            content: self.content.read().await.clone(),
            health_scanned: self.health.read().await.0,
            health_at_risk: self.health.read().await.1,
            health_repaired: self.health.read().await.2,
            health_distributed: self.health.read().await.3,
            health_scaled: self.health.read().await.4,
            health_degraded: self.health.read().await.5,
            health_fading: self.health.read().await.6,
            health_offloaded: self.health.read().await.7,
            health_surplus: self.health.read().await.8,
            tag_streams: {
                // Name the tags so the answer reads directly off the dashboard instead of needing a
                // profile + a hypothesis. Zero-count tags are omitted to keep it legible.
                const NAMES: [(usize, &str); 10] = [
                    (1, "ping"),
                    (2, "member"),
                    (3, "piece"),
                    (4, "sqlpage"),
                    (5, "invoke"),
                    (6, "registry"),
                    (7, "dht"),
                    (8, "board"),
                    (9, "sign_solicit"),
                    (10, "cheque"),
                ];
                let counts = zeph_transport::Transport::tag_stream_counts();
                NAMES
                    .iter()
                    .filter(|(i, _)| counts[*i] > 0)
                    .map(|(i, n)| (n.to_string(), counts[*i]))
                    .collect()
            },
            peer_stats: self
                .transport
                .peer_stats_dump()
                .into_iter()
                // Key by DIRECTION + peer: a peer that dialed us and a peer we dialed are separate
                // connections with separate counters, and collapsing them hid half the traffic.
                .map(|(dir, id, dump)| (format!("{}:{}", dir, hex::encode(&id[..6])), dump))
                .collect(),
            scan_queue: self.scan_queue.load(std::sync::atomic::Ordering::Relaxed),
            scan_due: self.scan_due.load(std::sync::atomic::Ordering::Relaxed),
            peers: self.peers.read().await.clone(),
            recent_events: self
                .recent_events
                .read()
                .await
                .iter()
                .rev()
                .cloned()
                .collect(),
            jobs: self.jobs.stats(),
            event_stats: EventStats {
                by_tag: self.event_counts.read().await.clone(),
                total: self.event_counts.read().await.values().sum(),
                subscribers: self.events.subscribers(),
                capacity: 256,
            },
            recent_jobs: self.jobs.recent_jobs(),
            in_flight_jobs: self.jobs.in_flight_jobs(),
            settings: self.settings.clone(),
        }
    }

    /// Derive the economy view — chain syncs + DHT lookups, i.e. the SLOW, network-touching work that
    /// must never sit on a request path. Called only by [`economy_refresh_loop`](Self::economy_refresh_loop).
    async fn derive_economy(&self) -> Economy {
        let me = parse_node_id(&self.node_id)
            .map(|n| n.0)
            .unwrap_or([0u8; 32]);
        let (verified, mismatched) = self.settlement.verification_stats();
        let mut anchors = Vec::new();
        for name in [
            crate::anchor::TOKEN_ANCHOR,
            crate::anchor::ECONOMY_EGRESS_ANCHOR,
        ] {
            if let Some(res) = self.anchor.resolve(name).await {
                anchors.push(AnchorRow {
                    name: name.to_string(),
                    cid: hex::encode(res.cid),
                    interface_version: res.interface_version as i64,
                });
            }
        }
        // ONE token fold serves both the balance and the claimed-set the records view needs (token owns
        // the dedup; economy owns the valuation — this is the one place legitimately holding both).
        let token_state = self.ledger.balance(me).await;
        let now_epoch = self.settlement.epoch();
        let my = self
            .economy
            .my_view_from_records(me, &token_state.claimed_epochs, now_epoch)
            .await;
        Economy {
            balance: token_state.balance,
            // From the CANONICAL records, not local settle state: settling is committee-gated, so most
            // nodes have no local record and would otherwise report 0 of their own money.
            reward_owed: my.owed,
            reward_settled: my.settled_bytes,
            pool: my.pool,
            // Read straight off the settlement store: these are protocol-wide facts (what the pool holds,
            // what exists), not this account's view.
            pool_total: self.economy.pool_total().await,
            total_supply: self.economy.total_supply().await,
            subscription_bytes: my.subscription_bytes,
            verified,
            mismatched,
            reciprocity: Some(self.cheque.reciprocity_snapshot()),
            anchors,
        }
    }

    /// Refresh the economy view off the request path, forever.
    ///
    /// A LOOP is single-flight by construction: the next derivation cannot start until this one
    /// finishes, however long it takes. That is the property the dashboard needs and could never get from
    /// polling — an HTTP handler has no way to refuse work, so a derivation slower than the poll interval
    /// piles up without bound. Here a slow derivation only means a staler number.
    ///
    /// The data justifies the cadence: a balance changes when this node transacts, and records advance
    /// once per EPOCH (5 min), so [`ECONOMY_REFRESH`] is far fresher than the data can change — while the
    /// dashboard stays as live as the operator likes. Poll rate and derivation cost are now decoupled.
    pub async fn economy_refresh_loop(self: std::sync::Arc<Self>) {
        loop {
            let eco = self.derive_economy().await;
            *self.economy_cache.write().await = Some(eco);
            tokio::time::sleep(ECONOMY_REFRESH).await;
        }
    }

    /// Record an event description in the bounded activity feed (last 40).
    pub async fn push_event(&self, desc: String) {
        let mut q = self.recent_events.write().await;
        q.push_back(desc);
        while q.len() > 40 {
            q.pop_front();
        }
    }

    /// Record an event: append to the feed AND increment its per-type counter.
    pub async fn record_event(&self, ev: &zeph_events::Event) {
        self.push_event(ev.describe()).await;
        *self
            .event_counts
            .write()
            .await
            .entry(ev.tag().to_string())
            .or_insert(0) += 1;
    }

    pub async fn set_storage(&self, stats: zeph_store::StoreStats) {
        *self.storage.write().await = (
            stats.cids as u64,
            stats.pieces as u64,
            stats.pinned as u64,
            stats.bytes,
        );
    }

    pub fn set_providing(&self, n: u64) {
        self.providing
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn set_content(&self, content: Vec<ContentInfo>) {
        *self.content.write().await = content;
    }

    pub async fn set_cid_health(&self, rows: Vec<serde_json::Value>) {
        *self.cid_health.write().await = rows;
    }

    #[allow(clippy::too_many_arguments)]
    /// Health-scan results (its own coordinator job).
    #[allow(clippy::too_many_arguments)]
    pub async fn set_scan(
        &self,
        scanned: usize,
        at_risk: usize,
        repaired_delta: u64,
        degraded_delta: u64,
        fading: usize,
        offloaded_delta: u64,
        surplus: usize,
    ) {
        let mut h = self.health.write().await;
        h.0 = scanned;
        h.1 = at_risk;
        h.2 += repaired_delta;
        h.5 += degraded_delta;
        h.6 = fading as u64;
        h.7 += offloaded_delta;
        h.8 = surplus;
    }

    /// One repair completed by a standalone Repair-priority job (repairs no
    /// longer run inside the scan job in production, so the scan's
    /// `repaired_delta` is 0 there and this keeps the cumulative count right).
    pub async fn add_repaired(&self, n: u64) {
        self.health.write().await.2 += n;
    }

    /// Health-scan delay-queue depth (total scheduled) + due-now backlog.
    pub fn set_scan_queue(&self, total: usize, due: usize) {
        self.scan_queue
            .store(total, std::sync::atomic::Ordering::Relaxed);
        self.scan_due
            .store(due, std::sync::atomic::Ordering::Relaxed);
    }

    /// Distribute/scale results (the separate Distribution job).
    pub async fn set_flow(&self, moved_delta: u64, scaled_delta: u64) {
        let mut h = self.health.write().await;
        h.3 += moved_delta;
        h.4 += scaled_delta;
    }

    /// Replace the peer table wholesale (fed by the membership layer). Emits
    /// PeerConnected/PeerDisconnected events for the delta in the alive set.
    pub async fn set_peers(&self, peers: Vec<PeerStatus>, passive: u32, census: u32) {
        self.census
            .store(census, std::sync::atomic::Ordering::Relaxed);
        use std::collections::HashSet;
        let alive = |ps: &[PeerStatus]| -> HashSet<String> {
            ps.iter()
                .filter(|p| p.alive)
                .map(|p| p.id.clone())
                .collect()
        };
        let new_alive = alive(&peers);
        let old_alive = alive(&self.peers.read().await);
        for id in new_alive.difference(&old_alive) {
            if let Some(n) = parse_node_id(id) {
                self.events.publish(zeph_events::Event::PeerConnected(n));
            }
        }
        for id in old_alive.difference(&new_alive) {
            if let Some(n) = parse_node_id(id) {
                self.events.publish(zeph_events::Event::PeerDisconnected(n));
            }
        }
        *self.peers.write().await = peers;
        self.passive_peers
            .store(passive, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Serve JSON-RPC 2.0 over the Unix socket: one request per line.
/// Methods: "status", "identity".
pub async fn serve_unix(state: Arc<State>, sock_path: PathBuf) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(&sock_path);
    let listener = tokio::net::UnixListener::bind(&sock_path)?;
    tracing::info!(socket = %sock_path.display(), "control socket listening");
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let response = handle_rpc(&state, &line).await;
                let mut bytes = response.to_string().into_bytes();
                bytes.push(b'\n');
                if write.write_all(&bytes).await.is_err() {
                    return;
                }
            }
        });
    }
}

async fn handle_rpc(state: &State, line: &str) -> serde_json::Value {
    let request: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::json!({
                "jsonrpc": "2.0", "id": null,
                "error": {"code": -32700, "message": format!("parse error: {e}")}
            })
        }
    };
    let id = request
        .get("id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    match request.get("method").and_then(|m| m.as_str()) {
        Some("status") => {
            let snapshot = state.snapshot().await;
            serde_json::json!({"jsonrpc": "2.0", "id": id,
                "result": serde_json::to_value(snapshot).expect("status serializes")})
        }
        Some("identity") => serde_json::json!({"jsonrpc": "2.0", "id": id,
            "result": {"node_id": state.node_id, "listen": state.listen}}),
        Some("publish") => rpc_publish(state, &request, id).await,
        Some("files") => rpc_files(state, &request, id).await,
        Some("get") => rpc_get(state, &request, id).await,
        Some("pin") => rpc_cid_op(state, &request, id, "pin").await,
        Some("unpin") => rpc_cid_op(state, &request, id, "unpin").await,
        Some("want") => rpc_cid_op(state, &request, id, "want").await,
        Some("unwant") => rpc_cid_op(state, &request, id, "unwant").await,
        Some("fetch") => rpc_cid_op(state, &request, id, "fetch").await,
        Some("delete") => rpc_cid_op(state, &request, id, "delete").await,
        Some("ban") => rpc_cid_op(state, &request, id, "ban").await,
        Some("unban") => rpc_cid_op(state, &request, id, "unban").await,
        Some("invoke") => rpc_invoke(state, &request, id).await,
        Some("deploy") => rpc_deploy(state, &request, id).await,
        Some("publish_program") => rpc_publish_program(state, &request, id).await,
        Some("program_advance") => rpc_program_advance(state, &request, id).await,
        Some("program_resolve") => rpc_program_resolve(state, &request, id).await,
        Some("resolve_name") => rpc_resolve_name(state, &request, id).await,
        Some("anchor_resolve") => rpc_anchor_resolve(state, &request, id).await,
        Some("ledger_balance") => rpc_ledger_balance(state, &request, id).await,
        Some("ledger_transfer") => rpc_ledger_transfer(state, &request, id).await,
        Some("ledger_claim") => rpc_ledger_claim(state, &request, id).await,
        Some("ledger_pay") => rpc_ledger_pay(state, &request, id).await,
        Some("ledger_reward_claim") => rpc_ledger_reward_claim(state, &request, id).await,
        Some("ledger_settle_epoch") => rpc_ledger_settle_epoch(state, &request, id).await,
        Some("ledger_verification") => rpc_ledger_verification(state, id).await,
        Some("gov") => rpc_gov(state, id).await,
        Some("gov_propose") => rpc_gov_propose(state, &request, id).await,
        Some("gov_sign") => rpc_gov_sign(state, &request, id).await,
        Some("gov_submit") => rpc_gov_submit(state, &request, id).await,
        Some("attest_bootstrap") => rpc_attest_bootstrap(state, &request, id).await,
        Some("attest_propose") => rpc_attest_propose(state, &request, id).await,
        Some("attest_cosign") => rpc_attest_cosign(state, &request, id).await,
        Some("attest_submit") => rpc_attest_submit(state, &request, id).await,
        Some("attest_status") => rpc_attest_status(state, &request, id).await,
        Some("sequence_log") => rpc_sequence_log(state, &request, id).await,
        Some("programs") => rpc_programs(state, id).await,
        Some("config") => rpc_config(state, id).await,
        Some("committee") => rpc_committee(state, id).await,
        Some("attest_list") => rpc_attest_list(state, id).await,
        Some("board") => rpc_board(state, id).await,
        Some("apps") => {
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result": apps_list(state).await})
        }
        Some("setmeta") => rpc_setmeta(state, &request, id).await,
        Some("delmeta") => rpc_cid_op(state, &request, id, "delmeta").await,
        Some("sql_exec") => rpc_sql_exec(state, &request, id).await,
        Some("sql_query") => rpc_sql_query(state, &request, id).await,
        Some("sql_recover") => rpc_sql_recover(state, &request, id).await,
        Some("sql_compact") => rpc_sql_compact(state, &request, id).await,
        _ => serde_json::json!({"jsonrpc": "2.0", "id": id,
            "error": {"code": -32601, "message": "method not found"}}),
    }
}

fn param<'a>(req: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    req.get("params").and_then(|p| p.get(key))
}

fn rpc_err(id: serde_json::Value, msg: String) -> serde_json::Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": msg}})
}

fn parse_cid(hex: &str) -> Option<zeph_core::Cid> {
    let bytes: [u8; 32] = hex::decode(hex).ok()?.try_into().ok()?;
    Some(zeph_core::Cid(bytes))
}

type PublishFut = Pin<Box<dyn Future<Output = anyhow::Result<(zeph_core::Cid, u64, bool)>> + Send>>;
type RestoreFut = Pin<Box<dyn Future<Output = anyhow::Result<usize>> + Send>>;

/// Recursively publish a file or directory tree → (manifest_cid, size, is_dir).
/// A directory publishes each child first, then a Dir manifest of the entries.
fn publish_path(engine: Arc<zeph_obj::ObjEngine>, path: PathBuf, pin: bool) -> PublishFut {
    Box::pin(async move {
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "item".into());
        let meta = std::fs::metadata(&path)?;
        if meta.is_file() {
            let data = std::fs::read(&path)?;
            let fp = engine
                .publish_file(&name, &guess_mime(&name), &data, pin)
                .await?;
            Ok((fp.manifest_cid, fp.size, false))
        } else if meta.is_dir() {
            let mut children: Vec<PathBuf> = std::fs::read_dir(&path)?
                .flatten()
                .map(|e| e.path())
                .collect();
            children.sort();
            let mut entries = Vec::new();
            for child in children {
                let cname = child
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let (cid, size, is_dir) = publish_path(engine.clone(), child, pin).await?;
                entries.push(zeph_obj::Entry {
                    name: cname,
                    size,
                    is_dir,
                    cid: cid.0,
                });
            }
            let total = entries.iter().map(|e| e.size).sum();
            let cid = engine.publish_dir(&name, entries, pin).await?;
            Ok((cid, total, true))
        } else {
            anyhow::bail!("unsupported path: {}", path.display())
        }
    })
}

/// Recursively reconstruct a manifest into `dest` (a file path for a File, a
/// directory for a Dir). Returns the number of files written.
fn reconstruct(
    engine: Arc<zeph_obj::ObjEngine>,
    manifest_cid: zeph_core::Cid,
    dest: PathBuf,
) -> RestoreFut {
    Box::pin(async move {
        match engine.fetch_manifest(manifest_cid).await? {
            zeph_obj::Manifest::File { segments, .. } => {
                // Concatenate the file's segments in order (each cid verifies its bytes).
                let mut bytes = Vec::new();
                for seg in segments {
                    bytes.extend(
                        engine
                            .get(zeph_core::Cid(seg.cid), zeph_obj::ConsumeMode::Seed)
                            .await?,
                    );
                }
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&dest, bytes)?;
                Ok(1)
            }
            zeph_obj::Manifest::Dir { entries, .. } => {
                std::fs::create_dir_all(&dest)?;
                let mut count = 0;
                for e in entries {
                    count += reconstruct(engine.clone(), zeph_core::Cid(e.cid), dest.join(&e.name))
                        .await?;
                }
                Ok(count)
            }
        }
    })
}

/// Escape a value for a single-quoted SQL string literal.
fn sql_esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Record a published item in this identity's DRIVE — a per-identity CraftSQL DB
/// indexing everything you've published (your files on the grid). Best-effort:
/// a drive hiccup never fails the publish itself.
/// SUBSTRATE: record a published CID in this identity's OWNED INDEX — a minimal,
/// always-maintained CraftSQL DB (`owned`: cid, published_at) so a publishing
/// node never loses track of its own content (content-addressing is one-way).
/// The rich "drive" view below is DERIVED from this + the manifests (app on top).
async fn owned_add(state: &State, cid: &str) {
    let now = state.clock.now().millis();
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS owned(cid TEXT PRIMARY KEY, published_at INTEGER);\n\
         INSERT OR IGNORE INTO owned(cid, published_at) VALUES ('{}', {});",
        sql_esc(cid),
        now,
    );
    // The drive is PRIVATE — the owned index is encrypted under this identity's key.
    if let Ok(mut db) = state.craftsql.open_private("owned").await {
        if let Err(e) = db.write(&sql).await {
            tracing::warn!(%e, "owned index write failed");
        }
    }
}

/// Register a deployed CraftCOM app in the private "apps" index (name → the app's
/// SYSTEM-object cid). Upserts by name — re-deploying updates the cid (a light
/// name→current-CID versioning, cf. CRAFTCOM_DESIGN §13).
async fn apps_add(
    craftsql: std::sync::Arc<zeph_sql::CraftSql>,
    clock: std::sync::Arc<zeph_core::hlc::Clock>,
    name: String,
    cid: String,
    version: u64,
) {
    let now = clock.now().millis();
    // Best-effort migration: add the `version` column to a pre-existing table (a
    // no-op / ignored error if the table is absent or already migrated).
    if let Ok(mut db) = craftsql.open_private("apps").await {
        let _ = db
            .write("ALTER TABLE apps ADD COLUMN version INTEGER DEFAULT 1")
            .await;
    }
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS apps(name TEXT PRIMARY KEY, cid TEXT, version INTEGER, deployed_at INTEGER);\n\
         INSERT INTO apps(name, cid, version, deployed_at) VALUES ('{}', '{}', {}, {})\n\
         ON CONFLICT(name) DO UPDATE SET cid = excluded.cid, version = excluded.version, deployed_at = excluded.deployed_at;",
        sql_esc(&name),
        sql_esc(&cid),
        version,
        now,
    );
    if let Ok(mut db) = craftsql.open_private("apps").await {
        if let Err(e) = db.write(&sql).await {
            tracing::warn!(%e, "apps index write failed");
        }
    }
}

/// The deployed-apps registry: `{columns, rows}` of (name, cid, deployed_at).
async fn apps_list(state: &State) -> serde_json::Value {
    let empty =
        serde_json::json!({"columns": ["name", "cid", "version", "deployed_at"], "rows": []});
    match state.craftsql.open_private("apps").await {
        Ok(db) => db
            .query("SELECT name, cid, version, deployed_at FROM apps ORDER BY deployed_at DESC")
            .unwrap_or(empty),
        Err(_) => empty,
    }
}

/// Remove a CID from the drive index (on delete).
async fn owned_remove(state: &State, cid: &str) {
    let sql = format!("DELETE FROM owned WHERE cid = '{}';", sql_esc(cid));
    if let Ok(mut db) = state.craftsql.open_private("owned").await {
        let _ = db.write(&sql).await;
    }
}

/// Soft-delete YOUR OWN content: remove it from your drive, then unpin + unwant so
/// it's evictable and fades from the network (nothing wants it). Re-publishable —
/// NO tombstone (that's `ban`). For a private file, also unpins the ciphertext, so
/// as the local capsule is dropped + fades this is the best-effort crypto-shred
/// (Tier 2, docs/CRYPTO_SHRED_DESIGN.md): your copies go, network copies fade.
async fn soft_delete(state: &State, cid: zeph_core::Cid) -> anyhow::Result<()> {
    // Forget the whole file/folder chain (manifest/envelope + content/ciphertext +
    // any folder children) so nothing is orphaned.
    let _ = state.engine.forget_chain(cid).await;
    owned_remove(state, &cid.to_hex()).await;
    Ok(())
}

/// The DRIVE (bundled reference app): read `owner`'s owned index and enrich each
/// entry from its manifest (name/size/mime/is_dir). No denormalized copy — the
/// manifest is the source of truth; the drive is a view. `{columns, rows}`.
async fn drive_list(state: &State, owner: zeph_core::NodeId) -> serde_json::Value {
    let cols = serde_json::json!([
        "cid",
        "name",
        "size",
        "mime",
        "is_dir",
        "published_at",
        "is_private"
    ]);
    let empty = serde_json::json!({"columns": cols, "rows": []});
    let db = match state.craftsql.open_reader(owner, "owned").await {
        Ok(d) => d,
        Err(_) => return empty,
    };
    let owned = match tokio::task::spawn_blocking(move || {
        db.query("SELECT cid, published_at FROM owned ORDER BY published_at DESC")
    })
    .await
    {
        Ok(Ok(v)) => v,
        _ => return empty,
    };
    let rows_in = owned
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for r in rows_in {
        let Some(a) = r.as_array() else { continue };
        let cid_hex = a.first().and_then(|v| v.as_str()).unwrap_or("");
        let pub_at = a.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        let Some(cid) = parse_cid(cid_hex) else {
            continue;
        };
        // A private item is an EncryptedEnvelope — decrypt it for name/size/mime;
        // a public item resolves via its manifest.
        let is_private = state
            .engine
            .get(cid, zeph_obj::ConsumeMode::Drop)
            .await
            .map(|b| zeph_obj::EncryptedEnvelope::is_envelope(&b))
            .unwrap_or(false);
        let (name, size, mime, is_dir) = if is_private {
            match state.engine.get_private(cid).await {
                Ok(pf) => (pf.name, pf.content.len() as u64, pf.mime, false),
                Err(_) => ("(locked)".to_string(), 0, "encrypted".to_string(), false),
            }
        } else {
            match state.engine.fetch_manifest(cid).await {
                Ok(m) => {
                    let mime = match &m {
                        zeph_obj::Manifest::File { mime, .. } => mime.clone(),
                        _ => "inode/directory".to_string(),
                    };
                    (m.name().to_string(), m.size(), mime, m.is_dir())
                }
                Err(_) => ("(unavailable)".to_string(), 0u64, String::new(), false),
            }
        };
        out.push(serde_json::json!([
            cid_hex,
            name,
            size,
            mime,
            i32::from(is_dir),
            pub_at,
            is_private
        ]));
    }
    serde_json::json!({"columns": cols, "rows": out})
}

/// List this identity's DRIVE (or another owner's via `owner`) — the files
/// indexed in the CraftSQL `drive` DB, newest first. Empty if never published.
async fn rpc_files(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let owner = match param(req, "owner").and_then(|v| v.as_str()) {
        Some(h) => match parse_node_id(h) {
            Some(n) => n,
            None => return rpc_err(id, "owner must be 64 hex chars".into()),
        },
        None => match parse_node_id(&state.node_id) {
            Some(n) => n,
            None => return rpc_err(id, "self node id unparseable".into()),
        },
    };
    serde_json::json!({"jsonrpc":"2.0","id":id,"result": drive_list(state, owner).await})
}

async fn rpc_publish(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(path) = param(req, "path").and_then(|v| v.as_str()) else {
        return rpc_err(id, "publish needs a 'path'".into());
    };
    let pin = param(req, "pin").and_then(|v| v.as_bool()).unwrap_or(true);
    let private = param(req, "private")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let name = std::path::Path::new(path)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".into());
    // Directory → recursive folder manifest.
    if std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false) {
        if private {
            return rpc_err(id, "private publish supports files only (phase 2)".into());
        }
        return match publish_path(state.engine.clone(), PathBuf::from(path), pin).await {
            Ok((cid, size, _)) => {
                owned_add(state, &cid.to_hex()).await;
                state.events.publish(zeph_events::Event::CidWritten {
                    cid,
                    name: Some(name.clone()),
                    pinned: pin,
                });
                serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                    "cid": cid.to_hex(), "name": name, "size": size, "is_dir": true, "pinned": pin,
                }})
            }
            Err(e) => rpc_err(id, format!("publish folder failed: {e}")),
        };
    }
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) => return rpc_err(id, format!("reading {path}: {e}")),
    };
    let mime = guess_mime(&name);
    if private {
        return match state.engine.publish_private(&name, &mime, &data, pin).await {
            Ok(pp) => {
                owned_add(state, &pp.envelope_cid.to_hex()).await;
                state.events.publish(zeph_events::Event::CidWritten {
                    cid: pp.envelope_cid,
                    name: Some(name.clone()),
                    pinned: pin,
                });
                serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                    "cid": pp.envelope_cid.to_hex(), "name": name, "mime": mime,
                    "size": pp.size, "private": true, "durable": pp.durable, "pinned": pin,
                }})
            }
            Err(e) => rpc_err(id, format!("private publish failed: {e}")),
        };
    }
    match state.engine.publish_file(&name, &mime, &data, pin).await {
        Ok(fp) => {
            owned_add(state, &fp.manifest_cid.to_hex()).await;
            state.events.publish(zeph_events::Event::CidWritten {
                cid: fp.manifest_cid,
                name: Some(name.clone()),
                pinned: fp.pinned,
            });
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                "cid": fp.manifest_cid.to_hex(), "content_cid": fp.content_cid.to_hex(),
                "name": name, "mime": mime, "size": fp.size,
                "durable": fp.durable, "pinned": fp.pinned, "bytes": data.len(),
            }})
        }
        Err(e) => rpc_err(id, format!("publish failed: {e}")),
    }
}

/// Guess a MIME type from a filename extension (best-effort).
fn guess_mime(name: &str) -> String {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "txt" => "text/plain",
        "md" => "text/markdown",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "json" => "application/json",
        "pdf" => "application/pdf",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "mp3" => "audio/mpeg",
        "zip" => "application/zip",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
    .to_string()
}

async fn rpc_get(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(cid_hex), Some(output)) = (
        param(req, "cid").and_then(|v| v.as_str()),
        param(req, "output").and_then(|v| v.as_str()),
    ) else {
        return rpc_err(id, "get needs 'cid' and 'output'".into());
    };
    let Some(cid) = parse_cid(cid_hex) else {
        return rpc_err(id, "cid must be 64 hex chars".into());
    };
    // Partial/RANGE read: fetch + (for a private file, decrypt) ONLY the segments overlapping
    // [offset, offset+length) — cheap streaming/seek over a large file.
    if let (Some(offset), Some(length)) = (
        param(req, "offset").and_then(|v| v.as_u64()),
        param(req, "length").and_then(|v| v.as_u64()),
    ) {
        let bytes = match state.engine.get(cid, zeph_obj::ConsumeMode::Drop).await {
            Ok(b) if zeph_obj::EncryptedEnvelope::is_envelope(&b) => {
                state.engine.get_private_range(cid, offset, length).await
            }
            _ => state.engine.fetch_file_range(cid, offset, length).await,
        };
        return match bytes {
            Ok(bytes) => match std::fs::write(output, &bytes) {
                Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                    "path": output, "range": true, "bytes": bytes.len(),
                }}),
                Err(e) => rpc_err(id, format!("writing {output}: {e}")),
            },
            Err(e) => rpc_err(id, format!("range read failed: {e}")),
        };
    }
    // Encrypted? An envelope CID decrypts (owner only) to the private file.
    if let Ok(bytes) = state.engine.get(cid, zeph_obj::ConsumeMode::Drop).await {
        if zeph_obj::EncryptedEnvelope::is_envelope(&bytes) {
            return match state.engine.get_private(cid).await {
                Ok(pf) => {
                    let out_path = std::path::Path::new(output);
                    let dest = if out_path.is_dir() {
                        out_path.join(&pf.name)
                    } else {
                        out_path.to_path_buf()
                    };
                    match std::fs::write(&dest, &pf.content) {
                        Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                            "path": dest.to_string_lossy(), "name": pf.name,
                            "is_dir": false, "private": true, "bytes": pf.content.len(),
                        }}),
                        Err(e) => rpc_err(id, format!("writing {}: {e}", dest.display())),
                    }
                }
                Err(e) => rpc_err(id, format!("decrypt failed (not your content?): {e}")),
            };
        }
    }
    // A manifest CID restores a named file or a folder tree; a raw content CID
    // just writes bytes.
    match state.engine.fetch_manifest(cid).await {
        Ok(m) => {
            let out_path = std::path::Path::new(output);
            let (name, is_dir) = (m.name().to_string(), m.is_dir());
            let dest = if out_path.is_dir() {
                out_path.join(&name)
            } else {
                out_path.to_path_buf()
            };
            match reconstruct(state.engine.clone(), cid, dest.clone()).await {
                Ok(files) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                    "path": dest.to_string_lossy(), "name": name,
                    "is_dir": is_dir, "files": files,
                }}),
                Err(e) => rpc_err(id, format!("restore failed: {e}")),
            }
        }
        Err(_) => match state.engine.get(cid, zeph_obj::ConsumeMode::Seed).await {
            Ok(bytes) => match std::fs::write(output, &bytes) {
                Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id,
                    "result": {"bytes": bytes.len(), "path": output, "name": null}}),
                Err(e) => rpc_err(id, format!("writing {output}: {e}")),
            },
            Err(e) => rpc_err(id, format!("get failed: {e}")),
        },
    }
}

fn parse_node_id(s: &str) -> Option<zeph_core::NodeId> {
    let s = s.trim();
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(zeph_core::NodeId(out))
}

/// Execute write SQL against this node's own CraftSQL database `ns`, committing
/// and publishing the new KIND_ROOT head.
async fn rpc_sql_exec(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(ns), Some(sql)) = (
        param(req, "ns").and_then(|v| v.as_str()),
        param(req, "sql").and_then(|v| v.as_str()),
    ) else {
        return rpc_err(id, "sql_exec needs 'ns' and 'sql'".into());
    };
    let mut db = match state.craftsql.open(ns).await {
        Ok(d) => d,
        Err(e) => return rpc_err(id, format!("open failed: {e}")),
    };
    match db.write(sql).await {
        Ok(()) => {
            if let Some(root) = db.root() {
                state.events.publish(zeph_events::Event::PageCommitted {
                    namespace: ns.to_string(),
                    root,
                });
            }
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
                "ok": true, "root": db.root().map(|c| c.to_hex()),
            }})
        }
        Err(e) => rpc_err(id, format!("sql_exec failed: {e}")),
    }
}

/// Query a CraftSQL database — this node's own, or another owner's (`owner` hex).
async fn rpc_sql_query(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(ns), Some(sql)) = (
        param(req, "ns").and_then(|v| v.as_str()),
        param(req, "sql").and_then(|v| v.as_str()),
    ) else {
        return rpc_err(id, "sql_query needs 'ns' and 'sql'".into());
    };
    let owner = match param(req, "owner").and_then(|v| v.as_str()) {
        Some(h) => match parse_node_id(h) {
            Some(n) => n,
            None => return rpc_err(id, "owner must be 64 hex chars".into()),
        },
        None => match parse_node_id(&state.node_id) {
            Some(n) => n,
            None => return rpc_err(id, "self node id unparseable".into()),
        },
    };
    let db = match state.craftsql.open_reader(owner, ns).await {
        Ok(d) => d,
        Err(e) => return rpc_err(id, format!("open_reader failed: {e}")),
    };
    // Run the query off the async workers — a lazy read blocks on the sync→async
    // fetch bridge, which must not hold a runtime worker.
    let sql = sql.to_string();
    match tokio::task::spawn_blocking(move || db.query(&sql)).await {
        Ok(Ok(v)) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": v}),
        Ok(Err(e)) => rpc_err(id, format!("query failed: {e}")),
        Err(e) => rpc_err(id, format!("query task: {e}")),
    }
}

/// Compact one of this node's own CraftSQL DBs.
async fn rpc_sql_compact(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(ns) = param(req, "ns").and_then(|v| v.as_str()) else {
        return rpc_err(id, "sql_compact needs 'ns'".into());
    };
    match state.craftsql.compact(ns).await {
        Ok(n) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"reclaimed": n}}),
        Err(e) => rpc_err(id, format!("compact failed: {e}")),
    }
}

/// Rebuild a CraftSQL DB (own or another owner's) from its durable generations,
/// discovered via the network manifest.
async fn rpc_sql_recover(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(ns) = param(req, "ns").and_then(|v| v.as_str()) else {
        return rpc_err(id, "sql_recover needs 'ns'".into());
    };
    let owner = match param(req, "owner").and_then(|v| v.as_str()) {
        Some(h) => match parse_node_id(h) {
            Some(n) => n,
            None => return rpc_err(id, "owner must be 64 hex chars".into()),
        },
        None => match parse_node_id(&state.node_id) {
            Some(n) => n,
            None => return rpc_err(id, "self node id unparseable".into()),
        },
    };
    match state.craftsql.recover_owner(owner, ns).await {
        Ok(n) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"restored": n}}),
        Err(e) => rpc_err(id, format!("recover failed: {e}")),
    }
}

async fn rpc_publish_program(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(h) = req["params"].get("wasm").and_then(|v| v.as_str()) else {
        return rpc_err(id, "publish_program needs 'wasm' hex".into());
    };
    let Ok(bytes) = hex::decode(h.trim()) else {
        return rpc_err(id, "bad wasm hex".into());
    };
    // Content cid = what a governance SetProgram would reference (and nodes fetch by).
    let content_cid = hex::encode(zeph_core::Cid::of(&bytes).0);
    match state.engine.publish_system(&bytes).await {
        Ok(_) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":
            {"cid": content_cid, "size": bytes.len()}}),
        Err(e) => rpc_err(id, format!("publish failed: {e}")),
    }
}

/// Parse a required 32-byte hex field (e.g. a program cid) from the params.
fn parse_hex32(v: Option<&serde_json::Value>) -> Result<[u8; 32], String> {
    let h = v.and_then(|x| x.as_str()).ok_or("missing hex field")?;
    <[u8; 32]>::try_from(hex::decode(h.trim()).map_err(|_| "bad hex")?.as_slice())
        .map_err(|_| "expected 32 bytes".to_string())
}

/// Parse an optional hex-bytes field (empty/absent → empty vec).
fn parse_hex_bytes(v: Option<&serde_json::Value>) -> Result<Vec<u8>, String> {
    match v.and_then(|x| x.as_str()) {
        Some(h) if !h.trim().is_empty() => hex::decode(h.trim()).map_err(|_| "bad hex".into()),
        _ => Ok(Vec::new()),
    }
}

/// Advance a generic program account: run <program> on `(state, request)` — the program IS the
/// writer. Returns {account, root}.
async fn rpc_program_advance(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let seed = match parse_hex_bytes(req["params"].get("seed")) {
        Ok(s) => s,
        Err(_) => return rpc_err(id, "bad seed hex".into()),
    };
    let request = match parse_hex_bytes(req["params"].get("request")) {
        Ok(r) => r,
        Err(_) => return rpc_err(id, "bad request hex".into()),
    };
    match state
        .accounts
        .advance(program, program, &seed, &request)
        .await
    {
        Ok(r) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":
            {"account": hex::encode(r.account), "root": hex::encode(r.new_root)}}),
        Err(e) => rpc_err(id, e.to_string()),
    }
}

/// Read a generic program account's current (local) state. Returns {account, state, size}.
async fn rpc_program_resolve(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let seed = match parse_hex_bytes(req["params"].get("seed")) {
        Ok(s) => s,
        Err(_) => return rpc_err(id, "bad seed hex".into()),
    };
    let st = state.accounts.resolve(program, &seed).await;
    let account = hex::encode(zeph_com::pda(&program, &seed).0);
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":
        {"account": account, "state": hex::encode(&st), "size": st.len()}})
}

/// Resolve a published app name → its current cid WITHOUT fetching content. Mirrors the
/// registry resolution done inside `rpc_invoke`, but stops at the name→cid lookup: this is the
/// resolve-only path that lets a resolve be tested (and tolerate a briefly-unreachable writer,
/// via the replica-fallback in `HeadRegistry::resolve`) with no object fetch. Params:
/// `{ "owner": "<hex64>", "name": "<str>" }`. Returns `{ "cid": "<hex64>" }` or `{ "cid": null }`.
async fn rpc_resolve_name(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(owner_hex), Some(name)) = (
        param(req, "owner").and_then(|v| v.as_str()),
        param(req, "name").and_then(|v| v.as_str()),
    ) else {
        return rpc_err(id, "resolve_name needs 'owner' and 'name'".into());
    };
    let owner: [u8; 32] = match hex::decode(owner_hex.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
    {
        Some(o) => o,
        None => return rpc_err(id, "owner must be 64 hex chars".into()),
    };
    let cid = state.registry.resolve(owner, name).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"cid": cid.map(hex::encode)}})
}

/// Resolve a K1 anchor: a canonical name → its governance-pinned program cid + interface version +
/// the deterministic sentinel owner. `cid: null` when nothing is anchored at that name.
async fn rpc_anchor_resolve(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(name) = param(req, "name").and_then(|v| v.as_str()) else {
        return rpc_err(id, "anchor_resolve needs a 'name'".into());
    };
    match state.anchor.resolve(name).await {
        Some(res) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
            "cid": hex::encode(res.cid),
            "interface_version": res.interface_version,
            "owner": hex::encode(crate::anchor::AnchorDispatcher::anchor_owner(&res.cid)),
        }}),
        None => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"cid": null}}),
    }
}

/// Fold `account` (default: this node's own account) → its token balance.
async fn rpc_ledger_balance(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let account: [u8; 32] = match param(req, "account").and_then(|v| v.as_str()) {
        Some(hex) => match hex::decode(hex.trim()).ok().and_then(|b| b.try_into().ok()) {
            Some(a) => a,
            None => return rpc_err(id, "account must be 64 hex chars".into()),
        },
        None => match parse_node_id(&state.node_id) {
            Some(n) => n.0,
            None => return rpc_err(id, "self node id unparseable".into()),
        },
    };
    let bal = state.ledger.balance(account).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
        "account": hex::encode(account), "balance": bal.balance,
    }})
}

/// Pay `amount` of this node's balance into the epoch egress pool.
async fn rpc_ledger_pay(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(amount) = param(req, "amount").and_then(|v| v.as_u64()) else {
        return rpc_err(id, "pay needs 'amount'".into());
    };
    let ok = state.ledger.pay(amount).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"committed": ok}})
}

/// Claim this node's reward share for `epoch` (single-use).
async fn rpc_ledger_reward_claim(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(epoch) = param(req, "epoch").and_then(|v| v.as_u64()) else {
        return rpc_err(id, "reward_claim needs 'epoch'".into());
    };
    let ok = state.ledger.reward_claim(epoch).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"committed": ok}})
}

/// DEV override: inject `pool` and settle an epoch with THIS node's contribution (`bytes`), exercising
/// the settlement math offline. The PRODUCTION path is the automatic `SettlementService` cross-node loop
/// (announce → converge → deterministic settle); this bypasses it. Returns this node's resolved share +
/// the remaining `unallocated` pool.
async fn rpc_ledger_settle_epoch(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(epoch), Some(pool_in), Some(bytes)) = (
        param(req, "epoch").and_then(|v| v.as_u64()),
        param(req, "pool").and_then(|v| v.as_u64()),
        param(req, "bytes").and_then(|v| v.as_u64()),
    ) else {
        return rpc_err(id, "settle_epoch needs 'epoch', 'pool', and 'bytes'".into());
    };
    let me = match parse_node_id(&state.node_id) {
        Some(n) => n.0,
        None => return rpc_err(id, "self node id unparseable".into()),
    };
    let rec = state
        .economy
        .dev_settle_epoch(
            epoch,
            pool_in,
            vec![zeph_reward::Contribution {
                provider: me,
                bytes,
                cumulative_bytes: bytes, // dev override: no running history to carry
            }],
        )
        .await;
    let pool = state.economy.pool_unallocated().await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
        "epoch": epoch, "share": rec.share_of(&me), "pool_unallocated": pool,
    }})
}

/// The settlement re-execution verification tally: epochs whose canonical committee-attested record this
/// node independently re-computed and confirmed (`verified`) vs found divergent (`mismatched`), plus the
/// current distributable pool — observability for the correctness-by-re-execution loop.
async fn rpc_ledger_verification(state: &State, id: serde_json::Value) -> serde_json::Value {
    let (verified, mismatched) = state.settlement.verification_stats();
    let pool = state.economy.pool_unallocated().await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
        "verified": verified, "mismatched": mismatched, "pool_unallocated": pool,
    }})
}

/// Transfer `amount` from this node's account to `to` (a debit; the recipient later claims it).
async fn rpc_ledger_transfer(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(to_hex), Some(amount)) = (
        param(req, "to").and_then(|v| v.as_str()),
        param(req, "amount").and_then(|v| v.as_u64()),
    ) else {
        return rpc_err(id, "transfer needs 'to' (64 hex) and 'amount'".into());
    };
    let to: [u8; 32] = match hex::decode(to_hex.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
    {
        Some(t) => t,
        None => return rpc_err(id, "to must be 64 hex chars".into()),
    };
    let ok = state.ledger.transfer(to, amount).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"committed": ok}})
}

/// Claim a transfer `(debit_account, debit_nonce)` credited to this node's account.
async fn rpc_ledger_claim(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let (Some(from_hex), Some(nonce)) = (
        param(req, "debit_account").and_then(|v| v.as_str()),
        param(req, "debit_nonce").and_then(|v| v.as_u64()),
    ) else {
        return rpc_err(
            id,
            "claim needs 'debit_account' (64 hex) and 'debit_nonce'".into(),
        );
    };
    let from: [u8; 32] = match hex::decode(from_hex.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
    {
        Some(f) => f,
        None => return rpc_err(id, "debit_account must be 64 hex chars".into()),
    };
    let ok = state.ledger.claim(from, nonce).await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"committed": ok}})
}

async fn rpc_deploy(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(path) = param(req, "path").and_then(|v| v.as_str()) else {
        return rpc_err(id, "deploy needs a 'path'".into());
    };
    let name = param(req, "name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            std::path::Path::new(path)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
        })
        .unwrap_or_else(|| "app".into());
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => return rpc_err(id, format!("read {path}: {e}")),
    };
    match deploy_bytes(state, &name, &bytes).await {
        Ok(result) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(e) => rpc_err(id, e),
    }
}

/// Parse a `GovAction` from rpc params.
fn parse_gov_action(p: &serde_json::Value) -> Result<zeph_com::GovAction, String> {
    use zeph_com::GovAction;
    let hex32 = |v: Option<&serde_json::Value>| -> Result<[u8; 32], String> {
        let h = v.and_then(|x| x.as_str()).ok_or("missing hex field")?;
        <[u8; 32]>::try_from(hex::decode(h.trim()).map_err(|_| "bad hex")?.as_slice())
            .map_err(|_| "expected 32 bytes".to_string())
    };
    match p.get("action").and_then(|v| v.as_str()) {
        Some("add") => Ok(GovAction::AddGovernor {
            governor: hex32(p.get("governor"))?,
        }),
        Some("remove") => Ok(GovAction::RemoveGovernor {
            governor: hex32(p.get("governor"))?,
        }),
        Some("threshold") => Ok(GovAction::SetThreshold {
            threshold: p
                .get("value")
                .and_then(|v| v.as_u64())
                .ok_or("missing value")?,
        }),
        Some("set_program") => Ok(GovAction::SetProgram {
            name: p
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or("missing name")?
                .to_string(),
            cid: hex32(p.get("cid"))?,
        }),
        Some("set_config") => Ok(GovAction::SetConfig {
            key: p
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("missing key")?
                .to_string(),
            value: p
                .get("value")
                .and_then(|v| v.as_i64())
                .ok_or("missing value")?,
        }),
        _ => Err("unknown action (add|remove|threshold|set_program|set_config)".into()),
    }
}

fn gov_set_json(set: &zeph_com::GovernanceSet, is_gov: bool) -> serde_json::Value {
    serde_json::json!({
        "governors": set.members.iter().map(hex::encode).collect::<Vec<_>>(),
        "threshold": set.threshold,
        "seq": set.seq,
        "is_governor": is_gov,
    })
}

async fn rpc_programs(state: &State, id: serde_json::Value) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = state
        .gov
        .rows()
        .await
        .into_iter()
        .map(|(name, cid, ver)| serde_json::json!([name, cid, ver]))
        .collect();
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"programs": rows}})
}

/// All governed config `(key, value)` pairs (the SetConfig registry — reciprocity grants, anchor ifaces).
async fn rpc_config(state: &State, id: serde_json::Value) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = state
        .gov
        .list_config()
        .await
        .into_iter()
        .map(|(k, v)| serde_json::json!([k, v]))
        .collect();
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"config": rows}})
}

/// The current rotating EPOCH COMMITTEE (automated attestation) for the anchored token program —
/// the computed k-of-n quorum that orders its writes this epoch.
async fn rpc_committee(state: &State, id: serde_json::Value) -> serde_json::Value {
    let program = crate::ledger::token_program_cid();
    let epoch = crate::epoch::epoch_at(state.clock.now().millis());
    let quorum = state
        .epoch_committee
        .committee_for_epoch(&program, epoch)
        .await;
    let (members, threshold) = quorum
        .map(|q| {
            (
                q.members.iter().map(hex::encode).collect::<Vec<_>>(),
                q.threshold,
            )
        })
        .unwrap_or_default();
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
        "epoch": epoch, "program": crate::anchor::TOKEN_ANCHOR, "members": members, "threshold": threshold,
    }})
}

/// All locally-known ATTESTATION quorums (user-declared k-of-n authority) — owner, program, members, seq.
async fn rpc_attest_list(state: &State, id: serde_json::Value) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = state
        .attest
        .list()
        .await
        .into_iter()
        .map(|(owner, program, quorum, seq)| {
            let (members, threshold) = quorum
                .map(|q| {
                    (
                        q.members.iter().map(hex::encode).collect::<Vec<_>>(),
                        q.threshold,
                    )
                })
                .unwrap_or_default();
            serde_json::json!({
                "owner": hex::encode(owner), "program": hex::encode(program),
                "members": members, "threshold": threshold, "seq": seq,
            })
        })
        .collect();
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"quorums": rows}})
}

/// The verification board (§ VERIFICATION_DESIGN) — open re-execution consistency: totals + one row
/// per posted request with its live agreeing-verdict tally against the app's declared `k`.
async fn rpc_board(state: &State, id: serde_json::Value) -> serde_json::Value {
    let v = state.board.dashboard().await;
    let rows: Vec<serde_json::Value> = v
        .rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "program": r.program, "func": r.func, "k": r.k, "set": r.set,
                "agreements": r.agreements, "satisfied": r.satisfied, "producer": r.producer,
            })
        })
        .collect();
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
        "requests": v.requests, "verdicts": v.verdicts, "satisfied": v.satisfied, "rows": rows,
    }})
}

async fn rpc_gov(state: &State, id: serde_json::Value) -> serde_json::Value {
    let set = state.gov.current().await;
    let ig = state.gov.is_governor().await;
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": gov_set_json(&set, ig)})
}

async fn rpc_gov_propose(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    if !state.gov.is_governor().await {
        return rpc_err(id, "this node is not a governor".into());
    }
    let action = match parse_gov_action(&req["params"]) {
        Ok(a) => a,
        Err(e) => return rpc_err(id, e),
    };
    let approval = state.gov.draft(action).await;
    // Try to apply now (sufficient at 1-of-1); else hand back the partial for co-signing.
    match state.gov.submit(&approval).await {
        Ok(set) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result":
            {"applied": true, "set": gov_set_json(&set, true)}}),
        Err(_) => {
            let hex = hex::encode(postcard::to_allocvec(&approval).unwrap_or_default());
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result":
                {"applied": false, "approval": hex, "note": "needs more governor signatures (gov_sign)"}})
        }
    }
}

async fn rpc_gov_sign(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(h) = req["params"].get("approval").and_then(|v| v.as_str()) else {
        return rpc_err(id, "gov_sign needs 'approval' hex".into());
    };
    let Ok(bytes) = hex::decode(h.trim()) else {
        return rpc_err(id, "bad approval hex".into());
    };
    let Ok(mut approval) = postcard::from_bytes::<zeph_com::GovernanceApproval>(&bytes) else {
        return rpc_err(id, "undecodable approval".into());
    };
    // Add this node's signature (if a governor + not already present).
    match state.gov.cosign(&mut approval).await {
        Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result":
            {"approval": hex::encode(postcard::to_allocvec(&approval).unwrap_or_default())}}),
        Err(e) => rpc_err(id, e.to_string()),
    }
}

async fn rpc_gov_submit(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(h) = req["params"].get("approval").and_then(|v| v.as_str()) else {
        return rpc_err(id, "gov_submit needs 'approval' hex".into());
    };
    let Ok(bytes) = hex::decode(h.trim()) else {
        return rpc_err(id, "bad approval hex".into());
    };
    let Ok(approval) = postcard::from_bytes::<zeph_com::GovernanceApproval>(&bytes) else {
        return rpc_err(id, "undecodable approval".into());
    };
    match state.gov.submit(&approval).await {
        Ok(set) => {
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result": gov_set_json(&set, true)})
        }
        Err(e) => rpc_err(id, e.to_string()),
    }
}

// ---- Attestation control plane (per-program quorum authority; Package A manual cosign) ----

async fn rpc_attest_bootstrap(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let Some(list) = req["params"].get("members").and_then(|v| v.as_str()) else {
        return rpc_err(
            id,
            "attest_bootstrap needs 'members' (comma-separated hex)".into(),
        );
    };
    let mut members = Vec::new();
    for h in list.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
        match hex::decode(h)
            .ok()
            .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok())
        {
            Some(m) => members.push(m),
            None => return rpc_err(id, format!("bad member hex: {h}")),
        }
    }
    let threshold = req["params"]
        .get("threshold")
        .and_then(|v| v.as_u64())
        .unwrap_or(members.len() as u64) as usize;
    state.attest.bootstrap(program, members, threshold).await;
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"bootstrapped": true}})
}

async fn rpc_attest_propose(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let Some(stmt) = req["params"].get("statement").and_then(|v| v.as_str()) else {
        return rpc_err(id, "attest_propose needs 'statement'".into());
    };
    match state
        .attest
        .propose(program, stmt.as_bytes().to_vec())
        .await
    {
        Some(att) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":{
            "attestation": hex::encode(postcard::to_allocvec(&att).unwrap_or_default()),
            "note": "send to each quorum member for attest-cosign, then attest-submit the k-of-n result"}}),
        None => rpc_err(id, "no quorum bootstrapped for this program".into()),
    }
}

async fn rpc_attest_cosign(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(h) = req["params"].get("attestation").and_then(|v| v.as_str()) else {
        return rpc_err(id, "attest_cosign needs 'attestation' hex".into());
    };
    let Ok(mut att) = hex::decode(h.trim())
        .ok()
        .and_then(|b| postcard::from_bytes::<zeph_com::Attestation>(&b).ok())
        .ok_or(())
    else {
        return rpc_err(id, "undecodable attestation".into());
    };
    state.attest.cosign(&mut att).await;
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":{
        "attestation": hex::encode(postcard::to_allocvec(&att).unwrap_or_default())}})
}

async fn rpc_attest_submit(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let Some(att) = req["params"]
        .get("attestation")
        .and_then(|v| v.as_str())
        .and_then(|h| hex::decode(h.trim()).ok())
        .and_then(|b| postcard::from_bytes::<zeph_com::Attestation>(&b).ok())
    else {
        return rpc_err(
            id,
            "attest_submit needs a decodable 'attestation' hex".into(),
        );
    };
    if state.attest.submit(program, att).await {
        serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"submitted": true}})
    } else {
        rpc_err(
            id,
            "attestation rejected: bad quorum, wrong seq, or program not bootstrapped".into(),
        )
    }
}

async fn rpc_attest_status(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let Some(stmt) = req["params"].get("statement").and_then(|v| v.as_str()) else {
        return rpc_err(id, "attest_status needs 'statement'".into());
    };
    // The quorum is keyed by (owner, program). Default owner = this node (checking a program you
    // own); pass `owner` (64 hex) to check another owner's quorum for the program.
    let owner = match req["params"].get("owner").and_then(|v| v.as_str()) {
        Some(o) => match parse_node_id(o) {
            Some(n) => n.0,
            None => return rpc_err(id, "owner must be 64 hex chars".into()),
        },
        None => state.attest.owner(),
    };
    let authorized = state
        .attest
        .is_authorized(&owner, &program, stmt.as_bytes())
        .await;
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":{"authorized": authorized, "owner": hex::encode(owner)}})
}

/// Read an account's committed ordered-write log — the sequencer's cross-node serve path. Keyed by
/// `(owner, program, account)`; default owner = this node, `owner` (64 hex) reads another owner's.
/// Syncs from peers first, so ANY node returns the same committed order. Returns the length + each
/// nonce's payload (hex).
async fn rpc_sequence_log(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let program = match parse_hex32(req["params"].get("program")) {
        Ok(p) => p,
        Err(e) => return rpc_err(id, e),
    };
    let account = match parse_hex32(req["params"].get("account")) {
        Ok(a) => a,
        Err(e) => return rpc_err(id, e),
    };
    let owner = match req["params"].get("owner").and_then(|v| v.as_str()) {
        Some(o) => match parse_node_id(o) {
            Some(n) => n.0,
            None => return rpc_err(id, "owner must be 64 hex chars".into()),
        },
        None => state.attest.owner(),
    };
    let (len, entries) = match state.sequence.sequence_of(owner, program, account).await {
        Some(log) => {
            let entries: Vec<String> = (0..log.next_nonce())
                .map(|n| hex::encode(log.payload_at(n).unwrap_or_default()))
                .collect();
            (log.next_nonce(), entries)
        }
        None => (0, vec![]),
    };
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":{
        "owner": hex::encode(owner), "len": len, "entries": entries}})
}

/// Deploy `bytes` as a system app under `name`: publish a want-based system object,
/// announce the signed KIND_APP head (version = prev+1), and record it locally. Shared
/// by the CLI (`rpc_deploy`, reads a file) and the dashboard (`api_deploy`, hex bytes).
async fn deploy_bytes(
    state: &State,
    name: &str,
    bytes: &[u8],
) -> Result<serde_json::Value, String> {
    if name.is_empty() || name.chars().any(|c| c.is_control()) {
        return Err("invalid app name (empty or control chars reserved)".into());
    }
    // Content-addressing: the cid is BLAKE3(bytes) — known instantly, so there is NO need to wait
    // for the publish to distribute pieces to peers. The node retains its own copy; durability is
    // reached asynchronously (background pushes + health scan). Retain+distribute in the background
    // and register the cid now, so a deploy stays sub-second even when a peer is slow.
    let cid = zeph_core::Cid::of(bytes);
    {
        let engine = state.engine.clone();
        let blob = bytes.to_vec();
        tokio::spawn(async move {
            let _ = engine.publish_system(&blob).await;
        });
    }
    let version = match parse_node_id(&state.node_id) {
        Some(own) => {
            state
                .registry
                .current_version(crate::headreg::RT_PROGRAM, own.0, name)
                .await
                + 1
        }
        None => 1,
    };
    // Register into the program registry — the registry program validates the submission
    // (e.g. rejects an invalid name), failing the deploy if so. The program-account store
    // persists + publishes it durably; no DHT announce.
    state
        .registry
        .register(
            crate::headreg::RT_PROGRAM,
            name,
            cid.0,
            version,
            state.clock.now().millis(),
        )
        .await
        .map_err(|e| e.to_string())?;
    // The app index (the UI's "apps" table) is local bookkeeping — a CraftSQL write whose
    // page-commit publishes durably, hitting the same slow-peer publish path. Don't block the
    // deploy on it; fire-and-forget.
    {
        let craftsql = state.craftsql.clone();
        let clock = state.clock.clone();
        let (n, c) = (name.to_string(), cid.to_hex());
        tokio::spawn(async move {
            apps_add(craftsql, clock, n, c, version).await;
        });
    }
    Ok(serde_json::json!({
        "name": name, "cid": cid.to_hex(), "size": bytes.len(), "version": version
    }))
}

/// Invoke a CraftCOM app LOCALLY: run `func` from the WASM at `wasm_cid` against
/// this node's `app.<app_ns>` namespace. Caller = this node's own identity (a local
/// invocation); remote callers come in over INVOKE_ALPN with their own identity.
async fn rpc_invoke(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let p = &req["params"];
    let func = p.get("func").and_then(|v| v.as_str()).unwrap_or("run");
    // K1 anchor invoke: dispatch to the governance-pinned program behind a canonical anchor name.
    // The sentinel owner drives the quorum lookup — never a caller-supplied owner.
    if let Some(anchor_name) = p
        .get("anchor")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        let caller = match parse_node_id(&state.node_id) {
            Some(n) => n.0,
            None => return rpc_err(id, "self node id unparseable".into()),
        };
        let input = p
            .get("input")
            .and_then(|v| v.as_str())
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_default();
        return match state
            .anchor
            .invoke_anchor(anchor_name, func, input, caller)
            .await
        {
            Ok(out) => {
                serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"output": hex::encode(out)}})
            }
            Err(e) => rpc_err(id, format!("anchor invoke failed: {e}")),
        };
    }
    // Resolve the app's WASM cid — by NAME (`<pubhex>/<name>` or `<name>` = own,
    // via the signed KIND_APP head) or by a raw `wasm_cid`.
    // `program_owner` = the registry-authenticated publisher when resolved by name — the identity
    // whose declared quorum `attest` consults. `None` for a raw-cid invoke (no authenticated owner);
    // it MUST come from this node's own registry resolution, never a caller-supplied field, so an
    // invoker can't self-authorize.
    let (wasm_cid, default_ns, program_owner) = if let Some(nm) = p
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        let (publisher, app_name) = match nm.split_once('/') {
            Some((ph, n)) => (parse_node_id(ph), n),
            None => (parse_node_id(&state.node_id), nm),
        };
        let Some(publisher) = publisher else {
            return rpc_err(id, "name: bad publisher (expected <hex>/<app>)".into());
        };
        // Registry resolution: the program-name registry itself now handles cross-node
        // resolution (a non-writer queries the designated writer over REGISTRY_ALPN), so
        // there is no DHT/KIND_APP fallback — a `None` here is a genuine not-found.
        match state.registry.resolve(publisher.0, app_name).await {
            Some(cid) => (cid, app_name.to_string(), Some(publisher.0)),
            None => return rpc_err(id, format!("app '{nm}' not found (deploy it first?)")),
        }
    } else {
        let wasm_hex = p.get("wasm_cid").and_then(|v| v.as_str()).unwrap_or("");
        match parse_cid(wasm_hex) {
            Some(cid) => (cid.0, String::new(), None),
            None => return rpc_err(id, "provide 'name' or a 64-hex 'wasm_cid'".into()),
        }
    };
    let app_ns = p
        .get("app_ns")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or(default_ns);
    let caller = match parse_node_id(&state.node_id) {
        Some(n) => n.0,
        None => return rpc_err(id, "self node id unparseable".into()),
    };
    let input = p
        .get("input")
        .and_then(|v| v.as_str())
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_default();
    let ireq = zeph_com::InvokeRequest {
        app_ns,
        wasm_cid,
        func: func.to_string(),
        input,
    };
    match state.com.invoke(&ireq, caller, program_owner).await {
        Ok(out) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {
            "output": hex::encode(out)
        }}),
        Err(e) => rpc_err(id, format!("invoke failed: {e}")),
    }
}

async fn rpc_cid_op(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
    op: &str,
) -> serde_json::Value {
    let Some(cid) = param(req, "cid")
        .and_then(|v| v.as_str())
        .and_then(parse_cid)
    else {
        return rpc_err(id, format!("{op} needs a valid 'cid'"));
    };
    let result = match op {
        "pin" => state.engine.pin_chain(cid).await.map(|_| ()),
        "unpin" => state.engine.unpin_chain(cid).await.map(|_| ()),
        "want" => state.engine.want_chain(cid).await.map(|_| ()),
        "unwant" => state.engine.unwant_chain(cid).await.map(|_| ()),
        "fetch" => state
            .engine
            .get(cid, zeph_obj::ConsumeMode::Seed)
            .await
            .map(|_| ()),
        "delete" => soft_delete(state, cid).await,
        "ban" => state.engine.ban_chain(cid).await.map(|_| ()),
        "unban" => state.engine.unban_chain(cid).await.map(|_| ()),
        "delmeta" => state.engine.del_meta(cid).await,
        _ => unreachable!(),
    };
    match result {
        Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"ok": true}}),
        Err(e) => rpc_err(id, format!("{op} failed: {e}")),
    }
}

/// Set (edit) this node's metadata-envelope comment for a CID.
async fn rpc_setmeta(
    state: &State,
    req: &serde_json::Value,
    id: serde_json::Value,
) -> serde_json::Value {
    let Some(cid) = param(req, "cid")
        .and_then(|v| v.as_str())
        .and_then(parse_cid)
    else {
        return rpc_err(id, "setmeta needs a valid 'cid'".into());
    };
    let comment = param(req, "comment")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    match state.engine.set_meta(cid, comment).await {
        Ok(()) => serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"ok": true}}),
        Err(e) => rpc_err(id, format!("setmeta failed: {e}")),
    }
}

/// Client side of the Unix socket API (used by `zeph status`).
pub async fn query_unix(sock_path: &PathBuf, method: &str) -> anyhow::Result<serde_json::Value> {
    query_unix_params(sock_path, method, serde_json::json!({})).await
}

/// Client with params.
pub async fn query_unix_params(
    sock_path: &PathBuf,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    use anyhow::Context;
    let stream = tokio::net::UnixStream::connect(sock_path)
        .await
        .with_context(|| {
            format!(
                "connecting {} — is the daemon running?",
                sock_path.display()
            )
        })?;
    let (read, mut write) = stream.into_split();
    let request =
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    write.write_all(format!("{request}\n").as_bytes()).await?;
    let mut lines = BufReader::new(read).lines();
    let line = lines
        .next_line()
        .await?
        .context("daemon closed the connection without answering")?;
    let response: serde_json::Value = serde_json::from_str(&line)?;
    if let Some(err) = response.get("error") {
        anyhow::bail!("daemon error: {err}");
    }
    Ok(response
        .get("result")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

// ── Web dashboard (MU.2) ────────────────────────────────────────────────────

/// Dashboard HTML, embedded at compile time — no external assets, works
/// offline and over SSH tunnels.
const DASHBOARD_HTML: &str = include_str!("../../../webui/index.html");

/// Load or create the dashboard auth token (`<data_dir>/control.token`,
/// 0600). Persisted so an open dashboard survives daemon restarts.
/// PUBLIC network-stats endpoint (`GET /stats`) — token-free, CORS-open, safe-to-expose JSON for
/// the zeph.craft.ec website's live-network section. Replaces the retired tracker's
/// `--public-stats-port` (which served zeros once nodes stopped announcing to it): `nodes` comes
/// from the CONVERGED membership census (a real network-wide count), `provider_records` from this
/// node's DHT record store, and the content/storage figures are THIS node's local view (honest,
/// slightly understating the cluster — a network-wide aggregate is future work). Field names match
/// the tracker's schema exactly so the website needs no change. Binds 0.0.0.0 (public by design —
/// exposes only counts, no ids/addresses/content).
pub async fn serve_public_stats(
    state: Arc<State>,
    membership: Arc<zeph_membership::Membership>,
    dht: Arc<zeph_dht::DhtNode>,
    relays: usize,
    storage_quota_gib: f64,
    port: u16,
) -> anyhow::Result<()> {
    use axum::extract::State as AxumState;
    use axum::routing::get;

    #[derive(Clone)]
    struct StatsCtx {
        state: Arc<State>,
        membership: Arc<zeph_membership::Membership>,
        dht: Arc<zeph_dht::DhtNode>,
        relays: usize,
        capacity_bytes: u64,
    }

    async fn stats(AxumState(ctx): AxumState<StatsCtx>) -> impl axum::response::IntoResponse {
        let nodes = ctx.membership.census().await.len();
        let (cids, pieces, _pinned, bytes) = *ctx.state.storage.read().await;
        let capacity = ctx.capacity_bytes;
        let body = serde_json::json!({
            "nodes": nodes,
            "content_cids": cids,
            "provider_records": ctx.dht.stored_len(),
            "relays": ctx.relays,
            "pieces_tracked": pieces,
            "storage_used_bytes": bytes,
            "storage_capacity_bytes": capacity,
            "storage_available_bytes": capacity.saturating_sub(bytes),
        });
        (
            [
                (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                (axum::http::header::CACHE_CONTROL, "no-store"),
            ],
            axum::Json(body),
        )
    }

    let ctx = StatsCtx {
        state,
        membership,
        dht,
        relays,
        capacity_bytes: (storage_quota_gib * 1024.0 * 1024.0 * 1024.0) as u64,
    };
    let app = axum::Router::new()
        .route("/stats", get(stats))
        .with_state(ctx);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(stats = %format!("http://{addr}/stats"), "public stats listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn load_or_create_token(data_dir: &std::path::Path) -> anyhow::Result<String> {
    let path = data_dir.join("control.token");
    if path.exists() {
        return Ok(std::fs::read_to_string(&path)?.trim().to_string());
    }
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
    let token = hex::encode(bytes);
    std::fs::write(&path, &token)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(token)
}

#[derive(Clone)]
struct HttpCtx {
    state: Arc<State>,
    token: Arc<String>,
}

/// Serve the dashboard on 127.0.0.1 only. `GET /` returns the embedded page
/// with the token injected; `GET /api/status?token=…` returns live JSON.
/// A malicious website in the local browser cannot read either cross-origin
/// without the token.
pub async fn serve_http(state: Arc<State>, token: String, port: u16) -> anyhow::Result<()> {
    use axum::extract::{Query, State as AxumState};
    use axum::http::StatusCode;
    use axum::response::{Html, IntoResponse};
    use axum::routing::get;

    #[derive(serde::Deserialize)]
    struct TokenParam {
        #[serde(default)]
        token: String,
    }

    async fn index(AxumState(ctx): AxumState<HttpCtx>) -> Html<String> {
        Html(DASHBOARD_HTML.replace("__TOKEN__", &ctx.token))
    }

    async fn api_status(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        axum::Json(ctx.state.snapshot().await).into_response()
    }

    #[derive(serde::Deserialize)]
    struct Action {
        op: String,
        cid: String,
    }

    async fn api_action(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
        axum::Json(action): axum::Json<Action>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let Some(cid) = parse_cid(&action.cid) else {
            return (StatusCode::BAD_REQUEST, "bad cid").into_response();
        };
        let r = match action.op.as_str() {
            "pin" => ctx.state.engine.pin_chain(cid).await.map(|_| ()),
            "unpin" => ctx.state.engine.unpin_chain(cid).await.map(|_| ()),
            "want" => ctx.state.engine.want_chain(cid).await.map(|_| ()),
            "unwant" => ctx.state.engine.unwant_chain(cid).await.map(|_| ()),
            "fetch" => ctx
                .state
                .engine
                .get(cid, zeph_obj::ConsumeMode::Seed)
                .await
                .map(|_| ()),
            "delete" => soft_delete(&ctx.state, cid).await,
            "ban" => ctx.state.engine.ban_chain(cid).await.map(|_| ()),
            "unban" => ctx.state.engine.unban_chain(cid).await.map(|_| ()),
            other => Err(anyhow::anyhow!("unknown op {other}")),
        };
        match r {
            Ok(()) => axum::Json(serde_json::json!({"ok": true})).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }

    async fn api_files(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let Some(owner) = parse_node_id(&ctx.state.node_id) else {
            return axum::Json(serde_json::json!({"columns": [], "rows": []})).into_response();
        };
        axum::Json(drive_list(&ctx.state, owner).await).into_response()
    }

    // SSE: stream the event bus (foundation §52) to external subscribers. This is
    // the exposure step — the internal bus made reactive over the control API, so
    // apps react to node events without being wired into the kernel.
    async fn api_events(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
        use futures::StreamExt;
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let rx = ctx.state.events.subscribe();
        let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|r| {
            std::future::ready(match r {
                Ok(ev) => {
                    let data = serde_json::json!({
                        "tag": ev.tag(), "desc": ev.describe(), "cid": ev.cid_hex(),
                    })
                    .to_string();
                    Some(Ok::<_, std::convert::Infallible>(
                        SseEvent::default().event(ev.tag()).data(data),
                    ))
                }
                Err(_) => None, // lagged subscriber: skip, resume at newest
            })
        });
        Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response()
    }

    // CraftCOM: invoke a user-level app locally (caller = this node). Body:
    // {app, wasm (cid), func, input?}. Returns the agent's return value + fuel.
    #[derive(serde::Deserialize)]
    struct InvokeBody {
        app: String,
        wasm: String,
        #[serde(default = "default_func")]
        func: String,
        input: Option<String>,
    }
    fn default_func() -> String {
        "run".into()
    }
    async fn api_invoke(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
        axum::Json(body): axum::Json<InvokeBody>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let Some(cid) = parse_cid(&body.wasm) else {
            return (StatusCode::BAD_REQUEST, "bad wasm cid").into_response();
        };
        let Some(caller) = parse_node_id(&ctx.state.node_id) else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "self id").into_response();
        };
        let ireq = zeph_com::InvokeRequest {
            app_ns: body.app,
            wasm_cid: cid.0,
            func: body.func,
            input: body.input.map(|s| s.into_bytes()).unwrap_or_default(),
        };
        // Dashboard invoke is by raw cid → no registry-authenticated owner → attest UNAVAILABLE.
        match ctx.state.com.invoke(&ireq, caller.0, None).await {
            Ok(out) => axum::Json(serde_json::json!({
                "output": hex::encode(out)
            }))
            .into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }

    // Read a user-level app's OWN namespace (this node's `app.<app>` DB).
    #[derive(serde::Deserialize)]
    struct AppQuery {
        token: String,
        app: String,
        sql: String,
    }
    async fn api_app_query(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(q): Query<AppQuery>,
    ) -> axum::response::Response {
        if q.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let ns = format!("app.{}", q.app);
        match ctx.state.craftsql.open(&ns).await {
            Ok(db) => match db.query(&q.sql) {
                Ok(v) => axum::Json(v).into_response(),
                Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            },
            Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }

    async fn api_apps(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        axum::Json(apps_list(&ctx.state).await).into_response()
    }

    #[derive(serde::Deserialize)]
    struct SqlParams {
        #[serde(default)]
        token: String,
        ns: String,
        sql: String,
        #[serde(default)]
        owner: String,
    }

    // Read a query against one of YOUR databases (or another owner's, cross-owner read).
    async fn api_sql(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(q): Query<SqlParams>,
    ) -> axum::response::Response {
        if q.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let owner = if q.owner.trim().is_empty() {
            parse_node_id(&ctx.state.node_id)
        } else {
            parse_node_id(q.owner.trim())
        };
        let Some(owner) = owner else {
            return (StatusCode::BAD_REQUEST, "bad owner id").into_response();
        };
        let db = match ctx.state.craftsql.open_reader(owner, &q.ns).await {
            Ok(d) => d,
            Err(e) => return (StatusCode::BAD_REQUEST, format!("open: {e}")).into_response(),
        };
        let sql = q.sql.clone();
        match tokio::task::spawn_blocking(move || db.query(&sql)).await {
            Ok(Ok(v)) => axum::Json(v).into_response(),
            Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }

    #[derive(serde::Deserialize)]
    struct DeployBody {
        name: String,
        /// hex-encoded WASM bytes.
        wasm: String,
    }

    async fn api_deploy(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
        axum::Json(body): axum::Json<DeployBody>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let Ok(bytes) = hex::decode(body.wasm.trim()) else {
            return (StatusCode::BAD_REQUEST, "wasm must be hex").into_response();
        };
        if bytes.is_empty() {
            return (StatusCode::BAD_REQUEST, "empty wasm").into_response();
        }
        match deploy_bytes(&ctx.state, body.name.trim(), &bytes).await {
            Ok(v) => axum::Json(v).into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
        }
    }

    async fn api_pending(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let rows: Vec<serde_json::Value> = ctx
            .state
            .engine
            .pending_durability()
            .into_iter()
            .map(|(cid, have, floor)| {
                serde_json::json!({"cid": hex::encode(cid), "have": have, "floor": floor})
            })
            .collect();
        axum::Json(serde_json::json!({ "pending": rows })).into_response()
    }

    async fn api_cids(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        axum::Json(serde_json::json!({ "cids": ctx.state.cid_health.read().await.clone() }))
            .into_response()
    }

    async fn api_programs(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let rows: Vec<serde_json::Value> = ctx
            .state
            .gov
            .rows()
            .await
            .into_iter()
            .map(|(name, cid, ver)| serde_json::json!([name, cid, ver]))
            .collect();
        axum::Json(serde_json::json!({ "programs": rows })).into_response()
    }
    async fn api_committee(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let r = rpc_committee(&ctx.state, serde_json::Value::Null).await;
        axum::Json(r.get("result").cloned().unwrap_or_default()).into_response()
    }
    async fn api_config(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let r = rpc_config(&ctx.state, serde_json::Value::Null).await;
        axum::Json(r.get("result").cloned().unwrap_or_default()).into_response()
    }
    async fn api_attest_list(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let r = rpc_attest_list(&ctx.state, serde_json::Value::Null).await;
        axum::Json(r.get("result").cloned().unwrap_or_default()).into_response()
    }
    async fn api_board(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let r = rpc_board(&ctx.state, serde_json::Value::Null).await;
        axum::Json(r.get("result").cloned().unwrap_or_default()).into_response()
    }

    async fn api_governance(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let set = ctx.state.gov.current().await;
        let ig = ctx.state.gov.is_governor().await;
        axum::Json(gov_set_json(&set, ig)).into_response()
    }

    // Head-registry status: the open, owner-signed, sharded registry's live view from THIS
    // node — current writer-election epoch, eligible node count, how many of the live shards
    // this node currently writes, and the per-type head counts (program heads, DB roots,
    // manifests) across the shards it writes.
    async fn api_registry(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let st = ctx.state.registry.status().await;
        axum::Json(serde_json::json!({
            "epoch": st.epoch,
            "eligible": st.eligible,
            "writer_shards": st.writer_shards,
            "shard_count": st.shard_count,
            "program_heads": st.program_heads,
            "dbroots": st.dbroots,
            "manifests": st.manifests,
        }))
        .into_response()
    }

    // Browsable head-registry entries — the GLOBAL union of every member's local heads, grouped
    // by type (program heads / DB roots / manifests). Each shard is K-replicated across the
    // members, so the union of all members' local views is the complete registry. The gather is
    // concurrent, per-peer-failure-tolerant, and bounded (~3s) so a dead/slow member can't hang
    // the UI. Heavier than a local read — invoked on tab load, NOT per status poll.
    async fn api_registry_entries(
        AxumState(ctx): AxumState<HttpCtx>,
        Query(params): Query<TokenParam>,
    ) -> axum::response::Response {
        if params.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        let e = ctx.state.registry.entries_global().await;
        let (nprograms, ndbroots, nmanifests) =
            (e.programs.len(), e.dbroots.len(), e.manifests.len());
        let contributors = e.contributors;
        let rows = |v: Vec<crate::headreg::HeadRow>| -> Vec<serde_json::Value> {
            v.into_iter()
                .map(|r| {
                    serde_json::json!({
                        "owner": r.owner,
                        "name": r.name,
                        "cid": r.cid,
                        "version": r.version,
                    })
                })
                .collect()
        };
        axum::Json(serde_json::json!({
            "programs": rows(e.programs),
            "dbroots": rows(e.dbroots),
            "manifests": rows(e.manifests),
            // Global union counts (reflect the merged view) + how many nodes contributed.
            "program_heads": nprograms,
            "dbroot_count": ndbroots,
            "manifest_count": nmanifests,
            "contributors": contributors,
        }))
        .into_response()
    }

    let ctx = HttpCtx {
        state,
        token: Arc::new(token),
    };
    let app = axum::Router::new()
        .route("/", get(index))
        .route("/api/status", get(api_status))
        .route("/api/files", get(api_files))
        .route("/api/events", get(api_events))
        .route("/api/action", axum::routing::post(api_action))
        .route("/api/invoke", axum::routing::post(api_invoke))
        .route("/api/app_query", get(api_app_query))
        .route("/api/apps", get(api_apps))
        .route("/api/sql", get(api_sql))
        .route("/api/deploy", axum::routing::post(api_deploy))
        .route("/api/governance", get(api_governance))
        .route("/api/registry", get(api_registry))
        .route("/api/registry/entries", get(api_registry_entries))
        .route("/api/programs", get(api_programs))
        .route("/api/committee", get(api_committee))
        .route("/api/config", get(api_config))
        .route("/api/attest_list", get(api_attest_list))
        .route("/api/board", get(api_board))
        .route("/api/pending", get(api_pending))
        .route("/api/cids", get(api_cids))
        .with_state(ctx);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(dashboard = %format!("http://{addr}"), "dashboard listening");
    axum::serve(listener, app).await?;
    Ok(())
}
