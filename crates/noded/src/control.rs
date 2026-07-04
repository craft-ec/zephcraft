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
    pub trackers: Vec<String>,
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
    pub store_cids: u64,
    pub store_pieces: u64,
    pub store_pinned: u64,
    pub store_bytes: u64,
    pub providing: u64,
    pub trackers: String,
    pub content: Vec<ContentInfo>,
    /// HealthScan: last-pass scanned + at-risk CIDs, cumulative pieces repaired.
    pub health_scanned: usize,
    pub health_at_risk: usize,
    pub health_repaired: u64,
    pub health_distributed: u64,
    pub health_scaled: u64,
    pub health_degraded: u64,
    pub health_fading: u64,
    pub peers: Vec<PeerStatus>,
    /// Recent node events (activity feed), newest first.
    pub recent_events: Vec<String>,
    /// Background job coordinator activity (foundation §51).
    pub jobs: zeph_sched::JobStats,
    /// Event-bus activity (foundation §52) — totals + per-type + subscribers.
    pub event_stats: EventStats,
    /// Most recent finished jobs (newest first).
    pub recent_jobs: Vec<zeph_sched::JobRecord>,
    /// Node configuration (read-only Settings view).
    pub settings: NodeSettings,
}

pub struct State {
    pub clock: std::sync::Arc<zeph_core::hlc::Clock>,
    pub node_id: String,
    pub reach: String,
    pub relays: String,
    pub trackers: String,
    pub listen: String,
    pub started: Instant,
    pub engine: Arc<zeph_obj::ObjEngine>,
    pub peers: RwLock<Vec<PeerStatus>>,
    pub passive_peers: std::sync::atomic::AtomicU32,
    pub storage: RwLock<(u64, u64, u64, u64)>, // (cids, pieces, pinned, bytes)
    pub providing: std::sync::atomic::AtomicU64,
    pub content: RwLock<Vec<ContentInfo>>,
    pub health: RwLock<(usize, usize, u64, u64, u64, u64, u64)>, // scanned, at_risk, repaired, moved, scaled, degraded, fading
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
    /// Node configuration snapshot (Settings view).
    pub settings: NodeSettings,
}

impl State {
    pub async fn snapshot(&self) -> Status {
        let hlc = self.clock.now();
        Status {
            hlc_ms: hlc.millis(),
            hlc_logical: hlc.logical(),
            node_id: self.node_id.clone(),
            reach: self.reach.clone(),
            relays: self.relays.clone(),
            listen: self.listen.clone(),
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
            store_cids: self.storage.read().await.0,
            store_pieces: self.storage.read().await.1,
            store_pinned: self.storage.read().await.2,
            store_bytes: self.storage.read().await.3,
            providing: self.providing.load(std::sync::atomic::Ordering::Relaxed),
            trackers: self.trackers.clone(),
            content: self.content.read().await.clone(),
            health_scanned: self.health.read().await.0,
            health_at_risk: self.health.read().await.1,
            health_repaired: self.health.read().await.2,
            health_distributed: self.health.read().await.3,
            health_scaled: self.health.read().await.4,
            health_degraded: self.health.read().await.5,
            health_fading: self.health.read().await.6,
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
            settings: self.settings.clone(),
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

    #[allow(clippy::too_many_arguments)]
    pub async fn set_health(
        &self,
        scanned: usize,
        at_risk: usize,
        repaired_delta: u64,
        moved_delta: u64,
        scaled_delta: u64,
        degraded_delta: u64,
        fading: usize,
    ) {
        let mut h = self.health.write().await;
        h.0 = scanned;
        h.1 = at_risk;
        h.2 += repaired_delta;
        h.3 += moved_delta;
        h.4 += scaled_delta;
        h.5 += degraded_delta;
        h.6 = fading as u64;
    }

    /// Replace the peer table wholesale (fed by the membership layer). Emits
    /// PeerConnected/PeerDisconnected events for the delta in the alive set.
    pub async fn set_peers(&self, peers: Vec<PeerStatus>, passive: u32) {
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
        Some("unban") => rpc_cid_op(state, &request, id, "unban").await,
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
            zeph_obj::Manifest::File { content, .. } => {
                let bytes = engine
                    .get(zeph_core::Cid(content), zeph_obj::ConsumeMode::Seed)
                    .await?;
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
    // For a private file, forget the ciphertext it points at too.
    if let Ok(bytes) = state.engine.get(cid, zeph_obj::ConsumeMode::Drop).await {
        if let Some(env) = zeph_obj::EncryptedEnvelope::decode(&bytes) {
            let _ = state
                .engine
                .forget_local(zeph_core::Cid(env.ciphertext_cid))
                .await;
        }
    }
    let _ = state.engine.forget_local(cid).await;
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
        "pin" => state.engine.pin(cid).await,
        "unpin" => state.engine.unpin(cid).await,
        "want" => state.engine.want(cid).await,
        "unwant" => state.engine.unwant(cid).await,
        "fetch" => state
            .engine
            .get(cid, zeph_obj::ConsumeMode::Seed)
            .await
            .map(|_| ()),
        "delete" => soft_delete(state, cid).await,
        "ban" => state.engine.delete_local(cid).await,
        "unban" => state.engine.undelete(cid).await,
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
            "pin" => ctx.state.engine.pin(cid).await,
            "unpin" => ctx.state.engine.unpin(cid).await,
            "want" => ctx.state.engine.want(cid).await,
            "unwant" => ctx.state.engine.unwant(cid).await,
            "fetch" => ctx
                .state
                .engine
                .get(cid, zeph_obj::ConsumeMode::Seed)
                .await
                .map(|_| ()),
            "delete" => soft_delete(&ctx.state, cid).await,
            "ban" => ctx.state.engine.delete_local(cid).await,
            "unban" => ctx.state.engine.undelete(cid).await,
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
        .with_state(ctx);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(dashboard = %format!("http://{addr}"), "dashboard listening");
    axum::serve(listener, app).await?;
    Ok(())
}
