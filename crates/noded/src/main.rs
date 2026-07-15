//! `zeph` — the ZephCraft node daemon (headless; one implementation for all
//! platforms). M1.3 worker skeleton + MU.1 control API.
//!
//! Boot order (foundation §12, skeleton subset): identity → transport →
//! control servers → serve loop → heartbeat.

// jemalloc as the global allocator (see Cargo.toml): glibc malloc's per-thread
// arenas retain freed memory under the seed's bursty serve+mint churn, bloating
// RSS to multi-GB (measured 8GB on a seed vs <1GB on peers) — a glibc-arena
// artifact, not a leak. jemalloc returns memory to the OS aggressively. Gated to
// non-MSVC so the fix ships on Linux (fleet) and macOS (Mac node) alike.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc runtime config, read at allocator init (before main) via the symbol
// jemalloc looks up. DEFAULT jemalloc does NOT run a purge thread and decays
// dirty pages lazily (10s), so under the seed's bursty serve+mint churn RSS
// still climbs to multi-GB (measured 4.4GB in 6min). A dedicated background
// purge thread + short decay returns freed pages to the OS promptly — RSS then
// holds flat at ~550MB (measured, matching the true working set). Baked into the
// binary so it applies on every node with no per-host env config. The &[u8]
// begins with the data pointer jemalloc reads as `const char *`; NUL-terminated.
// `background_thread` is Linux/pthread-only (jemalloc warns + ignores it on
// macOS), so the fleet (Linux) gets the purge thread while the low-churn Mac
// node gets decay-only — same result there, no startup warning.
#[cfg(all(not(target_env = "msvc"), target_os = "linux"))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0\0";

#[cfg(all(not(target_env = "msvc"), not(target_os = "linux")))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:0\0";

mod account;
mod attest;
mod board;
mod control;
mod governance;
mod headreg;
mod peers;
mod registry_heads;
mod registry_net;
mod shard_root;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use zeph_core::Cid;
use zeph_crypto::Keystore;
use zeph_membership::Membership;
use zeph_obj::{ObjConfig, ObjEngine, PeerSource};
use zeph_store::Store;

/// Delay-queue of CIDs due for a health check, ordered by next-due time (min-heap via Reverse).
type DueQueue = std::sync::Mutex<
    std::collections::BinaryHeap<std::cmp::Reverse<(std::time::Instant, Cid, std::time::Duration)>>,
>;
use zeph_transport::{PeerAddr, Reach, Transport};

#[derive(Parser)]
#[command(
    name = "zeph",
    version,
    about = "ZephCraft node — decentralized storage network (Craftec)"
)]
struct Cli {
    /// Data directory (keys, config.toml, zeph.sock). Default: ~/.zeph
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    run: RunArgs,
}

#[derive(Subcommand)]
enum Command {
    /// Query the running daemon's live peer table over the control socket.
    Status,
    /// Publish a file: encode, store+pin, spread across the network (via the
    /// running daemon). Pins by default.
    Publish {
        /// File to store.
        file: PathBuf,
        /// Do not pin (uploader pins by default).
        #[arg(long)]
        no_pin: bool,
        /// Encrypt: publish a PRIVATE object (only you can read it). Files only.
        #[arg(long)]
        private: bool,
    },
    /// Fetch content by CID alone — the daemon resolves providers via the
    /// tracker (no peer address needed).
    Get {
        /// Content id (64 hex chars) printed by `zeph publish`.
        cid: String,
        /// Output path.
        #[arg(short, long)]
        output: PathBuf,
        /// Partial/range read: byte offset to start at (streaming/seek). Requires --length.
        /// Only the covering 8 MiB segments are fetched — cheap for large files.
        #[arg(long)]
        offset: Option<u64>,
        /// Partial/range read: number of bytes to fetch from --offset.
        #[arg(long)]
        length: Option<u64>,
    },
    /// Pin a CID (hold whole content, exempt from eviction).
    Pin { cid: String },
    /// Unpin a CID (revert to normal, evictable lifecycle).
    Unpin { cid: String },
    /// Want a CID: keep-alive intent (holds the whole file alive; cascades).
    Want { cid: String },
    /// Unwant a CID (drop the keep-alive intent; cascades).
    Unwant { cid: String },
    /// Remove a file from your drive: unlist it + unpin so it fades from the
    /// network (nothing wants it). Re-publishable. For a private file, also unpins
    /// the ciphertext (best-effort crypto-shred). Your copies go; network copies
    /// fade — not a guarantee for published objects.
    Delete { cid: String },
    /// Ban a CID on THIS node: tombstone it — refuse to host + block resurrection.
    /// For moderating content off your node; sticky until unbanned.
    Ban { cid: String },
    /// Invoke a CraftCOM app on this node: run WASM (published, by CID) against your
    /// `app.<ns>` namespace. Prints the agent's return value.
    Invoke {
        /// Invoke by NAME (resolves the signed head): `<name>` (own) or
        /// `<publisher_hex>/<name>`. Alternative to --wasm.
        #[arg(long)]
        name: Option<String>,
        /// The app's data namespace (defaults to the app name).
        #[arg(long)]
        app: Option<String>,
        /// CID of the published .wasm app (alternative to --name).
        #[arg(long)]
        wasm: Option<String>,
        #[arg(long, default_value = "run")]
        func: String,
        /// Optional input passed to the agent (UTF-8 bytes, via the `input` host fn).
        #[arg(long)]
        input: Option<String>,
    },
    /// Resolve a published app NAME to its current cid — WITHOUT fetching content.
    /// Tolerant of a briefly-unreachable writer (tries the shard's replicas in turn).
    Resolve {
        /// `<publisher_hex>/<app>` — the owner's node id (64 hex) and the app name.
        #[arg(long)]
        name: String,
    },
    /// Deploy a CraftCOM app: publish the WASM as a SYSTEM object (durable, managed
    /// like a database — NOT a drive file) and register it by name.
    Deploy {
        /// The .wasm (or .wat) app file.
        file: PathBuf,
        /// App name (defaults to the file stem). Re-deploying updates it.
        #[arg(long)]
        name: Option<String>,
    },
    /// List your deployed CraftCOM apps.
    Apps,
    /// Publish a WASM program to the grid (returns its content cid).
    PublishProgram { file: String },
    /// Advance a generic program account: run <program> on its state (the program is the writer).
    ProgramAdvance {
        #[arg(long)]
        program: String,
        #[arg(long)]
        seed: String,
        #[arg(long, default_value = "")]
        request: String,
    },
    /// Read a generic program account's state.
    ProgramResolve {
        #[arg(long)]
        program: String,
        #[arg(long)]
        seed: String,
    },
    /// List network-owned programs and their canonical cids.
    Programs,
    /// Show the governance set (governors, threshold, seq).
    Gov,
    /// Propose a governance change (signs with this node's key; applies at 1-of-1).
    GovPropose {
        #[arg(long)]
        add: Option<String>,
        #[arg(long)]
        remove: Option<String>,
        #[arg(long)]
        threshold: Option<u64>,
        /// name=cid : set a network-owned program's canonical wasm cid.
        #[arg(long)]
        set_program: Option<String>,
        /// key=value : set a protocol config value (integer), e.g. `shard_bits=9`.
        #[arg(long)]
        set_config: Option<String>,
    },
    /// Bootstrap a program's attestation quorum (member pubkeys + k-of-n).
    AttestBootstrap {
        #[arg(long)]
        program: String,
        /// Comma-separated member node-id hexes.
        #[arg(long)]
        members: String,
        #[arg(long)]
        threshold: Option<u64>,
    },
    /// Propose a statement for a program's quorum to authorize (prints the attestation hex to
    /// pass to each member for `attest-cosign`).
    AttestPropose {
        #[arg(long)]
        program: String,
        #[arg(long)]
        statement: String,
    },
    /// Add THIS node's signature to an in-flight attestation hex (a quorum member cosigning).
    AttestCosign {
        #[arg(long)]
        attestation: String,
    },
    /// Submit a collected k-of-n attestation hex to a program's quorum chain.
    AttestSubmit {
        #[arg(long)]
        program: String,
        #[arg(long)]
        attestation: String,
    },
    /// Check whether a program's quorum has authorized a statement. Defaults to THIS node's own
    /// quorum for the program; pass `--owner <hex>` to check another owner's.
    AttestStatus {
        #[arg(long)]
        program: String,
        #[arg(long)]
        statement: String,
        /// Owner node-id hex whose quorum to check (default: this node).
        #[arg(long)]
        owner: Option<String>,
    },
    /// Execute write SQL against your own CraftSQL database `ns`
    /// (commits + publishes the KIND_ROOT head).
    SqlExec {
        #[arg(long)]
        ns: String,
        #[arg(long)]
        sql: String,
    },
    /// Query a CraftSQL database — yours, or another owner's via --owner <hex>.
    SqlQuery {
        #[arg(long)]
        ns: String,
        #[arg(long)]
        sql: String,
        #[arg(long)]
        owner: Option<String>,
    },
    /// Rebuild a CraftSQL database from its durable generations, discovered via
    /// the network manifest — resurrects a DB from (owner, namespace) alone.
    SqlRecover {
        #[arg(long)]
        ns: String,
        #[arg(long)]
        owner: Option<String>,
    },
    /// Compact a CraftSQL database you own: fold accumulated generations into one
    /// base snapshot + reclaim superseded page objects (bounds storage growth).
    SqlCompact {
        #[arg(long)]
        ns: String,
    },
    /// List your drive — everything you've published (or another owner's via
    /// --owner <hex>), from the per-identity CraftSQL index.
    Files {
        #[arg(long)]
        owner: Option<String>,
    },
}

#[derive(clap::Args)]
struct RunArgs {
    /// Peer to heartbeat: <node_id_hex>@<ip:port>[,<ip:port>...]
    /// Repeatable; adds to peers from config.toml.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Reachability: "local" (direct sockets only) or "relayed" (iroh relays
    /// + discovery; use for WAN). Overrides config.toml.
    #[arg(long, global = true)]
    reach: Option<String>,

    /// Heartbeat interval in seconds. Overrides config.toml.
    #[arg(long)]
    heartbeat_secs: Option<u64>,

    /// Fixed UDP listen port (0 = OS-assigned). Overrides config.toml.
    /// Servers behind a firewall should fix this and allow it (udp).
    #[arg(long)]
    listen_port: Option<u16>,

    /// Dashboard port on 127.0.0.1 (0 disables). Overrides config.toml.
    #[arg(long)]
    dashboard_port: Option<u16>,

    /// PUBLIC network-stats port on 0.0.0.0 (0 disables; token-free JSON at /stats for the
    /// website's live-network section). Overrides config.toml.
    #[arg(long)]
    public_stats_port: Option<u16>,

    /// Relay URL (repeatable). REPLACES config.toml relay_urls when given.
    #[arg(long = "relay-url", global = true)]
    relay_urls: Vec<String>,

    /// Do not append n0's public relays as fallback (our mesh only).
    #[arg(long, global = true)]
    no_fallback_relays: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(default)]
struct Config {
    /// "local" | "relayed"
    reach: String,
    heartbeat_secs: u64,
    /// Fixed UDP listen port; 0 = OS-assigned.
    listen_port: u16,
    /// Web dashboard port on 127.0.0.1; 0 disables. Remote access: ssh -L.
    dashboard_port: u16,
    /// PUBLIC network-stats port on 0.0.0.0; 0 (default) disables. Token-free `GET /stats`
    /// JSON (counts only) for the website's live-network section — the retired tracker's
    /// `--public-stats-port` replacement.
    public_stats_port: u16,
    /// Relay mesh (foundation §26): our relays first; n0 appended as
    /// fallback unless fallback_relays = false.
    relay_urls: Vec<String>,
    fallback_relays: bool,
    /// Relays this node OPERATES and vouches for (foundation §26).
    /// Empty = not a relay op.
    relay_operator_urls: Vec<String>,
    /// Storage this node offers to the network, in GiB.
    storage_quota_gib: f64,
    /// Bootstrap peers: <node_id_hex>@<ip:port>[,...]
    peers: Vec<String>,
    /// Erasure generation size k (decode threshold). Default 8.
    erasure_k: usize,
    /// Distinct-peer threshold for `durable`. Default 8.
    durability_threshold: usize,
    /// HealthScan availability-probe timeout (seconds). Default 2.
    probe_timeout_secs: u64,
    /// Scaling: pulls/cycle above which a hot CID recruits a provider. Default 20.
    scale_threshold: u32,
    /// Degradation: pulls/cycle below which a surplus CID sheds to floor. Default 5.
    degrade_threshold: u32,
    /// Fade grace: content fetched within this stays demand-alive (seconds). Default 1 day.
    fade_grace_secs: u64,
    /// Health-scan/re-announce pacing delay between chunks of cids (seconds). Default 1.
    pace_delay_secs: u64,
    /// Eviction cooldown: an evicted CID is not refilled for this (seconds). Default 30 days.
    eviction_cooldown_secs: u64,
    /// Health-scan / lifecycle loop interval (seconds). Default 30.
    health_scan_secs: u64,
    /// Provider re-announce interval (seconds). Default 120.
    reannounce_secs: u64,
    /// Governance genesis: governor node-id hexes. Empty = seed 1-of-1 with this node.
    #[serde(default)]
    governance_governors: Vec<String>,
    /// Governance genesis threshold (k). Default 1.
    #[serde(default = "one")]
    governance_threshold: usize,
    /// Serve INBOUND DHT traffic (adds the DHT ALPN to the accept list). The DHT is
    /// unconditionally the sole content router regardless; this only gates whether this node
    /// answers other nodes' DHT queries. Default false (client-only).
    routing_dht: bool,
    /// DHT bootstrap seed peer addresses (also seed membership bootstrap/recovery). Default empty.
    dht_seeds: Vec<String>,
}

fn one() -> usize {
    1
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reach: "local".to_string(),
            heartbeat_secs: 5,
            listen_port: 0,
            dashboard_port: 9945,
            public_stats_port: 0,
            relay_urls: vec!["https://relay1.zeph.craft.ec".to_string()],
            fallback_relays: true,
            relay_operator_urls: Vec::new(),
            storage_quota_gib: 10.0,
            peers: Vec::new(),
            erasure_k: 8,
            durability_threshold: 8,
            probe_timeout_secs: 2,
            scale_threshold: 20,
            degrade_threshold: 5,
            fade_grace_secs: 24 * 60 * 60,
            pace_delay_secs: 1,
            eviction_cooldown_secs: 30 * 24 * 60 * 60,
            health_scan_secs: 30,
            reannounce_secs: 120,
            governance_governors: Vec::new(),
            governance_threshold: 1,
            routing_dht: false,
            dht_seeds: Vec::new(),
        }
    }
}

/// Load `<data_dir>/config.toml`, writing the defaults on first run.
fn load_or_init_config(data_dir: &Path) -> anyhow::Result<Config> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;
    let path = data_dir.join("config.toml");
    if path.exists() {
        let raw = std::fs::read_to_string(&path)?;
        toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
    } else {
        let cfg = Config::default();
        std::fs::write(&path, toml::to_string_pretty(&cfg)?)?;
        Ok(cfg)
    }
}

fn resolve_data_dir(cli_dir: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    match cli_dir {
        Some(dir) => Ok(dir),
        None => Ok(dirs::home_dir()
            .context("no home directory; pass --data-dir")?
            .join(".zeph")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Start jemalloc's background purge thread (Linux). Setting background_thread
    // via the malloc_conf symbol does NOT reliably start the thread at early init
    // (measured: symbol-only RSS still climbed to 3GB); enabling it at runtime
    // here does. Combined with the baked short decay times (_rjem_malloc_conf),
    // freed pages are returned to the OS promptly and the seed's RSS holds
    // ~550MB instead of ballooning to 8GB. Best-effort: ignore if unsupported.
    #[cfg(all(not(target_env = "msvc"), target_os = "linux"))]
    let _ = tikv_jemalloc_ctl::background_thread::write(true);

    let cli = Cli::parse();
    let data_dir = resolve_data_dir(cli.data_dir)?;

    match cli.command {
        Some(Command::Status) => cmd_status(&data_dir).await,
        Some(Command::Publish {
            file,
            no_pin,
            private,
        }) => cmd_publish(&data_dir, &file, !no_pin, private).await,
        Some(Command::Get {
            cid,
            output,
            offset,
            length,
        }) => cmd_get(&data_dir, &cid, &output, offset, length).await,
        Some(Command::Pin { cid }) => cmd_cid_op(&data_dir, "pin", &cid).await,
        Some(Command::Unpin { cid }) => cmd_cid_op(&data_dir, "unpin", &cid).await,
        Some(Command::Want { cid }) => cmd_cid_op(&data_dir, "want", &cid).await,
        Some(Command::Unwant { cid }) => cmd_cid_op(&data_dir, "unwant", &cid).await,
        Some(Command::Delete { cid }) => cmd_cid_op(&data_dir, "delete", &cid).await,
        Some(Command::Ban { cid }) => cmd_cid_op(&data_dir, "ban", &cid).await,
        Some(Command::Invoke {
            name,
            app,
            wasm,
            func,
            input,
        }) => {
            cmd_invoke(
                &data_dir,
                name.as_deref(),
                app.as_deref(),
                wasm.as_deref(),
                &func,
                input.as_deref(),
            )
            .await
        }
        Some(Command::Resolve { name }) => cmd_resolve(&data_dir, &name).await,
        Some(Command::Deploy { file, name }) => cmd_deploy(&data_dir, &file, name.as_deref()).await,
        Some(Command::Apps) => cmd_apps(&data_dir).await,
        Some(Command::PublishProgram { file }) => cmd_publish_program(&data_dir, &file).await,
        Some(Command::ProgramAdvance {
            program,
            seed,
            request,
        }) => cmd_program_advance(&data_dir, &program, &seed, &request).await,
        Some(Command::ProgramResolve { program, seed }) => {
            cmd_program_resolve(&data_dir, &program, &seed).await
        }
        Some(Command::Programs) => cmd_programs(&data_dir).await,
        Some(Command::Gov) => cmd_gov(&data_dir).await,
        Some(Command::GovPropose {
            add,
            remove,
            threshold,
            set_program,
            set_config,
        }) => cmd_gov_propose(&data_dir, add, remove, threshold, set_program, set_config).await,
        Some(Command::AttestBootstrap {
            program,
            members,
            threshold,
        }) => {
            cmd_attest(
                &data_dir,
                "attest_bootstrap",
                serde_json::json!({"program": program, "members": members, "threshold": threshold}),
            )
            .await
        }
        Some(Command::AttestPropose { program, statement }) => {
            cmd_attest(
                &data_dir,
                "attest_propose",
                serde_json::json!({"program": program, "statement": statement}),
            )
            .await
        }
        Some(Command::AttestCosign { attestation }) => {
            cmd_attest(
                &data_dir,
                "attest_cosign",
                serde_json::json!({"attestation": attestation}),
            )
            .await
        }
        Some(Command::AttestSubmit {
            program,
            attestation,
        }) => {
            cmd_attest(
                &data_dir,
                "attest_submit",
                serde_json::json!({"program": program, "attestation": attestation}),
            )
            .await
        }
        Some(Command::AttestStatus {
            program,
            statement,
            owner,
        }) => {
            cmd_attest(
                &data_dir,
                "attest_status",
                serde_json::json!({"program": program, "statement": statement, "owner": owner}),
            )
            .await
        }
        Some(Command::SqlExec { ns, sql }) => cmd_sql_exec(&data_dir, &ns, &sql).await,
        Some(Command::SqlQuery { ns, sql, owner }) => {
            cmd_sql_query(&data_dir, owner.as_deref(), &ns, &sql).await
        }
        Some(Command::SqlRecover { ns, owner }) => {
            cmd_sql_recover(&data_dir, owner.as_deref(), &ns).await
        }
        Some(Command::SqlCompact { ns }) => cmd_sql_compact(&data_dir, &ns).await,
        Some(Command::Files { owner }) => cmd_files(&data_dir, owner.as_deref()).await,
        None => cmd_run(&data_dir, cli.run).await,
    }
}

/// Generic attestation control-plane command: send `method` + `params` over the control socket and
/// pretty-print the result (the attest-* CLI verbs all share this shape).
async fn cmd_attest(
    data_dir: &Path,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<()> {
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), method, params).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&r).unwrap_or_else(|_| r.to_string())
    );
    Ok(())
}

async fn cmd_publish(data_dir: &Path, file: &Path, pin: bool, private: bool) -> anyhow::Result<()> {
    let abs =
        std::fs::canonicalize(file).with_context(|| format!("resolving {}", file.display()))?;
    let params = serde_json::json!({"path": abs.to_string_lossy(), "pin": pin, "private": private});
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "publish", params).await?;
    let cid = r.get("cid").and_then(|v| v.as_str()).unwrap_or("?");
    let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("item");
    let size = r.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
    let is_private = r.get("private").and_then(|v| v.as_bool()).unwrap_or(false);
    if r.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!("published folder {name}/ ({size} bytes total)");
    } else {
        println!(
            "published {}{name} ({size} bytes · {}) — durable={}, pinned={}",
            if is_private { "🔒 private " } else { "" },
            r.get("mime")
                .and_then(|v| v.as_str())
                .unwrap_or("application/octet-stream"),
            r.get("durable").and_then(|v| v.as_bool()).unwrap_or(false),
            r.get("pinned").and_then(|v| v.as_bool()).unwrap_or(false),
        );
    }
    if is_private {
        println!("cid {cid}   (PRIVATE — only your node can `zeph get {cid} -o <path>`)");
    } else {
        println!("cid {cid}   (share this — `zeph get {cid} -o <path>` restores it by name)");
    }
    println!("ZEPH_CID {cid}");
    Ok(())
}

async fn cmd_get(
    data_dir: &Path,
    cid: &str,
    output: &Path,
    offset: Option<u64>,
    length: Option<u64>,
) -> anyhow::Result<()> {
    let abs = std::path::absolute(output).unwrap_or_else(|_| output.to_path_buf());
    let mut params = serde_json::json!({"cid": cid, "output": abs.to_string_lossy()});
    if let (Some(o), Some(l)) = (offset, length) {
        params["offset"] = serde_json::json!(o);
        params["length"] = serde_json::json!(l);
    }
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "get", params).await?;
    let path = r
        .get("path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| output.display().to_string());
    if r.get("range").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!(
            "fetched range [{}..+{}] → {path} ({} bytes, only covering segments)",
            offset.unwrap_or(0),
            length.unwrap_or(0),
            r.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    } else if r.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!(
            "restored folder → {path} ({} files, cids verified)",
            r.get("files").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    } else if r.get("files").is_some() {
        println!(
            "restored {} → {path} (providers resolved via the DHT, cid verified)",
            r.get("name").and_then(|v| v.as_str()).unwrap_or("file"),
        );
    } else if r.get("private").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!(
            "🔓 decrypted {} → {path} ({} bytes · private)",
            r.get("name").and_then(|v| v.as_str()).unwrap_or("file"),
            r.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    } else {
        println!(
            "fetched {} bytes → {path} (raw content, cid verified)",
            r.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    }
    Ok(())
}

async fn cmd_cid_op(data_dir: &Path, op: &str, cid: &str) -> anyhow::Result<()> {
    let params = serde_json::json!({"cid": cid});
    control::query_unix_params(&data_dir.join("zeph.sock"), op, params).await?;
    println!("{op} {cid} ok");
    Ok(())
}

async fn cmd_invoke(
    data_dir: &Path,
    name: Option<&str>,
    app: Option<&str>,
    wasm: Option<&str>,
    func: &str,
    input: Option<&str>,
) -> anyhow::Result<()> {
    if name.is_none() && wasm.is_none() {
        anyhow::bail!("provide --name <app> or --wasm <cid>");
    }
    let params = serde_json::json!({
        "name": name, "app_ns": app, "wasm_cid": wasm, "func": func, "input": input
    });
    let res = control::query_unix_params(&data_dir.join("zeph.sock"), "invoke", params).await?;
    let output = res.get("output").and_then(|v| v.as_str()).unwrap_or("");
    let label = name.or(app).unwrap_or("app");
    if output.is_empty() {
        println!("app '{label}' committed (empty)");
    } else {
        println!("app '{label}' committed {output}");
    }
    Ok(())
}

/// `zeph resolve --name <ownerhex>/<app>` — print the app's current cid (or `not found`)
/// WITHOUT fetching content. Splits on the FIRST '/', mirroring `invoke --name <pub>/<app>`.
async fn cmd_resolve(data_dir: &Path, name: &str) -> anyhow::Result<()> {
    let (owner, app) = match name.split_once('/') {
        Some((ph, n)) => (ph, n),
        None => anyhow::bail!("name: bad publisher (expected <hex>/<app>)"),
    };
    let params = serde_json::json!({"owner": owner, "name": app});
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "resolve_name", params).await?;
    match r.get("cid").and_then(|v| v.as_str()) {
        Some(cid) => println!("{cid}"),
        None => println!("not found"),
    }
    Ok(())
}

async fn cmd_deploy(data_dir: &Path, file: &Path, name: Option<&str>) -> anyhow::Result<()> {
    let abs =
        std::fs::canonicalize(file).with_context(|| format!("resolving {}", file.display()))?;
    let params = serde_json::json!({"path": abs.to_string_lossy(), "name": name});
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "deploy", params).await?;
    let n = r.get("name").and_then(|v| v.as_str()).unwrap_or("app");
    let cid = r.get("cid").and_then(|v| v.as_str()).unwrap_or("?");
    let size = r.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
    let ver = r.get("version").and_then(|v| v.as_u64()).unwrap_or(1);
    println!("deployed app '{n}' v{ver} ({size} bytes, system object) - cid {cid}");
    println!("invoke by name: zeph invoke --name {n}");
    Ok(())
}

async fn cmd_publish_program(data_dir: &Path, file: &str) -> anyhow::Result<()> {
    let bytes = std::fs::read(file)?;
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let params = serde_json::json!({ "wasm": hex });
    let res =
        control::query_unix_params(&data_dir.join("zeph.sock"), "publish_program", params).await?;
    let cid = res.get("cid").and_then(|v| v.as_str()).unwrap_or("");
    println!("published {} ({} bytes)", file, bytes.len());
    println!("cid: {cid}");
    println!("activate: zeph gov-propose --set-program <name>={cid}");
    Ok(())
}

async fn cmd_program_advance(
    data_dir: &Path,
    program: &str,
    seed: &str,
    request: &str,
) -> anyhow::Result<()> {
    let params = serde_json::json!({"program": program, "seed": seed, "request": request});
    let res =
        control::query_unix_params(&data_dir.join("zeph.sock"), "program_advance", params).await?;
    println!(
        "account {}",
        res.get("account").and_then(|v| v.as_str()).unwrap_or("")
    );
    println!(
        "root {}",
        res.get("root").and_then(|v| v.as_str()).unwrap_or("")
    );
    Ok(())
}

async fn cmd_program_resolve(data_dir: &Path, program: &str, seed: &str) -> anyhow::Result<()> {
    let params = serde_json::json!({"program": program, "seed": seed});
    let res =
        control::query_unix_params(&data_dir.join("zeph.sock"), "program_resolve", params).await?;
    println!(
        "account {}",
        res.get("account").and_then(|v| v.as_str()).unwrap_or("")
    );
    println!(
        "state ({} bytes): {}",
        res.get("size").and_then(|v| v.as_u64()).unwrap_or(0),
        res.get("state").and_then(|v| v.as_str()).unwrap_or("")
    );
    Ok(())
}

async fn cmd_programs(data_dir: &Path) -> anyhow::Result<()> {
    let res = control::query_unix(&data_dir.join("zeph.sock"), "programs").await?;
    let rows = res
        .get("programs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    println!("{:<16} {:<5} CANONICAL CID", "PROGRAM", "VER");
    for r in rows {
        let a = r.as_array().cloned().unwrap_or_default();
        let name = a.first().and_then(|v| v.as_str()).unwrap_or("");
        let cid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
        let ver = a.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
        println!("{:<16} v{:<4} {}", name, ver, &cid[..cid.len().min(24)]);
    }
    Ok(())
}

async fn cmd_gov(data_dir: &Path) -> anyhow::Result<()> {
    let res = control::query_unix(&data_dir.join("zeph.sock"), "gov").await?;
    let govs = res
        .get("governors")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    println!(
        "governance: {}-of-{}  seq {}  (you are {}a governor)",
        res.get("threshold").and_then(|v| v.as_u64()).unwrap_or(0),
        govs.len(),
        res.get("seq").and_then(|v| v.as_u64()).unwrap_or(0),
        if res
            .get("is_governor")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            ""
        } else {
            "NOT "
        },
    );
    for g in govs {
        if let Some(h) = g.as_str() {
            println!("  {}", h);
        }
    }
    Ok(())
}

async fn cmd_gov_propose(
    data_dir: &Path,
    add: Option<String>,
    remove: Option<String>,
    threshold: Option<u64>,
    set_program: Option<String>,
    set_config: Option<String>,
) -> anyhow::Result<()> {
    let params = if let Some(h) = add {
        serde_json::json!({"action": "add", "governor": h})
    } else if let Some(h) = remove {
        serde_json::json!({"action": "remove", "governor": h})
    } else if let Some(t) = threshold {
        serde_json::json!({"action": "threshold", "value": t})
    } else if let Some(np) = set_program {
        let (name, cid) = np
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--set-program name=cid"))?;
        serde_json::json!({"action": "set_program", "name": name, "cid": cid})
    } else if let Some(kv) = set_config {
        let (key, value) = kv
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--set-config key=value"))?;
        let value: i64 = value
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("--set-config value must be an integer"))?;
        serde_json::json!({"action": "set_config", "key": key, "value": value})
    } else {
        anyhow::bail!("give one of --add/--remove/--threshold/--set-program/--set-config");
    };
    let res =
        control::query_unix_params(&data_dir.join("zeph.sock"), "gov_propose", params).await?;
    if res
        .get("applied")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        let set = res.get("set").cloned().unwrap_or_default();
        println!(
            "applied. governance now {}-of-{}  seq {}",
            set.get("threshold").and_then(|v| v.as_u64()).unwrap_or(0),
            set.get("governors")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            set.get("seq").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    } else {
        println!("proposal drafted + signed. needs more governor signatures.");
        println!(
            "approval: {}",
            res.get("approval").and_then(|v| v.as_str()).unwrap_or("")
        );
        println!("other governors: zeph gov-sign <approval>, then zeph gov-submit <approval>");
    }
    Ok(())
}

async fn cmd_apps(data_dir: &Path) -> anyhow::Result<()> {
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "apps", serde_json::json!({}))
        .await?;
    let rows = r
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        println!("no apps deployed - zeph deploy <app.wasm> --name <name>");
        return Ok(());
    }
    println!("{:<16} {:<5} CID", "NAME", "VER");
    for row in rows {
        let a = row.as_array().cloned().unwrap_or_default();
        let name = a.first().and_then(|v| v.as_str()).unwrap_or("");
        let cid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
        let ver = a.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
        println!("{:<16} v{:<4} {}", name, ver, &cid[..cid.len().min(16)]);
    }
    Ok(())
}

/// `zeph status` — print the daemon's live peer table.
async fn cmd_sql_exec(data_dir: &Path, ns: &str, sql: &str) -> anyhow::Result<()> {
    let params = serde_json::json!({"ns": ns, "sql": sql});
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "sql_exec", params).await?;
    println!(
        "committed — root {}",
        r.get("root")
            .and_then(|v| v.as_str())
            .unwrap_or("(unchanged)")
    );
    Ok(())
}

async fn cmd_sql_query(
    data_dir: &Path,
    owner: Option<&str>,
    ns: &str,
    sql: &str,
) -> anyhow::Result<()> {
    let mut params = serde_json::json!({"ns": ns, "sql": sql});
    if let Some(o) = owner {
        params["owner"] = serde_json::json!(o);
    }
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "sql_query", params).await?;
    let cols = r
        .get("columns")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let rows = r
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let cell = |v: &serde_json::Value| match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "".into(),
        other => other.to_string(),
    };
    println!("{}", cols.iter().map(cell).collect::<Vec<_>>().join(" | "));
    for row in &rows {
        if let Some(a) = row.as_array() {
            println!("{}", a.iter().map(cell).collect::<Vec<_>>().join(" | "));
        }
    }
    println!(
        "({} row{})",
        rows.len(),
        if rows.len() == 1 { "" } else { "s" }
    );
    Ok(())
}

fn trunc(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

fn human_size(n: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// The memory budget this process runs under: its cgroup-v2 `memory.max`
/// (what systemd `MemoryMax=` writes). None (no limit, "max", non-Linux)
/// disables the resource gauge.
fn detect_memory_budget() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let cg = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = cg.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
        let max = std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.max")).ok()?;
        max.trim().parse::<u64>().ok() // "max" (no limit) fails the parse → None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Current process RSS in bytes (Linux: /proc/self/statm resident pages).
fn read_self_rss() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

async fn cmd_files(data_dir: &Path, owner: Option<&str>) -> anyhow::Result<()> {
    let mut params = serde_json::json!({});
    if let Some(o) = owner {
        params["owner"] = serde_json::json!(o);
    }
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "files", params).await?;
    let rows = r
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        println!("(no files — publish something with `zeph publish <path>`)");
        return Ok(());
    }
    // columns: cid, name, size, mime, is_dir, published_at
    println!("{:<28} {:>9}  {:<22} CID", "NAME", "SIZE", "TYPE");
    for row in &rows {
        let Some(a) = row.as_array() else { continue };
        let cid = a.first().and_then(|v| v.as_str()).unwrap_or("");
        let name = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
        let size = a.get(2).and_then(|v| v.as_u64()).unwrap_or(0);
        let is_dir = a.get(4).and_then(|v| v.as_i64()).unwrap_or(0) != 0;
        let mime = a.get(3).and_then(|v| v.as_str()).unwrap_or("");
        let private = a.get(6).and_then(|v| v.as_bool()).unwrap_or(false);
        let kind = if is_dir { "folder" } else { mime };
        let name = if private {
            format!("🔒 {name}")
        } else {
            name.to_string()
        };
        println!(
            "{:<28} {:>9}  {:<22} {}",
            trunc(&name, 28),
            human_size(size),
            trunc(kind, 22),
            &cid[..16.min(cid.len())]
        );
    }
    println!(
        "({} file{})",
        rows.len(),
        if rows.len() == 1 { "" } else { "s" }
    );
    Ok(())
}

async fn cmd_sql_compact(data_dir: &Path, ns: &str) -> anyhow::Result<()> {
    let params = serde_json::json!({ "ns": ns });
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "sql_compact", params).await?;
    println!(
        "compacted — reclaimed {} superseded object(s)",
        r.get("reclaimed").and_then(|v| v.as_u64()).unwrap_or(0)
    );
    Ok(())
}

async fn cmd_sql_recover(data_dir: &Path, owner: Option<&str>, ns: &str) -> anyhow::Result<()> {
    let mut params = serde_json::json!({ "ns": ns });
    if let Some(o) = owner {
        params["owner"] = serde_json::json!(o);
    }
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "sql_recover", params).await?;
    println!(
        "recovered {} object(s) from durable generations",
        r.get("restored").and_then(|v| v.as_u64()).unwrap_or(0)
    );
    Ok(())
}

async fn cmd_status(data_dir: &Path) -> anyhow::Result<()> {
    let result = control::query_unix(&data_dir.join("zeph.sock"), "status").await?;
    let status: control::Status = serde_json::from_value(result)?;
    println!("node   {}", status.node_id);
    let alive = status.peers.iter().filter(|p| p.alive).count();
    println!(
        "reach  {}   wire v{}   uptime {}s   boot {}   peers {}/{} active · {} passive",
        status.reach,
        status.wire_version,
        status.uptime_secs,
        status.boot_stage,
        alive,
        status.peers.len(),
        status.passive_peers
    );
    println!("erasure {}", status.erasure);
    println!("hlc    {}.{}", status.hlc_ms, status.hlc_logical);
    println!("relays {}", status.relays);
    println!(
        "store  {} cids · {} pieces · {} pinned · {:.1} MiB · providing {}",
        status.store_cids,
        status.store_pieces,
        status.store_pinned,
        status.store_bytes as f64 / (1024.0 * 1024.0),
        status.providing,
    );
    println!(
        "health scanned {} · at-risk {} · repaired {} · distributed {}",
        status.health_scanned,
        status.health_at_risk,
        status.health_repaired,
        status.health_distributed,
    );
    if !status.in_flight_jobs.is_empty() {
        println!("in-flight jobs ({}):", status.in_flight_jobs.len());
        for j in &status.in_flight_jobs {
            println!("  {}  {:.1}s", j.key, j.elapsed_ms as f64 / 1000.0);
        }
    }
    if !status.content.is_empty() {
        println!("network content ({} cids):", status.content.len());
        for c in status.content.iter().take(10) {
            println!(
                "  {}…  {} providers · {} pinned",
                &c.cid[..24.min(c.cid.len())],
                c.providers,
                c.pinned
            );
        }
    }
    println!("listen {}", status.listen);
    if status.peers.is_empty() {
        println!("peers  (none known yet)");
        return Ok(());
    }
    println!(
        "\n{:<14} {:<7} {:>9} {:>7}  ADDRS",
        "PEER", "STATE", "RTT", "SKEW"
    );
    for peer in &status.peers {
        let state = if peer.alive { "alive" } else { "down" };
        let rtt = peer
            .rtt_us
            .map(|us| format!("{:.1}ms", us as f64 / 1000.0))
            .unwrap_or_else(|| "-".into());
        let skew = peer
            .skew_ms
            .map(|ms| format!("{ms}ms"))
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<14} {:<7} {:>9} {:>7}  {}",
            &peer.id[..12.min(peer.id.len())],
            state,
            rtt,
            skew,
            peer.addrs
        );
    }
    Ok(())
}

/// Run the daemon (default command).
async fn cmd_run(data_dir: &Path, args: RunArgs) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut cfg = load_or_init_config(data_dir)?;
    if let Some(reach) = args.reach {
        cfg.reach = reach;
    }
    if let Some(hb) = args.heartbeat_secs {
        cfg.heartbeat_secs = hb;
    }
    if let Some(port) = args.listen_port {
        cfg.listen_port = port;
    }
    if let Some(port) = args.dashboard_port {
        cfg.dashboard_port = port;
    }
    if let Some(port) = args.public_stats_port {
        cfg.public_stats_port = port;
    }
    if !args.relay_urls.is_empty() {
        cfg.relay_urls = args.relay_urls;
    }
    if args.no_fallback_relays {
        cfg.fallback_relays = false;
    }
    cfg.peers.extend(args.peers);

    let reach = match cfg.reach.as_str() {
        "local" => Reach::LocalOnly,
        "relayed" => Reach::Relayed,
        other => anyhow::bail!("invalid reach `{other}`: expected \"local\" or \"relayed\""),
    };

    // Fail fast on malformed peer addresses.
    let mut peers: Vec<PeerAddr> = cfg
        .peers
        .iter()
        .map(|s| s.parse().map_err(anyhow::Error::from))
        .collect::<anyhow::Result<_>>()?;
    // Seed membership from dht_seeds too. With only dht_seeds configured, the membership bootstrap
    // would be EMPTY, so a fully-isolated node could never re-bootstrap (its recover_isolated has
    // no seed to dial) — this is what left the relay Mac STUCK at eligible=1 after losing its
    // overlay, unable to reconnect even once its peers returned.
    for entry in &cfg.dht_seeds {
        if let Ok(addr) = entry.parse::<PeerAddr>() {
            if !peers.iter().any(|p| p.node_id() == addr.node_id()) {
                peers.push(addr);
            }
        }
    }

    let relay_urls = cfg
        .relay_urls
        .iter()
        .map(|u| {
            u.parse()
                .map_err(|e| anyhow::anyhow!("relay url `{u}`: {e}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let identity = Arc::new(Keystore::new(data_dir.join("keys")).init_or_load()?);
    // MUX (element 1): one connection per peer carries every protocol via a
    // per-stream tag. Migrated protocols dial/serve MUX_ALPN; the remaining
    // legacy per-ALPN entries drop as each protocol moves over.
    let alpns = vec![zeph_transport::MUX_ALPN.to_vec()];
    // dht/registry/sqlpage/invoke/member are muxed (per-stream tag) — no ALPN.
    let transport = Arc::new(
        Transport::bind_with_relays(
            identity.secret_key_bytes(),
            reach,
            alpns,
            cfg.listen_port,
            relay_urls,
            cfg.fallback_relays,
        )
        .await?,
    );

    // Storage engine: persistent store + DHT routing + obj.
    let store = Arc::new(Store::open(data_dir.join("store"))?);
    // Content-routing backend: the Kademlia DHT is the sole production router (the tracker is
    // retired). Kept as `Option` (always `Some`) so the persistence/serve sites below read cleanly.
    let dht_node = Some(zeph_dht::DhtNode::new(
        identity.clone(),
        transport.clone(),
        48 * 3600 * 1000, // 48h record TTL (foundation §3)
    ));
    // Persist the DHT record store under the data dir. A content-routing DHT on fixed-identity
    // infra nodes should survive restart with its records intact (like IPFS) — an in-memory-only
    // store loses everything on restart and forces a false-at-risk repair storm until re-announce
    // repopulates it. Load on boot (expiring + re-verifying), checkpoint every 120s, save on exit.
    let dht_records_path = data_dir.join("dht_records.bin");
    let dht_table_path = data_dir.join("dht_table.bin");
    if let Some(dht) = &dht_node {
        let n = dht.load_records(&dht_records_path);
        if n > 0 {
            tracing::info!(loaded = n, "dht: restored persisted record store");
        }
        // Also restore the routing table so the overlay re-forms INSTANTLY on restart instead of
        // re-bootstrapping from seeds — the overlay-complete scan gate then clears at once.
        let t = dht.load_table(&dht_table_path);
        if t > 0 {
            tracing::info!(contacts = t, "dht: restored routing table");
        }
        let (dht2, rpath, tpath) = (
            dht.clone(),
            dht_records_path.clone(),
            dht_table_path.clone(),
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(120));
            interval.tick().await; // skip the immediate tick
            loop {
                interval.tick().await;
                match dht2.save_records(&rpath) {
                    Ok(n) => tracing::debug!(saved = n, "dht: checkpointed record store"),
                    Err(e) => tracing::warn!(error = %e, "dht: record store checkpoint failed"),
                }
                match dht2.save_table(&tpath) {
                    Ok(n) => tracing::debug!(saved = n, "dht: checkpointed routing table"),
                    Err(e) => tracing::warn!(error = %e, "dht: routing table checkpoint failed"),
                }
            }
        });
    }
    let routing: Arc<dyn zeph_routing::ContentRouting> = Arc::new(zeph_routing::DhtRouting::new(
        dht_node
            .as_ref()
            .expect("dht_node is always constructed")
            .clone(),
    ));
    // Candidate-peer source: SWIM membership (census is in-network on the DHT). The membership
    // handle is injected after membership is built.
    let mem_peers = peers::MembershipPeers::new();
    let peer_source: Arc<dyn zeph_obj::PeerSource> = mem_peers.clone();
    let engine = ObjEngine::with_peer_source(
        transport.clone(),
        store.clone(),
        routing.clone(),
        peer_source,
        ObjConfig {
            k: cfg.erasure_k,
            durability_threshold: cfg.durability_threshold,
            capacity_bytes: (cfg.storage_quota_gib * 1024.0 * 1024.0 * 1024.0) as u64,
            probe_timeout: Duration::from_secs(cfg.probe_timeout_secs),
            scale_threshold: cfg.scale_threshold,
            degrade_threshold: cfg.degrade_threshold,
            fade_grace: Duration::from_secs(cfg.fade_grace_secs),
            eviction_cooldown: Duration::from_secs(cfg.eviction_cooldown_secs),
            pace_delay: Duration::from_secs(cfg.pace_delay_secs),
            active_set_k: 4, // element 2 choke: K distinct active push peers (default)
            ..ObjConfig::default()  // file segmentation (8 MiB / K=32) — defaults
        },
    );
    // The owner's encryption keypair (PRE), derived from the identity seed —
    // enables `publish --private` / private reads (ENCRYPTION_DESIGN.md).
    engine.set_enc_keypair(zeph_cipher::EncKeypair::from_identity_seed(
        &identity.secret_key_bytes(),
    ));

    // Generic program accounts — any program's single-writer state (the program IS the writer).
    // Built FIRST so the program registry (and CraftSQL's registry-backed head stores) can share
    // this same store Arc.
    let account_store = std::sync::Arc::new(account::ProgramAccountStore::open(
        engine.clone(),
        data_dir,
        transport.clock(),
    ));
    // The registry's per-shard state is a CraftSQL DB (SQL_REGISTRY_DESIGN). Its own CraftSql
    // engine, owned by this node, with a BLOB-backed RootStore (shard-DB roots stored in account
    // blobs, NOT back through the registry — breaks the recursion). ObjDurable gives the shard pages
    // the DEFAULT erasure-coded durability (each commit's changed pages coded + distributed +
    // repaired — parity with the old blob registry, which published every state via publish_system),
    // on top of K-replica row-push. NO PageSource: replicas build their shard DBs from pushed
    // submissions and backfill via GetState, so no cross-node page fetch is needed. (Shard-DB
    // namespaces are slash-free so the `.gens` durability sidecar path stays valid — see ns_of.)
    let shard_root_store =
        std::sync::Arc::new(shard_root::ShardRootStore::new(account_store.clone()));
    let shard_sql = std::sync::Arc::new(
        zeph_sql::CraftSql::register(
            data_dir.join("regshards"),
            shard_root_store.clone(),
            transport.node_id(),
        )?
        .with_durable(Arc::new(zeph_sql::ObjDurable::new(engine.clone()))),
    );

    // Phase 4c: the durable program-name registry — a THIN consumer of the account store.
    // The writer for each epoch is ELECTED deterministically from the membership view + HLC
    // clock (a rotating writer); non-writers forward to the current epoch's writer. Built BEFORE
    // CraftSQL so its DB roots/manifests can ride this same registry substrate.
    let head_registry = std::sync::Arc::new(headreg::HeadRegistry::open(
        identity.clone(),
        account_store.clone(),
        transport.clock(),
        transport.clone(),
        shard_sql.clone(),
        shard_root_store.clone(),
    ));

    // CraftSQL: SQLite over content-addressed pages; single-writer head via
    // KIND_ROOT, cross-node page fetch over the transport. Heads (DB roots) and durability
    // manifests are published/resolved through the owner-signed registry (RT_DBROOT/RT_MANIFEST),
    // not the DHT — persisted + replicated by the program-account store.
    let sql_dir = data_dir.join("sqlpages");
    let sql_heads = Arc::new(registry_heads::RegistryRootStore::new(
        head_registry.clone(),
        transport.clock(),
    ));
    let sql_source = Arc::new(zeph_sql::TransportPageSource::new(
        transport.clone(),
        mem_peers.clone(),
    ));
    let sql_manifests = Arc::new(registry_heads::RegistryManifestStore::new(
        head_registry.clone(),
        transport.clock(),
    ));
    let craftsql = Arc::new(
        zeph_sql::CraftSql::register(&sql_dir, sql_heads, transport.node_id())?
            .with_source(sql_source)
            .with_durable(Arc::new(zeph_sql::ObjDurable::new(engine.clone())))
            .with_manifests(sql_manifests)
            .with_enc_keypair(Arc::new(zeph_cipher::EncKeypair::from_identity_seed(
                &identity.secret_key_bytes(),
            ))),
    );

    // CraftCOM: the sovereign-app runtime. Apps run sandboxed against this node's
    // CraftBackend (writes its own `app.<ns>` namespaces), invoked locally (control
    // API) or remotely (INVOKE_ALPN, caller = the authenticated peer).
    let com_backend: Arc<dyn zeph_com::AppBackend> = Arc::new(zeph_com::CraftBackend::new(
        craftsql.clone(),
        engine.clone(),
        transport.clock(),
        // The OWNER's PRE keypair (this identity's) — lets the `pre_grant` host fn delegate this
        // node's data to a recipient without the secret ever leaving the backend (K3 sharing).
        zeph_cipher::EncKeypair::from_identity_seed(&identity.secret_key_bytes()),
    ));
    // The verification board service is the app runtime's verify() backend (post + await a
    // certificate), and it also serves/gossips/verifies over tag::BOARD (wired further below).
    let board_service =
        board::BoardService::new(identity.clone(), transport.clone(), engine.clone());
    // The attestation store is the app runtime's attest() backend: per-program quorum chains;
    // attest() = "is this statement authorized by the program's quorum?" GLOBAL like governance —
    // each chain is published + pulled cross-node (obj + routing), so attest() works on any node.
    let attest_store =
        attest::AttestStore::open(identity.clone(), data_dir, engine.clone(), routing.clone());
    let com_service = Arc::new(zeph_com::InvokeService::new(
        zeph_com::TransitionRuntime::new()?,
        engine.clone(),
        com_backend,
        Some(board_service.clone()),
        Some(attest_store.clone()),
        None, // sequence_backend — the ordering sequencer's node store is wired in P3
    ));

    tracing::info!(
        node_id = %identity.node_id().to_hex(),
        data_dir = %data_dir.display(),
        reach = %cfg.reach,
        peers = peers.len(),
        "zeph node started"
    );
    // Machine-readable self address — paste into another node's --peer flag.
    println!("ZEPH_ADDR {}", transport.addr());

    // The node event bus (foundation §52): subsystems publish, apps subscribe.
    let events = zeph_events::EventBus::default();
    engine.set_events(events.clone());

    // Background job coordinator (foundation §51): the periodic lifecycle, re-announce, and
    // future per-item reactive jobs run THROUGH it — prioritized, deduped (a slow pass can't
    // stack), retried, and metered. It is a BOUNDED-CONCURRENCY scheduler, so it runs as a
    // small worker pool: priority ORDERS contended work without starving a class. At
    // concurrency 1 it degenerated into a serial queue where a long, frequent Distribution
    // job (re-announce, which grows with held content) perpetually starved the lowest
    // priority — HealthScan, the durability-maintenance pass — so it never ran (scanned=0).
    let jobs = zeph_sched::JobCoordinator::new(8);
    // BOOT CONVERGENCE runs the queue nearly one-at-a-time (user directive:
    // establish and converge sequentially, never storm) — restored to full
    // width once the post-boot wave drains (see the scan feeder phases).
    jobs.set_active_cap(2);

    // Demand-driven scaling: the serve path fires a CID here the instant its served-pull count
    // crosses scale_threshold; a bounded worker recruits one more provider right then. Scaling
    // reacts to ACCESS, not to any scan/distribute cadence — so a healthy CID that's backing off
    // its durability re-check (up to ~32min) still gets an extra replica the moment it goes hot.
    {
        let (scale_tx, mut scale_rx) = tokio::sync::mpsc::unbounded_channel::<Cid>();
        engine.set_scale_trigger(scale_tx);
        let scale_engine = engine.clone();
        let scale_jobs = jobs.clone();
        tokio::spawn(async move {
            while let Some(cid) = scale_rx.recv().await {
                let eng = scale_engine.clone();
                // Recruit through the coordinator too — one job manager for all reactive work.
                scale_jobs.submit(
                    format!("scale:{}", cid.to_hex()),
                    zeph_sched::Priority::Distribution,
                    1,
                    move || {
                        let eng = eng.clone();
                        async move {
                            eng.scale_one(cid).await;
                            Ok(())
                        }
                    },
                );
            }
        });
    }

    // Governance: one durable, self-verifying chain that derives BOTH the governor set
    // and the program registry, published + resolved cross-node (seeded 1-of-1 with this
    // node's key by default).
    let gov_governors: Vec<[u8; 32]> = cfg
        .governance_governors
        .iter()
        .filter_map(|h| <[u8; 32]>::try_from(hex::decode(h.trim()).ok()?.as_slice()).ok())
        .collect();
    let governance_store = std::sync::Arc::new(governance::GovernanceChainStore::open(
        identity.clone(),
        data_dir,
        &gov_governors,
        cfg.governance_threshold,
        engine.clone(),
        routing.clone(),
    ));
    let state = Arc::new(control::State {
        clock: transport.clock(),
        boot_stage: tokio::sync::RwLock::new("booting".to_string()),
        node_id: identity.node_id().to_hex(),
        reach: cfg.reach.clone(),
        relays: if matches!(reach, Reach::LocalOnly) {
            "none (local)".to_string()
        } else {
            format!(
                "{}{}",
                cfg.relay_urls.join(", "),
                if cfg.fallback_relays {
                    " + n0 fallback"
                } else {
                    " (exclusive)"
                }
            )
        },
        listen: transport.addr().to_string(),
        started: std::time::Instant::now(),
        engine: engine.clone(),
        peers: tokio::sync::RwLock::new(Vec::new()),
        passive_peers: std::sync::atomic::AtomicU32::new(0),
        census: std::sync::atomic::AtomicU32::new(0),
        storage: tokio::sync::RwLock::new((0, 0, 0, 0)),
        providing: std::sync::atomic::AtomicU64::new(0),
        content: tokio::sync::RwLock::new(Vec::new()),
        cid_health: tokio::sync::RwLock::new(Vec::new()),
        health: tokio::sync::RwLock::new((0, 0, 0, 0, 0, 0, 0, 0, 0)),
        scan_queue: std::sync::atomic::AtomicUsize::new(0),
        scan_due: std::sync::atomic::AtomicUsize::new(0),
        craftsql: craftsql.clone(),
        events: events.clone(),
        recent_events: tokio::sync::RwLock::new(std::collections::VecDeque::new()),
        jobs: jobs.clone(),
        event_counts: tokio::sync::RwLock::new(std::collections::BTreeMap::new()),
        hosting_cids: std::sync::atomic::AtomicU64::new(0),
        com: com_service.clone(),
        registry: head_registry.clone(),
        gov: governance_store.clone(),
        accounts: account_store.clone(),
        attest: attest_store.clone(),
        settings: {
            let oc = engine.config();
            control::NodeSettings {
                reach: cfg.reach.clone(),
                listen_port: cfg.listen_port,
                dashboard_port: cfg.dashboard_port,
                heartbeat_secs: cfg.heartbeat_secs,
                fallback_relays: cfg.fallback_relays,
                probe_timeout_secs: oc.probe_timeout.as_secs(),
                relay_urls: cfg.relay_urls.clone(),
                relay_operator_urls: cfg.relay_operator_urls.clone(),
                peers: cfg.peers.clone(),
                storage_quota_gib: cfg.storage_quota_gib,
                erasure_k: oc.k,
                durability_threshold: oc.durability_threshold,
                scale_threshold: oc.scale_threshold,
                degrade_threshold: oc.degrade_threshold,
                fade_grace_secs: oc.fade_grace.as_secs(),
                eviction_cooldown_secs: oc.eviction_cooldown.as_secs(),
                health_scan_secs: cfg.health_scan_secs,
                reannounce_secs: cfg.reannounce_secs,
                data_dir: data_dir.display().to_string(),
            }
        },
    });

    // Engine heavy-work router: publish distribution and durability repair are
    // DETECTED by the engine but SCHEDULED here through the coordinator — publish
    // bursts become bounded, deduped Encoding jobs instead of a spawn per publish,
    // and repair finally runs at Repair priority (it used to execute inline inside
    // the HealthScan job, leaving the top priority tier unused).
    {
        let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel::<zeph_obj::EngineWork>();
        engine.set_work_trigger(work_tx);
        let work_engine = engine.clone();
        let work_jobs = jobs.clone();
        let work_state = state.clone();
        tokio::spawn(async move {
            while let Some(item) = work_rx.recv().await {
                match item {
                    zeph_obj::EngineWork::PublishDistribute(cid) => {
                        let eng = work_engine.clone();
                        work_jobs.submit(
                            format!("publish:{}", cid.to_hex()),
                            zeph_sched::Priority::Encoding,
                            1,
                            move || {
                                let eng = eng.clone();
                                async move {
                                    eng.distribute_initial(cid).await;
                                    Ok(())
                                }
                            },
                        );
                    }
                    zeph_obj::EngineWork::Repair(cid) => {
                        let eng = work_engine.clone();
                        let st = work_state.clone();
                        // max_attempts=1: repair_cid returning false is a valid
                        // outcome (another holder won the election), not an error
                        // to retry; the next scan pass re-detects if still at risk.
                        work_jobs.submit(
                            format!("repair:{}", cid.to_hex()),
                            zeph_sched::Priority::Repair,
                            1,
                            move || {
                                let (eng, st) = (eng.clone(), st.clone());
                                async move {
                                    let landed = eng.repair_cid(cid).await;
                                    if landed > 0 {
                                        st.add_repaired(landed as u64).await;
                                    }
                                    Ok(())
                                }
                            },
                        );
                    }
                }
            }
        });
    }

    // Activity feed: drain the event bus into the bounded recent-events buffer
    // (foundation §52 consumer). Any other subscriber — or the control API — can
    // subscribe independently for its own reactive logic.
    let feed_state = state.clone();
    let mut feed_rx = events.subscribe();
    tokio::spawn(async move {
        loop {
            match feed_rx.recv().await {
                Ok(ev) => feed_state.record_event(&ev).await,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let control_state = state.clone();
    let sock_path = data_dir.join("zeph.sock");
    tokio::spawn(async move {
        if let Err(err) = control::serve_unix(control_state, sock_path).await {
            tracing::error!(%err, "control socket server failed");
        }
    });

    if cfg.dashboard_port != 0 {
        let token = control::load_or_create_token(data_dir)?;
        let http_state = state.clone();
        let port = cfg.dashboard_port;
        tokio::spawn(async move {
            if let Err(err) = control::serve_http(http_state, token, port).await {
                tracing::error!(%err, "dashboard server failed");
            }
        });
    }

    // Fully muxed: every protocol is a per-stream tag on the shared per-peer
    // connection — no legacy per-ALPN connection handlers remain.
    let (ping_stream_tx, mut ping_rx) = tokio::sync::mpsc::channel(32);
    let mut stream_handlers: Vec<(u8, tokio::sync::mpsc::Sender<zeph_transport::TaggedStream>)> =
        vec![(zeph_transport::tag::PING, ping_stream_tx)];
    if let Some(dht) = &dht_node {
        let (dht_stream_tx, dht_stream_rx) = tokio::sync::mpsc::channel(32);
        stream_handlers.push((zeph_transport::tag::DHT, dht_stream_tx));
        dht.clone().serve(dht_stream_rx);
    }
    let (piece_stream_tx, piece_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::PIECE, piece_stream_tx));
    let (member_stream_tx, member_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::MEMBER, member_stream_tx));
    let (registry_stream_tx, registry_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::REGISTRY, registry_stream_tx));
    let (sqlpage_stream_tx, sqlpage_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::SQLPAGE, sqlpage_stream_tx));
    let (invoke_stream_tx, invoke_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::INVOKE, invoke_stream_tx));
    // Verification board handler (the service was built above as the com verify backend): gossips +
    // verifies over tag::BOARD (additive — old nodes drop it).
    let (board_stream_tx, board_stream_rx) = tokio::sync::mpsc::channel(32);
    stream_handlers.push((zeph_transport::tag::BOARD, board_stream_tx));
    let server = transport.clone();
    tokio::spawn(async move { server.serve(stream_handlers).await });
    tokio::spawn(engine.clone().serve(piece_stream_rx));
    tokio::spawn(zeph_sql::serve_pages(sql_dir.clone(), sqlpage_stream_rx));
    tokio::spawn(zeph_com::serve_invocations(
        invoke_stream_rx,
        com_service.clone(),
    ));
    // Serve cross-node registry requests (writer path advances; queries resolve).
    tokio::spawn(head_registry.clone().serve(registry_stream_rx));
    // Serve + gossip + verify the verification board.
    tokio::spawn(board_service.clone().serve(board_stream_rx));
    // "Pending distribution" completion (per-incomplete-cid DHT resolve + deficit pushes)
    // — network fan-out, so it runs THROUGH the coordinator (Distribution priority,
    // deduped: a slow pass coalesces with the next tick instead of stacking). This loop
    // only ticks the schedule. It was an inline loop before — the one distribution path
    // that bypassed the coordinator entirely (audit finding).
    let pending_engine = engine.clone();
    let pending_jobs = jobs.clone();
    let pending_ready = head_registry.clone();
    tokio::spawn(async move {
        // Boot ordering (user directive): let the node SETTLE first — census
        // convergence + connection warmup — before background pushes start.
        // Scans/pushes during warmup queued minutes behind the dial layer.
        pending_ready.wait_ready().await;
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(12));
        loop {
            iv.tick().await;
            let eng = pending_engine.clone();
            pending_jobs.submit(
                "distribute_pending",
                zeph_sched::Priority::Distribution,
                1,
                move || {
                    let eng = eng.clone();
                    async move {
                        eng.distribute_pending().await;
                        Ok(())
                    }
                },
            );
        }
    });
    // Governance anti-entropy: 30s cadence. Governance changes are rare and human-initiated, so
    // 5s adoption latency bought nothing — and the per-tick resolve+fetch across the census was a
    // constant stream of QUIC handshakes that congested slow links (membership pings timed out on
    // the relay-Mac while ICMP on the same path was clean). Fetches are also version-gated now, so
    // a steady-state tick is census-many DHT gets and NO content fetches.
    let gov_tick = governance_store.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            iv.tick().await;
            gov_tick.tick().await;
        }
    });

    // Re-announce provider records for everything we hold (pins + pieces) —
    // CHUNKED: the due list becomes ~25-cid coordinator jobs that interleave
    // with scans/repairs, instead of one O(held) job holding a slot through
    // the startup burst (measured 22s — every run's max_job once the
    // distribute sweep died). Steady-state due lists are near-zero.
    // NOTE: CraftSQL DB heads + durability manifests are NOT re-announced
    // here — they ride the owner-signed registry substrate (RT_DBROOT /
    // RT_MANIFEST); `CraftSql::reannounce_heads` is retained but unused.
    let announce_engine = engine.clone();
    let announce_jobs = jobs.clone();
    let reannounce_secs = cfg.reannounce_secs.max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(reannounce_secs));
        loop {
            interval.tick().await;
            let due = announce_engine.due_announcements();
            let total = due.len();
            for (i, chunk) in due.chunks(25).enumerate() {
                let e = announce_engine.clone();
                let batch: Vec<_> = chunk.to_vec();
                announce_jobs.submit(
                    format!("reannounce:{i}"),
                    zeph_sched::Priority::Distribution,
                    1,
                    move || {
                        let (e, batch) = (e.clone(), batch.clone());
                        async move {
                            e.announce_batch(&batch).await;
                            Ok(())
                        }
                    },
                );
            }
            if total > 0 {
                tracing::info!(cids = total, "re-announce scheduled (chunked)");
            }
            let e = announce_engine.clone();
            announce_jobs.submit(
                "reannounce_wants",
                zeph_sched::Priority::Distribution,
                1,
                move || {
                    let e = e.clone();
                    async move {
                        e.reannounce_wants().await;
                        Ok(())
                    }
                },
            );
        }
    });
    let ping_clock = transport.clock();
    tokio::spawn(async move {
        while let Some(stream) = ping_rx.recv().await {
            tokio::spawn(Transport::handle_ping_stream(ping_clock.clone(), stream));
        }
    });

    // Membership: bootstrap from configured peers; probes drive the peer table.
    let membership = Membership::new(
        transport.clone(),
        zeph_membership::Config {
            probe_interval: Duration::from_secs(cfg.heartbeat_secs.max(1)),
            ..Default::default()
        },
    );
    membership.start(peers, member_stream_rx);
    state.set_boot_stage("census-settling").await;
    {
        let (st, reg) = (state.clone(), head_registry.clone());
        tokio::spawn(async move {
            reg.wait_ready().await;
            st.set_boot_stage("lifecycle-running").await;
        });
    }
    governance_store.set_membership(membership.clone()).await;
    head_registry.set_membership(membership.clone()).await;
    board_service.set_membership(membership.clone()).await;
    attest_store.set_membership(membership.clone()).await;
    head_registry.set_programs(governance_store.clone()).await;
    head_registry.clone().set_jobs(jobs.clone()).await;

    // Resource manager: the node reads its OWN memory budget from its cgroup
    // (the systemd MemoryMax it runs under) and samples RSS every 5s. Above
    // HIGH the coordinator defers routine jobs (only Repair starts); above
    // CRITICAL nothing new starts and inbound piece/registry intake sheds
    // ("busy" — senders retry on their next pass). No budget (no cgroup limit,
    // non-Linux) → gauge off, behavior unchanged. Mechanism only: the budget
    // comes from the environment, never from the binary.
    if let Some(budget) = detect_memory_budget() {
        let gauge = zeph_sched::ResourceGauge::new();
        gauge.set_budget(budget);
        jobs.set_gauge(gauge.clone());
        let g = gauge.clone();
        engine.set_shed_gate(std::sync::Arc::new(move || g.critical()));
        // Graded offer/grant admission (TRANSFER_PLANE_V2 §3): critical → grant
        // nothing (start no ingest); high → admit only the durability-critical
        // class, and just one piece; otherwise a bounded healthy batch. Senders
        // redirect the ungranted remainder to peers with capacity.
        let g = gauge.clone();
        engine.set_grant_gate(std::sync::Arc::new(move |class, items| {
            if g.critical() {
                0
            } else if g.high() {
                if class == zeph_obj::CLASS_CRITICAL {
                    1.min(items)
                } else {
                    0
                }
            } else {
                items.min(zeph_obj::MAX_GRANT_PER_OFFER)
            }
        }));
        let g = gauge.clone();
        head_registry.set_shed_gate(std::sync::Arc::new(move || g.critical()));
        let g = gauge.clone();
        tokio::spawn(async move {
            // 1s cadence: allocation bursts (pipelined ingest) can blow through
            // a capped node's headroom between coarser samples before the shed
            // gates ever see the pressure.
            let mut iv = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                iv.tick().await;
                if let Some(rss) = read_self_rss() {
                    g.set_rss(rss);
                }
            }
        });
        tracing::info!(
            budget_mib = budget / (1 << 20),
            "resource gauge armed from cgroup memory limit"
        );
    }
    mem_peers.set(membership.clone()).await;
    // PUBLIC stats endpoint (0 = disabled): token-free counts for the website's live-network
    // section, served from the converged census + this node's store/DHT — the retired tracker's
    // `--public-stats-port` replacement (api.zeph.craft.ec/stats proxies here).
    if cfg.public_stats_port != 0 {
        if let Some(dht) = &dht_node {
            let stats_state = state.clone();
            let stats_membership = membership.clone();
            let stats_dht = dht.clone();
            let relays = cfg.relay_urls.len();
            let quota = cfg.storage_quota_gib;
            let port = cfg.public_stats_port;
            tokio::spawn(async move {
                if let Err(err) = control::serve_public_stats(
                    stats_state,
                    stats_membership,
                    stats_dht,
                    relays,
                    quota,
                    port,
                )
                .await
                {
                    tracing::error!(%err, "public stats server failed");
                }
            });
        }
    }
    // Membership is the health scan's LIVENESS source (on both routing paths): a holder that
    // SWIM marks dead is excluded from durability counts so repair fires, instead of its stale
    // provider record lingering until TTL.
    engine.set_liveness(mem_peers.clone());
    // DHT overlay (flag-gated): bootstrap from seed peers, then expire stale records hourly.
    // Provider republish rides on the existing re-announce loop (routing.announce → DHT put).
    if let Some(dht) = &dht_node {
        let seeds: Vec<zeph_dht::Contact> = cfg
            .dht_seeds
            .iter()
            .filter_map(|entry| {
                let addr: PeerAddr = entry.parse().ok()?;
                let id_bytes: [u8; 32] = hex::decode(entry.split('@').next()?)
                    .ok()?
                    .try_into()
                    .ok()?;
                Some(zeph_dht::Contact {
                    id: zeph_core::NodeId(id_bytes),
                    addr,
                })
            })
            .collect();
        let seeded = seeds.len();
        let dht_b = dht.clone();
        tokio::spawn(async move {
            dht_b.bootstrap(seeds).await;
            tracing::info!(seeds = seeded, "dht overlay bootstrapped");
        });
        let dht_e = dht.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(3600));
            iv.tick().await;
            loop {
                iv.tick().await;
                let n = dht_e.expire();
                if n > 0 {
                    tracing::debug!(expired = n, "dht records expired");
                }
            }
        });
    }

    // Seed membership from the configured seed peers — the tracker-free bootstrap: a node
    // bootstraps from the network without any hardcoded peer. SWIM probing takes over from there.
    let seed_membership = membership.clone();
    let config_seeds: Vec<PeerAddr> = cfg
        .dht_seeds
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            if !config_seeds.is_empty() {
                seed_membership.seed(config_seeds.clone()).await;
            }
        }
    });

    // HealthScan: periodically verify availability of held content and repair
    // (the self-healing control loop). Runs every epoch (30s).
    let health_engine = engine.clone();
    let health_state = state.clone();
    let health_jobs = jobs.clone();
    let health_scan_secs = cfg.health_scan_secs.max(1);
    // HealthScan scheduler: a per-CID work QUEUE, not a sweep. Every held CID is an INDIVIDUAL
    // item on a delay-queue keyed by when it is next due. Bounded-concurrency workers pull the
    // earliest-due item, scan just that one CID, then re-enqueue it for its next check — so
    // coverage is a continuous cycle whose interval EMERGES from throughput (~ N / scan-rate),
    // floored at health_scan_secs for small sets. A cheap discovery pass feeds new CIDs in as
    // items; gone CIDs drop out on completion. Scales to 100k+ CIDs: bounded memory (<= N items
    // + a few in flight), no O(N) sweep, and no job that holds a slot while it sleeps.
    // Adaptive re-check bounds: at-risk (and freshly discovered) cids re-check at recheck_min;
    // healthy cids back off geometrically up to recheck_max (min * 64).
    let recheck_min = Duration::from_secs(health_scan_secs);
    let recheck_max = Duration::from_secs(health_scan_secs.saturating_mul(64));
    // Elected-scan safety net (v2 element 4): a non-winner's own snapshot older
    // than this forces an unconditional refresh scan, so a phantom-elected cid
    // (winner still alive + cached but no longer holds it) can't go unscanned.
    // 4× recheck_max: negligible aggregate-resolve cost in the steady window,
    // yet bounds worst-case detection to a few minutes at defaults.
    let scan_stale_ceiling = recheck_max.saturating_mul(4);
    let hs_queue: Arc<DueQueue> =
        Arc::new(std::sync::Mutex::new(std::collections::BinaryHeap::new()));
    let hs_seen: Arc<std::sync::Mutex<std::collections::HashSet<Cid>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

    // Discovery: push any held CID we aren't already tracking (published OR hosted-for-others),
    // due immediately. A cheap set-diff (no DHT) — it just FEEDS individual items into the queue.
    {
        let (eng, q, seen) = (engine.clone(), hs_queue.clone(), hs_seen.clone());
        let feeder_ready = head_registry.clone();
        let feeder_jobs = jobs.clone();
        tokio::spawn(async move {
            // ONE THING AT A TIME (user directive): boot phases run
            // SEQUENTIALLY, each to completion, instead of storming every
            // work class into the same 8 job slots at once.
            // Phase 1: census settle (readiness gate).
            feeder_ready.wait_ready().await;
            // Phase 2: let the registry replication / reannounce wave DRAIN
            // (they own the queue right after settle) before scans compete
            // for the same slots and dial lanes. Bounded wait.
            // Let the wave FORM before waiting for it to drain — checking
            // instantly after settle trivially passed (queue still empty)
            // and the boot clamp never applied.
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;
            let phase2 = tokio::time::Instant::now();
            loop {
                if feeder_jobs.stats().queue_depth < 32
                    || phase2.elapsed() > std::time::Duration::from_secs(300)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            // Stable state reached: open the queue to full width.
            feeder_jobs.set_active_cap(8);
            tracing::info!(
                "boot phase: converged — full concurrency restored, scan feed starting (dripped)"
            );
            // Phase 3: DRIP the initial scan backlog over ~2 minutes instead
            // of dumping thousands of due-now jobs.
            let mut first_pass = true;
            loop {
                let now = std::time::Instant::now();
                let mut i: u64 = 0;
                for cid in eng.store().cids() {
                    let is_new = seen.lock().expect("seen").insert(cid);
                    if is_new {
                        let due = if first_pass {
                            i += 1;
                            now + std::time::Duration::from_millis((i % 1200) * 100)
                        } else {
                            now
                        };
                        q.lock()
                            .expect("q")
                            .push(std::cmp::Reverse((due, cid, recheck_min)));
                    }
                }
                first_pass = false;
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }

    // Workers: pull the earliest-due CID, scan it under a concurrency cap (a permit frees only
    // when a scan completes — "submit as they are completed"), then re-enqueue it.
    {
        let (eng, st, q, seen, live, coord, dht) = (
            engine.clone(),
            state.clone(),
            hs_queue.clone(),
            hs_seen.clone(),
            mem_peers.clone(),
            jobs.clone(),
            dht_node
                .as_ref()
                .expect("dht_node is always constructed")
                .clone(),
        );
        tokio::spawn(async move {
            // Wait for the peer view AND the DHT overlay to SETTLE before the FIRST scan — two
            // distinct readiness states. (1) membership converged: the alive-peer count is stable,
            // not merely non-empty. (2) the Kademlia overlay is formed: the routing table has
            // contacts and has stopped growing, so `resolve` can actually reach the nodes holding
            // provider records. Scanning against a half-formed overlay returns thin provider lists
            // and manufactures false at-risk repair work — the node is still INITIALIZING until the
            // overlay is complete. Bounded by a max grace so a genuinely-alone or slow-to-form node
            // still proceeds (and maintains its own pinned content).
            let start = std::time::Instant::now();
            let mut last_peers = usize::MAX;
            let mut last_table = usize::MAX;
            let mut stable_since = start;
            loop {
                let peers = live.peers().await.len();
                let table = dht.table_len();
                if peers != last_peers || table != last_table {
                    last_peers = peers;
                    last_table = table;
                    stable_since = std::time::Instant::now();
                }
                let ready =
                    peers > 0 && table > 0 && stable_since.elapsed() >= Duration::from_secs(10);
                if ready || start.elapsed() >= Duration::from_secs(90) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            loop {
                let next = q.lock().expect("q").peek().map(|r| r.0 .0);
                let Some(due) = next else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                };
                let now = std::time::Instant::now();
                if due > now {
                    // Wait until due, but wake at least once a second to notice newly-fed items.
                    tokio::time::sleep((due - now).min(Duration::from_secs(1))).await;
                    continue;
                }
                let (cid, delay) = match q.lock().expect("q").pop() {
                    Some(item) => (item.0 .1, item.0 .2),
                    None => continue,
                };
                // ELECTED SCAN (v2 element 4): only the rendezvous-elected
                // scanner for (cid, epoch) does the real DHT-resolving scan;
                // every OTHER holder skips it here (no resolve, no job slot) and
                // reschedules on the provider-aware cadence. This turns N×-holder
                // duplicated lookups into ~one resolve per cid per interval. A
                // first scan, a stale snapshot (safety net), or winning the
                // election all pass. Runs inline (cheap: snapshot + cached alive).
                if !eng.should_scan(&cid, scan_stale_ceiling).await {
                    let holders = eng.live_providers(&cid).clamp(1, 16);
                    let next = (delay * 2).max(recheck_min * holders).min(recheck_max);
                    q.lock().expect("q").push(std::cmp::Reverse((
                        std::time::Instant::now() + next,
                        cid,
                        next,
                    )));
                    continue;
                }
                // Hand the WORK to the coordinator — the single job manager (bounded concurrency,
                // dedup, retries, stats). The delay-queue only SCHEDULES *when* each cid is due;
                // the scan itself is now a coordinator job, visible in job stats like distribute
                // and re-announce. Deduped by cid so a slow scan can't stack.
                let (e2, s2, q2, seen2) = (eng.clone(), st.clone(), q.clone(), seen.clone());
                let submitted = coord.submit(
                    format!("scan:{}", cid.to_hex()),
                    zeph_sched::Priority::HealthScan,
                    1,
                    move || {
                        let (eng, st, q, seen) =
                            (e2.clone(), s2.clone(), q2.clone(), seen2.clone());
                        async move {
                            let r = eng.health_scan_chunk(&[cid]).await;
                            st.set_scan(
                                r.scanned,
                                r.at_risk,
                                r.repaired as u64,
                                r.degraded as u64,
                                r.fading,
                                r.offloaded as u64,
                                r.surplus,
                            )
                            .await;
                            if r.moved > 0 {
                                st.set_flow(r.moved as u64, 0).await;
                            }
                            if r.repaired > 0 || r.degraded > 0 || r.offloaded > 0 {
                                tracing::info!(
                                    at_risk = r.at_risk,
                                    repaired = r.repaired,
                                    degraded = r.degraded,
                                    offloaded = r.offloaded,
                                    "health scan: repair / degrade / offload"
                                );
                            }
                            // Re-enqueue for the next check if still held; else stop tracking it.
                            if eng.store().piece_count(&cid) > 0 || eng.store().has_content(&cid) {
                                // Adaptive backoff: an at-risk/converging cid stays HOT
                                // (recheck_min) — repair fires only when the epoch winner
                                // scans, so slowing any node's at-risk cadence slows healing.
                                // A HEALTHY cid backs off PROVIDER-AWARE: every holder runs
                                // its own clock, so effective check rate = holders x per-node
                                // rate — a well-replicated cid skips the early backoff rungs
                                // (post-restart scan storms live there) while its effective
                                // cluster-wide interval stays ~recheck_min.
                                let next = if eng.converging(&cid) {
                                    recheck_min
                                } else {
                                    let holders = eng.live_providers(&cid).clamp(1, 16);
                                    (delay * 2).max(recheck_min * holders).min(recheck_max)
                                };
                                q.lock().expect("q").push(std::cmp::Reverse((
                                    std::time::Instant::now() + next,
                                    cid,
                                    next,
                                )));
                            } else {
                                seen.lock().expect("seen").remove(&cid);
                            }
                            Ok(())
                        }
                    },
                );
                // Already in-flight (deduped) — reschedule so the cid is never dropped.
                if !submitted {
                    q.lock().expect("q").push(std::cmp::Reverse((
                        std::time::Instant::now() + delay,
                        cid,
                        delay,
                    )));
                }
            }
        });
    }

    // Scale (demand-map drain) + quota enforcement on a steady tick — both are
    // cheap no-ops when idle. The census-gated distribute() SWEEP that lived here
    // is DELETED (Transfer Plane v2 S3): membership-change rebalancing now rides
    // each cid's scan (`rebalance_cid`, lazy, paced, zero extra lookups) — the
    // sweep held coordinator slots for 30-44s per node on every census change and
    // re-ballooned rejoining nodes (the measured self-sustaining loop). This also
    // retires the fired-before-submit dedup bug the harness review found.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(health_scan_secs));
        interval.tick().await; // skip immediate tick at startup
        loop {
            interval.tick().await;
            let (e, st) = (health_engine.clone(), health_state.clone());
            health_jobs.submit(
                "scale_quota",
                zeph_sched::Priority::Distribution,
                1,
                move || {
                    let (e, st) = (e.clone(), st.clone());
                    async move {
                        let sc = e.scale().await;
                        e.enforce_quota().await;
                        st.set_flow(0, sc.scaled as u64).await;
                        if sc.scaled > 0 {
                            tracing::info!(scaled = sc.scaled, "demand scale");
                        }
                        Ok(())
                    }
                },
            );
        }
    });

    // Build the dashboard content list from the LOCAL store (curated pins/wants/bans +
    // hosted count) + this node's relationship (held pieces / pin / want / floor).
    let content_state = state.clone();
    let content_store = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        // Resolved names for curated cids we can't decode locally (too few pieces)
        // — fetched once from the network, then cached so the tab stays named.
        let mut name_cache: std::collections::HashMap<
            [u8; 32],
            (String, u64, bool, Option<String>),
        > = std::collections::HashMap::new();
        loop {
            interval.tick().await;
            // A DHT has no global enumeration: the content list is LOCAL knowledge
            // only — cids we hold, want, or have banned. New cids enter it by
            // fetch/pin/want (discover-by-cid), never by listing the network.
            // The content list is the user's CURATED set: cids they explicitly
            // PINNED, WANTED, or BANNED. Pieces the node merely HOSTS for the
            // network (parked by Distribution, not user-chosen) are COUNTED
            // separately (`hosting_cids`) — never itemized here.
            let banned: std::collections::HashSet<_> =
                content_store.tombstoned_cids().into_iter().collect();
            let wanted: std::collections::HashSet<_> =
                content_store.wanted_cids().into_iter().collect();
            let mut curated: std::collections::HashSet<_> = wanted.iter().copied().collect();
            curated.extend(banned.iter().copied());
            let mut hosting = 0u64;
            for cid in content_store.cids() {
                if content_store.is_system(&cid) {
                    continue;
                }
                if content_store.is_pinned(&cid) {
                    curated.insert(cid);
                } else if !wanted.contains(&cid) && !banned.contains(&cid) {
                    hosting += 1; // held for the network, not user-curated
                }
            }
            content_state
                .hosting_cids
                .store(hosting, std::sync::atomic::Ordering::Relaxed);
            curated.retain(|cid| !content_store.is_system(cid));
            // A content/ciphertext object backing a curated manifest/envelope is
            // PART of that file — hide it so the file shows as ONE named entry.
            // Banned cids always stay (ban must be reversible).
            let mut referenced = std::collections::HashSet::new();
            for cid in &curated {
                for child in content_state.engine.referenced_objects(cid) {
                    referenced.insert(child.0);
                }
            }
            curated.retain(|cid| banned.contains(cid) || !referenced.contains(&cid.0));
            let mut out = Vec::new();
            let mut fetches = 0u32; // cap network name-resolves per pass
            for cid in curated {
                let mut e = control::ContentInfo {
                    cid: cid.to_hex(),
                    providers: 0,
                    pinned: 0,
                    pieces: 0,
                    wants: 0,
                    k: 0,
                    floor: 0,
                    local_pieces: content_store.piece_count(&cid),
                    local_pinned: content_store.is_pinned(&cid),
                    local_wanted: content_store.is_wanted(&cid),
                    local_tombstoned: banned.contains(&cid),
                    name: None,
                    size: 0,
                    is_dir: false,
                    mime: None,
                    published_at: None,
                    publisher: None,
                    comment: None,
                };
                let mut big = false;
                if let Some(gen) = content_store.generation(&cid) {
                    e.k = gen.k as usize;
                    e.floor = zeph_obj::floor_for_k(gen.k as usize);
                    big = gen.total_len > 64 * 1024;
                }
                // Resolve name/size/type/mime, best-first: (1) decode from local
                // pieces; else (2) a cached name; else (3) fetch the manifest from
                // the network ONCE (capped per pass) and cache it. Works for BANNED
                // cids too — fetch_manifest reads the label in-memory (Drop), it
                // does not re-host the content.
                if !big && !name_cache.contains_key(&cid.0) {
                    let m = content_state
                        .engine
                        .decode_local(&cid)
                        .and_then(|b| zeph_obj::Manifest::decode(&b));
                    let m = match m {
                        Some(m) => Some(m),
                        None if fetches < 4 => {
                            fetches += 1;
                            content_state.engine.fetch_manifest(cid).await.ok()
                        }
                        None => None,
                    };
                    if let Some(m) = m {
                        let mime = match &m {
                            zeph_obj::Manifest::File { mime, .. } => Some(mime.clone()),
                            _ => None,
                        };
                        name_cache
                            .insert(cid.0, (m.name().to_string(), m.size(), m.is_dir(), mime));
                    }
                }
                if let Some((n, s, dir, mime)) = name_cache.get(&cid.0) {
                    e.name = Some(n.clone());
                    e.size = *s;
                    e.is_dir = *dir;
                    e.mime = mime.clone();
                }
                out.push(e);
            }
            content_state.set_content(out).await;
        }
    });

    // Dashboard health view: per-cid diagnostics (verdict, held vs effective pieces, floor, last
    // scan, next scan from the scheduler queue) for ALL held cids, so any durability issue is
    // visible at a glance and grouped by condition in the UI.
    let hv_engine = engine.clone();
    let hv_state = state.clone();
    let hv_queue = hs_queue.clone();
    let hv_clock = transport.clock();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_secs(5));
        loop {
            iv.tick().await;
            let due_now = std::time::Instant::now();
            let due_map: std::collections::HashMap<[u8; 32], std::time::Instant> = {
                let q = hv_queue.lock().expect("q");
                q.iter()
                    .map(|std::cmp::Reverse((d, c, _))| (c.0, *d))
                    .collect()
            };
            let due_now_count = due_map.values().filter(|d| **d <= due_now).count();
            hv_state.set_scan_queue(due_map.len(), due_now_count);
            let now_ms = hv_clock.now().millis();
            let store = hv_engine.store();
            let mut rows = Vec::new();
            for cid in store.cids() {
                let h = hv_engine.cid_health(&cid);
                let (eff, floor, lprov, last_ms, decision, action) = h
                    .as_ref()
                    .map(|h| {
                        (
                            h.effective,
                            h.floor,
                            h.live_providers,
                            h.last_scan_ms,
                            h.decision.clone(),
                            h.action.clone(),
                        )
                    })
                    .unwrap_or((0, 0, 0, 0, String::new(), String::new()));
                let verdict = if hv_engine.is_at_risk(&cid) {
                    "at-risk"
                } else if hv_engine.is_fading(&cid) {
                    "fading"
                } else if last_ms == 0 {
                    "pending"
                } else if floor > 0 && eff > floor + (floor / 8).max(2) {
                    "surplus"
                } else {
                    "durable"
                };
                let scanned_ago = (last_ms != 0).then(|| now_ms.saturating_sub(last_ms) / 1000);
                let next_secs = due_map
                    .get(&cid.0)
                    .map(|d| d.saturating_duration_since(due_now).as_secs());
                rows.push(serde_json::json!({
                    "cid": cid.to_hex(),
                    "verdict": verdict,
                    "held": store.piece_count(&cid),
                    "effective": eff,
                    "floor": floor,
                    "live_providers": lprov,
                    "pinned": store.is_pinned(&cid),
                    "wanted": store.is_wanted(&cid),
                    "scanned_ago_s": scanned_ago,
                    "next_scan_s": next_secs,
                    "decision": decision,
                    "action": action,
                }));
            }
            hv_state.set_cid_health(rows).await;
        }
    });

    // Sync membership + storage state into the control API every second.
    let sync_state = state.clone();
    let sync_membership = membership.clone();
    let sync_store = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let stats = sync_store.stats();
            sync_state.set_providing(stats.cids as u64);
            sync_state.set_storage(stats).await;
            let snap = sync_membership.snapshot().await;
            let mut table: Vec<control::PeerStatus> = Vec::new();
            for (id, st) in snap.active.iter().chain(snap.dead.iter()) {
                table.push(control::PeerStatus {
                    id: id.to_hex(),
                    addrs: st.addr.to_string(),
                    alive: st.alive,
                    rtt_us: st.rtt_us,
                    skew_ms: st.skew_ms,
                    last_seen_unix: st.last_seen_unix,
                    consecutive_failures: st.consecutive_failures,
                });
            }
            table.sort_by(|a, b| (!a.alive).cmp(&!b.alive).then(a.id.cmp(&b.id)));
            let census = sync_membership.census().await.len() as u32;
            sync_state
                .set_peers(table, snap.passive_count as u32, census)
                .await;
        }
    });

    // Wait for SIGINT (ctrl-c) or SIGTERM (systemctl stop) — the latter is how deploys restart us.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    // Persist the DHT record store so the node comes back with it intact, not empty.
    if let Some(dht) = &dht_node {
        match dht.save_records(&dht_records_path) {
            Ok(n) => tracing::info!(saved = n, "dht: persisted record store on shutdown"),
            Err(e) => tracing::warn!(error = %e, "dht: shutdown save failed"),
        }
        match dht.save_table(&dht_table_path) {
            Ok(n) => tracing::info!(saved = n, "dht: persisted routing table on shutdown"),
            Err(e) => tracing::warn!(error = %e, "dht: routing table shutdown save failed"),
        }
    }
    events.publish(zeph_events::Event::Shutdown);
    transport.close().await;
    Ok(())
}
