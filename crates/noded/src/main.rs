//! `zeph` — the ZephCraft node daemon (headless; one implementation for all
//! platforms). M1.3 worker skeleton + MU.1 control API.
//!
//! Boot order (foundation §12, skeleton subset): identity → transport →
//! control servers → serve loop → heartbeat.

mod appreg;
mod committee;
mod control;
mod governance;
mod progreg;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use zeph_crypto::Keystore;
use zeph_membership::Membership;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_routing::{ContentRouting, TrackerRouting};
use zeph_store::Store;
use zeph_transport::{alpn, PeerAddr, Reach, Transport};

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

    /// Relay URL (repeatable). REPLACES config.toml relay_urls when given.
    #[arg(long = "relay-url", global = true)]
    relay_urls: Vec<String>,

    /// Do not append n0's public relays as fallback (our mesh only).
    #[arg(long, global = true)]
    no_fallback_relays: bool,

    /// Tracker to announce to / resolve from: <node_id_hex>@<addr>.
    /// Repeatable; REPLACES config.toml trackers when given.
    #[arg(long = "tracker", global = true)]
    trackers: Vec<String>,
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
    /// Relay mesh (foundation §26): our relays first; n0 appended as
    /// fallback unless fallback_relays = false.
    relay_urls: Vec<String>,
    fallback_relays: bool,
    /// Trackers to announce to / resolve from: <node_id_hex>@<addr>.
    trackers: Vec<String>,
    /// Relays this node OPERATES and vouches for — announced into the
    /// tracker's relay registry (foundation §26). Empty = not a relay op.
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
    /// Eviction cooldown: an evicted CID is not refilled for this (seconds). Default 30 days.
    eviction_cooldown_secs: u64,
    /// Health-scan / lifecycle loop interval (seconds). Default 30.
    health_scan_secs: u64,
    /// Provider + CraftSQL-head re-announce interval (seconds). Default 120.
    reannounce_secs: u64,
    /// Governance genesis: governor node-id hexes. Empty = seed 1-of-1 with this node.
    #[serde(default)]
    governance_governors: Vec<String>,
    /// Governance genesis threshold (k). Default 1.
    #[serde(default = "one")]
    governance_threshold: usize,
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
            relay_urls: vec!["https://relay1.zeph.craft.ec".to_string()],
            fallback_relays: true,
            trackers: Vec::new(),
            relay_operator_urls: Vec::new(),
            storage_quota_gib: 10.0,
            peers: Vec::new(),
            erasure_k: 8,
            durability_threshold: 8,
            probe_timeout_secs: 2,
            scale_threshold: 20,
            degrade_threshold: 5,
            fade_grace_secs: 24 * 60 * 60,
            eviction_cooldown_secs: 30 * 24 * 60 * 60,
            health_scan_secs: 30,
            reannounce_secs: 120,
            governance_governors: Vec::new(),
            governance_threshold: 1,
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
    let cli = Cli::parse();
    let data_dir = resolve_data_dir(cli.data_dir)?;

    match cli.command {
        Some(Command::Status) => cmd_status(&data_dir).await,
        Some(Command::Publish {
            file,
            no_pin,
            private,
        }) => cmd_publish(&data_dir, &file, !no_pin, private).await,
        Some(Command::Get { cid, output }) => cmd_get(&data_dir, &cid, &output).await,
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
        Some(Command::Deploy { file, name }) => cmd_deploy(&data_dir, &file, name.as_deref()).await,
        Some(Command::Apps) => cmd_apps(&data_dir).await,
        Some(Command::Programs) => cmd_programs(&data_dir).await,
        Some(Command::Gov) => cmd_gov(&data_dir).await,
        Some(Command::GovPropose {
            add,
            remove,
            threshold,
            set_program,
        }) => cmd_gov_propose(&data_dir, add, remove, threshold, set_program).await,
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

async fn cmd_get(data_dir: &Path, cid: &str, output: &Path) -> anyhow::Result<()> {
    let abs = std::path::absolute(output).unwrap_or_else(|_| output.to_path_buf());
    let params = serde_json::json!({"cid": cid, "output": abs.to_string_lossy()});
    let r = control::query_unix_params(&data_dir.join("zeph.sock"), "get", params).await?;
    let path = r
        .get("path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| output.display().to_string());
    if r.get("is_dir").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!(
            "restored folder → {path} ({} files, cids verified)",
            r.get("files").and_then(|v| v.as_u64()).unwrap_or(0),
        );
    } else if r.get("files").is_some() {
        println!(
            "restored {} → {path} (resolved via tracker, cid verified)",
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
    let value = res.get("value").and_then(|v| v.as_i64()).unwrap_or(-1);
    let fuel = res.get("fuel_used").and_then(|v| v.as_u64()).unwrap_or(0);
    let label = name.or(app).unwrap_or("app");
    println!("app '{label}' returned {value}  (fuel {fuel})");
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
    } else {
        anyhow::bail!("give one of --add/--remove/--threshold/--set-program");
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
        "reach  {}   wire v{}   uptime {}s   peers {}/{} active · {} passive",
        status.reach,
        status.wire_version,
        status.uptime_secs,
        alive,
        status.peers.len(),
        status.passive_peers
    );
    println!("erasure {}", status.erasure);
    println!("hlc    {}.{}", status.hlc_ms, status.hlc_logical);
    println!("relays {}", status.relays);
    println!("trackers {}", status.trackers);
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
    if !args.relay_urls.is_empty() {
        cfg.relay_urls = args.relay_urls;
    }
    if args.no_fallback_relays {
        cfg.fallback_relays = false;
    }
    if !args.trackers.is_empty() {
        cfg.trackers = args.trackers.clone();
    }
    cfg.peers.extend(args.peers);

    let reach = match cfg.reach.as_str() {
        "local" => Reach::LocalOnly,
        "relayed" => Reach::Relayed,
        other => anyhow::bail!("invalid reach `{other}`: expected \"local\" or \"relayed\""),
    };

    // Fail fast on malformed peer addresses.
    let peers: Vec<PeerAddr> = cfg
        .peers
        .iter()
        .map(|s| s.parse().map_err(anyhow::Error::from))
        .collect::<anyhow::Result<_>>()?;

    let relay_urls = cfg
        .relay_urls
        .iter()
        .map(|u| {
            u.parse()
                .map_err(|e| anyhow::anyhow!("relay url `{u}`: {e}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let identity = Arc::new(Keystore::new(data_dir.join("keys")).init_or_load()?);
    let transport = Arc::new(
        Transport::bind_with_relays(
            identity.secret_key_bytes(),
            reach,
            vec![
                alpn::PING.to_vec(),
                zeph_membership::ALPN.to_vec(),
                zeph_obj::ALPN.to_vec(),
                zeph_sql::PAGE_ALPN.to_vec(),
                zeph_com::INVOKE_ALPN.to_vec(),
                zeph_com::ATTEST_ALPN.to_vec(),
                zeph_com::ENDORSE_ALPN.to_vec(),
            ],
            cfg.listen_port,
            relay_urls,
            cfg.fallback_relays,
        )
        .await?,
    );

    // Storage engine: persistent store + tracker routing + obj.
    let store = Arc::new(Store::open(data_dir.join("store"))?);
    let tracker_addrs: Vec<PeerAddr> = cfg.trackers.iter().filter_map(|t| t.parse().ok()).collect();
    let routing = Arc::new(TrackerRouting::new(
        transport.clone(),
        identity.clone(),
        tracker_addrs,
        env!("CARGO_PKG_VERSION").to_string(),
    ));
    let engine = ObjEngine::new(
        transport.clone(),
        store.clone(),
        routing.clone(),
        ObjConfig {
            k: cfg.erasure_k,
            durability_threshold: cfg.durability_threshold,
            capacity_bytes: (cfg.storage_quota_gib * 1024.0 * 1024.0 * 1024.0) as u64,
            probe_timeout: Duration::from_secs(cfg.probe_timeout_secs),
            scale_threshold: cfg.scale_threshold,
            degrade_threshold: cfg.degrade_threshold,
            fade_grace: Duration::from_secs(cfg.fade_grace_secs),
            eviction_cooldown: Duration::from_secs(cfg.eviction_cooldown_secs),
        },
    );
    // The owner's encryption keypair (PRE), derived from the identity seed —
    // enables `publish --private` / private reads (ENCRYPTION_DESIGN.md).
    engine.set_enc_keypair(zeph_cipher::EncKeypair::from_identity_seed(
        &identity.secret_key_bytes(),
    ));

    // CraftSQL: SQLite over content-addressed pages; single-writer head via
    // KIND_ROOT, cross-node page fetch over the transport.
    let sql_dir = data_dir.join("sqlpages");
    let routing_dyn: Arc<dyn zeph_routing::ContentRouting> = routing.clone();
    let sql_heads = Arc::new(zeph_sql::RoutingRootStore::new(routing_dyn.clone()));
    let sql_source = Arc::new(zeph_sql::TransportPageSource::new(
        transport.clone(),
        routing_dyn,
    ));
    let sql_manifests = Arc::new(zeph_sql::RoutingManifestStore::new(routing.clone()));
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
    ));
    let com_service = Arc::new(zeph_com::InvokeService::new(
        zeph_com::Runtime::new()?,
        engine.clone(),
        com_backend,
    ));
    // Attestation service: serve the RegistryProgram to the committee (phase 4d).
    let attest_service = Arc::new(
        zeph_com::AttestService::new(
            zeph_com::AttestedRuntime::new()?,
            engine.clone(),
            identity.clone(),
        )
        .with_native(Arc::new(zeph_com::RegistryProgram)),
    );

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

    // Background job coordinator (foundation §51): the periodic lifecycle +
    // re-announce run THROUGH it — prioritized, deduped (a slow pass can't
    // stack), retried, and metered. Serial for now (concurrency 1); the
    // primitive supports more for future per-item reactive jobs.
    let jobs = zeph_sched::JobCoordinator::new(1);

    // Control state, shared by the heartbeat loop and the control servers.
    // Phase 4c: the durable app-name registry backing (self-attested v1 ramp).
    let appreg_store = std::sync::Arc::new(appreg::AppRegistry::open(
        identity.clone(),
        engine.clone(),
        routing.clone(),
        data_dir,
    ));
    // Program registry: native bootstrap map (program name -> canonical cid), seeded
    // with the app-registry program; governance-authorized.
    let program_store = std::sync::Arc::new(progreg::ProgramRegistryStore::open(data_dir));
    // Governance: the live governor set (seeded 1-of-1 with this node's key by default).
    let gov_governors: Vec<[u8; 32]> = cfg
        .governance_governors
        .iter()
        .filter_map(|h| <[u8; 32]>::try_from(hex::decode(h.trim()).ok()?.as_slice()).ok())
        .collect();
    let governance_store = std::sync::Arc::new(governance::GovernanceStore::open(
        identity.clone(),
        data_dir,
        &gov_governors,
        cfg.governance_threshold,
    ));
    // Phase 4g: the committee-chain store (membership wired once it is up).
    let committee_store = std::sync::Arc::new(committee::CommitteeChainStore::new(
        identity.clone(),
        engine.clone(),
        routing.clone(),
        transport.clone(),
    ));
    let state = Arc::new(control::State {
        clock: transport.clock(),
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
        trackers: if cfg.trackers.is_empty() {
            "none configured".to_string()
        } else {
            format!("{} configured", cfg.trackers.len())
        },
        listen: transport.addr().to_string(),
        started: std::time::Instant::now(),
        engine: engine.clone(),
        peers: tokio::sync::RwLock::new(Vec::new()),
        passive_peers: std::sync::atomic::AtomicU32::new(0),
        storage: tokio::sync::RwLock::new((0, 0, 0, 0)),
        providing: std::sync::atomic::AtomicU64::new(0),
        content: tokio::sync::RwLock::new(Vec::new()),
        health: tokio::sync::RwLock::new((0, 0, 0, 0, 0, 0, 0)),
        craftsql: craftsql.clone(),
        events: events.clone(),
        recent_events: tokio::sync::RwLock::new(std::collections::VecDeque::new()),
        jobs: jobs.clone(),
        event_counts: tokio::sync::RwLock::new(std::collections::BTreeMap::new()),
        hosting_cids: std::sync::atomic::AtomicU64::new(0),
        com: com_service.clone(),
        routing: routing.clone(),
        appreg: appreg_store.clone(),
        committee: committee_store.clone(),
        governance: governance_store.clone(),
        programs: program_store.clone(),
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
                trackers: cfg.trackers.clone(),
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

    // ALPN dispatcher: ping + membership + pieces share the endpoint.
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel(32);
    let (member_tx, member_rx) = tokio::sync::mpsc::channel(32);
    let (piece_tx, piece_rx) = tokio::sync::mpsc::channel(32);
    let (sqlpage_tx, sqlpage_rx) = tokio::sync::mpsc::channel(32);
    let (invoke_tx, invoke_rx) = tokio::sync::mpsc::channel(32);
    let (attest_tx, attest_rx) = tokio::sync::mpsc::channel(32);
    let (endorse_tx, mut endorse_rx) = tokio::sync::mpsc::channel(32);
    let server = transport.clone();
    tokio::spawn(async move {
        server
            .serve(vec![
                (alpn::PING.to_vec(), ping_tx),
                (zeph_membership::ALPN.to_vec(), member_tx),
                (zeph_obj::ALPN.to_vec(), piece_tx),
                (zeph_sql::PAGE_ALPN.to_vec(), sqlpage_tx),
                (zeph_com::INVOKE_ALPN.to_vec(), invoke_tx),
                (zeph_com::ATTEST_ALPN.to_vec(), attest_tx),
                (zeph_com::ENDORSE_ALPN.to_vec(), endorse_tx),
            ])
            .await
    });
    tokio::spawn(engine.clone().serve(piece_rx));
    tokio::spawn(zeph_sql::serve_pages(sql_dir.clone(), sqlpage_rx));
    tokio::spawn(zeph_com::serve_invocations(invoke_rx, com_service.clone()));
    tokio::spawn(zeph_com::serve_attestations(
        attest_rx,
        attest_service.clone(),
    ));
    // Serve committee-endorsement requests (epoch rollover).
    let endorse_store = committee_store.clone();
    tokio::spawn(async move {
        while let Some(conn) = endorse_rx.recv().await {
            let store = endorse_store.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(64 * 1024).await else {
                        break;
                    };
                    let reply = match postcard::from_bytes::<zeph_com::EndorseRequest>(&bytes) {
                        Ok(req) => store.endorse(&req).await,
                        Err(_) => None,
                    };
                    let _ = send
                        .write_all(&postcard::to_allocvec(&reply).unwrap_or_default())
                        .await;
                    let _ = send.finish();
                }
            });
        }
    });
    // Committee-chain tick loop: genesis bootstrap + epoch rollover.
    let tick_store = committee_store.clone();
    let tick_clock = transport.clock();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            iv.tick().await;
            tick_store.tick(tick_clock.now().millis()).await;
        }
    });

    // Announce this node into the tracker's node registry (map/census),
    // immediately and periodically.
    // Announce the node into the registry AND re-announce provider records
    // for everything we hold (pins + pieces), immediately (first tick) and
    // periodically — so held content stays discoverable across restart,
    // churn, and tracker restart. Interval well inside the provider TTL and
    // short enough to recover quickly from a tracker restart.
    let announce_engine = engine.clone();
    let announce_relays = cfg.relay_operator_urls.clone();
    let announce_sql = craftsql.clone();
    let announce_jobs = jobs.clone();
    let reannounce_secs = cfg.reannounce_secs.max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(reannounce_secs));
        loop {
            interval.tick().await;
            let e = announce_engine.clone();
            let relays = announce_relays.clone();
            let sql = announce_sql.clone();
            // Distribution priority: getting records to peers matters, but yields
            // to Repair. Deduped so a slow re-announce can't stack.
            announce_jobs.submit(
                "reannounce",
                zeph_sched::Priority::Distribution,
                1,
                move || {
                    let e = e.clone();
                    let relays = relays.clone();
                    let sql = sql.clone();
                    async move {
                        let _ = e.announce_node().await;
                        for relay in &relays {
                            let _ = e.announce_relay(relay.clone()).await;
                        }
                        let n = e.reannounce_providers().await;
                        if n > 0 {
                            tracing::info!(cids = n, "re-announced provider records");
                        }
                        // Re-publish owned DB heads + manifests (lost on tracker
                        // restart otherwise). First tick fires immediately, so this
                        // also restores heads right after our own restart.
                        let h = sql.reannounce_heads().await;
                        if h > 0 {
                            tracing::info!(dbs = h, "re-announced CraftSQL heads/manifests");
                        }
                        Ok(())
                    }
                },
            );
        }
    });
    let ping_clock = transport.clock();
    tokio::spawn(async move {
        while let Some(conn) = ping_rx.recv().await {
            tokio::spawn(Transport::handle_ping_conn(ping_clock.clone(), conn));
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
    membership.start(peers, member_rx);
    // Wire the registry's live committee coordinator now that membership is up.
    appreg_store
        .set_coordinator(transport.clone(), membership.clone())
        .await;
    committee_store.set_membership(membership.clone()).await;
    appreg_store.set_programs(program_store.clone()).await;

    // Discover peers from the tracker's node registry and seed membership —
    // a node bootstraps from the network without any hardcoded peer.
    let seed_membership = membership.clone();
    let seed_routing = routing.clone();
    let me_id = transport.node_id();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            if let Ok(nodes) = seed_routing.nodes().await {
                let candidates: Vec<PeerAddr> = nodes
                    .into_iter()
                    .filter(|(id, _)| *id != me_id)
                    .filter_map(|(_, np)| np.addr.parse().ok())
                    .collect();
                if !candidates.is_empty() {
                    seed_membership.seed(candidates).await;
                }
            }
        }
    });

    // HealthScan: periodically verify availability of held content and repair
    // (the self-healing control loop). Runs every epoch (30s).
    let health_engine = engine.clone();
    let health_state = state.clone();
    let health_jobs = jobs.clone();
    let health_scan_secs = cfg.health_scan_secs.max(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(health_scan_secs));
        interval.tick().await; // skip immediate tick at startup
        loop {
            interval.tick().await;
            let e = health_engine.clone();
            let st = health_state.clone();
            // The self-healing pass runs THROUGH the coordinator: HealthScan
            // priority, deduped so a pass that runs long can't stack on the next
            // tick. (Runs as one unit to preserve the proven scan→distribute→
            // scale→evict order; split into per-priority jobs when scale needs it.)
            health_jobs.submit(
                "lifecycle",
                zeph_sched::Priority::HealthScan,
                1,
                move || {
                    let e = e.clone();
                    let st = st.clone();
                    async move {
                        let r = e.health_scan().await;
                        let d = e.distribute().await;
                        let sc = e.scale().await;
                        e.enforce_quota().await;
                        st.set_health(
                            r.scanned,
                            r.at_risk,
                            r.repaired as u64,
                            d.moved as u64,
                            sc.scaled as u64,
                            r.degraded as u64,
                            r.fading,
                        )
                        .await;
                        if r.repaired > 0 || d.moved > 0 || sc.scaled > 0 || r.degraded > 0 {
                            tracing::info!(
                                at_risk = r.at_risk,
                                repaired = r.repaired,
                                moved = d.moved,
                                scaled = sc.scaled,
                                degraded = r.degraded,
                                "lifecycle: repair / distribute / scale / degrade"
                            );
                        }
                        Ok(())
                    }
                },
            );
        }
    });

    // Poll the tracker for the network's content list + overlay THIS node's
    // relationship (held pieces / pin / want / floor) for the dashboard.
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
            sync_state.set_peers(table, snap.passive_count as u32).await;
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    events.publish(zeph_events::Event::Shutdown);
    transport.close().await;
    Ok(())
}
