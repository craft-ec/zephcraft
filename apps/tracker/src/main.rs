//! ZephCraft tracker — ContentRouting impl #1 (decision R7).
//!
//! Anyone can run one. Holds three announce-based registries (content
//! providers, nodes, relays), each signed + TTL'd. Multiple trackers
//! multiply the data source; clients union results by node_id.

mod dashboard;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use zeph_crypto::Keystore;
use zeph_routing::{serve, Registry, RegistryConfig};
use zeph_transport::{Reach, Transport};

#[derive(Parser)]
#[command(name = "tracker", version, about = "ZephCraft tracker service")]
struct Cli {
    /// Data directory (identity keystore). Default: ~/.zeph-tracker
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Fixed UDP listen port (0 = OS-assigned).
    #[arg(long, default_value_t = 0)]
    listen_port: u16,
    /// Reachability: "relayed" (WAN) or "local".
    #[arg(long, default_value = "relayed")]
    reach: String,
    /// Dashboard port on 127.0.0.1 (0 disables).
    #[arg(long, default_value_t = 9946)]
    dashboard_port: u16,
    /// Public aggregate-stats port on 127.0.0.1 (0 disables) — for the
    /// landing page, exposed via reverse proxy. Aggregate counts only.
    #[arg(long, default_value_t = 9947)]
    public_stats_port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let data_dir = cli
        .data_dir
        .or_else(|| dirs::home_dir().map(|h| h.join(".zeph-tracker")))
        .ok_or_else(|| anyhow::anyhow!("no data dir; pass --data-dir"))?;
    let reach = if cli.reach == "local" {
        Reach::LocalOnly
    } else {
        Reach::Relayed
    };

    let identity = Keystore::new(data_dir.join("keys")).init_or_load()?;
    let transport = Arc::new(
        Transport::bind(
            identity.secret_key_bytes(),
            reach,
            vec![zeph_routing::ALPN.to_vec()],
            cli.listen_port,
        )
        .await?,
    );

    let registry = Arc::new(Registry::new(RegistryConfig::default()));
    tracing::info!(node_id = %identity.node_id().to_hex(), "tracker started");
    println!("TRACKER_ADDR {}", transport.addr());

    if cli.dashboard_port != 0 {
        let token = dashboard::load_or_create_token(&data_dir)?;
        let reg = registry.clone();
        let port = cli.dashboard_port;
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve(reg, token, port).await {
                tracing::error!(%e, "tracker dashboard failed");
            }
        });
    }
    if cli.public_stats_port != 0 {
        let reg = registry.clone();
        let port = cli.public_stats_port;
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve_public(reg, port).await {
                tracing::error!(%e, "public stats endpoint failed");
            }
        });
    }

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let serve_transport = transport.clone();
    tokio::spawn(async move {
        serve_transport
            .serve(vec![(zeph_routing::ALPN.to_vec(), tx)])
            .await
    });
    serve(registry, transport.clone(), rx).await;
    Ok(())
}
