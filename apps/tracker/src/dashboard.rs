//! Tracker web dashboard: registry counts, live nodes, relays, top content.
//! Bound to 127.0.0.1 only, token-authed (same posture as the node's).
//! This is also the data source for the future public network map (MU.4).

use std::path::Path;
use std::sync::Arc;

use zeph_routing::Registry;

const DASHBOARD_HTML: &str = include_str!("../../../webui/tracker.html");

/// Load or create the dashboard token (`<data_dir>/control.token`, 0600).
pub fn load_or_create_token(data_dir: &Path) -> anyhow::Result<String> {
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
struct Ctx {
    registry: Arc<Registry>,
    token: Arc<String>,
}

/// Public aggregate-stats endpoint for the landing page: CORS-open, no auth,
/// counts only (no addresses). Bind 127.0.0.1; a reverse proxy exposes it.
pub async fn serve_public(registry: Arc<Registry>, port: u16) -> anyhow::Result<()> {
    use axum::response::IntoResponse;
    use axum::routing::get;

    async fn stats(
        axum::extract::State(reg): axum::extract::State<Arc<Registry>>,
    ) -> axum::response::Response {
        let json = axum::Json(reg.public_stats());
        let mut resp = json.into_response();
        resp.headers_mut().insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            axum::http::HeaderValue::from_static("*"),
        );
        resp
    }

    let app = axum::Router::new()
        .route("/stats", get(stats))
        .route(
            "/",
            get(|| async { "zeph tracker public stats: GET /stats" }),
        )
        .with_state(registry);
    // Bind 0.0.0.0 so the reverse proxy (Docker) can reach it; the host
    // firewall restricts direct access to the proxy networks, and the data is
    // aggregate/public anyway. TLS is terminated at the proxy.
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(public = %format!("http://{addr}/stats"), "public stats endpoint listening");
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn serve(registry: Arc<Registry>, token: String, port: u16) -> anyhow::Result<()> {
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::{Html, IntoResponse};
    use axum::routing::get;

    #[derive(serde::Deserialize)]
    struct TokenParam {
        #[serde(default)]
        token: String,
    }

    async fn index(State(ctx): State<Ctx>) -> Html<String> {
        Html(DASHBOARD_HTML.replace("__TOKEN__", &ctx.token))
    }

    async fn api(State(ctx): State<Ctx>, Query(p): Query<TokenParam>) -> axum::response::Response {
        if p.token != *ctx.token {
            return (StatusCode::UNAUTHORIZED, "invalid token").into_response();
        }
        axum::Json(ctx.registry.snapshot(50)).into_response()
    }

    let ctx = Ctx {
        registry,
        token: Arc::new(token),
    };
    let app = axum::Router::new()
        .route("/", get(index))
        .route("/api/registry", get(api))
        .with_state(ctx);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(dashboard = %format!("http://{addr}"), "tracker dashboard listening");
    axum::serve(listener, app).await?;
    Ok(())
}
