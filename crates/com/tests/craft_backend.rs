//! CraftCOM phase 3 GATE — an agent's SQL writes route through the real
//! [`CraftBackend`], persist to CraftSQL, and reload from the committed head.
//!
//! Builds a single real node (transport + obj engine + CraftSQL), wires a
//! `CraftBackend`, runs a WASM agent that CREATEs a table and INSERTs a row via the
//! `sql_execute` host function, then a FRESH query (a new `open`, which re-resolves
//! the committed root) reads the row back — proving persistence, not just in-memory
//! mutation.

use std::path::Path;
use std::sync::Arc;

use zeph_com::{
    AppBackend, CapabilityGrant, CraftBackend, TransitionCtx, TransitionRuntime, DEFAULT_FUEL,
};
use zeph_core::hlc::Clock;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_routing::ContentRouting;
use zeph_sql::{CraftSql, ObjDurable, RoutingManifestStore, RoutingRootStore, TransportPageSource};
use zeph_store::Store;
use zeph_testkit::MemNet;
use zeph_transport::{Reach, Transport};

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

/// A real node: obj engine + CraftSQL serving the piece + page ALPNs. Returns the
/// CraftSQL + engine so a CraftBackend can be built over them.
async fn node(tracker: &MemNet, dir: &Path) -> (Arc<CraftSql>, Arc<ObjEngine>) {
    let id = Arc::new(NodeIdentity::generate());
    let node_id = id.node_id();
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_obj::ALPN.to_vec(), zeph_sql::PAGE_ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir.join("obj")).unwrap());
    let routing = tracker.routing(id.clone(), t.addr());
    let engine = ObjEngine::with_peer_source(
        t.clone(),
        store,
        routing.clone(),
        Arc::new(tracker.peers()),
        ObjConfig::default(),
    );
    let sql_dir = dir.join("sqlpages");
    let routing_dyn: Arc<dyn ContentRouting> = routing.clone();
    let craftsql = Arc::new(
        CraftSql::register(
            &sql_dir,
            Arc::new(RoutingRootStore::new(routing_dyn.clone())),
            node_id,
        )
        .unwrap()
        .with_source(Arc::new(TransportPageSource::new(
            t.clone(),
            Arc::new(tracker.peers()),
        )))
        .with_durable(Arc::new(ObjDurable::new(engine.clone())))
        .with_manifests(Arc::new(RoutingManifestStore::new(routing_dyn.clone()))),
    );
    let (obj_tx, obj_rx) = tokio::sync::mpsc::channel(64);
    let (sql_tx, sql_rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move {
        st.serve(vec![
            (zeph_obj::ALPN.to_vec(), obj_tx),
            (zeph_sql::PAGE_ALPN.to_vec(), sql_tx),
        ])
        .await
    });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(obj_rx).await });
    let sdir = sql_dir.clone();
    tokio::spawn(async move { zeph_sql::serve_pages(sdir, sql_rx).await });
    routing.announce_node(0, 0).await.unwrap();
    (craftsql, engine)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_writes_persist_to_craftsql_and_reload() {
    let tracker = start_tracker();
    let dir = tempfile::tempdir().unwrap();
    let (craftsql, engine) = node(&tracker, dir.path()).await;
    let backend: Arc<dyn AppBackend> =
        Arc::new(CraftBackend::new(craftsql, engine, Arc::new(Clock::new())));

    // Agent: CREATE a table, then INSERT a row — both via `sql_execute`, both
    // confined (structurally) to (own, "app/feed"). The unified ABI: `run()` takes no
    // result and declares its output (the INSERT's rows-affected) via `commit`.
    let rt = TransitionRuntime::new().unwrap();
    let wasm = br#"(module
        (import "craftcom" "sql_execute" (func $exec (param i32 i32) (result i64)))
        (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0)  "CREATE TABLE posts(body TEXT)")
        (data (i32.const 64) "INSERT INTO posts VALUES('x')")
        (func (export "run")
            (drop (call $exec (i32.const 0)  (i32.const 29)))
            (i64.store (i32.const 200) (call $exec (i32.const 64) (i32.const 29)))
            (drop (call $commit (i32.const 200) (i32.const 8)))))"#;
    let ctx = TransitionCtx::new(
        Vec::new(), // apps have no account blob
        Vec::new(),
        [0u8; 32],
        "feed".into(),
        Some(backend.clone()),
    );
    let out = rt
        .run_program(wasm, "run", ctx, DEFAULT_FUEL, &CapabilityGrant::full())
        .await
        .unwrap();
    // The committed bytes are the little-endian rows-affected of the INSERT (>= 0).
    let affected = i64::from_le_bytes(out.as_slice().try_into().unwrap());
    assert!(
        affected >= 0,
        "sql_execute host function did not error (got {affected})"
    );

    // A FRESH query (new open → re-resolves the committed root) reads the row back.
    // This is the real gate: the write PERSISTED, not just mutated memory.
    let got = backend
        .sql_query(None, "feed", "SELECT body FROM posts")
        .await
        .unwrap();
    assert!(
        got.contains('x'),
        "the agent's write persisted + reloaded from the CraftSQL head; got: {got}"
    );
}
