//! CraftCOM phase 4 GATE — node B remotely invokes an app on node A as a DISTINCT
//! identity. A runs the agent against ITS OWN state, but knows the caller is B (the
//! QUIC-authenticated peer). Proves: load-WASM-by-CID + run with caller identity +
//! remote invocation over the ALPN.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_com::{
    invoke_remote, serve_invocations, AppBackend, CraftBackend, InvokeRequest, InvokeService,
    Runtime, INVOKE_ALPN,
};
use zeph_core::{hlc::Clock, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_routing::{ContentRouting, Registry, RegistryConfig, TrackerRouting};
use zeph_sql::{CraftSql, ObjDurable, RoutingManifestStore, RoutingRootStore, TransportPageSource};
use zeph_store::Store;
use zeph_transport::{Connection, PeerAddr, Reach, Transport};

async fn start_tracker() -> Arc<Transport> {
    let id = NodeIdentity::generate();
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_routing::ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let registry = Arc::new(Registry::new(RegistryConfig::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_routing::ALPN.to_vec(), tx)]).await });
    let rt = t.clone();
    tokio::spawn(async move { zeph_routing::serve(registry, rt, rx).await });
    t
}

struct Node {
    transport: Arc<Transport>,
    node_id: NodeId,
    craftsql: Arc<CraftSql>,
    engine: Arc<ObjEngine>,
    /// Incoming invocation connections (this node hosting apps for others).
    invoke_rx: mpsc::Receiver<Connection>,
}

async fn node(tracker: &Transport, dir: &Path) -> Node {
    let id = Arc::new(NodeIdentity::generate());
    let node_id = id.node_id();
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![
                zeph_obj::ALPN.to_vec(),
                zeph_sql::PAGE_ALPN.to_vec(),
                INVOKE_ALPN.to_vec(),
            ],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir.join("obj")).unwrap());
    let routing = Arc::new(TrackerRouting::new(
        t.clone(),
        id.clone(),
        vec![tracker.addr()],
        "test".into(),
    ));
    let engine = ObjEngine::new(t.clone(), store, routing.clone(), ObjConfig::default());
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
            routing_dyn.clone(),
        )))
        .with_durable(Arc::new(ObjDurable::new(engine.clone())))
        .with_manifests(Arc::new(RoutingManifestStore::new(routing_dyn.clone()))),
    );
    let (obj_tx, obj_rx) = mpsc::channel(64);
    let (sql_tx, sql_rx) = mpsc::channel(64);
    let (invoke_tx, invoke_rx) = mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move {
        st.serve(vec![
            (zeph_obj::ALPN.to_vec(), obj_tx),
            (zeph_sql::PAGE_ALPN.to_vec(), sql_tx),
            (INVOKE_ALPN.to_vec(), invoke_tx),
        ])
        .await
    });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(obj_rx).await });
    let sdir = sql_dir.clone();
    tokio::spawn(async move { zeph_sql::serve_pages(sdir, sql_rx).await });
    routing.announce_node(0, 0).await.unwrap();
    Node {
        transport: t,
        node_id,
        craftsql,
        engine,
        invoke_rx,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_b_invokes_an_app_on_node_a_as_a_distinct_identity() {
    let tracker = start_tracker().await;
    let da = tempfile::tempdir().unwrap();
    let db = tempfile::tempdir().unwrap();
    let host = node(&tracker, da.path()).await; // A — hosts the app
    let caller = node(&tracker, db.path()).await; // B — invokes it

    // A publishes the app WASM (WAT text; the runtime compiles it) → its CID.
    let guestbook = br#"(module
        (import "craftcom" "sql_execute" (func $exec (param i32 i32) (result i64)))
        (import "craftcom" "caller" (func $who (param i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0)  "CREATE TABLE g(x TEXT)")
        (data (i32.const 64) "INSERT INTO g VALUES('hi')")
        (func (export "run") (result i64)
            (drop (call $exec (i32.const 0)  (i32.const 22)))
            (drop (call $exec (i32.const 64) (i32.const 26)))
            (drop (call $who (i32.const 200) (i32.const 32)))
            (i64.load8_u (i32.const 200))))"#;
    let wasm_cid = host.engine.publish(guestbook, true).await.unwrap().cid.0;

    // A stands up the invocation service (its own CraftBackend) + serves the ALPN.
    let backend: Arc<dyn AppBackend> = Arc::new(CraftBackend::new(
        host.craftsql.clone(),
        host.engine.clone(),
        Arc::new(Clock::new()),
    ));
    let service = Arc::new(InvokeService::new(
        Runtime::new().unwrap(),
        host.engine.clone(),
        backend,
    ));
    tokio::spawn(serve_invocations(host.invoke_rx, service));

    // B invokes the app on A. The agent returns caller[0] — proving A ran it with
    // B's authenticated identity as the caller.
    let req = InvokeRequest {
        app_ns: "guestbook".into(),
        wasm_cid,
        func: "run".into(),
        input: Vec::new(),
    };
    let host_addr: PeerAddr = host.transport.addr();
    let result = invoke_remote(&caller.transport, &host_addr, &req)
        .await
        .unwrap();
    assert_eq!(
        result, caller.node_id.0[0] as i64,
        "A ran the agent with B's identity as the caller"
    );

    // …and the write landed in A's OWN app namespace (remote invocation had effect).
    let got = host
        .craftsql
        .open("app.guestbook")
        .await
        .unwrap()
        .query("SELECT x FROM g")
        .unwrap()
        .to_string();
    assert!(
        got.contains("hi"),
        "the remote invocation mutated A's state: {got}"
    );
}
