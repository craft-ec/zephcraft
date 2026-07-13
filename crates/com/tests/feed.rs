//! CraftCOM phase 5 GATE — the federated mini-feed. Two participants (A, B) each
//! run the SAME feed app to write a post to their OWN `app.feed` namespace
//! (sovereign, sandboxed). A third node (C) runs the app's `aggregate` function,
//! which reads ACROSS both participants' feeds (cross-node) and merges — proving the
//! sovereign-app model with NO shared writer and NO protocol consensus.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_com::{
    serve_invocations, AppBackend, CraftBackend, InvokeRequest, InvokeService, TransitionRuntime,
};
use zeph_core::{hlc::Clock, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_sql::{CraftSql, ObjDurable, TransportPageSource};
use zeph_store::Store;
use zeph_testkit::{MemHeads, MemNet};
use zeph_transport::{Reach, Transport};

// The feed app: `post` writes a row to the caller's OWN feed; `aggregate` reads a
// list of participants (input = concatenated 32-byte ids) and counts how many have a
// non-empty feed — the deterministic cross-user merge.
const FEED_WAT: &[u8] = br#"(module
  (import "craftcom" "sql_execute" (func $exec (param i32 i32) (result i64)))
  (import "craftcom" "sql_query" (func $query (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "craftcom" "input" (func $input (param i32 i32) (result i32)))
  (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
  (memory (export "memory") 4)
  (data (i32.const 0)   "CREATE TABLE IF NOT EXISTS posts(body TEXT)")
  (data (i32.const 128) "INSERT INTO posts VALUES('hi')")
  (data (i32.const 256) "SELECT body FROM posts")
  (func (export "post")
    (drop (call $exec (i32.const 0)   (i32.const 43)))
    (drop (call $exec (i32.const 128) (i32.const 30))))
  (func (export "aggregate")
    (local $a i32) (local $b i32)
    (drop (call $input (i32.const 1024) (i32.const 64)))
    (local.set $a (call $query (i32.const 1024) (i32.const 32) (i32.const 256) (i32.const 22)
                                (i32.const 2048) (i32.const 1000)))
    (local.set $b (call $query (i32.const 1056) (i32.const 32) (i32.const 256) (i32.const 22)
                                (i32.const 3072) (i32.const 1000)))
    ;; commit a single byte: how many of the two feeds were non-empty (0/1/2).
    (i32.store8 (i32.const 5000)
      (i32.add
        (i32.gt_s (local.get $a) (i32.const 2))
        (i32.gt_s (local.get $b) (i32.const 2))))
    (drop (call $commit (i32.const 5000) (i32.const 1)))))"#;

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

/// A node with a live CraftCOM invocation service over its own CraftBackend.
struct Node {
    node_id: NodeId,
    engine: Arc<ObjEngine>,
    service: Arc<InvokeService>,
}

async fn node(tracker: &MemNet, dir: &Path, heads: &MemHeads) -> Node {
    let id = Arc::new(NodeIdentity::generate());
    let node_id = id.node_id();
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_transport::MUX_ALPN.to_vec(), zeph_obj::ALPN.to_vec()],
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
    let craftsql = Arc::new(
        CraftSql::register(&sql_dir, heads.root_store(node_id), node_id)
            .unwrap()
            .with_source(Arc::new(TransportPageSource::new(
                t.clone(),
                Arc::new(tracker.peers()),
            )))
            .with_durable(Arc::new(ObjDurable::new(engine.clone()))),
    );
    let (obj_tx, obj_rx) = mpsc::channel(64);
    let (sql_tx, sql_rx) = mpsc::channel(64);
    let (invoke_tx, invoke_rx) = mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move {
        st.serve(vec![
            (zeph_transport::tag::PIECE, obj_tx),
            (zeph_transport::tag::SQLPAGE, sql_tx),
            (zeph_transport::tag::INVOKE, invoke_tx),
        ])
        .await
    });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(obj_rx).await });
    let sdir = sql_dir.clone();
    tokio::spawn(async move { zeph_sql::serve_pages(sdir, sql_rx).await });
    routing.announce_node(0, 0).await.unwrap();
    let backend: Arc<dyn AppBackend> = Arc::new(CraftBackend::new(
        craftsql,
        engine.clone(),
        Arc::new(Clock::new()),
    ));
    let service = Arc::new(InvokeService::new(
        TransitionRuntime::new().unwrap(),
        engine.clone(),
        backend,
        None,
        None,
    ));
    tokio::spawn(serve_invocations(invoke_rx, service.clone()));
    Node {
        node_id,
        engine,
        service,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn federated_feed_aggregates_across_sovereign_participants() {
    let tracker = start_tracker();
    let heads = MemHeads::new();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = node(&tracker, dirs[0].path(), &heads).await;
    let b = node(&tracker, dirs[1].path(), &heads).await;
    let c = node(&tracker, dirs[2].path(), &heads).await; // observer / aggregator

    // The app is published once; every node uses the same CID.
    let wasm_cid = a.engine.publish(FEED_WAT, true).await.unwrap().cid.0;

    // A and B each POST to their OWN feed (sovereign, single-writer, sandboxed).
    let post = |ns: &str| InvokeRequest {
        app_ns: ns.into(),
        wasm_cid,
        func: "post".into(),
        input: Vec::new(),
    };
    a.service.invoke(&post("feed"), a.node_id.0).await.unwrap();
    b.service.invoke(&post("feed"), b.node_id.0).await.unwrap();

    // Give the heads a moment to announce so C can resolve them cross-node.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    // C aggregates: read participants A + B (input = their concatenated ids), count
    // how many have a non-empty feed. Cross-node reads, deterministic merge.
    let mut input = Vec::with_capacity(64);
    input.extend_from_slice(&a.node_id.0);
    input.extend_from_slice(&b.node_id.0);
    let agg = InvokeRequest {
        app_ns: "feed".into(),
        wasm_cid,
        func: "aggregate".into(),
        input,
    };
    let out = c.service.invoke(&agg, c.node_id.0).await.unwrap();
    assert_eq!(
        out,
        vec![2],
        "the aggregation read BOTH sovereign feeds cross-node (each non-empty), committed as bytes"
    );
}
