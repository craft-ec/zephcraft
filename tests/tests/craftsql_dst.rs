//! DST: CraftSQL survives node churn.
//!
//! Seeded, multi-round churn over a real transport + tracker cluster. The owner
//! writes a database; its page generations distribute across holders as SYSTEM
//! pieces. Each round we KILL a random holder and spawn a fresh empty one, then
//! run the lifecycle (HealthScan repair + distribute). The invariant: a
//! brand-new node — holding nothing — must rebuild the whole database from the
//! network (resolve head + manifest, reconstruct generations from surviving
//! erasure pieces) and read it back byte-correct, EVERY round. That is "the
//! database survives churn" — data + head + manifest all recovered from name.
//!
//! Heavy (real sockets + SQLite); ignored by default:
//!   cargo test -p zeph-tests dst_craftsql -- --ignored --nocapture

use std::path::Path;
use std::sync::Arc;

use rand::{rngs::StdRng, Rng, SeedableRng};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_sql::{CraftSql, ObjDurable, TransportPageSource};
use zeph_store::Store;
use zeph_testkit::{MemHeads, MemNet, MemRouting};
use zeph_transport::{Reach, Transport};

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

/// A full node: obj engine (erasure/repair) + CraftSQL, serving both the piece
/// ALPN and the SQL page ALPN. Dropping/killing closes the transport (= death).
struct SqlNode {
    id: NodeId,
    engine: Arc<ObjEngine>,
    routing: Arc<MemRouting>,
    transport: Arc<Transport>,
    craftsql: Arc<CraftSql>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SqlNode {
    async fn kill(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.transport.close().await;
    }
}

async fn sql_node(tracker: &MemNet, heads: &MemHeads, dir: &Path) -> SqlNode {
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
        ObjConfig {
            probe_timeout: std::time::Duration::from_millis(200),
            scale_threshold: 3,
            ..ObjConfig::default()
        },
    );
    let sql_dir = dir.join("sqlpages");
    let craftsql = Arc::new(
        CraftSql::register(&sql_dir, heads.root_store(node_id), node_id)
            .unwrap()
            .with_source(Arc::new(TransportPageSource::new(
                t.clone(),
                Arc::new(tracker.peers()),
            )))
            .with_durable(Arc::new(ObjDurable::new(engine.clone())))
            .with_manifests(heads.manifest_store(node_id)),
    );
    let (obj_tx, obj_rx) = tokio::sync::mpsc::channel(64);
    let (sql_tx, sql_rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    let serve = tokio::spawn(async move {
        st.serve(vec![
            (zeph_obj::ALPN.to_vec(), obj_tx),
            (zeph_sql::PAGE_ALPN.to_vec(), sql_tx),
        ])
        .await
    });
    let se = engine.clone();
    let obj_task = tokio::spawn(async move { se.serve(obj_rx).await });
    let sdir = sql_dir.clone();
    let sql_task = tokio::spawn(async move { zeph_sql::serve_pages(sdir, sql_rx).await });
    SqlNode {
        id: node_id,
        engine,
        routing,
        transport: t,
        craftsql,
        tasks: vec![serve, obj_task, sql_task],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn dst_craftsql_survives_churn() {
    let mut rng = StdRng::seed_from_u64(0x5EED_5EE7);
    let tracker = start_tracker();
    let heads = MemHeads::new();
    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    let mut mk = || {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().to_path_buf();
        dirs.push(d);
        p
    };

    // Owner + 5 holders.
    let owner = sql_node(&tracker, &heads, &mk()).await;
    owner.routing.announce_node(0, 0).await.unwrap();
    let owner_id = owner.id;
    let mut holders: Vec<SqlNode> = Vec::new();
    for _ in 0..5 {
        let d = mk();
        let n = sql_node(&tracker, &heads, &d).await;
        n.routing.announce_node(0, 0).await.unwrap();
        holders.push(n);
    }

    // Owner writes the database — generations distribute as system pieces.
    {
        let mut db = owner.craftsql.open("dst").await.unwrap();
        db.write("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'alpha'),(2,'beta'),(3,'gamma');")
            .await
            .unwrap();
    }
    // Spread + repair onto holders.
    for _ in 0..6 {
        for h in &holders {
            h.engine.health_scan().await;
            h.engine.distribute().await;
        }
        owner.engine.health_scan().await;
        owner.engine.distribute().await;
    }

    // Churn: each round kills a holder, spawns a fresh one, repairs, then a
    // brand-new node must rebuild + read the DB entirely from the network.
    for round in 0..4 {
        if holders.len() > 3 {
            let i = rng.gen_range(0..holders.len());
            holders.remove(i).kill().await;
        }
        let d = mk();
        let fresh = sql_node(&tracker, &heads, &d).await;
        fresh.routing.announce_node(0, 0).await.unwrap();
        holders.push(fresh);

        for _ in 0..8 {
            for h in &holders {
                h.engine.health_scan().await;
                h.engine.distribute().await;
            }
            owner.engine.health_scan().await;
            owner.engine.distribute().await;
        }

        // INVARIANT: a node holding nothing recovers the DB from name alone.
        let reader = sql_node(&tracker, &heads, &mk()).await;
        reader.routing.announce_node(0, 0).await.unwrap();
        let restored = reader
            .craftsql
            .recover_owner(owner_id, "dst")
            .await
            .unwrap();
        assert!(
            restored > 0,
            "round {round}: reconstructed the DB from surviving erasure pieces"
        );
        let db2 = reader.craftsql.open_reader(owner_id, "dst").await.unwrap();
        let cnt: i64 = db2
            .conn()
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cnt, 3, "round {round}: all rows present after churn");
        let v: String = db2
            .conn()
            .query_row("SELECT v FROM t WHERE id=2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "beta", "round {round}: byte-correct after churn");
        drop(db2);
        reader.kill().await;
    }
}
