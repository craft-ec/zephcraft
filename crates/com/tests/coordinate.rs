//! Phase 3b GATE — attestation coordination over the wire. Three committee members
//! each run the SAME deterministic program and return their own signed attestation; a
//! coordinator collects a k-of-n quorum into an `AttestedCommit` that verifies against
//! the epoch committee. No trusted coordinator, no shared secret — the coordinator
//! only gathers independently-signed attestations.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_com::{
    collect_commit, pda, select_committee, serve_attestations, AttestService, AttestedRuntime,
    ATTEST_ALPN,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_store::Store;
use zeph_testkit::MemNet;
use zeph_transport::{Connection, PeerAddr, Reach, Transport};

// Deterministic program: read the first input byte `b`, commit `[b, b*2]`.
const DOUBLE_WAT: &[u8] = br#"(module
  (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
  (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "run")
    (drop (call $input (i32.const 0) (i32.const 64)))
    (i32.store8 (i32.const 100) (i32.load8_u (i32.const 0)))
    (i32.store8 (i32.const 101) (i32.mul (i32.load8_u (i32.const 0)) (i32.const 2)))
    (drop (call $commit (i32.const 100) (i32.const 2)))))"#;

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

struct Member {
    node_id: NodeId,
    addr: PeerAddr,
    transport: Arc<Transport>,
    engine: Arc<ObjEngine>,
}

async fn member(tracker: &MemNet, dir: &Path) -> Member {
    let id = Arc::new(NodeIdentity::generate());
    let node_id = id.node_id();
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_obj::ALPN.to_vec(), ATTEST_ALPN.to_vec()],
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
    let (obj_tx, obj_rx) = mpsc::channel(64);
    let (att_tx, att_rx) = mpsc::channel::<Connection>(64);
    let st = t.clone();
    tokio::spawn(async move {
        st.serve(vec![
            (zeph_obj::ALPN.to_vec(), obj_tx),
            (ATTEST_ALPN.to_vec(), att_tx),
        ])
        .await
    });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(obj_rx).await });
    routing.announce_node(0, 0).await.unwrap();
    let service = Arc::new(AttestService::new(
        AttestedRuntime::new().unwrap(),
        engine.clone(),
        id.clone(),
    ));
    tokio::spawn(serve_attestations(att_rx, service));
    Member {
        node_id,
        addr: t.addr(),
        transport: t,
        engine,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committee_attests_a_commit_over_the_wire() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut members = Vec::new();
    for d in &dirs {
        members.push(member(&tracker, d.path()).await);
    }

    // Each member holds the program locally (same content-addressed CID).
    let mut wasm_cid = [0u8; 32];
    for m in &members {
        wasm_cid = m.engine.publish(DOUBLE_WAT, true).await.unwrap().cid.0;
    }

    // The committee = all three members (n=3, k=2).
    let eligible: Vec<[u8; 32]> = members.iter().map(|m| m.node_id.0).collect();
    let committee = select_committee(&eligible, 1, 3, 2);
    let addrs: Vec<PeerAddr> = members.iter().map(|m| m.addr.clone()).collect();

    // A coordinator (member 0) fans the run out to the committee and collects a quorum.
    let commit = collect_commit(
        &members[0].transport,
        &addrs,
        &committee,
        wasm_cid,
        b"registry".to_vec(),
        [0u8; 32],
        "run",
        vec![10u8],
        Vec::new(), // WASM program: prev_state unused
    )
    .await
    .expect("the committee reached a k-of-n quorum over the wire");

    // ≥k members independently attested the SAME deterministic output, and the
    // collected commit is a valid advance of the program's PDA account.
    assert!(commit.attestations.len() >= 2);
    let adv = committee
        .verify_commit(&commit)
        .expect("the collected commit is a valid PDA advance");
    assert_eq!(adv.new_root, Cid::of(&[10u8, 20]).0);
    assert_eq!(adv.account, pda(&wasm_cid, b"registry"));
}
