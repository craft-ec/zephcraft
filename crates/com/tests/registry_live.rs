//! Phase 4b GATE — the app-name registry, LIVE over the committee. Three committee
//! members each run the native `RegistryProgram`; a coordinator submits a signed head,
//! the committee attests the registry transition over the wire, and the advanced state
//! resolves the registered name. Program-owned (PDA), no keyholder, quorum-attested.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use zeph_com::{
    collect_commit, pda, registry_program_cid, select_committee, serve_attestations, AttestService,
    AttestedRuntime, HeadSubmission, RegistryProgram, RegistryState, ATTEST_ALPN, REGISTRY_SEED,
};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ObjConfig, ObjEngine};
use zeph_store::Store;
use zeph_testkit::MemNet;
use zeph_transport::{Connection, PeerAddr, Reach, Transport};

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

struct Member {
    node_id: NodeId,
    addr: PeerAddr,
    transport: Arc<Transport>,
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
    // Each member registers the native registry program it will attest.
    let service = Arc::new(
        AttestService::new(AttestedRuntime::new().unwrap(), engine.clone(), id.clone())
            .with_native(Arc::new(RegistryProgram)),
    );
    tokio::spawn(serve_attestations(att_rx, service));
    Member {
        node_id,
        addr: t.addr(),
        transport: t,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_advances_live_over_the_committee() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut members = Vec::new();
    for d in &dirs {
        members.push(member(&tracker, d.path()).await);
    }
    let eligible: Vec<[u8; 32]> = members.iter().map(|m| m.node_id.0).collect();
    let committee = select_committee(&eligible, 1, 3, 2);
    let addrs: Vec<PeerAddr> = members.iter().map(|m| m.addr.clone()).collect();

    // Genesis registry state.
    let state = RegistryState::default();
    let prev_root = state.root();

    // A publisher submits a signed head.
    let publisher = NodeIdentity::generate();
    let sub = HeadSubmission::sign(&publisher, "feed", [1u8; 32], 1);

    // The committee runs the native registry transition and attests it over the wire.
    let commit = collect_commit(
        &members[0].transport,
        &addrs,
        &committee,
        registry_program_cid(),
        REGISTRY_SEED.to_vec(),
        prev_root,
        "", // native program: func unused
        sub.encode(),
        state.encode(), // the prior state the agents run the transition on
    )
    .await
    .expect("the committee attested the registry advance over the wire");

    // The advance is a valid move of the registry PDA account...
    let adv = committee
        .verify_commit(&commit)
        .expect("the collected commit advances the registry PDA");
    assert_eq!(adv.account, pda(&registry_program_cid(), REGISTRY_SEED));
    assert_eq!(adv.prev_root, prev_root);

    // ...to exactly the state that resolves the registered name.
    let new_state = state.apply(&sub).unwrap();
    assert_eq!(adv.new_root, new_state.root());
    assert_eq!(
        new_state
            .resolve(&publisher.node_id().0, "feed")
            .unwrap()
            .cid,
        [1u8; 32],
        "the name resolves to the published cid after the live committee advance"
    );
}
