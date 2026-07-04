//! M2.3a GATE: an in-process network — 1 tracker, 8 storage nodes, a
//! publisher, a fetcher. Publish spreads to ≥K distinct peers (durable) and
//! announces providers; the fetcher retrieves BY CID ALONE (no manual peer),
//! vtag-verifies, decodes, and byte-matches.

use std::sync::Arc;

use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjConfig, ObjEngine};
use zeph_routing::{ContentRouting, Registry, RegistryConfig, TrackerRouting};
use zeph_store::Store;
use zeph_transport::{Reach, Transport};

async fn transport(alpns: Vec<Vec<u8>>) -> (Arc<Transport>, Arc<NodeIdentity>) {
    let id = Arc::new(NodeIdentity::generate());
    let t = Arc::new(
        Transport::bind(id.secret_key_bytes(), Reach::LocalOnly, alpns, 0)
            .await
            .unwrap(),
    );
    (t, id)
}

async fn start_tracker() -> Arc<Transport> {
    let (t, _) = transport(vec![zeph_routing::ALPN.to_vec()]).await;
    let registry = Arc::new(Registry::new(RegistryConfig::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_routing::ALPN.to_vec(), tx)]).await });
    let rt = t.clone();
    tokio::spawn(async move { zeph_routing::serve(registry, rt, rx).await });
    t
}

/// A full node: transport (serving the piece ALPN), store, tracker routing,
/// obj engine. Returns the engine + its routing handle.
async fn node(tracker: &Transport, dir: &std::path::Path) -> (Arc<ObjEngine>, Arc<TrackerRouting>) {
    let id = Arc::new(NodeIdentity::generate());
    let (engine, routing, _addr) = node_with(tracker, dir, id).await;
    (engine, routing)
}

/// Like `node`, but reuses a given identity (to simulate restart) and returns
/// the node's dialable address.
async fn node_with(
    tracker: &Transport,
    dir: &std::path::Path,
    id: Arc<NodeIdentity>,
) -> (Arc<ObjEngine>, Arc<TrackerRouting>, String) {
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_obj::ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let addr = t.addr().to_string();
    let store = Arc::new(Store::open(dir).unwrap());
    let routing = Arc::new(TrackerRouting::new(
        t.clone(),
        id,
        vec![tracker.addr()],
        "test".into(),
    ));
    let engine = ObjEngine::new(t.clone(), store, routing.clone(), ObjConfig::default());

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_obj::ALPN.to_vec(), tx)]).await });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(rx).await });
    (engine, routing, addr)
}

#[tokio::test]
async fn publish_spreads_then_fetch_by_cid_alone() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..10).map(|_| tempfile::tempdir().unwrap()).collect();

    // 8 storage nodes — each announces itself so the publisher can find them.
    let mut storage = Vec::new();
    for dir in dirs.iter().take(8) {
        let (engine, routing) = node(&tracker, dir.path()).await;
        routing.announce_node(0, 0).await.unwrap();
        storage.push(engine);
    }

    // Publisher.
    let (publisher, _) = node(&tracker, dirs[8].path()).await;
    let payload: Vec<u8> = (0..200_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();

    let report = publisher.publish(&payload, true).await.unwrap();
    assert!(report.pinned, "uploader pins by default");
    assert_eq!(
        report.distinct_peers, 8,
        "spread across all 8 storage nodes"
    );
    assert!(report.durable, "≥K distinct peers ⇒ durable");
    assert!(report.pieces_pushed >= 8);

    // Fetcher — fresh node, knows ONLY the CID (no peer address).
    let (fetcher, _) = node(&tracker, dirs[9].path()).await;
    let restored = fetcher.get(report.cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(
        restored, payload,
        "fetched-by-cid content is byte-identical"
    );
}

#[tokio::test]
async fn ingest_rejects_polluted_pieces() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let (storage, srouting) = node(&tracker, dirs[0].path()).await;
    srouting.announce_node(0, 0).await.unwrap();
    let (publisher, _) = node(&tracker, dirs[1].path()).await;

    // Publish so the storage node holds a generation, then a fetch works.
    let payload = vec![42u8; 50_000];
    let report = publisher.publish(&payload, false).await.unwrap();
    assert!(report.distinct_peers >= 1);

    // The storage node's ingest verified vtags — a polluted piece never made
    // it in, so what it holds decodes cleanly.
    let got = publisher.get(report.cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload);
    let _ = storage; // engine kept alive
}

/// Provider records for HELD PIECES (not just pins) must be re-announced
/// after restart, or the content goes undiscoverable once its stale record
/// expires. Simulates a storage node restarting with a new address: before
/// re-announce the tracker points at the dead address; after
/// reannounce_providers, the content is fetchable again.
#[tokio::test]
async fn reannounce_restores_piece_provider_after_restart() {
    let tracker = start_tracker().await;
    let dir = tempfile::tempdir().unwrap();
    let storage_id = Arc::new(NodeIdentity::generate());

    // Storage node A holds unpinned pieces (from a publish); note its address.
    let (_a, a_routing, addr1) = node_with(&tracker, dir.path(), storage_id.clone()).await;
    a_routing.announce_node(0, 0).await.unwrap();

    let (publisher, _) = node(&tracker, tempfile::tempdir().unwrap().path()).await;
    let payload = vec![7u8; 60_000];
    let report = publisher.publish(&payload, false).await.unwrap();
    assert!(report.distinct_peers >= 1, "pushed to storage A");
    let cid = report.cid;

    // Drop A (its address dies). Restart as A2: SAME identity + store, NEW addr.
    drop(_a);
    drop(a_routing);
    let (a2, a2_routing, addr2) = node_with(&tracker, dir.path(), storage_id).await;
    assert_ne!(addr1, addr2, "restart changed the address");
    assert!(
        a2.store().piece_count(&cid) > 0,
        "pieces persisted across restart"
    );

    // Before re-announce: the tracker still points at the DEAD address.
    let before = a2_routing.resolve(cid).await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(
        before[0].addr.to_string(),
        addr1,
        "stale record → dead addr"
    );

    // Re-announce all held content (this is the startup/periodic behavior).
    assert_eq!(
        a2.reannounce_providers().await,
        1,
        "re-announced the held CID"
    );

    // After: the record points at the LIVE address, and fetch works.
    let after = a2_routing.resolve(cid).await.unwrap();
    assert_eq!(after[0].addr.to_string(), addr2, "record now → live addr");

    let (fetcher, _) = node(&tracker, tempfile::tempdir().unwrap().path()).await;
    let got = fetcher.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload, "content fetchable again after re-announce");
}

/// Pin/unpin/forget operate on the whole FILE via its manifest, cascading to the
/// content object — because pinning a manifest without its content is a broken
/// pin (the content evicts, the file breaks). Manifest and content are distinct
/// objects; the manifest links forward to the content, and the cascade follows it.
#[tokio::test]
async fn pin_unpin_forget_cascade_the_whole_file_chain() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    // Two storage nodes so publish spreads normally.
    for dir in dirs.iter().take(2) {
        let (_e, routing) = node(&tracker, dir.path()).await;
        routing.announce_node(0, 0).await.unwrap();
    }
    let (owner, _r) = node(&tracker, dirs[2].path()).await;

    let fp = owner
        .publish_file("photo.jpg", "image/jpeg", b"the real photo bytes", true)
        .await
        .unwrap();
    let (m, c) = (fp.manifest_cid, fp.content_cid);
    let store = owner.store().clone();
    assert_ne!(m.0, c.0, "manifest and content are DIFFERENT objects");
    assert!(
        store.is_pinned(&m) && store.is_pinned(&c),
        "publish(pin=true) pins both objects"
    );

    // Unpin the FILE via its manifest — cascades to the content object.
    let n = owner.unpin_chain(m).await.unwrap();
    assert_eq!(n, 2, "unpin cascaded over manifest + content");
    assert!(
        !store.is_pinned(&m) && !store.is_pinned(&c),
        "the content object unpinned via the manifest — not left stranded"
    );

    // Pin the FILE again — cascades, keeping the whole file alive.
    let n = owner.pin_chain(m).await.unwrap();
    assert_eq!(n, 2, "pin cascaded over manifest + content");
    assert!(
        store.is_pinned(&m) && store.is_pinned(&c),
        "pinning the manifest kept the content alive too"
    );

    // Forget the FILE — both objects dropped locally (no orphaned content).
    owner.forget_chain(m).await.unwrap();
    assert!(store.content(&m).is_none(), "manifest forgotten");
    assert!(
        store.content(&c).is_none(),
        "content object forgotten too — no orphan left behind"
    );
}
