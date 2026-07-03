//! HealthScan GATE: the make-or-break proof that data survives node death.
//! Publish spreads n pieces across holders; we KILL a third of them (verified
//! availability drops below the durability floor); surviving holders run
//! HealthScan, which live-probes providers (dead ones don't answer → not
//! counted), detects the deficit, rendezvous-elects a repairer, and recodes
//! fresh pieces back onto live peers until availability recovers to the floor —
//! all discovered by CID alone, no manual peering.

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

/// A full storage node kept alive by its returned handles. Dropping the tuple
/// kills the node (transport closes) — that is how we simulate death.
struct Node {
    engine: Arc<ObjEngine>,
    routing: Arc<TrackerRouting>,
    transport: Arc<Transport>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Node {
    /// Simulate death: stop serving and close the endpoint so peers' probes
    /// fail fast instead of being answered by lingering spawned tasks.
    async fn kill(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.transport.close().await;
    }
}

async fn node(tracker: &Transport, dir: &std::path::Path) -> Node {
    let id = Arc::new(NodeIdentity::generate());
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
    let store = Arc::new(Store::open(dir).unwrap());
    let routing = Arc::new(TrackerRouting::new(
        t.clone(),
        id,
        vec![tracker.addr()],
        "test".into(),
    ));
    let engine = ObjEngine::new(
        t.clone(),
        store,
        routing.clone(),
        ObjConfig {
            probe_timeout: std::time::Duration::from_millis(200),
            scale_threshold: 3,
            ..ObjConfig::default()
        },
    );
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    let serve_task =
        tokio::spawn(async move { st.serve(vec![(zeph_obj::ALPN.to_vec(), tx)]).await });
    let se = engine.clone();
    let engine_task = tokio::spawn(async move { se.serve(rx).await });
    Node {
        engine,
        routing,
        transport: t,
        tasks: vec![serve_task, engine_task],
    }
}

#[tokio::test]
async fn content_self_heals_after_holder_death() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..8).map(|_| tempfile::tempdir().unwrap()).collect();

    // 5 storage holders announce themselves; publish spreads across them.
    let mut holders = Vec::new();
    for dir in dirs.iter().take(5) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }

    // Publisher (holds nothing itself: pin=false) spreads n pieces.
    let publisher = node(&tracker, dirs[6].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    let report = publisher.engine.publish(&payload, false).await.unwrap();
    let cid = report.cid;
    // Content must be WANTED to be repaired — holding is not implicit want (Fade).
    holders[0].engine.want(cid).await.unwrap();
    let floor = 32usize; // n = target_pieces(K=8)
    assert_eq!(report.distinct_peers, 5, "spread across all 5 holders");
    assert!(report.pieces_pushed >= floor, "pushed the full generation");

    // Sanity: fetchable now.
    let got = publisher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload, "fetchable before churn");

    // KILL 1 of 5 holders (close endpoint). Its provider record lingers on the
    // tracker (stale) — HealthScan must not be fooled by it.
    for dead in holders.split_off(4) {
        dead.kill().await;
    }

    // Verified availability across the 4 survivors is now below the floor.
    let have_after_death = verified_have(&holders, cid).await;
    assert!(
        have_after_death < floor,
        "after death verified HAVE {have_after_death} should be < floor {floor}"
    );

    // Run HealthScan across survivors until availability recovers.
    let mut recovered = 0;
    for _ in 0..60 {
        for h in &holders {
            h.engine.health_scan().await;
        }
        recovered = verified_have(&holders, cid).await;
        if recovered >= floor {
            break;
        }
    }
    assert!(
        recovered >= floor,
        "HealthScan repaired verified HAVE back to the floor: {recovered} >= {floor}"
    );

    // And the content is still fetchable, byte-identical, by CID alone.
    let fetcher = node(&tracker, dirs[7].path()).await;
    let restored = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(
        restored, payload,
        "content survives churn + repair, byte-identical"
    );
}

/// FADE demand dimension: content with recent FETCH activity (within the grace
/// window) stays alive and is repaired — even with no pin and no want. This is
/// the contrast to unwanted_content_fades (same setup, minus the fetch).
#[tokio::test]
async fn recently_fetched_content_is_repaired() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..7).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    let mut holders = Vec::new();
    for dir in dirs.iter().take(5) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[5].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 1) as u8)
        .collect();
    // No pin, no want.
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;

    // A real FETCH — the serving holders record recent access (last_served).
    let fetcher = node(&tracker, dirs[6].path()).await;
    assert_eq!(
        fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap(),
        payload
    );

    // Kill a holder → below the floor.
    for dead in holders.split_off(4) {
        dead.kill().await;
    }
    assert!(verified_have(&holders, cid).await < floor, "below floor");

    // Recently fetched ⇒ alive ⇒ repaired back to the floor (within grace).
    let mut recovered = 0;
    for _ in 0..60 {
        for h in &holders {
            h.engine.health_scan().await;
        }
        recovered = verified_have(&holders, cid).await;
        if recovered >= floor {
            break;
        }
    }
    assert!(
        recovered >= floor,
        "recent fetch kept it alive: {recovered} >= {floor}"
    );
}

/// FADE: content nothing wants (no pin, no want, no demand) is NOT repaired —
/// it is left to erode. Fail-safe and reversible: once a WANT appears, repair
/// resumes. This is what makes "replicate only what matters" real.
#[tokio::test]
async fn unwanted_content_fades_then_want_resumes_repair() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..7).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    let mut holders = Vec::new();
    for dir in dirs.iter().take(5) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[5].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 2) as u8)
        .collect();
    // Published WITHOUT pin, and nobody wants it.
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;

    // Kill a holder → below the floor.
    for dead in holders.split_off(4) {
        dead.kill().await;
    }
    let after_death = verified_have(&holders, cid).await;
    assert!(after_death < floor, "below floor after death");

    // FADE: unwanted → repair does NOT fire. HAVE stays down.
    for _ in 0..20 {
        for h in &holders {
            let r = h.engine.health_scan().await;
            assert_eq!(r.repaired, 0, "unwanted content is not repaired");
        }
    }
    assert!(
        verified_have(&holders, cid).await < floor,
        "unwanted content faded (not restored to floor)"
    );

    // Now someone WANTs it — repair resumes.
    holders[0].engine.want(cid).await.unwrap();
    let mut recovered = 0;
    for _ in 0..60 {
        for h in &holders {
            h.engine.health_scan().await;
        }
        recovered = verified_have(&holders, cid).await;
        if recovered >= floor {
            break;
        }
    }
    assert!(
        recovered >= floor,
        "want resumed repair: {recovered} >= {floor}"
    );
}

/// Pin policy: the distributed floor is maintained EVEN when a pinner exists/// Pin policy: the distributed floor is maintained EVEN when a pinner exists
/// (pin != spread). A pinned CID driven below the floor still gets repaired
/// back up — the pinner participates as a mint source but is not a substitute
/// for spread.
#[tokio::test]
async fn pinned_content_still_repairs_to_floor() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..7).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    let mut holders = Vec::new();
    for dir in dirs.iter().take(5) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[5].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 4) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;

    // One survivor PINS the content (now holds the whole file).
    holders[0].engine.pin(cid).await.unwrap();
    assert!(holders[0].engine.store().is_pinned(&cid), "holder 0 pins");

    // Kill a holder → distributed pieces drop below the floor.
    for dead in holders.split_off(4) {
        dead.kill().await;
    }
    let have0 = verified_have(&holders, cid).await;
    assert!(have0 < floor, "below floor after death: {have0}");

    // Repair still runs despite the pinner, restoring the distributed floor.
    let mut recovered = 0;
    for _ in 0..60 {
        for h in &holders {
            h.engine.health_scan().await;
        }
        recovered = verified_have(&holders, cid).await;
        if recovered >= floor {
            break;
        }
    }
    assert!(
        recovered >= floor,
        "floor maintained under a pin: {recovered} >= {floor}"
    );
}

/// Read-spreading: a single download pulls pieces from MULTIPLE providers/// Read-spreading: a single download pulls pieces from MULTIPLE providers
/// concurrently (not draining one), so download load distributes — the
/// property that makes Scaling self-regulating.
#[tokio::test]
async fn fetch_reads_spread_across_providers() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..7).map(|_| tempfile::tempdir().unwrap()).collect();

    // 4 holders; publish spreads the generation across them (~8 pieces each).
    let mut holders = Vec::new();
    for dir in dirs.iter().take(4) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[4].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 5) as u8)
        .collect();
    let report = publisher.engine.publish(&payload, false).await.unwrap();
    let cid = report.cid;
    assert_eq!(report.distinct_peers, 4, "spread across 4 holders");

    // One download.
    let fetcher = node(&tracker, dirs[5].path()).await;
    let got = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload);

    // The read landed on more than one provider (not a single drained source).
    let served = holders
        .iter()
        .filter(|h| h.engine.served_pulls(&cid) > 0)
        .count();
    assert!(
        served >= 2,
        "download pulled from multiple providers (served by {served})"
    );
}

/// Scaling: DOWNLOAD demand (actual piece-pull requests, NOT the WANT signal)
/// recruits additional providers for bandwidth headroom — and no downloads
/// recruit nobody.
#[tokio::test]
async fn content_scales_under_download_demand() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();

    // One provider holds the whole generation.
    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 7) as u8)
        .collect();
    let report = publisher.engine.publish(&payload, false).await.unwrap();
    let cid = report.cid;
    assert_eq!(report.distinct_peers, 1, "all pieces on the one provider");

    // Two empty nodes join (candidate providers).
    let mut spares = Vec::new();
    for dir in dirs.iter().skip(2).take(2) {
        let n = node(&tracker, dir.path()).await;
        n.routing.announce_node(0, 0).await.unwrap();
        spares.push(n);
    }
    let spare_pieces = |spares: &[Node]| -> usize {
        spares
            .iter()
            .map(|n| n.engine.store().piece_count(&cid))
            .sum()
    };

    // Baseline: NO downloads ⇒ Scaling recruits nobody.
    for _ in 0..3 {
        let r = s0.engine.scale().await;
        assert_eq!(r.scaled, 0, "no demand ⇒ no scaling");
    }
    assert_eq!(
        spare_pieces(&spares),
        0,
        "spares still empty with no demand"
    );

    // Generate DOWNLOAD demand: fetch the content repeatedly (each pulls
    // pieces from s0, incrementing its served-request count).
    let fetcher = node(&tracker, dirs[4].path()).await;
    for _ in 0..4 {
        let got = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
        assert_eq!(got, payload);
    }

    // Now the CID is hot: Scaling recruits a new provider.
    let mut recruited = false;
    for _ in 0..5 {
        s0.engine.scale().await;
        if spare_pieces(&spares) > 0 {
            recruited = true;
            break;
        }
    }
    assert!(
        recruited,
        "download demand recruited an additional provider (Scaling)"
    );
}

/// Degradation: once download demand fades, a CID scaled ABOVE the durability
/// floor sheds its surplus back DOWN to the floor — and stops exactly there,
/// never below (Repair defends the floor).
#[tokio::test]
async fn content_degrades_to_floor_when_demand_fades() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    // One provider holds the whole generation.
    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 3) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;

    // Two empty nodes join.
    let mut nodes = vec![s0];
    for dir in dirs.iter().skip(2).take(2) {
        let n = node(&tracker, dir.path()).await;
        n.routing.announce_node(0, 0).await.unwrap();
        nodes.push(n);
    }
    let total = |nodes: &[Node]| -> usize {
        nodes
            .iter()
            .map(|n| n.engine.store().piece_count(&cid))
            .sum()
    };

    // Drive Scaling with real downloads to create surplus above the floor.
    let fetcher = node(&tracker, dirs[4].path()).await;
    for _ in 0..2 {
        for _ in 0..2 {
            let _ = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
        }
        nodes[0].engine.scale().await;
    }
    let surplus = total(&nodes);
    assert!(
        surplus > floor,
        "scaling created surplus {surplus} > floor {floor}"
    );

    // Demand fades (no more fetches). Degradation sheds back to the floor.
    let mut settled = 0;
    for _ in 0..40 {
        for n in &nodes {
            n.engine.health_scan().await;
        }
        settled = total(&nodes);
        if settled <= floor {
            break;
        }
    }
    assert_eq!(settled, floor, "degraded to the floor exactly, not below");

    // Still fetchable at the floor.
    let got = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(
        got, payload,
        "content intact at the floor after degradation"
    );
}

/// Local reconstruction: a node holding >=k of its own pieces decodes the whole
/// content with NO network fetch — so pin/get succeed instantly and survive a
/// depleted network (the earlier pin-failure cause).
#[tokio::test]
async fn get_reconstructs_from_local_pieces() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..90_000u32)
        .map(|i| (i.wrapping_mul(40503) >> 6) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    assert!(
        s0.engine.store().piece_count(&cid) >= 8,
        "s0 holds >=k pieces"
    );

    // Kill the publisher AND the tracker: no network to fetch from at all.
    publisher.kill().await;
    tracker.close().await;

    // s0 still reconstructs — purely from its own pieces.
    let got = s0.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload, "decoded locally with no reachable network");
}

/// Eviction cooldown: an evicted CID enters a cooldown that BLOCKS the
/// lifecycle from refilling it (anti-thrash) — until a manual want/pin clears
/// the cooldown and re-acquisition is allowed again.
#[tokio::test]
async fn eviction_cooldown_blocks_refill_until_wanted() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

    let s = node(&tracker, dirs[0].path()).await;
    s.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload = vec![7u8; 40_000];
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    assert!(
        s.engine.store().piece_count(&cid) > 0,
        "s holds the content"
    );

    // Evict everything → the CID enters cooldown.
    s.engine.store().evict_to(0).unwrap();
    assert_eq!(s.engine.store().piece_count(&cid), 0, "evicted");
    assert!(
        s.engine
            .store()
            .is_in_cooldown(&cid, std::time::Duration::from_secs(3600)),
        "in cooldown after eviction"
    );

    // Re-publish → the lifecycle tries to refill s, but ingest REJECTS it.
    publisher.engine.publish(&payload, false).await.unwrap();
    assert_eq!(
        s.engine.store().piece_count(&cid),
        0,
        "cooldown blocked the refill"
    );

    // Manual WANT clears the cooldown — now re-acquisition is allowed.
    s.engine.want(cid).await.unwrap();
    publisher.engine.publish(&payload, false).await.unwrap();
    assert!(
        s.engine.store().piece_count(&cid) > 0,
        "want cleared cooldown → refill accepted"
    );
}

/// MANIFEST: publish a named file (content + File manifest), then fetch it
/// back by the manifest CID alone and recover its name, mime, and bytes.
#[tokio::test]
async fn file_manifest_publish_and_fetch_by_name() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();

    let mut holders = Vec::new();
    for dir in dirs.iter().take(4) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[4].path()).await;
    let data: Vec<u8> = (0..50_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 7) as u8)
        .collect();

    let fp = publisher
        .engine
        .publish_file("holiday.jpg", "image/jpeg", &data, false)
        .await
        .unwrap();
    assert_ne!(
        fp.manifest_cid, fp.content_cid,
        "manifest and content are distinct objects"
    );
    assert_eq!(fp.size, data.len() as u64);

    // A fresh node fetches by the manifest CID alone → name/mime/bytes.
    let fetcher = node(&tracker, dirs[5].path()).await;
    let (name, mime, bytes) = fetcher.engine.fetch_file(fp.manifest_cid).await.unwrap();
    assert_eq!(name, "holiday.jpg");
    assert_eq!(mime, "image/jpeg");
    assert_eq!(bytes, data, "file bytes recovered byte-identical");
}

/// FOLDER manifest: a Dir manifest names entries pointing at child (file)
/// manifests; fetching the dir CID reveals the tree, and each child fetches
/// back byte-identical.
#[tokio::test]
async fn folder_manifest_lists_and_fetches_children() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut holders = Vec::new();
    for dir in dirs.iter().take(4) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[4].path()).await;

    // Publish two files, then a folder manifest naming them.
    let a = b"file A contents".to_vec();
    let b = b"file B, different".to_vec();
    let fa = publisher
        .engine
        .publish_file("a.txt", "text/plain", &a, false)
        .await
        .unwrap();
    let fb = publisher
        .engine
        .publish_file("b.txt", "text/plain", &b, false)
        .await
        .unwrap();
    let entries = vec![
        zeph_obj::Entry {
            name: "a.txt".into(),
            size: a.len() as u64,
            is_dir: false,
            cid: fa.manifest_cid.0,
        },
        zeph_obj::Entry {
            name: "b.txt".into(),
            size: b.len() as u64,
            is_dir: false,
            cid: fb.manifest_cid.0,
        },
    ];
    let dir_cid = publisher
        .engine
        .publish_dir("album", entries, false)
        .await
        .unwrap();

    // A fresh node fetches the folder tree by the dir CID alone.
    let fetcher = node(&tracker, dirs[5].path()).await;
    let m = fetcher.engine.fetch_manifest(dir_cid).await.unwrap();
    let zeph_obj::Manifest::Dir { name, entries } = m else {
        panic!("not a dir")
    };
    assert_eq!(name, "album");
    assert_eq!(entries.len(), 2);
    // Each child fetches back by name, byte-identical.
    for (e, want) in entries.iter().zip([&a, &b]) {
        let (n, _mime, bytes) = fetcher
            .engine
            .fetch_file(zeph_core::Cid(e.cid))
            .await
            .unwrap();
        assert_eq!(&bytes, want, "child {n} byte-identical");
    }
}

/// METADATA ENVELOPE (KIND_META): publishing a file auto-attaches an editable
/// envelope; edits preserve published_at and supersede; a second publisher adds
/// its own envelope (multi-writer, min resolves the default); withdrawal removes
/// only that publisher's claim — the manifest CID never changes.
#[tokio::test]
async fn metadata_envelope_publish_edit_multiwriter_withdraw() {
    use zeph_routing::ContentRouting;
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let a = node(&tracker, dirs[0].path()).await;
    a.routing.announce_node(0, 0).await.unwrap();
    let b = node(&tracker, dirs[1].path()).await;
    b.routing.announce_node(0, 0).await.unwrap();

    let data = b"envelope test bytes".to_vec();
    let cid = a
        .engine
        .publish_file("doc.txt", "text/plain", &data, false)
        .await
        .unwrap()
        .manifest_cid;

    let find = |cs: &Vec<zeph_routing::ContentEntry>, cid: zeph_core::Cid| {
        cs.iter().find(|c| c.cid == cid).cloned()
    };

    // Auto-announced envelope on publish.
    let e = find(&a.routing.content().await.unwrap(), cid).expect("cid listed");
    assert_eq!(e.metas.len(), 1, "one envelope after publish");
    let t0 = e.metas[0].published_at;
    assert!(t0 > 0 && e.metas[0].comment.is_none());

    // Edit the comment — published_at is PRESERVED, record superseded.
    a.engine.set_meta(cid, Some("draft".into())).await.unwrap();
    let e = find(&a.routing.content().await.unwrap(), cid).unwrap();
    assert_eq!(e.metas.len(), 1);
    assert_eq!(e.metas[0].comment.as_deref(), Some("draft"));
    assert_eq!(e.metas[0].published_at, t0, "edit preserves published_at");

    // A SECOND publisher attaches its own envelope (multi-writer).
    b.engine.set_meta(cid, Some("mirror".into())).await.unwrap();
    let e = find(&b.routing.content().await.unwrap(), cid).unwrap();
    assert_eq!(e.metas.len(), 2, "two independent envelopes");
    let first = e.metas.iter().map(|m| m.published_at).min().unwrap();
    assert_eq!(
        first, t0,
        "min(published_at) resolves the canonical first-published"
    );

    // A withdraws its envelope — only its claim goes; B's remains.
    a.engine.del_meta(cid).await.unwrap();
    let e = find(&a.routing.content().await.unwrap(), cid).unwrap();
    assert_eq!(e.metas.len(), 1, "only the withdrawing publisher removed");
    assert_eq!(e.metas[0].comment.as_deref(), Some("mirror"));
}

/// KIND_ROOT: the single-writer DB root pointer with compare-and-swap — the
/// CraftSQL head. First write succeeds; a stale CAS (wrong prev) conflicts; a
/// correct CAS advances it; another identity's root is independent; seq can't
/// roll back.
#[tokio::test]
async fn root_pointer_compare_and_swap() {
    use zeph_routing::{ContentRouting, RoutingError};
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let owner = node(&tracker, dirs[0].path()).await;
    let reader = node(&tracker, dirs[1].path()).await;
    let oid = owner.transport.node_id();

    let r1 = zeph_core::Cid::of(b"db-root-v1");
    let r2 = zeph_core::Cid::of(b"db-root-v2");

    // No root yet.
    assert!(reader
        .routing
        .resolve_root(oid, "")
        .await
        .unwrap()
        .is_none());

    // First write: expect no prior root.
    owner.routing.publish_root("", r1, None, 0).await.unwrap();
    let cur = reader.routing.resolve_root(oid, "").await.unwrap().unwrap();
    assert_eq!(cur.root_cid, r1);
    assert_eq!(cur.seq, 0);

    // Stale CAS: still expects "no prior root" though current is r1 → conflict.
    let stale = owner.routing.publish_root("", r2, None, 1).await;
    assert!(
        matches!(stale, Err(RoutingError::Conflict(_))),
        "stale prev rejected"
    );
    assert_eq!(
        reader
            .routing
            .resolve_root(oid, "")
            .await
            .unwrap()
            .unwrap()
            .root_cid,
        r1
    );

    // Correct CAS: expect r1, advance seq → wins.
    owner
        .routing
        .publish_root("", r2, Some(r1), 1)
        .await
        .unwrap();
    assert_eq!(
        reader
            .routing
            .resolve_root(oid, "")
            .await
            .unwrap()
            .unwrap()
            .root_cid,
        r2
    );

    // Anti-rollback: correct prev but non-advancing seq → rejected.
    let rollback = owner.routing.publish_root("", r1, Some(r2), 1).await;
    assert!(
        matches!(rollback, Err(RoutingError::Conflict(_))),
        "seq must advance"
    );

    // A DIFFERENT identity's root is fully independent (single-writer isolation).
    let r3 = zeph_core::Cid::of(b"readers-own-db");
    reader.routing.publish_root("", r3, None, 0).await.unwrap();
    assert_eq!(
        reader
            .routing
            .resolve_root(oid, "")
            .await
            .unwrap()
            .unwrap()
            .root_cid,
        r2,
        "owner untouched"
    );
    let rid = reader.transport.node_id();
    assert_eq!(
        reader
            .routing
            .resolve_root(rid, "")
            .await
            .unwrap()
            .unwrap()
            .root_cid,
        r3
    );
}

/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and
/// the interest count is visible network-wide via the tracker; withdrawing it
/// clears the count.
#[tokio::test]
async fn want_signal_propagates_and_withdraws() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();

    let publisher = node(&tracker, dirs[0].path()).await;
    publisher.routing.announce_node(0, 0).await.unwrap();
    let payload = vec![9u8; 40_000];
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;

    // A node that holds NOTHING wants the content.
    let wanter = node(&tracker, dirs[1].path()).await;
    assert_eq!(
        wanter.engine.store().piece_count(&cid),
        0,
        "wanter holds nothing"
    );
    wanter.engine.want(cid).await.unwrap();

    // Any node observes the want via the tracker content list.
    let observer = node(&tracker, dirs[2].path()).await;
    let want_count = |list: &[zeph_routing::ContentEntry]| {
        list.iter().find(|c| c.cid == cid).map_or(0, |c| c.wants)
    };
    let content = observer.routing.content().await.unwrap();
    assert!(want_count(&content) >= 1, "want is visible network-wide");

    // Withdraw clears it.
    wanter.engine.unwant(cid).await.unwrap();
    let content = observer.routing.content().await.unwrap();
    assert_eq!(want_count(&content), 0, "unwant clears the interest");
}

/// DST CHURN HARNESS (M2 capstone): a seeded, multi-round churn simulation.
/// Each round kills a random live node and spawns a fresh empty one, then runs
/// the lifecycle; WANTED content must stay retrievable byte-identical AND above
/// k throughout — the invariant that "what matters survives churn". Ignored by
/// default (heavy); run with `cargo test -p zeph-obj dst_churn -- --ignored`.
#[tokio::test]
#[ignore]
async fn dst_churn_wanted_content_survives() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(0xD57_C0FFEE);
    let k = 8usize;
    let tracker = start_tracker().await;
    let mut dirs: Vec<tempfile::TempDir> = Vec::new();
    let mut mk = || {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().to_path_buf();
        dirs.push(d);
        path
    };

    // Initial network of 6 holders.
    let mut nodes: Vec<Node> = Vec::new();
    for _ in 0..6 {
        let path = mk();
        let n = node(&tracker, &path).await;
        n.routing.announce_node(0, 0).await.unwrap();
        nodes.push(n);
    }

    // Publish one WANTED file.
    let ppath = mk();
    let publisher = node(&tracker, &ppath).await;
    let payload: Vec<u8> = (0..100_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 3) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    nodes[0].engine.want(cid).await.unwrap();

    for round in 0..4 {
        // Churn: kill a random live node (keep ≥4), spawn a fresh empty one.
        if nodes.len() > 4 {
            let i = rng.gen_range(0..nodes.len());
            nodes.remove(i).kill().await;
        }
        let path = mk();
        let n = node(&tracker, &path).await;
        n.routing.announce_node(0, 0).await.unwrap();
        nodes.push(n);

        // Let the lifecycle repair + migrate pieces onto the surviving/new set.
        for _ in 0..8 {
            for h in &nodes {
                h.engine.health_scan().await;
                h.engine.distribute().await;
            }
        }

        // INVARIANT 1: still above k across live holders.
        let have = verified_have(&nodes, cid).await;
        assert!(have >= k, "round {round}: HAVE {have} fell below k={k}");

        // INVARIANT 2: still retrievable byte-identical, by CID alone.
        let fpath = mk();
        let fetcher = node(&tracker, &fpath).await;
        let got = fetcher.engine.get(cid, ConsumeMode::Drop).await;
        assert!(
            got.as_ref().map(|b| b == &payload).unwrap_or(false),
            "round {round}: wanted content lost ({:?})",
            got.err()
        );
        fetcher.kill().await;
    }
}

/// Sum the pieces the LIVE holders actually hold for `cid` (ground truth,/// Sum the pieces the LIVE holders actually hold for `cid` (ground truth,
/// bypassing the tracker's advisory counts).
async fn verified_have(holders: &[Node], cid: zeph_core::Cid) -> usize {
    holders
        .iter()
        .map(|h| h.engine.store().piece_count(&cid))
        .sum()
}

/// Distribution (spin-up): a node that holds ALL of a CID's pieces spreads
/// them onto freshly-joined empty nodes. Because Distribution MOVES (not
/// copies), the TOTAL piece count is conserved — this is spread, not new
/// redundancy — while the content ends up across many distinct holders.
#[tokio::test]
async fn content_spreads_to_newly_joined_nodes() {
    let tracker = start_tracker().await;
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();

    // A single storage node holds everything: publish spreads to it alone.
    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 9) as u8)
        .collect();
    let report = publisher.engine.publish(&payload, false).await.unwrap();
    let cid = report.cid;
    // Wanted, so the lifecycle maintains + spreads it (Distribution is fade-gated).
    s0.engine.want(cid).await.unwrap();
    assert_eq!(
        report.distinct_peers, 1,
        "all pieces landed on the one node"
    );
    let total = s0.engine.store().piece_count(&cid);
    assert!(
        total >= 32,
        "s0 over-concentrated with the whole generation"
    );

    // Now 3 empty nodes join.
    let mut nodes = vec![s0];
    for dir in dirs.iter().skip(2).take(3) {
        let n = node(&tracker, dir.path()).await;
        n.routing.announce_node(0, 0).await.unwrap();
        nodes.push(n);
    }

    // Run Distribution until the concentration spreads out.
    let mut spread_ok = false;
    for _ in 0..80 {
        for n in &nodes {
            n.engine.distribute().await;
        }
        let counts: Vec<usize> = nodes
            .iter()
            .map(|n| n.engine.store().piece_count(&cid))
            .collect();
        let live_total: usize = counts.iter().sum();
        assert_eq!(
            live_total, total,
            "MOVE conserves total pieces (no new redundancy)"
        );
        let holders = counts.iter().filter(|c| **c > 0).count();
        let max = *counts.iter().max().unwrap();
        if holders >= 4 && max <= total / 2 {
            spread_ok = true;
            break;
        }
    }
    assert!(spread_ok, "pieces spread across the newly-joined nodes");

    // Still fetchable by CID alone after redistribution.
    let fetcher = node(&tracker, dirs[5].path()).await;
    let got = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload, "content still fetchable after redistribution");
}
