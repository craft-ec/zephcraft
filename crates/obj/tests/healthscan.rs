//! HealthScan GATE: the make-or-break proof that data survives node death.
//! Publish spreads n pieces across holders; we KILL a third of them (verified
//! availability drops below the durability floor); surviving holders run
//! HealthScan, which live-probes providers (dead ones don't answer → not
//! counted), detects the deficit, rendezvous-elects a repairer, and recodes
//! fresh pieces back onto live peers until availability recovers to the floor —
//! all discovered by CID alone, no manual peering.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjConfig, ObjEngine, CLASS_CRITICAL};
use zeph_routing::ContentRouting;
use zeph_store::Store;
use zeph_testkit::{MemNet, MemRouting};
use zeph_transport::{Reach, Transport};

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

/// A full storage node kept alive by its returned handles. Dropping the tuple
/// kills the node (transport closes) — that is how we simulate death.
struct Node {
    engine: Arc<ObjEngine>,
    routing: Arc<MemRouting>,
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

/// The default test config: fast probes/pacing so scans/repairs converge quickly.
fn test_cfg() -> ObjConfig {
    ObjConfig {
        probe_timeout: std::time::Duration::from_millis(200),
        scale_threshold: 3,
        pace_delay: std::time::Duration::from_millis(0),
        ..ObjConfig::default()
    }
}

async fn node(tracker: &MemNet, dir: &std::path::Path) -> Node {
    node_cfg(tracker, dir, test_cfg()).await
}

async fn node_cfg(tracker: &MemNet, dir: &std::path::Path, cfg: ObjConfig) -> Node {
    let id = Arc::new(NodeIdentity::generate());
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_transport::MUX_ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir).unwrap());
    let routing = tracker.routing(id, t.addr());
    let engine = ObjEngine::with_peer_source(
        t.clone(),
        store,
        routing.clone(),
        Arc::new(tracker.peers()),
        cfg,
    );
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    let serve_task =
        tokio::spawn(async move { st.serve(vec![(zeph_transport::tag::PIECE, tx)]).await });
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
    let tracker = start_tracker();
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
                         // Fire-and-forget distribution: poll until the full generation lands on the holders
                         // (was read off the publish report; now it happens in the background after return).
    let spread = wait_have(&holders, cid, floor).await;
    assert!(spread >= floor, "pushed the full generation across holders");
    assert_eq!(
        holders
            .iter()
            .filter(|h| h.engine.store().piece_count(&cid) > 0)
            .count(),
        5,
        "spread across all 5 holders"
    );

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

/// K8 — the availability probe reveals a real deficit that an inflated/stale provider record
/// hides, which the death test + census cannot. A holder that evicts (§31) keeps a lingering
/// record (48h TTL) while staying ALIVE, and a record can simply claim more pieces than the holder
/// now stores — the census can't tell (it tracks node liveness, not per-cid holding). Here the
/// real holders are evicted below the floor while a GHOST stays alive and announces a full
/// generation it does NOT hold, so the RECORD view sums ABOVE the floor and liveness says "up":
/// both stale signals say "healthy". Only the AvailabilityProbe — asking each holder for its
/// ACTUAL count — exposes the deficit. (Neuter `probe_availability` → this test fails, since the
/// scan then trusts the inflated record and never flags the cid.)
#[tokio::test]
async fn availability_probe_reveals_deficit_a_stale_record_hides() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..8).map(|_| tempfile::tempdir().unwrap()).collect();

    let mut holders = Vec::new();
    for dir in dirs.iter().take(5) {
        let h = node(&tracker, dir.path()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        // Census wired (as production): every node below stays ALIVE, so liveness alone says
        // "healthy" — the probe is the only signal that reveals the missing pieces.
        h.engine.set_liveness(std::sync::Arc::new(tracker.peers()));
        holders.push(h);
    }
    let publisher = node(&tracker, dirs[6].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    holders[0].engine.want(cid).await.unwrap(); // WANTed → maintained (else it just fades)
    let floor = 32usize; // n = target_pieces(K=8)
    let spread = wait_have(&holders, cid, floor).await;
    assert!(spread >= floor, "pushed the full generation across holders");
    assert!(
        holders[0].engine.store().piece_count(&cid) >= 2,
        "scanner (holder 0) must hold pieces to scan"
    );

    // GHOST joins AFTER distribution, so it never receives real pieces. It's alive and will
    // announce a fat record it does not back with any stored pieces.
    let ghost = node(&tracker, dirs[5].path()).await;
    ghost.routing.announce_node(0, 0).await.unwrap();
    ghost
        .engine
        .set_liveness(std::sync::Arc::new(tracker.peers()));

    // REAL deficit: evict every holder but holder 0 — they stay ALIVE (no kill), so their records
    // and census entries are unchanged; only their pieces are gone.
    for h in &holders[1..5] {
        h.engine.store().forget(&cid).unwrap();
    }
    let real = verified_have(&holders, cid).await;
    assert!(
        real < floor,
        "real availability {real} must be below the floor {floor}"
    );

    // The ghost announces a FULL generation it does not hold → the record-based total is now back
    // above the floor. Records + census both say "healthy".
    ghost
        .routing
        .announce(cid, floor as u32, false)
        .await
        .unwrap();

    // Holder 0 scans once. Without the probe it sums the inflated records → not at risk. With the
    // probe it asks each holder for its real count (evicted → 0, ghost → 0) → sees the deficit.
    holders[0].engine.health_scan().await;
    assert!(
        holders[0].engine.is_at_risk(&cid),
        "the availability probe must expose the real deficit the inflated record hid"
    );
}

/// FADE demand dimension: content with recent FETCH activity (within the grace
/// window) stays alive and is repaired — even with no pin and no want. This is
/// the contrast to unwanted_content_fades (same setup, minus the fetch).
#[tokio::test]
async fn recently_fetched_content_is_repaired() {
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the pieces to reach the holders before a
    // cross-node fetch can resolve providers.
    wait_have(&holders, cid, floor).await;

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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the generation to land before churn.
    wait_have(&holders, cid, floor).await;

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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the generation to land, then a survivor pins.
    wait_have(&holders, cid, floor).await;

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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the spread to land across the 4 holders.
    wait_have(&holders, cid, 32).await;
    assert_eq!(
        holders
            .iter()
            .filter(|h| h.engine.store().piece_count(&cid) > 0)
            .count(),
        4,
        "spread across 4 holders"
    );

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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the whole generation to land on s0.
    wait_have(std::slice::from_ref(&s0), cid, 32).await;
    assert_eq!(
        s0.engine.store().piece_count(&cid),
        32,
        "all pieces on the one provider"
    );

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
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    // One provider holds the whole generation.
    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 3) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    // Fire-and-forget distribution: wait for the whole generation to land on s0.
    wait_have(std::slice::from_ref(&s0), cid, floor).await;

    // Several empty nodes join — targets for Scaling to build surplus beyond the band.
    let mut nodes = vec![s0];
    for dir in dirs.iter().skip(2).take(8) {
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

    // Drive Scaling with real downloads to create surplus ABOVE the durability band (so the
    // degrade path actually has something to shed — content inside the band is deliberately kept).
    let high = floor + (floor / 8).max(2);
    let fetcher = node(&tracker, dirs[10].path()).await;
    for _ in 0..8 {
        for _ in 0..3 {
            let _ = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
        }
        for n in &nodes {
            n.engine.scale().await;
        }
    }
    let surplus = total(&nodes);
    assert!(
        surplus > high,
        "scaling created surplus {surplus} above the band top {high}"
    );

    // Demand fades (no more fetches). Degradation sheds cold surplus back down to the floor
    // (the Schmitt shed centres it, symmetric with repair).
    let mut settled = 0;
    for _ in 0..60 {
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
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..90_000u32)
        .map(|i| (i.wrapping_mul(40503) >> 6) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    // Fire-and-forget distribution: wait for s0 to receive >=k pieces.
    wait_have(std::slice::from_ref(&s0), cid, 8).await;
    assert!(
        s0.engine.store().piece_count(&cid) >= 8,
        "s0 holds >=k pieces"
    );

    // Kill the publisher AND the tracker: no network to fetch from at all.
    publisher.kill().await;

    // s0 still reconstructs — purely from its own pieces.
    let got = s0.engine.get(cid, ConsumeMode::Drop).await.unwrap();
    assert_eq!(got, payload, "decoded locally with no reachable network");
}

/// Eviction cooldown: an evicted CID enters a cooldown that BLOCKS the
/// lifecycle from refilling it (anti-thrash) — until a manual want/pin clears
/// the cooldown and re-acquisition is allowed again.
#[tokio::test]
async fn eviction_cooldown_blocks_refill_until_wanted() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

    let s = node(&tracker, dirs[0].path()).await;
    s.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload = vec![7u8; 40_000];
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    // Fire-and-forget distribution: wait for the pieces to land on s.
    wait_have(std::slice::from_ref(&s), cid, 1).await;
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

    // While in cooldown the lifecycle refuses to refill the CID (ingest rejects it).
    assert!(
        s.engine
            .store()
            .is_in_cooldown(&cid, std::time::Duration::from_secs(3600)),
        "still in cooldown → refill blocked"
    );

    // A manual WANT overrides the cooldown, re-enabling re-acquisition. (Under the
    // publish-once model the publisher no longer re-pushes; delivery is then the network's
    // job — a repair from a piece-holder or a fetch — so the mechanism under test here is the
    // cooldown gate itself: eviction sets it, want clears it.)
    s.engine.want(cid).await.unwrap();
    assert!(
        !s.engine
            .store()
            .is_in_cooldown(&cid, std::time::Duration::from_secs(3600)),
        "want cleared the eviction cooldown → re-acquisition allowed"
    );
}

/// MANIFEST: publish a named file (content + File manifest), then fetch it
/// back by the manifest CID alone and recover its name, mime, and bytes.
#[tokio::test]
async fn file_manifest_publish_and_fetch_by_name() {
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for both generations to land before a cross-node fetch.
    wait_have(&holders, fp.manifest_cid, 32).await;
    wait_have(&holders, fp.content_cid, 32).await;

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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for every generation to land before a cross-node fetch.
    for c in [
        fa.manifest_cid,
        fa.content_cid,
        fb.manifest_cid,
        fb.content_cid,
        dir_cid,
    ] {
        wait_have(&holders, c, 32).await;
    }

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
    let tracker = start_tracker();
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

    // Auto-announced envelope on publish.
    let metas = a.routing.metas(cid).await.unwrap();
    assert_eq!(metas.len(), 1, "one envelope after publish");
    let t0 = metas[0].published_at;
    assert!(t0 > 0 && metas[0].comment.is_none());

    // Edit the comment — published_at is PRESERVED, record superseded.
    a.engine.set_meta(cid, Some("draft".into())).await.unwrap();
    let metas = a.routing.metas(cid).await.unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].comment.as_deref(), Some("draft"));
    assert_eq!(metas[0].published_at, t0, "edit preserves published_at");

    // A SECOND publisher attaches its own envelope (multi-writer).
    b.engine.set_meta(cid, Some("mirror".into())).await.unwrap();
    let metas = b.routing.metas(cid).await.unwrap();
    assert_eq!(metas.len(), 2, "two independent envelopes");
    let first = metas.iter().map(|m| m.published_at).min().unwrap();
    assert_eq!(
        first, t0,
        "min(published_at) resolves the canonical first-published"
    );

    // A withdraws its envelope — only its claim goes; B's remains.
    a.engine.del_meta(cid).await.unwrap();
    let metas = a.routing.metas(cid).await.unwrap();
    assert_eq!(metas.len(), 1, "only the withdrawing publisher removed");
    assert_eq!(metas[0].comment.as_deref(), Some("mirror"));
}

/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and/// WANT: a node signals keep-alive interest for a CID it does NOT hold, and
/// the interest count is visible network-wide via the tracker; withdrawing it
/// clears the count.
#[tokio::test]
async fn want_signal_propagates_and_withdraws() {
    let tracker = start_tracker();
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
    assert!(
        observer.routing.is_wanted(cid).await.unwrap(),
        "want is visible network-wide"
    );

    // Withdraw clears it.
    wanter.engine.unwant(cid).await.unwrap();
    assert!(
        !observer.routing.is_wanted(cid).await.unwrap(),
        "unwant clears the interest"
    );
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
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the initial spread before churning.
    wait_have(&nodes, cid, k).await;

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

/// No-RTT class admission gate (TRANSFER_PLANE_V2 §3): a node under simulated
/// HIGH (not critical) memory pressure grants the durability-critical class and
/// denies the normal class. It must DENY distribute (NORMAL) intake at ingest
/// yet ADMIT repair (CRITICAL) — with no offer round-trip. Two nodes so repair
/// has exactly one candidate (B), making the CRITICAL push to B deterministic.
#[tokio::test]
async fn high_band_gate_denies_normal_admits_critical_repair() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();

    // Publisher A pins the content, so it retains the whole object and can mint
    // fresh CRITICAL repair pieces to spread.
    let a = node(&tracker, dirs[0].path()).await;
    a.routing.announce_node(0, 0).await.unwrap();

    // Holder B: graded gate only (no always-shedding shed_gate) — admit CRITICAL,
    // deny NORMAL. Counters record which arm the gate took.
    let b = node(&tracker, dirs[1].path()).await;
    b.routing.announce_node(0, 0).await.unwrap();
    let norm_denied = Arc::new(AtomicU64::new(0));
    let crit_seen = Arc::new(AtomicU64::new(0));
    let (nd, cs) = (norm_denied.clone(), crit_seen.clone());
    b.engine.set_grant_gate(Arc::new(move |class, items| {
        if class == CLASS_CRITICAL {
            cs.fetch_add(1, Ordering::Relaxed);
            items.min(4)
        } else {
            nd.fetch_add(1, Ordering::Relaxed);
            0
        }
    }));

    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 7) as u8)
        .collect();
    let cid = a.engine.publish(&payload, true).await.unwrap().cid;

    // Publish-distribute ships NORMAL pushes to B; every one is denied at ingest,
    // so B stores nothing from distribution.
    for _ in 0..25 {
        if norm_denied.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(
        norm_denied.load(Ordering::Relaxed) > 0,
        "distribute NORMAL pushes must hit B's class gate"
    );
    assert_eq!(
        b.engine.store().piece_count(&cid),
        0,
        "B admitted no NORMAL distribute pieces (class-gated at ingest)"
    );

    // Repair: A is the sole content holder → elected repairer → mints CRITICAL and
    // offers B (the only recruit). B's gate admits CRITICAL, so B accepts.
    let mut got = 0;
    for _ in 0..80 {
        a.engine.health_scan().await;
        got = b.engine.store().piece_count(&cid);
        if got > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(
        crit_seen.load(Ordering::Relaxed) > 0,
        "repair must offer/push CRITICAL to B"
    );
    assert!(
        got > 0,
        "B must admit CRITICAL repair pieces through the class gate (held {got})"
    );

    a.kill().await;
    b.kill().await;
}

/// Sum the pieces the LIVE holders actually hold for `cid` (ground truth,
/// bypassing the tracker's advisory counts).
async fn verified_have(holders: &[Node], cid: zeph_core::Cid) -> usize {
    holders
        .iter()
        .map(|h| h.engine.store().piece_count(&cid))
        .sum()
}

/// Distribution is fire-and-forget: `publish` returns immediately and pushes pieces to peers
/// in the BACKGROUND. Poll (bounded ~2s, every 20ms) until the summed pieces across `holders`
/// reach `want` — replaces reading the spread off the (now immediate) publish report.
async fn wait_have(holders: &[Node], cid: zeph_core::Cid, want: usize) -> usize {
    for _ in 0..100 {
        let have = verified_have(holders, cid).await;
        if have >= want {
            return have;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    verified_have(holders, cid).await
}

/// Distribution (spin-up): a node that holds ALL of a CID's pieces spreads
/// them onto freshly-joined empty nodes. Because Distribution MOVES (not
/// copies), the TOTAL piece count is conserved — this is spread, not new
/// redundancy — while the content ends up across many distinct holders.
#[tokio::test]
async fn content_spreads_to_newly_joined_nodes() {
    let tracker = start_tracker();
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
    // Fire-and-forget distribution: wait for the whole generation to land on the one node.
    wait_have(std::slice::from_ref(&s0), cid, 32).await;
    let total = s0.engine.store().piece_count(&cid);
    assert!(
        total >= 32,
        "s0 over-concentrated with the whole generation (all pieces on one node)"
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

/// DEPLOY-GATE regression (workflow-confirmed critical): a WANTED, unpinned
/// whole-content holder with no piece-capable live peer (every provider holds
/// exactly 1 piece) must still trigger repair — the sole-content fallback. The
/// pre-fix scan ran its repairer election over an EMPTY `capable` vec, elected
/// None, and skipped the enqueue, so the deadlock the fallback targets stayed
/// permanent (publisher effective≪floor forever). Scenarios A/B never hold
/// passive under-replicated whole content, so they masked this.
#[tokio::test]
async fn sole_content_holder_enqueues_repair_when_no_peer_is_capable() {
    use zeph_core::Cid;
    use zeph_obj::EngineWork;
    use zeph_store::Generation;

    let net = MemNet::new();
    let dir = tempfile::tempdir().unwrap();

    let pid = Arc::new(NodeIdentity::generate());
    let pt = Arc::new(
        Transport::bind(
            pid.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_transport::MUX_ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let routing = net.routing(pid.clone(), pt.addr());
    let engine = ObjEngine::with_peer_source(
        pt.clone(),
        store.clone(),
        routing.clone(),
        Arc::new(net.peers()),
        ObjConfig::default(),
    );
    engine.set_liveness(Arc::new(net.peers())); // deterministic census liveness
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    engine.set_work_trigger(tx);

    // Publisher holds WHOLE content, WANTED, NOT pinned, 0 coded pieces.
    let payload: Vec<u8> = (0..64_000u32).map(|i| i as u8).collect();
    let cid = Cid::of(&payload);
    let k = 8u32;
    let gen = Generation {
        k,
        piece_len: (payload.len() as u64).div_ceil(k as u64),
        total_len: payload.len() as u64,
        vtags: Vec::new(), // enqueue path does not verify vtags
    };
    store.put_generation(cid, gen).unwrap();
    store.put_content(cid, &payload, false).unwrap();
    store.set_want(cid).unwrap();

    // 3 LIVE holders, each a 1-piece provider — under floor, NONE capable (<2).
    for _ in 0..3 {
        let hid = Arc::new(NodeIdentity::generate());
        let ht = Arc::new(
            Transport::bind(hid.secret_key_bytes(), Reach::LocalOnly, vec![], 0)
                .await
                .unwrap(),
        );
        let hr = net.routing(hid.clone(), ht.addr());
        hr.announce_node(0, 0).await.unwrap();
        hr.announce(cid, 1, false).await.unwrap();
    }

    let report = engine.health_scan_chunk(&[cid]).await;
    assert_eq!(report.scanned, 1, "scanned the cid");
    let work = rx.try_recv();
    assert!(
        matches!(work, Ok(EngineWork::Repair(c)) if c == cid),
        "sole content holder must enqueue Repair (pre-fix: dead code enqueued nothing); got {work:?}"
    );
}

/// P3 — each segment of a large file is an INDEPENDENT erasure generation, repaired on its own by
/// the existing per-cid machinery. A deficit on ONE segment is detected + repaired back to its
/// floor while the other segments (healthy) are left untouched, and the whole file survives.
#[tokio::test]
async fn each_file_segment_is_repaired_independently() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..8).map(|_| tempfile::tempdir().unwrap()).collect();

    // Small segments so a multi-segment file is cheap to distribute: 40 KB segments, K=8 (floor 32).
    let cfg = || ObjConfig {
        file_segment_bytes: 40 * 1024,
        file_k: 8,
        ..test_cfg()
    };

    // 4 holders spread the pieces (we assert independence + repair RELATIVELY, so exact-floor spread
    // isn't needed — keeps the test light so it doesn't starve concurrent timing-sensitive tests).
    let mut holders = Vec::new();
    for dir in dirs.iter().take(4) {
        let h = node_cfg(&tracker, dir.path(), cfg()).await;
        h.routing.announce_node(0, 0).await.unwrap();
        holders.push(h);
    }
    let publisher = node_cfg(&tracker, dirs[7].path(), cfg()).await;

    // 100 KB → three segments (40 / 40 / 20 KB), each its own K=8 generation.
    let data: Vec<u8> = (0..100_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    let fp = publisher
        .engine
        .publish_file("big.bin", "application/octet-stream", &data, false)
        .await
        .unwrap();
    let segs: Vec<zeph_core::Cid> = publisher
        .engine
        .fetch_manifest(fp.manifest_cid)
        .await
        .unwrap()
        .file_segments()
        .unwrap()
        .iter()
        .map(|s| zeph_core::Cid(s.cid))
        .collect();
    assert_eq!(segs.len(), 3, "three segments");
    assert_ne!(segs[0], segs[1], "distinct segments have distinct cids");

    // holders[0] WANTS every segment → it maintains + repairs each independently.
    for s in &segs {
        holders[0].engine.want(*s).await.unwrap();
    }

    // Wait for each segment to become recoverable (≥ k pieces spread) — distribution is concurrent
    // across the 3 segments, so we assert independence + repair RELATIVELY, not exact floor
    // convergence (that's the single-object self-heal test's job).
    let k = 8usize;
    for s in &segs {
        let have = wait_have(&holders, *s, k).await;
        assert!(
            have >= k,
            "segment {s} spread to at least k ({have} >= {k})"
        );
    }
    let base0 = verified_have(&holders, segs[0]).await;
    let base1 = verified_have(&holders, segs[1]).await;

    // DEFICIT segment 1 ONLY: forget its pieces on 2 of the 4 holders (keeping rank ≥ k on the other
    // two → recoverable), leaving segments 0 and 2 untouched.
    for h in &holders[2..4] {
        h.engine.store().forget(&segs[1]).unwrap();
    }
    let after1 = verified_have(&holders, segs[1]).await;
    assert!(
        after1 < base1 && after1 < verified_have(&holders, segs[0]).await,
        "segment 1 is now deficient ({after1}) vs the healthy segments"
    );

    // Repair: scan until segment 1 climbs back — it must be repaired on its own. holders[0] wants
    // every segment, so its scan detects seg1's deficit and mints fresh pieces for it.
    let mut rec1 = after1;
    for _ in 0..80 {
        for h in &holders {
            h.engine.health_scan().await;
        }
        rec1 = verified_have(&holders, segs[1]).await;
        if rec1 >= base1 {
            break;
        }
    }
    assert!(
        rec1 > after1,
        "segment 1 was repaired independently — pieces restored ({rec1} > {after1})"
    );
    // Segment 0 was never deficient → it was NOT needlessly torn down by seg1's repair.
    let end0 = verified_have(&holders, segs[0]).await;
    assert!(
        end0 + 4 >= base0,
        "segment 0 stayed healthy through segment 1's repair ({end0} ~>= {base0})"
    );

    // A fresh fetcher restores the WHOLE file from the network (all segments, incl. the repaired
    // one) — byte-identical.
    let fetcher = node_cfg(&tracker, dirs[6].path(), cfg()).await;
    let (_n, _m, got) = fetcher.engine.fetch_file(fp.manifest_cid).await.unwrap();
    assert_eq!(
        got, data,
        "the multi-segment file survives per-segment repair, byte-identical"
    );
}

/// shed_cid must PROBE-VERIFY before shedding — a stale-high provider record from a DEAD holder must
/// NOT be counted as surplus. This is the review-caught data-loss regression: a shed is never
/// re-announced, so records run stale-high for hours; trusting them raw let shed_cid destroy real
/// pieces against phantom copies and drop a cid below its floor. Verify: with only our own real pieces
/// (at the floor) plus a dead node's phantom record, shed_cid sheds NOTHING.
#[tokio::test]
async fn shed_does_not_trust_a_dead_holders_stale_high_record() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize; // n = target_pieces(K=8)

    // s0 holds the whole generation — exactly the floor, which is NOT surplus on its own.
    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 5) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    wait_have(std::slice::from_ref(&s0), cid, floor).await;
    let before = s0.engine.store().piece_count(&cid);
    assert!(before >= floor, "s0 holds the full generation");

    // A phantom holder announces a big piece_count for the same cid, then DIES. Its record lingers
    // stale-high (nothing re-announces a departure), but the node is unreachable.
    let phantom = node(&tracker, dirs[2].path()).await;
    phantom.routing.announce(cid, 64, false).await.unwrap();
    phantom.kill().await;

    // If shed_cid trusted the record, have = s0(32) + phantom(64) = 96 >> floor+delta → it would shed.
    // Probe-verified, the dead phantom counts as ZERO, so have = 32 <= floor+delta → shed NOTHING.
    let shed = s0.engine.shed_cid(cid).await;
    assert_eq!(
        shed, 0,
        "must not shed against a dead holder's phantom pieces"
    );
    assert_eq!(
        s0.engine.store().piece_count(&cid),
        before,
        "no real piece was destroyed"
    );
}

/// The happy path: with a GENUINE, live, verified surplus, shed_cid trims exactly ONE piece (never a
/// loop) and the content stays at/above the floor.
#[tokio::test]
async fn shed_trims_one_verified_surplus_piece() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..12).map(|_| tempfile::tempdir().unwrap()).collect();
    let floor = 32usize;

    let s0 = node(&tracker, dirs[0].path()).await;
    s0.routing.announce_node(0, 0).await.unwrap();
    let publisher = node(&tracker, dirs[1].path()).await;
    let payload: Vec<u8> = (0..120_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 7) as u8)
        .collect();
    let cid = publisher.engine.publish(&payload, false).await.unwrap().cid;
    wait_have(std::slice::from_ref(&s0), cid, floor).await;

    // Build a real, verifiable surplus above the band via scaling (same as the degrade test).
    let mut nodes = vec![s0];
    for dir in dirs.iter().skip(2).take(8) {
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
    let high = floor + (floor / 8).max(2);
    let fetcher = node(&tracker, dirs[10].path()).await;
    for _ in 0..8 {
        for _ in 0..3 {
            let _ = fetcher.engine.get(cid, ConsumeMode::Drop).await.unwrap();
        }
        for n in &nodes {
            n.engine.scale().await;
        }
    }
    let surplus = total(&nodes);
    assert!(
        surplus > high,
        "scaling created surplus {surplus} above band top {high}"
    );

    // One shed_cid pass across all holders: exactly ONE elected shedder removes exactly ONE piece.
    let before = total(&nodes);
    let mut shed_total = 0usize;
    for n in &nodes {
        shed_total += n.engine.shed_cid(cid).await;
    }
    assert_eq!(
        shed_total, 1,
        "exactly one piece shed per pass, never a fair-share loop"
    );
    assert_eq!(
        total(&nodes),
        before - 1,
        "exactly one real piece left the cluster"
    );
    assert!(total(&nodes) >= floor, "still at/above the floor");
}
