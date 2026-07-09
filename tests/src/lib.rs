//! Integration test crate for the Craftec node: DST harness (foundation §46),
//! multi-node cluster tests, and nemesis scenarios land here.
//!
//! [`TestNode`] assembles a FULL in-process node from the REAL production
//! components — transport (LocalOnly QUIC), HyParView/SWIM membership, the
//! Kademlia DHT + `DhtRouting`, the persistent store, the obj engine, and the
//! `JobCoordinator` — wired exactly like `crates/noded/src/main.rs::cmd_run`.
//! NO testkit in-memory doubles: the doubles are why the production transfer
//! convoys never showed up in tests (docs/TRANSFER_PLANE_V2.md, "Acceptance
//! harness").
//!
//! Deliberately NOT assembled (not part of the transfer plane): the head
//! registry, governance, CraftSQL, CraftCOM, the control socket/dashboard,
//! and the public stats server. Where `cmd_run` gates boot phases on the
//! registry's census-stability readiness flag, the harness gates on census
//! stability directly (same 10s-stable shape, same 20s bounded wait).

use std::cmp::Reverse;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::task::JoinHandle;
use zeph_core::{Cid, NodeId};
use zeph_dht::{Contact, DhtNode};
use zeph_membership::Membership;
use zeph_obj::{EngineWork, ObjConfig, ObjEngine, PeerSource};
use zeph_sched::{JobCoordinator, Priority};
use zeph_store::Store;
use zeph_transport::{alpn, PeerAddr, Reach, Transport};

/// Delay-queue of CIDs due for a health check, ordered by next-due time
/// (min-heap via `Reverse`) — mirrors noded's `DueQueue`.
type DueQueue = std::sync::Mutex<std::collections::BinaryHeap<Reverse<(Instant, Cid, Duration)>>>;

// ── Production defaults, mirrored from noded's `Config::default()` ──────────
const HEARTBEAT_SECS: u64 = 5;
const HEALTH_SCAN_SECS: u64 = 30;
const REANNOUNCE_SECS: u64 = 120;
const DHT_RECORD_TTL_MS: u64 = 48 * 3600 * 1000;

/// `MembershipPeers` — a [`PeerSource`] backed by SWIM membership; candidate
/// peers for piece placement come from real-time in-network liveness. The
/// membership handle is injected after construction, since membership is built
/// later than the obj engine. Replicates `noded/src/peers.rs` (a binary-private
/// module, so the shape is mirrored here).
pub struct MembershipPeers {
    membership: tokio::sync::RwLock<Option<Arc<Membership>>>,
}

impl MembershipPeers {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            membership: tokio::sync::RwLock::new(None),
        })
    }

    /// Inject the membership handle once it exists.
    pub async fn set(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }
}

#[async_trait::async_trait]
impl PeerSource for MembershipPeers {
    async fn peers(&self) -> Vec<(NodeId, PeerAddr)> {
        match self.membership.read().await.as_ref() {
            Some(m) => m
                .snapshot()
                .await
                .active
                .into_iter()
                .filter(|(_, ps)| ps.alive)
                .map(|(id, ps)| (id, ps.addr))
                .collect(),
            None => Vec::new(),
        }
    }
}

/// A full in-process node built from the real production components, wired
/// like `cmd_run`. Spawn the first node with no seeds; bootstrap later nodes
/// from an earlier node's [`TestNode::contact`].
pub struct TestNode {
    pub node_id: NodeId,
    pub addr: PeerAddr,
    /// This node's DHT contact — pass to later nodes' `spawn` as their seed.
    pub contact: Contact,
    pub engine: Arc<ObjEngine>,
    pub jobs: JobCoordinator,
    pub membership: Arc<Membership>,
    pub dht: Arc<DhtNode>,
    pub transport: Arc<Transport>,
    tasks: Vec<JoinHandle<()>>,
    _data_dir: tempfile::TempDir,
}

impl TestNode {
    /// Spawn a full node. `seeds` bootstraps BOTH membership and the DHT
    /// (production's `dht_seeds`, which feed both — see `cmd_run`).
    pub async fn spawn(seeds: &[Contact]) -> Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let identity =
            Arc::new(zeph_crypto::Keystore::new(data_dir.path().join("keys")).init_or_load()?);

        // ALPNs: the transfer plane's protocols (cmd_run's list minus the
        // skipped planes). Every harness node serves DHT traffic, like fleet
        // data nodes running `routing_dht = true`.
        let alpns = vec![
            alpn::PING.to_vec(),
            zeph_membership::ALPN.to_vec(),
            zeph_obj::ALPN.to_vec(),
            zeph_dht::ALPN.to_vec(),
        ];
        let transport = Arc::new(
            Transport::bind(identity.secret_key_bytes(), Reach::LocalOnly, alpns, 0).await?,
        );

        // Storage engine: persistent store + DHT routing + obj (cmd_run order).
        let store = Arc::new(Store::open(data_dir.path().join("store"))?);
        let dht = DhtNode::new(identity.clone(), transport.clone(), DHT_RECORD_TTL_MS);
        let routing: Arc<dyn zeph_routing::ContentRouting> =
            Arc::new(zeph_routing::DhtRouting::new(dht.clone()));
        let mem_peers = MembershipPeers::new();
        let peer_source: Arc<dyn PeerSource> = mem_peers.clone();
        // ObjConfig mirrors noded's Config::default() → ObjConfig mapping.
        let engine = ObjEngine::with_peer_source(
            transport.clone(),
            store.clone(),
            routing,
            peer_source,
            ObjConfig {
                k: 8,
                durability_threshold: 8,
                capacity_bytes: 10 * 1024 * 1024 * 1024,
                probe_timeout: Duration::from_secs(2),
                scale_threshold: 20,
                degrade_threshold: 5,
                fade_grace: Duration::from_secs(24 * 60 * 60),
                eviction_cooldown: Duration::from_secs(30 * 24 * 60 * 60),
                pace_delay: Duration::from_secs(1),
            },
        );

        // Background job coordinator (foundation §51), with cmd_run's boot
        // convergence clamp: the queue runs nearly one-at-a-time until the
        // post-boot wave drains (restored by the scan feeder phases below).
        let jobs = JobCoordinator::new(8);
        jobs.set_active_cap(2);

        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        // Demand-driven scaling: serve path fires a CID the instant its pull
        // count crosses scale_threshold; recruit through the coordinator.
        {
            let (scale_tx, mut scale_rx) = tokio::sync::mpsc::unbounded_channel::<Cid>();
            engine.set_scale_trigger(scale_tx);
            let scale_engine = engine.clone();
            let scale_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(cid) = scale_rx.recv().await {
                    let eng = scale_engine.clone();
                    scale_jobs.submit(
                        format!("scale:{}", cid.to_hex()),
                        Priority::Distribution,
                        1,
                        move || {
                            let eng = eng.clone();
                            async move {
                                eng.scale_one(cid).await;
                                Ok(())
                            }
                        },
                    );
                }
            }));
        }

        // Engine heavy-work router: publish distribution and durability repair
        // are DETECTED by the engine but SCHEDULED through the coordinator.
        {
            let (work_tx, mut work_rx) = tokio::sync::mpsc::unbounded_channel::<EngineWork>();
            engine.set_work_trigger(work_tx);
            let work_engine = engine.clone();
            let work_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(item) = work_rx.recv().await {
                    match item {
                        EngineWork::PublishDistribute(cid) => {
                            let eng = work_engine.clone();
                            work_jobs.submit(
                                format!("publish:{}", cid.to_hex()),
                                Priority::Encoding,
                                1,
                                move || {
                                    let eng = eng.clone();
                                    async move {
                                        eng.distribute_initial(cid).await;
                                        Ok(())
                                    }
                                },
                            );
                        }
                        EngineWork::Repair(cid) => {
                            let eng = work_engine.clone();
                            // max_attempts=1: repair_cid returning 0 is a valid
                            // outcome (another holder won the election).
                            work_jobs.submit(
                                format!("repair:{}", cid.to_hex()),
                                Priority::Repair,
                                1,
                                move || {
                                    let eng = eng.clone();
                                    async move {
                                        let _landed = eng.repair_cid(cid).await;
                                        Ok(())
                                    }
                                },
                            );
                        }
                    }
                }
            }));
        }

        // ALPN dispatcher: ping + membership + pieces + dht share the endpoint.
        let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel(32);
        let (member_tx, member_rx) = tokio::sync::mpsc::channel(32);
        let (piece_tx, piece_rx) = tokio::sync::mpsc::channel(32);
        let (dht_tx, dht_rx) = tokio::sync::mpsc::channel(32);
        let handlers = vec![
            (alpn::PING.to_vec(), ping_tx),
            (zeph_membership::ALPN.to_vec(), member_tx),
            (zeph_obj::ALPN.to_vec(), piece_tx),
            (zeph_dht::ALPN.to_vec(), dht_tx),
        ];
        dht.clone().serve(dht_rx);
        let server = transport.clone();
        tasks.push(tokio::spawn(async move { server.serve(handlers).await }));
        tasks.push(tokio::spawn(engine.clone().serve(piece_rx)));
        let ping_clock = transport.clock();
        tasks.push(tokio::spawn(async move {
            while let Some(conn) = ping_rx.recv().await {
                tokio::spawn(Transport::handle_ping_conn(ping_clock.clone(), conn));
            }
        }));

        // Membership: bootstrap from seed peers; probes drive the peer table.
        let membership = Membership::new(
            transport.clone(),
            zeph_membership::Config {
                probe_interval: Duration::from_secs(HEARTBEAT_SECS),
                ..Default::default()
            },
        );
        let boot_peers: Vec<PeerAddr> = seeds.iter().map(|c| c.addr.clone()).collect();
        membership.start(boot_peers.clone(), member_rx);
        mem_peers.set(membership.clone()).await;
        // Membership is the health scan's LIVENESS source: a holder SWIM marks
        // dead is excluded from durability counts so repair fires.
        engine.set_liveness(mem_peers.clone());

        // DHT overlay: bootstrap from seed contacts.
        {
            let dht_b = dht.clone();
            let dht_seeds = seeds.to_vec();
            tasks.push(tokio::spawn(async move {
                dht_b.bootstrap(dht_seeds).await;
            }));
        }

        // Re-seed membership from the configured seeds every 10s (cmd_run's
        // isolation-recovery loop).
        if !boot_peers.is_empty() {
            let seed_membership = membership.clone();
            let seed_addrs = boot_peers.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    seed_membership.seed(seed_addrs.clone()).await;
                }
            }));
        }

        // "Pending distribution" completion — through the coordinator, gated on
        // boot settle (cmd_run gates on the registry's readiness flag; the
        // registry is skipped, so gate on census stability directly).
        {
            let pending_engine = engine.clone();
            let pending_jobs = jobs.clone();
            let pending_membership = membership.clone();
            tasks.push(tokio::spawn(async move {
                wait_census_settled(&pending_membership).await;
                let mut iv = tokio::time::interval(Duration::from_secs(12));
                loop {
                    iv.tick().await;
                    let eng = pending_engine.clone();
                    pending_jobs.submit(
                        "distribute_pending",
                        Priority::Distribution,
                        1,
                        move || {
                            let eng = eng.clone();
                            async move {
                                eng.distribute_pending().await;
                                Ok(())
                            }
                        },
                    );
                }
            }));
        }

        // Re-announce provider records for everything held, immediately (first
        // tick) and periodically.
        {
            let announce_engine = engine.clone();
            let announce_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(REANNOUNCE_SECS));
                loop {
                    interval.tick().await;
                    let e = announce_engine.clone();
                    announce_jobs.submit("reannounce", Priority::Distribution, 1, move || {
                        let e = e.clone();
                        async move {
                            let _ = e.reannounce_providers().await;
                            Ok(())
                        }
                    });
                }
            }));
        }

        // HealthScan scheduler: a per-CID work QUEUE, not a sweep (cmd_run's
        // delay-queue: discovery feeder + workers, adaptive re-check bounds).
        let recheck_min = Duration::from_secs(HEALTH_SCAN_SECS);
        let recheck_max = Duration::from_secs(HEALTH_SCAN_SECS * 64);
        let hs_queue: Arc<DueQueue> =
            Arc::new(std::sync::Mutex::new(std::collections::BinaryHeap::new()));
        let hs_seen: Arc<std::sync::Mutex<std::collections::HashSet<Cid>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

        // Discovery feeder: boot phases run SEQUENTIALLY (settle → wave drain →
        // full concurrency → dripped scan feed), as in cmd_run.
        {
            let (eng, q, seen) = (engine.clone(), hs_queue.clone(), hs_seen.clone());
            let feeder_membership = membership.clone();
            let feeder_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                // Phase 1: census settle (readiness gate).
                wait_census_settled(&feeder_membership).await;
                // Phase 2: let the post-settle wave FORM, then drain (bounded).
                tokio::time::sleep(Duration::from_secs(20)).await;
                let phase2 = Instant::now();
                loop {
                    if feeder_jobs.stats().queue_depth < 32
                        || phase2.elapsed() > Duration::from_secs(300)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                // Stable state reached: open the queue to full width.
                feeder_jobs.set_active_cap(8);
                // Phase 3: DRIP the initial scan backlog instead of dumping
                // thousands of due-now jobs.
                let mut first_pass = true;
                loop {
                    let now = Instant::now();
                    let mut i: u64 = 0;
                    for cid in eng.store().cids() {
                        let is_new = seen.lock().expect("seen").insert(cid);
                        if is_new {
                            let due = if first_pass {
                                i += 1;
                                now + Duration::from_millis((i % 1200) * 100)
                            } else {
                                now
                            };
                            q.lock().expect("q").push(Reverse((due, cid, recheck_min)));
                        }
                    }
                    first_pass = false;
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }));
        }

        // Worker: pull the earliest-due CID, submit scan:{cid} through the
        // coordinator, re-enqueue with the converging / provider-aware backoff.
        {
            let (eng, q, seen, live, coord, dht_w) = (
                engine.clone(),
                hs_queue.clone(),
                hs_seen.clone(),
                mem_peers.clone(),
                jobs.clone(),
                dht.clone(),
            );
            tasks.push(tokio::spawn(async move {
                // Wait for the peer view AND the DHT overlay to SETTLE before
                // the FIRST scan (cmd_run: stable peers + routing table for
                // 10s, bounded by a 90s max grace).
                let start = Instant::now();
                let mut last_peers = usize::MAX;
                let mut last_table = usize::MAX;
                let mut stable_since = start;
                loop {
                    let peers = live.peers().await.len();
                    let table = dht_w.table_len();
                    if peers != last_peers || table != last_table {
                        last_peers = peers;
                        last_table = table;
                        stable_since = Instant::now();
                    }
                    let ready =
                        peers > 0 && table > 0 && stable_since.elapsed() >= Duration::from_secs(10);
                    if ready || start.elapsed() >= Duration::from_secs(90) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                loop {
                    let next = q.lock().expect("q").peek().map(|r| r.0 .0);
                    let Some(due) = next else {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    };
                    let now = Instant::now();
                    if due > now {
                        // Wake at least once a second to notice newly-fed items.
                        tokio::time::sleep((due - now).min(Duration::from_secs(1))).await;
                        continue;
                    }
                    let (cid, delay) = match q.lock().expect("q").pop() {
                        Some(item) => (item.0 .1, item.0 .2),
                        None => continue,
                    };
                    let (e2, q2, seen2) = (eng.clone(), q.clone(), seen.clone());
                    let submitted = coord.submit(
                        format!("scan:{}", cid.to_hex()),
                        Priority::HealthScan,
                        1,
                        move || {
                            let (eng, q, seen) = (e2.clone(), q2.clone(), seen2.clone());
                            async move {
                                let _r = eng.health_scan_chunk(&[cid]).await;
                                // Re-enqueue for the next check if still held;
                                // else stop tracking it.
                                if eng.store().piece_count(&cid) > 0
                                    || eng.store().has_content(&cid)
                                {
                                    // Adaptive backoff: an at-risk/converging
                                    // cid stays HOT; a healthy cid backs off
                                    // PROVIDER-AWARE (cmd_run's exact logic).
                                    let next = if eng.converging(&cid) {
                                        recheck_min
                                    } else {
                                        let holders = eng.live_providers(&cid).clamp(1, 16);
                                        (delay * 2).max(recheck_min * holders).min(recheck_max)
                                    };
                                    q.lock().expect("q").push(Reverse((
                                        Instant::now() + next,
                                        cid,
                                        next,
                                    )));
                                } else {
                                    seen.lock().expect("seen").remove(&cid);
                                }
                                Ok(())
                            }
                        },
                    );
                    // Already in-flight (deduped) — reschedule, never drop.
                    if !submitted {
                        q.lock()
                            .expect("q")
                            .push(Reverse((Instant::now() + delay, cid, delay)));
                    }
                }
            }));
        }

        // Distribute / scale: census-gated distribute (fires when the census
        // digest has been stable 2 ticks, plus a ~10min heartbeat), scale +
        // enforce_quota every tick — cmd_run's exact loop.
        {
            let dist_membership = membership.clone();
            let dist_engine = engine.clone();
            let dist_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(HEALTH_SCAN_SECS));
                interval.tick().await; // skip immediate tick at startup
                let mut last_digest: Option<[u8; 32]> = None;
                let mut stable_ticks: u32 = 0;
                let mut fired_for_digest = false;
                let mut ticks_since_run: u32 = 0;
                loop {
                    interval.tick().await;
                    let mut ids: Vec<[u8; 32]> = dist_membership
                        .census()
                        .await
                        .into_iter()
                        .map(|(n, _)| n.0)
                        .collect();
                    ids.sort();
                    let digest = Cid::of(&ids.concat()).0;
                    if last_digest != Some(digest) {
                        last_digest = Some(digest);
                        stable_ticks = 0;
                        fired_for_digest = false;
                    } else {
                        stable_ticks = stable_ticks.saturating_add(1);
                    }
                    ticks_since_run = ticks_since_run.saturating_add(1);
                    let run_distribute =
                        (!fired_for_digest && stable_ticks >= 2) || ticks_since_run >= 20;
                    if run_distribute {
                        fired_for_digest = true;
                        ticks_since_run = 0;
                    }
                    let e = dist_engine.clone();
                    dist_jobs.submit("distribute", Priority::Distribution, 1, move || {
                        let e = e.clone();
                        async move {
                            if run_distribute {
                                let _ = e.distribute().await;
                            }
                            let _ = e.scale().await;
                            e.enforce_quota().await;
                            Ok(())
                        }
                    });
                }
            }));
        }

        Ok(Self {
            node_id: transport.node_id(),
            addr: transport.addr(),
            contact: dht.contact(),
            engine,
            jobs,
            membership,
            dht,
            transport,
            tasks,
            _data_dir: data_dir,
        })
    }

    /// Tear the node down: abort the harness tasks and close the transport
    /// (internally-spawned membership/DHT/coordinator loops die with it).
    pub async fn shutdown(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        self.transport.close().await;
    }
}

/// Wait (bounded, 20s — cmd_run's `READY_WAIT_SECS`) for the census to have
/// been stable for 10s (`READY_STABLE_SECS`) — the harness stand-in for the
/// skipped registry's readiness gate.
async fn wait_census_settled(membership: &Arc<Membership>) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = 0usize;
    let mut stable_since = Instant::now();
    loop {
        let n = membership.census().await.len();
        if n != last {
            last = n;
            stable_since = Instant::now();
        }
        if stable_since.elapsed() >= Duration::from_secs(10) || Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
