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
//! registry (NOTE: in production its replicate/migrate jobs run on the SAME
//! JobCoordinator being baselined — a known load-fidelity gap), governance
//! (its 30s anti-entropy tick's census-many DHT gets likewise), CraftSQL,
//! CraftCOM, the control socket/dashboard, the public stats server, the DHT
//! record/table persistence checkpoints + hourly expire loop (fresh tempdirs;
//! nothing fires inside a test window), the resource gauge / shed gates
//! (no cgroup budget on dev boxes; REQUIRED for a future Scenario C capped
//! receiver), and the engine event bus / PRE keypair (no consumers here).
//! Where `cmd_run` gates boot phases on the registry's census-stability
//! readiness flag, the harness gates on census stability directly (same
//! 10s-stable shape, same 20s bounded caller wait as `wait_ready`).

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
// noded/src/headreg.rs readiness-gate constants (READY_STABLE_SECS /
// READY_WAIT_SECS), mirrored by `wait_census_settled`.
const READY_STABLE_SECS: u64 = 10;
const READY_WAIT_SECS: u64 = 20;

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
        // CENSUS, not the size-5 active view — mirrors noded/src/peers.rs
        // (the active view capped liveness filtering + placement at ~5 peers:
        // the seed at_risk=100 anomaly and the placement skew).
        match self.membership.read().await.as_ref() {
            Some(m) => m.liveness_census().await,
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
    /// Cumulative count of DHT `resolve()` calls this node issued — the
    /// elected-scan proof (element 4): aggregate resolves should be ~O(cids)
    /// per interval, not O(cids × replication).
    pub resolves: Arc<std::sync::atomic::AtomicU64>,
    tasks: Vec<JoinHandle<()>>,
    _data_dir: tempfile::TempDir,
}

/// A `ContentRouting` that counts `resolve()` calls and delegates the rest —
/// the instrument for the elected-scan proof.
struct CountingRouting {
    inner: Arc<dyn zeph_routing::ContentRouting>,
    resolves: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl zeph_routing::ContentRouting for CountingRouting {
    async fn announce(&self, cid: Cid, pc: u32, pinned: bool) -> zeph_routing::Result<()> {
        self.inner.announce(cid, pc, pinned).await
    }
    async fn resolve(&self, cid: Cid) -> zeph_routing::Result<Vec<zeph_routing::ProviderRecord>> {
        self.resolves
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.resolve(cid).await
    }
    async fn withdraw(&self, cid: Cid) -> zeph_routing::Result<()> {
        self.inner.withdraw(cid).await
    }
    async fn announce_want(&self, cid: Cid) -> zeph_routing::Result<()> {
        self.inner.announce_want(cid).await
    }
    async fn withdraw_want(&self, cid: Cid) -> zeph_routing::Result<()> {
        self.inner.withdraw_want(cid).await
    }
    async fn is_wanted(&self, cid: Cid) -> zeph_routing::Result<bool> {
        self.inner.is_wanted(cid).await
    }
    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> zeph_routing::Result<()> {
        self.inner.announce_meta(cid, published_at, comment).await
    }
    async fn withdraw_meta(&self, cid: Cid) -> zeph_routing::Result<()> {
        self.inner.withdraw_meta(cid).await
    }
    async fn metas(&self, cid: Cid) -> zeph_routing::Result<Vec<zeph_routing::MetaRecord>> {
        self.inner.metas(cid).await
    }
}

impl TestNode {
    /// Spawn a full node. `seeds` bootstraps BOTH membership and the DHT
    /// (production's `dht_seeds`, which feed both — see `cmd_run`).
    pub async fn spawn(seeds: &[Contact]) -> Result<Self> {
        Self::spawn_in(tempfile::tempdir()?, seeds).await
    }

    /// Spawn into an EXISTING data dir — a restart reloads the same identity
    /// (keys/) and the full held store from disk, i.e. the same NodeId with
    /// its content, exactly like a production restart.
    pub async fn spawn_in(data_dir: tempfile::TempDir, seeds: &[Contact]) -> Result<Self> {
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
        // The transport is already bound: close it gracefully on failure so a
        // partially-built node never leaks a live endpoint into the cluster.
        let store = match Store::open(data_dir.path().join("store")) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                transport.close().await;
                return Err(e.into());
            }
        };
        let dht = DhtNode::new(identity.clone(), transport.clone(), DHT_RECORD_TTL_MS);
        // Restore DHT records + routing table if this is a restart (present) —
        // production persists these, so a restart re-forms from them instead of
        // cold-bootstrapping. No-op (0 loaded) on a fresh dir.
        dht.load_records(&data_dir.path().join("dht_records.bin"));
        dht.load_table(&data_dir.path().join("dht_table.bin"));
        let resolves = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let routing: Arc<dyn zeph_routing::ContentRouting> = Arc::new(CountingRouting {
            inner: Arc::new(zeph_routing::DhtRouting::new(dht.clone())),
            resolves: resolves.clone(),
        });
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

        // Re-announce provider records — CHUNKED coordinator jobs (mirrors
        // cmd_run post-chunking: the due list becomes ~25-cid jobs instead of
        // one O(held) slot-hogging walk).
        {
            let announce_engine = engine.clone();
            let announce_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(REANNOUNCE_SECS));
                loop {
                    interval.tick().await;
                    let due = announce_engine.due_announcements();
                    for (i, chunk) in due.chunks(25).enumerate() {
                        let e = announce_engine.clone();
                        let batch: Vec<_> = chunk.to_vec();
                        announce_jobs.submit(
                            format!("reannounce:{i}"),
                            Priority::Distribution,
                            1,
                            move || {
                                let (e, batch) = (e.clone(), batch.clone());
                                async move {
                                    e.announce_batch(&batch).await;
                                    Ok(())
                                }
                            },
                        );
                    }
                    let e = announce_engine.clone();
                    announce_jobs.submit(
                        "reannounce_wants",
                        Priority::Distribution,
                        1,
                        move || {
                            let e = e.clone();
                            async move {
                                e.reannounce_wants().await;
                                Ok(())
                            }
                        },
                    );
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
                                eng.health_scan_chunk(&[cid]).await;
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

        // Scale + quota tick — mirrors cmd_run POST-S3: the census-gated
        // distribute() sweep is DELETED in production (lazy rebalance rides
        // each cid's scan), so the harness must not run it either.
        {
            let dist_engine = engine.clone();
            let dist_jobs = jobs.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(HEALTH_SCAN_SECS));
                interval.tick().await; // skip immediate tick at startup
                loop {
                    interval.tick().await;
                    let e = dist_engine.clone();
                    dist_jobs.submit("scale_quota", Priority::Distribution, 1, move || {
                        let e = e.clone();
                        async move {
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
            resolves,
            tasks,
            _data_dir: data_dir,
        })
    }

    /// Tear the node down: abort the harness tasks (awaiting each so the
    /// cancellations have landed before returning), then close the transport.
    ///
    /// Residual (no stop APIs exist; contained by the per-test runtime
    /// teardown): Membership's internally-spawned probe/shuffle loops, the
    /// JobCoordinator dispatcher, and any in-flight coordinator jobs keep
    /// running against the closed transport. Do NOT reuse a shut-down node's
    /// `contact` as a bootstrap seed — DHT seeds are evict/tombstone-exempt,
    /// so a dead seed would be redialed forever.
    pub async fn shutdown(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
            let _ = t.await;
        }
        self.transport.close().await;
    }

    /// Shut down but RETAIN the data dir (store/ + keys/ + DHT snapshots) so the
    /// node can be restarted with the same identity + full store. Persists the
    /// DHT record store + routing table (production does; the harness otherwise
    /// cold-starts them) so a restart measures re-announce/re-scan, not DHT
    /// cold-start. `announced_at` is deliberately NOT persisted — its empty
    /// state on boot is the re-announce burst under test.
    pub async fn shutdown_retain(mut self) -> RestartHandle {
        let dir = self._data_dir.path().to_path_buf();
        let _ = self.dht.save_records(&dir.join("dht_records.bin"));
        let _ = self.dht.save_table(&dir.join("dht_table.bin"));
        for t in self.tasks.drain(..) {
            t.abort();
            let _ = t.await;
        }
        self.transport.close().await;
        RestartHandle {
            node_id: self.node_id,
            data_dir: self._data_dir,
        }
    }

    /// Restart a retained node into its old data dir (same NodeId + store).
    pub async fn restart(h: RestartHandle, seeds: &[Contact]) -> Result<Self> {
        Self::spawn_in(h.data_dir, seeds).await
    }
}

/// Holds a shut-down node's data dir so store/ + keys/ + DHT snapshots survive
/// until restart (or drop).
pub struct RestartHandle {
    pub node_id: NodeId,
    data_dir: tempfile::TempDir,
}

/// Wait (bounded, `READY_WAIT_SECS` — the `wait_ready` caller bound) for the
/// census to have been stable for `READY_STABLE_SECS` — the harness stand-in
/// for the skipped registry's readiness gate (noded/src/headreg.rs).
async fn wait_census_settled(membership: &Arc<Membership>) {
    let deadline = Instant::now() + Duration::from_secs(READY_WAIT_SECS);
    let mut last = usize::MAX;
    let mut stable_since = Instant::now();
    loop {
        let n = membership.census().await.len();
        if n != last {
            last = n;
            stable_since = Instant::now();
        }
        if stable_since.elapsed() >= Duration::from_secs(READY_STABLE_SECS)
            || Instant::now() >= deadline
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
