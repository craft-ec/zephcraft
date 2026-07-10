//! Background Job Coordinator (foundation §51) — a prioritized, deduplicated,
//! bounded-concurrency scheduler for background work (repair, encoding, piece
//! distribution, health scan, eviction).
//!
//! Detected needs (event bus) and periodic timers submit jobs; the coordinator
//! runs them **highest-priority-first**, at most `concurrency` at a time,
//! **coalescing duplicate keys** (§49 singleflight) and **retrying with
//! backoff**. It exists so durability-critical work (Repair) preempts routine
//! work (Eviction) instead of competing destructively — the shift from N
//! independent polling loops to one managed, event-driven work queue.
//!
//! This is Part I coordination — it schedules work, it does not execute code
//! (that's CraftCOM). `concurrency = 1` makes it a serial priority queue.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrd};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// Job priority (foundation §51 table). Higher runs first; declaration order is
/// low→high so `derive(Ord)` makes `Repair` the maximum (the max-heap pops it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Eviction,     // 5 — lowest: no urgency
    HealthScan,   // 4 — periodic, tolerates delay
    Distribution, // 3 — encoded data must reach peers
    Encoding,     // 2 — new data needs redundancy quickly
    Repair,       // 1 — highest: durability at risk
}

/// Work CLASS, derived from the job key prefix (Transfer Plane v2 element 5).
/// Priority orders URGENCY (Repair preempts); class adds FAIRNESS — a per-class
/// in-flight cap so no single class (a scan flood, a reannounce burst) occupies
/// every slot and starves the others. Derived, not passed, so no submit-site
/// changes: [`JobClass::from_key`] is the single source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobClass {
    Repair,
    Publish,
    Distribute,
    Scan,
    Pushstate,
    Reannounce,
    Scale,
    Eviction,
    Other,
}

impl JobClass {
    pub const ALL: [JobClass; 9] = [
        JobClass::Repair,
        JobClass::Publish,
        JobClass::Distribute,
        JobClass::Scan,
        JobClass::Pushstate,
        JobClass::Reannounce,
        JobClass::Scale,
        JobClass::Eviction,
        JobClass::Other,
    ];
    const COUNT: usize = Self::ALL.len();

    /// Stable array index (fieldless enum → `as usize`).
    #[inline]
    fn idx(self) -> usize {
        self as usize
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobClass::Repair => "repair",
            JobClass::Publish => "publish",
            JobClass::Distribute => "distribute",
            JobClass::Scan => "scan",
            JobClass::Pushstate => "pushstate",
            JobClass::Reannounce => "reannounce",
            JobClass::Scale => "scale",
            JobClass::Eviction => "eviction",
            JobClass::Other => "other",
        }
    }

    /// Classify a job KEY. Order matters (prefix specificity). A new submit-site
    /// prefix not listed here falls into `Other` (cap 2) — the classifier test
    /// enumerating every real prefix is the guard against silent throttling.
    pub fn from_key(key: &str) -> JobClass {
        if key.starts_with("repair:") {
            JobClass::Repair
        } else if key.starts_with("scan:") {
            JobClass::Scan
        } else if key.starts_with("publish:") {
            JobClass::Publish
        } else if key.starts_with("pushstate:") {
            JobClass::Pushstate
        } else if key.starts_with("reannounce") {
            JobClass::Reannounce // reannounce:{i} AND reannounce_wants
        } else if key.starts_with("scale") {
            JobClass::Scale // scale:{cid} AND scale_quota
        } else if key == "distribute_pending" {
            JobClass::Distribute
        } else if key == "eviction" {
            JobClass::Eviction
        } else {
            JobClass::Other
        }
    }

    /// Default per-class in-flight cap. Repair is UNCAPPED (durability must be
    /// able to use every slot); routine classes get headroom so nothing starves.
    fn default_cap(self, concurrency: usize) -> usize {
        let c = concurrency.max(1);
        match self {
            JobClass::Repair => c, // uncapped
            JobClass::Publish => c.min(4),
            JobClass::Distribute => c.min(4),
            JobClass::Scan => (c / 2).max(1),
            JobClass::Pushstate => c.min(2),
            JobClass::Reannounce => c.min(2),
            JobClass::Scale => c.min(2),
            JobClass::Eviction => c.min(2),
            JobClass::Other => c.min(2),
        }
    }
}

type JobFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type JobFactory = Box<dyn Fn() -> JobFuture + Send + Sync>;

struct QueuedJob {
    priority: Priority,
    class: JobClass,
    seq: u64,
    key: String,
    max_attempts: u32,
    factory: JobFactory,
}

impl PartialEq for QueuedJob {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for QueuedJob {}
impl Ord for QueuedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; within a priority, lower seq first (FIFO).
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for QueuedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Resource gauge SUPPLEMENTING the coordinator: an external sampler feeds the
/// process RSS, the environment supplies a memory budget (cgroup limit or
/// config), and the dispatcher consults the resulting pressure before starting
/// jobs — above the HIGH mark only Repair (durability-critical) jobs start;
/// above CRITICAL nothing new starts and inbound intake should shed (callers
/// poll [`Self::critical`]). Mechanism only: the gauge holds no policy about
/// what to sample or what the budget is — the node wires both.
#[derive(Default)]
pub struct ResourceGauge {
    /// Current process RSS in bytes (sampler-updated).
    rss: AtomicU64,
    /// Budget in bytes; 0 = gauge disabled (never reports pressure).
    budget: AtomicU64,
}

/// Start gating routine work above this fraction of the budget.
const GAUGE_HIGH: f64 = 0.85;
/// Stop starting ANY new job / shed inbound intake above this fraction.
const GAUGE_CRITICAL: f64 = 0.95;

impl ResourceGauge {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Set the memory budget (0 disables the gauge).
    pub fn set_budget(&self, bytes: u64) {
        self.budget.store(bytes, AtomicOrd::Relaxed);
    }

    /// Feed the current process RSS (called by the node's sampler).
    pub fn set_rss(&self, bytes: u64) {
        self.rss.store(bytes, AtomicOrd::Relaxed);
    }

    /// Budget used, in percent (0 when the gauge is disabled).
    pub fn load_pct(&self) -> u8 {
        let budget = self.budget.load(AtomicOrd::Relaxed);
        if budget == 0 {
            return 0;
        }
        let rss = self.rss.load(AtomicOrd::Relaxed);
        ((rss as f64 / budget as f64) * 100.0).min(255.0) as u8
    }

    fn over(&self, frac: f64) -> bool {
        let budget = self.budget.load(AtomicOrd::Relaxed);
        budget != 0 && self.rss.load(AtomicOrd::Relaxed) as f64 > budget as f64 * frac
    }

    /// Above the high-water mark: only durability-critical work should start.
    pub fn high(&self) -> bool {
        self.over(GAUGE_HIGH)
    }

    /// Above the critical mark: start nothing new; inbound intake should shed
    /// (reply "busy" and let the sender's next pass retry).
    pub fn critical(&self) -> bool {
        self.over(GAUGE_CRITICAL)
    }
}

#[derive(Default)]
struct Counters {
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    deduped: AtomicU64,
    in_flight: AtomicU64,
    deferred: AtomicU64,
    /// Dispatch rounds where a class-cap skip left the head job queued.
    class_deferred: AtomicU64,
}

/// A snapshot of coordinator activity.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct JobStats {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub retried: u64,
    pub deduped: u64,
    pub in_flight: u64,
    /// Jobs waiting in the priority queue right now.
    pub queue_depth: u64,
    /// Configured max concurrent jobs.
    pub concurrency: u64,
    /// Dispatch rounds where memory pressure deferred the queue head.
    #[serde(default)]
    pub deferred: u64,
    /// Dispatch rounds where a class-cap skip deferred the head job (fairness).
    #[serde(default)]
    pub class_deferred: u64,
    /// Per-class in-flight counts right now (fairness observability).
    #[serde(default)]
    pub class_in_flight: std::collections::BTreeMap<String, u64>,
    /// Memory budget used, percent (0 = gauge disabled).
    #[serde(default)]
    pub mem_load_pct: u8,
}

/// One finished job, for the recent-jobs view.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobRecord {
    pub key: String,
    pub ok: bool,
    pub ms: u64,
}

struct Inner {
    queue: Mutex<BinaryHeap<QueuedJob>>,
    inflight: Mutex<HashSet<String>>,
    notify: Notify,
    sem: Arc<Semaphore>,
    seq: AtomicU64,
    counters: Counters,
    concurrency: usize,
    recent: Mutex<VecDeque<JobRecord>>,
    /// Optional resource gauge; when wired the dispatcher gates on pressure.
    gauge: std::sync::OnceLock<Arc<ResourceGauge>>,
    /// Dynamic concurrency clamp (<= `concurrency`): boot convergence runs the
    /// queue nearly one-at-a-time (same discipline as the health-scan delay
    /// queue), then opens to full width once the node reaches stable state.
    active_cap: std::sync::atomic::AtomicUsize,
    /// Per-class in-flight counts, reserved under the queue lock at dispatch and
    /// released in `run_job` — the fairness bound (element 5).
    class_inflight: [AtomicU64; JobClass::COUNT],
    /// Per-class in-flight caps (index by `JobClass::idx`).
    class_cap: [std::sync::atomic::AtomicUsize; JobClass::COUNT],
}

/// A cheaply-cloneable handle to the coordinator.
#[derive(Clone)]
pub struct JobCoordinator {
    inner: Arc<Inner>,
}

impl JobCoordinator {
    /// Create a coordinator running at most `concurrency` jobs at once and spawn
    /// its dispatcher. `concurrency = 1` is a serial priority queue.
    pub fn new(concurrency: usize) -> Self {
        let inner = Arc::new(Inner {
            queue: Mutex::new(BinaryHeap::new()),
            inflight: Mutex::new(HashSet::new()),
            notify: Notify::new(),
            sem: Arc::new(Semaphore::new(concurrency.max(1))),
            seq: AtomicU64::new(0),
            counters: Counters::default(),
            concurrency: concurrency.max(1),
            recent: Mutex::new(VecDeque::new()),
            gauge: std::sync::OnceLock::new(),
            active_cap: std::sync::atomic::AtomicUsize::new(concurrency.max(1)),
            class_inflight: std::array::from_fn(|_| AtomicU64::new(0)),
            class_cap: std::array::from_fn(|i| {
                std::sync::atomic::AtomicUsize::new(JobClass::ALL[i].default_cap(concurrency))
            }),
        });
        let dispatch = inner.clone();
        tokio::spawn(dispatcher(dispatch));
        Self { inner }
    }

    /// Submit a job. `key` deduplicates: if a job with the same key is already
    /// queued or running, this is coalesced (returns `false`). `factory` is
    /// invoked once per attempt so the job can be retried. Returns `true` if
    /// enqueued.
    pub fn submit<F, Fut>(
        &self,
        key: impl Into<String>,
        priority: Priority,
        max_attempts: u32,
        factory: F,
    ) -> bool
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let key = key.into();
        if !self.inner.inflight.lock().unwrap().insert(key.clone()) {
            self.inner.counters.deduped.fetch_add(1, AtomicOrd::Relaxed);
            return false;
        }
        let seq = self.inner.seq.fetch_add(1, AtomicOrd::Relaxed);
        let class = JobClass::from_key(&key);
        let factory: JobFactory = Box::new(move || Box::pin(factory()));
        self.inner.queue.lock().unwrap().push(QueuedJob {
            priority,
            class,
            seq,
            key,
            max_attempts: max_attempts.max(1),
            factory,
        });
        self.inner
            .counters
            .submitted
            .fetch_add(1, AtomicOrd::Relaxed);
        self.inner.notify.notify_one();
        true
    }

    /// A snapshot of activity counters.
    pub fn stats(&self) -> JobStats {
        let c = &self.inner.counters;
        JobStats {
            submitted: c.submitted.load(AtomicOrd::Relaxed),
            completed: c.completed.load(AtomicOrd::Relaxed),
            failed: c.failed.load(AtomicOrd::Relaxed),
            retried: c.retried.load(AtomicOrd::Relaxed),
            deduped: c.deduped.load(AtomicOrd::Relaxed),
            in_flight: c.in_flight.load(AtomicOrd::Relaxed),
            queue_depth: self.inner.queue.lock().unwrap().len() as u64,
            concurrency: self.inner.concurrency as u64,
            deferred: c.deferred.load(AtomicOrd::Relaxed),
            class_deferred: c.class_deferred.load(AtomicOrd::Relaxed),
            class_in_flight: JobClass::ALL
                .iter()
                .map(|cl| {
                    (
                        cl.as_str().to_string(),
                        self.inner.class_inflight[cl.idx()].load(AtomicOrd::Relaxed),
                    )
                })
                .collect(),
            mem_load_pct: self
                .inner
                .gauge
                .get()
                .map(|g| g.load_pct())
                .unwrap_or_default(),
        }
    }

    /// Current per-class in-flight counts (alloc-light sampler for tests/harness).
    pub fn class_in_flight(&self) -> Vec<(&'static str, u64)> {
        JobClass::ALL
            .iter()
            .map(|cl| {
                (
                    cl.as_str(),
                    self.inner.class_inflight[cl.idx()].load(AtomicOrd::Relaxed),
                )
            })
            .collect()
    }

    /// Override a class's in-flight cap (defaults set at construction).
    pub fn set_class_cap(&self, class: JobClass, cap: usize) {
        self.inner.class_cap[class.idx()].store(cap.max(1), AtomicOrd::Relaxed);
        self.inner.notify.notify_one();
    }

    /// Wire the resource gauge — the dispatcher then defers routine work above
    /// the high-water mark and starts nothing above critical.
    pub fn set_gauge(&self, gauge: Arc<ResourceGauge>) {
        let _ = self.inner.gauge.set(gauge);
    }

    /// Clamp effective concurrency to `n` (restored by calling again with the
    /// full width). The dispatcher stops starting jobs while `in_flight >= n`.
    pub fn set_active_cap(&self, n: usize) {
        self.inner.active_cap.store(n.max(1), AtomicOrd::Relaxed);
        self.inner.notify.notify_one();
    }

    /// The most recent finished jobs, newest first (bounded history).
    pub fn recent_jobs(&self) -> Vec<JobRecord> {
        self.inner
            .recent
            .lock()
            .unwrap()
            .iter()
            .rev()
            .cloned()
            .collect()
    }
}

async fn dispatcher(inner: Arc<Inner>) {
    loop {
        // Pop the highest-priority job, waiting if the queue is empty — or if
        // memory pressure gates the queue head: above HIGH only Repair starts,
        // above CRITICAL nothing starts. Gated rounds re-check on a short tick
        // (pressure recedes without any new submit to wake us).
        let job = loop {
            let popped = {
                // Respect the dynamic clamp: during boot convergence the queue
                // drains nearly one-at-a-time (user directive — same queue
                // discipline as the health-scan drip).
                let cap = inner.active_cap.load(AtomicOrd::Relaxed);
                if inner.counters.in_flight.load(AtomicOrd::Relaxed) >= cap as u64 {
                    tokio::select! {
                        _ = inner.notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                    }
                    continue;
                }
                let mut q = inner.queue.lock().unwrap();
                if matches!(inner.gauge.get(), Some(g) if g.critical()) {
                    if q.peek().is_some() {
                        inner.counters.deferred.fetch_add(1, AtomicOrd::Relaxed);
                    }
                    None
                } else {
                    // SELECT-UNDER-LOCK (element 5 fairness): walk the heap in
                    // priority order and take the first job whose CLASS is under
                    // its in-flight cap; skip (re-push) higher-priority jobs whose
                    // class is saturated so no class monopolizes the slots. Repair
                    // is uncapped, so it is never skipped. The class slot is
                    // RESERVED here (under the lock, single dispatcher) so two
                    // same-class jobs can't both pass the cap in one round.
                    let high = matches!(inner.gauge.get(), Some(g) if g.high());
                    let mut skipped: Vec<QueuedJob> = Vec::new();
                    let mut chosen = None;
                    while let Some(j) = q.pop() {
                        if high && j.priority < Priority::Repair {
                            inner.counters.deferred.fetch_add(1, AtomicOrd::Relaxed);
                            skipped.push(j);
                            continue;
                        }
                        let i = j.class.idx();
                        let ci = inner.class_inflight[i].load(AtomicOrd::Relaxed);
                        let cap = inner.class_cap[i].load(AtomicOrd::Relaxed) as u64;
                        if ci >= cap {
                            skipped.push(j);
                            continue;
                        }
                        inner.class_inflight[i].fetch_add(1, AtomicOrd::Relaxed);
                        chosen = Some(j);
                        break;
                    }
                    let deferred_any = chosen.is_none() && !skipped.is_empty();
                    for j in skipped {
                        q.push(j);
                    }
                    if deferred_any {
                        inner
                            .counters
                            .class_deferred
                            .fetch_add(1, AtomicOrd::Relaxed);
                    }
                    chosen
                }
            };
            match popped {
                Some(j) => break j,
                None => {
                    tokio::select! {
                        _ = inner.notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                }
            }
        };
        // Bound concurrency: hold a permit for the job's lifetime.
        let permit = inner.sem.clone().acquire_owned().await.expect("sem open");
        tokio::spawn(run_job(inner.clone(), job, permit));
    }
}

async fn run_job(inner: Arc<Inner>, job: QueuedJob, _permit: OwnedSemaphorePermit) {
    inner.counters.in_flight.fetch_add(1, AtomicOrd::Relaxed);
    let started = Instant::now();
    let mut attempt = 0u32;
    let ok = loop {
        attempt += 1;
        match (job.factory)().await {
            Ok(()) => {
                inner.counters.completed.fetch_add(1, AtomicOrd::Relaxed);
                break true;
            }
            Err(e) if attempt >= job.max_attempts => {
                inner.counters.failed.fetch_add(1, AtomicOrd::Relaxed);
                tracing::warn!(key = %job.key, attempts = attempt, error = %e, "job failed");
                break false;
            }
            Err(e) => {
                inner.counters.retried.fetch_add(1, AtomicOrd::Relaxed);
                tracing::debug!(key = %job.key, attempt, error = %e, "job retry");
                // Exponential backoff, capped.
                let shift = (attempt - 1).min(6);
                tokio::time::sleep(Duration::from_millis(100u64 << shift)).await;
            }
        }
    };
    inner.counters.in_flight.fetch_sub(1, AtomicOrd::Relaxed);
    // Release the class slot in the SAME place as the global in_flight, and wake
    // the dispatcher so a job skipped for this class's cap runs promptly (not
    // only on the next timer tick). Decrement here-and-only-here (mirrors
    // in_flight) so no completion path can leak a class slot.
    inner.class_inflight[job.class.idx()].fetch_sub(1, AtomicOrd::Relaxed);
    inner.notify.notify_one();
    inner.inflight.lock().unwrap().remove(&job.key);
    {
        let mut recent = inner.recent.lock().unwrap();
        recent.push_back(JobRecord {
            key: job.key,
            ok,
            ms: started.elapsed().as_millis() as u64,
        });
        while recent.len() > 20 {
            recent.pop_front();
        }
    }
    // Permit drops here → a concurrency slot frees.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy(priority: Priority, seq: u64) -> QueuedJob {
        let key = format!("k{seq}");
        QueuedJob {
            priority,
            class: JobClass::from_key(&key),
            seq,
            key,
            max_attempts: 1,
            factory: Box::new(|| Box::pin(async { Ok(()) })),
        }
    }

    #[test]
    fn priority_then_fifo_ordering() {
        let mut h = BinaryHeap::new();
        h.push(dummy(Priority::Eviction, 0));
        h.push(dummy(Priority::Repair, 1));
        h.push(dummy(Priority::HealthScan, 2));
        h.push(dummy(Priority::Repair, 3)); // second Repair, later seq
                                            // Repair (seq1) → Repair (seq3) → HealthScan → Eviction.
        assert_eq!(h.pop().unwrap().seq, 1, "highest priority, earliest first");
        assert_eq!(h.pop().unwrap().seq, 3, "same priority is FIFO");
        assert_eq!(h.pop().unwrap().priority, Priority::HealthScan);
        assert_eq!(h.pop().unwrap().priority, Priority::Eviction);
    }

    #[tokio::test]
    async fn dedup_coalesces_same_key() {
        let jc = JobCoordinator::new(1);
        let gate = Arc::new(tokio::sync::Notify::new());
        let g = gate.clone();
        // First job blocks until released, keeping "dup" in-flight.
        assert!(jc.submit("dup", Priority::HealthScan, 1, move || {
            let g = g.clone();
            async move {
                g.notified().await;
                Ok(())
            }
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Same key while in-flight → coalesced.
        assert!(!jc.submit("dup", Priority::HealthScan, 1, || async { Ok(()) }));
        assert_eq!(jc.stats().deduped, 1);
        gate.notify_one();
    }

    #[tokio::test]
    async fn gauge_gates_routine_work_but_not_repair() {
        let jc = JobCoordinator::new(2);
        let gauge = ResourceGauge::new();
        gauge.set_budget(100);
        gauge.set_rss(90); // HIGH (>85%) but not CRITICAL (<95%)
        jc.set_gauge(gauge.clone());

        let ran_evict = Arc::new(AtomicUsize::new(0));
        let ran_repair = Arc::new(AtomicUsize::new(0));
        let (e, r) = (ran_evict.clone(), ran_repair.clone());
        jc.submit("e", Priority::Eviction, 1, move || {
            let e = e.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        jc.submit("r", Priority::Repair, 1, move || {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            ran_repair.load(Ordering::SeqCst),
            1,
            "Repair runs under HIGH pressure"
        );
        assert_eq!(
            ran_evict.load(Ordering::SeqCst),
            0,
            "routine work deferred under HIGH pressure"
        );
        assert!(jc.stats().deferred > 0, "deferral is visible in stats");

        // CRITICAL: even Repair must not start.
        gauge.set_rss(99);
        let blocked = Arc::new(AtomicUsize::new(0));
        let b = blocked.clone();
        jc.submit("r2", Priority::Repair, 1, move || {
            let b = b.clone();
            async move {
                b.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            blocked.load(Ordering::SeqCst),
            0,
            "nothing starts above CRITICAL"
        );

        // Pressure recedes → deferred jobs dispatch on the re-check tick.
        gauge.set_rss(10);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(ran_evict.load(Ordering::SeqCst), 1, "deferred job ran");
        assert_eq!(blocked.load(Ordering::SeqCst), 1, "critical-gated job ran");
    }

    #[tokio::test]
    async fn concurrency_is_bounded() {
        let jc = JobCoordinator::new(2);
        let cur = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        for i in 0..6 {
            let cur = cur.clone();
            let peak = peak.clone();
            jc.submit(format!("j{i}"), Priority::HealthScan, 1, move || {
                let cur = cur.clone();
                let peak = peak.clone();
                async move {
                    let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    cur.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            });
        }
        // Wait for all six to complete.
        while jc.stats().completed < 6 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            peak.load(Ordering::SeqCst) <= 2,
            "never exceeds concurrency"
        );
    }

    #[tokio::test]
    async fn retries_until_success() {
        let jc = JobCoordinator::new(1);
        let attempts = Arc::new(AtomicUsize::new(0));
        let a = attempts.clone();
        jc.submit("flaky", Priority::Repair, 3, move || {
            let a = a.clone();
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    anyhow::bail!("transient");
                }
                Ok(())
            }
        });
        while jc.stats().completed < 1 && jc.stats().failed < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(jc.stats().completed, 1, "eventually succeeds");
        assert_eq!(attempts.load(Ordering::SeqCst), 3, "took three attempts");
        assert_eq!(jc.stats().retried, 2);
    }

    #[test]
    fn classifier_maps_all_known_prefixes() {
        use JobClass::*;
        let cases = [
            ("repair:abc", Repair),
            ("scan:abc", Scan),
            ("publish:abc", Publish),
            ("pushstate:0:8:5", Pushstate),
            ("reannounce:3", Reannounce),
            ("reannounce_wants", Reannounce),
            ("scale:abc", Scale),
            ("scale_quota", Scale),
            ("distribute_pending", Distribute),
            ("eviction", Eviction),
        ];
        for (key, want) in cases {
            assert_eq!(
                JobClass::from_key(key),
                want,
                "{key} must classify as {want:?}, not Other (silent throttle)"
            );
        }
        // A genuinely-unknown prefix falls to Other (the safety net).
        assert_eq!(JobClass::from_key("mystery_job"), JobClass::Other);
        // Repair is never capped below full concurrency (durability guard).
        assert_eq!(JobClass::Repair.default_cap(8), 8);
    }

    #[tokio::test]
    async fn per_class_cap_prevents_starvation() {
        let jc = JobCoordinator::new(8);
        // Poll-a-flag gate (robust vs Notify's single-permit coalescing).
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Flood 20 blocking scan jobs — Scan cap is 4 of 8.
        for i in 0..20 {
            let r = released.clone();
            jc.submit(format!("scan:{i}"), Priority::HealthScan, 1, move || {
                let r = r.clone();
                async move {
                    while !r.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Ok(())
                }
            });
        }
        // A repair and a publish that just complete — must NOT be stuck behind
        // the scan flood (their classes have free slots).
        let done = Arc::new(AtomicUsize::new(0));
        for key in ["repair:x", "publish:y"] {
            let d = done.clone();
            jc.submit(key, Priority::Repair, 1, move || {
                let d = d.clone();
                async move {
                    d.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            });
        }

        // Let the dispatcher settle: scan in-flight caps at 4, repair+publish finish.
        let mut max_scan = 0u64;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let scan = jc
                .class_in_flight()
                .into_iter()
                .find(|(c, _)| *c == "scan")
                .map(|(_, n)| n)
                .unwrap_or(0);
            max_scan = max_scan.max(scan);
            if done.load(Ordering::SeqCst) == 2 {
                break;
            }
        }
        assert!(
            max_scan <= 4,
            "scan in-flight must never exceed its cap of 4, saw {max_scan}"
        );
        assert_eq!(
            done.load(Ordering::SeqCst),
            2,
            "repair + publish completed despite the 20-scan flood (no starvation)"
        );
        assert!(jc.stats().class_deferred > 0, "class-cap skips are counted");

        // Release the flood; everything drains.
        released.store(true, Ordering::SeqCst);
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if jc.stats().completed >= 22 {
                break;
            }
        }
        assert_eq!(jc.stats().completed, 22, "all jobs drain after release");
        for (c, n) in jc.class_in_flight() {
            assert_eq!(n, 0, "class {c} in-flight returns to 0");
        }
    }
}
