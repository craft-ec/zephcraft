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

type JobFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type JobFactory = Box<dyn Fn() -> JobFuture + Send + Sync>;

struct QueuedJob {
    priority: Priority,
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
        let factory: JobFactory = Box::new(move || Box::pin(factory()));
        self.inner.queue.lock().unwrap().push(QueuedJob {
            priority,
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
            mem_load_pct: self
                .inner
                .gauge
                .get()
                .map(|g| g.load_pct())
                .unwrap_or_default(),
        }
    }

    /// Wire the resource gauge — the dispatcher then defers routine work above
    /// the high-water mark and starts nothing above critical.
    pub fn set_gauge(&self, gauge: Arc<ResourceGauge>) {
        let _ = self.inner.gauge.set(gauge);
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
                let mut q = inner.queue.lock().unwrap();
                let allowed = match inner.gauge.get() {
                    Some(g) if g.critical() => None,
                    Some(g) if g.high() => q
                        .peek()
                        .filter(|j| j.priority >= Priority::Repair)
                        .map(|_| ()),
                    _ => q.peek().map(|_| ()),
                };
                match allowed {
                    Some(()) => q.pop(),
                    None => {
                        if q.peek().is_some() {
                            inner.counters.deferred.fetch_add(1, AtomicOrd::Relaxed);
                        }
                        None
                    }
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
        QueuedJob {
            priority,
            seq,
            key: format!("k{seq}"),
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
}
