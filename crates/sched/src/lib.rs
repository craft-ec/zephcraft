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

#[derive(Default)]
struct Counters {
    submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    deduped: AtomicU64,
    in_flight: AtomicU64,
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
        }
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
        // Pop the highest-priority job, waiting if the queue is empty.
        let job = loop {
            let popped = inner.queue.lock().unwrap().pop();
            match popped {
                Some(j) => break j,
                None => inner.notify.notified().await,
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
