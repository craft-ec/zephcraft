//! Hybrid Logical Clock (foundation §42, skew policy per §62.1).
//!
//! 64-bit timestamp: 48-bit wall-clock milliseconds ‖ 16-bit logical
//! counter. `now()` is strictly monotonic; `merge()` additionally moves the
//! clock past a remote timestamp, CLAMPING remote values more than
//! `MAX_SKEW_MS` ahead of local wall time so a peer with a broken clock
//! cannot drag ours into the future (warn-and-accept: the message is still
//! processed, the skew is reported for logging/metrics).
//!
//! Deviation (recorded): 100ms disk persistence of the clock (§42) is
//! deferred — wall time dominates on restart and nothing durable orders by
//! HLC before M2.

use std::sync::atomic::{AtomicU64, Ordering};

/// Strict-path skew tolerance (attestation, SIGNED_WRITE) and the clamp
/// bound for ordinary-path merges.
pub const MAX_SKEW_MS: u64 = 500;

/// A packed HLC timestamp: `(millis << 16) | logical`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn from_parts(millis: u64, logical: u16) -> Self {
        Self(((millis & 0xFFFF_FFFF_FFFF) << 16) | logical as u64)
    }

    pub fn millis(&self) -> u64 {
        self.0 >> 16
    }

    pub fn logical(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }
}

/// Outcome of merging a remote timestamp.
#[derive(Debug, Clone, Copy)]
pub struct Merge {
    /// Our clock value after the merge (strictly greater than both our
    /// previous value and the effective remote value).
    pub now: Timestamp,
    /// Absolute wall-clock difference |remote_ms - local_wall_ms|.
    pub skew_ms: u64,
    /// True if the remote was ahead beyond MAX_SKEW_MS and its contribution
    /// was clamped (ordinary path: accept + warn; strict paths reject).
    pub clamped: bool,
}

#[derive(Debug, Default)]
pub struct Clock {
    last: AtomicU64,
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Clock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Local/send event: strictly greater than every previous value, and at
    /// least the current wall time.
    pub fn now(&self) -> Timestamp {
        loop {
            let last = self.last.load(Ordering::SeqCst);
            let candidate = (wall_ms() << 16).max(last + 1);
            if self
                .last
                .compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Timestamp(candidate);
            }
        }
    }

    /// Receive event: advance past the remote timestamp (clamped to
    /// wall + MAX_SKEW_MS if the remote clock is too far ahead).
    pub fn merge(&self, remote: Timestamp) -> Merge {
        let wall = wall_ms();
        let skew_ms = remote.millis().abs_diff(wall);
        let clamped = remote.millis() > wall + MAX_SKEW_MS;
        let effective = if clamped {
            Timestamp::from_parts(wall + MAX_SKEW_MS, 0)
        } else {
            remote
        };
        loop {
            let last = self.last.load(Ordering::SeqCst);
            let candidate = (wall << 16).max(last + 1).max(effective.0 + 1);
            if self
                .last
                .compare_exchange(last, candidate, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Merge {
                    now: Timestamp(candidate),
                    skew_ms,
                    clamped,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GATE: monotonicity property — rapid calls are strictly increasing,
    /// across threads, with globally unique values.
    #[test]
    fn now_is_strictly_monotonic_across_threads() {
        let clock = std::sync::Arc::new(Clock::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let clock = clock.clone();
            handles.push(std::thread::spawn(move || {
                let mut previous = clock.now();
                let mut seen = Vec::with_capacity(10_000);
                for _ in 0..10_000 {
                    let ts = clock.now();
                    assert!(ts > previous, "must be strictly increasing per thread");
                    previous = ts;
                    seen.push(ts.0);
                }
                seen
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "values are globally unique");
    }

    #[test]
    fn merge_moves_past_remote_and_stays_monotonic() {
        let clock = Clock::new();
        let before = clock.now();
        let remote = Timestamp(before.0 + (100 << 16)); // 100ms ahead, within tolerance
        let merge = clock.merge(remote);
        assert!(merge.now > remote, "clock advances past remote");
        assert!(merge.now > before);
        assert!(!merge.clamped);
        assert!(clock.now() > merge.now, "still monotonic after merge");
    }

    #[test]
    fn merge_clamps_far_future_remote() {
        let clock = Clock::new();
        let far_future = Timestamp::from_parts(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 3_600_000, // one hour ahead
            0,
        );
        let merge = clock.merge(far_future);
        assert!(merge.clamped, "hour-ahead remote must be clamped");
        assert!(merge.skew_ms > 3_500_000 / 1000 * 900, "skew reported");
        assert!(
            merge.now.millis() < far_future.millis(),
            "our clock must NOT jump an hour forward (clamped to wall + {MAX_SKEW_MS}ms)"
        );
        // A lagging remote merges without clamping and without regression.
        let past = Timestamp::from_parts(1_000, 0);
        let merge = clock.merge(past);
        assert!(!merge.clamped);
        assert!(merge.now.millis() >= past.millis());
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let ts = Timestamp::from_parts(0x0000_1122_3344, 0x5566);
        assert_eq!(ts.millis(), 0x0000_1122_3344);
        assert_eq!(ts.logical(), 0x5566);
    }
}
