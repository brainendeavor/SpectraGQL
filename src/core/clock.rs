use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Hybrid Logical Clock (HLC) timestamp as proposed by Kulkarni & Demirbas.
///
/// Combines a physical wall-clock timestamp (in milliseconds) with a logical sequence counter.
/// Provides strict monotonic causal ordering across distributed nodes without requiring
/// synchronized atomic/GPS clocks.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical wall-clock time in milliseconds since Unix epoch
    pub physical: u64,
    /// Logical counter for causality within the same physical millisecond or drift
    pub logical: u32,
}

impl HlcTimestamp {
    pub fn new(physical: u64, logical: u32) -> Self {
        HlcTimestamp { physical, logical }
    }

    /// Returns a compact, sortable string representation: `<physical_ms>-<logical_counter>`
    pub fn to_compact_string(&self) -> String {
        format!("{:013}-{:06}", self.physical, self.logical)
    }
}

impl fmt::Debug for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HLC({}:{})", self.physical, self.logical)
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:013}.{:06}", self.physical, self.logical)
    }
}

/// Thread-safe Hybrid Logical Clock generator using lock-free atomic compare-and-swap (CAS).
///
/// State is bit-packed into a single 64-bit atomic integer:
/// - Upper 48 bits: Physical wall-clock timestamp in milliseconds (~8,900 years of range).
/// - Lower 16 bits: Logical sequence counter (up to 65,535 operations per millisecond per node).
pub struct HlcClock {
    pub(crate) state: AtomicU64,
}

impl HlcClock {
    pub const MAX_ALLOWED_CLOCK_DRIFT_MS: u64 = 60_000;
    const LOGICAL_MASK: u64 = 0xFFFF;
    const MAX_LOGICAL: u32 = 0xFFFF;

    pub fn new() -> Self {
        let initial_physical = Self::get_physical_time();
        let packed = (initial_physical << 16) & !Self::LOGICAL_MASK;
        HlcClock {
            state: AtomicU64::new(packed),
        }
    }

    fn get_physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Generates the next monotonic HLC timestamp for a local event using lock-free CAS.
    pub fn now(&self) -> HlcTimestamp {
        let physical_now = Self::get_physical_time();
        let mut current = self.state.load(Ordering::Acquire);

        loop {
            let curr_physical = current >> 16;
            let curr_logical = (current & Self::LOGICAL_MASK) as u32;

            let (next_physical, next_logical) = if physical_now > curr_physical {
                (physical_now, 0u32)
            } else if curr_logical < Self::MAX_LOGICAL {
                (curr_physical, curr_logical + 1)
            } else {
                // Counter exhausted within the current millisecond:
                // Advance physical time by 1ms and reset logical counter to 0.
                (curr_physical.saturating_add(1), 0u32)
            };

            let next_packed = (next_physical << 16) | (next_logical as u64 & Self::LOGICAL_MASK);
            match self.state.compare_exchange_weak(
                current,
                next_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return HlcTimestamp::new(next_physical, next_logical),
                Err(actual) => current = actual,
            }
        }
    }

    /// Updates local clock causality upon receiving a remote HLC timestamp using lock-free CAS.
    #[allow(dead_code)]
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let physical_now = Self::get_physical_time();

        // Guard against Byzantine or misconfigured remote nodes poisoning the local clock.
        let max_tolerated_physical = physical_now.saturating_add(Self::MAX_ALLOWED_CLOCK_DRIFT_MS);
        let safe_remote_physical = if remote.physical > max_tolerated_physical {
            log::warn!(
                "HLC remote clock drift anomaly detected: remote={}, physical_now={}, max_allowed={}. Capping remote physical.",
                remote.physical,
                physical_now,
                max_tolerated_physical
            );
            max_tolerated_physical
        } else {
            remote.physical
        };

        let mut current = self.state.load(Ordering::Acquire);

        loop {
            let curr_physical = current >> 16;
            let curr_logical = (current & Self::LOGICAL_MASK) as u32;

            let mut new_physical = physical_now.max(curr_physical).max(safe_remote_physical);
            let next_logical = if new_physical == curr_physical && new_physical == safe_remote_physical {
                match curr_logical.max(remote.logical).checked_add(1) {
                    Some(val) if val <= Self::MAX_LOGICAL => val,
                    _ => {
                        new_physical = new_physical.saturating_add(1);
                        0
                    }
                }
            } else if new_physical == curr_physical {
                match curr_logical.checked_add(1) {
                    Some(val) if val <= Self::MAX_LOGICAL => val,
                    _ => {
                        new_physical = new_physical.saturating_add(1);
                        0
                    }
                }
            } else if new_physical == safe_remote_physical {
                match remote.logical.checked_add(1) {
                    Some(val) if val <= Self::MAX_LOGICAL => val,
                    _ => {
                        new_physical = new_physical.saturating_add(1);
                        0
                    }
                }
            } else {
                0
            };

            let next_packed = (new_physical << 16) | (next_logical as u64 & Self::LOGICAL_MASK);
            match self.state.compare_exchange_weak(
                current,
                next_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return HlcTimestamp::new(new_physical, next_logical),
                Err(actual) => current = actual,
            }
        }
    }

    /// Generates a time-sortable UUIDv7 paired with its originating HLC timestamp.
    pub fn now_uuidv7(&self) -> (Uuid, HlcTimestamp) {
        let hlc = self.now();
        // Generate UUIDv7 (RFC 9562)
        let id = Uuid::now_v7();
        (id, hlc)
    }

    /// Returns a reference to the global static HLC clock.
    pub fn global() -> &'static HlcClock {
        static CLOCK: std::sync::OnceLock<HlcClock> = std::sync::OnceLock::new();
        CLOCK.get_or_init(HlcClock::new)
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_monotonicity() {
        let clock = HlcClock::new();
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();

        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn test_hlc_remote_causality_update() {
        let clock = HlcClock::new();
        let current = clock.now();

        // Simulate receiving a message from a node in the future
        let future_remote = HlcTimestamp::new(current.physical + 10000, 5);
        let updated = clock.update(future_remote);

        assert!(updated > future_remote);
        assert_eq!(updated.physical, future_remote.physical);
        assert_eq!(updated.logical, 6);

        // Subsequent local event must still be greater
        let next_local = clock.now();
        assert!(next_local >= updated);
    }

    #[test]
    fn test_uuidv7_generation() {
        let clock = HlcClock::new();
        let (id1, hlc1) = clock.now_uuidv7();
        let (id2, hlc2) = clock.now_uuidv7();

        assert_eq!(id1.get_version_num(), 7);
        assert_eq!(id2.get_version_num(), 7);
        assert!(hlc1 < hlc2);
        assert!(id1 <= id2);
    }

    #[test]
    fn test_hlc_drift_capping() {
        let clock = HlcClock::new();
        let current = clock.now();

        // Simulate receiving a message from a node far in the future (e.g., 1 hour ahead)
        let far_future_remote = HlcTimestamp::new(current.physical + 3_600_000, 42);
        let updated = clock.update(far_future_remote);

        // Must be capped within MAX_ALLOWED_CLOCK_DRIFT_MS (60s)
        let max_tolerated = current.physical + HlcClock::MAX_ALLOWED_CLOCK_DRIFT_MS;
        assert!(updated.physical <= max_tolerated + 1000); // allow small delta for test execution time
        assert!(updated.physical < far_future_remote.physical);
    }

    #[test]
    fn test_hlc_logical_overflow_protection() {
        let clock = HlcClock::new();
        let current = clock.now();

        // Force logical counter to 16-bit max (0xFFFF)
        clock.state.store((current.physical << 16) | 0xFFFF, Ordering::SeqCst);

        // The next call to now() must not panic or wrap to (current.physical, 0)
        let next = clock.now();
        assert_eq!(next.logical, 0);
        assert_eq!(next.physical, current.physical + 1);
        assert!(next > current);
    }

    #[test]
    fn test_hlc_concurrent_monotonicity() {
        use std::sync::Arc;
        use std::collections::BTreeSet;

        let clock = Arc::new(HlcClock::new());
        let mut handles = Vec::new();
        let threads = 20;
        let iters_per_thread = 200;

        for _ in 0..threads {
            let clk = Arc::clone(&clock);
            handles.push(std::thread::spawn(move || {
                let mut ts = Vec::with_capacity(iters_per_thread);
                for _ in 0..iters_per_thread {
                    ts.push(clk.now());
                }
                ts
            }));
        }

        let mut all_timestamps = Vec::with_capacity(threads * iters_per_thread);
        for h in handles {
            all_timestamps.extend(h.join().unwrap());
        }

        let total_count = all_timestamps.len();
        // Assert all timestamps are unique: zero collisions
        let unique_set: BTreeSet<_> = all_timestamps.iter().copied().collect();
        assert_eq!(unique_set.len(), total_count, "Detected duplicate HLC timestamps across threads!");
    }
}
