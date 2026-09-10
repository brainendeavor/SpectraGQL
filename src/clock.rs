use std::fmt;
use std::sync::Mutex;
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

/// Thread-safe Hybrid Logical Clock generator.
pub struct HlcClock {
    state: Mutex<HlcTimestamp>,
}

impl HlcClock {
    pub fn new() -> Self {
        let initial_physical = Self::get_physical_time();
        HlcClock {
            state: Mutex::new(HlcTimestamp::new(initial_physical, 0)),
        }
    }

    fn get_physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Generates the next monotonic HLC timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let physical_now = Self::get_physical_time();
        let mut state = self.state.lock().unwrap();

        if physical_now > state.physical {
            state.physical = physical_now;
            state.logical = 0;
        } else {
            state.logical += 1;
        }

        *state
    }

    /// Updates local clock causality upon receiving a remote HLC timestamp.
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let physical_now = Self::get_physical_time();
        let mut state = self.state.lock().unwrap();

        let new_physical = physical_now.max(state.physical).max(remote.physical);

        if new_physical == state.physical && new_physical == remote.physical {
            state.logical = state.logical.max(remote.logical) + 1;
        } else if new_physical == state.physical {
            state.logical += 1;
        } else if new_physical == remote.physical {
            state.logical = remote.logical + 1;
        } else {
            state.logical = 0;
        }

        state.physical = new_physical;
        *state
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
}
