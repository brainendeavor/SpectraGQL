use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Detailed record of a GraphQL or HTTP transaction processed through SpectraGQL.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficRecord {
    pub id: String,
    pub hlc: String,
    pub timestamp_epoch_ms: u64,
    pub timestamp_formatted: String,
    pub client_ip: String,
    pub method: String,
    pub path: String,
    pub operation_name: Option<String>,
    pub operation_type: String,
    pub mode: String,
    pub status_code: u16,
    pub receipt_status: Option<String>,
    pub latency_ms: f64,
    pub target: String,
    pub query_preview: Option<String>,
    pub variables_preview: Option<String>,
}

/// Aggregated traffic statistics.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficStats {
    pub total_requests: u64,
    pub mode_b_requests: u64,
    pub mode_a_requests: u64,
    pub error_requests: u64,
    pub avg_latency_ms: f64,
    pub buffer_size: usize,
}

/// Full response payload for GET /admin/api/v1/traffic.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficResponse {
    pub stats: TrafficStats,
    pub records: Vec<TrafficRecord>,
}

/// Thread-safe in-memory circular buffer recording recent traffic and metrics.
#[derive(Clone)]
pub struct TrafficRecorder {
    max_entries: usize,
    records: Arc<RwLock<VecDeque<TrafficRecord>>>,
    total_requests: Arc<AtomicU64>,
    mode_b_requests: Arc<AtomicU64>,
    mode_a_requests: Arc<AtomicU64>,
    error_requests: Arc<AtomicU64>,
    total_latency_us: Arc<AtomicU64>,
    recorded_count: Arc<AtomicU64>,
}

impl TrafficRecorder {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: if max_entries == 0 { 200 } else { max_entries },
            records: Arc::new(RwLock::new(VecDeque::with_capacity(max_entries))),
            total_requests: Arc::new(AtomicU64::new(0)),
            mode_b_requests: Arc::new(AtomicU64::new(0)),
            mode_a_requests: Arc::new(AtomicU64::new(0)),
            error_requests: Arc::new(AtomicU64::new(0)),
            total_latency_us: Arc::new(AtomicU64::new(0)),
            recorded_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Records an inbound or completed transaction into the rolling buffer.
    /// Uses non-blocking atomics and try_write to guarantee zero contention on the hot path.
    pub fn record(&self, record: TrafficRecord) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if record.mode.contains("Mode B") {
            self.mode_b_requests.fetch_add(1, Ordering::Relaxed);
        } else if record.mode.contains("Mode A") {
            self.mode_a_requests.fetch_add(1, Ordering::Relaxed);
        }

        if record.status_code >= 400 || record.receipt_status.as_deref() == Some("DISPATCH_FAILED") {
            self.error_requests.fetch_add(1, Ordering::Relaxed);
        }

        // Lock-free atomic latency tracking
        let us = (record.latency_ms * 1000.0).max(0.0) as u64;
        self.total_latency_us.fetch_add(us, Ordering::Relaxed);
        self.recorded_count.fetch_add(1, Ordering::Relaxed);

        // Non-blocking buffer write: never block a hot-path edge worker thread
        if let Ok(mut lock) = self.records.try_write() {
            lock.push_front(record);
            while lock.len() > self.max_entries {
                lock.pop_back();
            }
        }
    }

    /// Fetches the recent traffic slice and current statistics.
    pub fn get_response(&self, limit: usize) -> TrafficResponse {
        let records = if let Ok(lock) = self.records.read() {
            let n = if limit == 0 { lock.len() } else { limit.min(lock.len()) };
            lock.iter().take(n).cloned().collect()
        } else {
            Vec::new()
        };

        let total = self.total_requests.load(Ordering::Relaxed);
        let mode_b = self.mode_b_requests.load(Ordering::Relaxed);
        let mode_a = self.mode_a_requests.load(Ordering::Relaxed);
        let errors = self.error_requests.load(Ordering::Relaxed);
        let count = self.recorded_count.load(Ordering::Relaxed);

        let avg_latency = if count > 0 {
            let total_us = self.total_latency_us.load(Ordering::Relaxed);
            (total_us as f64 / 1000.0) / (count as f64)
        } else {
            0.0
        };

        TrafficResponse {
            stats: TrafficStats {
                total_requests: total,
                mode_b_requests: mode_b,
                mode_a_requests: mode_a,
                error_requests: errors,
                avg_latency_ms: (avg_latency * 100.0).round() / 100.0,
                buffer_size: records.len(),
            },
            records,
        }
    }

    /// Clears the recorded buffer and resets latency accumulators.
    pub fn clear(&self) {
        if let Ok(mut lock) = self.records.write() {
            lock.clear();
        }
        self.total_latency_us.store(0, Ordering::Relaxed);
        self.recorded_count.store(0, Ordering::Relaxed);
    }
}

/// Formats epoch milliseconds into a readable UTC timestamp string: HH:MM:SS.mmm
pub fn format_timestamp(epoch_ms: u64) -> String {
    let total_secs = epoch_ms / 1000;
    let ms = epoch_ms % 1000;
    let s = total_secs % 60;
    let m = (total_secs / 60) % 60;
    let h = (total_secs / 3600) % 24;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}

/// Helper to get current epoch milliseconds.
pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
