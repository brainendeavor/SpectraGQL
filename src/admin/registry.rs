use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// A single log entry sent by a consumer worker.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLogEntry {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hlc: Option<String>,
}

/// Payload sent by consumer workers via POST /admin/api/v1/telemetry/report.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTelemetryReport {
    pub worker_id: String,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub sink: Option<String>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub uptime_seconds: Option<u64>,
    #[serde(default)]
    pub processed_events: Option<u64>,
    #[serde(default)]
    pub total_errors: Option<u64>,
    #[serde(default)]
    pub last_event_hlc: Option<String>,
    #[serde(default)]
    pub logs: Option<Vec<WorkerLogEntry>>,
}

/// Detailed log response returned to the Admin UI for a specific worker.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLogsResponse {
    pub worker_id: String,
    #[serde(default)]
    pub app_id: Option<String>,
    pub status: String,
    pub uptime_seconds: u64,
    pub processed_events: u64,
    pub total_errors: u64,
    pub last_event_hlc: Option<String>,
    pub logs: Vec<WorkerLogEntry>,
}

/// Lightweight summary of a worker used in consumer lists and health checks.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSummary {
    pub worker_id: String,
    #[serde(default)]
    pub app_id: Option<String>,
    pub sink: String,
    pub stream: String,
    pub status: String,
    pub uptime_seconds: u64,
    pub processed_events: u64,
    pub total_errors: u64,
    pub last_seen_secs_ago: u64,
}

struct WorkerState {
    worker_id: String,
    app_id: Option<String>,
    sink: String,
    stream: String,
    status: String,
    uptime_seconds: u64,
    processed_events: u64,
    total_errors: u64,
    last_event_hlc: Option<String>,
    last_heartbeat: Instant,
    logs: VecDeque<WorkerLogEntry>,
}

/// Thread-safe in-memory registry holding recent status and logs for downstream consumer workers.
/// Enforces bounded memory limits per worker (default 200 logs max) and detects unresponsive workers.
#[derive(Clone)]
pub struct WorkerRegistry {
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    max_logs_per_worker: usize,
    liveness_timeout: Duration,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new(200, Duration::from_secs(15))
    }
}

impl WorkerRegistry {
    pub fn new(max_logs_per_worker: usize, liveness_timeout: Duration) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            max_logs_per_worker: if max_logs_per_worker == 0 { 200 } else { max_logs_per_worker },
            liveness_timeout,
        }
    }

    /// Records or updates a worker report. Bounded memory: caps log entries to max_logs_per_worker.
    pub fn record_report(&self, report: WorkerTelemetryReport) {
        let mut map = match self.workers.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let entry = map.entry(report.worker_id.clone()).or_insert_with(|| WorkerState {
            worker_id: report.worker_id.clone(),
            app_id: report.app_id.clone(),
            sink: report.sink.clone().unwrap_or_else(|| "unknown".to_string()),
            stream: report.stream.clone().unwrap_or_else(|| "default".to_string()),
            status: "online".to_string(),
            uptime_seconds: 0,
            processed_events: 0,
            total_errors: 0,
            last_event_hlc: None,
            last_heartbeat: Instant::now(),
            logs: VecDeque::with_capacity(self.max_logs_per_worker),
        });

        if let Some(a) = report.app_id {
            entry.app_id = Some(a);
        }
        if let Some(s) = report.sink {
            entry.sink = s;
        }
        if let Some(st) = report.stream {
            entry.stream = st;
        }
        if let Some(status) = report.status {
            entry.status = status;
        }
        if let Some(uptime) = report.uptime_seconds {
            entry.uptime_seconds = uptime;
        }
        if let Some(processed) = report.processed_events {
            entry.processed_events = processed;
        }
        if let Some(errors) = report.total_errors {
            entry.total_errors = errors;
        }
        if let Some(hlc) = report.last_event_hlc {
            entry.last_event_hlc = Some(hlc);
        }
        entry.last_heartbeat = Instant::now();

        if let Some(new_logs) = report.logs {
            for log in new_logs {
                entry.logs.push_front(log);
            }
            while entry.logs.len() > self.max_logs_per_worker {
                entry.logs.pop_back();
            }
        }
    }

    /// Retrieves up to `limit` logs for a specific worker.
    pub fn get_worker_logs(&self, worker_id: &str, limit: usize) -> Option<WorkerLogsResponse> {
        let map = match self.workers.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let state = map.get(worker_id)?;
        let elapsed = state.last_heartbeat.elapsed();
        let is_alive = elapsed <= self.liveness_timeout;

        let status = if is_alive {
            state.status.clone()
        } else {
            "offline".to_string()
        };

        let take_count = if limit == 0 { 50 } else { limit.min(state.logs.len()) };
        let logs: Vec<WorkerLogEntry> = state.logs.iter().take(take_count).cloned().collect();

        Some(WorkerLogsResponse {
            worker_id: state.worker_id.clone(),
            app_id: state.app_id.clone(),
            status,
            uptime_seconds: state.uptime_seconds,
            processed_events: state.processed_events,
            total_errors: state.total_errors,
            last_event_hlc: state.last_event_hlc.clone(),
            logs,
        })
    }

    /// Returns summaries of all known workers, marking stale ones as offline.
    pub fn get_active_workers(&self) -> Vec<WorkerSummary> {
        self.get_active_workers_with_filter(None)
    }

    /// Returns summaries of all known workers with optional app_id filtering.
    pub fn get_active_workers_with_filter(&self, app_filter: Option<&str>) -> Vec<WorkerSummary> {
        let map = match self.workers.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let mut summaries = Vec::with_capacity(map.len());
        for state in map.values() {
            if let Some(app) = app_filter {
                if !app.is_empty() && app != "all" {
                    if let Some(ref w_app) = state.app_id {
                        if !w_app.eq_ignore_ascii_case(app) {
                            continue;
                        }
                    } else if !state.worker_id.to_lowercase().contains(&app.to_lowercase()) {
                        continue;
                    }
                }
            }

            let elapsed = state.last_heartbeat.elapsed();
            let is_alive = elapsed <= self.liveness_timeout;
            let status = if is_alive {
                state.status.clone()
            } else {
                "offline".to_string()
            };

            summaries.push(WorkerSummary {
                worker_id: state.worker_id.clone(),
                app_id: state.app_id.clone(),
                sink: state.sink.clone(),
                stream: state.stream.clone(),
                status,
                uptime_seconds: state.uptime_seconds,
                processed_events: state.processed_events,
                total_errors: state.total_errors,
                last_seen_secs_ago: elapsed.as_secs(),
            });
        }
        summaries.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        summaries
    }

    /// Clears all recorded workers (for testing or reset).
    pub fn clear(&self) {
        if let Ok(mut map) = self.workers.write() {
            map.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_registry_record_and_get() {
        let registry = WorkerRegistry::new(10, Duration::from_secs(5));

        let report = WorkerTelemetryReport {
            worker_id: "test-worker-1".to_string(),
            app_id: Some("coeval".to_string()),
            sink: Some("nats".to_string()),
            stream: Some("SPECTRA".to_string()),
            status: Some("healthy".to_string()),
            uptime_seconds: Some(42),
            processed_events: Some(100),
            total_errors: Some(1),
            last_event_hlc: Some("1789266803844-000000".to_string()),
            logs: Some(vec![
                WorkerLogEntry {
                    timestamp: "2026-09-13T03:00:00Z".to_string(),
                    level: "INFO".to_string(),
                    message: "Started test worker".to_string(),
                    hlc: None,
                },
                WorkerLogEntry {
                    timestamp: "2026-09-13T03:00:01Z".to_string(),
                    level: "INFO".to_string(),
                    message: "Processed event #1".to_string(),
                    hlc: Some("1789266803844-000000".to_string()),
                },
            ]),
        };

        registry.record_report(report);

        let data = registry.get_worker_logs("test-worker-1", 10).expect("Worker should be found");
        assert_eq!(data.worker_id, "test-worker-1");
        assert_eq!(data.app_id.as_deref(), Some("coeval"));
        assert_eq!(data.status, "healthy");
        assert_eq!(data.uptime_seconds, 42);
        assert_eq!(data.processed_events, 100);
        assert_eq!(data.total_errors, 1);
        assert_eq!(data.last_event_hlc.as_deref(), Some("1789266803844-000000"));
        assert_eq!(data.logs.len(), 2);
    }

    #[test]
    fn test_worker_registry_log_capacity_bounded() {
        let registry = WorkerRegistry::new(3, Duration::from_secs(5));

        let mut logs = Vec::new();
        for i in 1..=5 {
            logs.push(WorkerLogEntry {
                timestamp: format!("2026-09-13T03:00:0{}Z", i),
                level: "INFO".to_string(),
                message: format!("Log msg {}", i),
                hlc: None,
            });
        }

        registry.record_report(WorkerTelemetryReport {
            worker_id: "bounded-worker".to_string(),
            app_id: None,
            sink: None,
            stream: None,
            status: None,
            uptime_seconds: None,
            processed_events: None,
            total_errors: None,
            last_event_hlc: None,
            logs: Some(logs),
        });

        let data = registry.get_worker_logs("bounded-worker", 10).unwrap();
        assert_eq!(data.logs.len(), 3);
        assert_eq!(data.logs[0].message, "Log msg 5");
    }

    #[test]
    fn test_worker_registry_liveness_timeout() {
        let registry = WorkerRegistry::new(10, Duration::from_millis(50));

        registry.record_report(WorkerTelemetryReport {
            worker_id: "expiring-worker".to_string(),
            app_id: None,
            sink: Some("kafka".to_string()),
            stream: Some("events".to_string()),
            status: Some("active".to_string()),
            uptime_seconds: Some(10),
            processed_events: Some(5),
            total_errors: None,
            last_event_hlc: None,
            logs: None,
        });

        let data = registry.get_worker_logs("expiring-worker", 10).unwrap();
        assert_eq!(data.status, "active");

        std::thread::sleep(Duration::from_millis(60));

        let data = registry.get_worker_logs("expiring-worker", 10).unwrap();
        assert_eq!(data.status, "offline");

        let summaries = registry.get_active_workers();
        assert_eq!(summaries[0].status, "offline");
    }

    #[test]
    fn test_worker_registry_app_filtering() {
        let registry = WorkerRegistry::new(10, Duration::from_secs(5));

        registry.record_report(WorkerTelemetryReport {
            worker_id: "w-coeval".to_string(),
            app_id: Some("coeval".to_string()),
            sink: Some("nats".to_string()),
            stream: Some("SPECTRA".to_string()),
            status: Some("active".to_string()),
            uptime_seconds: Some(10),
            processed_events: Some(5),
            total_errors: None,
            last_event_hlc: None,
            logs: None,
        });

        registry.record_report(WorkerTelemetryReport {
            worker_id: "w-humanbase".to_string(),
            app_id: Some("humanbase".to_string()),
            sink: Some("nats".to_string()),
            stream: Some("SPECTRA".to_string()),
            status: Some("active".to_string()),
            uptime_seconds: Some(10),
            processed_events: Some(5),
            total_errors: None,
            last_event_hlc: None,
            logs: None,
        });

        // Query all
        let all = registry.get_active_workers_with_filter(None);
        assert_eq!(all.len(), 2);

        // Query coeval
        let coeval = registry.get_active_workers_with_filter(Some("coeval"));
        assert_eq!(coeval.len(), 1);
        assert_eq!(coeval[0].worker_id, "w-coeval");

        // Query humanbase
        let hb = registry.get_active_workers_with_filter(Some("humanbase"));
        assert_eq!(hb.len(), 1);
        assert_eq!(hb[0].worker_id, "w-humanbase");

        // Query nonexistent
        let none = registry.get_active_workers_with_filter(Some("unknown"));
        assert_eq!(none.len(), 0);
    }
}
