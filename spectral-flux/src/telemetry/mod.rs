use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLogEntry {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hlc: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub struct TelemetryClient {
    pub worker_id: String,
    pub sink: String,
    pub stream: Option<String>,
    pub started_at: Instant,
    pub processed_events: AtomicU64,
    pub error_count: AtomicU64,
    pub last_hlc: Mutex<Option<String>>,
    log_buffer: Mutex<VecDeque<WorkerLogEntry>>,
    buffer_capacity: usize,
    http_client: reqwest::Client,
}

impl TelemetryClient {
    pub fn new(
        worker_id: String,
        sink: String,
        stream: Option<String>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            worker_id,
            sink,
            stream,
            started_at: Instant::now(),
            processed_events: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_hlc: Mutex::new(None),
            log_buffer: Mutex::new(VecDeque::with_capacity(buffer_capacity)),
            buffer_capacity,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn record_log(&self, level: &str, message: &str, hlc: Option<String>) {
        let entry = WorkerLogEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            level: level.to_string(),
            message: message.to_string(),
            hlc,
        };
        if let Ok(mut buf) = self.log_buffer.lock() {
            if buf.len() >= self.buffer_capacity {
                buf.pop_front();
            }
            buf.push_back(entry);
        }
    }

    pub fn get_recent_logs(&self) -> Vec<WorkerLogEntry> {
        if let Ok(buf) = self.log_buffer.lock() {
            buf.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    pub fn increment_processed(&self, hlc: Option<&str>) {
        self.processed_events.fetch_add(1, Ordering::Relaxed);
        if let Some(h) = hlc {
            if let Ok(mut l) = self.last_hlc.lock() {
                *l = Some(h.to_string());
            }
        }
    }

    pub fn increment_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn build_report(&self) -> WorkerTelemetryReport {
        let last_hlc = self.last_hlc.lock().ok().and_then(|h| h.clone());
        WorkerTelemetryReport {
            worker_id: self.worker_id.clone(),
            app_id: Some("spectral-flux".to_string()),
            sink: Some(self.sink.clone()),
            stream: self.stream.clone(),
            status: Some("running".to_string()),
            uptime_seconds: Some(self.started_at.elapsed().as_secs()),
            processed_events: Some(self.processed_events.load(Ordering::Relaxed)),
            total_errors: Some(self.error_count.load(Ordering::Relaxed)),
            last_event_hlc: last_hlc,
            logs: Some(self.get_recent_logs()),
        }
    }

    pub async fn send_heartbeat(&self, gateway_admin_url: &str) -> Result<()> {
        let report = self.build_report();
        let target_url = format!("{}/admin/api/v1/telemetry/report", gateway_admin_url.trim_end_matches('/'));
        let res = self
            .http_client
            .post(&target_url)
            .json(&report)
            .send()
            .await?;

        if !res.status().is_success() {
            log::debug!("Telemetry heartbeat rejected by gateway: status {}", res.status());
        }
        Ok(())
    }

    pub fn start_heartbeat_task(
        self: Arc<Self>,
        gateway_admin_url: String,
        interval: Duration,
        stop_signal: Arc<AtomicBool>,
    ) {
        tokio::spawn(async move {
            while !stop_signal.load(Ordering::Relaxed) {
                tokio::time::sleep(interval).await;
                if let Err(e) = self.send_heartbeat(&gateway_admin_url).await {
                    log::debug!("Telemetry heartbeat failed: {}", e);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_client_ring_buffer_and_report() {
        let client = TelemetryClient::new(
            "worker-1".to_string(),
            "NATS".to_string(),
            Some("mutations".to_string()),
            3,
        );

        client.record_log("INFO", "Starting up", None);
        client.record_log("INFO", "Connected", None);
        client.record_log("INFO", "Ready", None);
        client.record_log("INFO", "Event 1 received", Some("hlc-100".to_string()));

        let logs = client.get_recent_logs();
        // Capped at 3 entries
        assert_eq!(logs.len(), 3);
        assert_eq!(logs[0].message, "Connected");
        assert_eq!(logs[2].message, "Event 1 received");

        client.increment_processed(Some("hlc-100"));
        client.increment_error();

        let report = client.build_report();
        assert_eq!(report.worker_id, "worker-1");
        assert_eq!(report.processed_events, Some(1));
        assert_eq!(report.total_errors, Some(1));
        assert_eq!(report.last_event_hlc, Some("hlc-100".to_string()));
    }

    #[test]
    fn test_telemetry_report_json_schema_matches_gateway() {
        let client = TelemetryClient::new(
            "flux-worker-7".to_string(),
            "kafka".to_string(),
            Some("mutations.v1".to_string()),
            10,
        );

        client.record_log("WARN", "Network latency spike", Some("hlc-200".to_string()));
        client.increment_processed(Some("hlc-200"));
        client.increment_error();

        let report = client.build_report();
        let json_val = serde_json::to_value(&report).unwrap();

        // Verify camelCase JSON keys matching gateway WorkerRegistry schema
        assert_eq!(json_val["workerId"], "flux-worker-7");
        assert_eq!(json_val["appId"], "spectral-flux");
        assert_eq!(json_val["sink"], "kafka");
        assert_eq!(json_val["stream"], "mutations.v1");
        assert_eq!(json_val["status"], "running");
        assert_eq!(json_val["processedEvents"], 1);
        assert_eq!(json_val["totalErrors"], 1);
        assert_eq!(json_val["lastEventHlc"], "hlc-200");

        let logs = json_val["logs"].as_array().unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0]["level"], "WARN");
        assert_eq!(logs[0]["message"], "Network latency spike");
        assert_eq!(logs[0]["hlc"], "hlc-200");
    }

    #[test]
    fn test_telemetry_hlc_monotonic_updates() {
        let client = TelemetryClient::new("worker-hlc".to_string(), "nats".to_string(), None, 5);

        assert_eq!(client.build_report().last_event_hlc, None);

        client.increment_processed(Some("100-node1"));
        assert_eq!(client.build_report().last_event_hlc.as_deref(), Some("100-node1"));

        client.increment_processed(Some("101-node1"));
        assert_eq!(client.build_report().last_event_hlc.as_deref(), Some("101-node1"));

        // Event processed without HLC leaves existing last_hlc intact
        client.increment_processed(None);
        assert_eq!(client.build_report().last_event_hlc.as_deref(), Some("101-node1"));
    }
}
