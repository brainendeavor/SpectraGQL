use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpectraAppSummary {
    pub id: String,
    pub name: String,
    pub domains: Vec<String>,
    pub upstream: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminStatusResponse {
    pub version: String,
    pub uptime_seconds: u64,
    pub sync_enabled: bool,
    pub sync_dispatch_policy: String,
    pub sync_timeout_ms: u64,
    pub async_routes_count: usize,
    pub mode_a_enabled: bool,
    pub mode_a_dispatch_policy: String,
    pub mode_a_timeout_ms: u64,
    pub mode_b_routes_count: usize,
    pub broker_method: String,
    pub broker_addr: String,
    pub broker_status: String,
    #[serde(default)]
    pub apps: Vec<SpectraAppSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminRouteEntry {
    pub name: String,
    pub operation: String,
    pub mode: String,
    pub upstream: String,
    pub upstream_addr: String,
    pub receipt_status: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub dispatch_policy: String,
    #[serde(default)]
    pub is_policy_override: bool,
    #[serde(default)]
    pub interceptors: Vec<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminNamedUpstream {
    pub name: String,
    pub addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminRoutesResponse {
    pub default_upstream: String,
    pub default_upstream_addr: String,
    pub named_upstreams: Vec<AdminNamedUpstream>,
    pub routes: Vec<AdminRouteEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminMutationCoverageEntry {
    pub field_name: String,
    pub classification: String, // "AsyncReceipt", "TargetedService", "DefaultUpstream"
    pub target_upstream: Option<String>,
    pub mode: Option<String>, // "Async", "Sync"
    pub receipt_status: Option<String>,
    #[serde(default)]
    pub route_name: Option<String>,
    #[serde(default)]
    pub dispatch_policy: Option<String>,
    #[serde(default)]
    pub is_policy_override: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminSchemaCoverageResponse {
    pub total_mutations: usize,
    pub async_count: usize,
    pub targeted_count: usize,
    pub default_fallback_count: usize,
    pub strangled_count: usize,
    pub mode_b_count: usize,
    pub monolith_fallback_count: usize,
    pub drift_count: usize,
    pub coverage_percent: f64,
    pub mutations: Vec<AdminMutationCoverageEntry>,
    pub drifted_routes: Vec<String>,
    pub last_refreshed_utc: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminSubscriptionsResponse {
    pub enabled: bool,
    pub active_connections: usize,
    pub topic_prefix: String,
    pub active_topics_count: usize,
    pub active_topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminIdempotencyResponse {
    pub backend: String,
    pub ttl_secs: u64,
    pub max_capacity: usize,
    pub active_records: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigResponse {
    pub backend: String,
    pub descriptor: String,
    pub version: u64,
    pub updated_at_epoch_ms: u64,
    pub hash: String,
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigValidateRequest {
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigSummary {
    pub apps_count: usize,
    pub routes_count: usize,
    pub named_upstreams_count: usize,
    pub interceptors_count: usize,
    pub broker_method: String,
    pub broker_addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigValidateResponse {
    pub valid: bool,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    pub summary: Option<AdminConfigSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigUpdateRequest {
    pub content: String,
    #[serde(default = "default_true")]
    pub reload: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigUpdateResponse {
    pub success: bool,
    pub version: u64,
    pub hash: String,
    pub updated_at_epoch_ms: u64,
    pub reloaded: bool,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminConfigReloadResponse {
    pub success: bool,
    pub version: u64,
    pub hash: String,
    pub updated_at_epoch_ms: u64,
    pub reloaded: bool,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminDnsUpstreamEntry {
    pub name: String,
    pub target: String,
    pub current_addr: String,
    pub previous_addr: Option<String>,
    pub changed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminDnsRescanResponse {
    pub status: String,
    pub total_upstreams: usize,
    pub changed_count: usize,
    pub duration_ms: f64,
    pub upstreams: Vec<AdminDnsUpstreamEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminDnsStatusResponse {
    pub enabled: bool,
    pub interval_secs: u64,
    pub total_upstreams: usize,
    pub upstreams: Vec<AdminDnsUpstreamEntry>,
}

pub use crate::admin::eventsink::{ConsumerMetrics, EventSinkResponse, StreamMetrics};
pub use crate::admin::traffic::{TrafficRecord, TrafficResponse, TrafficStats};
