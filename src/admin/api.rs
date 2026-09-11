use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminStatusResponse {
    pub version: String,
    pub uptime_seconds: u64,
    pub mode_a_enabled: bool,
    pub mode_a_dispatch_policy: String,
    pub mode_a_timeout_ms: u64,
    pub mode_b_routes_count: usize,
    pub broker_method: String,
    pub broker_addr: String,
    pub broker_status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminRouteEntry {
    pub name: String,
    pub operation: String,
    pub mode: String,
    pub upstream: String,
    pub upstream_addr: String,
    pub receipt_status: String,
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
    pub classification: String, // "Strangled", "EdgeTerminatedModeB", "MonolithFallback"
    pub target_upstream: Option<String>,
    pub mode: Option<String>,
    pub receipt_status: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminSchemaCoverageResponse {
    pub total_mutations: usize,
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
