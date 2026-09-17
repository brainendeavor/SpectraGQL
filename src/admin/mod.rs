pub mod api;
pub mod eventsink;
pub mod registry;
pub mod router;
pub mod schema_inspector;
pub mod traffic;

pub use eventsink::{ConsumerMetrics, EventSinkInspector, EventSinkResponse, StreamMetrics};
pub use registry::{WorkerLogEntry, WorkerLogsResponse, WorkerRegistry, WorkerSummary, WorkerTelemetryReport};
pub use router::{AdminRoute, AdminRouter};
pub use traffic::{TrafficRecord, TrafficRecorder, TrafficResponse, TrafficStats, format_timestamp, now_epoch_ms};

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use pingora::http::ResponseHeader;
use pingora::proxy::Session;

use crate::admin::api::{
    AdminIdempotencyResponse, AdminNamedUpstream, AdminRouteEntry, AdminRoutesResponse,
    AdminStatusResponse, AdminSubscriptionsResponse,
};
use crate::admin::schema_inspector::SchemaInspector;
use crate::core::config::{SpectraAdminConfig, SpectraConfig, SpectraRouteConfig};
use crate::idempotency::IdempotencyEngine;
use crate::subscriptions::SubscriptionHub;

pub const ADMIN_HTML: &str = include_str!("assets/admin.html");

/// Checks if a client IP address matches any pattern in the allowlist.
/// Supports both exact IP matches ("127.0.0.1", "::1") and CIDR ranges ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16").
pub fn is_ip_allowed(client_ip: &IpAddr, allowlist: &[String]) -> bool {
    for entry in allowlist {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        if let Some((net_str, prefix_str)) = entry.split_once('/') {
            let prefix_len: u32 = match prefix_str.parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            match (client_ip, net_str.parse::<IpAddr>()) {
                (IpAddr::V4(client_v4), Ok(IpAddr::V4(net_v4))) => {
                    if prefix_len > 32 {
                        continue;
                    }
                    if prefix_len == 0 {
                        return true;
                    }
                    let client_num = u32::from(*client_v4);
                    let net_num = u32::from(net_v4);
                    let mask = !((1u64 << (32 - prefix_len)) - 1) as u32;
                    if (client_num & mask) == (net_num & mask) {
                        return true;
                    }
                }
                (IpAddr::V6(client_v6), Ok(IpAddr::V6(net_v6))) => {
                    if prefix_len > 128 {
                        continue;
                    }
                    if prefix_len == 0 {
                        return true;
                    }
                    let client_num = u128::from(*client_v6);
                    let net_num = u128::from(net_v6);
                    let mask = !((1u128 << (128 - prefix_len)) - 1);
                    if (client_num & mask) == (net_num & mask) {
                        return true;
                    }
                }
                _ => {}
            }
        } else if let Ok(allowed_ip) = entry.parse::<IpAddr>() {
            if client_ip == &allowed_ip {
                return true;
            }
        }
    }
    false
}

/// Extracts the client IP from standard proxy headers or session socket.
pub fn extract_client_ip(session: &Session) -> IpAddr {
    // 1. Check X-Forwarded-For header
    if let Some(xff) = session.req_header().headers.get("x-forwarded-for") {
        if let Ok(xff_str) = xff.to_str() {
            if let Some(first) = xff_str.split(',').next() {
                if let Ok(ip) = first.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    // 2. Check X-Real-IP header
    if let Some(xri) = session.req_header().headers.get("x-real-ip") {
        if let Ok(xri_str) = xri.to_str() {
            if let Ok(ip) = xri_str.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }

    // 3. Check session socket if available
    if let Some(addr) = session.client_addr() {
        if let Some(sock_addr) = addr.as_inet() {
            return sock_addr.ip();
        }
    }

    // Default fallback to 127.0.0.1
    IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
}

#[derive(Clone)]
pub struct AdminEngine {
    pub config: SpectraAdminConfig,
    pub start_time: Instant,
    pub schema_inspector: SchemaInspector,
    pub default_upstream_addr: String,
    pub default_upstream_name: String,
    pub named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
    pub routes: Arc<HashMap<String, SpectraRouteConfig>>,
    pub broker_method: String,
    pub broker_addr: String,
    pub mode_a_enabled: bool,
    pub mode_a_dispatch_policy: String,
    pub mode_a_timeout_ms: u64,
    pub traffic_recorder: Arc<TrafficRecorder>,
    pub eventsink_inspector: EventSinkInspector,
    pub worker_registry: Arc<WorkerRegistry>,
    pub apps: Vec<crate::admin::api::SpectraAppSummary>,
    pub router: AdminRouter,
}

impl AdminEngine {
    pub(crate) fn new(
        config: SpectraAdminConfig,
        spectra_cfg: &SpectraConfig,
        named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
        routes: Arc<HashMap<String, SpectraRouteConfig>>,
        traffic_recorder: Arc<TrafficRecorder>,
        worker_registry: Arc<WorkerRegistry>,
    ) -> Self {
        let schema_inspector = SchemaInspector::new();
        let default_upstream_addr = spectra_cfg.gql_upstream().addr.clone();
        let default_upstream_name = spectra_cfg.gql_upstream().name.clone();
        let broker_method = spectra_cfg.gql_dispatch().method.clone();
        let broker_addr = spectra_cfg.gql_dispatch().addr.clone();
        let eventsink_inspector = EventSinkInspector::new(&broker_method, &broker_addr);
        let router = AdminRouter::new(&config.path_prefix);
        let apps = spectra_cfg
            .apps
            .iter()
            .map(|a| crate::admin::api::SpectraAppSummary {
                id: a.id.clone(),
                name: if a.name.is_empty() { a.id.clone() } else { a.name.clone() },
                domains: a.domains.clone(),
                upstream: a.upstream.clone(),
            })
            .collect();

        AdminEngine {
            config,
            start_time: Instant::now(),
            schema_inspector,
            default_upstream_addr,
            default_upstream_name,
            named_upstreams,
            routes,
            broker_method,
            broker_addr,
            mode_a_enabled: spectra_cfg.gql.mode_a.enabled,
            mode_a_dispatch_policy: spectra_cfg.gql.mode_a.dispatch_policy.as_str().to_string(),
            mode_a_timeout_ms: spectra_cfg.gql.mode_a.timeout_ms,
            traffic_recorder,
            eventsink_inspector,
            worker_registry,
            apps,
            router,
        }
    }

    /// Handles an incoming administrative request directly in Pingora request_filter.
    /// Returns Ok(true) indicating the request was handled and response sent.
    pub async fn handle_request(
        &self,
        session: &mut Session,
        idempotency_engine: &IdempotencyEngine,
        subscription_hub: &SubscriptionHub,
    ) -> pingora::Result<bool> {
        let client_ip = extract_client_ip(session);
        if !is_ip_allowed(&client_ip, &self.config.allowed_ips) {
            return self.handle_forbidden(session, &client_ip).await;
        }

        let path = session.req_header().uri.path().to_string();
        let method = session.req_header().method.clone();

        if let Some((route, params)) = self.router.match_route(&method, &path) {
            match route {
                AdminRoute::Dashboard => self.handle_dashboard(session).await,
                AdminRoute::Status => self.handle_status(session).await,
                AdminRoute::Routes => self.handle_routes(session).await,
                AdminRoute::Schema => self.handle_schema(session).await,
                AdminRoute::SchemaRefresh => self.handle_schema_refresh(session).await,
                AdminRoute::Subscriptions => {
                    self.handle_subscriptions(session, subscription_hub).await
                }
                AdminRoute::Idempotency => {
                    self.handle_idempotency(session, idempotency_engine).await
                }
                AdminRoute::Eventsink => self.handle_eventsink(session).await,
                AdminRoute::Traffic => self.handle_traffic(session).await,
                AdminRoute::TrafficClear => self.handle_traffic_clear(session).await,
                AdminRoute::TelemetryReport => self.handle_telemetry_report(session).await,
                AdminRoute::Workers => self.handle_workers(session).await,
                AdminRoute::WorkerLogs => {
                    let worker_id = params.get("id").map(|s| s.as_str()).unwrap_or("");
                    self.handle_worker_logs(session, worker_id).await
                }
            }
        } else {
            self.handle_not_found(session, &path).await
        }
    }

    async fn handle_forbidden(
        &self,
        session: &mut Session,
        client_ip: &IpAddr,
    ) -> pingora::Result<bool> {
        log::warn!(
            "Admin access denied for client IP: {} (not in allowed_ips)",
            client_ip
        );
        let body = serde_json::json!({
            "error": "Forbidden",
            "message": format!("Access denied for IP '{}'. Configure allowed_ips in spectra.toml to grant access.", client_ip)
        });
        self.respond_json(session, 403, &body).await
    }

    async fn handle_dashboard(&self, session: &mut Session) -> pingora::Result<bool> {
        if self.config.enable_ui {
            let mut header = ResponseHeader::build(200, None)?;
            let _ = header.insert_header("content-type", "text/html; charset=utf-8");
            let _ = header.insert_header("content-length", ADMIN_HTML.len().to_string());
            if let Err(e) = session.write_response_header(Box::new(header), false).await {
                log::debug!("Client disconnected before dashboard header write: {}", e);
                return Ok(true);
            }
            if let Err(e) = session
                .write_response_body(Some(bytes::Bytes::from(ADMIN_HTML)), true)
                .await
            {
                log::debug!("Client disconnected before dashboard body write: {}", e);
                return Ok(true);
            }
            Ok(true)
        } else {
            let body = serde_json::json!({
                "error": "Not Found",
                "message": "Admin UI is disabled in spectra.toml (enable_ui = false)"
            });
            self.respond_json(session, 404, &body).await
        }
    }

    async fn handle_status(&self, session: &mut Session) -> pingora::Result<bool> {
        let async_count = self
            .routes
            .values()
            .filter(|r| r.mode.is_async_command_receipt())
            .count();

        let status_resp = AdminStatusResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            sync_enabled: self.mode_a_enabled,
            sync_dispatch_policy: self.mode_a_dispatch_policy.clone(),
            sync_timeout_ms: self.mode_a_timeout_ms,
            async_routes_count: async_count,
            mode_a_enabled: self.mode_a_enabled,
            mode_a_dispatch_policy: self.mode_a_dispatch_policy.clone(),
            mode_a_timeout_ms: self.mode_a_timeout_ms,
            mode_b_routes_count: async_count,
            broker_method: self.broker_method.clone(),
            broker_addr: self.broker_addr.clone(),
            broker_status: "online".to_string(),
            apps: self.apps.clone(),
        };
        self.respond_json(session, 200, &status_resp).await
    }

    async fn handle_routes(&self, session: &mut Session) -> pingora::Result<bool> {
        let mut route_entries = Vec::new();
        for (name, r) in self.routes.iter() {
            let target_name = r.upstream.clone().unwrap_or_else(|| "default".to_string());
            let target_addr = self
                .named_upstreams
                .get(&target_name)
                .map(|a| a.to_string())
                .unwrap_or_else(|| self.default_upstream_addr.clone());

            let (dispatch_policy, is_policy_override) = match r.mode {
                crate::core::types::ExecutionStrategy::AsyncCommandReceipt => {
                    ("event_sink".to_string(), false)
                }
                crate::core::types::ExecutionStrategy::SyncUpstreamExecution => {
                    if let Some(policy) = r.dispatch_policy {
                        (policy.as_str().to_string(), true)
                    } else {
                        (self.mode_a_dispatch_policy.clone(), false)
                    }
                }
            };

            route_entries.push(AdminRouteEntry {
                name: name.clone(),
                operation: r.operation.clone(),
                mode: r.mode.display_name().to_string(),
                upstream: target_name,
                upstream_addr: target_addr,
                receipt_status: r.receipt_status.clone(),
                enabled: r.enabled,
                dispatch_policy,
                is_policy_override,
                interceptors: r.interceptors.clone(),
            });
        }
        route_entries.sort_by(|a, b| a.name.cmp(&b.name));

        let mut named_upstreams_list = Vec::new();
        for (name, addr) in self.named_upstreams.iter() {
            named_upstreams_list.push(AdminNamedUpstream {
                name: name.clone(),
                addr: addr.to_string(),
            });
        }
        named_upstreams_list.sort_by(|a, b| a.name.cmp(&b.name));

        let routes_resp = AdminRoutesResponse {
            default_upstream: self.default_upstream_name.clone(),
            default_upstream_addr: self.default_upstream_addr.clone(),
            named_upstreams: named_upstreams_list,
            routes: route_entries,
        };
        self.respond_json(session, 200, &routes_resp).await
    }

    async fn handle_schema(&self, session: &mut Session) -> pingora::Result<bool> {
        if let Some(coverage) = self.schema_inspector.get_coverage().await {
            return self.respond_json(session, 200, &coverage).await;
        }

        // Attempt on-demand refresh
        let upstream_url = format!("http://{}/graphql", self.default_upstream_addr);
        match self
            .schema_inspector
            .refresh(&upstream_url, &self.routes)
            .await
        {
            Ok(coverage) => self.respond_json(session, 200, &coverage).await,
            Err(err) => {
                let err_body = serde_json::json!({
                    "error": "Upstream Introspection Unavailable",
                    "details": err,
                    "upstream_url": upstream_url,
                    "suggestion": "Ensure the upstream GraphQL service is running and accessible."
                });
                self.respond_json(session, 502, &err_body).await
            }
        }
    }

    async fn handle_schema_refresh(&self, session: &mut Session) -> pingora::Result<bool> {
        let upstream_url = format!("http://{}/graphql", self.default_upstream_addr);
        match self
            .schema_inspector
            .refresh(&upstream_url, &self.routes)
            .await
        {
            Ok(coverage) => self.respond_json(session, 200, &coverage).await,
            Err(err) => {
                let err_body = serde_json::json!({
                    "error": "Introspection Refresh Failed",
                    "details": err,
                    "upstream_url": upstream_url
                });
                self.respond_json(session, 502, &err_body).await
            }
        }
    }

    async fn handle_subscriptions(
        &self,
        session: &mut Session,
        subscription_hub: &SubscriptionHub,
    ) -> pingora::Result<bool> {
        let active_conns = subscription_hub.active_connection_count().await;
        let active_topics = subscription_hub.active_topics().await;

        let subs_resp = AdminSubscriptionsResponse {
            enabled: true,
            active_connections: active_conns,
            topic_prefix: "spectra".to_string(),
            active_topics_count: active_topics.len(),
            active_topics,
        };
        self.respond_json(session, 200, &subs_resp).await
    }

    async fn handle_idempotency(
        &self,
        session: &mut Session,
        idempotency_engine: &IdempotencyEngine,
    ) -> pingora::Result<bool> {
        let idemp_resp = AdminIdempotencyResponse {
            backend: idempotency_engine.backend_name().to_string(),
            ttl_secs: idempotency_engine.ttl_secs(),
            max_capacity: idempotency_engine.max_capacity(),
            active_records: idempotency_engine.active_record_count(),
        };
        self.respond_json(session, 200, &idemp_resp).await
    }

    async fn handle_eventsink(&self, session: &mut Session) -> pingora::Result<bool> {
        let mut eventsink_resp = self.eventsink_inspector.inspect().await;

        // Merge active workers reporting via Telemetry API into consumer list if not already present
        for worker in self.worker_registry.get_active_workers() {
            if !eventsink_resp.consumers.iter().any(|c| c.name == worker.worker_id) {
                eventsink_resp.consumers.push(ConsumerMetrics {
                    name: worker.worker_id.clone(),
                    stream_name: worker.stream.clone(),
                    created: format!("via Telemetry API (uptime {}s)", worker.uptime_seconds),
                    filter_subject: Some(format!("{}.*", worker.stream)),
                    num_pending: 0,
                    num_ack_pending: 0,
                    num_redelivered: 0,
                    num_waiting: 0,
                    ack_floor_seq: 0,
                    last_delivered_seq: worker.processed_events,
                    push_bound: worker.status != "offline",
                    status: Some(worker.status.clone()),
                });
            }
        }

        self.respond_json(session, 200, &eventsink_resp).await
    }

    async fn handle_traffic(&self, session: &mut Session) -> pingora::Result<bool> {
        let app_param = session
            .req_header()
            .uri
            .query()
            .and_then(|q| {
                q.split('&').find_map(|pair| {
                    let mut parts = pair.split('=');
                    if parts.next()? == "app" {
                        parts.next()
                    } else {
                        None
                    }
                })
            });
        let traffic_resp = self.traffic_recorder.get_response_with_filter(50, app_param);
        self.respond_json(session, 200, &traffic_resp).await
    }

    async fn handle_traffic_clear(&self, session: &mut Session) -> pingora::Result<bool> {
        self.traffic_recorder.clear();
        let clear_resp = serde_json::json!({ "status": "cleared" });
        self.respond_json(session, 200, &clear_resp).await
    }

    async fn handle_telemetry_report(&self, session: &mut Session) -> pingora::Result<bool> {
        let mut body_bytes = Vec::new();
        while let Some(chunk) = session.read_request_body().await? {
            body_bytes.extend_from_slice(&chunk);
            if body_bytes.len() > 1024 * 1024 {
                break;
            }
        }

        match serde_json::from_slice::<crate::admin::WorkerTelemetryReport>(&body_bytes) {
            Ok(report) => {
                log::debug!(
                    "Received telemetry report from worker '{}' (status: {:?}, logs: {})",
                    report.worker_id,
                    report.status,
                    report.logs.as_ref().map(|l| l.len()).unwrap_or(0)
                );
                self.worker_registry.record_report(report);
                let ok_resp = serde_json::json!({ "status": "accepted" });
                self.respond_json(session, 200, &ok_resp).await
            }
            Err(err) => {
                let err_resp = serde_json::json!({
                    "error": "Invalid telemetry report JSON",
                    "details": err.to_string()
                });
                self.respond_json(session, 400, &err_resp).await
            }
        }
    }

    async fn handle_workers(&self, session: &mut Session) -> pingora::Result<bool> {
        let app_param = session
            .req_header()
            .uri
            .query()
            .and_then(|q| {
                q.split('&').find_map(|pair| {
                    let mut parts = pair.split('=');
                    if parts.next()? == "app" {
                        parts.next()
                    } else {
                        None
                    }
                })
            });
        let mut workers = self.worker_registry.get_active_workers_with_filter(app_param);
        if workers.is_empty() {
            // Zero-Touch Broker-Native Discovery: Synthesize worker summaries from broker consumers
            let eventsink_resp = self.eventsink_inspector.inspect().await;
            for c in eventsink_resp.consumers {
                let status = c.status.clone().unwrap_or_else(|| c.compute_status());
                workers.push(crate::admin::WorkerSummary {
                    worker_id: c.name,
                    app_id: None,
                    sink: eventsink_resp.broker_type.clone(),
                    stream: c.stream_name,
                    status,
                    uptime_seconds: 0,
                    processed_events: c.ack_floor_seq,
                    total_errors: c.num_redelivered as u64,
                    last_seen_secs_ago: 0,
                });
            }
        }
        self.respond_json(session, 200, &workers).await
    }

    async fn handle_worker_logs(&self, session: &mut Session, worker_id: &str) -> pingora::Result<bool> {
        if !worker_id.is_empty() {
            // 1. Check in-memory WorkerRegistry first (works for all sinks: NATS, Kafka, Redis, SierraDB, Iggy)
            if let Some(logs_data) = self.worker_registry.get_worker_logs(worker_id, 100) {
                return self.respond_json(session, 200, &logs_data).await;
            }

            // 2. Fallback to native broker query (e.g. NATS Request-Reply SWTP)
            match self.eventsink_inspector.query_worker_logs(worker_id, 100).await {
                Ok(logs_data) => return self.respond_json(session, 200, &logs_data).await,
                Err(err) => {
                    let err_body = serde_json::json!({
                        "workerId": worker_id,
                        "status": "unavailable",
                        "error": err
                    });
                    return self.respond_json(session, 502, &err_body).await;
                }
            }
        }
        self.handle_not_found(session, &format!("/admin/api/v1/workers/{}/logs", worker_id)).await
    }

    async fn handle_not_found(&self, session: &mut Session, path: &str) -> pingora::Result<bool> {
        let body = serde_json::json!({
            "error": "Not Found",
            "path": path,
            "available_endpoints": [
                "/admin",
                "/admin/api/v1/status",
                "/admin/api/v1/eventsink",
                "/admin/api/v1/traffic",
                "/admin/api/v1/traffic/clear",
                "/admin/api/v1/workers",
                "/admin/api/v1/workers/:id/logs",
                "/admin/api/v1/telemetry/report",
                "/admin/api/v1/routes",
                "/admin/api/v1/schema",
                "/admin/api/v1/schema/refresh",
                "/admin/api/v1/subscriptions",
                "/admin/api/v1/idempotency"
            ]
        });
        self.respond_json(session, 404, &body).await
    }

    async fn respond_json<T: serde::Serialize>(
        &self,
        session: &mut Session,
        status: u16,
        data: &T,
    ) -> pingora::Result<bool> {
        let body_bytes = serde_json::to_vec(data).unwrap_or_else(|_| b"{}".to_vec());
        let mut header = ResponseHeader::build(status, None)?;
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header("content-length", body_bytes.len().to_string());
        let _ = header.insert_header("cache-control", "no-store");

        if let Err(e) = session.write_response_header(Box::new(header), false).await {
            log::debug!("Client disconnected before admin response header write: {}", e);
            return Ok(true);
        }

        if let Err(e) = session
            .write_response_body(Some(bytes::Bytes::from(body_bytes)), true)
            .await
        {
            log::debug!("Client disconnected before admin response body write: {}", e);
            return Ok(true);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_ip_allowed_exact_ipv4() {
        let allowlist = vec!["127.0.0.1".to_string(), "192.168.1.100".to_string()];

        assert!(is_ip_allowed(
            &"127.0.0.1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        assert!(is_ip_allowed(
            &"192.168.1.100".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        assert!(!is_ip_allowed(
            &"192.168.1.101".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        assert!(!is_ip_allowed(
            &"10.0.0.1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
    }

    #[test]
    fn test_is_ip_allowed_cidr_ranges() {
        let allowlist = vec![
            "10.0.0.0/8".to_string(),
            "172.16.0.0/12".to_string(),
            "192.168.0.0/16".to_string(),
        ];

        // 10.x.x.x
        assert!(is_ip_allowed(
            &"10.5.20.1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        // 172.16-31.x.x
        assert!(is_ip_allowed(
            &"172.20.1.1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        assert!(!is_ip_allowed(
            &"172.32.1.1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        // 192.168.x.x
        assert!(is_ip_allowed(
            &"192.168.10.5".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        // Public IP
        assert!(!is_ip_allowed(
            &"203.0.113.5".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
    }

    #[test]
    fn test_is_ip_allowed_ipv6() {
        let allowlist = vec!["::1".to_string(), "fe80::/10".to_string()];

        assert!(is_ip_allowed(&"::1".parse::<IpAddr>().unwrap(), &allowlist));
        assert!(is_ip_allowed(
            &"fe80::1ff:fe23:4567".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
        assert!(!is_ip_allowed(
            &"2001:db8::1".parse::<IpAddr>().unwrap(),
            &allowlist
        ));
    }

    #[test]
    fn test_admin_html_embedding() {
        assert!(ADMIN_HTML.contains("<!DOCTYPE html>"));
        assert!(ADMIN_HTML.contains("SpectraGQL"));
        assert!(ADMIN_HTML.contains("Appliance Gateway Admin"));
        assert!(ADMIN_HTML.contains("Mutation Routing Breakdown"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/status"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/schema"));
    }

    #[test]
    fn test_admin_responses_serialization() {
        let status = AdminStatusResponse {
            version: "0.1.0".to_string(),
            uptime_seconds: 120,
            sync_enabled: true,
            sync_dispatch_policy: "ResponseWithFailure".to_string(),
            sync_timeout_ms: 3000,
            async_routes_count: 1,
            mode_a_enabled: true,
            mode_a_dispatch_policy: "ResponseWithFailure".to_string(),
            mode_a_timeout_ms: 3000,
            mode_b_routes_count: 1,
            broker_method: "NATS".to_string(),
            broker_addr: "127.0.0.1:4222".to_string(),
            broker_status: "online".to_string(),
            apps: vec![],
        };
        let status_json = serde_json::to_string(&status).unwrap();
        assert!(status_json.contains("\"version\":\"0.1.0\""));
        assert!(status_json.contains("\"broker_method\":\"NATS\""));
        assert!(status_json.contains("\"sync_enabled\":true"));

        let routes = AdminRoutesResponse {
            default_upstream: "core".to_string(),
            default_upstream_addr: "127.0.0.1:4000".to_string(),
            named_upstreams: vec![AdminNamedUpstream {
                name: "inventory".to_string(),
                addr: "127.0.0.1:5001".to_string(),
            }],
            routes: vec![AdminRouteEntry {
                name: "inv_upd".to_string(),
                operation: "adjustInventory".to_string(),
                mode: "Sync".to_string(),
                upstream: "inventory".to_string(),
                upstream_addr: "127.0.0.1:5001".to_string(),
                receipt_status: "ACCEPTED".to_string(),
                enabled: true,
                dispatch_policy: "response_with_failure".to_string(),
                is_policy_override: false,
                interceptors: vec![],
            }],
        };
        let routes_json = serde_json::to_string(&routes).unwrap();
        assert!(routes_json.contains("\"adjustInventory\""));
        assert!(routes_json.contains("\"inventory\""));
        assert!(routes_json.contains("\"dispatch_policy\":\"response_with_failure\""));

        let idemp = AdminIdempotencyResponse {
            backend: "memory".to_string(),
            ttl_secs: 300,
            max_capacity: 10000,
            active_records: 42,
        };
        let idemp_json = serde_json::to_string(&idemp).unwrap();
        assert!(idemp_json.contains("\"backend\":\"memory\""));
        assert!(idemp_json.contains("\"active_records\":42"));
    }
}
