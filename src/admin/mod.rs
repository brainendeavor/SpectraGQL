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
    AdminConfigReloadResponse, AdminConfigResponse, AdminConfigSummary, AdminConfigUpdateRequest,
    AdminConfigUpdateResponse, AdminConfigValidateRequest, AdminConfigValidateResponse,
    AdminIdempotencyResponse, AdminNamedUpstream, AdminRouteEntry, AdminRoutesResponse,
    AdminStatusResponse, AdminSubscriptionsResponse,
};
use crate::admin::schema_inspector::SchemaInspector;
use crate::core::config::{SpectraAdminConfig, SpectraConfig, SpectraRouteConfig};
use crate::gateway::DynamicGatewayState;
use crate::idempotency::IdempotencyEngine;
use crate::interceptors::request::constant_time_eq;
use crate::subscriptions::SubscriptionHub;

pub const ADMIN_HTML: &str = include_str!("assets/admin.html");
pub const FAVICON_SVG: &str = include_str!("assets/favicon.svg");

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
    pub dynamic_state: Option<Arc<arc_swap::ArcSwap<crate::gateway::DynamicGatewayState>>>,
    pub config_store: Option<Arc<dyn crate::core::ConfigStore>>,
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
            dynamic_state: None,
            config_store: None,
        }
    }

    pub fn with_dynamic_state(
        mut self,
        dynamic_state: Arc<arc_swap::ArcSwap<crate::gateway::DynamicGatewayState>>,
    ) -> Self {
        self.dynamic_state = Some(dynamic_state);
        self
    }

    pub fn with_config_store(
        mut self,
        config_store: Arc<dyn crate::core::ConfigStore>,
    ) -> Self {
        self.config_store = Some(config_store);
        self
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
                AdminRoute::Favicon => self.handle_favicon(session).await,
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
                AdminRoute::ConfigGet => self.handle_config_get(session).await,
                AdminRoute::ConfigValidate => self.handle_config_validate(session).await,
                AdminRoute::ConfigUpdate => self.handle_config_update(session).await,
                AdminRoute::ConfigReload => self.handle_config_reload(session).await,
                AdminRoute::DnsStatus => self.handle_dns_status(session).await,
                AdminRoute::DnsRescan => self.handle_dns_rescan(session).await,
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

    async fn handle_favicon(&self, session: &mut Session) -> pingora::Result<bool> {
        let svg = FAVICON_SVG;
        let mut header = ResponseHeader::build(200, None)?;
        let _ = header.insert_header("content-type", "image/svg+xml");
        let _ = header.insert_header("content-length", svg.len().to_string());
        let _ = header.insert_header("cache-control", "public, max-age=86400, immutable");
        if let Err(e) = session.write_response_header(Box::new(header), false).await {
            log::debug!("Client disconnected before favicon header write: {}", e);
            return Ok(true);
        }
        if let Err(e) = session
            .write_response_body(Some(bytes::Bytes::from_static(svg.as_bytes())), true)
            .await
        {
            log::debug!("Client disconnected before favicon body write: {}", e);
            return Ok(true);
        }
        Ok(true)
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
        let (async_count, mode_a_enabled, mode_a_dispatch_policy, mode_a_timeout_ms, apps) =
            if let Some(ds) = &self.dynamic_state {
                let state = ds.load();
                let count = state
                    .routes
                    .values()
                    .filter(|r| r.mode.is_async_command_receipt())
                    .count();
                let app_summaries = state
                    .apps
                    .iter()
                    .map(|a| crate::admin::api::SpectraAppSummary {
                        id: a.id.clone(),
                        name: a.name.clone(),
                        domains: a.domains.clone(),
                        upstream: a.upstream.clone(),
                    })
                    .collect();
                (
                    count,
                    state.mode_a.enabled,
                    state.mode_a.dispatch_policy.as_str().to_string(),
                    state.mode_a.timeout_ms,
                    app_summaries,
                )
            } else {
                let count = self
                    .routes
                    .values()
                    .filter(|r| r.mode.is_async_command_receipt())
                    .count();
                (
                    count,
                    self.mode_a_enabled,
                    self.mode_a_dispatch_policy.clone(),
                    self.mode_a_timeout_ms,
                    self.apps.clone(),
                )
            };

        let status_resp = AdminStatusResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            sync_enabled: mode_a_enabled,
            sync_dispatch_policy: mode_a_dispatch_policy.clone(),
            sync_timeout_ms: mode_a_timeout_ms,
            async_routes_count: async_count,
            mode_a_enabled,
            mode_a_dispatch_policy,
            mode_a_timeout_ms,
            mode_b_routes_count: async_count,
            broker_method: self.broker_method.clone(),
            broker_addr: self.broker_addr.clone(),
            broker_status: "online".to_string(),
            apps,
        };
        self.respond_json(session, 200, &status_resp).await
    }

    async fn handle_routes(&self, session: &mut Session) -> pingora::Result<bool> {
        let (routes, named_upstreams, mode_a_dispatch_policy) = if let Some(ds) = &self.dynamic_state {
            let state = ds.load();
            (state.routes.clone(), state.named_upstreams.clone(), state.mode_a.dispatch_policy.as_str().to_string())
        } else {
            (self.routes.clone(), self.named_upstreams.clone(), self.mode_a_dispatch_policy.clone())
        };

        let mut route_entries = Vec::new();
        for (name, r) in routes.iter() {
            let target_name = r.upstream.clone().unwrap_or_else(|| "default".to_string());
            let target_addr = named_upstreams
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
                        (mode_a_dispatch_policy.clone(), false)
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
        for (name, addr) in named_upstreams.iter() {
            named_upstreams_list.push(AdminNamedUpstream {
                name: name.clone(),
                addr: addr.to_string(),
            });
        }
        named_upstreams_list.sort_by(|a, b| a.name.cmp(&b.name));

        let default_addr = named_upstreams
            .get("default")
            .map(|a| a.to_string())
            .unwrap_or_else(|| self.default_upstream_addr.clone());

        let routes_resp = AdminRoutesResponse {
            default_upstream: self.default_upstream_name.clone(),
            default_upstream_addr: default_addr,
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

    fn is_write_authorized(&self, session: &Session) -> bool {
        let expected_token = std::env::var("SPECTRA_ADMIN_TOKEN")
            .ok()
            .or_else(|| std::env::var("SPECTRA_DEPLOY_TOKEN").ok())
            .or_else(|| std::env::var("SPECTRAGQL_DEPLOY_TOKEN").ok())
            .or_else(|| self.config.deploy_token.clone());

        let expected = match expected_token {
            Some(t) if !t.trim().is_empty() => t,
            _ => {
                log::warn!("Admin write operation authorized without configured token (set SPECTRA_ADMIN_TOKEN for authenticated control plane)");
                return true;
            }
        };

        let req_header = session.req_header();
        let auth_val = req_header
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer ")))
            .or_else(|| {
                req_header
                    .headers
                    .get("x-spectra-admin-token")
                    .and_then(|v| v.to_str().ok())
            })
            .or_else(|| {
                req_header
                    .headers
                    .get("x-spectra-deploy-token")
                    .and_then(|v| v.to_str().ok())
            })
            .or_else(|| {
                req_header
                    .headers
                    .get("x-spectra-deploy-key")
                    .and_then(|v| v.to_str().ok())
            });

        match auth_val {
            Some(provided) => constant_time_eq(provided.trim().as_bytes(), expected.trim().as_bytes()),
            None => false,
        }
    }

    async fn handle_config_get(&self, session: &mut Session) -> pingora::Result<bool> {
        let backend = self
            .config_store
            .as_ref()
            .map(|cs| cs.backend_name().to_string())
            .unwrap_or_else(|| "memory".to_string());
        let descriptor = self
            .config_store
            .as_ref()
            .map(|cs| cs.descriptor())
            .unwrap_or_else(|| "in-memory (ephemeral)".to_string());

        let (version, updated_at_epoch_ms, hash, content) = if let Some(ds) = &self.dynamic_state {
            let state = ds.load();
            (
                state.version,
                state.updated_at_epoch_ms,
                state.config_hash.clone(),
                state.raw_config.as_ref().clone(),
            )
        } else {
            let raw = if let Some(store) = &self.config_store {
                store.load_config().await.ok().flatten().unwrap_or_default()
            } else {
                String::new()
            };
            (1, 0, String::new(), raw)
        };

        let resp = AdminConfigResponse {
            backend,
            descriptor,
            version,
            updated_at_epoch_ms,
            hash,
            content,
        };
        self.respond_json(session, 200, &resp).await
    }

    async fn handle_config_validate(&self, session: &mut Session) -> pingora::Result<bool> {
        let mut body_bytes = Vec::new();
        while let Some(chunk) = session.read_request_body().await? {
            body_bytes.extend_from_slice(&chunk);
            if body_bytes.len() > 2 * 1024 * 1024 {
                break;
            }
        }

        let content = match serde_json::from_slice::<AdminConfigValidateRequest>(&body_bytes) {
            Ok(req) => req.content,
            Err(_) => match String::from_utf8(body_bytes) {
                Ok(s) => s,
                Err(_) => {
                    let resp = AdminConfigValidateResponse {
                        valid: false,
                        errors: vec!["Request body is neither valid JSON nor valid UTF-8".to_string()],
                        warnings: vec![],
                        summary: None,
                    };
                    return self.respond_json(session, 400, &resp).await;
                }
            },
        };

        match SpectraConfig::from_toml_str(&content) {
            Ok(cfg) => {
                let mut warnings = Vec::new();

                if let Err(e) = cfg.resolve_all_upstreams() {
                    warnings.push(format!("Upstream resolution warning: {}", e));
                }

                for (name, r) in &cfg.gql.routes {
                    if !r.enabled {
                        warnings.push(format!("Route '{}' ({}) is disabled", name, r.operation));
                    }
                }

                let summary = AdminConfigSummary {
                    apps_count: cfg.apps.len(),
                    routes_count: cfg.gql.routes.len(),
                    named_upstreams_count: cfg.upstreams.len(),
                    interceptors_count: cfg.interceptors.len(),
                    broker_method: cfg.gql_dispatch().method.clone(),
                    broker_addr: cfg.gql_dispatch().addr.clone(),
                };

                let resp = AdminConfigValidateResponse {
                    valid: true,
                    errors: vec![],
                    warnings,
                    summary: Some(summary),
                };
                self.respond_json(session, 200, &resp).await
            }
            Err(e) => {
                let resp = AdminConfigValidateResponse {
                    valid: false,
                    errors: vec![e.to_string()],
                    warnings: vec![],
                    summary: None,
                };
                self.respond_json(session, 200, &resp).await
            }
        }
    }

    async fn handle_config_update(&self, session: &mut Session) -> pingora::Result<bool> {
        if !self.is_write_authorized(session) {
            let body = serde_json::json!({
                "error": "Unauthorized",
                "message": "Missing or invalid admin authorization token (set SPECTRA_ADMIN_TOKEN or provide valid Authorization: Bearer <token>)"
            });
            return self.respond_json(session, 401, &body).await;
        }

        let mut body_bytes = Vec::new();
        while let Some(chunk) = session.read_request_body().await? {
            body_bytes.extend_from_slice(&chunk);
            if body_bytes.len() > 2 * 1024 * 1024 {
                break;
            }
        }

        let (content, reload) = match serde_json::from_slice::<AdminConfigUpdateRequest>(&body_bytes) {
            Ok(req) => (req.content, req.reload),
            Err(_) => match String::from_utf8(body_bytes) {
                Ok(s) => (s, true),
                Err(_) => {
                    let body = serde_json::json!({
                        "error": "Bad Request",
                        "message": "Request body is neither valid JSON nor valid UTF-8"
                    });
                    return self.respond_json(session, 400, &body).await;
                }
            },
        };

        let parsed_cfg = match SpectraConfig::from_toml_str(&content) {
            Ok(cfg) => cfg,
            Err(e) => {
                let body = serde_json::json!({
                    "error": "Invalid Configuration",
                    "message": format!("Configuration parsing error: {}", e)
                });
                return self.respond_json(session, 400, &body).await;
            }
        };

        if let Some(store) = &self.config_store {
            if let Err(e) = store.save_config(&content).await {
                log::error!("Failed to persist configuration to store: {}", e);
                let body = serde_json::json!({
                    "error": "Storage Error",
                    "message": format!("Failed to persist configuration to store: {}", e)
                });
                return self.respond_json(session, 500, &body).await;
            }
        }

        let (version, hash, updated_at) = if reload {
            if let Some(dynamic_state) = &self.dynamic_state {
                let prev = dynamic_state.load();
                let new_version = prev.version + 1;
                match DynamicGatewayState::new_from_config(&parsed_cfg, content, new_version) {
                    Ok(new_state) => {
                        let v = new_state.version;
                        let h = new_state.config_hash.clone();
                        let u = new_state.updated_at_epoch_ms;
                        dynamic_state.store(Arc::new(new_state));
                        log::info!("Admin: Hot-reloaded configuration to version #{}", v);
                        (v, h, u)
                    }
                    Err(e) => {
                        log::error!("Hot reload failed after persisting config: {}", e);
                        let body = serde_json::json!({
                            "error": "Reload Error",
                            "message": format!("Configuration saved to store, but hot-reload failed: {}", e)
                        });
                        return self.respond_json(session, 500, &body).await;
                    }
                }
            } else {
                (1, "untracked".to_string(), 0)
            }
        } else {
            let (v, h, u) = if let Some(ds) = &self.dynamic_state {
                let s = ds.load();
                (s.version, s.config_hash.clone(), s.updated_at_epoch_ms)
            } else {
                (1, "untracked".to_string(), 0)
            };
            (v, h, u)
        };

        let resp = AdminConfigUpdateResponse {
            success: true,
            version,
            hash,
            updated_at_epoch_ms: updated_at,
            reloaded: reload,
            message: if reload {
                "Configuration saved and runtime hot-reloaded successfully".to_string()
            } else {
                "Configuration saved to store (hot-reload skipped)".to_string()
            },
        };
        self.respond_json(session, 200, &resp).await
    }

    async fn handle_config_reload(&self, session: &mut Session) -> pingora::Result<bool> {
        if !self.is_write_authorized(session) {
            let body = serde_json::json!({
                "error": "Unauthorized",
                "message": "Missing or invalid admin authorization token (set SPECTRA_ADMIN_TOKEN or provide valid Authorization: Bearer <token>)"
            });
            return self.respond_json(session, 401, &body).await;
        }

        let store = match &self.config_store {
            Some(s) => s,
            None => {
                let body = serde_json::json!({
                    "error": "No Config Store",
                    "message": "No persistent config store is configured on this gateway instance"
                });
                return self.respond_json(session, 400, &body).await;
            }
        };

        let content = match store.load_config().await {
            Ok(Some(c)) => c,
            Ok(None) => {
                let body = serde_json::json!({
                    "error": "Not Found",
                    "message": "No configuration found in persistent store"
                });
                return self.respond_json(session, 404, &body).await;
            }
            Err(e) => {
                let body = serde_json::json!({
                    "error": "Storage Error",
                    "message": format!("Failed to read configuration from store: {}", e)
                });
                return self.respond_json(session, 500, &body).await;
            }
        };

        let parsed_cfg = match SpectraConfig::from_toml_str(&content) {
            Ok(cfg) => cfg,
            Err(e) => {
                let body = serde_json::json!({
                    "error": "Invalid Configuration In Store",
                    "message": format!("Configuration in store failed parsing: {}", e)
                });
                return self.respond_json(session, 500, &body).await;
            }
        };

        let (version, hash, updated_at) = if let Some(dynamic_state) = &self.dynamic_state {
            let prev = dynamic_state.load();
            let new_version = prev.version + 1;
            match DynamicGatewayState::new_from_config(&parsed_cfg, content, new_version) {
                Ok(new_state) => {
                    let v = new_state.version;
                    let h = new_state.config_hash.clone();
                    let u = new_state.updated_at_epoch_ms;
                    dynamic_state.store(Arc::new(new_state));
                    log::info!("Admin: Hot-reloaded configuration from persistent store to version #{}", v);
                    (v, h, u)
                }
                Err(e) => {
                    let body = serde_json::json!({
                        "error": "Reload Error",
                        "message": format!("Failed to apply configuration from store: {}", e)
                    });
                    return self.respond_json(session, 500, &body).await;
                }
            }
        } else {
            (1, "untracked".to_string(), 0)
        };

        let resp = AdminConfigReloadResponse {
            success: true,
            version,
            hash,
            updated_at_epoch_ms: updated_at,
            reloaded: true,
            message: "Configuration reloaded from store and hot-swapped into gateway runtime".to_string(),
        };
        self.respond_json(session, 200, &resp).await
    }

    async fn handle_dns_status(&self, session: &mut Session) -> pingora::Result<bool> {
        let resp = if let Some(dynamic_state) = &self.dynamic_state {
            DynamicGatewayState::get_dns_status(dynamic_state)
        } else {
            crate::admin::api::AdminDnsStatusResponse {
                enabled: false,
                interval_secs: 0,
                total_upstreams: 0,
                upstreams: vec![],
            }
        };
        self.respond_json(session, 200, &resp).await
    }

    async fn handle_dns_rescan(&self, session: &mut Session) -> pingora::Result<bool> {
        if !self.is_write_authorized(session) {
            let body = serde_json::json!({
                "error": "Unauthorized",
                "message": "Missing or invalid admin authorization token (set SPECTRA_ADMIN_TOKEN or provide valid Authorization: Bearer <token>)"
            });
            return self.respond_json(session, 401, &body).await;
        }

        let resp = if let Some(dynamic_state) = &self.dynamic_state {
            match DynamicGatewayState::rescan_dns(dynamic_state) {
                Ok(res) => res,
                Err(e) => {
                    log::error!("DNS rescan failed: {}", e);
                    let body = serde_json::json!({
                        "error": "DNS Rescan Failed",
                        "message": format!("DNS rescan encountered an error: {}", e)
                    });
                    return self.respond_json(session, 500, &body).await;
                }
            }
        } else {
            let body = serde_json::json!({
                "error": "No Dynamic State",
                "message": "Dynamic gateway state is not initialized"
            });
            return self.respond_json(session, 500, &body).await;
        };

        self.respond_json(session, 200, &resp).await
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
                "/admin/api/v1/idempotency",
                "/admin/api/v1/config",
                "/admin/api/v1/config/validate",
                "/admin/api/v1/config/reload",
                "/admin/api/v1/dns",
                "/admin/api/v1/dns/rescan"
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
        assert!(ADMIN_HTML.contains("SPECTRA"));
        assert!(ADMIN_HTML.contains("GQL"));
        assert!(ADMIN_HTML.contains("Mutation Routing Breakdown"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/status"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/schema"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/dns/rescan"));
        assert!(ADMIN_HTML.contains("Rescan DNS"));
        assert!(ADMIN_HTML.contains("toast-notification"));
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

    #[test]
    fn test_admin_dns_responses_serialization() {
        let status = crate::admin::api::AdminDnsStatusResponse {
            enabled: true,
            interval_secs: 15,
            total_upstreams: 1,
            upstreams: vec![crate::admin::api::AdminDnsUpstreamEntry {
                name: "default".to_string(),
                target: "api.internal.service:8080".to_string(),
                current_addr: "10.0.0.2:8080".to_string(),
                previous_addr: None,
                changed: false,
            }],
        };
        let status_json = serde_json::to_string(&status).unwrap();
        assert!(status_json.contains("\"enabled\":true"));
        assert!(status_json.contains("\"interval_secs\":15"));
        assert!(status_json.contains("\"api.internal.service:8080\""));

        let rescan = crate::admin::api::AdminDnsRescanResponse {
            status: "success".to_string(),
            total_upstreams: 1,
            changed_count: 1,
            duration_ms: 2.5,
            upstreams: vec![crate::admin::api::AdminDnsUpstreamEntry {
                name: "default".to_string(),
                target: "api.internal.service:8080".to_string(),
                current_addr: "10.0.0.2:8080".to_string(),
                previous_addr: Some("10.0.0.1:8080".to_string()),
                changed: true,
            }],
        };
        let rescan_json = serde_json::to_string(&rescan).unwrap();
        assert!(rescan_json.contains("\"status\":\"success\""));
        assert!(rescan_json.contains("\"changed_count\":1"));
        assert!(rescan_json.contains("\"changed\":true"));
    }
}
