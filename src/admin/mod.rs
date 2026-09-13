pub mod api;
pub mod eventsink;
pub mod schema_inspector;
pub mod traffic;

pub use eventsink::{ConsumerMetrics, EventSinkInspector, EventSinkResponse, StreamMetrics};
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
}

impl AdminEngine {
    pub(crate) fn new(
        config: SpectraAdminConfig,
        spectra_cfg: &SpectraConfig,
        named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
        routes: Arc<HashMap<String, SpectraRouteConfig>>,
        traffic_recorder: Arc<TrafficRecorder>,
    ) -> Self {
        let schema_inspector = SchemaInspector::new();
        let default_upstream_addr = spectra_cfg.gql_upstream().addr.clone();
        let default_upstream_name = spectra_cfg.gql_upstream().name.clone();
        let broker_method = spectra_cfg.gql_dispatch().method.clone();
        let broker_addr = spectra_cfg.gql_dispatch().addr.clone();
        let eventsink_inspector = EventSinkInspector::new(&broker_method, &broker_addr);

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
            mode_a_dispatch_policy: format!("{:?}", spectra_cfg.gql.mode_a.dispatch_policy),
            mode_a_timeout_ms: spectra_cfg.gql.mode_a.timeout_ms,
            traffic_recorder,
            eventsink_inspector,
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
            log::warn!(
                "Admin access denied for client IP: {} (not in allowed_ips)",
                client_ip
            );
            let mut header = ResponseHeader::build(403, None).unwrap();
            let _ = header.insert_header("content-type", "application/json");
            session.set_keepalive(None);
            session.write_response_header(Box::new(header), false).await?;
            let body = serde_json::json!({
                "error": "Forbidden",
                "message": format!("Access denied for IP '{}'. Configure allowed_ips in spectra.toml to grant access.", client_ip)
            });
            session
                .write_response_body(Some(bytes::Bytes::from(body.to_string())), true)
                .await?;
            return Ok(true);
        }

        let path = session.req_header().uri.path().to_string();
        let method = session.req_header().method.clone();

        // UI Dashboard
        if path == self.config.path_prefix
            || path == format!("{}/", self.config.path_prefix)
            || path == format!("{}/index.html", self.config.path_prefix)
        {
            if self.config.enable_ui {
                let mut header = ResponseHeader::build(200, None).unwrap();
                let _ = header.insert_header("content-type", "text/html; charset=utf-8");
                session.set_keepalive(None);
                session.write_response_header(Box::new(header), false).await?;
                session
                    .write_response_body(Some(bytes::Bytes::from(ADMIN_HTML)), true)
                    .await?;
                return Ok(true);
            } else {
                let mut header = ResponseHeader::build(404, None).unwrap();
                let _ = header.insert_header("content-type", "application/json");
                session.set_keepalive(None);
                session.write_response_header(Box::new(header), false).await?;
                let body = serde_json::json!({
                    "error": "Not Found",
                    "message": "Admin UI is disabled in spectra.toml (enable_ui = false)"
                });
                session
                    .write_response_body(Some(bytes::Bytes::from(body.to_string())), true)
                    .await?;
                return Ok(true);
            }
        }

        // REST API: GET /admin/api/v1/status
        if path == "/admin/api/v1/status" || path == "/admin/api/status" {
            let mode_b_count = self
                .routes
                .values()
                .filter(|r| r.mode == crate::core::types::OperationMode::B)
                .count();

            let status_resp = AdminStatusResponse {
                version: env!("CARGO_PKG_VERSION").to_string(),
                uptime_seconds: self.start_time.elapsed().as_secs(),
                mode_a_enabled: self.mode_a_enabled,
                mode_a_dispatch_policy: self.mode_a_dispatch_policy.clone(),
                mode_a_timeout_ms: self.mode_a_timeout_ms,
                mode_b_routes_count: mode_b_count,
                broker_method: self.broker_method.clone(),
                broker_addr: self.broker_addr.clone(),
                broker_status: "online".to_string(),
            };
            return self.respond_json(session, 200, &status_resp).await;
        }

        // REST API: GET /admin/api/v1/routes
        if path == "/admin/api/v1/routes" || path == "/admin/api/routes" {
            let mut route_entries = Vec::new();
            for (name, r) in self.routes.iter() {
                let target_name = r.upstream.clone().unwrap_or_else(|| "default".to_string());
                let target_addr = self
                    .named_upstreams
                    .get(&target_name)
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| self.default_upstream_addr.clone());

                route_entries.push(AdminRouteEntry {
                    name: name.clone(),
                    operation: r.operation.clone(),
                    mode: format!("{:?}", r.mode),
                    upstream: target_name,
                    upstream_addr: target_addr,
                    receipt_status: r.receipt_status.clone(),
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
            return self.respond_json(session, 200, &routes_resp).await;
        }

        // REST API: GET /admin/api/v1/schema & POST /admin/api/v1/schema/refresh
        if path == "/admin/api/v1/schema" || path == "/admin/api/schema" {
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
                Ok(coverage) => return self.respond_json(session, 200, &coverage).await,
                Err(err) => {
                    let err_body = serde_json::json!({
                        "error": "Upstream Introspection Unavailable",
                        "details": err,
                        "upstream_url": upstream_url,
                        "suggestion": "Ensure the upstream GraphQL service is running and accessible."
                    });
                    return self.respond_json(session, 502, &err_body).await;
                }
            }
        }

        if (path == "/admin/api/v1/schema/refresh" || path == "/admin/api/schema/refresh")
            && method == http::Method::POST
        {
            let upstream_url = format!("http://{}/graphql", self.default_upstream_addr);
            match self
                .schema_inspector
                .refresh(&upstream_url, &self.routes)
                .await
            {
                Ok(coverage) => return self.respond_json(session, 200, &coverage).await,
                Err(err) => {
                    let err_body = serde_json::json!({
                        "error": "Introspection Refresh Failed",
                        "details": err,
                        "upstream_url": upstream_url
                    });
                    return self.respond_json(session, 502, &err_body).await;
                }
            }
        }

        // REST API: GET /admin/api/v1/subscriptions
        if path == "/admin/api/v1/subscriptions" || path == "/admin/api/subscriptions" {
            let active_conns = subscription_hub.active_connection_count().await;
            let active_topics = subscription_hub.active_topics().await;

            let subs_resp = AdminSubscriptionsResponse {
                enabled: true,
                active_connections: active_conns,
                topic_prefix: "spectra".to_string(),
                active_topics_count: active_topics.len(),
                active_topics,
            };
            return self.respond_json(session, 200, &subs_resp).await;
        }

        // REST API: GET /admin/api/v1/idempotency
        if path == "/admin/api/v1/idempotency" || path == "/admin/api/idempotency" {
            let idemp_resp = AdminIdempotencyResponse {
                backend: idempotency_engine.backend_name().to_string(),
                ttl_secs: idempotency_engine.ttl_secs(),
                max_capacity: idempotency_engine.max_capacity(),
                active_records: idempotency_engine.active_record_count(),
            };
            return self.respond_json(session, 200, &idemp_resp).await;
        }

        // REST API: GET /admin/api/v1/eventsink
        if path == "/admin/api/v1/eventsink" || path == "/admin/api/eventsink" {
            let eventsink_resp = self.eventsink_inspector.inspect().await;
            return self.respond_json(session, 200, &eventsink_resp).await;
        }

        // REST API: GET /admin/api/v1/traffic
        if path == "/admin/api/v1/traffic" || path == "/admin/api/traffic" {
            let traffic_resp = self.traffic_recorder.get_response(50);
            return self.respond_json(session, 200, &traffic_resp).await;
        }

        // REST API: POST /admin/api/v1/traffic/clear
        if (path == "/admin/api/v1/traffic/clear" || path == "/admin/api/traffic/clear")
            && method == http::Method::POST
        {
            self.traffic_recorder.clear();
            let clear_resp = serde_json::json!({ "status": "cleared" });
            return self.respond_json(session, 200, &clear_resp).await;
        }

        // REST API: GET /admin/api/v1/workers/:id/logs
        if path.starts_with("/admin/api/v1/workers/") && path.ends_with("/logs") {
            let worker_id = path
                .strip_prefix("/admin/api/v1/workers/")
                .unwrap_or("")
                .strip_suffix("/logs")
                .unwrap_or("");
            if !worker_id.is_empty() {
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
        }

        // Unknown admin route
        let mut header = ResponseHeader::build(404, None).unwrap();
        let _ = header.insert_header("content-type", "application/json");
        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;
        let body = serde_json::json!({
            "error": "Not Found",
            "path": path,
            "available_endpoints": [
                "/admin",
                "/admin/api/v1/status",
                "/admin/api/v1/eventsink",
                "/admin/api/v1/traffic",
                "/admin/api/v1/traffic/clear",
                "/admin/api/v1/workers/:id/logs",
                "/admin/api/v1/routes",
                "/admin/api/v1/schema",
                "/admin/api/v1/schema/refresh",
                "/admin/api/v1/subscriptions",
                "/admin/api/v1/idempotency"
            ]
        });
        session
            .write_response_body(Some(bytes::Bytes::from(body.to_string())), true)
            .await?;
        Ok(true)
    }

    async fn respond_json<T: serde::Serialize>(
        &self,
        session: &mut Session,
        status: u16,
        data: &T,
    ) -> pingora::Result<bool> {
        let mut header = ResponseHeader::build(status, None).unwrap();
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header("cache-control", "no-store");
        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;

        let body_bytes = serde_json::to_vec(data).unwrap_or_else(|_| b"{}".to_vec());
        session
            .write_response_body(Some(bytes::Bytes::from(body_bytes)), true)
            .await?;
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
        assert!(ADMIN_HTML.contains("Strangler-Fig Coverage"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/status"));
        assert!(ADMIN_HTML.contains("/admin/api/v1/schema"));
    }

    #[test]
    fn test_admin_responses_serialization() {
        let status = AdminStatusResponse {
            version: "0.1.0".to_string(),
            uptime_seconds: 120,
            mode_a_enabled: true,
            mode_a_dispatch_policy: "ResponseWithFailure".to_string(),
            mode_a_timeout_ms: 3000,
            mode_b_routes_count: 1,
            broker_method: "NATS".to_string(),
            broker_addr: "127.0.0.1:4222".to_string(),
            broker_status: "online".to_string(),
        };
        let status_json = serde_json::to_string(&status).unwrap();
        assert!(status_json.contains("\"version\":\"0.1.0\""));
        assert!(status_json.contains("\"broker_method\":\"NATS\""));

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
                mode: "A".to_string(),
                upstream: "inventory".to_string(),
                upstream_addr: "127.0.0.1:5001".to_string(),
                receipt_status: "ACCEPTED".to_string(),
            }],
        };
        let routes_json = serde_json::to_string(&routes).unwrap();
        assert!(routes_json.contains("\"adjustInventory\""));
        assert!(routes_json.contains("\"inventory\""));

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
