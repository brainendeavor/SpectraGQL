use std::collections::HashMap;
use matchit::Router;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminRoute {
    Dashboard,
    Favicon,
    Status,
    Routes,
    Schema,
    SchemaRefresh,
    Subscriptions,
    Idempotency,
    Eventsink,
    Traffic,
    TrafficClear,
    TelemetryReport,
    Workers,
    WorkerLogs,
    ConfigGet,
    ConfigValidate,
    ConfigUpdate,
    ConfigReload,
    DnsStatus,
    DnsRescan,
    AuthVerify,
}

#[derive(Clone)]
pub struct AdminRouter {
    get_router: Router<AdminRoute>,
    post_router: Router<AdminRoute>,
    put_router: Router<AdminRoute>,
    path_prefix: String,
}

impl AdminRouter {
    pub fn new(path_prefix: &str) -> Self {
        let clean_prefix = if path_prefix.is_empty() || path_prefix == "/" {
            "/admin".to_string()
        } else {
            path_prefix.trim_end_matches('/').to_string()
        };

        let mut get_router = Router::new();
        let mut post_router = Router::new();
        let mut put_router = Router::new();

        // UI Dashboard
        let _ = get_router.insert(&clean_prefix, AdminRoute::Dashboard);
        let _ = get_router.insert(format!("{}/", clean_prefix), AdminRoute::Dashboard);
        let _ = get_router.insert(format!("{}/index.html", clean_prefix), AdminRoute::Dashboard);
        if clean_prefix != "/admin" {
            let _ = get_router.insert("/admin", AdminRoute::Dashboard);
            let _ = get_router.insert("/admin/", AdminRoute::Dashboard);
            let _ = get_router.insert("/admin/index.html", AdminRoute::Dashboard);
        }

        // Favicon routes
        let _ = get_router.insert(format!("{}/favicon.svg", clean_prefix), AdminRoute::Favicon);
        let _ = get_router.insert(format!("{}/favicon.ico", clean_prefix), AdminRoute::Favicon);
        if clean_prefix != "/admin" {
            let _ = get_router.insert("/admin/favicon.svg", AdminRoute::Favicon);
            let _ = get_router.insert("/admin/favicon.ico", AdminRoute::Favicon);
        }

        // GET endpoints
        let get_routes = [
            ("/admin/api/v1/status", AdminRoute::Status),
            ("/admin/api/status", AdminRoute::Status),
            ("/admin/api/v1/routes", AdminRoute::Routes),
            ("/admin/api/routes", AdminRoute::Routes),
            ("/admin/api/v1/schema", AdminRoute::Schema),
            ("/admin/api/schema", AdminRoute::Schema),
            ("/admin/api/v1/subscriptions", AdminRoute::Subscriptions),
            ("/admin/api/subscriptions", AdminRoute::Subscriptions),
            ("/admin/api/v1/idempotency", AdminRoute::Idempotency),
            ("/admin/api/idempotency", AdminRoute::Idempotency),
            ("/admin/api/v1/eventsink", AdminRoute::Eventsink),
            ("/admin/api/eventsink", AdminRoute::Eventsink),
            ("/admin/api/v1/traffic", AdminRoute::Traffic),
            ("/admin/api/traffic", AdminRoute::Traffic),
            ("/admin/api/v1/workers", AdminRoute::Workers),
            ("/admin/api/workers", AdminRoute::Workers),
            ("/admin/api/v1/workers/{id}/logs", AdminRoute::WorkerLogs),
            ("/admin/api/workers/{id}/logs", AdminRoute::WorkerLogs),
            ("/admin/api/v1/config", AdminRoute::ConfigGet),
            ("/admin/api/config", AdminRoute::ConfigGet),
            ("/admin/api/v1/dns", AdminRoute::DnsStatus),
            ("/admin/api/dns", AdminRoute::DnsStatus),
            ("/admin/api/v1/auth/verify", AdminRoute::AuthVerify),
            ("/admin/api/auth/verify", AdminRoute::AuthVerify),
        ];

        for (pattern, route) in get_routes {
            let _ = get_router.insert(pattern, route);
            if clean_prefix != "/admin" {
                let custom_pattern = pattern.replacen("/admin", &clean_prefix, 1);
                let _ = get_router.insert(custom_pattern, route);
            }
        }

        // POST endpoints
        let post_routes = [
            ("/admin/api/v1/schema/refresh", AdminRoute::SchemaRefresh),
            ("/admin/api/schema/refresh", AdminRoute::SchemaRefresh),
            ("/admin/api/v1/traffic/clear", AdminRoute::TrafficClear),
            ("/admin/api/traffic/clear", AdminRoute::TrafficClear),
            ("/admin/api/v1/telemetry/report", AdminRoute::TelemetryReport),
            ("/admin/api/telemetry/report", AdminRoute::TelemetryReport),
            ("/admin/api/v1/config/validate", AdminRoute::ConfigValidate),
            ("/admin/api/config/validate", AdminRoute::ConfigValidate),
            ("/admin/api/v1/config/reload", AdminRoute::ConfigReload),
            ("/admin/api/config/reload", AdminRoute::ConfigReload),
            ("/admin/api/v1/config", AdminRoute::ConfigUpdate),
            ("/admin/api/config", AdminRoute::ConfigUpdate),
            ("/admin/api/v1/dns/rescan", AdminRoute::DnsRescan),
            ("/admin/api/dns/rescan", AdminRoute::DnsRescan),
            ("/admin/api/v1/auth/verify", AdminRoute::AuthVerify),
            ("/admin/api/auth/verify", AdminRoute::AuthVerify),
        ];

        for (pattern, route) in post_routes {
            let _ = post_router.insert(pattern, route);
            if clean_prefix != "/admin" {
                let custom_pattern = pattern.replacen("/admin", &clean_prefix, 1);
                let _ = post_router.insert(custom_pattern, route);
            }
        }

        // PUT endpoints
        let put_routes = [
            ("/admin/api/v1/config", AdminRoute::ConfigUpdate),
            ("/admin/api/config", AdminRoute::ConfigUpdate),
        ];

        for (pattern, route) in put_routes {
            let _ = put_router.insert(pattern, route);
            if clean_prefix != "/admin" {
                let custom_pattern = pattern.replacen("/admin", &clean_prefix, 1);
                let _ = put_router.insert(custom_pattern, route);
            }
        }

        AdminRouter {
            get_router,
            post_router,
            put_router,
            path_prefix: clean_prefix,
        }
    }

    pub fn match_route(
        &self,
        method: &http::Method,
        path: &str,
    ) -> Option<(AdminRoute, HashMap<String, String>)> {
        let router = match *method {
            http::Method::GET => &self.get_router,
            http::Method::POST => &self.post_router,
            http::Method::PUT => &self.put_router,
            _ => return None,
        };

        if let Ok(matched) = router.at(path) {
            let mut params = HashMap::new();
            for (k, v) in matched.params.iter() {
                params.insert(k.to_string(), v.to_string());
            }
            return Some((*matched.value, params));
        }

        // Fallback for trailing slash normalization
        if path.len() > 1 && path.ends_with('/') {
            let trimmed = path.trim_end_matches('/');
            if let Ok(matched) = router.at(trimmed) {
                let mut params = HashMap::new();
                for (k, v) in matched.params.iter() {
                    params.insert(k.to_string(), v.to_string());
                }
                return Some((*matched.value, params));
            }
        }

        None
    }

    pub fn path_prefix(&self) -> &str {
        &self.path_prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    #[test]
    fn test_admin_router_dashboard_routes() {
        let router = AdminRouter::new("/admin");

        assert_eq!(
            router.match_route(&Method::GET, "/admin").map(|(r, _)| r),
            Some(AdminRoute::Dashboard)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/").map(|(r, _)| r),
            Some(AdminRoute::Dashboard)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/index.html").map(|(r, _)| r),
            Some(AdminRoute::Dashboard)
        );
    }

    #[test]
    fn test_admin_router_api_routes() {
        let router = AdminRouter::new("/admin");

        // Status
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/status").map(|(r, _)| r),
            Some(AdminRoute::Status)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/status").map(|(r, _)| r),
            Some(AdminRoute::Status)
        );

        // Routes
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/routes").map(|(r, _)| r),
            Some(AdminRoute::Routes)
        );

        // Schema
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/schema").map(|(r, _)| r),
            Some(AdminRoute::Schema)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/schema/refresh").map(|(r, _)| r),
            Some(AdminRoute::SchemaRefresh)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/schema/refresh"),
            None
        );

        // Subscriptions
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/subscriptions").map(|(r, _)| r),
            Some(AdminRoute::Subscriptions)
        );

        // Idempotency
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/idempotency").map(|(r, _)| r),
            Some(AdminRoute::Idempotency)
        );

        // Eventsink
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/eventsink").map(|(r, _)| r),
            Some(AdminRoute::Eventsink)
        );

        // Traffic
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/traffic").map(|(r, _)| r),
            Some(AdminRoute::Traffic)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/traffic/clear").map(|(r, _)| r),
            Some(AdminRoute::TrafficClear)
        );

        // Telemetry Report
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/telemetry/report").map(|(r, _)| r),
            Some(AdminRoute::TelemetryReport)
        );

        // Workers
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/workers").map(|(r, _)| r),
            Some(AdminRoute::Workers)
        );

        // Parameterized Worker Logs
        let (route, params) = router
            .match_route(&Method::GET, "/admin/api/v1/workers/order-worker-42/logs")
            .unwrap();
        assert_eq!(route, AdminRoute::WorkerLogs);
        assert_eq!(params.get("id").map(|s| s.as_str()), Some("order-worker-42"));

        // Favicon
        assert_eq!(
            router.match_route(&Method::GET, "/admin/favicon.svg").map(|(r, _)| r),
            Some(AdminRoute::Favicon)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/favicon.ico").map(|(r, _)| r),
            Some(AdminRoute::Favicon)
        );

        // Config routes
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/config").map(|(r, _)| r),
            Some(AdminRoute::ConfigGet)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/config/validate").map(|(r, _)| r),
            Some(AdminRoute::ConfigValidate)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/config").map(|(r, _)| r),
            Some(AdminRoute::ConfigUpdate)
        );
        assert_eq!(
            router.match_route(&Method::PUT, "/admin/api/v1/config").map(|(r, _)| r),
            Some(AdminRoute::ConfigUpdate)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/config/reload").map(|(r, _)| r),
            Some(AdminRoute::ConfigReload)
        );

        // DNS routes
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/dns").map(|(r, _)| r),
            Some(AdminRoute::DnsStatus)
        );
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/dns/rescan").map(|(r, _)| r),
            Some(AdminRoute::DnsRescan)
        );

        // Auth Verify routes
        assert_eq!(
            router.match_route(&Method::POST, "/admin/api/v1/auth/verify").map(|(r, _)| r),
            Some(AdminRoute::AuthVerify)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/admin/api/v1/auth/verify").map(|(r, _)| r),
            Some(AdminRoute::AuthVerify)
        );

        // Unknown
        assert_eq!(router.match_route(&Method::GET, "/admin/api/unknown"), None);
    }

    #[test]
    fn test_admin_router_custom_prefix() {
        let router = AdminRouter::new("/spectra-admin");

        assert_eq!(
            router.match_route(&Method::GET, "/spectra-admin").map(|(r, _)| r),
            Some(AdminRoute::Dashboard)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/spectra-admin/favicon.svg").map(|(r, _)| r),
            Some(AdminRoute::Favicon)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/spectra-admin/favicon.ico").map(|(r, _)| r),
            Some(AdminRoute::Favicon)
        );
        assert_eq!(
            router.match_route(&Method::GET, "/spectra-admin/api/v1/status").map(|(r, _)| r),
            Some(AdminRoute::Status)
        );
    }
}
