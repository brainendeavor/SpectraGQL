use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use matchit::Router;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RouteDefinition {
    pub method: String,
    pub relative_path: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl RouteDefinition {
    pub fn new<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        method: S1,
        relative_path: S2,
        description: S3,
    ) -> Self {
        Self {
            method: method.into(),
            relative_path: relative_path.into(),
            description: description.into(),
            timeout_ms: None,
        }
    }

    pub fn with_timeout<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        method: S1,
        relative_path: S2,
        description: S3,
        timeout_ms: u64,
    ) -> Self {
        Self {
            method: method.into(),
            relative_path: relative_path.into(),
            description: description.into(),
            timeout_ms: Some(timeout_ms),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegisteredRoute {
    pub fluxcell_name: String,
    pub mount_path: String,
    pub method: String,
    pub relative_path: String,
    pub full_path: String,
}

#[derive(Debug, Clone)]
pub struct RouteMatch<'a> {
    pub fluxcell_name: &'a str,
    pub mount_path: &'a str,
    pub relative_path: &'a str,
    pub full_path: &'a str,
    pub params: HashMap<String, String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RouterError {
    #[error("Route collision: path '{path}' already registered by fluxcell '{existing_fluxcell}' (attempted by '{new_fluxcell}')")]
    Collision {
        path: String,
        existing_fluxcell: String,
        new_fluxcell: String,
    },
    #[error("Route not found: {method} {path}")]
    NotFound {
        method: String,
        path: String,
    },
    #[error("Method not allowed: {method} for {path} (Allowed: {})", allowed.join(", "))]
    MethodNotAllowed {
        method: String,
        path: String,
        allowed: Vec<String>,
    },
    #[error("Invalid route pattern: {0}")]
    InvalidPattern(String),
}

pub struct FluxRouter {
    // Map of METHOD -> matchit::Router<RegisteredRoute>
    routers: HashMap<String, Router<RegisteredRoute>>,
    // Set of all full paths mapped to the registering fluxcell for quick collision reporting
    registered_paths: HashMap<String, String>,
    // Map of fluxcell_name -> (mount_path, Vec<RouteDefinition>) for rebuild on unregister
    fluxcell_routes: HashMap<String, (String, Vec<RouteDefinition>)>,
}

impl FluxRouter {
    pub fn new() -> Self {
        Self {
            routers: HashMap::new(),
            registered_paths: HashMap::new(),
            fluxcell_routes: HashMap::new(),
        }
    }

    pub fn is_reserved_mount_path(mount_path: &str) -> bool {
        let clean = clean_path_prefix(mount_path);
        clean == "/admin"
            || clean.starts_with("/admin/")
            || clean == "/graphql"
            || clean == "/gql"
            || clean.starts_with("/_flux")
            || clean == "/healthz"
            || clean == "/readyz"
            || clean == "/metrics"
    }

    pub fn register_fluxcell_routes(
        &mut self,
        fluxcell_name: &str,
        mount_path: &str,
        routes: &[RouteDefinition],
    ) -> Result<(), RouterError> {
        let clean_mount = clean_path_prefix(mount_path);

        if Self::is_reserved_mount_path(&clean_mount) {
            return Err(RouterError::InvalidPattern(format!(
                "Mount path '{}' is reserved for system infrastructure",
                clean_mount
            )));
        }

        for route in routes {
            let clean_rel = clean_path_suffix(&route.relative_path);
            let full_path = if clean_mount.is_empty() && clean_rel.is_empty() {
                "/".to_string()
            } else {
                format!("{}{}", clean_mount, clean_rel)
            };
            let method = route.method.to_uppercase();

            let route_key = format!("{} {}", method, full_path);

            if let Some(existing_fluxcell) = self.registered_paths.get(&route_key) {
                return Err(RouterError::Collision {
                    path: full_path,
                    existing_fluxcell: existing_fluxcell.clone(),
                    new_fluxcell: fluxcell_name.to_string(),
                });
            }

            let reg = RegisteredRoute {
                fluxcell_name: fluxcell_name.to_string(),
                mount_path: if clean_mount.is_empty() { "/".to_string() } else { clean_mount.clone() },
                method: method.clone(),
                relative_path: if clean_rel.is_empty() { "/".to_string() } else { clean_rel },
                full_path: full_path.clone(),
            };

            let router = self.routers.entry(method).or_default();
            router
                .insert(&full_path, reg)
                .map_err(|e| RouterError::InvalidPattern(e.to_string()))?;

            self.registered_paths
                .insert(route_key, fluxcell_name.to_string());
        }

        self.fluxcell_routes
            .insert(fluxcell_name.to_string(), (mount_path.to_string(), routes.to_vec()));

        Ok(())
    }

    pub fn unregister_fluxcell_routes(&mut self, fluxcell_name: &str) -> bool {
        if self.fluxcell_routes.remove(fluxcell_name).is_some() {
            self.rebuild_routers();
            true
        } else {
            false
        }
    }

    fn rebuild_routers(&mut self) {
        self.routers.clear();
        self.registered_paths.clear();
        let old_routes = std::mem::take(&mut self.fluxcell_routes);
        for (name, (mount, routes)) in old_routes {
            let _ = self.register_fluxcell_routes(&name, &mount, &routes);
        }
    }

    pub fn list_registered_routes(&self) -> Vec<RegisteredRoute> {
        let mut list = Vec::new();
        for (name, (mount, routes)) in &self.fluxcell_routes {
            let clean_mount = clean_path_prefix(mount);
            for r in routes {
                let clean_rel = clean_path_suffix(&r.relative_path);
                let full_path = if clean_mount.is_empty() && clean_rel.is_empty() {
                    "/".to_string()
                } else {
                    format!("{}{}", clean_mount, clean_rel)
                };
                list.push(RegisteredRoute {
                    fluxcell_name: name.clone(),
                    mount_path: mount.clone(),
                    method: r.method.to_uppercase(),
                    relative_path: r.relative_path.clone(),
                    full_path,
                });
            }
        }
        list.sort_by(|a, b| a.full_path.cmp(&b.full_path));
        list
    }

    pub fn lookup<'a>(&'a self, method: &str, path: &str) -> Result<RouteMatch<'a>, RouterError> {
        let method_upper = method.to_uppercase();
        let normalized_path = if path.len() > 1 && path.ends_with('/') {
            path.trim_end_matches('/')
        } else {
            path
        };

        if let Some(router) = self.routers.get(&method_upper) {
            if let Ok(matched) = router.at(normalized_path) {
                let mut params = HashMap::new();
                for (k, v) in matched.params.iter() {
                    params.insert(k.to_string(), v.to_string());
                }
                return Ok(RouteMatch {
                    fluxcell_name: &matched.value.fluxcell_name,
                    mount_path: &matched.value.mount_path,
                    relative_path: &matched.value.relative_path,
                    full_path: &matched.value.full_path,
                    params,
                });
            }
        }

        // Check if any other registered method supports this route for RFC 9110 MethodNotAllowed
        let mut allowed = Vec::new();
        for (m, router) in &self.routers {
            if m != &method_upper && router.at(normalized_path).is_ok() {
                allowed.push(m.clone());
            }
        }

        if !allowed.is_empty() {
            allowed.sort();
            return Err(RouterError::MethodNotAllowed {
                method: method_upper,
                path: path.to_string(),
                allowed,
            });
        }

        Err(RouterError::NotFound {
            method: method_upper,
            path: path.to_string(),
        })
    }
}

impl Default for FluxRouter {
    fn default() -> Self {
        Self::new()
    }
}

pub fn clean_path_prefix(prefix: &str) -> String {
    let p = prefix.trim().trim_matches('/');
    if p.is_empty() {
        "".to_string()
    } else {
        format!("/{}", p)
    }
}

pub fn clean_path_suffix(suffix: &str) -> String {
    let s = suffix.trim().trim_matches('/');
    if s.is_empty() {
        "".to_string()
    } else {
        format!("/{}", s)
    }
}

pub fn simple_url_decode(input: &str) -> String {
    let mut result = String::new();
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let h1 = chars.next();
            let h2 = chars.next();
            if let (Some(h1), Some(h2)) = (h1, h2) {
                if let Ok(byte) = u8::from_str_radix(&format!("{}{}", h1, h2), 16) {
                    result.push(byte as char);
                    continue;
                }
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

#[async_trait::async_trait]
pub trait FluxcellHttpDispatcher: Send + Sync {
    async fn dispatch(
        &self,
        fluxcell_name: &str,
        relative_path: &str,
        method: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), anyhow::Error>;
}

pub async fn handle_request<B>(
    req: Request<B>,
    router: Arc<std::sync::RwLock<FluxRouter>>,
    telemetry: Arc<crate::telemetry::TelemetryClient>,
    dispatcher: Arc<dyn FluxcellHttpDispatcher>,
    deployer: Option<Arc<crate::deployer::FluxcellDeployer>>,
) -> Result<Response<Full<bytes::Bytes>>, std::convert::Infallible>
where
    B: hyper::body::Body + Send + 'static,
    B::Data: Send,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let method = req.method().to_string();
    let raw_path = req.uri().path().to_string();
    let query_string = req.uri().query().map(|q| q.to_string());
    let path = if raw_path.len() > 1 && raw_path.ends_with('/') {
        raw_path.trim_end_matches('/').to_string()
    } else {
        raw_path
    };

    // 1. Built-in liveness / readiness probes
    if path == "/healthz" {
        let uptime = telemetry.started_at.elapsed().as_secs();
        let body = serde_json::json!({
            "status": "ok",
            "uptime_seconds": uptime,
            "processed_events": telemetry.processed_events.load(std::sync::atomic::Ordering::Relaxed),
            "errors": telemetry.error_count.load(std::sync::atomic::Ordering::Relaxed),
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(bytes::Bytes::from(body.to_string())))
            .unwrap());
    }

    if path == "/readyz" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(bytes::Bytes::from("{\"status\":\"ready\"}")))
            .unwrap());
    }

    if path == "/metrics" {
        let uptime = telemetry.started_at.elapsed().as_secs();
        let body = serde_json::json!({
            "worker_id": telemetry.worker_id,
            "uptime_seconds": uptime,
            "processed_events": telemetry.processed_events.load(std::sync::atomic::Ordering::Relaxed),
            "errors": telemetry.error_count.load(std::sync::atomic::Ordering::Relaxed),
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(bytes::Bytes::from(body.to_string())))
            .unwrap());
    }

    if path == "/admin/logs" {
        let logs = telemetry.get_recent_logs();
        let body = serde_json::to_string(&logs).unwrap_or_else(|_| "[]".to_string());
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(bytes::Bytes::from(body)))
            .unwrap());
    }

    // 2. Deployment Governance & Admin Lockdown Controls
    if path == "/admin/api/v1/security/lockdown" && method == "POST" {
        if let Some(dep) = &deployer {
            dep.guard().emergency_lockdown();
            let body = serde_json::json!({
                "status": "locked_down",
                "external_deploy_enabled": dep.guard().is_external_deploy_allowed(),
                "dev_upload_enabled": dep.guard().is_dev_upload_allowed(),
                "message": "Emergency lockdown activated. All deployments frozen."
            });
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(body.to_string())))
                .unwrap());
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    if path == "/_flux/deployer/status" && method == "GET" {
        if let Some(dep) = &deployer {
            let records = dep.registry().list_records();
            let body = serde_json::json!({
                "external_deploy_enabled": dep.guard().is_external_deploy_allowed(),
                "dev_upload_enabled": dep.guard().is_dev_upload_allowed(),
                "auto_activate": dep.config().auto_activate,
                "fluxcells": records,
            });
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(body.to_string())))
                .unwrap());
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    if path == "/_flux/deployer/history" && method == "GET" {
        if let Some(dep) = &deployer {
            let events = dep.registry().list_audit_events();
            let body = serde_json::json!({ "events": events });
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(body.to_string())))
                .unwrap());
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    if path == "/_flux/deployer/upload" && method == "POST" {
        if let Some(dep) = &deployer {
            let mut name = req.headers().get("X-Fluxcell-Name").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            let mut mount = req.headers().get("X-Fluxcell-Mount").and_then(|v| v.to_str().ok()).map(|s| s.to_string());

            if let Some(q) = &query_string {
                for param in q.split('&') {
                    let mut kv = param.split('=');
                    if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                        if k == "name" && name.is_none() {
                            name = Some(simple_url_decode(v));
                        } else if k == "mount" && mount.is_none() {
                            mount = Some(simple_url_decode(v));
                        }
                    }
                }
            }

            let cell_name = match name {
                Some(n) if !n.trim().is_empty() => n.trim().to_string(),
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from("{\"error\":\"Missing required 'name' parameter or 'X-Fluxcell-Name' header\"}")))
                        .unwrap());
                }
            };

            let mount_path = match mount {
                Some(m) if !m.trim().is_empty() => m.trim().to_string(),
                _ => format!("/api/{}", cell_name),
            };

            let body_bytes = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes().to_vec(),
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(bytes::Bytes::from(format!("Failed to read body: {}", e))))
                        .unwrap());
                }
            };

            match dep.stage_uploaded_artifact(&cell_name, body_bytes, &mount_path, None, None) {
                Ok(record) => {
                    let body = serde_json::to_string(&record).unwrap_or_default();
                    return Ok(Response::builder()
                        .status(StatusCode::CREATED)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(body)))
                        .unwrap());
                }
                Err(e) => {
                    let status = if !dep.guard().is_dev_upload_allowed() {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    return Ok(Response::builder()
                        .status(status)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(serde_json::json!({
                            "error": "DEPLOY_ERROR",
                            "message": e.to_string()
                        }).to_string())))
                        .unwrap());
                }
            }
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    if path == "/_flux/deployer/activate" && method == "POST" {
        if let Some(dep) = &deployer {
            let body_bytes = match req.into_body().collect().await {
                Ok(collected) => collected.to_bytes().to_vec(),
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(bytes::Bytes::from(format!("Failed to read body: {}", e))))
                        .unwrap());
                }
            };

            #[derive(serde::Deserialize)]
            struct ActivateRequest {
                name: String,
                sha256: String,
            }

            let activate_req: ActivateRequest = match serde_json::from_slice(&body_bytes) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(format!("{{\"error\":\"INVALID_PAYLOAD\",\"message\":\"{}\"}}", e))))
                        .unwrap());
                }
            };

            match dep.activate(&activate_req.name, &activate_req.sha256) {
                Ok(record) => {
                    let body = serde_json::to_string(&record).unwrap_or_default();
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(body)))
                        .unwrap());
                }
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(serde_json::json!({
                            "error": "ACTIVATION_ERROR",
                            "message": e.to_string()
                        }).to_string())))
                        .unwrap());
                }
            }
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    if path.starts_with("/_flux/deployer/fluxcells") && method == "DELETE" {
        if let Some(dep) = &deployer {
            let cell_name = if path.len() > "/_flux/deployer/fluxcells/".len() {
                path["/_flux/deployer/fluxcells/".len()..].trim_matches('/').to_string()
            } else {
                let mut q_name = None;
                if let Some(q) = &query_string {
                    for param in q.split('&') {
                        let mut kv = param.split('=');
                        if let (Some("name"), Some(v)) = (kv.next(), kv.next()) {
                            q_name = Some(v.to_string());
                        }
                    }
                }
                q_name.unwrap_or_default()
            };

            if cell_name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("Content-Type", "application/json")
                    .body(Full::new(bytes::Bytes::from("{\"error\":\"Fluxcell name required\"}")))
                    .unwrap());
            }

            match dep.remove(&cell_name) {
                Ok(record) => {
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(serde_json::json!({
                            "status": "removed",
                            "fluxcell": record
                        }).to_string())))
                        .unwrap());
                }
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .body(Full::new(bytes::Bytes::from(serde_json::json!({
                            "error": "REMOVE_ERROR",
                            "message": e.to_string()
                        }).to_string())))
                        .unwrap());
                }
            }
        } else {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from("{\"error\":\"DEPLOYER_NOT_ENABLED\"}")))
                .unwrap());
        }
    }

    // 3. Lookup route in FluxRouter
    let (fluxcell_name, relative_path_matched) = {
        let router_lock = router.read().unwrap();
        match router_lock.lookup(&method, &path) {
            Ok(m) => (m.fluxcell_name.to_string(), m.relative_path.to_string()),
            Err(RouterError::NotFound { .. }) => {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("Content-Type", "application/json")
                    .body(Full::new(bytes::Bytes::from(
                        "{\"error\":\"NOT_FOUND\",\"message\":\"No fluxcell route matches request\"}",
                    )))
                    .unwrap());
            }
            Err(RouterError::MethodNotAllowed { allowed, .. }) => {
                let allow_header = allowed.join(", ");
                return Ok(Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header("Content-Type", "application/json")
                    .header("Allow", allow_header)
                    .body(Full::new(bytes::Bytes::from(
                        "{\"error\":\"METHOD_NOT_ALLOWED\"}",
                    )))
                    .unwrap());
            }
            Err(e) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "application/json")
                    .body(Full::new(bytes::Bytes::from(format!(
                        "{{\"error\":\"ROUTER_ERROR\",\"message\":\"{}\"}}",
                        e
                    ))))
                    .unwrap());
            }
        }
    };

    // 4. Collect headers & body
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.as_str().to_string(), val.to_string())))
        .collect();

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes().to_vec(),
        Err(e) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(bytes::Bytes::from(format!("Failed to read body: {}", e))))
                .unwrap());
        }
    };

    // 5. Dispatch to Fluxcell
    let relative_path = if let Some(q) = &query_string {
        format!("{}?{}", relative_path_matched, q)
    } else {
        relative_path_matched
    };

    match dispatcher
        .dispatch(&fluxcell_name, &relative_path, &method, headers, body_bytes)
        .await
    {
        Ok((status_code, resp_headers, resp_body)) => {
            let mut builder = Response::builder().status(status_code);
            for (k, v) in resp_headers {
                builder = builder.header(k, v);
            }
            Ok(builder.body(Full::new(bytes::Bytes::from(resp_body))).unwrap())
        }
        Err(e) => {
            telemetry.increment_error();
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(format!(
                    "{{\"error\":\"FLUXCELL_DISPATCH_ERROR\",\"message\":\"{}\"}}",
                    e
                ))))
                .unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDispatcher {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl FluxcellHttpDispatcher for MockDispatcher {
        async fn dispatch(
            &self,
            _fluxcell_name: &str,
            _relative_path: &str,
            _method: &str,
            _headers: Vec<(String, String)>,
            _body: Vec<u8>,
        ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), anyhow::Error> {
            if self.should_fail {
                Err(anyhow::anyhow!("Mock execution error"))
            } else {
                Ok((self.status, self.headers.clone(), self.body.clone()))
            }
        }
    }

    #[test]
    fn test_router_registration_and_lookup() {
        let mut router = FluxRouter::new();

        let magic_link_routes = vec![
            RouteDefinition::new("GET", "/verify", "Verify token"),
            RouteDefinition::new("POST", "/verify", "Redeem token"),
            RouteDefinition::new("GET", "/status", "Status"),
        ];

        let webhook_routes = vec![
            RouteDefinition::new("GET", "/health", "Health"),
            RouteDefinition::new("GET", "/dlq", "DLQ status"),
        ];

        router.register_fluxcell_routes("magic-link", "/auth", &magic_link_routes).unwrap();
        router.register_fluxcell_routes("webhook", "/api/webhooks", &webhook_routes).unwrap();

        // Verify lookup
        let m = router.lookup("GET", "/auth/verify").unwrap();
        assert_eq!(m.fluxcell_name, "magic-link");
        assert_eq!(m.mount_path, "/auth");
        assert_eq!(m.relative_path, "/verify");

        let m2 = router.lookup("GET", "/api/webhooks/dlq").unwrap();
        assert_eq!(m2.fluxcell_name, "webhook");
        assert_eq!(m2.mount_path, "/api/webhooks");
        assert_eq!(m2.relative_path, "/dlq");
    }

    #[test]
    fn test_router_collision_detection() {
        let mut router = FluxRouter::new();

        let fluxcell1_routes = vec![RouteDefinition::new("GET", "/verify", "First")];
        let fluxcell2_routes = vec![RouteDefinition::new("GET", "/verify", "Conflicting")];

        // Mount first to /auth
        router.register_fluxcell_routes("auth-v1", "/auth", &fluxcell1_routes).unwrap();

        // Attempt to mount second to /auth with overlapping route
        let err = router.register_fluxcell_routes("auth-v2", "/auth", &fluxcell2_routes).unwrap_err();
        
        match err {
            RouterError::Collision { path, existing_fluxcell, new_fluxcell } => {
                assert_eq!(path, "/auth/verify");
                assert_eq!(existing_fluxcell, "auth-v1");
                assert_eq!(new_fluxcell, "auth-v2");
            }
            other => panic!("Expected Collision error, got: {:?}", other),
        }
    }

    #[test]
    fn test_router_method_not_allowed_and_rfc9110_allow_header() {
        let mut router = FluxRouter::new();

        let routes = vec![
            RouteDefinition::new("GET", "/verify", "Check token"),
            RouteDefinition::new("POST", "/verify", "Redeem token"),
        ];

        router.register_fluxcell_routes("auth", "/auth", &routes).unwrap();

        // DELETE /auth/verify should return MethodNotAllowed with allowed: ["GET", "POST"]
        let err = router.lookup("DELETE", "/auth/verify").unwrap_err();
        match err {
            RouterError::MethodNotAllowed { method, path, allowed } => {
                assert_eq!(method, "DELETE");
                assert_eq!(path, "/auth/verify");
                assert_eq!(allowed, vec!["GET".to_string(), "POST".to_string()]);
            }
            other => panic!("Expected MethodNotAllowed, got: {:?}", other),
        }

        // Truly nonexistent path returns NotFound
        let err_not_found = router.lookup("GET", "/auth/nonexistent").unwrap_err();
        assert!(matches!(err_not_found, RouterError::NotFound { .. }));
    }

    #[test]
    fn test_router_trailing_slash_normalization() {
        let mut router = FluxRouter::new();

        let routes = vec![RouteDefinition::new("GET", "/verify", "Verify")];

        router.register_fluxcell_routes("auth", "/auth", &routes).unwrap();

        // Should resolve both with and without trailing slash
        let m1 = router.lookup("GET", "/auth/verify").unwrap();
        let m2 = router.lookup("GET", "/auth/verify/").unwrap();
        assert_eq!(m1.full_path, m2.full_path);
        assert_eq!(m1.relative_path, m2.relative_path);
    }

    #[test]
    fn test_router_root_mount_path_cleaning() {
        let mut router = FluxRouter::new();

        let routes = vec![RouteDefinition::new("GET", "/health", "Health")];

        // Mount at root "" or "/"
        router.register_fluxcell_routes("root_cell", "", &routes).unwrap();

        let m = router.lookup("GET", "/health").unwrap();
        assert_eq!(m.full_path, "/health");
        assert_eq!(m.relative_path, "/health");
    }

    #[tokio::test]
    async fn test_handle_request_probes() {
        let router = Arc::new(std::sync::RwLock::new(FluxRouter::new()));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test-1".to_string(),
            "nats".to_string(),
            None,
            100,
        ));
        telemetry.record_log("INFO", "Initialized test", None);

        let dispatcher = Arc::new(MockDispatcher {
            status: 200,
            headers: vec![],
            body: vec![],
            should_fail: false,
        });

        // 1. /healthz
        let req = Request::builder().uri("/healthz").body(Full::new(bytes::Bytes::new())).unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "ok");

        // 2. /healthz/ (with trailing slash)
        let req = Request::builder().uri("/healthz/").body(Full::new(bytes::Bytes::new())).unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 3. /readyz
        let req = Request::builder().uri("/readyz").body(Full::new(bytes::Bytes::new())).unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // 4. /metrics
        let req = Request::builder().uri("/metrics").body(Full::new(bytes::Bytes::new())).unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["worker_id"], "worker-test-1");

        // 5. /admin/logs
        let req = Request::builder().uri("/admin/logs").body(Full::new(bytes::Bytes::new())).unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Vec<serde_json::Value> = serde_json::from_slice(&body_bytes).unwrap();
        assert!(!json.is_empty());
        assert_eq!(json[0]["message"], "Initialized test");
    }

    #[tokio::test]
    async fn test_handle_request_route_dispatch_success_with_custom_headers() {
        let mut router = FluxRouter::new();
        router.register_fluxcell_routes("auth", "/auth", &[RouteDefinition::new(
            "POST",
            "/login",
            "Login redirect",
        )]).unwrap();

        let router = Arc::new(std::sync::RwLock::new(router));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test".to_string(),
            "nats".to_string(),
            None,
            100,
        ));

        let dispatcher = Arc::new(MockDispatcher {
            status: 302,
            headers: vec![
                ("Location".to_string(), "/dashboard".to_string()),
                ("Set-Cookie".to_string(), "session=abc123xyz; Path=/".to_string()),
            ],
            body: b"Redirecting...".to_vec(),
            should_fail: false,
        });

        let req = Request::builder()
            .method("POST")
            .uri("/auth/login")
            .body(Full::new(bytes::Bytes::from("{\"username\":\"admin\"}")))
            .unwrap();

        let resp = handle_request(req, router, telemetry, dispatcher, None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(resp.headers().get("Location").unwrap(), "/dashboard");
        assert_eq!(resp.headers().get("Set-Cookie").unwrap(), "session=abc123xyz; Path=/");

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes.as_ref(), b"Redirecting...");
    }

    #[tokio::test]
    async fn test_handle_request_method_not_allowed_header() {
        let mut router = FluxRouter::new();
        router.register_fluxcell_routes("auth", "/auth", &[RouteDefinition::new(
            "GET",
            "/verify",
            "Verify",
        )]).unwrap();

        let router = Arc::new(std::sync::RwLock::new(router));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test".to_string(),
            "nats".to_string(),
            None,
            100,
        ));
        let dispatcher = Arc::new(MockDispatcher {
            status: 200,
            headers: vec![],
            body: vec![],
            should_fail: false,
        });

        let req = Request::builder()
            .method("DELETE")
            .uri("/auth/verify")
            .body(Full::new(bytes::Bytes::new()))
            .unwrap();

        let resp = handle_request(req, router, telemetry, dispatcher, None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.headers().get("Allow").unwrap(), "GET");
    }

    #[tokio::test]
    async fn test_handle_request_dispatcher_error_increments_telemetry() {
        let mut router = FluxRouter::new();
        router.register_fluxcell_routes("flaky", "/flaky", &[RouteDefinition::new(
            "GET",
            "/fail",
            "Failing route",
        )]).unwrap();

        let router = Arc::new(std::sync::RwLock::new(router));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test".to_string(),
            "nats".to_string(),
            None,
            100,
        ));
        let dispatcher = Arc::new(MockDispatcher {
            status: 200,
            headers: vec![],
            body: vec![],
            should_fail: true,
        });

        let req = Request::builder()
            .method("GET")
            .uri("/flaky/fail")
            .body(Full::new(bytes::Bytes::new()))
            .unwrap();

        let resp = handle_request(req, router, telemetry.clone(), dispatcher, None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(telemetry.error_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_http_adversarial_oversized_body() {
        let mut router = FluxRouter::new();
        router.register_fluxcell_routes("echo", "/echo", &[RouteDefinition::new(
            "POST",
            "/data",
            "Echo data",
        )]).unwrap();

        let router = Arc::new(std::sync::RwLock::new(router));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test".to_string(),
            "nats".to_string(),
            None,
            100,
        ));

        // 3MB payload
        let big_body = vec![b'Z'; 3 * 1024 * 1024];
        let big_body_clone = big_body.clone();

        struct EchoDispatcher;
        #[async_trait::async_trait]
        impl FluxcellHttpDispatcher for EchoDispatcher {
            async fn dispatch(
                &self,
                _fluxcell_name: &str,
                _relative_path: &str,
                _method: &str,
                _headers: Vec<(String, String)>,
                body: Vec<u8>,
            ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), anyhow::Error> {
                Ok((200, vec![], body))
            }
        }

        let req = Request::builder()
            .method("POST")
            .uri("/echo/data")
            .body(Full::new(bytes::Bytes::from(big_body)))
            .unwrap();

        let resp = handle_request(req, router, telemetry, Arc::new(EchoDispatcher), None).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(resp_bytes.len(), big_body_clone.len());
    }

    #[tokio::test]
    async fn test_http_adversarial_path_traversal_attempts() {
        let mut router = FluxRouter::new();
        router.register_fluxcell_routes("auth", "/auth", &[RouteDefinition::new(
            "GET",
            "/verify",
            "Verify",
        )]).unwrap();

        let router = Arc::new(std::sync::RwLock::new(router));
        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "worker-test".to_string(),
            "nats".to_string(),
            None,
            100,
        ));
        let dispatcher = Arc::new(MockDispatcher {
            status: 200,
            headers: vec![],
            body: vec![],
            should_fail: false,
        });

        let malicious_uris = vec![
            "/auth/../../etc/passwd",
            "/auth/..%2f..%2fetc/shadow",
            "/api/webhooks/../../secret.key",
            "/auth/%00/verify",
        ];

        for uri in malicious_uris {
            let req = Request::builder()
                .method("GET")
                .uri(uri)
                .body(Full::new(bytes::Bytes::new()))
                .unwrap();

            let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), None).await.unwrap();
            // Should be safely rejected with 404 Not Found without panicking or path traversal
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn test_handle_request_deployer_governance_and_lockdown() {
        let temp_dir = std::env::temp_dir().join(format!("spectral_deploy_http_{}", uuid::Uuid::new_v4()));
        let mut dep_cfg = crate::config::DeployerConfig::default();
        dep_cfg.enabled = true;
        dep_cfg.storage_dir = temp_dir.to_string_lossy().to_string();
        dep_cfg.external_deploy_enabled = true;
        dep_cfg.dev_upload_enabled = true;

        let guard = Arc::new(crate::deployer::DeployerGuard::new(true, true));
        let registry = Arc::new(crate::deployer::DeployerRegistry::new(&temp_dir).unwrap());
        let wasm_host = Arc::new(crate::wasm::WasmHost::new(5, None).unwrap());
        let router = Arc::new(std::sync::RwLock::new(FluxRouter::new()));

        let deployer = Arc::new(crate::deployer::FluxcellDeployer::new(
            dep_cfg,
            guard.clone(),
            registry,
            wasm_host,
            router.clone(),
        ));

        let telemetry = Arc::new(crate::telemetry::TelemetryClient::new(
            "test-worker".to_string(),
            "nats".to_string(),
            None,
            100,
        ));
        let dispatcher = Arc::new(MockDispatcher {
            status: 200,
            headers: vec![],
            body: vec![],
            should_fail: false,
        });

        // 1. GET /_flux/deployer/status
        let req = Request::builder()
            .method("GET")
            .uri("/_flux/deployer/status")
            .body(Full::new(bytes::Bytes::new()))
            .unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), Some(deployer.clone())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["external_deploy_enabled"], true);
        assert_eq!(json["dev_upload_enabled"], true);

        // 2. POST /admin/api/v1/security/lockdown
        let req = Request::builder()
            .method("POST")
            .uri("/admin/api/v1/security/lockdown")
            .body(Full::new(bytes::Bytes::new()))
            .unwrap();
        let resp = handle_request(req, router.clone(), telemetry.clone(), dispatcher.clone(), Some(deployer.clone())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "locked_down");
        assert_eq!(json["external_deploy_enabled"], false);
        assert_eq!(json["dev_upload_enabled"], false);

        // Verify guard state changed immediately
        assert!(!guard.is_external_deploy_allowed());
        assert!(!guard.is_dev_upload_allowed());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
