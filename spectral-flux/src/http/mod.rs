use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use matchit::Router;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDefinition {
    pub method: String,
    pub relative_path: String,
    pub description: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, thiserror::Error)]
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
    #[error("Method not allowed: {method} for {path}")]
    MethodNotAllowed {
        method: String,
        path: String,
    },
    #[error("Invalid route pattern: {0}")]
    InvalidPattern(String),
}

pub struct FluxRouter {
    // Map of METHOD -> matchit::Router<RegisteredRoute>
    routers: HashMap<String, Router<RegisteredRoute>>,
    // Set of all full paths mapped to the registering fluxcell for quick collision reporting
    registered_paths: HashMap<String, String>,
}

impl FluxRouter {
    pub fn new() -> Self {
        Self {
            routers: HashMap::new(),
            registered_paths: HashMap::new(),
        }
    }

    pub fn register_fluxcell_routes(
        &mut self,
        fluxcell_name: &str,
        mount_path: &str,
        routes: &[RouteDefinition],
    ) -> Result<(), RouterError> {
        let clean_mount = clean_path_prefix(mount_path);

        for route in routes {
            let clean_rel = clean_path_suffix(&route.relative_path);
            let full_path = format!("{}{}", clean_mount, clean_rel);
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
                mount_path: clean_mount.clone(),
                method: method.clone(),
                relative_path: clean_rel,
                full_path: full_path.clone(),
            };

            let router = self.routers.entry(method).or_default();
            router
                .insert(&full_path, reg)
                .map_err(|e| RouterError::InvalidPattern(e.to_string()))?;

            self.registered_paths
                .insert(route_key, fluxcell_name.to_string());
        }

        Ok(())
    }

    pub fn lookup<'a>(&'a self, method: &str, path: &str) -> Result<RouteMatch<'a>, RouterError> {
        let method_upper = method.to_uppercase();
        let router = self.routers.get(&method_upper).ok_or_else(|| {
            RouterError::NotFound {
                method: method_upper.clone(),
                path: path.to_string(),
            }
        })?;

        match router.at(path) {
            Ok(matched) => {
                let mut params = HashMap::new();
                for (k, v) in matched.params.iter() {
                    params.insert(k.to_string(), v.to_string());
                }
                Ok(RouteMatch {
                    fluxcell_name: &matched.value.fluxcell_name,
                    mount_path: &matched.value.mount_path,
                    relative_path: &matched.value.relative_path,
                    full_path: &matched.value.full_path,
                    params,
                })
            }
            Err(matchit::MatchError::NotFound) => Err(RouterError::NotFound {
                method: method_upper,
                path: path.to_string(),
            }),
        }
    }
}

impl Default for FluxRouter {
    fn default() -> Self {
        Self::new()
    }
}

fn clean_path_prefix(prefix: &str) -> String {
    let mut p = prefix.trim();
    if !p.starts_with('/') {
        p = &p[..];
        return format!("/{}", p.trim_end_matches('/'));
    }
    p.trim_end_matches('/').to_string()
}

fn clean_path_suffix(suffix: &str) -> String {
    let s = suffix.trim();
    if !s.starts_with('/') {
        format!("/{}", s)
    } else {
        s.to_string()
    }
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

pub async fn handle_request(
    req: Request<Incoming>,
    router: Arc<FluxRouter>,
    telemetry: Arc<crate::telemetry::TelemetryClient>,
    dispatcher: Arc<dyn FluxcellHttpDispatcher>,
) -> Result<Response<Full<bytes::Bytes>>, std::convert::Infallible> {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();

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

    // 2. Lookup route in FluxRouter
    let route_match = match router.lookup(&method, &path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(
                    "{\"error\":\"NOT_FOUND\",\"message\":\"No fluxcell route matches request\"}",
                )))
                .unwrap());
        }
        Err(RouterError::MethodNotAllowed { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("Content-Type", "application/json")
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
    };

    // 3. Collect headers & body
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

    // 4. Dispatch to Fluxcell
    let fluxcell_name = route_match.fluxcell_name.to_string();
    let relative_path = route_match.relative_path.to_string();

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

    #[test]
    fn test_router_registration_and_lookup() {
        let mut router = FluxRouter::new();

        let magic_link_routes = vec![
            RouteDefinition {
                method: "GET".to_string(),
                relative_path: "/verify".to_string(),
                description: "Verify token".to_string(),
            },
            RouteDefinition {
                method: "POST".to_string(),
                relative_path: "/verify".to_string(),
                description: "Redeem token".to_string(),
            },
            RouteDefinition {
                method: "GET".to_string(),
                relative_path: "/status".to_string(),
                description: "Status".to_string(),
            },
        ];

        let webhook_routes = vec![
            RouteDefinition {
                method: "GET".to_string(),
                relative_path: "/health".to_string(),
                description: "Health".to_string(),
            },
            RouteDefinition {
                method: "GET".to_string(),
                relative_path: "/dlq".to_string(),
                description: "DLQ status".to_string(),
            },
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

        // Verify 404
        assert!(router.lookup("GET", "/auth/nonexistent").is_err());
        assert!(router.lookup("DELETE", "/auth/verify").is_err());
    }

    #[test]
    fn test_router_collision_detection() {
        let mut router = FluxRouter::new();

        let fluxcell1_routes = vec![RouteDefinition {
            method: "GET".to_string(),
            relative_path: "/verify".to_string(),
            description: "First".to_string(),
        }];

        let fluxcell2_routes = vec![RouteDefinition {
            method: "GET".to_string(),
            relative_path: "/verify".to_string(),
            description: "Conflicting".to_string(),
        }];

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
}
