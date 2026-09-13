use crate::core::clock::HlcClock;
use crate::core::config::{SpectraModeAConfig, SpectraRouteConfig, SpectraSubscriptionsConfig};
use crate::gateway::filters::{
    AdminFilter, HealthFilter, IdempotencyFilter, IdempotencyInterceptResult, StrategyRouter,
    TelemetryDispatcher,
};
use crate::gateway::SpectraProxyService;
use crate::gateway::{
    ExtraServiceParams, PathRouter, ProxyService, REQUEST_ID_HEADER, SpectraProxyCtx,
    new_proxy_service,
};
use crate::idempotency::IdempotencyEngine;
use crate::protocol::{ResponseBody, RequestDecoder};
use crate::telemetry::DispatchHandler;

use anyhow::{Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use pingora::ErrorType::ConnectNoRoute;
use pingora::http::ResponseHeader;
use pingora::proxy::{ProxyHttp, Session};
use pingora::upstreams::peer::HttpPeer;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::ServiceHandle;

#[derive(Clone)]
pub struct ServiceConfig {
    pub service: Arc<ProxyService>,
    pub upstream_addr: std::net::SocketAddr,
    pub upstream_routes: String,
}

#[derive(Clone)]
pub struct CompositeServiceProxy {
    upstream_services: Vec<ServiceConfig>,
    router: PathRouter,
    pub idempotency_engine: Arc<IdempotencyEngine>,
    pub named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
    pub mode_a: SpectraModeAConfig,
    pub routes: Arc<HashMap<String, SpectraRouteConfig>>,
    pub subscription_hub: Arc<crate::subscriptions::SubscriptionHub>,
    pub subscriptions_config: SpectraSubscriptionsConfig,
    pub admin_engine: Option<crate::admin::AdminEngine>,
    pub interceptor_manager: Arc<crate::interceptors::InterceptorManager>,
    pub traffic_recorder: Arc<crate::admin::TrafficRecorder>,
}

impl ServiceConfig {
    pub fn new(
        service_type: &str,
        upstream_address: &str,
        upstream_routes: &str,
        dispatch_method: &str,
        dispatch_address: &str,
        extra_params: ExtraServiceParams,
    ) -> Self {
        let upstream_addr = upstream_address.to_socket_addrs().unwrap().next().unwrap();
        let proxy_service =
            new_proxy_service(service_type, dispatch_method, dispatch_address, extra_params).unwrap();
        let service = Arc::new(proxy_service);

        ServiceConfig {
            service,
            upstream_addr,
            upstream_routes: upstream_routes.to_string(),
        }
    }
}

impl CompositeServiceProxy {
    pub fn new() -> Self {
        CompositeServiceProxy {
            upstream_services: vec![],
            router: PathRouter::new(),
            idempotency_engine: Arc::new(IdempotencyEngine::default()),
            named_upstreams: Arc::new(HashMap::new()),
            mode_a: SpectraModeAConfig::default(),
            routes: Arc::new(HashMap::new()),
            subscription_hub: Arc::new(crate::subscriptions::SubscriptionHub::new()),
            subscriptions_config: SpectraSubscriptionsConfig::default(),
            admin_engine: None,
            interceptor_manager: Arc::new(crate::interceptors::InterceptorManager::empty()),
            traffic_recorder: Arc::new(crate::admin::TrafficRecorder::new(250)),
        }
    }

    pub fn with_routing(
        mut self,
        named_upstreams: HashMap<String, std::net::SocketAddr>,
        mode_a: SpectraModeAConfig,
        routes: HashMap<String, SpectraRouteConfig>,
    ) -> Self {
        self.named_upstreams = Arc::new(named_upstreams);
        self.mode_a = mode_a;
        self.routes = Arc::new(routes);
        self
    }

    pub fn with_idempotency_engine(
        mut self,
        idempotency_engine: Arc<IdempotencyEngine>,
    ) -> Self {
        self.idempotency_engine = idempotency_engine;
        self
    }

    pub fn with_subscriptions(
        mut self,
        subscription_hub: Arc<crate::subscriptions::SubscriptionHub>,
        subscriptions_config: SpectraSubscriptionsConfig,
    ) -> Self {
        self.subscription_hub = subscription_hub;
        self.subscriptions_config = subscriptions_config;
        self
    }

    pub fn with_admin(mut self, admin_engine: crate::admin::AdminEngine) -> Self {
        self.admin_engine = Some(admin_engine);
        self
    }

    pub fn with_interceptor_manager(
        mut self,
        interceptor_manager: Arc<crate::interceptors::InterceptorManager>,
    ) -> Self {
        self.interceptor_manager = interceptor_manager;
        self
    }

    pub fn with_traffic_recorder(
        mut self,
        traffic_recorder: Arc<crate::admin::TrafficRecorder>,
    ) -> Self {
        self.traffic_recorder = traffic_recorder;
        self
    }

    pub fn add_service_config(&mut self, service_config: ServiceConfig) {
        let service_handle = self.upstream_services.len();
        let routes = service_config.upstream_routes.clone();
        for route in routes.split(',') {
            self.router
                .add_service_handle(route.to_string().trim(), service_handle)
                .expect("Failed to add service at {} to composite service.")
        }
        self.upstream_services.push(service_config);
    }

    pub fn get_service_config(&self, service_handle: ServiceHandle) -> &ServiceConfig {
        &self.upstream_services[service_handle]
    }

    pub fn get_service_handle_by_path(&self, path: &str) -> Option<ServiceHandle> {
        self.router.get_service_handle(path)
    }

    pub fn get_service_config_and_handle_by_path(
        &self,
        path: &str,
    ) -> Result<(&ServiceConfig, ServiceHandle)> {
        match self.get_service_handle_by_path(path) {
            Some(service_handle) => {
                return Ok((self.get_service_config(service_handle), service_handle));
            }
            None => {
                bail!("No proxy service for {}", path);
            }
        }
    }

    #[allow(dead_code)]
    pub fn subscription_hub(&self) -> Arc<crate::subscriptions::SubscriptionHub> {
        self.subscription_hub.clone()
    }

    pub async fn handle_websocket_subscription(&self, session: &mut Session) -> pingora::Result<bool> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let sec_key = session
            .req_header()
            .headers
            .get("sec-websocket-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if sec_key.is_empty() {
            let resp = pingora::http::ResponseHeader::build(400, None).unwrap();
            session.write_response_header(Box::new(resp), false).await?;
            session.write_response_body(Some(bytes::Bytes::from("Missing Sec-WebSocket-Key")), true).await?;
            return Ok(true);
        }

        let accept_key = crate::subscriptions::compute_accept_key(&sec_key);

        let requested_subprotocol = session
            .req_header()
            .headers
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut resp = pingora::http::ResponseHeader::build(101, None).unwrap();
        let _ = resp.insert_header(http::header::CONNECTION, "Upgrade");
        let _ = resp.insert_header(http::header::UPGRADE, "websocket");
        let _ = resp.insert_header("sec-websocket-accept", accept_key);
        if let Some(subprotocol) = requested_subprotocol {
            if subprotocol.contains("graphql-transport-ws") {
                let _ = resp.insert_header("sec-websocket-protocol", "graphql-transport-ws");
            } else if let Some(first) = subprotocol.split(',').next() {
                let _ = resp.insert_header("sec-websocket-protocol", first.trim());
            }
        }

        session.write_response_header(Box::new(resp), false).await?;

        let (ws_io, mut pingora_io) = tokio::io::duplex(64 * 1024);
        let actor = crate::subscriptions::ConnectionActor::new(
            self.subscription_hub.clone(),
            self.subscriptions_config.topic_prefix.clone(),
            self.subscriptions_config.keepalive_secs,
            self.subscriptions_config.client_buffer_capacity,
        );

        let ws_stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            ws_io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        ).await;

        let mut actor_handle = tokio::spawn(async move {
            let _ = actor.run(ws_stream).await;
        });

        let mut buf = [0u8; 8192];

        loop {
            tokio::select! {
                read_res = session.as_downstream_mut().read_body_or_idle(false) => {
                    match read_res {
                        Ok(Some(bytes)) => {
                            if bytes.is_empty() {
                                break;
                            }
                            if let Err(e) = pingora_io.write_all(&bytes).await {
                                log::debug!("WebSocket pump: write to ws_io failed: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            log::debug!("WebSocket pump: downstream read error: {}", e);
                            break;
                        }
                    }
                }

                duplex_read = pingora_io.read(&mut buf) => {
                    match duplex_read {
                        Ok(n) if n > 0 => {
                            let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                            if let Err(e) = session.write_response_body(Some(chunk), false).await {
                                log::debug!("WebSocket pump: downstream write error: {}", e);
                                break;
                            }
                        }
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            log::debug!("WebSocket pump: duplex read error: {}", e);
                            break;
                        }
                    }
                }

                _ = &mut actor_handle => {
                    break;
                }
            }
        }

        actor_handle.abort();
        Ok(true)
    }
}

// TODO: absorb SpectraProxyCtx
pub struct CompositeServiceProxyCtx {
    pub proxy_context: SpectraProxyCtx,
    pub service_handle: Option<ServiceHandle>,
    pub service: Option<Arc<ProxyService>>,
}

#[async_trait]
impl ProxyHttp for CompositeServiceProxy {
    type CTX = CompositeServiceProxyCtx;
    fn new_ctx(&self) -> Self::CTX {
        let (request_id, hlc) = HlcClock::global().now_uuidv7();
        let proxy_context = SpectraProxyCtx {
            request_id,
            hlc,
            start_time: std::time::Instant::now(),
            request_topic: "spectra".to_string(),
            request_info: None,
            response_parts: None,
            response_body: None,
            buffer: vec![],
            idempotency_key: None,
            is_replay: false,
            target_upstream_addr: None,
            is_mode_b_terminated: false,
            dispatch_policy: self.mode_a.dispatch_policy,
            active_operation: None,
            has_response_interception: false,
            query_preview: None,
            variables_preview: None,
        };
        CompositeServiceProxyCtx {
            proxy_context,
            service_handle: None,
            service: None,
        }
    }

    async fn early_request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let req = session.req_header();
        let path = req.uri.path();

        // 1. Health check probes do not need an upstream service
        if HealthFilter::is_health_probe(path) {
            return Ok(());
        }

        // 2. Admin Engine requests do not need an upstream service
        if AdminFilter::is_admin_request(self.admin_engine.as_ref(), path) {
            return Ok(());
        }

        match self.get_service_config_and_handle_by_path(path) {
            Ok((service_config, service_handle)) => {
                ctx.service_handle = Some(service_handle);
                ctx.service = Some(service_config.service.clone());
            }
            Err(e) => {
                log::error!("{}", e);
                return Err(pingora::Error::explain(
                    ConnectNoRoute,
                    format!("No route configured for {}.", path),
                ));
            }
        };
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Box<HttpPeer>> {
        let upstream_addr = if let Some(target) = ctx.proxy_context.target_upstream_addr {
            target
        } else if let Some(handle) = ctx.service_handle {
            let service_config = self.get_service_config(handle);
            service_config.upstream_addr
        } else {
            return Err(pingora::Error::explain(
                ConnectNoRoute,
                "No upstream peer available.",
            ));
        };

        let upstream_ip_port = format!("{}:{}", upstream_addr.ip(), upstream_addr.port());
        let peer = HttpPeer::new(upstream_addr, false, upstream_ip_port);
        Ok(Box::new(peer))
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // 1. Health check probes
        if HealthFilter::handle(session).await? {
            return Ok(true);
        }

        // 2. Admin Engine routing
        if AdminFilter::handle(
            session,
            self.admin_engine.as_ref(),
            &self.idempotency_engine,
            &self.subscription_hub,
        )
        .await?
        {
            return Ok(true);
        }

        // 3. Check for WebSocket Subscription Upgrade
        if self.subscriptions_config.enabled && crate::subscriptions::is_websocket_upgrade(session.req_header()) {
            let path = session.req_header().uri.path();
            if path == "/graphql" || path == "/gql" || path.starts_with("/graphql") || path.starts_with("/gql") {
                log::info!("CompositeServiceProxy: terminating WebSocket subscription upgrade on '{}'", path);
                return self.handle_websocket_subscription(session).await;
            }
        }

        log::info!(
            "request_filter uuid: {} hlc: {}",
            ctx.proxy_context.request_id,
            ctx.proxy_context.hlc
        );
        let req = session.req_header_mut();
        if let Err(e) =
            req.append_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string())
        {
            log::warn!(
                "Failed to append request header for request_id: {} {}",
                ctx.proxy_context.request_id,
                e
            );
        };
        if let Err(e) =
            req.append_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string())
        {
            log::warn!("Failed to append x-spectra-hlc: {}", e);
        }

        // 4. Check for explicit Idempotency-Key in request headers
        match IdempotencyFilter::handle_explicit_key(
            session,
            &self.idempotency_engine,
            &ctx.proxy_context.request_id,
            ctx.proxy_context.hlc,
        )
        .await?
        {
            IdempotencyInterceptResult::Conflict => return Ok(true),
            IdempotencyInterceptResult::Replay => {
                ctx.proxy_context.is_replay = true;
                let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
                let client_ip = crate::admin::extract_client_ip(session).to_string();
                self.traffic_recorder.record(crate::admin::TrafficRecord {
                    id: ctx.proxy_context.request_id.to_string(),
                    hlc: ctx.proxy_context.hlc.to_compact_string(),
                    timestamp_epoch_ms: crate::admin::now_epoch_ms(),
                    timestamp_formatted: crate::admin::format_timestamp(crate::admin::now_epoch_ms()),
                    client_ip,
                    method: session.req_header().method.to_string(),
                    path: session.req_header().uri.path().to_string(),
                    operation_name: None,
                    operation_type: "Mutation".to_string(),
                    mode: "Replay (Idempotency)".to_string(),
                    status_code: 200,
                    receipt_status: Some("REPLAY".to_string()),
                    latency_ms,
                    target: "Idempotency Cache".to_string(),
                    query_preview: None,
                    variables_preview: None,
                });
                return Ok(true);
            }
            IdempotencyInterceptResult::NewKey(k) => {
                ctx.proxy_context.idempotency_key = Some(k);
            }
            IdempotencyInterceptResult::None => {}
        }

        // Enable retry buffering and read request body if POST
        if session.req_header().method == http::Method::POST {
            session.enable_retry_buffering();
            while let Some(chunk) = session.read_request_body().await? {
                ctx.proxy_context.buffer.extend_from_slice(&chunk);
            }
        }

        if !ctx.proxy_context.buffer.is_empty() {
            let mut body_str = match std::str::from_utf8(&ctx.proxy_context.buffer) {
                Ok(s) => s.to_string(),
                Err(_) => return Ok(false),
            };

            let (request_protocol, dispatch_method) = match ctx.service.as_ref() {
                Some(s) => (s.get_request_protocol().clone(), s.get_dispatch_method().clone()),
                None => return Ok(false),
            };

            if let Ok(request_info) = request_protocol.decode_request(
                ctx.proxy_context.request_id,
                ctx.proxy_context.hlc,
                session.req_header().as_owned_parts(),
                &body_str,
            ) {
                ctx.proxy_context.request_topic =
                    dispatch_method.get_dispatch_topic(&request_info);
                ctx.proxy_context.request_info = Some(request_info.clone());

                let operation_name = request_info
                    .gql
                    .as_ref()
                    .and_then(|g| g.operation_name.clone());
                ctx.proxy_context.active_operation = operation_name.clone();

                // Zero-overhead preview extraction: reuse already-parsed AST without re-parsing or formatting on hot path
                let (qp, vp) = if let Some(gql) = &request_info.gql {
                    let json = gql.json_body();
                    let q = json.get("query").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let v = json.get("variables").and_then(|vars| {
                        if vars.is_null()
                            || (vars.is_object() && vars.as_object().map_or(false, |o| o.is_empty()))
                        {
                            None
                        } else {
                            Some(vars.to_string()) // compact unformatted JSON; lazy-formatted in client browser
                        }
                    });
                    (q, v)
                } else if !body_str.is_empty() {
                    (Some(body_str.chars().take(500).collect()), None)
                } else {
                    (None, None)
                };

                ctx.proxy_context.query_preview = qp;
                ctx.proxy_context.variables_preview = vp;

                // Evaluate Request Interceptors (global + route-specific)
                let req_pipeline = self
                    .interceptor_manager
                    .get_request_pipeline(operation_name.as_deref());
                if !req_pipeline.is_empty() {
                    let mut interceptor_ctx = crate::interceptors::InterceptorContext::new(
                        ctx.proxy_context.request_id,
                        ctx.proxy_context.hlc,
                    );
                    if let Some(g) = &request_info.gql {
                        interceptor_ctx.operation_name = g.operation_name.clone();
                        interceptor_ctx.operation_type = Some(g.operation_type.clone());
                    }

                    let mut req_parts = session.req_header().as_owned_parts();
                    let verdict = req_pipeline.intercept_request(
                        &mut interceptor_ctx,
                        &mut req_parts,
                        &body_str,
                    );

                    match verdict {
                        crate::interceptors::InterceptorVerdict::Pass => {}
                        crate::interceptors::InterceptorVerdict::Reject(rejection) => {
                            log::info!(
                                "Request rejected at edge by interceptor: code={}, status={}",
                                rejection.code,
                                rejection.status_code
                            );
                            let err_body = rejection.to_graphql_response();
                            let mut header = pingora::http::ResponseHeader::build(
                                rejection.status_code.as_u16(),
                                None,
                            )
                            .unwrap();
                            let _ = header.insert_header("content-type", "application/json");
                            let _ = header.insert_header(
                                REQUEST_ID_HEADER,
                                ctx.proxy_context.request_id.to_string(),
                            );
                            let _ = header.insert_header(
                                "x-spectra-hlc",
                                ctx.proxy_context.hlc.to_compact_string(),
                            );

                            session.set_keepalive(None);
                            session.write_response_header(Box::new(header), false).await?;
                            session
                                    .write_response_body(Some(bytes::Bytes::from(err_body)), true)
                                    .await?;

                            let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
                            let client_ip = crate::admin::extract_client_ip(session).to_string();

                            // Emit edge rejection audit event to broker
                            let op_clean = request_info
                                .gql
                                .as_ref()
                                .and_then(|g| g.operation_name.clone())
                                .unwrap_or_else(|| "anonymous".to_string())
                                .to_ascii_lowercase();
                            let rejection_topic = format!("interceptors.rejected.{}", op_clean);

                            let rejection_payload = serde_json::json!({
                                "requestId": ctx.proxy_context.request_id.to_string(),
                                "hlc": ctx.proxy_context.hlc.to_compact_string(),
                                "operationName": request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                "operationType": request_info.gql.as_ref().map(|g| g.operation_type.to_string()).unwrap_or_else(|| "GQL".to_string()),
                                "rejectionCode": rejection.code,
                                "statusCode": rejection.status_code.as_u16(),
                                "reason": rejection.message,
                                "clientIp": client_ip,
                                "timestamp": crate::admin::now_epoch_ms(),
                                "queryPreview": ctx.proxy_context.query_preview.clone(),
                                "variablesPreview": ctx.proxy_context.variables_preview.clone(),
                            });

                            let audit_handler = dispatch_method.clone();
                            let audit_topic = rejection_topic.clone();
                            let audit_payload = rejection_payload.to_string();
                            tokio::spawn(async move {
                                let _ = audit_handler.dispatch_payload(&audit_topic, &audit_payload).await;
                            });

                            self.traffic_recorder.record(crate::admin::TrafficRecord {
                                id: ctx.proxy_context.request_id.to_string(),
                                hlc: ctx.proxy_context.hlc.to_compact_string(),
                                timestamp_epoch_ms: crate::admin::now_epoch_ms(),
                                timestamp_formatted: crate::admin::format_timestamp(crate::admin::now_epoch_ms()),
                                client_ip,
                                method: session.req_header().method.to_string(),
                                path: session.req_header().uri.path().to_string(),
                                operation_name: request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                operation_type: request_info.gql.as_ref().map(|g| g.operation_type.to_string()).unwrap_or_else(|| "GQL".to_string()),
                                mode: "Rejected (Edge)".to_string(),
                                status_code: rejection.status_code.as_u16(),
                                receipt_status: Some("REJECTED".to_string()),
                                latency_ms,
                                target: format!("Sink: {}", rejection_topic),
                                query_preview: ctx.proxy_context.query_preview.clone(),
                                variables_preview: ctx.proxy_context.variables_preview.clone(),
                            });

                            return Ok(true);
                        }
                        crate::interceptors::InterceptorVerdict::Transform { headers, body } => {
                            if let Some(h) = headers {
                                for (k, v) in h {
                                    if let Some(k) = k {
                                        let _ = session
                                            .req_header_mut()
                                            .insert_header(k, v.to_str().unwrap_or(""));
                                    }
                                }
                            }
                            if let Some(b) = body {
                                if let Ok(s) = std::str::from_utf8(&b) {
                                    body_str = s.to_string();
                                }
                                ctx.proxy_context.buffer = b;
                            }
                        }
                    }
                }

                    // If no explicit idempotency key, register fingerprint for mutations
                    let is_mutation = request_info
                        .gql
                        .as_ref()
                        .map(|g| g.operation_type == crate::protocol::GraphQLOperationType::Mutation)
                        .unwrap_or(false);

                    if ctx.proxy_context.idempotency_key.is_none() && is_mutation {
                        let client_id = session
                            .req_header()
                            .headers
                            .get("x-forwarded-for")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("client")
                            .to_string();
                        let op_name = request_info
                            .gql
                            .as_ref()
                            .and_then(|g| g.operation_name.as_deref());
                        match IdempotencyFilter::handle_fingerprint(
                            session,
                            &self.idempotency_engine,
                            &ctx.proxy_context.request_id,
                            ctx.proxy_context.hlc,
                            &client_id,
                            op_name,
                            &body_str,
                        )
                        .await?
                        {
                            IdempotencyInterceptResult::Conflict => return Ok(true),
                            IdempotencyInterceptResult::Replay => {
                                ctx.proxy_context.is_replay = true;
                                let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
                                let client_ip = crate::admin::extract_client_ip(session).to_string();
                                self.traffic_recorder.record(crate::admin::TrafficRecord {
                                    id: ctx.proxy_context.request_id.to_string(),
                                    hlc: ctx.proxy_context.hlc.to_compact_string(),
                                    timestamp_epoch_ms: crate::admin::now_epoch_ms(),
                                    timestamp_formatted: crate::admin::format_timestamp(crate::admin::now_epoch_ms()),
                                    client_ip,
                                    method: session.req_header().method.to_string(),
                                    path: session.req_header().uri.path().to_string(),
                                    operation_name: op_name.map(|s| s.to_string()),
                                    operation_type: "Mutation".to_string(),
                                    mode: "Replay (Fingerprint)".to_string(),
                                    status_code: 200,
                                    receipt_status: Some("REPLAY".to_string()),
                                    latency_ms,
                                    target: "Idempotency Cache".to_string(),
                                    query_preview: ctx.proxy_context.query_preview.clone(),
                                    variables_preview: ctx.proxy_context.variables_preview.clone(),
                                });
                                return Ok(true);
                            }
                            IdempotencyInterceptResult::NewKey(k) => {
                                ctx.proxy_context.idempotency_key = Some(k);
                            }
                            IdempotencyInterceptResult::None => {}
                        }
                    }

                    // Check route overrides using StrategyRouter
                    if let Some(route) = StrategyRouter::match_route(&self.routes, &request_info) {
                        if route.mode.is_async_edge_command() {
                            let res = StrategyRouter::handle_mode_b_edge(
                                session,
                                ctx,
                                route,
                                &request_info,
                                &dispatch_method,
                                &self.idempotency_engine,
                            )
                            .await;

                            let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
                            let client_ip = crate::admin::extract_client_ip(session).to_string();

                            self.traffic_recorder.record(crate::admin::TrafficRecord {
                                id: ctx.proxy_context.request_id.to_string(),
                                hlc: ctx.proxy_context.hlc.to_compact_string(),
                                timestamp_epoch_ms: crate::admin::now_epoch_ms(),
                                timestamp_formatted: crate::admin::format_timestamp(crate::admin::now_epoch_ms()),
                                client_ip,
                                method: session.req_header().method.to_string(),
                                path: session.req_header().uri.path().to_string(),
                                operation_name: request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                operation_type: "Mutation".to_string(),
                                mode: "Async (Receipt)".to_string(),
                                status_code: 200,
                                receipt_status: Some(route.receipt_status.clone()),
                                latency_ms,
                                target: format!("NATS: {}", ctx.proxy_context.request_topic),
                                query_preview: ctx.proxy_context.query_preview.clone(),
                                variables_preview: ctx.proxy_context.variables_preview.clone(),
                            });

                            return res;
                        }

                        if let Some(addr) =
                            StrategyRouter::resolve_mode_a_upstream(route, &self.named_upstreams)
                        {
                            ctx.proxy_context.target_upstream_addr = Some(addr);
                        }
                    }

                    StrategyRouter::handle_mode_a_dispatch_policy(
                        ctx.proxy_context.dispatch_policy,
                        &request_info,
                        &dispatch_method,
                    )
                    .await;
            }
        }

        Ok(false)
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if ctx.proxy_context.request_info.is_some() {
            return Ok(());
        }
        if let Some(b) = body {
            ctx.proxy_context.buffer.extend(&b[..]);
        }
        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        resp: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        ctx.proxy_context.buffer.clear();
        if let Err(e) = resp.append_header(
            REQUEST_ID_HEADER,
            ctx.proxy_context.request_id.to_string(),
        ) {
            log::warn!(
                "Failed to append response header for request_id: {} {}",
                ctx.proxy_context.request_id,
                e
            )
        }
        if let Err(e) = resp.append_header(
            "x-spectra-hlc",
            ctx.proxy_context.hlc.to_compact_string(),
        ) {
            log::warn!("Failed to append x-spectra-hlc: {}", e);
        }
        ctx.proxy_context.response_parts = Some(resp.as_owned_parts());

        // Check if response interceptors are registered for this operation
        if self.interceptor_manager.has_response_interceptors(ctx.proxy_context.active_operation.as_deref()) {
            ctx.proxy_context.has_response_interception = true;
            // Remove content-length so downstream HTTP framing uses chunked encoding,
            // avoiding framing corruption if the payload is resized or transformed.
            let _ = resp.remove_header("content-length");
        }

        return Ok(());
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        // 1. Fast path: If no response interceptors are registered, stream chunks through with zero buffering!
        if !ctx.proxy_context.has_response_interception {
            if let Some(b) = body {
                ctx.proxy_context.buffer.extend(&b[..]);
            }

            if end_of_stream {
                let body_str = std::str::from_utf8(&ctx.proxy_context.buffer).unwrap_or_default();
                let response_body = match ctx.proxy_context.response_parts.as_ref() {
                    Some(parts) => ResponseBody::new(&parts.headers, body_str),
                    None => ResponseBody::new(&http::HeaderMap::new(), body_str),
                };
                ctx.proxy_context.response_body = Some(response_body);
                ctx.proxy_context.buffer.clear();
            }

            return Ok(None);
        }

        // 2. Intercepted path: Buffer chunks until end_of_stream to allow full-body inspection/transformation
        if let Some(b) = body.take() {
            ctx.proxy_context.buffer.extend(&b[..]);
        }

        if end_of_stream {
            let resp_pipeline = self
                .interceptor_manager
                .get_response_pipeline(ctx.proxy_context.active_operation.as_deref());
            let mut interceptor_ctx = crate::interceptors::InterceptorContext::new(
                ctx.proxy_context.request_id,
                ctx.proxy_context.hlc,
            );
            interceptor_ctx.operation_name = ctx.proxy_context.active_operation.clone();

            let mut fake_parts = http::response::Response::builder()
                .body(())
                .unwrap()
                .into_parts()
                .0;
            if let Some(parts) = &ctx.proxy_context.response_parts {
                fake_parts.status = parts.status;
                fake_parts.headers = parts.headers.clone();
            }

            let verdict = resp_pipeline.intercept_response(
                &interceptor_ctx,
                &mut fake_parts,
                &ctx.proxy_context.buffer,
            );

            let final_bytes = match verdict {
                crate::interceptors::InterceptorVerdict::Pass => {
                    std::mem::take(&mut ctx.proxy_context.buffer)
                }
                crate::interceptors::InterceptorVerdict::Transform { headers: _, body } => {
                    body.unwrap_or_else(|| std::mem::take(&mut ctx.proxy_context.buffer))
                }
                crate::interceptors::InterceptorVerdict::Reject(rejection) => {
                    log::info!(
                        "Response rejected by interceptor: code={}, status={}",
                        rejection.code,
                        rejection.status_code
                    );
                    rejection.to_graphql_response().into_bytes()
                }
            };

            let body_str = std::str::from_utf8(&final_bytes).unwrap_or_default().to_string();
            let response_body = match ctx.proxy_context.response_parts.as_ref() {
                Some(parts) => ResponseBody::new(&parts.headers, &body_str),
                None => ResponseBody::new(&http::HeaderMap::new(), &body_str),
            };
            ctx.proxy_context.response_body = Some(response_body);
            ctx.proxy_context.buffer.clear();

            *body = Some(Bytes::from(final_bytes));
        }

        Ok(None)
    }

    async fn logging(&self, session: &mut Session, e: Option<&pingora::Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let dispatch_method = match ctx.service.as_ref() {
            Some(s) => s.get_dispatch_method().clone(),
            None => return,
        };

        if !ctx.proxy_context.is_mode_b_terminated && !ctx.proxy_context.is_replay {
            let path = session.req_header().uri.path();
            if path.starts_with("/gql") || path.starts_with("/graphql") {
                let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
                let status_code = ctx
                    .proxy_context
                    .response_parts
                    .as_ref()
                    .map(|p| p.status.as_u16())
                    .unwrap_or(if e.is_some() { 502 } else { 200 });

                let op_type = ctx
                    .proxy_context
                    .request_info
                    .as_ref()
                    .and_then(|r| r.gql.as_ref())
                    .map(|g| g.operation_type.to_string())
                    .unwrap_or_else(|| "Query".to_string());

                let op_name = ctx
                    .proxy_context
                    .active_operation
                    .clone()
                    .or_else(|| {
                        ctx.proxy_context
                            .request_info
                            .as_ref()
                            .and_then(|r| r.gql.as_ref())
                            .and_then(|g| g.operation_name.clone())
                    });

                let target_str = if let Some(t) = ctx.proxy_context.target_upstream_addr {
                    format!("Upstream: {}", t)
                } else if let Some(h) = ctx.service_handle {
                    format!("Upstream: {}", self.get_service_config(h).upstream_addr)
                } else {
                    "Upstream".to_string()
                };

                let (query_preview, variables_preview) = if ctx.proxy_context.query_preview.is_some() {
                    (
                        ctx.proxy_context.query_preview.clone(),
                        ctx.proxy_context.variables_preview.clone(),
                    )
                } else {
                    let qp = ctx.proxy_context.request_info.as_ref().and_then(|r| {
                        if !r.http.uri.query().unwrap_or("").is_empty() {
                            Some(r.http.uri.to_string())
                        } else {
                            None
                        }
                    });
                    (qp, None)
                };

                self.traffic_recorder.record(crate::admin::TrafficRecord {
                    id: ctx.proxy_context.request_id.to_string(),
                    hlc: ctx.proxy_context.hlc.to_compact_string(),
                    timestamp_epoch_ms: crate::admin::now_epoch_ms(),
                    timestamp_formatted: crate::admin::format_timestamp(crate::admin::now_epoch_ms()),
                    client_ip: crate::admin::extract_client_ip(session).to_string(),
                    method: session.req_header().method.to_string(),
                    path: path.to_string(),
                    operation_name: op_name,
                    operation_type: op_type,
                    mode: "Sync (Proxy)".to_string(),
                    status_code,
                    receipt_status: None,
                    latency_ms,
                    target: target_str,
                    query_preview,
                    variables_preview,
                });
            }
        }

        TelemetryDispatcher::dispatch_logging(
            session,
            e,
            ctx,
            &dispatch_method,
            &self.idempotency_engine,
        )
        .await;
    }
}

#[allow(unused_imports)]
pub use crate::gateway::filters::strategy::generate_command_receipt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::clock::HlcClock;
    use crate::core::types::ExecutionStrategy;
    use crate::protocol::parse_graphql_operation;
    use std::net::SocketAddr;

    #[test]
    fn test_generate_command_receipt_schema() {
        let (command_id, hlc) = HlcClock::global().now_uuidv7();
        let receipt = generate_command_receipt("importCatalog", &command_id, &hlc, "ACCEPTED");

        assert_eq!(
            receipt["data"]["importCatalog"]["commandId"],
            command_id.to_string()
        );
        assert_eq!(
            receipt["data"]["importCatalog"]["status"],
            "ACCEPTED"
        );
        assert_eq!(
            receipt["data"]["importCatalog"]["hlc"],
            hlc.to_compact_string()
        );
    }

    #[test]
    fn test_multi_upstream_routing_resolution() {
        let mut named_upstreams = HashMap::new();
        let default_addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let inventory_addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let crm_addr: SocketAddr = "127.0.0.1:5002".parse().unwrap();

        named_upstreams.insert("default".to_string(), default_addr);
        named_upstreams.insert("inventory".to_string(), inventory_addr);
        named_upstreams.insert("crm".to_string(), crm_addr);

        let mut routes = HashMap::new();
        routes.insert(
            "inventory_update".to_string(),
            SpectraRouteConfig {
                operation: "adjustInventory".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                upstream: Some("inventory".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );
        routes.insert(
            "customer_address".to_string(),
            SpectraRouteConfig {
                operation: "updateCustomerAddress".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                upstream: Some("crm".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );
        routes.insert(
            "bulk_import".to_string(),
            SpectraRouteConfig {
                operation: "importCatalog".to_string(),
                mode: ExecutionStrategy::AsyncCommandReceipt,
                upstream: None,
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );

        let proxy = CompositeServiceProxy::new().with_routing(
            named_upstreams,
            SpectraModeAConfig::default(),
            routes,
        );

        // 1. Mutation matching inventory override
        let op_inv = parse_graphql_operation("mutation { adjustInventory(itemId: 1) { id } }").unwrap();
        let matched_inv = proxy.routes.values().find(|r| op_inv.matches_operation(&r.operation));
        assert!(matched_inv.is_some());
        let inv_route = matched_inv.unwrap();
        assert_eq!(inv_route.mode, ExecutionStrategy::SyncUpstreamExecution);
        let target_addr = proxy.named_upstreams.get(inv_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, inventory_addr);

        // 2. Mutation matching CRM override
        let op_crm = parse_graphql_operation("mutation UpdateAddress { updateCustomerAddress(id: 1) { ok } }").unwrap();
        let matched_crm = proxy.routes.values().find(|r| op_crm.matches_operation(&r.operation));
        assert!(matched_crm.is_some());
        let crm_route = matched_crm.unwrap();
        assert_eq!(crm_route.mode, ExecutionStrategy::SyncUpstreamExecution);
        let target_addr = proxy.named_upstreams.get(crm_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, crm_addr);

        // 3. Mutation matching Async Command Receipt (zero upstream hop)
        let op_mode_b = parse_graphql_operation("mutation { importCatalog(file: \"a.csv\") { status } }").unwrap();
        let matched_b = proxy.routes.values().find(|r| op_mode_b.matches_operation(&r.operation));
        assert!(matched_b.is_some());
        let b_route = matched_b.unwrap();
        assert_eq!(b_route.mode, ExecutionStrategy::AsyncCommandReceipt);
        assert_eq!(b_route.receipt_status, "ACCEPTED");

        // 4. Query or unmapped mutation -> falls back to default upstream
        let op_query = parse_graphql_operation("query { hero { name } }").unwrap();
        let matched_query = proxy.routes.values().find(|r| op_query.matches_operation(&r.operation));
        assert!(matched_query.is_none());

        let op_unmapped = parse_graphql_operation("mutation { addReview(stars: 5) { id } }").unwrap();
        let matched_unmapped = proxy.routes.values().find(|r| op_unmapped.matches_operation(&r.operation));
        assert!(matched_unmapped.is_none());
    }
}

