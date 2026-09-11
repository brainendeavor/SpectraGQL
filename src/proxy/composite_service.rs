use crate::clock::HlcClock;
use crate::dispatch::DispatchHandler;
use crate::payload::{RequestInfo, ResponseBody, ResponseInfo, TerminalEvent};
use crate::proxy::SpectraProxyService;
use crate::proxy::{
    ExtraServiceParams, PathRouter, ProxyService, REQUEST_ID_HEADER, SpectraProxyCtx,
    new_proxy_service,
};
use crate::ratify::RequestRatification;

use anyhow::{Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use pingora::ErrorType::ConnectNoRoute;
use pingora::http::ResponseHeader;
use pingora::proxy::{ProxyHttp, Session};
use pingora::upstreams::peer::HttpPeer;
use std::collections::{HashMap, HashSet};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::ServiceHandle;
use crate::spectra_config::{
    ModeADispatchPolicy, OperationMode, SpectraModeAConfig, SpectraRouteConfig,
};

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
    pub idempotency_engine: Arc<crate::ratify::IdempotencyEngine>,
    pub named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
    pub mode_a: SpectraModeAConfig,
    pub routes: Arc<HashMap<String, SpectraRouteConfig>>,
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
            idempotency_engine: Arc::new(crate::ratify::IdempotencyEngine::default()),
            named_upstreams: Arc::new(HashMap::new()),
            mode_a: SpectraModeAConfig::default(),
            routes: Arc::new(HashMap::new()),
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

    fn log_dispatch_result_error(&self, ctx: &SpectraProxyCtx, result: pingora::Result<()>) -> bool {
        if let Err(e) = result {
            log::error!(
                "DISPATCH failed. {} request_topic: {}\n{:?}",
                e,
                ctx.request_topic,
                ctx.buffer
            );
            return false;
        }
        return true;
    }

}

// TODO: absorb SpectraProxyCtx
pub struct CompositeServiceProxyCtx {
    proxy_context: SpectraProxyCtx,
    service_handle: Option<ServiceHandle>,
    service: Option<Arc<ProxyService>>,
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
            response_actions: HashSet::new(),
            buffer: vec![],
            idempotency_key: None,
            is_replay: false,
            target_upstream_addr: None,
            is_mode_b_terminated: false,
            dispatch_policy: self.mode_a.dispatch_policy,
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

        // Check for explicit Idempotency-Key in request headers
        if let Some(key) = crate::ratify::IdempotencyEngine::extract_idempotency_key(&session.req_header().headers) {
            match self.idempotency_engine.check_or_insert(&key, ctx.proxy_context.hlc) {
                crate::ratify::IdempotencyOutcome::Conflict { hlc } => {
                    log::warn!("Idempotency conflict for key: {}, in-flight hlc: {}", key, hlc);
                    let mut header = pingora::http::ResponseHeader::build(409, None).unwrap();
                    let _ = header.insert_header("content-type", "application/json");
                    let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                    let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
                    let body = format!(
                        r#"{{"errors":[{{"message":"A mutation with idempotency key '{}' is currently in flight","extensions":{{"code":"CONFLICT","hlc":"{}"}}}}]}}"#,
                        key, hlc.to_compact_string()
                    );
                    session.set_keepalive(None);
                    session.write_response_header(Box::new(header), false).await?;
                    session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
                    return Ok(true);
                }
                crate::ratify::IdempotencyOutcome::Replay { status_code, headers, body, .. } => {
                    log::info!("Idempotency replay for key: {}", key);
                    ctx.proxy_context.is_replay = true;
                    let mut header = pingora::http::ResponseHeader::build(status_code, None).unwrap();
                    for (k, v) in headers {
                        let _ = header.insert_header(k, v);
                    }
                    let _ = header.insert_header(crate::ratify::idempotency::REPLAY_HEADER, "true");
                    let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                    let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
                    session.set_keepalive(None);
                    session.write_response_header(Box::new(header), false).await?;
                    session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
                    return Ok(true);
                }
                crate::ratify::IdempotencyOutcome::New => {
                    ctx.proxy_context.idempotency_key = Some(key);
                }
            }
        }

        // Enable retry buffering and read request body if POST
        if session.req_header().method == http::Method::POST {
            session.enable_retry_buffering();
            while let Some(chunk) = session.read_request_body().await? {
                ctx.proxy_context.buffer.extend_from_slice(&chunk);
            }
        }

        if !ctx.proxy_context.buffer.is_empty() {
            if let Ok(body_str) = std::str::from_utf8(&ctx.proxy_context.buffer) {
                let service = match ctx.service.as_ref() {
                    Some(s) => s,
                    None => return Ok(false),
                };

                if let Ok(request_info) = service.get_request_protocol().ratify_request(
                    ctx.proxy_context.request_id,
                    ctx.proxy_context.hlc,
                    session.req_header().as_owned_parts(),
                    body_str,
                ) {
                    let dispatch_method = service.get_dispatch_method();
                    ctx.proxy_context.request_topic =
                        dispatch_method.get_dispatch_topic(&request_info);
                    ctx.proxy_context.request_info = Some(request_info.clone());

                    // If no explicit idempotency key, register fingerprint for mutations
                    let is_mutation = request_info
                        .gql
                        .as_ref()
                        .map(|g| g.operation_type == crate::payload::GraphQLOperationType::Mutation)
                        .unwrap_or(false);

                    if ctx.proxy_context.idempotency_key.is_none() && is_mutation {
                        let client_id = session
                            .req_header()
                            .headers
                            .get("x-forwarded-for")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("client");
                        let op_name = request_info
                            .gql
                            .as_ref()
                            .and_then(|g| g.operation_name.as_deref());
                        let fingerprint = crate::ratify::IdempotencyEngine::compute_fingerprint(
                            client_id,
                            op_name,
                            body_str,
                        );
                        match self.idempotency_engine.check_or_insert(&fingerprint, ctx.proxy_context.hlc) {
                            crate::ratify::IdempotencyOutcome::Conflict { hlc } => {
                                log::warn!("Idempotency conflict for fingerprint: {}, in-flight hlc: {}", fingerprint, hlc);
                                let mut header = pingora::http::ResponseHeader::build(409, None).unwrap();
                                let _ = header.insert_header("content-type", "application/json");
                                let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                                let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
                                let body = format!(
                                    r#"{{"errors":[{{"message":"A mutation with idempotency key is currently in flight","extensions":{{"code":"CONFLICT","hlc":"{}"}}}}]}}"#,
                                    hlc.to_compact_string()
                                );
                                session.set_keepalive(None);
                                session.write_response_header(Box::new(header), false).await?;
                                session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
                                return Ok(true);
                            }
                            crate::ratify::IdempotencyOutcome::Replay { status_code, headers, body, .. } => {
                                log::info!("Idempotency replay for fingerprint");
                                ctx.proxy_context.is_replay = true;
                                let mut header = pingora::http::ResponseHeader::build(status_code, None).unwrap();
                                for (k, v) in headers {
                                    let _ = header.insert_header(k, v);
                                }
                                let _ = header.insert_header(crate::ratify::idempotency::REPLAY_HEADER, "true");
                                let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                                let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
                                session.set_keepalive(None);
                                session.write_response_header(Box::new(header), false).await?;
                                session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
                                return Ok(true);
                            }
                            crate::ratify::IdempotencyOutcome::New => {
                                ctx.proxy_context.idempotency_key = Some(fingerprint);
                            }
                        }
                    }

                    // Check route overrides
                    let matched_route = request_info.gql.as_ref().and_then(|gql| {
                        self.routes.values().find(|r| gql.matches_operation(&r.operation))
                    });

                    if let Some(route) = matched_route {
                        if route.mode == OperationMode::B {
                            log::info!("Executing Mode B edge termination for operation: {}", route.operation);
                            ctx.proxy_context.is_mode_b_terminated = true;

                            // Commit command to event broker
                            let dispatch_result = dispatch_method.dispatch_request_info(&request_info).await;
                            self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);

                            // Synthesize deterministic Command Receipt
                            let field_name = request_info
                                .gql
                                .as_ref()
                                .and_then(|g| g.root_fields.first().cloned())
                                .or_else(|| request_info.gql.as_ref().and_then(|g| g.operation_name.clone()))
                                .unwrap_or_else(|| route.operation.clone());

                            let receipt_json = generate_command_receipt(
                                &field_name,
                                &ctx.proxy_context.request_id,
                                &ctx.proxy_context.hlc,
                                &route.receipt_status,
                            );
                            let receipt_body = receipt_json.to_string();

                            // Complete idempotency tracking
                            if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                                let mut fake_headers = http::HeaderMap::new();
                                fake_headers.insert("content-type", "application/json".parse().unwrap());
                                fake_headers.insert(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string().parse().unwrap());
                                fake_headers.insert("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string().parse().unwrap());
                                self.idempotency_engine.complete(key, ctx.proxy_context.hlc, 200, &fake_headers, &receipt_body);
                            }

                            let mut header = pingora::http::ResponseHeader::build(200, None).unwrap();
                            let _ = header.insert_header("content-type", "application/json");
                            let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                            let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());

                            session.set_keepalive(None);
                            session.write_response_header(Box::new(header), false).await?;
                            session.write_response_body(Some(bytes::Bytes::from(receipt_body)), true).await?;

                            return Ok(true);
                        }

                        // Mode A with specific upstream override
                        if let Some(upstream_name) = &route.upstream {
                            if let Some(addr) = self.named_upstreams.get(upstream_name) {
                                log::info!(
                                    "Mode A route override: op '{}' routing to microservice '{}' ({})",
                                    route.operation,
                                    upstream_name,
                                    addr
                                );
                                ctx.proxy_context.target_upstream_addr = Some(*addr);
                            } else {
                                log::warn!(
                                    "Named upstream '{}' not found in config for op '{}', using default",
                                    upstream_name,
                                    route.operation
                                );
                            }
                        }
                    }

                    // Mode A dispatch policy handling
                    match ctx.proxy_context.dispatch_policy {
                        ModeADispatchPolicy::RawAudit => {
                            let dispatch_result = dispatch_method.dispatch_request_info(&request_info).await;
                            self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
                        }
                        ModeADispatchPolicy::ResponseOnly
                        | ModeADispatchPolicy::ResponseWithFailure => {
                            // Defer dispatch until response logging
                        }
                    }
                }
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

        // TODO: invoke ratify_response here?

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

        Ok(None)
    }

    async fn logging(&self, session: &mut Session, e: Option<&pingora::Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        if ctx.proxy_context.is_replay {
            log::info!("Idempotent replay served; skipping event dispatch.");
            ctx.proxy_context.buffer.clear();
            return;
        }

        if ctx.proxy_context.is_mode_b_terminated {
            log::info!("Mode B command receipt completed; edge event already dispatched.");
            ctx.proxy_context.buffer.clear();
            return;
        }

        let service = match ctx.service.as_ref() {
            Some(s) => s,
            None => return,
        };
        let dispatch_method = service.get_dispatch_method();

        let duration_ms = ctx.proxy_context.start_time.elapsed().as_millis() as u64;
        let request_id = ctx.proxy_context.request_id;
        let hlc = ctx.proxy_context.hlc;

        let request_info = ctx.proxy_context.request_info.take().unwrap_or_else(|| {
            RequestInfo::new(request_id, hlc, session.req_header().as_owned_parts())
        });

        let op_name = request_info.gql.as_ref().and_then(|g| g.operation_name.clone());

        match e {
            Some(error) => {
                if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                    self.idempotency_engine.remove(key);
                }
                if ctx.proxy_context.dispatch_policy == ModeADispatchPolicy::ResponseOnly {
                    log::info!("Mode A response_only: suppressing event dispatch on connection error: {}", error);
                } else {
                    let terminal_event = TerminalEvent::failure(
                        request_id,
                        hlc,
                        duration_ms,
                        op_name,
                        request_info,
                        None,
                        error.to_string(),
                    );
                    let failed_topic = format!("{}.failed", ctx.proxy_context.request_topic);
                    let dispatch_result = dispatch_method
                        .dispatch_terminal_event(&failed_topic, &terminal_event)
                        .await;
                    self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
                }
            }
            None => {
                let http_response_headers = match ctx.proxy_context.response_parts.as_ref() {
                    Some(parts) => parts.headers.clone(),
                    None => http::HeaderMap::new(),
                };
                let status_code = ctx.proxy_context.response_parts.as_ref().map(|p| p.status);
                let response_body = ctx
                    .proxy_context
                    .response_body
                    .take()
                    .unwrap_or_else(|| ResponseBody::new(&http_response_headers, ""));

                let is_http_err = status_code
                    .map(|s| s.is_client_error() || s.is_server_error())
                    .unwrap_or(false);

                if is_http_err {
                    if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                        self.idempotency_engine.remove(key);
                    }
                    if ctx.proxy_context.dispatch_policy == ModeADispatchPolicy::ResponseOnly {
                        log::info!(
                            "Mode A response_only: suppressing event dispatch on HTTP error: {}",
                            status_code.unwrap()
                        );
                    } else {
                        let response_info = ResponseInfo::new(
                            request_id,
                            hlc,
                            response_body,
                            http_response_headers,
                        );
                        let terminal_event = TerminalEvent::failure(
                            request_id,
                            hlc,
                            duration_ms,
                            op_name,
                            request_info,
                            Some(response_info),
                            format!("HTTP {}", status_code.unwrap()),
                        );
                        let failed_topic = format!("{}.failed", ctx.proxy_context.request_topic);
                        let dispatch_result = dispatch_method
                            .dispatch_terminal_event(&failed_topic, &terminal_event)
                            .await;
                        self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
                    }
                } else {
                    // Complete idempotency tracking for successful response
                    if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                        let status_u16 = status_code.map(|s| s.as_u16()).unwrap_or(200);
                        let body_str = response_body.text.as_deref().unwrap_or_else(|| response_body.json.get());
                        self.idempotency_engine.complete(key, hlc, status_u16, &http_response_headers, body_str);
                    }

                    let response_info = ResponseInfo::new(
                        request_id,
                        hlc,
                        response_body,
                        http_response_headers,
                    );
                    let terminal_event = TerminalEvent::success(
                        request_id,
                        hlc,
                        duration_ms,
                        op_name,
                        request_info,
                        response_info,
                    );
                    let dispatch_result = dispatch_method
                        .dispatch_terminal_event(&ctx.proxy_context.request_topic, &terminal_event)
                        .await;
                    self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
                }
            }
        }
        ctx.proxy_context.buffer.clear();
    }
}

pub fn generate_command_receipt(
    field_or_op: &str,
    command_id: &uuid::Uuid,
    hlc: &crate::clock::HlcTimestamp,
    receipt_status: &str,
) -> serde_json::Value {
    serde_json::json!({
        "data": {
            field_or_op: {
                "commandId": command_id.to_string(),
                "status": receipt_status,
                "hlc": hlc.to_compact_string(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::parse_graphql_operation;
    use std::net::SocketAddr;

    #[test]
    fn test_generate_command_receipt_schema() {
        let (command_id, hlc) = crate::clock::HlcClock::global().now_uuidv7();
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
                mode: OperationMode::A,
                upstream: Some("inventory".to_string()),
                receipt_status: "ACCEPTED".to_string(),
            },
        );
        routes.insert(
            "customer_address".to_string(),
            SpectraRouteConfig {
                operation: "updateCustomerAddress".to_string(),
                mode: OperationMode::A,
                upstream: Some("crm".to_string()),
                receipt_status: "ACCEPTED".to_string(),
            },
        );
        routes.insert(
            "bulk_import".to_string(),
            SpectraRouteConfig {
                operation: "importCatalog".to_string(),
                mode: OperationMode::B,
                upstream: None,
                receipt_status: "ACCEPTED".to_string(),
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
        assert_eq!(inv_route.mode, OperationMode::A);
        let target_addr = proxy.named_upstreams.get(inv_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, inventory_addr);

        // 2. Mutation matching CRM override
        let op_crm = parse_graphql_operation("mutation UpdateAddress { updateCustomerAddress(id: 1) { ok } }").unwrap();
        let matched_crm = proxy.routes.values().find(|r| op_crm.matches_operation(&r.operation));
        assert!(matched_crm.is_some());
        let crm_route = matched_crm.unwrap();
        assert_eq!(crm_route.mode, OperationMode::A);
        let target_addr = proxy.named_upstreams.get(crm_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, crm_addr);

        // 3. Mutation matching Mode B (async receipt, zero upstream hop)
        let op_mode_b = parse_graphql_operation("mutation { importCatalog(file: \"a.csv\") { status } }").unwrap();
        let matched_b = proxy.routes.values().find(|r| op_mode_b.matches_operation(&r.operation));
        assert!(matched_b.is_some());
        let b_route = matched_b.unwrap();
        assert_eq!(b_route.mode, OperationMode::B);
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

