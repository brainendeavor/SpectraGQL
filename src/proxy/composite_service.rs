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
use std::collections::HashSet;
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
    pub idempotency_engine: Arc<crate::ratify::IdempotencyEngine>,
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
        }
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
        if let Some(handle) = ctx.service_handle {
            let service_config = self.get_service_config(handle);
            // consider delegating to the service's upstream_peer method
            let upstream_addr = service_config.upstream_addr;
            let upstream_ip_port = format!("{}:{}", upstream_addr.ip(), upstream_addr.port());
            let peer = HttpPeer::new(upstream_addr, false, upstream_ip_port);
            Ok(Box::new(peer))
        } else {
            Err(pingora::Error::explain(
                ConnectNoRoute,
                "No upstream peer available.",
            ))
        }
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

        Ok(false)
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        log::info!("request_body_filter");
        if let Some(b) = body {
            ctx.proxy_context.buffer.extend(&b[..]);
        }
        if end_of_stream {
            if let Ok(body_str) = std::str::from_utf8(&ctx.proxy_context.buffer) {
                log::info!("Request Body: {}", body_str);
                let service = ctx
                    .service
                    .as_ref()
                    .expect("Service not assigned. request_body_filter");
                match service.get_request_protocol().ratify_request(
                    ctx.proxy_context.request_id,
                    ctx.proxy_context.hlc,
                    session.req_header().as_owned_parts(),
                    body_str,
                ) {
                    Ok(request_info) => {
                        let dispatch_method = service.get_dispatch_method();
                        ctx.proxy_context.request_topic =
                            dispatch_method.get_dispatch_topic(&request_info);
                        ctx.proxy_context.request_info = Some(request_info.clone());

                        // If no explicit idempotency key was provided, register fingerprint for mutations
                        if ctx.proxy_context.idempotency_key.is_none() {
                            let is_mutation = request_info
                                .gql
                                .as_ref()
                                .map(|g| g.operation_type == crate::payload::GraphQLOperationType::Mutation)
                                .unwrap_or(false);
                            if is_mutation {
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
                                let _ = self.idempotency_engine.check_or_insert(&fingerprint, ctx.proxy_context.hlc);
                                ctx.proxy_context.idempotency_key = Some(fingerprint);
                            }
                        }

                        let dispatch_result =
                            dispatch_method.dispatch_request_info(&request_info).await;
                        self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
                        log::info!("ratify_request invoked.")
                    }
                    Err(e) => {
                        log::error!("ratify_request error: {}", e);
                        // TODO: return appropriate pingora error
                        // return pingora::AcceptError
                    }
                }
            }
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
                let terminal_event = TerminalEvent::failure(
                    request_id,
                    hlc,
                    duration_ms,
                    op_name,
                    request_info,
                    None,
                    error.to_string(),
                );
                let dispatch_result = dispatch_method
                    .dispatch_terminal_event(&ctx.proxy_context.request_topic, &terminal_event)
                    .await;
                self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
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

                // Complete idempotency tracking for successful or HTTP-outcome requests
                if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                    let status_u16 = status_code.map(|s| s.as_u16()).unwrap_or(200);
                    let body_str = response_body.text.as_deref().unwrap_or_else(|| response_body.json.get());
                    self.idempotency_engine.complete(key, hlc, status_u16, &http_response_headers, body_str);
                }

                let is_http_err = status_code
                    .map(|s| s.is_client_error() || s.is_server_error())
                    .unwrap_or(false);

                let response_info = ResponseInfo::new(
                    request_id,
                    hlc,
                    response_body,
                    http_response_headers,
                );

                let terminal_event = if is_http_err {
                    TerminalEvent::failure(
                        request_id,
                        hlc,
                        duration_ms,
                        op_name,
                        request_info,
                        Some(response_info),
                        format!("HTTP {}", status_code.unwrap()),
                    )
                } else {
                    TerminalEvent::success(
                        request_id,
                        hlc,
                        duration_ms,
                        op_name,
                        request_info,
                        response_info,
                    )
                };

                let dispatch_result = dispatch_method
                    .dispatch_terminal_event(&ctx.proxy_context.request_topic, &terminal_event)
                    .await;
                self.log_dispatch_result_error(&ctx.proxy_context, dispatch_result);
            }
        }
        ctx.proxy_context.buffer.clear();
    }
}
