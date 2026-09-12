use crate::core::config::ModeADispatchPolicy;
use crate::gateway::composite_service::CompositeServiceProxyCtx;
use crate::idempotency::IdempotencyEngine;
use crate::protocol::{RequestInfo, ResponseBody, ResponseInfo};
use crate::telemetry::event::TerminalEvent;
use crate::telemetry::{DispatchHandler, DispatchMethod};
use pingora::proxy::Session;
use std::sync::Arc;

/// Dispatches completion telemetry events to configured message brokers
/// during the proxy logging phase.
pub struct TelemetryDispatcher;

impl TelemetryDispatcher {
    pub async fn dispatch_logging(
        session: &mut Session,
        error: Option<&pingora::Error>,
        ctx: &mut CompositeServiceProxyCtx,
        dispatch_method: &DispatchMethod,
        idempotency_engine: &Arc<IdempotencyEngine>,
    ) {
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

        let duration_ms = ctx.proxy_context.start_time.elapsed().as_millis() as u64;
        let request_id = ctx.proxy_context.request_id;
        let hlc = ctx.proxy_context.hlc;

        let request_info = ctx.proxy_context.request_info.take().unwrap_or_else(|| {
            RequestInfo::new(request_id, hlc, session.req_header().as_owned_parts())
        });

        let op_name = request_info.gql.as_ref().and_then(|g| g.operation_name.clone());

        match error {
            Some(err) => {
                if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                    idempotency_engine.remove(key).await;
                }
                if ctx.proxy_context.dispatch_policy == ModeADispatchPolicy::ResponseOnly {
                    log::info!(
                        "Mode A response_only: suppressing event dispatch on connection error: {}",
                        err
                    );
                } else {
                    let terminal_event = TerminalEvent::failure(
                        request_id,
                        hlc,
                        duration_ms,
                        op_name,
                        request_info,
                        None,
                        err.to_string(),
                    );
                    let failed_topic = format!("{}.failed", ctx.proxy_context.request_topic);
                    let dispatch_result = dispatch_method
                        .dispatch_terminal_event(&failed_topic, &terminal_event)
                        .await;
                    Self::log_dispatch_error(
                        &ctx.proxy_context.request_topic,
                        &ctx.proxy_context.buffer,
                        dispatch_result,
                    );
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
                        idempotency_engine.remove(key).await;
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
                        Self::log_dispatch_error(
                            &ctx.proxy_context.request_topic,
                            &ctx.proxy_context.buffer,
                            dispatch_result,
                        );
                    }
                } else {
                    // Complete idempotency tracking for successful response
                    if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                        let status_u16 = status_code.map(|s| s.as_u16()).unwrap_or(200);
                        let body_str = response_body
                            .text
                            .as_deref()
                            .unwrap_or_else(|| response_body.json.get());
                        idempotency_engine
                            .complete(key, hlc, status_u16, &http_response_headers, body_str)
                            .await;
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
                    Self::log_dispatch_error(
                        &ctx.proxy_context.request_topic,
                        &ctx.proxy_context.buffer,
                        dispatch_result,
                    );
                }
            }
        }
        ctx.proxy_context.buffer.clear();
    }

    fn log_dispatch_error(
        request_topic: &str,
        buffer: &[u8],
        dispatch_result: pingora::Result<()>,
    ) {
        if let Err(e) = dispatch_result {
            log::error!(
                "DISPATCH failed. {:?} context: {} request_topic: {}\n{:?}",
                e.etype(),
                e.to_string(),
                request_topic,
                buffer
            );
        }
    }
}
