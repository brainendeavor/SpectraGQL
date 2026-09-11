use crate::dispatch::DispatchHandler;
use crate::payload::RequestInfo;
use crate::proxy::composite_service::CompositeServiceProxyCtx;
use crate::proxy::REQUEST_ID_HEADER;
use crate::ratify::IdempotencyEngine;
use crate::spectra_config::{ModeADispatchPolicy, SpectraRouteConfig};
use pingora::proxy::Session;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Generates a standardized GraphQL command receipt conforming to Mode B edge contract.
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
                "hlc": hlc.to_compact_string(),
                "status": receipt_status
            }
        }
    })
}

/// Routes GraphQL operations according to configured ExecutionStrategy (Mode A vs Mode B).
pub struct StrategyRouter;

impl StrategyRouter {
    /// Matches an inbound request against configured route overrides.
    pub fn match_route<'a>(
        routes: &'a HashMap<String, SpectraRouteConfig>,
        request_info: &RequestInfo,
    ) -> Option<&'a SpectraRouteConfig> {
        request_info
            .gql
            .as_ref()
            .and_then(|gql| routes.values().find(|r| gql.matches_operation(&r.operation)))
    }

    /// Handles Mode B (AsyncEdgeCommand): commits command to event broker and returns deterministic receipt.
    pub async fn handle_mode_b_edge(
        session: &mut Session,
        ctx: &mut CompositeServiceProxyCtx,
        route: &SpectraRouteConfig,
        request_info: &RequestInfo,
        dispatch_method: &crate::dispatch::DispatchMethod,
        idempotency_engine: &Arc<IdempotencyEngine>,
    ) -> pingora::Result<bool> {
        log::info!("Executing Mode B edge termination for operation: {}", route.operation);
        ctx.proxy_context.is_mode_b_terminated = true;

        // Commit command to event broker
        let dispatch_result = dispatch_method.dispatch_request_info(request_info).await;
        let dispatch_ok = match &dispatch_result {
            Ok(_) => true,
            Err(e) => {
                log::error!(
                    "DISPATCH failed. {:?} context: {} request_topic: {}\n{:?}",
                    e.etype(),
                    e.to_string(),
                    ctx.proxy_context.request_topic,
                    ctx.proxy_context.buffer
                );
                false
            }
        };

        let receipt_status = if dispatch_ok {
            route.receipt_status.as_str()
        } else {
            "DISPATCH_FAILED"
        };

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
            receipt_status,
        );
        let receipt_body = receipt_json.to_string();

        if dispatch_ok {
            // Complete idempotency tracking
            if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                let mut fake_headers = http::HeaderMap::new();
                fake_headers.insert("content-type", "application/json".parse().unwrap());
                fake_headers.insert(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string().parse().unwrap());
                fake_headers.insert("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string().parse().unwrap());
                idempotency_engine
                    .complete(key, ctx.proxy_context.hlc, 200, &fake_headers, &receipt_body)
                    .await;
            }
        } else {
            // Dispatch failed: remove in-progress idempotency key so client can retry
            if let Some(key) = ctx.proxy_context.idempotency_key.as_ref() {
                idempotency_engine.remove(key).await;
            }
        }

        let mut header = pingora::http::ResponseHeader::build(200, None).unwrap();
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
        let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
        if !dispatch_ok {
            let _ = header.insert_header("x-spectra-dispatch", "failed");
        }

        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;
        session.write_response_body(Some(bytes::Bytes::from(receipt_body)), true).await?;

        Ok(true)
    }

    /// Evaluates Mode A route override and upstream resolution.
    pub fn resolve_mode_a_upstream(
        route: &SpectraRouteConfig,
        named_upstreams: &HashMap<String, SocketAddr>,
    ) -> Option<SocketAddr> {
        if let Some(upstream_name) = &route.upstream {
            if let Some(addr) = named_upstreams.get(upstream_name) {
                log::info!(
                    "Mode A route override: op '{}' routing to microservice '{}' ({})",
                    route.operation,
                    upstream_name,
                    addr
                );
                return Some(*addr);
            } else {
                log::warn!(
                    "Named upstream '{}' not found in config for op '{}', using default",
                    upstream_name,
                    route.operation
                );
            }
        }
        None
    }

    /// Handles Mode A dispatch policy (RawAudit vs deferred response).
    pub async fn handle_mode_a_dispatch_policy(
        policy: ModeADispatchPolicy,
        request_info: &RequestInfo,
        dispatch_method: &crate::dispatch::DispatchMethod,
    ) {
        match policy {
            ModeADispatchPolicy::RawAudit => {
                let _ = dispatch_method.dispatch_request_info(request_info).await;
            }
            ModeADispatchPolicy::ResponseOnly | ModeADispatchPolicy::ResponseWithFailure => {
                // Defer dispatch until response logging
            }
        }
    }
}
