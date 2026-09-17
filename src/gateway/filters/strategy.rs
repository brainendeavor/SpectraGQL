use crate::core::clock::HlcTimestamp;
use crate::core::config::{ModeADispatchPolicy, SpectraRouteConfig};
use crate::gateway::composite_service::CompositeServiceProxyCtx;
use crate::gateway::REQUEST_ID_HEADER;
use crate::idempotency::IdempotencyEngine;
use crate::protocol::RequestInfo;
use crate::telemetry::{DispatchHandler, DispatchMethod};
use pingora::proxy::Session;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Standardized GraphQL command receipt conforming to Mode B edge contract.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandReceipt {
    pub command_id: String,
    pub hlc: String,
    pub status: String,
}

/// Generates a standardized GraphQL command receipt conforming to Mode B edge contract.
pub fn generate_command_receipt(
    field_or_op: &str,
    command_id: &uuid::Uuid,
    hlc: &HlcTimestamp,
    receipt_status: &str,
) -> serde_json::Value {
    let receipt = CommandReceipt {
        command_id: command_id.to_string(),
        hlc: hlc.to_compact_string(),
        status: receipt_status.to_string(),
    };
    serde_json::json!({
        "data": {
            field_or_op: receipt
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
        dispatch_method: &DispatchMethod,
        idempotency_engine: &Arc<IdempotencyEngine>,
    ) -> pingora::Result<bool> {
        log::info!("Executing Mode B edge termination for operation: {}", route.operation);
        ctx.proxy_context.is_mode_b_terminated = true;

        // Commit command to event broker (guaranteed sanitized)
        let sanitized_request = request_info.clone().into_sanitized();
        let dispatch_result = dispatch_method.dispatch_request_info(&sanitized_request).await;
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
                fake_headers.insert("content-type", http::HeaderValue::from_static("application/json"));
                if let Ok(val) = http::HeaderValue::try_from(ctx.proxy_context.request_id.to_string()) {
                    fake_headers.insert(REQUEST_ID_HEADER, val);
                }
                if let Ok(val) = http::HeaderValue::try_from(ctx.proxy_context.hlc.to_compact_string()) {
                    fake_headers.insert("x-spectra-hlc", val);
                }
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

        let mut header = pingora::http::ResponseHeader::build(200, None)?;
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header("access-control-allow-origin", "*");
        let _ = header.insert_header(
            "access-control-expose-headers",
            "x-spectra-request-id, x-spectra-hlc, x-spectra-dispatch, x-spectra-idempotent-replay",
        );
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
        dispatch_method: &DispatchMethod,
    ) {
        match policy {
            ModeADispatchPolicy::RawAudit => {
                let sanitized_request = request_info.clone().into_sanitized();
                let _ = dispatch_method.dispatch_request_info(&sanitized_request).await;
            }
            ModeADispatchPolicy::ResponseOnly
            | ModeADispatchPolicy::ResponseWithFailure
            | ModeADispatchPolicy::None => {
                // Defer dispatch until response logging, or suppress in ModeADispatchPolicy::None
            }
        }
    }
}
