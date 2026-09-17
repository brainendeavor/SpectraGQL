use crate::core::clock::HlcTimestamp;
use crate::gateway::REQUEST_ID_HEADER;
use crate::idempotency::{IdempotencyEngine, IdempotencyOutcome, REPLAY_HEADER};
use crate::protocol::GraphQLErrorResponse;
use pingora::proxy::Session;
use std::sync::Arc;
use uuid::Uuid;

pub enum IdempotencyInterceptResult {
    Conflict,
    Replay,
    NewKey(String),
    None,
}

/// Pipeline filter coordinating idempotency key extraction, in-flight locking,
/// conflict rejection (409), and cached replay responses.
pub struct IdempotencyFilter;

impl IdempotencyFilter {
    /// Checks for an explicit Idempotency-Key in request headers.
    /// If in-flight conflict or cached replay, renders HTTP response directly.
    pub async fn handle_explicit_key(
        session: &mut Session,
        engine: &Arc<IdempotencyEngine>,
        request_id: &Uuid,
        hlc: HlcTimestamp,
    ) -> pingora::Result<IdempotencyInterceptResult> {
        if let Some(key) = IdempotencyEngine::extract_idempotency_key(&session.req_header().headers) {
            match engine.check_or_insert(&key, hlc).await {
                IdempotencyOutcome::Conflict { hlc: conflict_hlc } => {
                    log::warn!(
                        "Idempotency conflict for key: {}, in-flight hlc: {}",
                        key,
                        conflict_hlc
                    );
                    Self::write_conflict_response(session, request_id, hlc, &key, conflict_hlc).await?;
                    return Ok(IdempotencyInterceptResult::Conflict);
                }
                IdempotencyOutcome::Replay {
                    status_code,
                    headers,
                    body,
                    ..
                } => {
                    log::info!("Idempotency replay for key: {}", key);
                    Self::write_replay_response(session, request_id, hlc, status_code, headers, body).await?;
                    return Ok(IdempotencyInterceptResult::Replay);
                }
                IdempotencyOutcome::New => {
                    return Ok(IdempotencyInterceptResult::NewKey(key));
                }
            }
        }
        Ok(IdempotencyInterceptResult::None)
    }

    /// Evaluates or inserts a computed mutation fingerprint when no explicit key was provided.
    pub async fn handle_fingerprint(
        session: &mut Session,
        engine: &Arc<IdempotencyEngine>,
        request_id: &Uuid,
        hlc: HlcTimestamp,
        client_id: &str,
        operation_name: Option<&str>,
        body_str: &str,
    ) -> pingora::Result<IdempotencyInterceptResult> {
        let fingerprint = IdempotencyEngine::compute_fingerprint(client_id, operation_name, body_str);
        match engine.check_or_insert(&fingerprint, hlc).await {
            IdempotencyOutcome::Conflict { hlc: conflict_hlc } => {
                log::warn!(
                    "Idempotency conflict for fingerprint: {}, in-flight hlc: {}",
                    fingerprint,
                    conflict_hlc
                );
                Self::write_fingerprint_conflict_response(session, request_id, hlc, conflict_hlc).await?;
                Ok(IdempotencyInterceptResult::Conflict)
            }
            IdempotencyOutcome::Replay {
                status_code,
                headers,
                body,
                ..
            } => {
                log::info!("Idempotency replay for fingerprint");
                Self::write_replay_response(session, request_id, hlc, status_code, headers, body).await?;
                Ok(IdempotencyInterceptResult::Replay)
            }
            IdempotencyOutcome::New => Ok(IdempotencyInterceptResult::NewKey(fingerprint)),
        }
    }

    async fn write_conflict_response(
        session: &mut Session,
        request_id: &Uuid,
        hlc: HlcTimestamp,
        key: &str,
        conflict_hlc: HlcTimestamp,
    ) -> pingora::Result<()> {
        let mut header = pingora::http::ResponseHeader::build(409, None)?;
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header(REQUEST_ID_HEADER, request_id.to_string());
        let _ = header.insert_header("x-spectra-hlc", hlc.to_compact_string());
        let body = GraphQLErrorResponse::conflict(Some(key), conflict_hlc).to_json_string();
        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;
        session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
        Ok(())
    }

    async fn write_fingerprint_conflict_response(
        session: &mut Session,
        request_id: &Uuid,
        hlc: HlcTimestamp,
        conflict_hlc: HlcTimestamp,
    ) -> pingora::Result<()> {
        let mut header = pingora::http::ResponseHeader::build(409, None)?;
        let _ = header.insert_header("content-type", "application/json");
        let _ = header.insert_header(REQUEST_ID_HEADER, request_id.to_string());
        let _ = header.insert_header("x-spectra-hlc", hlc.to_compact_string());
        let body = GraphQLErrorResponse::conflict(None, conflict_hlc).to_json_string();
        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;
        session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
        Ok(())
    }

    async fn write_replay_response(
        session: &mut Session,
        request_id: &Uuid,
        hlc: HlcTimestamp,
        status_code: u16,
        headers: Vec<(String, String)>,
        body: String,
    ) -> pingora::Result<()> {
        let mut header = pingora::http::ResponseHeader::build(status_code, None)?;
        for (k, v) in headers {
            let _ = header.insert_header(k, v);
        }
        let _ = header.insert_header("access-control-allow-origin", "*");
        let _ = header.insert_header(
            "access-control-expose-headers",
            "x-spectra-request-id, x-spectra-hlc, x-spectra-dispatch, x-spectra-idempotent-replay",
        );
        let _ = header.insert_header(REPLAY_HEADER, "true");
        let _ = header.insert_header(REQUEST_ID_HEADER, request_id.to_string());
        let _ = header.insert_header("x-spectra-hlc", hlc.to_compact_string());
        session.set_keepalive(None);
        session.write_response_header(Box::new(header), false).await?;
        session.write_response_body(Some(bytes::Bytes::from(body)), true).await?;
        Ok(())
    }
}
