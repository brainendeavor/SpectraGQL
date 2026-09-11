use serde::Serialize;
use uuid::Uuid;

use crate::clock::HlcTimestamp;
use crate::payload::{PayloadType, RequestInfo, ResponseInfo, SanitizedPayload};

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationOutcome {
    Success,
    Failed,
    Rejected,
}

pub type EventStatus = OperationOutcome;

/// Unified completion event envelope combining request intent,
/// response outcome, duration, and causal timestamps (UUIDv7 + HLC).
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CompletionEvent {
    #[serde(with = "uuid::serde::simple")]
    pub id: Uuid,
    pub hlc: HlcTimestamp,
    pub status: OperationOutcome,
    pub duration_ms: u64,
    pub operation_name: Option<String>,
    pub request: SanitizedPayload<RequestInfo>,
    pub response: Option<ResponseInfo>,
    pub error: Option<String>,
    #[serde(rename = "type")]
    pub payload_type: PayloadType,
}

pub type TerminalEvent = CompletionEvent;

impl CompletionEvent {
    pub fn success(
        id: Uuid,
        hlc: HlcTimestamp,
        duration_ms: u64,
        operation_name: Option<String>,
        request: impl Into<SanitizedPayload<RequestInfo>>,
        response: ResponseInfo,
    ) -> Self {
        CompletionEvent {
            id,
            hlc,
            status: OperationOutcome::Success,
            duration_ms,
            operation_name,
            request: request.into(),
            response: Some(response),
            error: None,
            payload_type: PayloadType::Response,
        }
    }

    pub fn failure(
        id: Uuid,
        hlc: HlcTimestamp,
        duration_ms: u64,
        operation_name: Option<String>,
        request: impl Into<SanitizedPayload<RequestInfo>>,
        response: Option<ResponseInfo>,
        error: String,
    ) -> Self {
        CompletionEvent {
            id,
            hlc,
            status: OperationOutcome::Failed,
            duration_ms,
            operation_name,
            request: request.into(),
            response,
            error: Some(error),
            payload_type: PayloadType::Response,
        }
    }

    pub fn rejected(
        id: Uuid,
        hlc: HlcTimestamp,
        duration_ms: u64,
        operation_name: Option<String>,
        request: impl Into<SanitizedPayload<RequestInfo>>,
        reason: String,
    ) -> Self {
        CompletionEvent {
            id,
            hlc,
            status: OperationOutcome::Rejected,
            duration_ms,
            operation_name,
            request: request.into(),
            response: None,
            error: Some(reason),
            payload_type: PayloadType::Response,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HlcClock;
    use crate::payload::ResponseBody;
    use http::HeaderMap;

    #[test]
    fn test_terminal_event_success_serialization() {
        let clock = HlcClock::new();
        let (id, hlc) = clock.now_uuidv7();
        let req_parts = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let request_info = RequestInfo::new(id, hlc, req_parts);
        let resp_headers = HeaderMap::new();
        let resp_body = ResponseBody::new(&resp_headers, r#"{"data":{"user":{"id":"123"}}}"#);
        let response_info = ResponseInfo::new(id, hlc, resp_body, resp_headers);

        let event = TerminalEvent::success(
            id,
            hlc,
            42,
            Some("CreateUser".to_string()),
            request_info,
            response_info,
        );

        assert_eq!(event.status, EventStatus::Success);
        assert_eq!(event.duration_ms, 42);
        assert_eq!(event.operation_name.as_deref(), Some("CreateUser"));
        assert!(event.error.is_none());
        assert!(event.response.is_some());

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""status":"SUCCESS""#));
        assert!(json.contains(r#""durationMs":42"#));
        assert!(json.contains(r#""operationName":"CreateUser""#));
        assert!(json.contains(r#""type":"Response""#));
    }

    #[test]
    fn test_terminal_event_failure_serialization() {
        let clock = HlcClock::new();
        let (id, hlc) = clock.now_uuidv7();
        let req_parts = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let request_info = RequestInfo::new(id, hlc, req_parts);

        let event = TerminalEvent::failure(
            id,
            hlc,
            15,
            Some("UpdateAccount".to_string()),
            request_info,
            None,
            "Connection reset by peer".to_string(),
        );

        assert_eq!(event.status, EventStatus::Failed);
        assert_eq!(event.error.as_deref(), Some("Connection reset by peer"));
        assert!(event.response.is_none());

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains(r#""status":"FAILED""#));
        assert!(json.contains(r#""error":"Connection reset by peer""#));
    }

    #[test]
    fn test_terminal_event_sanitization() {
        let clock = HlcClock::new();
        let (id, hlc) = clock.now_uuidv7();
        let req_parts = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header("authorization", "Bearer secret-jwt-token-12345")
            .header("cookie", "session_token=abcde")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut request_info = RequestInfo::new(id, hlc, req_parts);
        let gql_info = crate::payload::GraphQLRequestInfo::new(
            r#"{"query":"mutation Login { login(password: \"my_secret_pass\") }","variables":{"password":"my_secret_pass"}}"#
        );
        let _ = gql_info.gql_request_body();
        request_info.gql = Some(gql_info);

        let resp_headers = HeaderMap::new();
        let resp_body = ResponseBody::new(&resp_headers, r#"{"data":{"login":{"ok":true}}}"#);
        let response_info = ResponseInfo::new(id, hlc, resp_body, resp_headers);

        let event = TerminalEvent::success(
            id,
            hlc,
            25,
            Some("Login".to_string()),
            request_info,
            response_info,
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("secret-jwt-token-12345"));
        assert!(!json.contains("my_secret_pass"));
        assert!(json.contains("Bearer [REDACTED]"));
        assert!(json.contains("[REDACTED]"));
        assert!(json.contains(r#"\"password\":\"[REDACTED]\""#));
    }
}
