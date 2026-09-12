use http::{HeaderMap, Request};
use spectragql::HlcClock;
use spectragql::gateway::filters::strategy::generate_command_receipt;
use spectragql::interceptors::InterceptorRejection;
use spectragql::protocol::{
    GraphQLErrorResponse, GraphQLRequestInfo, RawPayload, RequestInfo, ResponseBody, ResponseInfo,
    SanitizedPayload,
};
use spectragql::telemetry::{CompletionEvent, EventStatus};

#[test]
fn test_typestate_raw_to_sanitized_transition() {
    let clock = HlcClock::new();
    let (id, hlc) = clock.now_uuidv7();

    let req = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", "Bearer super-secret-jwt-token-999")
        .header("x-api-key", "secret-api-key-888")
        .body(())
        .unwrap();
    let (parts, _) = req.into_parts();

    let mut request_info = RequestInfo::new(id, hlc, parts);
    let gql_info = GraphQLRequestInfo::new(
        r#"{"query":"mutation ChangePassword($newPass: String!) { changePassword(password: $newPass) }","variables":{"newPass":"my-super-secret-pw"}}"#,
    );
    let _ = gql_info.gql_request_body();
    request_info.gql = Some(gql_info);

    // Wrap in compile-time Raw state
    let raw: RawPayload<RequestInfo> = RawPayload::new(request_info);
    assert_eq!(raw.request_id, id);

    // Transition state from Raw to Sanitized
    let sanitized: SanitizedPayload<RequestInfo> = raw.sanitize();

    // Verify Deref ergonomics
    assert_eq!(sanitized.request_id, id);
    assert_eq!(sanitized.http.method, "POST");

    // Verify PII redactions
    let auth_val = sanitized.http.headers.get("authorization").unwrap().to_str().unwrap();
    assert_eq!(auth_val, "Bearer [REDACTED]");

    let api_key_val = sanitized.http.headers.get("x-api-key").unwrap().to_str().unwrap();
    assert_eq!(api_key_val, "[REDACTED]");

    let gql_body = sanitized.gql.as_ref().unwrap().gql_request_body().unwrap();
    let serialized_gql = serde_json::to_string(&gql_body).unwrap();
    assert!(!serialized_gql.contains("my-super-secret-pw"));
    assert!(serialized_gql.contains("[REDACTED]"));
}

#[test]
fn test_request_info_into_sanitized_convenience() {
    let clock = HlcClock::new();
    let (id, hlc) = clock.now_uuidv7();

    let req = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", "Bearer token-123")
        .body(())
        .unwrap();
    let (parts, _) = req.into_parts();

    let request_info = RequestInfo::new(id, hlc, parts);
    let sanitized: SanitizedPayload<RequestInfo> = request_info.into_sanitized();

    let auth_header = sanitized.http.headers.get("authorization").unwrap().to_str().unwrap();
    assert_eq!(auth_header, "Bearer [REDACTED]");
}

#[test]
fn test_completion_event_requires_sanitized_payload() {
    let clock = HlcClock::new();
    let (id, hlc) = clock.now_uuidv7();

    let req_parts = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("x-auth-token", "secret_auth_token_val")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let request_info = RequestInfo::new(id, hlc, req_parts);

    let resp_headers = HeaderMap::new();
    let resp_body = ResponseBody::new(&resp_headers, r#"{"data":{"result":"ok"}}"#);
    let response_info = ResponseInfo::new(id, hlc, resp_body, resp_headers);

    // Passing RequestInfo directly transitions it into SanitizedPayload
    let event = CompletionEvent::success(
        id,
        hlc,
        35,
        Some("TestOp".to_string()),
        request_info,
        response_info,
    );

    assert_eq!(event.status, EventStatus::Success);
    assert_eq!(event.duration_ms, 35);

    // event.request is a SanitizedPayload<RequestInfo>
    let token_val = event.request.http.headers.get("x-auth-token").unwrap().to_str().unwrap();
    assert_eq!(token_val, "[REDACTED]");

    let json = serde_json::to_string(&event).unwrap();
    assert!(!json.contains("secret_auth_token_val"));
    assert!(json.contains("[REDACTED]"));
    assert!(json.contains(r#""status":"SUCCESS""#));
}

#[test]
fn test_graphql_error_response_serialization() {
    let clock = HlcClock::new();
    let (_, hlc) = clock.now_uuidv7();

    // In-flight conflict with key
    let conflict_with_key = GraphQLErrorResponse::conflict(Some("order-key-42"), hlc);
    let json_str = conflict_with_key.to_json_string();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(
        parsed["errors"][0]["message"],
        "A mutation with idempotency key 'order-key-42' is currently in flight"
    );
    assert_eq!(parsed["errors"][0]["extensions"]["code"], "CONFLICT");
    assert_eq!(
        parsed["errors"][0]["extensions"]["hlc"],
        hlc.to_compact_string()
    );

    // In-flight conflict without key (fingerprint conflict)
    let conflict_fp = GraphQLErrorResponse::conflict(None, hlc);
    let json_fp_str = conflict_fp.to_json_string();
    let parsed_fp: serde_json::Value = serde_json::from_str(&json_fp_str).unwrap();

    assert_eq!(
        parsed_fp["errors"][0]["message"],
        "A mutation with idempotency key is currently in flight"
    );
    assert_eq!(parsed_fp["errors"][0]["extensions"]["code"], "CONFLICT");

    // Generic error
    let generic_err = GraphQLErrorResponse::error("NOT_FOUND", "Entity does not exist");
    let json_generic = generic_err.to_json_string();
    let parsed_generic: serde_json::Value = serde_json::from_str(&json_generic).unwrap();
    assert_eq!(parsed_generic["errors"][0]["message"], "Entity does not exist");
    assert_eq!(parsed_generic["errors"][0]["extensions"]["code"], "NOT_FOUND");
}

#[test]
fn test_interceptor_rejection_converts_to_graphql_error_response() {
    let rejection = InterceptorRejection::new(
        http::StatusCode::BAD_REQUEST,
        "GRAPHQL_SYNTAX_ERROR",
        "Unexpected token at position 12",
    )
    .with_details(serde_json::json!({ "line": 1, "column": 12 }));

    let json_str = rejection.to_graphql_response();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(parsed["errors"][0]["message"], "Unexpected token at position 12");
    assert_eq!(parsed["errors"][0]["extensions"]["code"], "GRAPHQL_SYNTAX_ERROR");
    assert_eq!(parsed["errors"][0]["extensions"]["details"]["line"], 1);
    assert_eq!(parsed["errors"][0]["extensions"]["details"]["column"], 12);
}

#[test]
fn test_command_receipt_schema_consistency() {
    let clock = HlcClock::new();
    let (cmd_id, hlc) = clock.now_uuidv7();

    let receipt = generate_command_receipt("processOrder", &cmd_id, &hlc, "QUEUED");
    assert_eq!(receipt["data"]["processOrder"]["commandId"], cmd_id.to_string());
    assert_eq!(receipt["data"]["processOrder"]["hlc"], hlc.to_compact_string());
    assert_eq!(receipt["data"]["processOrder"]["status"], "QUEUED");
}
