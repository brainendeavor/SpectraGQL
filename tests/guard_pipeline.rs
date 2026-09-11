use spectragql::clock::HlcTimestamp;
use spectragql::guards::request::{
    GraphQLSyntaxGuard, HeaderValidationGuard, RequestGuardPipeline,
};
use spectragql::guards::response::{ResponseGuardPipeline, SensitiveDataResponseGuard};
use spectragql::guards::rules::NativeRuleEvaluator;
use spectragql::guards::{GuardContext, GuardRejection, GuardVerdict};
use std::sync::Arc;
use uuid::Uuid;

fn test_hlc() -> HlcTimestamp {
    HlcTimestamp::new(1700000000000, 0)
}

#[test]
fn test_guard_context_initialization() {
    let req_id = Uuid::new_v4();
    let hlc = test_hlc();
    let ctx = GuardContext::new(req_id, hlc);

    assert_eq!(ctx.request_id, req_id);
    assert_eq!(ctx.hlc, hlc);
    assert!(ctx.operation_name.is_none());
    assert!(ctx.operation_type.is_none());
    assert!(ctx.metadata.is_empty());
}

#[test]
fn test_guard_rejection_graphql_envelope() {
    let rejection = GuardRejection::new(
        http::StatusCode::BAD_REQUEST,
        "SYNTAX_ERROR",
        "Failed to parse query",
    )
    .with_details(serde_json::json!({"line": 12, "column": 5}));

    let json_resp = rejection.to_graphql_response();
    let val: serde_json::Value = serde_json::from_str(&json_resp).expect("Valid JSON");

    assert!(val.get("errors").is_some());
    let error = &val["errors"][0];
    assert_eq!(error["message"], "Failed to parse query");
    assert_eq!(error["extensions"]["code"], "SYNTAX_ERROR");
    assert_eq!(error["extensions"]["details"]["line"], 12);
}

#[test]
fn test_graphql_syntax_guard_valid_and_invalid() {
    let syntax_guard = GraphQLSyntaxGuard;
    let req_id = Uuid::new_v4();
    let hlc = test_hlc();

    // 1. Valid GraphQL query
    let mut ctx = GuardContext::new(req_id, hlc);
    let mut req = http::Request::builder()
        .uri("/graphql")
        .method("POST")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let valid_body = r#"{"query": "query GetViewer { viewer { id name } }", "operationName": "GetViewer"}"#;
    let res = syntax_guard.guard_request(&mut ctx, &mut req, valid_body);
    assert_eq!(res.unwrap(), GuardVerdict::Pass);
    assert_eq!(ctx.operation_name.as_deref(), Some("GetViewer"));
    assert_eq!(
        ctx.operation_type,
        Some(spectragql::payload::GraphQLOperationType::Query)
    );

    // 2. Non-GraphQL path passes transparently
    let mut ctx_rest = GuardContext::new(req_id, hlc);
    let mut req_rest = http::Request::builder()
        .uri("/api/users")
        .method("GET")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res_rest = syntax_guard.guard_request(&mut ctx_rest, &mut req_rest, "");
    assert_eq!(res_rest.unwrap(), GuardVerdict::Pass);

    // 3. Empty body rejected
    let mut ctx_empty = GuardContext::new(req_id, hlc);
    let res_empty = syntax_guard.guard_request(&mut ctx_empty, &mut req, "   ");
    assert!(res_empty.is_err());
    let err = res_empty.unwrap_err();
    assert_eq!(err.status_code, http::StatusCode::BAD_REQUEST);
    assert_eq!(err.code, "GRAPHQL_PARSE_FAILED");

    // 4. Malformed syntax rejected
    let mut ctx_malformed = GuardContext::new(req_id, hlc);
    let malformed_body = r#"{"query": "query { viewer { id"}"#; // unclosed braces
    let res_malformed = syntax_guard.guard_request(&mut ctx_malformed, &mut req, malformed_body);
    assert!(res_malformed.is_err());
    let err = res_malformed.unwrap_err();
    assert_eq!(err.status_code, http::StatusCode::BAD_REQUEST);
    assert_eq!(err.code, "GRAPHQL_SYNTAX_ERROR");
}

#[test]
fn test_header_validation_guard() {
    let header_guard = HeaderValidationGuard::default();
    let mut ctx = GuardContext::new(Uuid::new_v4(), test_hlc());

    // 1. POST with application/json passes
    let mut req_post_json = http::Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(
        header_guard
            .guard_request(&mut ctx, &mut req_post_json, "{}")
            .unwrap(),
        GuardVerdict::Pass
    );

    // 2. POST with text/plain rejected
    let mut req_post_plain = http::Request::builder()
        .method("POST")
        .header("content-type", "text/plain")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res = header_guard.guard_request(&mut ctx, &mut req_post_plain, "hello");
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, "INVALID_CONTENT_TYPE");

    // 3. GET without content-type passes
    let mut req_get = http::Request::builder()
        .method("GET")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(
        header_guard
            .guard_request(&mut ctx, &mut req_get, "")
            .unwrap(),
        GuardVerdict::Pass
    );
}

#[test]
fn test_sensitive_data_response_guard() {
    let response_guard = SensitiveDataResponseGuard::new().with_forbidden_tokens(vec![
        "SUPER_SECRET_KEY".to_string(),
        "stripe_sk_live_".to_string(),
    ]);

    let ctx = GuardContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    // 1. Clean response passes
    let clean_body = b"{\"data\":{\"user\":{\"name\":\"Alice\"}}}";
    assert_eq!(
        response_guard
            .guard_response(&ctx, &mut resp, clean_body)
            .unwrap(),
        GuardVerdict::Pass
    );

    // 2. Response with forbidden token rejected
    let leaked_body = b"{\"data\":{\"user\":{\"secret\":\"SUPER_SECRET_KEY_12345\"}}}";
    let res = response_guard.guard_response(&ctx, &mut resp, leaked_body);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err.status_code, http::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(err.code, "DATA_LEAK_PREVENTED");
}

#[test]
fn test_native_rule_evaluator_integration() {
    let mut evaluator = NativeRuleEvaluator::new();
    evaluator.register("sensitive_data_check", |input| {
        // Flag if customer SSN or unmasked PAN exists
        if let Some(ssn) = input.pointer("/data/customer/ssn") {
            return !ssn.as_str().unwrap_or("").starts_with("***-**-");
        }
        false
    });

    let eval_arc = Arc::new(evaluator);
    let guard = SensitiveDataResponseGuard::new().with_evaluator(eval_arc);
    let ctx = GuardContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    // Masked SSN passes
    let masked_body = br#"{"data":{"customer":{"name":"Bob","ssn":"***-**-1234"}}}"#;
    assert_eq!(
        guard.guard_response(&ctx, &mut resp, masked_body).unwrap(),
        GuardVerdict::Pass
    );

    // Unmasked SSN triggers rule rejection
    let unmasked_body = br#"{"data":{"customer":{"name":"Bob","ssn":"123-45-6789"}}}"#;
    let res = guard.guard_response(&ctx, &mut resp, unmasked_body);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code, "DATA_LEAK_PREVENTED");
}

#[test]
fn test_guard_pipeline_orchestration() {
    let req_pipeline = RequestGuardPipeline::new()
        .with_guard(HeaderValidationGuard::default())
        .with_guard(GraphQLSyntaxGuard);

    let mut ctx = GuardContext::new(Uuid::new_v4(), test_hlc());

    // 1. Compliant request passes entire pipeline
    let mut req_valid = http::Request::builder()
        .uri("/graphql")
        .method("POST")
        .header("content-type", "application/json")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let valid_body = r#"{"query": "mutation UpdateProfile { updateProfile { id } }"}"#;
    let res = req_pipeline.guard_request(&mut ctx, &mut req_valid, valid_body);
    assert_eq!(res.unwrap(), GuardVerdict::Pass);
    assert_eq!(ctx.operation_name.as_deref(), Some("UpdateProfile"));

    // 2. Request failing first guard short-circuits before second guard runs
    let mut req_invalid_header = http::Request::builder()
        .uri("/graphql")
        .method("POST")
        .header("content-type", "text/html")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res_short_circuit =
        req_pipeline.guard_request(&mut ctx, &mut req_invalid_header, valid_body);
    assert!(res_short_circuit.is_err());
    assert_eq!(
        res_short_circuit.unwrap_err().code,
        "INVALID_CONTENT_TYPE"
    );

    // 3. Response pipeline orchestration
    let resp_pipeline = ResponseGuardPipeline::new().with_guard(
        SensitiveDataResponseGuard::new().with_forbidden_tokens(vec!["LEAK".to_string()]),
    );

    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;
    assert!(
        resp_pipeline
            .guard_response(&ctx, &mut resp, b"{\"data\": \"LEAK\"}")
            .is_err()
    );
    assert!(
        resp_pipeline
            .guard_response(&ctx, &mut resp, b"{\"data\": \"CLEAN\"}")
            .is_ok()
    );
}

#[test]
fn test_response_interceptor_transform_anonymization() {
    use spectragql::guards::{InterceptorVerdict, ResponseInterceptor, ResponseInterceptorPipeline};

    struct CustomerAnonymizerInterceptor;

    impl ResponseInterceptor for CustomerAnonymizerInterceptor {
        fn intercept_response(
            &self,
            _ctx: &GuardContext,
            _parts: &mut http::response::Parts,
            body: &[u8],
        ) -> InterceptorVerdict {
            if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(body) {
                // Anonymize user names and emails
                if let Some(user) = val.pointer_mut("/data/viewer") {
                    if user.get("name").is_some() {
                        user["name"] = serde_json::json!("ANONYMIZED_USER");
                    }
                    if user.get("email").is_some() {
                        user["email"] = serde_json::json!("redacted@example.com");
                    }
                }
                let modified = serde_json::to_vec(&val).unwrap();
                return InterceptorVerdict::Transform {
                    headers: None,
                    body: Some(modified),
                };
            }
            InterceptorVerdict::Pass
        }
    }

    let pipeline = ResponseInterceptorPipeline::new().with_interceptor(CustomerAnonymizerInterceptor);
    let ctx = GuardContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    let original_body = br#"{"data":{"viewer":{"id":"usr_123","name":"John Doe","email":"john@secret.org"}}}"#;
    let verdict = pipeline.intercept_response(&ctx, &mut resp, original_body);

    match verdict {
        InterceptorVerdict::Transform { body: Some(new_body), .. } => {
            let json_str = String::from_utf8(new_body).expect("Valid UTF-8");
            assert!(json_str.contains("ANONYMIZED_USER"));
            assert!(json_str.contains("redacted@example.com"));
            assert!(!json_str.contains("John Doe"));
            assert!(!json_str.contains("john@secret.org"));
            assert!(json_str.contains("usr_123")); // Preserved ID
        }
        _ => panic!("Expected InterceptorVerdict::Transform"),
    }
}

