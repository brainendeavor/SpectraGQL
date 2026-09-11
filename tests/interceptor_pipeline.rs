use spectragql::clock::HlcTimestamp;
use spectragql::interceptors::evaluators::CelRuleEvaluator;
use spectragql::interceptors::request::{
    GraphQLSyntaxInterceptor, HeaderValidationInterceptor, RequestInterceptorPipeline,
};
use spectragql::interceptors::response::{
    ResponseInterceptor, ResponseInterceptorPipeline, SensitiveDataResponseInterceptor,
};
use spectragql::interceptors::rules::{NativeRuleEvaluator, RuleEvaluator};
use spectragql::interceptors::{
    InterceptorContext, InterceptorRejection, InterceptorVerdict, RequestInterceptor,
};
use std::sync::Arc;
use uuid::Uuid;

fn test_hlc() -> HlcTimestamp {
    HlcTimestamp::new(1700000000000, 0)
}

#[test]
fn test_interceptor_context_initialization() {
    let req_id = Uuid::new_v4();
    let hlc = test_hlc();
    let ctx = InterceptorContext::new(req_id, hlc);

    assert_eq!(ctx.request_id, req_id);
    assert_eq!(ctx.hlc, hlc);
    assert!(ctx.operation_name.is_none());
    assert!(ctx.operation_type.is_none());
    assert!(ctx.metadata.is_empty());
}

#[test]
fn test_interceptor_rejection_graphql_envelope() {
    let rejection = InterceptorRejection::new(
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
fn test_graphql_syntax_interceptor_valid_and_invalid() {
    let syntax_interceptor = GraphQLSyntaxInterceptor;
    let req_id = Uuid::new_v4();
    let hlc = test_hlc();

    // 1. Valid GraphQL query
    let mut ctx = InterceptorContext::new(req_id, hlc);
    let mut req = http::Request::builder()
        .uri("/graphql")
        .method("POST")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let valid_body = r#"{"query": "query GetViewer { viewer { id name } }", "operationName": "GetViewer"}"#;
    let res = syntax_interceptor.intercept_request(&mut ctx, &mut req, valid_body);
    assert_eq!(res, InterceptorVerdict::Pass);
    assert_eq!(ctx.operation_name.as_deref(), Some("GetViewer"));
    assert_eq!(
        ctx.operation_type,
        Some(spectragql::payload::GraphQLOperationType::Query)
    );

    // 2. Non-GraphQL path passes transparently
    let mut ctx_rest = InterceptorContext::new(req_id, hlc);
    let mut req_rest = http::Request::builder()
        .uri("/api/users")
        .method("GET")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res_rest = syntax_interceptor.intercept_request(&mut ctx_rest, &mut req_rest, "");
    assert_eq!(res_rest, InterceptorVerdict::Pass);

    // 3. Empty body rejected
    let mut ctx_empty = InterceptorContext::new(req_id, hlc);
    let res_empty = syntax_interceptor.intercept_request(&mut ctx_empty, &mut req, "   ");
    match res_empty {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.status_code, http::StatusCode::BAD_REQUEST);
            assert_eq!(err.code, "GRAPHQL_PARSE_FAILED");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }

    // 4. Malformed syntax rejected
    let mut ctx_malformed = InterceptorContext::new(req_id, hlc);
    let malformed_body = r#"{"query": "query { viewer { id"}"#; // unclosed braces
    let res_malformed = syntax_interceptor.intercept_request(&mut ctx_malformed, &mut req, malformed_body);
    match res_malformed {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.status_code, http::StatusCode::BAD_REQUEST);
            assert_eq!(err.code, "GRAPHQL_SYNTAX_ERROR");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
}

#[test]
fn test_header_validation_interceptor() {
    let header_interceptor = HeaderValidationInterceptor::default();
    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());

    // 1. POST with application/json passes
    let mut req_post_json = http::Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(
        header_interceptor.intercept_request(&mut ctx, &mut req_post_json, "{}"),
        InterceptorVerdict::Pass
    );

    // 2. POST with text/plain rejected
    let mut req_post_plain = http::Request::builder()
        .method("POST")
        .header("content-type", "text/plain")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res = header_interceptor.intercept_request(&mut ctx, &mut req_post_plain, "hello");
    match res {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.code, "INVALID_CONTENT_TYPE");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }

    // 3. GET without content-type passes
    let mut req_get = http::Request::builder()
        .method("GET")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(
        header_interceptor.intercept_request(&mut ctx, &mut req_get, ""),
        InterceptorVerdict::Pass
    );
}

#[test]
fn test_sensitive_data_response_interceptor() {
    let response_interceptor = SensitiveDataResponseInterceptor::new().with_forbidden_tokens(vec![
        "SUPER_SECRET_KEY".to_string(),
        "stripe_sk_live_".to_string(),
    ]);

    let ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    // 1. Clean response passes
    let clean_body = b"{\"data\":{\"user\":{\"name\":\"Alice\"}}}";
    assert_eq!(
        response_interceptor.intercept_response(&ctx, &mut resp, clean_body),
        InterceptorVerdict::Pass
    );

    // 2. Response with forbidden token rejected
    let leaked_body = b"{\"data\":{\"user\":{\"secret\":\"SUPER_SECRET_KEY_12345\"}}}";
    let res = response_interceptor.intercept_response(&ctx, &mut resp, leaked_body);
    match res {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.status_code, http::StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(err.code, "DATA_LEAK_PREVENTED");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
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
    let interceptor = SensitiveDataResponseInterceptor::new().with_evaluator(eval_arc);
    let ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    // Masked SSN passes
    let masked_body = br#"{"data":{"customer":{"name":"Bob","ssn":"***-**-1234"}}}"#;
    assert_eq!(
        interceptor.intercept_response(&ctx, &mut resp, masked_body),
        InterceptorVerdict::Pass
    );

    // Unmasked SSN triggers rule rejection
    let unmasked_body = br#"{"data":{"customer":{"name":"Bob","ssn":"123-45-6789"}}}"#;
    let res = interceptor.intercept_response(&ctx, &mut resp, unmasked_body);
    match res {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.code, "DATA_LEAK_PREVENTED");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
}

#[test]
fn test_interceptor_pipeline_orchestration() {
    let req_pipeline = RequestInterceptorPipeline::new()
        .with_interceptor(HeaderValidationInterceptor::default())
        .with_interceptor(GraphQLSyntaxInterceptor);

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());

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
    let res = req_pipeline.intercept_request(&mut ctx, &mut req_valid, valid_body);
    assert_eq!(res, InterceptorVerdict::Pass);
    assert_eq!(ctx.operation_name.as_deref(), Some("UpdateProfile"));

    // 2. Request failing first interceptor short-circuits before second interceptor runs
    let mut req_invalid_header = http::Request::builder()
        .uri("/graphql")
        .method("POST")
        .header("content-type", "text/html")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let res_short_circuit =
        req_pipeline.intercept_request(&mut ctx, &mut req_invalid_header, valid_body);
    match res_short_circuit {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.code, "INVALID_CONTENT_TYPE");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }

    // 3. Response pipeline orchestration
    let resp_pipeline = ResponseInterceptorPipeline::new().with_interceptor(
        SensitiveDataResponseInterceptor::new().with_forbidden_tokens(vec!["LEAK".to_string()]),
    );

    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;
    match resp_pipeline.intercept_response(&ctx, &mut resp, b"{\"data\": \"LEAK\"}") {
        InterceptorVerdict::Reject(err) => {
            assert_eq!(err.code, "DATA_LEAK_PREVENTED");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
    assert_eq!(
        resp_pipeline.intercept_response(&ctx, &mut resp, b"{\"data\": \"CLEAN\"}"),
        InterceptorVerdict::Pass
    );
}

#[test]
fn test_response_interceptor_transform_anonymization() {
    struct CustomerAnonymizerInterceptor;

    impl ResponseInterceptor for CustomerAnonymizerInterceptor {
        fn intercept_response(
            &self,
            _ctx: &InterceptorContext,
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
    let ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
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

#[test]
fn test_cel_rule_evaluator_standalone() {
    let mut cel = CelRuleEvaluator::new();

    // 1. Mandatory tenant header check
    cel.register("tenant_check", r#"request.headers["x-tenant-id"] != """#)
        .expect("CEL compile");

    // 2. Numeric threshold on mutation limit
    cel.register("limit_check", r#"variables.limit <= 100"#)
        .expect("CEL compile");

    // 3. Array membership
    cel.register("role_check", r#""admin" in claims.roles"#)
        .expect("CEL compile");

    // Test tenant_check
    let valid_req = serde_json::json!({
        "request": {
            "headers": {
                "x-tenant-id": "tenant_abc123"
            }
        }
    });
    assert!(cel.evaluate("tenant_check", &valid_req).unwrap());

    let invalid_req = serde_json::json!({
        "request": {
            "headers": {
                "x-tenant-id": ""
            }
        }
    });
    assert!(!cel.evaluate("tenant_check", &invalid_req).unwrap());

    // Test limit_check
    let ok_limit = serde_json::json!({
        "variables": { "limit": 50 }
    });
    assert!(cel.evaluate("limit_check", &ok_limit).unwrap());

    let excess_limit = serde_json::json!({
        "variables": { "limit": 500 }
    });
    assert!(!cel.evaluate("limit_check", &excess_limit).unwrap());

    // Test role_check
    let admin_user = serde_json::json!({
        "claims": { "roles": ["user", "admin"] }
    });
    assert!(cel.evaluate("role_check", &admin_user).unwrap());

    let regular_user = serde_json::json!({
        "claims": { "roles": ["user", "viewer"] }
    });
    assert!(!cel.evaluate("role_check", &regular_user).unwrap());
}

#[test]
fn test_cel_rule_evaluator_response_interceptor_leak_prevention() {
    let mut cel = CelRuleEvaluator::new();
    // Flag if customer SSN is unmasked: true means violation!
    cel.register(
        "sensitive_data_check",
        r#"data.customer.ssn != "" && !data.customer.ssn.startsWith("***-**-")"#,
    )
    .expect("CEL compile");

    let eval_arc = Arc::new(cel);
    let interceptor = SensitiveDataResponseInterceptor::new().with_evaluator(eval_arc);

    let ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    // Masked SSN passes
    let masked_body = br#"{"data":{"customer":{"name":"Alice","ssn":"***-**-4321"}}}"#;
    assert_eq!(
        interceptor.intercept_response(&ctx, &mut resp, masked_body),
        InterceptorVerdict::Pass
    );

    // Unmasked SSN is rejected!
    let leaked_body = br#"{"data":{"customer":{"name":"Bob","ssn":"000-12-3456"}}}"#;
    let verdict = interceptor.intercept_response(&ctx, &mut resp, leaked_body);
    match verdict {
        InterceptorVerdict::Reject(rejection) => {
            assert_eq!(rejection.code, "DATA_LEAK_PREVENTED");
            assert_eq!(
                rejection.status_code,
                http::StatusCode::INTERNAL_SERVER_ERROR
            );
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
}
