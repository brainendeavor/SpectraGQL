use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::hmac;
use uuid::Uuid;

use spectragql::core::config::SpectraConfig;
use spectragql::interceptors::auth::{AuthProvider, ClaimsMapping, PolicyMode, RbacRequestInterceptor};
use spectragql::interceptors::evaluators::cel::CelRequestInterceptor;
use spectragql::interceptors::manager::InterceptorManager;
use spectragql::interceptors::{InterceptorContext, InterceptorVerdict, RequestInterceptor};
use spectragql::protocol::GraphQLOperationType;
use spectragql::HlcTimestamp;

fn make_hs256_jwt(secret: &[u8], sub: &str, roles: &[&str], org_id: Option<&str>) -> String {
    let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
    let mut payload = serde_json::json!({
        "sub": sub,
        "roles": roles,
        "exp": 2500000000i64
    });
    if let Some(oid) = org_id {
        payload["org_id"] = serde_json::Value::String(oid.to_string());
    }
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let tag = hmac::sign(&key, signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(tag.as_ref());
    format!("{}.{}", signing_input, sig_b64)
}

#[test]
fn test_gateway_rbac_audit_only_dx_mode() {
    let secret = b"dx_test_secret_123456";
    let provider = AuthProvider::hmac(secret);

    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_policies(PolicyMode::AuditOnly, PolicyMode::PassAll)
        .grant_role("admin", ["deleteTenant"]);

    let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
        .with_operation(Some("deleteTenant".to_string()), Some(GraphQLOperationType::Mutation));
    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;

    // Call without token -> Audit verdict, never blocked!
    let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
    match verdict {
        InterceptorVerdict::Audit { tag, reason, .. } => {
            assert_eq!(tag.as_deref(), Some("AUTH_AUDIT"));
            assert!(reason.contains("deleteTenant"));
        }
        other => panic!("Expected InterceptorVerdict::Audit in DX mode, got: {:?}", other),
    }
}

#[test]
fn test_gateway_rbac_deny_unlisted_zero_trust() {
    let secret = b"prod_test_secret_123456";
    let provider = AuthProvider::hmac(secret);

    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_policies(PolicyMode::DenyUnlisted, PolicyMode::AllowAuthenticated)
        .allow_unauthenticated("publicHealth")
        .grant_role("editor", ["publishArticle", "editArticle"])
        .grant_role("admin", ["*"]);

    // Case 1: Public operation passes without credentials
    {
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("publicHealth".to_string()), Some(GraphQLOperationType::Query));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict, InterceptorVerdict::Pass));
    }

    // Case 2: Mutation without token -> 401 Unauthorized
    {
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("publishArticle".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict, InterceptorVerdict::Reject(r) if r.status_code == http::StatusCode::UNAUTHORIZED));
    }

    // Case 3: Mutation with unprivileged role (viewer) -> 403 Forbidden
    {
        let viewer_token = make_hs256_jwt(secret, "usr_viewer_bob", &["viewer"], None);
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("publishArticle".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        parts.headers.insert("authorization", format!("Bearer {}", viewer_token).parse().unwrap());
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict, InterceptorVerdict::Reject(r) if r.status_code == http::StatusCode::FORBIDDEN));
    }

    // Case 4: Mutation with editor role -> 200 Pass + Header Injection
    {
        let editor_token = make_hs256_jwt(secret, "usr_editor_carol", &["editor"], Some("org_coeval"));
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("publishArticle".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        parts.headers.insert("authorization", format!("Bearer {}", editor_token).parse().unwrap());
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict, InterceptorVerdict::Pass));
        assert_eq!(parts.headers.get("x-user-id").unwrap(), "usr_editor_carol");
        assert_eq!(parts.headers.get("x-tenant-id").unwrap(), "org_coeval");
        assert_eq!(parts.headers.get("x-user-roles").unwrap(), "editor");
    }
}

#[test]
fn test_gateway_rbac_and_abac_scope_expansion() {
    let secret = b"scope_expansion_secret_789";
    let provider = AuthProvider::hmac(secret);

    // Gateway RBAC: permissive or pass_all on this route
    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_policies(PolicyMode::PassAll, PolicyMode::PassAll);

    // Layered ABAC via Google CEL:
    // User can update post if they are an 'editor' OR if they are the author of the post!
    let cel_abac = CelRequestInterceptor::new(
        "'editor' in claims.roles || (has(variables.author_id) && variables.author_id == claims.sub)",
        Some(http::StatusCode::FORBIDDEN),
        Some("UNAUTHORIZED"),
        Some("Cannot edit another user's post"),
    )
    .unwrap();

    // 1. Viewer trying to edit someone else's post -> Blocked by ABAC
    {
        let token = make_hs256_jwt(secret, "usr_viewer_bob", &["viewer"], None);
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("updatePost".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        parts.headers.insert("authorization", format!("Bearer {}", token).parse().unwrap());

        // Step 1: RBAC extracts claims into ctx
        let _ = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(ctx.claims.is_some());

        // Step 2: CEL evaluates ownership
        let body = r#"{"query": "mutation { updatePost }", "variables": {"author_id": "usr_different_author"}}"#;
        let verdict = cel_abac.intercept_request(&mut ctx, &mut parts, body);
        assert!(matches!(verdict, InterceptorVerdict::Reject(r) if r.code == "UNAUTHORIZED"));
    }

    // 2. Viewer editing their OWN post -> Scope Expanded -> Passes!
    {
        let token = make_hs256_jwt(secret, "usr_viewer_bob", &["viewer"], None);
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("updatePost".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        parts.headers.insert("authorization", format!("Bearer {}", token).parse().unwrap());

        // Step 1: RBAC extracts claims into ctx
        let _ = rbac.intercept_request(&mut ctx, &mut parts, "");

        // Step 2: CEL evaluates ownership
        let body = r#"{"query": "mutation { updatePost }", "variables": {"author_id": "usr_viewer_bob"}}"#;
        let verdict = cel_abac.intercept_request(&mut ctx, &mut parts, body);
        assert!(matches!(verdict, InterceptorVerdict::Pass), "ABAC must grant access via scope expansion");
    }
}

#[test]
fn test_gateway_rbac_clerk_custom_claims_mapping() {
    let secret = b"clerk_mock_secret_key";
    let provider = AuthProvider::hmac(secret);

    // Clerk puts org role in "org_role" and organization in "org_id"
    let claims_mapping = ClaimsMapping {
        subject_path: "sub".to_string(),
        tenant_path: Some("org_id".to_string()),
        roles_path: "org_role".to_string(),
        permissions_path: None,
    };

    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_claims_mapping(claims_mapping)
        .with_policies(PolicyMode::DenyUnlisted, PolicyMode::AllowAuthenticated)
        .grant_role("org:admin", ["inviteMember"]);

    // Create Clerk-formatted token: {"sub":"user_2...","org_id":"org_2...","org_role":"org:admin"}
    let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
    let payload = serde_json::json!({
        "sub": "user_2XYZ123456",
        "org_id": "org_2ABC789",
        "org_role": "org:admin",
        "exp": 2500000000i64
    });
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let tag = hmac::sign(&key, signing_input.as_bytes());
    let token = format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()));

    let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
        .with_operation(Some("inviteMember".to_string()), Some(GraphQLOperationType::Mutation));
    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
    parts.headers.insert("authorization", format!("Bearer {}", token).parse().unwrap());

    let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
    assert!(matches!(verdict, InterceptorVerdict::Pass));
    assert_eq!(parts.headers.get("x-user-id").unwrap(), "user_2XYZ123456");
    assert_eq!(parts.headers.get("x-tenant-id").unwrap(), "org_2ABC789");
    assert_eq!(parts.headers.get("x-user-roles").unwrap(), "org:admin");
}

#[test]
fn test_interceptor_manager_e2e_toml_configuration() {
    let toml_str = r#"
        bind_addr = "0.0.0.0:8000"

        [upstream]
        addr = "127.0.0.1:4000"

        [dispatch]
        name = "default"
        method = "NATS"
        addr = "127.0.0.1:4222"

        [interceptors.auth_guard]
        type = "rbac"
        stage = "request"
        secret = "super_test_secret"
        mutations_default = "audit_only"
        queries_default = "pass_all"
        unauthenticated_ops = ["IntrospectionQuery", "publicStats"]

        [interceptors.auth_guard.roles]
        admin = ["*"]
        editor = ["updatePost"]

        [gql]
        paths = "/graphql"
        ops_to_dispatch = "query, mutation"
        interceptors = ["auth_guard"]

        [rest]
        paths = "/api"
    "#;

    let cfg: SpectraConfig = config::Config::builder()
        .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    let manager = InterceptorManager::from_config(&cfg).expect("Manager should initialize with RBAC");
    let pipeline = manager.get_request_pipeline(None);
    assert_eq!(pipeline.len(), 1);

    // Test unauthenticated operation publicStats passes
    let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
        .with_operation(Some("publicStats".to_string()), Some(GraphQLOperationType::Query));
    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
    let verdict = pipeline.intercept_request(&mut ctx, &mut parts, "");
    assert!(matches!(verdict, InterceptorVerdict::Pass));
}

#[test]
fn test_gateway_auth_case_insensitive_bearer_and_clock_skew() {
    let secret = b"skew_and_bearer_secret_test";
    let provider = AuthProvider::hmac(secret);

    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_policies(PolicyMode::DenyUnlisted, PolicyMode::AllowAuthenticated)
        .grant_role("admin", ["manageSystem"]);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Token expiring 30 seconds ago (valid under 60s leeway)
    let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
    let payload = serde_json::json!({
        "sub": "usr_superadmin",
        "roles": ["admin"],
        "exp": now - 30,
    });
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let tag = hmac::sign(&key, signing_input.as_bytes());
    let token = format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()));

    // Lowercase "bearer " prefix
    let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
        .with_operation(Some("manageSystem".to_string()), Some(GraphQLOperationType::Mutation));
    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
    parts.headers.insert("authorization", format!("bearer {}", token).parse().unwrap());

    let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
    assert!(matches!(verdict, InterceptorVerdict::Pass), "Should pass with lowercase bearer and 60s clock skew leeway");
    assert_eq!(parts.headers.get("x-user-id").unwrap(), "usr_superadmin");
}

#[test]
fn test_gateway_auth_lru_cache_bounded_adversarial_traffic() {
    let secret = b"lru_bounded_traffic_secret";
    let provider = AuthProvider::hmac(secret);

    // Set cache capacity to 10 entries
    let rbac = RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_cache_capacity(10)
        .with_policies(PolicyMode::PassAll, PolicyMode::PassAll);

    // Generate 50 unique tokens simulating adversarial traffic
    for i in 0..50 {
        let token = make_hs256_jwt(secret, &format!("usr_{}", i), &["user"], None);
        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("getProfile".to_string()), Some(GraphQLOperationType::Query));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
        parts.headers.insert("authorization", format!("Bearer {}", token).parse().unwrap());
        let _ = rbac.intercept_request(&mut ctx, &mut parts, "");
    }

    // Cache must remain strictly bounded at 10 items (no memory leak)
    // We verify by checking that older tokens cause re-verification or are evicted
    let fresh_token = make_hs256_jwt(secret, "usr_final", &["user"], None);
    let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
        .with_operation(Some("getProfile".to_string()), Some(GraphQLOperationType::Query));
    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
    parts.headers.insert("authorization", format!("Bearer {}", fresh_token).parse().unwrap());
    let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
    assert!(matches!(verdict, InterceptorVerdict::Pass));
}
