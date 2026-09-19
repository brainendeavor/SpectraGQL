use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ServerBuilder;
use spectra_flux::http::{handle_request, FluxRouter, RouteDefinition};
use spectra_flux::storage::create_storage;
use spectra_flux::telemetry::TelemetryClient;
use spectra_flux::wasm::WasmHost;
use spectragql::HlcClock;
use spectragql::gateway::generate_command_receipt;
use tokio::net::TcpListener;

async fn start_spectral_flux_server() -> (SocketAddr, Arc<dyn spectra_flux::storage::FluxStorage>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let storage = create_storage("kevy", None).await.unwrap();
    let wasm_host = Arc::new(WasmHost::new(5, Some(storage.clone())).unwrap());

    let mut router = FluxRouter::new();
    let magic_routes = vec![
        RouteDefinition::new("GET", "/verify", "Verify magic link"),
        RouteDefinition::new("POST", "/verify", "Redeem magic link"),
        RouteDefinition::new("GET", "/status", "Status"),
        RouteDefinition::new("GET", "/.well-known/jwks.json", "JWKS"),
        RouteDefinition::new("GET", "/.well-known/openid-configuration", "OIDC"),
    ];
    let webhook_routes = vec![
        RouteDefinition::new("GET", "/health", "Health"),
        RouteDefinition::new("GET", "/dlq", "DLQ"),
        RouteDefinition::new("POST", "/test", "Test"),
    ];

    router.register_fluxcell_routes("magic_link", "/auth", &magic_routes).unwrap();
    router.register_fluxcell_routes("webhook", "/webhooks", &webhook_routes).unwrap();

    let router_arc = Arc::new(std::sync::RwLock::new(router));
    let telemetry_arc = Arc::new(TelemetryClient::new(
        "test-worker-1".to_string(),
        "nats".to_string(),
        Some("spectra".to_string()),
        200,
    ));

    let storage_ret = storage.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(res) => res,
                Err(_) => break,
            };

            let io = TokioIo::new(stream);
            let r_clone = router_arc.clone();
            let t_clone = telemetry_arc.clone();
            let d_clone = wasm_host.clone();

            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    handle_request(req, r_clone.clone(), t_clone.clone(), d_clone.clone(), None, None)
                });

                let _ = ServerBuilder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, service)
                    .await;
            });
        }
    });

    (addr, storage_ret)
}

#[tokio::test]
async fn test_full_cqrs_magic_link_saga() {
    let (flux_addr, storage) = start_spectral_flux_server().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // 1. Edge Gateway Mode B Termination Simulation
    // Client invokes mutation: requestMagicLink(email: "alice@example.com")
    let (cmd_id, hlc) = HlcClock::global().now_uuidv7();
    let receipt = generate_command_receipt("requestMagicLink", &cmd_id, &hlc, "ACCEPTED");

    assert_eq!(receipt["data"]["requestMagicLink"]["status"], "ACCEPTED");
    assert_eq!(receipt["data"]["requestMagicLink"]["commandId"], cmd_id.to_string());
    assert_eq!(receipt["data"]["requestMagicLink"]["hlc"], hlc.to_compact_string());

    // 2. Broker Event Ingestion in Spectral Flux
    // Event payload contains email, fluxcell mints token and persists in Kevy
    let email = "alice@example.com";
    let token = fluxcell_magic_link::mint_magic_token(email);
    assert_eq!(token.len(), 64);

    // Save token in storage with 900s TTL (mimicking broker consumer loop)
    storage.set(&format!("magic_token:{}", token), email, 900).await.unwrap();

    // 3. User verification via Mode A / direct HTTP probe to /auth/verify?token=...
    let verify_url = format!("http://{}/auth/verify?token={}", flux_addr, token);
    let resp1 = client.get(&verify_url).send().await.expect("Failed to call verify endpoint");
    assert_eq!(resp1.status(), 200, "First verification attempt must succeed with HTTP 200");

    let body1: serde_json::Value = resp1.json().await.unwrap();
    assert_eq!(body1["status"], "VERIFIED");
    assert_eq!(body1["email"], email);
    assert!(body1["session_id"].is_string());
    assert!(body1["token"].is_string(), "Token must be minted as a standard JWT");
    assert_eq!(body1["roles"], serde_json::json!(["viewer"]));

    // 4. Verify OIDC Discovery endpoint
    let oidc_url = format!("http://{}/auth/.well-known/openid-configuration", flux_addr);
    let oidc_resp = client.get(&oidc_url).send().await.expect("Failed to call OIDC endpoint");
    assert_eq!(oidc_resp.status(), 200);
    let oidc_body: serde_json::Value = oidc_resp.json().await.unwrap();
    assert_eq!(oidc_body["issuer"], format!("http://{}", flux_addr));

    // 5. SpectraGQL RBAC & ABAC Verification with minted token
    let jwt_token = body1["token"].as_str().unwrap();
    let jwt_secret = std::env::var("SPECTRA_JWT_SECRET")
        .unwrap_or_else(|_| "spectra_secret_key_default".to_string());
    let provider = spectragql::interceptors::AuthProvider::hmac(jwt_secret.as_bytes());

    let rbac_interceptor = spectragql::interceptors::RbacRequestInterceptor::new()
        .with_provider(provider)
        .with_policies(spectragql::interceptors::PolicyMode::DenyUnlisted, spectragql::interceptors::PolicyMode::PassAll)
        .grant_role("editor", ["publishArticle"]);

    let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
    parts.headers.insert("authorization", format!("Bearer {}", jwt_token).parse().unwrap());

    // Attempting publishArticle without 'editor' role -> 403 Forbidden!
    let mut ctx_mutation = spectragql::interceptors::InterceptorContext::new(uuid::Uuid::now_v7(), hlc.clone())
        .with_operation(Some("publishArticle".to_string()), Some(spectragql::protocol::GraphQLOperationType::Mutation));
    let verdict_mut = rbac_interceptor.intercept_request(&mut ctx_mutation, &mut parts, "");
    assert!(matches!(verdict_mut, spectragql::interceptors::InterceptorVerdict::Reject(rej) if rej.status_code == http::StatusCode::FORBIDDEN));

    // ABAC CEL Scope Expansion: Author can update post even with viewer role
    let cel_abac = spectragql::interceptors::CelRequestInterceptor::new(
        "'editor' in claims.roles || variables.author == claims.sub",
        Some(http::StatusCode::FORBIDDEN),
        Some("FORBIDDEN"),
        Some("Access Denied"),
    ).unwrap();

    let mut ctx_abac = spectragql::interceptors::InterceptorContext::new(uuid::Uuid::now_v7(), hlc.clone())
        .with_operation(Some("updatePost".to_string()), Some(spectragql::protocol::GraphQLOperationType::Mutation));
    // Pass through RBAC interceptor on updatePost (which passes in PassAll mode)
    let _ = rbac_interceptor.intercept_request(&mut ctx_abac, &mut parts, "");
    // Now evaluate CEL ABAC on body
    let abac_body = format!(r#"{{"query": "mutation {{ updatePost }}", "variables": {{"author": "{}"}}}}"#, email);
    let verdict_abac = cel_abac.intercept_request(&mut ctx_abac, &mut parts, &abac_body);
    assert!(matches!(verdict_abac, spectragql::interceptors::InterceptorVerdict::Pass), "ABAC scope expansion must pass for resource author");

    // 6. Double-spend / Replay Attack Prevention (Atomic GETDEL)
    // Attempting to reuse the same token immediately returns HTTP 401
    let resp2 = client.get(&verify_url).send().await.expect("Failed to call verify endpoint again");
    assert_eq!(resp2.status(), 401, "Second verification attempt must fail with HTTP 401 Unauthorized");

    let body2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body2["error"], "INVALID_OR_EXPIRED_TOKEN");
}

#[tokio::test]
async fn test_full_cqrs_webhook_signature_and_dlq_saga() {
    let (flux_addr, _) = start_spectral_flux_server().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // 1. Webhook Signature Verification
    let secret = b"super-shared-secret-key-1234";
    let payload = r#"{"event":"order.placed","orderId":"ord-9988","amount":120.50}"#;
    let signature = fluxcell_webhook::compute_hmac_sha256(secret, payload.as_bytes());

    assert!(fluxcell_webhook::verify_signature(secret, payload.as_bytes(), &signature));
    assert!(!fluxcell_webhook::verify_signature(b"wrong-secret", payload.as_bytes(), &signature));

    // 2. Health and DLQ endpoints
    let health_url = format!("http://{}/webhooks/health", flux_addr);
    let health_resp = client.get(&health_url).send().await.expect("Failed to get health");
    assert_eq!(health_resp.status(), 200);

    let dlq_url = format!("http://{}/webhooks/dlq", flux_addr);
    let dlq_resp = client.get(&dlq_url).send().await.expect("Failed to get dlq");
    assert_eq!(dlq_resp.status(), 200);
    let dlq_entries: Vec<serde_json::Value> = dlq_resp.json().await.unwrap();
    assert_eq!(dlq_entries.len(), 0);
}

#[tokio::test]
async fn test_telemetry_worker_registration_and_logs() {
    let (flux_addr, _) = start_spectral_flux_server().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Query /admin/logs on Spectral Flux
    let logs_url = format!("http://{}/admin/logs", flux_addr);
    let logs_resp = client.get(&logs_url).send().await.expect("Failed to get admin logs");
    assert_eq!(logs_resp.status(), 200);

    // Query /healthz probe
    let healthz_url = format!("http://{}/healthz", flux_addr);
    let healthz_resp = client.get(&healthz_url).send().await.expect("Failed to get healthz");
    assert_eq!(healthz_resp.status(), 200);
    let healthz_body: serde_json::Value = healthz_resp.json().await.unwrap();
    assert_eq!(healthz_body["status"], "ok");
}
