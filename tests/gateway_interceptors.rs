use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use spectragql::core::config::SpectraConfig;
use spectragql::interceptors::evaluators::wasm::{
    CircuitBreakerConfig, FailMode, WasmEngineConfig, WasmInterceptorEvaluator, WasmPluginConfig,
};
use spectragql::interceptors::manager::InterceptorManager;
use spectragql::interceptors::{InterceptorContext, InterceptorVerdict};
use spectragql::HlcTimestamp;
use uuid::Uuid;

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

/// A lightweight in-process HTTP mock upstream server for testing interceptor proxying.
struct MockUpstream {
    addr: SocketAddr,
    request_count: Arc<AtomicUsize>,
}

impl MockUpstream {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&request_count);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let count = Arc::clone(&count_clone);

                tokio::spawn(async move {
                    let mut full_bytes = Vec::new();
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        full_bytes.extend_from_slice(&buf[..n]);
                        let s = String::from_utf8_lossy(&full_bytes);
                        if let Some(header_end) = s.find("\r\n\r\n") {
                            let mut cl = 0;
                            for line in s[..header_end].lines() {
                                if line.to_ascii_lowercase().starts_with("content-length:") {
                                    if let Some(val) = line.split(':').nth(1) {
                                        cl = val.trim().parse::<usize>().unwrap_or(0);
                                    }
                                }
                            }
                            if full_bytes.len() >= header_end + 4 + cl {
                                break;
                            }
                        }
                    }

                    let req_str = String::from_utf8_lossy(&full_bytes);
                    count.fetch_add(1, Ordering::SeqCst);

                    let (status_line, body) = if req_str.contains("sensitiveOp") {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"sensitiveOp":{"secret":"classified_secret_xyz","public_info":"hello"}}}"#,
                        )
                    } else if req_str.contains("streamOp") {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"streamOp":{"id":"stream-001","status":"STREAMING"}}}"#,
                        )
                    } else {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"viewer":{"id":"usr-42","name":"Arthur Dent"}}}"#,
                        )
                    };

                    let response = format!(
                        "{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status_line,
                        body.len(),
                        body
                    );

                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                });
            }
        });

        MockUpstream { addr, request_count }
    }
}

async fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

const TRANSFORM_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 4096) "{\"verdict\":\"transform\",\"body\":\"{\\\"data\\\":{\\\"viewer\\\":{\\\"name\\\":\\\"ANONYMIZED_BY_WASM\\\"}}}\"}")
  (func (export "spectragql_allocate") (param i32) (result i32)
    i32.const 1024
  )
  (func (export "spectragql_deallocate") (param i32 i32))
  (func (export "spectragql_intercept_response") (param i32 i32) (result i64)
    i64.const 0x00001000_0000005A
  )
)
"#;

#[tokio::test]
async fn test_e2e_gateway_cel_request_rejection_and_response_leak_prevention() {
    let mock_upstream = MockUpstream::start().await;
    let gateway_port = get_free_port().await;

    // Create a temporary environment-specific toml file: `e2e_interceptors-spectra.toml`
    let env_name = format!("e2e_interceptors_{}", gateway_port);
    let toml_file_path = PathBuf::from(format!("{}-spectra.toml", env_name));
    let toml_content = format!(
        r#"
bind_addr = "127.0.0.1:{gateway_port}"

[upstream]
addr = "127.0.0.1:{upstream_port}"
name = "default"

[gql]
paths = "/graphql,/gql"
interceptors = ["auth_guard"]

[gql.routes.sensitive_route]
operation = "sensitiveOp"
mode = "A"
interceptors = ["leak_guard"]

[gql.routes.stream_route]
operation = "streamOp"
mode = "A"
interceptors = []

[interceptors.auth_guard]
stage = "request"
type = "cel"
expr = "'authorization' in request.headers && request.headers['authorization'].startsWith('Bearer ')"

[interceptors.leak_guard]
stage = "response"
type = "cel"
expr = "!body.contains('classified_secret')"
"#,
        gateway_port = gateway_port,
        upstream_port = mock_upstream.addr.port(),
    );

    std::fs::write(&toml_file_path, toml_content).expect("Failed to write test toml config");
    let _toml_guard = TempFileGuard(toml_file_path);

    let bin_path = env!("CARGO_BIN_EXE_spectragql");

    let child = Command::new(bin_path)
        .env("SPECTRA_ENV", &env_name)
        .env("SPECTRA_BIND_ADDR", format!("127.0.0.1:{}", gateway_port))
        .env("SPECTRA_UPSTREAM_ADDR", format!("127.0.0.1:{}", mock_upstream.addr.port()))
        .spawn()
        .expect("Failed to spawn spectragql binary");

    let mut _guard = ProcessGuard(child);

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let health_url = format!("http://127.0.0.1:{}/healthz", gateway_port);
    let mut ready = false;
    let mut last_err = String::new();
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        match client.get(&health_url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    ready = true;
                    break;
                } else {
                    last_err = format!("Status: {}", resp.status());
                }
            }
            Err(e) => {
                last_err = format!("Error: {:#?}", e);
                if let Ok(Some(status)) = _guard.0.try_wait() {
                    last_err = format!("Child exited prematurely with status: {:?}", status);
                    break;
                }
            }
        }
    }
    assert!(ready, "Gateway failed to become ready: {}", last_err);

    let gql_url = format!("http://127.0.0.1:{}/graphql", gateway_port);

    // -------------------------------------------------------------
    // Test 1: CEL Request Edge Rejection (Missing Authorization Header)
    // -------------------------------------------------------------
    let unauthorized_query = serde_json::json!({
        "query": "query GetViewer { viewer { id name } }"
    });

    let resp_unauth = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .body(unauthorized_query.to_string())
        .send()
        .await
        .expect("Failed to send unauthorized query");

    assert_eq!(resp_unauth.status(), 403, "Expected 403 Forbidden for missing auth header");
    let unauth_text = resp_unauth.text().await.unwrap();
    let unauth_body: serde_json::Value = serde_json::from_str(&unauth_text).expect("Valid GraphQL error JSON");
    assert!(unauth_body.get("errors").is_some());
    let err = &unauth_body["errors"][0];
    assert_eq!(err["extensions"]["code"], "POLICY_VIOLATION");
    assert_eq!(
        mock_upstream.request_count.load(Ordering::SeqCst),
        0,
        "Upstream must NOT be called when edge rejection occurs!"
    );

    // -------------------------------------------------------------
    // Test 2: CEL Request Pass (Valid Bearer Token)
    // -------------------------------------------------------------
    let authorized_query = serde_json::json!({
        "query": "query GetViewer { viewer { id name } }"
    });

    let resp_auth = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-12345")
        .body(authorized_query.to_string())
        .send()
        .await
        .expect("Failed to send authorized query");

    assert_eq!(resp_auth.status(), 200);
    let auth_text = resp_auth.text().await.unwrap();
    let auth_body: serde_json::Value = serde_json::from_str(&auth_text).expect("Valid JSON response");
    assert_eq!(auth_body["data"]["viewer"]["name"], "Arthur Dent");
    assert_eq!(
        mock_upstream.request_count.load(Ordering::SeqCst),
        1,
        "Upstream should be called exactly once for passed request"
    );

    // -------------------------------------------------------------
    // Test 3: CEL Response Leak Prevention (Scrubbing Sensitive Data)
    // -------------------------------------------------------------
    let sensitive_query = serde_json::json!({
        "query": "query sensitiveOp { sensitiveOp { secret public_info } }"
    });

    let resp_sensitive = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-12345")
        .body(sensitive_query.to_string())
        .send()
        .await
        .expect("Failed to send sensitive query");

    let sensitive_text = resp_sensitive.text().await.unwrap();
    assert!(
        !sensitive_text.contains("classified_secret_xyz"),
        "CRITICAL LEAK DETECTED: Upstream secret was not scrubbed by response interceptor!"
    );
    let sensitive_json: serde_json::Value = serde_json::from_str(&sensitive_text).unwrap();
    assert!(sensitive_json.get("errors").is_some());
    let leak_err = &sensitive_json["errors"][0];
    assert_eq!(leak_err["extensions"]["code"], "DATA_LEAK_PREVENTED");
    assert_eq!(
        mock_upstream.request_count.load(Ordering::SeqCst),
        2,
        "Upstream was called, but response was scrubbed at edge before downstream emission"
    );

    // -------------------------------------------------------------
    // Test 4: Zero-Cost Streaming Pass-Through for Unintercepted Operations
    // -------------------------------------------------------------
    let stream_query = serde_json::json!({
        "query": "query streamOp { streamOp { id status } }"
    });

    let resp_stream = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-12345")
        .body(stream_query.to_string())
        .send()
        .await
        .expect("Failed to send stream query");

    assert_eq!(resp_stream.status(), 200);
    let stream_text = resp_stream.text().await.unwrap();
    let stream_body: serde_json::Value = serde_json::from_str(&stream_text).expect("Valid JSON response");
    assert_eq!(stream_body["data"]["streamOp"]["status"], "STREAMING");
    assert_eq!(mock_upstream.request_count.load(Ordering::SeqCst), 3);
}

#[test]
fn test_interceptor_manager_wasm_response_transformation_in_memory() {
    let wasm_bytes = wat::parse_str(TRANSFORM_WAT).expect("Valid WAT");

    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let eval = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());

    let plugin_cfg = WasmPluginConfig {
        timeout_ms: 25,
        max_memory_bytes: 10 * 1024 * 1024,
        fail_mode: FailMode::FailClosed,
        circuit_breaker: CircuitBreakerConfig {
            consecutive_failure_threshold: 5,
            cooloff_duration: Duration::from_secs(30),
        },
    };

    eval.load_wasm_bytes("anonymizer", &wasm_bytes, plugin_cfg)
        .expect("Failed to load WASM module");

    // Build manager directly with evaluator
    let mut manager = InterceptorManager::empty();
    let resp_interceptor = spectragql::interceptors::evaluators::wasm::WasmResponseInterceptor::new(
        Arc::clone(&eval),
        "anonymizer",
    );
    let mut pipeline = spectragql::interceptors::response::ResponseInterceptorPipeline::default();
    pipeline = pipeline.with_interceptor(resp_interceptor);
    manager.set_route_response_pipeline("getViewer", pipeline);

    assert!(manager.has_response_interceptors(Some("getViewer")));
    assert!(!manager.has_response_interceptors(Some("otherOp")));

    let resp_pipeline = manager.get_response_pipeline(Some("getViewer"));
    let ctx = InterceptorContext::new(Uuid::new_v4(), HlcTimestamp::new(1700000000000, 0));
    let mut parts = http::Response::builder().body(()).unwrap().into_parts().0;
    let initial_body = br#"{"data":{"viewer":{"name":"Real User Name"}}}"#;

    let verdict = resp_pipeline.intercept_response(&ctx, &mut parts, initial_body);
    match verdict {
        InterceptorVerdict::Transform { headers: _, body } => {
            let transformed_str = String::from_utf8(body.unwrap()).unwrap();
            assert!(transformed_str.contains("ANONYMIZED_BY_WASM"));
        }
        other => panic!("Expected Transform verdict from WASM plugin, got: {:?}", other),
    }
}

#[test]
fn test_route_scoping_and_execution_order_in_manager() {
    let toml_str = r#"
        bind_addr = "0.0.0.0:8000"

        [upstream]
        addr = "127.0.0.1:4000"

        [dispatch]
        name = "default"
        method = "NATS"
        addr = "127.0.0.1:4222"

        [gql]
        paths = "/graphql"
        ops_to_dispatch = "query, mutation"
        interceptors = ["global_auth"]

        [gql.routes.admin_route]
        operation = "deleteAccount"
        mode = "A"
        interceptors = ["route_admin_only"]

        [interceptors.global_auth]
        type = "cel"
        stage = "request"
        expression = "'authorization' in request.headers"
        status_code = 401
        code = "UNAUTHORIZED"
        message = "Authorization header required"

        [interceptors.route_admin_only]
        type = "cel"
        stage = "request"
        expression = "request.headers['authorization'].contains('admin')"
        status_code = 403
        code = "FORBIDDEN"
        message = "Admin access required"

        [rest]
        paths = "/api"
    "#;

    let config: SpectraConfig = config::Config::builder()
        .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    let manager = InterceptorManager::from_config(&config).expect("Manager should initialize cleanly");

    // Route: "publicOp" (not configured with route interceptors)
    // Pipeline should have length 1 (only global_auth)
    let public_pipeline = manager.get_request_pipeline(Some("publicOp"));
    assert_eq!(public_pipeline.len(), 1);

    // Route: "deleteAccount" (has route_admin_only)
    // Pipeline should have length 2 (global_auth + route_admin_only)
    let admin_pipeline = manager.get_request_pipeline(Some("deleteAccount"));
    assert_eq!(admin_pipeline.len(), 2);

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), HlcTimestamp::new(1700000000000, 0));
    let mut req_no_auth = http::Request::builder().body(()).unwrap().into_parts().0;

    // Case A: Missing authorization -> global_auth fails first with 401 UNAUTHORIZED
    let verdict_a = admin_pipeline.intercept_request(&mut ctx, &mut req_no_auth, "{}");
    match verdict_a {
        InterceptorVerdict::Reject(rejection) => {
            assert_eq!(rejection.status_code, http::StatusCode::UNAUTHORIZED);
            assert_eq!(rejection.code, "UNAUTHORIZED");
        }
        _ => panic!("Expected rejection from global interceptor"),
    }

    // Case B: Authorization present, but not admin -> global_auth passes, route_admin_only rejects with 403 FORBIDDEN
    let mut req_user_auth = http::Request::builder()
        .header("authorization", "Bearer user_token")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let verdict_b = admin_pipeline.intercept_request(&mut ctx, &mut req_user_auth, "{}");
    match verdict_b {
        InterceptorVerdict::Reject(rejection) => {
            assert_eq!(rejection.status_code, http::StatusCode::FORBIDDEN);
            assert_eq!(rejection.code, "FORBIDDEN");
        }
        _ => panic!("Expected rejection from route interceptor"),
    }

    // Case C: Authorization is admin -> both global_auth and route_admin_only pass!
    let mut req_admin_auth = http::Request::builder()
        .header("authorization", "Bearer admin_super_secret")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let verdict_c = admin_pipeline.intercept_request(&mut ctx, &mut req_admin_auth, "{}");
    assert_eq!(verdict_c, InterceptorVerdict::Pass);
}
