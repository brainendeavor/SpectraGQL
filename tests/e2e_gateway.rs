use std::net::SocketAddr;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A lightweight in-process HTTP mock upstream server for testing Mode A forwarding and idempotency.
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

                    let (status_line, body) = if req_str.contains("GetViewer") {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"viewer":{"id":"usr-42","name":"Arthur Dent"}}}"#,
                        )
                    } else if req_str.contains("createReview") {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"createReview":{"id":"rev-100","rating":5}}}"#,
                        )
                    } else if req_str.contains("failingMutation") {
                        (
                            "HTTP/1.1 500 Internal Server Error",
                            r#"{"errors":[{"message":"Simulated database failure"}]}"#,
                        )
                    } else {
                        (
                            "HTTP/1.1 200 OK",
                            r#"{"data":{"result":"default_ok"}}"#,
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

#[tokio::test]
async fn test_e2e_gateway_mode_a_and_idempotency_replay() {
    let mock_upstream = MockUpstream::start().await;
    let gateway_port = get_free_port().await;

    let bin_path = env!("CARGO_BIN_EXE_spectragql");

    let unreachable_dispatch_port = get_free_port().await;

    // Spawn the real SpectraGQL gateway binary
    let child = Command::new(bin_path)
        .env("SPECTRA_BIND_ADDR", format!("127.0.0.1:{}", gateway_port))
        .env("SPECTRA_UPSTREAM_ADDR", format!("127.0.0.1:{}", mock_upstream.addr.port()))
        .env("SPECTRA_GQL_PATHS", "/graphql,/gql")
        .env("SPECTRA_DISPATCH_METHOD", "nats")
        .env("SPECTRA_DISPATCH_ADDR", format!("127.0.0.1:{}", unreachable_dispatch_port))
        .env("SPECTRA_ADMIN_ENABLED", "true")
        .env("SPECTRA_ADMIN_ALLOWED_IPS", "127.0.0.1")
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
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    last_err = format!("Status: {} Body: {}", status, text);
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
    assert!(ready, "Gateway failed to become ready within timeout. Last result: {}", last_err);

    let gql_url = format!("http://127.0.0.1:{}/graphql", gateway_port);

    // 1. Mode A Query Proxying: Verify headers and data
    let query_payload = serde_json::json!({
        "query": "query GetViewer { viewer { id name } }"
    });

    let resp = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .body(query_payload.to_string())
        .send()
        .await
        .expect("Failed to send query");

    assert_eq!(resp.status(), 200);
    assert!(resp.headers().contains_key("x-spectra-request-id"));
    assert!(resp.headers().contains_key("x-spectra-hlc"));

    let text1 = resp.text().await.unwrap();
    let body_json: serde_json::Value = serde_json::from_str(&text1).unwrap();
    assert_eq!(body_json["data"]["viewer"]["name"], "Arthur Dent");
    assert_eq!(mock_upstream.request_count.load(Ordering::SeqCst), 1);

    // 2. Mode A Idempotency Replay: Send mutation with Idempotency-Key twice
    let mutation_payload = serde_json::json!({
        "query": "mutation { createReview(rating: 5) { id rating } }"
    });

    let resp1 = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .header("idempotency-key", "e2e-order-key-777")
        .body(mutation_payload.to_string())
        .send()
        .await
        .expect("Failed to send first mutation");

    assert_eq!(resp1.status(), 200);
    let text1_mut = resp1.text().await.unwrap();
    let body1: serde_json::Value = serde_json::from_str(&text1_mut).unwrap();
    assert_eq!(body1["data"]["createReview"]["id"], "rev-100");
    // Upstream was called for the first time
    assert_eq!(mock_upstream.request_count.load(Ordering::SeqCst), 2);

    // Send the exact same mutation with the same idempotency key
    let resp2 = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .header("idempotency-key", "e2e-order-key-777")
        .body(mutation_payload.to_string())
        .send()
        .await
        .expect("Failed to send duplicate mutation");

    assert_eq!(resp2.status(), 200);
    assert_eq!(
        resp2.headers().get("x-spectra-idempotent-replay").and_then(|v| v.to_str().ok()),
        Some("true"),
        "Expected x-spectra-idempotent-replay header on duplicate request"
    );
    let text2_mut = resp2.text().await.unwrap();
    let body2: serde_json::Value = serde_json::from_str(&text2_mut).unwrap();
    assert_eq!(body2["data"]["createReview"]["id"], "rev-100");

    // Critical assertion: Upstream request count MUST STILL BE 2!
    // The gateway served the second response entirely from cache!
    assert_eq!(
        mock_upstream.request_count.load(Ordering::SeqCst),
        2,
        "Upstream was called on replay! Idempotency cache failed to prevent duplicate execution"
    );

    // 3. Mode B Edge Termination with Dispatch Failure
    // Operation "importCatalog" is routed to Mode B.
    // Since NATS is not running in this test, dispatch fails.
    // SpectraGQL MUST return DISPATCH_FAILED receipt and NOT hit upstream.
    let mode_b_payload = serde_json::json!({
        "query": "mutation BulkImport { importCatalog(file: \"catalog.csv\") { id status } }"
    });

    let resp_b = client
        .post(&gql_url)
        .header("content-type", "application/json")
        .body(mode_b_payload.to_string())
        .send()
        .await
        .expect("Failed to send Mode B mutation");

    assert_eq!(resp_b.status(), 200);
    assert_eq!(
        resp_b.headers().get("x-spectra-dispatch").and_then(|v| v.to_str().ok()),
        Some("failed"),
        "Expected x-spectra-dispatch: failed header"
    );
    let text_b = resp_b.text().await.unwrap();
    let body_b: serde_json::Value = serde_json::from_str(&text_b).unwrap();
    assert_eq!(body_b["data"]["importCatalog"]["status"], "DISPATCH_FAILED");
    assert!(body_b["data"]["importCatalog"]["commandId"].is_string());

    // Upstream request count MUST STILL BE 2 (Mode B terminated at edge!)
    assert_eq!(
        mock_upstream.request_count.load(Ordering::SeqCst),
        2,
        "Mode B operation should never forward to upstream"
    );

    // 4. Admin API Endpoints
    let status_url = format!("http://127.0.0.1:{}/admin/api/v1/status", gateway_port);
    let admin_resp = client.get(&status_url).send().await.unwrap();
    assert_eq!(admin_resp.status(), 200);
    let admin_text = admin_resp.text().await.unwrap();
    let admin_json: serde_json::Value = serde_json::from_str(&admin_text).unwrap();
    assert_eq!(admin_json["mode_a_enabled"], true);

    let live_url = format!("http://127.0.0.1:{}/livez", gateway_port);
    let live_resp = client.get(&live_url).send().await.unwrap();
    assert_eq!(live_resp.status(), 200);
}

