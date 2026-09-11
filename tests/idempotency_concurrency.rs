use std::sync::Arc;
use std::time::Duration;
use http::{HeaderMap, HeaderValue};
use spectragql::clock::HlcTimestamp;
use spectragql::ratify::{IdempotencyEngine, IdempotencyOutcome};

#[tokio::test]
async fn test_concurrent_thundering_herd_on_idempotency_key() {
    let engine = Arc::new(IdempotencyEngine::new(Duration::from_secs(60), 1000));
    let key = "concurrent-order-12345";

    let concurrency = 50;
    let mut handles = Vec::new();

    for i in 0..concurrency {
        let engine_clone = Arc::clone(&engine);
        let hlc = HlcTimestamp::new(1700000000000 + i, i as u32);
        handles.push(tokio::spawn(async move {
            engine_clone.check_or_insert(key, hlc).await
        }));
    }

    let mut new_count = 0;
    let mut conflict_count = 0;

    for handle in handles {
        let outcome = handle.await.unwrap();
        match outcome {
            IdempotencyOutcome::New => new_count += 1,
            IdempotencyOutcome::Conflict { .. } => conflict_count += 1,
            IdempotencyOutcome::Replay { .. } => panic!("Unexpected replay before completion"),
        }
    }

    // Exactly one concurrent task must have acquired the lock as New
    assert_eq!(new_count, 1, "Expected exactly 1 New outcome across concurrent tasks");
    // All other 49 tasks must have received Conflict
    assert_eq!(conflict_count, concurrency - 1, "Expected all other tasks to receive Conflict");

    // Complete the transaction
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    engine
        .complete(
            key,
            HlcTimestamp::new(1700000000000, 0),
            200,
            &headers,
            r#"{"data":{"createOrder":{"id":"order-12345","status":"CONFIRMED"}}}"#,
        )
        .await;

    // All subsequent attempts must receive Replay
    let replay_outcome = engine
        .check_or_insert(key, HlcTimestamp::new(1700000001000, 0))
        .await;

    match replay_outcome {
        IdempotencyOutcome::Replay {
            status_code,
            body,
            headers: resp_headers,
            ..
        } => {
            assert_eq!(status_code, 200);
            assert!(body.contains("order-12345"));
            assert!(resp_headers.iter().any(|(k, v)| k == "content-type" && v == "application/json"));
        }
        other => panic!("Expected Replay outcome, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_idempotency_capacity_eviction_or_graceful_handling() {
    let capacity = 5;
    let engine = IdempotencyEngine::new(Duration::from_secs(60), capacity);

    // Insert capacity items
    for i in 0..capacity {
        let key = format!("key-{}", i);
        let outcome = engine
            .check_or_insert(&key, HlcTimestamp::new(1000, i as u32))
            .await;
        assert_eq!(outcome, IdempotencyOutcome::New);
    }

    assert_eq!(engine.active_record_count(), capacity);

    // Inserting beyond capacity should not crash or panic
    let extra_outcome = engine
        .check_or_insert("extra-key", HlcTimestamp::new(2000, 0))
        .await;
    // Engine either evicts or accepts/rejects gracefully
    assert!(
        extra_outcome == IdempotencyOutcome::New || matches!(extra_outcome, IdempotencyOutcome::Conflict { .. })
    );
}

#[tokio::test]
async fn test_idempotency_fingerprint_generation_stability() {
    let fp1 = IdempotencyEngine::compute_fingerprint(
        "192.168.1.1",
        Some("CreateUser"),
        r#"{"name":"Alice","email":"alice@example.com"}"#,
    );
    let fp2 = IdempotencyEngine::compute_fingerprint(
        "192.168.1.1",
        Some("CreateUser"),
        r#"{"name":"Alice","email":"alice@example.com"}"#,
    );
    let fp_different_ip = IdempotencyEngine::compute_fingerprint(
        "192.168.1.2",
        Some("CreateUser"),
        r#"{"name":"Alice","email":"alice@example.com"}"#,
    );

    assert_eq!(fp1, fp2);
    assert_ne!(fp1, fp_different_ip);
    assert!(fp1.starts_with("sha256:"));
}
