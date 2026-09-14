use std::sync::Arc;
use std::time::Duration;
use http::{HeaderMap, HeaderValue};
use spectragql::HlcTimestamp;
use spectragql::idempotency::{IdempotencyEngine, IdempotencyOutcome};

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

#[tokio::test]
async fn test_idempotency_edge_rejection_removal_allows_immediate_retry() {
    let engine = IdempotencyEngine::new(Duration::from_secs(60), 100);
    let key = "rejection-retry-key";
    let hlc = HlcTimestamp::new(1000, 0);

    // 1. First request acquires lock
    let first = engine.check_or_insert(key, hlc).await;
    assert_eq!(first, IdempotencyOutcome::New);

    // 2. Immediate second request gets Conflict
    let conflict = engine.check_or_insert(key, hlc).await;
    assert_eq!(conflict, IdempotencyOutcome::Conflict { hlc });

    // 3. Edge interceptor rejects request (e.g. auth failed) -> remove() is invoked
    engine.remove(key).await;

    // 4. Client retries with valid credentials -> immediately gets New without waiting for TTL
    let retry = engine.check_or_insert(key, HlcTimestamp::new(1001, 0)).await;
    assert_eq!(retry, IdempotencyOutcome::New);
}

#[tokio::test]
async fn test_idempotency_concurrent_multi_key_flood() {
    let engine = Arc::new(IdempotencyEngine::new(Duration::from_secs(60), 1000));
    let num_keys = 10;
    let concurrency_per_key = 10; // Total 100 concurrent tasks
    let mut handles = Vec::new();

    for key_idx in 0..num_keys {
        let key = format!("batch-key-{}", key_idx);
        for task_idx in 0..concurrency_per_key {
            let engine_clone = Arc::clone(&engine);
            let k = key.clone();
            let hlc = HlcTimestamp::new(1000 + key_idx, task_idx as u32);
            handles.push(tokio::spawn(async move {
                let outcome = engine_clone.check_or_insert(&k, hlc).await;
                (k, outcome)
            }));
        }
    }

    let mut outcomes_by_key = std::collections::HashMap::new();
    for handle in handles {
        let (k, outcome) = handle.await.unwrap();
        outcomes_by_key.entry(k).or_insert_with(Vec::new).push(outcome);
    }

    // For every distinct key, verify exactly 1 New and exactly 9 Conflicts
    for (k, outcomes) in outcomes_by_key {
        let new_count = outcomes.iter().filter(|o| **o == IdempotencyOutcome::New).count();
        let conflict_count = outcomes.iter().filter(|o| matches!(**o, IdempotencyOutcome::Conflict { .. })).count();
        assert_eq!(new_count, 1, "Key {} should have had exactly 1 New outcome", k);
        assert_eq!(conflict_count, concurrency_per_key - 1, "Key {} should have had {} Conflicts", k, concurrency_per_key - 1);
    }
}
