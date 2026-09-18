use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ServerBuilder;
use spectra_flux::db::traits::{FluxDb, FluxTx};
use spectra_flux::db::DatabaseRegistry;
use spectra_flux::http::{handle_request, FluxRouter};
use spectra_flux::telemetry::TelemetryClient;
use spectra_flux::wasm::{FluxcellWasmConfig, WasmHost};
use spectragql::gateway::generate_command_receipt;
use spectragql::HlcClock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

struct MockFluxTx {
    votes: Arc<Mutex<HashMap<(String, String), i64>>>,
    executed_queries: Arc<Mutex<Vec<String>>>,
    committed: Arc<AtomicUsize>,
    rolled_back: Arc<AtomicUsize>,
}

#[async_trait]
impl FluxTx for MockFluxTx {
    async fn execute(&mut self, sql: &str, params: &[serde_json::Value]) -> anyhow::Result<u64> {
        self.executed_queries.lock().unwrap().push(sql.to_string());

        if sql.contains("INSERT INTO coeval_votes") {
            let entity_id = params.get(0).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let user_id = params.get(1).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let val = params.get(3).and_then(|v| v.as_i64()).unwrap_or(0);
            self.votes.lock().unwrap().insert((entity_id, user_id), val);
        } else if sql.contains("DELETE FROM coeval_votes") {
            let entity_id = params.get(0).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let user_id = params.get(1).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            self.votes.lock().unwrap().remove(&(entity_id, user_id));
        }

        Ok(1)
    }

    async fn query(&mut self, sql: &str, params: &[serde_json::Value]) -> anyhow::Result<Vec<serde_json::Value>> {
        self.executed_queries.lock().unwrap().push(sql.to_string());

        if sql.contains("COUNT") {
            let entity_id = params.get(0).and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let votes_lock = self.votes.lock().unwrap();
            let mut upvotes: u64 = 0;
            let mut downvotes: u64 = 0;
            for ((eid, _), &val) in votes_lock.iter() {
                if eid == &entity_id {
                    if val > 0 {
                        upvotes += 1;
                    } else if val < 0 {
                        downvotes += 1;
                    }
                }
            }
            return Ok(vec![serde_json::json!({
                "upvotes": upvotes,
                "downvotes": downvotes
            })]);
        }

        Ok(vec![])
    }

    async fn commit(self: Box<Self>) -> anyhow::Result<()> {
        self.committed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> anyhow::Result<()> {
        self.rolled_back.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone)]
struct MockFluxDb {
    votes: Arc<Mutex<HashMap<(String, String), i64>>>,
    executed_queries: Arc<Mutex<Vec<String>>>,
    committed: Arc<AtomicUsize>,
    rolled_back: Arc<AtomicUsize>,
}

impl MockFluxDb {
    fn new() -> Self {
        Self {
            votes: Arc::new(Mutex::new(HashMap::new())),
            executed_queries: Arc::new(Mutex::new(Vec::new())),
            committed: Arc::new(AtomicUsize::new(0)),
            rolled_back: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl FluxDb for MockFluxDb {
    async fn begin_tx(&self) -> anyhow::Result<Box<dyn FluxTx>> {
        Ok(Box::new(MockFluxTx {
            votes: self.votes.clone(),
            executed_queries: self.executed_queries.clone(),
            committed: self.committed.clone(),
            rolled_back: self.rolled_back.clone(),
        }))
    }
}

async fn start_spectral_flux_with_coeval_vote_wasm() -> (SocketAddr, Arc<WasmHost>, MockFluxDb) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_db = MockFluxDb::new();
    let mut db_registry = DatabaseRegistry::new();
    db_registry.register("default", Arc::new(mock_db.clone()), true);

    let wasm_host = Arc::new(WasmHost::with_db(5, None, Some(Arc::new(db_registry))).unwrap());

    // Path to compiled coeval_vote.wasm artifact from OpenCoEval
    let wasm_paths = [
        "/Users/bmo/code/CoEval/OpenCoEval/dist/fluxcells/coeval_vote.wasm",
        "/Volumes/SABRENTEAM/code/cargo-targets/wasm32-wasip1/release/coeval_vote.wasm",
    ];

    let mut wasm_bytes = None;
    for path in &wasm_paths {
        if let Ok(bytes) = std::fs::read(path) {
            wasm_bytes = Some(bytes);
            break;
        }
    }

    let bytes = wasm_bytes.expect("coeval_vote.wasm not found. Ensure ./build.sh has run in OpenCoEval/fluxcells/coeval-vote");

    // 1. Verify default subscriptions exported by WASM module
    wasm_host
        .register_wasm_bytes("coeval_vote", &bytes, FluxcellWasmConfig::default())
        .expect("Failed to register coeval_vote.wasm");

    let routes = wasm_host
        .get_fluxcell_routes("coeval_vote")
        .expect("coeval_vote did not export routes");
    assert!(!routes.is_empty(), "Expected at least 1 route from coeval_vote");

    let default_subs = wasm_host
        .get_fluxcell_subscriptions("coeval_vote")
        .expect("coeval_vote did not export subscriptions");
    assert!(
        default_subs.contains(&"mutation.coeval.recordvote".to_string()),
        "Default subscriptions must include mutation.coeval.recordvote"
    );

    // 2. Verify subscription override capability (e.g. subscriptions = [] in TOML)
    wasm_host
        .register_wasm_bytes_with_subs(
            "coeval_vote_empty_subs",
            &bytes,
            FluxcellWasmConfig::default(),
            Some(vec![]),
        )
        .expect("Failed to register coeval_vote with empty subscriptions override");
    let overridden_subs = wasm_host
        .get_fluxcell_subscriptions("coeval_vote_empty_subs")
        .expect("empty_subs cell failed to return subscriptions");
    assert!(
        overridden_subs.is_empty(),
        "Overriding subscriptions with [] must deactivate queue subscriptions"
    );

    let mut router = FluxRouter::new();
    router
        .register_fluxcell_routes("coeval_vote", "/votes", &routes)
        .expect("Failed to register coeval_vote routes in FluxRouter");

    let router_arc = Arc::new(std::sync::RwLock::new(router));
    let telemetry_arc = Arc::new(TelemetryClient::new(
        "test-coeval-flux".to_string(),
        "nats".to_string(),
        Some("SPECTRA".to_string()),
        200,
    ));

    let host_ret = wasm_host.clone();
    let db_ret = mock_db.clone();

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

    (addr, host_ret, db_ret)
}

#[tokio::test]
async fn test_full_cqrs_coeval_vote_wasm_saga() {
    let (flux_addr, wasm_host, mock_db) = start_spectral_flux_with_coeval_vote_wasm().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let company_id = "88888888-4444-4444-4444-121212121212";

    // 1. Edge Gateway Mode B Receipt Test (receipt contract verification)
    let (cmd_id, hlc) = HlcClock::global().now_uuidv7();
    let receipt = generate_command_receipt("recordVote", &cmd_id, &hlc, "ACCEPTED");

    assert_eq!(receipt["data"]["recordVote"]["status"], "ACCEPTED");
    assert_eq!(receipt["data"]["recordVote"]["commandId"], cmd_id.to_string());
    assert_eq!(receipt["data"]["recordVote"]["hlc"], hlc.to_compact_string());

    // 2. Synchronous Mode A Invocation of coeval_vote.wasm Fluxcell
    // Hono app server issues POST /votes/mutate directly via internal HTTP
    let mutate_url = format!("http://{}/votes/mutate", flux_addr);
    let vote_payload = serde_json::json!({
        "companyId": company_id,
        "value": 1,
        "userId": "alice",
        "hlc": hlc.to_compact_string()
    });

    let resp_mutate = client.post(&mutate_url).json(&vote_payload).send().await.expect("Failed to call /votes/mutate");
    assert_eq!(resp_mutate.status(), 200);

    let mutate_val: serde_json::Value = resp_mutate.json().await.unwrap();
    assert_eq!(mutate_val["status"], "ACCEPTED");
    assert_eq!(mutate_val["companyId"], company_id);
    assert_eq!(mutate_val["upvotes"], 1);
    assert_eq!(mutate_val["downvotes"], 0);
    assert_eq!(mutate_val["score"], 1);

    // 3. Verify Database Transaction Execution:
    // Ensure that the fluxcell performed atomic DB operations via host_db and committed
    assert!(
        mock_db.committed.load(Ordering::Relaxed) >= 1,
        "Expected at least 1 committed database transaction from Mode A execution"
    );
    let queries = mock_db.executed_queries.lock().unwrap().clone();
    assert!(
        queries.iter().any(|q| q.contains("INSERT INTO coeval_votes")),
        "Expected INSERT INTO coeval_votes query in transaction"
    );
    assert!(
        queries.iter().any(|q| q.contains("SELECT COUNT(*)")),
        "Expected SELECT COUNT(*) aggregate query in transaction"
    );
    assert!(
        queries.iter().any(|q| q.contains("UPDATE coeval_entities")),
        "Expected UPDATE coeval_entities query in transaction"
    );

    // 4. Duplicate Queue Event Deduplication (Monotonic HLC):
    // Simulate broker consumer delivering the same event from the queue.
    // coeval-vote should detect that incoming HLC <= existing HLC and return IGNORED_STALE_HLC
    let committed_before_dup = mock_db.committed.load(Ordering::Relaxed);
    let dup_event_res = wasm_host
        .invoke_event("coeval_vote", &vote_payload)
        .expect("Failed to invoke duplicate event on coeval_vote");
    assert_eq!(
        dup_event_res["status"],
        "IGNORED_STALE_HLC",
        "Duplicate queue event must be discarded by monotonic HLC idempotency guard"
    );
    assert_eq!(
        mock_db.committed.load(Ordering::Relaxed),
        committed_before_dup,
        "Duplicate event must NOT execute new database transactions"
    );

    // 5. Direct HTTP Probe to /votes/stats?company_id=...
    let stats_url = format!("http://{}/votes/stats?company_id={}", flux_addr, company_id);
    let resp1 = client.get(&stats_url).send().await.expect("Failed to call /votes/stats");
    assert_eq!(resp1.status(), 200);

    let stats1: serde_json::Value = resp1.json().await.unwrap();
    assert_eq!(stats1["company_id"], company_id);
    assert_eq!(stats1["upvotes"], 1);
    assert_eq!(stats1["downvotes"], 0);
    assert_eq!(stats1["score"], 1);

    // 6. Second User Vote: Bob downvotes (-1) synchronously
    let (_, hlc2) = HlcClock::global().now_uuidv7();
    let bob_payload = serde_json::json!({
        "companyId": company_id,
        "value": -1,
        "userId": "bob",
        "hlc": hlc2.to_compact_string()
    });

    let bob_resp = client.post(&mutate_url).json(&bob_payload).send().await.unwrap();
    assert_eq!(bob_resp.status(), 200);
    let bob_val: serde_json::Value = bob_resp.json().await.unwrap();
    assert_eq!(bob_val["status"], "ACCEPTED");
    assert_eq!(bob_val["upvotes"], 1);
    assert_eq!(bob_val["downvotes"], 1);
    assert_eq!(bob_val["score"], 0); // 1 - 1 = 0

    // HTTP probe verifies updated score
    let resp2 = client.get(&stats_url).send().await.expect("Failed to call /votes/stats again");
    let stats2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(stats2["score"], 0);
    assert_eq!(stats2["total_votes"], 2);

    // 7. Query /votes/user-vote
    let user_vote_url = format!("http://{}/votes/user-vote?company_id={}&user_id=alice", flux_addr, company_id);
    let user_resp = client.get(&user_vote_url).send().await.unwrap();
    assert_eq!(user_resp.status(), 200);
    let user_vote: serde_json::Value = user_resp.json().await.unwrap();
    assert_eq!(user_vote["value"], 1);

    // 8. Poison Pill Rejection: Invalid UUID returns HTTP 400
    let poison_payload = serde_json::json!({
        "companyId": "definitely-not-a-valid-uuid",
        "value": 1,
        "userId": "malicious"
    });
    let poison_resp = client.post(&mutate_url).json(&poison_payload).send().await.unwrap();
    assert_eq!(poison_resp.status(), 400);
    let poison_val: serde_json::Value = poison_resp.json().await.unwrap();
    assert_eq!(poison_val["status"], "POISON_PILL_REJECTED");

    // 9. Stale HLC Rejection: Event arriving with older HLC
    let stale_payload = serde_json::json!({
        "companyId": company_id,
        "value": -1,
        "userId": "alice",
        "hlc": "1.000001" // Very old timestamp
    });
    let stale_resp = client.post(&mutate_url).json(&stale_payload).send().await.unwrap();
    assert_eq!(stale_resp.status(), 200);
    let stale_val: serde_json::Value = stale_resp.json().await.unwrap();
    assert_eq!(stale_val["status"], "IGNORED_STALE_HLC");

    // 10. Service Health Check
    let health_url = format!("http://{}/votes/health", flux_addr);
    let health_resp = client.get(&health_url).send().await.unwrap();
    assert_eq!(health_resp.status(), 200);
    let health_val: serde_json::Value = health_resp.json().await.unwrap();
    assert_eq!(health_val["status"], "healthy");
    assert_eq!(health_val["fluxcell"], "coeval-vote");
    assert_eq!(health_val["poison_pills_rejected"], 1);
    assert!(health_val["stale_hlc_suppressed"].as_u64().unwrap() >= 2);
}
