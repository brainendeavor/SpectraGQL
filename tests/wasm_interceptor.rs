use spectragql::HlcTimestamp;
use spectragql::interceptors::evaluators::wasm::{
    CircuitBreakerConfig, FailMode, WasmEngineConfig, WasmInterceptorEvaluator, WasmPluginConfig,
    WasmRequestInterceptor, WasmResponseInterceptor,
};
use spectragql::interceptors::{
    InterceptorContext, InterceptorVerdict, RequestInterceptor, ResponseInterceptor,
};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn test_hlc() -> HlcTimestamp {
    HlcTimestamp::new(1700000000000, 0)
}

const PASSING_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 2048) "{\"verdict\":\"pass\"}")
  (func (export "spectragql_allocate") (param i32) (result i32)
    i32.const 1024
  )
  (func (export "spectragql_deallocate") (param i32 i32))
  (func (export "spectragql_intercept_request") (param i32 i32) (result i64)
    i64.const 0x00000800_00000012
  )
  (func (export "spectragql_intercept_response") (param i32 i32) (result i64)
    i64.const 0x00000800_00000012
  )
)
"#;

const REJECTING_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (data (i32.const 3072) "{\"verdict\":\"reject\",\"status_code\":403,\"code\":\"FORBIDDEN\",\"message\":\"Access denied by WASM\"}")
  (func (export "spectragql_allocate") (param i32) (result i32)
    i32.const 1024
  )
  (func (export "spectragql_deallocate") (param i32 i32))
  (func (export "spectragql_intercept_request") (param i32 i32) (result i64)
    i64.const 0x00000C00_0000005B
  )
)
"#;

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

const INFINITE_LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "spectragql_allocate") (param i32) (result i32)
    i32.const 1024
  )
  (func (export "spectragql_deallocate") (param i32 i32))
  (func (export "spectragql_intercept_request") (param i32 i32) (result i64)
    (loop (br 0))
    i64.const 0
  )
  (func (export "spectragql_intercept_response") (param i32 i32) (result i64)
    (loop (br 0))
    i64.const 0
  )
)
"#;

#[test]
fn test_strict_aot_guardrail_prevents_uncompiled_wasm() {
    // Default config has strict_aot = true, allow_jit = false
    let engine_cfg = WasmEngineConfig::default();
    assert!(engine_cfg.strict_aot);
    assert!(!engine_cfg.allow_jit);

    let evaluator = WasmInterceptorEvaluator::new(engine_cfg).expect("WasmEngine init");
    let plugin_cfg = WasmPluginConfig::default();

    // Loading raw uncompiled WASM in strict mode must fail with an informative error!
    let res = evaluator.load_wat("uncompiled_plugin", PASSING_WAT, plugin_cfg);
    assert!(res.is_err());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("In production mode (strict_aot = true)"));
    assert!(err_msg.contains("precompiled AOT artifacts (.cwasm) are required"));
}

#[test]
fn test_allow_jit_permits_runtime_compilation_for_development() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: true,
        allow_jit: true, // explicitly allowed for dev/tests
        epoch_tick_interval_ms: 1,
    };

    let evaluator = WasmInterceptorEvaluator::new(engine_cfg).expect("WasmEngine init");
    let plugin_cfg = WasmPluginConfig::default();

    // Loading raw WAT succeeds when allow_jit is enabled
    evaluator
        .load_wat("dev_plugin", PASSING_WAT, plugin_cfg)
        .expect("Load WAT in dev mode");

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let verdict = evaluator.intercept_request("dev_plugin", &mut ctx, &mut req, "{}");
    assert_eq!(verdict, InterceptorVerdict::Pass);
}

#[test]
fn test_aot_precompilation_and_cwasm_file_loading() {
    let engine_cfg = WasmEngineConfig::default();
    let evaluator = WasmInterceptorEvaluator::new(engine_cfg).expect("WasmEngine init");

    // 1. Parse WAT and precompile to target-specific .cwasm binary
    let wasm_bytes = wat::parse_str(PASSING_WAT).expect("parse WAT");
    let cwasm_bytes = evaluator.precompile(&wasm_bytes).expect("precompile module");
    assert!(!cwasm_bytes.is_empty());

    // 2. Save .cwasm to disk
    let temp_dir = std::env::temp_dir();
    let cwasm_path = temp_dir.join(format!("test_plugin_{}.cwasm", Uuid::new_v4()));
    std::fs::write(&cwasm_path, &cwasm_bytes).expect("write cwasm");

    // 3. Load precompiled .cwasm file (zero Cranelift JIT overhead)
    evaluator
        .load_cwasm_file("aot_plugin", &cwasm_path, WasmPluginConfig::default())
        .expect("load cwasm file");

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let verdict = evaluator.intercept_request("aot_plugin", &mut ctx, &mut req, "{}");
    assert_eq!(verdict, InterceptorVerdict::Pass);

    let _ = std::fs::remove_file(cwasm_path);
}

#[test]
fn test_wasm_request_interceptor_rejection() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());
    evaluator
        .load_wat("reject_plugin", REJECTING_WAT, WasmPluginConfig::default())
        .unwrap();

    let interceptor = WasmRequestInterceptor::new(evaluator, "reject_plugin");
    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let verdict = interceptor.intercept_request(&mut ctx, &mut req, "query { secret }");
    match verdict {
        InterceptorVerdict::Reject(rejection) => {
            assert_eq!(rejection.status_code, http::StatusCode::FORBIDDEN);
            assert_eq!(rejection.code, "FORBIDDEN");
            assert_eq!(rejection.message, "Access denied by WASM");
        }
        _ => panic!("Expected InterceptorVerdict::Reject"),
    }
}

#[test]
fn test_wasm_response_interceptor_payload_transformation() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());
    evaluator
        .load_wat("transform_plugin", TRANSFORM_WAT, WasmPluginConfig::default())
        .unwrap();

    let interceptor = WasmResponseInterceptor::new(evaluator, "transform_plugin");
    let ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut resp = http::Response::builder().body(()).unwrap().into_parts().0;

    let original_body = br#"{"data":{"viewer":{"name":"Original Name"}}}"#;
    let verdict = interceptor.intercept_response(&ctx, &mut resp, original_body);

    match verdict {
        InterceptorVerdict::Transform { body: Some(new_body), .. } => {
            let json_str = String::from_utf8(new_body).expect("Valid UTF-8");
            assert!(json_str.contains("ANONYMIZED_BY_WASM"));
        }
        other => panic!("Expected InterceptorVerdict::Transform, got: {:?}", other),
    }
}

#[test]
fn test_wasm_epoch_timeout_interruption() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1, // 1ms ticker
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());

    // Configure a strict 5ms execution deadline
    let plugin_cfg = WasmPluginConfig {
        timeout_ms: 5,
        fail_mode: FailMode::FailClosed,
        ..Default::default()
    };

    evaluator
        .load_wat("infinite_loop", INFINITE_LOOP_WAT, plugin_cfg)
        .unwrap();

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    let start = std::time::Instant::now();
    let verdict = evaluator.intercept_request("infinite_loop", &mut ctx, &mut req, "{}");
    let elapsed = start.elapsed();

    // Verify it aborted within a small window
    assert!(elapsed < Duration::from_millis(500), "Execution took too long: {:?}", elapsed);

    match verdict {
        InterceptorVerdict::Reject(rejection) => {
            assert_eq!(rejection.status_code, http::StatusCode::GATEWAY_TIMEOUT);
            assert_eq!(rejection.code, "WASM_EXECUTION_TIMEOUT");
            assert!(rejection.message.contains("exceeded deadline of 5ms"));
        }
        _ => panic!("Expected InterceptorVerdict::Reject with timeout"),
    }
}

#[test]
fn test_wasm_circuit_breaker_fail_closed() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());

    // Circuit breaker configured to trip after 3 consecutive failures
    let plugin_cfg = WasmPluginConfig {
        timeout_ms: 3,
        fail_mode: FailMode::FailClosed,
        circuit_breaker: CircuitBreakerConfig {
            consecutive_failure_threshold: 3,
            cooloff_duration: Duration::from_secs(60),
        },
        ..Default::default()
    };

    evaluator
        .load_wat("circuit_fail_closed", INFINITE_LOOP_WAT, plugin_cfg)
        .unwrap();

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    // Failures 1, 2, 3: fail due to timeout
    for _ in 0..3 {
        let v = evaluator.intercept_request("circuit_fail_closed", &mut ctx, &mut req, "{}");
        match v {
            InterceptorVerdict::Reject(r) => assert_eq!(r.code, "WASM_EXECUTION_TIMEOUT"),
            _ => panic!("Expected timeout"),
        }
    }

    // Circuit is now OPEN!
    assert!(evaluator.is_circuit_open("circuit_fail_closed"));

    // 4th call: fast-fails immediately via open circuit breaker without running WASM!
    let fast_fail_start = std::time::Instant::now();
    let v_open = evaluator.intercept_request("circuit_fail_closed", &mut ctx, &mut req, "{}");
    let fast_fail_elapsed = fast_fail_start.elapsed();

    assert!(fast_fail_elapsed < Duration::from_millis(2));
    match v_open {
        InterceptorVerdict::Reject(r) => {
            assert_eq!(r.status_code, http::StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(r.code, "WASM_CIRCUIT_OPEN");
            assert!(r.message.contains("circuit is open"));
        }
        _ => panic!("Expected WASM_CIRCUIT_OPEN"),
    }
}

#[test]
fn test_wasm_circuit_breaker_fail_open() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());

    // Circuit breaker configured with FailOpen policy
    let plugin_cfg = WasmPluginConfig {
        timeout_ms: 3,
        fail_mode: FailMode::FailOpen,
        circuit_breaker: CircuitBreakerConfig {
            consecutive_failure_threshold: 2,
            cooloff_duration: Duration::from_secs(60),
        },
        ..Default::default()
    };

    evaluator
        .load_wat("circuit_fail_open", INFINITE_LOOP_WAT, plugin_cfg)
        .unwrap();

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    // Failures 1 & 2: timeout, but because FailOpen is active, returns Pass
    assert_eq!(
        evaluator.intercept_request("circuit_fail_open", &mut ctx, &mut req, "{}"),
        InterceptorVerdict::Pass
    );
    assert_eq!(
        evaluator.intercept_request("circuit_fail_open", &mut ctx, &mut req, "{}"),
        InterceptorVerdict::Pass
    );

    // Circuit is now OPEN!
    assert!(evaluator.is_circuit_open("circuit_fail_open"));

    // 3rd call: passes transparently without running WASM
    assert_eq!(
        evaluator.intercept_request("circuit_fail_open", &mut ctx, &mut req, "{}"),
        InterceptorVerdict::Pass
    );
}

#[test]
fn test_wasm_circuit_breaker_half_open_recovery() {
    let engine_cfg = WasmEngineConfig {
        strict_aot: false,
        allow_jit: true,
        epoch_tick_interval_ms: 1,
    };
    let evaluator = Arc::new(WasmInterceptorEvaluator::new(engine_cfg).unwrap());

    // Short cooloff duration for testing (50ms)
    let plugin_cfg = WasmPluginConfig {
        timeout_ms: 3,
        fail_mode: FailMode::FailClosed,
        circuit_breaker: CircuitBreakerConfig {
            consecutive_failure_threshold: 2,
            cooloff_duration: Duration::from_millis(50),
        },
        ..Default::default()
    };

    evaluator
        .load_wat("circuit_recovery", INFINITE_LOOP_WAT, plugin_cfg.clone())
        .unwrap();

    let mut ctx = InterceptorContext::new(Uuid::new_v4(), test_hlc());
    let mut req = http::Request::builder()
        .uri("/graphql")
        .body(())
        .unwrap()
        .into_parts()
        .0;

    // Trip circuit
    let _ = evaluator.intercept_request("circuit_recovery", &mut ctx, &mut req, "{}");
    let _ = evaluator.intercept_request("circuit_recovery", &mut ctx, &mut req, "{}");
    assert!(evaluator.is_circuit_open("circuit_recovery"));

    // Wait for cooloff window to expire
    std::thread::sleep(Duration::from_millis(60));

    // Reload plugin with passing WAT to simulate healed module
    evaluator
        .load_wat("circuit_recovery", PASSING_WAT, plugin_cfg)
        .unwrap();

    // Probe request is allowed and succeeds, resetting circuit to CLOSED!
    let probe_verdict = evaluator.intercept_request("circuit_recovery", &mut ctx, &mut req, "{}");
    assert_eq!(probe_verdict, InterceptorVerdict::Pass);
    assert!(!evaluator.is_circuit_open("circuit_recovery"));
}
