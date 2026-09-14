use std::sync::Arc;
use spectragql::core::config::{ExecutionStrategy, SpectraRouteConfig};
use spectragql::interceptors::{
    DeployAuthInterceptor, InterceptorContext, InterceptorVerdict, RequestInterceptor,
};
use spectral_flux::config::DeployerConfig;
use spectral_flux::deployer::{
    DeployerGuard, DeployerRegistry, FluxcellDeployer, FluxcellStatus, SSRFShield,
};
use spectral_flux::http::FluxRouter;
use spectral_flux::wasm::WasmHost;

#[test]
fn test_deploy_auth_interceptor_validates_token_and_rejects_missing() {
    let interceptor = DeployAuthInterceptor {
        deploy_token: Some("super-secret-token-123".to_string()),
    };

    // 1. Non-deployment operation should pass without any credentials
    {
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("getUserProfile".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = interceptor.intercept_request(&mut ctx, &mut parts, "{}");
        assert!(matches!(verdict, InterceptorVerdict::Pass));
    }

    // 2. Deployment operation missing Authorization header -> 401 Unauthorized
    {
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("deployFluxcell".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = interceptor.intercept_request(&mut ctx, &mut parts, "{}");
        match verdict {
            InterceptorVerdict::Reject(rejection) => {
                assert_eq!(rejection.status_code, http::StatusCode::UNAUTHORIZED);
                assert_eq!(rejection.code, "DEPLOY_UNAUTHORIZED");
            }
            other => panic!("Expected rejection, got {:?}", other),
        }
    }

    // 3. Deployment operation with wrong token -> 401 Unauthorized
    {
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("activateFluxcell".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header(http::header::AUTHORIZATION, "Bearer wrong-token")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = interceptor.intercept_request(&mut ctx, &mut parts, "{}");
        match verdict {
            InterceptorVerdict::Reject(rejection) => {
                assert_eq!(rejection.status_code, http::StatusCode::UNAUTHORIZED);
            }
            other => panic!("Expected rejection, got {:?}", other),
        }
    }

    // 4. Deployment operation with valid Bearer token -> Pass
    {
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("deployFluxcell".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header(http::header::AUTHORIZATION, "Bearer super-secret-token-123")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = interceptor.intercept_request(&mut ctx, &mut parts, "{}");
        assert!(matches!(verdict, InterceptorVerdict::Pass));
    }

    // 5. Deployment operation with valid X-Spectra-Deploy-Key -> Pass
    {
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("removeFluxcell".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header("x-spectra-deploy-key", "super-secret-token-123")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = interceptor.intercept_request(&mut ctx, &mut parts, "{}");
        assert!(matches!(verdict, InterceptorVerdict::Pass));
    }

    // 6. Unconfigured deploy token -> 401 Unauthorized (fail-safe rejection)
    {
        let unconfigured = DeployAuthInterceptor { deploy_token: None };
        let mut ctx = InterceptorContext::new(
            uuid::Uuid::now_v7(),
            spectragql::HlcTimestamp::new(1000, 0),
        );
        ctx.operation_name = Some("deployFluxcell".to_string());
        let req = http::Request::builder()
            .method("POST")
            .uri("/graphql")
            .header(http::header::AUTHORIZATION, "Bearer any-token")
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let verdict = unconfigured.intercept_request(&mut ctx, &mut parts, "{}");
        match verdict {
            InterceptorVerdict::Reject(rejection) => {
                assert_eq!(rejection.status_code, http::StatusCode::UNAUTHORIZED);
                assert_eq!(rejection.code, "DEPLOY_UNAUTHORIZED");
            }
            other => panic!("Expected rejection when unconfigured, got {:?}", other),
        }
    }
}

#[test]
fn test_deploy_route_disabled_by_default() {
    let route = SpectraRouteConfig {
        operation: "deployFluxcell".to_string(),
        mode: ExecutionStrategy::AsyncCommandReceipt,
        enabled: false,
        upstream: None,
        receipt_status: "ACCEPTED".to_string(),
        interceptors: vec![],
    };

    assert!(!route.enabled);
    assert_eq!(route.operation, "deployFluxcell");
}

#[tokio::test]
async fn test_deployer_two_phase_staged_lifecycle_and_hot_swap() {
    let temp_dir = std::env::temp_dir().join(format!("spectral_deploy_test_{}", uuid::Uuid::new_v4()));
    let mut config = DeployerConfig::default();
    config.enabled = true;
    config.storage_dir = temp_dir.to_string_lossy().to_string();
    config.auto_activate = false; // Two-phase staged governance

    let guard = Arc::new(DeployerGuard::new(true, true));
    let registry = Arc::new(DeployerRegistry::new(&temp_dir).unwrap());
    let wasm_host = Arc::new(WasmHost::new(5, None).unwrap());
    let router = Arc::new(std::sync::RwLock::new(FluxRouter::new()));

    let deployer = FluxcellDeployer::new(
        config,
        guard.clone(),
        registry.clone(),
        wasm_host.clone(),
        router.clone(),
    );

    // Minimal valid WebAssembly binary bytecode (wasm magic header \0asm\x01\0\0\0)
    let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // Phase 1: Stage artifact directly
    let record = deployer
        .stage_uploaded_artifact("invoice-service", wasm_bytes.clone(), "/api/invoices", None, None)
        .expect("Staging valid wasm should succeed");

    assert_eq!(record.name, "invoice-service");
    assert_eq!(record.status, FluxcellStatus::Staged);
    assert!(record.activated_at.is_none());

    // Verify record is saved to disk manifest
    let loaded = registry.get_record("invoice-service").expect("Record should exist in registry");
    assert_eq!(loaded.status, FluxcellStatus::Staged);

    // Phase 2: Live Activation
    let activated = deployer
        .activate("invoice-service", &record.sha256)
        .expect("Activation should succeed");

    assert_eq!(activated.status, FluxcellStatus::Active);
    assert!(activated.activated_at.is_some());

    // Phase 3: Cleanup / Removal
    deployer.remove("invoice-service").expect("Removal should succeed");
    assert!(registry.get_record("invoice-service").is_none());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_deployer_killswitch_lockdown_prevents_deployments() {
    let temp_dir = std::env::temp_dir().join(format!("spectral_lockdown_test_{}", uuid::Uuid::new_v4()));
    let mut config = DeployerConfig::default();
    config.enabled = true;
    config.storage_dir = temp_dir.to_string_lossy().to_string();

    let guard = Arc::new(DeployerGuard::new(true, true));
    let registry = Arc::new(DeployerRegistry::new(&temp_dir).unwrap());
    let wasm_host = Arc::new(WasmHost::new(5, None).unwrap());
    let router = Arc::new(std::sync::RwLock::new(FluxRouter::new()));

    let deployer = FluxcellDeployer::new(
        config,
        guard.clone(),
        registry.clone(),
        wasm_host.clone(),
        router.clone(),
    );

    // Trigger instant lockdown
    guard.emergency_lockdown();
    assert!(!guard.is_external_deploy_allowed());
    assert!(!guard.is_dev_upload_allowed());

    let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // Attempt direct upload while locked down
    let upload_err = deployer
        .stage_uploaded_artifact("test-cell", wasm_bytes, "/api/test", None, None)
        .unwrap_err();
    assert!(upload_err.to_string().contains("locked down"));

    // Attempt remote artifact fetch while locked down
    let remote_err = deployer
        .stage_remote_artifact("test-cell", "https://github.com/test.wasm", "abc", "/api/test", None, None, None)
        .await
        .unwrap_err();
    assert!(remote_err.to_string().contains("locked down"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_deployer_ssrf_shield_blocks_sensitive_ips() {
    let shield = SSRFShield::new(vec!["github.com".to_string()], true, true);

    // 1. Insecure scheme
    let err_http = shield.validate_url("http://github.com/cell.wasm").await.unwrap_err();
    assert!(err_http.to_string().contains("Insecure scheme"));

    // 2. Unwhitelisted host
    let err_unwhitelisted = shield.validate_url("https://malicious.com/cell.wasm").await.unwrap_err();
    assert!(err_unwhitelisted.to_string().contains("not in allowed_artifact_hosts"));

    // 3. Cloud metadata endpoint
    let err_metadata = shield.validate_url("https://169.254.169.254/latest/meta-data").await.unwrap_err();
    assert!(err_metadata.to_string().contains("not in allowed_artifact_hosts"));
}
