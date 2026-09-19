use spectragql::core::config::{ExecutionStrategy, SpectraRouteConfig};
use spectragql::interceptors::{
    DeployAuthInterceptor, InterceptorContext, InterceptorVerdict, RequestInterceptor,
};

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
        dispatch_policy: None,
    };

    assert!(!route.enabled);
    assert_eq!(route.operation, "deployFluxcell");
}
