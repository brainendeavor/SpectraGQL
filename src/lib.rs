pub mod admin;
pub mod core;
pub mod gateway;
pub mod idempotency;
pub mod interceptors;
pub mod protocol;
pub mod subscriptions;
pub mod telemetry;

pub use core::{
    ExecutionStrategy, HlcClock, HlcTimestamp, IdempotencyBackendType, InterceptorConfig,
    InterceptorStage, InterceptorType, ModeADispatchPolicy, OperationMode, OperationOutcome,
    SpectraAdminConfig, SpectraConfig, SpectraDispatchConfig, SpectraGqlConfig,
    SpectraIdempotencyConfig, SpectraModeAConfig, SpectraRestConfig, SpectraRouteConfig,
    SpectraSubscriptionsConfig, SpectraUpstreamConfig, SpectraWasmConfig,
};
pub use gateway::{CompositeService, ExtraServiceParams, ServiceConfig, generate_command_receipt};
pub use idempotency::{IdempotencyEngine, IdempotencyOutcome, IdempotencyRecord};
pub use interceptors::{
    CelRequestInterceptor, CelResponseInterceptor, CelRuleEvaluator, CircuitBreakerConfig,
    FailMode, InterceptorContext, InterceptorManager, InterceptorRejection, InterceptorVerdict,
    RequestInterceptor, RequestInterceptorPipeline, ResponseInterceptor,
    ResponseInterceptorPipeline, RuleEvaluator, Sanitizer, WasmCircuitBreaker, WasmEngineConfig,
    WasmInterceptorEvaluator, WasmPluginConfig, WasmRequestInterceptor, WasmResponseInterceptor,
};
pub use protocol::{
    GraphQLError, GraphQLErrorResponse, GraphQLOperationType, GraphQLRequestInfo, HttpRequestInfo,
    ProtocolDecoder, RequestDecoder, RequestInfo, ResponseBody, ResponseInfo, SanitizedPayload,
    parse_graphql_operation, parse_graphql_operation_with_name,
};
pub use telemetry::{
    CompletionEvent, DispatchHandler, DispatchMethod, EventEncoder, EventSink, EventStatus,
    JsonEventEncoder, TerminalEvent, find_dispatch_handler_by_method,
};

use anyhow::Result;
use std::sync::Arc;

/// Builds a fully-configured CompositeService from a SpectraConfig.
/// This allows tests to construct the gateway pipeline directly without starting Pingora's full process.
pub fn build_composite_service(spectra_configuration: &SpectraConfig) -> Result<CompositeService> {
    let gql_dispatch = spectra_configuration.gql_dispatch();
    let mut extra_params = ExtraServiceParams::new();
    extra_params.insert(
        "ops_to_dispatch".to_string(),
        spectra_configuration.gql.ops_to_dispatch.clone(),
    );
    let gql_service = ServiceConfig::new(
        "gql",
        spectra_configuration.gql_upstream().addr.as_str(),
        spectra_configuration.gql.paths.as_str(),
        gql_dispatch.method.as_str(),
        gql_dispatch.addr.as_str(),
        extra_params,
    );

    let rest_dispatch = spectra_configuration.rest_dispatch();
    let rest_service = ServiceConfig::new(
        "http",
        spectra_configuration.rest_upstream().addr.as_str(),
        spectra_configuration.rest.paths.as_str(),
        rest_dispatch.method.as_str(),
        rest_dispatch.addr.as_str(),
        ExtraServiceParams::new(),
    );

    let idempotency_engine = match spectra_configuration.idempotency.backend {
        IdempotencyBackendType::Redis => {
            let redis_addr = spectra_configuration
                .idempotency
                .addr
                .as_deref()
                .unwrap_or("127.0.0.1:6379");
            Arc::new(IdempotencyEngine::new_redis(
                redis_addr,
                std::time::Duration::from_secs(spectra_configuration.idempotency.ttl_secs),
            ))
        }
        IdempotencyBackendType::Memory => {
            Arc::new(IdempotencyEngine::new(
                std::time::Duration::from_secs(spectra_configuration.idempotency.ttl_secs),
                spectra_configuration.idempotency.max_capacity,
            ))
        }
    };

    let named_upstreams = spectra_configuration.resolve_all_upstreams()?;
    let subscription_hub = Arc::new(crate::subscriptions::SubscriptionHub::new());
    let traffic_recorder = Arc::new(crate::admin::TrafficRecorder::new(250));
    let worker_registry = Arc::new(crate::admin::WorkerRegistry::default());

    let admin_engine = crate::admin::AdminEngine::new(
        spectra_configuration.admin.clone(),
        spectra_configuration,
        Arc::new(named_upstreams.clone()),
        Arc::new(spectra_configuration.gql.routes.clone()),
        traffic_recorder.clone(),
        worker_registry,
    );

    let interceptor_manager = InterceptorManager::from_config(spectra_configuration)?;

    let mut composite_service = CompositeService::new()
        .with_routing(
            named_upstreams,
            spectra_configuration.gql.mode_a.clone(),
            spectra_configuration.gql.routes.clone(),
        )
        .with_idempotency_engine(idempotency_engine)
        .with_subscriptions(subscription_hub, spectra_configuration.subscriptions.clone())
        .with_admin(admin_engine)
        .with_interceptor_manager(Arc::new(interceptor_manager))
        .with_traffic_recorder(traffic_recorder);
    composite_service.add_service_config(gql_service);
    composite_service.add_service_config(rest_service);

    Ok(composite_service)
}
