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
    SpectraAdminConfig, SpectraAppConfig, SpectraConfig, SpectraDispatchConfig, SpectraGqlConfig,
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
    // 1 & 2. Initialize persistent config store and hydrate/seed configuration
    // Prefer preserving original file content with comments if available
    let default_raw = std::env::var("SPECTRA_CONFIG_CONTENT")
        .or_else(|_| std::env::var("SPECTRAGQL_CONFIG_CONTENT"))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            let path_to_read = std::env::var("SPECTRA_CONFIG")
                .or_else(|_| std::env::var("SPECTRAGQL_CONFIG"))
                .unwrap_or_else(|_| "spectra.toml".to_string());
            std::fs::read_to_string(&path_to_read)
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| {
            spectra_configuration
                .to_toml_string()
                .unwrap_or_default()
        });

    let (config_store, effective_cfg, active_raw) = {
        let runner = async {
            let store = crate::core::ConfigStoreFactory::create(&spectra_configuration.config_store).await?;
            let (eff_cfg, raw) = match store.load_config().await {
                Ok(Some(stored_raw)) if !stored_raw.trim().is_empty() => {
                    match SpectraConfig::from_toml_str(&stored_raw) {
                        Ok(parsed) => {
                            log::info!(
                                "Boot: Loaded active configuration from persistent store (backend: {})",
                                store.backend_name()
                            );
                            (parsed, stored_raw)
                        }
                        Err(e) => {
                            log::warn!(
                                "Boot: Stored configuration invalid ({}), falling back to startup config",
                                e
                            );
                            (spectra_configuration.clone(), default_raw.clone())
                        }
                    }
                }
                Ok(_) => {
                    if !default_raw.is_empty() {
                        if let Err(e) = store.save_config(&default_raw).await {
                            log::warn!("Boot: Failed to seed default configuration to store: {}", e);
                        } else {
                            log::info!(
                                "Boot: Seeded initial configuration to store (backend: {})",
                                store.backend_name()
                            );
                        }
                    }
                    (spectra_configuration.clone(), default_raw.clone())
                }
                Err(e) => {
                    log::warn!(
                        "Boot: Failed to query store ({}), using startup configuration",
                        e
                    );
                    (spectra_configuration.clone(), default_raw.clone())
                }
            };
            Ok::<_, anyhow::Error>((store, eff_cfg, raw))
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(runner))?
        } else {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
            rt.block_on(runner)?
        }
    };

    // 3. Build DynamicGatewayState
    let dynamic_state_obj = crate::gateway::DynamicGatewayState::new_from_config(&effective_cfg, active_raw, 1)?;
    let dynamic_state = Arc::new(arc_swap::ArcSwap::from_pointee(dynamic_state_obj));

    let gql_dispatch = effective_cfg.gql_dispatch();
    let mut extra_params = ExtraServiceParams::new();
    extra_params.insert(
        "ops_to_dispatch".to_string(),
        effective_cfg.gql.ops_to_dispatch.clone(),
    );
    let gql_service = ServiceConfig::try_new(
        "gql",
        effective_cfg.gql_upstream().addr.as_str(),
        effective_cfg.gql.paths.as_str(),
        gql_dispatch.method.as_str(),
        gql_dispatch.addr.as_str(),
        extra_params,
    )?;

    let rest_dispatch = effective_cfg.rest_dispatch();
    let rest_service = ServiceConfig::try_new(
        "http",
        effective_cfg.rest_upstream().addr.as_str(),
        effective_cfg.rest.paths.as_str(),
        rest_dispatch.method.as_str(),
        rest_dispatch.addr.as_str(),
        ExtraServiceParams::new(),
    )?;

    let idempotency_engine = match effective_cfg.idempotency.backend {
        IdempotencyBackendType::Redis => {
            let redis_addr = effective_cfg
                .idempotency
                .addr
                .as_deref()
                .unwrap_or("127.0.0.1:6379");
            Arc::new(IdempotencyEngine::new_redis(
                redis_addr,
                std::time::Duration::from_secs(effective_cfg.idempotency.ttl_secs),
            ))
        }
        IdempotencyBackendType::Memory => {
            Arc::new(IdempotencyEngine::new(
                std::time::Duration::from_secs(effective_cfg.idempotency.ttl_secs),
                effective_cfg.idempotency.max_capacity,
            ))
        }
    };

    let (named_upstreams, upstream_targets) = effective_cfg.resolve_all_upstreams_with_targets()?;
    let subscription_hub = Arc::new(crate::subscriptions::SubscriptionHub::new());
    let traffic_recorder = Arc::new(crate::admin::TrafficRecorder::new(250));
    let worker_registry = Arc::new(crate::admin::WorkerRegistry::default());

    let admin_engine = crate::admin::AdminEngine::new(
        effective_cfg.admin.clone(),
        &effective_cfg,
        Arc::new(named_upstreams.clone()),
        Arc::new(effective_cfg.gql.routes.clone()),
        traffic_recorder.clone(),
        worker_registry,
    )
    .with_dynamic_state(dynamic_state.clone())
    .with_config_store(config_store.clone());

    let interceptor_manager = InterceptorManager::from_config(&effective_cfg)?;

    let mut composite_service = CompositeService::new()
        .with_routing_with_targets(
            named_upstreams,
            upstream_targets,
            effective_cfg.gql.mode_a.clone(),
            effective_cfg.gql.routes.clone(),
        )
        .with_apps(
            effective_cfg.apps.clone(),
            effective_cfg.default_app.clone(),
        )
        .with_idempotency_engine(idempotency_engine)
        .with_subscriptions(subscription_hub, effective_cfg.subscriptions.clone())
        .with_admin(admin_engine)
        .with_interceptor_manager(Arc::new(interceptor_manager))
        .with_traffic_recorder(traffic_recorder)
        .with_telemetry(effective_cfg.telemetry.clone())
        .with_dynamic_state(dynamic_state);
    composite_service.add_service_config(gql_service);
    composite_service.add_service_config(rest_service);

    Ok(composite_service)
}
