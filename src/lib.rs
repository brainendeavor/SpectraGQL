pub mod admin;
pub mod clock;
pub mod dispatch;
pub mod guards;
pub mod payload;
pub mod proxy;
pub mod ratify;
pub mod spectra_config;
pub mod subscriptions;

pub use dispatch::{EventEncoder, EventSink, JsonEventEncoder};
pub use guards::{
    GuardContext, GuardRejection, GuardVerdict, RequestGuard, ResponseGuard, RuleEvaluator,
};
pub use payload::{CompletionEvent, OperationOutcome, TerminalEvent};
pub use spectra_config::{ExecutionStrategy, OperationMode, SpectraConfig};

use anyhow::Result;
use std::sync::Arc;

use crate::proxy::{CompositeService, ExtraServiceParams, ServiceConfig};

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
        crate::spectra_config::IdempotencyBackendType::Redis => {
            let redis_addr = spectra_configuration
                .idempotency
                .addr
                .as_deref()
                .unwrap_or("127.0.0.1:6379");
            Arc::new(crate::ratify::IdempotencyEngine::new_redis(
                redis_addr,
                std::time::Duration::from_secs(spectra_configuration.idempotency.ttl_secs),
            ))
        }
        crate::spectra_config::IdempotencyBackendType::Memory => {
            Arc::new(crate::ratify::IdempotencyEngine::new(
                std::time::Duration::from_secs(spectra_configuration.idempotency.ttl_secs),
                spectra_configuration.idempotency.max_capacity,
            ))
        }
    };

    let named_upstreams = spectra_configuration.resolve_all_upstreams()?;
    let subscription_hub = Arc::new(crate::subscriptions::SubscriptionHub::new());

    let admin_engine = crate::admin::AdminEngine::new(
        spectra_configuration.admin.clone(),
        spectra_configuration,
        Arc::new(named_upstreams.clone()),
        Arc::new(spectra_configuration.gql.routes.clone()),
    );

    let mut composite_service = CompositeService::new()
        .with_routing(
            named_upstreams,
            spectra_configuration.gql.mode_a.clone(),
            spectra_configuration.gql.routes.clone(),
        )
        .with_idempotency_engine(idempotency_engine)
        .with_subscriptions(subscription_hub, spectra_configuration.subscriptions.clone())
        .with_admin(admin_engine);
    composite_service.add_service_config(gql_service);
    composite_service.add_service_config(rest_service);

    Ok(composite_service)
}
