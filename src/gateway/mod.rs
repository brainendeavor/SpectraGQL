pub mod filters;
mod composite_service;
mod gql_service;
mod http_service;

pub use composite_service::{
    CompositeServiceProxy, CompositeServiceProxy as CompositeService, DynamicGatewayState,
    ServiceConfig,
};
pub use filters::generate_command_receipt;
pub use gql_service::GraphQLServiceProxy as GraphQLService;
pub use http_service::HttpServiceProxy as HttpService;

use crate::core::clock::HlcTimestamp;
use crate::core::config::ModeADispatchPolicy;
use crate::protocol::{ProtocolDecoder, RequestInfo, ResponseBody};
use crate::telemetry::DispatchMethod;
use anyhow::{Result, anyhow};
use enum_dispatch::enum_dispatch;
use matchit::Router;
use std::collections::HashMap;
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-spectra-request-id";
pub type ServiceHandle = usize;
pub type ExtraServiceParams = HashMap<String, String>;

#[enum_dispatch]
pub trait SpectraProxyService {
    fn get_request_protocol(&self) -> &ProtocolDecoder;
    fn get_dispatch_method(&self) -> &DispatchMethod;
}

#[derive(Clone, strum_macros::Display)]
#[enum_dispatch(SpectraProxyService)]
pub enum ProxyService {
    #[strum(ascii_case_insensitive, to_string = "http")]
    Http(HttpService),
    #[strum(ascii_case_insensitive, to_string = "gql")]
    GraphQL(GraphQLService),
}

// TODO: collapse into CompositeServiceProxyCtx
pub struct SpectraProxyCtx {
    pub request_id: Uuid,
    pub hlc: HlcTimestamp,
    pub start_time: std::time::Instant,
    pub request_topic: String,
    pub request_info: Option<RequestInfo>,
    pub response_parts: Option<http::response::Parts>,
    pub response_body: Option<ResponseBody>,
    pub buffer: Vec<u8>,
    pub idempotency_key: Option<String>,
    pub is_replay: bool,
    pub target_upstream_addr: Option<std::net::SocketAddr>,
    pub is_mode_b_terminated: bool,
    pub dispatch_policy: ModeADispatchPolicy,
    pub active_operation: Option<String>,
    pub has_response_interception: bool,
    pub query_preview: Option<String>,
    pub variables_preview: Option<String>,
    pub app_id: String,
    pub audit_tag: Option<String>,
    pub audit_rule: Option<String>,
    pub response_preview: Option<String>,
    pub error_preview: Option<String>,
}

#[derive(Clone)]
pub struct PathRouter {
    service_router: Router<ServiceHandle>,
}

impl PathRouter {
    pub fn new() -> Self {
        PathRouter {
            service_router: Router::new(),
        }
    }

    pub fn add_service_handle(&mut self, route: &str, service_handle: ServiceHandle) -> Result<()> {
        self.service_router
            .insert(route, service_handle)
            .map_err(anyhow::Error::msg)
    }

    pub fn get_service_handle(&self, path: &str) -> Option<ServiceHandle> {
        match self.service_router.at(path) {
            Ok(matched_service) => Some(*matched_service.value),
            Err(_) => None,
        }
    }
}

pub fn new_proxy_service(
    service_type: &str,
    dispatch_method: &str,
    dispatch_endpoint: &str,
    extra_params: ExtraServiceParams,
    // ops_to_dispatch: &str, // generalize this...maybe pass the config all the way through?
) -> Result<ProxyService> {
    match service_type.to_lowercase().as_str() {
        "http" => Ok(ProxyService::Http(
            HttpService::new(dispatch_method, dispatch_endpoint, extra_params).into(),
        )),
        "gql" => Ok(ProxyService::GraphQL(
            GraphQLService::new(dispatch_method, dispatch_endpoint, extra_params).into(),
        )),
        _ => Err(anyhow!("Unsupported service type: {}", service_type)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_router_with_root_and_catchall() {
        let mut router = PathRouter::new();
        // 0: gql service
        router.add_service_handle("/gql", 0).unwrap();
        router.add_service_handle("/graphql", 0).unwrap();

        // 1: http service
        router.add_service_handle("/", 1).unwrap();
        router.add_service_handle("/{*path}", 1).unwrap();
        router.add_service_handle("/api", 1).unwrap();
        router.add_service_handle("/api/{*path}", 1).unwrap();

        // Check gql
        assert_eq!(router.get_service_handle("/gql"), Some(0));
        assert_eq!(router.get_service_handle("/graphql"), Some(0));

        // Check root and paths
        assert_eq!(router.get_service_handle("/"), Some(1));
        assert_eq!(router.get_service_handle("/public/table.js"), Some(1));
        assert_eq!(router.get_service_handle("/api"), Some(1));
        assert_eq!(router.get_service_handle("/api/v1/status"), Some(1));
    }
}
