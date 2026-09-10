mod composite_service;
mod gql_service;
mod http_service;

pub use composite_service::CompositeServiceProxy as CompositeService;
pub use composite_service::ServiceConfig;
pub use gql_service::GraphQLServiceProxy as GraphQLService;
pub use http_service::HttpServiceProxy as HttpService;

use crate::dispatch::DispatchMethod;
use crate::payload::ResponseBody;
use crate::ratify::{RatificationProtocol, RatifyResponseAction};
use anyhow::{Result, anyhow};
use enum_dispatch::enum_dispatch;
use matchit::Router;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-spectra-request-id";
pub type ServiceHandle = usize;
pub type ExtraServiceParams = HashMap<String, String>;

#[enum_dispatch]
pub trait SpectraProxyService {
    fn get_request_protocol(&self) -> &RatificationProtocol;
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
    pub hlc: crate::clock::HlcTimestamp,
    pub start_time: std::time::Instant,
    pub request_topic: String,
    pub request_info: Option<crate::payload::RequestInfo>,
    pub response_parts: Option<http::response::Parts>,
    pub response_body: Option<ResponseBody>,
    #[allow(dead_code)]
    pub response_actions: HashSet<RatifyResponseAction>,
    pub buffer: Vec<u8>,
    pub idempotency_key: Option<String>,
    pub is_replay: bool,
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
