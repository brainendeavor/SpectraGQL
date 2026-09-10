pub mod gql_ratifier;
pub mod http_ratifier;
pub mod idempotency;
pub mod sanitizer;

pub use idempotency::{IdempotencyEngine, IdempotencyOutcome};
pub use sanitizer::Sanitizer;

use anyhow::Result;
use enum_dispatch::enum_dispatch;
use std::collections::HashSet;

use crate::payload::RequestInfo;

#[derive(Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum RatifyResponseAction {
    Respond,
    Dispatch,
}

#[enum_dispatch]
pub trait RequestRatification {
    fn ratify_request(
        &self,
        request_id: uuid::Uuid,
        hlc: crate::clock::HlcTimestamp,
        http_request_parts: http::request::Parts,
        http_request_body: &str,
    ) -> Result<RequestInfo>;
    #[allow(dead_code)]
    fn ratify_response(&self, request_info: &RequestInfo) -> Result<HashSet<RatifyResponseAction>>;
}

#[derive(Clone)]
#[enum_dispatch(RequestRatification)]
pub enum RatificationProtocol {
    HttpProtocol(http_ratifier::HttpRequestRatifier),
    HttpLua(http_ratifier::LuaRatifier),
    GraphQLJson(gql_ratifier::GraphQLRatifier),
}
