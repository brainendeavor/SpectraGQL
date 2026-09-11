pub mod gql_ratifier;
pub mod http_ratifier;
pub mod idempotency;
pub mod sanitizer;

pub use idempotency::{IdempotencyEngine, IdempotencyOutcome};
pub use sanitizer::Sanitizer;

use anyhow::Result;
use enum_dispatch::enum_dispatch;

use crate::payload::RequestInfo;

#[enum_dispatch]
pub trait RequestRatification {
    fn ratify_request(
        &self,
        request_id: uuid::Uuid,
        hlc: crate::clock::HlcTimestamp,
        http_request_parts: http::request::Parts,
        http_request_body: &str,
    ) -> Result<RequestInfo>;
}

#[derive(Clone)]
#[enum_dispatch(RequestRatification)]
pub enum RatificationProtocol {
    HttpProtocol(http_ratifier::HttpRequestRatifier),
    HttpLua(http_ratifier::LuaRatifier),
    GraphQLJson(gql_ratifier::GraphQLRatifier),
}
