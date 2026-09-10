pub mod lua;
pub use lua::HttpRequestRatifierLua as LuaRatifier;

use crate::payload::RequestInfo;
use crate::ratify::{RatifyResponseAction, RequestRatification};
use anyhow::Result;
use std::collections::HashSet;

#[derive(Clone)]
pub struct HttpRequestRatifier {}

impl HttpRequestRatifier {
    pub fn new() -> Self {
        HttpRequestRatifier {}
    }
}

impl RequestRatification for HttpRequestRatifier {
    fn ratify_request(
        &self,
        request_id: uuid::Uuid,
        hlc: crate::clock::HlcTimestamp,
        http_request_parts: http::request::Parts,
        _http_request_body: &str,
    ) -> Result<RequestInfo> {
        log::info!("enum_dispatch RequestRatification -> HttpRequestRatifier!");
        Ok(RequestInfo::new(request_id, hlc, http_request_parts))
    }

    fn ratify_response(
        &self,
        _request_info: &RequestInfo,
    ) -> Result<HashSet<RatifyResponseAction>> {
        Ok(HashSet::from_iter([
            RatifyResponseAction::Respond,
            RatifyResponseAction::Dispatch,
        ]))
    }
}
