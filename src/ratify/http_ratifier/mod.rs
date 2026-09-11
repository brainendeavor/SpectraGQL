pub mod lua;
pub use lua::HttpRequestRatifierLua as LuaRatifier;

use crate::payload::RequestInfo;
use crate::ratify::RequestRatification;
use anyhow::Result;

#[derive(Clone)]
pub struct HttpRequestRatifier {}

impl HttpRequestRatifier {
    #[allow(dead_code)]
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
}
