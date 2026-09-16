use serde::Serialize;
use std::fmt::Debug;

#[derive(Serialize, Debug, Clone)]
pub struct HttpRequestInfo {
    #[serde(with = "http_serde::method")]
    pub method: http::Method,
    #[serde(with = "http_serde::uri")]
    pub uri: http::Uri,
    #[serde(with = "http_serde::version")]
    pub version: http::Version,
    #[serde(with = "http_serde::header_map")]
    pub headers: http::HeaderMap,
}

impl HttpRequestInfo {
    pub fn new(http_parts: http::request::Parts) -> Self {
        HttpRequestInfo {
            method: http_parts.method,
            uri: http_parts.uri,
            version: http_parts.version,
            headers: http_parts.headers,
        }
    }
}
