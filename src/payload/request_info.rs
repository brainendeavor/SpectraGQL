use mlua::{
    IntoLua,
    prelude::{LuaUserData, LuaUserDataFields},
};
use serde::Serialize;
use std::fmt;

use crate::clock::HlcTimestamp;
use crate::payload::{GraphQLRequestInfo, HttpRequestInfo, PayloadType};

const REQUEST_INFO_VERSION: u8 = 1;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RequestInfo {
    #[serde(with = "uuid::serde::simple")]
    pub request_id: uuid::Uuid,
    pub hlc: HlcTimestamp,
    pub http: HttpRequestInfo,
    pub gql: Option<GraphQLRequestInfo>,
    // todo: use Arc here
    pub raw_request_body: Option<Vec<u8>>,
    #[serde(rename = "type")]
    payload_type: PayloadType,
    version: u8,
}

impl RequestInfo {
    pub fn new(
        request_id: uuid::Uuid,
        hlc: HlcTimestamp,
        http_request_parts: http::request::Parts,
    ) -> Self {
        RequestInfo {
            request_id,
            hlc,
            http: HttpRequestInfo::new(http_request_parts),
            gql: None,
            raw_request_body: None,
            payload_type: PayloadType::Request,
            version: REQUEST_INFO_VERSION,
        }
    }

    /// Recursively sanitizes headers, GraphQL body, and raw body before writing to event streams.
    pub fn sanitize(&mut self) {
        let sanitizer = crate::ratify::Sanitizer::default_sanitizer();

        let mut sanitized_headers = http::HeaderMap::new();
        for (k, v) in self.http.headers.iter() {
            if sanitizer.is_sensitive_key(k.as_str()) {
                if let Ok(s) = v.to_str() {
                    if s.to_lowercase().starts_with("bearer ") {
                        sanitized_headers.insert(k.clone(), http::HeaderValue::from_static("Bearer [REDACTED]"));
                    } else {
                        sanitized_headers.insert(k.clone(), http::HeaderValue::from_static("[REDACTED]"));
                    }
                } else {
                    sanitized_headers.insert(k.clone(), http::HeaderValue::from_static("[REDACTED]"));
                }
            } else {
                sanitized_headers.insert(k.clone(), v.clone());
            }
        }
        self.http.headers = sanitized_headers;

        if let Some(gql) = self.gql.as_mut() {
            gql.sanitize(sanitizer);
        }

        if let Some(body) = self.raw_request_body.as_mut() {
            if let Ok(s) = std::str::from_utf8(body) {
                let sanitized = sanitizer.sanitize_json_str(s);
                *body = sanitized.into_bytes();
            }
        }
    }

    /// Transforms this request into a compile-time verified `SanitizedPayload<RequestInfo>`.
    pub fn into_sanitized(mut self) -> crate::payload::SanitizedPayload<Self> {
        self.sanitize();
        crate::payload::SanitizedPayload::new_unchecked(self)
    }
}

impl fmt::Display for RequestInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.gql {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string().to_lowercase();
                match gql_op.operation_name.to_owned() {
                    Some(operation_name) => {
                        write!(f, "{}.{}", op_type, operation_name.to_lowercase())
                    }
                    None => write!(f, "{}.anonymous", op_type),
                }
            }
            None => write!(f, "HTTP {}", &self.http.method),
        }
    }
}

impl LuaUserData for RequestInfo {
    fn add_fields<F: LuaUserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("request_id", |_, this| Ok(this.request_id.to_string()));
        fields.add_field_method_get("hlc", |_, this| Ok(this.hlc.to_compact_string()));
        fields.add_field_method_get("http", |lua, this| this.http.clone().into_lua(lua));
    }
}
