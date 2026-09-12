use std::fmt::Debug;

use mlua::prelude::{LuaUserData, LuaUserDataFields};
use serde::Serialize;

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

impl LuaUserData for HttpRequestInfo {
    fn add_fields<F: LuaUserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("method", |_, this| Ok(this.method.to_string()));
        fields.add_field_method_get("uri", |_, this| Ok(this.uri.to_string()));
        fields.add_field_method_get("version", |_, this| Ok(format!("{:?}", this.version)));
        fields.add_field_method_get("headers", |lua, this| {
            let headers = lua.create_table()?;
            this.headers.iter().for_each(|(key, value)| {
                headers
                    .set(key.to_string(), value.to_str().unwrap_or_default())
                    .unwrap_or_default()
            });
            Ok(headers)
        });
    }
}
