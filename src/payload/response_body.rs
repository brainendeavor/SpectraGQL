use http::HeaderMap;
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize, Debug, Clone)]
pub struct ResponseBody {
    pub json: Arc<Box<serde_json::value::RawValue>>,
    pub text: Option<String>,
}

impl ResponseBody {
    pub fn new(http_headers: &HeaderMap, raw_body: &str) -> Self {
        let mut text: Option<String> = None;
        // let text = Some(raw_body.to_string());
        let content_type = http_headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let is_json_candidate = content_type.contains("json")
            || raw_body.trim().starts_with('{')
            || raw_body.trim().starts_with('[');
        let raw_value_json = if is_json_candidate {
            // uncomment for no syntactic json, when serde_json attempts to serialize will nullify
            // let raw_body_string = raw_body.to_string();

            // attempt to parse/validate json syntax
            let raw_body_string = json::parse(raw_body)
                .unwrap_or(json::JsonValue::Null)
                .dump();
            log::info!(
                "content-type: {} json::parse: {}",
                content_type,
                raw_body_string
            );
            // if raw_body isn't valid json, then fall back to text
            if raw_body_string.eq("null") {
                text = Some(raw_body.to_string());
            };
            serde_json::value::RawValue::from_string(raw_body_string).unwrap_or_default()
        } else {
            text = Some(raw_body.to_string());
            Default::default()
        };
        ResponseBody {
            json: Arc::new(raw_value_json),
            text,
        }
    }
}
