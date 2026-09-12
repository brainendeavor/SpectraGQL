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
            match serde_json::from_str::<serde_json::Value>(raw_body) {
                Ok(val) => {
                    let serialized = val.to_string();
                    log::info!("content-type: {} serde_json: {}", content_type, serialized);
                    serde_json::value::RawValue::from_string(serialized).unwrap_or_default()
                }
                Err(_) => {
                    text = Some(raw_body.to_string());
                    Default::default()
                }
            }
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
