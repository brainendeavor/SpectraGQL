use serde::Serialize;
use uuid;

use crate::clock::HlcTimestamp;
use crate::payload::{PayloadType, ResponseBody};

const RESPONSE_INFO_VERSION: u8 = 1;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResponseInfo {
    #[serde(with = "uuid::serde::simple")]
    pub request_id: uuid::Uuid,
    pub hlc: HlcTimestamp,

    #[serde(with = "http_serde::header_map")]
    pub http_response_headers: http::HeaderMap,
    pub response_body: ResponseBody,
    #[serde(rename = "type")]
    payload_type: PayloadType,
    version: u8,
}

impl ResponseInfo {
    pub fn new(
        request_id: uuid::Uuid,
        hlc: HlcTimestamp,
        response_body: ResponseBody,
        http_response_headers: http::HeaderMap,
    ) -> Self {
        ResponseInfo {
            request_id,
            hlc,
            http_response_headers,
            response_body,
            payload_type: PayloadType::Response,
            version: RESPONSE_INFO_VERSION,
        }
    }
}
