use std::fmt::Debug;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumIs, EnumString};

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GraphQLRequestBody {
    pub query: String,
    pub operation_name: Option<String>,
    pub variables: Option<String>,
    pub extensions: Option<String>,
}

impl GraphQLRequestBody {
    pub fn new(query: String) -> Self {
        GraphQLRequestBody {
            query,
            operation_name: None,
            variables: None,
            extensions: None,
        }
    }

    pub fn new_from_json_str(json_body: &str) -> Result<Self> {
        let gql_json = json::parse(json_body)?;
        log::info!("GraphQLRequest.gql_json: {:?}", gql_json);
        Ok(GraphQLRequestBody::new_from_json_value(&gql_json))
    }

    pub fn new_from_json_value(json_value: &json::JsonValue) -> Self {
        let mut gql_json = json_value.to_owned();
        GraphQLRequestBody {
            query: gql_json["query"]
                .take_string()
                .unwrap_or_else(|| gql_json.to_string()),
            operation_name: gql_json["operationName"].take_string(),
            variables: gql_json["variables"].take_string(),
            extensions: gql_json["extensions"].take_string(),
        }
    }
}

#[derive(
    Display, EnumIs, Debug, Deserialize, Serialize, Clone, Hash, PartialEq, Eq, EnumString,
)]
pub enum GraphQLOperationType {
    #[strum(ascii_case_insensitive)]
    Query,
    #[strum(ascii_case_insensitive)]
    Mutation,
    #[strum(ascii_case_insensitive)]
    Subscription,
    #[strum(ascii_case_insensitive)]
    Unknown,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GraphQLRequestInfo {
    pub operation_type: GraphQLOperationType,
    pub operation_name: Option<String>, // redundant if well formed json in body
    // pub request_body: GraphQLRequestBody,
    raw_body: Option<String>,

    #[serde(serialize_with = "json_body_string")]
    json_body: json::JsonValue,
}

fn json_body_string<S>(json_body: &json::JsonValue, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&json_body.dump())
}

// The intent here is the extract this data from the body. Theoretically possible to
// include the complete AST but right now, just provide the operation type and extract a name.
impl GraphQLRequestInfo {
    pub fn new(request_json: &str) -> Self {
        let mut gql_request_info = GraphQLRequestInfo {
            operation_type: GraphQLOperationType::Unknown,
            operation_name: None,
            raw_body: None,
            json_body: json::JsonValue::Null,
        };
        if let Err(e) = gql_request_info.set_request_json(request_json) {
            log::error!("Invalid JSON for gql request: {}", e);
        }
        gql_request_info
    }

    pub fn set_request_json(&mut self, request_json: &str) -> Result<&mut Self> {
        self.raw_body = Some(request_json.to_string());
        // self.json_body = json::parse(request_json)?;
        return Ok(self);
    }

    pub fn gql_request_body(&self) -> Result<GraphQLRequestBody> {
        match self.raw_body.as_ref() {
            Some(json_body) => {
                let parsed_json_body = json::parse(&json_body)?;
                return Ok(GraphQLRequestBody::new_from_json_value(&parsed_json_body));
            }
            None => {
                return Err(anyhow!("Request body has not been set."));
            }
        }
    }

    pub fn sanitize(&mut self, sanitizer: &crate::ratify::Sanitizer) {
        if let Some(raw) = self.raw_body.as_mut() {
            *raw = sanitizer.sanitize_json_str(raw);
        }
        let dump = self.json_body.dump();
        if !dump.is_empty() && dump != "null" {
            let sanitized = sanitizer.sanitize_json_str(&dump);
            if let Ok(parsed) = json::parse(&sanitized) {
                self.json_body = parsed;
            }
        }
    }
}
