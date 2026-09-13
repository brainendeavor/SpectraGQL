use std::fmt::Debug;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumIs, EnumString};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GraphQLRequestBody {
    pub query: String,
    pub operation_name: Option<String>,
    pub variables: Option<serde_json::Value>,
    pub extensions: Option<serde_json::Value>,
}

#[allow(dead_code)]
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
        let parsed: Self = serde_json::from_str(json_body)?;
        log::info!("GraphQLRequest.parsed: {:?}", parsed);
        Ok(parsed)
    }

    pub fn new_from_json_value(val: &serde_json::Value) -> Result<Self> {
        let parsed: Self = serde_json::from_value(val.clone())?;
        Ok(parsed)
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
    pub operation_name: Option<String>,
    pub root_fields: Vec<String>,
    raw_body: Option<String>,
    json_body: serde_json::Value,
}

impl GraphQLRequestInfo {
    pub fn new(request_json: &str) -> Self {
        let mut gql_request_info = GraphQLRequestInfo {
            operation_type: GraphQLOperationType::Unknown,
            operation_name: None,
            root_fields: Vec::new(),
            raw_body: None,
            json_body: serde_json::Value::Null,
        };
        if let Err(e) = gql_request_info.set_request_json(request_json) {
            log::error!("Invalid JSON for gql request: {}", e);
        }
        gql_request_info
    }

    pub fn matches_operation(&self, target: &str) -> bool {
        if let Some(name) = &self.operation_name {
            if name.eq_ignore_ascii_case(target) {
                return true;
            }
        }
        self.root_fields.iter().any(|f| f.eq_ignore_ascii_case(target))
    }

    pub fn set_request_json(&mut self, request_json: &str) -> Result<&mut Self> {
        self.raw_body = Some(request_json.to_string());
        if let Ok(val) = serde_json::from_str(request_json) {
            self.json_body = val;
        }
        Ok(self)
    }

    pub fn gql_request_body(&self) -> Result<GraphQLRequestBody> {
        match self.raw_body.as_ref() {
            Some(json_body) => {
                let parsed_body: GraphQLRequestBody = serde_json::from_str(json_body)?;
                Ok(parsed_body)
            }
            None => Err(anyhow!("Request body has not been set.")),
        }
    }

    #[inline]
    pub fn json_body(&self) -> &serde_json::Value {
        &self.json_body
    }

    pub fn sanitize(&mut self, sanitizer: &crate::interceptors::Sanitizer) {
        if let Some(raw) = self.raw_body.as_mut() {
            *raw = sanitizer.sanitize_json_str(raw);
        }
        if !self.json_body.is_null() {
            sanitizer.sanitize_value(&mut self.json_body);
        }
    }
}
