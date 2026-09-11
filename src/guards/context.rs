use std::collections::HashMap;
use uuid::Uuid;
use crate::clock::HlcTimestamp;
use crate::payload::GraphQLOperationType;

/// Execution context passed through RequestGuard and ResponseGuard pipelines.
#[derive(Debug, Clone)]
pub struct GuardContext {
    pub request_id: Uuid,
    pub hlc: HlcTimestamp,
    pub operation_name: Option<String>,
    pub operation_type: Option<GraphQLOperationType>,
    pub metadata: HashMap<String, String>,
    pub extensions: HashMap<String, serde_json::Value>,
}

impl GuardContext {
    pub fn new(request_id: Uuid, hlc: HlcTimestamp) -> Self {
        Self {
            request_id,
            hlc,
            operation_name: None,
            operation_type: None,
            metadata: HashMap::new(),
            extensions: HashMap::new(),
        }
    }

    pub fn with_operation(
        mut self,
        name: Option<String>,
        op_type: Option<GraphQLOperationType>,
    ) -> Self {
        self.operation_name = name;
        self.operation_type = op_type;
        self
    }
}

/// Verdict returned by a guard when evaluation succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardVerdict {
    /// The request/response passed guard evaluation without modification.
    Pass,
    /// The request/response was mutated by the guard (e.g. headers injected or sanitized).
    Mutated,
}

/// Rejection returned when a guard blocks an inbound request or outbound response.
#[derive(Debug, Clone)]
pub struct GuardRejection {
    pub status_code: http::StatusCode,
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

impl GuardRejection {
    pub fn new(status_code: http::StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status_code,
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Renders a GraphQL error response JSON string matching the standard GraphQL spec:
    /// `{"errors": [{"message": "...", "extensions": {"code": "..."}}]}`
    pub fn to_graphql_response(&self) -> String {
        let mut err_obj = serde_json::json!({
            "message": self.message,
            "extensions": {
                "code": self.code,
            }
        });
        if let Some(details) = &self.details {
            if let Some(map) = err_obj.get_mut("extensions").and_then(|e| e.as_object_mut()) {
                map.insert("details".to_string(), details.clone());
            }
        }
        serde_json::json!({
            "errors": [err_obj]
        })
        .to_string()
    }
}

impl std::fmt::Display for GuardRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GuardRejection({}: [{} - {}])",
            self.status_code, self.code, self.message
        )
    }
}

impl std::error::Error for GuardRejection {}

/// Errors produced during rule evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleEvaluationError {
    NotFound(String),
    EvaluationFailed(String),
    InvalidInput(String),
}

impl std::fmt::Display for RuleEvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleEvaluationError::NotFound(rule) => write!(f, "Rule not found: {}", rule),
            RuleEvaluationError::EvaluationFailed(msg) => write!(f, "Rule evaluation failed: {}", msg),
            RuleEvaluationError::InvalidInput(msg) => write!(f, "Invalid rule input: {}", msg),
        }
    }
}

impl std::error::Error for RuleEvaluationError {}
