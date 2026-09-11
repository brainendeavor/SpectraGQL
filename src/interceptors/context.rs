use std::collections::HashMap;
use uuid::Uuid;
use crate::clock::HlcTimestamp;
use crate::payload::GraphQLOperationType;

/// Execution context passed through RequestInterceptor and ResponseInterceptor pipelines.
#[derive(Debug, Clone)]
pub struct InterceptorContext {
    pub request_id: Uuid,
    pub hlc: HlcTimestamp,
    pub operation_name: Option<String>,
    pub operation_type: Option<GraphQLOperationType>,
    pub metadata: HashMap<String, String>,
    pub extensions: HashMap<String, serde_json::Value>,
}

impl InterceptorContext {
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

/// Verdict returned by an interceptor.
/// Allows pure pass-through, defensive rejection, or active payload transformation (e.g. anonymization, shaping).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptorVerdict {
    /// The request/response passed without modification.
    Pass,
    /// The request/response was rejected due to a contract or policy violation.
    Reject(InterceptorRejection),
    /// The headers, query, or body were transformed.
    Transform {
        headers: Option<http::HeaderMap>,
        body: Option<Vec<u8>>,
    },
}

/// Rejection returned when an interceptor blocks an inbound request or outbound response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterceptorRejection {
    pub status_code: http::StatusCode,
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

impl InterceptorRejection {
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
        crate::payload::GraphQLErrorResponse::from(self).to_json_string()
    }
}

impl std::fmt::Display for InterceptorRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "InterceptorRejection({}: [{} - {}])",
            self.status_code, self.code, self.message
        )
    }
}

impl std::error::Error for InterceptorRejection {}

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
