use std::collections::HashMap;
use uuid::Uuid;
use crate::core::clock::HlcTimestamp;
use crate::protocol::GraphQLOperationType;

/// Structured authentication identity and authorization claims.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuthClaims {
    pub subject: Option<String>,
    pub tenant_id: Option<String>,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
    #[serde(default)]
    pub raw_claims: serde_json::Value,
}

impl AuthClaims {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: Some(subject.into()),
            tenant_id: None,
            roles: Vec::new(),
            permissions: Vec::new(),
            raw_claims: serde_json::Value::Null,
        }
    }

    pub fn with_roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn with_permissions(mut self, permissions: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.permissions = permissions.into_iter().map(Into::into).collect();
        self
    }
}

/// Execution context passed through RequestInterceptor and ResponseInterceptor pipelines.
#[derive(Debug, Clone)]
pub struct InterceptorContext {
    pub request_id: Uuid,
    pub hlc: HlcTimestamp,
    pub operation_name: Option<String>,
    pub operation_type: Option<GraphQLOperationType>,
    pub json_body: Option<serde_json::Value>,
    pub metadata: HashMap<String, String>,
    pub extensions: HashMap<String, serde_json::Value>,
    pub duration_ms: u64,
    pub claims: Option<AuthClaims>,
}

impl InterceptorContext {
    pub fn new(request_id: Uuid, hlc: HlcTimestamp) -> Self {
        Self {
            request_id,
            hlc,
            operation_name: None,
            operation_type: None,
            json_body: None,
            metadata: HashMap::new(),
            extensions: HashMap::new(),
            duration_ms: 0,
            claims: None,
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

    pub fn with_claims(mut self, claims: AuthClaims) -> Self {
        self.claims = Some(claims);
        self
    }

    pub fn is_authenticated(&self) -> bool {
        self.claims.as_ref().map(|c| c.subject.is_some()).unwrap_or(false)
    }

    pub fn has_role(&self, role: &str) -> bool {
        if let Some(ref c) = self.claims {
            c.roles.iter().any(|r| r == role || r == "*")
        } else {
            false
        }
    }

    pub fn has_any_role(&self, roles: &[&str]) -> bool {
        if let Some(ref c) = self.claims {
            c.roles.iter().any(|r| r == "*" || roles.contains(&r.as_str()))
        } else {
            false
        }
    }

    pub fn has_permission(&self, perm: &str) -> bool {
        if let Some(ref c) = self.claims {
            c.permissions.iter().any(|p| p == perm || p == "*")
                || c.roles.iter().any(|r| r == "admin" || r == "*")
        } else {
            false
        }
    }
}

/// Verdict returned by an interceptor.
/// Allows pure pass-through, defensive rejection, active payload transformation, or non-destructive observation/auditing.
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
    /// The request/response passed without modification, but an audit/observation rule was triggered.
    Audit {
        rule_name: String,
        tag: Option<String>,
        reason: String,
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
        crate::protocol::GraphQLErrorResponse::from(self).to_json_string()
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
