use serde::{Deserialize, Serialize};

/// Source location within a GraphQL document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphQLErrorLocation {
    pub line: usize,
    pub column: usize,
}

/// Standard GraphQL error object matching the official GraphQL specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphQLError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<GraphQLErrorLocation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Standard GraphQL response envelope for error scenarios:
/// `{"errors": [...]}`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphQLErrorResponse {
    pub errors: Vec<GraphQLError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl GraphQLError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            locations: None,
            path: None,
            extensions: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        let mut extensions = self.extensions.unwrap_or_default();
        extensions.insert("code".to_string(), serde_json::Value::String(code.into()));
        self.extensions = Some(extensions);
        self
    }

    pub fn with_extension(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        let mut extensions = self.extensions.unwrap_or_default();
        extensions.insert(key.into(), value);
        self.extensions = Some(extensions);
        self
    }
}

impl GraphQLErrorResponse {
    pub fn single(error: GraphQLError) -> Self {
        Self {
            errors: vec![error],
            data: None,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::single(GraphQLError::new(message).with_code(code))
    }

    /// Constructs a standardized 409 Conflict GraphQL error response for in-flight mutations.
    pub fn conflict(key: Option<&str>, conflict_hlc: crate::clock::HlcTimestamp) -> Self {
        let msg = match key {
            Some(k) => format!("A mutation with idempotency key '{}' is currently in flight", k),
            None => "A mutation with idempotency key is currently in flight".to_string(),
        };
        let err = GraphQLError::new(msg)
            .with_code("CONFLICT")
            .with_extension("hlc", serde_json::Value::String(conflict_hlc.to_compact_string()));
        Self::single(err)
    }

    /// Serializes the error envelope into a compact JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| r#"{"errors":[{"message":"Internal error"}]}"#.to_string())
    }
}

impl From<&crate::guards::GuardRejection> for GraphQLErrorResponse {
    fn from(rejection: &crate::guards::GuardRejection) -> Self {
        let mut err = GraphQLError::new(&rejection.message).with_code(&rejection.code);
        if let Some(details) = &rejection.details {
            err = err.with_extension("details", details.clone());
        }
        GraphQLErrorResponse::single(err)
    }
}

impl From<crate::guards::GuardRejection> for GraphQLErrorResponse {
    fn from(rejection: crate::guards::GuardRejection) -> Self {
        GraphQLErrorResponse::from(&rejection)
    }
}
