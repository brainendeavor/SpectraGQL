use crate::interceptors::context::{
    InterceptorContext, InterceptorRejection, InterceptorVerdict,
};

/// Inbound contract enforcement and transformation port.
/// Intercepts incoming requests at the network edge to validate syntax,
/// enforce policies, or mutate headers/body before upstream proxying or command dispatch.
pub trait RequestInterceptor: Send + Sync {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict;
}

/// Enforces valid GraphQL syntax and populates InterceptorContext with parsed operation details.
#[derive(Debug, Default, Clone)]
pub struct GraphQLSyntaxInterceptor;

impl RequestInterceptor for GraphQLSyntaxInterceptor {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict {
        let path = parts.uri.path();
        // Only enforce for GraphQL endpoints
        if !path.contains("graphql") && !path.contains("gql") {
            return InterceptorVerdict::Pass;
        }

        // Must be non-empty
        if body.trim().is_empty() {
            return InterceptorVerdict::Reject(InterceptorRejection::new(
                http::StatusCode::BAD_REQUEST,
                "GRAPHQL_PARSE_FAILED",
                "Empty GraphQL request body",
            ));
        }

        // Parse outer JSON envelope { "query": "..." }
        let parsed_json: serde_json::Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::BAD_REQUEST,
                    "GRAPHQL_PARSE_FAILED",
                    format!("Invalid JSON request body: {}", e),
                ));
            }
        };

        let query = match parsed_json.get("query").and_then(|q| q.as_str()) {
            Some(q) => q,
            None => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::BAD_REQUEST,
                    "GRAPHQL_PARSE_FAILED",
                    "Missing 'query' field in GraphQL request body",
                ));
            }
        };

        let op_name_requested = parsed_json
            .get("operationName")
            .and_then(|o| o.as_str());

        let op = match crate::protocol::parse_graphql_operation_with_name(
            query,
            op_name_requested,
        ) {
            Ok(op) => op,
            Err(e) => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::BAD_REQUEST,
                    "GRAPHQL_SYNTAX_ERROR",
                    format!("Failed to parse GraphQL query: {}", e),
                ));
            }
        };

        ctx.operation_name = op.operation_name;
        ctx.operation_type = Some(op.operation_type);

        InterceptorVerdict::Pass
    }
}

/// Validates mandatory HTTP headers (e.g. content-type application/json for POST).
#[derive(Debug, Clone)]
pub struct HeaderValidationInterceptor {
    pub require_json_content_type: bool,
}

impl Default for HeaderValidationInterceptor {
    fn default() -> Self {
        Self {
            require_json_content_type: true,
        }
    }
}

impl RequestInterceptor for HeaderValidationInterceptor {
    fn intercept_request(
        &self,
        _ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        _body: &str,
    ) -> InterceptorVerdict {
        if self.require_json_content_type && parts.method == http::Method::POST {
            let content_type = parts
                .headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !content_type.contains("application/json") {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "INVALID_CONTENT_TYPE",
                    "Content-Type must be application/json",
                ));
            }
        }
        InterceptorVerdict::Pass
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// Enforces authentication and token verification on deployment mutation operations.
#[derive(Debug, Clone)]
pub struct DeployAuthInterceptor {
    pub deploy_token: Option<String>,
}

impl DeployAuthInterceptor {
    pub fn new(deploy_token: Option<String>) -> Self {
        let deploy_token = deploy_token
            .or_else(|| std::env::var("SPECTRA_DEPLOY_TOKEN").ok())
            .or_else(|| std::env::var("SPECTRAGQL_DEPLOY_TOKEN").ok());
        Self { deploy_token }
    }
}

impl Default for DeployAuthInterceptor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl RequestInterceptor for DeployAuthInterceptor {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        _body: &str,
    ) -> InterceptorVerdict {
        let op_name = ctx.operation_name.as_deref().unwrap_or("").to_ascii_lowercase();
        let is_deploy_op = op_name.contains("deployfluxcell")
            || op_name.contains("activatefluxcell")
            || op_name.contains("removefluxcell");

        if !is_deploy_op {
            return InterceptorVerdict::Pass;
        }

        let expected = match &self.deploy_token {
            Some(token) if !token.trim().is_empty() => token,
            _ => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::UNAUTHORIZED,
                    "DEPLOY_UNAUTHORIZED",
                    "Deployment authorization token is not configured on the gateway (set SPECTRA_DEPLOY_TOKEN or deploy_token in spectra.toml)",
                ));
            }
        };

        let auth_header = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer ")))
            .or_else(|| {
                parts
                    .headers
                    .get("x-spectra-deploy-key")
                    .and_then(|v| v.to_str().ok())
            });

        match auth_header {
            Some(provided) if constant_time_eq(provided.trim().as_bytes(), expected.trim().as_bytes()) => {}
            _ => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::UNAUTHORIZED,
                    "DEPLOY_UNAUTHORIZED",
                    "Invalid or missing deployment authorization token",
                ));
            }
        }

        InterceptorVerdict::Pass
    }
}

use std::sync::Arc;

/// Pipeline composing multiple RequestInterceptors sequentially.
/// Short-circuits immediately on the first InterceptorRejection.
#[derive(Clone, Default)]
pub struct RequestInterceptorPipeline {
    interceptors: Vec<Arc<dyn RequestInterceptor>>,
}

impl RequestInterceptorPipeline {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn with_interceptor<I: RequestInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    pub fn with_arc_interceptor(mut self, interceptor: Arc<dyn RequestInterceptor>) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.interceptors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.interceptors.len()
    }

    pub fn extend(&mut self, other: &RequestInterceptorPipeline) {
        self.interceptors.extend(other.interceptors.iter().cloned());
    }

    pub fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict {
        for interceptor in &self.interceptors {
            match interceptor.intercept_request(ctx, parts, body) {
                InterceptorVerdict::Pass => {}
                other => return other,
            }
        }
        InterceptorVerdict::Pass
    }
}
