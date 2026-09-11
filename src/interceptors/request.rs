use crate::interceptors::context::{
    GuardVerdict, InterceptorContext, InterceptorRejection, InterceptorVerdict,
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

    /// Compatibility helper for legacy request guard callers.
    fn guard_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, InterceptorRejection> {
        match self.intercept_request(ctx, parts, body) {
            InterceptorVerdict::Pass => Ok(GuardVerdict::Pass),
            InterceptorVerdict::Transform { .. } => Ok(GuardVerdict::Mutated),
            InterceptorVerdict::Reject(rejection) => Err(rejection),
        }
    }
}

/// Backwards compatibility alias for RequestInterceptor.
pub use RequestInterceptor as RequestGuard;

/// Enforces valid GraphQL syntax and populates InterceptorContext with parsed operation details.
#[derive(Debug, Default, Clone)]
pub struct GraphQLSyntaxInterceptor;

pub use GraphQLSyntaxInterceptor as GraphQLSyntaxGuard;

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

        let op = match crate::payload::gql_parser::parse_graphql_operation_with_name(
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

impl GraphQLSyntaxInterceptor {
    pub fn guard_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, InterceptorRejection> {
        <Self as RequestInterceptor>::guard_request(self, ctx, parts, body)
    }
}

/// Validates mandatory HTTP headers (e.g. content-type application/json for POST).
#[derive(Debug, Clone)]
pub struct HeaderValidationInterceptor {
    pub require_json_content_type: bool,
}

pub use HeaderValidationInterceptor as HeaderValidationGuard;

impl Default for HeaderValidationInterceptor {
    fn default() -> Self {
        Self {
            require_json_content_type: true,
        }
    }
}

impl HeaderValidationInterceptor {
    pub fn guard_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, InterceptorRejection> {
        <Self as RequestInterceptor>::guard_request(self, ctx, parts, body)
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

/// Pipeline composing multiple RequestInterceptors sequentially.
/// Short-circuits immediately on the first InterceptorRejection.
#[derive(Default)]
pub struct RequestInterceptorPipeline {
    interceptors: Vec<Box<dyn RequestInterceptor>>,
}

pub use RequestInterceptorPipeline as RequestGuardPipeline;

impl RequestInterceptorPipeline {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn with_interceptor<I: RequestInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.interceptors.push(Box::new(interceptor));
        self
    }

    pub fn with_guard<G: RequestInterceptor + 'static>(self, guard: G) -> Self {
        self.with_interceptor(guard)
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

    pub fn guard_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, InterceptorRejection> {
        match self.intercept_request(ctx, parts, body) {
            InterceptorVerdict::Pass => Ok(GuardVerdict::Pass),
            InterceptorVerdict::Transform { .. } => Ok(GuardVerdict::Mutated),
            InterceptorVerdict::Reject(rejection) => Err(rejection),
        }
    }
}
