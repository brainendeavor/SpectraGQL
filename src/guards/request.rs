use crate::guards::context::{GuardContext, GuardRejection, GuardVerdict};

/// Inbound contract enforcement port.
/// Protects upstream services and event streams from malformed syntax,
/// unauthorized payloads, or non-compliant headers at the network edge.
pub trait RequestGuard: Send + Sync {
    fn guard_request(
        &self,
        ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, GuardRejection>;
}

/// Enforces valid GraphQL syntax and populates GuardContext with parsed operation details.
#[derive(Debug, Default, Clone)]
pub struct GraphQLSyntaxGuard;

impl RequestGuard for GraphQLSyntaxGuard {
    fn guard_request(
        &self,
        ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, GuardRejection> {
        let path = parts.uri.path();
        // Only enforce for GraphQL endpoints
        if !path.contains("graphql") && !path.contains("gql") {
            return Ok(GuardVerdict::Pass);
        }

        // Must be non-empty
        if body.trim().is_empty() {
            return Err(GuardRejection::new(
                http::StatusCode::BAD_REQUEST,
                "GRAPHQL_PARSE_FAILED",
                "Empty GraphQL request body",
            ));
        }

        // Parse outer JSON envelope { "query": "..." }
        let parsed_json: serde_json::Value = serde_json::from_str(body).map_err(|e| {
            GuardRejection::new(
                http::StatusCode::BAD_REQUEST,
                "GRAPHQL_PARSE_FAILED",
                format!("Invalid JSON request body: {}", e),
            )
        })?;

        let query = parsed_json
            .get("query")
            .and_then(|q| q.as_str())
            .ok_or_else(|| {
                GuardRejection::new(
                    http::StatusCode::BAD_REQUEST,
                    "GRAPHQL_PARSE_FAILED",
                    "Missing 'query' field in GraphQL request body",
                )
            })?;

        let op_name_requested = parsed_json
            .get("operationName")
            .and_then(|o| o.as_str());

        let op = crate::payload::gql_parser::parse_graphql_operation_with_name(
            query,
            op_name_requested,
        )
        .map_err(|e| {
            GuardRejection::new(
                http::StatusCode::BAD_REQUEST,
                "GRAPHQL_SYNTAX_ERROR",
                format!("Failed to parse GraphQL query: {}", e),
            )
        })?;

        ctx.operation_name = op.operation_name;
        ctx.operation_type = Some(op.operation_type);

        Ok(GuardVerdict::Pass)
    }
}

impl GraphQLSyntaxGuard {
    pub fn guard_request(
        &self,
        ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, GuardRejection> {
        <Self as RequestGuard>::guard_request(self, ctx, parts, body)
    }
}

/// Validates mandatory HTTP headers (e.g. content-type application/json for POST).
#[derive(Debug, Clone)]
pub struct HeaderValidationGuard {
    pub require_json_content_type: bool,
}

impl Default for HeaderValidationGuard {
    fn default() -> Self {
        Self {
            require_json_content_type: true,
        }
    }
}

impl HeaderValidationGuard {
    pub fn guard_request(
        &self,
        ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, GuardRejection> {
        <Self as RequestGuard>::guard_request(self, ctx, parts, body)
    }
}

impl RequestGuard for HeaderValidationGuard {
    fn guard_request(
        &self,
        _ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        _body: &str,
    ) -> Result<GuardVerdict, GuardRejection> {
        if self.require_json_content_type && parts.method == http::Method::POST {
            let content_type = parts
                .headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !content_type.contains("application/json") {
                return Err(GuardRejection::new(
                    http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "INVALID_CONTENT_TYPE",
                    "Content-Type must be application/json",
                ));
            }
        }
        Ok(GuardVerdict::Pass)
    }
}

/// Pipeline composing multiple RequestGuards sequentially.
/// Short-circuits immediately on the first GuardRejection.
#[derive(Default)]
pub struct RequestGuardPipeline {
    guards: Vec<Box<dyn RequestGuard>>,
}

impl RequestGuardPipeline {
    pub fn new() -> Self {
        Self { guards: Vec::new() }
    }

    pub fn with_guard<G: RequestGuard + 'static>(mut self, guard: G) -> Self {
        self.guards.push(Box::new(guard));
        self
    }

    pub fn guard_request(
        &self,
        ctx: &mut GuardContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> Result<GuardVerdict, GuardRejection> {
        let mut overall_verdict = GuardVerdict::Pass;
        for guard in &self.guards {
            match guard.guard_request(ctx, parts, body)? {
                GuardVerdict::Pass => {}
                GuardVerdict::Mutated => {
                    overall_verdict = GuardVerdict::Mutated;
                }
            }
        }
        Ok(overall_verdict)
    }
}
