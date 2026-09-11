use crate::interceptors::context::{
    GuardContext, GuardRejection, GuardVerdict, InterceptorVerdict,
};
use crate::interceptors::rules::RuleEvaluator;
use std::sync::Arc;

/// Outbound contract, leakage protection, and transformation port.
/// Inspects upstream responses before returning them to client consumers,
/// allowing defensive rejection (PII / contract checks) or active transformation (anonymization, payload shaping).
pub trait ResponseInterceptor: Send + Sync {
    fn intercept_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict;

    /// Compatibility helper for legacy response guard callers.
    fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        match self.intercept_response(ctx, parts, body) {
            InterceptorVerdict::Pass => Ok(GuardVerdict::Pass),
            InterceptorVerdict::Transform { .. } => Ok(GuardVerdict::Mutated),
            InterceptorVerdict::Reject(rejection) => Err(rejection),
        }
    }
}

/// Backwards compatibility alias for ResponseInterceptor.
pub use ResponseInterceptor as ResponseGuard;

/// Scans outbound response bodies for forbidden sensitive tokens or rules.
#[derive(Clone)]
pub struct SensitiveDataResponseInterceptor {
    evaluator: Option<Arc<dyn RuleEvaluator>>,
    forbidden_tokens: Vec<String>,
}

pub use SensitiveDataResponseInterceptor as SensitiveDataResponseGuard;

impl SensitiveDataResponseInterceptor {
    pub fn new() -> Self {
        Self {
            evaluator: None,
            forbidden_tokens: Vec::new(),
        }
    }

    pub fn with_evaluator(mut self, evaluator: Arc<dyn RuleEvaluator>) -> Self {
        self.evaluator = Some(evaluator);
        self
    }

    pub fn with_forbidden_tokens(mut self, tokens: Vec<String>) -> Self {
        self.forbidden_tokens = tokens;
        self
    }

    pub fn intercept_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        <Self as ResponseInterceptor>::intercept_response(self, ctx, parts, body)
    }

    pub fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        <Self as ResponseInterceptor>::guard_response(self, ctx, parts, body)
    }
}

impl Default for SensitiveDataResponseInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseInterceptor for SensitiveDataResponseInterceptor {
    fn intercept_response(
        &self,
        _ctx: &GuardContext,
        _parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        if let Ok(body_str) = std::str::from_utf8(body) {
            for token in &self.forbidden_tokens {
                if body_str.contains(token) {
                    return InterceptorVerdict::Reject(GuardRejection::new(
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        "DATA_LEAK_PREVENTED",
                        format!("Response contains forbidden token: {}", token),
                    ));
                }
            }

            if let Some(evaluator) = &self.evaluator {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(body_str) {
                    match evaluator.evaluate("sensitive_data_check", &json_val) {
                        Ok(true) => {
                            return InterceptorVerdict::Reject(GuardRejection::new(
                                http::StatusCode::INTERNAL_SERVER_ERROR,
                                "DATA_LEAK_PREVENTED",
                                "Response failed sensitive data evaluation rule",
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }
        InterceptorVerdict::Pass
    }
}

/// Pipeline composing multiple ResponseInterceptors sequentially.
/// Short-circuits immediately on the first GuardRejection.
#[derive(Default)]
pub struct ResponseInterceptorPipeline {
    interceptors: Vec<Box<dyn ResponseInterceptor>>,
}

pub use ResponseInterceptorPipeline as ResponseGuardPipeline;

impl ResponseInterceptorPipeline {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn with_interceptor<I: ResponseInterceptor + 'static>(mut self, interceptor: I) -> Self {
        self.interceptors.push(Box::new(interceptor));
        self
    }

    pub fn with_guard<G: ResponseInterceptor + 'static>(self, guard: G) -> Self {
        self.with_interceptor(guard)
    }

    pub fn intercept_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        let mut current_body: Option<Vec<u8>> = None;
        let mut current_headers: Option<http::HeaderMap> = None;

        for interceptor in &self.interceptors {
            let effective_body = current_body.as_deref().unwrap_or(body);
            match interceptor.intercept_response(ctx, parts, effective_body) {
                InterceptorVerdict::Pass => {}
                InterceptorVerdict::Reject(rejection) => return InterceptorVerdict::Reject(rejection),
                InterceptorVerdict::Transform { headers, body } => {
                    if let Some(h) = headers {
                        current_headers = Some(h);
                    }
                    if let Some(b) = body {
                        current_body = Some(b);
                    }
                }
            }
        }

        if current_body.is_some() || current_headers.is_some() {
            InterceptorVerdict::Transform {
                headers: current_headers,
                body: current_body,
            }
        } else {
            InterceptorVerdict::Pass
        }
    }

    pub fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        match self.intercept_response(ctx, parts, body) {
            InterceptorVerdict::Pass => Ok(GuardVerdict::Pass),
            InterceptorVerdict::Transform { .. } => Ok(GuardVerdict::Mutated),
            InterceptorVerdict::Reject(rejection) => Err(rejection),
        }
    }
}
