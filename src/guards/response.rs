use crate::guards::context::{GuardContext, GuardRejection, GuardVerdict};
use crate::guards::rules::RuleEvaluator;
use std::sync::Arc;

/// Outbound contract and leakage protection port.
/// Inspects upstream responses before returning them to client consumers,
/// preventing accidental data leaks, PII breaches, or invalid structures.
pub trait ResponseGuard: Send + Sync {
    fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection>;
}

/// Scans outbound response bodies for forbidden sensitive tokens or rules.
#[derive(Clone)]
pub struct SensitiveDataResponseGuard {
    evaluator: Option<Arc<dyn RuleEvaluator>>,
    forbidden_tokens: Vec<String>,
}

impl SensitiveDataResponseGuard {
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

    pub fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        <Self as ResponseGuard>::guard_response(self, ctx, parts, body)
    }
}

impl Default for SensitiveDataResponseGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseGuard for SensitiveDataResponseGuard {
    fn guard_response(
        &self,
        _ctx: &GuardContext,
        _parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        if let Ok(body_str) = std::str::from_utf8(body) {
            for token in &self.forbidden_tokens {
                if body_str.contains(token) {
                    return Err(GuardRejection::new(
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
                            return Err(GuardRejection::new(
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
        Ok(GuardVerdict::Pass)
    }
}

/// Pipeline composing multiple ResponseGuards sequentially.
/// Short-circuits immediately on the first GuardRejection.
#[derive(Default)]
pub struct ResponseGuardPipeline {
    guards: Vec<Box<dyn ResponseGuard>>,
}

impl ResponseGuardPipeline {
    pub fn new() -> Self {
        Self { guards: Vec::new() }
    }

    pub fn with_guard<G: ResponseGuard + 'static>(mut self, guard: G) -> Self {
        self.guards.push(Box::new(guard));
        self
    }

    pub fn guard_response(
        &self,
        ctx: &GuardContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> Result<GuardVerdict, GuardRejection> {
        let mut overall_verdict = GuardVerdict::Pass;
        for guard in &self.guards {
            match guard.guard_response(ctx, parts, body)? {
                GuardVerdict::Pass => {}
                GuardVerdict::Mutated => {
                    overall_verdict = GuardVerdict::Mutated;
                }
            }
        }
        Ok(overall_verdict)
    }
}
