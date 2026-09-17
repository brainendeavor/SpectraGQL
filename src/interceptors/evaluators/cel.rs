use std::collections::HashMap;
use std::sync::Arc;
use cel::{Context, Program, Value};
use crate::interceptors::context::RuleEvaluationError;
use crate::interceptors::rules::RuleEvaluator;

/// Converts a `serde_json::Value` into an equivalent `cel::Value`.
pub fn json_to_cel(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(Arc::new(s.clone())),
        serde_json::Value::Array(arr) => {
            let list: Vec<Value> = arr.iter().map(json_to_cel).collect();
            Value::List(Arc::new(list))
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(cel::objects::Key::String(Arc::new(k.clone())), json_to_cel(v));
            }
            Value::Map(cel::objects::Map {
                map: Arc::new(map),
            })
        }
    }
}

/// Declarative RuleEvaluator powered by Google Common Expression Language (CEL).
/// Expressions are compiled once to ASTs and evaluated in sub-microsecond time.
#[derive(Clone, Default)]
pub struct CelRuleEvaluator {
    programs: HashMap<String, Arc<Program>>,
}

impl CelRuleEvaluator {
    pub fn new() -> Self {
        Self {
            programs: HashMap::new(),
        }
    }

    /// Compiles and registers a CEL expression under a given rule name.
    pub fn register(&mut self, name: impl Into<String>, expression: &str) -> Result<&mut Self, String> {
        let program = Program::compile(expression)
            .map_err(|e| format!("Failed to compile CEL expression '{}': {:?}", expression, e))?;
        self.programs.insert(name.into(), Arc::new(program));
        Ok(self)
    }

    pub fn has_rule(&self, name: &str) -> bool {
        self.programs.contains_key(name)
    }
}

impl RuleEvaluator for CelRuleEvaluator {
    fn evaluate(&self, rule_name: &str, input: &serde_json::Value) -> Result<bool, RuleEvaluationError> {
        let program = match self.programs.get(rule_name) {
            Some(p) => p,
            None => return Err(RuleEvaluationError::NotFound(rule_name.to_string())),
        };

        let mut ctx = Context::default();
        if let serde_json::Value::Object(map) = input {
            for (k, v) in map {
                ctx.add_variable(k.clone(), json_to_cel(v));
            }
        }
        ctx.add_variable("input", json_to_cel(input));

        match program.execute(&ctx) {
            Ok(Value::Bool(b)) => Ok(b),
            Ok(other) => Err(RuleEvaluationError::EvaluationFailed(format!(
                "CEL expression did not return a boolean, got: {:?}",
                other
            ))),
            Err(e) => Err(RuleEvaluationError::EvaluationFailed(format!(
                "CEL evaluation error: {:?}",
                e
            ))),
        }
    }
}

use crate::interceptors::context::{InterceptorContext, InterceptorRejection, InterceptorVerdict};
use crate::interceptors::request::RequestInterceptor;
use crate::interceptors::response::ResponseInterceptor;

/// Action taken when a CEL expression evaluates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CelAction {
    /// Invariant enforcement: expression must be true to pass; false rejects.
    #[default]
    Reject,
    /// Non-destructive audit trigger: expression true flags/audits without modifying payload.
    Audit,
}

impl CelAction {
    pub fn from_str_opt(s: Option<&str>) -> Self {
        match s.map(|x| x.to_ascii_lowercase()).as_deref() {
            Some("audit") | Some("flag") => CelAction::Audit,
            _ => CelAction::Reject,
        }
    }
}

/// Request interceptor that evaluates a declarative CEL expression against the request context.
/// In Reject mode: if true, passes; if false, rejects.
/// In Audit mode: if true, flags audit without rejecting.
#[derive(Clone)]
pub struct CelRequestInterceptor {
    name: String,
    program: Arc<Program>,
    status_code: http::StatusCode,
    rejection_code: String,
    rejection_message: String,
    action: CelAction,
    tag: Option<String>,
}

impl CelRequestInterceptor {
    pub fn new(
        expression: &str,
        status_code: Option<http::StatusCode>,
        rejection_code: Option<impl Into<String>>,
        rejection_message: Option<impl Into<String>>,
    ) -> Result<Self, String> {
        Self::with_options(
            "cel_request",
            expression,
            status_code,
            rejection_code,
            rejection_message,
            CelAction::Reject,
            None::<String>,
        )
    }

    pub fn with_options(
        name: impl Into<String>,
        expression: &str,
        status_code: Option<http::StatusCode>,
        rejection_code: Option<impl Into<String>>,
        rejection_message: Option<impl Into<String>>,
        action: CelAction,
        tag: Option<impl Into<String>>,
    ) -> Result<Self, String> {
        let program = Program::compile(expression)
            .map_err(|e| format!("Failed to compile CEL expression '{}': {:?}", expression, e))?;
        Ok(Self {
            name: name.into(),
            program: Arc::new(program),
            status_code: status_code.unwrap_or(http::StatusCode::FORBIDDEN),
            rejection_code: rejection_code
                .map(|c| c.into())
                .unwrap_or_else(|| "POLICY_VIOLATION".to_string()),
            rejection_message: rejection_message
                .map(|m| m.into())
                .unwrap_or_else(|| "Request failed CEL validation policy".to_string()),
            action,
            tag: tag.map(|t| t.into()),
        })
    }
}

impl RequestInterceptor for CelRequestInterceptor {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict {
        let mut cel_ctx = Context::default();

        // 1. Headers map
        let mut headers_map = HashMap::new();
        for (k, v) in &parts.headers {
            if let Ok(s) = v.to_str() {
                headers_map.insert(
                    cel::objects::Key::String(Arc::new(k.as_str().to_string())),
                    Value::String(Arc::new(s.to_string())),
                );
            }
        }
        let headers_val = Value::Map(cel::objects::Map {
            map: Arc::new(headers_map),
        });
        cel_ctx.add_variable("headers", headers_val.clone());

        // 2. Request object: method, uri, path, headers
        let mut req_map = HashMap::new();
        req_map.insert(
            cel::objects::Key::String(Arc::new("method".to_string())),
            Value::String(Arc::new(parts.method.to_string())),
        );
        req_map.insert(
            cel::objects::Key::String(Arc::new("uri".to_string())),
            Value::String(Arc::new(parts.uri.to_string())),
        );
        req_map.insert(
            cel::objects::Key::String(Arc::new("path".to_string())),
            Value::String(Arc::new(parts.uri.path().to_string())),
        );
        req_map.insert(
            cel::objects::Key::String(Arc::new("headers".to_string())),
            headers_val,
        );
        cel_ctx.add_variable(
            "request",
            Value::Map(cel::objects::Map {
                map: Arc::new(req_map),
            }),
        );

        // 3. Operation name
        if let Some(name) = &ctx.operation_name {
            cel_ctx.add_variable("operation_name", Value::String(Arc::new(name.clone())));
        } else {
            cel_ctx.add_variable("operation_name", Value::Null);
        }

        // 4. Variables & query from parsed body if JSON
        if let Ok(json_body) = serde_json::from_str::<serde_json::Value>(body) {
            if let Some(vars) = json_body.get("variables") {
                cel_ctx.add_variable("variables", json_to_cel(vars));
            } else {
                cel_ctx.add_variable(
                    "variables",
                    Value::Map(cel::objects::Map {
                        map: Arc::new(HashMap::new()),
                    }),
                );
            }
            if let Some(q) = json_body.get("query").and_then(|q| q.as_str()) {
                cel_ctx.add_variable("query", Value::String(Arc::new(q.to_string())));
            }
            cel_ctx.add_variable("body", json_to_cel(&json_body));
        } else {
            cel_ctx.add_variable(
                "variables",
                Value::Map(cel::objects::Map {
                    map: Arc::new(HashMap::new()),
                }),
            );
            cel_ctx.add_variable("body", Value::String(Arc::new(body.to_string())));
        }
        cel_ctx.add_variable("raw_body", Value::String(Arc::new(body.to_string())));

        let eval_result = self.program.execute(&cel_ctx);
        match self.action {
            CelAction::Reject => match eval_result {
                Ok(Value::Bool(true)) => InterceptorVerdict::Pass,
                Ok(Value::Bool(false)) => InterceptorVerdict::Reject(InterceptorRejection::new(
                    self.status_code,
                    self.rejection_code.clone(),
                    self.rejection_message.clone(),
                )),
                Ok(other) => {
                    log::warn!("CelRequestInterceptor: expected boolean, got: {:?}", other);
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        self.status_code,
                        self.rejection_code.clone(),
                        format!("CEL expression returned non-boolean: {:?}", other),
                    ))
                }
                Err(e) => {
                    log::warn!("CelRequestInterceptor evaluation failed: {:?}", e);
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        self.status_code,
                        self.rejection_code.clone(),
                        format!("CEL evaluation error: {:?}", e),
                    ))
                }
            },
            CelAction::Audit => match eval_result {
                Ok(Value::Bool(true)) => InterceptorVerdict::Audit {
                    rule_name: self.name.clone(),
                    tag: self.tag.clone(),
                    reason: self.rejection_message.clone(),
                },
                Ok(Value::Bool(false)) => InterceptorVerdict::Pass,
                Ok(other) => {
                    log::warn!("CelRequestInterceptor (audit): expected boolean, got: {:?}", other);
                    InterceptorVerdict::Pass
                }
                Err(e) => {
                    log::warn!("CelRequestInterceptor (audit) evaluation failed: {:?}", e);
                    InterceptorVerdict::Pass
                }
            },
        }
    }
}

/// Response interceptor that evaluates a declarative CEL expression against the response context.
/// In Reject mode: if true, passes; if false, rejects (e.g. data leak detected).
/// In Audit mode: if true, flags audit without modifying response bytes.
#[derive(Clone)]
pub struct CelResponseInterceptor {
    name: String,
    program: Arc<Program>,
    status_code: http::StatusCode,
    rejection_code: String,
    rejection_message: String,
    action: CelAction,
    tag: Option<String>,
}

impl CelResponseInterceptor {
    pub fn new(
        expression: &str,
        status_code: Option<http::StatusCode>,
        rejection_code: Option<impl Into<String>>,
        rejection_message: Option<impl Into<String>>,
    ) -> Result<Self, String> {
        Self::with_options(
            "cel_response",
            expression,
            status_code,
            rejection_code,
            rejection_message,
            CelAction::Reject,
            None::<String>,
        )
    }

    pub fn with_options(
        name: impl Into<String>,
        expression: &str,
        status_code: Option<http::StatusCode>,
        rejection_code: Option<impl Into<String>>,
        rejection_message: Option<impl Into<String>>,
        action: CelAction,
        tag: Option<impl Into<String>>,
    ) -> Result<Self, String> {
        let program = Program::compile(expression)
            .map_err(|e| format!("Failed to compile CEL expression '{}': {:?}", expression, e))?;
        Ok(Self {
            name: name.into(),
            program: Arc::new(program),
            status_code: status_code.unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
            rejection_code: rejection_code
                .map(|c| c.into())
                .unwrap_or_else(|| "DATA_LEAK_PREVENTED".to_string()),
            rejection_message: rejection_message
                .map(|m| m.into())
                .unwrap_or_else(|| "Response failed outbound CEL policy check".to_string()),
            action,
            tag: tag.map(|t| t.into()),
        })
    }
}

impl ResponseInterceptor for CelResponseInterceptor {
    fn intercept_response(
        &self,
        ctx: &InterceptorContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        let mut cel_ctx = Context::default();

        // 1. Status code
        cel_ctx.add_variable("status", Value::UInt(parts.status.as_u16() as u64));

        // 2. Latency & context
        cel_ctx.add_variable("duration_ms", Value::UInt(ctx.duration_ms));
        cel_ctx.add_variable("request_id", Value::String(Arc::new(ctx.request_id.to_string())));
        if let Some(op) = &ctx.operation_name {
            cel_ctx.add_variable("operation_name", Value::String(Arc::new(op.clone())));
        } else {
            cel_ctx.add_variable("operation_name", Value::Null);
        }

        // 3. Headers
        let mut headers_map = HashMap::new();
        for (k, v) in &parts.headers {
            if let Ok(s) = v.to_str() {
                headers_map.insert(
                    cel::objects::Key::String(Arc::new(k.as_str().to_string())),
                    Value::String(Arc::new(s.to_string())),
                );
            }
        }
        cel_ctx.add_variable(
            "headers",
            Value::Map(cel::objects::Map {
                map: Arc::new(headers_map),
            }),
        );

        // 4. Response JSON fields: data, errors, etc.
        if let Ok(body_str) = std::str::from_utf8(body) {
            cel_ctx.add_variable("body", Value::String(Arc::new(body_str.to_string())));
            cel_ctx.add_variable("raw_body", Value::String(Arc::new(body_str.to_string())));
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(body_str) {
                if let serde_json::Value::Object(map) = &json_val {
                    for (k, v) in map {
                        cel_ctx.add_variable(k.clone(), json_to_cel(v));
                    }
                }
                cel_ctx.add_variable("response", json_to_cel(&json_val));
            } else {
                cel_ctx.add_variable("response", Value::String(Arc::new(body_str.to_string())));
            }
        }

        let eval_result = self.program.execute(&cel_ctx);
        match self.action {
            CelAction::Reject => match eval_result {
                Ok(Value::Bool(true)) => InterceptorVerdict::Pass,
                Ok(Value::Bool(false)) => InterceptorVerdict::Reject(InterceptorRejection::new(
                    self.status_code,
                    self.rejection_code.clone(),
                    self.rejection_message.clone(),
                )),
                Ok(other) => {
                    log::warn!("CelResponseInterceptor: expected boolean, got: {:?}", other);
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        self.status_code,
                        self.rejection_code.clone(),
                        format!("CEL expression returned non-boolean: {:?}", other),
                    ))
                }
                Err(e) => {
                    log::warn!("CelResponseInterceptor evaluation failed: {:?}", e);
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        self.status_code,
                        self.rejection_code.clone(),
                        format!("CEL evaluation error: {:?}", e),
                    ))
                }
            },
            CelAction::Audit => match eval_result {
                Ok(Value::Bool(true)) => InterceptorVerdict::Audit {
                    rule_name: self.name.clone(),
                    tag: self.tag.clone(),
                    reason: self.rejection_message.clone(),
                },
                Ok(Value::Bool(false)) => InterceptorVerdict::Pass,
                Ok(other) => {
                    log::warn!("CelResponseInterceptor (audit): expected boolean, got: {:?}", other);
                    InterceptorVerdict::Pass
                }
                Err(e) => {
                    log::warn!("CelResponseInterceptor (audit) evaluation failed: {:?}", e);
                    InterceptorVerdict::Pass
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::clock::HlcTimestamp;
    use crate::interceptors::response::ResponseInterceptor;
    use uuid::Uuid;

    #[test]
    fn test_cel_audit_action_response_triggers_audit() {
        let interceptor = CelResponseInterceptor::with_options(
            "slow_query_auditor",
            "duration_ms > 100",
            None,
            None::<String>,
            Some("Latency threshold exceeded"),
            CelAction::Audit,
            Some("SLOW_QUERY"),
        )
        .unwrap();

        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0));
        ctx.duration_ms = 250;

        let mut parts = http::response::Response::builder()
            .status(200)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let body = b"{\"data\":{\"user\":{\"id\":\"123\"}}}";

        let verdict = interceptor.intercept_response(&ctx, &mut parts, body);
        match verdict {
            InterceptorVerdict::Audit { rule_name, tag, reason } => {
                assert_eq!(rule_name, "slow_query_auditor");
                assert_eq!(tag.as_deref(), Some("SLOW_QUERY"));
                assert_eq!(reason, "Latency threshold exceeded");
            }
            other => panic!("Expected InterceptorVerdict::Audit, got: {:?}", other),
        }
    }

    #[test]
    fn test_cel_audit_action_response_pass_when_false() {
        let interceptor = CelResponseInterceptor::with_options(
            "slow_query_auditor",
            "duration_ms > 100",
            None,
            None::<String>,
            Some("Latency threshold exceeded"),
            CelAction::Audit,
            Some("SLOW_QUERY"),
        )
        .unwrap();

        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0));
        ctx.duration_ms = 45;

        let mut parts = http::response::Response::builder()
            .status(200)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let body = b"{\"data\":{\"user\":{\"id\":\"123\"}}}";

        let verdict = interceptor.intercept_response(&ctx, &mut parts, body);
        assert!(matches!(verdict, InterceptorVerdict::Pass));
    }

    #[test]
    fn test_cel_reject_action_response_blocks_when_false() {
        let interceptor = CelResponseInterceptor::with_options(
            "require_200",
            "status == 200",
            Some(http::StatusCode::BAD_GATEWAY),
            Some("UPSTREAM_NOT_OK"),
            Some("Upstream status must be 200"),
            CelAction::Reject,
            None::<String>,
        )
        .unwrap();

        let ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0));
        let mut parts = http::response::Response::builder()
            .status(500)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let body = b"internal error";

        let verdict = interceptor.intercept_response(&ctx, &mut parts, body);
        match verdict {
            InterceptorVerdict::Reject(rej) => {
                assert_eq!(rej.status_code, http::StatusCode::BAD_GATEWAY);
                assert_eq!(rej.code, "UPSTREAM_NOT_OK");
            }
            other => panic!("Expected InterceptorVerdict::Reject, got: {:?}", other),
        }
    }
}
