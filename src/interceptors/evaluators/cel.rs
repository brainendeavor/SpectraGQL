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
