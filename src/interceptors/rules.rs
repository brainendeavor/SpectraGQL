use std::collections::HashMap;
use std::sync::Arc;
use crate::interceptors::context::RuleEvaluationError;

/// Abstract rule evaluation port.
/// Allows native Rust closures, expression evaluators, or future Wasmtime plugins
/// to evaluate criteria against arbitrary JSON payloads without modifying the pipeline.
pub trait RuleEvaluator: Send + Sync {
    fn evaluate(&self, rule_name: &str, input: &serde_json::Value) -> Result<bool, RuleEvaluationError>;
}

/// Predicate function type for native rule evaluation.
pub type RulePredicate = Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>;

/// Native Rust implementation of RuleEvaluator using in-memory predicate registry.
#[derive(Default, Clone)]
pub struct NativeRuleEvaluator {
    rules: HashMap<String, RulePredicate>,
}

impl NativeRuleEvaluator {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }

    pub fn register<F>(&mut self, name: impl Into<String>, predicate: F) -> &mut Self
    where
        F: Fn(&serde_json::Value) -> bool + Send + Sync + 'static,
    {
        self.rules.insert(name.into(), Arc::new(predicate));
        self
    }

    pub fn unregister(&mut self, name: &str) -> Option<RulePredicate> {
        self.rules.remove(name)
    }

    pub fn has_rule(&self, name: &str) -> bool {
        self.rules.contains_key(name)
    }
}

impl RuleEvaluator for NativeRuleEvaluator {
    fn evaluate(&self, rule_name: &str, input: &serde_json::Value) -> Result<bool, RuleEvaluationError> {
        match self.rules.get(rule_name) {
            Some(pred) => Ok(pred(input)),
            None => Err(RuleEvaluationError::NotFound(rule_name.to_string())),
        }
    }
}
