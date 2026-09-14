use serde_json::Value;
use std::collections::HashSet;

pub const DEFAULT_SENSITIVE_KEYS: &[&str] = &[
    "password",
    "pass",
    "token",
    "secret",
    "apikey",
    "api_key",
    "creditcard",
    "credit_card",
    "cvv",
    "cvc",
    "ssn",
    "pin",
    "auth",
    "authorization",
];

pub const REDACTED_PLACEHOLDER: &str = "[REDACTED]";

/// Argument Sanitizer and PII Redaction engine.
/// Recursively sanitizes JSON values and GraphQL variables before writing to event brokers.
#[derive(Clone, Debug)]
pub struct Sanitizer {
    sensitive_keys: HashSet<String>,
}

fn split_key_tokens(key: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut prev_is_lower = false;

    for ch in key.chars() {
        if ch == '_' || ch == '-' || ch == '.' || ch == ' ' {
            if !current.is_empty() {
                tokens.push(current.to_lowercase());
                current.clear();
            }
            prev_is_lower = false;
        } else if ch.is_uppercase() {
            if prev_is_lower && !current.is_empty() {
                tokens.push(current.to_lowercase());
                current.clear();
            }
            current.push(ch);
            prev_is_lower = false;
        } else {
            current.push(ch);
            prev_is_lower = true;
        }
    }
    if !current.is_empty() {
        tokens.push(current.to_lowercase());
    }
    tokens
}

impl Sanitizer {
    pub fn new(custom_keys: Option<Vec<String>>) -> Self {
        let mut sensitive_keys = HashSet::new();
        for key in DEFAULT_SENSITIVE_KEYS {
            sensitive_keys.insert(key.to_lowercase());
        }
        if let Some(extras) = custom_keys {
            for key in extras {
                sensitive_keys.insert(key.to_lowercase());
            }
        }
        Sanitizer { sensitive_keys }
    }

    pub fn is_sensitive_key(&self, key: &str) -> bool {
        let normalized = key.to_lowercase().replace(['-', '_'], "");
        // 1. Exact normalized match (e.g. "creditcard" or "apikey")
        for k in &self.sensitive_keys {
            let norm_k = k.replace(['-', '_'], "");
            if normalized == norm_k {
                return true;
            }
        }

        // 2. Tokenized word-boundary match (e.g. "user_password", "accessToken", "admin_pin")
        let tokens = split_key_tokens(key);
        for token in &tokens {
            for k in &self.sensitive_keys {
                let norm_k = k.replace(['-', '_'], "");
                if token == &norm_k {
                    return true;
                }
            }
        }

        // 3. Multi-token compound match (e.g. tokens ["credit", "card"] -> "creditcard" matches "credit_card")
        let max_window = 4.min(tokens.len());
        for i in 0..tokens.len() {
            for j in (i + 1)..=(i + max_window).min(tokens.len()) {
                let slice_joined = tokens[i..j].concat();
                for k in &self.sensitive_keys {
                    let norm_k = k.replace(['-', '_'], "");
                    if slice_joined == norm_k {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Recursively masks sensitive fields within a `serde_json::Value`.
    pub fn sanitize_value(&self, value: &mut Value) {
        self.sanitize_value_depth(value, 0);
    }

    fn sanitize_value_depth(&self, value: &mut Value, depth: usize) {
        if depth > 64 {
            // Guard against stack overflow recursion on adversarial deeply-nested JSON payloads
            return;
        }
        match value {
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    if self.is_sensitive_key(k) {
                        if let Value::String(s) = v {
                            if s.to_lowercase().starts_with("bearer ") {
                                *v = Value::String(format!("Bearer {}", REDACTED_PLACEHOLDER));
                                continue;
                            }
                        }
                        *v = Value::String(REDACTED_PLACEHOLDER.to_string());
                    } else if k.to_lowercase() == "query" {
                        if let Value::String(q) = v {
                            *q = self.sanitize_gql_query_str(q);
                        }
                    } else {
                        self.sanitize_value_depth(v, depth + 1);
                    }
                }
            }
            Value::Array(arr) => {
                for item in arr.iter_mut() {
                    self.sanitize_value_depth(item, depth + 1);
                }
            }
            Value::String(s) => {
                if s.to_lowercase().starts_with("bearer ") {
                    *s = format!("Bearer {}", REDACTED_PLACEHOLDER);
                }
            }
            _ => {}
        }
    }

    /// Sanitizes inline literal arguments in a GraphQL query string (e.g. `password: "secret"`).
    pub fn sanitize_gql_query_str(&self, query: &str) -> String {
        static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let re = PATTERN.get_or_init(|| {
            regex::Regex::new(
                r#"(?i)(password|pass|token|secret|apikey|api_key|creditcard|credit_card|cvv|cvc|ssn|pin)\s*:\s*"[^"]*""#,
            )
            .unwrap()
        });
        let res = re.replace_all(query, |caps: &regex::Captures| {
            let key = &caps[1];
            format!("{}: \"{}\"", key, REDACTED_PLACEHOLDER)
        });

        // Also sanitize bearer tokens within string literals if present
        static BEARER_PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let bearer_re = BEARER_PATTERN.get_or_init(|| {
            regex::Regex::new(r#"(?i)Bearer\s+[A-Za-z0-9\-_.~+/=]+"#).unwrap()
        });
        bearer_re
            .replace_all(&res, format!("Bearer {}", REDACTED_PLACEHOLDER))
            .to_string()
    }

    /// Sanitizes a raw JSON string. If the string is valid JSON, sensitive fields are redacted.
    /// If not valid JSON, treats it as a query string and masks inline arguments.
    pub fn sanitize_json_str(&self, raw: &str) -> String {
        if let Ok(mut val) = serde_json::from_str::<Value>(raw) {
            self.sanitize_value(&mut val);
            serde_json::to_string(&val).unwrap_or_else(|_| raw.to_string())
        } else {
            self.sanitize_gql_query_str(raw)
        }
    }

    /// Global singleton instance with default sensitive keys.
    pub fn default_sanitizer() -> &'static Sanitizer {
        static SANITIZER: std::sync::OnceLock<Sanitizer> = std::sync::OnceLock::new();
        SANITIZER.get_or_init(|| Sanitizer::new(None))
    }
}

impl Default for Sanitizer {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitizer_nested_fields() {
        let sanitizer = Sanitizer::default();
        let raw = r#"{
            "user": {
                "username": "alice",
                "password": "super_secret_password_123",
                "email": "alice@example.com"
            },
            "token": "jwt_token_abc"
        }"#;

        let sanitized = sanitizer.sanitize_json_str(raw);
        assert!(!sanitized.contains("super_secret_password_123"));
        assert!(!sanitized.contains("jwt_token_abc"));
        assert!(sanitized.contains(r#""password":"[REDACTED]""#));
        assert!(sanitized.contains(r#""token":"[REDACTED]""#));
        assert!(sanitized.contains(r#""username":"alice""#));
        assert!(sanitized.contains(r#""email":"alice@example.com""#));
    }

    #[test]
    fn test_sanitizer_array_of_objects() {
        let sanitizer = Sanitizer::default();
        let raw = r#"{
            "paymentMethods": [
                { "type": "card", "cardNumber": "4111222233334444", "cvv": "123" },
                { "type": "card", "cardNumber": "5555666677778888", "cvv": "456" }
            ]
        }"#;

        let sanitized = sanitizer.sanitize_json_str(raw);
        assert!(!sanitized.contains("123"));
        assert!(!sanitized.contains("456"));
        assert!(sanitized.contains(r#""cvv":"[REDACTED]""#));
        assert!(sanitized.contains(r#""type":"card""#));
    }

    #[test]
    fn test_sanitizer_bearer_token() {
        let sanitizer = Sanitizer::default();
        let raw = r#"{"authHeader": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}"#;
        let sanitized = sanitizer.sanitize_json_str(raw);
        assert!(sanitized.contains(r#""authHeader":"Bearer [REDACTED]""#));
    }

    #[test]
    fn test_sanitizer_custom_keys() {
        let sanitizer = Sanitizer::new(Some(vec!["biometric_hash".to_string()]));
        let raw = r#"{"name": "bob", "biometric_hash": "a8f59c02"}"#;
        let sanitized = sanitizer.sanitize_json_str(raw);
        assert!(sanitized.contains(r#""biometric_hash":"[REDACTED]""#));
    }

    #[test]
    fn test_sanitizer_query_string_syntax_validity() {
        let sanitizer = Sanitizer::default();
        let query = r#"mutation LoginUser { login(email: "alice@example.com", password: "supersecret123") { token } }"#;
        let sanitized = sanitizer.sanitize_gql_query_str(query);

        // Verify that the closing quote is intact and valid GraphQL
        assert!(sanitized.contains(r#"password: "[REDACTED]""#));
        assert!(!sanitized.contains("supersecret123"));

        // Verify the sanitized query can be parsed without syntax errors
        let parser = apollo_parser::Parser::new(&sanitized);
        let ast = parser.parse();
        assert!(ast.errors().next().is_none(), "Sanitized query has syntax errors: {:?}", ast.errors().collect::<Vec<_>>());
    }

    #[test]
    fn test_sanitizer_adversarial_deeply_nested_json() {
        let sanitizer = Sanitizer::default();
        // Construct 100 levels of nested objects
        let mut deeply_nested = serde_json::json!({ "password": "leaked_secret" });
        for _ in 0..100 {
            deeply_nested = serde_json::json!({ "nested": deeply_nested });
        }

        // Must not stack overflow
        sanitizer.sanitize_value(&mut deeply_nested);
    }

    #[test]
    fn test_sanitizer_long_token_key_dos() {
        let sanitizer = Sanitizer::default();
        // Construct a key with 500 underscore-separated tokens
        let attack_key = (0..500).map(|i| format!("part{}", i)).collect::<Vec<_>>().join("_");
        let start = std::time::Instant::now();
        let _ = sanitizer.is_sensitive_key(&attack_key);
        let elapsed = start.elapsed();
        // Should complete in well under 10ms with window capping
        assert!(elapsed < std::time::Duration::from_millis(50), "is_sensitive_key took too long: {:?}", elapsed);
    }
}
