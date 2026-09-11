use serde_json::json;
use spectragql::ratify::Sanitizer;

#[test]
fn test_sanitizer_does_not_redact_benign_substrings() {
    let sanitizer = Sanitizer::new(None);

    // Words that contain "pass", "pin", "auth" as substrings but are NOT sensitive keys
    assert!(!sanitizer.is_sensitive_key("passenger"));
    assert!(!sanitizer.is_sensitive_key("compass"));
    assert!(!sanitizer.is_sensitive_key("author"));
    assert!(!sanitizer.is_sensitive_key("authentic"));
    assert!(!sanitizer.is_sensitive_key("shipping"));
    assert!(!sanitizer.is_sensitive_key("spinning"));
    assert!(!sanitizer.is_sensitive_key("spine"));

    let mut payload = json!({
        "passenger": "Alice Johnson",
        "author": "George Orwell",
        "shipping": {
            "address": "123 Main St",
            "method": "Overnight"
        }
    });

    sanitizer.sanitize_value(&mut payload);

    assert_eq!(payload["passenger"], "Alice Johnson");
    assert_eq!(payload["author"], "George Orwell");
    assert_eq!(payload["shipping"]["address"], "123 Main St");
    assert_eq!(payload["shipping"]["method"], "Overnight");
}

#[test]
fn test_sanitizer_redacts_compound_sensitive_keys() {
    let sanitizer = Sanitizer::new(None);

    assert!(sanitizer.is_sensitive_key("password"));
    assert!(sanitizer.is_sensitive_key("user_password"));
    assert!(sanitizer.is_sensitive_key("userPassword"));
    assert!(sanitizer.is_sensitive_key("admin-password"));
    assert!(sanitizer.is_sensitive_key("access_token"));
    assert!(sanitizer.is_sensitive_key("accessToken"));
    assert!(sanitizer.is_sensitive_key("api_key"));
    assert!(sanitizer.is_sensitive_key("apiKey"));
    assert!(sanitizer.is_sensitive_key("credit_card"));
    assert!(sanitizer.is_sensitive_key("creditCard"));
    assert!(sanitizer.is_sensitive_key("admin_pin"));
    assert!(sanitizer.is_sensitive_key("pin_code"));

    let mut payload = json!({
        "user_password": "super-secret-pw",
        "accessToken": "ey1234567890",
        "profile": {
            "creditCard": "4111-2222-3333-4444",
            "pinCode": "1234"
        }
    });

    sanitizer.sanitize_value(&mut payload);

    assert_eq!(payload["user_password"], "[REDACTED]");
    assert_eq!(payload["accessToken"], "[REDACTED]");
    assert_eq!(payload["profile"]["creditCard"], "[REDACTED]");
    assert_eq!(payload["profile"]["pinCode"], "[REDACTED]");
}

#[test]
fn test_sanitizer_bearer_token_variations() {
    let sanitizer = Sanitizer::new(None);

    let mut payload = json!({
        "auth_header": "Bearer secret-token-value-xyz",
        "lower_bearer": "bearer secret-token-value-abc",
        "nested": [
            { "token_str": "Bearer my-jwt-token" }
        ]
    });

    sanitizer.sanitize_value(&mut payload);

    assert_eq!(payload["auth_header"], "Bearer [REDACTED]");
    assert_eq!(payload["lower_bearer"], "Bearer [REDACTED]");
    assert_eq!(payload["nested"][0]["token_str"], "Bearer [REDACTED]");
}

#[test]
fn test_sanitizer_deeply_nested_arrays_and_custom_keys() {
    let sanitizer = Sanitizer::new(Some(vec!["tax_id".to_string(), "biometric".to_string()]));

    let mut payload = json!({
        "records": [
            {
                "id": "1",
                "tax_id": "12-3456789",
                "details": {
                    "biometric": "fingerprint-hash"
                }
            },
            {
                "id": "2",
                "tax_id": "98-7654321",
                "details": {
                    "biometric": "iris-scan"
                }
            }
        ]
    });

    sanitizer.sanitize_value(&mut payload);

    assert_eq!(payload["records"][0]["id"], "1");
    assert_eq!(payload["records"][0]["tax_id"], "[REDACTED]");
    assert_eq!(payload["records"][0]["details"]["biometric"], "[REDACTED]");
    assert_eq!(payload["records"][1]["id"], "2");
    assert_eq!(payload["records"][1]["tax_id"], "[REDACTED]");
    assert_eq!(payload["records"][1]["details"]["biometric"], "[REDACTED]");
}
