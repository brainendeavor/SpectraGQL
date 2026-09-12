use std::collections::HashMap;
use serde_json::json;
use spectragql::admin::is_ip_allowed;
use spectragql::admin::schema_inspector::analyze_mutation_coverage;
use spectragql::core::config::{OperationMode, SpectraRouteConfig};

#[test]
fn test_ip_allowlist_boundary_and_wildcards() {
    // 0.0.0.0/0 matches any IPv4
    let any_v4 = vec!["0.0.0.0/0".to_string()];
    assert!(is_ip_allowed(&"1.2.3.4".parse().unwrap(), &any_v4));
    assert!(is_ip_allowed(&"192.168.1.1".parse().unwrap(), &any_v4));
    assert!(!is_ip_allowed(&"::1".parse().unwrap(), &any_v4));

    // ::/0 matches any IPv6
    let any_v6 = vec!["::/0".to_string()];
    assert!(is_ip_allowed(&"::1".parse().unwrap(), &any_v6));
    assert!(is_ip_allowed(&"2001:db8::1".parse().unwrap(), &any_v6));
    assert!(!is_ip_allowed(&"127.0.0.1".parse().unwrap(), &any_v6));

    // Invalid CIDR prefix lengths (>32 for IPv4, >128 for IPv6) should not panic
    let invalid_cidr = vec![
        "10.0.0.1/33".to_string(),
        "invalid-ip/24".to_string(),
        "".to_string(),
        "   ".to_string(),
    ];
    assert!(!is_ip_allowed(&"10.0.0.1".parse().unwrap(), &invalid_cidr));
}

#[test]
fn test_schema_inspector_read_only_schema_without_mutations() {
    let read_only_schema = json!({
        "data": {
            "__schema": {
                "queryType": { "name": "Query" },
                "mutationType": null
            }
        }
    });

    let routes = HashMap::new();
    let coverage = analyze_mutation_coverage(&read_only_schema, &routes).unwrap();

    assert_eq!(coverage.total_mutations, 0);
    assert_eq!(coverage.strangled_count, 0);
    assert_eq!(coverage.mode_b_count, 0);
    assert_eq!(coverage.monolith_fallback_count, 0);
    assert!(coverage.mutations.is_empty());
}

#[test]
fn test_schema_inspector_missing_schema_returns_error() {
    let invalid_json = json!({
        "data": {}
    });

    let routes = HashMap::new();
    let res = analyze_mutation_coverage(&invalid_json, &routes);
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Missing '__schema'"));
}

#[test]
fn test_schema_inspector_strangler_fig_classification() {
    let schema_json = json!({
        "data": {
            "__schema": {
                "mutationType": {
                    "fields": [
                        { "name": "createOrder" },
                        { "name": "updateCustomer" },
                        { "name": "legacyMutation" }
                    ]
                }
            }
        }
    });

    let mut routes = HashMap::new();
    routes.insert(
        "create_order".to_string(),
        SpectraRouteConfig {
            operation: "createOrder".to_string(),
            mode: OperationMode::B,
            upstream: None,
            receipt_status: "ACCEPTED".to_string(),
        },
    );
    routes.insert(
        "update_customer".to_string(),
        SpectraRouteConfig {
            operation: "updateCustomer".to_string(),
            mode: OperationMode::A,
            upstream: Some("crm-service".to_string()),
            receipt_status: "ACCEPTED".to_string(),
        },
    );

    let coverage = analyze_mutation_coverage(&schema_json, &routes).unwrap();

    assert_eq!(coverage.total_mutations, 3);
    assert_eq!(coverage.mode_b_count, 1);
    assert_eq!(coverage.strangled_count, 1);
    assert_eq!(coverage.monolith_fallback_count, 1);

    let create_order = coverage.mutations.iter().find(|m| m.field_name == "createOrder").unwrap();
    assert_eq!(create_order.classification, "EdgeTerminatedModeB");

    let update_cust = coverage.mutations.iter().find(|m| m.field_name == "updateCustomer").unwrap();
    assert_eq!(update_cust.classification, "Strangled");
    assert_eq!(update_cust.target_upstream, Some("crm-service".to_string()));

    let legacy = coverage.mutations.iter().find(|m| m.field_name == "legacyMutation").unwrap();
    assert_eq!(legacy.classification, "MonolithFallback");
}
