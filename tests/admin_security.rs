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
            interceptors: vec![],
        },
    );
    routes.insert(
        "update_customer".to_string(),
        SpectraRouteConfig {
            operation: "updateCustomer".to_string(),
            mode: OperationMode::A,
            upstream: Some("crm-service".to_string()),
            receipt_status: "ACCEPTED".to_string(),
            interceptors: vec![],
        },
    );

    let coverage = analyze_mutation_coverage(&schema_json, &routes).unwrap();

    assert_eq!(coverage.total_mutations, 3);
    assert_eq!(coverage.async_count, 1);
    assert_eq!(coverage.targeted_count, 1);
    assert_eq!(coverage.default_fallback_count, 1);
    assert_eq!(coverage.mode_b_count, 1);
    assert_eq!(coverage.strangled_count, 1);
    assert_eq!(coverage.monolith_fallback_count, 1);

    let create_order = coverage.mutations.iter().find(|m| m.field_name == "createOrder").unwrap();
    assert_eq!(create_order.classification, "AsyncReceipt");

    let update_cust = coverage.mutations.iter().find(|m| m.field_name == "updateCustomer").unwrap();
    assert_eq!(update_cust.classification, "TargetedService");
    assert_eq!(update_cust.target_upstream, Some("crm-service".to_string()));

    let legacy = coverage.mutations.iter().find(|m| m.field_name == "legacyMutation").unwrap();
    assert_eq!(legacy.classification, "DefaultUpstream");
}

#[test]
fn test_worker_registry_lifecycle_and_summaries() {
    use spectragql::admin::registry::{WorkerLogEntry, WorkerRegistry, WorkerTelemetryReport};
    use std::time::Duration;

    let registry = WorkerRegistry::new(100, Duration::from_secs(10));

    // Initially empty
    assert!(registry.get_active_workers().is_empty());
    assert!(registry.get_worker_logs("non-existent", 10).is_none());

    // Report from worker 1
    registry.record_report(WorkerTelemetryReport {
        worker_id: "projection-worker".to_string(),
        sink: Some("nats".to_string()),
        stream: Some("SPECTRA".to_string()),
        status: Some("healthy".to_string()),
        uptime_seconds: Some(120),
        processed_events: Some(50),
        total_errors: Some(0),
        last_event_hlc: Some("1789270000000-000000".to_string()),
        logs: Some(vec![
            WorkerLogEntry {
                timestamp: "2026-09-13T04:00:00Z".to_string(),
                level: "INFO".to_string(),
                message: "Processed batch 1".to_string(),
                hlc: None,
            },
        ]),
    });

    let active = registry.get_active_workers();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].worker_id, "projection-worker");
    assert_eq!(active[0].status, "healthy");
    assert_eq!(active[0].processed_events, 50);

    let logs = registry.get_worker_logs("projection-worker", 50).unwrap();
    assert_eq!(logs.worker_id, "projection-worker");
    assert_eq!(logs.logs.len(), 1);
    assert_eq!(logs.logs[0].message, "Processed batch 1");

    // Clear registry
    registry.clear();
    assert!(registry.get_active_workers().is_empty());
}

#[test]
fn test_worker_telemetry_json_payload_deserialization() {
    use spectragql::admin::registry::WorkerTelemetryReport;

    let raw_json = r#"{
        "workerId": "kafka-analytics-worker",
        "sink": "kafka",
        "stream": "events.telemetry",
        "status": "healthy",
        "uptimeSeconds": 3600,
        "processedEvents": 9999,
        "totalErrors": 2,
        "lastEventHlc": "1789273153940-000000",
        "logs": [
            {
                "timestamp": "2026-09-13T04:19:19.020Z",
                "level": "INFO",
                "message": "Processed recordVote",
                "hlc": "1789273153940-000000"
            }
        ]
    }"#;

    let report: WorkerTelemetryReport = serde_json::from_str(raw_json).expect("Must parse valid telemetry report");
    assert_eq!(report.worker_id, "kafka-analytics-worker");
    assert_eq!(report.sink.as_deref(), Some("kafka"));
    assert_eq!(report.processed_events, Some(9999));
    assert_eq!(report.logs.as_ref().unwrap().len(), 1);
    assert_eq!(report.logs.as_ref().unwrap()[0].level, "INFO");
}
