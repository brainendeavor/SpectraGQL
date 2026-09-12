use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::admin::api::{AdminMutationCoverageEntry, AdminSchemaCoverageResponse};
use crate::core::config::SpectraRouteConfig;
use crate::core::types::ExecutionStrategy;

/// Analyzes an upstream GraphQL introspection response JSON against configured routes
/// to compute strangler-fig migration progress, edge-terminated Mode B operations,
/// monolith fallbacks, and schema drift.
pub fn analyze_mutation_coverage(
    introspection_json: &serde_json::Value,
    routes: &HashMap<String, SpectraRouteConfig>,
) -> Result<AdminSchemaCoverageResponse, String> {
    // Navigate schema JSON: data.__schema.mutationType.fields or __schema.mutationType.fields
    let schema_root = if let Some(data) = introspection_json.get("data") {
        data.get("__schema")
    } else {
        introspection_json.get("__schema")
    };

    let Some(schema) = schema_root else {
        return Err("Missing '__schema' in introspection response".to_string());
    };

    let mutation_fields = schema
        .get("mutationType")
        .and_then(|m| m.get("fields"))
        .and_then(|f| f.as_array())
        .cloned()
        .unwrap_or_default();

    let mut mutations = Vec::new();
    let mut strangled_count = 0;
    let mut mode_b_count = 0;
    let mut monolith_fallback_count = 0;

    // Track matched route operation names to detect drift
    let mut matched_route_ops = std::collections::HashSet::new();

    for field in &mutation_fields {
        let field_name = field
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();

        if field_name.is_empty() {
            continue;
        }

        // Check if there is a configured route matching this mutation field
        let matched_route = routes.values().find(|r| r.operation == field_name);

        if let Some(route) = matched_route {
            matched_route_ops.insert(field_name.clone());
            match route.mode {
                ExecutionStrategy::AsyncEdgeCommand => {
                    mode_b_count += 1;
                    mutations.push(AdminMutationCoverageEntry {
                        field_name,
                        classification: "EdgeTerminatedModeB".to_string(),
                        target_upstream: None,
                        mode: Some("B".to_string()),
                        receipt_status: Some(route.receipt_status.clone()),
                    });
                }
                ExecutionStrategy::SyncUpstreamExecution => {
                    if let Some(upstream_name) = &route.upstream {
                        strangled_count += 1;
                        mutations.push(AdminMutationCoverageEntry {
                            field_name,
                            classification: "Strangled".to_string(),
                            target_upstream: Some(upstream_name.clone()),
                            mode: Some("A".to_string()),
                            receipt_status: None,
                        });
                    } else {
                        monolith_fallback_count += 1;
                        mutations.push(AdminMutationCoverageEntry {
                            field_name,
                            classification: "MonolithFallback".to_string(),
                            target_upstream: Some("default".to_string()),
                            mode: Some("A".to_string()),
                            receipt_status: None,
                        });
                    }
                }
            }
        } else {
            // No custom route -> falls through to default upstream monolith
            monolith_fallback_count += 1;
            mutations.push(AdminMutationCoverageEntry {
                field_name,
                classification: "MonolithFallback".to_string(),
                target_upstream: Some("default".to_string()),
                mode: Some("A".to_string()),
                receipt_status: None,
            });
        }
    }

    // Detect schema drift: routes defined in config whose operations were not in the schema
    let mut drifted_routes = Vec::new();
    for (route_name, route) in routes {
        if !matched_route_ops.contains(&route.operation) {
            // Only drift if the schema actually had mutation fields defined
            if !mutation_fields.is_empty() {
                drifted_routes.push(format!(
                    "Route '{}' (operation: '{}') not found in upstream GraphQL schema",
                    route_name, route.operation
                ));
            }
        }
    }
    drifted_routes.sort();

    let total_mutations = mutations.len();
    let drift_count = drifted_routes.len();
    let coverage_percent = if total_mutations > 0 {
        ((strangled_count + mode_b_count) as f64 / total_mutations as f64) * 100.0
    } else {
        0.0
    };

    let now_utc = chrono_or_simple_timestamp();

    Ok(AdminSchemaCoverageResponse {
        total_mutations,
        strangled_count,
        mode_b_count,
        monolith_fallback_count,
        drift_count,
        coverage_percent,
        mutations,
        drifted_routes,
        last_refreshed_utc: now_utc,
    })
}

fn chrono_or_simple_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{} UTC (unix: {})", secs, secs)
}

#[derive(Clone)]
pub struct SchemaInspector {
    cached_coverage: Arc<RwLock<Option<AdminSchemaCoverageResponse>>>,
    http_client: reqwest::Client,
}

impl SchemaInspector {
    pub fn new() -> Self {
        SchemaInspector {
            cached_coverage: Arc::new(RwLock::new(None)),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(4))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Refresh schema coverage by fetching introspection from upstream URL.
    pub async fn refresh(
        &self,
        upstream_url: &str,
        routes: &HashMap<String, SpectraRouteConfig>,
    ) -> Result<AdminSchemaCoverageResponse, String> {
        let query = serde_json::json!({
            "query": "query SpectraIntrospection { __schema { mutationType { fields { name description } } } }"
        });
        let query_bytes = serde_json::to_vec(&query).map_err(|e| e.to_string())?;

        let resp = self
            .http_client
            .post(upstream_url)
            .header("content-type", "application/json")
            .body(query_bytes)
            .send()
            .await
            .map_err(|e| format!("Upstream HTTP request failed to '{}': {}", upstream_url, e))?;

        let status = resp.status();
        if !status.is_success() {
            return Err(format!(
                "Upstream responded with HTTP {} during introspection",
                status
            ));
        }

        let resp_bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("Failed to read introspection response from upstream: {}", e))?;

        let json_body: serde_json::Value = serde_json::from_slice(&resp_bytes)
            .map_err(|e| format!("Failed to parse JSON introspection from upstream: {}", e))?;

        let coverage = analyze_mutation_coverage(&json_body, routes)?;

        let mut guard = self.cached_coverage.write().await;
        *guard = Some(coverage.clone());

        Ok(coverage)
    }

    /// Returns current cached coverage, or None if not yet introspected.
    pub async fn get_coverage(&self) -> Option<AdminSchemaCoverageResponse> {
        self.cached_coverage.read().await.clone()
    }

    /// Updates cache directly (useful for tests and synthetic schema feeds).
    pub async fn set_coverage(&self, coverage: AdminSchemaCoverageResponse) {
        let mut guard = self.cached_coverage.write().await;
        *guard = Some(coverage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_mutation_coverage_success() {
        let introspection_json = serde_json::json!({
            "data": {
                "__schema": {
                    "mutationType": {
                        "fields": [
                            { "name": "adjustInventory", "description": "Update inventory" },
                            { "name": "importCatalog", "description": "Bulk import" },
                            { "name": "updateUserEmail", "description": "Update email" }
                        ]
                    }
                }
            }
        });

        let mut routes = HashMap::new();
        routes.insert(
            "inventory_update".to_string(),
            SpectraRouteConfig {
                operation: "adjustInventory".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                upstream: Some("inventory".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );
        routes.insert(
            "bulk_import".to_string(),
            SpectraRouteConfig {
                operation: "importCatalog".to_string(),
                mode: ExecutionStrategy::AsyncEdgeCommand,
                upstream: None,
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );
        routes.insert(
            "ghost_operation".to_string(),
            SpectraRouteConfig {
                operation: "nonExistentMutation".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                upstream: Some("ghost_service".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
            },
        );

        let coverage = analyze_mutation_coverage(&introspection_json, &routes).unwrap();

        assert_eq!(coverage.total_mutations, 3);
        assert_eq!(coverage.strangled_count, 1); // adjustInventory
        assert_eq!(coverage.mode_b_count, 1); // importCatalog
        assert_eq!(coverage.monolith_fallback_count, 1); // updateUserEmail
        assert_eq!(coverage.drift_count, 1); // nonExistentMutation
        assert_eq!(coverage.drifted_routes.len(), 1);
        assert!(coverage.drifted_routes[0].contains("nonExistentMutation"));

        // Coverage percent: (1 + 1) / 3 = 66.666...%
        assert!((coverage.coverage_percent - 66.666).abs() < 0.01);

        let adjust_mut = coverage
            .mutations
            .iter()
            .find(|m| m.field_name == "adjustInventory")
            .unwrap();
        assert_eq!(adjust_mut.classification, "Strangled");
        assert_eq!(adjust_mut.target_upstream, Some("inventory".to_string()));

        let import_mut = coverage
            .mutations
            .iter()
            .find(|m| m.field_name == "importCatalog")
            .unwrap();
        assert_eq!(import_mut.classification, "EdgeTerminatedModeB");

        let email_mut = coverage
            .mutations
            .iter()
            .find(|m| m.field_name == "updateUserEmail")
            .unwrap();
        assert_eq!(email_mut.classification, "MonolithFallback");
        assert_eq!(email_mut.target_upstream, Some("default".to_string()));
    }
}
