use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum OperationMode {
    #[default]
    A,
    B,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModeADispatchPolicy {
    #[default]
    ResponseWithFailure,
    ResponseOnly,
    RawAudit,
}

fn default_true() -> bool {
    true
}

fn default_timeout_ms() -> u64 {
    3000
}

fn default_receipt_status() -> String {
    "ACCEPTED".to_string()
}

fn default_upstream_name() -> String {
    "default".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraModeAConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub dispatch_policy: ModeADispatchPolicy,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for SpectraModeAConfig {
    fn default() -> Self {
        SpectraModeAConfig {
            enabled: true,
            dispatch_policy: ModeADispatchPolicy::default(),
            timeout_ms: default_timeout_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraRouteConfig {
    pub operation: String,
    #[serde(default)]
    pub mode: OperationMode,
    pub upstream: Option<String>,
    #[serde(default = "default_receipt_status")]
    pub receipt_status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SpectraUpstreamConfig {
    #[serde(default = "default_upstream_name")]
    pub name: String,
    pub description: Option<String>,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SpectraDispatchConfig {
    pub name: String,
    pub description: Option<String>,
    pub method: String,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SpectraGqlConfig {
    pub paths: String,
    pub ops_to_dispatch: String,
    pub upstream: Option<SpectraUpstreamConfig>,
    pub dispatch: Option<SpectraDispatchConfig>,
    #[serde(default)]
    pub mode_a: SpectraModeAConfig,
    #[serde(default)]
    pub routes: HashMap<String, SpectraRouteConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct SpectraRestConfig {
    pub paths: String,
    pub upstream: Option<SpectraUpstreamConfig>,
    pub dispatch: Option<SpectraDispatchConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct SpectraConfig {
    pub bind_addr: String,
    pub gql: SpectraGqlConfig,
    pub rest: SpectraRestConfig,
    pub upstream: SpectraUpstreamConfig,
    #[serde(default)]
    pub upstreams: HashMap<String, SpectraUpstreamConfig>,
    pub dispatch: SpectraDispatchConfig,
}

impl SpectraConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let spectra_env = env::var("SPECTRA_ENV")
            .or_else(|_| env::var("SPECTRAGQL_ENV"))
            .unwrap_or_else(|_| "development".into());

        let config_builder = Config::builder()
            .set_default("bind_addr", "0.0.0.0:8000")?
            .set_default("upstream.addr", "localhost:4000")?
            .set_default("upstream.name", "default")?
            .set_default("dispatch.method", "nats")?
            .set_default("dispatch.addr", "127.0.0.1:4222")?
            .set_default("dispatch.name", "default")?
            .set_default("gql.paths", "/gql,/graphql")?
            .set_default("gql.ops_to_dispatch", "query, mutation, subscription")?
            .set_default("gql.mode_a.enabled", true)?
            .set_default("gql.mode_a.dispatch_policy", "response_with_failure")?
            .set_default("gql.mode_a.timeout_ms", 3000)?
            .set_default("rest.paths", "/api,/api/{*path}")?
            .add_source(File::with_name("spectra").required(false))
            .add_source(File::with_name(&format!("{spectra_env}-spectra")).required(false))
            .add_source(Environment::with_prefix("spectra").separator("_"))
            .add_source(Environment::with_prefix("spectragql").separator("_"))
            .build()?;

        config_builder.try_deserialize()
    }

    pub fn gql_upstream(&self) -> &SpectraUpstreamConfig {
        self.gql.upstream.as_ref().unwrap_or(&self.upstream)
    }

    pub fn gql_dispatch(&self) -> &SpectraDispatchConfig {
        self.gql.dispatch.as_ref().unwrap_or(&self.dispatch)
    }

    pub fn rest_upstream(&self) -> &SpectraUpstreamConfig {
        self.rest.upstream.as_ref().unwrap_or(&self.upstream)
    }

    pub fn rest_dispatch(&self) -> &SpectraDispatchConfig {
        self.rest.dispatch.as_ref().unwrap_or(&self.dispatch)
    }

    pub fn resolve_all_upstreams(
        &self,
    ) -> Result<HashMap<String, SocketAddr>, std::io::Error> {
        use std::net::ToSocketAddrs;
        let mut map = HashMap::new();

        // 1. Default upstream
        let def_addr_str = &self.gql_upstream().addr;
        let def_addr = def_addr_str
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Could not resolve default upstream: {}", def_addr_str),
                )
            })?;
        map.insert("default".to_string(), def_addr);
        map.insert(self.gql_upstream().name.clone(), def_addr);

        // 2. Named upstreams
        for (key, cfg) in &self.upstreams {
            let addr = cfg
                .addr
                .to_socket_addrs()?
                .next()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Could not resolve upstream {}: {}", key, cfg.addr),
                    )
                })?;
            map.insert(key.clone(), addr);
            if !cfg.name.is_empty() && cfg.name != "default" {
                map.insert(cfg.name.clone(), addr);
            }
        }

        Ok(map)
    }

    #[allow(dead_code)]
    pub fn resolve_upstream_addr<'a>(
        named_upstreams: &'a HashMap<String, SocketAddr>,
        default_addr: &'a SocketAddr,
        upstream_name: Option<&str>,
    ) -> &'a SocketAddr {
        upstream_name
            .and_then(|name| named_upstreams.get(name))
            .unwrap_or(default_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_a_dispatch_policy_defaults() {
        let default_mode_a = SpectraModeAConfig::default();
        assert!(default_mode_a.enabled);
        assert_eq!(
            default_mode_a.dispatch_policy,
            ModeADispatchPolicy::ResponseWithFailure
        );
        assert_eq!(default_mode_a.timeout_ms, 3000);
    }

    #[test]
    fn test_parse_full_spectra_toml_schema() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"

            [upstream]
            addr = "127.0.0.1:4000"
            name = "core_monolith"

            [upstreams.inventory]
            addr = "127.0.0.1:5001"

            [upstreams.crm]
            addr = "127.0.0.1:5002"

            [gql]
            paths = "/graphql,/gql"
            ops_to_dispatch = "mutation"

            [gql.mode_a]
            enabled = true
            dispatch_policy = "response_only"
            timeout_ms = 5000

            [gql.routes.inventory_update]
            operation = "adjustInventory"
            mode = "A"
            upstream = "inventory"

            [gql.routes.bulk_import]
            operation = "importCatalog"
            mode = "B"
            receipt_status = "ACCEPTED"

            [dispatch]
            method = "NATS"
            addr = "127.0.0.1:4222"
            name = "default"

            [rest]
            paths = "/api,/api/{*path}"
        "#;

        let cfg: SpectraConfig = Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(cfg.bind_addr, "0.0.0.0:8000");
        assert_eq!(cfg.upstream.addr, "127.0.0.1:4000");
        assert_eq!(cfg.upstreams.len(), 2);
        assert_eq!(cfg.upstreams.get("inventory").unwrap().addr, "127.0.0.1:5001");
        assert_eq!(cfg.upstreams.get("crm").unwrap().addr, "127.0.0.1:5002");

        assert_eq!(
            cfg.gql.mode_a.dispatch_policy,
            ModeADispatchPolicy::ResponseOnly
        );
        assert_eq!(cfg.gql.mode_a.timeout_ms, 5000);

        let inv_route = cfg.gql.routes.get("inventory_update").unwrap();
        assert_eq!(inv_route.operation, "adjustInventory");
        assert_eq!(inv_route.mode, OperationMode::A);
        assert_eq!(inv_route.upstream, Some("inventory".to_string()));

        let bulk_route = cfg.gql.routes.get("bulk_import").unwrap();
        assert_eq!(bulk_route.operation, "importCatalog");
        assert_eq!(bulk_route.mode, OperationMode::B);
        assert_eq!(bulk_route.receipt_status, "ACCEPTED");

        // Test upstream socket resolution
        let resolved = cfg.resolve_all_upstreams().unwrap();
        let default_addr = resolved.get("default").unwrap();
        assert_eq!(default_addr.port(), 4000);

        let inv_resolved = SpectraConfig::resolve_upstream_addr(
            &resolved,
            default_addr,
            Some("inventory"),
        );
        assert_eq!(inv_resolved.port(), 5001);

        let fallback_resolved = SpectraConfig::resolve_upstream_addr(
            &resolved,
            default_addr,
            Some("unknown_svc"),
        );
        assert_eq!(fallback_resolved.port(), 4000);
    }
}
