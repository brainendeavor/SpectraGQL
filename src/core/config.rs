use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

pub use super::types::{ExecutionStrategy, ModeADispatchPolicy, OperationMode};

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
    #[serde(default, alias = "strategy")]
    pub mode: ExecutionStrategy,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub upstream: Option<String>,
    #[serde(default = "default_receipt_status")]
    pub receipt_status: String,
    #[serde(default)]
    pub interceptors: Vec<String>,
    #[serde(default)]
    pub dispatch_policy: Option<ModeADispatchPolicy>,
}

impl SpectraRouteConfig {
    pub fn strategy(&self) -> ExecutionStrategy {
        self.mode
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterceptorStage {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterceptorType {
    Cel,
    Wasm,
    Native,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct InterceptorConfig {
    #[serde(rename = "type")]
    pub interceptor_type: InterceptorType,
    pub stage: InterceptorStage,
    // CEL configuration
    #[serde(alias = "expr")]
    pub expression: Option<String>,
    pub status_code: Option<u16>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub action: Option<String>,
    pub tag: Option<String>,
    // WASM configuration
    #[serde(alias = "module")]
    pub path: Option<String>,
    pub timeout_ms: Option<u64>,
    pub fail_mode: Option<crate::interceptors::FailMode>,
    #[serde(alias = "circuit_breaker_failures")]
    pub failure_threshold: Option<u32>,
    #[serde(alias = "circuit_breaker_reset_ms")]
    pub cooloff_duration_secs: Option<u64>,
    // Native configuration
    pub kind: Option<String>,
    pub token: Option<String>,
}

fn default_epoch_tick_interval_ms() -> u64 {
    1
}

fn default_wasm_timeout_ms() -> u64 {
    25
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraWasmConfig {
    #[serde(default = "default_true")]
    pub strict_aot: bool,
    #[serde(default)]
    pub allow_jit: bool,
    #[serde(default = "default_epoch_tick_interval_ms")]
    pub epoch_tick_interval_ms: u64,
    #[serde(default = "default_wasm_timeout_ms")]
    pub default_timeout_ms: u64,
}

impl Default for SpectraWasmConfig {
    fn default() -> Self {
        SpectraWasmConfig {
            strict_aot: true,
            allow_jit: false,
            epoch_tick_interval_ms: default_epoch_tick_interval_ms(),
            default_timeout_ms: default_wasm_timeout_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SpectraUpstreamConfig {
    #[serde(default = "default_upstream_name")]
    pub name: String,
    pub description: Option<String>,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyBackendType {
    #[default]
    Memory,
    Redis,
}

fn default_idempotency_ttl_secs() -> u64 {
    300
}

fn default_max_capacity() -> usize {
    10_000
}

fn default_sub_prefix() -> String {
    "spectra".to_string()
}

fn default_keepalive_secs() -> u64 {
    25
}

fn default_client_buffer_capacity() -> usize {
    256
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraSubscriptionsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_sub_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u64,
    #[serde(default = "default_client_buffer_capacity")]
    pub client_buffer_capacity: usize,
}

impl Default for SpectraSubscriptionsConfig {
    fn default() -> Self {
        SpectraSubscriptionsConfig {
            enabled: true,
            topic_prefix: default_sub_prefix(),
            keepalive_secs: default_keepalive_secs(),
            client_buffer_capacity: default_client_buffer_capacity(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraIdempotencyConfig {
    #[serde(default)]
    pub backend: IdempotencyBackendType,
    pub addr: Option<String>,
    #[serde(default = "default_idempotency_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_max_capacity")]
    pub max_capacity: usize,
}

impl Default for SpectraIdempotencyConfig {
    fn default() -> Self {
        SpectraIdempotencyConfig {
            backend: IdempotencyBackendType::Memory,
            addr: None,
            ttl_secs: default_idempotency_ttl_secs(),
            max_capacity: default_max_capacity(),
        }
    }
}

fn default_error_max_body_bytes() -> usize {
    4096
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ErrorCaptureConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_error_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for ErrorCaptureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_body_bytes: default_error_max_body_bytes(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct SpectraTelemetryConfig {
    #[serde(default)]
    pub error_capture: ErrorCaptureConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SpectraDispatchConfig {
    pub name: String,
    pub description: Option<String>,
    pub method: String,
    pub addr: String,
    #[serde(default)]
    pub reconnect_profile: Option<String>,
    #[serde(default)]
    pub reconnect_initial_ms: Option<u64>,
    #[serde(default)]
    pub reconnect_max_ms: Option<u64>,
}

impl SpectraDispatchConfig {
    pub fn is_interactive() -> bool {
        #[cfg(unix)]
        {
            unsafe {
                libc::isatty(libc::STDIN_FILENO) != 0 || libc::isatty(libc::STDOUT_FILENO) != 0
            }
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    pub fn initial_reconnect_ms(&self) -> u64 {
        if let Some(ms) = self.reconnect_initial_ms {
            return ms;
        }
        match self.reconnect_profile.as_deref() {
            Some("dev") | Some("development") => 250,
            Some("prod") | Some("production") => 10,
            _ => {
                if Self::is_interactive() {
                    250
                } else {
                    10
                }
            }
        }
    }

    pub fn max_reconnect_ms(&self) -> u64 {
        if let Some(ms) = self.reconnect_max_ms {
            return ms;
        }
        match self.reconnect_profile.as_deref() {
            Some("dev") | Some("development") => 5000,
            Some("prod") | Some("production") => 2000,
            _ => {
                if Self::is_interactive() {
                    5000
                } else {
                    2000
                }
            }
        }
    }
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
    #[serde(default)]
    pub interceptors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct SpectraRestConfig {
    pub paths: String,
    pub upstream: Option<SpectraUpstreamConfig>,
    pub dispatch: Option<SpectraDispatchConfig>,
}

fn default_admin_enabled() -> bool {
    false
}

fn default_admin_bind_addr() -> String {
    "0.0.0.0:8000".to_string()
}

fn default_admin_path_prefix() -> String {
    "/admin".to_string()
}

fn default_allowed_ips() -> Vec<String> {
    vec![
        "127.0.0.1".to_string(),
        "::1".to_string(),
        "10.0.0.0/8".to_string(),
        "172.16.0.0/12".to_string(),
        "192.168.0.0/16".to_string(),
    ]
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraAdminConfig {
    #[serde(default = "default_admin_enabled")]
    pub enabled: bool,
    #[serde(default = "default_admin_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_admin_path_prefix")]
    pub path_prefix: String,
    #[serde(default = "default_allowed_ips")]
    pub allowed_ips: Vec<String>,
    #[serde(default = "default_true")]
    pub enable_ui: bool,
    #[serde(default)]
    pub deploy_token: Option<String>,
}

impl Default for SpectraAdminConfig {
    fn default() -> Self {
        SpectraAdminConfig {
            enabled: default_admin_enabled(),
            bind_addr: default_admin_bind_addr(),
            path_prefix: default_admin_path_prefix(),
            allowed_ips: default_allowed_ips(),
            enable_ui: true,
            deploy_token: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpectraAppConfig {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default)]
    pub path_prefixes: Vec<String>,
    #[serde(default)]
    pub upstream: String,
    #[serde(default)]
    pub subject_prefix: Option<String>,
}

impl SpectraAppConfig {
    pub fn effective_subject_prefix(&self) -> String {
        self.subject_prefix
            .clone()
            .unwrap_or_else(|| format!("mutation.{}", self.id))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SpectraConfig {
    pub bind_addr: String,
    pub gql: SpectraGqlConfig,
    pub rest: SpectraRestConfig,
    pub upstream: SpectraUpstreamConfig,
    #[serde(default)]
    pub upstreams: HashMap<String, SpectraUpstreamConfig>,
    #[serde(default)]
    pub apps: Vec<SpectraAppConfig>,
    #[serde(default)]
    pub default_app: Option<String>,
    pub dispatch: SpectraDispatchConfig,
    #[serde(default)]
    pub idempotency: SpectraIdempotencyConfig,
    #[serde(default)]
    pub subscriptions: SpectraSubscriptionsConfig,
    #[serde(default)]
    pub admin: SpectraAdminConfig,
    #[serde(default)]
    pub interceptors: HashMap<String, InterceptorConfig>,
    #[serde(default)]
    pub wasm: SpectraWasmConfig,
    #[serde(default)]
    pub telemetry: SpectraTelemetryConfig,
}

impl SpectraConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let spectra_env = env::var("SPECTRA_ENV")
            .or_else(|_| env::var("SPECTRAGQL_ENV"))
            .unwrap_or_else(|_| "development".into());

        let mut builder = Config::builder()
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
            .set_default("idempotency.backend", "memory")?
            .set_default("idempotency.ttl_secs", 300)?
            .set_default("idempotency.max_capacity", 10000)?
            .set_default("subscriptions.enabled", true)?
            .set_default("subscriptions.topic_prefix", "spectra")?
            .set_default("subscriptions.keepalive_secs", 25)?
            .set_default("subscriptions.client_buffer_capacity", 256)?
            .set_default("admin.enabled", false)?
            .set_default("admin.bind_addr", "0.0.0.0:8000")?
            .set_default("admin.path_prefix", "/admin")?
            .set_default("admin.enable_ui", true)?
            .set_default("wasm.strict_aot", true)?
            .set_default("wasm.allow_jit", false)?
            .set_default("wasm.epoch_tick_interval_ms", 1)?
            .set_default("wasm.default_timeout_ms", 25)?
            .set_default("rest.paths", "/api,/api/{*path}")?
            .set_default("telemetry.error_capture.enabled", true)?
            .set_default("telemetry.error_capture.max_body_bytes", 4096)?;

        // 1. Explicit config file via SPECTRA_CONFIG or SPECTRAGQL_CONFIG takes top precedence
        if let Ok(config_path) = env::var("SPECTRA_CONFIG").or_else(|_| env::var("SPECTRAGQL_CONFIG")) {
            builder = builder.add_source(File::with_name(&config_path));
        } else if spectra_env != "development" {
            // Non-default environment (e.g. coeval, production, staging).
            // Check for environment-specific configuration files: {env}-spectra.toml or spectra.{env}.toml
            let hyphen_file = format!("{spectra_env}-spectra");
            let dot_file = format!("spectra.{spectra_env}");
            if std::path::Path::new(&format!("{hyphen_file}.toml")).exists() {
                builder = builder.add_source(File::with_name(&hyphen_file));
            } else if std::path::Path::new(&format!("{dot_file}.toml")).exists() {
                builder = builder.add_source(File::with_name(&dot_file));
            } else {
                builder = builder
                    .add_source(File::with_name("spectra").required(false))
                    .add_source(File::with_name(&hyphen_file).required(false));
            }
        } else {
            builder = builder
                .add_source(File::with_name("spectra").required(false))
                .add_source(File::with_name("development-spectra").required(false));
        }

        builder = builder
            .add_source(Environment::with_prefix("spectra").separator("_"))
            .add_source(Environment::with_prefix("spectragql").separator("_"));

        let builder = if let Ok(bind) = env::var("SPECTRA_BIND_ADDR").or_else(|_| env::var("SPECTRAGQL_BIND_ADDR")) {
            builder.set_override("bind_addr", bind)?
        } else {
            builder
        };

        let config_builder = builder.build()?;
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

    pub fn find_app_by_host(&self, host: &str) -> Option<&SpectraAppConfig> {
        let clean_host = host.split(':').next().unwrap_or(host).trim();
        self.apps.iter().find(|app| {
            app.domains.iter().any(|d| {
                let clean_d = d.split(':').next().unwrap_or(d).trim();
                clean_d.eq_ignore_ascii_case(clean_host)
            })
        })
    }

    pub fn find_app_by_id(&self, id: &str) -> Option<&SpectraAppConfig> {
        self.apps.iter().find(|app| app.id.eq_ignore_ascii_case(id))
    }

    pub fn find_app_by_path(&self, path: &str) -> Option<&SpectraAppConfig> {
        self.apps.iter().find(|app| {
            app.path_prefixes.iter().any(|prefix| {
                let clean_prefix = prefix.trim_end_matches('/');
                path == clean_prefix || path.starts_with(&format!("{}/", clean_prefix))
            })
        })
    }

    pub fn resolve_app<'a>(
        &'a self,
        host: Option<&str>,
        header_app: Option<&str>,
        path: &str,
    ) -> Option<&'a SpectraAppConfig> {
        if let Some(h_app) = header_app {
            if let Some(app) = self.find_app_by_id(h_app) {
                return Some(app);
            }
        }
        if let Some(h) = host {
            if let Some(app) = self.find_app_by_host(h) {
                return Some(app);
            }
        }
        if let Some(app) = self.find_app_by_path(path) {
            return Some(app);
        }
        if let Some(ref def_id) = self.default_app {
            if let Some(app) = self.find_app_by_id(def_id) {
                return Some(app);
            }
        }
        self.apps.first()
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
    fn test_mode_a_dispatch_policy_none_and_route_override() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"

            [upstream]
            addr = "127.0.0.1:4000"

            [gql]
            paths = "/graphql"
            ops_to_dispatch = "mutation"

            [gql.mode_a]
            dispatch_policy = "none"

            [gql.routes.record_vote]
            operation = "recordVote"
            mode = "A"
            dispatch_policy = "none"

            [gql.routes.adjust_inventory]
            operation = "adjustInventory"
            mode = "A"
            dispatch_policy = "response_only"

            [dispatch]
            method = "NATS"
            addr = "127.0.0.1:4222"
            name = "default"

            [rest]
            paths = "/api"
        "#;
        let cfg: SpectraConfig = Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(cfg.gql.mode_a.dispatch_policy, ModeADispatchPolicy::None);
        let vote_route = cfg.gql.routes.get("record_vote").unwrap();
        assert_eq!(vote_route.dispatch_policy, Some(ModeADispatchPolicy::None));
        let inv_route = cfg.gql.routes.get("adjust_inventory").unwrap();
        assert_eq!(inv_route.dispatch_policy, Some(ModeADispatchPolicy::ResponseOnly));
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

            [idempotency]
            backend = "redis"
            addr = "127.0.0.1:6379"
            ttl_secs = 600
            max_capacity = 20000

            [admin]
            enabled = true
            bind_addr = "0.0.0.0:8000"
            path_prefix = "/admin"
            allowed_ips = ["127.0.0.1", "10.0.0.0/8"]
            enable_ui = true

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

        assert_eq!(cfg.idempotency.backend, IdempotencyBackendType::Redis);
        assert_eq!(cfg.idempotency.addr, Some("127.0.0.1:6379".to_string()));
        assert_eq!(cfg.idempotency.ttl_secs, 600);
        assert_eq!(cfg.idempotency.max_capacity, 20000);

        assert!(cfg.admin.enabled);
        assert_eq!(cfg.admin.path_prefix, "/admin");
        assert_eq!(cfg.admin.allowed_ips, vec!["127.0.0.1", "10.0.0.0/8"]);
        assert!(cfg.admin.enable_ui);

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

    #[test]
    fn test_parse_interceptors_and_wasm_config() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"

            [upstream]
            addr = "127.0.0.1:4000"

            [dispatch]
            name = "default"
            method = "NATS"
            addr = "127.0.0.1:4222"

            [wasm]
            strict_aot = false
            allow_jit = true
            epoch_tick_interval_ms = 2
            default_timeout_ms = 50

            [interceptors.tenant_check]
            type = "cel"
            stage = "request"
            expression = 'request.headers["x-tenant-id"] != ""'
            status_code = 403
            code = "UNAUTHORIZED_TENANT"
            message = "Missing x-tenant-id header"

            [interceptors.wasm_anonymizer]
            type = "wasm"
            stage = "response"
            path = "plugins/anonymizer.cwasm"
            timeout_ms = 15
            fail_mode = "fail_open"
            failure_threshold = 4
            cooloff_duration_secs = 20

            [interceptors.syntax_validator]
            type = "native"
            stage = "request"
            kind = "graphql_syntax"

            [gql]
            paths = "/graphql"
            ops_to_dispatch = "query, mutation"
            interceptors = ["syntax_validator", "tenant_check"]

            [gql.routes.customer_profile]
            operation = "getCustomerProfile"
            mode = "A"
            interceptors = ["wasm_anonymizer"]

            [rest]
            paths = "/api"
        "#;

        let cfg: SpectraConfig = Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert!(!cfg.wasm.strict_aot);
        assert!(cfg.wasm.allow_jit);
        assert_eq!(cfg.wasm.epoch_tick_interval_ms, 2);
        assert_eq!(cfg.wasm.default_timeout_ms, 50);

        assert_eq!(cfg.interceptors.len(), 3);
        let tenant_interceptor = cfg.interceptors.get("tenant_check").unwrap();
        assert_eq!(tenant_interceptor.interceptor_type, InterceptorType::Cel);
        assert_eq!(tenant_interceptor.stage, InterceptorStage::Request);
        assert_eq!(tenant_interceptor.status_code, Some(403));
        assert_eq!(tenant_interceptor.code.as_deref(), Some("UNAUTHORIZED_TENANT"));

        let wasm_interceptor = cfg.interceptors.get("wasm_anonymizer").unwrap();
        assert_eq!(wasm_interceptor.interceptor_type, InterceptorType::Wasm);
        assert_eq!(wasm_interceptor.stage, InterceptorStage::Response);
        assert_eq!(wasm_interceptor.timeout_ms, Some(15));
        assert_eq!(wasm_interceptor.fail_mode, Some(crate::interceptors::FailMode::FailOpen));

        assert_eq!(cfg.gql.interceptors, vec!["syntax_validator", "tenant_check"]);
        let route = cfg.gql.routes.get("customer_profile").unwrap();
        assert_eq!(route.interceptors, vec!["wasm_anonymizer"]);
    }

    #[test]
    fn test_multi_app_configuration_and_resolution() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"
            default_app = "coeval"

            [upstream]
            addr = "127.0.0.1:4000"

            [dispatch]
            name = "default"
            method = "NATS"
            addr = "127.0.0.1:4222"

            [gql]
            paths = "/graphql"
            ops_to_dispatch = "mutation"

            [rest]
            paths = "/api"

            [[apps]]
            id = "coeval"
            name = "Open CoEval"
            domains = ["coeval.bio", "coeval.us"]
            path_prefixes = ["/coeval"]
            upstream = "coeval_upstream"
            subject_prefix = "mutation.coeval"

            [[apps]]
            id = "humanbase"
            name = "HumanBase"
            domains = ["humanbase.bio", "humanbase.io"]
            path_prefixes = ["/humanbase"]
            upstream = "humanbase_upstream"
        "#;

        let cfg: SpectraConfig = Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(cfg.apps.len(), 2);
        assert_eq!(cfg.default_app.as_deref(), Some("coeval"));

        // Host resolution (with and without port)
        let app1 = cfg.resolve_app(Some("coeval.bio:8000"), None, "/graphql").unwrap();
        assert_eq!(app1.id, "coeval");
        assert_eq!(app1.effective_subject_prefix(), "mutation.coeval");

        let app2 = cfg.resolve_app(Some("humanbase.io"), None, "/graphql").unwrap();
        assert_eq!(app2.id, "humanbase");
        assert_eq!(app2.effective_subject_prefix(), "mutation.humanbase");

        // Header resolution takes top priority
        let app3 = cfg.resolve_app(Some("coeval.bio"), Some("humanbase"), "/graphql").unwrap();
        assert_eq!(app3.id, "humanbase");

        // Path resolution when host is unknown
        let app4 = cfg.resolve_app(Some("api.internal"), None, "/humanbase/graphql").unwrap();
        assert_eq!(app4.id, "humanbase");

        // Fallback to default_app
        let app5 = cfg.resolve_app(Some("unknown.com"), None, "/graphql").unwrap();
        assert_eq!(app5.id, "coeval");
    }

    #[test]
    fn test_dispatch_config_reconnect_profiles() {
        let mut cfg = SpectraDispatchConfig {
            name: "default".into(),
            description: None,
            method: "NATS".into(),
            addr: "127.0.0.1:4222".into(),
            reconnect_profile: Some("production".into()),
            reconnect_initial_ms: None,
            reconnect_max_ms: None,
        };
        assert_eq!(cfg.initial_reconnect_ms(), 10);
        assert_eq!(cfg.max_reconnect_ms(), 2000);

        cfg.reconnect_profile = Some("development".into());
        assert_eq!(cfg.initial_reconnect_ms(), 250);
        assert_eq!(cfg.max_reconnect_ms(), 5000);

        cfg.reconnect_initial_ms = Some(50);
        cfg.reconnect_max_ms = Some(1500);
        assert_eq!(cfg.initial_reconnect_ms(), 50);
        assert_eq!(cfg.max_reconnect_ms(), 1500);
    }
}
