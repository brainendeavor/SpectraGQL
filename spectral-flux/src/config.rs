use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const MIN_TIMEOUT_MS: u64 = 10;
pub const MAX_TIMEOUT_MS: u64 = 300_000;

#[derive(Debug, Clone, Deserialize)]
pub struct FluxConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default)]
    pub broker: BrokerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub databases: HashMap<String, DatabaseInstanceConfig>,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub gateway_admin_url: Option<String>,
    #[serde(default = "default_profiles")]
    pub profiles: HashMap<String, ExecutionProfileConfig>,
    #[serde(default)]
    pub fluxcells: HashMap<String, FluxcellConfig>,
    #[serde(default)]
    pub deployer: DeployerConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedExecutionConfig {
    pub profile: String,
    pub timeout_ms: u64,
    pub max_instances: usize,
    pub offload: OffloadStrategy,
    pub max_memory_bytes: usize,
}

impl Default for FluxConfig {
    fn default() -> Self {
        Self::default_local()
    }
}

impl FluxConfig {
    pub fn load_from_file(path: &str) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path).required(false))
            .add_source(config::Environment::with_prefix("FLUX").separator("__"))
            .build()?;
        let cfg: Self = settings.try_deserialize()?;
        Ok(cfg)
    }

    pub fn from_toml_str(s: &str) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from_str(s, config::FileFormat::Toml))
            .build()?;
        let cfg: Self = settings.try_deserialize()?;
        Ok(cfg)
    }

    pub fn default_local() -> Self {
        Self {
            port: 8081,
            host: "0.0.0.0".to_string(),
            broker: BrokerConfig::default(),
            storage: StorageConfig::default(),
            databases: HashMap::new(),
            database: DatabaseConfig::default(),
            gateway_admin_url: Some("http://127.0.0.1:8000".to_string()),
            profiles: default_profiles(),
            fluxcells: HashMap::new(),
            deployer: DeployerConfig::default(),
        }
    }

    pub fn resolve_cell_execution(
        &self,
        cell_name: &str,
        cell_cfg: &FluxcellConfig,
        guest_profile: Option<&str>,
        guest_timeout_ms: Option<u64>,
        guest_max_memory_mb: Option<usize>,
    ) -> Result<ResolvedExecutionConfig> {
        // 1. Determine profile name: explicit cell config profile > guest self-declared profile
        let profile_name = cell_cfg.profile.as_deref().or(guest_profile);

        // 2. Base profile lookup
        let base_profile = match profile_name {
            Some(p) => self.profiles.get(p).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Fluxcell '{}' specifies unknown execution profile '{}'",
                    cell_name,
                    p
                )
            })?,
            None => {
                // If neither cell config nor guest declares a profile, check if explicit timeout_ms was given
                if cell_cfg.timeout_ms.is_none() && guest_timeout_ms.is_none() {
                    return Err(anyhow::anyhow!(
                        "Fluxcell '{}' rejected: execution profile must be explicitly configured (e.g. profile = 'standard', 'extended', 'batch') or explicit timeout_ms provided",
                        cell_name
                    ));
                }
                // When explicit timeout_ms is given without a named profile, use standard profile as template
                self.profiles.get("standard").cloned().unwrap_or_else(|| ExecutionProfileConfig {
                    timeout_ms: 10_000,
                    max_instances: 16,
                    offload: OffloadStrategy::BlockingPool,
                    max_memory_mb: Some(16),
                })
            }
        };

        // 3. Apply overrides: cell_cfg > guest declaration > base profile
        let final_timeout = cell_cfg
            .timeout_ms
            .or(guest_timeout_ms)
            .unwrap_or(base_profile.timeout_ms);

        if final_timeout < MIN_TIMEOUT_MS || final_timeout > MAX_TIMEOUT_MS {
            return Err(anyhow::anyhow!(
                "Fluxcell '{}' timeout_ms ({}) out of bounds: must be between {}ms and {}ms",
                cell_name,
                final_timeout,
                MIN_TIMEOUT_MS,
                MAX_TIMEOUT_MS
            ));
        }

        let final_instances = cell_cfg
            .max_instances
            .unwrap_or(base_profile.max_instances);
        if final_instances == 0 {
            return Err(anyhow::anyhow!(
                "Fluxcell '{}' max_instances must be greater than 0",
                cell_name
            ));
        }

        let final_memory_mb = cell_cfg
            .max_memory_mb
            .or(guest_max_memory_mb)
            .or(base_profile.max_memory_mb)
            .unwrap_or(16);

        Ok(ResolvedExecutionConfig {
            profile: profile_name.unwrap_or("custom").to_string(),
            timeout_ms: final_timeout,
            max_instances: final_instances,
            offload: base_profile.offload,
            max_memory_bytes: final_memory_mb * 1024 * 1024,
        })
    }
}

fn default_port() -> u16 {
    8081
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    #[serde(default = "default_broker_method")]
    pub method: String,
    #[serde(default = "default_broker_addr")]
    pub addr: String,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
}

fn default_broker_method() -> String {
    "in_memory".to_string()
}

fn default_broker_addr() -> String {
    "localhost".to_string()
}

fn default_consumer_group() -> String {
    "spectral-flux-workers".to_string()
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            method: default_broker_method(),
            addr: default_broker_addr(),
            stream: None,
            consumer_group: default_consumer_group(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_backend")]
    pub backend: String,
    #[serde(default)]
    pub addr: Option<String>,
}

fn default_storage_backend() -> String {
    "embedded_kevy".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_storage_backend(),
            addr: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseInstanceConfig {
    pub url: String,
    #[serde(default = "default_db_max_connections")]
    pub max_connections: usize,
    #[serde(default)]
    pub driver: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: Option<String>,
    #[serde(default = "default_db_max_connections")]
    pub max_connections: usize,
}

fn default_db_max_connections() -> usize {
    16
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("DATABASE_URL").ok(),
            max_connections: default_db_max_connections(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OffloadStrategy {
    BlockingPool,
    DedicatedWorker,
    Inline,
}

impl Default for OffloadStrategy {
    fn default() -> Self {
        Self::BlockingPool
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExecutionProfileConfig {
    pub timeout_ms: u64,
    #[serde(default = "default_max_instances")]
    pub max_instances: usize,
    #[serde(default)]
    pub offload: OffloadStrategy,
    #[serde(default)]
    pub max_memory_mb: Option<usize>,
}

fn default_max_instances() -> usize {
    16
}

impl ExecutionProfileConfig {
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.timeout_ms < MIN_TIMEOUT_MS || self.timeout_ms > MAX_TIMEOUT_MS {
            return Err(anyhow::anyhow!(
                "Execution profile '{}' timeout_ms ({}) out of bounds: must be between {}ms and {}ms",
                name,
                self.timeout_ms,
                MIN_TIMEOUT_MS,
                MAX_TIMEOUT_MS
            ));
        }
        if self.max_instances == 0 {
            return Err(anyhow::anyhow!(
                "Execution profile '{}' max_instances must be greater than 0",
                name
            ));
        }
        Ok(())
    }
}

pub fn default_profiles() -> HashMap<String, ExecutionProfileConfig> {
    let mut m = HashMap::new();
    m.insert(
        "standard".to_string(),
        ExecutionProfileConfig {
            timeout_ms: 10_000,
            max_instances: 16,
            offload: OffloadStrategy::BlockingPool,
            max_memory_mb: Some(16),
        },
    );
    m.insert(
        "extended".to_string(),
        ExecutionProfileConfig {
            timeout_ms: 120_000,
            max_instances: 4,
            offload: OffloadStrategy::BlockingPool,
            max_memory_mb: Some(32),
        },
    );
    m.insert(
        "batch".to_string(),
        ExecutionProfileConfig {
            timeout_ms: 300_000,
            max_instances: 2,
            offload: OffloadStrategy::DedicatedWorker,
            max_memory_mb: Some(64),
        },
    );
    m
}

#[derive(Debug, Clone, Deserialize)]
pub struct FluxcellConfig {
    pub wasm_module: String,
    pub mount_path: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub subscriptions: Option<Vec<String>>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_memory_mb: Option<usize>,
    #[serde(default)]
    pub max_instances: Option<usize>,
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_max_wasm_size() -> usize {
    20 * 1024 * 1024 // 20 MB
}

fn default_storage_dir() -> String {
    "fluxcells".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeployerConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    #[serde(default = "default_false")]
    pub external_deploy_enabled: bool,
    #[serde(default = "default_false")]
    pub dev_upload_enabled: bool,
    #[serde(default = "default_false")]
    pub auto_activate: bool,
    #[serde(default)]
    pub allowed_artifact_hosts: Vec<String>,
    #[serde(default = "default_true")]
    pub require_https: bool,
    #[serde(default = "default_true")]
    pub block_private_networks: bool,
    #[serde(default = "default_max_wasm_size")]
    pub max_wasm_size_bytes: usize,
    #[serde(default = "default_storage_dir")]
    pub storage_dir: String,
}

impl Default for DeployerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            external_deploy_enabled: false,
            dev_upload_enabled: false,
            auto_activate: false,
            allowed_artifact_hosts: Vec::new(),
            require_https: true,
            block_private_networks: true,
            max_wasm_size_bytes: default_max_wasm_size(),
            storage_dir: default_storage_dir(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = FluxConfig::default();
        assert_eq!(cfg.port, 8081);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.broker.method, "in_memory");
        assert_eq!(cfg.broker.addr, "localhost");
        assert_eq!(cfg.broker.consumer_group, "spectral-flux-workers");
        assert_eq!(cfg.storage.backend, "embedded_kevy");
        assert_eq!(cfg.storage.addr, None);
        assert_eq!(cfg.gateway_admin_url, Some("http://127.0.0.1:8000".to_string()));
        assert!(cfg.fluxcells.is_empty());
        assert!(!cfg.deployer.enabled);
        assert!(!cfg.deployer.external_deploy_enabled);
        assert!(!cfg.deployer.dev_upload_enabled);
        assert!(!cfg.deployer.auto_activate);
        assert!(cfg.deployer.require_https);
        assert!(cfg.deployer.block_private_networks);
        assert_eq!(cfg.deployer.storage_dir, "fluxcells");
    }

    #[test]
    fn test_partial_toml_deserialization() {
        let toml_str = r#"
            port = 9090
            host = "127.0.0.1"
        "#;
        let cfg = FluxConfig::from_toml_str(toml_str).unwrap();
        assert_eq!(cfg.port, 9090);
        assert_eq!(cfg.host, "127.0.0.1");
        // Broker and storage should fall back to defaults
        assert_eq!(cfg.broker.method, "in_memory");
        assert_eq!(cfg.storage.backend, "embedded_kevy");
    }

    #[test]
    fn test_full_toml_deserialization() {
        let toml_str = r#"
            port = 8888
            host = "10.0.0.1"
            gateway_admin_url = "http://gateway:8000"

            [broker]
            method = "nats"
            addr = "nats://10.0.0.2:4222"
            stream = "MUTATIONS"
            consumer_group = "flux-cluster"

            [storage]
            backend = "redis"
            addr = "redis://10.0.0.3:6379"

            [database]
            url = "postgres://postgres:secret@localhost:5432/coeval"
            max_connections = 32

            [fluxcells.magic_link]
            wasm_module = "cells/magic_link.wasm"
            mount_path = "/auth"
            enabled = true
            subscriptions = ["mutation.auth.login"]

            [fluxcells.webhook]
            wasm_module = "cells/webhook.wasm"
            mount_path = "/api/webhooks"
        "#;
        let cfg = FluxConfig::from_toml_str(toml_str).unwrap();
        assert_eq!(cfg.port, 8888);
        assert_eq!(cfg.host, "10.0.0.1");
        assert_eq!(cfg.gateway_admin_url.as_deref(), Some("http://gateway:8000"));
        assert_eq!(cfg.broker.method, "nats");
        assert_eq!(cfg.broker.addr, "nats://10.0.0.2:4222");
        assert_eq!(cfg.broker.stream.as_deref(), Some("MUTATIONS"));
        assert_eq!(cfg.broker.consumer_group, "flux-cluster");
        assert_eq!(cfg.storage.backend, "redis");
        assert_eq!(cfg.storage.addr.as_deref(), Some("redis://10.0.0.3:6379"));
        assert_eq!(cfg.database.url.as_deref(), Some("postgres://postgres:secret@localhost:5432/coeval"));
        assert_eq!(cfg.database.max_connections, 32);

        assert_eq!(cfg.fluxcells.len(), 2);
        let ml = &cfg.fluxcells["magic_link"];
        assert_eq!(ml.mount_path, "/auth");
        assert!(ml.enabled);
        assert_eq!(ml.subscriptions, Some(vec!["mutation.auth.login".to_string()]));

        let wh = &cfg.fluxcells["webhook"];
        assert_eq!(wh.mount_path, "/api/webhooks");
        assert!(wh.enabled); // defaults to true
        assert_eq!(wh.subscriptions, None);
    }

    #[test]
    fn test_invalid_toml_syntax_fails() {
        let bad_toml = "port = not_a_number";
        assert!(FluxConfig::from_toml_str(bad_toml).is_err());
    }

    #[test]
    fn test_invalid_port_range_fails() {
        let bad_port_toml = "port = 70000"; // Exceeds u16::MAX (65535)
        assert!(FluxConfig::from_toml_str(bad_port_toml).is_err());
    }

    #[test]
    fn test_env_var_overrides() {
        // Use a unique env var to avoid race condition across threads
        unsafe {
            std::env::set_var("FLUX__PORT", "7777");
            std::env::set_var("FLUX__HOST", "127.0.0.5");
            std::env::set_var("FLUX__BROKER__METHOD", "redis");
            std::env::set_var("FLUX__BROKER__ADDR", "redis://127.0.0.1:6379");
            std::env::set_var("FLUX__STORAGE__BACKEND", "valkey");
        }

        let cfg = FluxConfig::load_from_file("nonexistent.toml").unwrap();
        assert_eq!(cfg.port, 7777);
        assert_eq!(cfg.host, "127.0.0.5");
        assert_eq!(cfg.broker.method, "redis");
        assert_eq!(cfg.broker.addr, "redis://127.0.0.1:6379");
        assert_eq!(cfg.storage.backend, "valkey");

        unsafe {
            std::env::remove_var("FLUX__PORT");
            std::env::remove_var("FLUX__HOST");
            std::env::remove_var("FLUX__BROKER__METHOD");
            std::env::remove_var("FLUX__BROKER__ADDR");
            std::env::remove_var("FLUX__STORAGE__BACKEND");
        }
    }

    #[test]
    fn test_default_profiles_exist_and_valid() {
        let cfg = FluxConfig::default();
        assert!(cfg.profiles.contains_key("standard"));
        assert!(cfg.profiles.contains_key("extended"));
        assert!(cfg.profiles.contains_key("batch"));

        let std_prof = &cfg.profiles["standard"];
        assert_eq!(std_prof.timeout_ms, 10_000);
        assert_eq!(std_prof.max_instances, 16);
        assert_eq!(std_prof.offload, OffloadStrategy::BlockingPool);
        assert!(std_prof.validate("standard").is_ok());

        let ext_prof = &cfg.profiles["extended"];
        assert_eq!(ext_prof.timeout_ms, 120_000);
        assert_eq!(ext_prof.max_instances, 4);
        assert_eq!(ext_prof.offload, OffloadStrategy::BlockingPool);
        assert!(ext_prof.validate("extended").is_ok());

        let batch_prof = &cfg.profiles["batch"];
        assert_eq!(batch_prof.timeout_ms, 300_000);
        assert_eq!(batch_prof.max_instances, 2);
        assert_eq!(batch_prof.offload, OffloadStrategy::DedicatedWorker);
        assert!(batch_prof.validate("batch").is_ok());
    }

    #[test]
    fn test_resolve_cell_execution_explicit_profile() {
        let cfg = FluxConfig::default();
        let cell = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: Some("extended".to_string()),
            timeout_ms: None,
            max_memory_mb: None,
            max_instances: None,
        };

        let resolved = cfg.resolve_cell_execution("test_cell", &cell, None, None, None).unwrap();
        assert_eq!(resolved.profile, "extended");
        assert_eq!(resolved.timeout_ms, 120_000);
        assert_eq!(resolved.max_instances, 4);
        assert_eq!(resolved.offload, OffloadStrategy::BlockingPool);
        assert_eq!(resolved.max_memory_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn test_resolve_cell_execution_guest_declaration() {
        let cfg = FluxConfig::default();
        let cell = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: None,
            timeout_ms: None,
            max_memory_mb: None,
            max_instances: None,
        };

        // Guest declares "batch"
        let resolved = cfg.resolve_cell_execution("test_cell", &cell, Some("batch"), None, None).unwrap();
        assert_eq!(resolved.profile, "batch");
        assert_eq!(resolved.timeout_ms, 300_000);
        assert_eq!(resolved.max_instances, 2);
        assert_eq!(resolved.offload, OffloadStrategy::DedicatedWorker);
    }

    #[test]
    fn test_resolve_cell_execution_missing_profile_and_timeout_fails() {
        let cfg = FluxConfig::default();
        let cell = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: None,
            timeout_ms: None,
            max_memory_mb: None,
            max_instances: None,
        };

        // Neither host config nor guest provides profile or timeout -> MUST FAIL FAST
        let err = cfg.resolve_cell_execution("unconfigured_cell", &cell, None, None, None).unwrap_err();
        assert!(err.to_string().contains("execution profile must be explicitly configured"));
    }

    #[test]
    fn test_resolve_cell_execution_bounds_enforcement() {
        let cfg = FluxConfig::default();

        // 1. Timeout < 10ms rejected
        let cell_low = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: Some("standard".to_string()),
            timeout_ms: Some(5), // below MIN_TIMEOUT_MS
            max_memory_mb: None,
            max_instances: None,
        };
        let err_low = cfg.resolve_cell_execution("low_cell", &cell_low, None, None, None).unwrap_err();
        assert!(err_low.to_string().contains("out of bounds"));

        // 2. Timeout == 0 (unbounded) strictly rejected
        let cell_zero = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: Some("standard".to_string()),
            timeout_ms: Some(0),
            max_memory_mb: None,
            max_instances: None,
        };
        let err_zero = cfg.resolve_cell_execution("zero_cell", &cell_zero, None, None, None).unwrap_err();
        assert!(err_zero.to_string().contains("out of bounds"));

        // 3. Timeout > 300,000ms rejected
        let cell_high = FluxcellConfig {
            wasm_module: "test.wasm".to_string(),
            mount_path: "/test".to_string(),
            enabled: true,
            subscriptions: None,
            profile: Some("standard".to_string()),
            timeout_ms: Some(300_001),
            max_memory_mb: None,
            max_instances: None,
        };
        let err_high = cfg.resolve_cell_execution("high_cell", &cell_high, None, None, None).unwrap_err();
        assert!(err_high.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_custom_profile_in_toml() {
        let toml_str = r#"
            [profiles.heavy_calc]
            timeout_ms = 45000
            max_instances = 8
            offload = "dedicated_worker"
            max_memory_mb = 128

            [fluxcells.calc]
            wasm_module = "cells/calc.wasm"
            mount_path = "/calc"
            profile = "heavy_calc"
        "#;
        let cfg = FluxConfig::from_toml_str(toml_str).unwrap();
        assert!(cfg.profiles.contains_key("heavy_calc"));

        let cell = &cfg.fluxcells["calc"];
        let resolved = cfg.resolve_cell_execution("calc", cell, None, None, None).unwrap();
        assert_eq!(resolved.profile, "heavy_calc");
        assert_eq!(resolved.timeout_ms, 45_000);
        assert_eq!(resolved.max_instances, 8);
        assert_eq!(resolved.offload, OffloadStrategy::DedicatedWorker);
        assert_eq!(resolved.max_memory_bytes, 128 * 1024 * 1024);
    }
}
