use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

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
    pub gateway_admin_url: Option<String>,
    #[serde(default)]
    pub fluxcells: HashMap<String, FluxcellConfig>,
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
            gateway_admin_url: Some("http://127.0.0.1:8000".to_string()),
            fluxcells: HashMap::new(),
        }
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
pub struct FluxcellConfig {
    pub wasm_module: String,
    pub mount_path: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
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

            [fluxcells.magic_link]
            wasm_module = "cells/magic_link.wasm"
            mount_path = "/auth"
            enabled = true

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

        assert_eq!(cfg.fluxcells.len(), 2);
        let ml = &cfg.fluxcells["magic_link"];
        assert_eq!(ml.mount_path, "/auth");
        assert!(ml.enabled);

        let wh = &cfg.fluxcells["webhook"];
        assert_eq!(wh.mount_path, "/api/webhooks");
        assert!(wh.enabled); // defaults to true
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
}
