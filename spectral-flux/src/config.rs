use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct FluxConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
    pub broker: BrokerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub gateway_admin_url: Option<String>,
    #[serde(default)]
    pub fluxcells: HashMap<String, FluxcellConfig>,
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

    pub fn default_local() -> Self {
        Self {
            port: 8081,
            host: "0.0.0.0".to_string(),
            broker: BrokerConfig {
                method: "in_memory".to_string(),
                addr: "localhost".to_string(),
                stream: None,
                consumer_group: "spectral-flux-workers".to_string(),
            },
            storage: StorageConfig {
                backend: "embedded_kevy".to_string(),
                addr: None,
            },
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
    pub method: String,
    pub addr: String,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
}

fn default_consumer_group() -> String {
    "spectral-flux-workers".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub backend: String,
    #[serde(default)]
    pub addr: Option<String>,
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
