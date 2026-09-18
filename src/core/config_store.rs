use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use parking_lot::RwLock;

use super::config::SpectraConfigStoreConfig;

/// Obfuscates user credentials from a connection URL string for logging and observability.
pub fn sanitize_url(raw: &str) -> String {
    if let Some(at_idx) = raw.find('@') {
        if let Some(proto_idx) = raw.find("://") {
            let prefix = &raw[..proto_idx + 3];
            let rest = &raw[at_idx..];
            return format!("{}*****{}", prefix, rest);
        }
    }
    raw.to_string()
}

/// Asynchronous persistent storage interface for SpectraGQL configuration.
#[async_trait]
pub trait ConfigStore: Send + Sync {
    /// Loads the stored raw configuration string (e.g. TOML).
    /// Returns Ok(None) if no configuration exists in the store yet.
    async fn load_config(&self) -> Result<Option<String>>;

    /// Persists the raw configuration string.
    async fn save_config(&self, content: &str) -> Result<()>;

    /// Returns the short name of the backend (e.g. "postgres", "redis", "file", "memory").
    fn backend_name(&self) -> &'static str;

    /// Returns a descriptive target string for observability.
    fn descriptor(&self) -> String;
}

// ---------------------------------------------------------------------------
// Memory Backend (Ephemeral / Tests / Local Dev)
// ---------------------------------------------------------------------------

pub struct MemoryConfigStore {
    content: RwLock<Option<String>>,
}

impl MemoryConfigStore {
    pub fn new(initial: Option<String>) -> Self {
        Self {
            content: RwLock::new(initial),
        }
    }
}

#[async_trait]
impl ConfigStore for MemoryConfigStore {
    async fn load_config(&self) -> Result<Option<String>> {
        Ok(self.content.read().clone())
    }

    async fn save_config(&self, content: &str) -> Result<()> {
        *self.content.write() = Some(content.to_string());
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn descriptor(&self) -> String {
        "In-Memory (Ephemeral)".to_string()
    }
}

// ---------------------------------------------------------------------------
// File / Volume Mount Backend
// ---------------------------------------------------------------------------

pub struct FileConfigStore {
    path: PathBuf,
}

impl FileConfigStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl ConfigStore for FileConfigStore {
    async fn load_config(&self) -> Result<Option<String>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .with_context(|| format!("Failed to read config file from '{}'", self.path.display()))?;
        Ok(Some(content))
    }

    async fn save_config(&self, content: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("Failed to create parent directory '{}'", parent.display())
                })?;
            }
        }

        // Atomic write via temporary file replace
        let tmp_path = self.path.with_extension("tmp");
        tokio::fs::write(&tmp_path, content)
            .await
            .with_context(|| format!("Failed to write temporary config to '{}'", tmp_path.display()))?;
        tokio::fs::rename(&tmp_path, &self.path)
            .await
            .with_context(|| format!("Failed to replace config file at '{}'", self.path.display()))?;
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "file"
    }

    fn descriptor(&self) -> String {
        format!("File / Volume Mount: '{}'", self.path.display())
    }
}

// ---------------------------------------------------------------------------
// Redis Backend
// ---------------------------------------------------------------------------

pub struct RedisConfigStore {
    client: redis::Client,
    key: String,
    redis_url: String,
}

impl RedisConfigStore {
    pub fn new(redis_url: &str, key: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .with_context(|| format!("Failed to create Redis client for '{}'", sanitize_url(redis_url)))?;
        Ok(Self {
            client,
            key: key.to_string(),
            redis_url: redis_url.to_string(),
        })
    }
}

#[async_trait]
impl ConfigStore for RedisConfigStore {
    async fn load_config(&self) -> Result<Option<String>> {
        let mut conn = self.client.get_async_connection().await
            .with_context(|| format!("Failed to connect to Redis at '{}'", sanitize_url(&self.redis_url)))?;
        let res: Option<String> = redis::cmd("GET")
            .arg(&self.key)
            .query_async(&mut conn)
            .await
            .with_context(|| format!("Failed to GET config from Redis key '{}'", self.key))?;
        Ok(res)
    }

    async fn save_config(&self, content: &str) -> Result<()> {
        let mut conn = self.client.get_async_connection().await
            .with_context(|| format!("Failed to connect to Redis at '{}'", sanitize_url(&self.redis_url)))?;
        redis::cmd("SET")
            .arg(&self.key)
            .arg(content)
            .query_async::<_, ()>(&mut conn)
            .await
            .with_context(|| format!("Failed to SET config in Redis key '{}'", self.key))?;
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "redis"
    }

    fn descriptor(&self) -> String {
        format!("Redis: '{}' (key: '{}')", sanitize_url(&self.redis_url), self.key)
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL / RDBMS Backend
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
pub struct PostgresConfigStore {
    pool: deadpool_postgres::Pool,
    table_name: String,
    key: String,
    database_url: String,
}

#[cfg(feature = "postgres")]
impl PostgresConfigStore {
    pub fn new(database_url: &str, table_name: &str, key: &str) -> Result<Self> {
        let pg_config: tokio_postgres::Config = database_url
            .parse()
            .with_context(|| format!("Failed to parse database connection URL: {}", sanitize_url(database_url)))?;

        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        };

        let host = pg_config.get_hosts().first().cloned();
        let is_local = match host {
            Some(tokio_postgres::config::Host::Tcp(ref h)) => {
                h == "localhost" || h == "127.0.0.1" || h == "0.0.0.0"
            }
            _ => false,
        };

        let requires_tls = match pg_config.get_ssl_mode() {
            tokio_postgres::config::SslMode::Require => true,
            tokio_postgres::config::SslMode::Disable => false,
            _ => !is_local,
        };

        let pool = if requires_tls {
            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
            let mgr = deadpool_postgres::Manager::from_config(pg_config, tls, mgr_config);
            deadpool_postgres::Pool::builder(mgr)
                .max_size(4)
                .build()
                .context("Failed to build PostgreSQL connection pool with TLS")?
        } else {
            let mgr = deadpool_postgres::Manager::from_config(pg_config, tokio_postgres::NoTls, mgr_config);
            deadpool_postgres::Pool::builder(mgr)
                .max_size(4)
                .build()
                .context("Failed to build PostgreSQL connection pool with NoTls")?
        };

        Ok(Self {
            pool,
            table_name: table_name.to_string(),
            key: key.to_string(),
            database_url: database_url.to_string(),
        })
    }

    pub async fn ensure_table(&self) -> Result<()> {
        let client = self.pool.get().await
            .context("Failed to obtain PostgreSQL connection to initialize config table")?;
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                key VARCHAR(64) PRIMARY KEY,
                content TEXT NOT NULL,
                version BIGINT NOT NULL DEFAULT 1,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_by VARCHAR(128)
            )",
            self.table_name
        );
        client.batch_execute(&query).await
            .with_context(|| format!("Failed to create config table '{}'", self.table_name))?;
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ConfigStore for PostgresConfigStore {
    async fn load_config(&self) -> Result<Option<String>> {
        let client = self.pool.get().await
            .context("Failed to obtain PostgreSQL connection for load_config")?;
        let query = format!("SELECT content FROM {} WHERE key = $1", self.table_name);
        let rows = client.query(&query, &[&self.key]).await
            .with_context(|| format!("Failed to query config from PostgreSQL table '{}'", self.table_name))?;
        if let Some(row) = rows.first() {
            let content: String = row.get(0);
            Ok(Some(content))
        } else {
            Ok(None)
        }
    }

    async fn save_config(&self, content: &str) -> Result<()> {
        let client = self.pool.get().await
            .context("Failed to obtain PostgreSQL connection for save_config")?;
        let query = format!(
            "INSERT INTO {} (key, content, version, updated_at, updated_by)
             VALUES ($1, $2, 1, CURRENT_TIMESTAMP, 'admin_control_plane')
             ON CONFLICT (key) DO UPDATE
             SET content = EXCLUDED.content,
                 version = {}.version + 1,
                 updated_at = CURRENT_TIMESTAMP,
                 updated_by = EXCLUDED.updated_by",
            self.table_name, self.table_name
        );
        let content_str = content.to_string();
        client.execute(&query, &[&self.key, &content_str]).await
            .with_context(|| format!("Failed to upsert config into PostgreSQL table '{}'", self.table_name))?;
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    fn descriptor(&self) -> String {
        format!("PostgreSQL: '{}' (table: '{}', key: '{}')", sanitize_url(&self.database_url), self.table_name, self.key)
    }
}

// ---------------------------------------------------------------------------
// Factory & Resolution (Strict & Explicit Fail-Fast)
// ---------------------------------------------------------------------------

pub struct ConfigStoreFactory;

impl ConfigStoreFactory {
    pub async fn create(config: &SpectraConfigStoreConfig) -> Result<Arc<dyn ConfigStore>> {
        let backend = std::env::var("SPECTRA_CONFIG_STORE_BACKEND")
            .or_else(|_| std::env::var("SPECTRAGQL_CONFIG_STORE_BACKEND"))
            .unwrap_or_else(|_| config.backend.clone())
            .to_ascii_lowercase();

        match backend.as_str() {
            "memory" => {
                log::info!("ConfigStore: Initialized in-memory storage (ephemeral)");
                Ok(Arc::new(MemoryConfigStore::new(None)))
            }
            "file" => {
                let file_path = std::env::var("SPECTRA_CONFIG_FILE")
                    .or_else(|_| std::env::var("SPECTRAGQL_CONFIG_FILE"))
                    .or_else(|_| std::env::var("SPECTRA_CONFIG"))
                    .or_else(|_| std::env::var("SPECTRAGQL_CONFIG"))
                    .or_else(|_| config.file_path.clone().ok_or(std::env::VarError::NotPresent))
                    .unwrap_or_else(|_| "spectra.toml".to_string());

                log::info!("ConfigStore: Initialized file/volume storage at '{}'", file_path);
                Ok(Arc::new(FileConfigStore::new(file_path)))
            }
            "redis" => {
                let redis_url = std::env::var("SPECTRA_REDIS_URL")
                    .or_else(|_| std::env::var("SPECTRAGQL_REDIS_URL"))
                    .or_else(|_| std::env::var("REDIS_URL"))
                    .or_else(|_| config.redis_url.clone().ok_or(std::env::VarError::NotPresent))
                    .map_err(|_| anyhow!("Config store backend is 'redis', but neither SPECTRA_REDIS_URL nor REDIS_URL environment variables or config.redis_url are configured."))?;

                let store = RedisConfigStore::new(&redis_url, &config.key)?;
                log::info!("ConfigStore: Initialized Redis storage for key '{}'", config.key);
                Ok(Arc::new(store))
            }
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" | "rdbms" => {
                let database_url = std::env::var("SPECTRA_DATABASE_URL")
                    .or_else(|_| std::env::var("SPECTRAGQL_DATABASE_URL"))
                    .or_else(|_| std::env::var("DATABASE_URL"))
                    .or_else(|_| config.database_url.clone().ok_or(std::env::VarError::NotPresent))
                    .map_err(|_| anyhow!("Config store backend is 'postgres', but neither SPECTRA_DATABASE_URL nor DATABASE_URL environment variables or config.database_url are configured."))?;

                let store = PostgresConfigStore::new(&database_url, &config.table_name, &config.key)?;
                store.ensure_table().await?;
                log::info!(
                    "ConfigStore: Initialized PostgreSQL storage (table: '{}', key: '{}')",
                    config.table_name,
                    config.key
                );
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            "postgres" | "postgresql" | "rdbms" => {
                bail!("Config store backend 'postgres' requested, but SpectraGQL was compiled without the 'postgres' feature.");
            }
            unknown => {
                bail!(
                    "Unknown config store backend '{}'. Valid backends are: 'postgres', 'redis', 'file', 'memory'.",
                    unknown
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_config_store() {
        let store = MemoryConfigStore::new(Some("initial content".to_string()));
        assert_eq!(store.backend_name(), "memory");
        assert_eq!(store.descriptor(), "In-Memory (Ephemeral)");

        let loaded = store.load_config().await.unwrap();
        assert_eq!(loaded, Some("initial content".to_string()));

        store.save_config("updated content").await.unwrap();
        let loaded2 = store.load_config().await.unwrap();
        assert_eq!(loaded2, Some("updated content".to_string()));
    }

    #[tokio::test]
    async fn test_file_config_store_atomic_write_and_read() {
        let test_dir = std::path::PathBuf::from("./target/test_store");
        let test_file = test_dir.join("spectra_test.toml");
        let file_path = test_file.to_str().unwrap().to_string();

        let store = FileConfigStore::new(file_path.clone());
        assert_eq!(store.backend_name(), "file");
        assert!(store.descriptor().contains("spectra_test.toml"));

        store
            .save_config("bind_addr = \"0.0.0.0:9090\"\n")
            .await
            .unwrap();

        let content = store.load_config().await.unwrap();
        assert_eq!(content, Some("bind_addr = \"0.0.0.0:9090\"\n".to_string()));

        // Check file exists on filesystem
        assert!(test_file.exists());
        let read_fs = std::fs::read_to_string(&test_file).unwrap();
        assert_eq!(read_fs, "bind_addr = \"0.0.0.0:9090\"\n");

        // Clean up
        let _ = std::fs::remove_file(&test_file);
        let _ = std::fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_config_store_factory_memory() {
        let cfg = SpectraConfigStoreConfig {
            backend: "memory".to_string(),
            ..Default::default()
        };
        let store = ConfigStoreFactory::create(&cfg).await.unwrap();
        assert_eq!(store.backend_name(), "memory");
    }

    #[tokio::test]
    async fn test_config_store_factory_file_volume_mount() {
        let cfg = SpectraConfigStoreConfig {
            backend: "file".to_string(),
            file_path: Some("./target/test_volume.toml".to_string()),
            ..Default::default()
        };
        let store = ConfigStoreFactory::create(&cfg).await.unwrap();
        assert_eq!(store.backend_name(), "file");
        assert!(store.descriptor().contains("test_volume.toml"));
    }

    #[tokio::test]
    async fn test_config_store_factory_fail_fast_missing_postgres_url() {
        // Save and clear env vars
        let prev_spectra = std::env::var("SPECTRA_DATABASE_URL").ok();
        let prev_spectragql = std::env::var("SPECTRAGQL_DATABASE_URL").ok();
        let prev_db = std::env::var("DATABASE_URL").ok();

        unsafe {
            std::env::remove_var("SPECTRA_DATABASE_URL");
            std::env::remove_var("SPECTRAGQL_DATABASE_URL");
            std::env::remove_var("DATABASE_URL");
        }

        let cfg = SpectraConfigStoreConfig {
            backend: "postgres".to_string(),
            database_url: None,
            ..Default::default()
        };
        let res = ConfigStoreFactory::create(&cfg).await;
        match res {
            Ok(_) => panic!("Expected error due to missing postgres connection URL"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("neither SPECTRA_DATABASE_URL nor DATABASE_URL"));
            }
        }

        // Restore env vars
        unsafe {
            if let Some(v) = prev_spectra { std::env::set_var("SPECTRA_DATABASE_URL", v); }
            if let Some(v) = prev_spectragql { std::env::set_var("SPECTRAGQL_DATABASE_URL", v); }
            if let Some(v) = prev_db { std::env::set_var("DATABASE_URL", v); }
        }
    }

    #[tokio::test]
    async fn test_config_store_factory_fail_fast_missing_redis_url() {
        let prev_spectra = std::env::var("SPECTRA_REDIS_URL").ok();
        let prev_spectragql = std::env::var("SPECTRAGQL_REDIS_URL").ok();
        let prev_redis = std::env::var("REDIS_URL").ok();

        unsafe {
            std::env::remove_var("SPECTRA_REDIS_URL");
            std::env::remove_var("SPECTRAGQL_REDIS_URL");
            std::env::remove_var("REDIS_URL");
        }

        let cfg = SpectraConfigStoreConfig {
            backend: "redis".to_string(),
            redis_url: None,
            ..Default::default()
        };
        let res = ConfigStoreFactory::create(&cfg).await;
        match res {
            Ok(_) => panic!("Expected error due to missing redis connection URL"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("neither SPECTRA_REDIS_URL nor REDIS_URL"));
            }
        }

        unsafe {
            if let Some(v) = prev_spectra { std::env::set_var("SPECTRA_REDIS_URL", v); }
            if let Some(v) = prev_spectragql { std::env::set_var("SPECTRAGQL_REDIS_URL", v); }
            if let Some(v) = prev_redis { std::env::set_var("REDIS_URL", v); }
        }
    }

    #[tokio::test]
    async fn test_config_store_factory_unknown_backend() {
        let cfg = SpectraConfigStoreConfig {
            backend: "dynamodb".to_string(),
            ..Default::default()
        };
        let res = ConfigStoreFactory::create(&cfg).await;
        match res {
            Ok(_) => panic!("Expected error for unknown backend"),
            Err(e) => {
                let err = e.to_string();
                assert!(err.contains("Unknown config store backend 'dynamodb'"));
            }
        }
    }
}
