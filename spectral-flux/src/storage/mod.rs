use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::Duration;

#[async_trait::async_trait]
pub trait FluxStorage: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool>;
    async fn delete(&self, key: &str) -> Result<bool>;
    async fn get_del(&self, key: &str) -> Result<Option<String>>;
}

#[cfg(feature = "kevy")]
pub struct KevyStorage {
    store: Arc<kevy_embedded::Store>,
}

#[cfg(feature = "kevy")]
impl KevyStorage {
    pub fn new_in_memory() -> Result<Self> {
        let config = kevy_embedded::Config::default();
        let store = kevy_embedded::Store::open(config)
            .map_err(|e| anyhow!("Failed to open kevy-embedded store: {:?}", e))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    pub fn new_with_persist(path: &str) -> Result<Self> {
        let config = kevy_embedded::Config::default().with_persist(path);
        let store = kevy_embedded::Store::open(config)
            .map_err(|e| anyhow!("Failed to open persistent kevy-embedded store at {}: {:?}", path, e))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }
}

#[cfg(feature = "kevy")]
#[async_trait::async_trait]
impl FluxStorage for KevyStorage {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let res = self.store.get(key.as_bytes())
            .map_err(|e| anyhow!("kevy get error: {:?}", e))?;
        Ok(res.and_then(|bytes| String::from_utf8(bytes).ok()))
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        if ttl_seconds > 0 {
            let ttl = Duration::from_secs(ttl_seconds);
            self.store.set_with_ttl(key.as_bytes(), value.as_bytes(), ttl)
                .map_err(|e| anyhow!("kevy set_with_ttl error: {:?}", e))
        } else {
            self.store.set(key.as_bytes(), value.as_bytes())
                .map_err(|e| anyhow!("kevy set error: {:?}", e))
        }
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let count = self.store.del(&[key.as_bytes()])
            .map_err(|e| anyhow!("kevy del error: {:?}", e))?;
        Ok(count > 0)
    }

    async fn get_del(&self, key: &str) -> Result<Option<String>> {
        let res = self.store.getdel(key.as_bytes())
            .map_err(|e| anyhow!("kevy getdel error: {:?}", e))?;
        Ok(res.and_then(|bytes| String::from_utf8(bytes).ok()))
    }
}

pub struct RedisStorage {
    connection_manager: redis::aio::ConnectionManager,
}

impl RedisStorage {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let connection_manager = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self { connection_manager })
    }
}

#[async_trait::async_trait]
impl FluxStorage for RedisStorage {
    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.connection_manager.clone();
        let res: Option<String> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        Ok(res)
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        let mut conn = self.connection_manager.clone();
        let mut cmd = redis::cmd("SET");
        cmd.arg(key).arg(value);
        if ttl_seconds > 0 {
            cmd.arg("EX").arg(ttl_seconds);
        }
        let res: Option<String> = cmd.query_async(&mut conn).await?;
        Ok(res.is_some())
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let mut conn = self.connection_manager.clone();
        let deleted: u64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        Ok(deleted > 0)
    }

    async fn get_del(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.connection_manager.clone();
        let res: Option<String> = redis::cmd("GETDEL")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        Ok(res)
    }
}

pub async fn create_storage(backend: &str, addr: Option<&str>) -> Result<Arc<dyn FluxStorage>> {
    match backend {
        #[cfg(feature = "kevy")]
        "embedded_kevy" | "kevy" => {
            if let Some(path) = addr.filter(|p| !p.starts_with("redis://") && !p.is_empty()) {
                Ok(Arc::new(KevyStorage::new_with_persist(path)?))
            } else {
                Ok(Arc::new(KevyStorage::new_in_memory()?))
            }
        }
        "redis" | "valkey" => {
            let url = addr.unwrap_or("redis://127.0.0.1:6379");
            Ok(Arc::new(RedisStorage::new(url).await?))
        }
        other => Err(anyhow!("Unsupported storage backend: '{}'", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kevy_storage_basic_and_getdel() {
        let storage = KevyStorage::new_in_memory().unwrap();
        
        // Basic set & get
        let ok = storage.set("magic_link:tok123", "user@example.com", 60).await.unwrap();
        assert!(ok);
        let val = storage.get("magic_link:tok123").await.unwrap();
        assert_eq!(val.as_deref(), Some("user@example.com"));

        // Atomic getdel
        let redeemed = storage.get_del("magic_link:tok123").await.unwrap();
        assert_eq!(redeemed.as_deref(), Some("user@example.com"));

        // Second getdel should return None (already redeemed/deleted)
        let redeemed_again = storage.get_del("magic_link:tok123").await.unwrap();
        assert_eq!(redeemed_again, None);
    }
}
