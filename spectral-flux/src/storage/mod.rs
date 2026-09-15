use anyhow::{anyhow, Result};
use std::sync::Arc;
#[cfg(feature = "kevy")]
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

#[cfg(all(test, feature = "kevy"))]
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

    #[tokio::test]
    async fn test_kevy_storage_non_existent_key_operations() {
        let storage = KevyStorage::new_in_memory().unwrap();

        // Getting a missing key returns None
        let val = storage.get("missing_key_xyz").await.unwrap();
        assert_eq!(val, None);

        // Deleting a missing key returns false
        let deleted = storage.delete("missing_key_xyz").await.unwrap();
        assert!(!deleted);

        // getdel on missing key returns None
        let getdel_val = storage.get_del("missing_key_xyz").await.unwrap();
        assert_eq!(getdel_val, None);
    }

    #[tokio::test]
    async fn test_kevy_storage_overwrite_and_multi_key_isolation() {
        let storage = KevyStorage::new_in_memory().unwrap();

        // Set initial key
        assert!(storage.set("cell:key_a", "val_1", 0).await.unwrap());
        assert_eq!(storage.get("cell:key_a").await.unwrap().as_deref(), Some("val_1"));

        // Overwrite key
        assert!(storage.set("cell:key_a", "val_2", 0).await.unwrap());
        assert_eq!(storage.get("cell:key_a").await.unwrap().as_deref(), Some("val_2"));

        // Set distinct key B
        assert!(storage.set("cell:key_b", "val_b", 0).await.unwrap());
        assert_eq!(storage.get("cell:key_a").await.unwrap().as_deref(), Some("val_2"));
        assert_eq!(storage.get("cell:key_b").await.unwrap().as_deref(), Some("val_b"));

        // Delete key A, verify key B unaffected
        assert!(storage.delete("cell:key_a").await.unwrap());
        assert_eq!(storage.get("cell:key_a").await.unwrap(), None);
        assert_eq!(storage.get("cell:key_b").await.unwrap().as_deref(), Some("val_b"));
    }

    #[tokio::test]
    async fn test_kevy_storage_ttl_expiration() {
        let storage = KevyStorage::new_in_memory().unwrap();

        // Set key with 1 second TTL
        storage.set("ephemeral_token", "temporary_secret", 1).await.unwrap();
        assert_eq!(storage.get("ephemeral_token").await.unwrap().as_deref(), Some("temporary_secret"));

        // Wait 1.1s for TTL expiry
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Key should now be expired and return None
        let expired_val = storage.get("ephemeral_token").await.unwrap();
        assert_eq!(expired_val, None);
    }

    #[tokio::test]
    async fn test_kevy_storage_persistence() {
        let temp_dir = std::env::temp_dir().join(format!("spectral_kevy_test_{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.to_str().unwrap().to_string();

        {
            let storage = KevyStorage::new_with_persist(&db_path).unwrap();
            storage.set("persisted_token", "persisted_value", 0).await.unwrap();
            let val = storage.get("persisted_token").await.unwrap();
            assert_eq!(val.as_deref(), Some("persisted_value"));
        }

        // Re-open from same path
        {
            let storage = KevyStorage::new_with_persist(&db_path).unwrap();
            let val = storage.get("persisted_token").await.unwrap();
            assert_eq!(val.as_deref(), Some("persisted_value"));
        }

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_create_storage_factory() {
        let storage_kevy = create_storage("embedded_kevy", None).await;
        assert!(storage_kevy.is_ok());

        let storage_alias = create_storage("kevy", None).await;
        assert!(storage_alias.is_ok());

        let storage_invalid = create_storage("unknown_driver_xyz", None).await;
        match storage_invalid {
            Err(e) => assert!(e.to_string().contains("Unsupported storage backend")),
            Ok(_) => panic!("Expected storage_invalid to fail"),
        }
    }

    #[tokio::test]
    async fn test_storage_adversarial_corrupted_disk_file() {
        let temp_dir = std::env::temp_dir().join(format!("spectral_corrupt_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_file = temp_dir.join("corrupted.db");
        // Write raw garbage into the persistent file
        std::fs::write(&db_file, b"TOTAL_CORRUPTED_GARBAGE_RANDOM_BYTES_HEADER_12345").unwrap();

        let db_path = db_file.to_str().unwrap();
        // Opening should either error or safely handle without panicking or segfaulting
        let res = KevyStorage::new_with_persist(db_path);
        // Ensure result is handled safely
        match res {
            Ok(store) => {
                // If it opened by re-initializing fresh state, ensure it doesn't crash on operations
                let _ = store.get("test").await;
            }
            Err(_) => {
                // Properly detected file corruption and rejected
            }
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_storage_adversarial_high_concurrency_race() {
        let storage = Arc::new(KevyStorage::new_in_memory().unwrap());
        let num_tasks = 40;
        let iterations = 50;

        let mut handles = Vec::new();

        for task_id in 0..num_tasks {
            let store_clone = storage.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..iterations {
                    let key = format!("race:key:{}", (task_id + i) % 5);
                    let val = format!("val-{}-{}", task_id, i);

                    let _ = store_clone.set(&key, &val, 0).await;
                    let _ = store_clone.get(&key).await;

                    if i % 3 == 0 {
                        let _ = store_clone.get_del(&key).await;
                    }
                    if i % 5 == 0 {
                        let _ = store_clone.delete(&key).await;
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_storage_adversarial_large_payload() {
        let storage = KevyStorage::new_in_memory().unwrap();
        // 2MB payload
        let large_str = "A".repeat(2 * 1024 * 1024);

        assert!(storage.set("large_key", &large_str, 0).await.unwrap());
        let retrieved = storage.get("large_key").await.unwrap();
        assert_eq!(retrieved.map(|s| s.len()), Some(2 * 1024 * 1024));
    }

    #[tokio::test]
    async fn test_storage_adversarial_special_key_characters() {
        let storage = KevyStorage::new_in_memory().unwrap();
        let weird_keys = vec![
            "user:email+tag@example.com/oauth/callback",
            "emoji:🚀:auth:🔥",
            "spaces and tabs \t \r in key",
            "unicode:日本語:ключ:مفتاح",
        ];

        for k in &weird_keys {
            assert!(storage.set(k, "valid_value", 0).await.unwrap());
            let val = storage.get(k).await.unwrap();
            assert_eq!(val.as_deref(), Some("valid_value"));
            assert!(storage.delete(k).await.unwrap());
            assert_eq!(storage.get(k).await.unwrap(), None);
        }
    }
}
