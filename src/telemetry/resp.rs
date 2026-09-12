use std::sync::Arc;
use anyhow::{Result, anyhow};
use redis::aio::ConnectionManager;
use redis::Client;
use tokio::sync::OnceCell;

pub fn normalize_redis_url(addr: &str) -> String {
    let trimmed = addr.trim();
    if trimmed.starts_with("redis://") || trimmed.starts_with("rediss://") {
        trimmed.to_string()
    } else if let Some(stripped) = trimmed.strip_prefix("sierradb://") {
        format!("redis://{}", stripped)
    } else if let Some(stripped) = trimmed.strip_prefix("valkey://") {
        format!("redis://{}", stripped)
    } else {
        format!("redis://{}", trimmed)
    }
}

#[derive(Clone)]
pub struct RespClient {
    addr: String,
    manager: Arc<OnceCell<ConnectionManager>>,
}

impl RespClient {
    pub fn new(addr: &str) -> Self {
        RespClient {
            addr: normalize_redis_url(addr),
            manager: Arc::new(OnceCell::new()),
        }
    }

    #[allow(dead_code)]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn get_connection(&self) -> Result<ConnectionManager> {
        let mgr = self
            .manager
            .get_or_try_init(|| async {
                log::info!("Connecting to RESP server at {}...", self.addr);
                let client = Client::open(self.addr.as_str())
                    .map_err(|e| anyhow!("Failed to create RESP client for {}: {}", self.addr, e))?;
                let manager = ConnectionManager::new(client)
                    .await
                    .map_err(|e| anyhow!("Failed to initialize RESP ConnectionManager for {}: {}", self.addr, e))?;
                log::info!("Connected to RESP server at {} successfully", self.addr);
                Ok::<ConnectionManager, anyhow::Error>(manager)
            })
            .await?;
        Ok(mgr.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_redis_url() {
        assert_eq!(normalize_redis_url("127.0.0.1:6379"), "redis://127.0.0.1:6379");
        assert_eq!(normalize_redis_url("localhost:6379"), "redis://localhost:6379");
        assert_eq!(normalize_redis_url("redis://127.0.0.1:6379"), "redis://127.0.0.1:6379");
        assert_eq!(normalize_redis_url("rediss://secure-redis.io:6380"), "rediss://secure-redis.io:6380");
        assert_eq!(normalize_redis_url("sierradb://127.0.0.1:8848"), "redis://127.0.0.1:8848");
        assert_eq!(normalize_redis_url("valkey://127.0.0.1:6379"), "redis://127.0.0.1:6379");
    }
}
