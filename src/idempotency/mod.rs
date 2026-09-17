use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use http::HeaderMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::core::clock::HlcTimestamp;
use crate::telemetry::resp::RespClient;

pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(300); // 5 minutes
pub const DEFAULT_MAX_CAPACITY: usize = 10_000;
pub const DEFAULT_REDIS_KEY_PREFIX: &str = "spectra:idempotency";
const NUM_MEMORY_SHARDS: usize = 32;

pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub const ALT_IDEMPOTENCY_KEY_HEADER: &str = "x-idempotency-key";
pub const REPLAY_HEADER: &str = "x-spectra-idempotent-replay";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IdempotencyRecord {
    InProgress {
        hlc: HlcTimestamp,
        #[serde(default)]
        started_at_secs: u64,
    },
    Completed {
        hlc: HlcTimestamp,
        status_code: u16,
        headers: Vec<(String, String)>,
        body: String,
        #[serde(default)]
        completed_at_secs: u64,
    },
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyOutcome {
    /// Lock acquired for new operation. Proceed to execute upstream.
    New,
    /// An operation with the same idempotency key is currently in-flight.
    Conflict {
        hlc: HlcTimestamp,
    },
    /// Operation was already completed. Replay cached response without touching upstream.
    Replay {
        hlc: HlcTimestamp,
        status_code: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
}

pub struct MemoryShard {
    cache: Mutex<lru::LruCache<String, IdempotencyRecord>>,
}

impl MemoryShard {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap_or(NonZeroUsize::MIN);
        Self {
            cache: Mutex::new(lru::LruCache::new(cap)),
        }
    }
}

#[inline]
fn get_shard_index(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % NUM_MEMORY_SHARDS
}

pub enum IdempotencyBackend {
    Memory {
        shards: Box<[MemoryShard; NUM_MEMORY_SHARDS]>,
        max_capacity: usize,
        len: std::sync::atomic::AtomicUsize,
    },
    Redis {
        client: RespClient,
        key_prefix: String,
    },
}

/// Sliding window edge deduplication & replay cache for GraphQL mutations & HTTP requests.
/// Supports both in-memory local storage and distributed Redis/Valkey cache.
pub struct IdempotencyEngine {
    backend: IdempotencyBackend,
    ttl: Duration,
}

impl IdempotencyEngine {
    /// Creates an in-memory IdempotencyEngine.
    pub fn new(ttl: Duration, max_capacity: usize) -> Self {
        let shards = Box::new(std::array::from_fn(|_| MemoryShard::new(max_capacity)));
        IdempotencyEngine {
            backend: IdempotencyBackend::Memory {
                shards,
                max_capacity,
                len: std::sync::atomic::AtomicUsize::new(0),
            },
            ttl,
        }
    }

    /// Creates a distributed Redis-backed IdempotencyEngine.
    pub fn new_redis(addr: &str, ttl: Duration) -> Self {
        Self::new_redis_with_prefix(addr, ttl, DEFAULT_REDIS_KEY_PREFIX)
    }

    /// Creates a distributed Redis-backed IdempotencyEngine with custom key prefix.
    pub fn new_redis_with_prefix(addr: &str, ttl: Duration, prefix: &str) -> Self {
        IdempotencyEngine {
            backend: IdempotencyBackend::Redis {
                client: RespClient::new(addr),
                key_prefix: prefix.to_string(),
            },
            ttl,
        }
    }

    /// Checks if a key already exists. If not (or expired), registers it as `InProgress`.
    pub async fn check_or_insert(&self, key: &str, hlc: HlcTimestamp) -> IdempotencyOutcome {
        match &self.backend {
            IdempotencyBackend::Memory { shards, max_capacity, len } => {
                let now_secs = current_unix_secs();
                let ttl_secs = self.ttl.as_secs().max(1);
                let shard_idx = get_shard_index(key);
                let mut guard = shards[shard_idx].cache.lock();

                if let Some(record) = guard.get(key) {
                    match record {
                        IdempotencyRecord::InProgress { started_at_secs, hlc } => {
                            if now_secs.saturating_sub(*started_at_secs) < ttl_secs {
                                return IdempotencyOutcome::Conflict { hlc: *hlc };
                            }
                        }
                        IdempotencyRecord::Completed {
                            completed_at_secs,
                            hlc,
                            status_code,
                            headers,
                            body,
                        } => {
                            if now_secs.saturating_sub(*completed_at_secs) < ttl_secs {
                                return IdempotencyOutcome::Replay {
                                    hlc: *hlc,
                                    status_code: *status_code,
                                    headers: headers.clone(),
                                    body: body.clone(),
                                };
                            }
                        }
                    }
                }

                // Global capacity check: if total length across all shards is at or above max_capacity,
                // evict least recently used entry in O(1).
                if len.load(std::sync::atomic::Ordering::Relaxed) >= *max_capacity {
                    if guard.pop_lru().is_some() {
                        len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        for (i, shard) in shards.iter().enumerate() {
                            if i != shard_idx {
                                if let Some(mut other) = shard.cache.try_lock() {
                                    if other.pop_lru().is_some() {
                                        len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if guard.put(
                    key.to_string(),
                    IdempotencyRecord::InProgress {
                        started_at_secs: now_secs,
                        hlc,
                    },
                ).is_none() {
                    len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                IdempotencyOutcome::New
            }
            IdempotencyBackend::Redis { client, key_prefix } => {
                let redis_key = format!("{}:{}", key_prefix, key);
                let now_secs = current_unix_secs();
                let in_progress = IdempotencyRecord::InProgress {
                    hlc,
                    started_at_secs: now_secs,
                };
                let in_progress_json = match serde_json::to_string(&in_progress) {
                    Ok(j) => j,
                    Err(e) => {
                        log::error!("Failed to serialize in_progress record: {}", e);
                        return IdempotencyOutcome::New;
                    }
                };
                let ttl_secs = self.ttl.as_secs().max(1);

                let mut conn = match client.get_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("Idempotency Redis connection failed: {}, failing open", e);
                        return IdempotencyOutcome::New;
                    }
                };

                let mut set_cmd = redis::cmd("SET");
                set_cmd
                    .arg(&redis_key)
                    .arg(&in_progress_json)
                    .arg("NX")
                    .arg("EX")
                    .arg(ttl_secs);

                let set_result: Result<Option<String>, _> = set_cmd.query_async(&mut conn).await;
                match set_result {
                    Ok(Some(_)) => {
                        // Lock acquired
                        IdempotencyOutcome::New
                    }
                    Ok(None) => {
                        // Key exists - query current state
                        let mut get_cmd = redis::cmd("GET");
                        get_cmd.arg(&redis_key);
                        match get_cmd.query_async::<_, Option<String>>(&mut conn).await {
                            Ok(Some(raw_json)) => {
                                if let Ok(rec) = serde_json::from_str::<IdempotencyRecord>(&raw_json) {
                                    match rec {
                                        IdempotencyRecord::InProgress { hlc, .. } => {
                                            IdempotencyOutcome::Conflict { hlc }
                                        }
                                        IdempotencyRecord::Completed {
                                            hlc,
                                            status_code,
                                            headers,
                                            body,
                                            ..
                                        } => IdempotencyOutcome::Replay {
                                            hlc,
                                            status_code,
                                            headers,
                                            body,
                                        },
                                    }
                                } else {
                                    IdempotencyOutcome::New
                                }
                            }
                            _ => IdempotencyOutcome::New,
                        }
                    }
                    Err(e) => {
                        log::error!("Idempotency Redis SET NX failed: {}, failing open", e);
                        IdempotencyOutcome::New
                    }
                }
            }
        }
    }

    /// Marks an in-flight key as completed, caching its outcome for future replays.
    pub async fn complete(
        &self,
        key: &str,
        hlc: HlcTimestamp,
        status_code: u16,
        headers: &HeaderMap,
        body: &str,
    ) {
        let header_pairs: Vec<(String, String)> = headers
            .iter()
            .filter_map(|(k, v)| {
                let name = k.as_str().to_lowercase();
                // Strip hop-by-hop headers
                if name == "connection" || name == "keep-alive" || name == "transfer-encoding" {
                    None
                } else {
                    v.to_str().ok().map(|val| (name, val.to_string()))
                }
            })
            .collect();

        match &self.backend {
            IdempotencyBackend::Memory { shards, len, .. } => {
                let shard_idx = get_shard_index(key);
                let mut guard = shards[shard_idx].cache.lock();
                if guard.put(
                    key.to_string(),
                    IdempotencyRecord::Completed {
                        completed_at_secs: current_unix_secs(),
                        hlc,
                        status_code,
                        headers: header_pairs,
                        body: body.to_string(),
                    },
                ).is_none() {
                    len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            IdempotencyBackend::Redis { client, key_prefix } => {
                let redis_key = format!("{}:{}", key_prefix, key);
                let completed = IdempotencyRecord::Completed {
                    completed_at_secs: current_unix_secs(),
                    hlc,
                    status_code,
                    headers: header_pairs,
                    body: body.to_string(),
                };
                if let Ok(json_str) = serde_json::to_string(&completed) {
                    let ttl_secs = self.ttl.as_secs().max(1);
                    if let Ok(mut conn) = client.get_connection().await {
                        let mut cmd = redis::cmd("SET");
                        cmd.arg(&redis_key).arg(&json_str).arg("EX").arg(ttl_secs);
                        let _: Result<(), _> = cmd.query_async(&mut conn).await;
                    }
                }
            }
        }
    }

    /// Removes an in-flight entry so the client can retry if an error occurred before completion.
    pub async fn remove(&self, key: &str) {
        match &self.backend {
            IdempotencyBackend::Memory { shards, len, .. } => {
                let shard_idx = get_shard_index(key);
                let mut guard = shards[shard_idx].cache.lock();
                if guard.pop(key).is_some() {
                    len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            IdempotencyBackend::Redis { client, key_prefix } => {
                let redis_key = format!("{}:{}", key_prefix, key);
                if let Ok(mut conn) = client.get_connection().await {
                    let mut cmd = redis::cmd("DEL");
                    cmd.arg(&redis_key);
                    let _: Result<(), _> = cmd.query_async(&mut conn).await;
                }
            }
        }
    }

    /// Returns the backend type name ("memory" or "redis").
    pub fn backend_name(&self) -> &'static str {
        match &self.backend {
            IdempotencyBackend::Memory { .. } => "memory",
            IdempotencyBackend::Redis { .. } => "redis",
        }
    }

    /// Returns the configured idempotency TTL in seconds.
    pub fn ttl_secs(&self) -> u64 {
        self.ttl.as_secs()
    }

    /// Returns the maximum configured capacity (for in-memory backend).
    pub fn max_capacity(&self) -> usize {
        match &self.backend {
            IdempotencyBackend::Memory { max_capacity, .. } => *max_capacity,
            IdempotencyBackend::Redis { .. } => 0,
        }
    }

    /// Returns the count of active records currently held in memory.
    pub fn active_record_count(&self) -> usize {
        match &self.backend {
            IdempotencyBackend::Memory { len, .. } => {
                len.load(std::sync::atomic::Ordering::Relaxed)
            }
            IdempotencyBackend::Redis { .. } => 0,
        }
    }

    /// Helper to extract idempotency key from incoming HTTP headers.
    pub fn extract_idempotency_key(headers: &HeaderMap) -> Option<String> {
        if let Some(val) = headers.get(IDEMPOTENCY_KEY_HEADER) {
            if let Ok(s) = val.to_str() {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
        if let Some(val) = headers.get(ALT_IDEMPOTENCY_KEY_HEADER) {
            if let Ok(s) = val.to_str() {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
        None
    }

    /// Computes a deterministic SHA-256 fingerprint for a request when no explicit header is provided.
    pub fn compute_fingerprint(
        client_id: &str,
        operation_name: Option<&str>,
        body: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(client_id.as_bytes());
        hasher.update(b":");
        hasher.update(operation_name.unwrap_or("anonymous").as_bytes());
        hasher.update(b":");
        hasher.update(body.as_bytes());
        let result = hasher.finalize();
        format!("sha256:{:x}", result)
    }
}

impl Default for IdempotencyEngine {
    fn default() -> Self {
        Self::new(DEFAULT_IDEMPOTENCY_TTL, DEFAULT_MAX_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[tokio::test]
    async fn test_idempotency_new_and_complete() {
        let engine = IdempotencyEngine::new(Duration::from_secs(60), 100);
        let hlc = HlcTimestamp::new(1000, 0);

        // 1. Initial check is New
        let outcome = engine.check_or_insert("key-1", hlc).await;
        assert_eq!(outcome, IdempotencyOutcome::New);

        // 2. In-flight check returns Conflict
        let outcome = engine.check_or_insert("key-1", hlc).await;
        assert_eq!(outcome, IdempotencyOutcome::Conflict { hlc });

        // 3. Mark complete
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        engine
            .complete("key-1", hlc, 200, &headers, r#"{"data":{"ok":true}}"#)
            .await;

        // 4. Next check returns Replay with cached response
        let outcome = engine.check_or_insert("key-1", hlc).await;
        match outcome {
            IdempotencyOutcome::Replay {
                status_code,
                headers,
                body,
                ..
            } => {
                assert_eq!(status_code, 200);
                assert_eq!(body, r#"{"data":{"ok":true}}"#);
                assert!(headers.iter().any(|(k, v)| k == "content-type" && v == "application/json"));
            }
            _ => panic!("Expected Replay outcome"),
        }
    }

    #[tokio::test]
    async fn test_idempotency_remove_resets_state() {
        let engine = IdempotencyEngine::new(Duration::from_secs(60), 100);
        let hlc = HlcTimestamp::new(1000, 0);

        let outcome = engine.check_or_insert("retry-key", hlc).await;
        assert_eq!(outcome, IdempotencyOutcome::New);

        // Upstream failed, remove key
        engine.remove("retry-key").await;

        // Key should be available again as New
        let outcome = engine.check_or_insert("retry-key", hlc).await;
        assert_eq!(outcome, IdempotencyOutcome::New);
    }

    #[test]
    fn test_record_json_serialization() {
        let hlc = HlcTimestamp::new(1725980000000, 1);
        let in_progress = IdempotencyRecord::InProgress {
            hlc,
            started_at_secs: 1725980000,
        };
        let json = serde_json::to_string(&in_progress).unwrap();
        assert!(json.contains("\"state\":\"in_progress\""));
        assert!(json.contains("\"started_at_secs\":1725980000"));

        let deserialized: IdempotencyRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(in_progress, deserialized);

        let completed = IdempotencyRecord::Completed {
            hlc,
            status_code: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: r#"{"data":{"result":"ok"}}"#.to_string(),
            completed_at_secs: 1725980005,
        };
        let comp_json = serde_json::to_string(&completed).unwrap();
        assert!(comp_json.contains("\"state\":\"completed\""));
        assert!(comp_json.contains("\"status_code\":200"));
        assert!(comp_json.contains("content-type"));

        let comp_deserialized: IdempotencyRecord = serde_json::from_str(&comp_json).unwrap();
        assert_eq!(completed, comp_deserialized);
    }

    #[test]
    fn test_idempotency_header_extraction() {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_static("test-key-abc"));
        assert_eq!(
            IdempotencyEngine::extract_idempotency_key(&headers),
            Some("test-key-abc".to_string())
        );

        let mut alt_headers = HeaderMap::new();
        alt_headers.insert("x-idempotency-key", HeaderValue::from_static("test-alt-xyz"));
        assert_eq!(
            IdempotencyEngine::extract_idempotency_key(&alt_headers),
            Some("test-alt-xyz".to_string())
        );

        let empty_headers = HeaderMap::new();
        assert_eq!(
            IdempotencyEngine::extract_idempotency_key(&empty_headers),
            None
        );
    }

    #[test]
    fn test_compute_fingerprint() {
        let fp1 = IdempotencyEngine::compute_fingerprint("client-1", Some("CreateUser"), r#"{"name":"Alice"}"#);
        let fp2 = IdempotencyEngine::compute_fingerprint("client-1", Some("CreateUser"), r#"{"name":"Alice"}"#);
        let fp3 = IdempotencyEngine::compute_fingerprint("client-2", Some("CreateUser"), r#"{"name":"Alice"}"#);

        assert_eq!(fp1, fp2);
        assert_ne!(fp1, fp3);
        assert!(fp1.starts_with("sha256:"));
    }
}
