use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use sha2::{Digest, Sha256};
use http::HeaderMap;

use crate::clock::HlcTimestamp;

pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(300); // 5 minutes
pub const DEFAULT_MAX_CAPACITY: usize = 10_000;

pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub const ALT_IDEMPOTENCY_KEY_HEADER: &str = "x-idempotency-key";
pub const REPLAY_HEADER: &str = "x-spectra-idempotent-replay";

#[derive(Clone, Debug)]
pub enum IdempotencyRecord {
    InProgress {
        started_at: Instant,
        hlc: HlcTimestamp,
    },
    Completed {
        completed_at: Instant,
        hlc: HlcTimestamp,
        status_code: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
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

/// In-memory sliding window edge deduplication cache for GraphQL mutations & HTTP requests.
pub struct IdempotencyEngine {
    records: RwLock<HashMap<String, IdempotencyRecord>>,
    ttl: Duration,
    max_capacity: usize,
}

impl IdempotencyEngine {
    pub fn new(ttl: Duration, max_capacity: usize) -> Self {
        IdempotencyEngine {
            records: RwLock::new(HashMap::new()),
            ttl,
            max_capacity,
        }
    }

    /// Checks if a key already exists. If not (or expired), registers it as `InProgress`.
    pub fn check_or_insert(&self, key: &str, hlc: HlcTimestamp) -> IdempotencyOutcome {
        let now = Instant::now();

        // 1. Fast read-lock check
        {
            let read_guard = self.records.read().unwrap();
            if let Some(record) = read_guard.get(key) {
                match record {
                    IdempotencyRecord::InProgress { started_at, hlc } => {
                        if now.duration_since(*started_at) < self.ttl {
                            return IdempotencyOutcome::Conflict { hlc: *hlc };
                        }
                    }
                    IdempotencyRecord::Completed {
                        completed_at,
                        hlc,
                        status_code,
                        headers,
                        body,
                    } => {
                        if now.duration_since(*completed_at) < self.ttl {
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
        }

        // 2. Write lock to insert or overwrite expired
        let mut write_guard = self.records.write().unwrap();

        // Double check under write lock
        if let Some(record) = write_guard.get(key) {
            match record {
                IdempotencyRecord::InProgress { started_at, hlc } => {
                    if now.duration_since(*started_at) < self.ttl {
                        return IdempotencyOutcome::Conflict { hlc: *hlc };
                    }
                }
                IdempotencyRecord::Completed {
                    completed_at,
                    hlc,
                    status_code,
                    headers,
                    body,
                } => {
                    if now.duration_since(*completed_at) < self.ttl {
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

        // Enforce capacity bounds
        if write_guard.len() >= self.max_capacity {
            self.prune_expired_locked(&mut write_guard, now);
            // If still full, remove arbitrary entry to protect against unbounded growth
            if write_guard.len() >= self.max_capacity {
                if let Some(first_key) = write_guard.keys().next().cloned() {
                    write_guard.remove(&first_key);
                }
            }
        }

        write_guard.insert(
            key.to_string(),
            IdempotencyRecord::InProgress {
                started_at: now,
                hlc,
            },
        );

        IdempotencyOutcome::New
    }

    /// Marks an in-flight key as completed, caching its outcome for future replays.
    pub fn complete(
        &self,
        key: &str,
        hlc: HlcTimestamp,
        status_code: u16,
        headers: &HeaderMap,
        body: &str,
    ) {
        let mut write_guard = self.records.write().unwrap();
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

        write_guard.insert(
            key.to_string(),
            IdempotencyRecord::Completed {
                completed_at: Instant::now(),
                hlc,
                status_code,
                headers: header_pairs,
                body: body.to_string(),
            },
        );
    }

    /// Removes an in-flight entry so the client can retry if an error occurred before completion.
    pub fn remove(&self, key: &str) {
        let mut write_guard = self.records.write().unwrap();
        write_guard.remove(key);
    }

    /// Prunes expired records under an active write guard.
    fn prune_expired_locked(
        &self,
        records: &mut HashMap<String, IdempotencyRecord>,
        now: Instant,
    ) {
        records.retain(|_, record| match record {
            IdempotencyRecord::InProgress { started_at, .. } => {
                now.duration_since(*started_at) < self.ttl
            }
            IdempotencyRecord::Completed { completed_at, .. } => {
                now.duration_since(*completed_at) < self.ttl
            }
        });
    }

    /// Public method to prune expired entries.
    pub fn prune_expired(&self) {
        let mut write_guard = self.records.write().unwrap();
        self.prune_expired_locked(&mut write_guard, Instant::now());
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

    #[test]
    fn test_idempotency_new_and_complete() {
        let engine = IdempotencyEngine::new(Duration::from_secs(60), 100);
        let hlc = HlcTimestamp::new(1000, 0);

        // 1. Initial check is New
        let outcome = engine.check_or_insert("key-1", hlc);
        assert_eq!(outcome, IdempotencyOutcome::New);

        // 2. In-flight check returns Conflict
        let outcome = engine.check_or_insert("key-1", hlc);
        assert_eq!(outcome, IdempotencyOutcome::Conflict { hlc });

        // 3. Mark complete
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        engine.complete("key-1", hlc, 200, &headers, r#"{"data":{"ok":true}}"#);

        // 4. Next check returns Replay with cached response
        let outcome = engine.check_or_insert("key-1", hlc);
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

    #[test]
    fn test_idempotency_ttl_expiration() {
        // Very short TTL for test
        let engine = IdempotencyEngine::new(Duration::from_millis(10), 100);
        let hlc = HlcTimestamp::new(1000, 0);

        let outcome = engine.check_or_insert("expiring-key", hlc);
        assert_eq!(outcome, IdempotencyOutcome::New);

        // Sleep past TTL
        std::thread::sleep(Duration::from_millis(20));

        // Should be treated as expired and re-acquired as New
        let outcome = engine.check_or_insert("expiring-key", hlc);
        assert_eq!(outcome, IdempotencyOutcome::New);
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
