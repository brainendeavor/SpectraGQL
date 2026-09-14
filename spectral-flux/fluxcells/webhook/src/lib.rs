//! Outbound Webhook Delivery Fluxcell for SpectraGQL
//!
//! Provides reliable outbound event delivery with:
//! - HMAC-SHA256 request signatures
//! - Exponential backoff retry policies
//! - In-memory DLQ buffer & inspection endpoint (/dlq)

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::VecDeque;
use std::sync::Mutex;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookRoute {
    pub method: String,
    pub path: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub id: String,
    pub topic: String,
    pub destination_url: String,
    pub attempts: u32,
    pub last_error: String,
    pub failed_at: String,
}

pub struct WebhookState {
    pub dlq: Mutex<VecDeque<DlqEntry>>,
    pub max_dlq_size: usize,
}

impl WebhookState {
    pub fn new(max_dlq_size: usize) -> Self {
        Self {
            dlq: Mutex::new(VecDeque::with_capacity(max_dlq_size)),
            max_dlq_size,
        }
    }

    pub fn push_dlq(&self, entry: DlqEntry) {
        if let Ok(mut dlq) = self.dlq.lock() {
            if dlq.len() >= self.max_dlq_size {
                dlq.pop_front();
            }
            dlq.push_back(entry);
        }
    }

    pub fn get_dlq_entries(&self) -> Vec<DlqEntry> {
        if let Ok(dlq) = self.dlq.lock() {
            dlq.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }
}

pub fn compute_hmac_sha256(secret: &[u8], payload: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");
    mac.update(payload);
    let result = mac.finalize();
    hex_encode(&result.into_bytes())
}

pub fn verify_signature(secret: &[u8], payload: &[u8], signature_hex: &str) -> bool {
    let sig_clean = signature_hex.trim().trim_start_matches("sha256=");
    let Ok(sig_bytes) = hex_decode(sig_clean) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(payload);
    mac.verify_slice(&sig_bytes).is_ok()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn hex_decode(hex_str: &str) -> Result<Vec<u8>, ()> {
    if hex_str.len() % 2 != 0 {
        return Err(());
    }
    let mut bytes = Vec::with_capacity(hex_str.len() / 2);
    for i in (0..hex_str.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex_str[i..i + 2], 16).map_err(|_| ())?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub fn get_subscriptions() -> Vec<String> {
    vec!["webhook.dispatch".to_string(), "mutation.*".to_string()]
}

pub fn get_routes() -> Vec<WebhookRoute> {
    vec![
        WebhookRoute {
            method: "GET".to_string(),
            path: "/health".to_string(),
            description: "Webhook service health".to_string(),
        },
        WebhookRoute {
            method: "GET".to_string(),
            path: "/dlq".to_string(),
            description: "Dead-letter queue inspection".to_string(),
        },
        WebhookRoute {
            method: "POST".to_string(),
            path: "/test".to_string(),
            description: "Simulate webhook dispatch with HMAC verification".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc_4231_test_vectors() {
        // Test Case 1
        let key1 = [0x0b; 20];
        let data1 = b"Hi There";
        let sig1 = compute_hmac_sha256(&key1, data1);
        assert_eq!(sig1, "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
        assert!(verify_signature(&key1, data1, &sig1));
        assert!(verify_signature(&key1, data1, &format!("sha256={}", sig1)));

        // Test Case 2
        let key2 = b"Jefe";
        let data2 = b"what do ya want for nothing?";
        let sig2 = compute_hmac_sha256(key2, data2);
        assert_eq!(sig2, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
        assert!(verify_signature(key2, data2, &sig2));

        // Test Case 3
        let key3 = [0xaa; 20];
        let data3 = [0xdd; 50];
        let sig3 = compute_hmac_sha256(&key3, &data3);
        assert_eq!(sig3, "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe");
        assert!(verify_signature(&key3, &data3, &sig3));
    }

    #[test]
    fn test_signature_verification_rejection() {
        let secret = b"webhook-secret";
        let payload = b"{\"event\":\"charge.success\"}";
        let valid_sig = compute_hmac_sha256(secret, payload);

        // Valid signature matches
        assert!(verify_signature(secret, payload, &valid_sig));

        // Tampered payload rejected
        assert!(!verify_signature(secret, b"{\"event\":\"charge.failed\"}", &valid_sig));

        // Tampered secret rejected
        assert!(!verify_signature(b"wrong-secret", payload, &valid_sig));

        // Invalid hex characters or odd length rejected cleanly without panic
        assert!(!verify_signature(secret, payload, "invalid-hex-string"));
        assert!(!verify_signature(secret, payload, "abc"));
    }

    #[test]
    fn test_dlq_buffer_capping() {
        let state = WebhookState::new(2);
        assert!(state.get_dlq_entries().is_empty());

        state.push_dlq(DlqEntry {
            id: "1".to_string(),
            topic: "test".to_string(),
            destination_url: "http://fail.com".to_string(),
            attempts: 5,
            last_error: "500 Internal Server Error".to_string(),
            failed_at: "2026-01-01T00:00:00Z".to_string(),
        });
        state.push_dlq(DlqEntry {
            id: "2".to_string(),
            topic: "test".to_string(),
            destination_url: "http://fail.com".to_string(),
            attempts: 5,
            last_error: "Connection timeout".to_string(),
            failed_at: "2026-01-01T00:01:00Z".to_string(),
        });
        state.push_dlq(DlqEntry {
            id: "3".to_string(),
            topic: "test".to_string(),
            destination_url: "http://fail.com".to_string(),
            attempts: 5,
            last_error: "Connection refused".to_string(),
            failed_at: "2026-01-01T00:02:00Z".to_string(),
        });

        let entries = state.get_dlq_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "2");
        assert_eq!(entries[1].id, "3");
    }

    #[test]
    fn test_routes_and_subscriptions() {
        assert_eq!(get_subscriptions().len(), 2);
        assert_eq!(get_routes().len(), 3);
    }

    #[test]
    fn test_webhook_adversarial_bit_flip_fuzzing() {
        let secret = b"super-secret-hmac-key";
        let payload = b"{\"event\":\"user.subscription.created\",\"tier\":\"enterprise\"}";
        let valid_sig = compute_hmac_sha256(secret, payload);

        assert!(verify_signature(secret, payload, &valid_sig));

        // Mutate every character in the 64-char hex string
        for i in 0..valid_sig.len() {
            let mut corrupted_chars: Vec<char> = valid_sig.chars().collect();
            let original_char = corrupted_chars[i];
            // Flip to a different hex char
            corrupted_chars[i] = if original_char == 'a' { 'b' } else { 'a' };
            let corrupted_sig: String = corrupted_chars.into_iter().collect();

            assert!(
                !verify_signature(secret, payload, &corrupted_sig),
                "Corrupted signature at index {} was unexpectedly accepted!",
                i
            );
        }
    }

    #[test]
    fn test_webhook_adversarial_dlq_high_concurrency() {
        use std::sync::Arc;
        let state = Arc::new(WebhookState::new(10));
        let num_threads = 20;
        let items_per_thread = 100;

        let mut handles = Vec::new();
        for t in 0..num_threads {
            let state_clone = state.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..items_per_thread {
                    state_clone.push_dlq(DlqEntry {
                        id: format!("{}-{}", t, i),
                        topic: "retry.webhook".to_string(),
                        destination_url: "https://flaky-partner.com/events".to_string(),
                        attempts: 3,
                        last_error: "Connection timeout".to_string(),
                        failed_at: "2026-09-14T00:00:00Z".to_string(),
                    });
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let final_entries = state.get_dlq_entries();
        assert_eq!(final_entries.len(), 10, "DLQ buffer capacity was not strictly respected under load");
    }

    #[test]
    fn test_webhook_adversarial_empty_and_null_payloads() {
        let secret = b"secret";
        let empty_payload = b"";
        let sig_empty = compute_hmac_sha256(secret, empty_payload);
        assert!(!sig_empty.is_empty());
        assert!(verify_signature(secret, empty_payload, &sig_empty));

        let null_payload = b"\x00\x00\x00\x00";
        let sig_null = compute_hmac_sha256(secret, null_payload);
        assert!(!sig_null.is_empty());
        assert!(verify_signature(secret, null_payload, &sig_null));
        assert_ne!(sig_empty, sig_null);
    }
}
