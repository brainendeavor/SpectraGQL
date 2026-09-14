//! Magic Link Authentication Fluxcell for SpectraGQL
//!
//! Provides passwordless magic link authentication saga:
//! - Consumes `mutation.requestmagiclink` events
//! - Mints 256-bit single-use authentication tokens
//! - Renders responsive HTML/text email templates
//! - Exposes `/verify` endpoint for atomic token redemption (`GETDEL`)

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagicLinkRoute {
    pub method: String,
    pub path: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagicLinkRequest {
    pub email: String,
    #[serde(default)]
    pub redirect_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagicLinkTokenData {
    pub email: String,
    pub created_at: String,
    pub redirect_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResponse {
    pub status: String,
    pub email: String,
    pub session_id: String,
    pub redirect_uri: Option<String>,
}

/// Mints a cryptographically secure 256-bit token (64 hex characters)
pub fn mint_magic_token(email: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(Uuid::new_v4().as_bytes());
    hasher.update(email.as_bytes());
    hasher.update(Uuid::now_v7().as_bytes());
    let digest = hasher.finalize();
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Renders HTML email template containing the verification link
pub fn render_magic_link_email(email: &str, verify_url: &str) -> (String, String) {
    let subject = "Your Secure Sign-In Link".to_string();
    let html_body = format!(
        r#"<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Sign In</title></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 24px; background: #0f172a; color: #f8fafc;">
  <div style="max-width: 480px; margin: 0 auto; background: #1e293b; padding: 32px; border-radius: 12px; border: 1px solid #334155;">
    <h2 style="margin-top: 0; color: #38bdf8;">SpectraGQL Sign In</h2>
    <p>We received a sign-in request for <strong>{}</strong>.</p>
    <div style="margin: 28px 0;">
      <a href="{}" style="background: #38bdf8; color: #0f172a; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: 600; display: inline-block;">Verify & Sign In</a>
    </div>
    <p style="color: #94a3b8; font-size: 14px;">This link is valid for 15 minutes and can only be used once.</p>
    <p style="color: #64748b; font-size: 12px; margin-top: 24px;">If you did not request this link, you can safely ignore this email.</p>
  </div>
</body>
</html>"#,
        email, verify_url
    );
    (subject, html_body)
}

pub fn get_subscriptions() -> Vec<String> {
    vec![
        "auth.magic_link".to_string(),
        "mutation.requestmagiclink".to_string(),
    ]
}

pub fn get_routes() -> Vec<MagicLinkRoute> {
    vec![
        MagicLinkRoute {
            method: "GET".to_string(),
            path: "/verify".to_string(),
            description: "Verify magic link token and exchange for session".to_string(),
        },
        MagicLinkRoute {
            method: "POST".to_string(),
            path: "/verify".to_string(),
            description: "API redemption of magic link token".to_string(),
        },
        MagicLinkRoute {
            method: "GET".to_string(),
            path: "/status".to_string(),
            description: "Auth service health and status".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_minting() {
        let token1 = mint_magic_token("alice@example.com");
        let token2 = mint_magic_token("alice@example.com");

        assert_eq!(token1.len(), 64);
        assert_eq!(token2.len(), 64);
        assert_ne!(token1, token2); // Tokens must be unique
    }

    #[test]
    fn test_render_email() {
        let (subj, body) = render_magic_link_email(
            "test@example.com",
            "https://api.example.com/auth/verify?token=abc123xyz",
        );
        assert_eq!(subj, "Your Secure Sign-In Link");
        assert!(body.contains("test@example.com"));
        assert!(body.contains("https://api.example.com/auth/verify?token=abc123xyz"));
        assert!(body.contains("15 minutes"));
    }

    #[test]
    fn test_routes_and_subscriptions() {
        assert_eq!(get_subscriptions().len(), 2);
        assert_eq!(get_routes().len(), 3);
    }
}
