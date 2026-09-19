use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use lru::LruCache;
use parking_lot::Mutex;
use ring::hmac;
use ring::signature::{UnparsedPublicKey, ED25519, RSA_PKCS1_2048_8192_SHA256};

use crate::interceptors::context::{
    AuthClaims, InterceptorContext, InterceptorRejection, InterceptorVerdict,
};
use crate::interceptors::request::RequestInterceptor;
use crate::protocol::GraphQLOperationType;

/// Policy modes determining how unmapped or unauthorized operations are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyMode {
    /// Reminds developers to configure authz, evaluates all rules, and emits audit warnings without blocking (Default for DX).
    #[default]
    AuditOnly,
    /// Permissive mode: never blocks unauthenticated or unmapped traffic; injects claims when token is present.
    PassAll,
    /// Requires a valid authenticated token; allows any unmapped operation if identity is valid.
    AllowAuthenticated,
    /// Production zero-trust: blocks any operation not explicitly unauthenticated or permitted by role.
    DenyUnlisted,
}

impl PolicyMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "pass_all" | "permissive" => Self::PassAll,
            "allow_authenticated" => Self::AllowAuthenticated,
            "deny_unlisted" | "strict" | "zero_trust" => Self::DenyUnlisted,
            _ => Self::AuditOnly,
        }
    }
}

/// JSON paths to extract standard claims from diverse token formats (Clerk, Auth0, Supabase, Keycloak, etc.).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClaimsMapping {
    pub subject_path: String,
    pub tenant_path: Option<String>,
    pub roles_path: String,
    pub permissions_path: Option<String>,
}

impl Default for ClaimsMapping {
    fn default() -> Self {
        Self {
            subject_path: "sub".to_string(),
            tenant_path: Some("org_id".to_string()),
            roles_path: "roles".to_string(),
            permissions_path: Some("permissions".to_string()),
        }
    }
}

/// Key entry from a JSON Web Key Set (JWKS).
#[derive(Debug, Clone)]
pub enum JwksKey {
    RsaPkcs1 { der_bytes: Vec<u8> },
    Ed25519 { raw_bytes: Vec<u8> },
}

/// Universal token verification provider (HMAC secrets, static public keys, or remote JWKS).
#[derive(Clone)]
pub enum AuthProvider {
    Hmac(Arc<Vec<u8>>),
    Jwks {
        jwks_url: String,
        keys: Arc<RwLock<HashMap<String, JwksKey>>>,
        last_refreshed: Arc<RwLock<Option<Instant>>>,
        refresh_interval: Duration,
    },
    StaticKey {
        kid: Option<String>,
        key: JwksKey,
    },
}

impl AuthProvider {
    pub fn hmac(secret: impl AsRef<[u8]>) -> Self {
        Self::Hmac(Arc::new(secret.as_ref().to_vec()))
    }

    pub fn static_rsa(kid: Option<String>, n_b64url: &str, e_b64url: &str) -> Result<Self, String> {
        let n = decode_b64url(n_b64url).map_err(|e| format!("Invalid RSA modulus 'n': {}", e))?;
        let e = decode_b64url(e_b64url).map_err(|e| format!("Invalid RSA exponent 'e': {}", e))?;
        let der = rsa_components_to_der(&n, &e);
        Ok(Self::StaticKey {
            kid,
            key: JwksKey::RsaPkcs1 { der_bytes: der },
        })
    }

    pub fn static_ed25519(kid: Option<String>, x_b64url: &str) -> Result<Self, String> {
        let raw = decode_b64url(x_b64url).map_err(|e| format!("Invalid Ed25519 public key 'x': {}", e))?;
        Ok(Self::StaticKey {
            kid,
            key: JwksKey::Ed25519 { raw_bytes: raw },
        })
    }

    pub fn jwks(url: impl Into<String>, refresh_interval: Duration) -> Self {
        let jwks_url = url.into();
        let keys = Arc::new(RwLock::new(HashMap::new()));
        let last_refreshed = Arc::new(RwLock::new(None));

        if tokio::runtime::Handle::try_current().is_ok() {
            let url_clone = jwks_url.clone();
            let keys_clone = keys.clone();
            let last_clone = last_refreshed.clone();
            tokio::spawn(async move {
                loop {
                    match fetch_jwks(&url_clone).await {
                        Ok(fetched_keys) => {
                            log::info!("Successfully refreshed {} JWKS keys from {}", fetched_keys.len(), url_clone);
                            *keys_clone.write().unwrap_or_else(|e| e.into_inner()) = fetched_keys;
                            *last_clone.write().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
                        }
                        Err(e) => {
                            log::warn!("Failed to refresh JWKS from {}: {}", url_clone, e);
                        }
                    }
                    tokio::time::sleep(refresh_interval).await;
                }
            });
        }

        Self::Jwks {
            jwks_url,
            keys,
            last_refreshed,
            refresh_interval,
        }
    }

    /// Explicitly populates or overrides JWKS keys in-memory (useful for testing or initial hydration).
    pub fn set_jwks_keys(&self, new_keys: HashMap<String, JwksKey>) {
        if let Self::Jwks { keys, last_refreshed, .. } = self {
            *keys.write().unwrap_or_else(|e| e.into_inner()) = new_keys;
            *last_refreshed.write().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        }
    }

    /// Verifies standard JWT structure and returns parsed claims JSON if signature matches.
    pub fn verify_jwt(&self, token: &str) -> Result<serde_json::Value, String> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err("JWT must contain exactly three parts separated by dots".to_string());
        }

        let header_json: serde_json::Value = decode_json_b64url(parts[0])
            .map_err(|e| format!("Failed to parse JWT header: {}", e))?;
        let payload_json: serde_json::Value = decode_json_b64url(parts[1])
            .map_err(|e| format!("Failed to parse JWT payload: {}", e))?;
        let signature_bytes = decode_b64url(parts[2])
            .map_err(|e| format!("Failed to decode JWT signature: {}", e))?;

        let signed_data = format!("{}.{}", parts[0], parts[1]);
        let signed_bytes = signed_data.as_bytes();

        let alg = header_json
            .get("alg")
            .and_then(|v| v.as_str())
            .unwrap_or("RS256");
        let kid = header_json.get("kid").and_then(|v| v.as_str());

        match self {
            Self::Hmac(secret) => {
                if alg != "HS256" {
                    return Err(format!("Algorithm mismatch: expected HS256, got {}", alg));
                }
                let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_slice());
                hmac::verify(&key, signed_bytes, &signature_bytes)
                    .map_err(|_| "HMAC signature verification failed".to_string())?;
            }
            Self::StaticKey { kid: expected_kid, key } => {
                if let (Some(k1), Some(k2)) = (expected_kid, kid) {
                    if k1 != k2 {
                        return Err(format!("JWK Key ID mismatch: expected {}, got {}", k1, k2));
                    }
                }
                verify_key(key, alg, signed_bytes, &signature_bytes)?;
            }
            Self::Jwks { keys, .. } => {
                let lock = keys.read().unwrap_or_else(|e| e.into_inner());
                let matched_key = if let Some(k) = kid {
                    lock.get(k)
                } else {
                    lock.values().next()
                };

                match matched_key {
                    Some(key) => verify_key(key, alg, signed_bytes, &signature_bytes)?,
                    None => {
                        return Err(format!(
                            "No matching key found in JWKS for kid: {:?}",
                            kid
                        ));
                    }
                }
            }
        }

        // Validate expiration and not-before claims with 60s clock skew tolerance (RFC 7519)
        const CLOCK_SKEW_SECS: i64 = 60;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if let Some(exp) = payload_json.get("exp").and_then(|v| v.as_i64()) {
            if now > exp.saturating_add(CLOCK_SKEW_SECS) {
                return Err("JWT token has expired".to_string());
            }
        }

        if let Some(nbf) = payload_json.get("nbf").and_then(|v| v.as_i64()) {
            if now.saturating_add(CLOCK_SKEW_SECS) < nbf {
                return Err("JWT token not yet valid (nbf)".to_string());
            }
        }

        Ok(payload_json)
    }
}

/// Parses a JWKS JSON structure into a map of key ID -> JwksKey.
pub fn parse_jwks_from_json(json: &serde_json::Value) -> Result<HashMap<String, JwksKey>, String> {
    let keys_array = json
        .get("keys")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "JWKS JSON missing 'keys' array".to_string())?;

    let mut map = HashMap::new();
    for (idx, key_obj) in keys_array.iter().enumerate() {
        let kty = key_obj.get("kty").and_then(|v| v.as_str()).unwrap_or_default();
        let kid = key_obj
            .get("kid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("key_{}", idx));

        match kty {
            "RSA" => {
                let n = key_obj
                    .get("n")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("RSA key '{}' missing 'n' component", kid))?;
                let e = key_obj
                    .get("e")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("RSA key '{}' missing 'e' component", kid))?;
                let n_bytes = decode_b64url(n)
                    .map_err(|err| format!("Failed to decode 'n' for key '{}': {}", kid, err))?;
                let e_bytes = decode_b64url(e)
                    .map_err(|err| format!("Failed to decode 'e' for key '{}': {}", kid, err))?;
                let der = rsa_components_to_der(&n_bytes, &e_bytes);
                map.insert(kid, JwksKey::RsaPkcs1 { der_bytes: der });
            }
            "OKP" => {
                let crv = key_obj.get("crv").and_then(|v| v.as_str()).unwrap_or_default();
                if crv != "Ed25519" {
                    log::warn!("Unsupported OKP curve '{}' in JWKS for kid '{}'", crv, kid);
                    continue;
                }
                let x = key_obj
                    .get("x")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("Ed25519 key '{}' missing 'x' component", kid))?;
                let x_bytes = decode_b64url(x)
                    .map_err(|err| format!("Failed to decode 'x' for key '{}': {}", kid, err))?;
                map.insert(kid, JwksKey::Ed25519 { raw_bytes: x_bytes });
            }
            other => {
                log::debug!("Skipping unsupported JWK key type '{}' for kid '{}'", other, kid);
            }
        }
    }
    Ok(map)
}

/// Asynchronously fetches JWKS keys from a remote URL.
pub async fn fetch_jwks(url: &str) -> Result<HashMap<String, JwksKey>, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

    let response = client
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("Failed to fetch JWKS from '{}': {}", url, e))?;

    if !response.status().is_success() {
        return Err(format!(
            "JWKS endpoint '{}' returned HTTP {}",
            url,
            response.status()
        ));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read JWKS response bytes: {}", e))?;

    let json: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("Failed to parse JWKS JSON from '{}': {}", url, e))?;

    parse_jwks_from_json(&json)
}

fn verify_key(key: &JwksKey, alg: &str, signed_data: &[u8], sig: &[u8]) -> Result<(), String> {
    match key {
        JwksKey::RsaPkcs1 { der_bytes } => {
            if alg != "RS256" {
                return Err(format!("Expected RS256 algorithm for RSA key, got {}", alg));
            }
            let public_key = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, der_bytes);
            public_key
                .verify(signed_data, sig)
                .map_err(|_| "RSA signature verification failed".to_string())
        }
        JwksKey::Ed25519 { raw_bytes } => {
            if alg != "EdDSA" && alg != "ED25519" {
                return Err(format!("Expected EdDSA algorithm for Ed25519 key, got {}", alg));
            }
            let public_key = UnparsedPublicKey::new(&ED25519, raw_bytes);
            public_key
                .verify(signed_data, sig)
                .map_err(|_| "Ed25519 signature verification failed".to_string())
        }
    }
}

/// In-Memory L1 token cache entry.
#[derive(Clone)]
struct CachedAuth {
    claims: AuthClaims,
    expires_at: Instant,
}

const DEFAULT_L1_CACHE_CAPACITY: usize = 10_000;

/// High-velocity RequestInterceptor enforcing RBAC with decoupled OIDC/JWKS and policy mode continuum.
#[derive(Clone)]
pub struct RbacRequestInterceptor {
    provider: Option<AuthProvider>,
    claims_mapping: ClaimsMapping,
    mutations_default: PolicyMode,
    queries_default: PolicyMode,
    unauthenticated_ops: HashSet<String>,
    role_permissions: HashMap<String, Vec<String>>, // operation -> required roles
    l1_cache: Arc<Mutex<LruCache<String, CachedAuth>>>,
    cache_ttl: Duration,
}

impl RbacRequestInterceptor {
    pub fn new() -> Self {
        let mut unauthenticated_ops = HashSet::new();
        unauthenticated_ops.insert("IntrospectionQuery".to_string());
        unauthenticated_ops.insert("__schema".to_string());
        unauthenticated_ops.insert("requestMagicLink".to_string());
        unauthenticated_ops.insert("verifyMagicLink".to_string());

        Self {
            provider: None,
            claims_mapping: ClaimsMapping::default(),
            mutations_default: PolicyMode::AuditOnly,
            queries_default: PolicyMode::PassAll,
            unauthenticated_ops,
            role_permissions: HashMap::new(),
            l1_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_L1_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN),
            ))),
            cache_ttl: Duration::from_secs(60),
        }
    }

    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        if let Some(cap) = NonZeroUsize::new(capacity) {
            self.l1_cache = Arc::new(Mutex::new(LruCache::new(cap)));
        }
        self
    }

    pub fn with_provider(mut self, provider: AuthProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn with_claims_mapping(mut self, mapping: ClaimsMapping) -> Self {
        self.claims_mapping = mapping;
        self
    }

    pub fn with_policies(mut self, mutations: PolicyMode, queries: PolicyMode) -> Self {
        self.mutations_default = mutations;
        self.queries_default = queries;
        self
    }

    pub fn allow_unauthenticated(mut self, op: impl Into<String>) -> Self {
        self.unauthenticated_ops.insert(op.into());
        self
    }

    pub fn grant_role(mut self, role: impl Into<String>, ops: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let r = role.into();
        for op in ops {
            let op_name = op.into();
            self.role_permissions
                .entry(op_name)
                .or_default()
                .push(r.clone());
        }
        self
    }

    fn resolve_claims_from_token(&self, token: &str) -> Result<AuthClaims, String> {
        let now = Instant::now();
        // 1. Check L1 memory cache
        {
            let mut cache = self.l1_cache.lock();
            if let Some(cached) = cache.get(token) {
                if now < cached.expires_at {
                    return Ok(cached.claims.clone());
                }
            }
        }

        // 2. Cryptographic verify via configured provider
        let provider = match &self.provider {
            Some(p) => p,
            None => return Err("No authentication provider configured".to_string()),
        };

        let raw_json = provider.verify_jwt(token)?;
        let claims = extract_claims_from_json(&raw_json, &self.claims_mapping);

        // 3. Write into L1 cache
        {
            let mut cache = self.l1_cache.lock();
            cache.put(
                token.to_string(),
                CachedAuth {
                    claims: claims.clone(),
                    expires_at: now + self.cache_ttl,
                },
            );
        }

        Ok(claims)
    }
}

impl Default for RbacRequestInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestInterceptor for RbacRequestInterceptor {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        _body: &str,
    ) -> InterceptorVerdict {
        let op_name = ctx.operation_name.as_deref().unwrap_or("");

        // 1. Exempt public / unauthenticated operations
        if self.unauthenticated_ops.contains(op_name) {
            return InterceptorVerdict::Pass;
        }

        // 2. Extract Bearer token from headers (RFC 6750: case-insensitive scheme)
        let auth_header = parts
            .headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.trim());

        let mut token_opt = None;
        if let Some(hdr) = auth_header {
            if hdr.len() > 7 && hdr[..7].eq_ignore_ascii_case("bearer ") {
                token_opt = Some(hdr[7..].trim());
            }
        }

        // 3. Resolve identity claims if token is present
        let mut claims_opt = None;
        let mut token_error = None;
        if let Some(token) = token_opt {
            match self.resolve_claims_from_token(token) {
                Ok(c) => claims_opt = Some(c),
                Err(e) => token_error = Some(e),
            }
        }

        // Inject claims into InterceptorContext
        if let Some(ref c) = claims_opt {
            ctx.claims = Some(c.clone());

            // Propagate user identity upstream for Mode A queries
            if let Some(ref sub) = c.subject {
                if let Ok(v) = http::HeaderValue::from_str(sub) {
                    parts.headers.insert("x-user-id", v);
                }
            }
            if let Some(ref tid) = c.tenant_id {
                if let Ok(v) = http::HeaderValue::from_str(tid) {
                    parts.headers.insert("x-tenant-id", v);
                }
            }
            if !c.roles.is_empty() {
                let roles_csv = c.roles.join(",");
                if let Ok(v) = http::HeaderValue::from_str(&roles_csv) {
                    parts.headers.insert("x-user-roles", v);
                }
            }
        }

        // 4. Select policy mode based on operation type
        let is_mutation = matches!(ctx.operation_type, Some(GraphQLOperationType::Mutation));
        let policy_mode = if is_mutation {
            self.mutations_default
        } else {
            self.queries_default
        };

        // 5. Evaluate role requirements
        let required_roles = self.role_permissions.get(op_name);
        let has_permission = if let Some(req) = required_roles {
            if let Some(ref c) = claims_opt {
                c.roles.iter().any(|r| r == "*" || req.contains(r))
            } else {
                false
            }
        } else {
            // Unmapped operation
            match policy_mode {
                PolicyMode::PassAll => true,
                PolicyMode::AuditOnly => false, // Will trigger audit telemetry without blocking
                PolicyMode::AllowAuthenticated => claims_opt.is_some(),
                PolicyMode::DenyUnlisted => false,
            }
        };

        if has_permission {
            return InterceptorVerdict::Pass;
        }

        // 6. Apply policy mode action when unauthorized
        match policy_mode {
            PolicyMode::PassAll => InterceptorVerdict::Pass,
            PolicyMode::AuditOnly => InterceptorVerdict::Audit {
                rule_name: "rbac_audit_mode".to_string(),
                tag: Some("AUTH_AUDIT".to_string()),
                reason: format!(
                    "Operation '{}' executed without required role (identity: {:?}, token_err: {:?})",
                    op_name,
                    claims_opt.as_ref().and_then(|c| c.subject.as_deref()),
                    token_error
                ),
            },
            PolicyMode::AllowAuthenticated => {
                if claims_opt.is_some() {
                    InterceptorVerdict::Pass
                } else {
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        http::StatusCode::UNAUTHORIZED,
                        "AUTHENTICATION_REQUIRED",
                        token_error.unwrap_or_else(|| "Valid authentication token required".to_string()),
                    ))
                }
            }
            PolicyMode::DenyUnlisted => {
                if claims_opt.is_none() {
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        http::StatusCode::UNAUTHORIZED,
                        "AUTHENTICATION_REQUIRED",
                        token_error.unwrap_or_else(|| "Authentication token required".to_string()),
                    ))
                } else {
                    InterceptorVerdict::Reject(InterceptorRejection::new(
                        http::StatusCode::FORBIDDEN,
                        "FORBIDDEN",
                        format!(
                            "User '{}' does not possess required role for operation '{}'",
                            claims_opt.as_ref().and_then(|c| c.subject.as_deref()).unwrap_or("anonymous"),
                            op_name
                        ),
                    ))
                }
            }
        }
    }
}

fn extract_claims_from_json(json: &serde_json::Value, mapping: &ClaimsMapping) -> AuthClaims {
    let subject = json
        .get(&mapping.subject_path)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tenant_id = mapping
        .tenant_path
        .as_ref()
        .and_then(|p| json.get(p))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut roles = Vec::new();
    if let Some(val) = json.get(&mapping.roles_path) {
        if let Some(arr) = val.as_array() {
            for item in arr {
                if let Some(s) = item.as_str() {
                    roles.push(s.to_string());
                }
            }
        } else if let Some(s) = val.as_str() {
            roles.push(s.to_string());
        }
    }

    let mut permissions = Vec::new();
    if let Some(p) = &mapping.permissions_path {
        if let Some(val) = json.get(p) {
            if let Some(arr) = val.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        permissions.push(s.to_string());
                    }
                }
            }
        }
    }

    AuthClaims {
        subject,
        tenant_id,
        roles,
        permissions,
        raw_claims: json.clone(),
    }
}

fn decode_b64url(s: &str) -> Result<Vec<u8>, String> {
    URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .map_err(|e| format!("Base64url decode error: {}", e))
}

fn decode_json_b64url(s: &str) -> Result<serde_json::Value, String> {
    let bytes = decode_b64url(s)?;
    serde_json::from_slice(&bytes).map_err(|e| format!("JSON decode error: {}", e))
}

/// Converts raw RSA modulus 'n' and exponent 'e' into standard PKCS#1 DER format.
fn rsa_components_to_der(n: &[u8], e: &[u8]) -> Vec<u8> {
    fn encode_der_integer(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0x02); // INTEGER tag
        let needs_leading_zero = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
        let len = bytes.len() + if needs_leading_zero { 1 } else { 0 };
        encode_der_length(&mut out, len);
        if needs_leading_zero {
            out.push(0x00);
        }
        out.extend_from_slice(bytes);
        out
    }

    fn encode_der_length(out: &mut Vec<u8>, len: usize) {
        if len < 128 {
            out.push(len as u8);
        } else if len < 256 {
            out.push(0x81);
            out.push(len as u8);
        } else {
            out.push(0x82);
            out.push((len >> 8) as u8);
            out.push((len & 0xff) as u8);
        }
    }

    let int_n = encode_der_integer(n);
    let int_e = encode_der_integer(e);
    let total_len = int_n.len() + int_e.len();
    let mut seq = Vec::new();
    seq.push(0x30); // SEQUENCE tag
    encode_der_length(&mut seq, total_len);
    seq.extend_from_slice(&int_n);
    seq.extend_from_slice(&int_e);
    seq
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::clock::HlcTimestamp;
    use uuid::Uuid;

    #[test]
    fn test_hmac_jwt_verification_and_claims_extraction() {
        let secret = b"my_super_secret_jwt_key_2026_test";
        let provider = AuthProvider::hmac(secret);

        // Header: {"alg":"HS256","typ":"JWT"}
        let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        // Payload: {"sub":"usr_alice","roles":["editor"],"org_id":"tenant_coeval","exp":2000000000}
        let payload_b64 = "eyJzdWIiOiJ1c3JfYWxpY2UiLCJyb2xlcyI6WyJlZGl0b3IiXSwib3JnX2lkIjoidGVuYW50X2NvZXZhbCIsImV4cCI6MjAwMDAwMDAwMH0";

        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let tag = hmac::sign(&key, signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(tag.as_ref());

        let token = format!("{}.{}", signing_input, sig_b64);
        let claims_json = provider.verify_jwt(&token).expect("JWT should verify");
        assert_eq!(claims_json["sub"], "usr_alice");

        let mapping = ClaimsMapping::default();
        let auth_claims = extract_claims_from_json(&claims_json, &mapping);
        assert_eq!(auth_claims.subject.as_deref(), Some("usr_alice"));
        assert_eq!(auth_claims.tenant_id.as_deref(), Some("tenant_coeval"));
        assert_eq!(auth_claims.roles, vec!["editor"]);
    }

    #[test]
    fn test_rbac_interceptor_audit_only_mode_allows_with_audit() {
        let rbac = RbacRequestInterceptor::new()
            .with_policies(PolicyMode::AuditOnly, PolicyMode::PassAll)
            .grant_role("admin", ["deleteDatabase"]);

        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("deleteDatabase".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;

        // Anonymous call to protected mutation in AuditOnly mode -> Audit verdict, never blocked!
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        match verdict {
            InterceptorVerdict::Audit { tag, .. } => {
                assert_eq!(tag.as_deref(), Some("AUTH_AUDIT"));
            }
            other => panic!("Expected InterceptorVerdict::Audit in AuditOnly mode, got {:?}", other),
        }
    }

    #[test]
    fn test_rbac_interceptor_deny_unlisted_blocks_unauthorized() {
        let secret = b"secret_key_123";
        let provider = AuthProvider::hmac(secret);
        let rbac = RbacRequestInterceptor::new()
            .with_provider(provider)
            .with_policies(PolicyMode::DenyUnlisted, PolicyMode::AllowAuthenticated)
            .grant_role("editor", ["publishPost"]);

        let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
            .with_operation(Some("publishPost".to_string()), Some(GraphQLOperationType::Mutation));
        let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;

        // Case 1: Anonymous -> 401 Unauthorized
        let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict, InterceptorVerdict::Reject(rej) if rej.status_code == http::StatusCode::UNAUTHORIZED));

        // Case 2: Viewer role token -> 403 Forbidden
        let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        let payload_b64 = "eyJzdWIiOiJ1c3JfYm9iIiwicm9sZXMiOlsidmlld2VyIl0sImV4cCI6MjAwMDAwMDAwMH0";
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let tag = hmac::sign(&key, signing_input.as_bytes());
        let token = format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()));

        parts.headers.insert("authorization", format!("Bearer {}", token).parse().unwrap());
        let verdict2 = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict2, InterceptorVerdict::Reject(rej) if rej.status_code == http::StatusCode::FORBIDDEN));

        // Case 3: Editor role token -> 200 Pass!
        let payload_editor = "eyJzdWIiOiJ1c3JfY2Fyb2wiLCJyb2xlcyI6WyJlZGl0b3IiXSwiZXhwIjoyMDAwMDAwMDAwfQ";
        let signing_editor = format!("{}.{}", header_b64, payload_editor);
        let tag_editor = hmac::sign(&key, signing_editor.as_bytes());
        let token_editor = format!("{}.{}", signing_editor, URL_SAFE_NO_PAD.encode(tag_editor.as_ref()));

        parts.headers.insert("authorization", format!("Bearer {}", token_editor).parse().unwrap());
        let verdict3 = rbac.intercept_request(&mut ctx, &mut parts, "");
        assert!(matches!(verdict3, InterceptorVerdict::Pass));
        assert_eq!(parts.headers.get("x-user-id").unwrap(), "usr_carol");
        assert_eq!(parts.headers.get("x-user-roles").unwrap(), "editor");
    }

    #[test]
    fn test_lru_cache_bounded_eviction() {
        let secret = b"secret_for_lru_cache_test";
        let provider = AuthProvider::hmac(secret);
        // Set capacity to 2
        let rbac = RbacRequestInterceptor::new()
            .with_provider(provider)
            .with_cache_capacity(2);

        let make_token = |id: &str| -> String {
            let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
            let payload = serde_json::json!({
                "sub": id,
                "roles": ["user"],
                "exp": 2500000000i64
            });
            let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
            let signing_input = format!("{}.{}", header_b64, payload_b64);
            let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
            let tag = hmac::sign(&key, signing_input.as_bytes());
            format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()))
        };

        let t1 = make_token("user1");
        let t2 = make_token("user2");
        let t3 = make_token("user3");

        // Insert t1 and t2
        assert!(rbac.resolve_claims_from_token(&t1).is_ok());
        assert!(rbac.resolve_claims_from_token(&t2).is_ok());
        assert_eq!(rbac.l1_cache.lock().len(), 2);

        // Access t1 to make it most recently used, so t2 is LRU
        assert!(rbac.resolve_claims_from_token(&t1).is_ok());

        // Insert t3 -> should evict t2
        assert!(rbac.resolve_claims_from_token(&t3).is_ok());
        assert_eq!(rbac.l1_cache.lock().len(), 2);

        let cache = rbac.l1_cache.lock();
        assert!(cache.peek(&t1).is_some());
        assert!(cache.peek(&t2).is_none(), "t2 should have been evicted by LRU");
        assert!(cache.peek(&t3).is_some());
    }

    #[test]
    fn test_case_insensitive_bearer_header() {
        let secret = b"secret_case_insensitive_test";
        let provider = AuthProvider::hmac(secret);
        let rbac = RbacRequestInterceptor::new()
            .with_provider(provider)
            .with_policies(PolicyMode::DenyUnlisted, PolicyMode::DenyUnlisted)
            .grant_role("admin", ["manageSecret"]);

        let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        let payload = serde_json::json!({
            "sub": "usr_admin",
            "roles": ["admin"],
            "exp": 2500000000i64
        });
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let signing_input = format!("{}.{}", header_b64, payload_b64);
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
        let tag = hmac::sign(&key, signing_input.as_bytes());
        let token = format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()));

        // Test lowercase "bearer "
        {
            let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
                .with_operation(Some("manageSecret".to_string()), Some(GraphQLOperationType::Mutation));
            let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
            parts.headers.insert("authorization", format!("bearer {}", token).parse().unwrap());
            let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
            assert!(matches!(verdict, InterceptorVerdict::Pass));
        }

        // Test uppercase "BEARER "
        {
            let mut ctx = InterceptorContext::new(Uuid::now_v7(), HlcTimestamp::new(1000, 0))
                .with_operation(Some("manageSecret".to_string()), Some(GraphQLOperationType::Mutation));
            let mut parts = http::Request::builder().uri("/graphql").body(()).unwrap().into_parts().0;
            parts.headers.insert("authorization", format!("BEARER {}", token).parse().unwrap());
            let verdict = rbac.intercept_request(&mut ctx, &mut parts, "");
            assert!(matches!(verdict, InterceptorVerdict::Pass));
        }
    }

    #[test]
    fn test_jwt_clock_skew_and_nbf_validation() {
        let secret = b"secret_clock_skew_test";
        let provider = AuthProvider::hmac(secret);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let make_token = |exp: i64, nbf: Option<i64>| -> String {
            let header_b64 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
            let mut payload = serde_json::json!({
                "sub": "user_skew",
                "roles": ["user"],
                "exp": exp,
            });
            if let Some(n) = nbf {
                payload["nbf"] = serde_json::Value::from(n);
            }
            let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
            let signing_input = format!("{}.{}", header_b64, payload_b64);
            let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
            let tag = hmac::sign(&key, signing_input.as_bytes());
            format!("{}.{}", signing_input, URL_SAFE_NO_PAD.encode(tag.as_ref()))
        };

        // 1. Expired 30 seconds ago: within 60s clock skew leeway -> SHOULD PASS
        let t_leeway = make_token(now - 30, None);
        assert!(provider.verify_jwt(&t_leeway).is_ok());

        // 2. Expired 120 seconds ago: exceeds 60s leeway -> SHOULD FAIL
        let t_expired = make_token(now - 120, None);
        assert!(provider.verify_jwt(&t_expired).is_err());

        // 3. nbf 30 seconds into the future: within 60s leeway -> SHOULD PASS
        let t_nbf_ok = make_token(now + 1000, Some(now + 30));
        assert!(provider.verify_jwt(&t_nbf_ok).is_ok());

        // 4. nbf 120 seconds into the future: exceeds leeway -> SHOULD FAIL
        let t_nbf_future = make_token(now + 1000, Some(now + 120));
        assert!(provider.verify_jwt(&t_nbf_future).is_err());
    }

    #[test]
    fn test_parse_jwks_from_json_rsa_and_ed25519() {
        let n_b64 = URL_SAFE_NO_PAD.encode(&vec![42u8; 128]);
        let e_b64 = URL_SAFE_NO_PAD.encode(&[1u8, 0, 1]);
        let x_b64 = URL_SAFE_NO_PAD.encode(&vec![7u8; 32]);

        let jwks_json = serde_json::json!({
            "keys": [
                {
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": "rsa_key_1",
                    "n": n_b64,
                    "e": e_b64
                },
                {
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "kid": "ed_key_2",
                    "x": x_b64
                }
            ]
        });

        let keys = parse_jwks_from_json(&jwks_json).expect("JWKS parsing should succeed");
        assert_eq!(keys.len(), 2);
        assert!(matches!(keys.get("rsa_key_1"), Some(JwksKey::RsaPkcs1 { .. })));
        assert!(matches!(keys.get("ed_key_2"), Some(JwksKey::Ed25519 { .. })));
    }
}
