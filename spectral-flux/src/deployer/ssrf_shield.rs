use anyhow::{anyhow, Result};
use std::net::IpAddr;

#[derive(Debug, Clone)]
pub struct SSRFShield {
    allowed_artifact_hosts: Vec<String>,
    require_https: bool,
    block_private_networks: bool,
}

impl SSRFShield {
    pub fn new(
        allowed_artifact_hosts: Vec<String>,
        require_https: bool,
        block_private_networks: bool,
    ) -> Self {
        Self {
            allowed_artifact_hosts,
            require_https,
            block_private_networks,
        }
    }

    /// Validates an artifact URL against scheme, whitelist, and anti-SSRF rules.
    pub async fn validate_url(&self, raw_url: &str) -> Result<reqwest::Url> {
        let url = reqwest::Url::parse(raw_url)
            .map_err(|e| anyhow!("Invalid artifact URL format: {}", e))?;

        // 1. Scheme Check
        if self.require_https && url.scheme() != "https" {
            return Err(anyhow!(
                "Insecure scheme '{}': artifact URLs must strictly use https://",
                url.scheme()
            ));
        }

        let host = url
            .host_str()
            .ok_or_else(|| anyhow!("Artifact URL must contain a valid host"))?;

        // 2. Host / Prefix Whitelist Check
        if self.allowed_artifact_hosts.is_empty() {
            return Err(anyhow!(
                "Zero-trust rejection: no allowed_artifact_hosts are configured in deployer"
            ));
        }

        let full_spec = format!("{}{}", host, url.path());
        let host_allowed = self.allowed_artifact_hosts.iter().any(|allowed| {
            if allowed.ends_with('/') {
                full_spec.starts_with(allowed) || full_spec.starts_with(&format!("{}/", allowed.trim_end_matches('/')))
            } else {
                host == allowed || host.ends_with(&format!(".{}", allowed))
            }
        });

        if !host_allowed {
            return Err(anyhow!(
                "Host '{}' is not in allowed_artifact_hosts whitelist",
                host
            ));
        }

        // 3. Anti-SSRF Pre-Connect IP Address Verification
        if self.block_private_networks {
            self.verify_no_ssrf_destination(host, url.port().unwrap_or(443)).await?;
        }

        Ok(url)
    }

    async fn verify_no_ssrf_destination(&self, host: &str, port: u16) -> Result<()> {
        // Direct IP string check
        if let Ok(ip) = host.parse::<IpAddr>() {
            if is_forbidden_ip(&ip) {
                return Err(anyhow!(
                    "SSRF Shield: Direct IP destination '{}' is forbidden",
                    ip
                ));
            }
            return Ok(());
        }

        if host.eq_ignore_ascii_case("localhost") {
            return Err(anyhow!("SSRF Shield: 'localhost' destination is forbidden"));
        }

        // DNS Pre-Resolution
        let addr_spec = format!("{}:{}", host, port);
        match tokio::net::lookup_host(&addr_spec).await {
            Ok(addrs) => {
                for socket_addr in addrs {
                    let ip = socket_addr.ip();
                    if is_forbidden_ip(&ip) {
                        return Err(anyhow!(
                            "SSRF Shield: Host '{}' resolves to forbidden address {}",
                            host,
                            ip
                        ));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow!(
                    "SSRF Shield: Failed to resolve host '{}': {}",
                    host,
                    e
                ));
            }
        }

        Ok(())
    }
}

/// Checks if an IP address belongs to loopback, link-local (cloud metadata), or private subnets.
pub fn is_forbidden_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            // Loopback 127.0.0.0/8
            if octets[0] == 127 {
                return true;
            }
            // 0.0.0.0/8
            if octets[0] == 0 {
                return true;
            }
            // Cloud Metadata / Link-Local 169.254.0.0/16 (e.g. AWS IMDS 169.254.169.254)
            if octets[0] == 169 && octets[1] == 254 {
                return true;
            }
            // Private Class A: 10.0.0.0/8
            if octets[0] == 10 {
                return true;
            }
            // Private Class B: 172.16.0.0/12
            if octets[0] == 172 && (16..=31).contains(&octets[1]) {
                return true;
            }
            // Private Class C: 192.168.0.0/16
            if octets[0] == 192 && octets[1] == 168 {
                return true;
            }
            // Broadcast: 255.255.255.255
            if v4.is_broadcast() {
                return true;
            }
            false
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            // IPv6 link-local fe80::/10
            let segments = v6.segments();
            if (segments[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // IPv4-mapped IPv6
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_forbidden_ip(&IpAddr::V4(v4));
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ssrf_shield_rejects_insecure_scheme() {
        let shield = SSRFShield::new(vec!["example.com".to_string()], true, true);
        let err = shield.validate_url("http://example.com/cell.wasm").await.unwrap_err();
        assert!(err.to_string().contains("Insecure scheme"));
    }

    #[tokio::test]
    async fn test_ssrf_shield_rejects_unwhitelisted_host() {
        let shield = SSRFShield::new(vec!["github.com/my-org/".to_string()], true, true);
        let err = shield.validate_url("https://evil.com/cell.wasm").await.unwrap_err();
        assert!(err.to_string().contains("not in allowed_artifact_hosts"));
    }

    #[tokio::test]
    async fn test_ssrf_shield_rejects_cloud_metadata_ip() {
        let shield = SSRFShield::new(vec!["169.254.169.254".to_string()], false, true);
        let err = shield.validate_url("http://169.254.169.254/latest/meta-data").await.unwrap_err();
        assert!(err.to_string().contains("forbidden address") || err.to_string().contains("Direct IP destination"));
    }

    #[tokio::test]
    async fn test_ssrf_shield_rejects_loopback() {
        let shield = SSRFShield::new(vec!["127.0.0.1".to_string()], false, true);
        let err = shield.validate_url("http://127.0.0.1:8081/healthz").await.unwrap_err();
        assert!(err.to_string().contains("forbidden"));
    }

    #[tokio::test]
    async fn test_ssrf_shield_accepts_valid_whitelisted_prefix() {
        // Disable network lookup check for mock hostname in unit test
        let shield = SSRFShield::new(vec!["github.com/my-org/".to_string()], true, false);
        let url = shield.validate_url("https://github.com/my-org/invoice-mailer/releases/v1.0.0.wasm").await.unwrap();
        assert_eq!(url.host_str(), Some("github.com"));
    }
}
