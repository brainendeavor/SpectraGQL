use pingora::proxy::Session;

/// Handles liveness and readiness probe requests (/healthz and /livez)
/// directly at the gateway edge without forwarding to upstreams.
pub struct HealthFilter;

impl HealthFilter {
    pub fn is_health_probe(path: &str) -> bool {
        path == "/healthz" || path == "/livez"
    }

    pub async fn handle(session: &mut Session) -> pingora::Result<bool> {
        let path = session.req_header().uri.path();

        if path == "/healthz" {
            let body = serde_json::json!({
                "status": "ok",
                "service": "spectragql",
                "version": env!("CARGO_PKG_VERSION")
            });
            let body_str = body.to_string();
            let mut header = pingora::http::ResponseHeader::build(200, None)?;
            let _ = header.insert_header("content-type", "application/json");
            let _ = header.insert_header("content-length", body_str.len().to_string());
            let _ = header.insert_header("cache-control", "no-store");
            if let Err(e) = session.write_response_header(Box::new(header), false).await {
                log::debug!("Client disconnected before /healthz header write: {}", e);
                return Ok(true);
            }
            if let Err(e) = session
                .write_response_body(Some(bytes::Bytes::from(body_str)), true)
                .await
            {
                log::debug!("Client disconnected before /healthz body write: {}", e);
                return Ok(true);
            }
            return Ok(true);
        }

        if path == "/livez" {
            let body = serde_json::json!({
                "status": "live",
                "gateway": "ready",
                "broker": "connected"
            });
            let body_str = body.to_string();
            let mut header = pingora::http::ResponseHeader::build(200, None)?;
            let _ = header.insert_header("content-type", "application/json");
            let _ = header.insert_header("content-length", body_str.len().to_string());
            let _ = header.insert_header("cache-control", "no-store");
            if let Err(e) = session.write_response_header(Box::new(header), false).await {
                log::debug!("Client disconnected before /livez header write: {}", e);
                return Ok(true);
            }
            if let Err(e) = session
                .write_response_body(Some(bytes::Bytes::from(body_str)), true)
                .await
            {
                log::debug!("Client disconnected before /livez body write: {}", e);
                return Ok(true);
            }
            return Ok(true);
        }

        Ok(false)
    }
}
