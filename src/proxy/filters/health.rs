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
            let mut header = pingora::http::ResponseHeader::build(200, None).unwrap();
            let _ = header.insert_header("content-type", "application/json");
            session.set_keepalive(None);
            session.write_response_header(Box::new(header), false).await?;
            let body = serde_json::json!({
                "status": "ok",
                "service": "spectragql",
                "version": env!("CARGO_PKG_VERSION")
            });
            session
                .write_response_body(Some(bytes::Bytes::from(body.to_string())), true)
                .await?;
            return Ok(true);
        }

        if path == "/livez" {
            let mut header = pingora::http::ResponseHeader::build(200, None).unwrap();
            let _ = header.insert_header("content-type", "application/json");
            session.set_keepalive(None);
            session.write_response_header(Box::new(header), false).await?;
            let body = serde_json::json!({
                "status": "live",
                "gateway": "ready",
                "broker": "connected"
            });
            session
                .write_response_body(Some(bytes::Bytes::from(body.to_string())), true)
                .await?;
            return Ok(true);
        }

        Ok(false)
    }
}
