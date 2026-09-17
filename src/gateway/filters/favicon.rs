use pingora::proxy::Session;

/// Handles /favicon.ico and /favicon.svg requests directly at the gateway edge
/// without forwarding to upstream services.
pub struct FaviconFilter;

static FAVICON_SVG: &str = include_str!("../../../assets/favicon.svg");

impl FaviconFilter {
    #[inline]
    pub fn is_favicon_request(path: &str) -> bool {
        path == "/favicon.ico" || path == "/favicon.svg"
    }

    pub async fn handle(session: &mut Session) -> pingora::Result<bool> {
        let path = session.req_header().uri.path();
        if !Self::is_favicon_request(path) {
            return Ok(false);
        }

        let mut header = pingora::http::ResponseHeader::build(200, None)?;
        let _ = header.insert_header("content-type", "image/svg+xml");
        let _ = header.insert_header("content-length", FAVICON_SVG.len().to_string());
        let _ = header.insert_header("cache-control", "public, max-age=86400, immutable");
        if let Err(e) = session.write_response_header(Box::new(header), false).await {
            log::debug!("Client disconnected before favicon header write: {}", e);
            return Ok(true);
        }
        if let Err(e) = session
            .write_response_body(Some(bytes::Bytes::from_static(FAVICON_SVG.as_bytes())), true)
            .await
        {
            log::debug!("Client disconnected before favicon body write: {}", e);
            return Ok(true);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_favicon_request() {
        assert!(FaviconFilter::is_favicon_request("/favicon.ico"));
        assert!(FaviconFilter::is_favicon_request("/favicon.svg"));
        assert!(!FaviconFilter::is_favicon_request("/favicon.png"));
        assert!(!FaviconFilter::is_favicon_request("/graphql"));
        assert!(!FaviconFilter::is_favicon_request("/"));
    }

    #[test]
    fn test_favicon_asset_embedded() {
        assert!(FAVICON_SVG.contains("<svg"));
        assert!(FAVICON_SVG.contains("SpectraGQL Favicon"));
    }
}
