use std::sync::Arc;
use pingora::proxy::Session;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::config::SpectraSubscriptionsConfig;
use crate::subscriptions::SubscriptionHub;
use crate::subscriptions::engine::ConnectionActor;
use crate::subscriptions::protocol::compute_accept_key;

pub struct WebSocketHandler;

impl WebSocketHandler {
    /// Handles an incoming WebSocket upgrade request and pumps bidirectional traffic
    /// between the Pingora downstream session and the `ConnectionActor`.
    pub async fn handle(
        session: &mut Session,
        hub: Arc<SubscriptionHub>,
        config: &SpectraSubscriptionsConfig,
    ) -> pingora::Result<bool> {
        let sec_key = session
            .req_header()
            .headers
            .get("sec-websocket-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if sec_key.is_empty() {
            let resp = pingora::http::ResponseHeader::build(400, None)?;
            session.write_response_header(Box::new(resp), false).await?;
            session
                .write_response_body(Some(bytes::Bytes::from("Missing Sec-WebSocket-Key")), true)
                .await?;
            return Ok(true);
        }

        let accept_key = compute_accept_key(&sec_key);

        let requested_subprotocol = session
            .req_header()
            .headers
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut resp = pingora::http::ResponseHeader::build(101, None)?;
        let _ = resp.insert_header(http::header::CONNECTION, "Upgrade");
        let _ = resp.insert_header(http::header::UPGRADE, "websocket");
        let _ = resp.insert_header("sec-websocket-accept", accept_key);
        if let Some(subprotocol) = requested_subprotocol {
            if subprotocol.contains("graphql-transport-ws") {
                let _ = resp.insert_header("sec-websocket-protocol", "graphql-transport-ws");
            } else if let Some(first) = subprotocol.split(',').next() {
                let _ = resp.insert_header("sec-websocket-protocol", first.trim());
            }
        }

        session.write_response_header(Box::new(resp), false).await?;

        let (ws_io, mut pingora_io) = tokio::io::duplex(64 * 1024);
        let actor = ConnectionActor::new(
            hub,
            config.topic_prefix.clone(),
            config.keepalive_secs,
            config.client_buffer_capacity,
        );

        let ws_stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            ws_io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;

        let mut actor_handle = tokio::spawn(async move {
            let _ = actor.run(ws_stream).await;
        });

        let mut buf = [0u8; 8192];

        loop {
            tokio::select! {
                read_res = session.as_downstream_mut().read_body_or_idle(false) => {
                    match read_res {
                        Ok(Some(bytes)) => {
                            if bytes.is_empty() {
                                break;
                            }
                            if let Err(e) = pingora_io.write_all(&bytes).await {
                                log::debug!("WebSocket pump: write to ws_io failed: {}", e);
                                break;
                            }
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            log::debug!("WebSocket pump: downstream read error: {}", e);
                            break;
                        }
                    }
                }

                duplex_read = pingora_io.read(&mut buf) => {
                    match duplex_read {
                        Ok(n) if n > 0 => {
                            let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                            if let Err(e) = session.write_response_body(Some(chunk), false).await {
                                log::debug!("WebSocket pump: downstream write error: {}", e);
                                break;
                            }
                        }
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            log::debug!("WebSocket pump: duplex read error: {}", e);
                            break;
                        }
                    }
                }

                _ = &mut actor_handle => {
                    break;
                }
            }
        }

        actor_handle.abort();
        Ok(true)
    }
}
