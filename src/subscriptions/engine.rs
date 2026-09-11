use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow};
use apollo_parser::Parser;
use apollo_parser::cst::{CstNode, Definition, Selection, Value as CstValue};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use uuid::Uuid;

use crate::subscriptions::hub::{ConnectionId, SubscriptionHub};
use crate::subscriptions::protocol::{
    ClientMessage, ServerMessage, close_codes,
};

/// Derive the broker topic and GraphQL root field name from a subscription query and variables.
///
/// Example:
/// `subscription OnOrder($id: ID!) { orderUpdates(id: $id) { status } }` with `{"id": "42"}`
/// -> Topic: `spectra.events.orderupdates.42`, Root field: `orderUpdates`
pub fn derive_subscription_topic(
    prefix: &str,
    query_str: &str,
    variables: Option<&Value>,
) -> Result<(String, String)> {
    let parser = Parser::new(query_str);
    let ast = parser.parse();

    let errors: Vec<_> = ast.errors().collect();
    if !errors.is_empty() {
        let err_messages: Vec<String> = errors.iter().map(|e| e.message().to_string()).collect();
        return Err(anyhow!("GraphQL subscription syntax error: {}", err_messages.join("; ")));
    }

    let doc = ast.document();
    for def in doc.definitions() {
        if let Definition::OperationDefinition(op) = def {
            let is_subscription = match op.operation_type() {
                Some(op_type) => op_type.subscription_token().is_some(),
                None => false,
            };

            if !is_subscription {
                return Err(anyhow!("Operation must be a subscription"));
            }

            if let Some(selection_set) = op.selection_set() {
                for selection in selection_set.selections() {
                    if let Selection::Field(f) = selection {
                        let root_field = f.name().ok_or_else(|| anyhow!("Field missing name"))?.text().to_string();

                        let mut primary_arg = None;

                        if let Some(args) = f.arguments() {
                            for arg in args.arguments() {
                                let arg_name = arg.name().map(|n| n.text().to_string()).unwrap_or_default();
                                let arg_val = match arg.value() {
                                    Some(CstValue::Variable(v)) => {
                                        let var_name = v.name().map(|n| n.text().to_string()).unwrap_or_default();
                                        variables
                                            .and_then(|vars| vars.get(&var_name))
                                            .and_then(|val| match val {
                                                Value::String(s) => Some(s.clone()),
                                                Value::Number(n) => Some(n.to_string()),
                                                _ => None,
                                            })
                                    }
                                    Some(CstValue::StringValue(s)) => {
                                        let raw = s.source_string();
                                        Some(raw.trim_matches('"').to_string())
                                    }
                                    Some(CstValue::IntValue(i)) => {
                                        Some(i.source_string().to_string())
                                    }
                                    Some(CstValue::EnumValue(e)) => {
                                        e.name().map(|n| n.text().to_string())
                                    }
                                    _ => None,
                                };

                                if let Some(val) = arg_val {
                                    // Prioritize identity/identifier arguments
                                    let lower = arg_name.to_lowercase();
                                    if lower == "id" || lower.ends_with("id") || lower == "topic" {
                                        primary_arg = Some(val);
                                        break;
                                    } else if primary_arg.is_none() {
                                        primary_arg = Some(val);
                                    }
                                }
                            }
                        }

                        let root_field_lower = root_field.to_lowercase();
                        let topic = if let Some(arg) = primary_arg {
                            format!("{}.events.{}.{}", prefix, root_field_lower, arg)
                        } else {
                            format!("{}.events.{}", prefix, root_field_lower)
                        };

                        return Ok((topic, root_field));
                    }
                }
            }
        }
    }

    Err(anyhow!("No subscription operation found in query"))
}

/// Actor managing the bidirectional WebSocket connection lifecycle for a single client.
pub struct ConnectionActor {
    pub conn_id: ConnectionId,
    pub hub: Arc<SubscriptionHub>,
    pub topic_prefix: String,
    pub keepalive_duration: Duration,
    pub buffer_capacity: usize,
}

impl ConnectionActor {
    pub fn new(
        hub: Arc<SubscriptionHub>,
        topic_prefix: String,
        keepalive_secs: u64,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            conn_id: Uuid::new_v4(),
            hub,
            topic_prefix,
            keepalive_duration: Duration::from_secs(keepalive_secs),
            buffer_capacity,
        }
    }

    /// Run the connection actor loop on the given WebSocket stream.
    pub async fn run<S>(&self, ws_stream: WebSocketStream<S>) -> Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_sink, mut ws_stream) = ws_stream.split();
        let (client_tx, mut client_rx) = mpsc::channel::<ServerMessage>(self.buffer_capacity);

        let mut ping_interval = tokio::time::interval(self.keepalive_duration);
        // Skip first immediate tick
        ping_interval.tick().await;

        let mut initialized = false;

        loop {
            tokio::select! {
                // Outbound server message to client
                outgoing = client_rx.recv() => {
                    match outgoing {
                        Some(server_msg) => {
                            match serde_json::to_string(&server_msg) {
                                Ok(json) => {
                                    if let Err(e) = ws_sink.send(Message::text(json)).await {
                                        log::warn!("ConnectionActor: failed to send message to client '{}': {}", self.conn_id, e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    log::error!("ConnectionActor: serialization error: {}", e);
                                }
                            }
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }

                // Periodic heartbeat ping to keep connection alive through proxies/NAT
                _ = ping_interval.tick() => {
                    let ping_msg = ServerMessage::Ping { payload: None };
                    if let Ok(json) = serde_json::to_string(&ping_msg) {
                        if let Err(e) = ws_sink.send(Message::text(json)).await {
                            log::debug!("ConnectionActor: heartbeat ping failed: {}", e);
                            break;
                        }
                    }
                }

                // Inbound client message
                incoming = ws_stream.next() => {
                    let msg = match incoming {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            log::debug!("ConnectionActor: ws read error from '{}': {}", self.conn_id, e);
                            break;
                        }
                        None => {
                            // Stream closed by client
                            break;
                        }
                    };

                    if msg.is_close() {
                        log::debug!("ConnectionActor: client '{}' closed connection", self.conn_id);
                        break;
                    }

                    if msg.is_ping() {
                        let _ = ws_sink.send(Message::Pong(msg.into_data())).await;
                        continue;
                    }

                    if let Ok(text) = msg.to_text() {
                        let client_msg: ClientMessage = match serde_json::from_str(text) {
                            Ok(m) => m,
                            Err(e) => {
                                log::warn!("ConnectionActor: invalid client json: {}", e);
                                let close_frame = CloseFrame {
                                    code: CloseCode::from(close_codes::BAD_REQUEST),
                                    reason: "Invalid JSON".into(),
                                };
                                let _ = ws_sink.send(Message::Close(Some(close_frame))).await;
                                break;
                            }
                        };

                        match client_msg {
                            ClientMessage::ConnectionInit { payload: _ } => {
                                initialized = true;
                                let ack = ServerMessage::ConnectionAck { payload: None };
                                if let Ok(json) = serde_json::to_string(&ack) {
                                    let _ = ws_sink.send(Message::text(json)).await;
                                }
                            }
                            ClientMessage::Ping { payload } => {
                                let pong = ServerMessage::Pong { payload };
                                if let Ok(json) = serde_json::to_string(&pong) {
                                    let _ = ws_sink.send(Message::text(json)).await;
                                }
                            }
                            ClientMessage::Pong { .. } => {
                                // Handshake keepalive response
                            }
                            ClientMessage::Subscribe { id, payload } => {
                                if !initialized {
                                    let close_frame = CloseFrame {
                                        code: CloseCode::from(close_codes::UNAUTHORIZED),
                                        reason: "Unauthorized: connection_init required".into(),
                                    };
                                    let _ = ws_sink.send(Message::Close(Some(close_frame))).await;
                                    break;
                                }

                                if self.hub.has_subscription(&self.conn_id, &id).await {
                                    let close_frame = CloseFrame {
                                        code: CloseCode::from(close_codes::SUBSCRIBER_ALREADY_EXISTS),
                                        reason: format!("Subscriber for '{}' already exists", id).into(),
                                    };
                                    let _ = ws_sink.send(Message::Close(Some(close_frame))).await;
                                    break;
                                }

                                match derive_subscription_topic(&self.topic_prefix, &payload.query, payload.variables.as_ref()) {
                                    Ok((topic, root_field)) => {
                                        log::info!(
                                            "ConnectionActor: client '{}' subscribed (id: '{}') to topic '{}'",
                                            self.conn_id, id, topic
                                        );
                                        self.hub.register(self.conn_id, id, topic, root_field, client_tx.clone()).await;
                                    }
                                    Err(e) => {
                                        let error_msg = ServerMessage::Error {
                                            id,
                                            payload: vec![serde_json::json!({ "message": e.to_string() })],
                                        };
                                        if let Ok(json) = serde_json::to_string(&error_msg) {
                                            let _ = ws_sink.send(Message::text(json)).await;
                                        }
                                    }
                                }
                            }
                            ClientMessage::Complete { id } => {
                                log::info!("ConnectionActor: client '{}' completed subscription '{}'", self.conn_id, id);
                                self.hub.deregister(&self.conn_id, &id).await;
                            }
                        }
                    }
                }
            }
        }

        // Clean up connection from hub
        self.hub.deregister_connection(&self.conn_id).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_subscription_topic_with_variable() {
        let query = "subscription OnOrderUpdates($orderId: ID!) { orderUpdates(orderId: $orderId) { status } }";
        let variables = serde_json::json!({ "orderId": "order-99" });

        let (topic, root_field) =
            derive_subscription_topic("spectra", query, Some(&variables)).unwrap();

        assert_eq!(topic, "spectra.events.orderupdates.order-99");
        assert_eq!(root_field, "orderUpdates");
    }

    #[test]
    fn test_derive_subscription_topic_with_literal_string() {
        let query = r#"subscription { orderUpdates(id: "order-42") { status } }"#;

        let (topic, root_field) =
            derive_subscription_topic("spectra", query, None).unwrap();

        assert_eq!(topic, "spectra.events.orderupdates.order-42");
        assert_eq!(root_field, "orderUpdates");
    }

    #[test]
    fn test_derive_subscription_topic_without_args() {
        let query = "subscription { globalAlerts { message } }";

        let (topic, root_field) =
            derive_subscription_topic("spectra", query, None).unwrap();

        assert_eq!(topic, "spectra.events.globalalerts");
        assert_eq!(root_field, "globalAlerts");
    }

    #[test]
    fn test_derive_subscription_topic_rejects_query_and_mutation() {
        let query = "query GetUser { user { id } }";
        assert!(derive_subscription_topic("spectra", query, None).is_err());

        let mutation = "mutation CreateUser { createUser { id } }";
        assert!(derive_subscription_topic("spectra", mutation, None).is_err());
    }
}
