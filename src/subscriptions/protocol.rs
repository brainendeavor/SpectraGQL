use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

pub const GRAPHQL_TRANSPORT_WS_PROTOCOL: &str = "graphql-transport-ws";

/// Close codes defined by the `graphql-transport-ws` specification.
pub mod close_codes {
    pub const NORMAL_CLOSURE: u16 = 1000;
    pub const BAD_REQUEST: u16 = 4400;
    pub const UNAUTHORIZED: u16 = 4401;
    pub const FORBIDDEN: u16 = 4403;
    pub const CONNECTION_ACK_TIMEOUT: u16 = 4408;
    pub const SUBSCRIBER_ALREADY_EXISTS: u16 = 4409;
    pub const TOO_MANY_REQUESTS: u16 = 4429;
}

/// Incoming client messages over `graphql-transport-ws`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "connection_init")]
    ConnectionInit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "ping")]
    Ping {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "pong")]
    Pong {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "subscribe")]
    Subscribe {
        id: String,
        payload: SubscribePayload,
    },
    #[serde(rename = "complete")]
    Complete {
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribePayload {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variables: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "operationName")]
    pub operation_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
}

/// Outgoing server messages over `graphql-transport-ws`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "connection_ack")]
    ConnectionAck {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "ping")]
    Ping {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "pong")]
    Pong {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    #[serde(rename = "next")]
    Next {
        id: String,
        payload: ExecutionResult,
    },
    #[serde(rename = "error")]
    Error {
        id: String,
        payload: Vec<Value>,
    },
    #[serde(rename = "complete")]
    Complete {
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExecutionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
}

impl ExecutionResult {
    pub fn with_data(data: Value) -> Self {
        Self {
            data: Some(data),
            errors: None,
            extensions: None,
        }
    }

    pub fn with_error(message: &str) -> Self {
        Self {
            data: None,
            errors: Some(vec![serde_json::json!({ "message": message })]),
            extensions: None,
        }
    }
}

/// Compute the RFC 6455 `Sec-WebSocket-Accept` header value from a `Sec-WebSocket-Key`.
pub fn compute_accept_key(key: &str) -> String {
    derive_accept_key(key.as_bytes())
}

/// Checks if an incoming Pingora `RequestHeader` is a valid WebSocket upgrade request.
pub fn is_websocket_upgrade(req: &pingora::http::RequestHeader) -> bool {
    let has_upgrade = req
        .headers
        .get(http::header::UPGRADE)
        .is_some_and(|val| val.as_bytes().eq_ignore_ascii_case(b"websocket"));

    let has_connection = req
        .headers
        .get(http::header::CONNECTION)
        .is_some_and(|val| {
            val.to_str().unwrap_or("").split(',').any(|part| {
                part.trim().eq_ignore_ascii_case("upgrade")
            })
        });

    has_upgrade && has_connection
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_accept_key() {
        // Test vector from RFC 6455 Section 1.3
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = compute_accept_key(key);
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn test_deserialize_client_connection_init() {
        let json = r#"{"type":"connection_init","payload":{"token":"secret"}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        match msg {
            ClientMessage::ConnectionInit { payload } => {
                assert!(payload.is_some());
                assert_eq!(payload.unwrap()["token"], "secret");
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn test_deserialize_client_subscribe() {
        let json = r#"{
            "id": "sub-1",
            "type": "subscribe",
            "payload": {
                "query": "subscription OnOrder($id: ID!) { orderStatus(id: $id) { status } }",
                "variables": { "id": "42" },
                "operationName": "OnOrder"
            }
        }"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        match msg {
            ClientMessage::Subscribe { id, payload } => {
                assert_eq!(id, "sub-1");
                assert_eq!(payload.operation_name.as_deref(), Some("OnOrder"));
                assert_eq!(payload.variables.unwrap()["id"], "42");
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn test_serialize_server_next() {
        let msg = ServerMessage::Next {
            id: "sub-1".to_string(),
            payload: ExecutionResult::with_data(serde_json::json!({
                "orderStatus": { "status": "CONFIRMED" }
            })),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"next""#));
        assert!(json.contains(r#""id":"sub-1""#));
        assert!(json.contains(r#""CONFIRMED""#));
    }

    #[test]
    fn test_serialize_server_connection_ack() {
        let msg = ServerMessage::ConnectionAck { payload: None };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"connection_ack"}"#);
    }
}
