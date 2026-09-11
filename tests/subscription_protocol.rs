use std::sync::Arc;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use spectragql::subscriptions::{
    ConnectionActor, SubscriptionHub, ClientMessage, ServerMessage,
};
use spectragql::subscriptions::protocol::close_codes;

async fn setup_test_ws_pair(hub: Arc<SubscriptionHub>) -> (
    WebSocketStream<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);

    let server_ws = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    ).await;

    let client_ws = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    ).await;

    let actor = ConnectionActor::new(hub, "spectra".to_string(), 30, 64);
    let handle = tokio::spawn(async move {
        actor.run(server_ws).await
    });

    (client_ws, handle)
}

#[tokio::test]
async fn test_ws_connection_init_and_ack() {
    let hub = Arc::new(SubscriptionHub::new());
    let (mut client, _actor_handle) = setup_test_ws_pair(hub).await;

    // Send connection_init
    let init_msg = ClientMessage::ConnectionInit { payload: None };
    client.send(Message::text(serde_json::to_string(&init_msg).unwrap())).await.unwrap();

    // Read server response -> Expect connection_ack
    let resp = client.next().await.unwrap().unwrap();
    let text = resp.to_text().unwrap();
    let server_msg: ServerMessage = serde_json::from_str(text).unwrap();

    match server_msg {
        ServerMessage::ConnectionAck { .. } => {}
        other => panic!("Expected ConnectionAck, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ws_rejects_subscription_before_connection_init() {
    let hub = Arc::new(SubscriptionHub::new());
    let (mut client, _actor_handle) = setup_test_ws_pair(hub).await;

    // Send subscribe without sending connection_init
    let sub_msg = json!({
        "type": "subscribe",
        "id": "1",
        "payload": {
            "query": "subscription { orderUpdates { status } }"
        }
    });

    client.send(Message::text(sub_msg.to_string())).await.unwrap();

    // Expect close frame with code 4401 (UNAUTHORIZED)
    let resp = client.next().await.unwrap().unwrap();
    match resp {
        Message::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), close_codes::UNAUTHORIZED);
            assert!(frame.reason.contains("connection_init required"));
        }
        other => panic!("Expected Close frame with 4401, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ws_rejects_duplicate_subscription_id() {
    let hub = Arc::new(SubscriptionHub::new());
    let (mut client, _actor_handle) = setup_test_ws_pair(hub).await;

    // 1. Send connection_init
    let init_msg = ClientMessage::ConnectionInit { payload: None };
    client.send(Message::text(serde_json::to_string(&init_msg).unwrap())).await.unwrap();
    let _ack = client.next().await.unwrap().unwrap();

    // 2. Send first subscribe with id "sub-42"
    let sub_msg1 = json!({
        "type": "subscribe",
        "id": "sub-42",
        "payload": {
            "query": "subscription { orderUpdates { status } }"
        }
    });
    client.send(Message::text(sub_msg1.to_string())).await.unwrap();

    // Give actor a moment to register
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // 3. Send duplicate subscribe with same id "sub-42"
    let sub_msg2 = json!({
        "type": "subscribe",
        "id": "sub-42",
        "payload": {
            "query": "subscription { orderUpdates { status } }"
        }
    });
    client.send(Message::text(sub_msg2.to_string())).await.unwrap();

    // Expect close frame with code 4409 (SUBSCRIBER_ALREADY_EXISTS)
    let resp = client.next().await.unwrap().unwrap();
    match resp {
        Message::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), close_codes::SUBSCRIBER_ALREADY_EXISTS);
            assert!(frame.reason.contains("already exists"));
        }
        other => panic!("Expected Close frame with 4409, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ws_ping_pong_heartbeat() {
    let hub = Arc::new(SubscriptionHub::new());
    let (mut client, _actor_handle) = setup_test_ws_pair(hub).await;

    let ping_msg = json!({
        "type": "ping",
        "payload": { "check": "alive" }
    });
    client.send(Message::text(ping_msg.to_string())).await.unwrap();

    let resp = client.next().await.unwrap().unwrap();
    let text = resp.to_text().unwrap();
    let server_msg: ServerMessage = serde_json::from_str(text).unwrap();

    match server_msg {
        ServerMessage::Pong { payload } => {
            assert_eq!(payload.unwrap()["check"], "alive");
        }
        other => panic!("Expected Pong, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ws_subscription_broadcast_delivery_and_completion() {
    let hub = Arc::new(SubscriptionHub::new());
    let (mut client, _actor_handle) = setup_test_ws_pair(hub.clone()).await;

    // Init
    let init_msg = ClientMessage::ConnectionInit { payload: None };
    client.send(Message::text(serde_json::to_string(&init_msg).unwrap())).await.unwrap();
    let _ack = client.next().await.unwrap().unwrap();

    // Subscribe to orderUpdates with id: "order-99"
    let sub_msg = json!({
        "type": "subscribe",
        "id": "sub-1",
        "payload": {
            "query": "subscription { orderUpdates(id: \"order-99\") { status } }"
        }
    });
    client.send(Message::text(sub_msg.to_string())).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert_eq!(hub.active_connection_count().await, 1);
    let topics = hub.active_topics().await;
    assert!(topics.contains(&"spectra.events.orderupdates.order-99".to_string()));

    // Broadcast an event from backend
    let delivered = hub.broadcast(
        "spectra.events.orderupdates.order-99",
        json!({ "id": "order-99", "status": "SHIPPED" }),
    ).await;
    assert_eq!(delivered, 1);

    // Client receives Next message
    let resp = client.next().await.unwrap().unwrap();
    let text = resp.to_text().unwrap();
    let server_msg: ServerMessage = serde_json::from_str(text).unwrap();

    match server_msg {
        ServerMessage::Next { id, payload } => {
            assert_eq!(id, "sub-1");
            assert_eq!(payload.data.unwrap()["orderUpdates"]["status"], "SHIPPED");
        }
        other => panic!("Expected Next message, got {:?}", other),
    }

    // Client completes subscription
    let complete_msg = json!({
        "type": "complete",
        "id": "sub-1"
    });
    client.send(Message::text(complete_msg.to_string())).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Topics should be empty now
    let topics_after = hub.active_topics().await;
    assert!(topics_after.is_empty());
}
