use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;
use serde_json::Value;

use crate::subscriptions::protocol::{ExecutionResult, ServerMessage};

pub type ConnectionId = Uuid;
pub type SubId = String;

#[derive(Clone)]
struct SubscriptionRegistration {
    conn_id: ConnectionId,
    sub_id: SubId,
    #[allow(dead_code)]
    topic: String,
    root_field: String,
    tx: mpsc::Sender<ServerMessage>,
}

#[derive(Clone, Default)]
pub struct SubscriptionHub {
    // Map of topic -> Vec of subscriptions
    topics: Arc<RwLock<HashMap<String, Vec<SubscriptionRegistration>>>>,
    // Map of conn_id -> Vec of (sub_id, topic) for fast connection cleanup
    connections: Arc<RwLock<HashMap<ConnectionId, Vec<(SubId, String)>>>>,
}

impl SubscriptionHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new subscription for a client connection.
    pub async fn register(
        &self,
        conn_id: ConnectionId,
        sub_id: SubId,
        topic: String,
        root_field: String,
        tx: mpsc::Sender<ServerMessage>,
    ) {
        let registration = SubscriptionRegistration {
            conn_id,
            sub_id: sub_id.clone(),
            topic: topic.clone(),
            root_field,
            tx,
        };

        // Add to topic map
        {
            let mut topics_guard = self.topics.write().await;
            topics_guard
                .entry(topic.clone())
                .or_default()
                .retain(|sub| !(sub.conn_id == conn_id && sub.sub_id == sub_id));
            topics_guard.entry(topic.clone()).or_default().push(registration);
        }

        // Add to connection tracking
        {
            let mut conns_guard = self.connections.write().await;
            let list = conns_guard.entry(conn_id).or_default();
            list.retain(|(s_id, _)| s_id != &sub_id);
            list.push((sub_id, topic));
        }
    }

    /// Deregister a single subscription by client ID and sub ID.
    pub async fn deregister(&self, conn_id: &ConnectionId, sub_id: &str) {
        let mut target_topic = None;

        // Remove from connection tracking
        {
            let mut conns_guard = self.connections.write().await;
            if let Some(list) = conns_guard.get_mut(conn_id) {
                if let Some(pos) = list.iter().position(|(s_id, _)| s_id == sub_id) {
                    let (_, topic) = list.remove(pos);
                    target_topic = Some(topic);
                }
            }
        }

        // Remove from topic map
        if let Some(topic) = target_topic {
            let mut topics_guard = self.topics.write().await;
            if let Some(subs) = topics_guard.get_mut(&topic) {
                subs.retain(|sub| !(sub.conn_id == *conn_id && sub.sub_id == sub_id));
                if subs.is_empty() {
                    topics_guard.remove(&topic);
                }
            }
        }
    }

    /// Deregister all subscriptions for a closing client connection.
    pub async fn deregister_connection(&self, conn_id: &ConnectionId) {
        let mut conns_guard = self.connections.write().await;
        if let Some(sub_list) = conns_guard.remove(conn_id) {
            let mut topics_guard = self.topics.write().await;
            for (sub_id, topic) in sub_list {
                if let Some(subs) = topics_guard.get_mut(&topic) {
                    subs.retain(|sub| !(sub.conn_id == *conn_id && sub.sub_id == sub_id));
                    if subs.is_empty() {
                        topics_guard.remove(&topic);
                    }
                }
            }
        }
    }

    /// Total number of active subscriptions across all topics.
    pub async fn active_subscriptions_count(&self) -> usize {
        let topics_guard = self.topics.read().await;
        topics_guard.values().map(|v| v.len()).sum()
    }

    /// Check if a specific connection has an active subscription ID.
    pub async fn has_subscription(&self, conn_id: &ConnectionId, sub_id: &str) -> bool {
        let conns_guard = self.connections.read().await;
        if let Some(list) = conns_guard.get(conn_id) {
            list.iter().any(|(s_id, _)| s_id == sub_id)
        } else {
            false
        }
    }

    /// Broadcast a domain event payload to all active subscriptions on matching topic.
    ///
    /// Formats the payload as `{ "data": { "<root_field>": <payload> } }` if not already wrapped.
    /// Returns the number of clients that successfully received the event.
    pub async fn broadcast(&self, topic: &str, payload: Value) -> usize {
        let subscribers = {
            let topics_guard = self.topics.read().await;
            topics_guard.get(topic).cloned()
        };

        let Some(subscribers) = subscribers else {
            return 0;
        };

        let mut sent_count = 0;
        let mut dead_subscribers = Vec::new();

        for sub in subscribers {
            let formatted_data = if let Some(obj) = payload.as_object() {
                if obj.contains_key("data") {
                    payload.clone()
                } else {
                    serde_json::json!({ sub.root_field.as_str(): payload })
                }
            } else {
                serde_json::json!({ sub.root_field.as_str(): payload })
            };

            let next_msg = ServerMessage::Next {
                id: sub.sub_id.clone(),
                payload: ExecutionResult::with_data(formatted_data),
            };

            match sub.tx.try_send(next_msg) {
                Ok(_) => {
                    sent_count += 1;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    dead_subscribers.push((sub.conn_id, sub.sub_id));
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    log::warn!(
                        "SubscriptionHub: slow client buffer full for sub_id '{}' (conn '{}')",
                        sub.sub_id,
                        sub.conn_id
                    );
                }
            }
        }

        // Clean up any dead subscribers
        for (conn_id, sub_id) in dead_subscribers {
            self.deregister(&conn_id, &sub_id).await;
        }

        sent_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_broadcast() {
        let hub = SubscriptionHub::new();
        let conn_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(10);

        hub.register(
            conn_id,
            "sub-1".to_string(),
            "spectra.events.orders.42".to_string(),
            "orderUpdates".to_string(),
            tx,
        )
        .await;

        assert_eq!(hub.active_subscriptions_count().await, 1);
        assert!(hub.has_subscription(&conn_id, "sub-1").await);

        let sent = hub
            .broadcast(
                "spectra.events.orders.42",
                serde_json::json!({ "status": "CONFIRMED" }),
            )
            .await;
        assert_eq!(sent, 1);

        let received = rx.recv().await.unwrap();
        match received {
            ServerMessage::Next { id, payload } => {
                assert_eq!(id, "sub-1");
                let data = payload.data.unwrap();
                assert_eq!(data["orderUpdates"]["status"], "CONFIRMED");
            }
            _ => panic!("expected Next message"),
        }

        // Deregister
        hub.deregister(&conn_id, "sub-1").await;
        assert_eq!(hub.active_subscriptions_count().await, 0);
        assert!(!hub.has_subscription(&conn_id, "sub-1").await);
    }

    #[tokio::test]
    async fn test_deregister_connection_cleans_multiple_subs() {
        let hub = SubscriptionHub::new();
        let conn_id = Uuid::new_v4();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        hub.register(
            conn_id,
            "sub-1".to_string(),
            "spectra.events.orders.1".to_string(),
            "orderUpdates".to_string(),
            tx1,
        )
        .await;

        hub.register(
            conn_id,
            "sub-2".to_string(),
            "spectra.events.notifications".to_string(),
            "notifications".to_string(),
            tx2,
        )
        .await;

        assert_eq!(hub.active_subscriptions_count().await, 2);

        hub.deregister_connection(&conn_id).await;
        assert_eq!(hub.active_subscriptions_count().await, 0);
    }
}
