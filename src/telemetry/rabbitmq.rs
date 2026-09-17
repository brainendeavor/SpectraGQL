use std::sync::Arc;
use anyhow::{Result, anyhow};
use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use tokio::sync::OnceCell;

use crate::telemetry::DispatchHandler;
use crate::protocol::{RequestInfo, ResponseInfo};
use crate::telemetry::TerminalEvent;

pub fn normalize_amqp_url(addr: &str) -> String {
    let trimmed = addr.trim();
    if trimmed.starts_with("amqp://") || trimmed.starts_with("amqps://") {
        trimmed.to_string()
    } else if let Some(stripped) = trimmed.strip_prefix("rabbitmq://") {
        format!("amqp://{}", stripped)
    } else {
        format!("amqp://{}", trimmed)
    }
}

#[derive(Clone)]
pub struct RabbitMqDispatch {
    url: String,
    exchange: String,
    routing_prefix: String,
    connection: Arc<OnceCell<Connection>>,
}

impl RabbitMqDispatch {
    pub fn new(addr: &str) -> Self {
        Self::with_exchange_and_prefix(addr, "amq.topic", "spectra")
    }

    #[allow(dead_code)]
    pub fn with_prefix(addr: &str, prefix: &str) -> Self {
        Self::with_exchange_and_prefix(addr, "amq.topic", prefix)
    }

    pub fn with_exchange_and_prefix(addr: &str, exchange: &str, prefix: &str) -> Self {
        RabbitMqDispatch {
            url: normalize_amqp_url(addr),
            exchange: exchange.to_string(),
            routing_prefix: prefix.to_string(),
            connection: Arc::new(OnceCell::new()),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m == "rabbitmq" || m == "rabbit" || m == "amqp" || m == "amqps"
    }

    pub fn routing_key(&self, topic: &str) -> String {
        format!("{}.{}", self.routing_prefix, topic.replace(':', "."))
    }

    pub fn build_properties(
        content_type: &str,
        headers: &[(&str, &str)],
    ) -> BasicProperties {
        let mut field_table = FieldTable::default();
        for (k, v) in headers {
            field_table.insert(
                ShortString::from(*k),
                AMQPValue::LongString(v.to_string().into_bytes().into()),
            );
        }

        BasicProperties::default()
            .with_content_type(ShortString::from(content_type))
            .with_delivery_mode(2) // Persistent delivery
            .with_headers(field_table)
    }

    async fn get_connection(&self) -> Result<&Connection> {
        self.connection
            .get_or_try_init(|| async {
                log::info!("Connecting to RabbitMQ broker at {}...", self.url);
                let conn = Connection::connect(&self.url, ConnectionProperties::default())
                    .await
                    .map_err(|e| anyhow!("Failed to connect to RabbitMQ at {}: {}", self.url, e))?;
                log::info!("Connected to RabbitMQ at {} successfully", self.url);
                Ok::<Connection, anyhow::Error>(conn)
            })
            .await
    }

    async fn publish_raw(
        &self,
        routing_key: &str,
        payload: &str,
        properties: BasicProperties,
    ) -> pingora::Result<()> {
        let conn = self.get_connection().await.map_err(|e| {
            log::error!("RabbitMqDispatch: connection error: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("RabbitMQ connection error: {}", e),
            )
        })?;

        let channel = conn.create_channel().await.map_err(|e| {
            log::error!("RabbitMqDispatch: failed to open channel: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("RabbitMQ channel error: {}", e),
            )
        })?;

        let confirm = channel
            .basic_publish(
                ShortString::from(self.exchange.as_str()),
                ShortString::from(routing_key),
                BasicPublishOptions::default(),
                payload.as_bytes(),
                properties,
            )
            .await
            .map_err(|e| {
                log::error!(
                    "RabbitMqDispatch: publish to ex:'{}' rk:'{}' failed: {}",
                    self.exchange,
                    routing_key,
                    e
                );
                pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("RabbitMQ publish error: {}", e),
                )
            })?;

        let _ = confirm.await;
        log::info!(
            "RabbitMqDispatch: published to ex:'{}' rk:'{}'",
            self.exchange,
            routing_key
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::telemetry::sink::EventSink for RabbitMqDispatch {
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()> {
        let rk = self.routing_key(topic);
        let payload_str = std::str::from_utf8(payload).unwrap_or("");
        let properties = Self::build_properties("application/json", &[]);
        self.publish_raw(&rk, payload_str, properties).await
    }
}

impl DispatchHandler for RabbitMqDispatch {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String {
        match request_info.gql.as_ref() {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string();
                let op_name = gql_op
                    .operation_name
                    .clone()
                    .or_else(|| gql_op.root_fields.first().cloned())
                    .unwrap_or_else(|| "anonymous".to_string());
                format!("{}.{}", op_type, op_name).to_lowercase()
            }
            None => {
                format!(
                    "{}.{}",
                    request_info.http.method.to_string().to_uppercase(),
                    request_info
                        .http
                        .uri
                        .to_string()
                        .replace('.', "_")
                        .to_lowercase()
                )
            }
        }
    }

    async fn dispatch_request_info(&self, request_info: &RequestInfo) -> pingora::Result<()> {
        let payload_bytes = crate::telemetry::sink::JsonEventEncoder.encode_request(request_info)?;
        let payload = String::from_utf8_lossy(&payload_bytes);
        let rk = self.routing_key(&self.get_dispatch_topic(request_info));
        let req_id = request_info.request_id.to_string();
        let hlc = request_info.hlc.to_compact_string();
        let op_name = request_info
            .gql
            .as_ref()
            .and_then(|g| g.operation_name.as_deref())
            .unwrap_or("unknown");

        let headers = [
            ("event_type", "Command"),
            ("request_id", req_id.as_str()),
            ("hlc", hlc.as_str()),
            ("operation", op_name),
        ];

        let properties = Self::build_properties("application/json", &headers);
        self.publish_raw(&rk, &payload, properties).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string(&response_info) {
            let rk = self.routing_key(dispatch_topic);
            let req_id = response_info.request_id.to_string();
            let hlc = response_info.hlc.to_compact_string();

            let headers = [
                ("event_type", "Response"),
                ("request_id", req_id.as_str()),
                ("hlc", hlc.as_str()),
            ];

            let properties = Self::build_properties("application/json", &headers);
            self.publish_raw(&rk, &payload, properties).await?;
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &TerminalEvent,
    ) -> pingora::Result<()> {
        let payload_bytes = crate::telemetry::sink::JsonEventEncoder.encode_completion(terminal_event)?;
        let payload = String::from_utf8_lossy(&payload_bytes);
        let rk = self.routing_key(dispatch_topic);
        let req_id = terminal_event.request.request_id.to_string();
        let hlc = terminal_event.hlc.to_compact_string();
        let status = format!("{:?}", terminal_event.status);
        let op_name = terminal_event.operation_name.as_deref().unwrap_or("unknown");
        let duration_str = terminal_event.duration_ms.to_string();

        let headers = [
            ("event_type", "TerminalEvent"),
            ("status", status.as_str()),
            ("request_id", req_id.as_str()),
            ("hlc", hlc.as_str()),
            ("operation", op_name),
            ("duration_ms", duration_str.as_str()),
        ];

        let properties = Self::build_properties("application/json", &headers);
        self.publish_raw(&rk, &payload, properties).await
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        let rk = self.routing_key(topic);
        self.publish_raw(&rk, payload, Self::build_properties("application/json", &[])).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_amqp_url() {
        assert_eq!(
            normalize_amqp_url("127.0.0.1:5672"),
            "amqp://127.0.0.1:5672"
        );
        assert_eq!(
            normalize_amqp_url("amqp://guest:guest@localhost:5672/%2f"),
            "amqp://guest:guest@localhost:5672/%2f"
        );
        assert_eq!(
            normalize_amqp_url("amqps://secure-rabbit.corp:5671"),
            "amqps://secure-rabbit.corp:5671"
        );
        assert_eq!(
            normalize_amqp_url("rabbitmq://localhost:5672"),
            "amqp://localhost:5672"
        );
    }

    #[test]
    fn test_rabbitmq_supports_methods() {
        assert!(RabbitMqDispatch::supports_dispatch_method("rabbitmq"));
        assert!(RabbitMqDispatch::supports_dispatch_method("RABBITMQ"));
        assert!(RabbitMqDispatch::supports_dispatch_method("rabbit"));
        assert!(RabbitMqDispatch::supports_dispatch_method("amqp"));
        assert!(RabbitMqDispatch::supports_dispatch_method("amqps"));
        assert!(!RabbitMqDispatch::supports_dispatch_method("nats"));
    }

    #[test]
    fn test_rabbitmq_routing_key_mapping() {
        let dispatch = RabbitMqDispatch::new("127.0.0.1:5672");
        assert_eq!(
            dispatch.routing_key("mutation.adjustinventory"),
            "spectra.mutation.adjustinventory"
        );
        assert_eq!(
            dispatch.routing_key("mutation:adjustinventory:failed"),
            "spectra.mutation.adjustinventory.failed"
        );
    }

    #[test]
    fn test_rabbitmq_custom_prefix_and_exchange() {
        let dispatch = RabbitMqDispatch::with_exchange_and_prefix(
            "127.0.0.1:5672",
            "events_exchange",
            "custom_app",
        );
        assert_eq!(dispatch.exchange, "events_exchange");
        assert_eq!(
            dispatch.routing_key("orders.placed"),
            "custom_app.orders.placed"
        );
    }

    #[test]
    fn test_build_properties() {
        let headers = [("event_type", "Command"), ("request_id", "req-123")];
        let props = RabbitMqDispatch::build_properties("application/json", &headers);

        assert_eq!(props.content_type().as_ref().map(|s| s.as_str()), Some("application/json"));
        assert_eq!(*props.delivery_mode(), Some(2));
        assert!(props.headers().is_some());
    }
}
