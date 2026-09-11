use std::collections::BTreeMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use rskafka::chrono::Utc;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use tokio::sync::OnceCell;

use crate::dispatch::DispatchHandler;
use crate::payload::{RequestInfo, ResponseInfo, TerminalEvent};

pub fn normalize_kafka_hosts(addr: &str) -> Vec<String> {
    let trimmed = addr.trim();
    let stripped = if let Some(s) = trimmed.strip_prefix("kafka://") {
        s
    } else if let Some(s) = trimmed.strip_prefix("redpanda://") {
        s
    } else {
        trimmed
    };

    stripped
        .split(',')
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .collect()
}

#[derive(Clone)]
pub struct KafkaDispatch {
    hosts: Vec<String>,
    topic_prefix: String,
    partition: i32,
    client: Arc<OnceCell<Client>>,
}

impl KafkaDispatch {
    pub fn new(addr: &str) -> Self {
        Self::with_prefix_and_partition(addr, "spectra", 0)
    }

    #[allow(dead_code)]
    pub fn with_prefix(addr: &str, prefix: &str) -> Self {
        Self::with_prefix_and_partition(addr, prefix, 0)
    }

    pub fn with_prefix_and_partition(addr: &str, prefix: &str, partition: i32) -> Self {
        KafkaDispatch {
            hosts: normalize_kafka_hosts(addr),
            topic_prefix: prefix.to_string(),
            partition,
            client: Arc::new(OnceCell::new()),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m == "kafka" || m == "redpanda" || m == "kafka-cluster"
    }

    pub fn kafka_topic(&self, topic: &str) -> String {
        format!("{}.{}", self.topic_prefix, topic.replace(':', "."))
    }

    pub fn build_record(
        key: Option<&str>,
        payload: &str,
        headers: &[(&str, &str)],
    ) -> Record {
        let mut header_map = BTreeMap::new();
        for (k, v) in headers {
            header_map.insert(k.to_string(), v.as_bytes().to_vec());
        }
        Record {
            key: key.map(|k| k.as_bytes().to_vec()),
            value: Some(payload.as_bytes().to_vec()),
            headers: header_map,
            timestamp: Utc::now(),
        }
    }

    async fn get_client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                log::info!("Connecting to Kafka/Redpanda cluster at {:?}...", self.hosts);
                let client = ClientBuilder::new(self.hosts.clone())
                    .build()
                    .await
                    .map_err(|e| anyhow!("Failed to connect to Kafka/Redpanda at {:?}: {}", self.hosts, e))?;
                log::info!("Connected to Kafka/Redpanda at {:?} successfully", self.hosts);
                Ok::<Client, anyhow::Error>(client)
            })
            .await
    }

    async fn produce_record(&self, topic: &str, record: Record) -> pingora::Result<()> {
        let client = self.get_client().await.map_err(|e| {
            log::error!("KafkaDispatch: connection error: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("Kafka connection error: {}", e),
            )
        })?;

        let partition_client = client
            .partition_client(topic.to_string(), self.partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| {
                log::error!("KafkaDispatch: failed to get partition client for topic '{}': {}", topic, e);
                pingora::Error::explain(
                    pingora::ErrorType::ConnectError,
                    format!("Kafka partition error: {}", e),
                )
            })?;

        match partition_client.produce(vec![record], Compression::default()).await {
            Ok(offsets) => {
                log::info!("KafkaDispatch: produced to '{}' [p:{}], offsets: {:?}", topic, self.partition, offsets);
                Ok(())
            }
            Err(e) => {
                log::error!("KafkaDispatch: produce to '{}' failed: {}", topic, e);
                Err(pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("Kafka produce error: {}", e),
                ))
            }
        }
    }
}

impl DispatchHandler for KafkaDispatch {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String {
        match request_info.gql.as_ref() {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string();
                let op_name = gql_op
                    .operation_name
                    .to_owned()
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
        log::info!("KafkaDispatch.dispatch_request_info {}", request_info);
        if let Ok(payload) = serde_json::to_string_pretty(&request_info) {
            let topic = self.get_dispatch_topic(request_info);
            let kafka_topic = self.kafka_topic(&topic);
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

            let record = Self::build_record(Some(&req_id), &payload, &headers);
            self.produce_record(&kafka_topic, record).await?;
        }
        Ok(())
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            let kafka_topic = self.kafka_topic(dispatch_topic);
            let req_id = response_info.request_id.to_string();
            let hlc = response_info.hlc.to_compact_string();

            let headers = [
                ("event_type", "Response"),
                ("request_id", req_id.as_str()),
                ("hlc", hlc.as_str()),
            ];

            let record = Self::build_record(Some(&req_id), &payload, &headers);
            self.produce_record(&kafka_topic, record).await?;
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &TerminalEvent,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&terminal_event) {
            let kafka_topic = self.kafka_topic(dispatch_topic);
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

            let record = Self::build_record(Some(&req_id), &payload, &headers);
            self.produce_record(&kafka_topic, record).await?;
        }
        Ok(())
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        let kafka_topic = self.kafka_topic(topic);
        let record = Self::build_record(None, payload, &[]);
        self.produce_record(&kafka_topic, record).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_kafka_hosts() {
        assert_eq!(
            normalize_kafka_hosts("localhost:9092"),
            vec!["localhost:9092".to_string()]
        );
        assert_eq!(
            normalize_kafka_hosts("kafka://10.0.0.1:9092, 10.0.0.2:9092"),
            vec!["10.0.0.1:9092".to_string(), "10.0.0.2:9092".to_string()]
        );
        assert_eq!(
            normalize_kafka_hosts("redpanda://redpanda-broker:9092"),
            vec!["redpanda-broker:9092".to_string()]
        );
    }

    #[test]
    fn test_kafka_supports_methods() {
        assert!(KafkaDispatch::supports_dispatch_method("kafka"));
        assert!(KafkaDispatch::supports_dispatch_method("KAFKA"));
        assert!(KafkaDispatch::supports_dispatch_method("redpanda"));
        assert!(KafkaDispatch::supports_dispatch_method("kafka-cluster"));
        assert!(!KafkaDispatch::supports_dispatch_method("nats"));
    }

    #[test]
    fn test_kafka_topic_mapping() {
        let dispatch = KafkaDispatch::new("localhost:9092");
        assert_eq!(
            dispatch.kafka_topic("mutation.adjustinventory"),
            "spectra.mutation.adjustinventory"
        );
        assert_eq!(
            dispatch.kafka_topic("mutation:adjustinventory:failed"),
            "spectra.mutation.adjustinventory.failed"
        );
    }

    #[test]
    fn test_kafka_custom_prefix() {
        let dispatch = KafkaDispatch::with_prefix("localhost:9092", "production_events");
        assert_eq!(
            dispatch.kafka_topic("orders.placed"),
            "production_events.orders.placed"
        );
    }

    #[test]
    fn test_build_record() {
        let headers = [("event_type", "Command"), ("request_id", "req-123")];
        let record = KafkaDispatch::build_record(Some("req-123"), r#"{"data":true}"#, &headers);

        assert_eq!(record.key, Some(b"req-123".to_vec()));
        assert_eq!(record.value, Some(br#"{"data":true}"#.to_vec()));
        assert_eq!(record.headers.get("event_type"), Some(&b"Command".to_vec()));
        assert_eq!(record.headers.get("request_id"), Some(&b"req-123".to_vec()));
    }
}
