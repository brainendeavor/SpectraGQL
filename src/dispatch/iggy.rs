use std::sync::Arc;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use tokio::sync::OnceCell;

use iggy::prelude::*;

use crate::dispatch::DispatchHandler;
use crate::payload::{RequestInfo, ResponseInfo, TerminalEvent};

pub fn normalize_iggy_url(addr: &str) -> String {
    let trimmed = addr.trim();
    if trimmed.starts_with("iggy://") || trimmed.starts_with("iggy+tcp://") || trimmed.starts_with("iggy+http://") {
        trimmed.to_string()
    } else {
        format!("iggy://{}", trimmed)
    }
}

#[derive(Clone)]
pub struct IggyDispatch {
    url: String,
    stream_id: Identifier,
    topic_prefix: String,
    client: Arc<OnceCell<IggyClient>>,
}

impl IggyDispatch {
    pub fn new(addr: &str) -> Self {
        Self::with_stream_and_prefix(addr, "spectra", "spectra")
    }

    #[allow(dead_code)]
    pub fn with_prefix(addr: &str, prefix: &str) -> Self {
        Self::with_stream_and_prefix(addr, prefix, prefix)
    }

    pub fn with_stream_and_prefix(addr: &str, stream: &str, prefix: &str) -> Self {
        let stream_id = Identifier::named(stream).unwrap_or_else(|_| Identifier::numeric(1).unwrap());
        IggyDispatch {
            url: normalize_iggy_url(addr),
            stream_id,
            topic_prefix: prefix.to_string(),
            client: Arc::new(OnceCell::new()),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m == "iggy" || m == "apache-iggy" || m == "apache_iggy"
    }

    pub fn topic_id(&self, topic: &str) -> Identifier {
        let name = format!("{}.{}", self.topic_prefix, topic.replace(':', "."));
        Identifier::named(&name).unwrap_or_else(|_| Identifier::numeric(1).unwrap())
    }

    pub fn build_message(payload: &str) -> Result<IggyMessage> {
        IggyMessage::builder()
            .payload(Bytes::from(payload.to_string()))
            .build()
            .map_err(|e| anyhow!("Failed to build IggyMessage: {}", e))
    }

    async fn get_client(&self) -> Result<&IggyClient> {
        self.client
            .get_or_try_init(|| async {
                log::info!("Connecting to Apache Iggy server at {}...", self.url);
                let client = IggyClient::from_connection_string(&self.url)
                    .map_err(|e| anyhow!("Failed to parse Iggy connection string {}: {}", self.url, e))?;
                client.connect().await
                    .map_err(|e| anyhow!("Failed to connect to Iggy at {}: {}", self.url, e))?;
                log::info!("Connected to Apache Iggy at {} successfully", self.url);
                Ok::<IggyClient, anyhow::Error>(client)
            })
            .await
    }

    async fn send_message(&self, topic: &str, message: IggyMessage) -> pingora::Result<()> {
        let client = self.get_client().await.map_err(|e| {
            log::error!("IggyDispatch: connection error: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("Iggy connection error: {}", e),
            )
        })?;

        let topic_id = self.topic_id(topic);
        let partitioning = Partitioning::balanced();
        let mut messages = [message];
        client
            .send_messages(&self.stream_id, &topic_id, &partitioning, &mut messages)
            .await
            .map_err(|e| {
                log::error!("IggyDispatch: send to stream '{:?}', topic '{:?}' failed: {}", self.stream_id, topic_id, e);
                pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("Iggy send error: {}", e),
                )
            })?;

        log::info!("IggyDispatch: sent message to stream '{:?}', topic '{:?}'", self.stream_id, topic_id);
        Ok(())
    }
}

impl DispatchHandler for IggyDispatch {
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
        log::info!("IggyDispatch.dispatch_request_info {}", request_info);
        if let Ok(payload) = serde_json::to_string_pretty(&request_info) {
            let topic = self.get_dispatch_topic(request_info);
            if let Ok(msg) = Self::build_message(&payload) {
                self.send_message(&topic, msg).await?;
            }
        }
        Ok(())
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            if let Ok(msg) = Self::build_message(&payload) {
                self.send_message(dispatch_topic, msg).await?;
            }
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &TerminalEvent,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&terminal_event) {
            if let Ok(msg) = Self::build_message(&payload) {
                self.send_message(dispatch_topic, msg).await?;
            }
        }
        Ok(())
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        if let Ok(msg) = Self::build_message(payload) {
            self.send_message(topic, msg).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_iggy_url() {
        assert_eq!(
            normalize_iggy_url("127.0.0.1:8090"),
            "iggy://127.0.0.1:8090"
        );
        assert_eq!(
            normalize_iggy_url("iggy://localhost:8090"),
            "iggy://localhost:8090"
        );
        assert_eq!(
            normalize_iggy_url("iggy+tcp://10.0.0.1:8090"),
            "iggy+tcp://10.0.0.1:8090"
        );
    }

    #[test]
    fn test_iggy_supports_methods() {
        assert!(IggyDispatch::supports_dispatch_method("iggy"));
        assert!(IggyDispatch::supports_dispatch_method("IGGY"));
        assert!(IggyDispatch::supports_dispatch_method("apache-iggy"));
        assert!(IggyDispatch::supports_dispatch_method("apache_iggy"));
        assert!(!IggyDispatch::supports_dispatch_method("nats"));
    }

    #[test]
    fn test_build_message() {
        let msg = IggyDispatch::build_message(r#"{"hello":"iggy"}"#);
        assert!(msg.is_ok());
        let message = msg.unwrap();
        assert_eq!(message.payload, Bytes::from(r#"{"hello":"iggy"}"#));
    }
}
