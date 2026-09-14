use crate::telemetry::DispatchHandler;
use crate::telemetry::resp::RespClient;
use crate::telemetry::sink::EventSink;
use crate::protocol::{RequestInfo, ResponseInfo};
use crate::telemetry::TerminalEvent;

#[derive(Clone)]
pub struct SierraDbDispatch {
    client: RespClient,
    stream_prefix: String,
}

impl SierraDbDispatch {
    pub fn new(addr: &str) -> Self {
        SierraDbDispatch {
            client: RespClient::new(addr),
            stream_prefix: "spectra".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn with_prefix(addr: &str, prefix: &str) -> Self {
        SierraDbDispatch {
            client: RespClient::new(addr),
            stream_prefix: prefix.to_string(),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m == "sierradb" || m == "sierra" || m == "sierra-db"
    }

    pub fn stream_id(&self, topic: &str) -> String {
        format!("{}:{}", self.stream_prefix, topic.replace('.', ":"))
    }

    /// Builds the RESP3 `EAPPEND` command natively accepted by SierraDB:
    /// `EAPPEND <stream_id> <event_type> EXPECTED_VERSION any PAYLOAD <json>`
    pub fn build_eappend_cmd(stream_id: &str, event_type: &str, payload: &str) -> redis::Cmd {
        let mut cmd = redis::cmd("EAPPEND");
        cmd.arg(stream_id)
            .arg(event_type)
            .arg("EXPECTED_VERSION")
            .arg("any")
            .arg("PAYLOAD")
            .arg(payload);
        cmd
    }

    async fn append_event(&self, stream: &str, event_type: &str, payload: &str) -> pingora::Result<()> {
        let mut conn = self.client.get_connection().await.map_err(|e| {
            log::error!("SierraDB: failed to get connection: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("SierraDB connection error: {}", e),
            )
        })?;

        let cmd = Self::build_eappend_cmd(stream, event_type, payload);
        let val: redis::Value = cmd.query_async(&mut conn).await.map_err(|e| {
            log::error!(
                "SierraDB: EAPPEND to stream '{}' [{}] failed: {}",
                stream,
                event_type,
                e
            );
            pingora::Error::explain(
                pingora::ErrorType::WriteError,
                format!("SierraDB EAPPEND error: {}", e),
            )
        })?;

        log::info!(
            "SierraDB: EAPPEND to stream '{}' [{}] successful: {:?}",
            stream,
            event_type,
            val
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::telemetry::sink::EventSink for SierraDbDispatch {
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()> {
        let stream = self.stream_id(topic);
        let payload_str = std::str::from_utf8(payload).unwrap_or("");
        self.append_event(&stream, "Event", payload_str).await
    }
}

impl DispatchHandler for SierraDbDispatch {
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
        log::info!("SierraDbDispatch.dispatch_request_info {}", request_info);
        let payload = crate::telemetry::sink::JsonEventEncoder.encode_request(request_info)?;
        let topic = self.get_dispatch_topic(request_info);
        let stream = self.stream_id(&topic);
        let event_type = request_info
            .gql
            .as_ref()
            .and_then(|g| g.operation_name.as_deref())
            .unwrap_or("Command");

        let payload_str = String::from_utf8_lossy(&payload);
        self.append_event(&stream, event_type, &payload_str).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            let stream = self.stream_id(dispatch_topic);
            self.append_event(&stream, "Response", &payload).await?;
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &TerminalEvent,
    ) -> pingora::Result<()> {
        let payload = crate::telemetry::sink::JsonEventEncoder.encode_completion(terminal_event)?;
        let stream = self.stream_id(dispatch_topic);
        let event_type = format!("TerminalEvent:{:?}", terminal_event.status);
        let payload_str = String::from_utf8_lossy(&payload);
        self.append_event(&stream, &event_type, &payload_str).await
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        self.publish(topic, payload.as_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sierradb_supports_methods() {
        assert!(SierraDbDispatch::supports_dispatch_method("sierradb"));
        assert!(SierraDbDispatch::supports_dispatch_method("SIERRADB"));
        assert!(SierraDbDispatch::supports_dispatch_method("sierra"));
        assert!(SierraDbDispatch::supports_dispatch_method("sierra-db"));
        assert!(!SierraDbDispatch::supports_dispatch_method("redis"));
        assert!(!SierraDbDispatch::supports_dispatch_method("nats"));
    }

    #[test]
    fn test_sierradb_stream_id_mapping() {
        let dispatch = SierraDbDispatch::new("sierradb://127.0.0.1:8848");
        assert_eq!(
            dispatch.stream_id("mutation.adjustinventory"),
            "spectra:mutation:adjustinventory"
        );
        assert_eq!(
            dispatch.stream_id("mutation.adjustinventory.failed"),
            "spectra:mutation:adjustinventory:failed"
        );
    }

    #[test]
    fn test_sierradb_custom_prefix() {
        let dispatch = SierraDbDispatch::with_prefix("sierradb://127.0.0.1:8848", "events");
        assert_eq!(
            dispatch.stream_id("orders.placed"),
            "events:orders:placed"
        );
    }

    #[test]
    fn test_build_eappend_command() {
        let cmd = SierraDbDispatch::build_eappend_cmd(
            "spectra:mutation:adjustInventory",
            "AdjustInventory",
            r#"{"sku":"XYZ","qty":10}"#,
        );
        let arg_strings: Vec<String> = cmd
            .args_iter()
            .map(|arg| match arg {
                redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                _ => String::new(),
            })
            .collect();
        assert_eq!(
            arg_strings,
            vec![
                "EAPPEND",
                "spectra:mutation:adjustInventory",
                "AdjustInventory",
                "EXPECTED_VERSION",
                "any",
                "PAYLOAD",
                r#"{"sku":"XYZ","qty":10}"#,
            ]
        );
    }
}
