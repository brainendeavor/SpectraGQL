use crate::telemetry::DispatchHandler;
use crate::telemetry::resp::RespClient;
use crate::telemetry::sink::EventSink;
use crate::protocol::{RequestInfo, ResponseInfo};
use crate::telemetry::TerminalEvent;

#[derive(Clone)]
pub struct RedisStreamsDispatch {
    client: RespClient,
    stream_prefix: String,
}

impl RedisStreamsDispatch {
    pub fn new(addr: &str) -> Self {
        RedisStreamsDispatch {
            client: RespClient::new(addr),
            stream_prefix: "spectra".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn with_prefix(addr: &str, prefix: &str) -> Self {
        RedisStreamsDispatch {
            client: RespClient::new(addr),
            stream_prefix: prefix.to_string(),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m == "redis" || m == "redis_streams" || m == "redis-streams" || m == "valkey" || m == "dragonfly"
    }

    pub fn stream_key(&self, topic: &str) -> String {
        format!("{}:{}", self.stream_prefix, topic.replace('.', ":"))
    }

    pub fn build_xadd_cmd(stream: &str, fields: &[(&str, &str)]) -> redis::Cmd {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (k, v) in fields {
            cmd.arg(*k).arg(*v);
        }
        cmd
    }

    async fn write_to_stream(&self, stream: &str, fields: &[(&str, &str)]) -> pingora::Result<()> {
        let mut conn = self.client.get_connection().await.map_err(|e| {
            log::error!("RedisStreams: failed to get connection: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("Redis connection error: {}", e),
            )
        })?;

        let cmd = Self::build_xadd_cmd(stream, fields);
        let entry_id: String = cmd.query_async(&mut conn).await.map_err(|e| {
            log::error!("RedisStreams: XADD to '{}' failed: {}", stream, e);
            pingora::Error::explain(
                pingora::ErrorType::WriteError,
                format!("Redis XADD error: {}", e),
            )
        })?;

        log::info!("RedisStreams: XADD to '{}' successful, entry id: {}", stream, entry_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::telemetry::sink::EventSink for RedisStreamsDispatch {
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()> {
        let stream = self.stream_key(topic);
        let payload_str = std::str::from_utf8(payload).unwrap_or("");
        let fields = [
            ("event_type", "Event"),
            ("payload", payload_str),
        ];
        self.write_to_stream(&stream, &fields).await
    }
}

impl DispatchHandler for RedisStreamsDispatch {
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
        log::info!("RedisStreamsDispatch.dispatch {}", request_info);
        let payload_bytes = crate::telemetry::sink::JsonEventEncoder.encode_request(request_info)?;
        let payload = String::from_utf8_lossy(&payload_bytes);
        let topic = self.get_dispatch_topic(request_info);
        let stream = self.stream_key(&topic);
        let req_id_str = request_info.request_id.to_string();
        let hlc_str = request_info.hlc.to_compact_string();
        let op_name = request_info
            .gql
            .as_ref()
            .and_then(|g| g.operation_name.as_deref())
            .unwrap_or("unknown");

        let fields = [
            ("event_type", "Command"),
            ("request_id", req_id_str.as_str()),
            ("hlc", hlc_str.as_str()),
            ("operation", op_name),
            ("payload", payload.as_ref()),
        ];

        self.write_to_stream(&stream, &fields).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            let stream = self.stream_key(dispatch_topic);
            let req_id_str = response_info.request_id.to_string();
            let hlc_str = response_info.hlc.to_compact_string();

            let fields = [
                ("event_type", "Response"),
                ("request_id", req_id_str.as_str()),
                ("hlc", hlc_str.as_str()),
                ("payload", payload.as_str()),
            ];

            self.write_to_stream(&stream, &fields).await?;
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
        let stream = self.stream_key(dispatch_topic);
        let req_id_str = terminal_event.request.request_id.to_string();
        let hlc_str = terminal_event.hlc.to_compact_string();
        let duration_str = terminal_event.duration_ms.to_string();
        let status_str = format!("{:?}", terminal_event.status);
        let op_name = terminal_event.operation_name.as_deref().unwrap_or("unknown");

        let fields = [
            ("event_type", "TerminalEvent"),
            ("status", status_str.as_str()),
            ("request_id", req_id_str.as_str()),
            ("hlc", hlc_str.as_str()),
            ("operation", op_name),
            ("duration_ms", duration_str.as_str()),
            ("payload", payload.as_ref()),
        ];

        self.write_to_stream(&stream, &fields).await
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        self.publish(topic, payload.as_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_streams_supports_methods() {
        assert!(RedisStreamsDispatch::supports_dispatch_method("redis"));
        assert!(RedisStreamsDispatch::supports_dispatch_method("REDIS"));
        assert!(RedisStreamsDispatch::supports_dispatch_method("redis_streams"));
        assert!(RedisStreamsDispatch::supports_dispatch_method("redis-streams"));
        assert!(RedisStreamsDispatch::supports_dispatch_method("valkey"));
        assert!(RedisStreamsDispatch::supports_dispatch_method("dragonfly"));
        assert!(!RedisStreamsDispatch::supports_dispatch_method("nats"));
    }

    #[test]
    fn test_redis_streams_key_mapping() {
        let dispatch = RedisStreamsDispatch::new("127.0.0.1:6379");
        assert_eq!(
            dispatch.stream_key("mutation.adjustinventory"),
            "spectra:mutation:adjustinventory"
        );
        assert_eq!(
            dispatch.stream_key("mutation.adjustinventory.failed"),
            "spectra:mutation:adjustinventory:failed"
        );
    }

    #[test]
    fn test_redis_streams_custom_prefix() {
        let dispatch = RedisStreamsDispatch::with_prefix("127.0.0.1:6379", "app_events");
        assert_eq!(
            dispatch.stream_key("orders.created"),
            "app_events:orders:created"
        );
    }

    #[test]
    fn test_build_xadd_command() {
        let fields = [
            ("event_type", "Command"),
            ("request_id", "0191e704-5f50-7000-8000-000000000001"),
            ("hlc", "1725980000000:1"),
            ("payload", "{\"foo\":\"bar\"}"),
        ];
        let cmd = RedisStreamsDispatch::build_xadd_cmd("spectra:mutation:test", &fields);
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
                "XADD",
                "spectra:mutation:test",
                "*",
                "event_type",
                "Command",
                "request_id",
                "0191e704-5f50-7000-8000-000000000001",
                "hlc",
                "1725980000000:1",
                "payload",
                "{\"foo\":\"bar\"}",
            ]
        );
    }
}
