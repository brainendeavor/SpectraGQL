use std::sync::Arc;
use std::time::Duration;
use arc_swap::ArcSwapOption;
use tokio::sync::Mutex;

use crate::telemetry::DispatchHandler;
use crate::telemetry::sink::EventSink;
use crate::protocol::{RequestInfo, ResponseInfo};

#[derive(Clone)]
pub struct NatsDispatch {
    addr: String,
    jetstream: Arc<ArcSwapOption<async_nats::jetstream::Context>>,
    connect_lock: Arc<Mutex<()>>,
    initial_reconnect_ms: u64,
    max_reconnect_ms: u64,
}

impl NatsDispatch {
    pub fn new(addr: &str) -> Self {
        let is_tty = crate::core::config::SpectraDispatchConfig::is_interactive();
        let (initial_ms, max_ms): (u64, u64) = if is_tty { (250, 5000) } else { (10, 2000) };
        Self::new_with_options(addr, initial_ms, max_ms)
    }

    pub fn new_with_options(addr: &str, initial_reconnect_ms: u64, max_reconnect_ms: u64) -> Self {
        NatsDispatch {
            addr: addr.to_string(),
            jetstream: Arc::new(ArcSwapOption::empty()),
            connect_lock: Arc::new(Mutex::new(())),
            initial_reconnect_ms,
            max_reconnect_ms,
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        method.to_ascii_lowercase().contains("nats")
    }

    /// Atomically retrieves the active JetStream context without locks on the hot path.
    /// If disconnected or uninitialized, lazily connects under single-flight mutex.
    async fn get_jetstream(&self) -> Result<Arc<async_nats::jetstream::Context>, async_nats::Error> {
        if let Some(js) = self.jetstream.load_full() {
            return Ok(js);
        }

        let _guard = self.connect_lock.lock().await;
        if let Some(js) = self.jetstream.load_full() {
            return Ok(js);
        }

        log::info!("Connecting to NATS at {}...", self.addr);
        let initial_ms = self.initial_reconnect_ms;
        let max_ms = self.max_reconnect_ms;

        let options = async_nats::ConnectOptions::new()
            .reconnect_delay_callback(move |attempts| {
                let factor = 1u64.checked_shl(attempts.min(6) as u32).unwrap_or(64);
                let delay = std::cmp::min(initial_ms.saturating_mul(factor), max_ms);
                Duration::from_millis(delay)
            })
            .event_callback(|event| async move {
                match event {
                    async_nats::Event::Disconnected => {
                        log::warn!("NATS event: broker disconnected");
                    }
                    async_nats::Event::Connected => {
                        log::info!("NATS event: broker connected");
                    }
                    async_nats::Event::SlowConsumer(cid) => {
                        log::warn!("NATS event: slow consumer on client id {}", cid);
                    }
                    _ => {}
                }
            });

        let client = async_nats::connect_with_options(&self.addr, options).await?;
        log::info!("Connected to NATS at {} successfully", self.addr);
        let js = Arc::new(async_nats::jetstream::new(client));
        self.jetstream.store(Some(js.clone()));
        Ok(js)
    }

    /// Atomically evicts the disconnected context to release socket from kqueue.
    fn evict_client(&self) {
        if self.jetstream.swap(None).is_some() {
            log::warn!("Evicted disconnected NATS client from cache to drop stale socket");
        }
    }

    async fn write_to_nats(&self, subject: &str, data: &str) -> pingora::Result<()> {
        let jetstream = match self.get_jetstream().await {
            Ok(js) => js,
            Err(e) => {
                log::error!("write_to_nats connect/init error: {}", e);
                return Err(pingora::Error::explain(
                    pingora::ErrorType::ConnectError,
                    format!("NATS connect error: {}", e),
                ));
            }
        };

        let ack = match jetstream
            .publish(subject.to_string(), data.to_string().into())
            .await
        {
            Ok(ack) => ack,
            Err(e) => {
                log::error!("write_to_nats publish error: {}", e);
                self.evict_client();
                return Err(pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("NATS publish error: {}", e),
                ));
            }
        };

        if let Err(e) = ack.await {
            log::error!("write_to_nats ack error: {}\nsubject: {}\n{}", e, subject, data);
            self.evict_client();
            return Err(pingora::Error::explain(
                pingora::ErrorType::WriteError,
                format!("NATS ack error: {}", e),
            ));
        }

        log::info!("write_to_nats done.\n{}", data);
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::telemetry::sink::EventSink for NatsDispatch {
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()> {
        let data = std::str::from_utf8(payload).map_err(|e| {
            pingora::Error::explain(
                pingora::ErrorType::Custom("Utf8Error"),
                format!("Invalid UTF-8 payload: {}", e),
            )
        })?;
        self.write_to_nats(topic, data).await
    }
}

impl DispatchHandler for NatsDispatch {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String {
        match request_info.gql.as_ref() {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string();
                let op_name = gql_op
                    .operation_name
                    .clone()
                    .or_else(|| gql_op.root_fields.first().cloned())
                    .unwrap_or_else(|| "anonymous".to_string());
                return format!("{}.{}", op_type, op_name).to_lowercase();
            }
            None => {
                return format!(
                    "{}.{}",
                    request_info.http.method.to_string().to_uppercase(),
                    request_info
                        .http
                        .uri
                        .to_string()
                        .replace(".", "_")
                        .to_lowercase()
                );
            }
        }
    }

    async fn dispatch_request_info(&self, request_info: &RequestInfo) -> pingora::Result<()> {
        log::info!("NatsDispatch.dispatch {}", request_info);
        let payload = crate::telemetry::sink::JsonEventEncoder.encode_request(request_info)?;
        let subject = self.get_dispatch_topic(request_info);
        log::info!("dispatch operation_info: {}", subject);
        self.publish(&subject, &payload).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        log::info!("NatsDispatch.dispatch ResponseInfo {:?}", response_info);
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            match self.publish(dispatch_topic, payload.as_bytes()).await {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "Failed to write_to_nats. subject: {}\nerror: {}",
                        dispatch_topic,
                        e
                    );
                }
            };
        } else {
            log::error!("Failed to queue response: {:?}", response_info);
        };
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &crate::telemetry::TerminalEvent,
    ) -> pingora::Result<()> {
        log::info!("NatsDispatch.dispatch TerminalEvent {:?}", terminal_event.id);
        let payload = crate::telemetry::sink::JsonEventEncoder.encode_completion(terminal_event)?;
        
        // Approach 2: unified terminal event on primary topic
        self.publish(dispatch_topic, &payload).await?;

        // Approach 3: split topic notification if failed
        if terminal_event.status == crate::telemetry::EventStatus::Failed {
            let failed_topic = format!("{}.failed", dispatch_topic);
            let _ = self.publish(&failed_topic, &payload).await;
        }
        Ok(())
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        self.publish(topic, payload.as_bytes()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nats_dispatch_lifecycle_and_eviction() {
        let dispatch = NatsDispatch::new_with_options("127.0.0.1:4222", 50, 1000);
        assert_eq!(dispatch.initial_reconnect_ms, 50);
        assert_eq!(dispatch.max_reconnect_ms, 1000);
        assert!(dispatch.jetstream.load().is_none());

        // Evict on empty cell is a safe no-op
        dispatch.evict_client();
        assert!(dispatch.jetstream.load().is_none());
    }

    #[test]
    fn test_nats_supports_dispatch_method() {
        assert!(NatsDispatch::supports_dispatch_method("nats"));
        assert!(NatsDispatch::supports_dispatch_method("NATS"));
        assert!(NatsDispatch::supports_dispatch_method("nats-jetstream"));
        assert!(!NatsDispatch::supports_dispatch_method("kafka"));
    }
}
