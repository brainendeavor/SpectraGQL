use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::dispatch::DispatchHandler;
use crate::payload::{RequestInfo, ResponseInfo};

#[derive(Clone)]
pub struct NatsDispatch {
    addr: String,
    jetstream: Arc<OnceCell<async_nats::jetstream::Context>>,
}

impl NatsDispatch {
    pub fn new(addr: &str) -> Self {
        NatsDispatch {
            addr: addr.to_string(),
            jetstream: Arc::new(OnceCell::new()),
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        method.to_ascii_lowercase().contains("nats")
    }

    async fn get_jetstream(&self) -> Result<&async_nats::jetstream::Context, async_nats::Error> {
        self.jetstream
            .get_or_try_init(|| async {
                log::info!("Connecting to NATS at {}...", self.addr);
                let client = async_nats::connect(&self.addr).await?;
                log::info!("Connected to NATS at {} successfully", self.addr);
                Ok(async_nats::jetstream::new(client))
            })
            .await
    }

    async fn write_to_nats(&self, subject: &str, data: &str) -> pingora::Result<()> {
        let jetstream = self.get_jetstream().await.map_err(|e| {
            log::error!("write_to_nats connect/init error: {}", e);
            pingora::Error::explain(
                pingora::ErrorType::ConnectError,
                format!("NATS connect error: {}", e),
            )
        })?;

        let ack = jetstream
            .publish(subject.to_string(), data.to_string().into())
            .await
            .map_err(|e| {
                log::error!("write_to_nats publish: {}", e);
                pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("NATS publish error: {}", e),
                )
            })?;

        ack.await.map_err(|e| {
            log::error!("write_to_nats ack: {}\nsubject: {}\n{}", e, subject, data);
            pingora::Error::explain(
                pingora::ErrorType::WriteError,
                format!("NATS ack error: {}", e),
            )
        })?;

        log::info!("write_to_nats done.\n{}", data);
        Ok(())
    }
}

impl DispatchHandler for NatsDispatch {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String {
        match request_info.gql.as_ref() {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string();
                let op_name = gql_op
                    .operation_name
                    .to_owned()
                    .unwrap_or("anonymous".to_string());
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
        let payload = serde_json::to_string_pretty(&request_info).map_err(|e| {
            log::error!("Failed to serialize request: {:?}", e);
            pingora::Error::explain(
                pingora::ErrorType::Custom("SerializationError"),
                format!("Serialization error: {}", e),
            )
        })?;
        let subject = self.get_dispatch_topic(request_info);
        log::info!("dispatch operation_info: {}", subject);
        self.write_to_nats(&subject, &payload).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        log::info!("NatsDispatch.dispatch ResponseInfo {:?}", response_info);
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            match self.write_to_nats(dispatch_topic, &payload).await {
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
        terminal_event: &crate::payload::TerminalEvent,
    ) -> pingora::Result<()> {
        log::info!("NatsDispatch.dispatch TerminalEvent {:?}", terminal_event.id);
        if let Ok(payload) = serde_json::to_string_pretty(terminal_event) {
            // Approach 2: unified terminal event on primary topic
            match self.write_to_nats(dispatch_topic, &payload).await {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "Failed to write_to_nats terminal event. subject: {}\nerror: {}",
                        dispatch_topic,
                        e
                    );
                }
            };

            // Approach 3: split topic notification if failed
            if terminal_event.status == crate::payload::EventStatus::Failed {
                let failed_topic = format!("{}.failed", dispatch_topic);
                let _ = self.write_to_nats(&failed_topic, &payload).await;
            }
        } else {
            log::error!("Failed to serialize terminal event: {:?}", terminal_event);
        }
        Ok(())
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        match self.write_to_nats(topic, payload).await {
            Ok(_) => {}
            Err(e) => {
                log::error!("Failed to write_to_nats. subject: {}\nerror: {}", topic, e);
            }
        };
        Ok(())
    }
}
