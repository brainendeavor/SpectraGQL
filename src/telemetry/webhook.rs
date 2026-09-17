use std::time::Duration;

use crate::telemetry::DispatchHandler;
use crate::protocol::{RequestInfo, ResponseInfo};

#[derive(Clone)]
pub struct WebhookDispatch {
    url: String,
    client: reqwest::Client,
}

impl WebhookDispatch {
    pub fn new(url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        WebhookDispatch {
            url: url.to_string(),
            client,
        }
    }

    pub fn supports_dispatch_method(method: &str) -> bool {
        let m = method.to_ascii_lowercase();
        m.contains("webhook") || m.contains("http")
    }

    async fn send_webhook(&self, topic: &str, body: &str) -> pingora::Result<()> {
        log::info!("WebhookDispatch: POST to {} (topic: {})", self.url, topic);
        let resp = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .header("x-spectra-topic", topic)
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| {
                log::error!("WebhookDispatch: request failed: {}", e);
                pingora::Error::explain(
                    pingora::ErrorType::WriteError,
                    format!("Webhook request failed: {}", e),
                )
            })?;

        if resp.status().is_success() {
            log::info!("WebhookDispatch: success ({})", resp.status());
            Ok(())
        } else {
            log::warn!("WebhookDispatch: HTTP status {}", resp.status());
            Err(pingora::Error::explain(
                pingora::ErrorType::Custom("WebhookHTTPError"),
                format!("Webhook returned status {}", resp.status()),
            ))
        }
    }
}

#[async_trait::async_trait]
impl crate::telemetry::sink::EventSink for WebhookDispatch {
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()> {
        let body = std::str::from_utf8(payload).map_err(|e| {
            pingora::Error::explain(
                pingora::ErrorType::Custom("Utf8Error"),
                format!("Invalid UTF-8 payload: {}", e),
            )
        })?;
        self.send_webhook(topic, body).await
    }
}

impl DispatchHandler for WebhookDispatch {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String {
        crate::telemetry::topic::TopicResolver::resolve_dispatch_topic(request_info)
    }

    async fn dispatch_request_info(&self, request_info: &RequestInfo) -> pingora::Result<()> {
        use crate::telemetry::sink::{EventSink, JsonEventEncoder};
        let topic = self.get_dispatch_topic(request_info);
        let payload = JsonEventEncoder.encode_request(request_info)?;
        self.publish(&topic, &payload).await
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        use crate::telemetry::sink::EventSink;
        if let Ok(payload) = serde_json::to_string(&response_info) {
            let _ = self.publish(dispatch_topic, payload.as_bytes()).await;
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &crate::telemetry::TerminalEvent,
    ) -> pingora::Result<()> {
        use crate::telemetry::sink::JsonEventEncoder;
        if let Ok(payload) = JsonEventEncoder.encode_completion(terminal_event) {
            let status_str = match terminal_event.status {
                crate::telemetry::EventStatus::Success => "SUCCESS",
                crate::telemetry::EventStatus::Failed => "FAILED",
                crate::telemetry::EventStatus::Rejected => "REJECTED",
            };
            let body = String::from_utf8_lossy(&payload).to_string();
            let res = self
                .client
                .post(&self.url)
                .header("content-type", "application/json")
                .header("x-spectra-topic", dispatch_topic)
                .header("x-spectra-id", terminal_event.id.to_string())
                .header("x-spectra-hlc", terminal_event.hlc.to_compact_string())
                .header("x-spectra-status", status_str)
                .body(body)
                .send()
                .await;

            match res {
                Ok(resp) => {
                    if resp.status().is_success() {
                        log::info!("WebhookDispatch terminal event success ({})", resp.status());
                    } else {
                        log::warn!("WebhookDispatch terminal event HTTP status {}", resp.status());
                    }
                }
                Err(e) => {
                    log::error!("WebhookDispatch terminal event failed: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()> {
        use crate::telemetry::sink::EventSink;
        self.publish(topic, payload.as_bytes()).await
    }
}
