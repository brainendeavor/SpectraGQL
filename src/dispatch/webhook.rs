use std::time::Duration;

use crate::dispatch::DispatchHandler;
use crate::payload::{RequestInfo, ResponseInfo};

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

impl DispatchHandler for WebhookDispatch {
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
                        .replace(".", "_")
                        .to_lowercase()
                )
            }
        }
    }

    async fn dispatch_request_info(&self, request_info: &RequestInfo) -> pingora::Result<()> {
        let topic = self.get_dispatch_topic(request_info);
        if let Ok(payload) = serde_json::to_string_pretty(&request_info) {
            self.send_webhook(&topic, &payload).await?;
        }
        Ok(())
    }

    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(&response_info) {
            self.send_webhook(dispatch_topic, &payload).await?;
        }
        Ok(())
    }

    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &crate::payload::TerminalEvent,
    ) -> pingora::Result<()> {
        if let Ok(payload) = serde_json::to_string_pretty(terminal_event) {
            let status_str = match terminal_event.status {
                crate::payload::EventStatus::Success => "SUCCESS",
                crate::payload::EventStatus::Failed => "FAILED",
            };
            let res = self
                .client
                .post(&self.url)
                .header("content-type", "application/json")
                .header("x-spectra-topic", dispatch_topic)
                .header("x-spectra-id", terminal_event.id.to_string())
                .header("x-spectra-hlc", terminal_event.hlc.to_compact_string())
                .header("x-spectra-status", status_str)
                .body(payload)
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
        self.send_webhook(topic, payload).await
    }
}
