pub mod nats;
pub mod webhook;

use crate::payload::{RequestInfo, ResponseInfo, TerminalEvent};
use anyhow::{Result, bail};
use enum_dispatch::enum_dispatch;

#[derive(Clone)]
#[enum_dispatch(DispatchHandler)]
pub enum DispatchMethod {
    NatsJetstream(nats::NatsDispatch),
    Webhook(webhook::WebhookDispatch),
}

pub fn find_dispatch_handler_by_method(
    method: &str,
    endpoint: &str,
) -> Result<DispatchMethod> {
    if nats::NatsDispatch::supports_dispatch_method(method) {
        return Ok(nats::NatsDispatch::new(endpoint).into());
    }
    if webhook::WebhookDispatch::supports_dispatch_method(method) {
        return Ok(webhook::WebhookDispatch::new(endpoint).into());
    }
    bail!("Unsupported dispatch method: {}", method)
}

#[enum_dispatch]
pub trait DispatchHandler {
    fn get_dispatch_topic(&self, request_info: &RequestInfo) -> String;
    async fn dispatch_request_info(&self, request_info: &RequestInfo) -> pingora::Result<()>;
    #[allow(dead_code)]
    async fn dispatch_response_info(
        &self,
        dispatch_topic: &str,
        response_info: &ResponseInfo,
    ) -> pingora::Result<()>;
    async fn dispatch_terminal_event(
        &self,
        dispatch_topic: &str,
        terminal_event: &TerminalEvent,
    ) -> pingora::Result<()>;
    #[allow(dead_code)]
    async fn dispatch_payload(&self, topic: &str, payload: &str) -> pingora::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_dispatch_nats() {
        let handler = find_dispatch_handler_by_method("nats", "127.0.0.1:4222");
        assert!(handler.is_ok());
        match handler.unwrap() {
            DispatchMethod::NatsJetstream(_) => {}
            _ => panic!("Expected NatsJetstream"),
        }
    }

    #[test]
    fn test_find_dispatch_webhook() {
        let handler = find_dispatch_handler_by_method("webhook", "http://localhost:8080/events");
        assert!(handler.is_ok());
        match handler.unwrap() {
            DispatchMethod::Webhook(_) => {}
            _ => panic!("Expected Webhook"),
        }
    }

    #[test]
    fn test_find_dispatch_unsupported() {
        let handler = find_dispatch_handler_by_method("unknown_broker", "127.0.0.1:9999");
        assert!(handler.is_err());
    }
}
