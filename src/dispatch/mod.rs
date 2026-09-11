pub mod iggy;
pub mod kafka;
pub mod nats;
pub mod rabbitmq;
pub mod redis_streams;
pub mod resp;
pub mod sierradb;
pub mod webhook;

use crate::payload::{RequestInfo, ResponseInfo, TerminalEvent};
use anyhow::{Result, bail};
use enum_dispatch::enum_dispatch;

#[derive(Clone)]
#[enum_dispatch(DispatchHandler)]
pub enum DispatchMethod {
    Iggy(iggy::IggyDispatch),
    Kafka(kafka::KafkaDispatch),
    NatsJetstream(nats::NatsDispatch),
    RabbitMq(rabbitmq::RabbitMqDispatch),
    RedisStreams(redis_streams::RedisStreamsDispatch),
    SierraDb(sierradb::SierraDbDispatch),
    Webhook(webhook::WebhookDispatch),
}

pub fn find_dispatch_handler_by_method(
    method: &str,
    endpoint: &str,
) -> Result<DispatchMethod> {
    if nats::NatsDispatch::supports_dispatch_method(method) {
        return Ok(nats::NatsDispatch::new(endpoint).into());
    }
    if redis_streams::RedisStreamsDispatch::supports_dispatch_method(method) {
        return Ok(redis_streams::RedisStreamsDispatch::new(endpoint).into());
    }
    if sierradb::SierraDbDispatch::supports_dispatch_method(method) {
        return Ok(sierradb::SierraDbDispatch::new(endpoint).into());
    }
    if kafka::KafkaDispatch::supports_dispatch_method(method) {
        return Ok(kafka::KafkaDispatch::new(endpoint).into());
    }
    if rabbitmq::RabbitMqDispatch::supports_dispatch_method(method) {
        return Ok(rabbitmq::RabbitMqDispatch::new(endpoint).into());
    }
    if iggy::IggyDispatch::supports_dispatch_method(method) {
        return Ok(iggy::IggyDispatch::new(endpoint).into());
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
    fn test_find_dispatch_redis() {
        for method in &["redis", "redis_streams", "valkey", "dragonfly"] {
            let handler = find_dispatch_handler_by_method(method, "127.0.0.1:6379");
            assert!(handler.is_ok(), "failed for {}", method);
            match handler.unwrap() {
                DispatchMethod::RedisStreams(_) => {}
                _ => panic!("Expected RedisStreams for {}", method),
            }
        }
    }

    #[test]
    fn test_find_dispatch_sierradb() {
        for method in &["sierradb", "sierra", "sierra-db"] {
            let handler = find_dispatch_handler_by_method(method, "127.0.0.1:8848");
            assert!(handler.is_ok(), "failed for {}", method);
            match handler.unwrap() {
                DispatchMethod::SierraDb(_) => {}
                _ => panic!("Expected SierraDb for {}", method),
            }
        }
    }

    #[test]
    fn test_find_dispatch_kafka() {
        for method in &["kafka", "redpanda", "kafka-cluster"] {
            let handler = find_dispatch_handler_by_method(method, "127.0.0.1:9092");
            assert!(handler.is_ok(), "failed for {}", method);
            match handler.unwrap() {
                DispatchMethod::Kafka(_) => {}
                _ => panic!("Expected Kafka for {}", method),
            }
        }
    }

    #[test]
    fn test_find_dispatch_rabbitmq() {
        for method in &["rabbitmq", "rabbit", "amqp", "amqps"] {
            let handler = find_dispatch_handler_by_method(method, "127.0.0.1:5672");
            assert!(handler.is_ok(), "failed for {}", method);
            match handler.unwrap() {
                DispatchMethod::RabbitMq(_) => {}
                _ => panic!("Expected RabbitMq for {}", method),
            }
        }
    }

    #[test]
    fn test_find_dispatch_iggy() {
        for method in &["iggy", "apache-iggy", "apache_iggy"] {
            let handler = find_dispatch_handler_by_method(method, "127.0.0.1:8090");
            assert!(handler.is_ok(), "failed for {}", method);
            match handler.unwrap() {
                DispatchMethod::Iggy(_) => {}
                _ => panic!("Expected Iggy for {}", method),
            }
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
