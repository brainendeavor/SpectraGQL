use anyhow::{anyhow, Result};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct BrokerMessage {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
}

#[async_trait::async_trait]
pub trait BrokerConsumerAdapter: Send + Sync {
    async fn subscribe(
        &self,
        subjects: &[String],
        group: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BrokerMessage> + Send>>>;
    async fn ack(&self, message: &BrokerMessage) -> Result<()>;
    async fn nack(&self, message: &BrokerMessage, delay: Duration) -> Result<()>;
}

pub struct ChannelStream {
    rx: mpsc::Receiver<BrokerMessage>,
}

impl Stream for ChannelStream {
    type Item = BrokerMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// In-memory broker adapter for unit testing and local development
pub struct InMemoryBroker {
    sender: mpsc::Sender<BrokerMessage>,
    receiver: tokio::sync::Mutex<Option<mpsc::Receiver<BrokerMessage>>>,
}

impl InMemoryBroker {
    pub fn new(capacity: usize) -> (Self, mpsc::Sender<BrokerMessage>) {
        let (tx, rx) = mpsc::channel(capacity);
        let broker = Self {
            sender: tx.clone(),
            receiver: tokio::sync::Mutex::new(Some(rx)),
        };
        (broker, tx)
    }
}

#[async_trait::async_trait]
impl BrokerConsumerAdapter for InMemoryBroker {
    async fn subscribe(
        &self,
        _subjects: &[String],
        _group: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BrokerMessage> + Send>>> {
        let mut rx_guard = self.receiver.lock().await;
        let rx = rx_guard
            .take()
            .ok_or_else(|| anyhow!("InMemoryBroker receiver already subscribed"))?;
        let stream = ChannelStream { rx };
        Ok(Box::pin(stream))
    }

    async fn ack(&self, _message: &BrokerMessage) -> Result<()> {
        Ok(())
    }

    async fn nack(&self, message: &BrokerMessage, delay: Duration) -> Result<()> {
        let tx = self.sender.clone();
        let msg = message.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(msg).await;
        });
        Ok(())
    }
}

#[cfg(feature = "nats")]
pub struct NatsConsumerAdapter {
    client: async_nats::Client,
}

#[cfg(feature = "nats")]
impl NatsConsumerAdapter {
    pub async fn connect(addr: &str) -> Result<Self> {
        let client = async_nats::connect(addr)
            .await
            .map_err(|e| anyhow!("Failed to connect to NATS at {}: {}", addr, e))?;
        Ok(Self { client })
    }
}

#[cfg(feature = "nats")]
#[async_trait::async_trait]
impl BrokerConsumerAdapter for NatsConsumerAdapter {
    async fn subscribe(
        &self,
        subjects: &[String],
        group: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = BrokerMessage> + Send>>> {
        if subjects.is_empty() {
            return Err(anyhow!("No subjects provided for NATS subscription"));
        }

        // Subscribe to primary subject with queue group for load balancing across workers
        let subject = subjects[0].clone();
        let sub = self
            .client
            .queue_subscribe(subject, group.to_string())
            .await
            .map_err(|e| anyhow!("NATS queue_subscribe error: {}", e))?;

        use futures_util::StreamExt;
        let stream = sub.map(|msg| BrokerMessage {
            id: uuid::Uuid::now_v7().to_string(),
            topic: msg.subject.to_string(),
            payload: msg.payload.to_vec(),
        });

        Ok(Box::pin(stream))
    }

    async fn ack(&self, _message: &BrokerMessage) -> Result<()> {
        // Core NATS queue subscribe auto-acknowledges
        Ok(())
    }

    async fn nack(&self, _message: &BrokerMessage, _delay: Duration) -> Result<()> {
        Ok(())
    }
}

pub async fn create_broker(
    config: &crate::config::BrokerConfig,
) -> Result<Arc<dyn BrokerConsumerAdapter>> {
    match config.method.to_lowercase().as_str() {
        #[cfg(feature = "nats")]
        "nats" => {
            let adapter = NatsConsumerAdapter::connect(&config.addr).await?;
            Ok(Arc::new(adapter))
        }
        "in_memory" | "memory" => {
            let (adapter, _) = InMemoryBroker::new(1024);
            Ok(Arc::new(adapter))
        }
        other => Err(anyhow!("Unsupported broker method: '{}'", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_in_memory_broker_pub_sub() {
        let (broker, tx) = InMemoryBroker::new(10);
        let mut stream = broker
            .subscribe(&["test.events".to_string()], "group-1")
            .await
            .unwrap();

        tx.send(BrokerMessage {
            id: "msg-1".to_string(),
            topic: "test.events".to_string(),
            payload: b"hello flux".to_vec(),
        })
        .await
        .unwrap();

        let msg = stream.next().await.unwrap();
        assert_eq!(msg.id, "msg-1");
        assert_eq!(msg.topic, "test.events");
        assert_eq!(msg.payload, b"hello flux");
    }
}
