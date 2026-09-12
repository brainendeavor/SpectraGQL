use async_trait::async_trait;
use crate::protocol::{RequestInfo, SanitizedPayload};
use crate::telemetry::event::TerminalEvent;

/// Atomic transport sink for event publishing.
/// Implementing adapters only need to manage socket/connection semantics
/// and send raw payload bytes to a designated topic or stream.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Publishes raw bytes to the specified destination topic / subject / stream.
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()>;
}

/// Encoder contract separating serialization logic from transport adapters.
pub trait EventEncoder: Send + Sync {
    fn encode_request(&self, request: &RequestInfo) -> pingora::Result<Vec<u8>>;
    fn encode_completion(&self, completion: &TerminalEvent) -> pingora::Result<Vec<u8>>;

    /// Encodes a verified, sanitized request info payload.
    fn encode_sanitized_request(
        &self,
        request: &SanitizedPayload<RequestInfo>,
    ) -> pingora::Result<Vec<u8>> {
        self.encode_request(request)
    }
}

/// Standard JSON event encoder using serde_json.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonEventEncoder;

impl EventEncoder for JsonEventEncoder {
    fn encode_request(&self, request: &RequestInfo) -> pingora::Result<Vec<u8>> {
        serde_json::to_vec_pretty(request).map_err(|e| {
            log::error!("JsonEventEncoder: failed to serialize request: {:?}", e);
            pingora::Error::explain(
                pingora::ErrorType::Custom("SerializationError"),
                format!("Request serialization error: {}", e),
            )
        })
    }

    fn encode_completion(&self, completion: &TerminalEvent) -> pingora::Result<Vec<u8>> {
        serde_json::to_vec_pretty(completion).map_err(|e| {
            log::error!("JsonEventEncoder: failed to serialize completion event: {:?}", e);
            pingora::Error::explain(
                pingora::ErrorType::Custom("SerializationError"),
                format!("CompletionEvent serialization error: {}", e),
            )
        })
    }
}

impl JsonEventEncoder {
    pub fn encode_request(&self, request: &RequestInfo) -> pingora::Result<Vec<u8>> {
        <Self as EventEncoder>::encode_request(self, request)
    }

    pub fn encode_sanitized_request(
        &self,
        request: &SanitizedPayload<RequestInfo>,
    ) -> pingora::Result<Vec<u8>> {
        <Self as EventEncoder>::encode_sanitized_request(self, request)
    }

    pub fn encode_completion(&self, completion: &TerminalEvent) -> pingora::Result<Vec<u8>> {
        <Self as EventEncoder>::encode_completion(self, completion)
    }
}
