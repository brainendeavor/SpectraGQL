use crate::protocol::RequestInfo;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;

/// Marker type representing unverified, potentially tainted data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Raw;

/// Marker type representing verified, sanitized data safe for telemetry and stream persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Sanitized;

/// A type-state wrapper that tracks whether a payload has been sanitized.
///
/// Ensures at compile time that raw, potentially sensitive data (PII, tokens, secrets)
/// cannot be inadvertently published to event streams or telemetry sinks.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GuardedPayload<T, State = Sanitized> {
    payload: T,
    #[serde(skip)]
    _state: PhantomData<State>,
}

pub type RawPayload<T> = GuardedPayload<T, Raw>;
pub type SanitizedPayload<T> = GuardedPayload<T, Sanitized>;

impl<T> GuardedPayload<T, Raw> {
    /// Wraps an unverified payload in the `Raw` state.
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            _state: PhantomData,
        }
    }

    /// Access the underlying raw payload.
    pub fn inner(&self) -> &T {
        &self.payload
    }

    /// Unwraps the inner raw payload.
    pub fn into_inner(self) -> T {
        self.payload
    }

    /// Transitions to `SanitizedPayload` by applying a custom sanitization closure.
    pub fn sanitize_with<F>(self, sanitize_fn: F) -> SanitizedPayload<T>
    where
        F: FnOnce(T) -> T,
    {
        SanitizedPayload::new_unchecked(sanitize_fn(self.payload))
    }
}

impl<T> GuardedPayload<T, Sanitized> {
    /// Directly wraps a payload that has been verified or sanitized out-of-band.
    pub fn new_unchecked(payload: T) -> Self {
        Self {
            payload,
            _state: PhantomData,
        }
    }

    /// Access the underlying sanitized payload.
    pub fn inner(&self) -> &T {
        &self.payload
    }

    /// Unwraps the inner sanitized payload.
    pub fn into_inner(self) -> T {
        self.payload
    }
}

impl<T, State> Deref for GuardedPayload<T, State> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T: fmt::Debug, State> fmt::Debug for GuardedPayload<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GuardedPayload")
            .field(&self.payload)
            .finish()
    }
}

impl<T: fmt::Display, State> fmt::Display for GuardedPayload<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.payload)
    }
}

impl<T: PartialEq, State> PartialEq for GuardedPayload<T, State> {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload
    }
}

impl<T: Eq, State> Eq for GuardedPayload<T, State> {}

impl RawPayload<RequestInfo> {
    /// Sanitizes the raw RequestInfo in place and transitions state to `SanitizedPayload<RequestInfo>`.
    pub fn sanitize(mut self) -> SanitizedPayload<RequestInfo> {
        self.payload.sanitize();
        SanitizedPayload::new_unchecked(self.payload)
    }
}

impl From<RequestInfo> for SanitizedPayload<RequestInfo> {
    fn from(mut req: RequestInfo) -> Self {
        req.sanitize();
        SanitizedPayload::new_unchecked(req)
    }
}

impl From<RawPayload<RequestInfo>> for SanitizedPayload<RequestInfo> {
    fn from(raw: RawPayload<RequestInfo>) -> Self {
        raw.sanitize()
    }
}
