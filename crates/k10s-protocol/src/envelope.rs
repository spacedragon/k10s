//! Envelope types and frame decoding for the k10s control protocol.
//!
//! Every message on the wire is a JSON object with a `kind` discriminator and
//! a `payload` whose shape depends on the kind. Unknown kinds are reported as
//! protocol errors rather than panics, and unknown envelope fields are ignored.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::BootstrapResponse;
use crate::error::{ErrorCode, ErrorFrame, ErrorScope};
use crate::ids::{CorrelationId, RequestId, SubscriptionId};

/// Discriminators for client-to-server frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientKind {
    /// Initial handshake frame.
    Hello,
    /// Request to cancel a previously submitted request.
    CancelRequest,
    /// Request to subscribe to a stream of events.
    Subscribe,
    /// Request to unsubscribe from a stream of events.
    Unsubscribe,
    /// Acknowledge receipt of a stream of events.
    Ack,
    /// Keepalive ping.
    Ping,
}

/// Discriminators for server-to-client frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerKind {
    /// Response to a client request.
    Response,
    /// Confirmation of a subscription.
    Subscribed,
    /// Confirmation of an unsubscribe.
    Unsubscribed,
    /// A stream of events.
    Event,
    /// Notification that a resync is required.
    ResyncRequired,
    /// An error frame.
    Error,
    /// Keepalive pong.
    Pong,
    /// Notification that the server is shutting down.
    ShutdownNotice,
}

/// A client-to-server frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientFrame {
    /// The kind of frame.
    pub kind: ClientKind,
    /// The request ID, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    /// The subscription ID, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<SubscriptionId>,
    /// The payload.
    pub payload: Value,
}

/// A server-to-client frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerFrame {
    /// The kind of frame.
    pub kind: ServerKind,
    /// The request ID, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    /// The subscription ID, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<SubscriptionId>,
    /// The payload.
    pub payload: Value,
}

impl ServerFrame {
    /// Create a new response frame.
    #[must_use]
    pub fn response(request_id: RequestId, payload: impl Serialize) -> Self {
        Self {
            kind: ServerKind::Response,
            request_id: Some(request_id),
            subscription_id: None,
            payload: serde_json::to_value(payload).expect("payload must serialize"),
        }
    }

    /// Create a new error frame.
    #[must_use]
    pub fn error(error: ErrorFrame) -> Self {
        Self {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: None,
            payload: serde_json::to_value(error).expect("error frame must serialize"),
        }
    }
}

/// A protocol error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    /// The error code.
    pub code: ErrorCode,
    /// A safe, user-facing error message.
    pub message: String,
}

impl ProtocolError {
    /// Create a new protocol error.
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Decode a client frame from raw JSON.
///
/// Returns a `ProtocolError` if the frame is malformed or has an unknown kind.
pub fn decode_client_frame(raw: &str) -> Result<ClientFrame, ProtocolError> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|e| ProtocolError::new(ErrorCode::InvalidRequest, e.to_string()))?;

    let kind_str = value
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "missing kind"))?;

    let kind = match kind_str {
        "hello" => ClientKind::Hello,
        "cancelRequest" => ClientKind::CancelRequest,
        "subscribe" => ClientKind::Subscribe,
        "unsubscribe" => ClientKind::Unsubscribe,
        "ack" => ClientKind::Ack,
        "ping" => ClientKind::Ping,
        _ => {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedMessage,
                format!("unsupported message kind: {kind_str}"),
            ));
        }
    };

    let request_id = value
        .get("requestId")
        .and_then(|r| r.as_str())
        .map(RequestId::from);

    let subscription_id = value
        .get("subscriptionId")
        .and_then(|s| s.as_str())
        .map(SubscriptionId::from);

    let payload = value.get("payload").cloned().unwrap_or(Value::Null);

    Ok(ClientFrame {
        kind,
        request_id,
        subscription_id,
        payload,
    })
}

/// Decode a server frame from a JSON value.
///
/// Returns a `ProtocolError` if the frame is malformed or has an unknown kind.
pub fn decode_server_frame(value: Value) -> Result<ServerFrame, ProtocolError> {
    let kind_str = value
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "missing kind"))?;

    let kind = match kind_str {
        "response" => ServerKind::Response,
        "subscribed" => ServerKind::Subscribed,
        "unsubscribed" => ServerKind::Unsubscribed,
        "event" => ServerKind::Event,
        "resyncRequired" => ServerKind::ResyncRequired,
        "error" => ServerKind::Error,
        "pong" => ServerKind::Pong,
        "shutdownNotice" => ServerKind::ShutdownNotice,
        _ => {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedMessage,
                format!("unsupported message kind: {kind_str}"),
            ));
        }
    };

    let request_id = value
        .get("requestId")
        .and_then(|r| r.as_str())
        .map(RequestId::from);

    let subscription_id = value
        .get("subscriptionId")
        .and_then(|s| s.as_str())
        .map(SubscriptionId::from);

    let payload = value.get("payload").cloned().unwrap_or(Value::Null);

    Ok(ServerFrame {
        kind,
        request_id,
        subscription_id,
        payload,
    })
}

/// Validate a bootstrap response payload.
///
/// Returns an error if the payload is not a valid bootstrap response.
pub fn validate_bootstrap_response(payload: &Value) -> Result<BootstrapResponse, ProtocolError> {
    serde_json::from_value(payload.clone()).map_err(|e| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("invalid bootstrap response: {e}"),
        )
    })
}

/// Create an error frame for an unsupported message.
#[must_use]
pub fn unsupported_message_error(kind: &str, correlation_id: CorrelationId) -> ErrorFrame {
    ErrorFrame::new(
        ErrorCode::UnsupportedMessage,
        format!("unsupported message kind: {kind}"),
        crate::error::Retryability::Never,
        ErrorScope::Request,
        correlation_id.as_str().to_owned(),
    )
}
