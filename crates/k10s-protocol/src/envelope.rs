//! Stable envelopes and typed payloads for the k10s control protocol.
//!
//! The outer envelope is decoded before its payload. This lets older peers
//! report an unknown message kind without attempting to deserialize a payload
//! whose schema they do not understand.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::{BootstrapResponse, ProtocolVersion};
use crate::error::{ErrorCode, ErrorFrame, ErrorScope, Retryability};
use crate::ids::{CorrelationId, OperationId, RequestId, SessionId, SubscriptionId};

/// Discriminators for client-to-server frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientKind {
    /// Initial authentication and capability negotiation.
    Hello,
    /// A control request.
    Request,
    /// Cancel a previously submitted request.
    CancelRequest,
    /// Subscribe to a stream of events.
    Subscribe,
    /// End a subscription.
    Unsubscribe,
    /// Advance the session resume cursor.
    Ack,
    /// Keepalive ping.
    Ping,
}

/// Discriminators for server-to-client frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerKind {
    /// Successful authentication and negotiated protocol settings.
    Welcome,
    /// A response to a client request.
    Response,
    /// Confirmation of a subscription.
    Subscribed,
    /// Notification that a subscription has completed.
    Complete,
    /// A stream event.
    Event,
    /// Beginning of a chunked snapshot.
    SnapshotBegin,
    /// A chunk of a snapshot.
    SnapshotChunk,
    /// End of a chunked snapshot.
    SnapshotEnd,
    /// A background operation status update.
    OperationUpdate,
    /// Notification that a full resync is required.
    ResyncRequired,
    /// A structured error.
    Error,
    /// Keepalive pong.
    Pong,
    /// Notification that the server is shutting down.
    ShutdownNotice,
}

/// Stable client-to-server wire envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientFrame {
    /// Message discriminator.
    pub kind: ClientKind,
    /// Request identifier, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    /// Subscription identifier, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<SubscriptionId>,
    /// Connection sequence, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Kind-specific payload.
    #[serde(default)]
    pub payload: Value,
}

impl ClientFrame {
    /// Decode the kind-specific payload.
    pub fn decode_payload(&self) -> Result<ClientPayload, ProtocolError> {
        match self.kind {
            ClientKind::Hello => self.payload_as().map(ClientPayload::Hello),
            ClientKind::Request => self.payload_as().map(ClientPayload::Request),
            ClientKind::CancelRequest => self.payload_as().map(ClientPayload::CancelRequest),
            ClientKind::Subscribe => self.payload_as().map(ClientPayload::Subscribe),
            ClientKind::Unsubscribe => self.payload_as().map(ClientPayload::Unsubscribe),
            ClientKind::Ack => self.payload_as().map(ClientPayload::Ack),
            ClientKind::Ping => self.payload_as().map(ClientPayload::Ping),
        }
    }

    /// Return the request ID, when present.
    #[must_use]
    pub fn request_id(&self) -> Option<&RequestId> {
        self.request_id.as_ref()
    }

    /// Return the subscription ID, when present.
    #[must_use]
    pub fn subscription_id(&self) -> Option<&SubscriptionId> {
        self.subscription_id.as_ref()
    }

    /// Return the connection sequence, when present.
    #[must_use]
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    fn payload_as<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        decode_payload(&self.payload, format_args!("{:?}", self.kind))
    }
}

/// Stable server-to-client wire envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerFrame {
    /// Message discriminator.
    pub kind: ServerKind,
    /// Request identifier, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    /// Subscription identifier, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<SubscriptionId>,
    /// Monotonic connection sequence, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Kind-specific payload.
    #[serde(default)]
    pub payload: Value,
}

impl ServerFrame {
    /// Create a response frame whose payload is serialized as JSON.
    #[must_use]
    pub fn response(request_id: RequestId, payload: impl Serialize) -> Self {
        Self {
            kind: ServerKind::Response,
            request_id: Some(request_id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(payload).expect("response payload must serialize"),
        }
    }

    /// Decode the kind-specific payload.
    pub fn decode_payload(&self) -> Result<ServerPayload, ProtocolError> {
        match self.kind {
            ServerKind::Welcome => self.payload_as().map(ServerPayload::Welcome),
            ServerKind::Response => self.payload_as().map(ServerPayload::Response),
            ServerKind::Subscribed => self.payload_as().map(ServerPayload::Subscribed),
            ServerKind::Complete => self.payload_as().map(ServerPayload::Complete),
            ServerKind::Event => self.payload_as().map(ServerPayload::Event),
            ServerKind::SnapshotBegin => self.payload_as().map(ServerPayload::SnapshotBegin),
            ServerKind::SnapshotChunk => self.payload_as().map(ServerPayload::SnapshotChunk),
            ServerKind::SnapshotEnd => self.payload_as().map(ServerPayload::SnapshotEnd),
            ServerKind::OperationUpdate => self.payload_as().map(ServerPayload::OperationUpdate),
            ServerKind::ResyncRequired => self.payload_as().map(ServerPayload::ResyncRequired),
            ServerKind::Error => self.payload_as().map(ServerPayload::Error),
            ServerKind::Pong => self.payload_as().map(ServerPayload::Pong),
            ServerKind::ShutdownNotice => self.payload_as().map(ServerPayload::ShutdownNotice),
        }
    }

    /// Decode a response payload into its request-specific type.
    pub fn decode_response_payload<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        if self.kind != ServerKind::Response {
            return Err(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "frame is not a response",
            ));
        }
        self.payload_as()
    }

    /// Return the request ID, when present.
    #[must_use]
    pub fn request_id(&self) -> Option<&RequestId> {
        self.request_id.as_ref()
    }

    /// Return the subscription ID, when present.
    #[must_use]
    pub fn subscription_id(&self) -> Option<&SubscriptionId> {
        self.subscription_id.as_ref()
    }

    /// Return the connection sequence, when present.
    #[must_use]
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    fn payload_as<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        decode_payload(&self.payload, format_args!("{:?}", self.kind))
    }
}

/// A typed client payload after envelope dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientPayload {
    /// Authentication and negotiation payload.
    Hello(Hello),
    /// Request payload.
    Request(Request),
    /// Cancellation payload.
    CancelRequest(CancelRequest),
    /// Subscription payload.
    Subscribe(Subscribe),
    /// Unsubscribe payload.
    Unsubscribe(Unsubscribe),
    /// Resume-cursor acknowledgement.
    Ack(Ack),
    /// Keepalive ping.
    Ping(Ping),
}

/// A typed server payload after envelope dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerPayload {
    /// Negotiated session settings.
    Welcome(Welcome),
    /// Request-specific response data.
    Response(Response),
    /// Subscription confirmation.
    Subscribed(Subscribed),
    /// Subscription completion.
    Complete(Complete),
    /// Stream event.
    Event(Event),
    /// Snapshot start.
    SnapshotBegin(SnapshotBegin),
    /// Snapshot chunk.
    SnapshotChunk(SnapshotChunk),
    /// Snapshot end.
    SnapshotEnd(SnapshotEnd),
    /// Operation status.
    OperationUpdate(OperationUpdate),
    /// Required resync.
    ResyncRequired(ResyncRequired),
    /// Structured error.
    Error(ErrorFrame),
    /// Keepalive pong.
    Pong(Pong),
    /// Shutdown notice.
    ShutdownNotice(ShutdownNotice),
}

/// A protocol decoding error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    /// Stable error code.
    pub code: ErrorCode,
    /// Safe error message.
    pub message: String,
}

impl ProtocolError {
    /// Create a protocol error.
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Decode a client envelope from raw JSON.
pub fn decode_client_frame(raw: &str) -> Result<ClientFrame, ProtocolError> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error.to_string()))?;
    let kind = kind_name(&value)?;
    if !matches!(
        kind,
        "hello" | "request" | "cancelRequest" | "subscribe" | "unsubscribe" | "ack" | "ping"
    ) {
        return Err(unsupported_kind(kind));
    }
    let frame: ClientFrame = serde_json::from_value(value).map_err(invalid_envelope)?;
    validate_client_metadata(&frame)?;
    Ok(frame)
}

/// Decode a server envelope from a JSON value.
pub fn decode_server_frame(value: Value) -> Result<ServerFrame, ProtocolError> {
    let kind = kind_name(&value)?;
    if !matches!(
        kind,
        "welcome"
            | "response"
            | "subscribed"
            | "complete"
            | "event"
            | "snapshotBegin"
            | "snapshotChunk"
            | "snapshotEnd"
            | "operationUpdate"
            | "resyncRequired"
            | "error"
            | "pong"
            | "shutdownNotice"
    ) {
        return Err(unsupported_kind(kind));
    }
    let frame: ServerFrame = serde_json::from_value(value).map_err(invalid_envelope)?;
    validate_server_metadata(&frame)?;
    Ok(frame)
}

fn validate_client_metadata(frame: &ClientFrame) -> Result<(), ProtocolError> {
    match frame.kind {
        ClientKind::Request | ClientKind::CancelRequest => {
            require_metadata(frame.request_id.is_some(), frame.kind, "requestId")
        }
        ClientKind::Subscribe | ClientKind::Unsubscribe => require_metadata(
            frame.subscription_id.is_some(),
            frame.kind,
            "subscriptionId",
        ),
        ClientKind::Hello | ClientKind::Ack | ClientKind::Ping => Ok(()),
    }
}

fn validate_server_metadata(frame: &ServerFrame) -> Result<(), ProtocolError> {
    match frame.kind {
        ServerKind::Response => {
            require_metadata(frame.request_id.is_some(), frame.kind, "requestId")
        }
        ServerKind::Subscribed
        | ServerKind::Complete
        | ServerKind::Event
        | ServerKind::SnapshotBegin
        | ServerKind::SnapshotChunk
        | ServerKind::SnapshotEnd => {
            require_metadata(
                frame.subscription_id.is_some(),
                frame.kind,
                "subscriptionId",
            )?;
            require_metadata(frame.sequence.is_some(), frame.kind, "sequence")
        }
        ServerKind::OperationUpdate | ServerKind::ResyncRequired => {
            require_metadata(frame.sequence.is_some(), frame.kind, "sequence")
        }
        ServerKind::Welcome | ServerKind::Error | ServerKind::Pong | ServerKind::ShutdownNotice => {
            Ok(())
        }
    }
}

fn require_metadata(
    present: bool,
    kind: impl std::fmt::Debug,
    field: &str,
) -> Result<(), ProtocolError> {
    if present {
        Ok(())
    } else {
        Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("{kind:?} frame requires {field}"),
        ))
    }
}

fn kind_name(value: &Value) -> Result<&str, ProtocolError> {
    value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| ProtocolError::new(ErrorCode::InvalidRequest, "missing kind"))
}

fn unsupported_kind(kind: &str) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedMessage,
        format!("unsupported message kind: {kind}"),
    )
}

fn invalid_envelope(error: serde_json::Error) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::InvalidRequest,
        format!("invalid frame envelope: {error}"),
    )
}

fn decode_payload<T: DeserializeOwned>(
    payload: &Value,
    kind: impl std::fmt::Display,
) -> Result<T, ProtocolError> {
    serde_json::from_value(payload.clone()).map_err(|error| {
        ProtocolError::new(
            ErrorCode::InvalidRequest,
            format!("invalid {kind} payload: {error}"),
        )
    })
}

/// Create an error payload for an unsupported message.
#[must_use]
pub fn unsupported_message_error(kind: &str, correlation_id: CorrelationId) -> ErrorFrame {
    ErrorFrame::new(
        ErrorCode::UnsupportedMessage,
        format!("unsupported message kind: {kind}"),
        Retryability::Never,
        ErrorScope::Request,
        correlation_id.as_str().to_owned(),
    )
}

/// Validate a bootstrap response payload.
pub fn validate_bootstrap_response(payload: &Value) -> Result<BootstrapResponse, ProtocolError> {
    decode_payload(payload, "bootstrap response")
}

/// Initial authentication and capability negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hello {
    /// Client protocol major version.
    pub protocol_major: u16,
    /// Client protocol minor version.
    pub protocol_minor: u16,
    /// Client capability names.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Access token, sent only in the first frame.
    pub access_token: String,
    /// Previous server instance for resumption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_instance_id: Option<String>,
    /// Previous session for resumption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    /// Last acknowledged sequence for resumption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_acked_sequence: Option<u64>,
    /// Single-use ticket for a logs or exec stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_ticket: Option<String>,
}

/// Negotiated session settings returned after `Hello`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Welcome {
    /// Negotiated protocol version.
    pub protocol: ProtocolVersion,
    /// Negotiated capability intersection.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// New or resumed session ID.
    pub session_id: SessionId,
    /// Current server instance ID.
    pub server_instance_id: String,
    /// Whether the session was resumed.
    pub resume_status: ResumeStatus,
}

/// Session resumption result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResumeStatus {
    /// A new session was created.
    Fresh,
    /// The prior session was resumed.
    Resumed,
    /// The client must rebuild server-issued state.
    ResyncRequired,
}

/// A typed control request payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// Request operation name.
    #[serde(rename = "kind")]
    pub request_kind: String,
    /// Relative deadline in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<u64>,
    /// Idempotency key required by mutation operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Operation-specific request data.
    #[serde(default)]
    pub payload: Value,
}

/// Request-specific response data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Response(pub Value);

/// Idempotent cancellation of the envelope's request ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest;

/// Opaque Plan 1 subscription selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Subscribe(pub Value);

/// End the envelope's subscription ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unsubscribe;

/// Advance the session resume cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    /// Highest contiguous connection sequence processed by the client.
    pub last_acked_sequence: u64,
}

/// Keepalive ping payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ping;

/// Subscription confirmation payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribed;

/// Subscription completion payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Complete;

/// A server event payload. Sequence and subscription ID stay in the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// Event operation name.
    #[serde(rename = "kind")]
    pub event_kind: String,
    /// Backend resource revision, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Event-specific data.
    #[serde(default)]
    pub payload: Value,
}

/// Beginning of a chunked snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotBegin {
    /// Total chunk count.
    pub total_chunks: u32,
}

/// One chunk of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotChunk {
    /// Zero-based chunk index.
    pub chunk_index: u32,
    /// Normalized snapshot data.
    pub data: Value,
}

/// End of a chunked snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEnd {
    /// Checksum of the complete snapshot.
    pub checksum: String,
}

/// Background operation status update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationUpdate {
    /// Operation identifier.
    pub operation_id: OperationId,
    /// Current status.
    pub status: OperationStatus,
    /// Optional progress data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<Value>,
}

/// Background operation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationStatus {
    /// Waiting to run.
    Pending,
    /// Currently running.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Completed with an error.
    Failed,
    /// Cancelled by the client.
    Cancelled,
    /// The mutation may have reached Kubernetes, but no authoritative
    /// response was observed. Callers must refresh before deciding to retry.
    OutcomeUnknown,
    /// The backend no longer knows this operation: its ID expired out of
    /// the bounded store or was never seen by this server instance. Only
    /// clients derive this state; servers never send it.
    Unknown,
}

/// Required full-resync reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResyncRequired {
    /// Safe reason for requiring a resync.
    pub reason: String,
}

/// Keepalive pong payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pong;

/// Server shutdown notice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownNotice {
    /// Safe shutdown reason.
    pub reason: String,
    /// Suggested retry delay in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<u64>,
}
