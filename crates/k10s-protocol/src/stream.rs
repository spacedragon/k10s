//! Active log-stream contract plus reserved major-1 exec compatibility shapes.
//!
//! Active log sockets upgrade on [`crate::route::LOGS_PATH`]. Their mandatory
//! first frame is a JSON `hello` carrying the shared access token and a
//! single-use log ticket, which is redeemed only after authentication.
//! [`crate::route::EXEC_PATH`], [`StreamType::Exec`], the exec-only ticket
//! fields, and exec payload-kind numbers remain decodable for one compatibility
//! window only. The authenticated exec route is a fail-closed tombstone: it
//! never issues or redeems a ticket and never dispatches a backend operation.

use serde::{Deserialize, Serialize};

use crate::error::ErrorCode;

/// Control-socket request kind that issues a single-use stream ticket.
pub const REQUEST_STREAM_TICKET: &str = "stream.ticket";

/// Version of the binary payload header. Bumped on incompatible changes.
pub const STREAM_PAYLOAD_VERSION: u8 = 1;

/// Payload kinds carried in the binary frame header byte 1.
pub mod payload_kind {
    /// Active log data. Historically also non-TTY exec stdout.
    pub const STDOUT: u8 = 1;
    /// Reserved legacy non-TTY exec stderr discriminant; never emitted.
    pub const STDERR: u8 = 2;
    /// Reserved legacy TTY-output discriminant; never emitted.
    pub const TTY_OUTPUT: u8 = 3;
    /// Reserved legacy stdin discriminant; never consumed.
    pub const STDIN: u8 = 4;
    /// Reserved legacy terminal-resize discriminant; never consumed.
    pub const RESIZE: u8 = 5;
}

/// Active log stream type plus the decodable legacy exec discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamType {
    /// Tail container logs.
    Logs,
    /// Reserved legacy value rejected by the control tombstone.
    Exec,
}

/// The pod/container a stream attaches to. Carries no credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTarget {
    /// Kubernetes context.
    pub context: String,
    /// Namespace of the pod.
    pub namespace: String,
    /// Pod name.
    pub pod: String,
    /// Immutable UID of the selected Pod. Older clients may omit it, in
    /// which case the adapter binds the UID it observes at issuance.
    #[serde(default)]
    pub uid: String,
    /// Container within the pod.
    pub container: String,
}

/// Control-socket request payload issuing a single-use log ticket.
///
/// Log issuance validates and binds the target and history options. Requests
/// carrying the legacy exec stream type are decoded only so the server can
/// return a typed unsupported-message error before backend dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTicketRequest {
    /// Pod/container to attach to.
    pub target: StreamTarget,
    /// `Logs` is active; `Exec` selects the compatibility tombstone.
    pub stream_type: StreamType,
    /// Reserved legacy exec mode field. Active log requests set this false.
    pub tty: bool,
    /// Reserved legacy remote-command shape. It is ignored for logs and never
    /// executed when a tombstoned exec request is decoded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// Maximum historical lines requested for a logs stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_lines: Option<i64>,
    /// Relative history window for logs, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_seconds: Option<i64>,
    /// Read the previously terminated instance of the selected container.
    #[serde(default)]
    pub previous: bool,
    /// Ask Kubernetes to prefix log lines with source timestamps.
    #[serde(default)]
    pub timestamps: bool,
    /// Continue following new log output after the historical tail.
    #[serde(default)]
    pub follow: bool,
}

/// Response payload granting a single-use stream ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTicketResponse {
    /// Opaque single-use ticket ID. Redeemed in the first `hello` of a
    /// dedicated stream socket — never placed in any URL.
    pub ticket_id: String,
    /// Bound target.
    pub target: StreamTarget,
    /// Bound stream type.
    pub stream_type: StreamType,
    /// Reserved wire field; active log grants always set this false.
    pub tty: bool,
}

/// Client-to-server JSON text frames on a stream socket.
///
/// The first frame MUST be [`StreamClientMessage::Hello`]; every later
/// client message must be a versioned binary frame instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StreamClientMessage {
    /// Mandatory first frame: authenticates the token and carries the
    /// single-use log ticket. On the exec tombstone the ticket is deliberately
    /// ignored after authentication. Sent before anything else.
    Hello {
        /// Client protocol major version.
        protocol_major: u16,
        /// Shared access token; compared constant-time, never logged.
        access_token: String,
        /// Single-use stream ticket issued over the control socket.
        stream_ticket: String,
    },
}

/// Server-to-client JSON text frames on a stream socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum StreamServerMessage {
    /// Log ticket redeemed successfully; echoes the bound stream identity.
    Ready {
        /// Bound stream type.
        stream_type: StreamType,
        /// Reserved wire field; always false for active log streams.
        tty: bool,
        /// Selected container.
        container: String,
    },
    /// Typed failure; the server closes after sending this.
    Error {
        /// Stable error code.
        code: ErrorCode,
        /// Safe human-readable reason.
        message: String,
    },
}

/// Why a binary frame failed header validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamPayloadError {
    /// Frame shorter than the fixed two-byte header.
    TooShort,
    /// Unknown header version.
    UnknownVersion(u8),
    /// Unknown payload kind for the current version.
    UnknownKind(u8),
}

impl std::fmt::Display for StreamPayloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => formatter.write_str("frame shorter than the payload header"),
            Self::UnknownVersion(version) => {
                write!(formatter, "unknown payload version {version}")
            }
            Self::UnknownKind(kind) => write!(formatter, "unknown payload kind {kind}"),
        }
    }
}

impl std::error::Error for StreamPayloadError {}

/// One decoded binary stream payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedStreamPayload<'a> {
    /// Payload kind byte.
    pub kind: u8,
    /// Payload bytes after the header.
    pub data: &'a [u8],
}

/// Encode `data` behind the versioned two-byte payload header.
#[must_use]
pub fn encode_stream_payload(kind: u8, data: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(2 + data.len());
    frame.push(STREAM_PAYLOAD_VERSION);
    frame.push(kind);
    frame.extend_from_slice(data);
    frame
}

/// Decode and validate the versioned header of a binary frame. The whole
/// assembled message must be at least the header; unknown versions or kinds
/// are rejected. Legacy exec kinds remain recognized solely to keep their
/// numeric values reserved and are not consumed by active production paths.
pub fn decode_stream_payload(frame: &[u8]) -> Result<DecodedStreamPayload<'_>, StreamPayloadError> {
    let (&version, rest) = frame.split_first().ok_or(StreamPayloadError::TooShort)?;
    let (&kind, data) = rest.split_first().ok_or(StreamPayloadError::TooShort)?;
    if version != STREAM_PAYLOAD_VERSION {
        return Err(StreamPayloadError::UnknownVersion(version));
    }
    if !matches!(
        kind,
        payload_kind::STDOUT
            | payload_kind::STDERR
            | payload_kind::TTY_OUTPUT
            | payload_kind::STDIN
            | payload_kind::RESIZE
    ) {
        return Err(StreamPayloadError::UnknownKind(kind));
    }
    Ok(DecodedStreamPayload { kind, data })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_headers_round_trip_and_reject_unknown_versions() {
        let frame = encode_stream_payload(payload_kind::STDIN, b"echo hi\n");
        assert_eq!(frame[0], STREAM_PAYLOAD_VERSION);
        let decoded = decode_stream_payload(&frame).unwrap();
        assert_eq!(decoded.kind, payload_kind::STDIN);
        assert_eq!(decoded.data, b"echo hi\n");

        assert_eq!(
            decode_stream_payload(b""),
            Err(StreamPayloadError::TooShort)
        );
        assert_eq!(
            decode_stream_payload(&[9]),
            Err(StreamPayloadError::TooShort)
        );
        assert_eq!(
            decode_stream_payload(&[9, payload_kind::STDIN]),
            Err(StreamPayloadError::UnknownVersion(9))
        );
        assert_eq!(
            decode_stream_payload(&[STREAM_PAYLOAD_VERSION, 99]),
            Err(StreamPayloadError::UnknownKind(99))
        );
    }

    #[test]
    fn hello_messages_round_trip_with_camel_case_kinds() {
        let raw = r#"{"kind":"hello","protocolMajor":1,"accessToken":"secret","streamTicket":"t"}"#;
        let decoded: StreamClientMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(
            decoded,
            StreamClientMessage::Hello {
                protocol_major: 1,
                access_token: "secret".into(),
                stream_ticket: "t".into(),
            }
        );
    }

    #[test]
    fn log_ticket_requests_preserve_exact_uid_and_history_options() {
        let request = StreamTicketRequest {
            target: StreamTarget {
                context: "dev".into(),
                namespace: "default".into(),
                pod: "web".into(),
                uid: "uid-web".into(),
                container: "app".into(),
            },
            stream_type: StreamType::Logs,
            tty: false,
            command: Vec::new(),
            tail_lines: Some(200),
            since_seconds: Some(60),
            previous: false,
            timestamps: true,
            follow: true,
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["target"]["uid"], "uid-web");
        assert_eq!(value["tailLines"], 200);
        assert_eq!(value["sinceSeconds"], 60);
        assert_eq!(
            serde_json::from_value::<StreamTicketRequest>(value).unwrap(),
            request
        );
    }

    #[test]
    fn legacy_exec_ticket_requests_remain_decodable_for_the_tombstone() {
        let value = serde_json::json!({
            "target": {
                "context": "dev", "namespace": "default", "pod": "web",
                "uid": "uid-web", "container": "app"
            },
            "streamType": "exec",
            "tty": false,
            "command": ["/bin/sh", "-c", "printf exact"]
        });
        let request: StreamTicketRequest = serde_json::from_value(value).unwrap();
        assert_eq!(request.command, ["/bin/sh", "-c", "printf exact"]);
    }
}
