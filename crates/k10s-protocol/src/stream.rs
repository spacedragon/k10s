//! Dedicated logs/exec stream socket contract.
//!
//! Stream sockets are separate WebSocket upgrades on [`crate::route::
//! LOGS_PATH`] and [`crate::route::EXEC_PATH`]. The mandatory first frame is
//! a JSON `hello` carrying the shared access token and a single-use stream
//! ticket; the ticket is redeemed only after the token authenticates. All
//! handshake and status frames are JSON text frames tagged by `kind`; all
//! log/exec payloads are binary frames with a versioned one-byte-version +
//! one-byte-kind header so fragmentation limits can be enforced before any
//! payload dispatch.

use serde::{Deserialize, Serialize};

use crate::error::ErrorCode;

/// Control-socket request kind that issues a single-use stream ticket.
pub const REQUEST_STREAM_TICKET: &str = "stream.ticket";

/// Version of the binary payload header. Bumped on incompatible changes.
pub const STREAM_PAYLOAD_VERSION: u8 = 1;

/// Payload kinds carried in the binary frame header byte 1.
pub mod payload_kind {
    /// Non-TTY exec standard output.
    pub const STDOUT: u8 = 1;
    /// Non-TTY exec standard error (a distinct mode from TTY output).
    pub const STDERR: u8 = 2;
    /// TTY merged output: stdin echo, program output, everything in one.
    pub const TTY_OUTPUT: u8 = 3;
    /// Client-to-server TTY standard input.
    pub const STDIN: u8 = 4;
    /// Terminal resize; data is `cols` then `rows` as big-endian `u32`s.
    pub const RESIZE: u8 = 5;
}

/// Which stream a ticket opens. Serialized as `"logs"` / `"exec"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamType {
    /// Tail container logs.
    Logs,
    /// Attach to an exec session.
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

/// Control-socket request payload issuing a single-use stream ticket.
///
/// Issuance is a query: it validates existence, RBAC, and (for exec) binary
/// availability before any socket exists. The returned ticket binds exactly
/// this target, stream type, and mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTicketRequest {
    /// Pod/container to attach to.
    pub target: StreamTarget,
    /// Whether this opens logs or an exec session.
    pub stream_type: StreamType,
    /// Exec mode: `true` requests an explicit interactive shell (TTY with
    /// merged output), `false` the retained non-TTY mode with separated
    /// stdout/stderr. Ignored for logs.
    pub tty: bool,
    /// Exact remote command and arguments for exec. Older clients omitted
    /// this field, so an empty value is normalized to `/bin/sh` by the
    /// server. Ignored for logs.
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
    /// Bound exec mode echoed back.
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
    /// single-use ticket to redeem. Sent before anything else.
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
    /// Ticket redeemed successfully; echoes the bound stream identity.
    Ready {
        /// Bound stream type.
        stream_type: StreamType,
        /// Bound exec mode.
        tty: bool,
        /// Selected container.
        container: String,
    },
    /// Informational status, such as a resize acknowledgement.
    Status {
        /// Safe human-readable status message.
        message: String,
    },
    /// Typed failure; the server closes after sending this.
    Error {
        /// Stable error code.
        code: ErrorCode,
        /// Safe human-readable reason.
        message: String,
    },
    /// The exec session ended with the given exit code.
    Exit {
        /// Process exit code.
        exit_code: i32,
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
/// are rejected before the data is interpreted anywhere.
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

/// Decode resize payload bytes (`cols`, `rows` big-endian `u32` pair).
pub fn decode_resize_payload(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() != 8 {
        return None;
    }
    let cols = u32::from_be_bytes(data[0..4].try_into().ok()?);
    let rows = u32::from_be_bytes(data[4..8].try_into().ok()?);
    Some((cols, rows))
}

/// Encode resize payload bytes (`cols`, `rows` big-endian `u32` pair).
#[must_use]
pub fn encode_resize_payload(cols: u32, rows: u32) -> Vec<u8> {
    cols.to_be_bytes()
        .into_iter()
        .chain(rows.to_be_bytes())
        .collect()
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
    fn exec_ticket_requests_preserve_the_exact_remote_command() {
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

    #[test]
    fn resize_payload_round_trips() {
        let encoded = encode_resize_payload(120, 40);
        assert_eq!(decode_resize_payload(&encoded), Some((120, 40)));
        assert_eq!(decode_resize_payload(&encoded[..4]), None);
    }
}
