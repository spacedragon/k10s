//! Target-neutral wire and route contract for the k10s control protocol.
//!
//! This crate must stay free of platform-specific dependencies such as
//! kube-rs or Tokio; it is shared by the native and WASM clients as well as
//! the server.
//!
//! # Versioning
//!
//! The protocol is versioned with `(major, minor)` pairs. A client and server
//! are compatible when they agree on the major version; the minor version is
//! negotiated down to the lower of the two values. Unknown message kinds are
//! reported as [`ErrorCode::UnsupportedMessage`] errors rather than panics.

pub mod bootstrap;
pub mod envelope;
pub mod error;
pub mod ids;
pub mod route;

pub use bootstrap::{BootstrapResponse, ProtocolVersion, ServerInfo};
pub use envelope::{
    ClientFrame, ClientKind, ProtocolError, ServerFrame, ServerKind, decode_client_frame,
    decode_server_frame, unsupported_message_error, validate_bootstrap_response,
};
pub use error::{ErrorCode, ErrorFrame, ErrorScope, Retryability};
pub use ids::{CorrelationId, OperationId, RequestId, SessionId, SubscriptionId};
pub use route::{CONTROL_PATH, EXEC_PATH, LOGS_PATH};

/// Major protocol version.
pub const PROTOCOL_MAJOR: u16 = 1;
/// Minor protocol version.
pub const PROTOCOL_MINOR: u16 = 1;
