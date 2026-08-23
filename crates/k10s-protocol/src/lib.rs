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
pub mod metrics;
pub mod operation;
pub mod resource;
pub mod route;
pub mod subscription;

pub use bootstrap::{BootstrapResponse, Context, ProtocolVersion, ServerInfo};
pub use envelope::{
    Ack, CancelRequest, ClientFrame, ClientKind, ClientPayload, Complete, Event, Hello,
    OperationStatus, OperationUpdate, Ping, Pong, ProtocolError, Request, Response, ResumeStatus,
    ResyncRequired, ServerFrame, ServerKind, ServerPayload, ShutdownNotice, SnapshotBegin,
    SnapshotChunk, SnapshotEnd, Subscribe, Subscribed, Unsubscribe, Welcome, decode_client_frame,
    decode_server_frame, unsupported_message_error, validate_bootstrap_response,
};
pub use error::{ErrorCode, ErrorFrame, ErrorScope, Retryability};
pub use ids::{CorrelationId, OperationId, RequestId, SessionId, SubscriptionId};
pub use metrics::{
    CapacityUsage, ClusterTotals, INFRASTRUCTURE_EVENT_UPDATED, InfrastructureRequest,
    InfrastructureResponse, InfrastructureWatchSpec, MetricsAvailability, MetricsCondition,
    MetricsStatus, PodMetrics, ResourceMetricsResponse,
};
pub use operation::{
    OperationAccepted, REQUEST_YAML_APPLY, REQUEST_YAML_VALIDATE, ValidationTicket,
    YamlApplyRequest, YamlDiagnostic, YamlOutcome, YamlValidateRequest, buffer_hash,
};
pub use resource::{
    AttentionRow, BackendRevision, DetailRow, DetailSection, EventRow, GroupVersionKind,
    HealthLevel, NodeRow, OwnerReference, PersistentVolumeClaimRow, PersistentVolumeRow,
    RelatedGroup, ResourceCapabilities, ResourceDetailResponse, ResourceIdentity,
    ResourceListRequest, ResourceListResponse, ResourceListRow, ResourceRefRequest, ResourceScope,
    StorageClassRow, StorageInventory, WorkloadHealth, WorkloadKind,
};
pub use route::{CONTROL_PATH, EXEC_PATH, LOGS_PATH};
pub use subscription::{
    RESOURCE_EVENT_CHANGED, RESOURCE_EVENT_GONE, ResourceChanged, ResourceGone,
    ResourceSnapshotPage, ResourceTypeEntry, ResourceTypesRequest, ResourceTypesResponse,
    ResourceWatchSpec, SubscriptionSelector,
};

/// Major protocol version.
pub const PROTOCOL_MAJOR: u16 = 1;
/// Minor protocol version.
pub const PROTOCOL_MINOR: u16 = 1;
