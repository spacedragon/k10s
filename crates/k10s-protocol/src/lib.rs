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
pub mod context;
pub mod envelope;
pub mod error;
pub mod ids;
pub mod metrics;
pub mod operation;
pub mod port_forward;
pub mod resource;
pub mod route;
pub mod stream;
pub mod subscription;
pub mod traffic;

pub use bootstrap::{BootstrapResponse, Context, ContextAvailability, ProtocolVersion, ServerInfo};
pub use context::{
    ContextPermissionsRequest, ContextPermissionsResponse, ContextSwitchRequest,
    ContextSwitchResponse, PermissionCheck, PermissionOutcome, PermissionProbe,
    REQUEST_CONTEXT_PERMISSIONS, REQUEST_CONTEXT_SWITCH,
};
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
    CapacityUsage, ClusterTotals, ContainerMetrics, INFRASTRUCTURE_EVENT_UPDATED,
    InfrastructureRequest, InfrastructureResponse, InfrastructureWatchSpec, LauncherCounts,
    MetricsAvailability, MetricsCondition, MetricsStatus, PodMetrics, ResourceMetricsResponse,
};
pub use operation::{
    CreateJobRequest, CronJobSuspendRequest, DeletePreflightRequest, DeletePreflightResponse,
    DeletePropagation, DeleteRequest, OperationAccepted, OperationProgress, OperationSnapshotEntry,
    OperationStatusRequest, OperationStatusResponse, REQUEST_CRONJOB_SUSPEND,
    REQUEST_DELETE_PREFLIGHT, REQUEST_JOB_CREATE, REQUEST_OPERATION_STATUS,
    REQUEST_WORKLOAD_DELETE, REQUEST_WORKLOAD_RESTART, REQUEST_WORKLOAD_SCALE, REQUEST_YAML_APPLY,
    REQUEST_YAML_VALIDATE, RestartRequest, ScaleRequest, ValidationTicket, YamlApplyRequest,
    YamlDiagnostic, YamlOutcome, YamlValidateRequest, buffer_hash,
};
pub use port_forward::{
    CAPABILITY_POD_PORT_FORWARD, CAPABILITY_SERVICE_PORT_FORWARD, PORT_FORWARD_EVENT_SESSION,
    PortForwardFailure, PortForwardFailureCategory, PortForwardListRequest,
    PortForwardListResponse, PortForwardPodTarget, PortForwardPortSelector, PortForwardSession,
    PortForwardSessionEvent, PortForwardSessionId, PortForwardSessionState,
    PortForwardStartRequest, PortForwardStartResponse, PortForwardStopRequest,
    PortForwardStopResponse, PortForwardTarget, REQUEST_PORT_FORWARD_LIST,
    REQUEST_PORT_FORWARD_START, REQUEST_PORT_FORWARD_STOP,
};
pub use resource::{
    AttentionRow, BackendRevision, ContainerImageProjection, ContainerStateProjection,
    ContainerTerminationProjection, DeploymentProjection, DetailRow, DetailSection, EventRow,
    EventsCondition, GroupVersionKind, HealthLevel, NodeRow, OwnerReference,
    PersistentVolumeClaimRow, PersistentVolumeRow, PodContainerPort, PodContainerProjection,
    PodProjection, REQUEST_RESOURCE_RELATIONS, RelatedGroup, ReplicaSetProjection,
    ResourceCapabilities, ResourceConditionProjection, ResourceDetailResponse, ResourceIdentity,
    ResourceListRequest, ResourceListResponse, ResourceListRow, ResourceProjection,
    ResourceRefRequest, ResourceRelationsResponse, ResourceScope, ServicePort, ServiceProjection,
    StorageClassRow, StorageInventory, TargetPort, TransportProtocol, WorkloadHealth, WorkloadKind,
};
pub use route::{CONTROL_PATH, EXEC_PATH, LOGS_PATH};
pub use stream::{
    DecodedStreamPayload, REQUEST_STREAM_TICKET, STREAM_PAYLOAD_VERSION, StreamClientMessage,
    StreamPayloadError, StreamServerMessage, StreamTarget, StreamTicketRequest,
    StreamTicketResponse, StreamType, decode_stream_payload, encode_stream_payload, payload_kind,
};
pub use subscription::{
    RESOURCE_EVENT_CHANGED, RESOURCE_EVENT_GONE, ResourceChanged, ResourceGone,
    ResourceSnapshotPage, ResourceTypeEntry, ResourceTypesRequest, ResourceTypesResponse,
    ResourceWatchIdentity, ResourceWatchSpec, SubscriptionSelector,
};
pub use traffic::{TRAFFIC_EVENT_UPDATED, TrafficSample, TrafficWatchSpec};

/// Major protocol version.
pub const PROTOCOL_MAJOR: u16 = 1;
/// Minor protocol version.
///
/// v1.2 added the Service resource projection and port-forward session
/// lifecycle payloads. v1.3 added Pod, Deployment, and ReplicaSet
/// projections, per-container metrics, and restart capability metadata.
/// v1.4 added exact-identity resource watches for dedicated Detail authority.
/// v1.5 added context-scoped Kubernetes API transport traffic telemetry.
/// v1.6 retired active embedded exec while retaining a one-minor legacy
/// decode tombstone for major-version-1 clients, and added generalized
/// Service and Pod port-forward targets.
pub const PROTOCOL_MINOR: u16 = 6;
