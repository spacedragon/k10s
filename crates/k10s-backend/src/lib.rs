//! Backend kernel and behavior-level Kubernetes port for k10s.
//!
//! This crate owns all Kubernetes-facing product behavior. The kernel is the
//! sole protocol-facing interface and maps to normalized protocol payloads.
//! Fake data never escapes as fixture types.

pub mod catalog;
pub mod fake;
pub mod kernel;
pub mod operation;
pub mod port;
pub mod port_forward;
pub mod stream;
mod validation;
pub mod watch;

mod kube;
pub mod runtime;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use catalog::CatalogSnapshot;
pub use fake::{FakeKubernetes, FakeMetricsScenario, FakePortForwardSeam};
pub use kernel::{
    BackendKernel, BootstrapResult, InfrastructureResult, KernelQueryResult,
    ResourceRelationsResult,
};
pub use kube::{DISCOVERY_TTL, KubeAdapter, MAX_CACHED_CONTEXTS};
pub use operation::{
    AcceptOutcome, OperationEngine, OperationEvent, OperationState, OperationStatusData,
    Propagation, YamlValidationData,
};
pub use port::{
    AdapterError, ApiResourceDescriptor, BackendError, BackendEvent, Command, ContextAvailability,
    ContextInfo, ContextPermissionsData, ContextSwitchData, Gvk, KubernetesAccess, MetricsSample,
    OperationId, OwnerRef, PermissionCheck, PermissionOutcome, PermissionProbe, PodContainerPort,
    Query, QueryResult, RecordEvent, RecordEventsCondition, RelatedData, RelatedRecordGroup,
    ResourceListData, ResourceProjection, ResourceRecord, ResourceRef, ResourceTypesData,
    ResourceWatchIdentity, ServicePort, ServiceProjection, StreamGrant, StreamKind,
    StreamRouteKind, Subscribe, SubscriptionHandle, TargetPort, TransportProtocol,
};
pub use port_forward::{
    PortForwardConnector, PortForwardPortSelector, PortForwardRequest, PortForwardSeam,
    PortForwardStream, PortForwardTarget, RejectionCategory, ResolvedPortForward,
};
pub use runtime::{
    BackendMode, ContextRegistry, ExecPluginPreparation, KubePreparation, PreparedBackend,
    build_kernel, prepare_backend, prepare_kube_backend_from_paths,
};
pub use stream::{StreamChunk, StreamHub, StreamOrigin, StreamTicketResult};
