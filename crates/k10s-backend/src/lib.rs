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
pub mod stream;
mod validation;
pub mod watch;

mod kube;
pub mod runtime;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use catalog::CatalogSnapshot;
pub use fake::{FakeKubernetes, FakeMetricsScenario};
pub use kernel::{BackendKernel, BootstrapResult, InfrastructureResult, KernelQueryResult};
pub use kube::{DISCOVERY_TTL, KubeAdapter, MAX_CACHED_CONTEXTS};
pub use operation::{
    AcceptOutcome, OperationEngine, OperationEvent, OperationState, OperationStatusData,
    Propagation, YamlValidationData,
};
pub use port::{
    AdapterError, ApiResourceDescriptor, BackendError, BackendEvent, Command, ContextInfo,
    ContextPermissionsData, ContextSwitchData, Gvk, KubernetesAccess, MetricsSample, OperationId,
    OwnerRef, PermissionCheck, PermissionOutcome, PermissionProbe, Query, QueryResult, RecordEvent,
    RelatedData, RelatedRecordGroup, ResourceListData, ResourceProjection, ResourceRecord,
    ResourceRef, ResourceTypesData, ServicePort, ServiceProjection, StreamGrant, StreamInput,
    StreamKind, StreamRouteKind, Subscribe, SubscriptionHandle, TargetPort, TransportProtocol,
};
pub use runtime::{BackendMode, ContextRegistry, build_kernel};
pub use stream::{StreamChunk, StreamHub, StreamOrigin, StreamTicketResult};
