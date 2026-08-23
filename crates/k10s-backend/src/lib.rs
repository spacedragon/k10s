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
pub mod watch;

mod kube;
pub mod runtime;

pub use catalog::CatalogSnapshot;
pub use fake::{FakeKubernetes, FakeMetricsScenario};
pub use kernel::{BackendKernel, BootstrapResult, InfrastructureResult, KernelQueryResult};
pub use kube::KubeAdapter;
pub use operation::{
    OperationEvent, OperationState, OperationStatusData, Propagation, YamlValidationData,
};
pub use port::{
    AdapterError, BackendError, BackendEvent, Command, ContextInfo, Gvk, KubernetesAccess,
    MetricsSample, OperationId, OwnerRef, Query, QueryResult, RecordEvent, RelatedData,
    RelatedRecordGroup, ResourceListData, ResourceRecord, ResourceRef, StreamGrant, StreamInput,
    StreamKind, StreamRouteKind, Subscribe, SubscriptionHandle,
};
pub use runtime::{BackendMode, ContextRegistry, build_kernel};
pub use stream::{StreamChunk, StreamHub, StreamOrigin, StreamTicketResult};
