//! Backend kernel and behavior-level Kubernetes port for k10s.
//!
//! This crate owns all Kubernetes-facing product behavior. The kernel is the
//! sole protocol-facing interface and maps to normalized protocol payloads.
//! Fake data never escapes as fixture types.

pub mod catalog;
pub mod fake;
pub mod kernel;
pub mod port;

pub use catalog::CatalogSnapshot;
pub use fake::{FakeKubernetes, FakeMetricsScenario};
pub use kernel::{BackendKernel, BootstrapResult, InfrastructureResult, KernelQueryResult};
pub use port::{
    BackendError, BackendEvent, Command, Gvk, KubernetesAccess, MetricsSample, OperationId,
    OwnerRef, Query, QueryResult, ResourceListData, ResourceRecord, ResourceRef, StreamKind,
    Subscribe, SubscriptionHandle,
};
