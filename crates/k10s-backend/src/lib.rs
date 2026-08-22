//! Backend kernel and behavior-level Kubernetes port for k10s.
//!
//! This crate owns all Kubernetes-facing product behavior. The kernel is the
//! sole protocol-facing interface and maps to normalized protocol payloads.
//! Fake data never escapes as fixture types.

pub mod fake;
pub mod kernel;
pub mod port;

pub use fake::FakeKubernetes;
pub use kernel::{BackendKernel, BootstrapResult, KernelQueryResult};
pub use port::{
    BackendError, BackendEvent, Command, Gvk, KubernetesAccess, MetricsSample, OperationId,
    OwnerRef, Query, QueryResult, ResourceListData, ResourceRecord, ResourceRef, StreamKind,
    Subscribe, SubscriptionHandle,
};
