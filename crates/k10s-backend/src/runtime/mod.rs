//! Runtime backend composition: selecting which Kubernetes adapter backs a
//! server instance and building the kernel through one factory.
//!
//! Normal launches default to the real `Kube` adapter; `Fake` is only ever
//! produced by an explicit operator request (development flag or tests). The
//! factory never silently downgrades a failing kube launch into fake data —
//! configuration errors surface as typed startup failures instead.

mod cache;
pub(crate) mod cluster;
mod context;
pub(crate) mod supervisor;

use std::path::PathBuf;

use crate::fake::FakeKubernetes;
use crate::kernel::BackendKernel;
use crate::kube::KubeAdapter;
use crate::port::AdapterError;

pub use self::cache::{INITIAL_WATCH_REVISION, RevisionCounter, SummaryCache};
pub(crate) use self::cache::{now_rfc3339, record_from_row};
pub use self::cluster::{
    ClusterMetrics, ClusterWatches, METRICS_LINGER, METRICS_POLL_INTERVAL, MetricsApiState,
    MetricsCoverage, MetricsPollSource, MetricsSnapshot, ResourceUsageSample, WATCH_LINGER,
};
pub use self::context::{ContextRegistry, PreparedSwitch};
pub use self::supervisor::{
    ListedState, SelectionPublisher, WatchPhase, WatchRow, WatchSource, WatchUpdate,
};

/// A test-only scripted source factory: given a selection's GVK and optional
/// namespace, return a [`WatchSource`] to drive it (`None` falls back to the
/// real kube-rs path). Wired through
/// `KubeAdapter::with_scripted_watches` under the `testkit` feature.
#[cfg(feature = "testkit")]
pub type RuntimeWatchScript = std::sync::Arc<
    dyn Fn(&crate::port::Gvk, Option<&str>) -> Option<std::sync::Arc<dyn WatchSource>>
        + Send
        + Sync,
>;

/// Which Kubernetes adapter backs a running server instance.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendMode {
    /// Deterministic fake adapter for tests and explicit development opt-in.
    Fake,
    /// Real kube-rs adapter. `kubeconfig` is an explicit file path when given;
    /// otherwise standard discovery applies (`KUBECONFIG`, then
    /// `~/.kube/config`).
    Kube { kubeconfig: Option<PathBuf> },
}

/// Build the backend kernel for the selected mode through one factory seam.
///
/// Entry points must not construct kernels around adapters directly; going
/// through this factory keeps mode selection and error normalization in a
/// single, testable place. The kernel itself already erases adapter types,
/// so each branch stays concrete here.
pub fn build_kernel(mode: &BackendMode) -> Result<BackendKernel, AdapterError> {
    match mode {
        BackendMode::Fake => Ok(BackendKernel::new(FakeKubernetes::standard())),
        BackendMode::Kube { kubeconfig } => {
            KubeAdapter::from_kubeconfig(kubeconfig.as_deref()).map(BackendKernel::new)
        }
    }
}
