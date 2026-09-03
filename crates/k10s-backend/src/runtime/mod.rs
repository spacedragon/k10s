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

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::fake::FakeKubernetes;
use crate::kernel::BackendKernel;
use crate::kube::KubeAdapter;
use crate::port::AdapterError;

pub use self::cache::{INITIAL_WATCH_REVISION, RevisionCounter, SummaryCache};
pub(crate) use self::cache::{now_rfc3339, record_from_row};
pub use self::cluster::{
    ClusterMetrics, ClusterWatches, ContainerUsageSample, METRICS_LINGER, METRICS_POLL_INTERVAL,
    MetricsApiState, MetricsCoverage, MetricsPollSource, MetricsSnapshot, ResourceUsageSample,
    WATCH_LINGER,
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

/// Credential-free exec-plugin declaration captured during kube preparation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExecPluginPreparation {
    pub command: String,
    pub environment: BTreeMap<String, String>,
}

/// Reproduction metadata captured from the exact kubeconfig parse used by the kernel.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct KubePreparation {
    pub source_paths: Vec<PathBuf>,
    pub selected_context: String,
    pub exec_plugins: Vec<ExecPluginPreparation>,
}

/// One-shot factory result consumed by server and desktop launch description.
#[derive(Debug)]
pub struct PreparedBackend {
    kernel: BackendKernel,
    kube: Option<KubePreparation>,
}

impl PreparedBackend {
    pub fn into_kernel(self) -> BackendKernel {
        self.kernel
    }
    #[must_use]
    pub fn kube(&self) -> Option<&KubePreparation> {
        self.kube.as_ref()
    }
}

/// Prepare a backend once without independently rediscovering kube configuration.
pub fn prepare_backend(mode: &BackendMode) -> Result<PreparedBackend, AdapterError> {
    match mode {
        BackendMode::Fake => Ok(PreparedBackend {
            kernel: BackendKernel::new(FakeKubernetes::standard()),
            kube: None,
        }),
        BackendMode::Kube { kubeconfig } => {
            let sources = crate::kube::config::source_paths(kubeconfig.as_deref())?;
            let (contexts, parsed) = crate::kube::config::load_with_source(kubeconfig.as_deref())?;
            let selected_context =
                parsed
                    .current_context
                    .clone()
                    .ok_or(AdapterError::KubeconfigInvalid {
                        source: "prepared kubeconfig".into(),
                        detail: "no current context could be determined from the kubeconfig".into(),
                    })?;
            let mut exec_plugins = Vec::new();
            let selected_user = parsed
                .contexts
                .iter()
                .find(|named| named.name == selected_context)
                .and_then(|named| named.context.as_ref())
                .and_then(|context| context.user.as_deref());
            for named in parsed
                .auth_infos
                .iter()
                .filter(|named| Some(named.name.as_str()) == selected_user)
            {
                if let Some(exec) = named.auth_info.as_ref().and_then(|auth| auth.exec.as_ref()) {
                    let command = exec
                        .command
                        .clone()
                        .ok_or(AdapterError::KubeconfigInvalid {
                            source: "prepared kubeconfig".into(),
                            detail: "exec credential plugin has no command".into(),
                        })?;
                    let mut environment = BTreeMap::new();
                    for item in exec.env.as_deref().unwrap_or_default() {
                        environment
                            .extend(item.iter().map(|(key, value)| (key.clone(), value.clone())));
                    }
                    exec_plugins.push(ExecPluginPreparation {
                        command,
                        environment,
                    });
                }
            }
            let adapter = KubeAdapter::from_prepared_kubeconfig(contexts, parsed)?;
            Ok(PreparedBackend {
                kernel: BackendKernel::new(adapter),
                kube: Some(KubePreparation {
                    source_paths: sources,
                    selected_context,
                    exec_plugins,
                }),
            })
        }
    }
}

/// Build the backend kernel for the selected mode through one factory seam.
///
/// Entry points must not construct kernels around adapters directly; going
/// through this factory keeps mode selection and error normalization in a
/// single, testable place. The kernel itself already erases adapter types,
/// so each branch stays concrete here.
pub fn build_kernel(mode: &BackendMode) -> Result<BackendKernel, AdapterError> {
    prepare_backend(mode).map(PreparedBackend::into_kernel)
}
