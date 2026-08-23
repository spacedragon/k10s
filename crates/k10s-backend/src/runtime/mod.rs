//! Runtime backend composition: selecting which Kubernetes adapter backs a
//! server instance and building the kernel through one factory.
//!
//! Normal launches default to the real `Kube` adapter; `Fake` is only ever
//! produced by an explicit operator request (development flag or tests). The
//! factory never silently downgrades a failing kube launch into fake data —
//! configuration errors surface as typed startup failures instead.

mod context;

use std::path::PathBuf;

use crate::fake::FakeKubernetes;
use crate::kernel::BackendKernel;
use crate::kube::KubeAdapter;
use crate::port::AdapterError;

pub use self::context::ContextRegistry;

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
