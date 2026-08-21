//! Internal behavior-level Kubernetes access port.
//!
//! This is the sole seam between the backend kernel and Kubernetes adapters.
//! All future fake and kube-rs work must extend this same port rather than
//! adding side doors. The kernel is the sole protocol-facing interface and
//! owns mapping to normalized protocol payloads.

use std::future::Future;
use std::pin::Pin;

use serde::Serialize;

/// A behavior-level query to the Kubernetes adapter.
///
/// Only `Bootstrap` is implemented in this task; unsupported variants return
/// typed capability errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// Bootstrap: load contexts and server metadata.
    Bootstrap,
    /// Validate a YAML apply without submitting it.
    ValidateApply { context: String, yaml: String },
    /// Issue a stream ticket for logs or exec.
    StreamTicket { stream: StreamKind },
}

/// Kind of stream to open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamKind {
    /// Tail logs from a container.
    Logs {
        context: String,
        namespace: String,
        pod: String,
        container: String,
    },
    /// Attach to an exec session.
    Exec {
        context: String,
        namespace: String,
        pod: String,
        container: String,
    },
}

/// A behavior-level command (mutation) to the Kubernetes adapter.
///
/// All variants are unsupported in this task; they return typed capability
/// errors. `execute(Command)` always returns an `OperationId` when supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Apply a YAML manifest.
    Apply {
        context: String,
        yaml: String,
        idempotency_key: String,
    },
    /// Scale a deployment or replicaset.
    Scale {
        context: String,
        kind: String,
        namespace: String,
        name: String,
        replicas: u32,
    },
    /// Delete a resource.
    Delete {
        context: String,
        kind: String,
        namespace: String,
        name: String,
        idempotency_key: String,
    },
}

/// A behavior-level subscription to the Kubernetes adapter.
///
/// Only `BootstrapStatus` is implemented in this task; unsupported variants
/// return typed capability errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscribe {
    /// Opaque bootstrap-status subscription.
    BootstrapStatus,
    /// Subscribe to a resource list watch.
    ResourceList {
        context: String,
        kind: String,
        namespace: Option<String>,
    },
}

/// Result of a query to the Kubernetes adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapInfo),
}

/// Bootstrap information returned by the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapInfo {
    /// Available contexts.
    pub contexts: Vec<ContextInfo>,
}

/// Safe context metadata exposed to the UI.
///
/// Never exposes credentials or raw kubeconfig.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContextInfo {
    /// Context name.
    pub name: String,
    /// Cluster name.
    pub cluster: String,
    /// Default namespace, if set.
    pub namespace: Option<String>,
    /// Whether this is the current context.
    pub is_current: bool,
}

/// Typed errors from the Kubernetes adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendError {
    /// The requested capability is not supported by this adapter.
    Unsupported { capability: String },
    /// The request timed out.
    Timeout,
    /// The request was cancelled.
    Cancelled,
    /// An internal error occurred.
    Internal(String),
}

impl BackendError {
    /// Create an unsupported-capability error.
    #[must_use]
    pub fn unsupported(capability: impl Into<String>) -> Self {
        Self::Unsupported {
            capability: capability.into(),
        }
    }

    /// Return the capability name for unsupported errors.
    #[must_use]
    pub fn capability(&self) -> Option<&str> {
        match self {
            Self::Unsupported { capability } => Some(capability),
            _ => None,
        }
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported { capability } => write!(f, "unsupported capability: {capability}"),
            Self::Timeout => write!(f, "request timed out"),
            Self::Cancelled => write!(f, "request was cancelled"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for BackendError {}

/// An opaque identifier for a background operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationId(String);

impl OperationId {
    /// Create a new operation ID.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the operation ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for OperationId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for OperationId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// A handle to an active subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionHandle {
    /// Opaque subscription ID.
    pub id: String,
}

impl SubscriptionHandle {
    /// Create a new subscription handle.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// The internal behavior-level Kubernetes access port.
///
/// Implemented by the real (kube-rs) and fake adapters. The kernel is the
/// sole protocol-facing interface and owns mapping to normalized protocol
/// payloads. Fake data never escapes as fixture types.
pub trait KubernetesAccess: Send + Sync + std::fmt::Debug {
    /// Execute a behavior-level query.
    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>;

    /// Execute a behavior-level command (mutation).
    ///
    /// Always returns an `OperationId` when supported.
    fn execute<'a>(
        &'a self,
        cmd: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>>;

    /// Open a behavior-level subscription.
    fn subscribe<'a>(
        &'a self,
        req: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>>;
}
