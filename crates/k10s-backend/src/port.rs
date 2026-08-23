//! Internal behavior-level Kubernetes access port.
//!
//! This is the sole seam between the backend kernel and Kubernetes adapters.
//! All future fake and kube-rs work must extend this same port rather than
//! adding side doors. The kernel is the sole protocol-facing interface and
//! owns mapping to normalized protocol payloads.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use serde::Serialize;
use tokio::sync::broadcast;

use crate::catalog::CatalogSnapshot;

/// A behavior-level query to the Kubernetes adapter.
///
/// Unsupported variants return typed capability errors. Resource queries
/// carry backend-owned normalized types only; no Kubernetes client types
/// cross this seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// Bootstrap: load contexts and server metadata.
    Bootstrap,
    /// Validate a YAML apply without submitting it.
    ValidateApply { context: String, yaml: String },
    /// Issue a stream ticket for logs or exec.
    StreamTicket { stream: StreamKind },
    /// List one normalized resource type on one context.
    ResourceList {
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
    },
    /// Fetch normalized details for one resource.
    ResourceDetail { reference: ResourceRef },
    /// Resolve the related objects of one resource by controller-owner
    /// traversal (for example Deployment → ReplicaSet → Pod).
    ResourceRelations { reference: ResourceRef },
    /// Fetch the availability-gated metrics sample for one pod.
    ResourceMetrics { reference: ResourceRef },
    /// List the selectable resource types (built-ins and CRDs) of a context.
    ResourceTypes { context: String },
    /// Fetch the complete Overview, Nodes, and Storage projection.
    Infrastructure { context: String },
}

/// One selectable resource type behind [`Query::ResourceTypes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeEntry {
    /// Group/version/kind of the type.
    pub gvk: Gvk,
    /// Whether objects of this type live inside namespaces.
    pub namespaced: bool,
}

/// Selectable resource types of one context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTypesData {
    /// Context the types were read from.
    pub context: String,
    /// Types sorted by group/version/kind.
    pub types: Vec<TypeEntry>,
}

/// Kind of stream to open.
///
/// `Exec` carries the explicit mode: `tty: true` is an interactive shell
/// with merged output; `tty: false` is the retained non-TTY mode whose
/// stdout and stderr stay separated.
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
        tty: bool,
    },
}

/// Which dedicated socket route a stream belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRouteKind {
    /// The logs route.
    Logs,
    /// The exec route.
    Exec,
}

/// Inbound data on a live exec session, forwarded by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamInput {
    /// One line of TTY standard input.
    Stdin(String),
    /// Terminal resize.
    Resize { cols: u32, rows: u32 },
}

/// A behavior-level command (mutation) to the Kubernetes adapter.
///
/// All variants are unsupported in this task; they return typed capability
/// errors. `execute(Command)` always returns an `OperationId` when supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Apply a YAML manifest through a previously issued validation ticket.
    ///
    /// The adapter re-checks every binding before the apply runs — the
    /// ticket must exist unconsumed and unexpired, the declared target
    /// identity and buffer hash must match the ticket exactly, and the
    /// target revision must equal the validated revision. The ticket is
    /// consumed single-use.
    Apply {
        context: String,
        yaml: String,
        idempotency_key: String,
        ticket_id: String,
        buffer_hash: String,
        target: ResourceRef,
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
/// `BootstrapStatus` and resource watches are implemented; unsupported
/// variants return typed capability errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscribe {
    /// Opaque bootstrap-status subscription.
    BootstrapStatus,
    /// Watch one normalized resource type on one context.
    ResourceWatch {
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
    },
    /// Watch coalescible infrastructure telemetry for one context.
    Infrastructure { context: String },
    /// Redeem a single-use stream ticket in the kernel-owned Stream Hub.
    /// Returns a bounded receiver of stream chunks; the ticket is consumed
    /// exactly once.
    StreamRedeem {
        /// Ticket issued through [`Query::StreamTicket`].
        ticket_id: String,
        /// Dedicated route the redemption arrives on.
        route: StreamRouteKind,
    },
}

/// Result of a query to the Kubernetes adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapInfo),
    /// Normalized list snapshot for one resource type.
    ResourceList(ResourceListData),
    /// Normalized details for one resource.
    ResourceDetail(ResourceRecord),
    /// Backend-resolved related rows for one resource.
    ResourceRelations(RelatedData),
    /// Availability-gated metrics sample for one pod.
    ResourceMetrics(MetricsSample),
    /// Selectable resource types of one context.
    ResourceTypes(ResourceTypesData),
    /// Overview, Nodes, Storage, and cluster metrics catalog.
    Infrastructure(CatalogSnapshot),
    /// Guarded YAML validation outcome with an issued ticket when valid.
    YamlValidation(crate::operation::YamlValidationData),
    /// A single-use stream ticket was issued for one target.
    StreamTicket(StreamGrant),
}

/// An issued single-use stream ticket before protocol mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGrant {
    /// Opaque ticket ID redeemed in the stream socket's first `hello`.
    pub ticket_id: String,
    /// Bound stream identity and mode.
    pub stream: StreamKind,
}

/// Backend-owned group/version/kind of a resource type.
///
/// The kernel maps this to the protocol-facing payload; Kubernetes client
/// types never cross this port.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gvk {
    /// API group, empty for the core group.
    pub group: String,
    /// API version within the group.
    pub version: String,
    /// Kubernetes kind, such as `Deployment`.
    pub kind: String,
}

impl Gvk {
    /// Construct a group/version/kind triple.
    #[must_use]
    pub fn new(
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        }
    }

    /// Construct a core-group kind.
    #[must_use]
    pub fn core(version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self::new(String::new(), version, kind)
    }
}

/// Stable backend-owned identity of one resource.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceRef {
    /// Kubernetes context this object was read from.
    pub context: String,
    /// Type of the object.
    pub gvk: Gvk,
    /// Namespace, absent for cluster-scoped objects.
    pub namespace: Option<String>,
    /// Object name.
    pub name: String,
    /// Immutable server-assigned identifier.
    pub uid: String,
}

impl ResourceRef {
    /// Return the stable coalescing key used by bounded schedulers.
    #[must_use]
    pub fn coalescing_key(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.context,
            self.gvk.group,
            self.gvk.kind,
            self.namespace.as_deref().unwrap_or(""),
            self.name
        )
    }
}

/// One normalized resource row as observed by the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRecord {
    /// Stable identity of the row's object.
    pub reference: ResourceRef,
    /// Monotonic backend revision at which this row was last true.
    pub revision: u64,
    /// Object labels, sorted by key.
    pub labels: BTreeMap<String, String>,
    /// Human-readable status summary, such as `2/2 ready`.
    pub summary: String,
    /// Creation time formatted as RFC 3339.
    pub created_at: String,
    /// Owner chain references resolved by the adapter.
    pub owner_references: Vec<OwnerRef>,
    /// Deterministic events observed for this object.
    pub events: Vec<RecordEvent>,
}

/// A reference from a child object to its owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OwnerRef {
    /// Type of the owner.
    pub gvk: Gvk,
    /// Owner name.
    pub name: String,
    /// Owner UID matching [`ResourceRef::uid`] semantics.
    pub uid: String,
    /// Whether this owner is the managing controller.
    pub controller: bool,
}

/// One deterministic event observed for a resource.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordEvent {
    /// Short Kubernetes-style event reason, such as `Started`.
    pub reason: String,
    /// Human-readable event message.
    pub message: String,
    /// How many times this event repeated.
    pub count: u32,
    /// Last occurrence formatted as RFC 3339.
    pub last_seen: String,
}

/// One group of related records sharing a type, resolved by traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelatedRecordGroup {
    /// Type every record in this group shares.
    pub gvk: Gvk,
    /// Records sorted by stable identity.
    pub records: Vec<ResourceRecord>,
}

/// Backend-resolved related rows for one resource.
///
/// The adapter owns the owner-reference traversal; the kernel maps this
/// into protocol-facing related groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelatedData {
    /// The resource whose relations were resolved.
    pub reference: ResourceRef,
    /// Groups in deterministic type order.
    pub groups: Vec<RelatedRecordGroup>,
}

impl RelatedData {
    /// Related data with no groups; used when an adapter cannot traverse.
    #[must_use]
    pub fn empty(reference: ResourceRef) -> Self {
        Self {
            reference,
            groups: Vec::new(),
        }
    }
}

/// A normalized list snapshot for one resource type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceListData {
    /// Context the rows were read from.
    pub context: String,
    /// Type of the listed resources.
    pub gvk: Gvk,
    /// Namespace restriction echoed back, when set.
    pub namespace: Option<String>,
    /// Backend revision covering the whole snapshot.
    pub revision: u64,
    /// Sorted normalized rows.
    pub rows: Vec<ResourceRecord>,
    /// Deterministic generation timestamp formatted as RFC 3339.
    pub generated_at: String,
}

/// An availability-gated metrics sample for one pod.
///
/// Availability is derived from completeness by the kernel so that the wire
/// contract stays consistent: all values present means available, some means
/// partial, none means unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetricsSample {
    /// CPU usage in millicores, absent when not collected.
    pub cpu_millicores: Option<u64>,
    /// Working-set memory in bytes, absent when not collected.
    pub memory_bytes: Option<u64>,
    /// Deterministic collection timestamp formatted as RFC 3339.
    pub collected_at: Option<String>,
}

/// One event delivered on a backend subscription stream.
#[derive(Debug, Clone)]
pub enum BackendEvent {
    /// The full current snapshot for the watched selector.
    Snapshot(ResourceListData),
    /// One object changed; carries the full updated row.
    Changed(ResourceRecord),
    /// One object was removed at the given revision.
    Gone {
        reference: ResourceRef,
        revision: u64,
    },
    /// A complete infrastructure telemetry projection. The server coalesces
    /// these by context on its bounded P2 scheduler.
    Infrastructure(CatalogSnapshot),
    /// One chunk of a redeemed stream session. `exit_code` terminates it.
    Stream(crate::stream::StreamChunk),
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
    /// The addressed context or object does not exist.
    NotFound,
    /// The target changed since validation, or the ticket is stale,
    /// consumed, or tampered. Safe reason attached.
    Conflict(String),
    /// The operation was denied by authorization policy (RBAC).
    Forbidden,
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
            Self::NotFound => write!(f, "context or resource not found"),
            Self::Conflict(reason) => write!(f, "conflict: {reason}"),
            Self::Forbidden => write!(f, "access denied"),
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
///
/// Resource watches deliver [`BackendEvent`]s on a bounded broadcast
/// channel; a lagging consumer is reported through the receiver error so the
/// server can demand a resync rather than silently dropping deltas.
#[derive(Debug)]
pub struct SubscriptionHandle {
    /// Opaque subscription ID.
    pub id: String,
    events: Option<broadcast::Receiver<BackendEvent>>,
    /// Backend-owned stream binding, set by stream redemptions so the
    /// server can echo the bound target in its ready frame.
    stream: Option<StreamKind>,
}

impl SubscriptionHandle {
    /// Create a new subscription handle without an event stream.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            events: None,
            stream: None,
        }
    }

    /// Create a subscription handle carrying an event stream.
    #[must_use]
    pub fn with_events(id: impl Into<String>, events: broadcast::Receiver<BackendEvent>) -> Self {
        Self {
            id: id.into(),
            events: Some(events),
            stream: None,
        }
    }

    /// Attach the backend-owned stream binding of a redeemed ticket.
    #[must_use]
    pub fn with_stream(mut self, stream: StreamKind) -> Self {
        self.stream = Some(stream);
        self
    }

    /// Take the event stream, if this subscription carries one.
    #[must_use]
    pub fn take_events(&mut self) -> Option<broadcast::Receiver<BackendEvent>> {
        self.events.take()
    }

    /// Take the bound stream identity of a redeemed ticket.
    #[must_use]
    pub fn take_bound_stream(&mut self) -> Option<StreamKind> {
        self.stream.take()
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

    /// Forward inbound user input into a redeemed stream session.
    ///
    /// Sessions are keyed by their (consumed) ticket ID; unknown sessions
    /// are typed conflicts.
    fn stream_input<'a>(
        &'a self,
        ticket_id: &'a str,
        input: StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>>;
}
