//! Internal behavior-level Kubernetes access port.
//!
//! This is the sole seam between the backend kernel and Kubernetes adapters.
//! All future fake and kube-rs work must extend this same port rather than
//! adding side doors. The kernel is the sole protocol-facing interface and
//! owns mapping to normalized protocol payloads.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use serde::Serialize;
use tokio::sync::broadcast;

pub use k10s_protocol::ContextAvailability;

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
    /// Authoritatively dry-run deletion of one exact target and policy.
    DeletePreflight {
        target: ResourceRef,
        propagation: crate::operation::Propagation,
    },
    /// Resolve the related objects of one resource by controller-owner
    /// traversal (for example Deployment → ReplicaSet → Pod).
    ResourceRelations { reference: ResourceRef },
    /// Fetch the availability-gated metrics sample for one pod.
    ResourceMetrics { reference: ResourceRef },
    /// List the selectable resource types (built-ins and CRDs) of a context.
    ResourceTypes { context: String },
    /// Switch the current context after validating the destination's minimal
    /// read path. A failed prepare leaves the current context unchanged.
    ContextSwitch { to: String },
    /// Project advisory RBAC capabilities of one context through
    /// SelfSubjectAccessReviews. Outcomes are metadata only: later
    /// operations still hit the API server and respect its decisions.
    ContextPermissions {
        context: String,
        probes: Vec<PermissionProbe>,
    },
    /// Fetch the complete Overview, Nodes, and Storage projection.
    Infrastructure { context: String },
    /// Look up the current state of specific operations by ID. IDs the
    /// adapter no longer knows are simply absent from the answer.
    OperationStatus { operation_ids: Vec<String> },
}

/// One selectable resource type behind [`Query::ResourceTypes`].
///
/// Normalized discovery data only: adapters translate cluster responses into
/// this descriptor so Kubernetes client and openapi types never cross the seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResourceDescriptor {
    /// Group/version/kind of the type.
    pub gvk: Gvk,
    /// Plural API name used in cluster resource paths (for example
    /// `deployments`), normalized from discovery for every adapter.
    pub plural: String,
    /// Whether objects of this type live inside namespaces.
    pub namespaced: bool,
    /// Whether the cluster exposes a scale subresource for this type.
    pub supports_scale: bool,
    /// Whether the cluster advertises the watch verb for this type; a
    /// list-only type cannot back a live resource watch.
    pub supports_watch: bool,
    /// Whether discovery advertises patch on the main resource.
    pub supports_patch: bool,
    /// Whether discovery advertises create on the main resource collection.
    pub supports_create: bool,
    /// Whether discovery advertises delete on the main resource.
    pub supports_delete: bool,
}

/// Selectable resource types of one context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTypesData {
    /// Context the types were read from.
    pub context: String,
    /// Types sorted by group/version/kind.
    pub types: Vec<ApiResourceDescriptor>,
}

impl ResourceTypesData {
    /// Resolve one type by exact kind match in catalog order.
    #[must_use]
    pub fn find_kind(&self, kind: &str) -> Option<&ApiResourceDescriptor> {
        self.types.iter().find(|entry| entry.gvk.kind == kind)
    }

    /// Resolve one type by exact plural name match in catalog order.
    #[must_use]
    pub fn find_plural(&self, plural: &str) -> Option<&ApiResourceDescriptor> {
        self.types.iter().find(|entry| entry.plural == plural)
    }

    /// All types of one group/version slice, keeping catalog order.
    #[must_use]
    pub fn of_group_version(&self, group: &str, version: &str) -> Vec<&ApiResourceDescriptor> {
        self.types
            .iter()
            .filter(|entry| entry.gvk.group == group && entry.gvk.version == version)
            .collect()
    }
}

/// One requested advisory permission check behind
/// [`Query::ContextPermissions`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermissionProbe {
    /// Kubernetes verb to review, such as `list` or `delete`.
    pub verb: String,
    /// Resource plural to review, such as `pods`.
    pub resource: String,
    /// API group of the reviewed resource; absent means the core group, so
    /// grouped resources such as `apps/deployments` review the right group.
    pub group: Option<String>,
    /// Namespace restriction, when reviewed within one.
    pub namespace: Option<String>,
}

/// Hard bound on probes carried by one permission query, so one request can
/// never fan out into unbounded review traffic. Every adapter sharing this
/// port enforces the same bound.
pub(crate) const MAX_PROBES: usize = 32;

/// Reject probe sets past the documented bound before any backend work.
pub(crate) fn validate_probe_count(probes: &[PermissionProbe]) -> Result<(), BackendError> {
    if probes.len() > MAX_PROBES {
        return Err(BackendError::Conflict(format!(
            "permission review requests carry at most {MAX_PROBES} probes"
        )));
    }
    Ok(())
}

/// Collapse duplicate probes onto their first occurrence, preserving
/// first-seen order, so repeating a probe never changes the answer's shape.
pub(crate) fn distinct_probes(probes: Vec<PermissionProbe>) -> Vec<PermissionProbe> {
    let mut seen = HashSet::new();
    probes
        .into_iter()
        .filter(|probe| seen.insert(probe.clone()))
        .collect()
}

/// What authorization reported for one probe.
///
/// Advisory metadata only: it tells callers what later operations are
/// expected to be allowed so UIs can hint, and is never enforced client-side.
/// [`PermissionOutcome::Unknown`] is distinct from denied — it means the
/// review itself could not be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionOutcome {
    /// The review reported the action as allowed.
    Allowed,
    /// The review reported the action as denied.
    Denied,
    /// The review could not answer (rejected, unreachable, or errored).
    Unknown,
}

/// One answered advisory permission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionCheck {
    /// Kubernetes verb that was reviewed.
    pub verb: String,
    /// Resource plural that was reviewed.
    pub resource: String,
    /// API group the review asked about, echoed from the probe.
    pub group: Option<String>,
    /// Namespace restriction, when reviewed within one.
    pub namespace: Option<String>,
    /// What authorization reported.
    pub outcome: PermissionOutcome,
}

/// Advisory RBAC capability projection of one context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPermissionsData {
    /// Context the checks were reviewed against.
    pub context: String,
    /// One answered check per distinct probe, in request order.
    pub checks: Vec<PermissionCheck>,
}

/// A committed context switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSwitchData {
    /// Context that is current after the commit.
    pub current: String,
    /// Context that lost the current marker, when one existed.
    pub previous: Option<String>,
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
        uid: String,
        container: String,
        tail_lines: Option<i64>,
        since_seconds: Option<i64>,
        previous: bool,
        timestamps: bool,
        follow: bool,
    },
    /// Attach to an exec session.
    Exec {
        context: String,
        namespace: String,
        pod: String,
        uid: String,
        container: String,
        /// Exact remote command and arguments; never interpreted locally.
        command: Vec<String>,
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
/// Every variant returns an `OperationId` when supported; the mutation
/// itself is applied to adapter state immediately while the operation's
/// lifecycle advances deterministically. All commands carry an
/// idempotency key: replaying a key returns the original `OperationId`
/// instead of executing again.
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
    /// Scale one exact workload object to a replica count. The full
    /// identity (including UID) is re-checked before the mutation runs.
    Scale {
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
        name: String,
        uid: String,
        replicas: u32,
        idempotency_key: String,
    },
    /// Request a rollout restart of one exact workload object.
    Restart {
        target: ResourceRef,
        idempotency_key: String,
    },
    /// Create a Job from one exact Job or CronJob source.
    CreateJob {
        source: ResourceRef,
        idempotency_key: String,
    },
    /// Suspend or resume one exact CronJob.
    SetCronJobSuspended {
        target: ResourceRef,
        suspended: bool,
        idempotency_key: String,
    },
    /// Delete one exact object with an explicit propagation mode.
    Delete {
        target: ResourceRef,
        propagation: crate::operation::Propagation,
        resource_version: String,
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
    /// Subscribe to background operation lifecycle events. Late
    /// subscribers immediately receive the current state of every live
    /// (nonterminal) operation so reconnecting sessions resynchronize.
    Operations,
}

/// Result of a query to the Kubernetes adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Resource records intentionally remain inline at this adapter boundary.
pub enum QueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapInfo),
    /// Normalized list snapshot for one resource type.
    ResourceList(ResourceListData),
    /// Normalized details for one resource.
    ResourceDetail(ResourceRecord),
    /// Successful exact-target delete dry-run.
    DeletePreflight(k10s_protocol::DeletePreflightResponse),
    /// Backend-resolved related rows for one resource.
    ResourceRelations(RelatedData),
    /// Availability-gated metrics sample for one pod.
    ResourceMetrics(MetricsSample),
    /// Selectable resource types of one context.
    ResourceTypes(ResourceTypesData),
    /// A committed context switch.
    ContextSwitch(ContextSwitchData),
    /// Advisory RBAC capability projection of one context.
    ContextPermissions(ContextPermissionsData),
    /// Overview, Nodes, Storage, and cluster metrics catalog.
    Infrastructure(CatalogSnapshot),
    /// Guarded YAML validation outcome with an issued ticket when valid.
    YamlValidation(crate::operation::YamlValidationData),
    /// A single-use stream ticket was issued for one target.
    StreamTicket(StreamGrant),
    /// Current records for the requested operation IDs.
    OperationStatus(crate::operation::OperationStatusData),
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

    /// Return an unambiguous key for the exact immutable object identity.
    ///
    /// Unlike [`Self::coalescing_key`], this includes API version and UID and
    /// therefore must be used by idempotency fingerprints. Length prefixes
    /// prevent separator characters in context names from creating aliases.
    #[must_use]
    pub fn exact_identity_key(&self) -> String {
        let fields = [
            self.context.as_str(),
            self.gvk.group.as_str(),
            self.gvk.version.as_str(),
            self.gvk.kind.as_str(),
            self.namespace.as_deref().unwrap_or(""),
            self.name.as_str(),
            self.uid.as_str(),
        ];
        fields
            .into_iter()
            .map(|field| format!("{}:{field}", field.len()))
            .collect::<Vec<_>>()
            .join("|")
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
    /// Whether the authoritative event APIs were readable for this detail.
    pub events_condition: RecordEventsCondition,
    /// Authoritative YAML of the fetched object, rendered by the adapter and
    /// bound to its UID/resourceVersion. Empty for watch rows; detail reads
    /// always carry it so guarded edits can detect drift.
    pub manifest: String,
    /// Kind-specific structured projection; populated only for kinds with a
    /// designed projection and absent everywhere else.
    pub projection: Option<ResourceProjection>,
}

/// Availability of event decoration on an authoritative detail record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordEventsCondition {
    /// Both Kubernetes event API variants were read successfully.
    Available,
    /// At least one event API was unavailable or exceeded the total budget.
    Unavailable,
}

/// A kind-specific normalized projection carried by backend records.
///
/// Projections contain structured view-model data only: no raw Kubernetes
/// objects and no credential-bearing fields ever appear here. The kernel
/// maps them onto the protocol-facing payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceProjection {
    /// Normalized core/v1 Pod view model.
    Pod(PodProjection),
    /// Normalized apps/v1 Deployment view model.
    Deployment(DeploymentProjection),
    /// Normalized apps/v1 ReplicaSet rollout-history row.
    ReplicaSet(ReplicaSetProjection),
    /// Normalized core/v1 Service view model.
    Service(ServiceProjection),
}

/// A normalized condition shared by Pod and Deployment projections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceConditionProjection {
    /// Kubernetes condition type, such as `Ready` or `Progressing`.
    pub condition_type: String,
    /// Kubernetes condition status (`True`, `False`, or `Unknown`).
    pub status: String,
    /// Machine-readable reason when the source reported one.
    pub reason: Option<String>,
    /// Human-readable condition detail when the source reported one.
    pub message: Option<String>,
    /// Last transition time formatted as RFC 3339, when reported.
    pub last_transition_time: Option<String>,
}

/// Current lifecycle state of one Pod container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContainerStateProjection {
    /// The container is currently running.
    Running,
    /// The container is waiting to start or restart.
    Waiting {
        /// Authoritative waiting reason, such as `CrashLoopBackOff`.
        reason: Option<String>,
    },
    /// The current container instance has terminated.
    Terminated(ContainerTerminationProjection),
}

/// The most recent terminated instance of a restarted container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerTerminationProjection {
    /// Process exit code reported by the container runtime.
    pub exit_code: i32,
    /// Authoritative termination reason, when reported.
    pub reason: Option<String>,
}

/// One normalized Pod container status row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodContainerProjection {
    /// Exact container name used to join authoritative metrics samples.
    pub name: String,
    /// Declared container image, absent when Kubernetes did not report it.
    pub image: Option<String>,
    /// Current lifecycle state, absent when container status is unavailable.
    pub state: Option<ContainerStateProjection>,
    /// Current readiness reported by container status.
    pub ready: Option<bool>,
    /// Current restart count reported by container status.
    pub restart_count: Option<u32>,
    /// Most recent terminated instance, when a restart history exists.
    pub last_termination: Option<ContainerTerminationProjection>,
}

/// One declared Pod container port, retained with its declaring container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodContainerPort {
    /// Exact name of the container declaring this port.
    pub container_name: String,
    /// Declared port name, when supplied.
    pub name: Option<String>,
    /// Container port validated to the TCP/UDP/SCTP range.
    pub container_port: u16,
    /// Optional host port validated to the TCP/UDP/SCTP range.
    pub host_port: Option<u16>,
    /// Declared transport protocol, defaulted by Kubernetes to TCP.
    pub protocol: TransportProtocol,
}

/// A normalized Pod projection used by list and detail responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodProjection {
    /// Current Pod phase, absent when status is incomplete.
    pub phase: Option<String>,
    /// Number of ready containers, absent when status is incomplete.
    pub ready_containers: Option<u32>,
    /// Number of declared containers, absent when the spec is incomplete.
    pub total_containers: Option<u32>,
    /// Sum of authoritative container restart counts, when available.
    pub restart_count: Option<u32>,
    /// Containers in declaration order with status joined by exact name.
    pub containers: Vec<PodContainerProjection>,
    /// Pod conditions in backend-normalized deterministic order.
    pub conditions: Vec<ResourceConditionProjection>,
    /// Scheduled node name, absent while unscheduled or unavailable.
    pub node_name: Option<String>,
    /// Primary Pod IP, absent while unassigned or unavailable.
    pub pod_ip: Option<String>,
    /// Node IP hosting the Pod, absent while unscheduled or unavailable.
    pub host_ip: Option<String>,
    /// Kubernetes QoS class, absent when status is incomplete.
    pub qos_class: Option<String>,
    /// Explicit Pod priority, absent when the spec did not report one.
    pub priority: Option<i32>,
    /// Effective service account name, absent when the spec did not report one.
    pub service_account: Option<String>,
    /// Declared Pod restart policy, absent when the spec did not report one.
    pub restart_policy: Option<String>,
    /// Declared container ports in Pod spec order.
    pub ports: Vec<PodContainerPort>,
    /// Pod labels sorted by key.
    pub labels: BTreeMap<String, String>,
    /// Pod annotations sorted by key.
    pub annotations: BTreeMap<String, String>,
    /// Creation time formatted as RFC 3339, when available.
    pub created_at: Option<String>,
}

/// One name/image pair from a workload Pod template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerImageProjection {
    /// Exact container name from the template.
    pub name: String,
    /// Declared image, absent when Kubernetes did not report it.
    pub image: Option<String>,
}

/// A normalized Deployment projection used by list and detail responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentProjection {
    /// Desired replicas, absent when the spec is incomplete.
    pub desired_replicas: Option<u32>,
    /// Ready replicas, absent when status is incomplete.
    pub ready_replicas: Option<u32>,
    /// Replicas updated to the current template, when reported.
    pub updated_replicas: Option<u32>,
    /// Available replicas, absent when status is incomplete.
    pub available_replicas: Option<u32>,
    /// Deployment strategy, such as `RollingUpdate` or `Recreate`.
    pub strategy: Option<String>,
    /// Match labels from the Deployment selector, sorted by key.
    pub selector: BTreeMap<String, String>,
    /// Rolling-update maximum surge, normalized from integer or percentage.
    pub max_surge: Option<String>,
    /// Rolling-update maximum unavailable, normalized from integer or percentage.
    pub max_unavailable: Option<String>,
    /// Deployment conditions in backend-normalized deterministic order.
    pub conditions: Vec<ResourceConditionProjection>,
    /// Container images declared by the Pod template.
    pub template_containers: Vec<ContainerImageProjection>,
    /// Pod-template labels sorted by key.
    pub template_labels: BTreeMap<String, String>,
    /// Pod-template annotations sorted by key.
    pub template_annotations: BTreeMap<String, String>,
    /// Deployment labels sorted by key, including manager metadata.
    pub labels: BTreeMap<String, String>,
    /// Deployment annotations sorted by key, including manager metadata.
    pub annotations: BTreeMap<String, String>,
    /// Creation time formatted as RFC 3339, when available.
    pub created_at: Option<String>,
}

/// A normalized ReplicaSet projection used by rollout-history rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSetProjection {
    /// Parsed Deployment revision; rows without one never use this projection.
    pub revision: u64,
    /// Desired replicas, absent when the spec is incomplete.
    pub replicas: Option<u32>,
    /// Ready replicas, absent when status is incomplete.
    pub ready_replicas: Option<u32>,
    /// Creation time formatted as RFC 3339, when available.
    pub created_at: Option<String>,
}

/// Normalized core/v1 Service projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceProjection {
    /// Kubernetes Service type, such as `ClusterIP` or `NodePort`.
    pub service_type: String,
    /// Primary cluster IPs; empty for ExternalName Services.
    pub cluster_ips: Vec<String>,
    /// Selector labels, sorted by key.
    pub selector: BTreeMap<String, String>,
    /// External name of an ExternalName Service.
    pub external_name: Option<String>,
    /// Session affinity policy when explicitly set.
    pub session_affinity: Option<String>,
    /// External traffic policy when present.
    pub external_traffic_policy: Option<String>,
    /// Internal traffic policy when present.
    pub internal_traffic_policy: Option<String>,
    /// Every declared Service port, including non-forwardable ones.
    pub ports: Vec<ServicePort>,
}

/// One declared Service port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePort {
    /// Declared port name, absent for unnamed ports.
    pub name: Option<String>,
    /// Declared Service port number.
    pub service_port: u16,
    /// Resolved target port on backing Pods.
    pub target_port: TargetPort,
    /// Node port declared by NodePort or LoadBalancer Services.
    pub node_port: Option<u16>,
    /// Wire protocol of the port.
    pub protocol: TransportProtocol,
    /// Optional application protocol label.
    pub app_protocol: Option<String>,
}

/// The target port of a Service port declaration.
///
/// An omitted Kubernetes `targetPort` normalizes to
/// [`TargetPort::Number`] carrying the Service port number so the defaulted
/// case is explicit instead of reconstructed by consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetPort {
    /// A named target port.
    Name(String),
    /// A numeric target port.
    Number(u16),
}

/// Wire transport protocol of a Service port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportProtocol {
    /// TCP ports may be forwarded by the desktop application.
    Tcp,
    /// UDP ports render read-only and are never forwarded.
    Udp,
    /// SCTP ports render read-only and are never forwarded.
    Sctp,
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
    /// Authoritative backend revision covering the complete relation cut.
    pub revision: u64,
    /// Groups in deterministic type order.
    pub groups: Vec<RelatedRecordGroup>,
}

impl RelatedData {
    /// Related data with no groups; used when an adapter cannot traverse.
    #[must_use]
    pub fn empty(reference: ResourceRef, revision: u64) -> Self {
        Self {
            reference,
            revision,
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
    /// Per-container samples keyed by exact Metrics API names, sorted by name.
    pub containers: Vec<ContainerMetricsSample>,
}

/// One availability-gated usage sample for an exact container name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContainerMetricsSample {
    /// Exact container name reported by the Metrics API.
    pub name: String,
    /// CPU usage in millicores, absent when not reported.
    pub cpu_millicores: Option<u64>,
    /// Working-set memory in bytes, absent when not reported.
    pub memory_bytes: Option<u64>,
}

/// One event delivered on a backend subscription stream.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // Resource records intentionally remain inline on subscription events.
pub enum BackendEvent {
    /// One context became unavailable after a background credential refresh.
    /// Bootstrap-status subscribers use this to reconcile without waiting for
    /// an unrelated foreground request to observe the same failure.
    ContextUnavailable {
        /// Safe kubeconfig context name.
        context: String,
        /// Sanitized operator-facing failure reason.
        reason: String,
    },
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
    /// One background operation changed state.
    Operation(crate::operation::OperationEvent),
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
    /// Current credential availability.
    pub availability: ContextAvailability,
    /// Safe, bounded reason when the credential plugin is unavailable.
    pub unavailable_reason: Option<String>,
}

impl ContextInfo {
    /// Build an available context summary.
    #[must_use]
    pub fn available(
        name: impl Into<String>,
        cluster: impl Into<String>,
        namespace: Option<String>,
        is_current: bool,
    ) -> Self {
        Self {
            name: name.into(),
            cluster: cluster.into(),
            namespace,
            is_current,
            availability: ContextAvailability::Available,
            unavailable_reason: None,
        }
    }
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
    /// The context's exec credential helper failed. The reason is already
    /// sanitized and safe for operator-facing diagnostics.
    ContextUnavailable { context: String, reason: String },
    /// The operation was denied by authorization policy (RBAC).
    Forbidden,
    /// The request timed out.
    Timeout,
    /// The request was cancelled.
    Cancelled,
    /// An internal error occurred.
    Internal(String),
    /// A typed port-forward rejection with a stable category.
    PortForward {
        /// Stable category driving typed protocol failures.
        category: crate::port_forward::RejectionCategory,
        /// Short sanitized reason.
        message: String,
    },
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
            Self::ContextUnavailable { context, reason } => {
                write!(f, "context '{context}' is unavailable: {reason}")
            }
            Self::Forbidden => write!(f, "access denied"),
            Self::Timeout => write!(f, "request timed out"),
            Self::Cancelled => write!(f, "request was cancelled"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
            Self::PortForward { message, .. } => write!(f, "port forward rejected: {message}"),
        }
    }
}

impl std::error::Error for BackendError {}

/// Typed errors from constructing backend adapters at startup.
///
/// These are normalized away from Kubernetes client types so entry points can
/// report clear operator-facing failures. Messages deliberately never include
/// credential material (tokens, certificates, keys).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError {
    /// The kubeconfig file does not exist at this location.
    KubeconfigMissing(PathBuf),
    /// No kubeconfig was found through standard discovery: `KUBECONFIG` is
    /// unset (or empty) and no default `~/.kube/config` exists.
    KubeconfigNotConfigured,
    /// The kubeconfig exists but cannot be read, parsed, or validated; the
    /// detail names the problem without exposing file contents.
    KubeconfigInvalid { source: String, detail: String },
    /// Context summaries violated registry invariants (duplicate names or
    /// multiple current contexts), so nothing was committed.
    InvalidContextSummaries { detail: String },
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KubeconfigMissing(path) => {
                write!(f, "kubeconfig file not found at {}", path.display())
            }
            Self::KubeconfigNotConfigured => {
                f.write_str("no kubeconfig configured: set KUBECONFIG or create ~/.kube/config")
            }
            Self::KubeconfigInvalid { source, detail } => {
                write!(f, "invalid kubeconfig from {source}: {detail}")
            }
            Self::InvalidContextSummaries { detail } => {
                write!(f, "invalid context summaries: {detail}")
            }
        }
    }
}

impl std::error::Error for AdapterError {}

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

    /// Expose the adapter's port-forward seam when it supports forwarding;
    /// adapters without the capability return `None`.
    fn port_forward_connector(&self) -> Option<crate::port_forward::PortForwardConnector> {
        None
    }

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
