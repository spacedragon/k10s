//! Normalized resource view models for the k10s control protocol.
//!
//! Every payload in this module is a backend-owned normalized projection of a
//! Kubernetes object. Kubernetes client types (such as kube-rs) must never
//! leak into this crate; adapters translate before results cross the kernel.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Group, version, and kind of a resource type.
///
/// The group is empty for core (`v1`) API objects.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionKind {
    /// API group, empty for the core group.
    pub group: String,
    /// API version within the group.
    pub version: String,
    /// Plural-cased Kubernetes kind, such as `Deployment`.
    pub kind: String,
}

impl GroupVersionKind {
    /// Construct a core-group kind.
    #[must_use]
    pub fn core(version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            group: String::new(),
            version: version.into(),
            kind: kind.into(),
        }
    }
}

/// The workload kinds designed for dedicated windows and details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkloadKind {
    /// A declarative pod controller for stateless replicas.
    Deployment,
    /// The intermediate controller owned by a Deployment.
    ReplicaSet,
    /// A controller with stable network and storage identities.
    StatefulSet,
    /// A controller running one pod per eligible node.
    DaemonSet,
    /// A run-to-completion batch workload.
    Job,
    /// A time-scheduled Job template.
    CronJob,
    /// The smallest deployable unit.
    Pod,
}

impl WorkloadKind {
    /// Map a group/version/kind to a designed workload kind, if it is one.
    #[must_use]
    pub fn from_gvk(gvk: &GroupVersionKind) -> Option<Self> {
        match (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()) {
            ("apps", "v1", "Deployment") => Some(Self::Deployment),
            ("apps", "v1", "ReplicaSet") => Some(Self::ReplicaSet),
            ("apps", "v1", "StatefulSet") => Some(Self::StatefulSet),
            ("apps", "v1", "DaemonSet") => Some(Self::DaemonSet),
            ("batch", "v1", "Job") => Some(Self::Job),
            ("batch", "v1", "CronJob") => Some(Self::CronJob),
            ("", "v1", "Pod") => Some(Self::Pod),
            _ => None,
        }
    }
}

/// Whether an identity lives inside a namespace or at cluster scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceScope {
    /// The object exists inside a namespace.
    Namespaced,
    /// The object exists once per cluster.
    Cluster,
}

impl std::fmt::Display for ResourceScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Namespaced => formatter.write_str("namespaced"),
            Self::Cluster => formatter.write_str("cluster"),
        }
    }
}

/// Stable identity of one resource across requests and events.
///
/// Two identities with equal fields denote the same object on the same
/// context; the `uid` disambiguates recreations with identical names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceIdentity {
    /// Kubernetes context this object was read from.
    pub context: String,
    /// Type of the object.
    pub gvk: GroupVersionKind,
    /// Namespace, absent for cluster-scoped objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Object name.
    pub name: String,
    /// Immutable server-assigned identifier.
    pub uid: String,
}

impl ResourceIdentity {
    /// Return the scope implied by namespace presence.
    #[must_use]
    pub fn scope(&self) -> ResourceScope {
        if self.namespace.is_some() {
            ResourceScope::Namespaced
        } else {
            ResourceScope::Cluster
        }
    }

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

/// Monotonic backend revision assigned by the backend adapter.
///
/// Revisions strictly increase as backend state changes. Deltas carrying a
/// revision lower than or equal to the last applied revision are stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BackendRevision(u64);

impl BackendRevision {
    /// Construct a revision from its raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw revision value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for BackendRevision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// One normalized row of a resource list.
///
/// Kind-specific columns are projected into `summary` so that every list
/// window renders from the same shape. Structured kind-specific projections
/// are additionally exposed through the optional [`Self::projection`] field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceListRow {
    /// Stable identity of the row's object.
    pub identity: ResourceIdentity,
    /// Backend revision at which this row was last true.
    pub revision: BackendRevision,
    /// Object labels, sorted by key for deterministic wire order.
    pub labels: BTreeMap<String, String>,
    /// Human-readable status summary, such as `2/2 ready`.
    pub summary: String,
    /// Creation time formatted as RFC 3339.
    pub created_at: String,
    /// Kind-specific structured projection, added in protocol v1.2.
    ///
    /// Populated only for kinds with a designed projection; other kinds and
    /// legacy payloads decode as [`None`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<ResourceProjection>,
}

/// A kind-specific normalized projection shared by list rows and detail
/// responses.
///
/// Projections carry structured columns so windows never parse manifest
/// text or `summary`; they contain no raw Kubernetes objects and no
/// credential-bearing fields.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ResourceProjection {
    /// Normalized core/v1 Service view model.
    Service(ServiceProjection),
}

/// Normalized core/v1 Service projection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceProjection {
    /// Kubernetes Service type, such as `ClusterIP` or `NodePort`.
    pub service_type: String,
    /// Primary cluster IPs; empty for ExternalName Services.
    pub cluster_ips: Vec<String>,
    /// Selector labels, sorted by key for deterministic wire order.
    pub selector: BTreeMap<String, String>,
    /// External name of an ExternalName Service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_name: Option<String>,
    /// Session affinity policy when explicitly set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<String>,
    /// External traffic policy when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_traffic_policy: Option<String>,
    /// Internal traffic policy when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_traffic_policy: Option<String>,
    /// Every declared Service port, including non-forwardable ones.
    pub ports: Vec<ServicePort>,
}

/// One declared Service port.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    /// Declared port name, absent for unnamed ports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Declared Service port number.
    pub service_port: u16,
    /// Resolved target port on backing Pods.
    pub target_port: TargetPort,
    /// Node port declared by NodePort or LoadBalancer Services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<u16>,
    /// Wire protocol of the port.
    pub protocol: TransportProtocol,
    /// Optional application protocol label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
}

/// The target port of a Service port declaration.
///
/// An omitted Kubernetes `targetPort` normalizes to
/// [`TargetPort::Number`] carrying the Service port number, making the
/// defaulted case explicit on the wire instead of being reconstructed by
/// the UI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum TargetPort {
    /// A named target port.
    Name {
        /// Target port name as declared on backing Pods.
        name: String,
    },
    /// A numeric target port.
    Number {
        /// Target port number on backing Pods.
        number: u16,
    },
}

/// Wire transport protocol of a Service port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransportProtocol {
    /// TCP ports may be forwarded by the desktop application.
    Tcp,
    /// UDP ports render read-only and are never forwarded.
    Udp,
    /// SCTP ports render read-only and are never forwarded.
    Sctp,
}

/// A reference from a child object to its owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    /// Type of the owner.
    pub gvk: GroupVersionKind,
    /// Owner name.
    pub name: String,
    /// Owner UID matching [`ResourceIdentity::uid`] semantics.
    pub uid: String,
    /// Whether this owner is the managing controller.
    pub controller: bool,
}

/// One labeled row inside a detail section.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailRow {
    /// Stable row label, such as `Replicas`.
    pub label: String,
    /// Safe display value.
    pub value: String,
}

/// A titled group of detail rows rendered as one tab section.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailSection {
    /// Section title, such as `Overview`.
    pub title: String,
    /// Ordered rows within the section.
    pub rows: Vec<DetailRow>,
}

/// One deterministic event row of a resource's Events tab.
///
/// Events are resolved by the backend from the object's observed state; the
/// UI never synthesizes them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRow {
    /// Short Kubernetes-style event reason, such as `Started`.
    pub reason: String,
    /// Human-readable event message.
    pub message: String,
    /// How many times this event repeated.
    pub count: u32,
    /// Last occurrence formatted as RFC 3339.
    pub last_seen: String,
}

/// One backend-resolved group of related resources.
///
/// Rows are full normalized list rows so related tabs render through the
/// same projection as list windows; the traversal itself never crosses the
/// protocol boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelatedGroup {
    /// Group title shown as the section heading, such as `Pods`.
    pub title: String,
    /// Type every row in this group shares.
    pub gvk: GroupVersionKind,
    /// Sorted normalized rows of the group.
    pub rows: Vec<ResourceListRow>,
}

/// Server-asserted capabilities for one resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceCapabilities {
    /// YAML editing and apply flows may target this kind.
    pub can_edit_yaml: bool,
    /// Delete dialogs may target this kind.
    pub can_delete: bool,
    /// Scale actions are meaningful for this kind.
    pub can_scale: bool,
    /// Log tailing is available for this kind.
    pub can_view_logs: bool,
    /// Exec sessions are available for this kind.
    pub can_exec: bool,
}

impl Default for ResourceCapabilities {
    fn default() -> Self {
        Self {
            can_edit_yaml: true,
            can_delete: true,
            can_scale: false,
            can_view_logs: false,
            can_exec: false,
        }
    }
}

/// Payload describing a resource list query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceListRequest {
    /// Context to read from.
    pub context: String,
    /// Type of resources to list.
    pub gvk: GroupVersionKind,
    /// Optional namespace restriction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Identity of a single resource used by detail and metrics queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRefRequest {
    /// Full identity of the target object.
    pub identity: ResourceIdentity,
}

/// Response payload for a normalized resource list query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceListResponse {
    /// Context the rows were read from.
    pub context: String,
    /// Type of the listed resources.
    pub gvk: GroupVersionKind,
    /// Namespace restriction echoed back, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Backend revision covering the whole snapshot.
    pub revision: BackendRevision,
    /// Sorted normalized rows.
    pub rows: Vec<ResourceListRow>,
    /// Deterministic generation timestamp formatted as RFC 3339.
    pub generated_at: String,
    /// Capabilities asserted for this kind.
    pub capabilities: ResourceCapabilities,
}

/// Response payload for a single-resource detail query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDetailResponse {
    /// Stable identity of the detailed object.
    pub identity: ResourceIdentity,
    /// Backend revision at which the details were true.
    pub revision: BackendRevision,
    /// Creation time formatted as RFC 3339.
    pub created_at: String,
    /// Owner chain references resolved by the backend.
    pub owner_references: Vec<OwnerReference>,
    /// Ordered detail sections.
    pub sections: Vec<DetailSection>,
    /// Deterministic backend-resolved events for this object.
    pub events: Vec<EventRow>,
    /// Related rows resolved by backend owner traversal, grouped by type.
    pub related: Vec<RelatedGroup>,
    /// Capabilities asserted for this kind.
    pub capabilities: ResourceCapabilities,
    /// The current server-side YAML manifest of the object, rendered
    /// read-only and used as the base of guarded edits. Authored entirely by
    /// the backend; clients never synthesize it.
    pub manifest: String,
    /// Kind-specific structured projection, added in protocol v1.2.
    ///
    /// Populated only for kinds with a designed projection; other kinds and
    /// legacy payloads decode as [`None`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<ResourceProjection>,
}

/// Whether a workload-health bucket is healthy, warning, or failing.
///
/// UI code always renders this level together with [`WorkloadHealth::label`]
/// and never relies on the associated color alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HealthLevel {
    /// The resources in this bucket are healthy.
    Healthy,
    /// The resources need observation, for example while pending.
    Warning,
    /// The resources are unhealthy.
    Failure,
}

/// One explicitly labelled workload-health total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadHealth {
    /// Semantic level used for the status dot.
    pub level: HealthLevel,
    /// Human-readable status text shown beside the dot.
    pub label: String,
    /// Number of workloads in this state.
    pub count: u32,
}

/// One short Overview row for a pending or unhealthy resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttentionRow {
    /// Namespace, absent for a cluster-scoped resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Kubernetes kind.
    pub kind: String,
    /// Resource name.
    pub name: String,
    /// Short status such as `Pending` or `Degraded`.
    pub status: String,
    /// Short reason explaining why the row needs attention.
    pub reason: String,
}

/// One row of the Nodes inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRow {
    /// Node name.
    pub name: String,
    /// Ready state rendered as text.
    pub status: String,
    /// Node roles in deterministic display order.
    pub roles: Vec<String>,
    /// Kubernetes version reported by the node.
    pub kubernetes_version: String,
    /// CPU usage and capacity in millicores.
    pub cpu: crate::metrics::CapacityUsage,
    /// Memory usage and capacity in bytes.
    pub memory: crate::metrics::CapacityUsage,
    /// Scheduled pods and allocatable pod count.
    pub pods: crate::metrics::CapacityUsage,
    /// Deterministic backend-formatted age.
    pub age: String,
}

/// One PersistentVolumeClaim inventory row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaimRow {
    /// Claim namespace.
    pub namespace: String,
    /// Claim name.
    pub name: String,
    /// Claim phase.
    pub status: String,
    /// Requested or bound capacity formatted for display.
    pub capacity: String,
    /// Kubernetes access modes.
    pub access_modes: Vec<String>,
    /// StorageClass name.
    pub storage_class: String,
    /// Bound PersistentVolume name, or `—` when unbound.
    pub bound_volume: String,
    /// Deterministic backend-formatted age.
    pub age: String,
}

/// One PersistentVolume inventory row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeRow {
    /// Volume name.
    pub name: String,
    /// Volume phase.
    pub status: String,
    /// Capacity formatted for display.
    pub capacity: String,
    /// Kubernetes access modes.
    pub access_modes: Vec<String>,
    /// StorageClass name.
    pub storage_class: String,
    /// Bound claim formatted as `namespace/name`, or `—`.
    pub bound_claim: String,
    /// Reclaim policy.
    pub reclaim_policy: String,
    /// Deterministic backend-formatted age.
    pub age: String,
}

/// One StorageClass inventory row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageClassRow {
    /// StorageClass name.
    pub name: String,
    /// CSI or in-tree provisioner.
    pub provisioner: String,
    /// Default reclaim policy.
    pub reclaim_policy: String,
    /// Volume binding mode.
    pub volume_binding_mode: String,
    /// Deterministic backend-formatted age.
    pub age: String,
}

/// Storage rows grouped by the three selectable UI tabs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageInventory {
    /// PersistentVolumeClaims.
    pub persistent_volume_claims: Vec<PersistentVolumeClaimRow>,
    /// PersistentVolumes.
    pub persistent_volumes: Vec<PersistentVolumeRow>,
    /// StorageClasses.
    pub storage_classes: Vec<StorageClassRow>,
}
