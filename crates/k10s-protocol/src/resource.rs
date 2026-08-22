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
/// window renders from the same shape.
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
    /// Capabilities asserted for this kind.
    pub capabilities: ResourceCapabilities,
}
