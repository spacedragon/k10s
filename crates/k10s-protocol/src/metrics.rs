//! Normalized metrics payloads for the k10s control protocol.
//!
//! Metrics are availability-gated: a value is present only when the backend
//! actually collected it, and UI code must never render a missing metric as
//! zero.

use serde::{Deserialize, Serialize};

use crate::resource::{
    AttentionRow, BackendRevision, NodeRow, ResourceIdentity, StorageInventory, WorkloadHealth,
};

/// Event kind for a coalesced infrastructure telemetry update.
pub const INFRASTRUCTURE_EVENT_UPDATED: &str = "infrastructure.updated";

/// Availability of a metrics sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricsAvailability {
    /// All designed values were collected.
    Available,
    /// Some values were collected; missing values stay absent.
    Partial,
    /// No fresh values exist for the object.
    Unavailable,
}

impl std::fmt::Display for MetricsAvailability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Available => formatter.write_str("Available"),
            Self::Partial => formatter.write_str("Partial"),
            Self::Unavailable => formatter.write_str("Unavailable"),
        }
    }
}

/// Why metrics have their current availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricsCondition {
    /// All requested samples are within the freshness window.
    Fresh,
    /// The source returned only part of the requested sample.
    Partial,
    /// Kubernetes RBAC denied the metrics read.
    Forbidden,
    /// The newest known sample is outside the freshness window.
    Stale,
}

/// An optional used/capacity pair.
///
/// Values remain absent on the wire when they were not collected. Consumers
/// must render absence as `—`, never as zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapacityUsage {
    /// Current usage, absent when not reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used: Option<u64>,
    /// Capacity, absent when not reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<u64>,
}

impl CapacityUsage {
    /// Construct an explicitly availability-gated usage pair.
    #[must_use]
    pub const fn new(used: Option<u64>, capacity: Option<u64>) -> Self {
        Self { used, capacity }
    }
}

/// Cluster-wide totals displayed on Overview.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTotals {
    /// Total Nodes.
    pub nodes: u32,
    /// Total Pods.
    pub pods: u32,
    /// Total first-class workload controllers.
    pub workloads: u32,
    /// Persistent storage capacity in bytes.
    pub persistent_storage_bytes: u64,
}

/// Backend-owned counts displayed beside launcher resource groups.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LauncherCounts {
    pub events_warning: u32,
    pub workloads: u32,
    pub network: u32,
    pub config: u32,
    pub storage: u32,
    pub access: u32,
}

/// Availability, provenance, and freshness of the infrastructure metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsStatus {
    /// Explicit availability tri-state.
    pub availability: MetricsAvailability,
    /// Reason for the availability state.
    pub condition: MetricsCondition,
    /// Safe source name, for example `metrics.k8s.io`.
    pub source: String,
    /// Timestamp reported by the metrics source, if one was available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_updated_at: Option<String>,
    /// Textual explanation; status is never color-only.
    pub detail: String,
}

/// Query payload for the complete Overview, Nodes, and Storage projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureRequest {
    /// Kubernetes context to project.
    pub context: String,
}

/// Subscription selector for coalesced infrastructure telemetry updates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureWatchSpec {
    /// Kubernetes context to watch.
    pub context: String,
}

impl InfrastructureWatchSpec {
    /// Construct the canonical selector.
    #[must_use]
    pub fn new(context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
        }
    }
}

/// Complete backend-owned projection for the infrastructure windows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfrastructureResponse {
    /// Context the projection belongs to.
    pub context: String,
    /// Monotonic backend revision.
    pub revision: BackendRevision,
    /// Last successful refresh time.
    pub generated_at: String,
    /// Overview totals.
    pub totals: ClusterTotals,
    /// Counts and warning badges for the resource launcher.
    #[serde(default)]
    pub launcher: LauncherCounts,
    /// Aggregate CPU usage/capacity in millicores.
    pub cluster_cpu: CapacityUsage,
    /// Aggregate memory usage/capacity in bytes.
    pub cluster_memory: CapacityUsage,
    /// Scheduled/allocatable pod capacity.
    pub pod_capacity: CapacityUsage,
    /// Explicit metrics state and timestamps.
    pub metrics: MetricsStatus,
    /// Workload-health buckets.
    pub workload_health: Vec<WorkloadHealth>,
    /// Short unhealthy/pending table.
    pub attention: Vec<AttentionRow>,
    /// Node inventory.
    pub nodes: Vec<NodeRow>,
    /// Storage inventory grouped by selectable tab.
    pub storage: StorageInventory,
}

/// A normalized metrics sample for one pod.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodMetrics {
    /// Whether and how completely this sample was collected.
    pub availability: MetricsAvailability,
    /// CPU usage in millicores, absent when not collected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millicores: Option<u64>,
    /// Working-set memory in bytes, absent when not collected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// Deterministic collection timestamp formatted as RFC 3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collected_at: Option<String>,
}

impl PodMetrics {
    /// A sample for which nothing could be collected.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            availability: MetricsAvailability::Unavailable,
            cpu_millicores: None,
            memory_bytes: None,
            collected_at: None,
        }
    }
}

/// One availability-gated metrics sample keyed by exact container name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerMetrics {
    /// Exact container name reported by the metrics API.
    pub name: String,
    /// Availability-gated CPU and memory sample for this container.
    pub metrics: PodMetrics,
}

/// Response payload for a single-pod metrics query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetricsResponse {
    /// Identity of the sampled pod.
    pub identity: ResourceIdentity,
    /// Availability-gated metrics sample.
    pub metrics: PodMetrics,
    /// Per-container samples preserved under their exact reported names.
    #[serde(default)]
    pub containers: Vec<ContainerMetrics>,
}
