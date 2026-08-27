//! Backend-owned infrastructure catalog and protocol projection.
//!
//! Kubernetes adapters return this target-neutral catalog through the
//! behavior port. The kernel maps it to wire types here; UI code never reads
//! adapter fixtures or Kubernetes objects directly.

use k10s_protocol::{
    AttentionRow, BackendRevision, CapacityUsage, ClusterTotals, HealthLevel,
    InfrastructureResponse, MetricsAvailability, MetricsCondition, MetricsStatus, NodeRow,
    PersistentVolumeClaimRow, PersistentVolumeRow, StorageClassRow, StorageInventory,
    WorkloadHealth,
};

const GIB: u64 = 1_073_741_824;

/// Deterministic metrics behavior exposed by an adapter catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMetricsScenario {
    /// Every designed value is current.
    Full,
    /// At least one designed value is missing.
    Partial,
    /// RBAC denied metrics collection.
    Forbidden,
    /// The last sample is outside the freshness window.
    Stale,
}

/// Backend-owned infrastructure projection before wire mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    context: String,
    revision: u64,
    generated_at: String,
    totals: CatalogTotals,
    cluster_cpu: CatalogUsage,
    cluster_memory: CatalogUsage,
    pod_capacity: CatalogUsage,
    metrics: CatalogMetrics,
    workload_health: Vec<CatalogHealth>,
    attention: Vec<CatalogAttention>,
    nodes: Vec<CatalogNode>,
    storage: CatalogStorage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatalogTotals {
    nodes: u32,
    pods: u32,
    workloads: u32,
    persistent_storage_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatalogUsage {
    used: Option<u64>,
    capacity: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogMetrics {
    availability: MetricsAvailability,
    condition: MetricsCondition,
    source: String,
    source_updated_at: Option<String>,
    detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogHealth {
    level: HealthLevel,
    label: String,
    count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogAttention {
    namespace: Option<String>,
    kind: String,
    name: String,
    status: String,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogNode {
    name: String,
    status: String,
    roles: Vec<String>,
    kubernetes_version: String,
    cpu: CatalogUsage,
    memory: CatalogUsage,
    pods: CatalogUsage,
    age: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CatalogStorage {
    persistent_volume_claims: Vec<CatalogPvc>,
    persistent_volumes: Vec<CatalogPv>,
    storage_classes: Vec<CatalogStorageClass>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogPvc {
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) capacity: String,
    pub(crate) access_modes: Vec<String>,
    pub(crate) storage_class: String,
    pub(crate) bound_volume: String,
    pub(crate) age: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogPv {
    pub(crate) name: String,
    pub(crate) status: String,
    pub(crate) capacity: String,
    pub(crate) access_modes: Vec<String>,
    pub(crate) storage_class: String,
    pub(crate) bound_claim: String,
    pub(crate) reclaim_policy: String,
    pub(crate) age: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogStorageClass {
    pub(crate) name: String,
    pub(crate) provisioner: String,
    pub(crate) reclaim_policy: String,
    pub(crate) volume_binding_mode: String,
    pub(crate) age: String,
}

impl CatalogSnapshot {
    /// Build a live storage projection when the adapter has no metrics sample.
    pub(crate) fn live_storage(
        context: String,
        revision: u64,
        generated_at: String,
        persistent_volume_claims: Vec<CatalogPvc>,
        persistent_volumes: Vec<CatalogPv>,
        storage_classes: Vec<CatalogStorageClass>,
    ) -> Self {
        let persistent_storage_bytes = persistent_volume_claims
            .iter()
            .filter_map(|claim| quantity_bytes(&claim.capacity))
            .sum();
        Self {
            context,
            revision,
            generated_at,
            totals: CatalogTotals {
                nodes: 0,
                pods: 0,
                workloads: 0,
                persistent_storage_bytes,
            },
            cluster_cpu: CatalogUsage {
                used: None,
                capacity: None,
            },
            cluster_memory: CatalogUsage {
                used: None,
                capacity: None,
            },
            pod_capacity: CatalogUsage {
                used: None,
                capacity: None,
            },
            metrics: CatalogMetrics {
                availability: MetricsAvailability::Unavailable,
                condition: MetricsCondition::Partial,
                source: "kubernetes-api".into(),
                source_updated_at: None,
                detail: "Metrics are not collected for this snapshot".into(),
            },
            workload_health: Vec::new(),
            attention: Vec::new(),
            nodes: Vec::new(),
            storage: CatalogStorage {
                persistent_volume_claims,
                persistent_volumes,
                storage_classes,
            },
        }
    }
    /// Build the deterministic fake catalog. Scenario selection remains in
    /// the fake adapter; this module owns only normalized projection data.
    #[must_use]
    pub(crate) fn fake(
        context: impl Into<String>,
        revision: u64,
        scenario: CatalogMetricsScenario,
    ) -> Self {
        let context = context.into();
        if context == "prod-readonly" {
            return Self::fake_prod(context, revision, scenario);
        }
        let (cluster_cpu, cluster_memory, pod_capacity, metrics) = metrics(scenario);
        Self {
            context,
            revision,
            generated_at: "2026-08-21T01:05:00Z".into(),
            totals: CatalogTotals {
                nodes: 2,
                pods: 22,
                workloads: 6,
                persistent_storage_bytes: 60 * GIB,
            },
            cluster_cpu,
            cluster_memory,
            pod_capacity,
            metrics,
            workload_health: vec![
                CatalogHealth {
                    level: HealthLevel::Healthy,
                    label: "Healthy".into(),
                    count: 4,
                },
                CatalogHealth {
                    level: HealthLevel::Warning,
                    label: "Pending".into(),
                    count: 1,
                },
                CatalogHealth {
                    level: HealthLevel::Failure,
                    label: "Unhealthy".into(),
                    count: 1,
                },
            ],
            attention: vec![CatalogAttention {
                namespace: Some("default".into()),
                kind: "Deployment".into(),
                name: "checkout".into(),
                status: "Degraded".into(),
                reason: "1 replica unavailable".into(),
            }],
            nodes: vec![
                CatalogNode {
                    name: "dev-node-1".into(),
                    status: "Ready".into(),
                    roles: vec!["control-plane".into()],
                    kubernetes_version: "v1.34.0".into(),
                    cpu: node_usage(scenario, 2_200, 4_000, false),
                    memory: node_usage(scenario, 8 * GIB, 16 * GIB, false),
                    pods: node_usage(scenario, 12, 110, false),
                    age: "14d".into(),
                },
                CatalogNode {
                    name: "dev-node-2".into(),
                    status: "Not Ready".into(),
                    roles: vec!["worker".into()],
                    kubernetes_version: "v1.34.0".into(),
                    cpu: node_usage(scenario, 1_000, 4_000, false),
                    memory: node_usage(scenario, 4 * GIB, 16 * GIB, true),
                    pods: node_usage(scenario, 10, 110, false),
                    age: "8d".into(),
                },
            ],
            storage: CatalogStorage {
                persistent_volume_claims: vec![CatalogPvc {
                    namespace: "default".into(),
                    name: "postgres-data".into(),
                    status: "Bound".into(),
                    capacity: "20 GiB".into(),
                    access_modes: vec!["ReadWriteOnce".into()],
                    storage_class: "fast-ssd".into(),
                    bound_volume: "pv-postgres-data".into(),
                    age: "12d".into(),
                }],
                persistent_volumes: vec![CatalogPv {
                    name: "pv-postgres-data".into(),
                    status: "Bound".into(),
                    capacity: "20 GiB".into(),
                    access_modes: vec!["ReadWriteOnce".into()],
                    storage_class: "fast-ssd".into(),
                    bound_claim: "default/postgres-data".into(),
                    reclaim_policy: "Retain".into(),
                    age: "12d".into(),
                }],
                storage_classes: vec![CatalogStorageClass {
                    name: "fast-ssd".into(),
                    provisioner: "csi.example.com".into(),
                    reclaim_policy: "Delete".into(),
                    volume_binding_mode: "WaitForFirstConsumer".into(),
                    age: "90d".into(),
                }],
            },
        }
    }

    fn fake_prod(context: String, revision: u64, scenario: CatalogMetricsScenario) -> Self {
        let (cluster_cpu, cluster_memory, pod_capacity, metrics) = metrics(scenario);
        Self {
            context,
            revision,
            generated_at: "2026-08-21T01:05:00Z".into(),
            totals: CatalogTotals {
                nodes: 1,
                pods: 3,
                workloads: 1,
                persistent_storage_bytes: 0,
            },
            cluster_cpu,
            cluster_memory,
            pod_capacity,
            metrics,
            workload_health: vec![CatalogHealth {
                level: HealthLevel::Healthy,
                label: "Healthy".into(),
                count: 1,
            }],
            attention: Vec::new(),
            nodes: vec![CatalogNode {
                name: "prod-node-1".into(),
                status: "Ready".into(),
                roles: vec!["worker".into()],
                kubernetes_version: "v1.34.0".into(),
                cpu: cluster_cpu,
                memory: cluster_memory,
                pods: pod_capacity,
                age: "30d".into(),
            }],
            storage: CatalogStorage::default(),
        }
    }

    /// Context used as the P2 coalescing key.
    #[must_use]
    pub fn context(&self) -> &str {
        &self.context
    }

    /// Monotonic revision carried by a telemetry event.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Map the backend-owned catalog to the normalized wire response.
    #[must_use]
    pub fn into_protocol(self) -> InfrastructureResponse {
        InfrastructureResponse {
            context: self.context,
            revision: BackendRevision::new(self.revision),
            generated_at: self.generated_at,
            totals: ClusterTotals {
                nodes: self.totals.nodes,
                pods: self.totals.pods,
                workloads: self.totals.workloads,
                persistent_storage_bytes: self.totals.persistent_storage_bytes,
            },
            cluster_cpu: usage(self.cluster_cpu),
            cluster_memory: usage(self.cluster_memory),
            pod_capacity: usage(self.pod_capacity),
            metrics: MetricsStatus {
                availability: self.metrics.availability,
                condition: self.metrics.condition,
                source: self.metrics.source,
                source_updated_at: self.metrics.source_updated_at,
                detail: self.metrics.detail,
            },
            workload_health: self
                .workload_health
                .into_iter()
                .map(|health| WorkloadHealth {
                    level: health.level,
                    label: health.label,
                    count: health.count,
                })
                .collect(),
            attention: self
                .attention
                .into_iter()
                .map(|row| AttentionRow {
                    namespace: row.namespace,
                    kind: row.kind,
                    name: row.name,
                    status: row.status,
                    reason: row.reason,
                })
                .collect(),
            nodes: self
                .nodes
                .into_iter()
                .map(|node| NodeRow {
                    name: node.name,
                    status: node.status,
                    roles: node.roles,
                    kubernetes_version: node.kubernetes_version,
                    cpu: usage(node.cpu),
                    memory: usage(node.memory),
                    pods: usage(node.pods),
                    age: node.age,
                })
                .collect(),
            storage: StorageInventory {
                persistent_volume_claims: self
                    .storage
                    .persistent_volume_claims
                    .into_iter()
                    .map(|row| PersistentVolumeClaimRow {
                        namespace: row.namespace,
                        name: row.name,
                        status: row.status,
                        capacity: row.capacity,
                        access_modes: row.access_modes,
                        storage_class: row.storage_class,
                        bound_volume: row.bound_volume,
                        age: row.age,
                    })
                    .collect(),
                persistent_volumes: self
                    .storage
                    .persistent_volumes
                    .into_iter()
                    .map(|row| PersistentVolumeRow {
                        name: row.name,
                        status: row.status,
                        capacity: row.capacity,
                        access_modes: row.access_modes,
                        storage_class: row.storage_class,
                        bound_claim: row.bound_claim,
                        reclaim_policy: row.reclaim_policy,
                        age: row.age,
                    })
                    .collect(),
                storage_classes: self
                    .storage
                    .storage_classes
                    .into_iter()
                    .map(|row| StorageClassRow {
                        name: row.name,
                        provisioner: row.provisioner,
                        reclaim_policy: row.reclaim_policy,
                        volume_binding_mode: row.volume_binding_mode,
                        age: row.age,
                    })
                    .collect(),
            },
        }
    }
}

fn quantity_bytes(raw: &str) -> Option<u64> {
    const SUFFIXES: [(&str, u64); 6] = [
        ("Ei", 1 << 60),
        ("Pi", 1 << 50),
        ("Ti", 1 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ];
    for (suffix, multiplier) in SUFFIXES {
        if let Some(value) = raw.strip_suffix(suffix) {
            return value.parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    raw.parse().ok()
}

fn usage(value: CatalogUsage) -> CapacityUsage {
    CapacityUsage::new(value.used, value.capacity)
}

fn node_usage(
    scenario: CatalogMetricsScenario,
    used: u64,
    capacity: u64,
    missing_in_partial: bool,
) -> CatalogUsage {
    match scenario {
        CatalogMetricsScenario::Full => CatalogUsage {
            used: Some(used),
            capacity: Some(capacity),
        },
        CatalogMetricsScenario::Partial if !missing_in_partial => CatalogUsage {
            used: Some(used),
            capacity: Some(capacity),
        },
        CatalogMetricsScenario::Partial
        | CatalogMetricsScenario::Forbidden
        | CatalogMetricsScenario::Stale => CatalogUsage {
            used: None,
            capacity: Some(capacity),
        },
    }
}

fn metrics(
    scenario: CatalogMetricsScenario,
) -> (CatalogUsage, CatalogUsage, CatalogUsage, CatalogMetrics) {
    let cpu = CatalogUsage {
        used: Some(3_200),
        capacity: Some(8_000),
    };
    let memory = CatalogUsage {
        used: Some(12 * GIB),
        capacity: Some(32 * GIB),
    };
    let pods = CatalogUsage {
        used: Some(22),
        capacity: Some(220),
    };
    match scenario {
        CatalogMetricsScenario::Full => (
            cpu,
            memory,
            pods,
            CatalogMetrics {
                availability: MetricsAvailability::Available,
                condition: MetricsCondition::Fresh,
                source: "metrics.k8s.io".into(),
                source_updated_at: Some("2026-08-21T01:04:30Z".into()),
                detail: "All node metrics are current".into(),
            },
        ),
        CatalogMetricsScenario::Partial => (
            cpu,
            CatalogUsage {
                used: None,
                capacity: memory.capacity,
            },
            pods,
            CatalogMetrics {
                availability: MetricsAvailability::Partial,
                condition: MetricsCondition::Partial,
                source: "metrics.k8s.io".into(),
                source_updated_at: Some("2026-08-21T01:04:30Z".into()),
                detail: "Memory is missing for dev-node-2".into(),
            },
        ),
        CatalogMetricsScenario::Forbidden => (
            CatalogUsage {
                used: None,
                capacity: cpu.capacity,
            },
            CatalogUsage {
                used: None,
                capacity: memory.capacity,
            },
            CatalogUsage {
                used: None,
                capacity: pods.capacity,
            },
            CatalogMetrics {
                availability: MetricsAvailability::Unavailable,
                condition: MetricsCondition::Forbidden,
                source: "metrics.k8s.io".into(),
                source_updated_at: None,
                detail: "Forbidden: cannot list nodes.metrics.k8s.io".into(),
            },
        ),
        CatalogMetricsScenario::Stale => (
            CatalogUsage {
                used: None,
                capacity: cpu.capacity,
            },
            CatalogUsage {
                used: None,
                capacity: memory.capacity,
            },
            CatalogUsage {
                used: None,
                capacity: pods.capacity,
            },
            CatalogMetrics {
                availability: MetricsAvailability::Unavailable,
                condition: MetricsCondition::Stale,
                source: "metrics.k8s.io".into(),
                source_updated_at: Some("2026-08-21T00:30:00Z".into()),
                detail: "Last sample is outside the freshness window".into(),
            },
        ),
    }
}
