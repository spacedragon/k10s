//! Backend kernel: the sole protocol-facing interface.
//!
//! Owns all Kubernetes-facing product behavior. Maps to normalized protocol
//! payloads and enforces deadlines/cancellation. Fake data never escapes as
//! fixture types.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k10s_protocol::{
    BackendRevision, BootstrapResponse, Context, DetailRow, DetailSection, GroupVersionKind,
    MetricsAvailability, PodMetrics, ProtocolVersion, ResourceCapabilities, ResourceDetailResponse,
    ResourceIdentity, ResourceListResponse, ResourceListRow, ResourceMetricsResponse, ServerInfo,
    WorkloadKind,
};
use uuid::Uuid;

use crate::port::{
    BackendError, BootstrapInfo, Command, ContextInfo, Gvk, KubernetesAccess, MetricsSample,
    OperationId, Query, QueryResult, ResourceRecord, ResourceRef, Subscribe, SubscriptionHandle,
};

/// The backend kernel.
///
/// The sole protocol-facing interface. Owns mapping to normalized protocol
/// payloads and enforces deadlines/cancellation.
#[derive(Debug)]
pub struct BackendKernel {
    adapter: Arc<dyn KubernetesAccess>,
    server_instance_id: String,
}

impl BackendKernel {
    /// Create a new backend kernel with the given adapter.
    #[must_use]
    pub fn new(adapter: impl KubernetesAccess + 'static) -> Self {
        Self::new_with_instance_id(adapter, Uuid::new_v4().to_string())
    }

    /// Create a new backend kernel with a deterministic instance ID.
    ///
    /// Intended for tests and other contexts that need a stable identity.
    #[must_use]
    pub fn new_with_instance_id(
        adapter: impl KubernetesAccess + 'static,
        instance_id: impl Into<String>,
    ) -> Self {
        Self {
            adapter: Arc::new(adapter),
            server_instance_id: instance_id.into(),
        }
    }

    /// Return the server instance ID.
    #[must_use]
    pub fn server_instance_id(&self) -> &str {
        &self.server_instance_id
    }

    /// Execute a behavior-level query.
    ///
    /// Returns a protocol-facing result with normalized payloads.
    pub async fn query(&self, req: Query) -> Result<KernelQueryResult, BackendError> {
        self.query_with_deadline(req, None).await
    }

    /// Execute a behavior-level query with an optional deadline.
    ///
    /// If the deadline elapses before the adapter responds, the query is
    /// cancelled and a [`BackendError::Timeout`] is returned.
    pub async fn query_with_deadline(
        &self,
        req: Query,
        deadline: Option<Duration>,
    ) -> Result<KernelQueryResult, BackendError> {
        let metrics_reference = match &req {
            Query::ResourceMetrics { reference } => Some(reference.clone()),
            _ => None,
        };
        let fut = self.adapter.query(req);
        let result = match deadline {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| BackendError::Timeout)?,
            None => fut.await,
        };
        Ok(match result? {
            QueryResult::Bootstrap(info) => KernelQueryResult::Bootstrap(BootstrapResult::new(
                info,
                self.server_instance_id.clone(),
            )),
            QueryResult::ResourceList(data) => {
                KernelQueryResult::ResourceList(ResourceListResult::new(data))
            }
            QueryResult::ResourceDetail(record) => {
                KernelQueryResult::ResourceDetail(ResourceDetailResult::new(record))
            }
            QueryResult::ResourceMetrics(sample) => {
                KernelQueryResult::ResourceMetrics(ResourceMetricsResult::new(
                    metrics_reference
                        .as_ref()
                        .expect("resource metrics queries carry a reference"),
                    sample,
                ))
            }
        })
    }

    /// Execute a behavior-level command (mutation).
    ///
    /// Always returns an `OperationId` when supported.
    pub async fn execute(&self, cmd: Command) -> Result<OperationId, BackendError> {
        self.execute_with_deadline(cmd, None).await
    }

    /// Execute a behavior-level command with an optional deadline.
    ///
    /// If the deadline elapses before the adapter responds, the command is
    /// cancelled and a [`BackendError::Timeout`] is returned.
    pub async fn execute_with_deadline(
        &self,
        cmd: Command,
        deadline: Option<Duration>,
    ) -> Result<OperationId, BackendError> {
        let fut = self.adapter.execute(cmd);
        match deadline {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| BackendError::Timeout)?,
            None => fut.await,
        }
    }

    /// Open a behavior-level subscription.
    ///
    /// Subscriptions are long-lived; deadlines do not apply.
    pub async fn subscribe(&self, req: Subscribe) -> Result<SubscriptionHandle, BackendError> {
        self.adapter.subscribe(req).await
    }

    /// Map a snapshot slice into a normalized protocol page.
    #[must_use]
    pub fn snapshot_page(
        &self,
        revision: u64,
        rows: &[crate::port::ResourceRecord],
    ) -> k10s_protocol::ResourceSnapshotPage {
        k10s_protocol::ResourceSnapshotPage {
            revision: BackendRevision::new(revision),
            rows: rows.iter().map(map_row).collect(),
        }
    }

    /// Map a changed record into its normalized protocol delta.
    #[must_use]
    pub fn changed_delta(&self, record: &ResourceRecord) -> k10s_protocol::ResourceChanged {
        k10s_protocol::ResourceChanged {
            identity: map_identity(&record.reference),
            row: map_row(record),
        }
    }

    /// Map a removed reference into its normalized protocol delta.
    #[must_use]
    pub fn gone_delta(
        &self,
        reference: &ResourceRef,
        revision: u64,
    ) -> k10s_protocol::ResourceGone {
        k10s_protocol::ResourceGone {
            identity: map_identity(reference),
            revision: BackendRevision::new(revision),
        }
    }
}

/// Result of a kernel query.
#[derive(Debug, Clone)]
pub enum KernelQueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapResult),
    /// Normalized resource list result.
    ResourceList(ResourceListResult),
    /// Normalized single-resource detail result.
    ResourceDetail(ResourceDetailResult),
    /// Availability-gated pod metrics result.
    ResourceMetrics(ResourceMetricsResult),
}

impl KernelQueryResult {
    /// Return the context names for bootstrap results.
    #[must_use]
    pub fn context_names(&self) -> Vec<&str> {
        match self {
            Self::Bootstrap(b) => b.context_names(),
            _ => Vec::new(),
        }
    }

    /// Serialize the result to a JSON string.
    ///
    /// Returns the wire payload exactly as the server would place it in a
    /// `response` frame.
    #[must_use]
    pub fn serialized(&self) -> String {
        match self {
            Self::Bootstrap(b) => b.serialized(),
            Self::ResourceList(r) => r.serialized(),
            Self::ResourceDetail(r) => r.serialized(),
            Self::ResourceMetrics(r) => r.serialized(),
        }
    }

    /// Return the wire payload for bootstrap results.
    ///
    /// This is the exact [`BootstrapResponse`] payload the server sends in a
    /// `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> BootstrapResponse {
        match self {
            Self::Bootstrap(b) => b.wire_payload(),
            _ => panic!("wire_payload is only available for bootstrap results"),
        }
    }
}

/// Bootstrap result with protocol metadata and context information.
#[derive(Debug, Clone)]
pub struct BootstrapResult {
    /// The negotiated protocol version and capabilities.
    protocol: BootstrapResponse,
    /// Safe context metadata exposed to the UI.
    contexts: Vec<ContextInfo>,
}

impl BootstrapResult {
    /// Create a new bootstrap result.
    #[must_use]
    pub fn new(info: BootstrapInfo, server_instance_id: String) -> Self {
        Self {
            protocol: BootstrapResponse {
                protocol: ProtocolVersion { major: 1, minor: 1 },
                capabilities: vec!["logs.tail".into(), "exec.attach".into()],
                server: Some(ServerInfo {
                    instance_id: server_instance_id,
                    version: "0.1.0".into(),
                }),
                contexts: Vec::new(),
            },
            contexts: info.contexts,
        }
    }

    /// Return the context names.
    #[must_use]
    pub fn context_names(&self) -> Vec<&str> {
        self.contexts.iter().map(|c| c.name.as_str()).collect()
    }

    /// Return the wire payload: the exact `BootstrapResponse` the server
    /// sends in a `response` frame, including safe context metadata.
    #[must_use]
    pub fn wire_payload(&self) -> BootstrapResponse {
        let mut payload = self.protocol.clone();
        payload.contexts = self
            .contexts
            .iter()
            .map(|c| Context {
                name: c.name.clone(),
                cluster: c.cluster.clone(),
                namespace: c.namespace.clone(),
                is_current: c.is_current,
            })
            .collect();
        payload
    }

    /// Serialize the wire payload to a JSON string.
    ///
    /// Never includes credentials or tokens.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.wire_payload()).expect("BootstrapResponse must serialize")
    }
}

/// Normalized resource list mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceListResult {
    payload: ResourceListResponse,
}

impl ResourceListResult {
    /// Map backend-owned list data into the protocol-facing payload.
    #[must_use]
    pub fn new(data: crate::port::ResourceListData) -> Self {
        let rows = data.rows.iter().map(map_row).collect();
        Self {
            payload: ResourceListResponse {
                context: data.context,
                gvk: map_gvk(&data.gvk),
                namespace: data.namespace,
                revision: BackendRevision::new(data.revision),
                rows,
                generated_at: data.generated_at,
                capabilities: capabilities_for_gvk(&data.gvk),
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> ResourceListResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ResourceListResponse must serialize")
    }
}

/// Normalized single-resource detail mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceDetailResult {
    payload: ResourceDetailResponse,
}

impl ResourceDetailResult {
    /// Map a backend record into detail sections, owner references, and
    /// capabilities.
    #[must_use]
    pub fn new(record: ResourceRecord) -> Self {
        let identity = map_identity(&record.reference);
        let mut sections = vec![DetailSection {
            title: "Overview".into(),
            rows: vec![
                DetailRow {
                    label: "Kind".into(),
                    value: record.reference.gvk.kind.clone(),
                },
                DetailRow {
                    label: "Name".into(),
                    value: record.reference.name.clone(),
                },
                DetailRow {
                    label: match identity.scope() {
                        k10s_protocol::ResourceScope::Namespaced => "Namespace".into(),
                        k10s_protocol::ResourceScope::Cluster => "Scope".into(),
                    },
                    value: record
                        .reference
                        .namespace
                        .clone()
                        .unwrap_or_else(|| "Cluster-scoped".into()),
                },
                DetailRow {
                    label: "Status".into(),
                    value: record.summary.clone(),
                },
                DetailRow {
                    label: "Created".into(),
                    value: record.created_at.clone(),
                },
            ],
        }];
        if !record.labels.is_empty() {
            sections.push(DetailSection {
                title: "Labels".into(),
                rows: record
                    .labels
                    .iter()
                    .map(|(key, value)| DetailRow {
                        label: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            });
        }
        if !record.owner_references.is_empty() {
            sections.push(DetailSection {
                title: "Owner References".into(),
                rows: record
                    .owner_references
                    .iter()
                    .map(|owner| DetailRow {
                        label: owner.gvk.kind.clone(),
                        value: format!(
                            "{}{}",
                            owner.name,
                            if owner.controller {
                                " (controller)"
                            } else {
                                ""
                            }
                        ),
                    })
                    .collect(),
            });
        }
        let capabilities = capabilities_for_gvk(&record.reference.gvk);
        Self {
            payload: ResourceDetailResponse {
                revision: BackendRevision::new(record.revision),
                created_at: record.created_at,
                owner_references: record
                    .owner_references
                    .iter()
                    .map(|owner| k10s_protocol::OwnerReference {
                        gvk: map_gvk(&owner.gvk),
                        name: owner.name.clone(),
                        uid: owner.uid.clone(),
                        controller: owner.controller,
                    })
                    .collect(),
                sections,
                capabilities,
                identity,
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> ResourceDetailResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ResourceDetailResponse must serialize")
    }
}

/// Availability-gated pod metrics mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceMetricsResult {
    payload: ResourceMetricsResponse,
}

impl ResourceMetricsResult {
    /// Derive the availability tri-state from sample completeness so the
    /// wire contract stays consistent.
    #[must_use]
    pub fn new(reference: &ResourceRef, sample: MetricsSample) -> Self {
        let availability = match (&sample.cpu_millicores, &sample.memory_bytes) {
            (Some(_), Some(_)) => MetricsAvailability::Available,
            (None, None) => MetricsAvailability::Unavailable,
            _ => MetricsAvailability::Partial,
        };
        Self {
            payload: ResourceMetricsResponse {
                identity: map_identity(reference),
                metrics: PodMetrics {
                    availability,
                    cpu_millicores: sample.cpu_millicores,
                    memory_bytes: sample.memory_bytes,
                    collected_at: sample.collected_at,
                },
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> ResourceMetricsResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ResourceMetricsResponse must serialize")
    }
}

/// Map a backend group/version/kind into its protocol-facing type.
#[must_use]
pub fn map_gvk(gvk: &Gvk) -> GroupVersionKind {
    GroupVersionKind {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
    }
}

/// Map a backend resource reference into a protocol identity.
#[must_use]
fn map_identity(reference: &ResourceRef) -> ResourceIdentity {
    ResourceIdentity {
        context: reference.context.clone(),
        gvk: map_gvk(&reference.gvk),
        namespace: reference.namespace.clone(),
        name: reference.name.clone(),
        uid: reference.uid.clone(),
    }
}

/// Map one backend record into a normalized protocol list row.
#[must_use]
fn map_row(record: &ResourceRecord) -> ResourceListRow {
    ResourceListRow {
        identity: map_identity(&record.reference),
        revision: BackendRevision::new(record.revision),
        labels: record
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
        summary: record.summary.clone(),
        created_at: record.created_at.clone(),
    }
}

/// Derive per-kind capabilities asserted to the UI.
#[must_use]
fn capabilities_for_gvk(gvk: &Gvk) -> ResourceCapabilities {
    let mut capabilities = ResourceCapabilities::default();
    match WorkloadKind::from_gvk(&map_gvk(gvk)) {
        Some(WorkloadKind::Deployment | WorkloadKind::StatefulSet | WorkloadKind::DaemonSet) => {
            capabilities.can_scale = true
        }
        Some(WorkloadKind::Pod) => {
            capabilities.can_view_logs = true;
            capabilities.can_exec = true;
        }
        _ => {}
    }
    capabilities
}
