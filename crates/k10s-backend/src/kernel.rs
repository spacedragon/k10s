//! Backend kernel: the sole protocol-facing interface.
//!
//! Owns all Kubernetes-facing product behavior. Maps to normalized protocol
//! payloads and enforces deadlines/cancellation. Fake data never escapes as
//! fixture types.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k10s_protocol::{
    BackendRevision, BootstrapResponse, ContainerMetrics, Context, DetailRow, DetailSection,
    GroupVersionKind, InfrastructureResponse, MetricsAvailability, PodMetrics, ProtocolVersion,
    ResourceCapabilities, ResourceDetailResponse, ResourceIdentity, ResourceListResponse,
    ResourceListRow, ResourceMetricsResponse, ResourceProjection as WireProjection,
    ResourceRelationsResponse, ServerInfo, TargetPort, TransportProtocol, WorkloadKind,
};
use uuid::Uuid;

use crate::port::{
    BackendError, BootstrapInfo, Command, ContextInfo, Gvk, KubernetesAccess, MetricsSample,
    OperationId, Query, QueryResult, RecordEventsCondition, RelatedData, ResourceProjection,
    ResourceRecord, ResourceRef, ServicePort, Subscribe, SubscriptionHandle,
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
    /// Expose the backend-owned port-forward seam when the adapter supports
    /// forwarding; servers gate the capability separately.
    #[must_use]
    pub fn port_forward_connector(&self) -> Option<crate::port_forward::PortForwardConnector> {
        self.adapter.port_forward_connector()
    }

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
    /// The deadline covers the adapter read and protocol mapping.
    /// If it elapses anywhere along that path, the remaining work is
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
        let fut = async move {
            let result = self.adapter.query(req).await?;
            Ok(match result {
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
                QueryResult::DeletePreflight(response) => {
                    KernelQueryResult::DeletePreflight(response)
                }
                QueryResult::ResourceMetrics(sample) => {
                    KernelQueryResult::ResourceMetrics(ResourceMetricsResult::new(
                        metrics_reference
                            .as_ref()
                            .expect("resource metrics queries carry a reference"),
                        sample,
                    ))
                }
                QueryResult::ResourceTypes(data) => {
                    KernelQueryResult::ResourceTypes(ResourceTypesResult::new(data))
                }
                QueryResult::ContextSwitch(data) => {
                    KernelQueryResult::ContextSwitch(ContextSwitchResult::new(data))
                }
                QueryResult::ContextPermissions(data) => {
                    KernelQueryResult::ContextPermissions(ContextPermissionsResult::new(data))
                }
                QueryResult::Infrastructure(snapshot) => {
                    KernelQueryResult::Infrastructure(InfrastructureResult::new(snapshot))
                }
                QueryResult::YamlValidation(data) => {
                    KernelQueryResult::YamlValidate(crate::operation::YamlValidateResult::new(data))
                }
                QueryResult::StreamTicket(grant) => {
                    KernelQueryResult::StreamTicket(crate::stream::StreamTicketResult::new(grant))
                }
                QueryResult::OperationStatus(data) => KernelQueryResult::OperationStatus(
                    crate::operation::OperationStatusResult::new(data),
                ),
                QueryResult::ResourceRelations(data) => {
                    KernelQueryResult::ResourceRelations(ResourceRelationsResult::new(data))
                }
            })
        };
        match deadline {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| BackendError::Timeout)?,
            None => fut.await,
        }
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

    /// Map a backend infrastructure telemetry update to the wire payload.
    #[must_use]
    pub fn infrastructure_update(
        &self,
        snapshot: crate::catalog::CatalogSnapshot,
    ) -> InfrastructureResponse {
        snapshot.into_protocol()
    }
}

/// Result of a kernel query.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // Resource records intentionally remain inline at this port boundary.
pub enum KernelQueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapResult),
    /// Normalized resource list result.
    ResourceList(ResourceListResult),
    /// Normalized single-resource detail result.
    ResourceDetail(ResourceDetailResult),
    /// Successful exact-target delete dry-run.
    DeletePreflight(k10s_protocol::DeletePreflightResponse),
    /// Independently resolved related-resource groups.
    ResourceRelations(ResourceRelationsResult),
    /// Availability-gated pod metrics result.
    ResourceMetrics(ResourceMetricsResult),
    /// Selectable resource types for the GVK picker.
    ResourceTypes(ResourceTypesResult),
    /// A committed context switch.
    ContextSwitch(ContextSwitchResult),
    /// Advisory RBAC capability projection of one context.
    ContextPermissions(ContextPermissionsResult),
    /// Overview, Nodes, Storage, and metrics result.
    Infrastructure(InfrastructureResult),
    /// Guarded YAML validation outcome with its issued ticket when valid.
    YamlValidate(crate::operation::YamlValidateResult),
    /// A granted single-use stream ticket.
    StreamTicket(crate::stream::StreamTicketResult),
    /// Current state of the requested background operations.
    OperationStatus(crate::operation::OperationStatusResult),
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
            Self::DeletePreflight(r) => serde_json::to_string(r).expect("preflight serializes"),
            Self::ResourceRelations(r) => r.serialized(),
            Self::ResourceMetrics(r) => r.serialized(),
            Self::ResourceTypes(r) => r.serialized(),
            Self::ContextSwitch(r) => r.serialized(),
            Self::ContextPermissions(r) => r.serialized(),
            Self::Infrastructure(r) => r.serialized(),
            Self::YamlValidate(r) => r.serialized(),
            Self::StreamTicket(r) => r.serialized(),
            Self::OperationStatus(r) => r.serialized(),
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
                capabilities: vec!["logs.tail".into()],
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
                availability: c.availability,
                unavailable_reason: c.unavailable_reason.clone(),
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
    /// Map a backend record into detail sections, owner references,
    /// deterministic events, and capabilities. Relations load independently.
    #[must_use]
    pub fn new(record: ResourceRecord) -> Self {
        let identity = map_identity(&record.reference);
        // Adapters that fetched the real object render its YAML themselves,
        // bound to UID/resourceVersion; others keep the synthesized header.
        let manifest = if record.manifest.is_empty() {
            crate::operation::manifest_for(&record)
        } else {
            record.manifest.clone()
        };
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
                events_condition: match record.events_condition {
                    RecordEventsCondition::Available => k10s_protocol::EventsCondition::Available,
                    RecordEventsCondition::Unavailable => {
                        k10s_protocol::EventsCondition::Unavailable
                    }
                },
                events: record
                    .events
                    .iter()
                    .map(|event| k10s_protocol::EventRow {
                        reason: event.reason.clone(),
                        message: event.message.clone(),
                        count: event.count,
                        last_seen: event.last_seen.clone(),
                    })
                    .collect(),
                related: Vec::new(),
                capabilities,
                manifest,
                identity,
                projection: record.projection.as_ref().map(map_projection),
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

/// Independently resolved resource relations mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceRelationsResult {
    payload: ResourceRelationsResponse,
}

impl ResourceRelationsResult {
    #[must_use]
    pub fn new(data: RelatedData) -> Self {
        Self {
            payload: ResourceRelationsResponse {
                identity: map_identity(&data.reference),
                revision: BackendRevision::new(data.revision),
                groups: data
                    .groups
                    .iter()
                    .map(|group| k10s_protocol::RelatedGroup {
                        title: related_group_title(&group.gvk),
                        gvk: map_gvk(&group.gvk),
                        rows: group.records.iter().map(map_row).collect(),
                    })
                    .collect(),
            },
        }
    }

    #[must_use]
    pub fn wire_payload(&self) -> ResourceRelationsResponse {
        self.payload.clone()
    }

    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ResourceRelationsResponse must serialize")
    }
}

/// Availability-gated pod metrics mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceMetricsResult {
    payload: ResourceMetricsResponse,
}

/// Selectable resource types (built-ins and CRDs) mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ResourceTypesResult {
    payload: k10s_protocol::ResourceTypesResponse,
}

impl ResourceTypesResult {
    /// Map backend-owned type entries into the picker payload.
    #[must_use]
    pub fn new(data: crate::port::ResourceTypesData) -> Self {
        Self {
            payload: k10s_protocol::ResourceTypesResponse {
                context: data.context,
                types: data
                    .types
                    .into_iter()
                    .map(|entry| k10s_protocol::ResourceTypeEntry {
                        gvk: map_gvk(&entry.gvk),
                        namespaced: entry.namespaced,
                    })
                    .collect(),
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::ResourceTypesResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ResourceTypesResponse must serialize")
    }
}

/// A committed context switch mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ContextSwitchResult {
    payload: k10s_protocol::ContextSwitchResponse,
}

impl ContextSwitchResult {
    /// Map backend-owned switch data into the protocol-facing payload.
    #[must_use]
    pub fn new(data: crate::port::ContextSwitchData) -> Self {
        Self {
            payload: k10s_protocol::ContextSwitchResponse {
                current: data.current,
                previous: data.previous,
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::ContextSwitchResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ContextSwitchResponse must serialize")
    }
}

/// An advisory RBAC capability projection mapped for the protocol.
#[derive(Debug, Clone)]
pub struct ContextPermissionsResult {
    payload: k10s_protocol::ContextPermissionsResponse,
}

impl ContextPermissionsResult {
    /// Map backend-owned permission checks into the protocol-facing payload,
    /// keeping unknown states distinct from denied ones.
    #[must_use]
    pub fn new(data: crate::port::ContextPermissionsData) -> Self {
        Self {
            payload: k10s_protocol::ContextPermissionsResponse {
                context: data.context,
                checks: data
                    .checks
                    .into_iter()
                    .map(|check| k10s_protocol::PermissionCheck {
                        verb: check.verb,
                        resource: check.resource,
                        group: check.group,
                        namespace: check.namespace,
                        outcome: match check.outcome {
                            crate::port::PermissionOutcome::Allowed => {
                                k10s_protocol::PermissionOutcome::Allowed
                            }
                            crate::port::PermissionOutcome::Denied => {
                                k10s_protocol::PermissionOutcome::Denied
                            }
                            crate::port::PermissionOutcome::Unknown => {
                                k10s_protocol::PermissionOutcome::Unknown
                            }
                        },
                    })
                    .collect(),
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::ContextPermissionsResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("ContextPermissionsResponse must serialize")
    }
}

/// Infrastructure catalog mapped for the protocol.
#[derive(Debug, Clone)]
pub struct InfrastructureResult {
    payload: InfrastructureResponse,
}

impl InfrastructureResult {
    /// Map a backend-owned catalog into the protocol-facing payload.
    #[must_use]
    pub fn new(snapshot: crate::catalog::CatalogSnapshot) -> Self {
        Self {
            payload: snapshot.into_protocol(),
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> InfrastructureResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("InfrastructureResponse must serialize")
    }
}

impl ResourceMetricsResult {
    /// Derive the availability tri-state from sample completeness so the
    /// wire contract stays consistent.
    #[must_use]
    pub fn new(reference: &ResourceRef, sample: MetricsSample) -> Self {
        let MetricsSample {
            cpu_millicores,
            memory_bytes,
            collected_at,
            containers,
        } = sample;
        Self {
            payload: ResourceMetricsResponse {
                identity: map_identity(reference),
                metrics: pod_metrics(cpu_millicores, memory_bytes, collected_at.clone()),
                containers: containers
                    .into_iter()
                    .map(|container| ContainerMetrics {
                        name: container.name,
                        metrics: pod_metrics(
                            container.cpu_millicores,
                            container.memory_bytes,
                            collected_at.clone(),
                        ),
                    })
                    .collect(),
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

/// Derive the availability of one aggregate or container sample.
#[must_use]
fn pod_metrics(
    cpu_millicores: Option<u64>,
    memory_bytes: Option<u64>,
    collected_at: Option<String>,
) -> PodMetrics {
    let availability = match (&cpu_millicores, &memory_bytes) {
        (Some(_), Some(_)) => MetricsAvailability::Available,
        (None, None) => MetricsAvailability::Unavailable,
        _ => MetricsAvailability::Partial,
    };
    PodMetrics {
        availability,
        cpu_millicores,
        memory_bytes,
        collected_at,
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

/// Human title of one related group, pluralizing the kind deterministically.
#[must_use]
fn related_group_title(gvk: &Gvk) -> String {
    format!("{}s", gvk.kind)
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
        projection: record.projection.as_ref().map(map_projection),
    }
}

/// Map a backend projection onto its protocol-facing payload.
#[must_use]
fn map_projection(projection: &ResourceProjection) -> WireProjection {
    match projection {
        ResourceProjection::Pod(pod) => WireProjection::Pod(k10s_protocol::PodProjection {
            phase: pod.phase.clone(),
            ready_containers: pod.ready_containers,
            total_containers: pod.total_containers,
            restart_count: pod.restart_count,
            containers: pod.containers.iter().map(map_pod_container).collect(),
            conditions: pod.conditions.iter().map(map_condition).collect(),
            node_name: pod.node_name.clone(),
            pod_ip: pod.pod_ip.clone(),
            host_ip: pod.host_ip.clone(),
            qos_class: pod.qos_class.clone(),
            priority: pod.priority,
            service_account: pod.service_account.clone(),
            restart_policy: pod.restart_policy.clone(),
            ports: pod.ports.iter().map(map_pod_container_port).collect(),
            labels: pod.labels.clone(),
            annotations: pod.annotations.clone(),
            created_at: pod.created_at.clone(),
        }),
        ResourceProjection::Deployment(deployment) => {
            WireProjection::Deployment(k10s_protocol::DeploymentProjection {
                desired_replicas: deployment.desired_replicas,
                ready_replicas: deployment.ready_replicas,
                updated_replicas: deployment.updated_replicas,
                available_replicas: deployment.available_replicas,
                strategy: deployment.strategy.clone(),
                selector: deployment.selector.clone(),
                max_surge: deployment.max_surge.clone(),
                max_unavailable: deployment.max_unavailable.clone(),
                conditions: deployment.conditions.iter().map(map_condition).collect(),
                template_containers: deployment
                    .template_containers
                    .iter()
                    .map(|container| k10s_protocol::ContainerImageProjection {
                        name: container.name.clone(),
                        image: container.image.clone(),
                    })
                    .collect(),
                template_labels: deployment.template_labels.clone(),
                template_annotations: deployment.template_annotations.clone(),
                labels: deployment.labels.clone(),
                annotations: deployment.annotations.clone(),
                created_at: deployment.created_at.clone(),
            })
        }
        ResourceProjection::ReplicaSet(replica_set) => {
            WireProjection::ReplicaSet(k10s_protocol::ReplicaSetProjection {
                revision: replica_set.revision,
                replicas: replica_set.replicas,
                ready_replicas: replica_set.ready_replicas,
                created_at: replica_set.created_at.clone(),
                images: replica_set
                    .images
                    .iter()
                    .map(|container| k10s_protocol::ContainerImageProjection {
                        name: container.name.clone(),
                        image: container.image.clone(),
                    })
                    .collect(),
            })
        }
        ResourceProjection::Service(service) => {
            WireProjection::Service(k10s_protocol::ServiceProjection {
                service_type: service.service_type.clone(),
                cluster_ips: service.cluster_ips.clone(),
                selector: service
                    .selector
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                external_name: service.external_name.clone(),
                session_affinity: service.session_affinity.clone(),
                external_traffic_policy: service.external_traffic_policy.clone(),
                internal_traffic_policy: service.internal_traffic_policy.clone(),
                ports: service.ports.iter().map(map_service_port).collect(),
            })
        }
    }
}

/// Map one backend condition onto its protocol-facing payload.
#[must_use]
fn map_condition(
    condition: &crate::port::ResourceConditionProjection,
) -> k10s_protocol::ResourceConditionProjection {
    k10s_protocol::ResourceConditionProjection {
        condition_type: condition.condition_type.clone(),
        status: condition.status.clone(),
        reason: condition.reason.clone(),
        message: condition.message.clone(),
        last_transition_time: condition.last_transition_time.clone(),
    }
}

/// Map one backend Pod container onto its protocol-facing payload.
#[must_use]
fn map_pod_container(
    container: &crate::port::PodContainerProjection,
) -> k10s_protocol::PodContainerProjection {
    k10s_protocol::PodContainerProjection {
        name: container.name.clone(),
        image: container.image.clone(),
        state: container.state.as_ref().map(|state| match state {
            crate::port::ContainerStateProjection::Running => {
                k10s_protocol::ContainerStateProjection::Running
            }
            crate::port::ContainerStateProjection::Waiting { reason } => {
                k10s_protocol::ContainerStateProjection::Waiting {
                    reason: reason.clone(),
                }
            }
            crate::port::ContainerStateProjection::Terminated(termination) => {
                k10s_protocol::ContainerStateProjection::Terminated(map_termination(termination))
            }
        }),
        ready: container.ready,
        restart_count: container.restart_count,
        last_termination: container.last_termination.as_ref().map(map_termination),
    }
}

/// Map one backend declared Pod port onto its protocol-facing payload.
#[must_use]
fn map_pod_container_port(port: &crate::port::PodContainerPort) -> k10s_protocol::PodContainerPort {
    k10s_protocol::PodContainerPort {
        container_name: port.container_name.clone(),
        name: port.name.clone(),
        container_port: port.container_port,
        host_port: port.host_port,
        protocol: match port.protocol {
            crate::port::TransportProtocol::Tcp => TransportProtocol::Tcp,
            crate::port::TransportProtocol::Udp => TransportProtocol::Udp,
            crate::port::TransportProtocol::Sctp => TransportProtocol::Sctp,
        },
    }
}

/// Map one backend container termination onto its protocol-facing payload.
#[must_use]
fn map_termination(
    termination: &crate::port::ContainerTerminationProjection,
) -> k10s_protocol::ContainerTerminationProjection {
    k10s_protocol::ContainerTerminationProjection {
        exit_code: termination.exit_code,
        reason: termination.reason.clone(),
    }
}

/// Map one backend Service port onto its protocol-facing payload.
#[must_use]
fn map_service_port(port: &ServicePort) -> k10s_protocol::ServicePort {
    k10s_protocol::ServicePort {
        name: port.name.clone(),
        service_port: port.service_port,
        target_port: match &port.target_port {
            crate::port::TargetPort::Name(name) => TargetPort::Name { name: name.clone() },
            crate::port::TargetPort::Number(number) => TargetPort::Number { number: *number },
        },
        node_port: port.node_port,
        protocol: match port.protocol {
            crate::port::TransportProtocol::Tcp => TransportProtocol::Tcp,
            crate::port::TransportProtocol::Udp => TransportProtocol::Udp,
            crate::port::TransportProtocol::Sctp => TransportProtocol::Sctp,
        },
        app_protocol: port.app_protocol.clone(),
    }
}

/// Derive per-kind capabilities asserted to the UI.
#[must_use]
fn capabilities_for_gvk(gvk: &Gvk) -> ResourceCapabilities {
    let mut capabilities = ResourceCapabilities::default();
    match WorkloadKind::from_gvk(&map_gvk(gvk)) {
        Some(WorkloadKind::Deployment | WorkloadKind::StatefulSet | WorkloadKind::DaemonSet) => {
            capabilities.can_scale = true;
            capabilities.can_restart = true;
        }
        Some(WorkloadKind::Pod) => {
            capabilities.can_view_logs = true;
            capabilities.can_exec = true;
        }
        Some(WorkloadKind::ReplicaSet | WorkloadKind::Job | WorkloadKind::CronJob) | None => {}
    }
    capabilities
}
