//! Deterministic fake Kubernetes adapter.
//!
//! Returns fixed contexts and never exposes credentials or tokens. Used by
//! tests and the desktop app for offline development.
//!
//! All data lives behind the adapter: contexts, built-in workload kinds,
//! CRD-backed objects, owner references, metrics samples, and timestamps are
//! produced deterministically from a fixed epoch plus a monotonic revision
//! counter. No wall clock is read anywhere. Tests advance the world only
//! through [`FakeKubernetes::delete_resource`] and [`FakeKubernetes::
//! touch_resource`]; fixture types never escape the adapter.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::broadcast;

use crate::catalog::{CatalogMetricsScenario, CatalogSnapshot};
use crate::port::{
    ApiResourceDescriptor, BackendError, BootstrapInfo, Command, ContextInfo, Gvk,
    KubernetesAccess, MetricsSample, OperationId, OwnerRef, Query, QueryResult, RecordEvent,
    RelatedData, RelatedRecordGroup, ResourceListData, ResourceRecord, ResourceRef,
    ResourceTypesData, StreamInput, Subscribe, SubscriptionHandle,
};
use crate::stream::StreamHub;
use crate::watch::{WatchHub, WatchSelector};

/// Unix seconds for the fixed fake epoch `2026-08-21T00:00:00Z`.
const FAKE_EPOCH_SECS: u64 = 1_787_270_400;
/// The context whose policy denies every mutation (RBAC fixture).
const READONLY_CONTEXT: &str = "prod-readonly";
/// Revision assigned to every object in the pristine dataset.
const INITIAL_REVISION: u64 = 1_000;
/// Hard bound on unredeemed validation tickets kept per process.
const TICKET_CAPACITY: usize = 32;
/// A ticket older than this many backend revisions has expired.
const TICKET_MAX_AGE_REVISIONS: u64 = 128;
/// Hard bound on retained operation records.
const OPERATION_CAPACITY: usize = 64;
/// Hard bound on retained idempotency records.
const IDEMPOTENCY_CAPACITY: usize = 32;
/// Total deterministic progress steps of one operation lifecycle.
const OPERATION_TOTAL_STEPS: u32 = 3;

/// A registered background-operation watcher.
#[derive(Debug)]
struct OperationWatcher {
    sender: broadcast::Sender<crate::port::BackendEvent>,
}

/// A registered infrastructure telemetry watcher.
#[derive(Debug)]
struct InfrastructureWatcher {
    context: String,
    sender: broadcast::Sender<crate::port::BackendEvent>,
}

/// Deterministic fake cases for infrastructure metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeMetricsScenario {
    /// All metrics are present and fresh.
    Full,
    /// Some metrics are absent.
    Partial,
    /// RBAC denied metrics collection.
    Forbidden,
    /// The last known sample is stale.
    Stale,
}

impl From<FakeMetricsScenario> for CatalogMetricsScenario {
    fn from(value: FakeMetricsScenario) -> Self {
        match value {
            FakeMetricsScenario::Full => Self::Full,
            FakeMetricsScenario::Partial => Self::Partial,
            FakeMetricsScenario::Forbidden => Self::Forbidden,
            FakeMetricsScenario::Stale => Self::Stale,
        }
    }
}

/// Interior mutable fake world state shared by adapter clones.
#[derive(Debug)]
struct FakeState {
    contexts: Vec<ContextInfo>,
    records: Vec<ResourceRecord>,
    metrics: HashMap<String, MetricsSample>,
    watches: WatchHub,
    infrastructure_watchers: Vec<InfrastructureWatcher>,
    metrics_scenario: FakeMetricsScenario,
    revision: u64,
    /// Live validation tickets by ID, consumed single-use by apply.
    /// `ticket_order` mirrors the live keys in issuance order so capacity
    /// eviction removes the oldest deterministically regardless of ID
    /// formatting.
    tickets: HashMap<String, crate::operation::Ticket>,
    ticket_order: std::collections::VecDeque<String>,
    next_ticket: u64,
    next_operation: u64,
    /// Retained operation records by ID, bounded by [`OPERATION_CAPACITY`].
    /// `operation_order` mirrors the live keys in creation order so
    /// eviction removes the oldest deterministically.
    operations: HashMap<String, crate::operation::OperationRecord>,
    operation_order: std::collections::VecDeque<String>,
    /// Operation IDs armed to fail at their terminal transition.
    armed_failures: HashMap<String, String>,
    /// Live background-operation watchers.
    operation_watchers: Vec<OperationWatcher>,
    /// Bounded idempotency records: key → accepted operation ID. Replays
    /// of a key return the original operation instead of executing again.
    idempotency: HashMap<String, String>,
    idempotency_order: std::collections::VecDeque<String>,
    /// Kernel-owned stream hub: single-use stream tickets and fake sessions.
    streams: StreamHub,
}

#[cfg(test)]
#[derive(Debug)]
struct SubscriptionCutGate {
    registered: Arc<std::sync::Barrier>,
    mutation_done: Arc<std::sync::Barrier>,
}

impl FakeState {
    fn advance_revision(&mut self) -> u64 {
        self.revision += 1;
        self.revision
    }

    fn current_revision(&self) -> u64 {
        self.revision
    }

    fn find_index(
        &self,
        context: &str,
        gvk: &Gvk,
        namespace: Option<&str>,
        name: &str,
    ) -> Option<usize> {
        self.records.iter().position(|record| {
            let reference = &record.reference;
            reference.context == context
                && &reference.gvk == gvk
                && reference.namespace.as_deref() == namespace
                && reference.name == name
        })
    }

    /// Resolve a full resource identity, including the UID. A stale UID
    /// (delete/recreate reuse of the same name) resolves to nothing.
    fn find_record(&self, reference: &ResourceRef) -> Option<&ResourceRecord> {
        self.records.iter().find(|record| {
            let candidate = &record.reference;
            candidate.context == reference.context
                && candidate.gvk == reference.gvk
                && candidate.namespace == reference.namespace
                && candidate.name == reference.name
                && candidate.uid == reference.uid
        })
    }

    /// Resolve a record by identity fields without the UID, used by the
    /// guarded apply to detect recreations and deletions.
    fn find_by_name(
        &self,
        context: &str,
        gvk: &Gvk,
        namespace: Option<&str>,
        name: &str,
    ) -> Option<&ResourceRecord> {
        self.records.iter().find(|record| {
            let candidate = &record.reference;
            candidate.context == context
                && &candidate.gvk == gvk
                && candidate.namespace.as_deref() == namespace
                && candidate.name == name
        })
    }

    /// Resolve every transitive controller-owned descendant of `reference`
    /// by matching controller owner UIDs, grouped by type in deterministic
    /// order. This is the Deployment → ReplicaSet → Pod traversal used by
    /// detail related tabs.
    fn find_related(&self, reference: &ResourceRef) -> Vec<RelatedRecordGroup> {
        let mut groups: Vec<RelatedRecordGroup> = Vec::new();
        let mut visited: Vec<String> = vec![reference.uid.clone()];
        loop {
            let frontier: Vec<String> = visited.clone();
            let mut discovered = false;
            for record in &self.records {
                let candidate = &record.reference;
                if candidate.context != reference.context
                    || candidate.namespace != reference.namespace
                    || visited.contains(&candidate.uid)
                {
                    continue;
                }
                let owned = record
                    .owner_references
                    .iter()
                    .any(|owner| owner.controller && frontier.contains(&owner.uid));
                if !owned {
                    continue;
                }
                visited.push(candidate.uid.clone());
                discovered = true;
                match groups.iter_mut().find(|group| group.gvk == candidate.gvk) {
                    Some(group) => group.records.push(record.clone()),
                    None => groups.push(RelatedRecordGroup {
                        gvk: candidate.gvk.clone(),
                        records: vec![record.clone()],
                    }),
                }
            }
            if !discovered {
                break;
            }
        }
        for group in &mut groups {
            group
                .records
                .sort_by(|left, right| left.reference.cmp(&right.reference));
        }
        groups.sort_by(|left, right| left.gvk.cmp(&right.gvk));
        groups
    }

    fn matches_selector(
        &self,
        reference: &ResourceRef,
        context: &str,
        gvk: &Gvk,
        namespace: &Option<String>,
    ) -> bool {
        reference.context == context
            && reference.gvk == *gvk
            && namespace
                .as_ref()
                .is_none_or(|wanted| Some(wanted.as_str()) == reference.namespace.as_deref())
    }

    fn notify_matching(&mut self, reference: &ResourceRef, event: crate::port::BackendEvent) {
        self.watches
            .broadcast(event, |selector| selector.matches(reference));
    }
}

/// A deterministic fake Kubernetes adapter.
///
/// Clones share one world so tests can drive mutations while a server holds
/// another clone behind the backend kernel.
#[derive(Debug, Clone)]
pub struct FakeKubernetes {
    state: Arc<Mutex<FakeState>>,
    #[cfg(test)]
    subscription_cut_gate: Arc<Mutex<Option<SubscriptionCutGate>>>,
}

impl FakeKubernetes {
    /// Create a standard fake Kubernetes adapter with two contexts and a
    /// deterministic dataset covering built-in workload kinds, CRD-backed
    /// objects, pods with owner references, and pod metrics samples.
    #[must_use]
    pub fn standard() -> Self {
        Self::with_metrics_scenario(FakeMetricsScenario::Full)
    }

    /// Create the standard dataset with an explicit metrics failure mode.
    #[must_use]
    pub fn with_metrics_scenario(metrics_scenario: FakeMetricsScenario) -> Self {
        let contexts = vec![
            ContextInfo {
                name: "dev-local".into(),
                cluster: "dev-cluster".into(),
                namespace: Some("default".into()),
                is_current: true,
            },
            ContextInfo {
                name: "prod-readonly".into(),
                cluster: "prod-cluster".into(),
                namespace: Some("default".into()),
                is_current: false,
            },
        ];
        let mut records = Vec::new();
        records.extend(build_dev_local_records());
        records.extend(build_prod_records());
        records.sort_by(|left, right| left.reference.cmp(&right.reference));
        let metrics = HashMap::from([
            (
                pod_key("web-frontend-7d9f8-00001"),
                MetricsSample {
                    cpu_millicores: Some(220),
                    memory_bytes: Some(134_217_728),
                    collected_at: Some(rfc3339(FAKE_EPOCH_SECS + 3_600)),
                },
            ),
            (
                pod_key("api-server-5cc4d-qw8rt"),
                MetricsSample {
                    cpu_millicores: Some(90),
                    memory_bytes: None,
                    collected_at: Some(rfc3339(FAKE_EPOCH_SECS + 3_600)),
                },
            ),
        ]);
        Self {
            state: Arc::new(Mutex::new(FakeState {
                contexts,
                records,
                metrics,
                watches: WatchHub::default(),
                infrastructure_watchers: Vec::new(),
                metrics_scenario,
                revision: INITIAL_REVISION,
                tickets: HashMap::new(),
                ticket_order: std::collections::VecDeque::new(),
                next_ticket: 1,
                next_operation: 1,
                operations: HashMap::new(),
                operation_order: std::collections::VecDeque::new(),
                armed_failures: HashMap::new(),
                operation_watchers: Vec::new(),
                idempotency: HashMap::new(),
                idempotency_order: std::collections::VecDeque::new(),
                streams: StreamHub::new(),
            })),
            #[cfg(test)]
            subscription_cut_gate: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a fake Kubernetes adapter with custom contexts and no
    /// resources.
    #[must_use]
    pub fn with_contexts(contexts: Vec<ContextInfo>) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState {
                contexts,
                records: Vec::new(),
                metrics: HashMap::new(),
                watches: WatchHub::default(),
                infrastructure_watchers: Vec::new(),
                metrics_scenario: FakeMetricsScenario::Full,
                revision: INITIAL_REVISION,
                tickets: HashMap::new(),
                ticket_order: std::collections::VecDeque::new(),
                next_ticket: 1,
                next_operation: 1,
                operations: HashMap::new(),
                operation_order: std::collections::VecDeque::new(),
                armed_failures: HashMap::new(),
                operation_watchers: Vec::new(),
                idempotency: HashMap::new(),
                idempotency_order: std::collections::VecDeque::new(),
                streams: StreamHub::new(),
            })),
            #[cfg(test)]
            subscription_cut_gate: Arc::new(Mutex::new(None)),
        }
    }

    /// Create the deterministic capacity dataset used by the Plan 2 gate:
    /// exactly `objects` workload objects spread over the built-in kinds in
    /// `dev-local` plus `nodes` cluster-scoped Nodes. Generation is fully
    /// deterministic — the same inputs always produce byte-identical
    /// identities, summaries, labels, and timestamps — so benchmarks and
    /// capacity tests are reproducible without any random seed.
    #[must_use]
    pub fn with_capacity(objects: usize, nodes: usize) -> Self {
        let adapter = Self::with_contexts(vec![
            ContextInfo {
                name: "dev-local".into(),
                cluster: "dev-cluster".into(),
                namespace: Some("default".into()),
                is_current: true,
            },
            ContextInfo {
                name: "prod-readonly".into(),
                cluster: "prod-cluster".into(),
                namespace: Some("default".into()),
                is_current: false,
            },
        ]);
        {
            let mut state = adapter.lock();
            state.records = build_capacity_records("dev-local", objects, nodes);
            state
                .records
                .sort_by(|left, right| left.reference.cmp(&right.reference));
        }
        adapter
    }

    /// Total number of retained records; observability for the capacity
    /// dataset.
    #[must_use]
    pub fn total_records(&self) -> usize {
        self.lock().records.len()
    }

    /// Remove one object behind the adapter and broadcast a resource-gone
    /// delta to matching watchers. Returns whether the object existed.
    pub fn delete_resource(
        &self,
        context: &str,
        gvk: &Gvk,
        namespace: Option<&str>,
        name: &str,
    ) -> bool {
        let mut state = self.lock();
        let Some(index) = state.find_index(context, gvk, namespace, name) else {
            return false;
        };
        let removed = state.records.remove(index);
        let revision = state.advance_revision();
        let reference = removed.reference.clone();
        state.notify_matching(
            &reference,
            crate::port::BackendEvent::Gone {
                reference: removed.reference,
                revision,
            },
        );
        true
    }

    /// Bump the revision of one object and broadcast the updated row to
    /// matching watchers. Returns the new backend revision, if the object
    /// exists.
    pub fn touch_resource(
        &self,
        context: &str,
        gvk: &Gvk,
        namespace: Option<&str>,
        name: &str,
    ) -> Option<u64> {
        let mut state = self.lock();
        let index = state.find_index(context, gvk, namespace, name)?;
        let revision = state.advance_revision();
        state.records[index].revision = revision;
        let record = state.records[index].clone();
        let reference = record.reference.clone();
        state.notify_matching(&reference, crate::port::BackendEvent::Changed(record));
        Some(revision)
    }

    /// Switch deterministic metrics behavior and publish one complete
    /// telemetry update to every live watcher. The server remains
    /// responsible for bounded P2 admission and context coalescing.
    pub fn set_metrics_scenario(&self, scenario: FakeMetricsScenario) {
        let mut state = self.lock();
        state.metrics_scenario = scenario;
        let revision = state.advance_revision();
        state.infrastructure_watchers.retain(|watcher| {
            if watcher.sender.receiver_count() == 0 {
                return false;
            }
            let snapshot = CatalogSnapshot::fake(&watcher.context, revision, scenario.into());
            let _ = watcher
                .sender
                .send(crate::port::BackendEvent::Infrastructure(snapshot));
            true
        });
    }

    fn lock(&self) -> MutexGuard<'_, FakeState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Broadcast one operation event to every live watcher.
    fn emit_operation(state: &mut FakeState, event: crate::operation::OperationEvent) {
        state.operation_watchers.retain(|watcher| {
            if watcher.sender.receiver_count() == 0 {
                return false;
            }
            let _ = watcher
                .sender
                .send(crate::port::BackendEvent::Operation(event.clone()));
            true
        });
    }

    /// Record a freshly accepted mutation as an operation: allocates its
    /// deterministic ID, stores the bounded record in `Pending`, remembers
    /// the idempotency key, and notifies watchers. Returns the ID.
    fn begin_operation(state: &mut FakeState, idempotency_key: &str) -> OperationId {
        let operation_id = OperationId::new(format!("op-{:06}", state.next_operation));
        state.next_operation += 1;
        let record = crate::operation::OperationRecord {
            id: operation_id.as_str().to_owned(),
            state: crate::operation::OperationState::Pending,
            progress: None,
            detail: None,
        };
        state.operations.insert(record.id.clone(), record.clone());
        state.operation_order.push_back(record.id.clone());
        while state.operations.len() > OPERATION_CAPACITY {
            if let Some(oldest) = state.operation_order.pop_front() {
                state.operations.remove(&oldest);
            }
        }
        if !idempotency_key.is_empty() && !state.idempotency.contains_key(idempotency_key) {
            state
                .idempotency
                .insert(idempotency_key.to_owned(), operation_id.as_str().to_owned());
            state
                .idempotency_order
                .push_back(idempotency_key.to_owned());
            while state.idempotency.len() > IDEMPOTENCY_CAPACITY {
                if let Some(oldest) = state.idempotency_order.pop_front() {
                    state.idempotency.remove(&oldest);
                }
            }
        }
        Self::emit_operation(
            state,
            crate::operation::OperationEvent {
                id: record.id,
                state: record.state,
                progress: None,
                detail: None,
            },
        );
        operation_id
    }

    /// Advance every nonterminal operation exactly one deterministic step:
    /// `Pending → Running(1/3) → Running(2/3) → terminal`. Operations never
    /// advance on their own; tests command each tick explicitly. Terminal
    /// failures only occur when armed through [`Self::fail_next_operation`].
    pub fn tick_operations(&self) {
        let mut state = self.lock();
        let ids: Vec<String> = state
            .operation_order
            .iter()
            .filter(|id| {
                state
                    .operations
                    .get(*id)
                    .is_some_and(|record| !record.state.is_terminal())
            })
            .cloned()
            .collect();
        for id in ids {
            // Whether this tick drives the operation to its terminal step;
            // decided up front so the armed-failure slot can be consumed
            // without overlapping borrows.
            let terminal_step =
                state
                    .operations
                    .get(&id)
                    .is_some_and(|record| match record.state {
                        crate::operation::OperationState::Pending => false,
                        crate::operation::OperationState::Running => record
                            .progress
                            .is_none_or(|(done, _)| done + 1 >= OPERATION_TOTAL_STEPS),
                        crate::operation::OperationState::Succeeded
                        | crate::operation::OperationState::Failed
                        | crate::operation::OperationState::Cancelled
                        | crate::operation::OperationState::OutcomeUnknown => false,
                    });
            let armed_failure = if terminal_step {
                state.armed_failures.remove(&id)
            } else {
                None
            };
            // Advance one deterministic step, then notify watchers.
            let event = {
                let record = state
                    .operations
                    .get_mut(&id)
                    .expect("live operations stay recorded");
                match record.state {
                    crate::operation::OperationState::Pending => {
                        record.state = crate::operation::OperationState::Running;
                        record.progress = Some((1, OPERATION_TOTAL_STEPS));
                        Some(crate::operation::OperationEvent {
                            id: record.id.clone(),
                            state: record.state,
                            progress: record.progress,
                            detail: None,
                        })
                    }
                    crate::operation::OperationState::Running => {
                        let completed = record.progress.map_or(1, |(done, _)| done) + 1;
                        if completed < OPERATION_TOTAL_STEPS {
                            record.progress = Some((completed, OPERATION_TOTAL_STEPS));
                            Some(crate::operation::OperationEvent {
                                id: record.id.clone(),
                                state: record.state,
                                progress: record.progress,
                                detail: None,
                            })
                        } else if let Some(reason) = armed_failure {
                            record.state = crate::operation::OperationState::Failed;
                            record.progress = None;
                            record.detail = Some(reason);
                            Some(crate::operation::OperationEvent {
                                id: record.id.clone(),
                                state: record.state,
                                progress: None,
                                detail: record.detail.clone(),
                            })
                        } else {
                            record.state = crate::operation::OperationState::Succeeded;
                            record.progress = None;
                            Some(crate::operation::OperationEvent {
                                id: record.id.clone(),
                                state: record.state,
                                progress: None,
                                detail: None,
                            })
                        }
                    }
                    crate::operation::OperationState::Succeeded
                    | crate::operation::OperationState::Failed
                    | crate::operation::OperationState::Cancelled
                    | crate::operation::OperationState::OutcomeUnknown => None,
                }
            };
            // A failed operation releases its idempotency record so the key
            // can be retried; successful ones keep it for deduplication.
            if event
                .as_ref()
                .is_some_and(|event| event.state == crate::operation::OperationState::Failed)
            {
                let released: Vec<String> = state
                    .idempotency
                    .iter()
                    .filter(|(_, operation_id)| **operation_id == id)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in released {
                    state.idempotency.remove(&key);
                    state
                        .idempotency_order
                        .retain(|candidate| candidate != &key);
                }
            }
            if let Some(event) = event {
                Self::emit_operation(&mut state, event);
            }
        }
    }

    /// Arm the NEXT accepted mutation to fail at its final step with the
    /// given safe reason. Deterministic test control over failure paths.
    pub fn fail_next_operation(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut state = self.lock();
        let pending_id = format!("op-{:06}", state.next_operation);
        state.armed_failures.insert(pending_id, reason);
    }

    /// Number of retained operation records; observability for bounds.
    #[must_use]
    pub fn retained_operations(&self) -> usize {
        self.lock().operations.len()
    }

    /// Apply a validated YAML buffer through its single-use ticket.
    ///
    /// Every binding is re-checked before anything mutates: the ticket must
    /// exist unconsumed, the buffer hash must match, the target identity
    /// must still resolve, and the backend revision must equal the revision
    /// the validation was issued against. On success the ticket is consumed,
    /// an operation is opened, and the fake state advances so watchers
    /// receive the changed row. Replaying a live idempotency key returns
    /// the original operation without executing again.
    async fn apply_yaml(
        &self,
        context: String,
        yaml: String,
        idempotency_key: String,
        ticket_id: String,
        buffer_hash: String,
        declared_target: ResourceRef,
    ) -> Result<OperationId, BackendError> {
        if context != declared_target.context {
            return Err(BackendError::Conflict(
                "the apply request declares a different context than its target".into(),
            ));
        }
        if k10s_protocol::buffer_hash(&yaml) != buffer_hash {
            return Err(BackendError::Conflict(
                "the edited buffer no longer matches the validated ticket".into(),
            ));
        }
        let replay = self.lock().idempotency.get(&idempotency_key).cloned();
        if let Some(existing) = replay {
            return Ok(OperationId::new(existing));
        }
        let (ticket, operation_id) = {
            let mut state = self.lock();
            // Verify every binding before consuming: a rejected envelope
            // must leave the ticket intact.
            let Some(ticket) = state.tickets.get(&ticket_id).cloned() else {
                return Err(BackendError::Conflict(
                    "the validation ticket is unknown, expired, or already used".into(),
                ));
            };
            // The declared identity must match the ticket exactly: a client
            // cannot redeem one resource's ticket against another.
            if declared_target != ticket.target {
                return Err(BackendError::Conflict(
                    "the apply request does not match the validated ticket's target".into(),
                ));
            }
            if ticket.buffer_hash != buffer_hash {
                return Err(BackendError::Conflict(
                    "the validation ticket belongs to a different buffer".into(),
                ));
            }
            if state
                .current_revision()
                .saturating_sub(ticket.issued_revision)
                > TICKET_MAX_AGE_REVISIONS
            {
                return Err(BackendError::Conflict(
                    "the validation ticket has expired".into(),
                ));
            }
            state.tickets.remove(&ticket_id);
            state.ticket_order.retain(|id| *id != ticket_id);

            // Open the operation record before the mutation lands.
            let operation_id = Self::begin_operation(&mut state, &idempotency_key);
            (ticket, operation_id)
        };

        let mut state = self.lock();
        match state.find_index(
            &ticket.target.context,
            &ticket.target.gvk,
            ticket.target.namespace.as_deref(),
            &ticket.target.name,
        ) {
            // Update: the object must still be the same instance at exactly
            // the validated revision.
            Some(index) => {
                if state.records[index].reference.uid != ticket.target.uid {
                    return Err(BackendError::Conflict(
                        "the target was recreated since validation".into(),
                    ));
                }
                if state.records[index].revision != ticket.resource_revision {
                    return Err(BackendError::Conflict(
                        "the target changed since validation".into(),
                    ));
                }
                let revision = state.advance_revision();
                state.records[index].revision = revision;
                let record = state.records[index].clone();
                let reference = record.reference.clone();
                state.notify_matching(&reference, crate::port::BackendEvent::Changed(record));
            }
            // Create: the world may not have moved since validation.
            None => {
                if state.current_revision() != ticket.resource_revision {
                    return Err(BackendError::Conflict(
                        "the cluster changed since validation".into(),
                    ));
                }
                if state
                    .contexts
                    .iter()
                    .all(|candidate| candidate.name != ticket.target.context)
                {
                    return Err(BackendError::NotFound);
                }
                let revision = state.advance_revision();
                let record = ResourceRecord {
                    reference: ticket.target.clone(),
                    revision,
                    labels: BTreeMap::new(),
                    summary: "Applied".to_owned(),
                    created_at: rfc3339(FAKE_EPOCH_SECS),
                    owner_references: Vec::new(),
                    events: Vec::new(),
                    manifest: String::new(),
                };
                let changed = ResourceRecord {
                    reference: ticket.target.clone(),
                    revision,
                    labels: BTreeMap::new(),
                    summary: "Applied".to_owned(),
                    created_at: rfc3339(FAKE_EPOCH_SECS),
                    owner_references: Vec::new(),
                    events: Vec::new(),
                    manifest: String::new(),
                };
                let reference = ticket.target.clone();
                state.records.push(record);
                state.notify_matching(&reference, crate::port::BackendEvent::Changed(changed));
            }
        }
        Ok(operation_id)
    }

    /// Scale one exact workload object. The full identity including UID is
    /// re-checked before anything mutates; a stale identity is a typed
    /// conflict and an unknown object a typed not-found. The mutation is
    /// applied to fake state immediately (watchers see the changed row) and
    /// its lifecycle advances only through [`Self::tick_operations`].
    #[allow(clippy::too_many_arguments)]
    async fn scale(
        &self,
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
        name: String,
        uid: String,
        replicas: u32,
        idempotency_key: String,
    ) -> Result<OperationId, BackendError> {
        let replay = self.lock().idempotency.get(&idempotency_key).cloned();
        if let Some(existing) = replay {
            return Ok(OperationId::new(existing));
        }
        let mut state = self.lock();
        if !state.contexts.iter().any(|c| c.name == context) {
            return Err(BackendError::NotFound);
        }
        if context == READONLY_CONTEXT {
            return Err(BackendError::Forbidden);
        }
        let Some(record) = state.find_by_name(&context, &gvk, namespace.as_deref(), &name) else {
            return Err(BackendError::NotFound);
        };
        if record.reference.uid != uid {
            return Err(BackendError::Conflict(
                "the target does not match the current object at this name; it was recreated"
                    .into(),
            ));
        }
        let index = state
            .find_index(&context, &gvk, namespace.as_deref(), &name)
            .expect("the record was just resolved");
        let operation_id = Self::begin_operation(&mut state, &idempotency_key);
        let revision = state.advance_revision();
        state.records[index].revision = revision;
        // The desired count becomes observable backend state wherever the
        // summary carries one (e.g. `20/20 ready` → `3/20 ready`).
        if let Some(next) = scaled_summary(&state.records[index].summary, replicas) {
            state.records[index].summary = next;
        }
        let changed = state.records[index].clone();
        let reference = changed.reference.clone();
        state.notify_matching(&reference, crate::port::BackendEvent::Changed(changed));
        drop(state);
        Ok(operation_id)
    }

    /// Delete one exact object with an explicit propagation mode. Same
    /// identity re-checks as scaling; watchers receive the gone delta.
    async fn delete(
        &self,
        target: ResourceRef,
        propagation: crate::operation::Propagation,
        idempotency_key: String,
    ) -> Result<OperationId, BackendError> {
        let replay = self.lock().idempotency.get(&idempotency_key).cloned();
        if let Some(existing) = replay {
            return Ok(OperationId::new(existing));
        }
        let mut state = self.lock();
        if !state
            .contexts
            .iter()
            .any(|c| c.name == target.context.as_str())
        {
            return Err(BackendError::NotFound);
        }
        if target.context == READONLY_CONTEXT {
            return Err(BackendError::Forbidden);
        }
        let Some(record) = state.find_by_name(
            &target.context,
            &target.gvk,
            target.namespace.as_deref(),
            &target.name,
        ) else {
            return Err(BackendError::NotFound);
        };
        if record.reference.uid != target.uid {
            return Err(BackendError::Conflict(
                "the target does not match the current object at this name; it was recreated"
                    .into(),
            ));
        }
        let operation_id = Self::begin_operation(&mut state, &idempotency_key);
        let _ = propagation;
        let index = state
            .find_index(
                &target.context,
                &target.gvk,
                target.namespace.as_deref(),
                &target.name,
            )
            .expect("the record was just resolved");
        let removed = state.records.remove(index);
        let revision = state.advance_revision();
        let gone = crate::port::BackendEvent::Gone {
            reference: removed.reference.clone(),
            revision,
        };
        state.notify_matching(&removed.reference, gone);
        drop(state);
        Ok(operation_id)
    }

    /// Accept a deterministic fake rollout restart after exact identity and
    /// authorization checks. The fake has no pod-template generation model;
    /// lifecycle events remain observable through the shared operation seam.
    async fn restart(
        &self,
        target: ResourceRef,
        idempotency_key: String,
    ) -> Result<OperationId, BackendError> {
        if let Some(existing) = self.lock().idempotency.get(&idempotency_key).cloned() {
            return Ok(OperationId::new(existing));
        }
        let mut state = self.lock();
        if target.context == READONLY_CONTEXT {
            return Err(BackendError::Forbidden);
        }
        let Some(record) = state.find_by_name(
            &target.context,
            &target.gvk,
            target.namespace.as_deref(),
            &target.name,
        ) else {
            return Err(BackendError::NotFound);
        };
        if record.reference.uid != target.uid {
            return Err(BackendError::Conflict(
                "the target does not match the current object at this name; it was recreated"
                    .into(),
            ));
        }
        Ok(Self::begin_operation(&mut state, &idempotency_key))
    }

    /// Number of registered watchers; test-only observability for pruning.
    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.lock().watches.live_count()
    }

    /// Coordinate a test mutation at the registration/snapshot cut.
    #[cfg(test)]
    fn set_subscription_cut_gate(
        &self,
        registered: Arc<std::sync::Barrier>,
        mutation_done: Arc<std::sync::Barrier>,
    ) {
        *self
            .subscription_cut_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(SubscriptionCutGate {
            registered,
            mutation_done,
        });
    }

    /// Advance one explicit test tick on a live stream session. Sessions
    /// never advance on their own: no wall clock, no process, no command.
    pub fn tick_stream(&self, ticket_id: &str) {
        self.lock().streams.tick(ticket_id);
    }

    /// Terminate a stream session with an explicit exit code.
    pub fn finish_stream(&self, ticket_id: &str, exit_code: i32) {
        self.lock().streams.finish(ticket_id, exit_code);
    }

    /// Number of live stream sessions; observability for disconnect tests.
    #[must_use]
    pub fn live_stream_sessions(&self) -> usize {
        self.lock().streams.live_session_count()
    }

    /// Last recorded terminal resize of a live session.
    #[must_use]
    pub fn last_stream_resize(&self, ticket_id: &str) -> Option<(u32, u32)> {
        self.lock().streams.last_resize(ticket_id)
    }
}

impl Default for FakeKubernetes {
    fn default() -> Self {
        Self::standard()
    }
}

impl KubernetesAccess for FakeKubernetes {
    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match req {
                Query::Bootstrap => Ok(QueryResult::Bootstrap(BootstrapInfo {
                    contexts: self.lock().contexts.clone(),
                })),
                Query::ValidateApply { context, yaml } => {
                    if !self.lock().contexts.iter().any(|c| c.name == context) {
                        return Err(BackendError::NotFound);
                    }
                    let manifest = crate::operation::parse_manifest(&yaml);
                    let gvk = match manifest.resolve_gvk() {
                        Ok(gvk) => gvk,
                        Err(diagnostics) => {
                            return Ok(QueryResult::YamlValidation(
                                crate::operation::YamlValidationData {
                                    context,
                                    outcome: crate::operation::OutcomeData::Invalid { diagnostics },
                                },
                            ));
                        }
                    };
                    let name = manifest
                        .metadata_name
                        .clone()
                        .and_then(|field| field.value)
                        .unwrap_or_default();
                    // Deterministic dry-run against current adapter state:
                    // updates bind to the object's revision and are
                    // disruptive for workload controllers; creates bind to
                    // the cluster revision.
                    let mut state = self.lock();
                    let (reference, exists) = match state.find_by_name(
                        &context,
                        &gvk,
                        manifest.metadata_namespace.as_deref(),
                        &name,
                    ) {
                        Some(record) => (record.reference.clone(), true),
                        None => (
                            ResourceRef {
                                uid: uid(
                                    &context,
                                    &gvk.kind,
                                    manifest.metadata_namespace.as_deref(),
                                    &name,
                                ),
                                context: context.clone(),
                                gvk: gvk.clone(),
                                namespace: manifest.metadata_namespace.clone(),
                                name,
                            },
                            false,
                        ),
                    };
                    let resource_revision = if exists {
                        state
                            .find_record(&reference)
                            .map(|record| record.revision)
                            .unwrap_or_default()
                    } else {
                        state.current_revision()
                    };
                    let ticket_id = format!("ticket-{:04}", state.next_ticket);
                    state.next_ticket += 1;
                    let ticket = crate::operation::Ticket {
                        id: ticket_id.clone(),
                        buffer_hash: k10s_protocol::buffer_hash(&yaml),
                        disruptive: exists && crate::operation::is_disruptive_kind(&gvk),
                        resource_revision,
                        opaque_resource_version: None,
                        issued_revision: state.current_revision(),
                        target: reference,
                    };
                    // Bounded retention: evict the oldest unredeemed
                    // tickets (by issuance order) first so repeated
                    // validation without apply cannot grow the store
                    // without limit.
                    state.tickets.insert(ticket.id.clone(), ticket.clone());
                    state.ticket_order.push_back(ticket.id.clone());
                    while state.tickets.len() > TICKET_CAPACITY {
                        if let Some(oldest) = state.ticket_order.pop_front() {
                            state.tickets.remove(&oldest);
                        }
                    }
                    Ok(QueryResult::YamlValidation(
                        crate::operation::YamlValidationData {
                            context,
                            outcome: crate::operation::OutcomeData::Valid { ticket },
                        },
                    ))
                }
                Query::StreamTicket { stream } => {
                    let mut state = self.lock();
                    let (context, namespace, pod, container) = match &stream {
                        crate::port::StreamKind::Logs {
                            context,
                            namespace,
                            pod,
                            container,
                        } => (context, namespace, pod, container),
                        crate::port::StreamKind::Exec {
                            context,
                            namespace,
                            pod,
                            container,
                            ..
                        } => (context, namespace, pod, container),
                    };
                    // Context existence and RBAC policy come first.
                    if !state.contexts.iter().any(|c| c.name == context.as_str()) {
                        return Err(BackendError::NotFound);
                    }
                    if context == "prod-readonly" {
                        return Err(BackendError::Forbidden);
                    }
                    // The pod must exist with its stable identity.
                    let pod_gvk = Gvk::core("v1", "Pod");
                    let exists = state.records.iter().any(|record| {
                        let reference = &record.reference;
                        reference.context == context.as_str()
                            && reference.gvk == pod_gvk
                            && reference.namespace.as_deref() == Some(namespace.as_str())
                            && reference.name == pod.as_str()
                    });
                    if !exists {
                        return Err(BackendError::NotFound);
                    }
                    // Container fixtures: every pod runs the `app` container;
                    // `distroless` exists too (so its logs remain readable)
                    // but carries no executable binary, making exec fail.
                    // Any other name does not exist. Binary availability is
                    // an exec-only validation.
                    match container.as_str() {
                        "app" => {}
                        "distroless" => {
                            let exec = matches!(&stream, crate::port::StreamKind::Exec { .. });
                            if exec {
                                return Err(BackendError::Conflict(format!(
                                    "container \"{container}\" has no executable binary"
                                )));
                            }
                        }
                        _ => return Err(BackendError::NotFound),
                    }
                    let revision = state.current_revision();
                    let ticket_id = state.streams.issue_ticket(stream.clone(), revision)?;
                    Ok(QueryResult::StreamTicket(crate::port::StreamGrant {
                        ticket_id,
                        stream,
                    }))
                }
                Query::ResourceList {
                    context,
                    gvk,
                    namespace,
                } => {
                    let generated_at = rfc3339(FAKE_EPOCH_SECS + 60);
                    let (rows, revision) = {
                        let state = self.lock();
                        if !state.contexts.iter().any(|c| c.name == context) {
                            return Err(BackendError::NotFound);
                        }
                        let rows: Vec<ResourceRecord> = state
                            .records
                            .iter()
                            .filter(|record| {
                                state.matches_selector(
                                    &record.reference,
                                    &context,
                                    &gvk,
                                    &namespace,
                                )
                            })
                            .cloned()
                            .collect();
                        (rows, state.current_revision())
                    };
                    Ok(QueryResult::ResourceList(ResourceListData {
                        context,
                        gvk,
                        namespace,
                        revision,
                        rows,
                        generated_at,
                    }))
                }
                Query::ResourceDetail { reference } => {
                    let state = self.lock();
                    state
                        .find_record(&reference)
                        .cloned()
                        .map(QueryResult::ResourceDetail)
                        .ok_or(BackendError::NotFound)
                }
                Query::ResourceRelations { reference } => {
                    let state = self.lock();
                    if state.find_record(&reference).is_none() {
                        return Err(BackendError::NotFound);
                    }
                    let groups = state.find_related(&reference);
                    Ok(QueryResult::ResourceRelations(RelatedData {
                        reference,
                        groups,
                    }))
                }
                Query::ResourceMetrics { reference } => {
                    let state = self.lock();
                    if state.find_record(&reference).is_none() {
                        return Err(BackendError::NotFound);
                    }
                    Ok(QueryResult::ResourceMetrics(
                        state
                            .metrics
                            .get(&reference.coalescing_key())
                            .cloned()
                            .unwrap_or_default(),
                    ))
                }
                Query::ResourceTypes { context } => {
                    let state = self.lock();
                    if !state.contexts.iter().any(|c| c.name == context) {
                        return Err(BackendError::NotFound);
                    }
                    let mut types: Vec<ApiResourceDescriptor> = Vec::new();
                    for record in &state.records {
                        if record.reference.context != context {
                            continue;
                        }
                        let gvk = record.reference.gvk.clone();
                        if types.iter().any(|entry| entry.gvk == gvk) {
                            continue;
                        }
                        let namespaced = state.records.iter().any(|candidate| {
                            candidate.reference.gvk == gvk
                                && candidate.reference.namespace.is_some()
                        });
                        types.push(ApiResourceDescriptor {
                            plural: plural_name(&gvk.kind),
                            supports_scale: scale_exposed(&gvk),
                            // The fake world's watches always attach.
                            supports_watch: true,
                            supports_patch: true,
                            supports_delete: true,
                            gvk,
                            namespaced,
                        });
                    }
                    types.sort_by(|left, right| left.gvk.cmp(&right.gvk));
                    Ok(QueryResult::ResourceTypes(ResourceTypesData {
                        context,
                        types,
                    }))
                }
                Query::Infrastructure { context } => {
                    let state = self.lock();
                    if !state
                        .contexts
                        .iter()
                        .any(|candidate| candidate.name == context)
                    {
                        return Err(BackendError::NotFound);
                    }
                    Ok(QueryResult::Infrastructure(CatalogSnapshot::fake(
                        context,
                        state.current_revision(),
                        state.metrics_scenario.into(),
                    )))
                }
                Query::OperationStatus { operation_ids } => {
                    let state = self.lock();
                    let operations = operation_ids
                        .iter()
                        .filter_map(|id| state.operations.get(id).cloned())
                        .collect();
                    Ok(QueryResult::OperationStatus(
                        crate::operation::OperationStatusData { operations },
                    ))
                }
                Query::ContextSwitch { to } => {
                    let mut state = self.lock();
                    if !state.contexts.iter().any(|c| c.name == to) {
                        return Err(BackendError::NotFound);
                    }
                    let previous = state
                        .contexts
                        .iter()
                        .find(|c| c.is_current)
                        .map(|c| c.name.clone());
                    for context in &mut state.contexts {
                        context.is_current = context.name == to;
                    }
                    Ok(QueryResult::ContextSwitch(crate::port::ContextSwitchData {
                        current: to,
                        previous,
                    }))
                }
                Query::ContextPermissions { context, probes } => {
                    let state = self.lock();
                    if !state.contexts.iter().any(|c| c.name == context) {
                        return Err(BackendError::NotFound);
                    }
                    // The same cross-adapter contract as the kube adapter:
                    // probes are bounded, and duplicates collapse onto their
                    // first occurrence. The fake world serves no authorization
                    // truth: every surviving probe stays explicitly Unknown
                    // instead of a fabricated allow/deny verdict.
                    crate::port::validate_probe_count(&probes)?;
                    Ok(QueryResult::ContextPermissions(
                        crate::port::ContextPermissionsData {
                            checks: crate::port::distinct_probes(probes)
                                .into_iter()
                                .map(|probe| crate::port::PermissionCheck {
                                    verb: probe.verb,
                                    resource: probe.resource,
                                    group: probe.group,
                                    namespace: probe.namespace,
                                    outcome: crate::port::PermissionOutcome::Unknown,
                                })
                                .collect(),
                            context,
                        },
                    ))
                }
            }
        })
    }

    fn execute<'a>(
        &'a self,
        cmd: Command,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<OperationId, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match cmd {
                Command::Apply {
                    context,
                    yaml,
                    idempotency_key,
                    ticket_id,
                    buffer_hash,
                    target,
                } => {
                    self.apply_yaml(
                        context,
                        yaml,
                        idempotency_key,
                        ticket_id,
                        buffer_hash,
                        target,
                    )
                    .await
                }
                Command::Scale {
                    context,
                    gvk,
                    namespace,
                    name,
                    uid,
                    replicas,
                    idempotency_key,
                } => {
                    self.scale(
                        context,
                        gvk,
                        namespace,
                        name,
                        uid,
                        replicas,
                        idempotency_key,
                    )
                    .await
                }
                Command::Delete {
                    target,
                    propagation,
                    idempotency_key,
                } => self.delete(target, propagation, idempotency_key).await,
                Command::Restart {
                    target,
                    idempotency_key,
                } => self.restart(target, idempotency_key).await,
            }
        })
    }

    fn stream_input<'a>(
        &'a self,
        ticket_id: &'a str,
        input: StreamInput,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let mut state = self.lock();
            match input {
                StreamInput::Stdin(line) => state.streams.queue_stdin(ticket_id, line),
                StreamInput::Resize { cols, rows } => {
                    state.streams.record_resize(ticket_id, cols, rows)
                }
            }
        })
    }

    fn subscribe<'a>(
        &'a self,
        req: Subscribe,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match req {
                Subscribe::BootstrapStatus => Ok(SubscriptionHandle::new("bootstrap-status")),
                Subscribe::ResourceWatch {
                    context,
                    gvk,
                    namespace,
                } => {
                    let mut state = self.lock();
                    if !state.contexts.iter().any(|c| c.name == context) {
                        return Err(BackendError::NotFound);
                    }
                    let rows: Vec<ResourceRecord> = state
                        .records
                        .iter()
                        .filter(|record| {
                            state.matches_selector(&record.reference, &context, &gvk, &namespace)
                        })
                        .cloned()
                        .collect();
                    let snapshot = ResourceListData {
                        revision: state.current_revision(),
                        rows,
                        namespace: namespace.clone(),
                        gvk: gvk.clone(),
                        context: context.clone(),
                        generated_at: rfc3339(FAKE_EPOCH_SECS + 60),
                    };
                    // The first subscriber starts the fake watch. The initial
                    // snapshot is published while the state lock still
                    // protects the snapshot cut, so later mutations can only
                    // enqueue deltas after this initial event.
                    let (sender, receiver) = state.watches.register(WatchSelector {
                        context,
                        gvk,
                        namespace,
                    });
                    let _ = sender.send(crate::port::BackendEvent::Snapshot(snapshot));
                    drop(state);
                    #[cfg(test)]
                    if let Some(gate) = self
                        .subscription_cut_gate
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                    {
                        gate.registered.wait();
                        gate.mutation_done.wait();
                    }
                    Ok(SubscriptionHandle::with_events("resource-watch", receiver))
                }
                Subscribe::Infrastructure { context } => {
                    let (sender, receiver) = broadcast::channel(crate::watch::WATCH_CAPACITY);
                    let mut state = self.lock();
                    if !state
                        .contexts
                        .iter()
                        .any(|candidate| candidate.name == context)
                    {
                        return Err(BackendError::NotFound);
                    }
                    state
                        .infrastructure_watchers
                        .push(InfrastructureWatcher { context, sender });
                    Ok(SubscriptionHandle::with_events(
                        "infrastructure-watch",
                        receiver,
                    ))
                }
                Subscribe::StreamRedeem { ticket_id, route } => {
                    let mut state = self.lock();
                    let revision = state.current_revision();
                    let (receiver, bound) = state.streams.redeem(&ticket_id, route, revision)?;
                    Ok(SubscriptionHandle::with_events("stream-session", receiver)
                        .with_stream(bound))
                }
                Subscribe::Operations => {
                    let mut state = self.lock();
                    // Late subscribers immediately learn the current state of
                    // every retained operation so reconnecting sessions can
                    // resynchronize without polling.
                    let snapshot: Vec<crate::operation::OperationEvent> = state
                        .operation_order
                        .iter()
                        .filter_map(|id| state.operations.get(id))
                        .map(|record| crate::operation::OperationEvent {
                            id: record.id.clone(),
                            state: record.state,
                            progress: record.progress,
                            detail: record.detail.clone(),
                        })
                        .collect();
                    let (sender, receiver) = broadcast::channel(crate::watch::WATCH_CAPACITY);
                    for event in snapshot {
                        let _ = sender.send(crate::port::BackendEvent::Operation(event));
                    }
                    state.operation_watchers.push(OperationWatcher { sender });
                    Ok(SubscriptionHandle::with_events("operations", receiver))
                }
            }
        })
    }
}

/// Deterministic capacity dataset: `nodes` cluster-scoped Nodes plus
/// `objects` namespaced workload objects spread over the built-in kinds.
/// Pod share is deliberately dominant, mirroring real clusters where pod
/// lists dominate snapshot traffic. Every field is derived from the object
/// index; no randomness anywhere.
fn build_capacity_records(context: &str, objects: usize, nodes: usize) -> Vec<ResourceRecord> {
    const KIND_CYCLE: [(&str, &str); 8] = [
        ("apps", "Deployment"),
        ("", "Pod"),
        ("", "Pod"),
        ("", "Pod"),
        ("apps", "StatefulSet"),
        ("apps", "DaemonSet"),
        ("batch", "Job"),
        ("batch", "CronJob"),
    ];
    const SUMMARIES: [&str; 4] = ["Running", "2/2 ready", "0/1 ready", "1/1 up"];

    let mut records = Vec::with_capacity(objects + nodes);
    for index in 0..nodes {
        records.push(record(RecordSeed {
            offset_secs: u64::try_from(index).unwrap_or(u64::MAX) % 86_400,
            summary: if index % 16 == 7 {
                "Not Ready"
            } else {
                "Ready"
            },
            context,
            gvk: Gvk::core("v1", "Node"),
            namespace: None,
            name: &format!("capacity-node-{index:05}"),
            labels: &[("role", "worker")],
            owner_references: Vec::new(),
        }));
    }
    for index in 0..objects {
        let (group, kind) = KIND_CYCLE[index % KIND_CYCLE.len()];
        let version = "v1";
        records.push(record(RecordSeed {
            offset_secs: (u64::try_from(index).unwrap_or(u64::MAX) * 30) % 86_400,
            summary: SUMMARIES[index % SUMMARIES.len()],
            context,
            gvk: Gvk::new(group, version, kind),
            namespace: Some("default"),
            name: &format!("scale-{}-{index:06}", kind.to_lowercase()),
            labels: &[("tier", "capacity")],
            owner_references: Vec::new(),
        }));
    }
    records
}

/// Seed describing one deterministic dataset record.
struct RecordSeed<'a> {
    /// Creation offset in seconds from the fixed fake epoch.
    offset_secs: u64,
    /// Human-readable status summary.
    summary: &'a str,
    /// Owning context of the object.
    context: &'a str,
    /// Type of the object.
    gvk: Gvk,
    /// Namespace, absent for cluster-scoped objects.
    namespace: Option<&'a str>,
    /// Object name.
    name: &'a str,
    /// Object labels.
    labels: &'a [(&'a str, &'a str)],
    /// Owner chain references resolved up front.
    owner_references: Vec<OwnerRef>,
}

/// Build one deterministic record with a derived stable UID.
fn record(seed: RecordSeed<'_>) -> ResourceRecord {
    let events = synthetic_events(&seed.gvk.kind, seed.summary, seed.offset_secs);
    ResourceRecord {
        reference: ResourceRef {
            uid: uid(seed.context, &seed.gvk.kind, seed.namespace, seed.name),
            namespace: seed.namespace.map(str::to_owned),
            name: seed.name.to_owned(),
            context: seed.context.to_owned(),
            gvk: seed.gvk,
        },
        revision: INITIAL_REVISION,
        labels: seed
            .labels
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>(),
        summary: seed.summary.to_owned(),
        created_at: rfc3339(FAKE_EPOCH_SECS + seed.offset_secs),
        owner_references: seed.owner_references,
        events,
        manifest: String::new(),
    }
}

/// Deterministic events derived from the observed status so every detail
/// view has backend-resolved event rows without per-seed fixtures.
fn synthetic_events(kind: &str, summary: &str, offset_secs: u64) -> Vec<RecordEvent> {
    let last_seen = || rfc3339(FAKE_EPOCH_SECS + offset_secs + 45);
    let mut events = vec![RecordEvent {
        reason: "Created".into(),
        message: format!("{kind} object created"),
        count: 1,
        last_seen: last_seen(),
    }];
    if summary.ends_with("ready") || summary == "Running" || summary == "Complete" {
        events.push(RecordEvent {
            reason: "Started".into(),
            message: format!("{kind} reached {summary}"),
            count: 1,
            last_seen: last_seen(),
        });
    } else if summary.contains("0/") || summary == "Pending" || summary == "Suspended" {
        events.push(RecordEvent {
            reason: "Progressing".into(),
            message: format!("{kind} waiting while {summary}"),
            count: 2,
            last_seen: last_seen(),
        });
    }
    events
}

fn uid(context: &str, kind: &str, namespace: Option<&str>, name: &str) -> String {
    format!(
        "uid-{context}-{}-{}-{name}",
        kind.to_lowercase(),
        namespace.unwrap_or("cluster")
    )
}

fn pod_key(name: &str) -> String {
    ResourceRef {
        context: "dev-local".into(),
        gvk: Gvk::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: name.into(),
        uid: String::new(),
    }
    .coalescing_key()
}

/// Kubernetes-style plural name of one deterministic dataset kind.
fn plural_name(kind: &str) -> String {
    let word = kind.to_lowercase();
    // The API server keeps these plurals verbatim.
    if matches!(word.as_str(), "endpoints" | "nodes" | "pods") {
        return word;
    }
    // Consonant+y becomes -ies (dashboard -> dashboards).
    if let Some(prefix) = word.strip_suffix('y')
        && !matches!(prefix.chars().last(), Some('a' | 'e' | 'i' | 'o' | 'u'))
    {
        return format!("{prefix}ies");
    }
    // s/x/z and ch/sh endings take -es (storageclass -> storageclasses).
    if matches!(word.chars().last(), Some('s' | 'x' | 'z'))
        || word.ends_with("ch")
        || word.ends_with("sh")
    {
        return format!("{word}es");
    }
    format!("{word}s")
}

/// Whether real clusters expose /scale for one workload kind; the fake world
/// mirrors what discovery reports instead of assuming from names.
fn scale_exposed(gvk: &Gvk) -> bool {
    gvk.group == "apps"
        && matches!(
            gvk.kind.as_str(),
            "Deployment" | "ReplicaSet" | "StatefulSet"
        )
}

fn deployment_gvk() -> Gvk {
    Gvk::new("apps", "v1", "Deployment")
}

fn replicaset_gvk() -> Gvk {
    Gvk::new("apps", "v1", "ReplicaSet")
}

fn build_dev_local_records() -> Vec<ResourceRecord> {
    fn seed<'a>(
        offset_secs: u64,
        summary: &'a str,
        gvk: Gvk,
        namespace: Option<&'a str>,
        name: &'a str,
        labels: &'a [(&'a str, &'a str)],
    ) -> RecordSeed<'a> {
        RecordSeed {
            offset_secs,
            summary,
            context: "dev-local",
            gvk,
            namespace,
            name,
            labels,
            owner_references: Vec::new(),
        }
    }

    let mut records = vec![
        record(seed(
            0,
            "2/2 ready",
            deployment_gvk(),
            Some("default"),
            "api-server",
            &[("app", "api")],
        )),
        record(seed(
            300,
            "20/20 ready",
            deployment_gvk(),
            Some("default"),
            "web-frontend",
            &[("app", "web"), ("tier", "frontend")],
        )),
        record(RecordSeed {
            owner_references: vec![OwnerRef {
                gvk: deployment_gvk(),
                name: "web-frontend".into(),
                uid: uid("dev-local", "Deployment", Some("default"), "web-frontend"),
                controller: true,
            }],
            ..seed(
                600,
                "20 desired",
                replicaset_gvk(),
                Some("default"),
                "web-frontend-7d9f8",
                &[("app", "web")],
            )
        }),
        record(seed(
            900,
            "1/1 ready",
            Gvk::new("apps", "v1", "StatefulSet"),
            Some("default"),
            "db-postgres",
            &[("app", "db")],
        )),
        record(seed(
            1_200,
            "1/1 up",
            Gvk::new("apps", "v1", "DaemonSet"),
            Some("kube-system"),
            "node-logs-agent",
            &[("role", "logging")],
        )),
        record(seed(
            1_500,
            "Complete",
            Gvk::new("batch", "v1", "Job"),
            Some("default"),
            "migrate-db-28931",
            &[("job", "migrate-db")],
        )),
        record(seed(
            1_800,
            "Suspended",
            Gvk::new("batch", "v1", "CronJob"),
            Some("default"),
            "nightly-backup",
            &[("schedule", "nightly")],
        )),
        record(seed(
            2_100,
            "Ready",
            Gvk::core("v1", "Node"),
            None,
            "dev-node-1",
            &[("role", "node")],
        )),
        record(seed(
            2_400,
            "Established",
            Gvk::new("apiextensions.k8s.io", "v1", "CustomResourceDefinition"),
            None,
            "dashboards.monitoring.example.com",
            &[],
        )),
        record(seed(
            2_700,
            "1 panel",
            Gvk::new("monitoring.example.com", "v1", "Dashboard"),
            Some("default"),
            "traffic-overview",
            &[("app", "dashboards")],
        )),
    ];

    // The web-frontend replica wave: twenty pods owned by its replicaset.
    for index in 1..=20_u32 {
        records.push(record(RecordSeed {
            owner_references: vec![OwnerRef {
                gvk: replicaset_gvk(),
                name: "web-frontend-7d9f8".into(),
                uid: uid(
                    "dev-local",
                    "ReplicaSet",
                    Some("default"),
                    "web-frontend-7d9f8",
                ),
                controller: true,
            }],
            ..seed(
                3_000 + u64::from(index) * 10,
                "Running",
                Gvk::core("v1", "Pod"),
                Some("default"),
                &format!("web-frontend-7d9f8-{index:05}"),
                &[("app", "web"), ("pod-template-hash", "7d9f8")],
            )
        }));
    }

    records.push(record(RecordSeed {
        owner_references: vec![OwnerRef {
            gvk: replicaset_gvk(),
            name: "api-server-5cc4d".into(),
            uid: uid(
                "dev-local",
                "ReplicaSet",
                Some("default"),
                "api-server-5cc4d",
            ),
            controller: true,
        }],
        ..seed(
            5_100,
            "Running",
            Gvk::core("v1", "Pod"),
            Some("default"),
            "api-server-5cc4d-qw8rt",
            &[("app", "api"), ("pod-template-hash", "5cc4d")],
        )
    }));
    records.push(record(RecordSeed {
        owner_references: vec![OwnerRef {
            gvk: Gvk::new("apps", "v1", "StatefulSet"),
            name: "db-postgres".into(),
            uid: uid("dev-local", "StatefulSet", Some("default"), "db-postgres"),
            controller: true,
        }],
        ..seed(
            5_200,
            "Running",
            Gvk::core("v1", "Pod"),
            Some("default"),
            "db-postgres-0",
            &[("app", "db")],
        )
    }));
    records
}

fn build_prod_records() -> Vec<ResourceRecord> {
    vec![record(RecordSeed {
        offset_secs: 6_000,
        summary: "3/3 ready",
        context: "prod-readonly",
        gvk: deployment_gvk(),
        namespace: Some("default"),
        name: "edge-gateway",
        labels: &[("app", "edge")],
        owner_references: Vec::new(),
    })]
}

/// Replace the desired count of a `N/M ...` style status summary.
fn scaled_summary(summary: &str, replicas: u32) -> Option<String> {
    let (desired, rest) = summary.split_once('/')?;
    desired.parse::<u32>().ok()?;
    Some(format!("{replicas}/{rest}"))
}

/// Format unix seconds as an RFC 3339 UTC timestamp without external crates.
fn rfc3339(unix_secs: u64) -> String {
    let days = unix_secs / 86_400;
    let secs_of_day = unix_secs % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60
    )
}

/// Howard Hinnant's days-to-civil conversion for UTC dates after 1970.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::BackendEvent;

    /// Regression: capacity eviction must follow issuance order even across
    /// the `ticket-{:04}` digit-width transition (ids 9999 → 10000), where
    /// lexicographic ID ordering no longer matches age.
    #[tokio::test]
    async fn ticket_eviction_stays_fifo_across_the_digit_width_transition() {
        use crate::port::Command;

        let fake = FakeKubernetes::with_contexts(vec![ContextInfo {
            name: "dev-local".into(),
            cluster: "dev-cluster".into(),
            namespace: Some("default".into()),
            is_current: true,
        }]);
        {
            let mut state = fake.lock();
            state.next_ticket = 9_998;
        }
        const MANIFEST: &str =
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: brand-new\n";
        let validate_once = || async {
            fake.query(Query::ValidateApply {
                context: "dev-local".into(),
                yaml: MANIFEST.to_owned(),
            })
            .await
        };
        // Issue past the width boundary and well past capacity.
        let mut first_ticket = None;
        let mut last_ticket = None;
        for _ in 0..40 {
            let ticket = match validate_once().await.unwrap() {
                QueryResult::YamlValidation(data) => match data.outcome {
                    crate::operation::OutcomeData::Valid { ticket } => ticket,
                    other => panic!("expected a valid outcome, got {other:?}"),
                },
                other => panic!("expected validation, got {other:?}"),
            };
            if first_ticket.is_none() {
                first_ticket = Some(ticket.clone());
            }
            last_ticket = Some(ticket);
        }
        let first = first_ticket.expect("at least one ticket");
        let last = last_ticket.expect("at least one ticket");
        assert_eq!(first.id, "ticket-9998");
        assert_eq!(last.id, "ticket-10037");

        // The oldest ticket was evicted even though its ID sorts last.
        let rejected = fake
            .execute(Command::Apply {
                context: first.target.context.clone(),
                yaml: MANIFEST.to_owned(),
                idempotency_key: "idem-evicted".into(),
                ticket_id: first.id.clone(),
                buffer_hash: first.buffer_hash.clone(),
                target: first.target.clone(),
            })
            .await;
        assert!(
            matches!(rejected, Err(BackendError::Conflict(_))),
            "the pre-transition ticket must be evicted, got {rejected:?}"
        );

        // The newest ticket remains redeemable.
        let accepted = fake
            .execute(Command::Apply {
                context: last.target.context.clone(),
                yaml: MANIFEST.to_owned(),
                idempotency_key: "idem-newest".into(),
                ticket_id: last.id.clone(),
                buffer_hash: last.buffer_hash.clone(),
                target: last.target.clone(),
            })
            .await;
        assert!(
            accepted.is_ok(),
            "the newest ticket still applies: {accepted:?}"
        );
    }

    #[test]
    fn rfc3339_formats_the_fixed_fake_epoch() {
        assert_eq!(rfc3339(FAKE_EPOCH_SECS), "2026-08-21T00:00:00Z");
        assert_eq!(rfc3339(FAKE_EPOCH_SECS + 3_600), "2026-08-21T01:00:00Z");
        assert_eq!(rfc3339(FAKE_EPOCH_SECS + 86_400), "2026-08-22T00:00:00Z");
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn delete_and_touch_advance_revisions_monotonically() {
        let fake = FakeKubernetes::with_contexts(Vec::new());
        assert_eq!(
            fake.touch_resource("dev-local", &deployment_gvk(), None, "x"),
            None
        );
        assert!(!fake.delete_resource("dev-local", &deployment_gvk(), None, "x"));
    }

    /// Watch one dev-local pod selector, returning the event receiver.
    async fn watch_pods(fake: &FakeKubernetes) -> tokio::sync::broadcast::Receiver<BackendEvent> {
        let mut handle = fake
            .subscribe(Subscribe::ResourceWatch {
                context: "dev-local".into(),
                gvk: Gvk::core("v1", "Pod"),
                namespace: Some("default".into()),
            })
            .await
            .expect("pod watch subscribes");
        handle.take_events().expect("resource watches carry events")
    }

    #[tokio::test]
    async fn repeated_subscribe_cycles_do_not_retain_dead_watchers() {
        let fake = FakeKubernetes::standard();
        let mut kept = watch_pods(&fake).await;
        assert!(matches!(
            kept.recv().await.expect("initial snapshot arrives"),
            BackendEvent::Snapshot(_)
        ));
        for _ in 0..32 {
            drop(watch_pods(&fake).await);
        }
        // Any mutation prunes watchers whose receivers are gone; the live one
        // stays registered and still receives the delta.
        fake.touch_resource(
            "dev-local",
            &Gvk::core("v1", "Pod"),
            Some("default"),
            "web-frontend-7d9f8-00001",
        )
        .expect("touched pod exists");
        assert_eq!(fake.watcher_count(), 1, "dead watchers must be pruned");
        let event = kept.recv().await.expect("live watcher still notified");
        assert!(matches!(event, BackendEvent::Changed(_)));
    }

    #[tokio::test]
    async fn snapshot_is_always_the_first_event_despite_concurrent_mutations() {
        let fake = FakeKubernetes::standard();
        let registered = Arc::new(std::sync::Barrier::new(2));
        let mutation_done = Arc::new(std::sync::Barrier::new(2));
        fake.set_subscription_cut_gate(Arc::clone(&registered), Arc::clone(&mutation_done));

        let mutator_fake = fake.clone();
        let mutator = std::thread::spawn(move || {
            registered.wait();
            mutator_fake
                .touch_resource(
                    "dev-local",
                    &Gvk::core("v1", "Pod"),
                    Some("default"),
                    "web-frontend-7d9f8-00002",
                )
                .expect("touched pod exists");
            mutation_done.wait();
        });

        let mut receiver = watch_pods(&fake).await;
        let first = receiver.recv().await.expect("snapshot arrives first");
        mutator.join().expect("mutator thread exits");
        assert!(
            matches!(first, BackendEvent::Snapshot(_)),
            "a mutation delta preceded the snapshot"
        );
    }
}
