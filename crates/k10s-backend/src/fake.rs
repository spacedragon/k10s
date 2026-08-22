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
    BackendError, BootstrapInfo, Command, ContextInfo, Gvk, KubernetesAccess, MetricsSample,
    OperationId, OwnerRef, Query, QueryResult, ResourceListData, ResourceRecord, ResourceRef,
    Subscribe, SubscriptionHandle,
};

/// Unix seconds for the fixed fake epoch `2026-08-21T00:00:00Z`.
const FAKE_EPOCH_SECS: u64 = 1_787_270_400;
/// Revision assigned to every object in the pristine dataset.
const INITIAL_REVISION: u64 = 1_000;
/// Broadcast capacity per resource watch; bounded like every other queue.
const WATCH_CAPACITY: usize = 128;

/// A registered resource watcher.
#[derive(Debug)]
struct Watcher {
    context: String,
    gvk: Gvk,
    namespace: Option<String>,
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
    watchers: Vec<Watcher>,
    infrastructure_watchers: Vec<InfrastructureWatcher>,
    metrics_scenario: FakeMetricsScenario,
    revision: u64,
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
        self.watchers.retain(|watcher| {
            if watcher.sender.receiver_count() == 0 {
                return false;
            }
            if watcher.context == reference.context
                && watcher.gvk == reference.gvk
                && watcher
                    .namespace
                    .as_ref()
                    .is_none_or(|watched| Some(watched.as_str()) == reference.namespace.as_deref())
            {
                let _ = watcher.sender.send(event.clone());
            }
            true
        });
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
                watchers: Vec::new(),
                infrastructure_watchers: Vec::new(),
                metrics_scenario,
                revision: INITIAL_REVISION,
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
                watchers: Vec::new(),
                infrastructure_watchers: Vec::new(),
                metrics_scenario: FakeMetricsScenario::Full,
                revision: INITIAL_REVISION,
            })),
            #[cfg(test)]
            subscription_cut_gate: Arc::new(Mutex::new(None)),
        }
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

    /// Number of registered watchers; test-only observability for pruning.
    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.lock().watchers.len()
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
                Query::ValidateApply { .. } => Err(BackendError::unsupported("validate.apply")),
                Query::StreamTicket { .. } => Err(BackendError::unsupported("stream.ticket")),
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
            }
        })
    }

    fn execute<'a>(
        &'a self,
        cmd: Command,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<OperationId, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = cmd;
            Err(BackendError::unsupported("execute"))
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
                    let (sender, receiver) = broadcast::channel(WATCH_CAPACITY);
                    {
                        let mut state = self.lock();
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
                        let snapshot = ResourceListData {
                            revision: state.current_revision(),
                            rows,
                            namespace: namespace.clone(),
                            gvk: gvk.clone(),
                            context: context.clone(),
                            generated_at: rfc3339(FAKE_EPOCH_SECS + 60),
                        };
                        state.watchers.push(Watcher {
                            context,
                            gvk,
                            namespace,
                            sender: sender.clone(),
                        });
                        // Publish while the state lock still protects the
                        // snapshot cut: later mutations can only enqueue
                        // deltas after this initial event.
                        let _ = sender.send(crate::port::BackendEvent::Snapshot(snapshot));
                    }
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
                    let (sender, receiver) = broadcast::channel(WATCH_CAPACITY);
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
            }
        })
    }
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
    }
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
