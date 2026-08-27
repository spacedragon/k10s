//! Real Kubernetes adapter backed by kube-rs.
//!
//! Kube-rs types are confined to this module tree: the rest of k10s only ever
//! sees normalized port types and [`AdapterError`]s. Bootstrap is served from
//! a committed, credential-free context registry; cluster-facing discovery runs
//! through kube-rs against injected clients in tests or live API servers in
//! production, cached per context behind bounded, refreshable state.

mod auth;
mod auth_observer;
mod config;
mod create;
mod discovery;
mod events;
mod exec;
mod infrastructure;
mod logs;
pub(crate) mod metrics;
mod mutate;
mod normalize;
mod owners;
mod permissions;
mod port_forward;
mod read;
mod validation;
mod watch;

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};

pub use self::discovery::{DISCOVERY_TTL, MAX_CACHED_CONTEXTS};

use crate::port::{
    AdapterError, BackendError, BootstrapInfo, Command, ContextInfo, ContextPermissionsData,
    ContextSwitchData, Gvk, KubernetesAccess, OperationId, Query, QueryResult, ResourceListData,
    ResourceTypesData, StreamInput, Subscribe, SubscriptionHandle,
};
use crate::runtime::ContextRegistry;
use crate::runtime::cluster::{ClusterMetrics, ClusterWatches};
use crate::runtime::supervisor::WatchSource;
use crate::watch::WatchSelector;

/// A test-only override choosing scripted watch sources per selection.
///
/// Returning `None` falls back to the real kube-rs source so a script can
/// cover only the selections it cares about. The public alias lives in
/// [`crate::runtime`].
#[cfg(feature = "testkit")]
type WatchScript = crate::runtime::RuntimeWatchScript;

/// Debug wrapper over the scripted-watch holder (closures are not Debug).
#[cfg(feature = "testkit")]
#[derive(Clone)]
struct ScriptedWatches(Arc<std::sync::Mutex<Option<WatchScript>>>);

/// Stable per-context construction guards. The map is bounded by the
/// kubeconfig context set, and the guard itself is never held while this map
/// mutex is locked.
#[derive(Debug, Default)]
struct ClientBuildLocks {
    locks: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ClientBuildLocks {
    fn for_context(&self, context: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            locks
                .entry(context.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }
}

#[cfg(feature = "testkit")]
impl std::fmt::Debug for ScriptedWatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScriptedWatches")
    }
}

/// Real Kubernetes adapter that loads contexts from a kubeconfig file.
///
/// The committed [`ContextRegistry`] is bootstrap state; per-context cluster
/// clients, the bounded discovery catalog cache, and the supervised demand-
/// driven watch runtime are runtime state. Kube-rs types never leave this
/// module tree.
pub struct KubeAdapter {
    /// Committed bootstrap state behind a lock so context switches can swap
    /// the current-context marker atomically; readers always see one
    /// consistent registry snapshot.
    registry: Arc<StdMutex<ContextRegistry>>,
    /// Shared cluster client per context name: pre-injected in tests through
    /// [`Self::with_cluster_clients`], otherwise built on first use from the
    /// stored kubeconfig and cached here for reuse.
    clients: Arc<tokio::sync::Mutex<HashMap<String, kube::Client>>>,
    /// Coalesces lazy client construction so concurrent first use executes a
    /// credential plugin at most once.
    client_build_locks: ClientBuildLocks,
    /// Parsed kubeconfig seeding per-context client construction; absent when
    /// testkit pre-injected every context's client instead.
    kubeconfig_source: Option<kube::config::Kubeconfig>,
    /// Bounded per-context discovery catalog cache (LRU eviction, TTL refresh).
    catalogs: StdMutex<CatalogCache>,
    /// One immutable live discovery generation per context. Callers clone the
    /// shared future under this short-held lock, then await it without locks.
    catalog_flights: Arc<StdMutex<CatalogFlights>>,
    /// Supervised demand-driven watch runtime: one task per selection with
    /// atomic summary caches and lingered teardown.
    watches: ClusterWatches,
    /// Demand-driven resource-metrics poll registry: one collector per
    /// context, started only by active consumer requests and exited after a
    /// linger window without them.
    metrics: ClusterMetrics,
    /// Background availability transitions consumed by bootstrap-status
    /// subscriptions on every connected frontend.
    availability_events: tokio::sync::broadcast::Sender<crate::port::BackendEvent>,
    /// Serializes complete switch transactions: prepare, live destination
    /// validation, commit, and retirement run under one guard so overlapping
    /// switches cannot interleave their phases or retire the wrong runtime.
    switch_lock: tokio::sync::Mutex<()>,
    /// Serializes authoritative Bootstrap/Refresh probes.
    refresh_lock: tokio::sync::Mutex<()>,
    /// Shared lifecycle/idempotency authority for every real mutation.
    operations: crate::operation::OperationEngine,
    /// Process-local validation authority. Restarting the adapter drops every
    /// issued ticket, and the store itself enforces TTL and capacity bounds.
    validation_tickets: StdMutex<crate::validation::ticket::TicketStore>,
    /// Bounded single-use authority for real Kubernetes log streams.
    stream_tickets: logs::StreamTickets,
    /// Active real exec sessions accept only bounded stdin/resize commands.
    exec_sessions: std::sync::Arc<exec::ExecSessions>,
    /// Test-only scripted watch sources overriding the real kube-rs path.
    #[cfg(feature = "testkit")]
    watch_scripts: ScriptedWatches,
}

impl std::fmt::Debug for KubeAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Cluster clients own transport state; only the config source is reported.
        let mut debug = f.debug_struct("KubeAdapter");
        #[cfg(feature = "testkit")]
        let debug = debug.field("scripted_watches", &self.watch_scripts);
        debug
            .field("registry", &self.registry)
            .field("has_kubeconfig_source", &self.kubeconfig_source.is_some())
            .finish()
    }
}

impl KubeAdapter {
    /// Build an adapter from an explicit kubeconfig path or standard
    /// discovery (`KUBECONFIG`, then `~/.kube/config`).
    ///
    /// Follows the prepare-then-commit protocol: loading and validation run
    /// first (prepare), and only a complete, valid registry is installed as
    /// bootstrap state (commit). Any failure returns a normalized
    /// [`AdapterError`] without leaving partial state. Per-context cluster
    /// clients are built lazily on first discovery use so startup stays offline.
    pub fn from_kubeconfig(path: Option<&Path>) -> Result<Self, AdapterError> {
        // Prepare: load and validate credential-free summaries off-line; keep
        // the parsed kube-rs config as the lazy per-context client source.
        let (prepared, kubeconfig) = config::load_with_source(path)?;
        // Commit: install the complete registry and shared runtime state.
        let (availability_events, _) = tokio::sync::broadcast::channel(32);
        Ok(Self {
            registry: Arc::new(StdMutex::new(ContextRegistry::prepare(prepared)?)),
            clients: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            client_build_locks: ClientBuildLocks::default(),
            kubeconfig_source: Some(kubeconfig),
            catalogs: StdMutex::new(CatalogCache::new()),
            catalog_flights: Arc::new(StdMutex::new(CatalogFlights::default())),
            watches: ClusterWatches::default(),
            metrics: ClusterMetrics::default(),
            availability_events,
            switch_lock: tokio::sync::Mutex::new(()),
            refresh_lock: tokio::sync::Mutex::new(()),
            operations: crate::operation::OperationEngine::default(),
            validation_tickets: StdMutex::new(crate::validation::ticket::TicketStore::new()),
            stream_tickets: logs::StreamTickets::new(),
            exec_sessions: std::sync::Arc::new(exec::ExecSessions::default()),
            #[cfg(feature = "testkit")]
            watch_scripts: ScriptedWatches(Arc::new(std::sync::Mutex::new(None))),
        })
    }

    /// Build an adapter around pre-injected per-context cluster clients.
    ///
    /// Test seam for recorded tower services (see the `testkit` module): every
    /// context must be paired with exactly one client, and vice versa — the
    /// same prepare-then-commit guarantee as kubeconfig-based builds.
    #[cfg(feature = "testkit")]
    pub fn with_cluster_clients<S: Into<String>>(
        contexts: Vec<ContextInfo>,
        clients: impl IntoIterator<Item = (S, kube::Client)>,
    ) -> Result<Self, AdapterError> {
        let registry = ContextRegistry::prepare(contexts)?;

        let mut client_map = HashMap::new();
        for (raw_name, client) in clients {
            let name: String = raw_name.into();
            if !client_map.insert(name.clone(), client).is_none() {
                return Err(AdapterError::InvalidContextSummaries {
                    detail: format!("duplicate cluster client for context '{name}'"),
                });
            }
        }

        // Fail closed on wiring gaps instead of serving half a world.
        let complete = registry.contexts().len() == client_map.len()
            && registry
                .context_names()
                .iter()
                .all(|name| client_map.contains_key(*name));
        if !complete {
            return Err(AdapterError::InvalidContextSummaries {
                detail: "every context needs exactly one cluster client".into(),
            });
        }

        let (availability_events, _) = tokio::sync::broadcast::channel(32);
        Ok(Self {
            registry: Arc::new(StdMutex::new(registry)),
            clients: Arc::new(tokio::sync::Mutex::new(client_map)),
            client_build_locks: ClientBuildLocks::default(),
            kubeconfig_source: None,
            catalogs: StdMutex::new(CatalogCache::new()),
            catalog_flights: Arc::new(StdMutex::new(CatalogFlights::default())),
            watches: ClusterWatches::default(),
            metrics: ClusterMetrics::default(),
            availability_events,
            switch_lock: tokio::sync::Mutex::new(()),
            refresh_lock: tokio::sync::Mutex::new(()),
            operations: crate::operation::OperationEngine::default(),
            validation_tickets: StdMutex::new(crate::validation::ticket::TicketStore::new()),
            stream_tickets: logs::StreamTickets::new(),
            exec_sessions: std::sync::Arc::new(exec::ExecSessions::default()),
            #[cfg(feature = "testkit")]
            watch_scripts: ScriptedWatches(Arc::new(std::sync::Mutex::new(None))),
        })
    }

    /// Install a test-only scripted watch source factory, overriding the
    /// real kube-rs list/watch path per selection. Returning `None` from
    /// the script falls back to the real source for that selection.
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn with_scripted_watches(self, script: WatchScript) -> Self {
        *self
            .watch_scripts
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(script);
        self
    }

    /// Override the metrics collector's linger and poll cadence.
    ///
    /// Test seam so lifecycle tests run at assertable timescales instead of
    /// the production defaults (`METRICS_LINGER`, `METRICS_POLL_INTERVAL`).
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn with_metrics_timing(
        mut self,
        linger: std::time::Duration,
        poll_interval: std::time::Duration,
    ) -> Self {
        self.metrics = ClusterMetrics::new(linger, poll_interval);
        self
    }

    /// Shared handle to this adapter's metrics registry.
    ///
    /// Test/observability seam: clones observe the same collector state the
    /// adapter serves, so diagnostics and tests can inspect cached cuts and
    /// collector liveness (`snapshot_of` never touches a linger deadline)
    /// while a kernel owns the adapter itself.
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn metrics_registry(&self) -> ClusterMetrics {
        self.metrics.clone()
    }

    /// Shared handle to this adapter's watch registry.
    ///
    /// Test/observability seam: clones observe the same live selections the
    /// adapter serves, so tests can assert warm state and retirement while a
    /// kernel owns the adapter itself.
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn watches_registry(&self) -> ClusterWatches {
        self.watches.clone()
    }

    /// Shared operation engine used by recorded-service tests to model real
    /// submission lifecycles through the adapter's query/subscription seam.
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn operation_engine(&self) -> crate::operation::OperationEngine {
        self.operations.clone()
    }

    /// Number of active request guards joined to one catalog generation.
    ///
    /// Test-only synchronization seam for cancellation and panic assertions;
    /// registry ownership of the shared future is deliberately not counted.
    #[cfg(feature = "testkit")]
    #[must_use]
    pub fn catalog_waiter_count(&self, context: &str) -> usize {
        self.catalog_flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .get(context)
            .map_or(0, |flight| flight.active_waiters)
    }
}

impl KubernetesAccess for KubeAdapter {
    fn port_forward_connector(&self) -> Option<crate::port_forward::PortForwardConnector> {
        Some(self.port_forward_connector())
    }

    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match req {
                Query::Bootstrap => Ok(QueryResult::Bootstrap(BootstrapInfo {
                    contexts: self.refresh_context_availability().await,
                })),
                // Discovery is live in this task through the cached catalog path.
                Query::ResourceTypes { context } => self.resource_types(&context).await,
                // Prepare-then-commit context switching with advisory
                // permission projection.
                Query::ContextSwitch { to } => self.context_switch(to).await,
                Query::ContextPermissions {
                    context,
                    probes: checks,
                } => self.context_permissions(&context, checks).await,
                // Cluster-facing capabilities arrive with later Plan 3 tasks;
                // until then they are typed, not guessed.
                Query::ValidateApply { context, yaml } => self.validate_apply(context, yaml).await,
                Query::StreamTicket { stream } => self.issue_stream_ticket(stream).await,
                Query::ResourceList {
                    context,
                    gvk,
                    namespace,
                } => self.resource_list(context, gvk, namespace).await,
                Query::ResourceDetail { reference } => self.resource_detail(reference).await,
                Query::ResourceMetrics { reference } => self.resource_metrics(reference).await,
                Query::ResourceRelations { reference } => self.resource_relations(reference).await,
                Query::Infrastructure { context } => self.infrastructure(&context).await,
                Query::OperationStatus { operation_ids } => Ok(QueryResult::OperationStatus(
                    self.operations.status(&operation_ids),
                )),
            }
        })
    }

    fn execute<'a>(
        &'a self,
        cmd: Command,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<OperationId, BackendError>> + Send + 'a>>
    {
        Box::pin(async move { self.execute_mutation(cmd).await })
    }

    fn subscribe<'a>(
        &'a self,
        req: Subscribe,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match req {
                Subscribe::BootstrapStatus => Ok(SubscriptionHandle::with_events(
                    "bootstrap-status",
                    self.availability_events.subscribe(),
                )),
                Subscribe::ResourceWatch {
                    context,
                    gvk,
                    namespace,
                } => self.resource_watch(context, gvk, namespace).await,
                // Live infrastructure updates are refreshed explicitly by
                // the client for now. Accepting the subscription keeps the
                // capability available instead of replacing a successful
                // snapshot with an unsupported-capability placeholder.
                Subscribe::Infrastructure { context } => {
                    if !self.knows_context(&context) {
                        Err(BackendError::NotFound)
                    } else {
                        Ok(SubscriptionHandle::new(format!("infrastructure:{context}")))
                    }
                }
                Subscribe::StreamRedeem { ticket_id, route } => {
                    self.redeem_stream_ticket(ticket_id, route).await
                }
                Subscribe::Operations => Ok(SubscriptionHandle::with_events(
                    "operations",
                    self.operations.subscribe(),
                )),
            }
        })
    }

    fn stream_input<'a>(
        &'a self,
        ticket_id: &'a str,
        input: StreamInput,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async move { self.exec_sessions.send(ticket_id, input).await })
    }
}

impl KubeAdapter {
    /// Assemble the desktop overview from fresh normalized resource lists.
    /// Missing API kinds are skipped (clusters legitimately differ), while
    /// authorization and transport failures remain visible to the caller.
    async fn infrastructure(&self, context: &str) -> Result<QueryResult, BackendError> {
        if !self.knows_context(context) {
            return Err(BackendError::NotFound);
        }
        let catalog = self.catalog_for(context).await?;
        let overview_kinds = [
            "Node",
            "Pod",
            "Deployment",
            "StatefulSet",
            "DaemonSet",
            "Job",
            "CronJob",
        ];
        let descriptors = catalog
            .types
            .iter()
            .filter(|entry| overview_kinds.contains(&entry.gvk.kind.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let mut records = Vec::new();
        for descriptor in descriptors {
            match self
                .resource_list(context.to_owned(), descriptor.gvk, None)
                .await
            {
                Ok(QueryResult::ResourceList(list)) => records.extend(list.rows),
                Ok(_) => unreachable!("resource_list always returns a resource list"),
                Err(BackendError::NotFound) => continue,
                Err(error) => return Err(error),
            }
        }
        let (claims, volumes, classes) = self.storage_inventory(context).await?;
        let revision = self.watches.next_revision();
        Ok(QueryResult::Infrastructure(
            crate::catalog::CatalogSnapshot::live(
                context,
                revision,
                crate::runtime::now_rfc3339(),
                records,
                claims,
                volumes,
                classes,
            ),
        ))
    }

    /// Open a supervised demand-driven resource watch for one selection.
    ///
    /// The selection must name a known context and a type the discovery
    /// catalog lists; unknown selections are typed not-founds. The first
    /// subscriber starts one supervised task that relists, attaches a live
    /// watch at the list's opaque resourceVersion, and feeds an atomic
    /// summary cache behind a bounded broadcast channel.
    async fn resource_watch(
        &self,
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
    ) -> Result<SubscriptionHandle, BackendError> {
        if !self.knows_context(&context) {
            return Err(BackendError::NotFound);
        }
        let catalog = self.catalog_for(&context).await?;
        let Some(descriptor) = catalog.types.iter().find(|entry| entry.gvk == gvk) else {
            return Err(BackendError::NotFound);
        };
        // Scope and capability checks come before any task is spawned: a
        // cluster-scoped type cannot honor a namespace restriction, and a
        // list-only type could never attach a live stream — accepting either
        // would relist-loop against the API server forever.
        if namespace.is_some() && !descriptor.namespaced {
            return Err(BackendError::Conflict(
                "the requested type is cluster-scoped and cannot be watched within one namespace"
                    .into(),
            ));
        }
        if !descriptor.supports_watch {
            return Err(BackendError::unsupported("resource.watch"));
        }

        #[cfg(feature = "testkit")]
        let scripted = self
            .watch_scripts
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|script| script(&gvk, namespace.as_deref()));

        #[cfg(feature = "testkit")]
        let source: Arc<dyn WatchSource> = if let Some(source) = scripted {
            source
        } else {
            Arc::new(
                self.real_watch_source(&context, descriptor, namespace.clone())
                    .await?,
            )
        };

        #[cfg(not(feature = "testkit"))]
        let source: Arc<dyn WatchSource> = Arc::new(
            self.real_watch_source(&context, descriptor, namespace.clone())
                .await?,
        );

        let receiver = self.watches.subscribe(
            WatchSelector {
                context,
                gvk,
                namespace,
            },
            source,
        );
        Ok(SubscriptionHandle::with_events(
            "kube-resource-watch",
            receiver,
        ))
    }

    /// Build the real kube-rs list/watch source for one selection.
    async fn real_watch_source(
        &self,
        context: &str,
        descriptor: &crate::port::ApiResourceDescriptor,
        namespace: Option<String>,
    ) -> Result<watch::KubeWatchSource, BackendError> {
        let client = self.cluster_client(context).await?;
        Ok(watch::KubeWatchSource::new(
            client,
            context.to_owned(),
            descriptor.gvk.clone(),
            descriptor.plural.clone(),
            descriptor.namespaced,
            namespace,
        ))
    }

    /// Serve one on-demand list snapshot for one selection.
    ///
    /// Unknown contexts or types are typed not-founds; a namespace on a
    /// cluster-scoped type is a typed conflict. Rows are read fresh from the
    /// cluster, normalized into view models, sorted by stable identity, and
    /// stamped with one revision from the same monotonic counter the watch
    /// runtime publishes with.
    async fn resource_list(
        &self,
        context: String,
        gvk: Gvk,
        namespace: Option<String>,
    ) -> Result<QueryResult, BackendError> {
        if !self.knows_context(&context) {
            return Err(BackendError::NotFound);
        }
        let catalog = self.catalog_for(&context).await?;
        let Some(descriptor) = catalog.types.iter().find(|entry| entry.gvk == gvk) else {
            return Err(BackendError::NotFound);
        };
        if namespace.is_some() && !descriptor.namespaced {
            return Err(BackendError::Conflict(
                "the requested type is cluster-scoped and cannot be listed within one namespace"
                    .into(),
            ));
        }

        let client = self.cluster_client(&context).await?;
        let read = read::list_resource(
            &client,
            &context,
            &gvk,
            &descriptor.plural,
            descriptor.namespaced,
            namespace.as_deref(),
        )
        .await?;

        let revision = self.watches.next_revision();
        let mut records: Vec<_> = read
            .rows
            .iter()
            .map(|row| crate::runtime::record_from_row(row, revision))
            .collect();
        records.sort_by(|left, right| left.reference.cmp(&right.reference));
        Ok(QueryResult::ResourceList(ResourceListData {
            context,
            gvk,
            namespace,
            revision,
            rows: records,
            generated_at: crate::runtime::now_rfc3339(),
        }))
    }

    /// Serve one exact-identity detail read.
    ///
    /// The object is fetched by name, its UID is re-checked against the
    /// caller's reference (a reused name with another UID never resolves),
    /// and the response carries tailored normalized fields, newest-first
    /// events from both Event API variants, and YAML bound to the fetched
    /// UID/resourceVersion. The kernel composes related rows on top.
    async fn resource_detail(
        &self,
        reference: crate::port::ResourceRef,
    ) -> Result<QueryResult, BackendError> {
        let client = self.detail_client(&reference).await?;
        let descriptor = self
            .descriptor_for(&reference.context, &reference.gvk)
            .await?;
        if reference.namespace.is_some() && !descriptor.namespaced {
            return Err(BackendError::NotFound);
        }

        let read = match read::get_resource(
            &client,
            &descriptor.gvk,
            &descriptor.plural,
            descriptor.namespaced,
            reference.namespace.as_deref(),
            &reference,
        )
        .await
        {
            Ok(read) => read,
            Err(BackendError::NotFound) => {
                // An authoritative 404 or UID mismatch reconciles this exact
                // stale identity just as surely as a successful read. It does
                // not establish whether an outcome-unknown write succeeded,
                // but it may safely release the name-scoped retry gate.
                self.operations.refresh_scope(&reference.coalescing_key());
                return Err(BackendError::NotFound);
            }
            Err(error) => return Err(error),
        };

        let revision = self.watches.next_revision();
        let mut record = crate::runtime::record_from_row(&read.row, revision);
        let (events, condition) =
            events::events_for(&client, &reference, descriptor.namespaced).await;
        record.events = events;
        record.events_condition = condition;
        record.manifest = read.manifest;
        self.operations.refresh_scope(&reference.coalescing_key());
        Ok(QueryResult::ResourceDetail(record))
    }

    /// Serve one availability-gated pod metrics sample.
    ///
    /// The exact identity is verified first (a reused name with another UID
    /// is the same typed not-found as a vanished pod), then the context's
    /// metrics collector is engaged as an active consumer — starting it on
    /// first use, touching its linger deadline otherwise. The answer maps the
    /// latest collected cut honestly onto the port type: absent samples stay
    /// absent, stale cuts withhold their values while keeping their age, and
    /// nothing is ever inferred from requests or capacity.
    async fn resource_metrics(
        &self,
        reference: crate::port::ResourceRef,
    ) -> Result<QueryResult, BackendError> {
        if !self.knows_context(&reference.context) {
            return Err(BackendError::NotFound);
        }
        // Metrics identities exist only for pods.
        if reference.gvk != Gvk::core("v1", "Pod") {
            return Err(BackendError::NotFound);
        }
        let client = self.detail_client(&reference).await?;
        metrics::verify_pod_identity(&client, &reference).await?;

        let context = reference.context.clone();
        let snapshot = self
            .metrics
            .collect_for_consumer(&context, || {
                Arc::new(metrics::MetricsSource::new(client.clone(), context.clone()))
            })
            .await;
        Ok(QueryResult::ResourceMetrics(metrics::sample_for_reference(
            snapshot.as_deref(),
            &reference,
        )))
    }

    /// Serve one controller-UID relation traversal.
    ///
    /// The target's exact identity is verified first; candidates are swept
    /// once over the context's namespaced catalog inside the target's
    /// namespace (cluster-wide for cluster-scoped targets), then resolved
    /// transitively by controller owner UIDs only.
    async fn resource_relations(
        &self,
        reference: crate::port::ResourceRef,
    ) -> Result<QueryResult, BackendError> {
        let client = self.detail_client(&reference).await?;
        // Existence check with UID equality: relations on a vanished or
        // recreated object are typed not-founds, never guessed empties.
        let _ = read::get_resource(
            &client,
            &reference.gvk,
            &self
                .descriptor_for(&reference.context, &reference.gvk)
                .await?
                .plural,
            reference.namespace.is_some(),
            reference.namespace.as_deref(),
            &reference,
        )
        .await?;

        let candidates = owners::sweep_candidates(
            &client,
            &reference.context,
            &self.catalog_for(&reference.context).await?.types,
            reference.namespace.as_deref(),
        )
        .await;
        let revision = self.watches.next_revision();
        Ok(QueryResult::ResourceRelations(owners::related_data(
            reference,
            &candidates,
            revision,
        )))
    }

    /// Validate that a reference names a known context, then resolve its
    /// shared cluster client.
    async fn detail_client(
        &self,
        reference: &crate::port::ResourceRef,
    ) -> Result<kube::Client, BackendError> {
        if !self.knows_context(&reference.context) {
            return Err(BackendError::NotFound);
        }
        self.cluster_client(&reference.context).await
    }

    /// Resolve one context's discovery descriptor for a GVK.
    async fn descriptor_for(
        &self,
        context: &str,
        gvk: &Gvk,
    ) -> Result<crate::port::ApiResourceDescriptor, BackendError> {
        let catalog = self.catalog_for(context).await?;
        catalog
            .types
            .iter()
            .find(|entry| entry.gvk == *gvk)
            .cloned()
            .ok_or(BackendError::NotFound)
    }

    /// Serve one context's resource catalog through discovery.
    ///
    /// Unknown contexts are typed not-found; a fresh cached catalog is served
    /// without network traffic, and expired or invalidated catalogs trigger a
    /// re-discovery that replaces them under the same bounds as before.
    async fn resource_types(&self, context: &str) -> Result<QueryResult, BackendError> {
        Ok(QueryResult::ResourceTypes(self.catalog_for(context).await?))
    }

    /// Whether `context` names a committed registry entry.
    fn knows_context(&self, context: &str) -> bool {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .find(context)
            .is_some()
    }

    /// Switch the current context through the prepare-then-commit protocol.
    ///
    /// The whole transaction runs under one switch guard so overlapping
    /// switches serialize: each one captures `previous`, validates, commits,
    /// and retires as an atomic unit relative to every other switch. Prepare
    /// validates twice: the destination must be a known registry entry (before
    /// any traffic), and its minimal read path must actually work *now* — its
    /// client raises and discovery runs live against it even when a fresh
    /// cached catalog exists. A failed prepare returns a sanitized error with
    /// nothing observable moved. Only then does the commit swap the current
    /// marker atomically, followed by retirement of the replaced context's
    /// live runtime so no watcher or poller outlives its context's relevance.
    async fn context_switch(&self, to: String) -> Result<QueryResult, BackendError> {
        let _switch = self.switch_lock.lock().await;
        // Prepare (registry): reject unknown destinations off-line.
        let prepared = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .prepare_switch(&to)?;
        // Prepare (cluster): validate the destination read path with live
        // traffic — a fresh cached catalog proves nothing about right now.
        self.resolve_catalog(&to, CatalogPolicy::ForceLive).await?;
        // Commit: install the new current marker as one atomic swap.
        let previous = {
            let mut registry = self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry.commit_switch(prepared)?
        };
        // Retire: end the replaced context's watchers, collectors, and cached
        // catalog immediately. A redundant switch to the already-current
        // context retires nothing.
        if let Some(retired) = previous.as_deref().filter(|name| *name != to) {
            self.watches.retire_context(retired);
            self.metrics.retire_context(retired);
            self.invalidate_discovery(retired);
        }
        Ok(QueryResult::ContextSwitch(ContextSwitchData {
            current: to,
            previous,
        }))
    }

    /// Project advisory RBAC capabilities for one context.
    ///
    /// Every distinct probe becomes exactly one SelfSubjectAccessReview
    /// through the context's own client. The projection never fails the
    /// query over review outcomes — unavailable reviews degrade to explicit
    /// Unknown checks — and it never gates later reads or mutations.
    async fn context_permissions(
        &self,
        context: &str,
        probes: Vec<crate::port::PermissionProbe>,
    ) -> Result<QueryResult, BackendError> {
        if !self.knows_context(context) {
            return Err(BackendError::NotFound);
        }
        crate::port::validate_probe_count(&probes)?;
        let client = self.cluster_client(context).await?;
        let checks = permissions::project_capabilities(&client, probes).await;
        Ok(QueryResult::ContextPermissions(ContextPermissionsData {
            context: context.to_owned(),
            checks,
        }))
    }

    /// Resolve one context's discovery catalog through the bounded cache.
    async fn catalog_for(&self, context: &str) -> Result<ResourceTypesData, BackendError> {
        self.resolve_catalog(context, CatalogPolicy::UseFreshCache)
            .await
    }

    /// Resolve one context's catalog according to the caller's freshness
    /// policy while sharing any already-running live discovery generation.
    async fn resolve_catalog(
        &self,
        context: &str,
        policy: CatalogPolicy,
    ) -> Result<ResourceTypesData, BackendError> {
        if !self.knows_context(context) {
            return Err(BackendError::NotFound);
        }
        if let Some(entry) = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .find(context)
            .filter(|entry| entry.availability == crate::port::ContextAvailability::Unavailable)
        {
            return Err(BackendError::ContextUnavailable {
                context: entry.name.clone(),
                reason: entry
                    .unavailable_reason
                    .clone()
                    .unwrap_or_else(|| "credential plugin is unavailable".into()),
            });
        }

        if policy == CatalogPolicy::UseFreshCache {
            // Fast path: a fresh catalog already cached for this context.
            let cached = {
                let mut catalogs = self
                    .catalogs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                catalogs.fresh(context).cloned()
            };
            if let Some(data) = cached {
                return Ok(data);
            }
        }

        let started = std::time::Instant::now();
        let running = {
            let mut flights = self
                .catalog_flights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            flights.active.get_mut(context).map(|flight| {
                flight.active_waiters = flight
                    .active_waiters
                    .checked_add(1)
                    .expect("catalog discovery waiter count overflow");
                (flight.generation, flight.future.clone())
            })
        };
        let (generation, future, joined) = if let Some((generation, future)) = running {
            (generation, future, true)
        } else {
            let client = self.cluster_client(context).await?;
            let mut flights = self
                .catalog_flights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(flight) = flights.active.get_mut(context) {
                flight.active_waiters = flight
                    .active_waiters
                    .checked_add(1)
                    .expect("catalog discovery waiter count overflow");
                (flight.generation, flight.future.clone(), true)
            } else {
                // A previous generation may have filled the cache while this
                // caller was resolving its client. Recheck while holding the
                // same lock publishers use around insert-and-retire so a
                // cache user cannot accidentally create a redundant flight.
                if policy == CatalogPolicy::UseFreshCache {
                    let cached = self
                        .catalogs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .fresh(context)
                        .cloned();
                    if let Some(data) = cached {
                        return Ok(data);
                    }
                }
                let generation = flights.generations.entry(context.to_owned()).or_default();
                *generation += 1;
                let generation = *generation;
                let discovery_context = context.to_owned();
                let future = std::panic::AssertUnwindSafe(async move {
                    discovery::discover_resource_types(&client, &discovery_context).await
                })
                .catch_unwind()
                .map(|outcome| match outcome {
                    Ok(result) => result,
                    Err(_) => Err(BackendError::Internal(
                        "Kubernetes catalog discovery failed unexpectedly".into(),
                    )),
                })
                .boxed()
                .shared();
                flights.active.insert(
                    context.to_owned(),
                    CatalogFlight {
                        generation,
                        future: future.clone(),
                        active_waiters: 1,
                    },
                );
                (generation, future, false)
            }
        };

        let mut waiter = CatalogWaiter::new(
            Arc::clone(&self.catalog_flights),
            context.to_owned(),
            generation,
        );
        let result = future.await;
        {
            let mut flights = self
                .catalog_flights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let owns_generation = flights
                .active
                .get(context)
                .is_some_and(|flight| flight.generation == generation);
            if owns_generation {
                if let Ok(data) = &result {
                    self.catalogs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(context.to_owned(), data.clone());
                }
                flights.active.remove(context);
            }
        }
        waiter.disarm();
        tracing::debug!(
            context,
            generation,
            joined,
            duration_ms = started.elapsed().as_millis(),
            outcome = catalog_outcome(&result),
            "Kubernetes catalog discovery generation resolved"
        );
        result
    }

    /// Invalidate one context's cached discovery catalog so its next query
    /// re-discovers. Returns whether a cached entry was present.
    pub fn invalidate_discovery(&self, context: &str) -> bool {
        let mut catalogs = self
            .catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        catalogs.invalidate(context)
    }

    /// Resolve the shared cluster client for one context, building it lazily.
    async fn cluster_client(&self, context: &str) -> Result<kube::Client, BackendError> {
        if let Some(entry) = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .find(context)
            .filter(|entry| entry.availability == crate::port::ContextAvailability::Unavailable)
        {
            return Err(BackendError::ContextUnavailable {
                context: entry.name.clone(),
                reason: entry
                    .unavailable_reason
                    .clone()
                    .unwrap_or_else(|| "credential plugin is unavailable".into()),
            });
        }
        self.cluster_client_for_probe(context).await
    }

    /// Build a client during an explicit probe, including retrying a disabled
    /// context from Bootstrap/Refresh.
    async fn cluster_client_for_probe(&self, context: &str) -> Result<kube::Client, BackendError> {
        // Fast path under the shared per-context client map.
        {
            let clients = self.clients.lock().await;
            if let Some(client) = clients.get(context).cloned() {
                return Ok(client);
            }
        }

        let build_lock = self.client_build_locks.for_context(context);
        let _build = build_lock.lock().await;
        // Another request may have completed construction while this request
        // waited for the coalescing guard.
        {
            let clients = self.clients.lock().await;
            if let Some(client) = clients.get(context).cloned() {
                return Ok(client);
            }
        }

        let Some(kubeconfig_source) = &self.kubeconfig_source else {
            // No kubeconfig source and no injected client: a wiring gap that
            // must fail closed instead of silently degrading.
            return Err(BackendError::Internal(format!(
                "no cluster client is wired for context '{context}'"
            )));
        };

        let probe_generation = self.context_generation();
        let uses_exec_plugin = config::context_uses_exec(kubeconfig_source, context);
        let kubeconfig = match config::noninteractive_for_context(kubeconfig_source, context) {
            Ok(kubeconfig) => kubeconfig,
            Err(reason) => {
                self.mark_context_unavailable(probe_generation, context, reason.clone());
                return Err(BackendError::ContextUnavailable {
                    context: context.to_owned(),
                    reason,
                });
            }
        };
        let options = kube::config::KubeConfigOptions {
            context: Some(context.to_owned()),
            ..Default::default()
        };
        // Build this context's config offline; no network traffic happens here.
        let config = kube::config::Config::from_custom_kubeconfig(kubeconfig, &options)
            .await
            .map_err(|_| {
                BackendError::Internal(format!(
                    "cluster client for context '{context}' could not be initialized from the kubeconfig"
                ))
            })?;
        // Raise the shared transport stack for this validated config.
        let builder =
            tokio::task::spawn_blocking(move || kube::client::ClientBuilder::try_from(config))
                .await
                .map_err(|_| {
                    BackendError::Internal(format!(
                        "cluster client for context '{context}' could not raise its transport"
                    ))
                })?;
        let builder = match builder {
            Ok(builder) => builder.with_layer(&auth_observer::AuthObserverLayer::new(
                context,
                Arc::clone(&self.registry),
                Arc::clone(&self.clients),
                self.watches.clone(),
                self.metrics.clone(),
                uses_exec_plugin,
                self.availability_events.clone(),
            )),
            Err(error) => {
                if let Some(reason) = auth::classify_kube_error(&error, uses_exec_plugin) {
                    self.mark_context_unavailable(probe_generation, context, reason.clone());
                    return Err(BackendError::ContextUnavailable {
                        context: context.to_owned(),
                        reason,
                    });
                }
                return Err(BackendError::Internal(format!(
                    "cluster client for context '{context}' could not raise its transport"
                )));
            }
        };

        let client = builder.build();
        // Commit: share the built client with later queries of this context.
        {
            let mut clients = self.clients.lock().await;
            clients.insert(context.to_owned(), client.clone());
        }
        Ok(client)
    }

    fn context_generation(&self) -> u64 {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
            .0
    }

    fn mark_context_unavailable(&self, generation: u64, context: &str, reason: String) -> bool {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.mark_unavailable(generation, context, reason) {
            registry.choose_available_fallback();
            let reason = registry
                .find(context)
                .and_then(|entry| entry.unavailable_reason.as_deref())
                .unwrap_or("credential plugin is unavailable");
            tracing::warn!(
                context,
                reason,
                "Kubernetes context credential plugin is unavailable"
            );
            true
        } else {
            false
        }
    }

    fn mark_context_available(&self, generation: u64, context: &str) -> bool {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.mark_available(generation, context) {
            registry.choose_available_fallback();
            true
        } else {
            false
        }
    }

    async fn refresh_context_availability(&self) -> Vec<ContextInfo> {
        let _refresh = self.refresh_lock.lock().await;

        // Explicit Refresh retries disabled contexts in stable kubeconfig
        // order. Eviction guarantees the credential helper actually reruns.
        let unavailable = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contexts()
            .iter()
            .filter(|context| context.availability == crate::port::ContextAvailability::Unavailable)
            .map(|context| context.name.clone())
            .collect::<Vec<_>>();
        for context in unavailable {
            self.clients.lock().await.remove(&context);
            let generation = self.context_generation();
            if self.cluster_client_for_probe(&context).await.is_ok() {
                self.mark_context_available(generation, &context);
            }
        }

        // First bootstrap validates only the configured current context.
        let current_unknown = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contexts()
            .iter()
            .find(|context| {
                context.is_current
                    && context.availability == crate::port::ContextAvailability::Unknown
            })
            .map(|context| context.name.clone());
        if let Some(context) = current_unknown {
            let generation = self.context_generation();
            if self.cluster_client(&context).await.is_ok() {
                self.mark_context_available(generation, &context);
            }
        }

        let has_available_current = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contexts()
            .iter()
            .any(|context| {
                context.is_current
                    && context.availability == crate::port::ContextAvailability::Available
            });
        if !has_available_current {
            let unknown = self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contexts()
                .iter()
                .filter(|context| context.availability == crate::port::ContextAvailability::Unknown)
                .map(|context| context.name.clone())
                .collect::<Vec<_>>();
            for context in unknown {
                let generation = self.context_generation();
                if self.cluster_client(&context).await.is_ok()
                    && self.mark_context_available(generation, &context)
                {
                    break;
                }
            }
        }

        let registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.contexts().to_vec()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatalogPolicy {
    UseFreshCache,
    ForceLive,
}

type CatalogFuture = Shared<BoxFuture<'static, Result<ResourceTypesData, BackendError>>>;

struct CatalogFlight {
    generation: u64,
    future: CatalogFuture,
    active_waiters: usize,
}

#[derive(Default)]
struct CatalogFlights {
    generations: HashMap<String, u64>,
    active: HashMap<String, CatalogFlight>,
}

struct CatalogWaiter {
    flights: Arc<StdMutex<CatalogFlights>>,
    context: String,
    generation: u64,
    armed: bool,
}

impl CatalogWaiter {
    fn new(flights: Arc<StdMutex<CatalogFlights>>, context: String, generation: u64) -> Self {
        Self {
            flights,
            context,
            generation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CatalogWaiter {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut flights = self
            .flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let final_waiter = flights
            .active
            .get_mut(&self.context)
            .filter(|flight| flight.generation == self.generation)
            .is_some_and(|flight| {
                debug_assert!(flight.active_waiters > 0);
                flight.active_waiters = flight.active_waiters.saturating_sub(1);
                flight.active_waiters == 0
            });
        if final_waiter {
            flights.active.remove(&self.context);
        }
    }
}

fn catalog_outcome(result: &Result<ResourceTypesData, BackendError>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(BackendError::Unsupported { .. }) => "unsupported",
        Err(BackendError::NotFound) => "not_found",
        Err(BackendError::Conflict(_)) => "conflict",
        Err(BackendError::ContextUnavailable { .. }) => "context_unavailable",
        Err(BackendError::Forbidden) => "forbidden",
        Err(BackendError::Timeout) => "timeout",
        Err(BackendError::Cancelled) => "cancelled",
        Err(BackendError::Internal(_)) => "internal",
        Err(BackendError::PortForward { .. }) => "port_forward",
    }
}

/// One cached discovery catalog with its creation time for TTL checks.
#[derive(Debug)]
struct CatalogEntry {
    data: ResourceTypesData,
    created_at: tokio::time::Instant,
}

/// Bounded per-context discovery catalog cache.
///
/// Holds at most [`MAX_CACHED_CONTEXTS`] catalogs in LRU order; overflow evicts
/// the oldest entry. Entries older than [`DISCOVERY_TTL`] are not served and
/// are replaced by the next query's re-discovery (the documented refresh path).
#[derive(Debug)]
struct CatalogCache {
    /// Context names in access order, oldest first (eviction candidates).
    order: Vec<String>,
    entries: HashMap<String, CatalogEntry>,
}

impl CatalogCache {
    fn new() -> Self {
        Self {
            order: Vec::with_capacity(MAX_CACHED_CONTEXTS),
            entries: HashMap::new(),
        }
    }

    /// A not-yet-expired catalog for one context, marked recently used.
    fn fresh(&mut self, context: &str) -> Option<&ResourceTypesData> {
        if !self
            .entries
            .get(context)
            .is_some_and(|entry| entry.created_at.elapsed() < DISCOVERY_TTL)
        {
            return None;
        }
        self.mark_recent(context);
        Some(&self.entries[context].data)
    }

    /// Insert or refresh a catalog, evicting the oldest entries past the bound.
    fn insert(&mut self, context: String, data: ResourceTypesData) {
        if let Some(position) = self.order.iter().position(|name| *name == context) {
            self.order.remove(position);
        }
        self.entries.insert(
            context.clone(),
            CatalogEntry {
                data,
                created_at: tokio::time::Instant::now(),
            },
        );
        while self.order.len() >= MAX_CACHED_CONTEXTS {
            let evicted = self.order.remove(0);
            self.entries.remove(&evicted);
        }
        self.order.push(context);
    }

    /// Drop one context's catalog if present. Returns whether it existed.
    fn invalidate(&mut self, context: &str) -> bool {
        let removed = self.entries.remove(context).is_some();
        if removed {
            self.order.retain(|name| name != context);
        }
        removed
    }

    /// Move one key to the end of access order (most recently used).
    fn mark_recent(&mut self, context: &str) {
        if let Some(position) = self.order.iter().position(|name| name == context) {
            let moved = self.order.remove(position);
            self.order.push(moved);
        }
    }
}

impl KubeAdapter {
    /// Expose the backend-owned port-forward seam sharing this adapter's
    /// per-context clients.
    #[must_use]
    pub fn port_forward_connector(&self) -> crate::port_forward::PortForwardConnector {
        crate::port_forward::PortForwardConnector::new(Arc::new(
            port_forward::KubePortForwardSeam::shared(self.clients.clone()),
        ))
    }
}

#[cfg(test)]
mod client_build_lock_tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use futures_util::FutureExt;

    use super::{CatalogFlight, CatalogFlights, CatalogWaiter, ClientBuildLocks};
    use crate::port::{BackendError, ResourceTypesData};

    #[tokio::test]
    async fn one_context_build_never_blocks_another_context() {
        let locks = ClientBuildLocks::default();
        let slow = locks.for_context("slow");
        let same = locks.for_context("slow");
        let independent = locks.for_context("independent");
        let _slow_guard = slow.lock().await;

        assert!(same.try_lock().is_err(), "same-context builds coalesce");
        assert!(
            independent.try_lock().is_ok(),
            "a hung helper cannot block an unrelated context"
        );
    }

    #[test]
    fn panic_unwind_retires_the_final_catalog_waiter() {
        let flights = Arc::new(StdMutex::new(CatalogFlights::default()));
        let future = std::future::pending::<Result<ResourceTypesData, BackendError>>()
            .boxed()
            .shared();
        flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .insert(
                "panic-context".into(),
                CatalogFlight {
                    generation: 7,
                    future,
                    active_waiters: 1,
                },
            );

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let flights = Arc::clone(&flights);
            move || {
                let _waiter = CatalogWaiter::new(flights, "panic-context".into(), 7);
                panic!("synthetic waiter panic");
            }
        }));

        assert!(unwind.is_err());
        assert!(
            !flights
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .contains_key("panic-context"),
            "unwind drops the guard and retires the final matching waiter"
        );
    }
}
