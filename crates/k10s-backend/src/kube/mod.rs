//! Real Kubernetes adapter backed by kube-rs.
//!
//! Kube-rs types are confined to this module tree: the rest of k10s only ever
//! sees normalized port types and [`AdapterError`]s. Bootstrap is served from
//! a committed, credential-free context registry; cluster-facing discovery runs
//! through kube-rs against injected clients in tests or live API servers in
//! production, cached per context behind bounded, refreshable state.

mod config;
mod discovery;
mod watch;

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};

pub use self::discovery::{DISCOVERY_TTL, MAX_CACHED_CONTEXTS};

#[cfg(feature = "testkit")]
use crate::port::ContextInfo;

use crate::port::{
    AdapterError, BackendError, BootstrapInfo, Command, Gvk, KubernetesAccess, OperationId, Query,
    QueryResult, ResourceTypesData, StreamInput, Subscribe, SubscriptionHandle,
};
use crate::runtime::ContextRegistry;
use crate::runtime::cluster::ClusterWatches;
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
    registry: ContextRegistry,
    /// Shared cluster client per context name: pre-injected in tests through
    /// [`Self::with_cluster_clients`], otherwise built on first use from the
    /// stored kubeconfig and cached here for reuse.
    clients: tokio::sync::Mutex<HashMap<String, kube::Client>>,
    /// Parsed kubeconfig seeding per-context client construction; absent when
    /// testkit pre-injected every context's client instead.
    kubeconfig_source: Option<kube::config::Kubeconfig>,
    /// Bounded per-context discovery catalog cache (LRU eviction, TTL refresh).
    catalogs: StdMutex<CatalogCache>,
    /// Supervised demand-driven watch runtime: one task per selection with
    /// atomic summary caches and lingered teardown.
    watches: ClusterWatches,
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
        Ok(Self {
            registry: ContextRegistry::prepare(prepared)?,
            clients: tokio::sync::Mutex::new(HashMap::new()),
            kubeconfig_source: Some(kubeconfig),
            catalogs: StdMutex::new(CatalogCache::new()),
            watches: ClusterWatches::default(),
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

        Ok(Self {
            registry,
            clients: tokio::sync::Mutex::new(client_map),
            kubeconfig_source: None,
            catalogs: StdMutex::new(CatalogCache::new()),
            watches: ClusterWatches::default(),
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
}

impl KubernetesAccess for KubeAdapter {
    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match req {
                // Bootstrap is fully supported in Task 1: safe summaries only.
                Query::Bootstrap => Ok(QueryResult::Bootstrap(BootstrapInfo {
                    contexts: self.registry.contexts().to_vec(),
                })),
                // Discovery is live in this task through the cached catalog path.
                Query::ResourceTypes { context } => self.resource_types(&context).await,
                // Cluster-facing capabilities arrive with later Plan 3 tasks;
                // until then they are typed, not guessed.
                Query::ValidateApply { .. } => Err(BackendError::unsupported("validate.apply")),
                Query::StreamTicket { .. } => Err(BackendError::unsupported("stream.ticket")),
                Query::ResourceList { .. } => Err(BackendError::unsupported("resource.list")),
                Query::ResourceDetail { .. } => Err(BackendError::unsupported("resource.detail")),
                Query::ResourceMetrics { .. } => Err(BackendError::unsupported("resource.metrics")),
                Query::ResourceRelations { .. } => {
                    Err(BackendError::unsupported("resource.relations"))
                }
                Query::Infrastructure { .. } => Err(BackendError::unsupported("infrastructure")),
                Query::OperationStatus { .. } => Err(BackendError::unsupported("operation.status")),
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
                // Same protocol shape as the fake adapter's bootstrap status.
                Subscribe::BootstrapStatus => Ok(SubscriptionHandle::new("bootstrap-status")),
                Subscribe::ResourceWatch {
                    context,
                    gvk,
                    namespace,
                } => self.resource_watch(context, gvk, namespace).await,
                Subscribe::Infrastructure { .. } => {
                    Err(BackendError::unsupported("infrastructure.watch"))
                }
                Subscribe::StreamRedeem { .. } => Err(BackendError::unsupported("stream.redeem")),
                Subscribe::Operations => Err(BackendError::unsupported("operations")),
            }
        })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: StreamInput,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}

impl KubeAdapter {
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
        if self.registry.find(&context).is_none() {
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

    /// Serve one context's resource catalog through discovery.
    ///
    /// Unknown contexts are typed not-found; a fresh cached catalog is served
    /// without network traffic, and expired or invalidated catalogs trigger a
    /// re-discovery that replaces them under the same bounds as before.
    async fn resource_types(&self, context: &str) -> Result<QueryResult, BackendError> {
        Ok(QueryResult::ResourceTypes(self.catalog_for(context).await?))
    }

    /// Resolve one context's discovery catalog through the bounded cache.
    async fn catalog_for(&self, context: &str) -> Result<ResourceTypesData, BackendError> {
        if self.registry.find(context).is_none() {
            return Err(BackendError::NotFound);
        }

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

        // Slow path: discover through kube-rs, then publish under the bounds.
        let client = self.cluster_client(context).await?;
        let data = discovery::discover_resource_types(&client, context).await?;

        let mut catalogs = self
            .catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A concurrent query may have refreshed the catalog while we discovered.
        if let Some(data) = catalogs.fresh(context).cloned() {
            return Ok(data);
        }
        catalogs.insert(context.to_owned(), data.clone());
        Ok(data)
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
        // Fast path under the shared per-context client map.
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

        let options = kube::config::KubeConfigOptions {
            context: Some(context.to_owned()),
            ..Default::default()
        };
        // Build this context's config offline; no network traffic happens here.
        let config = kube::config::Config::from_custom_kubeconfig(kubeconfig_source.clone(), &options)
            .await
            .map_err(|_| {
                BackendError::Internal(format!(
                    "cluster client for context '{context}' could not be initialized from the kubeconfig"
                ))
            })?;
        // Raise the shared transport stack for this validated config.
        let builder = kube::client::ClientBuilder::try_from(config).map_err(|_| {
            BackendError::Internal(format!(
                "cluster client for context '{context}' could not raise its transport"
            ))
        })?;

        let client = builder.build();
        // Commit: share the built client with later queries of this context.
        {
            let mut clients = self.clients.lock().await;
            clients.insert(context.to_owned(), client.clone());
        }
        Ok(client)
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
