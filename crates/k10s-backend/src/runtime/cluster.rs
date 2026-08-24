//! Cluster-level registry of supervised demand-driven watch selections and
//! resource-metrics poll collectors.
//!
//! Watches: one supervised task exists per `(context, GVK, scope)` selection:
//! the first subscriber starts it, later subscribers of the same selection
//! share its bounded broadcast channel (and immediately receive the current
//! cache contents as a complete snapshot), and after the final unsubscribe
//! the selection lingers briefly before its task exits. Rejoining inside the
//! linger window revives the warm selection without a relist.
//!
//! Metrics: one poll collector exists per context. It is started only by an
//! active consumer (a metrics query), refreshed on every later consumer
//! touch, and exits after the linger window passes with no consumer activity.
//! No consumers means no polling.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

use crate::port::BackendEvent;
use crate::runtime::cache::RevisionCounter;
use crate::runtime::supervisor::{
    SelectionHandle, SelectionPublisher, WatchPhase, WatchSource, run_selection,
};
use crate::watch::WatchSelector;

/// How long a selection stays warm after its last subscriber leaves before
/// its supervised task exits.
pub const WATCH_LINGER: Duration = Duration::from_secs(5);

/// Poll cadence of a live metrics collector.
pub const METRICS_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a metrics collector stays up after its last consumer touch before
/// its poll task exits. Deliberately longer than [`METRICS_POLL_INTERVAL`] so
/// a steady consumer polling once per interval can never observe teardown.
pub const METRICS_LINGER: Duration = Duration::from_secs(90);

/// A collected Resource Metrics API cut for one context.
///
/// Everything here is what the cluster actually reported at poll time:
/// absent samples stay absent ([`ResourceUsageSample`] fields are `Option`),
/// coverage is computed against core Node membership, and pod capacity comes
/// exclusively from core Node allocatable — never from metrics data, and
/// never inferred from requests or capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Context the cut was collected from.
    pub context: String,
    /// Backend wall-clock time of poll completion, formatted as RFC 3339.
    pub collected_at: String,
    /// Newest source-reported sample timestamp (RFC 3339), when any sample
    /// carried one.
    pub source_updated_at: Option<String>,
    /// Source-reported scrape window in seconds, when reported.
    pub window_seconds: Option<u64>,
    /// Explicit state of the Metrics API itself for this cut.
    pub state: MetricsApiState,
    /// Per-node usage keyed by node name; only reporting nodes appear.
    pub node_usage: BTreeMap<String, ResourceUsageSample>,
    /// Per-pod usage keyed by `namespace/name`; only reporting pods appear.
    pub pod_usage: BTreeMap<String, ResourceUsageSample>,
    /// Core Node names observed at collection time — the honest membership
    /// against which node coverage is computed by identity, not count.
    pub node_names: Vec<String>,
    /// Summed allocatable pod capacity across core Nodes, when the core list
    /// was readable.
    pub pod_capacity_total: Option<u64>,
}

/// Whether the Metrics API answered, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsApiState {
    /// The API served list cuts.
    Ready,
    /// The API is not installed on this cluster.
    Absent,
    /// Kubernetes RBAC denied the read.
    Forbidden,
    /// The API could not be reached this cycle.
    Unreachable,
}

/// One usage sample as reported; absent fields stay absent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceUsageSample {
    /// CPU usage in millicores, absent when not reported.
    pub cpu_millicores: Option<u64>,
    /// Working-set memory in bytes, absent when not reported.
    pub memory_bytes: Option<u64>,
    /// Source-reported sample timestamp (RFC 3339), when the item carried
    /// one; gates this sample's own freshness independently of its cut.
    pub timestamp: Option<String>,
    /// Source-reported scrape window in seconds, when reported.
    pub window_seconds: Option<u64>,
}

/// Honest node coverage of one cut relative to core Node membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsCoverage {
    /// Every known node reported.
    Full,
    /// Some nodes reported; the rest stay absent rather than zeroed.
    Partial,
    /// Nothing reported (or the API did not answer), so no usage exists.
    Unavailable,
}

impl MetricsSnapshot {
    /// Coverage of node usage against the core Node membership observed at
    /// collection time. Every known node must have its own metrics entry:
    /// matching counts mean nothing when membership shifts between the core
    /// list and the metrics cut. Without a readable core denominator the cut
    /// stays partial instead of claiming completeness.
    #[must_use]
    pub fn node_coverage(&self) -> MetricsCoverage {
        if self.node_usage.is_empty() {
            return MetricsCoverage::Unavailable;
        }
        if self.state != MetricsApiState::Ready
            || self.node_names.is_empty()
            || !self
                .node_names
                .iter()
                .all(|name| self.node_usage.contains_key(name))
        {
            return MetricsCoverage::Partial;
        }
        MetricsCoverage::Full
    }
}

/// One poll-cycle producer for [`ClusterMetrics`].
///
/// Implementations perform exactly one collection pass per call and always
/// return a cut: failures are captured inside the snapshot's state instead of
/// erroring, so consumers can distinguish absent from forbidden from stale.
pub trait MetricsPollSource: Send + Sync + std::fmt::Debug {
    /// Run one collection cycle.
    fn poll(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = MetricsSnapshot> + Send + '_>>;
}

/// Registry owning every live metrics poll collector of one adapter.
///
/// Clones share one registry. Collectors are keyed by context and started
/// only through [`ClusterMetrics::collect_for_consumer`] — an active consumer
/// request. Each collector's task re-polls on a fixed cadence, refreshes its
/// cached cut atomically, and exits once no consumer has touched it for the
/// linger window, so quiet contexts never generate cluster traffic.
#[derive(Debug, Clone)]
pub struct ClusterMetrics {
    shared: Arc<MetricsShared>,
}

struct MetricsShared {
    linger: Duration,
    poll_interval: Duration,
    collectors: StdMutex<HashMap<String, CollectorEntry>>,
}

impl std::fmt::Debug for MetricsShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsShared")
            .field("linger", &self.linger)
            .field("poll_interval", &self.poll_interval)
            .field(
                "live_collectors",
                &self
                    .collectors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

/// One live collector registration.
struct CollectorEntry {
    id: u64,
    snapshot: watch::Receiver<Option<Arc<MetricsSnapshot>>>,
    /// Last consumer touch; drives the idle-linger exit of the poll task.
    last_touch: Arc<StdMutex<std::time::Instant>>,
    /// Immediate teardown path used by explicit retirements.
    cancel: CancellationToken,
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self::new(METRICS_LINGER, METRICS_POLL_INTERVAL)
    }
}

impl ClusterMetrics {
    /// Create an empty registry whose collectors exit after `linger` without
    /// a consumer touch and re-poll every `poll_interval` while alive.
    #[must_use]
    pub fn new(linger: Duration, poll_interval: Duration) -> Self {
        Self {
            shared: Arc::new(MetricsShared {
                linger,
                poll_interval,
                collectors: StdMutex::new(HashMap::new()),
            }),
        }
    }

    /// Serve one consumer request for `context`: starts its collector on
    /// first use (awaiting the first completed cycle), touches the linger
    /// deadline otherwise, and returns the latest cached cut.
    ///
    /// `source_factory` runs only when a new collector must be spawned.
    /// `None` means the collector exited between registration and the first
    /// completed cycle; callers treat that as an absent cut.
    pub async fn collect_for_consumer(
        &self,
        context: &str,
        source_factory: impl FnOnce() -> Arc<dyn MetricsPollSource>,
    ) -> Option<Arc<MetricsSnapshot>> {
        let receiver = {
            let mut collectors = self
                .shared
                .collectors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = collectors.get_mut(context) {
                // Warm join: refresh the linger deadline and serve the cache.
                *entry
                    .last_touch
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = std::time::Instant::now();
                entry.snapshot.clone()
            } else {
                static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let (sender, receiver) = watch::channel(None);
                let entry = CollectorEntry {
                    id,
                    snapshot: receiver.clone(),
                    last_touch: Arc::new(StdMutex::new(std::time::Instant::now())),
                    cancel: CancellationToken::new(),
                };
                let last_touch = Arc::clone(&entry.last_touch);
                let cancel = entry.cancel.clone();
                collectors.insert(context.to_owned(), entry);
                drop(collectors);
                self.spawn_poll_task(
                    context.to_owned(),
                    id,
                    sender,
                    source_factory(),
                    last_touch,
                    cancel,
                );
                receiver
            }
        };
        wait_for_first_cut(receiver).await
    }

    /// Latest cached cut for one context, without touching any linger
    /// deadline. Observability seam for diagnostics and tests.
    #[must_use]
    pub fn snapshot_of(&self, context: &str) -> Option<Arc<MetricsSnapshot>> {
        let collectors = self
            .shared
            .collectors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        collectors
            .get(context)
            .and_then(|entry| entry.snapshot.borrow().as_ref().cloned())
    }

    /// Number of live collectors; observability for linger behavior.
    #[must_use]
    pub fn live_collectors(&self) -> usize {
        self.shared
            .collectors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Immediately cancel and remove every collector serving one context.
    ///
    /// Context-switch retirement path: the replaced context's poll task ends
    /// now instead of waiting out its linger window, so no poller outlives
    /// its context's relevance. Returns how many collectors were retired.
    pub fn retire_context(&self, context: &str) -> usize {
        let victim = {
            let mut collectors = self
                .shared
                .collectors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            collectors.remove(context).map(|entry| entry.cancel)
        };
        match victim {
            Some(cancel) => {
                cancel.cancel();
                1
            }
            None => 0,
        }
    }

    /// Spawn one supervised poll task: collect immediately, publish the cut
    /// atomically onto the watch channel, then keep the cadence until no
    /// consumer has touched the collector for the linger window or an
    /// explicit retirement cancels it. Exit removes the registration only if
    /// it is still this task's own generation.
    fn spawn_poll_task(
        &self,
        context: String,
        id: u64,
        sender: watch::Sender<Option<Arc<MetricsSnapshot>>>,
        source: Arc<dyn MetricsPollSource>,
        last_touch: Arc<StdMutex<std::time::Instant>>,
        cancel: CancellationToken,
    ) {
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                let snapshot = source.poll().await;
                if sender.send(Some(Arc::new(snapshot))).is_err() {
                    break; // registry gone: nothing left to serve
                }
                // Hold the cadence, but exit early the moment the collector
                // is retired or has been idle past the linger window —
                // quiet contexts must not generate cluster traffic between
                // checks either.
                let cadence_end = std::time::Instant::now() + shared.poll_interval;
                let mut stop = false;
                while std::time::Instant::now() < cadence_end {
                    let idle = last_touch
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .elapsed()
                        >= shared.linger;
                    if idle || cancel.is_cancelled() {
                        stop = true;
                        break;
                    }
                    tokio::time::sleep(LINGER_POLL).await;
                }
                if stop
                    || last_touch
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .elapsed()
                        >= shared.linger
                {
                    break;
                }
            }
            remove_if_current(&shared, &context, id);
        });
    }
}

/// Await the first completed cut of a freshly spawned collector.
async fn wait_for_first_cut(
    mut receiver: watch::Receiver<Option<Arc<MetricsSnapshot>>>,
) -> Option<Arc<MetricsSnapshot>> {
    loop {
        if let Some(snapshot) = receiver.borrow_and_update().as_ref().cloned() {
            return Some(snapshot);
        }
        match receiver.changed().await {
            Ok(()) => continue,
            // A lagging consumer only missed intermediate cuts; keep waiting
            // for the next completed cycle rather than giving up.
            Err(_) => return None,
        }
    }
}

/// Remove a collector registration only if it is still this generation's;
/// a concurrent restart must never be torn down by a dying predecessor.
fn remove_if_current(shared: &MetricsShared, context: &str, id: u64) {
    let mut collectors = shared
        .collectors
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if collectors.get(context).is_some_and(|entry| entry.id == id) {
        collectors.remove(context);
    }
}

/// Poll cadence for subscriber-count checks while a selection is live; small
/// enough that teardown feels prompt, rare enough to cost nothing.
const LINGER_POLL: Duration = Duration::from_millis(20);

/// Registry state shared between subscribers and linger monitors.
struct Shared {
    linger: Duration,
    selections: StdMutex<HashMap<WatchSelector, SelectionHandle>>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("linger", &self.linger)
            .field(
                "live_selections",
                &self
                    .selections
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

/// Registry owning every live supervised watch selection of one adapter.
///
/// Clones share one registry; adapters embed a single instance behind their
/// interior-mutable state so subscription and teardown serialize on locks.
#[derive(Debug, Clone)]
pub struct ClusterWatches {
    shared: Arc<Shared>,
    revisions: RevisionCounter,
    capacity: usize,
}

impl Default for ClusterWatches {
    fn default() -> Self {
        Self::new(WATCH_LINGER)
    }
}

impl ClusterWatches {
    /// Create an empty registry whose selections linger for `linger` after
    /// their last subscriber leaves.
    #[must_use]
    pub fn new(linger: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                linger,
                selections: StdMutex::new(HashMap::new()),
            }),
            revisions: RevisionCounter::new(),
            capacity: crate::watch::WATCH_CAPACITY,
        }
    }

    /// Subscribe to one selection, starting its supervised task on first use.
    ///
    /// The returned receiver is bounded like every other backend queue; a
    /// lagging consumer surfaces through the broadcast error so callers can
    /// demand a resync instead of silently dropping deltas. Must be called
    /// inside a Tokio runtime: it spawns the supervisor and its monitor.
    pub fn subscribe(
        &self,
        selector: WatchSelector,
        source: Arc<dyn WatchSource>,
    ) -> broadcast::Receiver<BackendEvent> {
        let mut selections = self
            .shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(handle) = selections.get(&selector) {
            // Warm join: register first so no published snapshot can be
            // missed, then hand this subscriber the complete current state.
            // The join timestamp resets the linger deadline: even a join so
            // short the monitor never samples its presence pushes teardown a
            // full linger window away.
            *handle
                .last_join
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = std::time::Instant::now();
            let receiver = handle.sender.subscribe();
            handle.publisher.publish_current();
            return receiver;
        }

        let (sender, receiver) = broadcast::channel(self.capacity);
        let publisher = Arc::new(SelectionPublisher::new(
            selector.clone(),
            sender.clone(),
            self.revisions.clone(),
        ));
        let (phase_tx, phase_rx) = watch::channel(WatchPhase::Init);
        let cancel = CancellationToken::new();
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        selections.insert(
            selector.clone(),
            SelectionHandle {
                id,
                sender: sender.clone(),
                phases: phase_rx.clone(),
                publisher: Arc::clone(&publisher),
                cancel: cancel.clone(),
                last_join: StdMutex::new(std::time::Instant::now()),
            },
        );
        drop(selections);

        tokio::spawn(run_selection(source, publisher, phase_tx, cancel.clone()));
        self.spawn_linger_monitor(selector, id, sender, cancel);
        receiver
    }

    /// Allocate one revision from the same global monotonic counter the
    /// supervised watch publications use, so on-demand list snapshots can
    /// never move the system-wide revision stream backwards.
    #[must_use]
    pub fn next_revision(&self) -> u64 {
        self.revisions.next()
    }

    /// Current phase of one selection, when it exists.
    #[must_use]
    pub fn phase(&self, selector: &WatchSelector) -> Option<WatchPhase> {
        let selections = self
            .shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let handle = selections.get(selector)?;
        Some(*handle.phases.borrow())
    }

    /// Phase observer of one selection for ordering assertions and operator
    /// diagnostics.
    #[must_use]
    pub fn phases(&self, selector: &WatchSelector) -> Option<watch::Receiver<WatchPhase>> {
        self.shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(selector)
            .map(|handle| handle.phases.clone())
    }

    /// Number of live selections; observability for linger behavior.
    #[must_use]
    pub fn live_selections(&self) -> usize {
        self.shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Immediately cancel and remove every selection serving one context.
    ///
    /// Context-switch retirement path: the replaced context's supervised
    /// tasks end now instead of lingering, so no watcher outlives its
    /// context's relevance. Late subscribers start fresh selections.
    ///
    /// Returns how many selections were retired.
    pub fn retire_context(&self, context: &str) -> usize {
        let victims: Vec<CancellationToken> = {
            let mut selections = self
                .shared
                .selections
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let victims = selections
                .iter()
                .filter(|(selector, _)| selector.context == context)
                .map(|(_, handle)| handle.cancel.clone())
                .collect();
            selections.retain(|selector, _| selector.context != context);
            victims
        };
        for token in &victims {
            token.cancel();
        }
        victims.len()
    }

    /// Cached rows of one warm selection, when it exists.
    #[must_use]
    pub fn cached_rows(
        &self,
        selector: &WatchSelector,
    ) -> Option<Vec<crate::port::ResourceRecord>> {
        self.shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(selector)
            .map(|handle| handle.publisher.rows())
    }

    /// Whether one warm selection's cache currently predates a relist.
    #[must_use]
    pub fn cache_stale(&self, selector: &WatchSelector) -> Option<bool> {
        self.shared
            .selections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(selector)
            .map(|handle| handle.publisher.stale())
    }

    /// Exit a selection only after a full linger window has elapsed with no
    /// subscriber at any point in it. The final check-and-remove runs under
    /// the same registry lock as [`ClusterWatches::subscribe`] and rechecks
    /// the live receiver count, so a join racing the teardown either lands
    /// on the warm entry (monitor keeps waiting) or starts a fresh selection
    /// — never on a dying one. The monitor itself ends the moment its token
    /// is cancelled by an explicit retirement.
    fn spawn_linger_monitor(
        &self,
        selector: WatchSelector,
        id: u64,
        sender: broadcast::Sender<BackendEvent>,
        cancel: CancellationToken,
    ) {
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            let poll = shared.linger.min(LINGER_POLL).max(Duration::from_millis(5));
            let mut empty_since: Option<tokio::time::Instant> = None;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(poll) => {}
                }
                if sender.receiver_count() > 0 {
                    // Any live subscriber resets the linger deadline.
                    empty_since = None;
                    continue;
                }
                let since = match empty_since {
                    Some(since) => since,
                    None => *empty_since.insert(tokio::time::Instant::now()),
                };
                if since.elapsed() < shared.linger {
                    continue;
                }
                if remove_if_current_and_empty(&shared, &selector, id, shared.linger) {
                    cancel.cancel();
                    return;
                }
                // A concurrent subscribe re-registered or joined: keep watching.
                empty_since = None;
            }
        });
    }
}

/// Atomically remove the entry if it is still this monitor's registration,
/// it still has no receivers, and no join has happened within the linger
/// window. Holding the registry lock for the checks and the removal closes
/// the join/teardown race: a subscriber creates its receiver under the same
/// lock, so it is either counted here (entry kept) or joins after the entry
/// was removed (fresh selection) — never on a dying one.
fn remove_if_current_and_empty(
    shared: &Shared,
    selector: &WatchSelector,
    id: u64,
    linger: Duration,
) -> bool {
    let mut selections = shared.selections.lock().unwrap_or_else(|e| e.into_inner());
    match selections.get(selector) {
        Some(handle)
            if handle.id == id
                && handle.sender.receiver_count() == 0
                && handle
                    .last_join
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .elapsed()
                    >= linger =>
        {
            selections.remove(selector);
            true
        }
        _ => false,
    }
}
