//! Supervised list/watch cycle for one demand-driven selection.
//!
//! One supervised task per `(context, GVK, scope)` selection runs the
//! documented phase machine: `Init` while its LIST is in flight (previous
//! rows stay cached but flagged stale), `InitApply` once the cut is applied
//! as one atomic replacement, and `InitDone` when the live WATCH stream is
//! attached at the list's resource version. Any disconnect or source error
//! restarts the cycle; the child cancellation token ends the task.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::port::{BackendEvent, ResourceListData, ResourceRef};
use crate::runtime::cache::{RevisionCounter, SummaryCache, now_rfc3339};
use crate::watch::WatchSelector;

/// Cancellable pause between a failed cycle and the next relist.
pub const WATCH_RESTART_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// How long a freshly attached selection stays in `InitApply` before it is
/// considered established (`InitDone`): the first delta announces
/// establishment early, silence falls through to the window.
pub const WATCH_ESTABLISH_WINDOW: std::time::Duration = std::time::Duration::from_millis(200);

/// The phase of one supervised list/watch cycle for a selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPhase {
    /// The LIST of this cycle is in flight; previous rows stay cached but
    /// flagged stale until the relist completes.
    Init,
    /// The LIST result has been applied to the summary cache as one atomic
    /// replacement.
    InitApply,
    /// The live WATCH stream is established at the list's resource version;
    /// deltas flow and the cached rows are fresh.
    InitDone,
}

/// One normalized object row observed by a list/watch source before the
/// runtime stamps its monotonic backend revision.
#[derive(Debug, Clone)]
pub struct WatchRow {
    /// Stable identity of the object.
    pub reference: ResourceRef,
    /// Object labels keyed as reported by the cluster.
    pub labels: BTreeMap<String, String>,
    /// Human-readable status summary derived from normalized fields only.
    pub summary: String,
    /// Creation time formatted as RFC 3339 (empty when absent).
    pub created_at: String,
    /// Owner chain as reported by the cluster, in report order.
    pub owner_references: Vec<crate::port::OwnerRef>,
    /// Kind-specific structured projection; absent for kinds without a
    /// designed projection.
    pub projection: Option<crate::port::ResourceProjection>,
}

/// One normalized observation flowing from a live watch source into the
/// runtime.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum WatchUpdate {
    /// An object was added or changed; the full row replaces prior state.
    Upsert(WatchRow),
    /// An object was removed from the watched selection.
    Delete(ResourceRef),
}

/// The complete result of one LIST call for a selection.
#[derive(Debug, Clone)]
pub struct ListedState {
    /// Opaque Kubernetes resourceVersion of the list cut. Backend-internal:
    /// it never crosses the port seam nor reaches clients, which only ever
    /// see the backend-owned monotonic revision.
    pub resource_version: String,
    /// Rows in cluster order; the runtime sorts and publishes them atomically.
    pub rows: Vec<WatchRow>,
}

/// A live list/watch source for one selection, implemented by adapters.
///
/// The supervisor stays transport-agnostic through this seam: implementations
/// own their client stack and return only normalized updates or sanitized
/// error details — no cluster credentials or raw API types ever escape.
pub trait WatchSource: Send + Sync + std::fmt::Debug {
    /// Run a LIST for this selection against the cluster. An error restarts
    /// the cycle after [`WATCH_RESTART_BACKOFF`].
    fn list<'a>(&'a self)
    -> Pin<Box<dyn Future<Output = Result<ListedState, String>> + Send + 'a>>;

    /// Attach to the live watch stream resuming from an opaque resource
    /// version. The returned future completes when the stream ends or
    /// errors; normalized updates are reported through `out` while it runs.
    /// Ending always triggers a fresh relist, so sources may end freely on
    /// timeouts or transport errors.
    fn attach_watch<'a>(
        &'a self,
        resource_version: String,
        out: mpsc::UnboundedSender<WatchUpdate>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Publisher fusing the summary cache of one selection with its bounded
/// broadcast channel and the shared monotonic revision counter.
///
/// Every broadcast (list cut, live delta, warm-join snapshot) runs inside
/// one publication lock, so the order events reach the channel matches the
/// order revisions are allocated: a receiver can never observe a newer
/// delta followed by an older snapshot.
#[derive(Debug)]
pub struct SelectionPublisher {
    selector: WatchSelector,
    cache: SummaryCache,
    sender: broadcast::Sender<BackendEvent>,
    revisions: RevisionCounter,
    publish_order: std::sync::Mutex<()>,
}

impl SelectionPublisher {
    /// Create a publisher for one selection over an existing broadcast
    /// channel (shared by every subscriber of the selection).
    #[must_use]
    pub fn new(
        selector: WatchSelector,
        sender: broadcast::Sender<BackendEvent>,
        revisions: RevisionCounter,
    ) -> Self {
        let cache = SummaryCache::new_for(selector.clone());
        Self {
            selector,
            cache,
            sender,
            revisions,
            publish_order: std::sync::Mutex::new(()),
        }
    }

    /// Apply one list cut atomically and broadcast the full snapshot event.
    ///
    /// The snapshot event is published only after the complete cut sits in
    /// the cache: watchers can never receive half of a list. The snapshot
    /// carries the revision allocated for this cut — including for empty
    /// cuts, so the revision stream stays strictly increasing even when a
    /// selection relists to zero rows.
    pub fn apply_list(&self, listed: &ListedState) {
        let _order = self
            .publish_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (revision, records) = self.cache.replace(listed.rows.clone(), &self.revisions);
        let _ = self.sender.send(BackendEvent::Snapshot(ResourceListData {
            context: self.selector.context.clone(),
            gvk: self.selector.gvk.clone(),
            namespace: self.selector.namespace.clone(),
            generated_at: now_rfc3339(),
            revision,
            rows: records,
        }));
    }

    /// Apply one live delta and broadcast it.
    pub fn apply_update(&self, update: WatchUpdate) {
        let _order = self
            .publish_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self
            .sender
            .send(self.cache.apply_update(update, &self.revisions));
    }

    /// Broadcast the current cache contents as a complete snapshot for late
    /// joiners of a warm selection. Ordered against list cuts and live
    /// deltas by the same publication lock, so a joining receiver's event
    /// stream stays revision-monotonic.
    pub fn publish_current(&self) {
        let _order = self
            .publish_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let data = self.cache.publish_current(&self.revisions);
        let _ = self.sender.send(BackendEvent::Snapshot(data));
    }

    /// Whether the cache currently predates an in-flight relist.
    #[must_use]
    pub fn stale(&self) -> bool {
        self.cache.stale()
    }

    /// Current sorted cache contents.
    #[must_use]
    pub fn rows(&self) -> Vec<crate::port::ResourceRecord> {
        self.cache.snapshot()
    }

    /// Flag the cache stale at the start of a relist.
    pub fn mark_stale(&self) {
        self.cache.mark_stale();
    }
}

/// Bookkeeping the cluster registry keeps for one live selection.
#[derive(Debug)]
pub(crate) struct SelectionHandle {
    /// Monotonic ID distinguishing successive registrations of one selector.
    pub(crate) id: u64,
    pub(crate) sender: broadcast::Sender<BackendEvent>,
    pub(crate) phases: watch::Receiver<WatchPhase>,
    pub(crate) publisher: Arc<SelectionPublisher>,
    pub(crate) cancel: CancellationToken,
    /// Last time any subscriber joined this selection (registration or warm
    /// join). Teardown may only proceed a full linger after this instant,
    /// which covers subscribers too short-lived for the monitor's samples to
    /// ever observe their presence.
    pub(crate) last_join: std::sync::Mutex<std::time::Instant>,
}

/// Drive one supervised selection until its shutdown token fires.
///
/// The cycle never gives up on its own: every stream end or source error
/// loops back into a fresh `Init` after [`WATCH_RESTART_BACKOFF`]. Exit is
/// exclusively through the child cancellation token, which the cluster
/// registry cancels once the last subscriber has lingered out.
pub async fn run_selection(
    source: Arc<dyn WatchSource>,
    publisher: Arc<SelectionPublisher>,
    phases: watch::Sender<WatchPhase>,
    shutdown: CancellationToken,
) {
    loop {
        // Init: the relist is in flight; old rows remain visible but stale.
        let _ = phases.send(WatchPhase::Init);
        publisher.mark_stale();
        let list_cut = loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                listed = source.list() => match listed {
                    Ok(listed) => {
                        // InitApply: the whole cut lands as one atomic
                        // replacement before any watcher can observe half.
                        let _ = phases.send(WatchPhase::InitApply);
                        publisher.apply_list(&listed);
                        break listed.resource_version;
                    }
                    Err(_) => {
                        if backoff(&shutdown).await {
                            return;
                        }
                    }
                },
            }
        };

        // InitApply holds while the live watch establishes itself at the
        // list cut's opaque resourceVersion: the phase is announced only
        // after the first delta arrives or the establish window elapses,
        // whichever comes first.
        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<WatchUpdate>();
        let attach = source.attach_watch(list_cut, update_tx);
        tokio::pin!(attach);
        // Poll the attach future once so sources run their synchronous
        // setup before anything is announced.
        let attach_ended_immediately = matches!(
            attach
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop())),
            std::task::Poll::Ready(())
        );
        if attach_ended_immediately {
            if backoff(&shutdown).await {
                return;
            }
            continue;
        }
        let first_delta = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            update = update_rx.recv() => update,
            _ = tokio::time::sleep(WATCH_ESTABLISH_WINDOW) => None,
        };
        let _ = phases.send(WatchPhase::InitDone);
        if let Some(update) = first_delta {
            publisher.apply_update(update);
        }
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                // Biased polling drains buffered updates before noticing the
                // attach future completing, so no delta of an ended stream is
                // lost before the restart relists.
                update = update_rx.recv() => match update {
                    Some(update) => publisher.apply_update(update),
                    None => break,
                },
                _ = &mut attach => break,
            }
        }

        if backoff(&shutdown).await {
            return;
        }
    }
}

/// Cancellable restart pause. Returns true when shutdown won the race.
async fn backoff(shutdown: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(WATCH_RESTART_BACKOFF) => false,
    }
}
