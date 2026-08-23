//! Cluster-level registry of supervised demand-driven watch selections.
//!
//! One supervised task exists per `(context, GVK, scope)` selection: the
//! first subscriber starts it, later subscribers of the same selection share
//! its bounded broadcast channel (and immediately receive the current cache
//! contents as a complete snapshot), and after the final unsubscribe the
//! selection lingers briefly before its task exits. Rejoining inside the
//! linger window revives the warm selection without a relist.

use std::collections::HashMap;
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
                last_join: StdMutex::new(std::time::Instant::now()),
            },
        );
        drop(selections);

        tokio::spawn(run_selection(source, publisher, phase_tx, cancel.clone()));
        self.spawn_linger_monitor(selector, id, sender, cancel);
        receiver
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
    /// — never on a dying one.
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
                tokio::time::sleep(poll).await;
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
