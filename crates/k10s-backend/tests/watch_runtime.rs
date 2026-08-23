//! Supervised demand-driven watch runtime tests.
//!
//! Drives [`ClusterWatches`] with scripted [`WatchSource`] implementations —
//! fake list/watch streams, no cluster, no kube types — and asserts the
//! documented lifecycle: first-subscriber start, shared selections, lingered
//! final unsubscribe, `Init`/`InitApply`/`InitDone` phases, apply/delete
//! deltas, restart after stream end, stale-but-visible cache during relist,
//! atomic cache replacement, and monotonic backend revisions.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use k10s_backend::runtime::{
    ClusterWatches, ListedState, WatchPhase, WatchRow, WatchSource, WatchUpdate,
};
use k10s_backend::watch::WatchSelector;
use k10s_backend::{BackendEvent, ContextInfo, Gvk, KubernetesAccess, ResourceRef, Subscribe};
use tokio::sync::{Notify, mpsc, oneshot};

fn pods_gvk() -> Gvk {
    Gvk::core("v1", "Pod")
}

fn pod_selector(namespace: Option<&str>) -> WatchSelector {
    WatchSelector {
        context: "dev".into(),
        gvk: pods_gvk(),
        namespace: namespace.map(str::to_owned),
    }
}

fn pod_ref(name: &str) -> ResourceRef {
    ResourceRef {
        context: "dev".into(),
        gvk: pods_gvk(),
        namespace: Some("default".into()),
        name: name.into(),
        uid: format!("uid-{name}"),
    }
}

fn pod_row(name: &str, summary: &str) -> WatchRow {
    WatchRow {
        reference: pod_ref(name),
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: "2026-08-21T00:00:00Z".into(),
        owner_references: Vec::new(),
    }
}

/// Shared controllable state of one scripted list/watch source.
#[derive(Debug, Default)]
struct ScriptState {
    /// Queued LIST results; the last entry repeats when exhausted.
    lists: StdMutex<VecDeque<ListedState>>,
    /// Buffered updates flushed into the next attached watch stream.
    pending_updates: StdMutex<Vec<WatchUpdate>>,
    /// Live sink of the attached watch stream, when attached.
    live_sink: StdMutex<Option<mpsc::UnboundedSender<WatchUpdate>>>,
    /// When set, the next LIST blocks until the gate fires.
    list_gate: StdMutex<Option<oneshot::Receiver<()>>>,
    /// When set, the next WATCH attach blocks until the gate fires.
    attach_gate: StdMutex<Option<oneshot::Receiver<()>>>,
    /// Bumped whenever the test ends the current watch stream; attaches exit
    /// once the epoch moves past the one they attached at.
    stream_epoch: std::sync::atomic::AtomicU64,
    stream_reset: Notify,
    list_calls: AtomicUsize,
    watch_calls: AtomicUsize,
}

/// A scripted [`WatchSource`]: deterministic list cuts and a manually fed
/// watch stream the test drives directly.
#[derive(Debug)]
struct ScriptedSource {
    state: Arc<ScriptState>,
}

impl ScriptedSource {
    fn new(lists: Vec<ListedState>) -> (Self, Arc<ScriptState>) {
        let state = Arc::new(ScriptState {
            lists: StdMutex::new(lists.into()),
            ..Default::default()
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl ScriptState {
    fn push_update(&self, update: WatchUpdate) {
        if let Some(sink) = self
            .live_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            let _ = sink.send(update);
        } else {
            self.pending_updates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(update);
        }
    }

    fn end_stream(&self) {
        self.stream_epoch.fetch_add(1, Ordering::SeqCst);
        self.stream_reset.notify_waiters();
    }

    fn gate_next_list(&self, gate: oneshot::Receiver<()>) {
        *self
            .list_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
    }

    fn gate_next_attach(&self, gate: oneshot::Receiver<()>) {
        *self
            .attach_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(gate);
    }
}

impl WatchSource for ScriptedSource {
    fn list<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ListedState, String>> + Send + 'a>>
    {
        Box::pin(async move {
            self.state.list_calls.fetch_add(1, Ordering::SeqCst);
            let gate = self
                .state
                .list_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            let mut lists = self
                .state
                .lists
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match lists.pop_front() {
                Some(listed) => Ok(listed),
                None => Err("scripted source exhausted".into()),
            }
        })
    }

    fn attach_watch<'a>(
        &'a self,
        _resource_version: String,
        out: mpsc::UnboundedSender<WatchUpdate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.state.watch_calls.fetch_add(1, Ordering::SeqCst);
            let attach_gate = self
                .state
                .attach_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(gate) = attach_gate {
                let _ = gate.await;
            }
            {
                let mut pending = self
                    .state
                    .pending_updates
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for update in pending.drain(..) {
                    let _ = out.send(update);
                }
            }
            *self
                .state
                .live_sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(out);
            let attached_epoch = self.state.stream_epoch.load(Ordering::SeqCst);
            loop {
                if self.state.stream_epoch.load(Ordering::SeqCst) != attached_epoch {
                    return;
                }
                tokio::select! {
                    _ = self.state.stream_reset.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        })
    }
}

fn listed(resource_version: &str, rows: &[WatchRow]) -> ListedState {
    ListedState {
        resource_version: resource_version.to_owned(),
        rows: rows.to_vec(),
    }
}

async fn next_event(events: &mut tokio::sync::broadcast::Receiver<BackendEvent>) -> BackendEvent {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("event arrives within timeout")
        .expect("watch channel stays open")
}

async fn wait_for_phase(world: &ClusterWatches, selector: &WatchSelector, target: WatchPhase) {
    for _ in 0..600 {
        if world.phase(selector) == Some(target) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("selection never reached {target:?}");
}

#[tokio::test]
async fn first_subscriber_starts_one_supervised_selection() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![listed(
        "11",
        &[pod_row("web", "Running"), pod_row("api", "Pending")],
    )]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let mut events = world.subscribe(pod_selector(Some("default")), Arc::clone(&source));

    wait_for_phase(&world, &pod_selector(Some("default")), WatchPhase::InitDone).await;

    assert_eq!(script.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(script.watch_calls.load(Ordering::SeqCst), 1);

    let BackendEvent::Snapshot(snapshot) = next_event(&mut events).await else {
        panic!("first event must be the snapshot");
    };
    assert_eq!(snapshot.context, "dev");
    assert_eq!(snapshot.gvk, pods_gvk());
    assert_eq!(snapshot.namespace.as_deref(), Some("default"));
    let names: Vec<_> = snapshot
        .rows
        .iter()
        .map(|r| r.reference.name.as_str())
        .collect();
    assert_eq!(names, ["api", "web"], "snapshot rows arrive sorted");

    // The opaque Kubernetes resourceVersion never leaks into events.
    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("resource_version"));
    assert!(!debug.contains("\"11\""));
}

#[tokio::test]
async fn second_subscriber_shares_the_running_selection() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![listed("11", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let mut first = world.subscribe(selector.clone(), Arc::clone(&source));
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    let mut second = world.subscribe(selector.clone(), Arc::clone(&source));
    let _ = next_event(&mut first).await;

    // One supervised selection: exactly one LIST and one WATCH so far.
    assert_eq!(script.list_calls.load(Ordering::SeqCst), 1);
    assert_eq!(script.watch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(world.live_selections(), 1);

    // A late joiner still receives the full current snapshot.
    let BackendEvent::Snapshot(joined) = next_event(&mut second).await else {
        panic!("late joiner must receive a snapshot");
    };
    assert_eq!(joined.rows.len(), 1);
    assert_eq!(joined.rows[0].reference.name, "web");
}

#[tokio::test]
async fn final_unsubscribe_lingers_then_exits() {
    let world = ClusterWatches::new(Duration::from_millis(120));
    let (source, _script) = ScriptedSource::new(vec![listed("11", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let weak = Arc::downgrade(&source);
    let events = world.subscribe(selector.clone(), Arc::clone(&source));
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    drop(events);
    drop(source);

    // Still inside the linger window: the selection stays warm.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        world.live_selections(),
        1,
        "the selection lingers after the last unsubscribe"
    );

    for _ in 0..600 {
        if world.live_selections() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        world.live_selections(),
        0,
        "the selection exits after linger"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        weak.upgrade().is_none(),
        "the supervised task released its source"
    );
}

#[tokio::test]
async fn resubscribe_inside_the_linger_window_keeps_the_selection_alive() {
    let world = ClusterWatches::new(Duration::from_millis(150));
    let (source, script) = ScriptedSource::new(vec![listed("11", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let first = world.subscribe(selector.clone(), Arc::clone(&source));
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    drop(first);

    // Rejoin while the previous subscriber is still lingering out.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = world.subscribe(selector.clone(), Arc::clone(&source));
    let joined = next_event(&mut second).await;
    assert!(
        matches!(joined, BackendEvent::Snapshot(_)),
        "rejoining inside the linger window revives the warm selection"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(world.live_selections(), 1, "the warm selection survived");
    assert_eq!(
        script.list_calls.load(Ordering::SeqCst),
        1,
        "no relist was needed"
    );
}

#[tokio::test]
async fn phases_progress_through_init_init_apply_init_done() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![listed("11", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    // Gate both the LIST and the WATCH attach so every phase is observable
    // deterministically while the supervisor sits inside it.
    let (list_open, list_gate) = oneshot::channel();
    let (attach_open, attach_gate) = oneshot::channel();
    script.gate_next_list(list_gate);
    script.gate_next_attach(attach_gate);

    let _events = world.subscribe(selector.clone(), Arc::clone(&source));

    // Init: the gated relist is in flight.
    for _ in 0..200 {
        if world.phase(&selector) == Some(WatchPhase::Init) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        world.phase(&selector),
        Some(WatchPhase::Init),
        "the blocked list holds the selection in Init"
    );

    let mut phases = world.phases(&selector).expect("selection exposes phases");
    let mut observed = vec![*phases.borrow_and_update()];
    let _ = list_open.send(());

    // InitApply: the cut landed; the gated watch attach has not run yet.
    for _ in 0..200 {
        if world.phase(&selector) == Some(WatchPhase::InitApply) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        world.phase(&selector),
        Some(WatchPhase::InitApply),
        "the blocked attach holds the selection in InitApply"
    );
    observed.push(*phases.borrow_and_update());

    let _ = attach_open.send(());
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    observed.push(*phases.borrow_and_update());

    assert_eq!(
        observed,
        vec![
            WatchPhase::Init,
            WatchPhase::InitApply,
            WatchPhase::InitDone
        ],
        "the selection walks every documented phase in order"
    );
}

#[tokio::test]
async fn apply_and_delete_deltas_update_cache_and_broadcast_monotonically() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![listed("11", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let mut events = world.subscribe(selector.clone(), Arc::clone(&source));
    let BackendEvent::Snapshot(snapshot) = next_event(&mut events).await else {
        panic!("snapshot first");
    };
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;

    script.push_update(WatchUpdate::Upsert(pod_row("web", "CrashLoopBackOff")));
    script.push_update(WatchUpdate::Delete(pod_ref("web")));

    let BackendEvent::Changed(changed) = next_event(&mut events).await else {
        panic!("apply flows as a changed delta");
    };
    assert_eq!(changed.reference.name, "web");
    assert_eq!(changed.summary, "CrashLoopBackOff");
    assert!(
        changed.revision > snapshot.revision,
        "delta revisions advance past the snapshot"
    );

    let BackendEvent::Gone {
        reference,
        revision,
    } = next_event(&mut events).await
    else {
        panic!("delete flows as a gone delta");
    };
    assert_eq!(reference.name, "web");
    assert!(revision > changed.revision, "revisions stay monotonic");

    assert!(
        world.cached_rows(&selector).expect("warm cache").is_empty(),
        "the deleted row left the cache"
    );
}

#[tokio::test]
async fn watch_stream_restart_relists_and_republishes_atomically() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![
        listed("11", &[pod_row("web", "Running")]),
        listed(
            "22",
            &[pod_row("web", "Running"), pod_row("api", "Pending")],
        ),
    ]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let mut events = world.subscribe(selector.clone(), Arc::clone(&source));
    let BackendEvent::Snapshot(first) = next_event(&mut events).await else {
        panic!("snapshot first");
    };
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;

    script.end_stream();

    let BackendEvent::Snapshot(second) = next_event(&mut events).await else {
        panic!("restart republishes a full snapshot");
    };
    assert_eq!(second.rows.len(), 2, "the fresh cut replaces the old one");
    assert!(
        second.revision > first.revision,
        "restart revisions stay monotonic"
    );
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    assert_eq!(
        world.phase(&selector),
        Some(WatchPhase::InitDone),
        "the restarted selection settles back into InitDone"
    );
}

#[tokio::test]
async fn cache_stays_visible_but_marked_stale_during_relist() {
    let world = ClusterWatches::new(Duration::from_millis(50));
    let (source, script) = ScriptedSource::new(vec![
        listed("11", &[pod_row("web", "Running")]),
        listed(
            "22",
            &[pod_row("web", "Running"), pod_row("api", "Pending")],
        ),
    ]);
    let source: Arc<dyn WatchSource> = Arc::new(source);
    let selector = pod_selector(Some("default"));

    let _events = world.subscribe(selector.clone(), Arc::clone(&source));
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;

    // Block the next LIST, then force a restart: the relist hangs in Init.
    let (gate_tx, gate_rx) = oneshot::channel();
    script.gate_next_list(gate_rx);
    script.end_stream();
    for _ in 0..300 {
        if world.phase(&selector) == Some(WatchPhase::Init) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        world.phase(&selector),
        Some(WatchPhase::Init),
        "the blocked relist sits in Init"
    );
    assert_eq!(
        world.cache_stale(&selector),
        Some(true),
        "the cache is flagged stale during relist"
    );
    let rows = world
        .cached_rows(&selector)
        .expect("old rows remain visible");
    assert_eq!(rows.len(), 1, "stale data stays readable until replacement");
    assert_eq!(rows[0].reference.name, "web");

    // Release the gate: the relist completes and freshness returns.
    let _ = gate_tx.send(());
    wait_for_phase(&world, &selector, WatchPhase::InitDone).await;
    assert_eq!(world.cache_stale(&selector), Some(false));
    assert_eq!(
        world.cached_rows(&selector).expect("warm cache").len(),
        2,
        "the fresh cut replaced the stale rows"
    );
}

#[test]
fn cache_replacement_is_atomic_never_half_initialized() {
    use k10s_backend::runtime::{RevisionCounter, SummaryCache};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    let cache = SummaryCache::new_for(pod_selector(Some("default")));
    let revisions = RevisionCounter::new();
    let state_a: Vec<WatchRow> = (0..400)
        .map(|index| pod_row(&format!("a-{index:03}"), "Running"))
        .collect();

    let stop = Arc::new(AtomicBool::new(false));
    let saw_other_length = Arc::new(AtomicUsize::new(0));

    let reader_cache = Arc::new(cache);
    let writer_cache = Arc::clone(&reader_cache);
    // Seed the first full state before the reader starts so its baseline is
    // well-defined.
    let _ = writer_cache.replace(state_a, &revisions);
    let reader_stop = Arc::clone(&stop);
    let reader_flag = Arc::clone(&saw_other_length);
    let reader = std::thread::spawn(move || {
        while !reader_stop.load(Ordering::Relaxed) {
            let rows = reader_cache.snapshot();
            if rows.is_empty() {
                continue;
            }
            // Every coherent state carries rows of exactly one generation
            // prefix (`a-…` or `b-{round}-…`); a torn replacement would mix
            // two prefixes in one observed snapshot.
            let prefix = rows[0]
                .reference
                .name
                .rsplit_once('-')
                .map(|(head, _)| head.to_owned())
                .unwrap_or_default();
            if rows.iter().any(|row| {
                !row.reference.name.starts_with(&prefix)
                    || row.reference.name.rsplit_once('-').map(|(head, _)| head)
                        != Some(prefix.as_str())
            }) {
                reader_flag.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    for round in 0..200_u32 {
        let rows: Vec<WatchRow> = (0..160)
            .map(|index| pod_row(&format!("b-{round:03}-{index:03}"), "Pending"))
            .collect();
        let _ = writer_cache.replace(rows, &revisions);
    }
    let restored: Vec<WatchRow> = (0..400)
        .map(|index| pod_row(&format!("a-{index:03}"), "Running"))
        .collect();
    let _ = writer_cache.replace(restored, &revisions);
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("reader exits cleanly");

    assert_eq!(
        saw_other_length.load(Ordering::Relaxed),
        0,
        "a reader never observed a half-applied replacement"
    );
}

#[tokio::test]
async fn unknown_context_and_gvk_are_typed_not_founds_on_the_adapter() {
    use k10s_backend::testkit::RecordedApiServer;

    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let adapter = k10s_backend::KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "dev".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [("dev", client)],
    )
    .expect("adapter builds");

    let missing_context = adapter
        .subscribe(Subscribe::ResourceWatch {
            context: "missing".into(),
            gvk: pods_gvk(),
            namespace: Some("default".into()),
        })
        .await;
    assert!(
        matches!(missing_context, Err(k10s_backend::BackendError::NotFound)),
        "unknown contexts are typed not-founds: {missing_context:?}"
    );

    let missing_gvk = adapter
        .subscribe(Subscribe::ResourceWatch {
            context: "dev".into(),
            gvk: Gvk::new("example.com", "v1", "DoesNotExist"),
            namespace: None,
        })
        .await;
    assert!(
        matches!(missing_gvk, Err(k10s_backend::BackendError::NotFound)),
        "unknown gvk are typed not-founds: {missing_gvk:?}"
    );
}

#[tokio::test]
async fn kube_adapter_serves_a_scripted_resource_watch() {
    use k10s_backend::runtime::RuntimeWatchScript;
    use k10s_backend::testkit::RecordedApiServer;

    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let (source, _script) = ScriptedSource::new(vec![listed("31", &[pod_row("web", "Running")])]);
    let source: Arc<dyn WatchSource> = Arc::new(source);

    let scripted: RuntimeWatchScript =
        Arc::new(move |_gvk, _namespace| Some(Arc::clone(&source) as Arc<dyn WatchSource>));
    let adapter = k10s_backend::KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "dev".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [("dev", client)],
    )
    .expect("adapter builds")
    .with_scripted_watches(scripted);

    let mut handle = adapter
        .subscribe(Subscribe::ResourceWatch {
            context: "dev".into(),
            gvk: pods_gvk(),
            namespace: Some("default".into()),
        })
        .await
        .expect("scripted resource watch subscribes");
    let mut events = handle.take_events().expect("watches carry events");

    let BackendEvent::Snapshot(snapshot) = next_event(&mut events).await else {
        panic!("snapshot first");
    };
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].reference.name, "web");
    assert_eq!(
        snapshot.revision,
        k10s_backend::runtime::INITIAL_WATCH_REVISION
    );
}
