//! Axum-based control server lifecycle with ordered graceful shutdown.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;

use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    http::StatusCode,
    response::Response,
    routing::get,
};
use k10s_backend::BackendKernel;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower_http::services::{ServeDir, ServeFile};

use crate::{
    config::ServerConfig,
    control::serve_socket,
    probes::{Readiness, ReadinessState, health, ready},
};

#[derive(Debug, Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: Arc<Semaphore>,
    authenticated: Arc<Semaphore>,
    signals: DrainSignals,
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    upgrades: Arc<UpgradeGate>,
    readiness: Arc<Readiness>,
    gate: Arc<MutationGate>,
}

/// Registry of spawned connection tasks giving shutdown hard-abort ownership.
///
/// The tracker only counts tasks; it cannot stop one that ignores the force
/// signal. Every connection task handle is retained here so forced teardown can
/// abort and join any survivor before `shutdown` returns.
#[derive(Debug, Default)]
pub struct ConnectionTasks {
    handles: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl ConnectionTasks {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn track(&self, handle: JoinHandle<()>) {
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        handles.retain(|handle| !handle.is_finished());
        handles.push(handle);
    }

    fn take(&self) -> Vec<JoinHandle<()>> {
        std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Counts accepted WebSocket upgrades that have not yet become tracked tasks.
///
/// `TaskTracker::close` does not refuse later spawns, so an upgrade accepted
/// just before shutdown could register its task after the drain observed an
/// empty tracker. The guard is taken synchronously before the upgrade response
/// is returned and released only once the task has joined the tracker (or the
/// upgrade future is dropped), closing that window.
#[derive(Debug, Default)]
pub struct UpgradeGate(AtomicUsize);

impl UpgradeGate {
    /// Construct an idle gate.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Number of accepted upgrades not yet running as tracked tasks.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    fn register(self: &Arc<Self>) -> UpgradeGuard {
        self.0.fetch_add(1, Ordering::AcqRel);
        UpgradeGuard(Arc::clone(self))
    }
}

/// Releases one [`UpgradeGate`] registration on drop.
#[derive(Debug)]
struct UpgradeGuard(Arc<UpgradeGate>);

impl Drop for UpgradeGuard {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Ordered shutdown signals delivered to every connection task.
///
/// `drain` is fired by the coordinator once notices are sent and the mutation
/// gate is closed; `force` stays silent unless the hard deadline expires.
#[derive(Debug, Clone)]
pub struct DrainSignals {
    /// Starts per-socket notice delivery and read-only drain windows.
    pub drain: CancellationToken,
    /// Preempts all socket loops during hard-deadline teardown.
    pub force: CancellationToken,
}

/// Shared admission switch that stops mutating operations once draining begins.
#[derive(Debug, Default)]
pub struct MutationGate(AtomicBool);

impl MutationGate {
    /// Construct a gate admitting mutations.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(AtomicBool::new(true)))
    }

    /// Whether mutating operations may still be dispatched.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Approved shutdown order, encoded as an explicitly sequenced state machine.
///
/// [`ShutdownStage`] values double as the monotonic progress marker asserted by
/// [`ShutdownCoordinator::advance`], so the approved order cannot be reordered
/// accidentally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShutdownStage {
    Serving,
    NotReady,
    ApplicationConnectionsClosed,
    NoticeSentAndGateClosed,
    WatchesLogsAndExecCancelled,
    ForceClosed,
    TasksDrained,
}

impl ShutdownStage {
    fn label(self) -> &'static str {
        match self {
            Self::Serving => "serving",
            Self::NotReady => "not-ready",
            Self::ApplicationConnectionsClosed => "application-connections-closed",
            Self::NoticeSentAndGateClosed => "notice-sent-and-gate-closed",
            Self::WatchesLogsAndExecCancelled => "watches-logs-exec-cancelled",
            Self::ForceClosed => "force-closed",
            Self::TasksDrained => "tasks-drained",
        }
    }
}

/// Sequential driver publishing every approved shutdown transition as telemetry.
struct ShutdownCoordinator {
    stage: ShutdownStage,
    readiness: Arc<Readiness>,
    gate: Arc<MutationGate>,
}

impl ShutdownCoordinator {
    fn advance(&mut self, stage: ShutdownStage) {
        assert!(stage > self.stage, "shutdown stages must advance in order");
        self.stage = stage;
        tracing::info!(
            target: "k10s_server::lifecycle",
            stage = stage.label(),
            "shutdown stage reached"
        );
    }

    fn mark_not_ready(&mut self) {
        self.readiness.set(ReadinessState::Draining);
        self.advance(ShutdownStage::NotReady);
    }

    fn stop_accepting_application_connections(&mut self) {
        self.advance(ShutdownStage::ApplicationConnectionsClosed);
    }

    fn send_notice_and_close_mutation_gate(&mut self, drain: &CancellationToken) {
        self.gate.close();
        drain.cancel();
        self.advance(ShutdownStage::NoticeSentAndGateClosed);
    }

    fn cancel_watches_logs_and_exec(&mut self) {
        self.advance(ShutdownStage::WatchesLogsAndExecCancelled);
    }
}

/// Wait for tracked connection tasks and pending upgrades until the deadline.
///
/// Publishes whether everything actually drained; the `TasksDrained` stage is
/// only advanced on success so telemetry never claims a drain that did not
/// happen. Returns that outcome to the caller, which decides between reporting
/// success and forcing teardown.
async fn drain_tracked_tasks(
    deadline: tokio::time::Instant,
    connections: Arc<TaskTracker>,
    upgrades: Arc<UpgradeGate>,
    drained_in_time: Arc<AtomicBool>,
) -> bool {
    connections.close();
    let completed = tokio::time::timeout_at(deadline, async {
        // An upgrade accepted before shutdown holds a pending registration that
        // only converts into a tracked task (or drops) later; waiting on the
        // tracker alone would report an empty server while that socket was
        // still about to start.
        while !connections.is_empty() || upgrades.pending() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    drained_in_time.store(completed, Ordering::Release);
    if !completed {
        tracing::warn!(
            target: "k10s_server::lifecycle",
            "connection tasks did not drain within the deadline"
        );
    }
    completed
}

/// Force-close every surviving connection task and join it.
///
/// The force signal lets well-behaved sockets unwind through their bounded
/// flush path; anything still alive after the unwind window is aborted so no
/// task can outlive `shutdown`.
async fn abort_surviving_tasks(
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    unwind_window: Duration,
) {
    let _ = tokio::time::timeout(unwind_window, connections.wait()).await;
    let survivors = tasks.take();
    if survivors.is_empty() {
        return;
    }
    tracing::warn!(
        target: "k10s_server::lifecycle",
        count = survivors.len(),
        "aborting connection tasks that ignored forced teardown"
    );
    for handle in &survivors {
        handle.abort();
    }
    for handle in survivors {
        let _ = handle.await;
    }
}

/// Handle for an embeddable loopback server.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl ServerHandle {
    /// Bound loopback address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully stop and join the server.
    pub async fn shutdown(self) -> io::Result<()> {
        self.cancel.cancel();
        self.task.await.map_err(io::Error::other)?
    }
}

/// Spawn a test-friendly server on an ephemeral loopback port.
pub async fn spawn_loopback(
    config: ServerConfig,
    kernel: BackendKernel,
) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(run(listener, config, kernel, task_cancel));
    Ok(ServerHandle { addr, cancel, task })
}

/// Serve an existing listener until cancellation.
pub async fn run(
    listener: TcpListener,
    config: ServerConfig,
    kernel: BackendKernel,
    cancel: CancellationToken,
) -> io::Result<()> {
    run_with_assets(listener, config, kernel, cancel, None).await
}

/// Serve an existing listener and optionally host one exact Trunk distribution tree.
///
/// The approved shutdown order is driven by [`ShutdownCoordinator`]: the caller
/// cancellation token wakes only the coordinator, which marks readiness,
/// refuses application upgrades, and only then fires the dedicated drain token
/// observed by connection tasks. `drain_timeout` is one absolute deadline
/// measured from shutdown start; if tracked tasks survive past it they are
/// force-closed, remaining survivors are aborted and joined — all before the
/// graceful-shutdown future completes, so the listener still closes last and
/// `/healthz` stays reachable through forced teardown. The server then reports
/// [`io::ErrorKind::TimedOut`].
pub async fn run_with_assets(
    listener: TcpListener,
    config: ServerConfig,
    kernel: BackendKernel,
    cancel: CancellationToken,
    dist_dir: Option<PathBuf>,
) -> io::Result<()> {
    let drain_timeout = config.drain_timeout;
    let flush_window = config.graceful_flush_timeout;
    let readiness = Readiness::new();
    let connections = Arc::new(TaskTracker::new());
    let tasks = ConnectionTasks::new();
    let upgrades = UpgradeGate::new();
    let gate = MutationGate::new();
    let signals = DrainSignals {
        drain: CancellationToken::new(),
        force: CancellationToken::new(),
    };
    let app = router(
        config,
        kernel,
        Arc::clone(&readiness),
        Arc::clone(&connections),
        Arc::clone(&tasks),
        Arc::clone(&upgrades),
        Arc::clone(&gate),
        signals.clone(),
        dist_dir,
    );
    readiness.set(ReadinessState::Ready);
    let coordinator = Arc::new(std::sync::Mutex::new(ShutdownCoordinator {
        stage: ShutdownStage::Serving,
        readiness: Arc::clone(&readiness),
        gate,
    }));
    let drained_in_time = Arc::new(AtomicBool::new(false));
    let root = cancel.clone();
    let coordinator_connections = Arc::clone(&connections);
    let coordinator_tasks = Arc::clone(&tasks);
    let coordinator_upgrades = Arc::clone(&upgrades);
    let coordinator_flag = Arc::clone(&drained_in_time);
    let shutdown_coordinator = Arc::clone(&coordinator);
    // The whole teardown sequence runs inside this future so the listener only
    // stops accepting once draining or forced teardown has finished.
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            root.cancelled().await;
            // One absolute deadline: every wait below measures against this
            // instant, never against a fresh window.
            let deadline = tokio::time::Instant::now() + drain_timeout;
            {
                let mut coordinator = shutdown_coordinator
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                coordinator.mark_not_ready();
                coordinator.stop_accepting_application_connections();
                coordinator.send_notice_and_close_mutation_gate(&signals.drain);
                coordinator.cancel_watches_logs_and_exec();
            }
            let completed = drain_tracked_tasks(
                deadline,
                coordinator_connections.clone(),
                coordinator_upgrades,
                coordinator_flag,
            )
            .await;
            if completed {
                shutdown_coordinator
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .advance(ShutdownStage::TasksDrained);
                return;
            }
            tracing::warn!(
                target: "k10s_server::lifecycle",
                "drain deadline exceeded; forcing connection teardown"
            );
            signals.force.cancel();
            abort_surviving_tasks(
                coordinator_connections,
                coordinator_tasks,
                flush_window * 2 + Duration::from_millis(50),
            )
            .await;
            shutdown_coordinator
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .advance(ShutdownStage::ForceClosed);
        })
        .await;
    drop(connections);
    if drained_in_time.load(Ordering::Acquire) {
        return result;
    }
    Err(io::Error::from(io::ErrorKind::TimedOut))
}

/// Build the application router with independently testable readiness state.
///
/// The `signals.drain` token is fired by the shutdown coordinator only after
/// readiness has been marked draining and application upgrades have been
/// refused; `signals.force` is reserved for the hard-deadline teardown.
/// Accepted upgrades are registered through `upgrades` before the upgrade
/// response is returned, and every spawned connection task handle is retained
/// in `tasks` so forced teardown retains abort ownership.
#[allow(clippy::too_many_arguments)]
pub fn router(
    config: ServerConfig,
    kernel: BackendKernel,
    readiness: Arc<Readiness>,
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    upgrades: Arc<UpgradeGate>,
    gate: Arc<MutationGate>,
    signals: DrainSignals,
    dist_dir: Option<PathBuf>,
) -> Router {
    let state = AppState {
        unauthenticated: Arc::new(Semaphore::new(config.max_unauthenticated_connections)),
        authenticated: Arc::new(Semaphore::new(config.max_authenticated_connections)),
        config: Arc::new(config),
        kernel: Arc::new(kernel),
        signals,
        connections,
        tasks,
        upgrades,
        readiness,
        gate,
    };
    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready_probe))
        .route(k10s_protocol::CONTROL_PATH, get(control_upgrade))
        .route(k10s_protocol::LOGS_PATH, get(not_implemented))
        .route(k10s_protocol::EXEC_PATH, get(not_implemented))
        .with_state(state);
    if let Some(dist_dir) = dist_dir {
        let index = dist_dir.join("index.html");
        app =
            app.fallback_service(ServeDir::new(dist_dir).not_found_service(ServeFile::new(index)));
    }
    app
}

async fn ready_probe(State(state): State<AppState>) -> (StatusCode, &'static str) {
    ready(Arc::clone(&state.readiness)).await
}

async fn not_implemented() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

async fn control_upgrade(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    if state.readiness.state() != ReadinessState::Ready {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let permit = state
        .unauthenticated
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let config = state.config.clone();
    let kernel = state.kernel.clone();
    let auth = state.authenticated.clone();
    let signals = state.signals.clone();
    let gate = state.gate.clone();
    let connections = state.connections.clone();
    let tasks = state.tasks.clone();
    // Register the accepted upgrade before the response leaves the server so a
    // shutdown racing this request can never observe an empty tracker while
    // this socket is still about to start.
    let upgrade_guard = state.upgrades.register();
    Ok(ws
        .max_frame_size(config.max_frame_size)
        .max_message_size(config.max_message_size)
        .on_upgrade(move |socket| async move {
            let handle = connections.spawn(serve_socket(
                socket, config, kernel, permit, auth, gate, signals,
            ));
            tasks.track(handle);
            drop(upgrade_guard);
        }))
}
