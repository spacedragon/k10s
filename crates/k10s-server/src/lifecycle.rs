//! Axum-based control server lifecycle with ordered graceful shutdown.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
    readiness: Arc<Readiness>,
    gate: Arc<MutationGate>,
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

/// Wait for tracked connection tasks until the absolute deadline.
///
/// Publishes whether the tracker actually emptied; the `TasksDrained` stage is
/// only advanced on success so telemetry never claims a drain that did not
/// happen. Returns that outcome to the caller, which decides between reporting
/// success and forcing teardown.
async fn drain_tracked_tasks(
    deadline: tokio::time::Instant,
    connections: Arc<TaskTracker>,
    drained_in_time: Arc<AtomicBool>,
) -> bool {
    connections.close();
    let completed = tokio::time::timeout_at(deadline, connections.wait())
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
/// force-closed through the `force` signal and the server reports
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
    let force = signals.force.clone();
    let root = cancel.clone();
    let coordinator_connections = Arc::clone(&connections);
    let coordinator_flag = Arc::clone(&drained_in_time);
    let shutdown_coordinator = Arc::clone(&coordinator);
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
            let completed =
                drain_tracked_tasks(deadline, coordinator_connections, coordinator_flag).await;
            if completed {
                shutdown_coordinator
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .advance(ShutdownStage::TasksDrained);
            }
        })
        .await;
    connections.close();
    if drained_in_time.load(Ordering::Acquire) {
        return result;
    }
    tracing::warn!(
        target: "k10s_server::lifecycle",
        "drain deadline exceeded; forcing connection teardown"
    );
    force.cancel();
    // Force-close must complete before returning: every socket loop observes
    // the force signal immediately and unwinds through its bounded flush path.
    let unwind_window = flush_window.saturating_mul(2) + Duration::from_millis(50);
    if tokio::time::timeout(unwind_window, connections.wait())
        .await
        .is_err()
    {
        tracing::error!(
            target: "k10s_server::lifecycle",
            "connection tasks survived forced teardown"
        );
    }
    coordinator
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .advance(ShutdownStage::ForceClosed);
    Err(io::Error::from(io::ErrorKind::TimedOut))
}

/// Build the application router with independently testable readiness state.
///
/// The `signals.drain` token is fired by the shutdown coordinator only after
/// readiness has been marked draining and application upgrades have been
/// refused; `signals.force` is reserved for the hard-deadline teardown.
#[allow(clippy::too_many_arguments)]
pub fn router(
    config: ServerConfig,
    kernel: BackendKernel,
    readiness: Arc<Readiness>,
    connections: Arc<TaskTracker>,
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
    Ok(ws
        .max_frame_size(config.max_frame_size)
        .max_message_size(config.max_message_size)
        .on_upgrade(move |socket| async move {
            let _ = connections
                .spawn(serve_socket(
                    socket, config, kernel, permit, auth, gate, signals,
                ))
                .await;
        }))
}
