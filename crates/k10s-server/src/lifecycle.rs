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
    drain: CancellationToken,
    connections: Arc<TaskTracker>,
    readiness: Arc<Readiness>,
    gate: Arc<MutationGate>,
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

    async fn drain_tracked_tasks(&mut self, deadline: Duration, connections: Arc<TaskTracker>) {
        connections.close();
        let completed = tokio::time::timeout(deadline, connections.wait())
            .await
            .is_ok();
        tracing::info!(
            target: "k10s_server::lifecycle",
            stage = ShutdownStage::TasksDrained.label(),
            completed,
            "connection tasks joined within the drain deadline"
        );
        self.advance(ShutdownStage::TasksDrained);
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
pub async fn run_with_assets(
    listener: TcpListener,
    config: ServerConfig,
    kernel: BackendKernel,
    cancel: CancellationToken,
    dist_dir: Option<PathBuf>,
) -> io::Result<()> {
    let drain_timeout = config.drain_timeout;
    let readiness = Readiness::new();
    let connections = Arc::new(TaskTracker::new());
    let gate = MutationGate::new();
    let app = router(
        config,
        kernel,
        cancel.clone(),
        Arc::clone(&readiness),
        Arc::clone(&connections),
        Arc::clone(&gate),
        dist_dir,
    );
    readiness.set(ReadinessState::Ready);
    let mut coordinator = ShutdownCoordinator {
        stage: ShutdownStage::Serving,
        readiness: Arc::clone(&readiness),
        gate,
    };
    let root = cancel.clone();
    let drain_connections = Arc::clone(&connections);
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            root.cancelled().await;
            coordinator.mark_not_ready();
            coordinator.stop_accepting_application_connections();
            coordinator.send_notice_and_close_mutation_gate(&cancel);
            coordinator.cancel_watches_logs_and_exec();
            coordinator
                .drain_tracked_tasks(drain_timeout, drain_connections)
                .await;
        })
        .await;
    // Axum does not drain upgraded WebSocket tasks; join any straggler left past
    // the deadline so callers never drop the runtime mid-shutdown-notice.
    connections.close();
    connections.wait().await;
    result
}

/// Build the application router with independently testable readiness state.
#[allow(clippy::too_many_arguments)]
pub fn router(
    config: ServerConfig,
    kernel: BackendKernel,
    cancel: CancellationToken,
    readiness: Arc<Readiness>,
    connections: Arc<TaskTracker>,
    gate: Arc<MutationGate>,
    dist_dir: Option<PathBuf>,
) -> Router {
    let state = AppState {
        unauthenticated: Arc::new(Semaphore::new(config.max_unauthenticated_connections)),
        authenticated: Arc::new(Semaphore::new(config.max_authenticated_connections)),
        config: Arc::new(config),
        kernel: Arc::new(kernel),
        drain: cancel.clone(),
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
    let drain = state.drain.clone();
    let gate = state.gate.clone();
    let connections = state.connections.clone();
    Ok(ws
        .max_frame_size(config.max_frame_size)
        .max_message_size(config.max_message_size)
        .on_upgrade(move |socket| async move {
            let _ = connections
                .spawn(serve_socket(
                    socket, config, kernel, permit, auth, gate, drain,
                ))
                .await;
        }))
}
