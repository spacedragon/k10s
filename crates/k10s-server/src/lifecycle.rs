//! Axum-based control server lifecycle with ordered graceful shutdown.

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
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
    resume::{ResumeState, ResumeStore},
};

#[derive(Debug, Clone)]
pub(crate) struct AppState {
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    port_forward: Option<Arc<crate::port_forward::PortForwardManager>>,
    unauthenticated: Arc<Semaphore>,
    authenticated: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    signals: DrainSignals,
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    admission: Arc<Admission>,
    readiness: Arc<Readiness>,
    gate: Arc<MutationGate>,
    resume: ResumeStore,
}

/// Registry of spawned connection tasks giving shutdown hard-abort ownership.
///
/// The tracker only counts tasks; it cannot stop one that ignores the force
/// signal. Every connection and writer task registers an abort handle here so
/// forced teardown can stop any survivor; completion is observed through the
/// tracker (session tasks) and the handles' finished flags.
#[derive(Debug, Default)]
pub struct ConnectionTasks {
    aborts: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}

impl ConnectionTasks {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn track(&self, handle: &JoinHandle<()>) {
        let mut aborts = self
            .aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        aborts.retain(|abort| !abort.is_finished());
        aborts.push(handle.abort_handle());
    }

    fn abort_all(&self) {
        for abort in self
            .aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            abort.abort();
        }
    }

    fn is_empty(&self) -> bool {
        self.aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|abort| abort.is_finished())
    }
}

/// Single source of truth for control-socket admission and upgrade ownership.
///
/// Accepting an upgrade and shutting down are independent operations on a
/// multithreaded runtime, so the acceptance decision, the pending-upgrade
/// registration, and the conversion into a running tracked task all happen
/// under one lock. A drain or forced teardown observes [`Self::has_live`] —
/// pending and running entries alike — through that same lock, which makes the
/// handoff atomic: an upgrade is either fully registered before the drain
/// looks, or its handler sees the closed barrier and rejects.
#[derive(Debug, Default)]
pub struct Admission(Mutex<AdmissionState>);

#[derive(Debug, Default)]
struct AdmissionState {
    closed: bool,
    next_id: u64,
    /// Ids of accepted upgrades that have not finished their session task.
    ///
    /// An id is inserted at acceptance and removed exactly once the upgrade is
    /// withdrawn or its session task completes, so the set only ever holds
    /// live work — completed connections never accumulate.
    live: HashSet<u64>,
}

impl Admission {
    /// Construct an open admission barrier.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register an accepted upgrade unless shutdown has closed the barrier.
    ///
    /// The readiness decision is part of the same critical section, so a
    /// handler can never accept an upgrade the drain cannot observe.
    pub(crate) fn try_register(self: &Arc<Self>, readiness: &Readiness) -> Option<AdmissionGuard> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || readiness.state() != ReadinessState::Ready {
            return None;
        }
        let id = state.next_id;
        state.next_id += 1;
        state.live.insert(id);
        Some(AdmissionGuard {
            id: Some(id),
            admission: Arc::clone(self),
        })
    }

    /// Close the barrier and publish the not-ready transition under one lock.
    ///
    /// Holding the admission lock while flipping readiness makes the handler
    /// decision atomic with shutdown: a registration that observed `Ready` is
    /// fully inserted before any later drain observation of this mutex, and a
    /// handler running after this call observes both `Draining` and closed.
    pub(crate) fn close_barrier(&self, readiness: &Readiness) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        readiness.set(ReadinessState::Draining);
        state.closed = true;
    }

    /// Whether any pending upgrade or running session task is still live.
    fn has_live(&self) -> bool {
        !self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live
            .is_empty()
    }

    /// Retire one admission id.
    fn retire(&self, id: u64) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live
            .remove(&id);
    }
}

/// Releases one [`Admission`] registration on drop unless it was converted.
pub(crate) struct AdmissionGuard {
    id: Option<u64>,
    admission: Arc<Admission>,
}

impl std::fmt::Debug for AdmissionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionGuard")
            .field("id", &self.id)
            .finish()
    }
}

impl AdmissionGuard {
    /// Hand ownership over to the spawned session task.
    ///
    /// The returned [`RunningGuard`] must be moved into the session task's
    /// future so the entry retires when that task completes — normally,
    /// aborted, or dropped — without any shutdown-time scanning.
    pub(crate) fn confirm_running(mut self) -> RunningGuard {
        let id = self.id.take();
        if let Some(id) = id {
            return RunningGuard {
                id: Some(id),
                admission: Arc::clone(&self.admission),
            };
        }
        RunningGuard {
            id: None,
            admission: Arc::clone(&self.admission),
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.admission.retire(id);
        }
    }
}

/// Keeps one admission entry alive for the lifetime of its session task.
///
/// Held inside the spawned future: dropping it — on completion, abort, or
/// cancellation — retires the entry under the admission lock.
pub(crate) struct RunningGuard {
    id: Option<u64>,
    admission: Arc<Admission>,
}

impl std::fmt::Debug for RunningGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningGuard")
            .field("id", &self.id)
            .finish()
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.admission.retire(id);
        }
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
    admission: Arc<Admission>,
    drained_in_time: Arc<AtomicBool>,
) -> bool {
    connections.close();
    let completed = tokio::time::timeout_at(deadline, async {
        // An upgrade accepted before shutdown is visible as a live admission
        // entry until its task has joined the tracker, so the tracker alone
        // can never report an empty server while a socket is about to start.
        while !connections.is_empty() || admission.has_live() {
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

/// Force-close every surviving connection task and pending upgrade, joining them.
///
/// Force-aware sockets first get the unwind window to unwind cleanly through
/// their bounded flush path so clients observe proper close frames. Anything
/// still alive afterwards — including upgrades accepted but not yet converted
/// into tracked tasks, and per-socket writer tasks — is aborted and joined, so
/// nothing can outlive `shutdown`.
async fn abort_surviving_tasks(
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    admission: Arc<Admission>,
    unwind_window: Duration,
) {
    let _ = tokio::time::timeout(unwind_window, connections.wait()).await;
    let _ = tokio::time::timeout(unwind_window, async {
        loop {
            tasks.abort_all();
            if connections.is_empty() && tasks.is_empty() && !admission.has_live() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
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
    config
        .validate()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
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
    config
        .validate()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let drain_timeout = config.drain_timeout;
    let flush_window = config.graceful_flush_timeout;
    let probe_drain_grace = config.probe_drain_grace;
    let readiness = Readiness::new();
    let connections = Arc::new(TaskTracker::new());
    let tasks = ConnectionTasks::new();
    let admission = Admission::new();
    let gate = MutationGate::new();
    let signals = DrainSignals {
        drain: CancellationToken::new(),
        force: CancellationToken::new(),
    };
    let startup_readiness_delay = config.startup_readiness_delay;
    // The bounded port-forward manager activates only for servers that
    // advertise `service.portForward`; standalone and web never do.
    let port_forward = if config
        .capabilities
        .iter()
        .any(|capability| capability == k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD)
    {
        kernel.port_forward_connector().map(|connector| {
            let (events_tx, _) = tokio::sync::broadcast::channel(64);
            Arc::new(crate::port_forward::PortForwardManager::new(
                connector,
                cancel.child_token(),
                events_tx,
            ))
        })
    } else {
        None
    };
    let app = router_with_port_forward(
        config,
        kernel,
        Arc::clone(&readiness),
        Arc::clone(&connections),
        Arc::clone(&tasks),
        Arc::clone(&admission),
        Arc::clone(&gate),
        signals.clone(),
        dist_dir,
        port_forward.clone(),
    );
    let port_forward_for_shutdown = port_forward;
    if startup_readiness_delay.is_zero() {
        readiness.set(ReadinessState::Ready);
    } else {
        let startup_readiness = Arc::clone(&readiness);
        let startup_cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(startup_readiness_delay) => {
                    startup_readiness.set(ReadinessState::Ready);
                }
                () = startup_cancel.cancelled() => {}
            }
        });
    }
    let coordinator = Arc::new(std::sync::Mutex::new(ShutdownCoordinator {
        stage: ShutdownStage::Serving,
        readiness: Arc::clone(&readiness),
        gate,
    }));
    let drained_in_time = Arc::new(AtomicBool::new(false));
    let root = cancel.clone();
    let coordinator_connections = Arc::clone(&connections);
    let coordinator_tasks = Arc::clone(&tasks);
    let coordinator_admission = Arc::clone(&admission);
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
                // The not-ready flip happens under the admission barrier so no
                // upgrade can be accepted outside the drain's observation.
                coordinator_admission.close_barrier(&coordinator.readiness);
                coordinator.mark_not_ready();
                coordinator.stop_accepting_application_connections();
                coordinator.send_notice_and_close_mutation_gate(&signals.drain);
                coordinator.cancel_watches_logs_and_exec();
            }
            if let Some(manager) = port_forward_for_shutdown {
                // Listeners and pumps are cancelled and joined inside the
                // same absolute deadline as every other tracked task.
                manager.shutdown().await;
            }
            if !probe_drain_grace.is_zero() {
                tokio::time::sleep(probe_drain_grace).await;
            }
            let completed = drain_tracked_tasks(
                deadline,
                coordinator_connections.clone(),
                Arc::clone(&coordinator_admission),
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
                coordinator_admission,
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
    admission: Arc<Admission>,
    gate: Arc<MutationGate>,
    signals: DrainSignals,
    dist_dir: Option<PathBuf>,
) -> Router {
    router_with_port_forward(
        config,
        kernel,
        readiness,
        connections,
        tasks,
        admission,
        gate,
        signals,
        dist_dir,
        None,
    )
}

/// [`router`] with an explicit port-forward manager; production wiring
/// builds one when the capability is enabled and the adapter supports it.
#[allow(clippy::too_many_arguments)]
pub fn router_with_port_forward(
    config: ServerConfig,
    kernel: BackendKernel,
    readiness: Arc<Readiness>,
    connections: Arc<TaskTracker>,
    tasks: Arc<ConnectionTasks>,
    admission: Arc<Admission>,
    gate: Arc<MutationGate>,
    signals: DrainSignals,
    dist_dir: Option<PathBuf>,
    port_forward: Option<Arc<crate::port_forward::PortForwardManager>>,
) -> Router {
    let resume = Arc::new(Mutex::new(ResumeState::new(
        config.resume_max_journal_entries,
        config.resume_max_sessions,
        config.resume_entry_max_age,
    )));
    let state = AppState {
        unauthenticated: Arc::new(Semaphore::new(config.max_unauthenticated_connections)),
        authenticated: Arc::new(Semaphore::new(config.max_authenticated_connections)),
        streams: Arc::new(Semaphore::new(config.max_stream_connections)),
        config: Arc::new(config.clone()),
        kernel: Arc::new(kernel),
        port_forward: port_forward.clone(),
        signals,
        connections,
        tasks,
        admission,
        readiness,
        gate,
        resume,
    };
    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready_probe))
        .route(k10s_protocol::CONTROL_PATH, get(control_upgrade))
        .route(k10s_protocol::LOGS_PATH, get(crate::logs::upgrade))
        .route(k10s_protocol::EXEC_PATH, get(crate::exec::upgrade))
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

/// Shared same-origin policy for browser WebSocket upgrades. Native clients
/// send no Origin header and are unconstrained; spoofed X-Forwarded-* style
/// headers are deliberately ignored (see README security section).
fn verify_origin(headers: &axum::http::HeaderMap) -> Result<(), StatusCode> {
    let origin = headers.get("origin");
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok());
    if let Some(text) = origin.and_then(|value| value.to_str().ok()) {
        // Present and decodable: only a true same-origin pair is admitted; the
        // check below also requires a parseable Host header.
        if !crate::origin::origin_matches_host(Some(text), host) {
            tracing::warn!(
                target: "k10s_server::lifecycle",
                ?text,
                ?host,
                "cross-origin upgrade rejected"
            );
            return Err(StatusCode::FORBIDDEN);
        }
    } else if origin.is_some() {
        // Present but undecodable bytes must fail closed instead of collapsing
        // into the absent-Origin allowance for native clients; log without the
        // raw header value.
        tracing::warn!(
            target: "k10s_server::lifecycle",
            ?host,
            "upgrade rejected: undecodable Origin header"
        );
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

/// Shared upgrade guard for the dedicated logs/exec stream sockets: the Plan
/// 1 admission barrier and unauthenticated-connection semaphore apply, with
/// the stream-specific frame and message limits.
pub(crate) async fn stream_upgrade(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
    route: crate::streams::StreamRoute,
) -> Result<Response, StatusCode> {
    verify_origin(&headers)?;
    let Some(upgrade_guard) = state.admission.try_register(&state.readiness) else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    // The dedicated stream cap is enforced independently of the control
    // pool: a live authenticated stream holds no unauthenticated-control
    // permit, so streams can never starve new control authentication.
    let stream_permit = match state.streams.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            drop(upgrade_guard);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let permit = match state.unauthenticated.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            drop(stream_permit);
            drop(upgrade_guard);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let config = state.config.clone();
    let kernel = state.kernel.clone();
    let connections = state.connections.clone();
    let signals = state.signals.clone();
    Ok(ws
        .max_frame_size(config.max_stream_frame_size)
        .max_message_size(config.max_stream_message_size)
        .on_upgrade(move |socket| async move {
            let running_guard = upgrade_guard.confirm_running();
            connections.spawn(async move {
                let _running = running_guard;
                crate::streams::serve_stream(
                    socket,
                    config,
                    kernel,
                    route,
                    signals,
                    permit,
                    stream_permit,
                )
                .await;
            });
        }))
}

async fn control_upgrade(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    // Same-origin policy for browser upgrades (shared with the dedicated
    // stream routes).
    verify_origin(&headers)?;

    // Registration and the readiness decision share one critical section, so
    // an accepted upgrade is always observable by the drain, and once draining
    // began no upgrade can be accepted at all.
    let Some(upgrade_guard) = state.admission.try_register(&state.readiness) else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    let permit = match state.unauthenticated.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            drop(upgrade_guard);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let config = state.config.clone();
    let kernel = state.kernel.clone();
    let port_forward_for_session = state.port_forward.clone();
    let auth = state.authenticated.clone();
    let signals = state.signals.clone();
    let gate = state.gate.clone();
    let connections = state.connections.clone();
    let tasks = state.tasks.clone();
    let resume = state.resume.clone();
    Ok(ws
        .max_frame_size(config.max_frame_size)
        .max_message_size(config.max_message_size)
        .on_upgrade(move |socket| async move {
            // The running guard lives inside the session future, retiring the
            // admission entry whenever the task ends; the registry retains
            // abort handles for both this task and its writer.
            let running_guard = upgrade_guard.confirm_running();
            let tasks_for_session = Arc::clone(&tasks);
            let handle = connections.spawn(async move {
                let _running = running_guard;
                serve_socket(
                    socket,
                    config,
                    kernel,
                    port_forward_for_session,
                    permit,
                    auth,
                    gate,
                    signals,
                    tasks_for_session,
                    resume,
                )
                .await;
            });
            tasks.track(&handle);
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pending upgrade must block drain success until it converts or drops,
    /// and releasing it must let the drain complete. This is the deterministic
    /// handoff test: registration is observable through the same lock that the
    /// drain consults, so a paused callback cannot be missed.
    #[tokio::test]
    async fn drain_waits_for_pending_upgrades_before_declaring_success() {
        let connections = Arc::new(TaskTracker::new());
        let admission = Admission::new();
        let readiness = Readiness::new();
        readiness.set(ReadinessState::Ready);
        let flag = Arc::new(AtomicBool::new(false));
        connections.close();

        let guard = admission
            .try_register(&readiness)
            .expect("open barrier admits a ready upgrade");
        let timed_out = drain_tracked_tasks(
            tokio::time::Instant::now() + Duration::from_millis(30),
            Arc::clone(&connections),
            Arc::clone(&admission),
            Arc::clone(&flag),
        )
        .await;
        assert!(!timed_out, "pending upgrade must block drain success");
        assert!(!flag.load(Ordering::Acquire));

        drop(guard);
        let completed = drain_tracked_tasks(
            tokio::time::Instant::now() + Duration::from_secs(2),
            connections,
            Arc::clone(&admission),
            flag,
        )
        .await;
        assert!(completed, "released upgrade must let the drain complete");

        // After close_barrier no registration succeeds even when ready.
        admission.close_barrier(&readiness);
        assert!(admission.try_register(&readiness).is_none());
    }

    /// A tracked task must block drain success until it finishes.
    #[tokio::test]
    async fn drain_waits_for_tracked_tasks_until_they_finish() {
        let connections = Arc::new(TaskTracker::new());
        let admission = Admission::new();
        let flag = Arc::new(AtomicBool::new(false));
        connections.close();
        let handle = connections.spawn(std::future::pending::<()>());

        let drained = drain_tracked_tasks(
            tokio::time::Instant::now() + Duration::from_millis(30),
            Arc::clone(&connections),
            Arc::clone(&admission),
            Arc::clone(&flag),
        )
        .await;
        assert!(!drained, "live task must block drain success");

        // Aborting the survivor (as forced teardown does) unblocks the drain.
        handle.abort();
        let completed = drain_tracked_tasks(
            tokio::time::Instant::now() + Duration::from_secs(2),
            connections,
            admission,
            flag,
        )
        .await;
        assert!(completed, "aborted survivor must let the drain complete");
    }

    /// Completed connections must retire their admission entries: after churn
    /// the registry holds nothing, so neither memory nor shutdown work grows
    /// with historical sessions.
    #[tokio::test]
    async fn confirmed_entries_retire_when_their_tasks_finish() {
        let admission = Admission::new();
        let readiness = Readiness::new();
        readiness.set(ReadinessState::Ready);

        for _ in 0..8 {
            let guard = admission
                .try_register(&readiness)
                .expect("open barrier admits a ready upgrade");
            let running = guard.confirm_running();
            let handle = tokio::spawn(async move {
                let _running = running;
            });
            let _ = handle.await;
        }

        let state = admission
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            state.live.is_empty(),
            "registry must return to zero after churn: {:?}",
            state.live.len()
        );
    }
}
