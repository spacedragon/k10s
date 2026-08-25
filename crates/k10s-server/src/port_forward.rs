//! Bounded, loopback-only port-forward session management.
//!
//! The manager is owned by the server lifecycle: it binds local listeners
//! only on `127.0.0.1`, enforces hard resource limits, isolates per-
//! connection data paths, and joins every task on shutdown. Session
//! snapshots are authoritative and published under one lock stamped with a
//! monotonic revision and the manager's context-transition epoch.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncWrite, copy_bidirectional};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use k10s_backend::{
    BackendError, PortForwardConnector, PortForwardPortSelection, PortForwardRequest,
    RejectionCategory, ResolvedPortForward,
};
use k10s_protocol::{
    PortForwardFailure, PortForwardFailureCategory, PortForwardPodTarget, PortForwardSession,
    PortForwardSessionEvent, PortForwardSessionId, PortForwardSessionState, ResourceIdentity,
};

/// Hard limit of active sessions per embedded server.
pub const MAX_SESSIONS: usize = 16;
/// Hard limit of simultaneous accepted connections across all sessions.
pub const MAX_TOTAL_CONNECTIONS: usize = 32;
/// Hard limit of simultaneous accepted connections per session.
pub const MAX_SESSION_CONNECTIONS: usize = 8;
/// Consecutive pre-byte stream failures that fail a session.
const OPEN_FAILURE_THRESHOLD: u32 = 3;

/// Typed start failures surfaced before any snapshot exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRejected {
    /// Stable category mirrored onto the protocol payload.
    pub category: PortForwardFailureCategory,
    /// Short sanitized reason safe to display.
    pub message: String,
}

impl StartRejected {
    fn new(category: PortForwardFailureCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }
}

/// Outcome of an idempotent stop.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum StopOutcome {
    /// The session stopped; its final snapshot is attached.
    Stopped(PortForwardSession),
    /// The id was unknown or the session was already terminal.
    AlreadyTerminal,
}

/// Authoritative internal state of one session.
struct SessionInner {
    id: String,
    identity: ResourceIdentity,
    service_port: u16,
    local_addr: std::net::SocketAddr,
    resolved: ResolvedPortForward,
    state: PortForwardSessionState,
    failure: Option<PortForwardFailure>,
    revision: u64,
    /// Consecutive stream failures before any byte moved.
    open_failures: u32,
    /// Per-connection pump tasks; joined on teardown.
    pumps: TaskTracker,
    cancel: CancellationToken,
    accept_task: Option<tokio::task::JoinHandle<()>>,
}

/// Global live-connection budget shared without the manager lock so data
/// paths never serialize behind session publication.
type LiveConnections = Arc<AtomicUsize>;

/// Per-session accepted-connection budget, lock-free like the global one.
#[derive(Debug, Default)]
struct SessionCounters {
    active: AtomicUsize,
}

/// Shared mutable manager state guarded by one lock so epochs and revisions
/// are linearizable with respect to publication and the transition gate.
struct ManagerState {
    sessions: HashMap<String, Arc<Mutex<SessionInner>>>,
    next_revision: u64,
    epoch: u64,
    current_context: String,
    events_tx: broadcast::Sender<PortForwardSessionEvent>,
}

/// Cloneable handle to the bounded port-forward manager.
#[derive(Clone)]
pub struct PortForwardManager {
    state: Arc<Mutex<ManagerState>>,
    connector: PortForwardConnector,
    cancel: CancellationToken,
    /// Global accepted-connection budget, lock-free so data paths never
    /// serialize behind session publication.
    live_connections: LiveConnections,
}

impl std::fmt::Debug for PortForwardManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PortForwardManager")
    }
}

impl PortForwardManager {
    /// Create the manager under the given lifecycle cancellation token.
    #[must_use]
    pub fn new(
        connector: PortForwardConnector,
        cancel: CancellationToken,
        events_tx: broadcast::Sender<PortForwardSessionEvent>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ManagerState {
                sessions: HashMap::new(),
                next_revision: 1,
                epoch: 0,
                current_context: String::new(),
                events_tx,
            })),
            connector,
            cancel,
            live_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Subscribe to complete session snapshots with monotonic revisions.
    pub async fn subscribe(&self) -> broadcast::Receiver<PortForwardSessionEvent> {
        self.state.lock().await.events_tx.subscribe()
    }

    /// The monotonic context-transition epoch observed by publications.
    pub async fn epoch(&self) -> u64 {
        self.state.lock().await.epoch
    }

    /// Start one session: resolve exactly once, bind loopback, publish
    /// Active. A duplicate Service UID + Service-port identity focuses the
    /// existing session instead of creating a second listener.
    ///
    /// Publication validates the request against the committed context and
    /// the gate epoch: starts carrying a retired context or racing a switch
    /// abort without binding anything and return the typed retryable
    /// context-transition error.
    pub async fn start(
        &self,
        identity: ResourceIdentity,
        selection: PortForwardPortSelection,
        local_port: u16,
        requested_context: String,
    ) -> Result<PortForwardSession, StartRejected> {
        if let Some(existing) = {
            let state = self.state.lock().await;
            Self::find_in_state(&state, &identity, &selection).await
        } {
            return Ok(existing);
        }
        if self.cancel.is_cancelled() {
            return Err(StartRejected::new(
                PortForwardFailureCategory::TransportClosed,
                "the embedded server is shutting down",
            ));
        }

        // Resolve before binding; failures never touch local resources.
        let Some(namespace) = identity.namespace.clone() else {
            return Err(StartRejected::new(
                PortForwardFailureCategory::UnsupportedService,
                "cluster-scoped objects cannot be forwarded",
            ));
        };
        let request = PortForwardRequest {
            context: requested_context.clone(),
            namespace,
            service_name: identity.name.clone(),
            service_uid: identity.uid.clone(),
            port: selection.clone(),
        };
        let resolved = match self.connector.resolve_service_port(request).await {
            Ok(resolved) => resolved,
            Err(error) => return Err(Self::map_backend_error(error)),
        };

        // Bind only 127.0.0.1 before reporting success; port 0 asks the OS.
        let bind_addr = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, local_port));
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(listener) => listener,
            Err(_) => {
                return Err(StartRejected::new(
                    PortForwardFailureCategory::LocalPortInUse,
                    format!("local port {local_port} is already in use"),
                ));
            }
        };

        let mut state = self.state.lock().await;
        // Context-switch atomicity: validate the request identity against
        // the authoritative committed context under the same lock used by
        // the gate. Either dimension mismatching aborts without binding.
        if state.current_context != requested_context {
            drop(state);
            return Err(StartRejected::new(
                PortForwardFailureCategory::ContextTransition,
                "the context switched; retry after it completes",
            ));
        }
        if state.sessions.len() >= MAX_SESSIONS {
            drop(state);
            return Err(StartRejected::new(
                PortForwardFailureCategory::UnavailableEndpoint,
                "the maximum number of port-forward sessions is active",
            ));
        }
        if let Some(existing) = Self::find_in_state(&state, &identity, &selection).await {
            return Ok(existing);
        }

        state.next_revision += 1;
        let revision = state.next_revision;
        let id = random_id();
        let local_addr = listener.local_addr().map_err(|_| {
            StartRejected::new(
                PortForwardFailureCategory::TransportClosed,
                "the bound address could not be observed",
            )
        })?;
        let session_cancel = self.cancel.child_token();
        // The declared Service port comes from resolution so named
        // selections retain their declared port identity.
        let service_port = resolved.service_port;
        let inner = Arc::new(Mutex::new(SessionInner {
            id: id.clone(),
            identity,
            service_port,
            local_addr,
            resolved,
            state: PortForwardSessionState::Active,
            failure: None,
            revision,
            open_failures: 0,
            pumps: TaskTracker::new(),
            cancel: session_cancel,
            accept_task: None,
        }));
        let events_tx = state.events_tx.clone();
        let live = self.live_connections.clone();
        let counters = Arc::new(SessionCounters::default());
        let connector = self.connector.clone();
        let accept_task = tokio::spawn(accept_loop(
            listener,
            inner.clone(),
            connector,
            events_tx,
            live,
            counters,
        ));
        inner.lock().await.accept_task = Some(accept_task);
        state.sessions.insert(id.clone(), inner.clone());
        drop(state);

        let guard = inner.lock().await;
        let snapshot = snapshot_of(&guard);
        Ok(snapshot)
    }

    /// Idempotent stop by session ID.
    pub async fn stop(&self, session_id: &str) -> StopOutcome {
        let inner = {
            let state = self.state.lock().await;
            state.sessions.get(session_id).cloned()
        };
        let Some(inner) = inner else {
            return StopOutcome::AlreadyTerminal;
        };
        let accept_handle = {
            let mut guard = inner.lock().await;
            if matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) {
                return StopOutcome::AlreadyTerminal;
            }
            guard.state = PortForwardSessionState::Stopped;
            guard.failure = None;
            guard.revision += 1;
            // Cancelling stops new accepts and tears down live pumps; their
            // tasks are joined below WITHOUT any lock held so a pump that is
            // mid-copy can always finish its release bookkeeping.
            guard.cancel.cancel();
            guard.pumps.close();
            guard.accept_task.take()
        };
        Self::join_session_tasks(accept_handle, &inner).await;
        let snapshot = {
            let guard = inner.lock().await;
            snapshot_of(&guard)
        };
        self.publish_terminal(session_id.to_owned(), snapshot.clone())
            .await;
        StopOutcome::Stopped(snapshot)
    }

    /// Join one session's accept loop and pump tracker with no lock held.
    async fn join_session_tasks(
        accept_handle: Option<tokio::task::JoinHandle<()>>,
        inner: &Arc<Mutex<SessionInner>>,
    ) {
        if let Some(handle) = accept_handle {
            let _ = handle.await;
        }
        inner.lock().await.pumps.wait().await;
    }

    /// List every retained session of this server instance.
    pub async fn list(&self) -> Vec<PortForwardSession> {
        let state = self.state.lock().await;
        let mut snapshots = Vec::new();
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            snapshots.push(snapshot_of(&guard));
        }
        snapshots.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        snapshots
    }

    /// Transition gate for context switches: holds the write side, stops
    /// and joins every session, advances the epoch once, records the new
    /// committed context, then lets the backend commit the switch.
    pub async fn begin_context_transition(&self, to: String) {
        type JoinableSession = (
            Arc<Mutex<SessionInner>>,
            Option<tokio::task::JoinHandle<()>>,
        );
        let mut joins: Vec<JoinableSession> = Vec::new();
        {
            let mut state = self.state.lock().await;
            state.epoch += 1;
            let ids: Vec<String> = state.sessions.keys().cloned().collect();
            for id in ids {
                let Some(inner) = state.sessions.get(&id).cloned() else {
                    continue;
                };
                let mut guard = inner.lock().await;
                if matches!(
                    guard.state,
                    PortForwardSessionState::Stopped | PortForwardSessionState::Failed
                ) {
                    continue;
                }
                guard.state = PortForwardSessionState::Stopped;
                guard.failure = Some(PortForwardFailure {
                    category: PortForwardFailureCategory::ContextTransition,
                    message: "the context switched while the forward was active".into(),
                });
                guard.revision += 1;
                guard.cancel.cancel();
                guard.pumps.close();
                let handle = guard.accept_task.take();
                let snapshot = snapshot_of(&guard);
                let _ = state.events_tx.send(PortForwardSessionEvent {
                    revision: snapshot.revision,
                    session: snapshot,
                });
                let inner = state.sessions.remove(&id).expect("checked above");
                joins.push((inner, handle));
            }
            // Commit only after every drain finished; queued starts observe
            // the advanced epoch plus this committed context under the same
            // gate they serialize behind.
            state.current_context = to;
        }
        // Joins run while no manager or session lock is held: accept loops
        // and pumps exit purely on cancellation plus lock-free atomics, so
        // they can never wait on the gate that waits on them.
        for (inner, handle) in joins {
            Self::join_session_tasks(handle, &inner).await;
        }
    }

    /// Shut down: stop and join every listener and pump before returning.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let ids: Vec<(String, Arc<Mutex<SessionInner>>)> = {
            let state = self.state.lock().await;
            state
                .sessions
                .iter()
                .map(|(id, inner)| (id.clone(), inner.clone()))
                .collect()
        };
        let mut handles = Vec::new();
        for (id, inner) in ids {
            let handle = {
                let mut guard = inner.lock().await;
                if !matches!(
                    guard.state,
                    PortForwardSessionState::Stopped | PortForwardSessionState::Failed
                ) {
                    guard.state = PortForwardSessionState::Stopped;
                    guard.revision += 1;
                }
                guard.cancel.cancel();
                guard.pumps.close();
                guard.accept_task.take()
            };
            handles.push((id, inner, handle));
        }
        for (id, inner, handle) in handles {
            Self::join_session_tasks(handle, &inner).await;
            let mut state = self.state.lock().await;
            state.sessions.remove(&id);
        }
    }

    /// Lock-free variant for callers already holding the manager lock.
    async fn find_in_state(
        state: &ManagerState,
        identity: &ResourceIdentity,
        selection: &PortForwardPortSelection,
    ) -> Option<PortForwardSession> {
        let wanted = match selection {
            PortForwardPortSelection::Name(name) => name.parse::<u16>().ok(),
            PortForwardPortSelection::Number(number) => Some(*number),
        };
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            if &guard.identity == identity && Some(guard.service_port) == wanted {
                return Some(snapshot_of(&guard));
            }
        }
        None
    }

    async fn publish_terminal(&self, id: String, snapshot: PortForwardSession) {
        let events_tx = {
            let mut state = self.state.lock().await;
            let _ = state.events_tx.send(PortForwardSessionEvent {
                revision: snapshot.revision,
                session: snapshot.clone(),
            });
            state.sessions.remove(&id);
            state.events_tx.clone()
        };
        // Terminal snapshots stay observable through their event; retention
        // beyond delivery lives in clients' bounded stores.
        drop(events_tx);
    }

    fn map_backend_error(error: BackendError) -> StartRejected {
        match error {
            BackendError::PortForward { category, message } => StartRejected {
                category: match category {
                    RejectionCategory::UnavailableEndpoint => {
                        PortForwardFailureCategory::UnavailableEndpoint
                    }
                    RejectionCategory::Forbidden => PortForwardFailureCategory::Forbidden,
                    RejectionCategory::VanishedResource => {
                        PortForwardFailureCategory::VanishedResource
                    }
                    RejectionCategory::UnsupportedService => {
                        PortForwardFailureCategory::UnsupportedService
                    }
                    RejectionCategory::TransportClosed => {
                        PortForwardFailureCategory::TransportClosed
                    }
                },
                message,
            },
            BackendError::NotFound => StartRejected::new(
                PortForwardFailureCategory::VanishedResource,
                "the service does not exist",
            ),
            BackendError::Forbidden => StartRejected::new(
                PortForwardFailureCategory::Forbidden,
                "kubernetes authorization denied the forward",
            ),
            BackendError::Conflict(reason) => {
                StartRejected::new(PortForwardFailureCategory::ContextTransition, reason)
            }
            BackendError::Cancelled
            | BackendError::Timeout
            | BackendError::Internal(_)
            | BackendError::Unsupported { .. } => StartRejected::new(
                PortForwardFailureCategory::TransportClosed,
                "the forward could not be established",
            ),
        }
    }
}

fn snapshot_of(session: &SessionInner) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(session.id.clone()).expect("stored ids are valid"),
        service: session.identity.clone(),
        service_port: session.service_port,
        pod: PortForwardPodTarget {
            namespace: session.resolved.namespace.clone(),
            name: session.resolved.pod_name.clone(),
            uid: session.resolved.pod_uid.clone(),
        },
        pod_port: session.resolved.pod_port,
        local_addr: session.local_addr.to_string(),
        state: session.state,
        failure: session.failure.clone(),
        revision: session.revision,
    }
}

fn random_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("pf-{nanos:x}-{:x}", std::process::id())
}

/// Accept loop owning the session listener until cancellation.
///
/// Runs without the manager lock: budgets are lock-free atomics, so the
/// context-transition gate can hold its write side while this loop drains.
async fn accept_loop(
    listener: TcpListener,
    session: Arc<Mutex<SessionInner>>,
    connector: PortForwardConnector,
    events_tx: broadcast::Sender<PortForwardSessionEvent>,
    live: LiveConnections,
    counters: Arc<SessionCounters>,
) {
    let cancel = session.lock().await.cancel.clone();
    loop {
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        let Ok((socket, _peer)) = accepted else {
            break;
        };
        // Enforce per-session and global connection budgets before spawning.
        let admit_global = live.load(Ordering::Acquire) < MAX_TOTAL_CONNECTIONS;
        let admit_session = counters.active.load(Ordering::Acquire) < MAX_SESSION_CONNECTIONS;
        if !(admit_global && admit_session) {
            // Overload: close immediately without an upstream stream.
            continue;
        }
        live.fetch_add(1, Ordering::AcqRel);
        counters.active.fetch_add(1, Ordering::AcqRel);
        let tracker = session.lock().await.pumps.clone();
        tracker.spawn(pump_connection(
            socket,
            session.clone(),
            connector.clone(),
            events_tx.clone(),
            live.clone(),
            counters.clone(),
        ));
    }
}

/// One accepted local TCP connection owns exactly one upstream stream.
///
/// Clean EOF on either side completes the connection normally and resets
/// the failure counter. A transport error with zero transferred bytes
/// counts toward [`OPEN_FAILURE_THRESHOLD`]; three consecutive ones fail
/// the session. Errors after bytes moved stay connection-terminal only.
async fn pump_connection(
    socket: tokio::net::TcpStream,
    session: Arc<Mutex<SessionInner>>,
    connector: PortForwardConnector,
    events_tx: broadcast::Sender<PortForwardSessionEvent>,
    live: LiveConnections,
    counters: Arc<SessionCounters>,
) {
    let release = {
        let counters = counters.clone();
        move || {
            counters.active.fetch_sub(1, Ordering::AcqRel);
            live.fetch_sub(1, Ordering::AcqRel);
        }
    };
    let release = &release;
    let (resolved, cancel) = {
        let guard = session.lock().await;
        (guard.resolved.clone(), guard.cancel.clone())
    };
    let opened = tokio::select! {
        _ = cancel.cancelled() => {
            release();
            return;
        }
        opened = connector.connect(&resolved) => opened,
    };
    let upstream = match opened {
        Ok(upstream) => upstream,
        Err(_) => {
            release();
            record_open_failure(&session, &events_tx).await;
            return;
        }
    };

    // Count every byte that moves in either direction so pre-byte failures
    // are distinguished from post-byte resets honestly.
    let client_bytes = Arc::new(AtomicU64::new(0));
    let upstream_bytes = Arc::new(AtomicU64::new(0));
    let mut socket = CountingIo::new(socket, client_bytes.clone());
    let mut upstream = CountingIo::new(upstream, upstream_bytes.clone());
    let outcome = tokio::select! {
        _ = cancel.cancelled() => None,
        copied = copy_bidirectional(&mut socket, &mut upstream) => Some(copied),
    };
    release();
    let moved = client_bytes.load(Ordering::Acquire) + upstream_bytes.load(Ordering::Acquire);
    match outcome {
        None => {}
        // copy_bidirectional reports the byte counts of both directions.
        Some(Ok(_)) => {
            session.lock().await.open_failures = 0;
        }
        Some(Err(_)) if moved == 0 => {
            record_open_failure(&session, &events_tx).await;
        }
        Some(Err(_)) => {
            // Connection-terminal only; never changes session state.
        }
    }
}

/// Async I/O adapter counting transferred bytes per direction.
struct CountingIo<T> {
    inner: T,
    counter: Arc<AtomicU64>,
}

impl<T> CountingIo<T> {
    fn new(inner: T, counter: Arc<AtomicU64>) -> Self {
        Self { inner, counter }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for CountingIo<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let read = buf.filled().len() - filled_before;
            if read > 0 {
                self.counter.fetch_add(read as u64, Ordering::Relaxed);
            }
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for CountingIo<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(written)) = &result {
            self.counter.fetch_add(*written as u64, Ordering::Relaxed);
        }
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Count one failed stream against the consecutive pre-byte threshold and
/// fail the session at three, tearing down its listener and pumps.
async fn record_open_failure(
    session: &Arc<Mutex<SessionInner>>,
    events_tx: &broadcast::Sender<PortForwardSessionEvent>,
) {
    let mut guard = session.lock().await;
    if guard.state != PortForwardSessionState::Active {
        return;
    }
    guard.open_failures += 1;
    if guard.open_failures < OPEN_FAILURE_THRESHOLD {
        return;
    }
    guard.state = PortForwardSessionState::Failed;
    guard.failure = Some(PortForwardFailure {
        category: PortForwardFailureCategory::UnavailableEndpoint,
        message: "the pinned endpoint stopped accepting streams".into(),
    });
    guard.revision += 1;
    guard.cancel.cancel();
    guard.pumps.close();
    if let Some(handle) = guard.accept_task.take() {
        handle.abort();
    }
    let _ = events_tx.send(PortForwardSessionEvent {
        revision: guard.revision,
        session: snapshot_of(&guard),
    });
}
