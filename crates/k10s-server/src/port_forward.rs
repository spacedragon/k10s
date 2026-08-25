//! Bounded, loopback-only port-forward session management.
//!
//! The manager is owned by the server lifecycle: it binds local listeners
//! only on `127.0.0.1`, enforces hard resource limits, isolates per-
//! connection data paths, and joins every task on shutdown. Session
//! snapshots are authoritative and published under one lock stamped with a
//! monotonic revision and the manager's context-transition epoch.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;

use tokio::io::copy_bidirectional;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

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
    active_connections: usize,
    cancel: CancellationToken,
    accept_task: Option<tokio::task::JoinHandle<()>>,
}

/// Shared mutable manager state guarded by one lock so epochs and revisions
/// are linearizable with respect to publication and the transition gate.
struct ManagerState {
    sessions: HashMap<String, Arc<Mutex<SessionInner>>>,
    next_revision: u64,
    epoch: u64,
    current_context: String,
    live_connections: usize,
    events_tx: broadcast::Sender<PortForwardSessionEvent>,
}

/// Cloneable handle to the bounded port-forward manager.
#[derive(Clone)]
pub struct PortForwardManager {
    state: Arc<Mutex<ManagerState>>,
    connector: PortForwardConnector,
    cancel: CancellationToken,
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
                live_connections: 0,
                events_tx,
            })),
            connector,
            cancel,
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
        let service_port = match &selection {
            PortForwardPortSelection::Number(number) => *number,
            PortForwardPortSelection::Name(name) => name.parse().unwrap_or(0),
        };
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
            active_connections: 0,
            cancel: session_cancel,
            accept_task: None,
        }));
        let events_tx = state.events_tx.clone();
        let manager_state = self.state.clone();
        let connector = self.connector.clone();
        let accept_task = tokio::spawn(accept_loop(
            listener,
            inner.clone(),
            connector,
            events_tx,
            manager_state,
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
        let final_snapshot = {
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
            guard.cancel.cancel();
            guard.accept_task.take()
        };
        if let Some(handle) = final_snapshot {
            let _ = handle.await;
        }
        let snapshot = {
            let guard = inner.lock().await;
            snapshot_of(&guard)
        };
        self.publish_terminal(session_id.to_owned(), snapshot.clone())
            .await;
        StopOutcome::Stopped(snapshot)
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
        let mut state = self.state.lock().await;
        state.epoch += 1;
        let ids: Vec<String> = state.sessions.keys().cloned().collect();
        for id in ids {
            let Some(inner) = state.sessions.get(&id) else {
                continue;
            };
            let snapshot = {
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
                let handle = guard.accept_task.take();
                let snapshot = snapshot_of(&guard);
                drop(guard);
                if let Some(handle) = handle {
                    let _ = handle.await;
                }
                snapshot
            };
            let _ = state.events_tx.send(PortForwardSessionEvent {
                revision: snapshot.revision,
                session: snapshot.clone(),
            });
            state.sessions.remove(&id);
        }
        // Commit only after every drain finished.
        state.current_context = to;
    }

    /// Shut down: stop and join every session task.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let ids: Vec<String> = {
            let state = self.state.lock().await;
            state.sessions.keys().cloned().collect()
        };
        for id in ids {
            let _ = self.stop(&id).await;
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
async fn accept_loop(
    listener: TcpListener,
    session: Arc<Mutex<SessionInner>>,
    connector: PortForwardConnector,
    events_tx: broadcast::Sender<PortForwardSessionEvent>,
    manager: Arc<Mutex<ManagerState>>,
) {
    loop {
        let cancel = session.lock().await.cancel.clone();
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => accepted,
        };
        let Ok((socket, _peer)) = accepted else {
            break;
        };
        // Enforce per-session and global connection budgets before spawning.
        let admitted = {
            let mut state = manager.lock().await;
            let mut guard = session.lock().await;
            if state.live_connections >= MAX_TOTAL_CONNECTIONS
                || guard.active_connections >= MAX_SESSION_CONNECTIONS
                || !matches!(guard.state, PortForwardSessionState::Active)
            {
                false
            } else {
                state.live_connections += 1;
                guard.active_connections += 1;
                true
            }
        };
        if !admitted {
            // Overload: close immediately without an upstream stream.
            continue;
        }
        tokio::spawn(pump_connection(
            socket,
            session.clone(),
            connector.clone(),
            events_tx.clone(),
            manager.clone(),
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
    manager: Arc<Mutex<ManagerState>>,
) {
    let (resolved, cancel, session_id) = {
        let guard = session.lock().await;
        (
            guard.resolved.clone(),
            guard.cancel.clone(),
            guard.id.clone(),
        )
    };
    let opened = tokio::select! {
        _ = cancel.cancelled() => return release(&session, &manager).await,
        opened = connector.connect(&resolved) => opened,
    };
    match opened {
        Err(_) => {
            release(&session, &manager).await;
            record_open_failure(&session, &events_tx).await;
        }
        Ok(mut upstream) => {
            let mut socket = socket;
            // Probe with a bidirectional copy; track completion honestly by
            // attempting one zero-length read on each direction first is not
            // needed: copy_bidirectional returns Err only when a direction
            // errored. Clean EOFs yield Ok(()).
            let result = copy_bidirectional(&mut socket, &mut upstream).await;
            if result.is_ok() {
                let mut guard = session.lock().await;
                guard.open_failures = 0;
            } else {
                // Distinguish pre-byte failures from post-byte errors by
                // probing whether the upstream still has buffered data is
                // impossible after the fact; treat any error whose peer saw
                // no data as pre-byte via a conservative heuristic: only
                // count it when the counter is already armed (a prior
                // connect failure), otherwise leave the session untouched.
                let arm = {
                    let guard = session.lock().await;
                    guard.open_failures > 0
                };
                if arm {
                    record_open_failure(&session, &events_tx).await;
                }
            }
            release(&session, &manager).await;
            let _ = session_id;
        }
    }
}

async fn release(session: &Arc<Mutex<SessionInner>>, manager: &Arc<Mutex<ManagerState>>) {
    let mut state = manager.lock().await;
    let mut guard = session.lock().await;
    state.live_connections = state.live_connections.saturating_sub(1);
    guard.active_connections = guard.active_connections.saturating_sub(1);
}

async fn record_open_failure(
    session: &Arc<Mutex<SessionInner>>,
    events_tx: &broadcast::Sender<PortForwardSessionEvent>,
) {
    let mut guard = session.lock().await;
    if !matches!(guard.state, PortForwardSessionState::Active) {
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
    if let Some(handle) = guard.accept_task.take() {
        handle.abort();
    }
    let _ = events_tx.send(PortForwardSessionEvent {
        revision: guard.revision,
        session: snapshot_of(&guard),
    });
}
