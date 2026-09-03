//! Bounded, loopback-only port-forward session management.
//!
//! The manager is owned by the server lifecycle: it binds local listeners
//! only on `127.0.0.1`, enforces hard resource limits, isolates per-
//! connection data paths, and joins every task on shutdown. Session
//! snapshots are authoritative and published under one lock stamped with a
//! monotonic revision and the manager's context-transition epoch.

use std::collections::HashMap;
use std::future::Future;
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
    BackendError, PortForwardConnector, PortForwardRequest, RejectionCategory, ResolvedPortForward,
};
use k10s_protocol::{
    PortForwardFailure, PortForwardFailureCategory, PortForwardPodTarget, PortForwardSession,
    PortForwardSessionEvent, PortForwardSessionId, PortForwardSessionState, PortForwardTarget,
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
    target: PortForwardTarget,
    target_key: TargetKey,
    requested_local_port: u16,
    local_addr: std::net::SocketAddr,
    resolved: ResolvedPortForward,
    state: PortForwardSessionState,
    failure: Option<PortForwardFailure>,
    revision: u64,
    /// Consecutive stream failures before any byte moved.
    open_failures: u32,
    /// When the session reached a terminal state; drives bounded retention.
    terminal_at: Option<std::time::Instant>,
    /// Per-connection pump tasks; joined on teardown.
    pumps: TaskTracker,
    cancel: CancellationToken,
    accept_task: Option<tokio::task::JoinHandle<()>>,
}

/// Stable active-session equivalence. The local port is intentionally absent:
/// repeated starts focus an existing target regardless of bind preference.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetKey {
    Service {
        uid: String,
        source_port: u16,
    },
    Pod {
        uid: String,
        container_name: String,
        remote_port: u16,
    },
}

/// Global live-connection budget shared without the manager lock so data
/// paths never serialize behind session publication.
type LiveConnections = Arc<AtomicUsize>;

/// Per-session accepted-connection budget, lock-free like the global one.
#[derive(Debug, Default)]
struct SessionCounters {
    active: AtomicUsize,
}

/// Serializes revision allocation with event publication. This makes wire
/// order identical to revision order across Start, Stop, and async failures.
#[derive(Debug)]
struct PublicationClock {
    next: AtomicU64,
    gate: Mutex<()>,
}

type SharedPublicationClock = Arc<PublicationClock>;

/// Shared mutable manager state guarded by one lock so epochs and revisions
/// are linearizable with respect to publication and the transition gate.
struct ManagerState {
    sessions: HashMap<String, Arc<Mutex<SessionInner>>>,
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
    publication: SharedPublicationClock,
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
                epoch: 0,
                current_context: String::new(),
                events_tx,
            })),
            connector,
            cancel,
            publication: Arc::new(PublicationClock {
                next: AtomicU64::new(1),
                gate: Mutex::new(()),
            }),
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
    /// Active. A duplicate stable Service or Pod target focuses the existing
    /// session instead of creating a second listener.
    ///
    /// Publication validates the request against the committed context and
    /// the gate epoch: starts carrying a retired context or racing a switch
    /// abort without binding anything and return the typed retryable
    /// context-transition error.
    pub async fn start(
        &self,
        target: PortForwardTarget,
        requested_local_port: u16,
        requested_context: String,
    ) -> Result<PortForwardSession, StartRejected> {
        if let Err(message) = target.validate() {
            let category = match target {
                PortForwardTarget::Service { .. } => PortForwardFailureCategory::UnsupportedService,
                PortForwardTarget::Pod { .. } => PortForwardFailureCategory::UnsupportedPod,
            };
            return Err(StartRejected::new(category, message));
        }
        let observed_epoch = {
            let state = self.state.lock().await;
            if let Some(existing) = Self::find_exact_requested_target(&state, &target).await {
                return Ok(existing);
            }
            state.epoch
        };
        if self.cancel.is_cancelled() {
            return Err(StartRejected::new(
                PortForwardFailureCategory::TransportClosed,
                "the embedded server is shutting down",
            ));
        }

        // Resolve before binding; failures never touch local resources.
        let request = PortForwardRequest {
            context: requested_context.clone(),
            target: target.clone(),
        };
        let resolved = match self.connector.resolve(request).await {
            Ok(resolved) => resolved,
            Err(error) => return Err(Self::map_backend_error(error)),
        };
        let target_key = TargetKey::from_resolved(&target, &resolved);

        // Bind only 127.0.0.1 before reporting success; port 0 asks the OS.
        let bind_addr = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, requested_local_port));
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(listener) => listener,
            Err(_) => {
                return Err(StartRejected::new(
                    PortForwardFailureCategory::LocalPortInUse,
                    format!("local port {requested_local_port} is already in use"),
                ));
            }
        };

        let mut state = self.state.lock().await;
        Self::prune_expired(&mut state).await;
        // Context-switch atomicity: validate the request identity against
        // the authoritative committed context under the same lock used by
        // the gate. Either dimension mismatching aborts without binding.
        // An empty committed context means no switch ever happened yet;
        // the first accepted start commits its own context.
        if state.epoch != observed_epoch
            || (!state.current_context.is_empty() && state.current_context != requested_context)
        {
            drop(state);
            return Err(StartRejected::new(
                PortForwardFailureCategory::ContextTransition,
                "the context switched; retry after it completes",
            ));
        }
        let mut active_sessions = 0;
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            if !matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) {
                active_sessions += 1;
            }
        }
        if active_sessions >= MAX_SESSIONS {
            drop(state);
            return Err(StartRejected::new(
                PortForwardFailureCategory::UnavailableEndpoint,
                "the maximum number of port-forward sessions is active",
            ));
        }
        if let Some(existing) = Self::find_key_in_state(&state, &target_key).await {
            return Ok(existing);
        }

        let id = random_id();
        let local_addr = listener.local_addr().map_err(|_| {
            StartRejected::new(
                PortForwardFailureCategory::TransportClosed,
                "the bound address could not be observed",
            )
        })?;
        let session_cancel = self.cancel.child_token();
        let inner = Arc::new(Mutex::new(SessionInner {
            id: id.clone(),
            target,
            target_key,
            requested_local_port,
            local_addr,
            resolved,
            state: PortForwardSessionState::Active,
            failure: None,
            revision: 0,
            open_failures: 0,
            terminal_at: None,
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
            self.publication.clone(),
        ));
        let mut guard = inner.lock().await;
        guard.accept_task = Some(accept_task);
        drop(guard);
        state.sessions.insert(id.clone(), inner.clone());
        if state.current_context.is_empty() {
            state.current_context = requested_context;
        }
        // Publish the authoritative Active snapshot so every subscribed
        // panel converges without polling.
        {
            let mut guard = inner.lock().await;
            let _publication = self.publication.gate.lock().await;
            guard.revision = self.publication.next.fetch_add(1, Ordering::AcqRel);
            let _ = state.events_tx.send(PortForwardSessionEvent {
                revision: guard.revision,
                session: snapshot_of(&guard),
            });
        }
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
            guard.terminal_at = Some(std::time::Instant::now());
            // Cancelling stops new accepts and tears down live pumps; their
            // tasks are joined below WITHOUT any lock held so a pump that is
            // mid-copy can always finish its release bookkeeping.
            guard.cancel.cancel();
            guard.pumps.close();
            guard.accept_task.take()
        };
        Self::join_session_tasks(accept_handle, &inner).await;
        let snapshot = {
            let state = self.state.lock().await;
            let mut guard = inner.lock().await;
            let _publication = self.publication.gate.lock().await;
            guard.revision = self.publication.next.fetch_add(1, Ordering::AcqRel);
            let snapshot = snapshot_of(&guard);
            let _ = state.events_tx.send(PortForwardSessionEvent {
                revision: snapshot.revision,
                session: snapshot.clone(),
            });
            snapshot
        };
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
        // Clone the tracker out under a short lock: waiting while holding
        // the session mutex would deadlock against a pump that is finishing
        // its release bookkeeping.
        let tracker = inner.lock().await.pumps.clone();
        tracker.wait().await;
    }

    /// How long terminal snapshots stay observable before removal.
    pub const TERMINAL_RETENTION: std::time::Duration = std::time::Duration::from_secs(30);

    /// List every retained session of this server instance.
    ///
    /// Terminal snapshots are retained for a bounded interval for
    /// diagnostics, then pruned here under the manager lock.
    pub async fn list(&self) -> Vec<PortForwardSession> {
        self.list_snapshot().await.1
    }

    /// Return the resolved declared source port retained for compatibility
    /// encoding of a session snapshot.
    pub async fn resolved_source_port(&self, session_id: &str) -> Option<u16> {
        let inner = {
            let state = self.state.lock().await;
            state.sessions.get(session_id).cloned()
        }?;
        Some(inner.lock().await.resolved.source_port)
    }

    /// Capture retained sessions together with a manager-global watermark.
    /// The baseline is read before the snapshot and then raised to every
    /// included row revision, so a concurrently published event always
    /// makes either the response include it or the response detectably stale.
    pub async fn list_snapshot(&self) -> (u64, Vec<PortForwardSession>) {
        let mut revision = self
            .publication
            .next
            .load(Ordering::Acquire)
            .saturating_sub(1);
        let mut state = self.state.lock().await;
        Self::prune_expired(&mut state).await;
        let mut snapshots = Vec::new();
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            revision = revision.max(guard.revision);
            snapshots.push(snapshot_of(&guard));
        }
        snapshots.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        (revision, snapshots)
    }

    /// Transition gate for context switches: holds the write side, stops
    /// and joins every session, advances the epoch once, records the new
    /// committed context, then lets the backend commit the switch.
    pub async fn transition_context<T, E, F>(&self, to: String, commit: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        type JoinableSession = (
            Arc<Mutex<SessionInner>>,
            Option<tokio::task::JoinHandle<()>>,
        );
        let mut joins: Vec<JoinableSession> = Vec::new();
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
            guard.terminal_at = Some(std::time::Instant::now());
            let _publication = self.publication.gate.lock().await;
            guard.revision = self.publication.next.fetch_add(1, Ordering::AcqRel);
            guard.cancel.cancel();
            guard.pumps.close();
            let handle = guard.accept_task.take();
            let snapshot = snapshot_of(&guard);
            let _ = state.events_tx.send(PortForwardSessionEvent {
                revision: snapshot.revision,
                session: snapshot,
            });
            drop(guard);
            joins.push((inner, handle));
        }
        // Keep the manager gate held across drains and the backend commit:
        // no Start can publish in the gap between the two operations.
        for (inner, handle) in joins {
            Self::join_session_tasks(handle, &inner).await;
        }
        let result = commit.await;
        if result.is_ok() {
            state.current_context = to;
        }
        result
    }

    /// Test/helper transition whose commit cannot fail.
    pub async fn begin_context_transition(&self, to: String) {
        let _: Result<(), std::convert::Infallible> =
            self.transition_context(to, async { Ok(()) }).await;
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
                    guard.terminal_at = Some(std::time::Instant::now());
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

    async fn find_exact_requested_target(
        state: &ManagerState,
        target: &PortForwardTarget,
    ) -> Option<PortForwardSession> {
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            if matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) {
                // Terminal sessions never satisfy a new start; Retry must
                // create a fresh session instead of focusing a dead one.
                continue;
            }
            if targets_have_same_requested_identity(&guard.target, target) {
                return Some(snapshot_of(&guard));
            }
        }
        None
    }

    async fn find_key_in_state(
        state: &ManagerState,
        target_key: &TargetKey,
    ) -> Option<PortForwardSession> {
        for inner in state.sessions.values() {
            let guard = inner.lock().await;
            if !matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) && &guard.target_key == target_key
            {
                return Some(snapshot_of(&guard));
            }
        }
        None
    }

    async fn prune_expired(state: &mut ManagerState) {
        let mut expired = Vec::new();
        for (id, inner) in &state.sessions {
            let guard = inner.lock().await;
            if matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) && guard
                .terminal_at
                .is_some_and(|at| at.elapsed() > Self::TERMINAL_RETENTION)
            {
                expired.push(id.clone());
            }
        }
        for id in expired {
            state.sessions.remove(&id);
        }
        // Retention is time-bounded and count-bounded. A rapid sequence of
        // Fail -> Retry operations must not grow the map for the entire TTL.
        let mut terminal = Vec::new();
        for (id, inner) in &state.sessions {
            let guard = inner.lock().await;
            if matches!(
                guard.state,
                PortForwardSessionState::Stopped | PortForwardSessionState::Failed
            ) {
                terminal.push((guard.terminal_at, id.clone()));
            }
        }
        terminal.sort_by_key(|(at, _)| *at);
        let excess = terminal.len().saturating_sub(MAX_SESSIONS);
        for (_, id) in terminal.into_iter().take(excess) {
            state.sessions.remove(&id);
        }
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
                    RejectionCategory::UnsupportedPod => PortForwardFailureCategory::UnsupportedPod,
                    RejectionCategory::TransportClosed => {
                        PortForwardFailureCategory::TransportClosed
                    }
                    RejectionCategory::LocalPortInUse => PortForwardFailureCategory::LocalPortInUse,
                    RejectionCategory::ContextTransition => {
                        PortForwardFailureCategory::ContextTransition
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
            BackendError::ContextUnavailable { context, reason } => StartRejected::new(
                PortForwardFailureCategory::ContextTransition,
                format!("context '{context}' is unavailable: {reason}"),
            ),
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
        target: session.target.clone(),
        requested_local_port: session.requested_local_port,
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

impl TargetKey {
    fn from_resolved(target: &PortForwardTarget, resolved: &ResolvedPortForward) -> Self {
        match target {
            PortForwardTarget::Service { identity, .. } => Self::Service {
                uid: identity.uid.clone(),
                source_port: resolved.source_port,
            },
            PortForwardTarget::Pod {
                identity,
                container_name,
                remote_port,
            } => Self::Pod {
                uid: identity.uid.clone(),
                container_name: container_name.clone(),
                remote_port: *remote_port,
            },
        }
    }
}

fn targets_have_same_requested_identity(
    left: &PortForwardTarget,
    right: &PortForwardTarget,
) -> bool {
    match (left, right) {
        (
            PortForwardTarget::Service {
                identity: left_identity,
                port: left_port,
            },
            PortForwardTarget::Service {
                identity: right_identity,
                port: right_port,
            },
        ) => left_identity.uid == right_identity.uid && left_port == right_port,
        (
            PortForwardTarget::Pod {
                identity: left_identity,
                container_name: left_container,
                remote_port: left_port,
            },
            PortForwardTarget::Pod {
                identity: right_identity,
                container_name: right_container,
                remote_port: right_port,
            },
        ) => {
            left_identity.uid == right_identity.uid
                && left_container == right_container
                && left_port == right_port
        }
        _ => false,
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
    publication: SharedPublicationClock,
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
        // Reserve first, then bound-check: a plain load-then-add lets two
        // concurrent accept loops exceed the hard total.
        if live.fetch_add(1, Ordering::AcqRel) >= MAX_TOTAL_CONNECTIONS {
            live.fetch_sub(1, Ordering::AcqRel);
            continue;
        }
        if counters.active.fetch_add(1, Ordering::AcqRel) >= MAX_SESSION_CONNECTIONS {
            counters.active.fetch_sub(1, Ordering::AcqRel);
            live.fetch_sub(1, Ordering::AcqRel);
            continue;
        }
        let tracker = session.lock().await.pumps.clone();
        tracker.spawn(pump_connection(
            socket,
            session.clone(),
            connector.clone(),
            events_tx.clone(),
            live.clone(),
            counters.clone(),
            publication.clone(),
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
    publication: SharedPublicationClock,
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
            record_open_failure(&session, &events_tx, publication).await;
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
            record_open_failure(&session, &events_tx, publication).await;
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
    publication: SharedPublicationClock,
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
    guard.terminal_at = Some(std::time::Instant::now());
    guard.cancel.cancel();
    guard.pumps.close();
    if let Some(handle) = guard.accept_task.take() {
        handle.abort();
    }
    let _publication = publication.gate.lock().await;
    guard.revision = publication.next.fetch_add(1, Ordering::AcqRel);
    let _ = events_tx.send(PortForwardSessionEvent {
        revision: guard.revision,
        session: snapshot_of(&guard),
    });
}
