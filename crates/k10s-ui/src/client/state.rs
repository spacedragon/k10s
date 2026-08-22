use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use k10s_protocol::{
    Ack, BootstrapResponse, CancelRequest, ClientFrame, ClientKind, ErrorCode, ErrorFrame, Hello,
    PROTOCOL_MAJOR, PROTOCOL_MINOR, Request, RequestId, ResumeStatus, Retryability, ServerFrame,
    ServerKind, ServerPayload, SessionId, Subscribe, SubscriptionId,
};

/// Client connection lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientPhase {
    /// No active connection.
    Disconnected,
    /// The transport is open and the `Hello` is awaiting a `Welcome`.
    Authenticating,
    /// Authentication and protocol negotiation completed.
    Ready,
    /// Authentication failed; show the web connection gate.
    WebGate,
    /// The server speaks a different protocol major; the client must be upgraded.
    UpgradeRequired,
    /// Explicitly closed by the user or application lifecycle.
    Closed,
}

/// Client behavior configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Capabilities offered during negotiation.
    pub capabilities: Vec<String>,
    /// Initial retry ceiling.
    pub retry_base_ms: u64,
    /// Maximum retry ceiling.
    pub retry_cap_ms: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            capabilities: vec!["bootstrap-status".to_owned()],
            retry_base_ms: 250,
            retry_cap_ms: 30_000,
        }
    }
}

/// WebSocket endpoint and first-frame credential.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectTarget {
    url: String,
    access_token: String,
}

impl std::fmt::Debug for ConnectTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectTarget")
            .field("url", &self.url)
            .field("access_token", &"[REDACTED]")
            .finish()
    }
}

impl ConnectTarget {
    /// Create a connection target. The token is kept separate from the URL.
    #[must_use]
    pub fn new(url: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            access_token: access_token.into(),
        }
    }

    /// Credential-free WebSocket endpoint.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// A safe client-state error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// A server response did not correlate to a live request.
    UnknownResponse(RequestId),
    /// The operation is invalid in the current lifecycle phase.
    InvalidState(&'static str),
    /// A frame could not be encoded or decoded.
    Protocol(String),
    /// A sequenced event skipped one or more unacknowledged messages.
    SequenceGap {
        /// Next sequence expected by the client.
        expected: u64,
        /// Sequence received from the server.
        got: u64,
    },
    /// Authentication credentials were rejected.
    AuthenticationRejected,
    /// The server uses an incompatible protocol major.
    IncompatibleProtocol {
        /// Major supported by this client.
        client_major: u16,
        /// Major announced by the server.
        server_major: u16,
    },
    /// A structured server-side failure.
    Server(ErrorFrame),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownResponse(id) => write!(formatter, "unknown response ID {}", id.as_str()),
            Self::InvalidState(message) => formatter.write_str(message),
            Self::Protocol(message) => formatter.write_str(message),
            Self::SequenceGap { expected, got } => {
                write!(formatter, "sequence gap: expected {expected}, got {got}")
            }
            Self::AuthenticationRejected => formatter.write_str("authentication rejected"),
            Self::IncompatibleProtocol {
                client_major,
                server_major,
            } => write!(
                formatter,
                "incompatible protocol major: client {client_major}, server {server_major}"
            ),
            Self::Server(error) => formatter.write_str(&error.safe_message),
        }
    }
}

impl std::error::Error for ClientError {}

/// Request behaviors supported by the foundation client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Query {
    /// Retrieve server identity and safe Kubernetes contexts.
    Bootstrap,
}

/// A completed query value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    /// Bootstrap query result.
    Bootstrap(BootstrapResponse),
}

/// Opaque handle used to retrieve or cancel one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRequest {
    id: RequestId,
}

/// Handle for a desired bootstrap-status subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSubscription {
    id: SubscriptionId,
}

impl LiveSubscription {
    /// Client-selected subscription ID.
    #[must_use]
    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }
}

/// UI-owned state that survives transport recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalUiState {
    /// Currently selected Kubernetes context.
    pub selected_context: Option<String>,
}

/// A scheduled full-jitter reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySchedule {
    /// Zero-based consecutive retry number.
    pub attempt: u32,
    /// Exponential ceiling used for this draw.
    pub max_delay_ms: u64,
    /// Absolute time at which the retry becomes eligible.
    pub retry_at_ms: u64,
}

impl PendingRequest {
    /// Protocol request identifier.
    #[must_use]
    pub fn id(&self) -> &RequestId {
        &self.id
    }
}

#[derive(Debug)]
struct PendingEntry {
    query: Query,
    deadline_at_ms: Option<u64>,
}

/// Pure client protocol state.
#[derive(Debug)]
pub struct ClientState {
    config: ClientConfig,
    phase: ClientPhase,
    outbound: VecDeque<ClientFrame>,
    next_request_id: u128,
    pending: BTreeMap<RequestId, PendingEntry>,
    completed: BTreeMap<RequestId, QueryResult>,
    target: Option<ConnectTarget>,
    retry_attempt: u32,
    retry: Option<RetrySchedule>,
    reconnecting: bool,
    last_acked_sequence: Option<u64>,
    next_subscription_id: u128,
    live_subscriptions: HashMap<SubscriptionId, serde_json::Value>,
    active_subscriptions: HashSet<SubscriptionId>,
    server_bootstrap: Option<BootstrapResponse>,
    server_state_invalid: bool,
    local_ui: LocalUiState,
    session_id: Option<SessionId>,
    server_instance_id: Option<String>,
}

impl ClientState {
    /// Construct a disconnected client.
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            phase: ClientPhase::Disconnected,
            outbound: VecDeque::new(),
            next_request_id: 1,
            pending: BTreeMap::new(),
            completed: BTreeMap::new(),
            target: None,
            retry_attempt: 0,
            retry: None,
            reconnecting: false,
            last_acked_sequence: None,
            next_subscription_id: 1,
            live_subscriptions: HashMap::new(),
            active_subscriptions: HashSet::new(),
            server_bootstrap: None,
            server_state_invalid: true,
            local_ui: LocalUiState::default(),
            session_id: None,
            server_instance_id: None,
        }
    }

    /// Current connection phase.
    #[must_use]
    pub fn phase(&self) -> ClientPhase {
        self.phase
    }

    /// Start a fresh connection and queue the credential-bearing `Hello` frame.
    pub fn connect(&mut self, target: ConnectTarget) -> Result<(), ClientError> {
        self.target = Some(target.clone());
        self.retry_attempt = 0;
        self.retry = None;
        self.reconnecting = false;
        self.session_id = None;
        self.server_instance_id = None;
        self.last_acked_sequence = None;
        self.outbound.clear();
        self.live_subscriptions.clear();
        self.invalidate_server_state();
        self.queue_hello(target)?;
        self.phase = ClientPhase::Authenticating;
        Ok(())
    }

    fn queue_hello(&mut self, target: ConnectTarget) -> Result<(), ClientError> {
        let hello = Hello {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            capabilities: self.config.capabilities.clone(),
            access_token: target.access_token,
            server_instance_id: self.server_instance_id.clone(),
            session_id: self.session_id.clone(),
            last_acked_sequence: self.last_acked_sequence,
            stream_ticket: None,
        };
        self.outbound.push_back(ClientFrame {
            kind: ClientKind::Hello,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(hello).map_err(|error| {
                ClientError::Protocol(format!("could not encode hello: {error}"))
            })?,
        });
        let _endpoint_without_credentials = target.url;
        Ok(())
    }

    /// Remove the next frame waiting for transport delivery.
    pub fn take_outbound(&mut self) -> Option<ClientFrame> {
        self.outbound.pop_front()
    }

    /// Start and remember the Plan 1 bootstrap-status subscription.
    pub fn subscribe_bootstrap_status(&mut self) -> Result<LiveSubscription, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        let id = SubscriptionId::new(format!("bootstrap-status-{}", self.next_subscription_id));
        self.next_subscription_id = self.next_subscription_id.saturating_add(1);
        let selector = serde_json::json!({"kind":"bootstrapStatus"});
        self.live_subscriptions.insert(id.clone(), selector.clone());
        self.refresh_server_validity();
        self.queue_subscribe(id.clone(), selector)?;
        Ok(LiveSubscription { id })
    }

    fn queue_subscribe(
        &mut self,
        id: SubscriptionId,
        selector: serde_json::Value,
    ) -> Result<(), ClientError> {
        self.outbound.push_back(ClientFrame {
            kind: ClientKind::Subscribe,
            request_id: None,
            subscription_id: Some(id),
            sequence: None,
            payload: serde_json::to_value(Subscribe(selector)).map_err(|error| {
                ClientError::Protocol(format!("could not encode subscription: {error}"))
            })?,
        });
        Ok(())
    }

    /// UI state retained across reconnects and full resynchronization.
    #[must_use]
    pub fn local_ui(&self) -> &LocalUiState {
        &self.local_ui
    }

    /// Mutably access UI-owned state.
    pub fn local_ui_mut(&mut self) -> &mut LocalUiState {
        &mut self.local_ui
    }

    /// Last contiguous server sequence acknowledged by the client.
    #[must_use]
    pub fn last_acked_sequence(&self) -> Option<u64> {
        self.last_acked_sequence
    }

    /// Most recently received bootstrap state, if still valid.
    #[must_use]
    pub fn server_bootstrap(&self) -> Option<&BootstrapResponse> {
        self.server_bootstrap.as_ref()
    }

    /// Whether server-issued state needs rebuilding.
    #[must_use]
    pub fn server_state_invalid(&self) -> bool {
        self.server_state_invalid
    }

    /// Schedule a retry after transient transport loss using a supplied entropy draw.
    pub fn transport_lost(&mut self, now_ms: u64, entropy: u64) {
        if matches!(
            self.phase,
            ClientPhase::WebGate | ClientPhase::UpgradeRequired | ClientPhase::Closed
        ) {
            return;
        }
        self.phase = ClientPhase::Disconnected;
        self.reconnecting = true;
        self.invalidate_server_state();
        let exponent = self.retry_attempt.min(63);
        let ceiling = self
            .config
            .retry_base_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.config.retry_cap_ms);
        let delay = if ceiling == u64::MAX {
            entropy
        } else {
            entropy % (ceiling + 1)
        };
        self.retry = Some(RetrySchedule {
            attempt: self.retry_attempt,
            max_delay_ms: ceiling,
            retry_at_ms: now_ms.saturating_add(delay),
        });
        self.retry_attempt = self.retry_attempt.saturating_add(1);
    }

    /// Current reconnect timer.
    #[must_use]
    pub fn retry_schedule(&self) -> Option<RetrySchedule> {
        self.retry
    }

    /// Start a scheduled reconnect when its deadline has arrived.
    pub fn retry_if_due(&mut self, now_ms: u64) -> bool {
        if self.phase != ClientPhase::Disconnected {
            return false;
        }
        let Some(schedule) = self.retry else {
            return false;
        };
        if now_ms < schedule.retry_at_ms {
            return false;
        }
        let Some(target) = self.target.clone() else {
            return false;
        };
        self.retry = None;
        if self.queue_hello(target).is_err() {
            return false;
        }
        self.phase = ClientPhase::Authenticating;
        true
    }

    /// Explicit user-requested close. No reconnect occurs until [`Self::connect`].
    pub fn user_close(&mut self) {
        self.explicit_close();
    }

    /// Explicit application-lifecycle close. No reconnect occurs until [`Self::connect`].
    pub fn application_close(&mut self) {
        self.explicit_close();
    }

    fn explicit_close(&mut self) {
        self.phase = ClientPhase::Closed;
        self.retry = None;
        self.reconnecting = false;
        self.pending.clear();
        self.completed.clear();
        self.outbound.clear();
        self.target = None;
    }

    /// Begin a query without a client-side deadline.
    pub fn begin(&mut self, query: Query) -> Result<PendingRequest, ClientError> {
        self.begin_inner(query, None, None)
    }

    /// Begin a query with a relative deadline measured against `now_ms`.
    pub fn begin_with_deadline(
        &mut self,
        query: Query,
        now_ms: u64,
        relative_ms: u64,
    ) -> Result<PendingRequest, ClientError> {
        self.begin_inner(
            query,
            Some(now_ms.saturating_add(relative_ms)),
            Some(relative_ms),
        )
    }

    fn begin_inner(
        &mut self,
        query: Query,
        deadline_at_ms: Option<u64>,
        relative_deadline_ms: Option<u64>,
    ) -> Result<PendingRequest, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        let id = RequestId::from_u128(self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        let payload = Request {
            request_kind: match query {
                Query::Bootstrap => "bootstrap".to_owned(),
            },
            deadline: relative_deadline_ms,
            idempotency_key: None,
            payload: serde_json::Value::Null,
        };
        self.outbound.push_back(ClientFrame {
            kind: ClientKind::Request,
            request_id: Some(id.clone()),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(payload).map_err(|error| {
                ClientError::Protocol(format!("could not encode request: {error}"))
            })?,
        });
        self.pending.insert(
            id.clone(),
            PendingEntry {
                query,
                deadline_at_ms,
            },
        );
        Ok(PendingRequest { id })
    }

    /// Whether this request is still awaiting a response.
    #[must_use]
    pub fn is_pending(&self, request: &PendingRequest) -> bool {
        self.pending.contains_key(request.id())
    }

    /// Retrieve a completed result once.
    pub fn take(&mut self, request: PendingRequest) -> Option<QueryResult> {
        self.completed.remove(request.id())
    }

    /// Cancel a live request. Repeated cancellation is a no-op.
    pub fn cancel(&mut self, request: &PendingRequest) -> bool {
        if self.pending.remove(request.id()).is_none() {
            return false;
        }
        self.queue_cancel(request.id().clone());
        true
    }

    /// Cancel and return every request whose deadline has elapsed.
    pub fn expire_deadlines(&mut self, now_ms: u64) -> Vec<PendingRequest> {
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, entry)| entry.deadline_at_ms.is_some_and(|at| at <= now_ms))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            let _removed = self.pending.remove(id);
            self.queue_cancel(id.clone());
        }
        expired
            .into_iter()
            .map(|id| PendingRequest { id })
            .collect()
    }

    fn queue_cancel(&mut self, id: RequestId) {
        self.outbound.push_back(ClientFrame {
            kind: ClientKind::CancelRequest,
            request_id: Some(id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(CancelRequest)
                .expect("unit cancellation payload always serializes"),
        });
    }

    /// Apply one decoded server frame.
    pub fn apply(&mut self, frame: ServerFrame) -> Result<(), ClientError> {
        self.apply_at(frame, 0, 0)
    }

    /// Apply a frame with clock and entropy inputs used if it requests reconnect.
    pub fn apply_at(
        &mut self,
        frame: ServerFrame,
        now_ms: u64,
        entropy: u64,
    ) -> Result<(), ClientError> {
        if frame.kind == ServerKind::Response {
            let id = frame
                .request_id
                .clone()
                .ok_or_else(|| ClientError::Protocol("response missing request ID".to_owned()))?;
            let pending = self
                .pending
                .remove(&id)
                .ok_or_else(|| ClientError::UnknownResponse(id.clone()))?;
            let result = match pending.query {
                Query::Bootstrap => {
                    let bootstrap: BootstrapResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    self.server_bootstrap = Some(bootstrap.clone());
                    self.refresh_server_validity();
                    QueryResult::Bootstrap(bootstrap)
                }
            };
            self.completed.insert(id, result);
            return Ok(());
        }
        if let Some(sequence) = frame.sequence {
            let expected = self
                .last_acked_sequence
                .map_or(1, |last| last.saturating_add(1));
            if sequence > expected {
                self.rebuild_server_state()?;
                return Err(ClientError::SequenceGap {
                    expected,
                    got: sequence,
                });
            }
            if sequence == expected {
                self.last_acked_sequence = Some(sequence);
                self.queue_ack(sequence);
            } else {
                self.queue_ack(self.last_acked_sequence.unwrap_or(0));
                return Ok(());
            }
        }
        match frame
            .decode_payload()
            .map_err(|error| ClientError::Protocol(error.message))?
        {
            ServerPayload::Welcome(welcome) if self.phase == ClientPhase::Authenticating => {
                if welcome.protocol.major != PROTOCOL_MAJOR {
                    self.phase = ClientPhase::UpgradeRequired;
                    self.retry = None;
                    self.target = None;
                    return Err(ClientError::IncompatibleProtocol {
                        client_major: PROTOCOL_MAJOR,
                        server_major: welcome.protocol.major,
                    });
                }
                self.phase = ClientPhase::Ready;
                if matches!(welcome.resume_status, ResumeStatus::Fresh) {
                    self.last_acked_sequence = None;
                }
                self.session_id = Some(welcome.session_id.clone());
                self.server_instance_id = Some(welcome.server_instance_id.clone());
                let recover = self.reconnecting
                    || matches!(welcome.resume_status, ResumeStatus::ResyncRequired);
                self.retry = None;
                self.retry_attempt = 0;
                self.reconnecting = false;
                if recover {
                    self.rebuild_server_state()?;
                }
                Ok(())
            }
            ServerPayload::Subscribed(_) => {
                let id = frame.subscription_id.ok_or_else(|| {
                    ClientError::Protocol("subscribed frame missing subscription ID".to_owned())
                })?;
                if self.live_subscriptions.contains_key(&id) {
                    self.active_subscriptions.insert(id);
                }
                self.refresh_server_validity();
                Ok(())
            }
            ServerPayload::Event(_) => Ok(()),
            ServerPayload::ResyncRequired(_) => self.rebuild_server_state(),
            ServerPayload::Error(error)
                if self.phase == ClientPhase::Authenticating
                    && error.code == ErrorCode::Unauthorized =>
            {
                self.phase = ClientPhase::WebGate;
                self.retry = None;
                self.reconnecting = false;
                self.target = None;
                Err(ClientError::AuthenticationRejected)
            }
            ServerPayload::Error(error) => {
                if error.retryability == Retryability::AfterReconnect {
                    self.transport_lost(now_ms, entropy);
                }
                Err(ClientError::Server(error))
            }
            _ => Err(ClientError::Protocol("unexpected server frame".to_owned())),
        }
    }

    fn queue_ack(&mut self, sequence: u64) {
        self.outbound.push_back(ClientFrame {
            kind: ClientKind::Ack,
            request_id: None,
            subscription_id: None,
            sequence: Some(sequence),
            payload: serde_json::to_value(Ack {
                last_acked_sequence: sequence,
            })
            .expect("ack payload always serializes"),
        });
    }

    fn invalidate_server_state(&mut self) {
        self.server_bootstrap = None;
        self.server_state_invalid = true;
        self.active_subscriptions.clear();
        self.pending.clear();
        self.completed.clear();
    }

    fn rebuild_server_state(&mut self) -> Result<(), ClientError> {
        self.invalidate_server_state();
        let _bootstrap = self.begin(Query::Bootstrap)?;
        let subscriptions: Vec<_> = self
            .live_subscriptions
            .iter()
            .map(|(id, selector)| (id.clone(), selector.clone()))
            .collect();
        for (id, selector) in subscriptions {
            self.queue_subscribe(id, selector)?;
        }
        Ok(())
    }

    fn refresh_server_validity(&mut self) {
        self.server_state_invalid = self.server_bootstrap.is_none()
            || self.active_subscriptions.len() < self.live_subscriptions.len();
    }
}
