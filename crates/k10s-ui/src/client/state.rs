use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use k10s_protocol::{
    Ack, BackendRevision, BootstrapResponse, CancelRequest, ClientFrame, ClientKind, ErrorCode,
    ErrorFrame, ErrorScope, Hello, INFRASTRUCTURE_EVENT_UPDATED, InfrastructureRequest,
    InfrastructureResponse, InfrastructureWatchSpec, OperationAccepted, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, RESOURCE_EVENT_CHANGED, RESOURCE_EVENT_GONE, Request, RequestId,
    ResourceDetailResponse, ResourceIdentity, ResourceListRequest, ResourceListResponse,
    ResourceListRow, ResourceRefRequest, ResourceTypesRequest, ResourceTypesResponse, ResumeStatus,
    Retryability, ServerFrame, ServerKind, ServerPayload, SessionId, StreamTarget,
    StreamTicketRequest, StreamTicketResponse, StreamType, Subscribe, SubscriptionId,
    SubscriptionSelector, Unsubscribe, YamlApplyRequest, YamlOutcome, YamlValidateRequest,
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
    /// Hard maximum number of frames waiting for transport delivery.
    pub outbound_capacity: usize,
    /// Shared hard bound for pending and completed request results.
    pub request_capacity: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            capabilities: vec!["bootstrap-status".to_owned()],
            retry_base_ms: 250,
            retry_cap_ms: 30_000,
            outbound_capacity: 256,
            request_capacity: 256,
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
    /// A reliable frame could not enter the bounded outbound queue.
    OutboundOverload {
        /// Configured hard frame bound.
        capacity: usize,
    },
    /// Adding a desired subscription would make worst-case recovery impossible.
    LiveSubscriptionLimit {
        /// Maximum recoverable desired subscription count.
        limit: usize,
    },
    /// Pending and completed requests reached their shared retention bound.
    RequestRetentionLimit {
        /// Configured shared request retention bound.
        limit: usize,
    },
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
            Self::OutboundOverload { capacity } => {
                write!(formatter, "outbound queue reached capacity {capacity}")
            }
            Self::LiveSubscriptionLimit { limit } => {
                write!(formatter, "live subscription limit is {limit}")
            }
            Self::RequestRetentionLimit { limit } => {
                write!(formatter, "request retention limit is {limit}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

/// Request behaviors supported by the foundation client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// Retrieve server identity and safe Kubernetes contexts.
    Bootstrap,
    /// Retrieve a normalized resource list for one type on one context.
    ResourceList(ResourceListQuery),
    /// Retrieve normalized details for one resource identity.
    ResourceDetail(ResourceIdentity),
    /// List the selectable resource types of one context.
    ResourceTypes(ResourceTypesRequest),
    /// Retrieve Overview, Nodes, Storage, and cluster metrics for a context.
    Infrastructure(InfrastructureRequest),
    /// Validate a YAML buffer without submitting it; a valid outcome carries
    /// the backend-issued single-use ticket.
    YamlValidate {
        /// Kubernetes context the manifest targets.
        context: String,
        /// The exact YAML text to validate.
        yaml: String,
    },
    /// Issue a single-use stream ticket for a dedicated logs/exec socket.
    /// The ticket is redeemed in the stream socket's first `hello`, never
    /// placed in any URL.
    StreamTicket {
        /// Pod/container to attach to.
        target: StreamTarget,
        /// Whether this opens logs or an exec session.
        stream_type: StreamType,
        /// Exec mode: interactive TTY shell vs separated stdout/stderr.
        tty: bool,
    },
}

/// Command behaviors: mutations that return an `OperationId`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Apply a previously validated YAML buffer through its ticket.
    YamlApply {
        /// The validated apply request.
        request: YamlApplyRequest,
        /// Idempotency key for safe retries.
        idempotency_key: String,
    },
}

/// Selector for a normalized resource list query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceListQuery {
    /// Kubernetes context to read from.
    pub context: String,
    /// API group, empty for the core group.
    pub group: String,
    /// API version within the group.
    pub version: String,
    /// Kubernetes kind, such as `Deployment`.
    pub kind: String,
    /// Optional namespace restriction.
    pub namespace: Option<String>,
}

/// A completed query value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResult {
    /// Bootstrap query result.
    Bootstrap(BootstrapResponse),
    /// Normalized resource list result.
    ResourceList(ResourceListResponse),
    /// Normalized single-resource detail result with backend-resolved
    /// events and related rows.
    ResourceDetail(Box<ResourceDetailResponse>),
    /// Selectable resource types of one context (built-ins and CRDs).
    ResourceTypes(Box<ResourceTypesResponse>),
    /// Complete infrastructure projection.
    Infrastructure(Box<InfrastructureResponse>),
    /// Guarded YAML validation outcome; `Valid` carries the ticket.
    YamlValidate(Box<YamlOutcome>),
    /// A granted single-use stream ticket for a dedicated logs/exec socket.
    StreamTicket(Box<StreamTicketResponse>),
    /// An accepted apply command with its background operation ID.
    Applied(OperationAccepted),
}

/// The retained, applied list view of one resource watch subscription.
///
/// The view starts from a completed chunked snapshot and then applies only
/// contiguous revisions: a delta whose revision is not strictly newer than
/// the last applied revision is stale or out of order and is ignored, so a
/// replayed frame can never regress the list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceListState {
    revision: Option<BackendRevision>,
    rows: BTreeMap<ResourceIdentity, ResourceListRow>,
}

impl ResourceListState {
    /// Revision of the last applied snapshot page or delta; `None` until a
    /// snapshot has completed for this watch.
    #[must_use]
    pub fn revision(&self) -> Option<BackendRevision> {
        self.revision
    }

    /// Retained rows sorted by stable resource identity.
    pub fn rows(&self) -> impl Iterator<Item = &ResourceListRow> {
        self.rows.values()
    }

    fn apply_snapshot(&mut self, revision: BackendRevision, rows: Vec<ResourceListRow>) {
        self.revision = Some(revision);
        self.rows = rows
            .into_iter()
            .map(|row| (row.identity.clone(), row))
            .collect();
    }

    /// Apply one changed delta when its revision is strictly newer than the
    /// last applied revision; anything else is stale and ignored. Deltas
    /// that arrive before any snapshot are dropped the same way: there is
    /// no baseline to be contiguous with.
    fn apply_changed(&mut self, identity: ResourceIdentity, row: ResourceListRow) {
        if self.revision.is_none_or(|applied| row.revision <= applied) {
            return;
        }
        self.revision = Some(row.revision);
        self.rows.insert(identity, row);
    }

    /// Apply one gone delta under the same contiguous-revision rule.
    fn apply_gone(&mut self, identity: ResourceIdentity, revision: BackendRevision) {
        if self.revision.is_none_or(|applied| revision <= applied) {
            return;
        }
        self.revision = Some(revision);
        self.rows.remove(&identity);
    }
}

/// A reassembled resource snapshot delivered for one subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSnapshot {
    /// Backend revision covering the snapshot.
    pub revision: BackendRevision,
    /// Sorted normalized rows.
    pub rows: Vec<ResourceListRow>,
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

#[derive(Debug, Clone)]
enum PendingAction {
    Query(Query),
    Command(Command),
}

impl PendingAction {
    fn request_kind(&self) -> &'static str {
        match self {
            Self::Query(Query::Bootstrap) => "bootstrap",
            Self::Query(Query::ResourceList(_)) => "resource.list",
            Self::Query(Query::ResourceDetail(_)) => "resource.detail",
            Self::Query(Query::ResourceTypes(_)) => "resource.types",
            Self::Query(Query::Infrastructure(_)) => "infrastructure.get",
            Self::Query(Query::YamlValidate { .. }) => "yaml.validate",
            Self::Query(Query::StreamTicket { .. }) => k10s_protocol::REQUEST_STREAM_TICKET,
            Self::Command(Command::YamlApply { .. }) => "yaml.apply",
        }
    }

    fn encode_payload(&self) -> Result<serde_json::Value, ClientError> {
        fn encode(value: impl serde::Serialize) -> Result<serde_json::Value, ClientError> {
            serde_json::to_value(value).map_err(|error| {
                ClientError::Protocol(format!("could not encode request: {error}"))
            })
        }
        match self {
            Self::Query(Query::Bootstrap) => Ok(serde_json::Value::Null),
            Self::Query(Query::ResourceList(selector)) => encode(ResourceListRequest {
                context: selector.context.clone(),
                gvk: k10s_protocol::GroupVersionKind {
                    group: selector.group.clone(),
                    version: selector.version.clone(),
                    kind: selector.kind.clone(),
                },
                namespace: selector.namespace.clone(),
            }),
            Self::Query(Query::ResourceDetail(identity)) => encode(ResourceRefRequest {
                identity: identity.clone(),
            }),
            Self::Query(Query::ResourceTypes(request)) => encode(request),
            Self::Query(Query::Infrastructure(request)) => encode(request),
            Self::Query(Query::YamlValidate { context, yaml }) => encode(YamlValidateRequest {
                context: context.clone(),
                yaml: yaml.clone(),
            }),
            Self::Query(Query::StreamTicket {
                target,
                stream_type,
                tty,
            }) => encode(StreamTicketRequest {
                target: target.clone(),
                stream_type: *stream_type,
                tty: *tty,
            }),
            Self::Command(Command::YamlApply { request, .. }) => encode(request),
        }
    }

    fn idempotency_key(&self) -> Option<String> {
        match self {
            Self::Command(Command::YamlApply {
                idempotency_key, ..
            }) => Some(idempotency_key.clone()),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct PendingEntry {
    action: PendingAction,
    deadline_at_ms: Option<u64>,
    cancelled: bool,
}

/// In-progress reassembly of one chunked resource snapshot.
#[derive(Debug)]
struct SnapshotAssembly {
    total_chunks: u32,
    received_chunks: u32,
    revision: BackendRevision,
    rows: Vec<ResourceListRow>,
}

/// Pure client protocol state.
pub struct ClientState {
    config: ClientConfig,
    phase: ClientPhase,
    outbound: VecDeque<ClientFrame>,
    next_request_id: u128,
    pending: BTreeMap<RequestId, PendingEntry>,
    completed: BTreeMap<RequestId, QueryResult>,
    rebuilt_bootstrap: Option<PendingRequest>,
    target: Option<ConnectTarget>,
    retry_attempt: u32,
    retry: Option<RetrySchedule>,
    reconnecting: bool,
    last_acked_sequence: Option<u64>,
    next_subscription_id: u128,
    live_subscriptions: HashMap<SubscriptionId, serde_json::Value>,
    active_subscriptions: HashSet<SubscriptionId>,
    /// Typed selectors of live resource watches, used to filter deltas.
    resource_specs: HashMap<SubscriptionId, k10s_protocol::ResourceWatchSpec>,
    snapshot_assemblies: HashMap<SubscriptionId, SnapshotAssembly>,
    completed_snapshots: HashMap<SubscriptionId, ResourceSnapshot>,
    /// Retained applied list views, one per live resource watch.
    resource_lists: HashMap<SubscriptionId, ResourceListState>,
    infrastructure: HashMap<String, InfrastructureResponse>,
    server_bootstrap: Option<BootstrapResponse>,
    server_state_invalid: bool,
    local_ui: LocalUiState,
    session_id: Option<SessionId>,
    server_instance_id: Option<String>,
}

impl std::fmt::Debug for ClientState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientState")
            .field("config", &self.config)
            .field("phase", &self.phase)
            .field("outbound_len", &self.outbound.len())
            .field("pending_len", &self.pending.len())
            .field("completed_len", &self.completed.len())
            .field("rebuilt_bootstrap", &self.rebuilt_bootstrap)
            .field("target", &self.target)
            .field("retry_attempt", &self.retry_attempt)
            .field("retry", &self.retry)
            .field("reconnecting", &self.reconnecting)
            .field("last_acked_sequence", &self.last_acked_sequence)
            .field("live_subscriptions_len", &self.live_subscriptions.len())
            .field("active_subscriptions_len", &self.active_subscriptions.len())
            .field("snapshot_assemblies_len", &self.snapshot_assemblies.len())
            .field("completed_snapshots_len", &self.completed_snapshots.len())
            .field("resource_lists_len", &self.resource_lists.len())
            .field("infrastructure_len", &self.infrastructure.len())
            .field("server_state_invalid", &self.server_state_invalid)
            .field("local_ui", &self.local_ui)
            .field("session_id", &self.session_id)
            .field("server_instance_id", &self.server_instance_id)
            .finish()
    }
}

impl ClientState {
    /// Construct a disconnected client.
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        let outbound_capacity = config.outbound_capacity;
        Self {
            config,
            phase: ClientPhase::Disconnected,
            outbound: VecDeque::with_capacity(outbound_capacity),
            next_request_id: 1,
            pending: BTreeMap::new(),
            completed: BTreeMap::new(),
            rebuilt_bootstrap: None,
            target: None,
            retry_attempt: 0,
            retry: None,
            reconnecting: false,
            last_acked_sequence: None,
            next_subscription_id: 1,
            live_subscriptions: HashMap::new(),
            active_subscriptions: HashSet::new(),
            snapshot_assemblies: HashMap::new(),
            completed_snapshots: HashMap::new(),
            resource_specs: HashMap::new(),
            resource_lists: HashMap::new(),
            infrastructure: HashMap::new(),
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
        let frame = ClientFrame {
            kind: ClientKind::Hello,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(hello).map_err(|error| {
                ClientError::Protocol(format!("could not encode hello: {error}"))
            })?,
        };
        let _endpoint_without_credentials = target.url;
        self.enqueue_reliable(frame)
    }

    /// Remove the next frame waiting for transport delivery.
    pub fn take_outbound(&mut self) -> Option<ClientFrame> {
        if self.phase == ClientPhase::Closed {
            return None;
        }
        self.outbound.pop_front()
    }

    /// Number of frames waiting for transport delivery.
    #[must_use]
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    /// Number of desired subscriptions retained for recovery.
    #[must_use]
    pub fn live_subscription_count(&self) -> usize {
        self.live_subscriptions.len()
    }

    /// Maximum desired set that always leaves room for bootstrap and a
    /// sequenced resync acknowledgement.
    #[must_use]
    pub fn live_subscription_limit(&self) -> usize {
        if self.config.request_capacity == 0 {
            0
        } else {
            self.config.outbound_capacity.saturating_sub(2)
        }
    }

    /// Start and remember the Plan 1 bootstrap-status subscription.
    pub fn subscribe_bootstrap_status(&mut self) -> Result<LiveSubscription, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        let limit = self.live_subscription_limit();
        if self.live_subscriptions.len() >= limit {
            return Err(ClientError::LiveSubscriptionLimit { limit });
        }
        let id = SubscriptionId::new(format!("bootstrap-status-{}", self.next_subscription_id));
        self.next_subscription_id = self.next_subscription_id.saturating_add(1);
        let selector = serde_json::json!({"kind":"bootstrapStatus"});
        self.queue_subscribe(id.clone(), selector.clone())?;
        self.live_subscriptions.insert(id.clone(), selector);
        self.refresh_server_validity();
        Ok(LiveSubscription { id })
    }

    /// Start and remember a normalized resource watch subscription.
    ///
    /// The desired selector is retained so recovery resubscribes it after a
    /// reconnect, exactly like the bootstrap-status subscription.
    pub fn subscribe_resource(
        &mut self,
        context: impl Into<String>,
        group: impl Into<String>,
        version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
    ) -> Result<LiveSubscription, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        let limit = self.live_subscription_limit();
        if self.live_subscriptions.len() >= limit {
            return Err(ClientError::LiveSubscriptionLimit { limit });
        }
        let id = SubscriptionId::new(format!("resource-{}", self.next_subscription_id));
        self.next_subscription_id = self.next_subscription_id.saturating_add(1);
        let spec = k10s_protocol::ResourceWatchSpec {
            context: context.into(),
            gvk: k10s_protocol::GroupVersionKind {
                group: group.into(),
                version: version.into(),
                kind: kind.into(),
            },
            namespace,
        };
        let selector = serde_json::to_value(SubscriptionSelector::Resource(spec.clone())).map_err(
            |error| ClientError::Protocol(format!("could not encode selector: {error}")),
        )?;
        self.queue_subscribe(id.clone(), selector.clone())?;
        self.live_subscriptions.insert(id.clone(), selector);
        self.resource_specs.insert(id.clone(), spec);
        self.refresh_server_validity();
        Ok(LiveSubscription { id })
    }

    /// Subscribe to coalesced infrastructure telemetry for one context.
    pub fn subscribe_infrastructure(
        &mut self,
        context: impl Into<String>,
    ) -> Result<LiveSubscription, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        let limit = self.live_subscription_limit();
        if self.live_subscriptions.len() >= limit {
            return Err(ClientError::LiveSubscriptionLimit { limit });
        }
        let id = SubscriptionId::new(format!("infrastructure-{}", self.next_subscription_id));
        self.next_subscription_id = self.next_subscription_id.saturating_add(1);
        let selector = serde_json::to_value(SubscriptionSelector::Infrastructure(
            InfrastructureWatchSpec::new(context),
        ))
        .map_err(|error| ClientError::Protocol(format!("could not encode selector: {error}")))?;
        self.queue_subscribe(id.clone(), selector.clone())?;
        self.live_subscriptions.insert(id.clone(), selector);
        self.refresh_server_validity();
        Ok(LiveSubscription { id })
    }

    /// Stop retaining and recovering one desired subscription.
    pub fn unsubscribe(&mut self, subscription: &LiveSubscription) -> Result<bool, ClientError> {
        if !self.live_subscriptions.contains_key(subscription.id()) {
            return Ok(false);
        }
        self.enqueue_reliable(ClientFrame {
            kind: ClientKind::Unsubscribe,
            request_id: None,
            subscription_id: Some(subscription.id().clone()),
            sequence: None,
            payload: serde_json::to_value(Unsubscribe)
                .expect("unit unsubscribe payload always serializes"),
        })?;
        self.live_subscriptions.remove(subscription.id());
        self.active_subscriptions.remove(subscription.id());
        self.resource_specs.remove(subscription.id());
        self.snapshot_assemblies.remove(subscription.id());
        self.completed_snapshots.remove(subscription.id());
        self.resource_lists.remove(subscription.id());
        self.refresh_server_validity();
        Ok(true)
    }

    /// Latest response or telemetry update for a context. The response is
    /// retained across connection loss so the UI can mark it stale with its
    /// original timestamp.
    #[must_use]
    pub fn infrastructure(&self, context: &str) -> Option<&InfrastructureResponse> {
        self.infrastructure.get(context)
    }

    /// Take the completed snapshot reassembled for one subscription.
    pub fn take_resource_snapshot(&mut self, id: &SubscriptionId) -> Option<ResourceSnapshot> {
        self.completed_snapshots.remove(id)
    }

    /// The retained, applied list view of one live resource watch. The view
    /// starts from the latest completed snapshot and reflects every applied
    /// contiguous delta; it survives until the subscription is dropped or
    /// the connection generation is torn down.
    #[must_use]
    pub fn resource_list(&self, id: &SubscriptionId) -> Option<&ResourceListState> {
        self.resource_lists.get(id)
    }

    fn queue_subscribe(
        &mut self,
        id: SubscriptionId,
        selector: serde_json::Value,
    ) -> Result<(), ClientError> {
        self.enqueue_reliable(ClientFrame {
            kind: ClientKind::Subscribe,
            request_id: None,
            subscription_id: Some(id),
            sequence: None,
            payload: serde_json::to_value(Subscribe(selector)).map_err(|error| {
                ClientError::Protocol(format!("could not encode subscription: {error}"))
            })?,
        })
    }

    fn enqueue_reliable(&mut self, frame: ClientFrame) -> Result<(), ClientError> {
        self.ensure_outbound_slots(1)?;
        self.outbound.push_back(frame);
        Ok(())
    }

    fn ensure_outbound_slots(&mut self, additional: usize) -> Result<(), ClientError> {
        if self.outbound.len().saturating_add(additional) <= self.config.outbound_capacity {
            return Ok(());
        }
        self.fail_outbound_overload()
    }

    fn fail_outbound_overload<T>(&mut self) -> Result<T, ClientError> {
        // Retain the queue and request/subscription maps for diagnostics and
        // rollback observability, but make them undrainable until an explicit
        // new connection clears the failed generation.
        self.phase = ClientPhase::Closed;
        self.retry = None;
        self.reconnecting = false;
        self.target = None;
        self.server_state_invalid = true;
        self.active_subscriptions.clear();
        Err(ClientError::OutboundOverload {
            capacity: self.config.outbound_capacity,
        })
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
        self.outbound.clear();
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
    pub fn retry_if_due(&mut self, now_ms: u64) -> Result<bool, ClientError> {
        if self.phase != ClientPhase::Disconnected {
            return Ok(false);
        }
        let Some(schedule) = self.retry else {
            return Ok(false);
        };
        if now_ms < schedule.retry_at_ms {
            return Ok(false);
        }
        let Some(target) = self.target.clone() else {
            return Ok(false);
        };
        self.retry = None;
        self.queue_hello(target)?;
        self.phase = ClientPhase::Authenticating;
        Ok(true)
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
        self.begin_inner(PendingAction::Query(query), None, None)
    }

    /// Begin a command (mutation) that returns an `OperationId`.
    pub fn begin_command(&mut self, command: Command) -> Result<PendingRequest, ClientError> {
        self.begin_inner(PendingAction::Command(command), None, None)
    }

    /// Begin a query with a relative deadline measured against `now_ms`.
    pub fn begin_with_deadline(
        &mut self,
        query: Query,
        now_ms: u64,
        relative_ms: u64,
    ) -> Result<PendingRequest, ClientError> {
        self.begin_inner(
            PendingAction::Query(query),
            Some(now_ms.saturating_add(relative_ms)),
            Some(relative_ms),
        )
    }

    fn begin_inner(
        &mut self,
        action: PendingAction,
        deadline_at_ms: Option<u64>,
        relative_deadline_ms: Option<u64>,
    ) -> Result<PendingRequest, ClientError> {
        if self.phase != ClientPhase::Ready {
            return Err(ClientError::InvalidState("client is not ready"));
        }
        if self.pending.len().saturating_add(self.completed.len()) >= self.config.request_capacity {
            return Err(ClientError::RequestRetentionLimit {
                limit: self.config.request_capacity,
            });
        }
        let id = RequestId::from_u128(self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        let payload = Request {
            request_kind: action.request_kind().to_owned(),
            deadline: relative_deadline_ms,
            idempotency_key: action.idempotency_key(),
            payload: action.encode_payload()?,
        };
        let frame = ClientFrame {
            kind: ClientKind::Request,
            request_id: Some(id.clone()),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(payload).map_err(|error| {
                ClientError::Protocol(format!("could not encode request: {error}"))
            })?,
        };
        self.enqueue_reliable(frame)?;
        self.pending.insert(
            id.clone(),
            PendingEntry {
                action,
                deadline_at_ms,
                cancelled: false,
            },
        );
        Ok(PendingRequest { id })
    }

    /// Whether this request is still awaiting a response.
    #[must_use]
    pub fn is_pending(&self, request: &PendingRequest) -> bool {
        self.pending
            .get(request.id())
            .is_some_and(|entry| !entry.cancelled)
    }

    /// Retrieve a completed result once.
    pub fn take(&mut self, request: PendingRequest) -> Option<QueryResult> {
        self.completed.remove(request.id())
    }

    /// Take the bootstrap request created internally during recovery or resynchronization.
    pub(crate) fn take_rebuilt_bootstrap(&mut self) -> Option<PendingRequest> {
        self.rebuilt_bootstrap.take()
    }

    /// Cancel a live request. Repeated cancellation is a no-op.
    pub fn cancel(&mut self, request: &PendingRequest) -> Result<bool, ClientError> {
        if self
            .pending
            .get(request.id())
            .is_none_or(|entry| entry.cancelled)
        {
            return Ok(false);
        }
        self.queue_cancel(request.id().clone())?;
        self.pending
            .get_mut(request.id())
            .expect("request remains correlated after queuing cancellation")
            .cancelled = true;
        Ok(true)
    }

    /// Cancel and return every request whose deadline has elapsed.
    pub fn expire_deadlines(&mut self, now_ms: u64) -> Result<Vec<PendingRequest>, ClientError> {
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, entry)| {
                !entry.cancelled && entry.deadline_at_ms.is_some_and(|at| at <= now_ms)
            })
            .map(|(id, _)| id.clone())
            .collect();
        self.ensure_outbound_slots(expired.len())?;
        for id in &expired {
            self.queue_cancel(id.clone())?;
        }
        for id in &expired {
            self.pending
                .get_mut(id)
                .expect("expired request remains correlated")
                .cancelled = true;
        }
        Ok(expired
            .into_iter()
            .map(|id| PendingRequest { id })
            .collect())
    }

    fn queue_cancel(&mut self, id: RequestId) -> Result<(), ClientError> {
        self.enqueue_reliable(ClientFrame {
            kind: ClientKind::CancelRequest,
            request_id: Some(id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(CancelRequest)
                .expect("unit cancellation payload always serializes"),
        })
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
            let action = self
                .pending
                .get(&id)
                .map(|pending| (pending.action.clone(), pending.cancelled))
                .ok_or_else(|| ClientError::UnknownResponse(id.clone()))?;
            let (action, cancelled) = action;
            if cancelled {
                self.pending.remove(&id);
                return Ok(());
            }
            let result = match &action {
                PendingAction::Query(Query::Bootstrap) => {
                    let bootstrap: BootstrapResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    self.server_bootstrap = Some(bootstrap.clone());
                    self.refresh_server_validity();
                    QueryResult::Bootstrap(bootstrap)
                }
                PendingAction::Query(Query::ResourceList(_)) => {
                    let list: ResourceListResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::ResourceList(list)
                }
                PendingAction::Query(Query::ResourceDetail(_)) => {
                    let detail: ResourceDetailResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::ResourceDetail(Box::new(detail))
                }
                PendingAction::Query(Query::ResourceTypes(_)) => {
                    let types: ResourceTypesResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::ResourceTypes(Box::new(types))
                }
                PendingAction::Query(Query::Infrastructure(_)) => {
                    let response: InfrastructureResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    let replace = self
                        .infrastructure
                        .get(&response.context)
                        .is_none_or(|current| response.revision >= current.revision);
                    if replace {
                        self.infrastructure
                            .insert(response.context.clone(), response.clone());
                    }
                    QueryResult::Infrastructure(Box::new(response))
                }
                PendingAction::Query(Query::YamlValidate { .. }) => {
                    let outcome: YamlOutcome = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::YamlValidate(Box::new(outcome))
                }
                PendingAction::Query(Query::StreamTicket { .. }) => {
                    let granted: StreamTicketResponse = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::StreamTicket(Box::new(granted))
                }
                PendingAction::Command(Command::YamlApply { .. }) => {
                    let accepted: OperationAccepted = frame
                        .decode_response_payload()
                        .map_err(|error| ClientError::Protocol(error.message))?;
                    QueryResult::Applied(accepted)
                }
            };
            let _pending = self.pending.remove(&id);
            self.completed.insert(id, result);
            return Ok(());
        }

        let payload = frame
            .decode_payload()
            .map_err(|error| ClientError::Protocol(error.message))?;
        let sequence = frame.sequence;
        if let Some(sequence) = sequence {
            let expected = self
                .last_acked_sequence
                .map_or(1, |last| last.saturating_add(1));
            if sequence > expected {
                self.rebuild_server_state(0)?;
                return Err(ClientError::SequenceGap {
                    expected,
                    got: sequence,
                });
            }
            if sequence < expected {
                self.queue_ack(self.last_acked_sequence.unwrap_or(0))?;
                return Ok(());
            }
        }

        let result = match payload {
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
                    self.rebuild_server_state(0)?;
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
            ServerPayload::Event(event) => match event.event_kind.as_str() {
                RESOURCE_EVENT_CHANGED => {
                    let delta: k10s_protocol::ResourceChanged =
                        serde_json::from_value(event.payload)
                            .map_err(|error| ClientError::Protocol(error.to_string()))?;
                    let id = self.owned_subscription(&frame);
                    if id
                        .as_ref()
                        .is_some_and(|id| self.spec_matches(id, &delta.identity))
                        && let Some(state) =
                            id.as_ref().and_then(|id| self.resource_lists.get_mut(id))
                    {
                        // Before the first snapshot completes there is no
                        // baseline and the delta is dropped: the snapshot
                        // supersedes it.
                        state.apply_changed(delta.identity, delta.row);
                    }
                    Ok(())
                }
                RESOURCE_EVENT_GONE => {
                    let delta: k10s_protocol::ResourceGone = serde_json::from_value(event.payload)
                        .map_err(|error| ClientError::Protocol(error.to_string()))?;
                    let id = self.owned_subscription(&frame);
                    if id
                        .as_ref()
                        .is_some_and(|id| self.spec_matches(id, &delta.identity))
                        && let Some(state) =
                            id.as_ref().and_then(|id| self.resource_lists.get_mut(id))
                    {
                        state.apply_gone(delta.identity, delta.revision);
                    }
                    Ok(())
                }
                INFRASTRUCTURE_EVENT_UPDATED => {
                    if self.owned_subscription(&frame).is_some() {
                        let response: InfrastructureResponse =
                            serde_json::from_value(event.payload)
                                .map_err(|error| ClientError::Protocol(error.to_string()))?;
                        let replace = self
                            .infrastructure
                            .get(&response.context)
                            .is_none_or(|current| response.revision >= current.revision);
                        if replace {
                            self.infrastructure
                                .insert(response.context.clone(), response);
                        }
                    }
                    Ok(())
                }
                _ => Ok(()),
            },
            ServerPayload::SnapshotBegin(begin) => {
                if let Some(id) = self.owned_subscription(&frame) {
                    self.snapshot_assemblies.insert(
                        id,
                        SnapshotAssembly {
                            total_chunks: begin.total_chunks,
                            received_chunks: 0,
                            revision: BackendRevision::new(0),
                            rows: Vec::new(),
                        },
                    );
                }
                Ok(())
            }
            ServerPayload::SnapshotChunk(chunk) => {
                let Some(id) = self.owned_subscription(&frame) else {
                    return Ok(());
                };
                let page: k10s_protocol::ResourceSnapshotPage = serde_json::from_value(chunk.data)
                    .map_err(|error| ClientError::Protocol(error.to_string()))?;
                let Some(assembly) = self.snapshot_assemblies.get_mut(&id) else {
                    return Ok(());
                };
                if chunk.chunk_index != assembly.received_chunks
                    || assembly.received_chunks >= assembly.total_chunks
                {
                    return Err(ClientError::Protocol(
                        "snapshot chunk out of order".to_owned(),
                    ));
                }
                assembly.received_chunks += 1;
                assembly.revision = assembly.revision.max(page.revision);
                assembly.rows.extend(page.rows);
                if assembly.received_chunks == assembly.total_chunks {
                    let revision = assembly.revision;
                    let rows = std::mem::take(&mut assembly.rows);
                    self.completed_snapshots.insert(
                        id.clone(),
                        ResourceSnapshot {
                            revision,
                            rows: rows.clone(),
                        },
                    );
                    // The completed snapshot starts — or after a resync
                    // fully replaces — the retained applied view.
                    let mut state = ResourceListState::default();
                    state.apply_snapshot(revision, rows);
                    self.resource_lists.insert(id, state);
                }
                Ok(())
            }
            ServerPayload::SnapshotEnd(_end) => {
                let Some(id) = self.owned_subscription(&frame) else {
                    return Ok(());
                };
                match self.snapshot_assemblies.remove(&id) {
                    Some(assembly) if assembly.received_chunks == assembly.total_chunks => {
                        debug_assert!(self.completed_snapshots.contains_key(&id));
                        Ok(())
                    }
                    Some(_) | None => Err(ClientError::Protocol(
                        "snapshot ended before all chunks arrived".to_owned(),
                    )),
                }
            }
            ServerPayload::ResyncRequired(_) => {
                self.rebuild_server_state(usize::from(sequence.is_some()))
            }
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
            ServerPayload::Error(error)
                if self.phase == ClientPhase::Authenticating
                    && error.code == ErrorCode::IncompatibleProtocol =>
            {
                let server_major = error
                    .details
                    .as_ref()
                    .and_then(|details| details.get("serverProtocolMajor"))
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|major| u16::try_from(major).ok())
                    .unwrap_or(0);
                self.phase = ClientPhase::UpgradeRequired;
                self.retry = None;
                self.reconnecting = false;
                self.target = None;
                Err(ClientError::IncompatibleProtocol {
                    client_major: PROTOCOL_MAJOR,
                    server_major,
                })
            }
            ServerPayload::Error(error) => {
                let cancelled = if error.scope == ErrorScope::Request {
                    let id = frame.request_id.clone().ok_or_else(|| {
                        ClientError::Protocol("request error missing request ID".to_owned())
                    })?;
                    self.pending
                        .remove(&id)
                        .ok_or_else(|| ClientError::UnknownResponse(id.clone()))?
                        .cancelled
                } else {
                    false
                };
                if cancelled {
                    Ok(())
                } else if error.retryability == Retryability::AfterReconnect {
                    self.transport_lost(now_ms, entropy);
                    Err(ClientError::Server(error))
                } else {
                    Err(ClientError::Server(error))
                }
            }
            _ => Err(ClientError::Protocol("unexpected server frame".to_owned())),
        };
        result?;

        if let Some(sequence) = sequence {
            self.queue_ack(sequence)?;
            self.last_acked_sequence = Some(sequence);
        }
        Ok(())
    }

    fn queue_ack(&mut self, sequence: u64) -> Result<(), ClientError> {
        let ack = ClientFrame {
            kind: ClientKind::Ack,
            request_id: None,
            subscription_id: None,
            sequence: Some(sequence),
            payload: serde_json::to_value(Ack {
                last_acked_sequence: sequence,
            })
            .expect("ack payload always serializes"),
        };
        if let Some(queued) = self
            .outbound
            .iter_mut()
            .find(|frame| frame.kind == ClientKind::Ack)
        {
            *queued = ack;
            return Ok(());
        }
        self.enqueue_reliable(ack)
    }

    fn invalidate_server_state(&mut self) {
        self.server_bootstrap = None;
        self.server_state_invalid = true;
        self.active_subscriptions.clear();
        self.snapshot_assemblies.clear();
        self.completed_snapshots.clear();
        self.resource_specs.clear();
        self.resource_lists.clear();
        self.pending.clear();
        self.completed.clear();
        self.rebuilt_bootstrap = None;
    }

    /// Whether the watch behind `id` selected `identity`.
    fn spec_matches(
        &self,
        id: &SubscriptionId,
        identity: &k10s_protocol::ResourceIdentity,
    ) -> bool {
        self.resource_specs
            .get(id)
            .is_some_and(|spec| spec.matches(identity))
    }

    /// Return the subscription ID when the frame belongs to a desired
    /// subscription; frames from torn-down subscriptions are ignored.
    fn owned_subscription(&self, frame: &ServerFrame) -> Option<SubscriptionId> {
        let id = frame.subscription_id.clone()?;
        self.live_subscriptions.contains_key(&id).then_some(id)
    }

    fn rebuild_server_state(&mut self, reserved_outbound: usize) -> Result<(), ClientError> {
        if self.config.request_capacity == 0 {
            self.phase = ClientPhase::Closed;
            return Err(ClientError::RequestRetentionLimit { limit: 0 });
        }
        let required = 1_usize
            .saturating_add(self.live_subscriptions.len())
            .saturating_add(reserved_outbound);
        if required > self.config.outbound_capacity {
            return self.fail_outbound_overload();
        }
        self.outbound.clear();
        self.invalidate_server_state();
        let bootstrap = self.begin(Query::Bootstrap)?;
        self.rebuilt_bootstrap = Some(bootstrap);
        let subscriptions: Vec<_> = self
            .live_subscriptions
            .iter()
            .map(|(id, selector)| (id.clone(), selector.clone()))
            .collect();
        for (id, selector) in subscriptions {
            // Restore the typed watch selectors so retained list views keep
            // receiving filtered deltas on the rebuilt connection.
            if let Ok(SubscriptionSelector::Resource(spec)) =
                serde_json::from_value::<SubscriptionSelector>(selector.clone())
            {
                self.resource_specs.insert(id.clone(), spec);
            }
            self.queue_subscribe(id, selector)?;
        }
        Ok(())
    }

    fn refresh_server_validity(&mut self) {
        self.server_state_invalid = self.server_bootstrap.is_none()
            || self.active_subscriptions.len() < self.live_subscriptions.len();
    }
}
