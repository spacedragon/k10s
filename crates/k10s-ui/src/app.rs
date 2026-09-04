//! Minimal application state driven exclusively through the shared protocol client.

use web_time::Instant;

use std::collections::BTreeMap;

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_protocol::{
    ClientFrame, Context, ContextAvailability, ErrorCode, InfrastructureRequest, RequestId,
    ResourceDetailResponse, ResourceIdentity, ResourceListRow, ResourceMetricsResponse,
    ResourceTypeEntry, ResourceTypesRequest, ServerFrame, StreamTarget,
};

use crate::client::{
    BoundedInbox, ClientConfig, ClientError, ClientPhase, ClientState, Command, ConnectTarget,
    LiveSubscription, PendingRequest, Query, QueryResult, StreamRoute, StreamSession, StreamSignal,
    TransportError, WebSocketTransport,
};
use crate::ui::RowIdentity;
use crate::ui::dialogs::DialogAction;
use crate::ui::{ConnectionState as ShellConnectionState, InfrastructureLoad, UiShell};
use crate::ui::{
    DetailAuthority, DetailLifecycle, NamespaceCatalogState, PortForwardRetryErrors,
    PrimaryDetailState, RelationState, ResourceAction, ResourceFeed, SafeUiError, WindowFreshness,
    port_forward_start_authorization, retry_start_request,
};
use crate::workspace::{
    NamespaceScope, WindowId, WorkloadKind, WorkspaceCommand, WorkspaceEvent, WorkspaceSnapshot,
    WorkspaceState,
};

/// Bounded production control-event inbox sized to absorb one large
/// default snapshot page burst while the native frame loop drains it.
const CONTROL_INBOX_CAPACITY: usize = 256;

trait AppConnection: std::fmt::Debug {
    fn try_recv(&mut self) -> Option<WsEvent>;
    fn overflowed(&self) -> bool;
    fn send_frame(&mut self, frame: &ClientFrame) -> Result<(), TransportError>;
    fn close(&mut self);
}

trait ConnectionFactory: std::fmt::Debug {
    fn connect(&mut self, url: &str) -> Result<Box<dyn AppConnection>, TransportError>;
}

#[derive(Debug, Default)]
struct RealConnectionFactory;

#[derive(Debug)]
struct RealConnection {
    transport: WebSocketTransport,
    inbox: BoundedInbox,
}

impl ConnectionFactory for RealConnectionFactory {
    fn connect(&mut self, url: &str) -> Result<Box<dyn AppConnection>, TransportError> {
        let (transport, inbox) =
            WebSocketTransport::connect(url, Options::default(), CONTROL_INBOX_CAPACITY)?;
        Ok(Box::new(RealConnection { transport, inbox }))
    }
}

impl AppConnection for RealConnection {
    fn try_recv(&mut self) -> Option<WsEvent> {
        self.inbox.try_recv()
    }

    fn overflowed(&self) -> bool {
        self.inbox.overflowed()
    }

    fn send_frame(&mut self, frame: &ClientFrame) -> Result<(), TransportError> {
        self.transport.send_frame(frame)
    }

    fn close(&mut self) {
        self.transport.close();
    }
}

/// User-visible foundation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppView {
    /// The control connection is opening, authenticating, or bootstrapping.
    Connecting,
    /// Bootstrap data received over the authenticated control WebSocket.
    Ready {
        /// Identity of the embedded server instance.
        server_instance_id: String,
        /// Safe Kubernetes context names.
        context_names: Vec<String>,
        /// Safe context status used by the selector.
        contexts: Vec<Context>,
    },
    /// A safe connection or protocol failure.
    Failed {
        /// Credential-free error suitable for display.
        message: String,
    },
}

/// Minimal shared k10s application.
pub struct K10sApp {
    connection_url: String,
    access_token: String,
    client: ClientState,
    factory: Box<dyn ConnectionFactory>,
    connection: Option<Box<dyn AppConnection>>,
    /// Whether the current transport completed its WebSocket handshake.
    /// Sending on a still-connecting browser socket throws and ewebsock
    /// swallows the failure, silently dropping the frame; outbound frames
    /// therefore stay queued until [`WsEvent::Opened`] is observed.
    transport_open: bool,
    bootstrap: Option<PendingRequest>,
    bootstrap_subscription: Option<LiveSubscription>,
    infrastructure_request: Option<PendingRequest>,
    infrastructure_load: InfrastructureLoad,
    infrastructure_subscription: Option<LiveSubscription>,
    infrastructure_context: Option<String>,
    traffic_subscription: Option<LiveSubscription>,
    traffic_context: Option<String>,
    stream_sessions: BTreeMap<(WindowId, StreamRoute), StreamSession>,
    aggregate_log_sessions: BTreeMap<(WindowId, String, String, String, String), StreamSession>,
    pending_stream_tickets: BTreeMap<RequestId, PendingStreamTicket>,
    log_sources: BTreeMap<WindowId, LogSource>,
    log_session_sources: BTreeMap<WindowId, LogSource>,
    log_generations: BTreeMap<WindowId, u64>,
    log_session_generations: BTreeMap<WindowId, u64>,
    /// Canonical resource watches retained by visible workspace demand;
    /// rebuilt automatically on reconnect by shared client recovery.
    resource_subscriptions: BTreeMap<SubscriptionKey, RetainedSubscription>,
    /// Deterministic projection from every open list window to its shared
    /// canonical subscription.
    window_subscriptions: BTreeMap<WindowId, SubscriptionKey>,
    window_freshness_overrides: BTreeMap<WindowId, WindowFreshness>,
    /// Last authoritative rows retained when an individual watch is rejected.
    /// The owning window keeps useful read-only context until retry succeeds.
    window_retained_rows: BTreeMap<WindowId, Vec<ResourceListRow>>,
    window_last_sync_ms: BTreeMap<WindowId, u64>,
    rejected_subscription_keys: std::collections::BTreeSet<SubscriptionKey>,
    /// One cluster-scoped core/v1 Namespace watch shared by all namespaced
    /// list windows and kept warm for the connected context after first use.
    /// It is deliberately not represented as a fake window.
    namespace_subscription: Option<(String, LiveSubscription)>,
    namespace_catalog: NamespaceCatalogState,
    namespace_rejected_context: Option<String>,
    /// Authoritative session reconstruction requested after bootstrap or
    /// reconnect; events are subscribed before this list is issued.
    port_forward_list: Option<PendingRequest>,
    pending_port_forwards: Vec<PendingPortForward>,
    next_port_forward_issuance: u64,
    latest_focused_port_forward_issuance: Option<u64>,
    port_forward_retry_errors: PortForwardRetryErrors,
    port_forward_error: Option<String>,
    port_forward_switch_prompt: Option<String>,
    port_forward_switch_after_stop: Option<String>,
    /// Selectable resource types of the subscribed context (GVK picker).
    resource_types: Vec<ResourceTypeEntry>,
    /// The context whose types are cached or being fetched.
    types_context: Option<String>,
    /// In-flight `resource.types` request and the context it was issued for.
    types_request: Option<(String, PendingRequest)>,
    /// In-flight workload mutations awaiting their accepted operation.
    pending_mutations: BTreeMap<RequestId, PendingMutation>,
    /// Exact-target delete dry-runs awaiting authoritative server results.
    pending_delete_preflights: BTreeMap<RequestId, PendingDeletePreflight>,
    /// In-flight context switch awaiting the backend's verdict; local state
    /// moves only after it succeeds.
    pending_switch: Option<PendingSwitch>,
    /// The destination of the last failed switch, so a passive mismatch
    /// cannot retry-spam a broken context every frame.
    failed_switch: Option<String>,
    /// Backend-resolved details for every identity a window pinned, keyed by
    /// stable identity; rebuilt from `operation.status`-style queries.
    details: BTreeMap<ResourceIdentity, ResourceDetailResponse>,
    /// Revisioned exact-identity lifecycle retained independently for every
    /// authoritative resource source. Cross-source aggregation happens only
    /// when building the UI feed, never by last-writer arrival order.
    detail_lifecycles:
        std::collections::HashMap<k10s_protocol::SubscriptionId, DetailLifecycleSource>,
    primary_details: BTreeMap<ResourceIdentity, PrimaryDetailState>,
    /// In-flight detail requests per identity.
    detail_requests: BTreeMap<ResourceIdentity, PendingResourceRequest>,
    relations: BTreeMap<ResourceIdentity, RelationState>,
    relation_requests: BTreeMap<ResourceIdentity, PendingResourceRequest>,
    /// Last fresh metrics response for each currently pinned exact Pod.
    metrics: BTreeMap<ResourceIdentity, ResourceMetricsResponse>,
    /// Last completed or failed metrics check, used for TTL refresh/backoff.
    metric_checked_at: BTreeMap<ResourceIdentity, u64>,
    /// One in-flight metrics request per exact Pod identity across all windows.
    metric_requests: BTreeMap<ResourceIdentity, PendingResourceRequest>,
    resource_generation: u64,
    recovering: bool,
    view: AppView,
    shell: UiShell<ResourceIdentity>,
    external_shell_requests: Vec<crate::ui::ExternalShellTarget>,
    app_events: Vec<K10sAppEvent>,
    completed_bootstrap_once: bool,
    host_error: Option<SafeUiError>,
    /// Restorable window layouts not currently active, keyed by kube context.
    workspace_layouts: BTreeMap<String, WorkspaceSnapshot>,
    clock_started: Instant,
    jitter_counter: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K10sAppEvent {
    CommittedContextChanged { context: String },
    ControlConnectionReestablished { context: Option<String> },
}

/// A window's in-flight dedicated-stream ticket request.
#[derive(Debug)]
struct PendingStreamTicket {
    request: PendingRequest,
    route: StreamRoute,
    window: WindowId,
    log_source: Option<LogSource>,
    log_generation: Option<u64>,
    aggregate_target: Option<StreamTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogSource {
    target: StreamTarget,
    since_seconds: Option<i64>,
    previous: bool,
}

/// A window's in-flight workload mutation (scale or delete).
#[derive(Debug)]
struct PendingMutation {
    request: PendingRequest,
    window: WindowId,
}

#[derive(Debug)]
struct PendingDeletePreflight {
    request: PendingRequest,
    window: WindowId,
    target: ResourceIdentity,
    propagation: k10s_protocol::DeletePropagation,
}

/// A context switch sent to the backend whose response has not arrived.
#[derive(Debug)]
struct PendingSwitch {
    request: PendingRequest,
    to: String,
}

#[derive(Debug)]
struct PendingResourceRequest {
    request: PendingRequest,
    context: String,
    generation: u64,
}

/// Namespace part of a canonical watch identity. Cluster-scoped resources
/// deliberately do not reuse `AllNamespaces`: that is namespaced user intent,
/// while this variant is descriptor-derived wire behavior.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SubscriptionScope {
    Namespaced(crate::workspace::NamespaceScope),
    ClusterScoped,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubscriptionKey {
    context: String,
    gvk: k10s_protocol::GroupVersionKind,
    scope: SubscriptionScope,
    /// Effective namespace selector sent on the wire and included in the
    /// canonical identity used to share equivalent subscriptions.
    protocol_namespace: Option<String>,
    /// Exact pinned identity for a dedicated Detail watch; absent for Lists.
    identity: Option<ResourceIdentity>,
}

#[derive(Debug)]
struct RetainedSubscription {
    live: LiveSubscription,
    windows: std::collections::BTreeSet<WindowId>,
}

const DETAIL_LIFECYCLE_TOMBSTONE_CAP: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DetailLifecycleEvent {
    revision: k10s_protocol::BackendRevision,
    lifecycle: DetailLifecycle,
}

#[derive(Debug, Default)]
struct DetailLifecycleSource {
    snapshot_revision: Option<k10s_protocol::BackendRevision>,
    entries: BTreeMap<ResourceIdentity, DetailLifecycleEvent>,
}

#[derive(Debug)]
struct PendingPortForward {
    request: PendingRequest,
    intent: PendingPortForwardIntent,
    issuance: Option<u64>,
}

#[derive(Debug)]
enum PendingPortForwardIntent {
    StartModal(crate::ui::PortForwardModalGeneration),
    Retry(Box<k10s_protocol::PortForwardSession>),
    Stop,
}

/// Semantic Service detail projection for the web host.
#[derive(Debug, Clone, PartialEq)]
pub struct WebServiceDetail {
    /// `(label, value)` rows of the Overview panel, policies only when
    /// present.
    pub overview: Vec<(String, String)>,
    /// One structured read-only line per declared Service port.
    pub ports: Vec<String>,
}

impl std::fmt::Debug for K10sApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The access token is deliberately omitted: it never appears in
        // debug output, URLs, or logs.
        formatter
            .debug_struct("K10sApp")
            .field("connection_url", &self.connection_url)
            .field("access_token", &"[REDACTED]")
            .field("client", &self.client)
            .field("connection_active", &self.connection.is_some())
            .field("transport_open", &self.transport_open)
            .field("bootstrap", &self.bootstrap)
            .field("bootstrap_subscription", &self.bootstrap_subscription)
            .field("infrastructure_request", &self.infrastructure_request)
            .field(
                "infrastructure_subscription",
                &self.infrastructure_subscription,
            )
            .field("infrastructure_context", &self.infrastructure_context)
            .field("stream_sessions", &self.stream_sessions.len())
            .field("pending_stream_tickets", &self.pending_stream_tickets.len())
            .field("pending_switch", &self.pending_switch)
            .field("recovering", &self.recovering)
            .field("view", &self.view)
            .field("shell", &self.shell)
            .finish()
    }
}

impl K10sApp {
    /// Connect through the Task 5 transport and queue the protocol `Hello`.
    pub fn connect(target: ConnectTarget) -> Result<Self, TransportError> {
        Self::connect_with_factory(target, Box::new(RealConnectionFactory))
    }

    fn connect_with_factory(
        target: ConnectTarget,
        mut factory: Box<dyn ConnectionFactory>,
    ) -> Result<Self, TransportError> {
        let connection_url = target.url().to_owned();
        let target_token = target.access_token().to_owned();
        let mut client = ClientState::new(ClientConfig::default());
        client
            .connect(target)
            .map_err(|error| TransportError(error.to_string()))?;
        let connection = factory.connect(&connection_url)?;
        let access_token = target_token.clone();
        Ok(Self {
            connection_url,
            access_token,
            client,
            factory,
            connection: Some(connection),
            transport_open: false,
            bootstrap: None,
            bootstrap_subscription: None,
            infrastructure_request: None,
            infrastructure_load: InfrastructureLoad::Loading,
            infrastructure_subscription: None,
            infrastructure_context: None,
            traffic_subscription: None,
            traffic_context: None,
            stream_sessions: BTreeMap::new(),
            aggregate_log_sessions: BTreeMap::new(),
            pending_stream_tickets: BTreeMap::new(),
            log_sources: BTreeMap::new(),
            log_session_sources: BTreeMap::new(),
            log_generations: BTreeMap::new(),
            log_session_generations: BTreeMap::new(),
            resource_subscriptions: BTreeMap::new(),
            window_subscriptions: BTreeMap::new(),
            window_freshness_overrides: BTreeMap::new(),
            window_retained_rows: BTreeMap::new(),
            window_last_sync_ms: BTreeMap::new(),
            rejected_subscription_keys: std::collections::BTreeSet::new(),
            namespace_subscription: None,
            namespace_catalog: NamespaceCatalogState::NotDemanded,
            namespace_rejected_context: None,
            port_forward_list: None,
            pending_port_forwards: Vec::new(),
            next_port_forward_issuance: 1,
            latest_focused_port_forward_issuance: None,
            port_forward_retry_errors: PortForwardRetryErrors::default(),
            port_forward_error: None,
            port_forward_switch_prompt: None,
            port_forward_switch_after_stop: None,
            resource_types: Vec::new(),
            types_context: None,
            types_request: None,
            pending_mutations: BTreeMap::new(),
            pending_delete_preflights: BTreeMap::new(),
            pending_switch: None,
            failed_switch: None,
            details: BTreeMap::new(),
            detail_lifecycles: std::collections::HashMap::new(),
            primary_details: BTreeMap::new(),
            detail_requests: BTreeMap::new(),
            relations: BTreeMap::new(),
            relation_requests: BTreeMap::new(),
            metrics: BTreeMap::new(),
            metric_checked_at: BTreeMap::new(),
            metric_requests: BTreeMap::new(),
            resource_generation: 0,
            recovering: false,
            view: AppView::Connecting,
            shell: UiShell::new(),
            external_shell_requests: Vec::new(),
            app_events: Vec::new(),
            completed_bootstrap_once: false,
            host_error: None,
            workspace_layouts: BTreeMap::new(),
            clock_started: Instant::now(),
            jitter_counter: 0,
        })
    }

    /// Process all currently available transport events without blocking the UI thread.
    pub fn poll(&mut self) {
        let now_ms = u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.jitter_counter = self.jitter_counter.wrapping_add(1);
        let entropy = now_ms.rotate_left(17) ^ self.jitter_counter.wrapping_mul(0x9e37_79b9);
        self.poll_at(now_ms, entropy);
    }

    fn poll_at(&mut self, now_ms: u64, entropy: u64) {
        while let Some(event) = self
            .connection
            .as_mut()
            .and_then(|connection| connection.try_recv())
        {
            if matches!(event, WsEvent::Error(_) | WsEvent::Closed) {
                self.transient_loss(now_ms, entropy);
                break;
            }
            match self.handle_event(event, now_ms, entropy) {
                Ok(()) => {}
                Err(AppEventError::Transient) => {
                    self.transient_loss(now_ms, entropy);
                    break;
                }
                Err(AppEventError::Terminal(message)) => {
                    self.terminal_failure(message);
                    return;
                }
            }
        }
        if !Self::terminal_phase(self.client.phase())
            && self
                .connection
                .as_ref()
                .is_some_and(|connection| connection.overflowed())
        {
            self.transient_loss(now_ms, entropy);
        }
        self.reconnect_if_due(now_ms, entropy);
        self.poll_stream_sessions();
        self.finish_mutations();
    }

    /// Current user-visible state.
    #[must_use]
    pub fn view(&self) -> &AppView {
        &self.view
    }

    /// Persistent command-driven workspace rendered by the application shell.
    #[must_use]
    pub fn workspace(&self) -> &WorkspaceState<ResourceIdentity> {
        self.shell.workspace()
    }

    /// The persistable snapshot of the current workspace, for native hosts
    /// that restore sessions across restarts (see desktop state persistence).
    #[must_use]
    pub fn workspace_snapshot(&self) -> WorkspaceSnapshot {
        self.shell.workspace().snapshot()
    }

    /// All known layouts, including the currently rendered context.
    #[must_use]
    pub fn workspace_layouts(&self) -> BTreeMap<String, WorkspaceSnapshot> {
        let mut layouts = self.workspace_layouts.clone();
        let context = self.shell.workspace().context();
        if !context.is_empty() {
            layouts.insert(context.to_owned(), self.workspace_snapshot());
        }
        layouts
    }

    /// Install context-keyed layouts loaded by a native host. The matching
    /// layout is restored when bootstrap confirms or a switch enters it.
    pub fn restore_workspace_layouts(&mut self, layouts: BTreeMap<String, WorkspaceSnapshot>) {
        self.workspace_layouts = layouts;
    }

    fn commit_context_layout(&mut self, to: String) {
        let from = self.shell.workspace().context().to_owned();
        if from != to {
            self.revoke_external_shell();
            self.app_events.push(K10sAppEvent::CommittedContextChanged {
                context: to.clone(),
            });
        }
        if !from.is_empty() && from != to {
            self.workspace_layouts
                .insert(from, self.workspace_snapshot());
        }
        if let Some(snapshot) = self.workspace_layouts.get(&to).cloned() {
            self.shell
                .apply_workspace_command(WorkspaceCommand::RestoreSnapshot(snapshot));
        }
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::CommitContextSwitch { to })
        {
            self.handle_workspace_event(event);
        }
    }

    /// Restore a persisted workspace snapshot through the normal command
    /// path. Intended for native hosts reopening a session before rendering
    /// begins (desktop launch); mismatched or malformed snapshots leave the
    /// current workspace untouched.
    pub fn restore_workspace_snapshot(&mut self, snapshot: WorkspaceSnapshot) {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::RestoreSnapshot(snapshot))
        {
            self.handle_workspace_event(event);
        }
        self.reconcile_selected_resource_streams();
    }

    /// Whether the authenticated server negotiated Service port forwarding.
    #[must_use]
    pub fn port_forward_available(&self) -> bool {
        self.service_port_forward_available()
    }

    /// Whether the authenticated server negotiated Service port forwarding.
    #[must_use]
    pub fn service_port_forward_available(&self) -> bool {
        self.client.service_port_forward_available()
    }

    /// Whether the authenticated server negotiated Pod port forwarding.
    #[must_use]
    pub fn pod_port_forward_available(&self) -> bool {
        self.client.pod_port_forward_available()
    }

    /// Whether the authenticated server negotiated either port-forward target.
    #[must_use]
    pub fn any_port_forward_available(&self) -> bool {
        self.client.any_port_forward_available()
    }

    pub fn set_external_shell_availability(
        &mut self,
        availability: crate::ui::ExternalShellAvailability,
    ) {
        self.shell.set_external_shell_availability(availability);
        if matches!(
            availability,
            crate::ui::ExternalShellAvailability::Unavailable
        ) {
            self.external_shell_requests.clear();
        }
    }

    pub fn drain_external_shell_requests(&mut self) -> Vec<crate::ui::ExternalShellTarget> {
        std::mem::take(&mut self.external_shell_requests)
    }

    pub fn drain_app_events(&mut self) -> Vec<K10sAppEvent> {
        std::mem::take(&mut self.app_events)
    }

    pub fn set_host_error(&mut self, error: SafeUiError) {
        self.host_error = Some(error);
    }

    pub fn clear_host_error(&mut self) {
        self.host_error = None;
    }

    #[must_use]
    pub fn host_error(&self) -> Option<&SafeUiError> {
        self.host_error.as_ref()
    }

    fn revoke_external_shell(&mut self) {
        self.shell
            .set_external_shell_availability(crate::ui::ExternalShellAvailability::Unavailable);
        self.external_shell_requests.clear();
    }

    /// Current authoritative session snapshots used by native hosts/tests.
    #[must_use]
    pub fn port_forward_sessions(&self) -> Vec<&k10s_protocol::PortForwardSession> {
        self.client.port_forward_sessions()
    }

    /// Application-owned presentation error for a retained failed session.
    /// This never mutates or replaces the authoritative session snapshot.
    #[must_use]
    pub fn port_forward_retry_error(
        &self,
        session_id: &k10s_protocol::PortForwardSessionId,
    ) -> Option<&str> {
        self.port_forward_retry_errors.get(session_id)
    }

    /// Render the approved default-egui shell for the current connection view.
    pub fn render_ui(&mut self, ui: &mut egui::Ui) {
        if let Some(error) = &self.host_error {
            ui.colored_label(egui::Color32::from_rgb(190, 55, 55), error.message());
        }
        self.finish_port_forward_list();
        self.finish_port_forward_requests();
        let sessions = self
            .client
            .port_forward_sessions()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        self.port_forward_retry_errors.reconcile(&sessions);
        let (connection, contexts): (ShellConnectionState, &[Context]) = match &self.view {
            AppView::Connecting => (ShellConnectionState::Connecting, &[]),
            AppView::Ready { contexts, .. } => {
                (ShellConnectionState::Connected, contexts.as_slice())
            }
            AppView::Failed { .. } => (ShellConnectionState::Failed, &[]),
        };
        let selected_before = self.client.local_ui().selected_context.clone();
        let response = selected_before
            .as_deref()
            .and_then(|context| self.client.infrastructure(context))
            .cloned();
        self.shell.set_traffic_history(
            selected_before
                .as_deref()
                .and_then(|context| self.client.traffic(context))
                .into_iter()
                .flatten()
                .cloned(),
        );
        if let Some((sub_ctx, subscription)) = &self.namespace_subscription
            && Some(sub_ctx.as_str()) == selected_before.as_deref()
            && matches!(self.namespace_catalog, NamespaceCatalogState::Loading)
            && let Some(state) = self.client.resource_list(subscription.id())
        {
            let mut names: Vec<_> = state.rows().map(|row| row.identity.name.clone()).collect();
            names.sort();
            names.dedup();
            self.namespace_catalog = NamespaceCatalogState::Ready(names);
        }
        if let NamespaceCatalogState::Ready(names) = &self.namespace_catalog
            && let Some(first) = names.first()
        {
            self.shell.resolve_context_default_namespaces(first);
        }
        let mut feed = self.build_resource_feed();
        feed.render_time = response
            .as_ref()
            .and_then(|snapshot| crate::ui::system_time_from_rfc3339(&snapshot.generated_at))
            .or_else(|| Some(web_time::SystemTime::now()));
        let refresh = self.shell.show_with_contexts_and_resources_load(
            ui,
            connection,
            contexts,
            &mut self.client.local_ui_mut().selected_context,
            response.as_ref(),
            &feed,
            self.infrastructure_load,
        );
        for action in self.shell.drain_resource_actions() {
            self.handle_resource_action(action);
        }
        let resource_now_ms =
            u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.refresh_details_at(resource_now_ms);
        if let Some(context) = selected_before.as_deref()
            && let Err(error) = self.reconcile_resource_streams(context)
        {
            self.terminal_failure(error.to_string());
            return;
        }
        for action in self.shell.drain_port_forward_actions() {
            self.process_port_forward_action(ui.ctx(), action);
        }
        let selected_after = self.client.local_ui().selected_context.clone();
        let mut confirm_switch = false;
        let mut cancel_switch = false;
        if let Some(to) = self.port_forward_switch_prompt.as_deref() {
            egui::Window::new("Active port forwards")
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "{} active port-forward session(s) must stop before switching to {to}",
                        self.client
                            .port_forward_sessions()
                            .into_iter()
                            .filter(|session| matches!(
                                session.state,
                                k10s_protocol::PortForwardSessionState::Starting
                                    | k10s_protocol::PortForwardSessionState::Active
                                    | k10s_protocol::PortForwardSessionState::Stopping
                            ))
                            .count()
                    ));
                    if ui.button("Stop all and switch").clicked() {
                        confirm_switch = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel_switch = true;
                    }
                });
        }
        if cancel_switch {
            self.port_forward_switch_prompt = None;
        } else if confirm_switch && let Some(to) = self.port_forward_switch_prompt.take() {
            let session_ids: Vec<_> = self
                .client
                .port_forward_sessions()
                .into_iter()
                .filter(|session| {
                    matches!(
                        session.state,
                        k10s_protocol::PortForwardSessionState::Starting
                            | k10s_protocol::PortForwardSessionState::Active
                    )
                })
                .map(|session| session.id.clone())
                .collect();
            for session_id in session_ids {
                if let Ok(request) = self.client.begin(Query::PortForwardStop(session_id)) {
                    self.pending_port_forwards.push(PendingPortForward {
                        request,
                        intent: PendingPortForwardIntent::Stop,
                        issuance: None,
                    });
                }
            }
            self.port_forward_switch_after_stop = Some(to);
        }
        if self.port_forward_switch_after_stop.is_some()
            && !self
                .client
                .port_forward_sessions()
                .into_iter()
                .any(|session| {
                    matches!(
                        session.state,
                        k10s_protocol::PortForwardSessionState::Starting
                            | k10s_protocol::PortForwardSessionState::Active
                            | k10s_protocol::PortForwardSessionState::Stopping
                    )
                })
            && let Some(to) = self.port_forward_switch_after_stop.take()
            && let Err(error) = self.stage_context_switch(&to, true)
        {
            self.terminal_failure(error.to_string());
            return;
        }
        let retry_requested = refresh && connection != ShellConnectionState::Connected;
        let request_result = if retry_requested {
            let now_ms =
                u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.jitter_counter = self.jitter_counter.wrapping_add(1);
            let entropy = now_ms.rotate_left(17) ^ self.jitter_counter.wrapping_mul(0x9e37_79b9);
            self.retry_now(now_ms, entropy)
        } else if let Some((to, origin)) = self.shell.take_requested_context() {
            // A requested switch never moves local state here: it is sent to
            // the backend, and the workspace transition plus resubscriptions
            // happen only when the response confirms the destination.
            let active = self
                .client
                .port_forward_sessions()
                .into_iter()
                .any(|session| {
                    matches!(
                        session.state,
                        k10s_protocol::PortForwardSessionState::Starting
                            | k10s_protocol::PortForwardSessionState::Active
                            | k10s_protocol::PortForwardSessionState::Stopping
                    )
                });
            if active {
                self.port_forward_switch_prompt = Some(to);
                Ok(())
            } else {
                self.stage_context_switch(&to, origin.is_explicit())
            }
        } else if refresh {
            let bootstrap = if self.bootstrap.is_none() {
                self.client.begin(Query::Bootstrap).map(|request| {
                    self.bootstrap = Some(request);
                })
            } else {
                Ok(())
            };
            bootstrap.and_then(|()| {
                selected_after.as_deref().map_or(Ok(()), |context| {
                    if self.infrastructure_demanded() {
                        self.refresh_infrastructure(context)
                    } else {
                        Ok(())
                    }
                })
            })
        } else {
            Ok(())
        };
        let request_result = if retry_requested {
            request_result
        } else {
            request_result.and_then(|()| {
                self.flush_outbound()
                    .map_err(|error| ClientError::Protocol(format!("{error:?}")))
            })
        };
        let request_result = request_result.and_then(|()| {
            self.process_stream_requests()
                .map_err(|error| ClientError::Protocol(format!("{error:?}")))
        });
        let request_result = request_result.and_then(|()| {
            self.process_dialog_actions()
                .map_err(|error| ClientError::Protocol(format!("{error:?}")))
        });
        if let Err(error) = request_result {
            self.terminal_failure(error.to_string());
        }
    }

    fn process_port_forward_action(
        &mut self,
        ctx: &egui::Context,
        action: crate::ui::PortForwardAction,
    ) {
        self.port_forward_error = None;
        match action {
            crate::ui::PortForwardAction::OpenStart {
                target,
                remote_label,
                initial_local_port,
            } => {
                self.shell
                    .open_port_forward_start(target, remote_label, initial_local_port);
            }
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            } => {
                if let Err(reason) =
                    port_forward_start_authorization(&self.build_resource_feed(), request.target())
                {
                    self.shell.port_forward_start_failed_for(generation, reason);
                    return;
                }
                match self.client.begin(Query::PortForwardStart(request)) {
                    Ok(request) => {
                        let issuance = self.allocate_port_forward_issuance();
                        self.pending_port_forwards.push(PendingPortForward {
                            request,
                            intent: PendingPortForwardIntent::StartModal(generation),
                            issuance: Some(issuance),
                        });
                    }
                    Err(error) => self
                        .shell
                        .port_forward_start_failed(safe_port_forward_client_error(&error)),
                }
            }
            crate::ui::PortForwardAction::Stop(id) => {
                if let Ok(id) = k10s_protocol::PortForwardSessionId::try_new(id)
                    && let Ok(request) = self.client.begin(Query::PortForwardStop(id))
                {
                    self.pending_port_forwards.push(PendingPortForward {
                        request,
                        intent: PendingPortForwardIntent::Stop,
                        issuance: None,
                    });
                }
            }
            crate::ui::PortForwardAction::Retry(id) => {
                let source = self
                    .client
                    .port_forward_sessions()
                    .into_iter()
                    .find(|session| session.id == id)
                    .cloned();
                if let Some(source) = source {
                    match retry_start_request(&source) {
                        Ok(start) => match self.client.begin(Query::PortForwardStart(start)) {
                            Ok(request) => {
                                let issuance = self.allocate_port_forward_issuance();
                                self.pending_port_forwards.push(PendingPortForward {
                                    request,
                                    intent: PendingPortForwardIntent::Retry(Box::new(source)),
                                    issuance: Some(issuance),
                                });
                            }
                            Err(error) => self
                                .port_forward_retry_errors
                                .record(&source, safe_port_forward_client_error(&error)),
                        },
                        Err(message) => self.port_forward_retry_errors.record(&source, message),
                    }
                }
            }
            crate::ui::PortForwardAction::FocusSession(id) => {
                self.shell.focus_port_forward_session(id.as_str());
            }
            crate::ui::PortForwardAction::CopyAddress(address) => ctx.copy_text(address),
        }
    }

    /// Credential-free endpoint used by the shared transport.
    #[must_use]
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Whether authentication was terminally rejected and the web host must show its gate.
    #[must_use]
    pub fn requires_connection_gate(&self) -> bool {
        self.client.phase() == ClientPhase::WebGate
    }

    /// Render the minimal foundation view as text.
    #[must_use]
    pub fn render_text(&self) -> String {
        match &self.view {
            AppView::Connecting => "Connecting".to_owned(),
            AppView::Ready {
                server_instance_id,
                context_names,
                ..
            } => format!(
                "Server {server_instance_id}\nContexts: {}",
                context_names.join(", ")
            ),
            AppView::Failed { message } => format!("Connection failed: {message}"),
        }
    }

    /// Resource rows exposed to the semantic web host. They are the same
    /// authoritative watch projection rendered by the egui resource table.
    #[must_use]
    pub fn web_resource_rows(&self, kind: WorkloadKind) -> Vec<k10s_protocol::ResourceListRow> {
        let Some(window) = self
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == crate::workspace::WindowKind::Workload(kind))
            .max_by_key(|window| window.z)
            .map(|window| window.id)
        else {
            return Vec::new();
        };
        self.build_resource_feed()
            .window_lists
            .remove(&window)
            .unwrap_or_default()
    }

    /// Open/focus a workload through the shared command-driven workspace.
    pub fn web_activate_workload(&mut self, kind: WorkloadKind) -> Option<WindowId> {
        let events = self
            .shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                crate::workspace::LauncherItem::Workload(kind),
            ));
        for event in events {
            self.handle_workspace_event(event);
        }
        self.reconcile_selected_resource_streams();
        self.shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == crate::workspace::WindowKind::Workload(kind))
            .max_by_key(|window| window.z)
            .map(|window| window.id)
    }

    /// Change one open namespaced workload window through the same command
    /// path used by native controls, then reconcile its live watch.
    pub fn web_set_namespace_scope(
        &mut self,
        window: WindowId,
        scope: crate::workspace::NamespaceScope,
    ) {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(window, scope))
        {
            self.handle_workspace_event(event);
        }
        self.reconcile_selected_resource_streams();
    }

    /// Open/focus the singleton Services window through the shared
    /// command-driven workspace.
    pub fn web_activate_services(&mut self) -> Option<WindowId> {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                crate::workspace::LauncherItem::Services,
            ))
        {
            self.handle_workspace_event(event);
        }
        self.reconcile_selected_resource_streams();
        self.shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == crate::workspace::WindowKind::Services)
            .max_by_key(|window| window.z)
            .map(|window| window.id)
    }

    /// Service rows exposed to the semantic web host as
    /// `(uid, name, namespace, type label, ports label)` tuples, computed
    /// from the same normalized projections the native table renders. The
    /// uid lets row clicks pin the exact identity.
    #[must_use]
    pub fn web_service_rows(&self) -> Vec<(String, String, String, String, String)> {
        let Some(window) = self
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == crate::workspace::WindowKind::Services)
            .map(|window| window.id)
        else {
            return Vec::new();
        };
        self.build_resource_feed()
            .window_services
            .remove(&window)
            .unwrap_or_default()
            .into_iter()
            .map(|row| {
                let (type_label, ports_label) = match &row.projection {
                    Some(k10s_protocol::ResourceProjection::Service(projection)) => (
                        projection.service_type.clone(),
                        crate::ui::ports_column_label(projection),
                    ),
                    _ => ("—".to_owned(), "—".to_owned()),
                };
                (
                    row.identity.uid,
                    row.identity.name,
                    row.identity.namespace.unwrap_or_else(|| "—".to_owned()),
                    type_label,
                    ports_label,
                )
            })
            .collect()
    }

    /// Pin an exact projected row in a web-hosted workload window.
    pub fn web_select_resource(&mut self, window: WindowId, identity: ResourceIdentity) {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, identity))
        {
            self.handle_workspace_event(event);
        }
        self.refresh_details();
    }

    /// Select a Service row of the singleton Services window by its uid,
    /// mirroring [`Self::web_select_resource`] for the web host, which has
    /// no protocol types of its own.
    pub fn web_select_service(&mut self, window: WindowId, uid: &str) {
        let Some(identity) = self
            .build_resource_feed()
            .window_services
            .remove(&window)
            .unwrap_or_default()
            .into_iter()
            .find(|row| row.identity.uid == uid)
            .map(|row| row.identity)
        else {
            return;
        };
        self.web_select_resource(window, identity);
    }

    /// Semantic Service detail for the web host: `(label, value)` Overview
    /// rows plus one structured line per declared port, computed only from
    /// the normalized projection once the backend response arrived. `None`
    /// while unresolved or when no projection is carried. No Start/Stop
    /// controls exist anywhere in this surface.
    #[must_use]
    pub fn web_service_detail(&self, window: WindowId) -> Option<WebServiceDetail> {
        let (_, view) = self.web_selected_detail(window)?;
        let view = view?;
        let Some(k10s_protocol::ResourceProjection::Service(projection)) = &view.projection else {
            return None;
        };
        let mut overview = vec![
            ("Type".to_owned(), projection.service_type.clone()),
            (
                "Cluster IPs".to_owned(),
                if projection.cluster_ips.is_empty() {
                    "—".to_owned()
                } else {
                    projection.cluster_ips.join(", ")
                },
            ),
        ];
        if !projection.selector.is_empty() {
            let selector = projection
                .selector
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ");
            overview.push(("Selector".to_owned(), selector));
        }
        for (label, value) in [
            ("External name", &projection.external_name),
            ("Session affinity", &projection.session_affinity),
            (
                "External traffic policy",
                &projection.external_traffic_policy,
            ),
            (
                "Internal traffic policy",
                &projection.internal_traffic_policy,
            ),
        ] {
            if let Some(value) = value {
                overview.push((label.to_owned(), value.clone()));
            }
        }
        let ports = projection
            .ports
            .iter()
            .map(crate::ui::port_detail_label)
            .collect();
        Some(WebServiceDetail { overview, ports })
    }

    /// Select a detail tab through the same workspace state as native egui.
    pub fn web_set_detail_tab(&mut self, window: WindowId, tab: crate::workspace::DetailTab) {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(window, tab))
        {
            self.handle_workspace_event(event);
        }
        self.refresh_details();
    }

    /// Resolve the currently pinned identity and backend detail for a window.
    #[must_use]
    pub fn web_selected_detail(
        &self,
        window: WindowId,
    ) -> Option<(&ResourceIdentity, Option<&ResourceDetailResponse>)> {
        let identity =
            self.shell
                .workspace()
                .window(window)
                .and_then(|window| match &window.content {
                    crate::workspace::WindowContent::Detail(detail) => Some(&detail.identity),
                    crate::workspace::WindowContent::Resource(resource) => {
                        resource.detail.as_ref().map(|detail| &detail.identity)
                    }
                    crate::workspace::WindowContent::Services(service) => {
                        service.detail.as_ref().map(|detail| &detail.identity)
                    }
                    crate::workspace::WindowContent::PortForwards(_) => None,
                })?;
        Some((identity, self.details.get(identity)))
    }

    /// Open the real shared scale dialog for the selected resource.
    pub fn web_open_scale_dialog(&mut self, window: WindowId) {
        if let Some(identity) = self
            .web_selected_detail(window)
            .map(|(identity, _)| identity.clone())
        {
            self.shell
                .dialogs_mut()
                .open_scale(window, identity, Some(1));
        }
    }

    /// Open the real shared destructive confirmation for the selected resource.
    pub fn web_open_delete_dialog(&mut self, window: WindowId) {
        if let Some(identity) = self
            .web_selected_detail(window)
            .map(|(identity, _)| identity.clone())
        {
            self.shell.dialogs_mut().open_delete(window, identity);
        }
    }

    /// Current shared operation dialog kind, if one is open.
    #[must_use]
    pub fn web_dialog_kind(
        &self,
        window: WindowId,
    ) -> Option<crate::ui::dialogs::ActiveDialogKind> {
        self.shell.dialogs().active(window)
    }

    /// Request the selected Pod's real bounded logs stream.
    pub fn web_connect_logs(&mut self, window: WindowId) -> Result<(), ClientError> {
        let target = self
            .current_stream_target(window, StreamRoute::Logs)
            .ok_or_else(|| {
                ClientError::Protocol("selected resource cannot stream logs".to_owned())
            })?;
        let stores = self.shell.stream_stores_mut();
        stores.logs.ensure(window, target.clone()).connect();
        stores.logs.queue(
            window,
            crate::ui::tools::LogsAction::OpenLogs {
                window,
                target,
                since_seconds: Some(300),
                previous: false,
            },
        );
        self.process_stream_requests()
    }

    /// Semantic stream state rendered by the web adapter.
    #[must_use]
    pub fn web_stream_text(&self, window: WindowId) -> (String, Vec<String>) {
        let logs = self.shell.stream_stores().logs.get(window);
        let log_phase = logs
            .map(|logs| format!("{:?}", logs.phase()))
            .unwrap_or_else(|| "Disconnected".to_owned());
        let log_lines = logs
            .map(|logs| logs.visible_lines().cloned().collect())
            .unwrap_or_default();
        (log_phase, log_lines)
    }

    /// Intentionally recycle the control transport. This is both a useful
    /// operator action and a deterministic browser-E2E entry into the normal
    /// full-jitter reconnect/full-resync correctness path.
    pub fn web_reconnect(&mut self) {
        let now_ms = u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.jitter_counter = self.jitter_counter.wrapping_add(1);
        let entropy = now_ms.rotate_left(17) ^ self.jitter_counter.wrapping_mul(0x9e37_79b9);
        self.transient_loss(now_ms, entropy);
    }

    fn handle_event(
        &mut self,
        event: WsEvent,
        now_ms: u64,
        entropy: u64,
    ) -> Result<(), AppEventError> {
        match event {
            WsEvent::Opened => {
                self.transport_open = true;
                self.flush_outbound()
            }
            WsEvent::Message(WsMessage::Text(text)) => {
                let frame: ServerFrame = serde_json::from_str(&text).map_err(|error| {
                    AppEventError::Terminal(format!("could not decode server frame: {error}"))
                })?;
                let resource_delta = resource_delta_projection(&frame);
                let completed_resource_snapshot = (frame.kind
                    == k10s_protocol::ServerKind::SnapshotEnd)
                    .then(|| frame.subscription_id.clone())
                    .flatten()
                    .map(|subscription| {
                        let previous = self
                            .client
                            .resource_list(&subscription)
                            .map(|state| state.rows().map(|row| row.identity.clone()).collect())
                            .unwrap_or_default();
                        (subscription, previous)
                    });
                let replacement_snapshot_started = frame.kind
                    == k10s_protocol::ServerKind::SnapshotBegin
                    && frame.subscription_id.as_ref().is_some_and(|subscription| {
                        self.client
                            .resource_list(subscription)
                            .and_then(|state| state.revision())
                            .is_some()
                    });
                let server_rebuild_requested =
                    frame.kind == k10s_protocol::ServerKind::ResyncRequired;
                let stream_request_id = frame.request_id.clone();
                let stream_subscription_id = frame.subscription_id.clone();
                let apply_result = self.client.apply_at(frame, now_ms, entropy);
                let applied = apply_result.is_ok();
                if let Err(error) = apply_result {
                    let context_unavailable = match &error {
                        ClientError::Server(server_error) => {
                            server_error.details.as_ref().and_then(|details| {
                                if details.get("kind")?.as_str()? != "contextUnavailable" {
                                    return None;
                                }
                                Some((
                                    details.get("context")?.as_str()?.to_owned(),
                                    details
                                        .get("reason")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("credential plugin is unavailable")
                                        .to_owned(),
                                ))
                            })
                        }
                        _ => None,
                    };
                    let pending_destination = stream_request_id.as_ref().and_then(|id| {
                        self.pending_switch.as_ref().and_then(|pending| {
                            (pending.request.id() == id).then(|| pending.to.clone())
                        })
                    });
                    let context_unavailable = context_unavailable.filter(|(context, _)| {
                        pending_destination
                            .as_ref()
                            .is_none_or(|destination| destination == context)
                    });
                    let recovering_bootstrap_status_transition = context_unavailable.is_some()
                        && matches!(
                            &error,
                            ClientError::Server(server_error)
                                if server_error.scope == k10s_protocol::ErrorScope::Subscription
                                    && server_error.retryability
                                        == k10s_protocol::Retryability::AfterRefresh
                        )
                        && stream_subscription_id.as_ref().is_some_and(|id| {
                            self.bootstrap_subscription
                                .as_ref()
                                .is_some_and(|subscription| subscription.id() == id)
                        })
                        && (self.recovering || matches!(self.view, AppView::Connecting));
                    let reconciled_context_unavailable =
                        if let Some((context_name, reason)) = context_unavailable {
                            let found = if let AppView::Ready { contexts, .. } = &mut self.view {
                                contexts
                                    .iter_mut()
                                    .find(|context| context.name == context_name)
                                    .map(|context| {
                                        context.availability = ContextAvailability::Unavailable;
                                        context.unavailable_reason = Some(reason);
                                    })
                                    .is_some()
                            } else {
                                false
                            };
                            if found && self.bootstrap.is_none() {
                                self.bootstrap =
                                    Some(self.client.begin(Query::Bootstrap).map_err(|error| {
                                        AppEventError::Terminal(error.to_string())
                                    })?);
                            }
                            found
                        } else {
                            false
                        };
                    match error {
                        ClientError::SequenceGap { .. } => {
                            self.shell.yaml_editors_mut().connection_lost();
                            self.reset_port_forward_ui(Some("Server state changed; submit again"));
                            self.enter_resource_recovery();
                            self.recovering = true;
                            self.bootstrap = self.client.take_rebuilt_bootstrap();
                            self.view = AppView::Connecting;
                        }
                        ClientError::Server(ref server_error)
                            if server_error.scope == k10s_protocol::ErrorScope::Subscription
                                && stream_subscription_id.as_ref().is_some_and(|id| {
                                    self.namespace_subscription
                                        .as_ref()
                                        .is_some_and(|(_, subscription)| subscription.id() == id)
                                }) =>
                        {
                            let (context, subscription) = self
                                .namespace_subscription
                                .take()
                                .expect("matched namespace subscription exists");
                            self.retire_detail_lifecycle_source(subscription.id());
                            self.client.retire_rejected_subscription(&subscription);
                            self.namespace_catalog = NamespaceCatalogState::Unavailable(
                                SafeUiError::new(server_error.safe_message.clone()),
                            );
                            self.namespace_rejected_context = Some(context);
                        }
                        ClientError::Server(ref server_error)
                            if server_error.scope == k10s_protocol::ErrorScope::Subscription
                                && stream_subscription_id.as_ref().is_some_and(|id| {
                                    self.resource_subscriptions
                                        .values()
                                        .any(|entry| entry.live.id() == id)
                                }) =>
                        {
                            let id = stream_subscription_id
                                .as_ref()
                                .expect("matched subscription");
                            let key = self
                                .resource_subscriptions
                                .iter()
                                .find_map(|(key, entry)| {
                                    (entry.live.id() == id).then(|| key.clone())
                                })
                                .expect("matched subscription key");
                            let entry = self
                                .resource_subscriptions
                                .remove(&key)
                                .expect("matched subscription entry");
                            self.retire_detail_lifecycle_source(entry.live.id());
                            self.rejected_subscription_keys.insert(key.clone());
                            let retained_rows = self
                                .client
                                .resource_list(entry.live.id())
                                .map(|state| state.rows().cloned().collect::<Vec<_>>())
                                .unwrap_or_default();
                            self.client.retire_rejected_subscription(&entry.live);
                            let details = server_error.details.as_ref();
                            for window in entry.windows {
                                self.window_retained_rows
                                    .insert(window, retained_rows.clone());
                                let state = if server_error.code
                                    == k10s_protocol::ErrorCode::Unauthorized
                                {
                                    WindowFreshness::Forbidden {
                                        user: details
                                            .and_then(|v| v.get("user"))
                                            .and_then(serde_json::Value::as_str)
                                            .unwrap_or("current user")
                                            .to_owned(),
                                        verb: details
                                            .and_then(|v| v.get("verb"))
                                            .and_then(serde_json::Value::as_str)
                                            .unwrap_or("list")
                                            .to_owned(),
                                        resource: details
                                            .and_then(|v| v.get("resource"))
                                            .and_then(serde_json::Value::as_str)
                                            .unwrap_or(key.gvk.kind.as_str())
                                            .to_owned(),
                                        scope: details
                                            .and_then(|v| v.get("scope"))
                                            .and_then(serde_json::Value::as_str)
                                            .unwrap_or("--all-namespaces")
                                            .to_owned(),
                                    }
                                } else {
                                    WindowFreshness::Failed {
                                        message: server_error.safe_message.clone(),
                                    }
                                };
                                self.window_freshness_overrides.insert(window, state);
                            }
                        }
                        // A request-scoped mutation denial is projected into
                        // the originating dialog for a corrected retry; it
                        // never kills the control connection.
                        ClientError::Server(ref server_error)
                            if stream_request_id
                                .as_ref()
                                .is_some_and(|id| self.pending_mutations.contains_key(id)) =>
                        {
                            if let Some(entry) = stream_request_id
                                .as_ref()
                                .and_then(|id| self.pending_mutations.remove(id))
                                && let Some(mut dialog) =
                                    self.shell.dialogs_mut().active_mut(entry.window)
                            {
                                dialog.operation_failed(server_error.safe_message.clone());
                            }
                        }
                        ClientError::Server(ref server_error)
                            if stream_request_id.as_ref().is_some_and(|id| {
                                self.pending_port_forwards
                                    .iter()
                                    .any(|pending| pending.request.id() == id)
                            }) =>
                        {
                            let pending = stream_request_id.as_ref().and_then(|id| {
                                self.pending_port_forwards
                                    .iter()
                                    .position(|pending| pending.request.id() == id)
                                    .map(|index| self.pending_port_forwards.remove(index))
                            });
                            if let Some(pending) = pending {
                                match pending.intent {
                                    PendingPortForwardIntent::StartModal(generation) => {
                                        self.shell.port_forward_start_failed_for(
                                            generation,
                                            server_error.safe_message.clone(),
                                        )
                                    }
                                    PendingPortForwardIntent::Retry(source)
                                        if is_local_port_conflict(server_error) =>
                                    {
                                        self.port_forward_retry_errors.local_port_conflict(&source);
                                    }
                                    PendingPortForwardIntent::Retry(source) => {
                                        self.port_forward_retry_errors
                                            .record(&source, server_error.safe_message.clone());
                                    }
                                    PendingPortForwardIntent::Stop => {
                                        self.port_forward_error =
                                            Some(server_error.safe_message.clone());
                                    }
                                }
                            }
                        }
                        // A vanished object's detail query is dropped
                        // quietly: the pinned window keeps rendering its
                        // loading state until a newer selection arrives.
                        ClientError::Server(ref server_error)
                            if stream_request_id.as_ref().is_some_and(|id| {
                                self.detail_requests
                                    .values()
                                    .any(|pending| pending.request.id() == id)
                            }) =>
                        {
                            if let Some(id) = stream_request_id.as_ref()
                                && let Some(identity) = self
                                    .detail_requests
                                    .iter()
                                    .find(|(_, pending)| pending.request.id() == id)
                                    .map(|(identity, _)| identity.clone())
                            {
                                if let Some(pending) = self.detail_requests.remove(&identity) {
                                    let _ = self.client.take_failure(pending.request);
                                }
                                self.details.remove(&identity);
                                if self.is_resource_pinned(&identity) {
                                    self.primary_details.insert(
                                        identity,
                                        PrimaryDetailState::Failed(SafeUiError::new(
                                            server_error.safe_message.clone(),
                                        )),
                                    );
                                }
                            }
                        }
                        ClientError::Server(ref server_error)
                            if stream_request_id.as_ref().is_some_and(|id| {
                                self.relation_requests
                                    .values()
                                    .any(|pending| pending.request.id() == id)
                            }) =>
                        {
                            if let Some(id) = stream_request_id.as_ref()
                                && let Some(identity) = self
                                    .relation_requests
                                    .iter()
                                    .find(|(_, pending)| pending.request.id() == id)
                                    .map(|(identity, _)| identity.clone())
                            {
                                if let Some(pending) = self.relation_requests.remove(&identity) {
                                    let _ = self.client.take_failure(pending.request);
                                }
                                if self.is_resource_pinned(&identity) {
                                    let error = SafeUiError::new(server_error.safe_message.clone());
                                    match self.relations.get_mut(&identity) {
                                        Some(RelationState::Loaded {
                                            refreshing,
                                            refresh_error,
                                            ..
                                        }) => {
                                            *refreshing = false;
                                            *refresh_error = Some(error);
                                        }
                                        _ => {
                                            self.relations
                                                .insert(identity, RelationState::Failed(error));
                                        }
                                    }
                                } else {
                                    self.relations.remove(&identity);
                                }
                            }
                        }
                        ClientError::Server(ref server_error)
                            if server_error.retryability
                                != k10s_protocol::Retryability::AfterReconnect
                                && stream_request_id.as_ref().is_some_and(|id| {
                                    self.metric_requests
                                        .values()
                                        .any(|pending| pending.request.id() == id)
                                }) =>
                        {
                            if let Some(id) = stream_request_id.as_ref()
                                && let Some(identity) = self
                                    .metric_requests
                                    .iter()
                                    .find(|(_, pending)| pending.request.id() == id)
                                    .map(|(identity, _)| identity.clone())
                            {
                                if let Some(pending) = self.metric_requests.remove(&identity) {
                                    let _ = self.client.take_failure(pending.request);
                                }
                                self.metrics.remove(&identity);
                                if self.is_resource_pinned(&identity) {
                                    self.metric_checked_at.insert(identity, now_ms);
                                }
                            }
                        }
                        ClientError::Server(ref server_error)
                            if server_error.code
                                == k10s_protocol::ErrorCode::UnsupportedMessage
                                && server_error.scope == k10s_protocol::ErrorScope::Request
                                && server_error.retryability
                                    != k10s_protocol::Retryability::AfterReconnect
                                && stream_request_id.as_ref().is_some_and(|id| {
                                    self.infrastructure_request
                                        .as_ref()
                                        .is_some_and(|request| request.id() == id)
                                }) => {}
                        ClientError::Server(ref server_error)
                            if server_error.code
                                == k10s_protocol::ErrorCode::UnsupportedMessage
                                && server_error.scope
                                    == k10s_protocol::ErrorScope::Subscription
                                && server_error.retryability
                                    != k10s_protocol::Retryability::AfterReconnect
                                && stream_subscription_id.as_ref().is_some_and(|id| {
                                    self.infrastructure_subscription
                                        .as_ref()
                                        .is_some_and(|subscription| subscription.id() == id)
                                }) =>
                        {
                            if let Some(subscription) = self.infrastructure_subscription.take() {
                                let _ = self.client.retire_rejected_subscription(&subscription);
                            }
                            self.infrastructure_load = InfrastructureLoad::Unavailable;
                        }
                        // A request-scoped stream-ticket denial is projected
                        // into the requesting tool; it never kills the
                        // control connection or any other stream.
                        ClientError::Server(ref server_error)
                            if stream_request_id
                                .as_ref()
                                .is_some_and(|id| self.pending_stream_tickets.contains_key(id)) =>
                        {
                            if let Some(entry) = stream_request_id
                                .as_ref()
                                .and_then(|id| self.pending_stream_tickets.remove(id))
                            {
                                let reason = server_error.safe_message.clone();
                                let stale_log_attempt =
                                    entry.log_source.as_ref().is_some_and(|source| {
                                        self.log_sources.get(&entry.window) != Some(source)
                                            || entry.log_generation
                                                != self.log_generations.get(&entry.window).copied()
                                    });
                                if stale_log_attempt {
                                    return Ok(());
                                }
                                if entry.aggregate_target.is_some() {
                                    if self.aggregate_log_sources_exhausted(entry.window)
                                        && let Some(view) = self
                                            .shell
                                            .stream_stores_mut()
                                            .logs
                                            .get_mut(entry.window)
                                    {
                                        view.fail(&reason);
                                    }
                                    return Ok(());
                                }
                                if let Some(view) =
                                    self.shell.stream_stores_mut().logs.get_mut(entry.window)
                                {
                                    view.fail(&reason);
                                }
                            }
                        }
                        // A request-scoped switch rejection is projected into
                        // the switch flow itself: pending clears, the failed
                        // destination is recorded so passive reconciliation
                        // cannot retry-spam it, and selection, workspace, and
                        // the control connection all stay alive.
                        ClientError::Server(ref server_error)
                            if stream_request_id.as_ref().is_some_and(|id| {
                                self.pending_switch
                                    .as_ref()
                                    .is_some_and(|pending| pending.request.id() == id)
                            }) =>
                        {
                            if let Some(pending) = self.pending_switch.take() {
                                let failed = pending.to;
                                self.failed_switch = Some(failed.clone());
                            }
                        }
                        _ if reconciled_context_unavailable => {}
                        _ if recovering_bootstrap_status_transition => {
                            if self.bootstrap.is_none() {
                                self.bootstrap = self.client.take_rebuilt_bootstrap();
                            }
                            if self.bootstrap.is_none() {
                                self.bootstrap =
                                    Some(self.client.begin(Query::Bootstrap).map_err(|error| {
                                        AppEventError::Terminal(error.to_string())
                                    })?);
                            }
                        }
                        _ if self.client.phase() == ClientPhase::Disconnected => {
                            return Err(AppEventError::Transient);
                        }
                        _ => return Err(AppEventError::Terminal(error.to_string())),
                    }
                }
                if applied
                    && let Some((subscription, previous)) = completed_resource_snapshot
                    && let Some((revision, current)) =
                        self.client.resource_list(&subscription).and_then(|state| {
                            Some((
                                state.revision()?,
                                state.rows().map(|row| row.identity.clone()).collect(),
                            ))
                        })
                {
                    self.accept_detail_lifecycle_snapshot(
                        subscription,
                        revision,
                        previous,
                        current,
                    );
                }
                if applied
                    && let Some((subscription, identity, revision, lifecycle)) = resource_delta
                    && self
                        .client
                        .resource_list(&subscription)
                        .and_then(|state| state.revision())
                        == Some(revision)
                {
                    self.shell
                        .yaml_editors_mut()
                        .target_changed(&identity, revision);
                    self.record_detail_lifecycle(subscription, identity, revision, lifecycle);
                }
                if applied
                    && let Some(subscription) = stream_subscription_id.as_ref()
                    && self.client.resource_list(subscription).is_some()
                {
                    let windows: Vec<_> = self
                        .resource_subscriptions
                        .values()
                        .find(|entry| entry.live.id() == subscription)
                        .map(|entry| entry.windows.iter().copied().collect())
                        .unwrap_or_default();
                    for window in windows {
                        self.window_last_sync_ms.insert(window, now_ms);
                        self.window_freshness_overrides.remove(&window);
                        self.window_retained_rows.remove(&window);
                    }
                }
                if applied && (server_rebuild_requested || replacement_snapshot_started) {
                    // A replacement is assembled across chunks, so neither
                    // the identities in an early page nor identities removed
                    // from the new view can be projected safely page by page.
                    // Revoke server-issued authority at the accepted begin;
                    // connection_lost deliberately preserves dirty buffers.
                    self.shell.yaml_editors_mut().connection_lost();
                }
                if applied && server_rebuild_requested {
                    self.reset_port_forward_ui(Some("Server state changed; submit again"));
                    self.enter_resource_recovery();
                    self.recovering = true;
                    self.view = AppView::Connecting;
                }
                self.finish_infrastructure_request();
                self.finish_context_switch()?;
                if self.bootstrap.is_none() {
                    self.bootstrap = self.client.take_rebuilt_bootstrap();
                }
                if self.client.phase() == ClientPhase::Ready
                    && self.bootstrap.is_none()
                    && self.client.server_bootstrap().is_none()
                {
                    self.bootstrap = Some(
                        self.client
                            .begin(Query::Bootstrap)
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?,
                    );
                }
                if let Some(request) = self.bootstrap.clone()
                    && !self.client.is_pending(&request)
                    && let Some(QueryResult::Bootstrap(response)) = self.client.take(request)
                {
                    self.bootstrap = None;
                    let server_instance_id = response
                        .server
                        .ok_or_else(|| {
                            AppEventError::Terminal("bootstrap omitted server identity".to_owned())
                        })?
                        .instance_id;
                    if self.bootstrap_subscription.is_none() {
                        self.bootstrap_subscription = Some(
                            self.client
                                .subscribe_bootstrap_status()
                                .map_err(|error| AppEventError::Terminal(error.to_string()))?,
                        );
                    }
                    let context_names: Vec<String> = response
                        .contexts
                        .iter()
                        .map(|context| context.name.clone())
                        .collect();
                    let contexts = response.contexts;
                    // The server's `is_current` marker is the reconnect
                    // authority: a stale local selection must never outrank
                    // what the backend just reported as current. The local
                    // preference only breaks ties when no marker is carried.
                    let selected = contexts
                        .iter()
                        .find(|context| {
                            context.is_current
                                && context.availability != ContextAvailability::Unavailable
                        })
                        .map(|context| context.name.clone())
                        .or_else(|| {
                            self.client
                                .local_ui()
                                .selected_context
                                .clone()
                                .filter(|selected| {
                                    contexts.iter().any(|context| {
                                        context.name == *selected
                                            && context.availability
                                                != ContextAvailability::Unavailable
                                    })
                                })
                        })
                        .or_else(|| {
                            contexts
                                .iter()
                                .find(|context| {
                                    context.availability != ContextAvailability::Unavailable
                                })
                                .map(|context| context.name.clone())
                        });
                    self.client.local_ui_mut().selected_context = selected.clone();
                    let reestablished = self.completed_bootstrap_once;
                    self.completed_bootstrap_once = true;
                    self.view = AppView::Ready {
                        server_instance_id,
                        context_names,
                        contexts,
                    };
                    if reestablished {
                        self.app_events
                            .push(K10sAppEvent::ControlConnectionReestablished {
                                context: selected.clone(),
                            });
                    }
                    let reconstructing_port_forward_list = self.recovering;
                    self.recovering = false;
                    if self.client.any_port_forward_available() {
                        let _ = self
                            .client
                            .subscribe_port_forward_sessions()
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                        self.port_forward_list = Some(
                            if reconstructing_port_forward_list {
                                self.client.begin_port_forward_reconstruction()
                            } else {
                                self.client.begin(Query::PortForwardList)
                            }
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?,
                        );
                    } else {
                        self.port_forward_list = None;
                    }
                    if let Some(context) = selected {
                        // A switch left awaiting its answer from an older
                        // generation cannot outrank this fresh authority
                        // snapshot: retire it first (the client swallows any
                        // late answer to a cancelled request).
                        if let Some(pending) = self.pending_switch.take() {
                            let _ = self.client.cancel(&pending.request);
                        }
                        // The backend-confirmed context must land in the
                        // workspace immediately: streaming the new context
                        // while selections, detail state, and navigation
                        // guards still belong to the old one leaves the UI
                        // inconsistent until some later switch succeeds.
                        if self.shell.workspace().context() != context {
                            self.commit_context_layout(context.clone());
                        }
                        // The bootstrap answer already wrote the selection,
                        // so the render-time context-change path would skip
                        // it: reconcile the resource streams here.
                        self.reconcile_resource_streams(&context)
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                        self.select_infrastructure_context(&context)
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                        self.select_traffic_context(&context)
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                    }
                }
                self.flush_outbound()
            }
            WsEvent::Message(_) => Ok(()),
            WsEvent::Error(_) | WsEvent::Closed => Err(AppEventError::Transient),
        }
    }

    /// Send a requested context switch to the backend without touching any
    /// local state.
    ///
    /// Local navigation guards run first so a dirty YAML buffer or connected
    /// shell parks the request through the normal dialog flow; only a
    /// guard-clear request reaches the wire. An in-flight switch toward a
    /// different destination is superseded. A destination whose switch just
    /// failed is not re-requested by passive mismatch reconciliation, but an
    /// explicit user action lifts that suppression and goes back out; no
    /// extra failure chrome is needed because the unfulfilled pick stays
    /// visible in the top bar, which keeps showing the context the session
    /// actually serves.
    fn stage_context_switch(&mut self, to: &str, explicit: bool) -> Result<(), ClientError> {
        if self
            .pending_switch
            .as_ref()
            .is_some_and(|pending| pending.to == to)
        {
            return Ok(());
        }
        let committed = self.client.local_ui().selected_context.as_deref() == Some(to);
        let already_serving = self.shell.workspace().context() == to;
        if self.failed_switch.as_deref() == Some(to) && !committed {
            if !explicit {
                return Ok(());
            }
            // A deliberate re-pick of the same destination supersedes the
            // earlier failure.
            self.failed_switch = None;
        }
        if committed && already_serving {
            return Ok(());
        }
        if self.shell.workspace().context_switch_blockers().is_empty() {
            if let Some(pending) = self.pending_switch.take() {
                let _ = self.client.cancel(&pending.request);
            }
            let request = self
                .client
                .begin(Query::ContextSwitch { to: to.to_owned() })?;
            self.pending_switch = Some(PendingSwitch {
                request,
                to: to.to_owned(),
            });
            return Ok(());
        }
        // Guards are engaged: park the navigation through the normal
        // blocking path; resolving it re-emits the request.
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::ContextSwitch { to: to.to_owned() })
        {
            self.handle_workspace_event(event);
        }
        Ok(())
    }

    /// Complete the in-flight context switch: on success commit the local
    /// transition, resubscribe streams on the new context, and clear any
    /// failure record; on failure leave every local state untouched.
    fn finish_context_switch(&mut self) -> Result<(), AppEventError> {
        let Some(pending) = &self.pending_switch else {
            return Ok(());
        };
        if self.client.is_pending(&pending.request) {
            return Ok(());
        }
        let PendingSwitch { request, to } = self.pending_switch.take().expect("checked above");
        match self.client.take(request) {
            Some(QueryResult::ContextSwitch(response)) => {
                self.failed_switch = None;
                let current = response.current;
                self.client.local_ui_mut().selected_context = Some(current.clone());
                self.commit_context_layout(current);
                let current = self.shell.workspace().context().to_owned();
                self.reconcile_resource_streams(&current)
                    .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                self.select_infrastructure_context(&current)
                    .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                self.select_traffic_context(&current)
                    .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                Ok(())
            }
            Some(other) => Err(AppEventError::Terminal(format!(
                "context switch to '{to}' produced an unexpected answer: {other:?}"
            ))),
            None => {
                // The answer never arrived (transport loss or rebuild): the
                // selection stays where it was and the user can retry.
                self.failed_switch = Some(to);
                Ok(())
            }
        }
    }

    fn allocate_port_forward_issuance(&mut self) -> u64 {
        let issuance = self.next_port_forward_issuance;
        self.next_port_forward_issuance = self.next_port_forward_issuance.saturating_add(1);
        issuance
    }

    fn focus_successful_port_forward(&mut self, issuance: u64, session_id: &str) {
        if self
            .latest_focused_port_forward_issuance
            .is_none_or(|latest| issuance > latest)
        {
            self.latest_focused_port_forward_issuance = Some(issuance);
            self.shell.focus_port_forward_session(session_id);
        }
    }

    /// Send queued frames only through an opened transport.
    ///
    /// Before the handshake completes every browser-side send fails with
    /// `InvalidStateError`, which the transport adapter reports as success;
    /// draining now would silently drop frames like the protocol `Hello`.
    /// Queued frames survive until [`WsEvent::Opened`] triggers the flush.
    fn flush_outbound(&mut self) -> Result<(), AppEventError> {
        if !self.transport_open {
            return Ok(());
        }
        while let Some(frame) = self.client.take_outbound() {
            self.connection
                .as_mut()
                .ok_or(AppEventError::Transient)?
                .send_frame(&frame)
                .map_err(|_| AppEventError::Transient)?;
        }
        Ok(())
    }

    fn finish_infrastructure_request(&mut self) {
        let Some(request) = self.infrastructure_request.clone() else {
            return;
        };
        if self.client.is_pending(&request) {
            return;
        }
        if let Some(QueryResult::Infrastructure(_)) = self.client.take(request.clone()) {
            self.infrastructure_request = None;
            self.infrastructure_load = InfrastructureLoad::Available;
        } else if self
            .client
            .take_failure(request)
            .is_some_and(|failure| failure.code == k10s_protocol::ErrorCode::UnsupportedMessage)
        {
            self.infrastructure_request = None;
            self.infrastructure_load = InfrastructureLoad::Unavailable;
        }
    }

    fn finish_port_forward_list(&mut self) {
        if let Some(request) = self.port_forward_list.clone()
            && !self.client.is_pending(&request)
        {
            let _ = self.client.take(request);
            self.port_forward_list = None;
        }
    }

    fn finish_port_forward_requests(&mut self) {
        let completed = self
            .pending_port_forwards
            .iter()
            .enumerate()
            .filter(|(_, pending)| !self.client.is_pending(&pending.request))
            .map(|(index, _)| index)
            .rev()
            .collect::<Vec<_>>();
        let completed = completed
            .into_iter()
            .map(|index| self.pending_port_forwards.remove(index))
            .collect::<Vec<_>>();
        for pending in completed.into_iter().rev() {
            let issuance = pending.issuance;
            let Some(result) = self.client.take(pending.request) else {
                continue;
            };
            match (pending.intent, result) {
                (
                    PendingPortForwardIntent::StartModal(generation),
                    QueryResult::PortForwardStarted(response),
                ) => {
                    self.shell.port_forward_start_completed_for(generation);
                    self.focus_successful_port_forward(
                        issuance.expect("start requests carry issuance order"),
                        response.session.id.as_str(),
                    );
                }
                (
                    PendingPortForwardIntent::Retry(source),
                    QueryResult::PortForwardStarted(response),
                ) => {
                    self.port_forward_retry_errors.retry_succeeded(&source.id);
                    self.focus_successful_port_forward(
                        issuance.expect("retry requests carry issuance order"),
                        response.session.id.as_str(),
                    );
                }
                (PendingPortForwardIntent::Stop, QueryResult::PortForwardStopped(_)) => {}
                _ => {}
            }
        }
    }

    fn infrastructure_demanded(&self) -> bool {
        self.shell.workspace().windows().iter().any(|window| {
            matches!(
                window.kind,
                crate::workspace::WindowKind::Overview
                    | crate::workspace::WindowKind::Nodes
                    | crate::workspace::WindowKind::Storage
            )
        })
    }

    fn select_infrastructure_context(&mut self, context: &str) -> Result<(), ClientError> {
        if !self.infrastructure_demanded() {
            if let Some(subscription) = self.infrastructure_subscription.take() {
                self.client.unsubscribe(&subscription)?;
            }
            if let Some(request) = self.infrastructure_request.take() {
                if self.client.is_pending(&request) {
                    self.client.cancel(&request)?;
                } else {
                    let _ = self.client.take(request.clone());
                    let _ = self.client.take_failure(request);
                }
            }
            self.infrastructure_context = None;
            return Ok(());
        }
        if self.infrastructure_context.as_deref() != Some(context) {
            if let Some(subscription) = self.infrastructure_subscription.take() {
                self.client.unsubscribe(&subscription)?;
            }
            self.infrastructure_subscription =
                Some(self.client.subscribe_infrastructure(context.to_owned())?);
            self.infrastructure_context = Some(context.to_owned());
            self.refresh_infrastructure(context)?;
        }
        Ok(())
    }

    fn select_traffic_context(&mut self, context: &str) -> Result<(), ClientError> {
        if !self.client.traffic_available() {
            return Ok(());
        }
        if self.traffic_context.as_deref() != Some(context) {
            if let Some(subscription) = self.traffic_subscription.take() {
                self.client.unsubscribe(&subscription)?;
            }
            self.traffic_subscription = Some(self.client.subscribe_traffic(context.to_owned())?);
            self.traffic_context = Some(context.to_owned());
        }
        Ok(())
    }

    fn refresh_infrastructure(&mut self, context: &str) -> Result<(), ClientError> {
        if let Some(request) = self.infrastructure_request.take() {
            if self.client.is_pending(&request) {
                self.client.cancel(&request)?;
            } else {
                let _ = self.client.take(request.clone());
                let _ = self.client.take_failure(request);
            }
        }
        self.infrastructure_request = Some(self.client.begin(Query::Infrastructure(
            InfrastructureRequest {
                context: context.to_owned(),
            },
        ))?);
        self.infrastructure_load = InfrastructureLoad::Loading;
        Ok(())
    }

    fn retry_now(&mut self, now_ms: u64, entropy: u64) -> Result<(), ClientError> {
        if self.connection.is_some() || Self::terminal_phase(self.client.phase()) {
            return Ok(());
        }
        if self.client.phase() != ClientPhase::Disconnected {
            self.client.transport_lost(now_ms, entropy);
        }
        if !self.client.retry_if_due(u64::MAX)? {
            return Ok(());
        }
        match self.factory.connect(&self.connection_url) {
            Ok(connection) => {
                self.revoke_external_shell();
                self.transport_open = false;
                self.connection = Some(connection);
            }
            Err(_) => self.transient_loss(now_ms, entropy),
        }
        self.view = AppView::Connecting;
        Ok(())
    }

    fn transient_loss(&mut self, now_ms: u64, entropy: u64) {
        if Self::terminal_phase(self.client.phase()) {
            return;
        }
        if let Some(mut connection) = self.connection.take() {
            connection.close();
        }
        self.transport_open = false;
        self.revoke_external_shell();
        for (window, key) in &self.window_subscriptions {
            if let Some(rows) = self
                .resource_subscriptions
                .get(key)
                .and_then(|entry| self.client.resource_list(entry.live.id()))
                .map(|state| state.rows().cloned().collect())
            {
                self.window_retained_rows.insert(*window, rows);
            }
        }
        if self.client.phase() != ClientPhase::Disconnected {
            self.client.transport_lost(now_ms, entropy);
        }
        let retry = self.client.retry_schedule();
        for window in self.window_subscriptions.keys().copied() {
            let last_sync_age = self.window_last_sync_ms.get(&window).map_or_else(
                || "unknown".to_owned(),
                |synced| format_age(now_ms.saturating_sub(*synced)),
            );
            let (retry_in, attempt) = retry.map_or_else(
                || ("pending".to_owned(), 1),
                |schedule| {
                    (
                        format_duration(schedule.retry_at_ms.saturating_sub(now_ms)),
                        schedule.attempt.saturating_add(1),
                    )
                },
            );
            self.window_freshness_overrides.insert(
                window,
                WindowFreshness::Reconnecting {
                    last_sync_age,
                    retry_in,
                    attempt,
                },
            );
        }
        self.bootstrap = None;
        self.infrastructure_request = None;
        self.port_forward_list = None;
        // An unanswered switch request died with the transport; the selection
        // stays where it was and a retry needs a fresh user action.
        self.pending_switch = None;
        self.failed_switch = None;
        self.reset_port_forward_ui(Some("Connection lost; submit again"));
        self.teardown_stream_sessions();
        self.shell.yaml_editors_mut().connection_lost();
        // Server-issued details are stale after recovery and every in-flight
        // mutation lost its response channel: dialogs reopen for a safe
        // retry (the backend deduplicates by idempotency key).
        self.enter_resource_recovery();
        let mut failed_windows: Vec<WindowId> = Vec::new();
        for (_, entry) in std::mem::take(&mut self.pending_mutations) {
            failed_windows.push(entry.window);
        }
        for window in failed_windows {
            if let Some(mut dialog) = self.shell.dialogs_mut().active_mut(window) {
                dialog.operation_failed("connection lost; submit again");
            }
        }
        self.recovering = true;
        self.view = AppView::Connecting;
    }

    fn enter_resource_recovery(&mut self) {
        self.details.clear();
        self.detail_lifecycles.clear();
        self.primary_details.clear();
        self.detail_requests.clear();
        self.relations.clear();
        self.relation_requests.clear();
        self.metrics.clear();
        self.metric_checked_at.clear();
        self.metric_requests.clear();
        self.resource_generation = self.resource_generation.wrapping_add(1);
        if !matches!(self.namespace_catalog, NamespaceCatalogState::NotDemanded) {
            self.namespace_catalog = NamespaceCatalogState::Loading;
        }
        self.namespace_rejected_context = None;
    }

    /// Assemble the connected resource projection for one rendered frame:
    /// applied per-kind list views, Service rows, selectable types, and
    /// resolved details.
    fn build_resource_feed(&self) -> ResourceFeed {
        let dedicated_identities: std::collections::BTreeSet<_> = self
            .shell
            .workspace()
            .windows()
            .iter()
            .filter_map(|window| match &window.content {
                crate::workspace::WindowContent::Detail(detail) => {
                    detail.identity.as_row_identity().cloned()
                }
                _ => None,
            })
            .collect();
        let mut window_lists = std::collections::HashMap::new();
        let mut window_services = std::collections::HashMap::new();
        let mut lists = std::collections::HashMap::new();
        let mut services = None;
        for (window, key) in &self.window_subscriptions {
            let rows = self
                .resource_subscriptions
                .get(key)
                .and_then(|subscription| self.client.resource_list(subscription.live.id()))
                .map(|state| state.rows().cloned().collect::<Vec<_>>())
                .or_else(|| self.window_retained_rows.get(window).cloned());
            if let Some(rows) = rows {
                if key.gvk.group.is_empty() && key.gvk.version == "v1" && key.gvk.kind == "Service"
                {
                    window_services.insert(*window, rows);
                } else {
                    window_lists.insert(*window, rows);
                }
            }
        }
        for (key, subscription) in &self.resource_subscriptions {
            if !matches!(
                key.scope,
                SubscriptionScope::Namespaced(NamespaceScope::AllNamespaces)
            ) {
                continue;
            }
            let Some(state) = self.client.resource_list(subscription.live.id()) else {
                continue;
            };
            let rows = state.rows().cloned().collect::<Vec<_>>();
            match key.gvk.kind.as_str() {
                "Pod" => {
                    lists.insert(WorkloadKind::Pods, rows);
                }
                "Deployment" => {
                    lists.insert(WorkloadKind::Deployments, rows);
                }
                "Service" => services = Some(rows),
                _ => {}
            }
        }
        let namespace_catalog = match (&self.namespace_catalog, &self.namespace_subscription) {
            (NamespaceCatalogState::Loading, Some((_, subscription))) => self
                .client
                .resource_list(subscription.id())
                .map(|state| {
                    let mut names: Vec<_> =
                        state.rows().map(|row| row.identity.name.clone()).collect();
                    names.sort();
                    names.dedup();
                    NamespaceCatalogState::Ready(names)
                })
                .unwrap_or(NamespaceCatalogState::Loading),
            (state, _) => state.clone(),
        };
        let window_freshness: std::collections::HashMap<_, _> = window_lists
            .iter()
            .map(|(window, rows)| {
                let state = if rows.is_empty() {
                    WindowFreshness::ReadyEmpty
                } else {
                    WindowFreshness::Live {
                        last_sync_age: "just now".into(),
                    }
                };
                (*window, state)
            })
            .chain(window_services.iter().map(|(window, rows)| {
                let state = if rows.is_empty() {
                    WindowFreshness::ReadyEmpty
                } else {
                    WindowFreshness::Live {
                        last_sync_age: "just now".into(),
                    }
                };
                (*window, state)
            }))
            .chain(
                self.window_freshness_overrides
                    .iter()
                    .map(|(window, state)| (*window, state.clone())),
            )
            .collect();
        let aggregate_freshness = |sources: Vec<WindowFreshness>| {
            if sources.len() == 1 {
                sources.into_iter().next().unwrap()
            } else if !sources.is_empty() && sources.iter().all(WindowFreshness::mutations_allowed)
            {
                WindowFreshness::Live {
                    last_sync_age: "all sources live".into(),
                }
            } else {
                WindowFreshness::Failed {
                    message: "one or more authoritative sources are not live".into(),
                }
            }
        };
        let mut lifecycle_candidates: BTreeMap<
            ResourceIdentity,
            Vec<(
                k10s_protocol::BackendRevision,
                DetailLifecycle,
                WindowFreshness,
                bool,
            )>,
        > = BTreeMap::new();
        for (key, subscription) in &self.resource_subscriptions {
            let source_freshness = aggregate_freshness(
                subscription
                    .windows
                    .iter()
                    .filter_map(|window| window_freshness.get(window).cloned())
                    .collect(),
            );
            let source_state = self.detail_lifecycles.get(subscription.live.id());
            if let Some(state) = self.client.resource_list(subscription.live.id()) {
                for row in state.rows() {
                    let revision = source_state
                        .and_then(|source| source.snapshot_revision)
                        .map_or(row.revision, |snapshot| snapshot.max(row.revision));
                    lifecycle_candidates
                        .entry(row.identity.clone())
                        .or_default()
                        .push((
                            revision,
                            DetailLifecycle::Present,
                            source_freshness.clone(),
                            key.identity.is_some(),
                        ));
                }
            }
            if let Some(identity) = &key.identity
                && let Some(revision) = source_state.and_then(|source| source.snapshot_revision)
                && self
                    .client
                    .resource_list(subscription.live.id())
                    .is_some_and(|state| state.rows().all(|row| &row.identity != identity))
            {
                lifecycle_candidates
                    .entry(identity.clone())
                    .or_default()
                    .push((
                        revision,
                        DetailLifecycle::Gone,
                        source_freshness.clone(),
                        true,
                    ));
            }
            if let Some(source) = source_state {
                for (identity, event) in &source.entries {
                    lifecycle_candidates
                        .entry(identity.clone())
                        .or_default()
                        .push((
                            event.revision,
                            event.lifecycle,
                            source_freshness.clone(),
                            key.identity.is_some(),
                        ));
                }
            }
        }
        for (window, key) in &self.window_subscriptions {
            if self
                .resource_subscriptions
                .get(key)
                .is_some_and(|subscription| {
                    self.client.resource_list(subscription.live.id()).is_some()
                })
            {
                continue;
            }
            let Some(freshness) = window_freshness.get(window) else {
                continue;
            };
            let Some(rows) = self.window_retained_rows.get(window) else {
                continue;
            };
            for row in rows {
                lifecycle_candidates
                    .entry(row.identity.clone())
                    .or_default()
                    .push((
                        row.revision,
                        DetailLifecycle::Present,
                        freshness.clone(),
                        false,
                    ));
            }
        }
        let detail_authority = lifecycle_candidates
            .into_iter()
            .filter_map(|(identity, mut candidates)| {
                if dedicated_identities.contains(&identity) {
                    candidates.retain(|(_, _, _, exact)| *exact);
                }
                if candidates.is_empty() {
                    return None;
                }
                let newest = candidates
                    .iter()
                    .map(|(revision, _, _, _)| *revision)
                    .max()
                    .expect("candidate groups are never empty");
                let lifecycle = if candidates.iter().any(|(revision, lifecycle, _, _)| {
                    *revision == newest && *lifecycle == DetailLifecycle::Gone
                }) {
                    DetailLifecycle::Gone
                } else {
                    DetailLifecycle::Present
                };
                let freshness = if lifecycle == DetailLifecycle::Gone {
                    WindowFreshness::ReadyEmpty
                } else {
                    aggregate_freshness(
                        candidates
                            .into_iter()
                            .filter_map(|(_, lifecycle, freshness, _)| {
                                (lifecycle == DetailLifecycle::Present).then_some(freshness)
                            })
                            .collect(),
                    )
                };
                Some((
                    identity,
                    DetailAuthority {
                        freshness,
                        lifecycle,
                    },
                ))
            })
            .collect();
        ResourceFeed {
            render_time: None,
            window_freshness,
            detail_authority,
            namespace_catalog,
            lists,
            window_lists,
            services,
            window_services,
            types: self.resource_types.clone(),
            details: self.details.clone().into_iter().collect(),
            primary_details: self.primary_details.clone().into_iter().collect(),
            relations: self.relations.clone().into_iter().collect(),
            metrics: self.metrics.clone().into_iter().collect(),
            port_forward_available: self.client.port_forward_available(),
            pod_port_forward_available: self.client.pod_port_forward_available(),
            port_forward_list_state: if self.client.port_forward_reconstructing() {
                crate::ui::PortForwardListState::Reconstructing
            } else if self.port_forward_list.is_some() {
                crate::ui::PortForwardListState::Loading
            } else {
                crate::ui::PortForwardListState::Ready
            },
            port_forward_sessions: self
                .client
                .port_forward_sessions()
                .into_iter()
                .cloned()
                .collect(),
            port_forward_retry_errors: self
                .client
                .port_forward_sessions()
                .into_iter()
                .filter_map(|session| {
                    self.port_forward_retry_errors
                        .get(&session.id)
                        .map(|message| (session.id.clone(), message.to_owned()))
                })
                .collect(),
            port_forward_error: self.port_forward_error.clone(),
        }
    }

    /// Diff the watches demanded by open workspace list windows against the
    /// retained live set. Equal canonical keys share one bounded client
    /// subscription while every window retains its own projection mapping.
    fn reconcile_resource_streams(&mut self, context: &str) -> Result<(), ClientError> {
        use crate::workspace::{WindowContent, WindowKind};

        let namespace_context_changed = self
            .namespace_subscription
            .as_ref()
            .is_some_and(|(subscribed_context, _)| subscribed_context != context)
            || self
                .namespace_rejected_context
                .as_deref()
                .is_some_and(|rejected| rejected != context);
        if self
            .namespace_rejected_context
            .as_deref()
            .is_some_and(|rejected| rejected != context)
        {
            self.namespace_rejected_context = None;
        }
        let context_namespace = match &self.view {
            AppView::Ready { contexts, .. } => contexts
                .iter()
                .find(|candidate| candidate.name == context)
                .and_then(|candidate| candidate.namespace.as_deref()),
            _ => None,
        };
        let mut desired: BTreeMap<SubscriptionKey, std::collections::BTreeSet<WindowId>> =
            BTreeMap::new();
        let mut window_subscriptions = BTreeMap::new();
        let mut custom_open = false;
        let mut namespace_demanded = false;
        if let Some((sub_ctx, subscription)) = &self.namespace_subscription
            && sub_ctx == context
            && matches!(self.namespace_catalog, NamespaceCatalogState::Loading)
            && let Some(state) = self.client.resource_list(subscription.id())
        {
            let mut names: Vec<_> = state.rows().map(|row| row.identity.name.clone()).collect();
            names.sort();
            names.dedup();
            self.namespace_catalog = NamespaceCatalogState::Ready(names);
        }
        if let NamespaceCatalogState::Ready(names) = &self.namespace_catalog
            && let Some(first) = names.first()
        {
            self.shell.resolve_context_default_namespaces(first);
        }
        if self.shell.command_palette_open() {
            for (group, version, kind) in [
                ("", "v1", "Pod"),
                ("apps", "v1", "Deployment"),
                ("", "v1", "Service"),
            ] {
                desired
                    .entry(SubscriptionKey {
                        context: context.to_owned(),
                        gvk: k10s_protocol::GroupVersionKind {
                            group: group.to_owned(),
                            version: version.to_owned(),
                            kind: kind.to_owned(),
                        },
                        protocol_namespace: None,
                        scope: SubscriptionScope::Namespaced(NamespaceScope::AllNamespaces),
                        identity: None,
                    })
                    .or_default();
            }
            namespace_demanded = true;
        }
        for window in self.shell.workspace().windows() {
            let (gvk, scope, identity) = match (&window.kind, &window.content) {
                (WindowKind::Services, WindowContent::Services(state)) => (
                    k10s_protocol::GroupVersionKind::core("v1", "Service"),
                    SubscriptionScope::Namespaced(state.namespace_scope.clone()),
                    None,
                ),
                (WindowKind::Workload(kind), WindowContent::Resource(state)) => {
                    if *kind == WorkloadKind::CustomResources {
                        custom_open = true;
                        if self.types_context.as_deref() != Some(context)
                            || self.types_request.is_some()
                        {
                            continue;
                        }
                        let Some(selected) = state.custom_kind.as_deref() else {
                            continue;
                        };
                        let Some(descriptor) = self.resource_types.iter().find(|entry| {
                            format!(
                                "{}/{}/{}",
                                entry.gvk.group, entry.gvk.version, entry.gvk.kind
                            ) == selected
                        }) else {
                            continue;
                        };
                        let scope = if descriptor.namespaced {
                            SubscriptionScope::Namespaced(state.namespace_scope.clone())
                        } else {
                            SubscriptionScope::ClusterScoped
                        };
                        (descriptor.gvk.clone(), scope, None)
                    } else {
                        let resource_kind = *kind;
                        let Some((group, version, wire_kind)) = builtin_kind_gvk(resource_kind)
                        else {
                            continue;
                        };
                        let scope = if resource_kind.namespaced() {
                            SubscriptionScope::Namespaced(state.namespace_scope.clone())
                        } else {
                            SubscriptionScope::ClusterScoped
                        };
                        (
                            k10s_protocol::GroupVersionKind {
                                group: group.to_owned(),
                                version: version.to_owned(),
                                kind: wire_kind.to_owned(),
                            },
                            scope,
                            None,
                        )
                    }
                }
                (WindowKind::Detail, WindowContent::Detail(detail)) => {
                    if !self.client.exact_resource_watches_available() {
                        continue;
                    }
                    let identity = detail.identity.as_row_identity();
                    let Some(identity) = identity.filter(|identity| {
                        (identity.gvk.group.is_empty()
                            && identity.gvk.version == "v1"
                            && identity.gvk.kind == "Pod")
                            || (identity.gvk.group == "apps"
                                && identity.gvk.version == "v1"
                                && identity.gvk.kind == "Deployment")
                    }) else {
                        continue;
                    };
                    let Some(namespace) = identity.namespace.clone() else {
                        continue;
                    };
                    if identity.context != context {
                        continue;
                    }
                    (
                        identity.gvk.clone(),
                        SubscriptionScope::Namespaced(NamespaceScope::Namespace(namespace)),
                        Some(identity.clone()),
                    )
                }
                _ => continue,
            };
            let key = SubscriptionKey {
                context: context.to_owned(),
                gvk,
                protocol_namespace: match &scope {
                    SubscriptionScope::Namespaced(intent) => {
                        intent.resolve(context_namespace).map(str::to_owned)
                    }
                    SubscriptionScope::ClusterScoped => None,
                },
                scope,
                identity,
            };
            namespace_demanded |=
                key.identity.is_none() && matches!(key.scope, SubscriptionScope::Namespaced(_));
            desired.entry(key.clone()).or_default().insert(window.id);
            window_subscriptions.insert(window.id, key);
        }

        let removed: Vec<_> = self
            .resource_subscriptions
            .keys()
            .filter(|key| !desired.contains_key(*key))
            .cloned()
            .collect();
        let additions = desired
            .keys()
            .filter(|key| {
                !self.resource_subscriptions.contains_key(*key)
                    && !self.rejected_subscription_keys.contains(*key)
            })
            .count();
        let namespace_removed = usize::from(
            self.namespace_subscription
                .as_ref()
                .is_some_and(|(subscribed_context, _)| subscribed_context != context),
        );
        let namespace_added = usize::from(
            namespace_demanded
                && self.namespace_rejected_context.as_deref() != Some(context)
                && self
                    .namespace_subscription
                    .as_ref()
                    .is_none_or(|(subscribed_context, _)| subscribed_context != context),
        );
        if self.client.phase() == ClientPhase::Ready {
            self.client.preflight_subscription_changes(
                removed.len() + namespace_removed,
                additions + namespace_added,
            )?;
        }
        if namespace_removed == 1
            && let Some((_, subscription)) = self.namespace_subscription.take()
        {
            self.client.unsubscribe(&subscription)?;
            self.retire_detail_lifecycle_source(subscription.id());
            self.namespace_rejected_context = None;
        }
        for key in removed {
            let source = if let Some(entry) = self.resource_subscriptions.get(&key) {
                self.client.unsubscribe(&entry.live)?;
                Some(entry.live.id().clone())
            } else {
                None
            };
            self.resource_subscriptions.remove(&key);
            if let Some(source) = source {
                self.retire_detail_lifecycle_source(&source);
            }
        }
        if self.client.phase() != ClientPhase::Ready {
            self.window_subscriptions = window_subscriptions;
            self.resource_types.clear();
            self.types_context = None;
            self.types_request = None;
            return Ok(());
        }
        if namespace_added == 1 {
            let live = self
                .client
                .subscribe_resource(context, "", "v1", "Namespace", None)?;
            self.namespace_subscription = Some((context.to_owned(), live));
            self.namespace_catalog = NamespaceCatalogState::Loading;
        } else if namespace_context_changed && !namespace_demanded {
            self.namespace_catalog = NamespaceCatalogState::NotDemanded;
            self.namespace_rejected_context = None;
        }
        for (key, windows) in desired {
            if self.rejected_subscription_keys.contains(&key) {
                continue;
            }
            if let Some(entry) = self.resource_subscriptions.get_mut(&key) {
                entry.windows = windows;
                continue;
            }
            let live = match self.client.subscribe_resource_exact(
                key.context.clone(),
                key.gvk.group.clone(),
                key.gvk.version.clone(),
                key.gvk.kind.clone(),
                key.protocol_namespace.clone(),
                key.identity.clone(),
            ) {
                Ok(live) => live,
                Err(error) => {
                    window_subscriptions.retain(|_, desired_key| {
                        self.resource_subscriptions.contains_key(desired_key)
                    });
                    self.window_subscriptions = window_subscriptions;
                    return Err(error);
                }
            };
            self.resource_subscriptions
                .insert(key, RetainedSubscription { live, windows });
        }
        self.window_freshness_overrides
            .retain(|window, _| window_subscriptions.contains_key(window));
        self.window_retained_rows
            .retain(|window, _| window_subscriptions.contains_key(window));
        self.window_last_sync_ms
            .retain(|window, _| window_subscriptions.contains_key(window));
        self.rejected_subscription_keys
            .retain(|key| window_subscriptions.values().any(|wanted| wanted == key));
        self.window_subscriptions = window_subscriptions;

        if custom_open {
            let cache_ready =
                self.types_context.as_deref() == Some(context) && self.types_request.is_none();
            let request_current = self
                .types_request
                .as_ref()
                .is_some_and(|(requested, _)| requested == context);
            if !cache_ready && !request_current {
                if let Some((_, request)) = self.types_request.take() {
                    let _ = self.client.cancel(&request);
                }
                self.resource_types.clear();
                self.types_context = Some(context.to_owned());
                let request = self
                    .client
                    .begin(Query::ResourceTypes(ResourceTypesRequest {
                        context: context.to_owned(),
                    }))?;
                self.types_request = Some((context.to_owned(), request));
            }
        } else {
            if let Some((_, request)) = self.types_request.take() {
                let _ = self.client.cancel(&request);
            }
            self.resource_types.clear();
            self.types_context = None;
        }

        if self.infrastructure_demanded() {
            if matches!(self.view, AppView::Ready { .. })
                && self.client.phase() == ClientPhase::Ready
                && self.infrastructure_context.as_deref() != Some(context)
                && self.infrastructure_load != InfrastructureLoad::Unavailable
            {
                self.select_infrastructure_context(context)?;
            }
        } else {
            if let Some(subscription) = self.infrastructure_subscription.take() {
                self.client.unsubscribe(&subscription)?;
            }
            if let Some(request) = self.infrastructure_request.take() {
                if self.client.is_pending(&request) {
                    let _ = self.client.cancel(&request);
                } else {
                    let _ = self.client.take(request.clone());
                    let _ = self.client.take_failure(request);
                }
            }
            self.infrastructure_context = None;
        }
        Ok(())
    }

    fn reconcile_selected_resource_streams(&mut self) {
        let Some(context) = self.client.local_ui().selected_context.clone() else {
            return;
        };
        if let Err(error) = self.reconcile_resource_streams(&context).and_then(|()| {
            self.flush_outbound()
                .map_err(|error| ClientError::Protocol(format!("{error:?}")))
        }) {
            self.terminal_failure(error.to_string());
        }
    }

    /// Complete the in-flight `resource.types` request.
    fn finish_type_requests(&mut self) {
        let Some((_, request)) = &self.types_request else {
            return;
        };
        if self.client.is_pending(request) {
            return;
        }
        let Some((requested, request)) = self.types_request.take() else {
            unreachable!("just checked");
        };
        if let Some(QueryResult::ResourceTypes(response)) = self.client.take(request)
            && self.types_context.as_deref() == Some(requested.as_str())
        {
            self.resource_types = response.types;
            self.reconcile_selected_resource_streams();
        } else {
            // The answer no longer matches the selection: force a refetch on
            // the next reconciliation.
            self.types_context = None;
        }
    }

    /// Require current exact-identity authority at the final application
    /// dispatch boundary. Rendering may have happened several frames before
    /// an action is drained, so the dialog's earlier enabled state is never
    /// sufficient authority on its own.
    fn mutation_authority_allows(&self, target: &ResourceIdentity) -> bool {
        let primary_loaded = match self.primary_details.get(target) {
            Some(PrimaryDetailState::Loaded(_)) => true,
            Some(PrimaryDetailState::Loading | PrimaryDetailState::Failed(_)) => false,
            None => self.details.contains_key(target),
        };
        primary_loaded
            && self
                .build_resource_feed()
                .detail_authority
                .get(target)
                .is_some_and(DetailAuthority::mutations_allowed)
    }

    /// Drain rendering-time dialog actions into workload mutation commands.
    fn process_dialog_actions(&mut self) -> Result<(), ClientError> {
        for (window, action) in self.shell.drain_dialog_actions() {
            let target = match &action {
                DialogAction::RequestDeletePreflight { target, .. }
                | DialogAction::SubmitScale { target, .. }
                | DialogAction::SubmitDelete { target, .. } => target,
            };
            if !self.mutation_authority_allows(target) {
                if let Some(mut dialog) = self.shell.dialogs_mut().active_mut(window) {
                    dialog.operation_failed("window is not live; retry the list before mutating");
                }
                continue;
            }
            let command = match action {
                DialogAction::RequestDeletePreflight {
                    target,
                    propagation,
                } => {
                    let superseded = self
                        .pending_delete_preflights
                        .iter()
                        .filter_map(|(id, entry)| (entry.window == window).then_some(id.clone()))
                        .collect::<Vec<_>>();
                    for id in superseded {
                        if let Some(entry) = self.pending_delete_preflights.remove(&id) {
                            let _ = self.client.cancel(&entry.request);
                        }
                    }
                    let request = self.client.begin(Query::DeletePreflight(
                        k10s_protocol::DeletePreflightRequest {
                            identity: target.clone(),
                            propagation,
                        },
                    ))?;
                    self.pending_delete_preflights.insert(
                        request.id().clone(),
                        PendingDeletePreflight {
                            request,
                            window,
                            target,
                            propagation,
                        },
                    );
                    continue;
                }
                DialogAction::SubmitScale {
                    target,
                    replicas,
                    idempotency_key,
                } => Command::Scale {
                    target,
                    replicas,
                    idempotency_key,
                },
                DialogAction::SubmitDelete {
                    target,
                    propagation,
                    resource_version,
                    idempotency_key,
                } => Command::Delete {
                    target,
                    propagation,
                    resource_version,
                    idempotency_key,
                },
            };
            let request = self.client.begin_command(command)?;
            self.pending_mutations
                .insert(request.id().clone(), PendingMutation { request, window });
        }
        self.prune_detail_lifecycles();
        self.flush_outbound()
            .map_err(|error| ClientError::Protocol(format!("{error:?}")))
    }

    /// Complete in-flight mutations by reporting the accepted operation (or
    /// a lost-response failure) back to the originating dialog.
    fn finish_mutations(&mut self) {
        while let Some(id) = self
            .pending_mutations
            .iter()
            .find(|(_, entry)| !self.client.is_pending(&entry.request))
            .map(|(id, _)| id.clone())
        {
            let Some(entry) = self.pending_mutations.remove(&id) else {
                unreachable!("key came from this map");
            };
            let outcome = match self.client.take(entry.request) {
                Some(QueryResult::Applied(accepted)) => Ok(accepted.operation_id),
                _ => Err("submission lost; submit again"),
            };
            if let Some(mut dialog) = self.shell.dialogs_mut().active_mut(entry.window) {
                match outcome {
                    Ok(operation_id) => dialog.operation_accepted(operation_id),
                    Err(reason) => dialog.operation_failed(reason),
                }
            }
        }
        self.finish_type_requests();
        self.finish_delete_preflights();
        self.refresh_details();
    }

    fn finish_delete_preflights(&mut self) {
        while let Some(id) = self
            .pending_delete_preflights
            .iter()
            .find(|(_, entry)| !self.client.is_pending(&entry.request))
            .map(|(id, _)| id.clone())
        {
            let entry = self.pending_delete_preflights.remove(&id).unwrap();
            let preflight = match self.client.take(entry.request.clone()) {
                Some(QueryResult::DeletePreflight(response)) => {
                    crate::ui::dialogs::DestructivePreflight::Ready {
                        impact: response.impact,
                        dry_run: response.dry_run,
                        resource_version: response.resource_version,
                    }
                }
                _ => {
                    let failure = self.client.take_failure(entry.request);
                    match failure {
                        Some(failure) if failure.code == ErrorCode::Unauthorized => {
                            crate::ui::dialogs::DestructivePreflight::Forbidden(
                                failure.safe_message,
                            )
                        }
                        Some(failure) if failure.code == ErrorCode::Conflict => {
                            crate::ui::dialogs::DestructivePreflight::Conflict(failure.safe_message)
                        }
                        Some(failure) => crate::ui::dialogs::DestructivePreflight::DryRunFailed(
                            failure.safe_message,
                        ),
                        None => crate::ui::dialogs::DestructivePreflight::DryRunFailed(
                            "delete dry-run failed".into(),
                        ),
                    }
                }
            };
            if let Some(crate::ui::dialogs::DialogHandle::Delete(dialog)) =
                self.shell.dialogs_mut().active_mut(entry.window)
                && dialog.target() == &entry.target
                && dialog.propagation() == entry.propagation
            {
                dialog.set_preflight(preflight);
            }
        }
    }

    /// Issue detail queries for every identity a window pinned so the
    /// rendered feed carries backend-resolved views. Identities already
    /// resolved keep their cached response until transport loss.
    fn refresh_details(&mut self) {
        let now_ms = u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.refresh_details_at(now_ms);
    }

    fn restart_namespace_catalog_after_preflight(&mut self) -> Result<(), ClientError> {
        if let Some((_, subscription)) = &self.namespace_subscription
            && self.client.unsubscribe(subscription)?
        {
            let (_, subscription) = self
                .namespace_subscription
                .take()
                .expect("the unsubscribed Namespace handle was present");
            self.retire_detail_lifecycle_source(subscription.id());
        }
        self.namespace_rejected_context = None;
        self.namespace_catalog = NamespaceCatalogState::Loading;
        Ok(())
    }

    fn handle_resource_action(&mut self, action: ResourceAction) {
        match action {
            ResourceAction::OpenExternalShell { window, target } => {
                let available = matches!(
                    self.shell.external_shell_availability(),
                    crate::ui::ExternalShellAvailability::Available { generation }
                        if generation == target.generation
                );
                let valid = self
                    .workspace_stream_target(window)
                    .is_some_and(|candidate| {
                        candidate.namespace == target.namespace
                            && candidate.pod == target.pod
                            && candidate.uid == target.uid
                            && target.program == "/bin/sh"
                            && !target.container.is_empty()
                            && self.details.values().any(|view| {
                                view.identity.context == candidate.context
                                    && view.identity.gvk.group.is_empty()
                                    && view.identity.gvk.version == "v1"
                                    && view.identity.gvk.kind == "Pod"
                                    && view.identity.namespace.as_deref() == Some(&target.namespace)
                                    && view.identity.name == target.pod
                                    && view.identity.uid == target.uid
                                    && self.mutation_authority_allows(&view.identity)
                                    && crate::ui::PodRuntimeProjection::from_view(
                                        &view.identity,
                                        view,
                                    )
                                    .is_some_and(|runtime| runtime.contains(&target.container))
                            })
                    });
                if available && valid && self.external_shell_requests.is_empty() {
                    self.external_shell_requests.push(target);
                }
            }
            ResourceAction::Restart { window, target } => {
                if !self.mutation_authority_allows(&target) {
                    return;
                }
                let idempotency_key = format!(
                    "restart:{}:{}:{}",
                    target.uid,
                    window.0,
                    self.clock_started.elapsed().as_nanos()
                );
                match self.client.begin_command(Command::Restart {
                    target,
                    idempotency_key,
                }) {
                    Ok(request) => {
                        self.pending_mutations
                            .insert(request.id().clone(), PendingMutation { request, window });
                        if let Err(error) = self.flush_outbound() {
                            self.terminal_failure(format!("{error:?}"));
                        }
                    }
                    Err(error) => self.terminal_failure(error.to_string()),
                }
            }
            ResourceAction::RetryPrimary(identity) => {
                if !self.cancel_detail_request(&identity) {
                    return;
                }
                self.details.remove(&identity);
                self.primary_details.remove(&identity);
            }
            ResourceAction::RetryRelations(identity) => {
                if !self.cancel_relation_request(&identity) {
                    return;
                }
                match self.relations.get_mut(&identity) {
                    Some(RelationState::Loaded { refresh_error, .. }) => {
                        // Clearing only the failure marker re-arms the stale
                        // entry; its response remains visible until replaced.
                        *refresh_error = None;
                    }
                    _ => {
                        self.relations.insert(identity, RelationState::NotRequested);
                    }
                }
            }
            // Task 6 owns namespace-catalog lifecycle and consumes this
            // already-stable shell action variant there.
            ResourceAction::RetryNamespaceCatalog => {
                let removals = usize::from(self.namespace_subscription.is_some());
                if let Err(error) = self
                    .client
                    .preflight_subscription_changes(removals, 1)
                    .and_then(|()| self.restart_namespace_catalog_after_preflight())
                {
                    self.terminal_failure(error.to_string());
                    return;
                }
                self.reconcile_selected_resource_streams();
            }
            action @ (ResourceAction::RetryWindow(window)
            | ResourceAction::FullResyncWindow(window)) => {
                // A stale window may be the projection of a dead control
                // transport rather than a watch-local failure. Route its
                // retry through transport recovery and keep the cached rows
                // until a replacement snapshot arrives.
                if self.client.phase() != ClientPhase::Ready {
                    let now_ms =
                        u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                    self.jitter_counter = self.jitter_counter.wrapping_add(1);
                    let entropy =
                        now_ms.rotate_left(17) ^ self.jitter_counter.wrapping_mul(0x9e37_79b9);
                    if let Err(error) = self.retry_now(now_ms, entropy) {
                        self.terminal_failure(error.to_string());
                    }
                    return;
                }
                let Some(key) = self.window_subscriptions.get(&window).cloned() else {
                    return;
                };
                if matches!(action, ResourceAction::FullResyncWindow(_))
                    && matches!(key.scope, SubscriptionScope::Namespaced(_))
                {
                    let namespace_removals = usize::from(self.namespace_subscription.is_some());
                    let window_removals =
                        usize::from(self.resource_subscriptions.contains_key(&key));
                    if let Err(error) = self
                        .client
                        .preflight_subscription_changes(namespace_removals + window_removals, 2)
                        .and_then(|()| self.restart_namespace_catalog_after_preflight())
                    {
                        self.terminal_failure(error.to_string());
                        return;
                    }
                }
                self.rejected_subscription_keys.remove(&key);
                let affected: Vec<_> = self
                    .window_subscriptions
                    .iter()
                    .filter_map(|(candidate, candidate_key)| {
                        (candidate_key == &key).then_some(*candidate)
                    })
                    .collect();
                for affected_window in affected {
                    self.window_freshness_overrides.insert(
                        affected_window,
                        WindowFreshness::StaleRetrying {
                            last_sync_age: "cached".to_owned(),
                            retry_in: "now".to_owned(),
                            attempt: 1,
                        },
                    );
                }
                if let Some(subscription) = self.resource_subscriptions.remove(&key) {
                    let _ = self.client.unsubscribe(&subscription.live);
                    self.retire_detail_lifecycle_source(subscription.live.id());
                }
                self.reconcile_selected_resource_streams();
            }
        }
    }

    fn is_resource_pinned(&self, wanted: &ResourceIdentity) -> bool {
        self.shell.workspace().windows().iter().any(|window| {
            let identity = match &window.content {
                crate::workspace::WindowContent::Detail(detail) => {
                    detail.identity.as_row_identity()
                }
                crate::workspace::WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.identity.as_row_identity()),
                crate::workspace::WindowContent::Services(service) => service
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.identity.as_row_identity()),
                crate::workspace::WindowContent::PortForwards(_) => None,
            };
            identity == Some(wanted)
        })
    }

    fn tracked_detail_identities(&self) -> std::collections::BTreeSet<ResourceIdentity> {
        let mut tracked = std::collections::BTreeSet::new();
        for window in self.shell.workspace().windows() {
            let identity = match &window.content {
                crate::workspace::WindowContent::Detail(detail) => {
                    detail.identity.as_row_identity()
                }
                crate::workspace::WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.identity.as_row_identity()),
                crate::workspace::WindowContent::Services(service) => service
                    .detail
                    .as_ref()
                    .and_then(|detail| detail.identity.as_row_identity()),
                crate::workspace::WindowContent::PortForwards(_) => None,
            };
            if let Some(identity) = identity {
                tracked.insert(identity.clone());
            }
        }
        tracked.extend(self.shell.dialogs().targets().cloned());
        tracked
    }

    fn merge_detail_lifecycle_event(
        entries: &mut BTreeMap<ResourceIdentity, DetailLifecycleEvent>,
        identity: ResourceIdentity,
        incoming: DetailLifecycleEvent,
    ) {
        let replace = entries.get(&identity).is_none_or(|current| {
            incoming.revision > current.revision
                || (incoming.revision == current.revision
                    && incoming.lifecycle == DetailLifecycle::Gone
                    && current.lifecycle == DetailLifecycle::Present)
        });
        if replace {
            entries.insert(identity, incoming);
        }
    }

    fn record_detail_lifecycle(
        &mut self,
        source: k10s_protocol::SubscriptionId,
        identity: ResourceIdentity,
        revision: k10s_protocol::BackendRevision,
        lifecycle: DetailLifecycle,
    ) {
        let tracked = self.tracked_detail_identities().contains(&identity);
        let source_state = self.detail_lifecycles.entry(source).or_default();
        let had_entry = source_state.entries.contains_key(&identity);
        if lifecycle == DetailLifecycle::Gone || tracked || had_entry {
            Self::merge_detail_lifecycle_event(
                &mut source_state.entries,
                identity.clone(),
                DetailLifecycleEvent {
                    revision,
                    lifecycle,
                },
            );
        }
        if lifecycle == DetailLifecycle::Present
            && !tracked
            && source_state.entries.get(&identity).is_some_and(|entry| {
                entry.lifecycle == DetailLifecycle::Present && entry.revision <= revision
            })
        {
            source_state.entries.remove(&identity);
        }
        self.prune_detail_lifecycles();
    }

    fn accept_detail_lifecycle_snapshot(
        &mut self,
        source: k10s_protocol::SubscriptionId,
        revision: k10s_protocol::BackendRevision,
        previous: Vec<ResourceIdentity>,
        current: Vec<ResourceIdentity>,
    ) {
        let tracked = self.tracked_detail_identities();
        let current: std::collections::BTreeSet<_> = current.into_iter().collect();
        let source_state = self.detail_lifecycles.entry(source).or_default();
        source_state.snapshot_revision = Some(revision);

        let known: Vec<_> = source_state.entries.keys().cloned().collect();
        for identity in known {
            let lifecycle = if current.contains(&identity) {
                DetailLifecycle::Present
            } else {
                DetailLifecycle::Gone
            };
            Self::merge_detail_lifecycle_event(
                &mut source_state.entries,
                identity.clone(),
                DetailLifecycleEvent {
                    revision,
                    lifecycle,
                },
            );
            if lifecycle == DetailLifecycle::Present && !tracked.contains(&identity) {
                source_state.entries.remove(&identity);
            }
        }
        for identity in previous {
            if !current.contains(&identity) {
                Self::merge_detail_lifecycle_event(
                    &mut source_state.entries,
                    identity,
                    DetailLifecycleEvent {
                        revision,
                        lifecycle: DetailLifecycle::Gone,
                    },
                );
            }
        }
        for identity in current {
            if tracked.contains(&identity) {
                Self::merge_detail_lifecycle_event(
                    &mut source_state.entries,
                    identity,
                    DetailLifecycleEvent {
                        revision,
                        lifecycle: DetailLifecycle::Present,
                    },
                );
            }
        }
        self.prune_detail_lifecycles();
    }

    fn retire_detail_lifecycle_source(&mut self, source: &k10s_protocol::SubscriptionId) {
        self.detail_lifecycles.remove(source);
    }

    fn prune_detail_lifecycles(&mut self) {
        let tracked = self.tracked_detail_identities();
        let mut untracked: Vec<_> = self
            .detail_lifecycles
            .iter()
            .flat_map(|(source, state)| {
                state
                    .entries
                    .iter()
                    .filter(|(identity, _)| !tracked.contains(*identity))
                    .map(|(identity, event)| {
                        (
                            event.revision,
                            source.as_str().to_owned(),
                            source.clone(),
                            identity.clone(),
                        )
                    })
            })
            .collect();
        untracked.sort_by(|left, right| {
            (&left.0, &left.1, &left.3).cmp(&(&right.0, &right.1, &right.3))
        });
        let remove = untracked
            .len()
            .saturating_sub(DETAIL_LIFECYCLE_TOMBSTONE_CAP);
        for (_, _, source, identity) in untracked.into_iter().take(remove) {
            if let Some(source) = self.detail_lifecycles.get_mut(&source) {
                source.entries.remove(&identity);
            }
        }
    }

    /// Cancel one correlated primary request without orphaning it when the
    /// bounded outbound queue cannot accept the cancellation frame.
    fn cancel_detail_request(&mut self, identity: &ResourceIdentity) -> bool {
        let Some(request) = self
            .detail_requests
            .get(identity)
            .map(|pending| pending.request.clone())
        else {
            return true;
        };
        if self.client.cancel(&request).is_err() {
            return false;
        }
        self.detail_requests.remove(identity);
        let _ = self.client.take(request.clone());
        let _ = self.client.take_failure(request);
        true
    }

    /// Relation equivalent of [`Self::cancel_detail_request`].
    fn cancel_relation_request(&mut self, identity: &ResourceIdentity) -> bool {
        let Some(request) = self
            .relation_requests
            .get(identity)
            .map(|pending| pending.request.clone())
        else {
            return true;
        };
        if self.client.cancel(&request).is_err() {
            return false;
        }
        self.relation_requests.remove(identity);
        let _ = self.client.take(request.clone());
        let _ = self.client.take_failure(request);
        true
    }

    /// Metrics equivalent of [`Self::cancel_detail_request`].
    fn cancel_metric_request(&mut self, identity: &ResourceIdentity) -> bool {
        let Some(request) = self
            .metric_requests
            .get(identity)
            .map(|pending| pending.request.clone())
        else {
            return true;
        };
        if self.client.cancel(&request).is_err() {
            return false;
        }
        self.metric_requests.remove(identity);
        let _ = self.client.take(request.clone());
        let _ = self.client.take_failure(request);
        true
    }

    fn refresh_details_at(&mut self, now_ms: u64) {
        // Until bootstrap identifies the server's authoritative context, the
        // workspace may still contain selections from the lost generation.
        // Do not issue a detail read against that stale context; bootstrap
        // either commits a new context (clearing the selection) or confirms
        // the existing one before recovery ends.
        if self.recovering {
            return;
        }
        let mut pinned: Vec<ResourceIdentity> = Vec::new();
        for window in self.shell.workspace().windows() {
            let identity = match &window.content {
                crate::workspace::WindowContent::Detail(detail) => {
                    detail.identity.as_row_identity()
                }
                crate::workspace::WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .and_then(|d| d.identity.as_row_identity()),
                crate::workspace::WindowContent::Services(service) => service
                    .detail
                    .as_ref()
                    .and_then(|d| d.identity.as_row_identity()),
                crate::workspace::WindowContent::PortForwards(_) => None,
            };
            if let Some(identity) = identity
                && !pinned.iter().any(|known| known == identity)
            {
                pinned.push(identity.clone());
            }
        }
        let pinned_set: std::collections::BTreeSet<_> = pinned.iter().cloned().collect();
        for identity in self
            .detail_requests
            .keys()
            .filter(|identity| !pinned_set.contains(*identity))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.cancel_detail_request(&identity);
        }
        for identity in self
            .relation_requests
            .keys()
            .filter(|identity| !pinned_set.contains(*identity))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.cancel_relation_request(&identity);
        }
        self.details
            .retain(|identity, _| pinned_set.contains(identity));
        self.primary_details
            .retain(|identity, _| pinned_set.contains(identity));
        self.relations
            .retain(|identity, _| pinned_set.contains(identity));
        let desired = pinned
            .into_iter()
            .filter(|identity| {
                self.client.local_ui().selected_context.as_deref()
                    == Some(identity.context.as_str())
                    && !self.primary_details.contains_key(identity)
            })
            .collect::<Vec<_>>();
        for identity in desired {
            if self.detail_requests.contains_key(&identity) {
                continue;
            }
            match self.client.begin(Query::ResourceDetail(identity.clone())) {
                Ok(request) => {
                    self.primary_details
                        .insert(identity.clone(), PrimaryDetailState::Loading);
                    self.detail_requests.insert(
                        identity.clone(),
                        PendingResourceRequest {
                            request,
                            context: identity.context,
                            generation: self.resource_generation,
                        },
                    );
                }
                Err(_) => {
                    self.primary_details.insert(
                        identity,
                        PrimaryDetailState::Failed(SafeUiError::new("could not request details")),
                    );
                    continue;
                }
            }
        }
        // Collect completed detail responses.
        let completed: Vec<ResourceIdentity> = self
            .detail_requests
            .iter()
            .filter(|(_, pending)| !self.client.is_pending(&pending.request))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in completed {
            if let Some(pending) = self.detail_requests.remove(&identity) {
                let current_context = self.client.local_ui().selected_context.as_deref();
                if pending.generation != self.resource_generation
                    || current_context != Some(pending.context.as_str())
                {
                    let _ = self.client.take(pending.request);
                    self.details.remove(&identity);
                    self.primary_details.remove(&identity);
                    continue;
                }
                if let Some(QueryResult::ResourceDetail(response)) =
                    self.client.take(pending.request.clone())
                {
                    let response = *response;
                    if response.identity == identity {
                        self.details.insert(identity.clone(), response.clone());
                        self.primary_details
                            .insert(identity, PrimaryDetailState::Loaded(response));
                    }
                } else if let Some(failure) = self.client.take_failure(pending.request) {
                    self.primary_details.insert(
                        identity,
                        PrimaryDetailState::Failed(SafeUiError::new(failure.safe_message)),
                    );
                }
            }
        }

        self.refresh_metrics(now_ms, &pinned_set);
        self.refresh_relations(now_ms);
        self.flush_outbound()
            .map_err(|error| ClientError::Protocol(format!("{error:?}")))
            .ok();
    }

    fn refresh_metrics(
        &mut self,
        now_ms: u64,
        pinned: &std::collections::BTreeSet<ResourceIdentity>,
    ) {
        const METRICS_TTL_MS: u64 = 30_000;

        let desired: std::collections::BTreeSet<_> = pinned
            .iter()
            .filter(|identity| {
                identity.gvk.group.is_empty()
                    && identity.gvk.version == "v1"
                    && identity.gvk.kind == "Pod"
                    && self.client.local_ui().selected_context.as_deref()
                        == Some(identity.context.as_str())
                    && !self.resource_lifecycle_is_gone(identity)
            })
            .cloned()
            .collect();

        for identity in self
            .metric_requests
            .keys()
            .filter(|identity| !desired.contains(*identity))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.cancel_metric_request(&identity);
        }
        self.metrics
            .retain(|identity, _| desired.contains(identity));
        self.metric_checked_at
            .retain(|identity, _| desired.contains(identity));

        for identity in &desired {
            if self.metric_requests.contains_key(identity) {
                continue;
            }
            let due = self
                .metric_checked_at
                .get(identity)
                .is_none_or(|checked_at| now_ms.saturating_sub(*checked_at) >= METRICS_TTL_MS);
            if !due {
                continue;
            }
            // Metrics are useful only while fresh. A TTL expiry or failed
            // replacement therefore exposes absence instead of stale values.
            self.metrics.remove(identity);
            match self.client.begin(Query::ResourceMetrics(identity.clone())) {
                Ok(request) => {
                    self.metric_requests.insert(
                        identity.clone(),
                        PendingResourceRequest {
                            request,
                            context: identity.context.clone(),
                            generation: self.resource_generation,
                        },
                    );
                }
                Err(_) => {
                    self.metric_checked_at.insert(identity.clone(), now_ms);
                }
            }
        }

        let completed: Vec<_> = self
            .metric_requests
            .iter()
            .filter(|(_, pending)| !self.client.is_pending(&pending.request))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in completed {
            let Some(pending) = self.metric_requests.remove(&identity) else {
                continue;
            };
            if pending.generation != self.resource_generation
                || self.client.local_ui().selected_context.as_deref()
                    != Some(pending.context.as_str())
                || !desired.contains(&identity)
            {
                let _ = self.client.take(pending.request.clone());
                let _ = self.client.take_failure(pending.request);
                self.metrics.remove(&identity);
                self.metric_checked_at.remove(&identity);
                continue;
            }
            if let Some(QueryResult::ResourceMetrics(response)) =
                self.client.take(pending.request.clone())
            {
                let response = *response;
                if response.identity == identity {
                    self.metrics.insert(identity.clone(), response);
                    self.metric_checked_at.insert(identity, now_ms);
                }
            } else {
                let _ = self.client.take_failure(pending.request);
                self.metrics.remove(&identity);
                self.metric_checked_at.insert(identity, now_ms);
            }
        }
    }

    fn resource_lifecycle_is_gone(&self, identity: &ResourceIdentity) -> bool {
        let newest = self
            .detail_lifecycles
            .values()
            .filter_map(|source| source.entries.get(identity))
            .map(|event| event.revision)
            .max();
        newest.is_some_and(|newest| {
            self.detail_lifecycles.values().any(|source| {
                source.entries.get(identity).is_some_and(|event| {
                    event.revision == newest && event.lifecycle == DetailLifecycle::Gone
                })
            })
        })
    }

    fn refresh_relations(&mut self, now_ms: u64) {
        const RELATIONS_TTL_MS: u64 = 30_000;
        let current_context = self.client.local_ui().selected_context.as_deref();
        let wants_relations = |detail: &crate::workspace::DetailState<ResourceIdentity>| {
            detail.active_tab == crate::workspace::DetailTab::Pods
                || (detail.active_tab == crate::workspace::DetailTab::Overview
                    && detail.identity.gvk.group == "apps"
                    && detail.identity.gvk.version == "v1"
                    && detail.identity.gvk.kind == "Deployment")
        };
        let desired: std::collections::BTreeSet<ResourceIdentity> = self
            .shell
            .workspace()
            .windows()
            .iter()
            .filter_map(|window| match &window.content {
                crate::workspace::WindowContent::Detail(detail) if wants_relations(detail) => {
                    detail.identity.as_row_identity().cloned()
                }
                crate::workspace::WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .filter(|detail| wants_relations(detail))
                    .and_then(|detail| detail.identity.as_row_identity())
                    .cloned(),
                _ => None,
            })
            .filter(|identity| current_context == Some(identity.context.as_str()))
            .collect();

        for identity in desired {
            if self.relation_requests.contains_key(&identity) {
                continue;
            }
            let needs_request = match self.relations.get(&identity) {
                None | Some(RelationState::NotRequested) => true,
                Some(RelationState::Loaded {
                    loaded_at_ms,
                    refresh_error,
                    ..
                }) => {
                    refresh_error.is_none()
                        && now_ms.saturating_sub(*loaded_at_ms) >= RELATIONS_TTL_MS
                }
                Some(RelationState::Loading | RelationState::Failed(_)) => false,
            };
            if !needs_request {
                continue;
            }
            let refreshing_stale = matches!(
                self.relations.get(&identity),
                Some(RelationState::Loaded { .. })
            );
            if let Some(RelationState::Loaded {
                refreshing,
                refresh_error,
                ..
            }) = self.relations.get_mut(&identity)
            {
                *refreshing = true;
                *refresh_error = None;
            } else {
                self.relations
                    .insert(identity.clone(), RelationState::Loading);
            }
            let request = match self
                .client
                .begin(Query::ResourceRelations(identity.clone()))
            {
                Ok(request) => request,
                Err(_) => {
                    let error = SafeUiError::new("could not request related resources");
                    if refreshing_stale {
                        if let Some(RelationState::Loaded {
                            refreshing,
                            refresh_error,
                            ..
                        }) = self.relations.get_mut(&identity)
                        {
                            *refreshing = false;
                            *refresh_error = Some(error);
                        }
                    } else {
                        self.relations
                            .insert(identity, RelationState::Failed(error));
                    }
                    continue;
                }
            };
            self.relation_requests.insert(
                identity.clone(),
                PendingResourceRequest {
                    request,
                    context: identity.context,
                    generation: self.resource_generation,
                },
            );
        }

        let completed: Vec<_> = self
            .relation_requests
            .iter()
            .filter(|(_, pending)| !self.client.is_pending(&pending.request))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in completed {
            let Some(pending) = self.relation_requests.remove(&identity) else {
                continue;
            };
            if pending.generation != self.resource_generation
                || self.client.local_ui().selected_context.as_deref()
                    != Some(pending.context.as_str())
            {
                let _ = self.client.take(pending.request);
                self.relations.remove(&identity);
                continue;
            }
            if let Some(QueryResult::ResourceRelations(response)) =
                self.client.take(pending.request.clone())
            {
                let response = *response;
                if response.identity == identity {
                    self.relations.insert(
                        identity,
                        RelationState::Loaded {
                            response: std::sync::Arc::new(response),
                            loaded_at_ms: now_ms,
                            refreshing: false,
                            refresh_error: None,
                        },
                    );
                }
            } else if let Some(failure) = self.client.take_failure(pending.request) {
                let error = SafeUiError::new(failure.safe_message);
                match self.relations.get_mut(&identity) {
                    Some(RelationState::Loaded {
                        refreshing,
                        refresh_error,
                        ..
                    }) => {
                        *refreshing = false;
                        *refresh_error = Some(error);
                    }
                    _ => {
                        self.relations
                            .insert(identity, RelationState::Failed(error));
                    }
                }
            }
        }
    }

    fn reconnect_if_due(&mut self, now_ms: u64, entropy: u64) {
        if Self::terminal_phase(self.client.phase()) || self.connection.is_some() {
            return;
        }
        match self.client.retry_if_due(now_ms) {
            Ok(true) => match self.factory.connect(&self.connection_url) {
                Ok(connection) => {
                    self.revoke_external_shell();
                    self.transport_open = false;
                    self.connection = Some(connection);
                }
                Err(_) => self.transient_loss(now_ms, entropy),
            },
            Ok(false) => {}
            Err(error) => self.terminal_failure(error.to_string()),
        }
    }

    fn terminal_failure(&mut self, message: String) {
        if let Some(mut connection) = self.connection.take() {
            connection.close();
        }
        self.transport_open = false;
        self.revoke_external_shell();
        self.teardown_stream_sessions();
        self.reset_port_forward_ui(Some("Connection lost; submit again"));
        if let Some(subscription) = self.infrastructure_subscription.take() {
            let _ = self.client.unsubscribe(&subscription);
        }
        if let Some(request) = self.infrastructure_request.take() {
            let _ = self.client.cancel(&request);
        }
        self.infrastructure_context = None;
        self.view = AppView::Failed { message };
    }

    /// The pod/container a stream tool of `window` must be bound to,
    /// derived from the CURRENT workspace detail identity (integrated
    /// resource windows carry their detail inside the resource state;
    /// dedicated windows are Detail directly). This is the authoritative
    /// target even when the rendered tool cache lags one frame behind.
    fn workspace_stream_target(&self, window: WindowId) -> Option<StreamTarget> {
        let detail = self
            .shell
            .workspace()
            .window(window)
            .and_then(|w| match &w.content {
                crate::workspace::WindowContent::Detail(detail) => Some(detail),
                crate::workspace::WindowContent::Resource(resource) => resource.detail.as_ref(),
                crate::workspace::WindowContent::Services(service) => service.detail.as_ref(),
                crate::workspace::WindowContent::PortForwards(_) => None,
            })?;
        let identity = k10s_ui_row_identity(&detail.identity)?;
        let runtime = self
            .details
            .get(identity)
            .and_then(|view| crate::ui::PodRuntimeProjection::from_view(identity, view))?;
        Some(StreamTarget {
            context: identity.context.clone(),
            namespace: identity
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_owned()),
            pod: identity.name.clone(),
            uid: identity.uid.clone(),
            container: runtime.default_container().to_owned(),
        })
    }

    /// Resolve the authoritative target for a live route. Logs retain their
    /// per-window container choice while the workspace remains on the same
    /// pod; exec continues to use the typed projection's default container.
    fn current_stream_target(&self, window: WindowId, route: StreamRoute) -> Option<StreamTarget> {
        let workspace_target = self.workspace_stream_target(window)?;
        if route == StreamRoute::Logs
            && let Some(selected) = self.shell.stream_stores().logs.target_of(window)
            && crate::ui::tools::logs::same_workload(&selected, &workspace_target)
        {
            return Some(selected);
        }
        Some(workspace_target)
    }

    /// Apply workspace events produced by command application. Only the
    /// focus-raising events matter at this layer, and guard-resolved switch
    /// requests are staged toward the backend as explicit user actions.
    fn handle_workspace_event(&mut self, event: WorkspaceEvent<ResourceIdentity>) {
        match event {
            WorkspaceEvent::ContextSwitchRequested { to } => {
                if let Err(error) = self.stage_context_switch(&to, true) {
                    self.terminal_failure(error.to_string());
                }
            }
            WorkspaceEvent::ContextSwitched { .. } => self.retire_resource_context(),
            _ => {}
        }
        self.prune_detail_lifecycles();
    }

    fn retire_resource_context(&mut self) {
        for pending in std::mem::take(&mut self.pending_port_forwards) {
            if self.client.is_pending(&pending.request) {
                let _ = self.client.cancel(&pending.request);
            } else {
                let _ = self.client.take(pending.request.clone());
                let _ = self.client.take_failure(pending.request);
            }
        }
        self.reset_port_forward_ui(None);
        for identity in self.detail_requests.keys().cloned().collect::<Vec<_>>() {
            self.cancel_detail_request(&identity);
        }
        for identity in self.relation_requests.keys().cloned().collect::<Vec<_>>() {
            self.cancel_relation_request(&identity);
        }
        for identity in self.metric_requests.keys().cloned().collect::<Vec<_>>() {
            self.cancel_metric_request(&identity);
        }
        self.details.clear();
        self.detail_lifecycles.clear();
        self.primary_details.clear();
        self.relations.clear();
        self.metrics.clear();
        self.metric_checked_at.clear();
        self.resource_generation = self.resource_generation.wrapping_add(1);
    }

    fn reset_port_forward_ui(&mut self, modal_error: Option<&str>) {
        self.pending_port_forwards.clear();
        self.port_forward_retry_errors.clear();
        self.port_forward_error = None;
        if let Some(message) = modal_error {
            self.shell.port_forward_start_failed(message);
        } else {
            self.shell.dismiss_port_forward_start();
        }
    }

    /// Close every dedicated stream session and mark log tools disconnected.
    fn teardown_stream_sessions(&mut self) {
        for (_, mut session) in std::mem::take(&mut self.stream_sessions) {
            session.disconnect();
        }
        for (_, mut session) in std::mem::take(&mut self.aggregate_log_sessions) {
            session.disconnect();
        }
        self.pending_stream_tickets.clear();
        self.log_sources.clear();
        self.log_session_sources.clear();
        self.log_generations.clear();
        self.log_session_generations.clear();
        self.shell.stream_stores_mut().connection_lost();
    }

    fn desired_log_source(&self, window: WindowId) -> Option<LogSource> {
        let target = self.current_stream_target(window, StreamRoute::Logs)?;
        let view = self.shell.stream_stores().logs.get(window)?;
        Some(LogSource {
            target,
            since_seconds: view.since_seconds(),
            previous: view.previous(),
        })
    }

    /// Retire source modes that no longer match their owning log view and
    /// replace them with one ticket for the full current mode.
    fn reconcile_log_sources(
        &mut self,
        mut requested: BTreeMap<WindowId, LogSource>,
    ) -> Result<(), ClientError> {
        let stale_pending = self
            .pending_stream_tickets
            .iter()
            .filter(|(_, pending)| pending.route == StreamRoute::Logs)
            .filter(|(_, pending)| {
                pending.log_source.as_ref() != self.desired_log_source(pending.window).as_ref()
            })
            .map(|(id, pending)| (id.clone(), pending.window))
            .collect::<Vec<_>>();
        let mut restart = std::collections::BTreeSet::new();
        for (id, window) in stale_pending {
            let Some(request) = self
                .pending_stream_tickets
                .get(&id)
                .map(|pending| pending.request.clone())
            else {
                continue;
            };
            self.client.cancel(&request)?;
            self.pending_stream_tickets.remove(&id);
            let _ = self.client.take(request.clone());
            let _ = self.client.take_failure(request);
            restart.insert(window);
        }

        let stale_sessions = self
            .stream_sessions
            .iter()
            .filter(|((_, route), _)| *route == StreamRoute::Logs)
            .filter_map(|((window, _), session)| {
                let desired = self.desired_log_source(*window);
                let stale = self.log_session_sources.get(window).map_or_else(
                    || desired.as_ref().map(|source| &source.target) != Some(session.target()),
                    |current| desired.as_ref() != Some(current),
                );
                stale.then_some(*window)
            })
            .collect::<Vec<_>>();
        for window in stale_sessions {
            if let Some(mut session) = self.stream_sessions.remove(&(window, StreamRoute::Logs)) {
                session.disconnect();
            }
            self.log_session_sources.remove(&window);
            restart.insert(window);
        }

        for window in restart {
            if let Some(source) = self.desired_log_source(window) {
                requested.entry(window).or_insert(source);
            }
        }
        for (window, source) in requested {
            if self.desired_log_source(window).as_ref() != Some(&source) {
                continue;
            }
            let already_pending = self.pending_stream_tickets.values().any(|pending| {
                pending.window == window && pending.log_source.as_ref() == Some(&source)
            });
            let already_live = self
                .log_session_sources
                .get(&window)
                .is_some_and(|current| current == &source)
                && self
                    .stream_sessions
                    .contains_key(&(window, StreamRoute::Logs));
            if already_pending || already_live {
                continue;
            }
            let request = self.client.begin(Query::StreamTicket {
                target: source.target.clone(),
                since_seconds: source.since_seconds,
                previous: source.previous,
            })?;
            let generation = self
                .log_generations
                .get(&window)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("log attempt generation exhausted");
            self.log_sources.insert(window, source.clone());
            self.log_generations.insert(window, generation);
            if let Some(view) = self.shell.stream_stores_mut().logs.get_mut(window) {
                view.connect();
            }
            self.pending_stream_tickets.insert(
                request.id().clone(),
                PendingStreamTicket {
                    request,
                    route: StreamRoute::Logs,
                    window,
                    log_source: Some(source),
                    log_generation: Some(generation),
                    aggregate_target: None,
                },
            );
        }
        Ok(())
    }

    /// Reconcile live log sessions against the authoritative workspace state.
    fn reconcile_sessions(&mut self) {
        let mut stale: Vec<((WindowId, StreamRoute), &'static str)> = Vec::new();
        for (key, session) in self.stream_sessions.iter() {
            let (window, route) = *key;
            let target_matches =
                self.current_stream_target(window, route).as_ref() == Some(session.target());
            if !target_matches {
                stale.push((*key, "the log target changed"));
            }
        }
        for ((window, route), reason) in stale {
            if let Some(mut session) = self.stream_sessions.remove(&(window, route)) {
                session.disconnect();
            }
            if route == StreamRoute::Logs {
                self.log_session_sources.remove(&window);
            }
            let stores = self.shell.stream_stores_mut();
            if let Some(view) = stores.logs.get_mut(window) {
                view.fail(reason);
            }
        }
    }

    fn terminal_phase(phase: ClientPhase) -> bool {
        matches!(
            phase,
            ClientPhase::WebGate | ClientPhase::UpgradeRequired | ClientPhase::Closed
        )
    }

    /// Drain rendering-time log stream actions.
    fn process_stream_requests(&mut self) -> Result<(), ClientError> {
        let log_actions = self.shell.drain_log_actions();
        self.reconcile_aggregate_log_sessions()?;
        if log_actions.is_empty() {
            self.reconcile_log_sources(BTreeMap::new())?;
        }
        for (window, action) in log_actions {
            let crate::ui::tools::LogsAction::OpenLogs {
                target,
                since_seconds,
                previous,
                ..
            } = action
            else {
                let crate::ui::tools::LogsAction::OpenAggregateLogs {
                    targets,
                    since_seconds,
                    ..
                } = action
                else {
                    unreachable!()
                };
                self.start_aggregate_log_streams(window, targets, since_seconds)?;
                continue;
            };
            let source = LogSource {
                target: target.clone(),
                since_seconds,
                previous,
            };
            let generation = self
                .log_generations
                .get(&window)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("log attempt generation exhausted");
            for entry in self.pending_stream_tickets.values() {
                if entry.window == window
                    && entry.route == StreamRoute::Logs
                    && let Err(error) = self.client.cancel(&entry.request)
                {
                    self.restore_log_view_for_existing_session(window);
                    return Err(error);
                }
            }
            let request = match self.client.begin(Query::StreamTicket {
                target,
                since_seconds,
                previous,
            }) {
                Ok(request) => request,
                Err(error) => {
                    self.restore_log_view_for_existing_session(window);
                    return Err(error);
                }
            };
            // Nothing below this point is fallible: the replacement ticket
            // and generation become authoritative together.
            if let Some(mut session) = self.stream_sessions.remove(&(window, StreamRoute::Logs)) {
                session.disconnect();
            }
            self.log_session_sources.remove(&window);
            self.log_session_generations.remove(&window);
            self.log_sources.insert(window, source.clone());
            self.log_generations.insert(window, generation);
            // The tool moves to Connecting immediately; the Ready signal
            // completes the attach.
            if let Some(view) = self.shell.stream_stores_mut().logs.get_mut(window) {
                view.connect();
            }
            self.pending_stream_tickets.insert(
                request.id().clone(),
                PendingStreamTicket {
                    request,
                    route: StreamRoute::Logs,
                    window,
                    log_source: Some(source),
                    log_generation: Some(generation),
                    aggregate_target: None,
                },
            );
        }
        self.flush_outbound()
            .map_err(|error| ClientError::Protocol(format!("{error:?}")))
    }

    fn restore_log_view_for_existing_session(&mut self, window: WindowId) {
        if self
            .stream_sessions
            .contains_key(&(window, StreamRoute::Logs))
            && let Some(view) = self.shell.stream_stores_mut().logs.get_mut(window)
        {
            view.attach();
        }
    }

    fn start_aggregate_log_streams(
        &mut self,
        window: WindowId,
        targets: Vec<StreamTarget>,
        since_seconds: Option<i64>,
    ) -> Result<(), ClientError> {
        let stale_requests = self
            .pending_stream_tickets
            .iter()
            .filter(|(_, entry)| entry.window == window && entry.aggregate_target.is_some())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in stale_requests {
            if let Some(entry) = self.pending_stream_tickets.remove(&id) {
                self.client.cancel(&entry.request)?;
            }
        }
        let stale_sessions = self
            .aggregate_log_sessions
            .keys()
            .filter(|key| key.0 == window)
            .cloned()
            .collect::<Vec<_>>();
        for key in stale_sessions {
            if let Some(mut session) = self.aggregate_log_sessions.remove(&key) {
                session.disconnect();
            }
        }
        if let Some(view) = self.shell.stream_stores_mut().logs.get_mut(window) {
            view.connect();
        }
        for target in targets {
            let request = self.client.begin(Query::StreamTicket {
                target: target.clone(),
                since_seconds,
                previous: false,
            })?;
            self.pending_stream_tickets.insert(
                request.id().clone(),
                PendingStreamTicket {
                    request,
                    route: StreamRoute::Logs,
                    window,
                    log_source: None,
                    log_generation: None,
                    aggregate_target: Some(target),
                },
            );
        }
        Ok(())
    }

    fn reconcile_aggregate_log_sessions(&mut self) -> Result<(), ClientError> {
        let stale_requests = self
            .pending_stream_tickets
            .iter()
            .filter(|(_, entry)| {
                entry.aggregate_target.as_ref().is_some_and(|target| {
                    !self.aggregate_log_target_is_current(entry.window, target)
                })
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in stale_requests {
            if let Some(entry) = self.pending_stream_tickets.remove(&id) {
                self.client.cancel(&entry.request)?;
            }
        }
        let stale_sessions = self
            .aggregate_log_sessions
            .iter()
            .filter(|(key, session)| !self.aggregate_log_target_is_current(key.0, session.target()))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in stale_sessions {
            if let Some(mut session) = self.aggregate_log_sessions.remove(&key) {
                session.disconnect();
            }
        }
        Ok(())
    }

    fn aggregate_log_target_is_current(&self, window: WindowId, target: &StreamTarget) -> bool {
        let active = self
            .shell
            .workspace()
            .window(window)
            .and_then(|window| match &window.content {
                crate::workspace::WindowContent::Detail(detail) => Some(detail),
                crate::workspace::WindowContent::Resource(resource) => resource.detail.as_ref(),
                crate::workspace::WindowContent::Services(service) => service.detail.as_ref(),
                crate::workspace::WindowContent::PortForwards(_) => None,
            })
            .is_some_and(|detail| detail.active_tab == crate::workspace::DetailTab::Logs);
        active
            && self
                .shell
                .stream_stores()
                .logs
                .aggregate_targets(window)
                .is_some_and(|targets| targets.contains(target))
    }

    fn aggregate_log_sources_exhausted(&self, window: WindowId) -> bool {
        !self
            .pending_stream_tickets
            .values()
            .any(|entry| entry.window == window && entry.aggregate_target.is_some())
            && !self
                .aggregate_log_sessions
                .keys()
                .any(|key| key.0 == window)
    }

    /// Complete in-flight stream ticket requests and open their sockets.
    fn finish_stream_tickets(&mut self) {
        while let Some(id) = self
            .pending_stream_tickets
            .iter()
            .find(|(_, entry)| !self.client.is_pending(&entry.request))
            .map(|(id, _)| id.clone())
        {
            let Some(entry) = self.pending_stream_tickets.remove(&id) else {
                unreachable!("key came from this map");
            };
            let PendingStreamTicket {
                request,
                route,
                window,
                log_source,
                log_generation,
                aggregate_target,
            } = entry;
            if let Some(target) = aggregate_target {
                let still_current = self
                    .shell
                    .stream_stores()
                    .logs
                    .aggregate_targets(window)
                    .is_some_and(|targets| targets.contains(&target));
                if !still_current {
                    let _ = self.client.take(request);
                    continue;
                }
                if let Some(QueryResult::StreamTicket(granted)) = self.client.take(request) {
                    let mut session = StreamSession::new(StreamRoute::Logs, granted.target.clone());
                    match session.open_with_ticket(
                        &self.connection_url,
                        &self.access_token,
                        &granted.ticket_id,
                    ) {
                        Ok(()) => {
                            self.aggregate_log_sessions
                                .insert(aggregate_session_key(window, &granted.target), session);
                        }
                        Err(error) => {
                            if self.aggregate_log_sources_exhausted(window)
                                && let Some(view) =
                                    self.shell.stream_stores_mut().logs.get_mut(window)
                            {
                                view.fail(&format!(
                                    "could not open any related log stream: {error}"
                                ));
                            }
                        }
                    }
                }
                continue;
            }
            let source_current = log_source.as_ref().is_none_or(|source| {
                self.log_sources.get(&window) == Some(source)
                    && log_generation == self.log_generations.get(&window).copied()
            });
            if !source_current {
                let _ = self.client.take(request);
                continue;
            }
            if let Some(QueryResult::StreamTicket(granted)) = self.client.take(request) {
                let result = session_open(
                    &mut self.stream_sessions,
                    window,
                    route,
                    *granted,
                    &self.connection_url,
                    &self.access_token,
                );
                match result {
                    Ok(()) => {
                        if let Some(source) = log_source {
                            self.log_session_sources.insert(window, source);
                            self.log_session_generations.insert(
                                window,
                                log_generation.expect("log tickets carry an attempt generation"),
                            );
                        }
                    }
                    Err(error) => {
                        let reason = format!("could not open stream socket: {error}");
                        fail_stream_tool(&mut self.shell, window, route, &reason);
                    }
                }
            }
        }
    }

    /// Project dedicated-socket events into the connected tools.
    fn poll_stream_sessions(&mut self) {
        if let Err(error) = self.reconcile_log_sources(BTreeMap::new()) {
            self.terminal_failure(error.to_string());
            return;
        }
        if self.flush_outbound().is_err() {
            self.terminal_failure("could not send stream request".to_owned());
            return;
        }
        self.finish_stream_tickets();
        self.poll_aggregate_log_sessions();
        self.reconcile_sessions();
        let keys: Vec<(WindowId, StreamRoute)> = self.stream_sessions.keys().copied().collect();
        for key in keys {
            let (window, route) = key;
            let Some(session) = self.stream_sessions.get_mut(&key) else {
                continue;
            };
            let signals = session.poll();
            if signals.is_empty() {
                continue;
            }
            // The workspace identity is the authority for attach: a Ready
            // that arrives after the selection moved on must not engage
            // the new pod's guard or attach the old session.
            let bound_target = session.target().clone();
            let target_current =
                self.current_stream_target(window, route).as_ref() == Some(&bound_target);
            let source_current = route != StreamRoute::Logs
                || (self.log_session_sources.get(&window) == self.log_sources.get(&window)
                    && self.log_session_generations.get(&window)
                        == self.log_generations.get(&window));
            let target_current = target_current && source_current;
            let stores = self.shell.stream_stores_mut();
            {
                for signal in signals {
                    match signal {
                        StreamSignal::Ready { .. } => {
                            if !target_current {
                                continue;
                            }
                            if let Some(view) = stores.logs.get_mut(window) {
                                view.attach();
                            }
                        }
                        StreamSignal::Output(text) => {
                            if let Some(view) = stores.logs.get_mut(window) {
                                view.append(&text);
                            }
                        }
                        StreamSignal::Rejected(reason) => {
                            if let Some(view) = stores.logs.get_mut(window) {
                                view.fail(&reason);
                            }
                            self.stream_sessions.remove(&key);
                            self.log_session_sources.remove(&window);
                        }
                    }
                }
            }
        }
    }

    fn poll_aggregate_log_sessions(&mut self) {
        let keys = self
            .aggregate_log_sessions
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let Some(session) = self.aggregate_log_sessions.get_mut(&key) else {
                continue;
            };
            let target = session.target().clone();
            let signals = session.poll();
            let mut rejected = false;
            if let Some(view) = self.shell.stream_stores_mut().logs.get_mut(key.0) {
                for signal in signals {
                    match signal {
                        StreamSignal::Ready { .. } => view.attach(),
                        StreamSignal::Output(text) => {
                            for line in text.lines() {
                                view.append(&format!(
                                    "[{}/{}] {line}",
                                    target.pod, target.container
                                ));
                            }
                        }
                        StreamSignal::Rejected(reason) => {
                            view.append(&format!(
                                "[{}/{}] stream unavailable: {reason}",
                                target.pod, target.container
                            ));
                            rejected = true;
                        }
                    }
                }
            }
            if rejected {
                self.aggregate_log_sessions.remove(&key);
                if self.aggregate_log_sources_exhausted(key.0)
                    && let Some(view) = self.shell.stream_stores_mut().logs.get_mut(key.0)
                {
                    view.fail("all related log streams disconnected");
                }
            }
        }
    }
}

/// Extract the immutable identity/revision carried by a resource delta. The
/// caller projects it only after ClientState proves that exact revision was
/// accepted for the owning subscription, so stale/out-of-order deltas cannot
/// invalidate a still-current validation ticket.
fn resource_delta_projection(
    frame: &ServerFrame,
) -> Option<(
    k10s_protocol::SubscriptionId,
    ResourceIdentity,
    k10s_protocol::BackendRevision,
    DetailLifecycle,
)> {
    let subscription = frame.subscription_id.clone()?;
    let k10s_protocol::ServerPayload::Event(event) = frame.decode_payload().ok()? else {
        return None;
    };
    match event.event_kind.as_str() {
        k10s_protocol::RESOURCE_EVENT_CHANGED => {
            let delta: k10s_protocol::ResourceChanged =
                serde_json::from_value(event.payload).ok()?;
            Some((
                subscription,
                delta.identity,
                delta.row.revision,
                DetailLifecycle::Present,
            ))
        }
        k10s_protocol::RESOURCE_EVENT_GONE => {
            let delta: k10s_protocol::ResourceGone = serde_json::from_value(event.payload).ok()?;
            Some((
                subscription,
                delta.identity,
                delta.revision,
                DetailLifecycle::Gone,
            ))
        }
        _ => None,
    }
}

/// The wire identity of each built-in workload kind. Custom resources are
/// picker-driven and have no single GVK.
fn builtin_kind_gvk(kind: WorkloadKind) -> Option<(&'static str, &'static str, &'static str)> {
    match kind {
        WorkloadKind::Events => Some(("", "v1", "Event")),
        WorkloadKind::Namespaces => Some(("", "v1", "Namespace")),
        WorkloadKind::Deployments => Some(("apps", "v1", "Deployment")),
        WorkloadKind::StatefulSets => Some(("apps", "v1", "StatefulSet")),
        WorkloadKind::DaemonSets => Some(("apps", "v1", "DaemonSet")),
        WorkloadKind::Jobs => Some(("batch", "v1", "Job")),
        WorkloadKind::CronJobs => Some(("batch", "v1", "CronJob")),
        WorkloadKind::Pods => Some(("", "v1", "Pod")),
        WorkloadKind::CustomResources => None,
        WorkloadKind::Ingresses => Some(("networking.k8s.io", "v1", "Ingress")),
        WorkloadKind::Endpoints => Some(("", "v1", "Endpoints")),
        WorkloadKind::NetworkPolicies => Some(("networking.k8s.io", "v1", "NetworkPolicy")),
        WorkloadKind::ConfigMaps => Some(("", "v1", "ConfigMap")),
        WorkloadKind::Secrets => Some(("", "v1", "Secret")),
        WorkloadKind::PersistentVolumeClaims => Some(("", "v1", "PersistentVolumeClaim")),
        WorkloadKind::PersistentVolumes => Some(("", "v1", "PersistentVolume")),
        WorkloadKind::StorageClasses => Some(("storage.k8s.io", "v1", "StorageClass")),
        WorkloadKind::ServiceAccounts => Some(("", "v1", "ServiceAccount")),
        WorkloadKind::Roles => Some(("rbac.authorization.k8s.io", "v1", "Role")),
        WorkloadKind::RoleBindings => Some(("rbac.authorization.k8s.io", "v1", "RoleBinding")),
    }
}

/// Recover the protocol identity behind a workspace row identity.
fn k10s_ui_row_identity(identity: &ResourceIdentity) -> Option<&ResourceIdentity> {
    RowIdentity::as_row_identity(identity)
}

/// Open a dedicated socket with a granted ticket and register the session
/// under its owning window. Failures leave no half-open state behind.
fn session_open(
    sessions: &mut BTreeMap<(WindowId, StreamRoute), StreamSession>,
    window: WindowId,
    route: StreamRoute,
    granted: k10s_protocol::StreamTicketResponse,
    connection_url: &str,
    access_token: &str,
) -> Result<(), TransportError> {
    let mut session = StreamSession::new(route, granted.target.clone());
    session.open_with_ticket(connection_url, access_token, &granted.ticket_id)?;
    sessions.insert((window, route), session);
    Ok(())
}

fn aggregate_session_key(
    window: WindowId,
    target: &StreamTarget,
) -> (WindowId, String, String, String, String) {
    (
        window,
        target.namespace.clone(),
        target.pod.clone(),
        target.uid.clone(),
        target.container.clone(),
    )
}

/// Return one failed dedicated-socket open to the tool that requested it.
/// The control connection remains healthy and the tool stays reconnectable.
fn fail_stream_tool(
    shell: &mut UiShell<ResourceIdentity>,
    window: WindowId,
    route: StreamRoute,
    reason: &str,
) {
    let _ = route;
    if let Some(view) = shell.stream_stores_mut().logs.get_mut(window) {
        view.fail(reason);
    }
}

fn format_duration(milliseconds: u64) -> String {
    if milliseconds < 1_000 {
        format!("{milliseconds}ms")
    } else {
        format!("{}s", milliseconds.div_ceil(1_000))
    }
}

fn format_age(milliseconds: u64) -> String {
    format!("{} ago", format_duration(milliseconds))
}

#[derive(Debug)]
enum AppEventError {
    Transient,
    Terminal(String),
}

fn safe_port_forward_client_error(error: &ClientError) -> String {
    match error {
        ClientError::Server(error) => error.safe_message.clone(),
        ClientError::InvalidState(_) => "Port forwarding is not available right now".to_owned(),
        _ => "Could not start the port forward; try again".to_owned(),
    }
}

fn is_local_port_conflict(error: &k10s_protocol::ErrorFrame) -> bool {
    error.code == ErrorCode::Conflict
        && error
            .safe_message
            .to_ascii_lowercase()
            .contains("local port")
}

impl Drop for K10sApp {
    fn drop(&mut self) {
        self.client.application_close();
        if let Some(mut connection) = self.connection.take() {
            connection.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::rc::Rc;

    use egui_kittest::{
        Harness,
        kittest::{NodeT as _, Queryable as _},
    };
    use ewebsock::{WsEvent, WsMessage};
    use k10s_protocol::{
        BackendRevision, BootstrapResponse, ClientFrame, ClientKind, ContainerMetrics, Context,
        ErrorCode, ErrorFrame, ErrorScope, Event, GroupVersionKind, MetricsAvailability,
        PodMetrics, PortForwardFailure, PortForwardFailureCategory, PortForwardPodTarget,
        PortForwardSession, PortForwardSessionId, PortForwardSessionState, PortForwardStartRequest,
        PortForwardStartResponse, PortForwardTarget, ProtocolVersion, RequestId,
        ResourceCapabilities, ResourceChanged, ResourceDetailResponse, ResourceGone,
        ResourceIdentity, ResourceListRow, ResourceMetricsResponse, ResourceSnapshotPage,
        ResumeStatus, Retryability, ServerFrame, ServerKind, SessionId, SnapshotBegin,
        SnapshotChunk, SnapshotEnd, Subscribed, SubscriptionId, SubscriptionSelector,
        ValidationTicket, Welcome, YamlOutcome, buffer_hash,
    };

    use super::{
        AppConnection, AppEventError, AppView, ConnectionFactory, K10sApp, NamespaceCatalogState,
        PrimaryDetailState, RelationState, ResourceAction, SafeUiError, WindowFreshness,
    };
    use crate::client::{ClientPhase, ConnectTarget, PendingRequest, Query, TransportError};
    use crate::workspace::{
        NamespaceScope, PersistedListView, PersistedWindow, PersistedWindowKind, WindowContent,
        WindowGeom, WindowId, WindowKind, WorkloadKind, WorkspaceCommand, WorkspaceEvent,
    };

    #[test]
    fn production_control_inbox_holds_a_large_default_snapshot_burst() {
        assert_eq!(super::CONTROL_INBOX_CAPACITY, 256);
    }

    #[test]
    fn context_switches_restore_independent_window_layouts() {
        let (mut app, _state) = test_app(Vec::new());
        app.commit_context_layout("dev".into());
        app.shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods));
        let dev_snapshot = app.workspace_snapshot();

        app.commit_context_layout("prod".into());
        app.shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Jobs));
        let prod_snapshot = app.workspace_snapshot();
        assert_ne!(dev_snapshot, prod_snapshot);

        app.commit_context_layout("dev".into());
        assert_eq!(app.workspace_snapshot().windows, dev_snapshot.windows);
        app.commit_context_layout("prod".into());
        assert_eq!(app.workspace_snapshot().windows, prod_snapshot.windows);
        assert_eq!(app.workspace_layouts().len(), 2);
    }

    #[derive(Debug, Default)]
    pub(super) struct FactoryState {
        connect_count: usize,
        sent: Vec<ClientFrame>,
        received: usize,
        connections: VecDeque<ConnectionScript>,
    }

    #[derive(Debug, Default)]
    pub(super) struct ConnectionScript {
        events: VecDeque<WsEvent>,
        overflowed: bool,
    }

    #[derive(Debug, Clone)]
    struct FakeFactory(Rc<RefCell<FactoryState>>);

    #[derive(Debug)]
    struct FakeConnection {
        state: Rc<RefCell<FactoryState>>,
        script: ConnectionScript,
        closed: Rc<Cell<bool>>,
    }

    impl ConnectionFactory for FakeFactory {
        fn connect(&mut self, _: &str) -> Result<Box<dyn AppConnection>, TransportError> {
            let script = {
                let mut state = self.0.borrow_mut();
                state.connect_count += 1;
                state.connections.pop_front().unwrap_or_default()
            };
            Ok(Box::new(FakeConnection {
                state: Rc::clone(&self.0),
                script,
                closed: Rc::new(Cell::new(false)),
            }))
        }
    }

    impl AppConnection for FakeConnection {
        fn try_recv(&mut self) -> Option<WsEvent> {
            let event = self.script.events.pop_front();
            if event.is_some() {
                self.state.borrow_mut().received += 1;
            }
            event
        }

        fn overflowed(&self) -> bool {
            self.script.overflowed
        }

        fn send_frame(&mut self, frame: &ClientFrame) -> Result<(), TransportError> {
            self.state.borrow_mut().sent.push(frame.clone());
            Ok(())
        }

        fn close(&mut self) {
            self.closed.set(true);
        }
    }

    pub(super) fn test_app(scripts: Vec<ConnectionScript>) -> (K10sApp, Rc<RefCell<FactoryState>>) {
        let state = Rc::new(RefCell::new(FactoryState {
            connections: scripts.into(),
            ..FactoryState::default()
        }));
        let app = K10sApp::connect_with_factory(
            ConnectTarget::new("ws://127.0.0.1:1234/api/v1/control", "secret"),
            Box::new(FakeFactory(Rc::clone(&state))),
        )
        .unwrap();
        (app, state)
    }

    pub(super) fn server_message(frame: &ServerFrame) -> WsEvent {
        WsEvent::Message(WsMessage::Text(serde_json::to_string(frame).unwrap()))
    }

    fn request_kind(frame: &ClientFrame) -> Option<String> {
        let k10s_protocol::ClientPayload::Request(request) = frame.decode_payload().ok()? else {
            return None;
        };
        Some(request.request_kind)
    }

    fn resource_watches(
        state: &Rc<RefCell<FactoryState>>,
    ) -> Vec<k10s_protocol::ResourceWatchSpec> {
        all_resource_watches(state)
            .into_iter()
            .filter(|watch| watch.gvk != GroupVersionKind::core("v1", "Namespace"))
            .collect()
    }

    fn all_resource_watches(
        state: &Rc<RefCell<FactoryState>>,
    ) -> Vec<k10s_protocol::ResourceWatchSpec> {
        state
            .borrow()
            .sent
            .iter()
            .filter_map(|frame| {
                let k10s_protocol::ClientPayload::Subscribe(k10s_protocol::Subscribe(selector)) =
                    frame.decode_payload().ok()?
                else {
                    return None;
                };
                match serde_json::from_value(selector).ok()? {
                    SubscriptionSelector::Resource(spec) => Some(spec),
                    _ => None,
                }
            })
            .collect()
    }

    #[test]
    fn application_owns_the_overview_only_workspace_shell() {
        let (app, _) = test_app(Vec::new());

        assert_eq!(app.workspace().windows().len(), 1);
        assert_eq!(
            app.workspace().windows()[0].kind,
            crate::workspace::WindowKind::Overview
        );
    }

    #[test]
    fn accepted_watch_revision_invalidates_yaml_authority_but_stale_delta_does_not() {
        let (mut app, _) = test_app(Vec::new());
        app.client.apply(welcome()).unwrap();
        let subscription = app
            .client
            .subscribe_resource("dev", "apps", "v1", "Deployment", None)
            .unwrap();
        let subscription_id = subscription.id().clone();
        let identity = ResourceIdentity {
            context: "dev".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let row = |revision| ResourceListRow {
            identity: identity.clone(),
            revision: BackendRevision::new(revision),
            labels: Default::default(),
            summary: "Ready".into(),
            created_at: "2026-08-25T00:00:00Z".into(),
            projection: None,
        };
        let window = crate::workspace::WindowId(99);
        let manifest = "apiVersion: apps/v1\nkind: Deployment\n";
        let editor = app
            .shell
            .yaml_editors_mut()
            .open(window, identity.clone(), manifest);
        editor.begin_edit();
        editor.set_buffer(format!("{manifest}# local edit\n"));
        editor.review();
        editor.apply_outcome(&YamlOutcome::Valid {
            ticket: ValidationTicket {
                id: "validation-1".into(),
                target: identity.clone(),
                resource_revision: BackendRevision::new(10),
                buffer_hash: buffer_hash(editor.buffer()),
                disruptive: false,
            },
        });
        assert!(editor.can_apply());

        let absent_identity = ResourceIdentity {
            name: "removed".into(),
            uid: "uid-removed".into(),
            ..identity.clone()
        };
        let absent_window = crate::workspace::WindowId(100);
        let absent_editor =
            app.shell
                .yaml_editors_mut()
                .open(absent_window, absent_identity.clone(), manifest);
        absent_editor.begin_edit();
        absent_editor.set_buffer(format!("{manifest}# retained removed-object edit\n"));
        absent_editor.review();
        absent_editor.apply_outcome(&YamlOutcome::Valid {
            ticket: ValidationTicket {
                id: "validation-removed".into(),
                target: absent_identity,
                resource_revision: BackendRevision::new(10),
                buffer_hash: buffer_hash(absent_editor.buffer()),
                disruptive: false,
            },
        });
        assert!(absent_editor.can_apply());

        let frame = |kind, sequence, payload| ServerFrame {
            kind,
            request_id: None,
            subscription_id: Some(subscription_id.clone()),
            sequence: Some(sequence),
            payload,
        };
        app.handle_event(
            server_message(&frame(
                ServerKind::Subscribed,
                1,
                serde_json::to_value(Subscribed).unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotBegin,
                2,
                serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotChunk,
                3,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(10),
                        rows: vec![row(10)],
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotEnd,
                4,
                serde_json::to_value(SnapshotEnd {
                    checksum: "test".into(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();

        app.handle_event(
            server_message(&frame(
                ServerKind::Event,
                5,
                serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_CHANGED.into(),
                    revision: Some("9".into()),
                    payload: serde_json::to_value(ResourceChanged {
                        identity: identity.clone(),
                        row: row(9),
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        assert!(
            app.shell
                .yaml_editors_mut()
                .get(window)
                .unwrap()
                .can_apply(),
            "stale watch deltas are ignored"
        );

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::ResyncRequired,
                request_id: None,
                subscription_id: None,
                sequence: Some(6),
                payload: serde_json::json!({"reason": "journal unavailable"}),
            }),
            0,
            0,
        )
        .unwrap();

        for (kind, sequence, payload) in [
            (
                ServerKind::SnapshotBegin,
                7,
                serde_json::to_value(SnapshotBegin { total_chunks: 2 }).unwrap(),
            ),
            (
                ServerKind::SnapshotChunk,
                8,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(11),
                        rows: vec![row(11)],
                    })
                    .unwrap(),
                })
                .unwrap(),
            ),
            (
                ServerKind::SnapshotChunk,
                9,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 1,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(11),
                        rows: vec![],
                    })
                    .unwrap(),
                })
                .unwrap(),
            ),
            (
                ServerKind::SnapshotEnd,
                10,
                serde_json::to_value(SnapshotEnd {
                    checksum: "resync".into(),
                })
                .unwrap(),
            ),
        ] {
            app.handle_event(server_message(&frame(kind, sequence, payload)), 0, 0)
                .unwrap();
        }
        let editor = app.shell.yaml_editors_mut().get(window).unwrap();
        assert!(
            !editor.can_apply(),
            "a target in a non-final resync page loses authority"
        );
        assert!(editor.is_dirty(), "local edits survive resync invalidation");
        let absent_editor = app.shell.yaml_editors_mut().get(absent_window).unwrap();
        assert!(
            !absent_editor.can_apply(),
            "a target absent from the replacement loses authority"
        );
        assert!(
            absent_editor.is_dirty(),
            "removed-target local edits survive resync invalidation"
        );
    }

    #[test]
    fn only_accepted_exact_watch_revision_updates_detail_lifecycle() {
        let (mut app, _) = test_app(Vec::new());
        app.client.apply(welcome()).unwrap();
        let subscription = app
            .client
            .subscribe_resource("dev", "apps", "v1", "Deployment", None)
            .unwrap();
        let subscription_id = subscription.id().clone();
        let identity = ResourceIdentity {
            context: "dev".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web-old".into(),
        };
        let row = |identity: ResourceIdentity, revision| ResourceListRow {
            identity,
            revision: BackendRevision::new(revision),
            labels: Default::default(),
            summary: "Ready".into(),
            created_at: String::new(),
            projection: None,
        };
        let frame = |kind, sequence, payload| ServerFrame {
            kind,
            request_id: None,
            subscription_id: Some(subscription_id.clone()),
            sequence: Some(sequence),
            payload,
        };
        for (kind, sequence, payload) in [
            (
                ServerKind::Subscribed,
                1,
                serde_json::to_value(Subscribed).unwrap(),
            ),
            (
                ServerKind::SnapshotBegin,
                2,
                serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            ),
            (
                ServerKind::SnapshotChunk,
                3,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(10),
                        rows: vec![row(identity.clone(), 10)],
                    })
                    .unwrap(),
                })
                .unwrap(),
            ),
            (
                ServerKind::SnapshotEnd,
                4,
                serde_json::to_value(SnapshotEnd {
                    checksum: "initial".into(),
                })
                .unwrap(),
            ),
        ] {
            app.handle_event(server_message(&frame(kind, sequence, payload)), 0, 0)
                .unwrap();
        }

        let gone = |revision: u64| {
            serde_json::to_value(Event {
                event_kind: k10s_protocol::RESOURCE_EVENT_GONE.into(),
                revision: Some(revision.to_string()),
                payload: serde_json::to_value(ResourceGone {
                    identity: identity.clone(),
                    revision: BackendRevision::new(revision),
                })
                .unwrap(),
            })
            .unwrap()
        };
        app.handle_event(server_message(&frame(ServerKind::Event, 5, gone(9))), 0, 0)
            .unwrap();
        assert!(
            app.detail_lifecycles
                .get(&subscription_id)
                .is_none_or(|source| !source.entries.contains_key(&identity))
        );

        app.handle_event(server_message(&frame(ServerKind::Event, 6, gone(11))), 0, 0)
            .unwrap();
        assert_eq!(
            app.detail_lifecycles
                .get(&subscription_id)
                .and_then(|source| source.entries.get(&identity))
                .map(|entry| entry.lifecycle),
            Some(super::DetailLifecycle::Gone)
        );

        let mut recreated = identity.clone();
        recreated.uid = "uid-web-new".into();
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(recreated.clone()))
        {
            app.handle_workspace_event(event);
        }
        app.handle_event(
            server_message(&frame(
                ServerKind::Event,
                7,
                serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_CHANGED.into(),
                    revision: Some("12".into()),
                    payload: serde_json::to_value(ResourceChanged {
                        identity: recreated.clone(),
                        row: row(recreated.clone(), 12),
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        let source = app.detail_lifecycles.get(&subscription_id).unwrap();
        assert_eq!(
            source.entries.get(&identity).map(|entry| entry.lifecycle),
            Some(super::DetailLifecycle::Gone)
        );
        assert_eq!(
            source.entries.get(&recreated).map(|entry| entry.lifecycle),
            Some(super::DetailLifecycle::Present)
        );
    }

    fn welcome() -> ServerFrame {
        welcome_with_minor(k10s_protocol::PROTOCOL_MINOR)
    }

    fn welcome_with_minor(minor: u16) -> ServerFrame {
        ServerFrame {
            kind: ServerKind::Welcome,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(Welcome {
                protocol: ProtocolVersion { major: 1, minor },
                capabilities: vec![],
                session_id: SessionId::new("reconnected-session"),
                server_instance_id: "reconnected-server".to_owned(),
                resume_status: ResumeStatus::Fresh,
            })
            .unwrap(),
        }
    }

    #[test]
    fn transient_loss_schedules_a_fresh_transport_and_preserves_local_state() {
        let (mut app, state) = test_app(vec![
            ConnectionScript {
                events: VecDeque::from([WsEvent::Closed]),
                overflowed: false,
            },
            ConnectionScript {
                events: VecDeque::from([WsEvent::Opened]),
                overflowed: false,
            },
        ]);
        app.client.local_ui_mut().selected_context = Some("dev-local".to_owned());
        let window = crate::workspace::WindowId(77);
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let editor = app
            .shell
            .yaml_editors_mut()
            .open(window, identity.clone(), "original\n");
        editor.begin_edit();
        editor.set_buffer("dirty\n");
        editor.review();
        editor.apply_outcome(&YamlOutcome::Valid {
            ticket: ValidationTicket {
                id: "lost-ticket".into(),
                target: identity,
                resource_revision: BackendRevision::new(1),
                buffer_hash: buffer_hash(editor.buffer()),
                disruptive: false,
            },
        });
        assert!(editor.can_apply());

        app.poll_at(100, 10);
        assert_eq!(app.client.phase(), ClientPhase::Disconnected);
        app.shell.set_external_shell_availability(
            crate::ui::ExternalShellAvailability::Available { generation: 41 },
        );
        app.external_shell_requests
            .push(crate::ui::ExternalShellTarget {
                generation: 41,
                namespace: "default".into(),
                pod: "api".into(),
                uid: "uid-api".into(),
                container: "api".into(),
                program: "/bin/sh".into(),
            });
        assert_eq!(app.view(), &AppView::Connecting);
        assert_eq!(state.borrow().connect_count, 1);
        let editor = app.shell.yaml_editors_mut().get(window).unwrap();
        assert!(!editor.can_apply(), "socket loss revokes Apply authority");
        assert!(editor.is_dirty(), "socket loss preserves the dirty buffer");

        app.poll_at(120, 0);
        app.poll_at(120, 0);
        assert_eq!(state.borrow().connect_count, 2);
        assert_eq!(app.client.phase(), ClientPhase::Authenticating);
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("dev-local")
        );
        assert_eq!(state.borrow().sent.len(), 1, "fresh transport sends Hello");
    }

    #[test]
    fn polls_before_the_handshake_completes_keep_the_queued_hello_alive() {
        // Regression for the browser foundation smoke: a poll tick between
        // connect() and the socket's Opened used to drain the queued Hello
        // through a blind flush; the browser-side send failed silently on a
        // connecting WebSocket and the frame was lost forever.
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::new(),
            overflowed: false,
        }]);

        app.poll_at(22, 1);
        app.poll_at(52, 2);
        assert!(
            state.borrow().sent.is_empty(),
            "no frame may be sent through a still-connecting transport"
        );
        assert_eq!(app.client.outbound_len(), 1, "Hello stays queued");
    }

    #[test]
    fn retried_transports_send_their_hello_only_after_they_open() {
        // Same class of bug on the retry path: reconnect_if_due queues a
        // fresh Hello and creates a new CONNECTING socket inside one poll;
        // the same tick's trailing flush must not eat it again.
        let (mut app, state) = test_app(vec![
            ConnectionScript {
                events: VecDeque::from([WsEvent::Closed]),
                overflowed: false,
            },
            ConnectionScript {
                events: VecDeque::from([WsEvent::Closed]),
                overflowed: false,
            },
            ConnectionScript {
                events: VecDeque::from([WsEvent::Opened]),
                overflowed: false,
            },
        ]);
        app.client.local_ui_mut().selected_context = Some("dev-local".to_owned());

        app.poll_at(100, 10);
        assert_eq!(app.client.phase(), ClientPhase::Disconnected);

        // First retry fired: the new transport exists but has not opened,
        // so the freshly queued Hello must survive this tick's flushes.
        app.poll_at(10_000, 0);
        assert_eq!(
            app.shell.external_shell_availability(),
            crate::ui::ExternalShellAvailability::Unavailable
        );
        assert!(app.external_shell_requests.is_empty());
        assert_eq!(state.borrow().connect_count, 2);
        assert_eq!(app.client.phase(), ClientPhase::Authenticating);
        assert!(
            state.borrow().sent.is_empty(),
            "Hello waits for the transport to open"
        );
        assert_eq!(app.client.outbound_len(), 1);

        // The deadline close repeats; every generation behaves the same and
        // the replacement Hello again survives its creation tick.
        app.poll_at(20_000, 0);
        assert_eq!(state.borrow().connect_count, 3);
        assert!(state.borrow().sent.is_empty());
        assert_eq!(app.client.outbound_len(), 1);

        // Once a transport finally reports Opened the Hello goes out once.
        app.poll_at(30_000, 0);
        let sent = state.borrow().sent.clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].kind, ClientKind::Hello);
        assert_eq!(app.view(), &AppView::Connecting);
    }

    #[test]
    fn terminal_authentication_rejection_never_reconnects() {
        let error = ErrorFrame::new(
            ErrorCode::Unauthorized,
            "authentication failed",
            Retryability::Never,
            ErrorScope::Session,
            "auth",
        );
        let frame = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(error).unwrap(),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                WsEvent::Message(WsMessage::Text(serde_json::to_string(&frame).unwrap())),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);
        app.poll_at(10_000, 0);

        assert_eq!(app.client.phase(), ClientPhase::WebGate);
        assert!(matches!(app.view(), AppView::Failed { .. }));
        assert_eq!(state.borrow().connect_count, 1);
    }

    #[test]
    fn overflow_is_observed_after_the_exact_capacity_is_drained() {
        const CAPACITY: usize = 4;
        let events = (0..CAPACITY)
            .map(|index| WsEvent::Message(WsMessage::Binary(vec![index as u8])))
            .collect();
        let (mut app, state) = test_app(vec![ConnectionScript {
            events,
            overflowed: true,
        }]);

        app.poll_at(100, 1);

        assert_eq!(state.borrow().received, CAPACITY);
        assert_eq!(app.client.phase(), ClientPhase::Disconnected);
        assert_eq!(app.view(), &AppView::Connecting);
    }

    #[test]
    fn reconnect_bootstrap_response_reaches_ready_and_loads_infrastructure_once() {
        let response = ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let (mut app, state) = test_app(vec![
            ConnectionScript {
                events: VecDeque::from([
                    WsEvent::Opened,
                    WsEvent::Error("connection reset".to_owned()),
                ]),
                overflowed: false,
            },
            ConnectionScript {
                events: VecDeque::from([
                    WsEvent::Opened,
                    server_message(&welcome()),
                    server_message(&response),
                ]),
                overflowed: false,
            },
        ]);

        app.poll_at(100, 0);
        app.poll_at(100, 0);

        assert!(matches!(app.view(), AppView::Ready { .. }));
        let request_kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            request_kinds,
            ["bootstrap", "infrastructure.get"],
            "overview-only recovery loads bootstrap and one infrastructure snapshot on demand"
        );
        assert!(
            resource_watches(&state).is_empty(),
            "overview-only recovery creates no hidden resource watches"
        );
        assert_eq!(app.client.outbound_len(), 0);
    }

    #[test]
    fn infrastructure_subscription_ack_does_not_restart_bootstrap_or_snapshot_query() {
        let bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let subscribed = ServerFrame {
            kind: ServerKind::Subscribed,
            request_id: None,
            subscription_id: Some(SubscriptionId::new("infrastructure-1")),
            sequence: Some(1),
            payload: serde_json::to_value(Subscribed).unwrap(),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
                server_message(&subscribed),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);

        let request_kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            request_kinds,
            ["bootstrap", "infrastructure.get"],
            "subscription acknowledgement must not re-bootstrap or restart the snapshot"
        );
    }

    #[test]
    fn unsupported_infrastructure_request_is_panel_local_and_ends_loading() {
        let bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let unsupported = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(RequestId::from_u128(2)),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "unsafe backend-specific detail",
                Retryability::Never,
                ErrorScope::Request,
                "infrastructure",
            ))
            .unwrap(),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
                server_message(&unsupported),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        assert!(app.infrastructure_request.is_none());
        assert_eq!(
            app.infrastructure_load,
            super::InfrastructureLoad::Unavailable
        );
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| request_kind(frame).as_deref() == Some("infrastructure.get"))
                .count(),
            1,
            "render/poll does not retry unsupported infrastructure automatically"
        );
    }

    #[test]
    fn subscription_scoped_error_cannot_impersonate_an_infrastructure_request() {
        let (mut app, _) = ready_app();
        let request_id = app
            .infrastructure_request
            .as_ref()
            .expect("overview request is pending")
            .id()
            .clone();
        let wrong_scope = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(request_id),
            subscription_id: Some(SubscriptionId::new("unrelated-subscription")),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "wrong scope",
                Retryability::Never,
                ErrorScope::Subscription,
                "wrong-scope-request",
            ))
            .unwrap(),
        };

        assert!(
            app.handle_event(server_message(&wrong_scope), 110, 0)
                .is_err(),
            "malformed scope follows the existing safe error policy"
        );
        assert_eq!(app.infrastructure_load, super::InfrastructureLoad::Loading);
        assert!(app.infrastructure_request.is_some());
    }

    #[test]
    fn request_scoped_error_cannot_impersonate_an_infrastructure_subscription() {
        let (mut app, _) = ready_app();
        let unrelated = app.client.begin(Query::Bootstrap).unwrap();
        let subscription_id = app
            .infrastructure_subscription
            .as_ref()
            .expect("overview watch exists")
            .id()
            .clone();
        let wrong_scope = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(unrelated.id().clone()),
            subscription_id: Some(subscription_id),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "wrong scope",
                Retryability::Never,
                ErrorScope::Request,
                "wrong-scope-subscription",
            ))
            .unwrap(),
        };

        assert!(
            app.handle_event(server_message(&wrong_scope), 110, 0)
                .is_err()
        );
        assert_eq!(app.infrastructure_load, super::InfrastructureLoad::Loading);
        assert!(app.infrastructure_subscription.is_some());
    }

    #[test]
    fn unsupported_infrastructure_subscription_does_not_end_the_control_session() {
        let (mut app, state) = ready_app();
        app.web_activate_workload(WorkloadKind::Pods);
        let resource_subscriptions = app
            .resource_subscriptions
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let subscription_id = app
            .infrastructure_subscription
            .as_ref()
            .expect("ready overview has one infrastructure watch")
            .id()
            .clone();
        let unsupported = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(subscription_id),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "backend implementation detail",
                Retryability::Never,
                ErrorScope::Subscription,
                "infrastructure-watch",
            ))
            .unwrap(),
        };

        app.handle_event(server_message(&unsupported), 120, 0)
            .expect("unsupported capability remains panel-local");

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        assert!(app.infrastructure_subscription.is_none());
        assert_eq!(
            app.infrastructure_load,
            super::InfrastructureLoad::Unavailable
        );
        assert_eq!(
            app.resource_subscriptions
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            resource_subscriptions,
            "an infrastructure capability failure cannot disturb resource watches"
        );
        let before = state.borrow().sent.len();
        app.poll_at(130, 0);
        assert_eq!(
            state.borrow().sent.len(),
            before,
            "polling does not resubscribe"
        );

        app.refresh_infrastructure("dev-local").unwrap();
        app.flush_outbound().unwrap();
        assert_eq!(app.infrastructure_load, super::InfrastructureLoad::Loading);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| request_kind(frame).as_deref() == Some("infrastructure.get"))
                .count(),
            2,
            "only an explicit refresh retries the panel request"
        );
    }

    #[test]
    fn replacing_a_completed_infrastructure_failure_retires_its_old_outcome() {
        let (mut app, _) = ready_app();
        let old = app
            .infrastructure_request
            .clone()
            .expect("initial overview request is pending");
        let failure = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(old.id().clone()),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "unsupported",
                Retryability::Never,
                ErrorScope::Request,
                "overview",
            ))
            .unwrap(),
        };
        let _ = app.client.apply(failure);

        app.refresh_infrastructure("dev-local").unwrap();

        assert!(app.client.take_failure(old).is_none());
    }

    #[test]
    fn unsupported_infrastructure_subscription_retires_locally_when_outbound_is_full() {
        let (mut app, _) = ready_app();
        app.web_activate_workload(WorkloadKind::Pods);
        let resource_keys = app
            .resource_subscriptions
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let infrastructure = app
            .infrastructure_subscription
            .as_ref()
            .expect("overview watch exists")
            .id()
            .clone();
        for _ in 0..255 {
            app.client.begin(Query::Bootstrap).unwrap();
        }
        let _filler = app.client.subscribe_bootstrap_status().unwrap();
        assert_eq!(app.client.outbound_len(), 256);
        let desired_before = app.client.live_subscription_count();
        let unsupported = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(infrastructure),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "unsupported",
                Retryability::Never,
                ErrorScope::Subscription,
                "infrastructure-watch",
            ))
            .unwrap(),
        };

        app.handle_event(server_message(&unsupported), 120, 0)
            .expect("server rejection needs no outbound unsubscribe");

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert_eq!(
            app.client.live_subscription_count(),
            desired_before - 1,
            "the rejected infrastructure desire is retired without touching other watches"
        );
        assert!(app.infrastructure_subscription.is_none());
        assert_eq!(
            app.infrastructure_load,
            super::InfrastructureLoad::Unavailable
        );
        assert_eq!(
            app.resource_subscriptions
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            resource_keys
        );
    }

    #[test]
    fn reconnectable_unsupported_infrastructure_request_uses_transport_recovery() {
        let (mut app, _) = ready_app();
        let request_id = app
            .infrastructure_request
            .as_ref()
            .expect("overview request exists")
            .id()
            .clone();
        let reconnect = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(request_id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "server requires reconnect",
                Retryability::AfterReconnect,
                ErrorScope::Request,
                "infrastructure-request",
            ))
            .unwrap(),
        };
        assert!(matches!(
            app.handle_event(server_message(&reconnect), 100, 200),
            Err(super::AppEventError::Transient)
        ));
        app.transient_loss(100, 200);

        assert_eq!(app.client.phase(), ClientPhase::Disconnected);
        assert_eq!(app.view(), &AppView::Connecting);
        assert!(app.connection.is_none());
        assert!(app.infrastructure_request.is_none());
        assert_ne!(
            app.infrastructure_load,
            super::InfrastructureLoad::Unavailable
        );
        assert!(app.client.retry_schedule().is_some());
    }

    #[test]
    fn reconnectable_unsupported_infrastructure_subscription_uses_transport_recovery() {
        let (mut app, _) = ready_app();
        let subscription_id = app
            .infrastructure_subscription
            .as_ref()
            .expect("overview watch exists")
            .id()
            .clone();
        let reconnect = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(subscription_id),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::UnsupportedMessage,
                "server requires reconnect",
                Retryability::AfterReconnect,
                ErrorScope::Subscription,
                "infrastructure-subscription",
            ))
            .unwrap(),
        };
        assert!(matches!(
            app.handle_event(server_message(&reconnect), 100, 200),
            Err(super::AppEventError::Transient)
        ));
        app.transient_loss(100, 200);

        assert_eq!(app.client.phase(), ClientPhase::Disconnected);
        assert_eq!(app.view(), &AppView::Connecting);
        assert!(app.connection.is_none());
        assert!(app.infrastructure_request.is_none());
        assert!(
            app.infrastructure_subscription.is_some(),
            "desired watch survives for reconnect reconstruction"
        );
        assert_ne!(
            app.infrastructure_load,
            super::InfrastructureLoad::Unavailable
        );
        assert!(app.client.retry_schedule().is_some());
    }

    #[test]
    fn infrastructure_demand_driven_lifecycle_close_and_reopen() {
        use crate::workspace::{LauncherItem, WindowKind, WorkspaceCommand};

        let (mut app, _) = ready_app();
        assert!(app.infrastructure_demanded());
        assert!(app.infrastructure_subscription.is_some());
        assert_eq!(app.infrastructure_context.as_deref(), Some("dev-local"));

        // Close Overview window (only window open by default)
        let overview_window = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Overview)
            .map(|w| w.id)
            .expect("overview window exists");
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(overview_window));
        assert!(!app.infrastructure_demanded());

        app.reconcile_selected_resource_streams();
        assert!(
            app.infrastructure_subscription.is_none(),
            "closing overview must unsubscribe infrastructure"
        );
        assert!(
            app.infrastructure_request.is_none(),
            "closing overview must cancel pending infrastructure request"
        );
        assert!(
            app.infrastructure_context.is_none(),
            "closing overview must clear infrastructure context"
        );

        // Re-open Overview window via launcher
        app.shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Overview,
            ));
        assert!(app.infrastructure_demanded());

        app.reconcile_selected_resource_streams();
        assert!(
            app.infrastructure_subscription.is_some(),
            "reopening overview must subscribe infrastructure on demand"
        );
        assert!(
            app.infrastructure_request.is_some(),
            "reopening overview must initiate snapshot request on demand"
        );
        assert_eq!(
            app.infrastructure_context.as_deref(),
            Some("dev-local"),
            "infrastructure context is re-established"
        );
    }

    #[test]
    fn infrastructure_demand_driven_nodes_and_storage_windows() {
        use crate::workspace::{LauncherItem, WindowKind, WorkspaceCommand};

        let (mut app, _) = ready_app();

        // Close overview
        let overview = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Overview)
            .map(|w| w.id)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(overview));
        app.reconcile_selected_resource_streams();
        assert!(!app.infrastructure_demanded());
        assert!(app.infrastructure_subscription.is_none());

        // Open Nodes window
        app.shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
        assert!(app.infrastructure_demanded());
        app.reconcile_selected_resource_streams();
        assert!(app.infrastructure_subscription.is_some());
        assert_eq!(app.infrastructure_context.as_deref(), Some("dev-local"));

        // Close Nodes window and open Storage window
        let nodes = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Nodes)
            .map(|w| w.id)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(nodes));
        app.shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Storage,
            ));
        assert!(app.infrastructure_demanded());
        app.reconcile_selected_resource_streams();
        assert!(app.infrastructure_subscription.is_some());

        // Close Storage window: no infrastructure demanded
        let storage = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Storage)
            .map(|w| w.id)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(storage));
        assert!(!app.infrastructure_demanded());
        app.reconcile_selected_resource_streams();
        assert!(app.infrastructure_subscription.is_none());
    }

    #[test]
    fn connect_without_infrastructure_windows_never_queries_or_subscribes() {
        use crate::workspace::{WindowKind, WorkspaceCommand};

        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&ServerFrame::response(
                    RequestId::from_u128(1),
                    BootstrapResponse::fixture(),
                )),
            ]),
            overflowed: false,
        }]);

        // Close Overview window before connecting/readying
        let overview = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Overview)
            .map(|w| w.id)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(overview));
        assert!(!app.infrastructure_demanded());

        // Connect and process bootstrap response
        app.poll_at(100, 0);
        assert!(matches!(app.view(), AppView::Ready { .. }));

        // Verify that NO infrastructure.get request was sent
        let requests: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            requests,
            ["bootstrap"],
            "when no infrastructure window is open, only bootstrap is sent"
        );
        assert!(app.infrastructure_subscription.is_none());
        assert!(app.infrastructure_request.is_none());
        assert!(app.infrastructure_context.is_none());
    }

    #[test]
    fn sequence_gap_flushes_resync_on_the_existing_connection() {
        let initial_bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let gapped_event = ServerFrame {
            kind: ServerKind::Event,
            request_id: None,
            subscription_id: None,
            sequence: Some(2),
            payload: serde_json::json!({"kind":"bootstrapStatus","payload":null}),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&initial_bootstrap),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);
        let identity = deployment_identity("sequence-gap");
        start_relation_request(&mut app, &identity);
        app.relations.insert(
            identity.clone(),
            RelationState::Loaded {
                response: std::sync::Arc::new(k10s_protocol::ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(1),
                    groups: Vec::new(),
                }),
                loaded_at_ms: 0,
                refreshing: true,
                refresh_error: None,
            },
        );
        let generation = app.resource_generation;

        app.handle_event(server_message(&gapped_event), 101, 0)
            .unwrap();

        assert!(!matches!(app.view(), AppView::Failed { .. }));
        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(app.connection.is_some(), "existing connection remains live");
        assert_eq!(state.borrow().connect_count, 1);
        assert!(app.recovering);
        assert!(matches!(app.view(), AppView::Connecting));
        assert_eq!(app.resource_generation, generation.wrapping_add(1));
        assert!(app.detail_requests.is_empty());
        assert!(app.relation_requests.is_empty());
        assert!(app.primary_details.is_empty());
        assert!(app.relations.is_empty());
        let request_kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            request_kinds
                .iter()
                .filter(|kind| kind.as_str() == "bootstrap")
                .count(),
            2,
            "the gap adds one resync bootstrap: {request_kinds:?}"
        );
        assert_eq!(app.client.outbound_len(), 0);
    }

    #[test]
    fn sequence_gap_retires_port_forward_correlations_and_reenables_the_modal() {
        let source = failed_port_forward("pending-start", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&source.target);
        let generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "web · 8080/TCP", 8_080);
        app.shell
            .port_forward_start_modal_mut()
            .unwrap()
            .local_port_draft = "18080".into();
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(source.target, 18_080).unwrap();
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            },
        );
        assert_eq!(app.pending_port_forwards.len(), 1);

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Event,
                request_id: None,
                subscription_id: None,
                sequence: Some(2),
                payload: serde_json::json!({"kind":"bootstrapStatus","payload":null}),
            }),
            101,
            0,
        )
        .unwrap();

        assert!(app.pending_port_forwards.is_empty());
        let modal = app.shell.port_forward_start_modal().unwrap();
        assert!(!modal.pending);
        assert_eq!(modal.local_port_draft, "18080");
        assert_eq!(
            modal.error.as_deref(),
            Some("Server state changed; submit again")
        );
    }

    #[test]
    fn explicit_resync_retires_port_forward_correlations_and_reenables_the_modal() {
        let source = failed_port_forward("pending-start", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&source.target);
        let generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "web · 8080/TCP", 8_080);
        app.shell
            .port_forward_start_modal_mut()
            .unwrap()
            .local_port_draft = "18080".into();
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(source.target, 18_080).unwrap();
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            },
        );
        assert_eq!(app.pending_port_forwards.len(), 1);

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::ResyncRequired,
                request_id: None,
                subscription_id: None,
                sequence: Some(1),
                payload: serde_json::json!({"reason": "journal unavailable"}),
            }),
            101,
            0,
        )
        .unwrap();

        assert!(app.pending_port_forwards.is_empty());
        let modal = app.shell.port_forward_start_modal().unwrap();
        assert!(!modal.pending);
        assert_eq!(modal.local_port_draft, "18080");
        assert_eq!(
            modal.error.as_deref(),
            Some("Server state changed; submit again")
        );
    }

    #[test]
    fn after_reconnect_error_advances_backoff_only_once() {
        let error = ErrorFrame::new(
            ErrorCode::Internal,
            "reconnect required",
            Retryability::AfterReconnect,
            ErrorScope::Session,
            "session-error",
        );
        let frame = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(error).unwrap(),
        };
        let (mut app, _) = test_app(vec![ConnectionScript {
            events: VecDeque::from([WsEvent::Opened, server_message(&frame)]),
            overflowed: false,
        }]);

        app.poll_at(100, 200);

        let schedule = app.client.retry_schedule().unwrap();
        assert_eq!(schedule.attempt, 0);
        assert_eq!(schedule.max_delay_ms, 250);
        assert_eq!(schedule.retry_at_ms, 300);
    }

    #[test]
    fn stale_retry_starts_a_transport_without_issuing_a_non_ready_query() {
        fn render(ui: &mut egui::Ui, app: &mut K10sApp) {
            app.render_ui(ui);
        }

        let (mut app, state) = test_app(vec![
            ConnectionScript::default(),
            ConnectionScript::default(),
        ]);
        app.client.local_ui_mut().selected_context = Some("dev-local".into());
        app.transient_loss(100, 250);
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1_280.0, 800.0))
            .build_ui_state(render, app);

        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Retry")
            .click();
        harness.step();

        assert_eq!(state.borrow().connect_count, 2);
        assert_eq!(harness.state().client.phase(), ClientPhase::Authenticating);
        assert_eq!(harness.state().view(), &AppView::Connecting);
        assert!(harness.state().infrastructure_request.is_none());
    }

    #[test]
    fn failed_view_retry_reconnects_even_when_the_client_was_ready() {
        fn render(ui: &mut egui::Ui, app: &mut K10sApp) {
            app.render_ui(ui);
        }

        let (mut app, state) = test_app(vec![
            ConnectionScript::default(),
            ConnectionScript::default(),
        ]);
        app.client.apply(welcome()).unwrap();
        app.client.local_ui_mut().selected_context = Some("dev-local".into());
        app.terminal_failure("malformed server response".into());
        assert_eq!(app.client.phase(), ClientPhase::Ready);
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1_280.0, 800.0))
            .build_ui_state(render, app);

        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Retry")
            .click();
        harness.step();

        assert_eq!(state.borrow().connect_count, 2);
        assert_eq!(harness.state().client.phase(), ClientPhase::Authenticating);
        assert_eq!(harness.state().view(), &AppView::Connecting);
        assert!(harness.state().infrastructure_request.is_none());
    }

    pub(super) fn ready_app() -> (K10sApp, Rc<RefCell<FactoryState>>) {
        ready_app_with_minor(k10s_protocol::PROTOCOL_MINOR)
    }

    fn ready_app_with_port_forward_capabilities() -> (K10sApp, Rc<RefCell<FactoryState>>) {
        let mut bootstrap = BootstrapResponse::fixture();
        bootstrap.capabilities.extend([
            k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.into(),
            k10s_protocol::CAPABILITY_POD_PORT_FORWARD.into(),
        ]);
        let bootstrap = ServerFrame::response(RequestId::from_u128(1), bootstrap);
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
            ]),
            overflowed: false,
        }]);
        app.poll_at(100, 0);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        (app, state)
    }

    #[test]
    fn initial_port_forward_list_request_projects_loading_until_authority_completes() {
        let (mut app, _) = ready_app_with_port_forward_capabilities();

        assert!(app.port_forward_list.is_some());
        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Loading
        );

        let request = app.port_forward_list.clone().unwrap();
        app.handle_event(
            server_message(&ServerFrame::response(
                request.id().clone(),
                k10s_protocol::PortForwardListResponse {
                    revision: 0,
                    sessions: Vec::new(),
                },
            )),
            110,
            0,
        )
        .unwrap();
        app.finish_port_forward_list();

        let feed = app.build_resource_feed();
        assert_eq!(
            feed.port_forward_list_state,
            crate::ui::PortForwardListState::Ready
        );
        assert!(feed.port_forward_sessions.is_empty());
    }

    #[test]
    fn stale_initial_port_forward_list_transfers_to_one_reconstructing_replacement() {
        let (mut app, state) = ready_app_with_port_forward_capabilities();
        let initial = app.port_forward_list.clone().unwrap();
        while app.client.take_outbound().is_some() {}
        state.borrow_mut().sent.clear();
        app.client
            .apply_port_forward_response(
                k10s_protocol::REQUEST_PORT_FORWARD_START,
                &serde_json::to_value(k10s_protocol::PortForwardStartResponse {
                    session: failed_port_forward("pf-newer", 6),
                })
                .unwrap(),
            )
            .unwrap();

        app.handle_event(
            server_message(&ServerFrame::response(
                initial.id().clone(),
                k10s_protocol::PortForwardListResponse {
                    revision: 5,
                    sessions: Vec::new(),
                },
            )),
            110,
            0,
        )
        .unwrap();
        app.finish_port_forward_list();

        assert!(app.port_forward_list.is_none());
        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Reconstructing
        );
        let sent_replacements = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| {
                request_kind(frame).as_deref() == Some(k10s_protocol::REQUEST_PORT_FORWARD_LIST)
            })
            .count();
        let queued_replacements = std::iter::from_fn(|| app.client.take_outbound())
            .filter(|frame| {
                request_kind(frame).as_deref() == Some(k10s_protocol::REQUEST_PORT_FORWARD_LIST)
            })
            .count();
        assert_eq!(sent_replacements + queued_replacements, 1);
    }

    #[test]
    fn reconnect_port_forward_list_request_projects_reconstruction_until_completion() {
        let mut bootstrap = BootstrapResponse::fixture();
        bootstrap.capabilities.extend([
            k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.into(),
            k10s_protocol::CAPABILITY_POD_PORT_FORWARD.into(),
        ]);
        let bootstrap = ServerFrame::response(RequestId::from_u128(1), bootstrap);
        let (mut app, _) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
            ]),
            overflowed: false,
        }]);
        app.recovering = true;

        app.poll_at(100, 0);

        assert!(matches!(app.view(), AppView::Ready { .. }));
        assert!(app.port_forward_list.is_some());
        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Reconstructing
        );

        let request = app.port_forward_list.clone().unwrap();
        app.handle_event(
            server_message(&ServerFrame::response(
                request.id().clone(),
                k10s_protocol::PortForwardListResponse {
                    revision: 0,
                    sessions: Vec::new(),
                },
            )),
            110,
            0,
        )
        .unwrap();
        app.finish_port_forward_list();

        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Ready
        );
    }

    #[test]
    fn client_owned_lag_reconstruction_projects_until_its_replacement_list_completes() {
        let (mut app, _) = ready_app_with_port_forward_capabilities();
        let initial = app.port_forward_list.clone().unwrap();
        app.handle_event(
            server_message(&ServerFrame::response(
                initial.id().clone(),
                k10s_protocol::PortForwardListResponse {
                    revision: 0,
                    sessions: Vec::new(),
                },
            )),
            110,
            0,
        )
        .unwrap();
        app.finish_port_forward_list();
        let subscription = app
            .client
            .subscribe_port_forward_sessions()
            .unwrap()
            .unwrap();
        let lag = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(subscription.id().clone()),
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::Internal,
                "session stream lagged; refresh sessions",
                Retryability::AfterRefresh,
                ErrorScope::Subscription,
                "port-forward-lag",
            ))
            .unwrap(),
        };

        app.client.apply(lag).unwrap();

        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Reconstructing
        );
        let replacement = std::iter::from_fn(|| app.client.take_outbound())
            .find(|frame| {
                request_kind(frame).as_deref() == Some(k10s_protocol::REQUEST_PORT_FORWARD_LIST)
            })
            .and_then(|frame| frame.request_id)
            .unwrap();
        app.client
            .apply(ServerFrame::response(
                replacement,
                k10s_protocol::PortForwardListResponse {
                    revision: 1,
                    sessions: Vec::new(),
                },
            ))
            .unwrap();

        assert_eq!(
            app.build_resource_feed().port_forward_list_state,
            crate::ui::PortForwardListState::Ready
        );
    }

    fn ready_app_with_authorized_pod_port_forward(
        target: &PortForwardTarget,
    ) -> (K10sApp, Rc<RefCell<FactoryState>>) {
        let PortForwardTarget::Pod { identity, .. } = target else {
            panic!("fixture requires a Pod port-forward target");
        };
        let (mut app, state) = ready_app_with_port_forward_capabilities();
        let initial_port_forward_list = app.port_forward_list.clone().unwrap();
        app.client
            .apply(ServerFrame::response(
                initial_port_forward_list.id().clone(),
                k10s_protocol::PortForwardListResponse {
                    revision: 0,
                    sessions: Vec::new(),
                },
            ))
            .unwrap();
        app.finish_port_forward_list();
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let subscription = app
            .window_subscriptions
            .get(&window)
            .and_then(|key| app.resource_subscriptions.get(key))
            .map(|retained| retained.live.id().clone())
            .expect("Pod list owns an authoritative subscription");
        complete_resource_snapshot(
            &mut app,
            &subscription,
            1,
            vec![deployment_row(identity.clone(), 1)],
            true,
        );
        let detail = deployment_detail_fixture(identity);
        app.details.insert(identity.clone(), detail.clone());
        app.primary_details
            .insert(identity.clone(), PrimaryDetailState::Loaded(detail));
        assert!(
            crate::ui::port_forward_start_authorization(&app.build_resource_feed(), target).is_ok(),
            "fixture must establish current exact Pod authority"
        );
        (app, state)
    }

    fn failed_port_forward(id: &str, revision: u64) -> PortForwardSession {
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "web-0".into(),
            uid: "uid-pod".into(),
        };
        PortForwardSession {
            id: PortForwardSessionId::try_new(id).unwrap(),
            target: PortForwardTarget::Pod {
                identity,
                container_name: "web".into(),
                remote_port: 8_080,
            },
            requested_local_port: 18_080,
            pod: PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-0".into(),
                uid: "uid-pod".into(),
            },
            pod_port: 8_080,
            local_addr: String::new(),
            state: PortForwardSessionState::Failed,
            failure: Some(PortForwardFailure {
                category: PortForwardFailureCategory::LocalPortInUse,
                message: "local port 18080 is already in use".into(),
            }),
            revision,
        }
    }

    #[test]
    fn forged_start_action_without_exact_current_authority_fails_closed() {
        let (mut app, _) = ready_app_with_port_forward_capabilities();
        let target = failed_port_forward("forged", 1).target;
        let generation = app
            .shell
            .open_port_forward_start(target.clone(), "web · 8080/TCP", 8_080);
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(target, 18_080).unwrap();

        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            },
        );

        assert!(app.pending_port_forwards.is_empty());
        let modal = app.shell.port_forward_start_modal().unwrap();
        assert!(!modal.pending);
        assert_eq!(
            modal.error.as_deref(),
            Some("Port forwarding requires live, matching resource details")
        );
    }

    #[test]
    fn direct_start_dispatch_rechecks_port_forward_list_authority() {
        for list_state in [
            crate::ui::PortForwardListState::Loading,
            crate::ui::PortForwardListState::Reconstructing,
        ] {
            let target = failed_port_forward("authority", 1).target;
            let (mut app, _) = ready_app_with_authorized_pod_port_forward(&target);
            let list = match list_state {
                crate::ui::PortForwardListState::Loading => {
                    app.client.begin(Query::PortForwardList).unwrap()
                }
                crate::ui::PortForwardListState::Reconstructing => {
                    app.client.begin_port_forward_reconstruction().unwrap()
                }
                crate::ui::PortForwardListState::Ready => unreachable!(),
            };
            app.port_forward_list = Some(list);
            assert_eq!(
                app.build_resource_feed().port_forward_list_state,
                list_state
            );

            let generation =
                app.shell
                    .open_port_forward_start(target.clone(), "web · 8080/TCP", 8_080);
            app.shell.port_forward_start_modal_mut().unwrap().pending = true;
            let request = PortForwardStartRequest::try_target(target, 18_080).unwrap();

            app.process_port_forward_action(
                &egui::Context::default(),
                crate::ui::PortForwardAction::Start {
                    request,
                    generation,
                },
            );

            assert!(app.pending_port_forwards.is_empty());
            let modal = app.shell.port_forward_start_modal().unwrap();
            assert!(!modal.pending);
            assert_eq!(
                modal.error.as_deref(),
                Some("Port-forward sessions are still loading")
            );
        }
    }

    #[test]
    fn modal_start_error_and_success_are_routed_to_the_originating_dialog() {
        let failed = failed_port_forward("new-session", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&failed.target);
        let generation =
            app.shell
                .open_port_forward_start(failed.target.clone(), "web · 8080/TCP", 8_080);
        app.shell
            .port_forward_start_modal_mut()
            .unwrap()
            .local_port_draft = "18080".into();
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(failed.target.clone(), 18_080).unwrap();

        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request: request.clone(),
                generation,
            },
        );
        let rejected = app.pending_port_forwards[0].request.clone();
        let error = ErrorFrame::new(
            ErrorCode::Conflict,
            "local port 18080 is already in use",
            Retryability::UserAction,
            ErrorScope::Request,
            "start-error",
        );
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(rejected.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(error).unwrap(),
            }),
            0,
            0,
        )
        .unwrap();
        let modal = app.shell.port_forward_start_modal().unwrap();
        assert_eq!(modal.local_port_draft, "18080");
        assert!(!modal.pending);

        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            },
        );
        let accepted = app.pending_port_forwards[0].request.clone();
        let mut active = failed;
        active.state = PortForwardSessionState::Active;
        active.failure = None;
        active.local_addr = "127.0.0.1:18080".into();
        active.revision = 8;
        app.handle_event(
            server_message(&ServerFrame::response(
                accepted.id().clone(),
                PortForwardStartResponse {
                    session: active.clone(),
                },
            )),
            0,
            0,
        )
        .unwrap();
        app.finish_port_forward_requests();

        assert!(app.shell.port_forward_start_modal().is_none());
        let manager = app
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == crate::workspace::WindowKind::PortForwards)
            .unwrap();
        assert_eq!(
            app.workspace()
                .port_forward_state(manager.id)
                .unwrap()
                .focused_session
                .as_deref(),
            Some(active.id.as_str())
        );
    }

    #[test]
    fn cancelled_modal_start_success_preserves_reopened_modal_and_focuses_session() {
        let source = failed_port_forward("new-session", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&source.target);
        let first_generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "first", 8_080);
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(source.target.clone(), 18_080).unwrap();
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation: first_generation,
            },
        );
        let accepted = app.pending_port_forwards[0].request.clone();

        app.shell.dismiss_port_forward_start();
        let reopened_generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "second", 9_090);
        app.shell
            .port_forward_start_modal_mut()
            .unwrap()
            .local_port_draft = "19090".into();

        let mut active = source;
        active.state = PortForwardSessionState::Active;
        active.failure = None;
        active.local_addr = "127.0.0.1:18080".into();
        active.revision = 8;
        app.handle_event(
            server_message(&ServerFrame::response(
                accepted.id().clone(),
                PortForwardStartResponse {
                    session: active.clone(),
                },
            )),
            0,
            0,
        )
        .unwrap();
        app.finish_port_forward_requests();

        let modal = app.shell.port_forward_start_modal().unwrap();
        assert_eq!(modal.generation, reopened_generation);
        assert_eq!(modal.remote_label, "second");
        assert_eq!(modal.local_port_draft, "19090");
        assert!(!modal.pending);
        assert_eq!(modal.error, None);
        let manager = app
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == crate::workspace::WindowKind::PortForwards)
            .expect("successful starts activate the port-forward manager");
        assert_eq!(
            app.workspace()
                .port_forward_state(manager.id)
                .unwrap()
                .focused_session
                .as_deref(),
            Some(active.id.as_str())
        );
    }

    #[test]
    fn context_switch_retires_same_poll_completed_port_forward_result() {
        let source = failed_port_forward("new-session", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&source.target);
        let generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "web · 8080/TCP", 8_080);
        app.shell.port_forward_start_modal_mut().unwrap().pending = true;
        let request = PortForwardStartRequest::try_target(source.target.clone(), 18_080).unwrap();
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation,
            },
        );
        let completed = app.pending_port_forwards[0].request.clone();
        let mut active = source;
        active.state = PortForwardSessionState::Active;
        active.failure = None;
        active.local_addr = "127.0.0.1:18080".into();
        active.revision = 8;
        app.handle_event(
            server_message(&ServerFrame::response(
                completed.id().clone(),
                PortForwardStartResponse { session: active },
            )),
            0,
            0,
        )
        .unwrap();
        assert!(!app.client.is_pending(&completed));
        assert_eq!(app.pending_port_forwards.len(), 1);

        app.handle_workspace_event(WorkspaceEvent::ContextSwitched {
            to: "next-context".into(),
        });

        assert!(app.pending_port_forwards.is_empty());
        assert!(
            app.client.take(completed).is_none(),
            "context retirement consumes the completed result budget entry"
        );
    }

    #[test]
    fn newest_issued_success_keeps_focus_when_older_completion_arrives_later() {
        let source = failed_port_forward("source", 7);
        let (mut app, _) = ready_app_with_authorized_pod_port_forward(&source.target);
        let request = PortForwardStartRequest::try_target(source.target.clone(), 18_080).unwrap();

        let first_generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "first", 18_080);
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request: request.clone(),
                generation: first_generation,
            },
        );
        let first = app.pending_port_forwards[0].request.clone();

        let second_generation =
            app.shell
                .open_port_forward_start(source.target.clone(), "second", 18_080);
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Start {
                request,
                generation: second_generation,
            },
        );
        let second = app.pending_port_forwards[1].request.clone();

        let mut first_session = source.clone();
        first_session.id = PortForwardSessionId::try_new("first-session").unwrap();
        first_session.state = PortForwardSessionState::Active;
        first_session.failure = None;
        first_session.local_addr = "127.0.0.1:18080".into();
        first_session.revision = 8;
        let mut second_session = first_session.clone();
        second_session.id = PortForwardSessionId::try_new("second-session").unwrap();
        second_session.local_addr = "127.0.0.1:18081".into();
        second_session.revision = 9;

        app.handle_event(
            server_message(&ServerFrame::response(
                second.id().clone(),
                PortForwardStartResponse {
                    session: second_session.clone(),
                },
            )),
            0,
            0,
        )
        .unwrap();
        app.finish_port_forward_requests();
        app.handle_event(
            server_message(&ServerFrame::response(
                first.id().clone(),
                PortForwardStartResponse {
                    session: first_session,
                },
            )),
            0,
            0,
        )
        .unwrap();
        app.finish_port_forward_requests();

        let manager = app
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == crate::workspace::WindowKind::PortForwards)
            .unwrap();
        assert_eq!(
            app.workspace()
                .port_forward_state(manager.id)
                .unwrap()
                .focused_session
                .as_deref(),
            Some(second_session.id.as_str())
        );
    }

    #[test]
    fn retry_conflict_is_an_app_overlay_and_later_success_clears_it() {
        let (mut app, _) = ready_app();
        let source = failed_port_forward("failed", 7);
        app.client
            .apply_port_forward_response(
                k10s_protocol::REQUEST_PORT_FORWARD_START,
                &serde_json::to_value(PortForwardStartResponse {
                    session: source.clone(),
                })
                .unwrap(),
            )
            .unwrap();

        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Retry(source.id.clone()),
        );
        let frame = app.client.take_outbound().expect("retry request is queued");
        let k10s_protocol::ClientPayload::Request(wire) = frame.decode_payload().unwrap() else {
            panic!("retry must queue a request");
        };
        let retry: PortForwardStartRequest = serde_json::from_value(wire.payload).unwrap();
        assert_eq!(retry.target(), &source.target);
        assert_eq!(retry.local_port(), source.requested_local_port);
        let rejected = app.pending_port_forwards[0].request.clone();
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(rejected.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(ErrorFrame::new(
                    ErrorCode::Conflict,
                    "local port 18080 is already in use",
                    Retryability::UserAction,
                    ErrorScope::Request,
                    "retry-error",
                ))
                .unwrap(),
            }),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.port_forward_retry_error(&source.id),
            Some(crate::ui::RETRY_LOCAL_PORT_GUIDANCE)
        );
        assert_eq!(app.client.port_forward_sessions(), vec![&source]);

        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Retry(source.id.clone()),
        );
        let accepted = app.pending_port_forwards[0].request.clone();
        let mut replacement = source.clone();
        replacement.id = PortForwardSessionId::try_new("replacement").unwrap();
        replacement.state = PortForwardSessionState::Active;
        replacement.failure = None;
        replacement.local_addr = "127.0.0.1:18080".into();
        replacement.revision = 8;
        app.handle_event(
            server_message(&ServerFrame::response(
                accepted.id().clone(),
                PortForwardStartResponse {
                    session: replacement.clone(),
                },
            )),
            0,
            0,
        )
        .unwrap();
        app.finish_port_forward_requests();

        assert_eq!(app.port_forward_retry_error(&source.id), None);
        let manager = app
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == crate::workspace::WindowKind::PortForwards)
            .unwrap();
        assert_eq!(
            app.workspace()
                .port_forward_state(manager.id)
                .unwrap()
                .focused_session
                .as_deref(),
            Some("replacement")
        );
    }

    #[test]
    fn retry_server_rejection_is_scoped_to_its_source_row() {
        let (mut app, _) = ready_app();
        let source = failed_port_forward("failed", 7);
        app.client
            .apply_port_forward_response(
                k10s_protocol::REQUEST_PORT_FORWARD_START,
                &serde_json::to_value(PortForwardStartResponse {
                    session: source.clone(),
                })
                .unwrap(),
            )
            .unwrap();
        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Retry(source.id.clone()),
        );
        let rejected = app.pending_port_forwards[0].request.clone();

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(rejected.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(ErrorFrame::new(
                    ErrorCode::Unauthorized,
                    "access denied by policy",
                    Retryability::Never,
                    ErrorScope::Request,
                    "retry-forbidden",
                ))
                .unwrap(),
            }),
            0,
            0,
        )
        .unwrap();

        assert_eq!(
            app.port_forward_retry_error(&source.id),
            Some("access denied by policy")
        );
        assert_eq!(app.port_forward_error, None);
    }

    #[test]
    fn retry_local_dispatch_rejection_is_scoped_to_its_source_row() {
        let (mut app, _) = ready_app();
        let source = failed_port_forward("failed", 7);
        app.client
            .apply_port_forward_response(
                k10s_protocol::REQUEST_PORT_FORWARD_START,
                &serde_json::to_value(PortForwardStartResponse {
                    session: source.clone(),
                })
                .unwrap(),
            )
            .unwrap();
        app.client.application_close();

        app.process_port_forward_action(
            &egui::Context::default(),
            crate::ui::PortForwardAction::Retry(source.id.clone()),
        );

        assert!(app.pending_port_forwards.is_empty());
        assert_eq!(
            app.port_forward_retry_error(&source.id),
            Some("Port forwarding is not available right now")
        );
        assert_eq!(app.port_forward_error, None);
    }

    fn ready_app_with_minor(minor: u16) -> (K10sApp, Rc<RefCell<FactoryState>>) {
        let bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome_with_minor(minor)),
                server_message(&bootstrap),
            ]),
            overflowed: false,
        }]);
        app.poll_at(100, 0);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        (app, state)
    }

    fn namespace_row(name: &str, revision: u64) -> ResourceListRow {
        ResourceListRow {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: GroupVersionKind::core("v1", "Namespace"),
                namespace: None,
                name: name.into(),
                uid: format!("uid-{name}"),
            },
            revision: BackendRevision::new(revision),
            labels: Default::default(),
            summary: String::new(),
            created_at: String::new(),
            projection: None,
        }
    }

    fn namespace_frame(
        subscription: &SubscriptionId,
        kind: ServerKind,
        sequence: u64,
        payload: serde_json::Value,
    ) -> ServerFrame {
        ServerFrame {
            kind,
            request_id: None,
            subscription_id: Some(subscription.clone()),
            sequence: Some(sequence),
            payload,
        }
    }

    fn complete_namespace_snapshot(
        app: &mut K10sApp,
        subscription: &SubscriptionId,
        sequence: u64,
        rows: Vec<ResourceListRow>,
    ) {
        for frame in [
            namespace_frame(
                subscription,
                ServerKind::SnapshotBegin,
                sequence,
                serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            ),
            namespace_frame(
                subscription,
                ServerKind::SnapshotChunk,
                sequence + 1,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(sequence + 1),
                        rows,
                    })
                    .unwrap(),
                })
                .unwrap(),
            ),
            namespace_frame(
                subscription,
                ServerKind::SnapshotEnd,
                sequence + 2,
                serde_json::to_value(SnapshotEnd {
                    checksum: "test".into(),
                })
                .unwrap(),
            ),
        ] {
            app.handle_event(server_message(&frame), 0, 0).unwrap();
        }
    }

    fn complete_resource_snapshot(
        app: &mut K10sApp,
        subscription: &SubscriptionId,
        revision: u64,
        rows: Vec<ResourceListRow>,
        initial: bool,
    ) {
        let mut frames = Vec::new();
        if initial {
            frames.push(ServerFrame {
                kind: ServerKind::Subscribed,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(Subscribed).unwrap(),
            });
        }
        frames.extend([
            ServerFrame {
                kind: ServerKind::SnapshotBegin,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            },
            ServerFrame {
                kind: ServerKind::SnapshotChunk,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(revision),
                        rows,
                    })
                    .unwrap(),
                })
                .unwrap(),
            },
            ServerFrame {
                kind: ServerKind::SnapshotEnd,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(SnapshotEnd {
                    checksum: format!("snapshot-{revision}"),
                })
                .unwrap(),
            },
        ]);
        for frame in frames {
            app.handle_event(server_message(&frame), 0, 0).unwrap();
        }
    }

    fn apply_resource_changed(
        app: &mut K10sApp,
        subscription: &SubscriptionId,
        row: ResourceListRow,
    ) {
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Event,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_CHANGED.into(),
                    revision: Some(row.revision.to_string()),
                    payload: serde_json::to_value(ResourceChanged {
                        identity: row.identity.clone(),
                        row,
                    })
                    .unwrap(),
                })
                .unwrap(),
            }),
            0,
            0,
        )
        .unwrap();
    }

    fn apply_resource_gone(
        app: &mut K10sApp,
        subscription: &SubscriptionId,
        identity: ResourceIdentity,
        revision: u64,
    ) {
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Event,
                request_id: None,
                subscription_id: Some(subscription.clone()),
                sequence: None,
                payload: serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_GONE.into(),
                    revision: Some(revision.to_string()),
                    payload: serde_json::to_value(ResourceGone {
                        identity,
                        revision: BackendRevision::new(revision),
                    })
                    .unwrap(),
                })
                .unwrap(),
            }),
            0,
            0,
        )
        .unwrap();
    }

    fn deployment_row(identity: ResourceIdentity, revision: u64) -> ResourceListRow {
        ResourceListRow {
            identity,
            revision: BackendRevision::new(revision),
            labels: Default::default(),
            summary: "Ready".into(),
            created_at: String::new(),
            projection: None,
        }
    }

    fn overlapping_deployment_sources(
        app: &mut K10sApp,
    ) -> (WindowId, SubscriptionId, WindowId, SubscriptionId) {
        let broad_window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
                WorkloadKind::Deployments,
            ))
        {
            app.handle_workspace_event(event);
        }
        let narrow_window = app
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| {
                window.kind == crate::workspace::WindowKind::Workload(WorkloadKind::Deployments)
                    && window.id != broad_window
            })
            .map(|window| window.id)
            .next()
            .unwrap();
        app.web_set_namespace_scope(narrow_window, NamespaceScope::Namespace("default".into()));
        let subscription_for = |app: &K10sApp, window| {
            let key = app.window_subscriptions.get(&window).unwrap();
            app.resource_subscriptions
                .get(key)
                .unwrap()
                .live
                .id()
                .clone()
        };
        (
            broad_window,
            subscription_for(app, broad_window),
            narrow_window,
            subscription_for(app, narrow_window),
        )
    }

    fn projected_detail_lifecycle(
        app: &K10sApp,
        identity: &ResourceIdentity,
    ) -> Option<super::DetailLifecycle> {
        app.build_resource_feed()
            .detail_authority
            .get(identity)
            .map(|authority| authority.lifecycle)
    }

    fn freshness_arbitration_app(
        older_source_freshness: WindowFreshness,
    ) -> (K10sApp, WindowId, ResourceIdentity, ResourceIdentity) {
        let (mut app, _) = ready_app();
        let (older_window, older_source, _, newer_source) =
            overlapping_deployment_sources(&mut app);
        let target = deployment_identity("web-frontend");
        let unrelated = deployment_identity("unrelated-api");
        complete_resource_snapshot(
            &mut app,
            &older_source,
            10,
            vec![deployment_row(target.clone(), 10)],
            true,
        );
        complete_resource_snapshot(
            &mut app,
            &newer_source,
            20,
            vec![
                deployment_row(target.clone(), 20),
                deployment_row(unrelated.clone(), 20),
            ],
            true,
        );
        app.window_freshness_overrides
            .insert(older_window, older_source_freshness);
        for identity in [&target, &unrelated] {
            let detail = deployment_detail_fixture(identity);
            app.details.insert(identity.clone(), detail.clone());
            app.primary_details
                .insert(identity.clone(), PrimaryDetailState::Loaded(detail));
        }
        (app, older_window, target, unrelated)
    }

    fn deployment_detail_fixture(identity: &ResourceIdentity) -> ResourceDetailResponse {
        ResourceDetailResponse {
            identity: identity.clone(),
            revision: BackendRevision::new(20),
            created_at: String::new(),
            owner_references: Vec::new(),
            sections: Vec::new(),
            events: Vec::new(),
            events_condition: k10s_protocol::EventsCondition::Available,
            related: Vec::new(),
            capabilities: ResourceCapabilities {
                can_scale: true,
                ..ResourceCapabilities::default()
            },
            manifest: "apiVersion: apps/v1\nkind: Deployment\n".into(),
            projection: None,
        }
    }

    fn deployment_identity(name: &str) -> ResourceIdentity {
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        }
    }

    fn pin_deployment_without_request(app: &mut K10sApp, identity: &ResourceIdentity) -> WindowId {
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, identity.clone()));
        window
    }

    fn pod_identity(name: &str) -> ResourceIdentity {
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        }
    }

    #[test]
    fn committed_context_change_revokes_external_shell_and_clears_requests() {
        let (mut app, _) = ready_app();
        app.drain_app_events();
        app.shell.set_external_shell_availability(
            crate::ui::ExternalShellAvailability::Available { generation: 9 },
        );
        app.external_shell_requests
            .push(crate::ui::ExternalShellTarget {
                generation: 9,
                namespace: "default".into(),
                pod: "api".into(),
                uid: "uid-api".into(),
                container: "api".into(),
                program: "/bin/sh".into(),
            });

        app.commit_context_layout("other".into());

        assert_eq!(
            app.shell.external_shell_availability(),
            crate::ui::ExternalShellAvailability::Unavailable
        );
        assert!(app.drain_external_shell_requests().is_empty());
        assert_eq!(
            app.drain_app_events(),
            vec![super::K10sAppEvent::CommittedContextChanged {
                context: "other".into()
            }]
        );
    }

    #[test]
    fn successful_reconnect_install_revokes_external_shell_synchronously() {
        let (mut app, state) = test_app(vec![
            ConnectionScript::default(),
            ConnectionScript::default(),
        ]);
        app.transient_loss(10, 0);
        app.shell.set_external_shell_availability(
            crate::ui::ExternalShellAvailability::Available { generation: 8 },
        );
        app.external_shell_requests
            .push(crate::ui::ExternalShellTarget {
                generation: 8,
                namespace: "default".into(),
                pod: "api".into(),
                uid: "uid-api".into(),
                container: "api".into(),
                program: "/bin/sh".into(),
            });

        app.reconnect_if_due(u64::MAX, 0);

        assert_eq!(state.borrow().connect_count, 2);
        assert_eq!(
            app.shell.external_shell_availability(),
            crate::ui::ExternalShellAvailability::Unavailable
        );
        assert!(app.external_shell_requests.is_empty());
    }

    #[test]
    fn external_shell_dispatch_revalidates_generation_identity_container_and_authority() {
        let (mut app, _) = ready_app();
        let pod = pod_identity("api");
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let subscription = app
            .resource_subscriptions
            .get(app.window_subscriptions.get(&window).unwrap())
            .unwrap()
            .live
            .id()
            .clone();
        complete_resource_snapshot(
            &mut app,
            &subscription,
            1,
            vec![deployment_row(pod.clone(), 1)],
            true,
        );
        app.web_select_resource(window, pod.clone());
        let detail = super::stream_lifecycle_tests::detail_with_container(&pod, "api");
        app.details.insert(pod.clone(), detail.clone());
        app.primary_details
            .insert(pod.clone(), PrimaryDetailState::Loaded(detail));
        assert!(
            app.build_resource_feed()
                .detail_authority
                .get(&pod)
                .is_some_and(|authority| authority.mutations_allowed()),
            "fixture must be authoritative: {:?}",
            app.build_resource_feed().detail_authority.get(&pod)
        );
        app.shell.set_external_shell_availability(
            crate::ui::ExternalShellAvailability::Available { generation: 10 },
        );
        app.handle_resource_action(ResourceAction::OpenExternalShell {
            window,
            target: crate::ui::ExternalShellTarget {
                generation: 9,
                namespace: "default".into(),
                pod: "api".into(),
                uid: "uid-api".into(),
                container: "api".into(),
                program: "/bin/sh".into(),
            },
        });
        assert!(app.drain_external_shell_requests().is_empty());

        let valid = crate::ui::ExternalShellTarget {
            generation: 10,
            namespace: "default".into(),
            pod: "api".into(),
            uid: "uid-api".into(),
            container: "api".into(),
            program: "/bin/sh".into(),
        };
        app.handle_resource_action(ResourceAction::OpenExternalShell {
            window,
            target: valid.clone(),
        });
        assert_eq!(app.drain_external_shell_requests(), vec![valid.clone()]);
        for invalid in [
            crate::ui::ExternalShellTarget {
                namespace: "other".into(),
                ..valid.clone()
            },
            crate::ui::ExternalShellTarget {
                pod: "other".into(),
                ..valid.clone()
            },
            crate::ui::ExternalShellTarget {
                uid: "other".into(),
                ..valid.clone()
            },
            crate::ui::ExternalShellTarget {
                container: "other".into(),
                ..valid.clone()
            },
        ] {
            app.handle_resource_action(ResourceAction::OpenExternalShell {
                window,
                target: invalid,
            });
            assert!(app.drain_external_shell_requests().is_empty());
        }
        app.window_freshness_overrides.insert(
            window,
            WindowFreshness::Failed {
                message: "stale".into(),
            },
        );
        app.handle_resource_action(ResourceAction::OpenExternalShell {
            window,
            target: valid,
        });
        assert!(app.drain_external_shell_requests().is_empty());
    }

    fn pin_pod_without_request(app: &mut K10sApp, identity: &ResourceIdentity) -> WindowId {
        let events = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods));
        let window = events
            .iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(*id),
                _ => None,
            })
            .expect("pod window opens");
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, identity.clone()));
        window
    }

    fn metrics_response(identity: &ResourceIdentity) -> ResourceMetricsResponse {
        ResourceMetricsResponse {
            identity: identity.clone(),
            metrics: PodMetrics {
                availability: MetricsAvailability::Available,
                cpu_millicores: Some(135),
                memory_bytes: Some(96 * 1024 * 1024),
                collected_at: Some("2026-08-31T12:00:00Z".into()),
            },
            containers: vec![
                ContainerMetrics {
                    name: "api".into(),
                    metrics: PodMetrics {
                        availability: MetricsAvailability::Available,
                        cpu_millicores: Some(100),
                        memory_bytes: Some(64 * 1024 * 1024),
                        collected_at: Some("2026-08-31T12:00:00Z".into()),
                    },
                },
                ContainerMetrics {
                    name: "telemetry-sidecar".into(),
                    metrics: PodMetrics {
                        availability: MetricsAvailability::Partial,
                        cpu_millicores: Some(35),
                        memory_bytes: None,
                        collected_at: Some("2026-08-31T12:00:00Z".into()),
                    },
                },
            ],
        }
    }

    #[test]
    fn production_pod_metrics_request_reaches_feed_with_named_containers() {
        let (mut app, state) = ready_app();
        let pod = pod_identity("api-7f9d");
        pin_pod_without_request(&mut app, &pod);

        app.refresh_details_at(1);

        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.metrics")
                .count(),
            1
        );
        let request = app
            .metric_requests
            .get(&pod)
            .expect("metrics request is tracked")
            .request
            .clone();
        let response = metrics_response(&pod);
        app.handle_event(
            server_message(&ServerFrame::response(
                request.id().clone(),
                response.clone(),
            )),
            2,
            0,
        )
        .unwrap();
        app.refresh_details_at(2);

        assert_eq!(app.build_resource_feed().metrics.get(&pod), Some(&response));
        assert_eq!(
            app.build_resource_feed().metrics[&pod].containers[1].name,
            "telemetry-sidecar"
        );
    }

    #[test]
    fn duplicate_pod_detail_windows_share_one_metrics_query() {
        let (mut app, state) = ready_app();
        let pod = pod_identity("shared");
        pin_pod_without_request(&mut app, &pod);
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(pod.clone()))
        {
            app.handle_workspace_event(event);
        }
        pin_deployment_without_request(&mut app, &deployment_identity("not-a-pod"));
        let wrong_gvk = ResourceIdentity {
            gvk: GroupVersionKind {
                group: "metrics.k8s.io".into(),
                version: "v1beta1".into(),
                kind: "Pod".into(),
            },
            ..pod_identity("wrong-gvk")
        };
        pin_pod_without_request(&mut app, &wrong_gvk);

        app.refresh_details_at(1);
        app.refresh_details_at(2);

        assert_eq!(app.metric_requests.len(), 1);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.metrics")
                .count(),
            1
        );
    }

    #[test]
    fn mismatched_or_failed_metrics_never_enter_the_feed_or_storm_retries() {
        let (mut app, state) = ready_app();
        let pod = pod_identity("guarded");
        pin_pod_without_request(&mut app, &pod);
        app.refresh_details_at(1);
        let request = app.metric_requests[&pod].request.clone();
        let mut mismatch = metrics_response(&pod);
        mismatch.identity.uid = "uid-replacement".into();

        app.handle_event(
            server_message(&ServerFrame::response(request.id().clone(), mismatch)),
            2,
            0,
        )
        .unwrap();
        app.refresh_details_at(2);
        app.refresh_details_at(3);

        assert!(!app.build_resource_feed().metrics.contains_key(&pod));
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.metrics")
                .count(),
            1,
            "a failed exact-identity query waits for the normal refresh cadence"
        );

        app.refresh_details_at(30_002);
        let retry = app.metric_requests[&pod].request.clone();
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(retry.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(ErrorFrame::new(
                    ErrorCode::Unauthorized,
                    "metrics unavailable",
                    Retryability::Never,
                    ErrorScope::Request,
                    retry.id().as_str(),
                ))
                .unwrap(),
            }),
            30_003,
            0,
        )
        .unwrap();
        app.refresh_details_at(30_003);
        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(!app.build_resource_feed().metrics.contains_key(&pod));
        app.refresh_details_at(30_004);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.metrics")
                .count(),
            2,
            "a server error also waits for the normal refresh cadence"
        );
    }

    #[test]
    fn stale_pod_metrics_fail_closed_while_one_refresh_is_in_flight() {
        let (mut app, _) = ready_app();
        let pod = pod_identity("refreshing");
        pin_pod_without_request(&mut app, &pod);
        app.metrics.insert(pod.clone(), metrics_response(&pod));
        app.metric_checked_at.insert(pod.clone(), 10);

        app.refresh_details_at(30_009);
        assert!(app.build_resource_feed().metrics.contains_key(&pod));
        app.refresh_details_at(30_010);

        assert!(!app.build_resource_feed().metrics.contains_key(&pod));
        assert!(app.metric_requests.contains_key(&pod));
    }

    #[test]
    fn reconnectable_metrics_error_enters_the_normal_transport_recovery_path() {
        let (mut app, _) = ready_app();
        let pod = pod_identity("reconnect");
        pin_pod_without_request(&mut app, &pod);
        app.refresh_details_at(1);
        let request = app.metric_requests[&pod].request.clone();

        let result = app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(request.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(ErrorFrame::new(
                    ErrorCode::Internal,
                    "metrics collector moved",
                    Retryability::AfterReconnect,
                    ErrorScope::Request,
                    request.id().as_str(),
                ))
                .unwrap(),
            }),
            100,
            7,
        );

        assert!(matches!(result, Err(AppEventError::Transient)));
        assert_eq!(app.client.phase(), ClientPhase::Disconnected);
    }

    #[test]
    fn pod_metrics_are_pruned_on_close_gone_and_context_reset() {
        let (mut app, state) = ready_app();
        let pod = pod_identity("ephemeral");
        let window = pin_pod_without_request(&mut app, &pod);
        app.refresh_details_at(1);
        let closing = app.metric_requests[&pod].request.clone();

        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(window));
        app.refresh_details_at(2);
        assert!(!app.metrics.contains_key(&pod));
        assert!(!app.metric_checked_at.contains_key(&pod));
        assert!(!app.metric_requests.contains_key(&pod));
        assert!(state.borrow().sent.iter().any(|frame| {
            frame.kind == ClientKind::CancelRequest
                && frame.request_id.as_ref() == Some(closing.id())
        }));

        app.handle_event(
            server_message(&ServerFrame::response(
                closing.id().clone(),
                metrics_response(&pod),
            )),
            3,
            0,
        )
        .expect("a response already queued before cancellation is consumed");
        app.refresh_details_at(3);
        assert!(!app.build_resource_feed().metrics.contains_key(&pod));

        pin_pod_without_request(&mut app, &pod);
        app.metrics.insert(pod.clone(), metrics_response(&pod));
        app.metric_checked_at.insert(pod.clone(), 4);
        app.record_detail_lifecycle(
            SubscriptionId::new("pod-source"),
            pod.clone(),
            BackendRevision::new(9),
            super::DetailLifecycle::Gone,
        );
        app.refresh_details_at(5);
        assert!(!app.build_resource_feed().metrics.contains_key(&pod));

        app.detail_lifecycles.clear();
        app.metrics.insert(pod.clone(), metrics_response(&pod));
        app.metric_checked_at.insert(pod.clone(), 6);
        app.retire_resource_context();
        assert!(app.metrics.is_empty());
        assert!(app.metric_checked_at.is_empty());
        assert!(app.metric_requests.is_empty());
    }

    #[test]
    fn primary_loading_revokes_dialog_authority_before_mutation_dispatch() {
        let (mut app, _) = ready_app();
        let target = deployment_identity("web-frontend");
        let window = pin_deployment_without_request(&mut app, &target);
        let detail = deployment_detail_fixture(&target);
        app.details.insert(target.clone(), detail.clone());
        app.primary_details
            .insert(target.clone(), PrimaryDetailState::Loaded(detail));
        app.window_retained_rows.insert(
            window,
            vec![ResourceListRow {
                identity: target.clone(),
                revision: BackendRevision::new(1),
                labels: Default::default(),
                summary: "20/20 ready".into(),
                created_at: String::new(),
                projection: None,
            }],
        );
        app.window_freshness_overrides.insert(
            window,
            WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
        );
        assert!(app.mutation_authority_allows(&target));
        app.shell
            .dialogs_mut()
            .open_scale(window, target.clone(), Some(20));

        // The dialog was valid when opened, then its exact primary detail
        // began reloading before submission reached the application layer.
        app.primary_details
            .insert(target, PrimaryDetailState::Loading);
        app.shell.dialogs_mut().submit_active(window);
        app.process_dialog_actions().unwrap();

        assert!(
            app.pending_mutations.is_empty(),
            "revoked authority must block the command before client dispatch"
        );
    }

    #[test]
    fn detail_lifecycle_uses_newest_source_revision_and_gone_wins_equal_revision() {
        let (mut app, _) = ready_app();
        let (_, broad, _, narrow) = overlapping_deployment_sources(&mut app);
        let identity = deployment_identity("web-frontend");
        complete_resource_snapshot(
            &mut app,
            &broad,
            10,
            vec![deployment_row(identity.clone(), 10)],
            true,
        );
        complete_resource_snapshot(
            &mut app,
            &narrow,
            20,
            vec![deployment_row(identity.clone(), 20)],
            true,
        );
        apply_resource_gone(&mut app, &broad, identity.clone(), 30);

        apply_resource_changed(&mut app, &narrow, deployment_row(identity.clone(), 25));
        assert_eq!(
            projected_detail_lifecycle(&app, &identity),
            Some(super::DetailLifecycle::Gone),
            "an older Present from an overlapping source cannot revive newer Gone"
        );

        apply_resource_changed(&mut app, &narrow, deployment_row(identity.clone(), 30));
        assert_eq!(
            projected_detail_lifecycle(&app, &identity),
            Some(super::DetailLifecycle::Gone),
            "equal-revision Gone must dominate Present across sources"
        );
        assert!(
            !app.mutation_authority_allows(&identity),
            "final application dispatch must consume the ordered aggregate"
        );
    }

    #[test]
    fn older_stale_source_disables_detail_frame_and_dialog_for_newer_live_identity() {
        fn render(ui: &mut egui::Ui, app: &mut K10sApp) {
            app.render_ui(ui);
        }

        let (mut app, _, target, _) = freshness_arbitration_app(WindowFreshness::StaleRetrying {
            last_sync_age: "30s ago".into(),
            retry_in: "3s".into(),
            attempt: 1,
        });
        let detail = deployment_detail_fixture(&target);
        app.details.insert(target.clone(), detail.clone());
        app.primary_details
            .insert(target.clone(), PrimaryDetailState::Loaded(detail));
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target.clone()))
        {
            app.handle_workspace_event(event);
        }
        let detail_window = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| {
                matches!(
                    &window.content,
                    WindowContent::Detail(detail) if detail.identity == target
                )
            })
            .map(|window| window.id)
            .unwrap();
        app.shell
            .dialogs_mut()
            .open_scale(detail_window, target, Some(20));

        let mut harness = Harness::builder()
            .with_size(egui::vec2(1_280.0, 800.0))
            .build_ui_state(render, app);
        for _ in 0..3 {
            harness.step();
        }

        assert!(
            harness
                .get_by_role_and_label(
                    egui::accesskit::Role::Window,
                    "Deployment · default / web-frontend",
                )
                .get_by_role_and_label(egui::accesskit::Role::Button, "Scale…")
                .accesskit_node()
                .is_disabled(),
            "every contributing source must gate frame mutations"
        );
        assert!(
            harness
                .get_by_role_and_label(egui::accesskit::Role::Window, "Scale workload")
                .get_by_role_and_label(egui::accesskit::Role::Button, "Apply scale")
                .accesskit_node()
                .is_disabled(),
            "every contributing source must gate the active dialog"
        );
    }

    #[test]
    fn older_forbidden_source_blocks_final_dispatch_for_newer_live_identity() {
        let (mut app, window, target, _) = freshness_arbitration_app(WindowFreshness::Forbidden {
            user: "alice@example.com".into(),
            verb: "list".into(),
            resource: "deployments".into(),
            scope: "--all-namespaces".into(),
        });
        app.shell.dialogs_mut().open_scale(window, target, Some(20));
        app.shell.dialogs_mut().submit_active(window);

        app.process_dialog_actions().unwrap();

        assert!(
            app.pending_mutations.is_empty(),
            "final dispatch must fail closed when any contributing source is forbidden"
        );
    }

    #[test]
    fn retiring_stale_source_restores_live_authority_without_affecting_unrelated_identity() {
        let (mut app, stale_window, target, unrelated) =
            freshness_arbitration_app(WindowFreshness::StaleRetrying {
                last_sync_age: "30s ago".into(),
                retry_in: "3s".into(),
                attempt: 1,
            });
        assert!(!app.mutation_authority_allows(&target));
        assert!(
            app.mutation_authority_allows(&unrelated),
            "a stale source that does not contain the exact identity is irrelevant"
        );

        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(stale_window))
        {
            app.handle_workspace_event(event);
        }
        app.reconcile_selected_resource_streams();

        assert!(
            app.mutation_authority_allows(&target),
            "retiring the stale contribution leaves the live source authoritative"
        );
        assert!(app.mutation_authority_allows(&unrelated));
    }

    #[test]
    fn replacement_snapshot_omission_marks_only_that_source_identity_gone() {
        let (mut app, _) = ready_app();
        let (_, broad, _, narrow) = overlapping_deployment_sources(&mut app);
        let omitted = deployment_identity("web-frontend");
        let narrow_only = deployment_identity("api-server");
        complete_resource_snapshot(
            &mut app,
            &broad,
            10,
            vec![deployment_row(omitted.clone(), 10)],
            true,
        );
        complete_resource_snapshot(
            &mut app,
            &narrow,
            12,
            vec![deployment_row(narrow_only.clone(), 12)],
            true,
        );

        complete_resource_snapshot(&mut app, &broad, 20, Vec::new(), false);

        assert_eq!(
            projected_detail_lifecycle(&app, &omitted),
            Some(super::DetailLifecycle::Gone)
        );
        assert_eq!(
            projected_detail_lifecycle(&app, &narrow_only),
            Some(super::DetailLifecycle::Present),
            "a broad-source omission cannot tombstone an unrelated exact identity from another source"
        );
    }

    #[test]
    fn retiring_gone_source_reveals_remaining_present_source() {
        let (mut app, _) = ready_app();
        let (broad_window, broad, _, narrow) = overlapping_deployment_sources(&mut app);
        let identity = deployment_identity("web-frontend");
        complete_resource_snapshot(
            &mut app,
            &broad,
            10,
            vec![deployment_row(identity.clone(), 10)],
            true,
        );
        complete_resource_snapshot(
            &mut app,
            &narrow,
            12,
            vec![deployment_row(identity.clone(), 12)],
            true,
        );
        apply_resource_gone(&mut app, &broad, identity.clone(), 20);
        assert_eq!(
            projected_detail_lifecycle(&app, &identity),
            Some(super::DetailLifecycle::Gone)
        );

        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(broad_window))
        {
            app.handle_workspace_event(event);
        }
        app.reconcile_selected_resource_streams();

        assert!(!app.detail_lifecycles.contains_key(&broad));
        assert_eq!(
            projected_detail_lifecycle(&app, &identity),
            Some(super::DetailLifecycle::Present),
            "closing a source must retire only that source's tombstones"
        );
    }

    #[test]
    fn recreated_uid_is_independent_from_old_uid_tombstone_across_sources() {
        let (mut app, _) = ready_app();
        let (_, broad, _, narrow) = overlapping_deployment_sources(&mut app);
        let old = deployment_identity("web-frontend");
        let mut recreated = old.clone();
        recreated.uid = "uid-web-frontend-recreated".into();
        complete_resource_snapshot(
            &mut app,
            &broad,
            10,
            vec![deployment_row(old.clone(), 10)],
            true,
        );
        complete_resource_snapshot(
            &mut app,
            &narrow,
            15,
            vec![deployment_row(recreated.clone(), 15)],
            true,
        );
        apply_resource_gone(&mut app, &broad, old.clone(), 20);

        assert_eq!(
            projected_detail_lifecycle(&app, &old),
            Some(super::DetailLifecycle::Gone)
        );
        assert_eq!(
            projected_detail_lifecycle(&app, &recreated),
            Some(super::DetailLifecycle::Present)
        );
    }

    #[test]
    fn lifecycle_tombstones_are_bounded_and_preserve_tracked_identity() {
        const EXPECTED_TOMBSTONE_CAP: usize = 128;

        let (mut app, _) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        let key = app.window_subscriptions.get(&window).unwrap();
        let broad_subscription = app
            .resource_subscriptions
            .get(key)
            .unwrap()
            .live
            .id()
            .clone();
        complete_resource_snapshot(&mut app, &broad_subscription, 1, Vec::new(), true);
        let pinned = deployment_identity("pinned-target");
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(pinned.clone()))
        {
            app.handle_workspace_event(event);
        }
        app.reconcile_resource_streams("dev-local").unwrap();
        let exact_subscription = app
            .resource_subscriptions
            .iter()
            .find(|(key, _)| key.identity.as_ref() == Some(&pinned))
            .map(|(_, subscription)| subscription.live.id().clone())
            .expect("dedicated detail retains its exact watch");
        complete_resource_snapshot(&mut app, &exact_subscription, 2, Vec::new(), false);
        for index in 0..(EXPECTED_TOMBSTONE_CAP + 64) {
            let churn = deployment_identity(&format!("churn-{index}"));
            apply_resource_gone(
                &mut app,
                &broad_subscription,
                churn,
                u64::try_from(index).unwrap() + 3,
            );
        }

        assert_eq!(
            projected_detail_lifecycle(&app, &pinned),
            Some(super::DetailLifecycle::Gone),
            "tracked pinned identities survive cap eviction"
        );
        assert!(
            app.detail_lifecycles
                .values()
                .map(|source| source.entries.len())
                .sum::<usize>()
                <= EXPECTED_TOMBSTONE_CAP + 1,
            "untracked high-churn tombstones must remain bounded"
        );

        let pinned_window = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| {
                matches!(
                    &window.content,
                    WindowContent::Detail(detail) if detail.identity == pinned
                )
            })
            .map(|window| window.id)
            .unwrap();
        for event in app
            .shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(pinned_window))
        {
            app.handle_workspace_event(event);
        }
        assert!(
            app.detail_lifecycles
                .values()
                .map(|source| source.entries.len())
                .sum::<usize>()
                <= EXPECTED_TOMBSTONE_CAP,
            "an identity that is no longer pinned must join bounded tombstone eviction"
        );
    }

    fn exhaust_request_capacity(app: &mut K10sApp) {
        for _ in 0..1_000 {
            if app.client.begin(Query::Bootstrap).is_err() {
                return;
            }
        }
        panic!("client request capacity did not exhaust");
    }

    pub(super) fn saturate_cancel_outbound(app: &mut K10sApp) {
        let mut requests = Vec::new();
        for _ in 0..1_000 {
            match app.client.begin(Query::Bootstrap) {
                Ok(request) => requests.push(request),
                Err(_) => break,
            }
        }
        for request in requests {
            if app.client.cancel(&request).is_err() {
                return;
            }
        }
        panic!("client cancellation capacity did not saturate");
    }

    #[test]
    fn primary_begin_failure_is_explicitly_failed_without_pending_loading() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("begin-fails");
        pin_deployment_without_request(&mut app, &identity);
        exhaust_request_capacity(&mut app);

        app.refresh_details_at(1);

        assert!(matches!(
            app.primary_details.get(&identity),
            Some(PrimaryDetailState::Failed(error))
                if error.message() == "could not request details"
        ));
        assert!(!app.detail_requests.contains_key(&identity));
        app.refresh_details_at(2);
        assert!(matches!(
            app.primary_details.get(&identity),
            Some(PrimaryDetailState::Failed(_))
        ));
    }

    #[test]
    fn one_retry_action_starts_exactly_one_replacement_request() {
        let (mut app, state) = ready_app();
        let identity = deployment_identity("retry-once");
        let window = pin_deployment_without_request(&mut app, &identity);
        app.primary_details.insert(
            identity.clone(),
            PrimaryDetailState::Failed(SafeUiError::new("failed")),
        );
        let before_primary = state.borrow().sent.len();
        app.handle_resource_action(ResourceAction::RetryPrimary(identity.clone()));
        app.refresh_details_at(1);
        app.refresh_details_at(2);
        assert_eq!(
            state.borrow().sent[before_primary..]
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.detail")
                .count(),
            1
        );

        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        app.relations.insert(
            identity.clone(),
            RelationState::Failed(SafeUiError::new("failed")),
        );
        let before_relations = state.borrow().sent.len();
        app.handle_resource_action(ResourceAction::RetryRelations(identity));
        app.refresh_details_at(3);
        app.refresh_details_at(4);
        assert_eq!(
            state.borrow().sent[before_relations..]
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            1
        );
    }

    #[test]
    fn initial_relation_begin_failure_never_leaves_loading_without_pending() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("relations-begin-fails");
        let window = pin_deployment_without_request(&mut app, &identity);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        exhaust_request_capacity(&mut app);

        app.refresh_details_at(1);

        assert!(matches!(
            app.relations.get(&identity),
            Some(RelationState::Failed(error))
                if error.message() == "could not request related resources"
        ));
        assert!(!app.relation_requests.contains_key(&identity));
    }

    #[test]
    fn saturated_unpin_retains_correlations_until_terminal_frames_are_consumed() {
        let (mut app, _) = ready_app();
        let old = deployment_identity("old-selection");
        let new = deployment_identity("new-selection");
        let window = pin_deployment_without_request(&mut app, &old);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        app.refresh_details_at(1);
        let primary = app
            .detail_requests
            .get(&old)
            .expect("primary pending")
            .request
            .clone();
        let relations = app
            .relation_requests
            .get(&old)
            .expect("relations pending")
            .request
            .clone();
        saturate_cancel_outbound(&mut app);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, new));

        app.refresh_details_at(2);

        assert!(app.detail_requests.contains_key(&old));
        assert!(app.relation_requests.contains_key(&old));
        let feed = app.build_resource_feed();
        assert!(!feed.primary_details.contains_key(&old));
        assert!(!feed.relations.contains_key(&old));

        for request in [&primary, &relations] {
            let rejection = ServerFrame {
                kind: ServerKind::Error,
                request_id: Some(request.id().clone()),
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(ErrorFrame::new(
                    ErrorCode::Cancelled,
                    "retired request",
                    Retryability::Never,
                    ErrorScope::Request,
                    request.id().as_str(),
                ))
                .unwrap(),
            };
            app.handle_event(server_message(&rejection), 3, 0).unwrap();
        }

        assert!(!app.detail_requests.contains_key(&old));
        assert!(!app.relation_requests.contains_key(&old));
        assert!(app.client.take_failure(primary).is_none());
        assert!(app.client.take_failure(relations).is_none());
        let feed = app.build_resource_feed();
        assert!(!feed.primary_details.contains_key(&old));
        assert!(!feed.relations.contains_key(&old));
    }

    #[test]
    fn stale_relation_begin_failure_retains_rows_and_exposes_refresh_error() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("refresh-begin-fails");
        let window = pin_deployment_without_request(&mut app, &identity);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        app.relations.insert(
            identity.clone(),
            RelationState::Loaded {
                response: std::sync::Arc::new(k10s_protocol::ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(1),
                    groups: Vec::new(),
                }),
                loaded_at_ms: 0,
                refreshing: false,
                refresh_error: None,
            },
        );
        exhaust_request_capacity(&mut app);

        app.refresh_details_at(30_000);

        assert!(matches!(
            app.relations.get(&identity),
            Some(RelationState::Loaded {
                refreshing: false,
                refresh_error: Some(error),
                response,
                ..
            }) if error.message() == "could not request related resources"
                && response.identity == identity
        ));
        assert!(!app.relation_requests.contains_key(&identity));
    }

    #[test]
    fn stale_relation_server_failure_retains_rows_and_safe_message() {
        let (mut app, state) = ready_app();
        let identity = deployment_identity("refresh-server-fails");
        let window = pin_deployment_without_request(&mut app, &identity);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        app.relations.insert(
            identity.clone(),
            RelationState::Loaded {
                response: std::sync::Arc::new(k10s_protocol::ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(1),
                    groups: Vec::new(),
                }),
                loaded_at_ms: 0,
                refreshing: false,
                refresh_error: None,
            },
        );
        app.refresh_details_at(30_000);
        let request_id = app
            .relation_requests
            .get(&identity)
            .expect("refresh is pending")
            .request
            .id()
            .clone();
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(request_id.clone()),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::Unauthorized,
                "relations forbidden",
                Retryability::AfterRefresh,
                ErrorScope::Request,
                request_id.as_str(),
            ))
            .unwrap(),
        };

        app.handle_event(server_message(&rejection), 30_001, 0)
            .unwrap();

        assert!(matches!(
            app.relations.get(&identity),
            Some(RelationState::Loaded {
                refreshing: false,
                refresh_error: Some(error),
                response,
                ..
            }) if error.message() == "relations forbidden" && response.identity == identity
        ));
        assert!(!app.relation_requests.contains_key(&identity));

        let after_failure = state.borrow().sent.len();
        app.refresh_details_at(30_002);
        app.refresh_details_at(60_000);
        assert_eq!(
            state.borrow().sent.len(),
            after_failure,
            "passive frames must not retry a recorded refresh failure"
        );
        assert!(matches!(
            app.relations.get(&identity),
            Some(RelationState::Loaded {
                refresh_error: Some(error),
                ..
            }) if error.message() == "relations forbidden"
        ));

        app.handle_resource_action(ResourceAction::RetryRelations(identity.clone()));
        app.refresh_details_at(60_001);
        app.refresh_details_at(60_002);
        assert_eq!(
            state.borrow().sent[after_failure..]
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            1,
            "one explicit retry starts one replacement"
        );
        assert!(matches!(
            app.relations.get(&identity),
            Some(RelationState::Loaded {
                refreshing: true,
                refresh_error: None,
                ..
            })
        ));
    }

    #[test]
    fn deployment_relations_are_demanded_on_overview_and_deduplicated_across_views() {
        let (mut app, state) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        let identity = deployment_identity("api");
        app.web_select_resource(window, identity.clone());
        app.refresh_details_at(1);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            1,
            "an open Deployment Overview demands rollout relations"
        );
        app.web_set_detail_tab(window, crate::workspace::DetailTab::Pods);
        app.refresh_details_at(2);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            1,
            "switching Overview to Pods shares the pending relations request"
        );

        let dedicated = app
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(window) => Some(window),
                _ => None,
            })
            .expect("dedicated Deployment detail opens on Overview");
        app.refresh_details_at(3);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            1,
            "integrated and dedicated views deduplicate by exact identity"
        );
        assert!(matches!(
            app.relations.get(&identity),
            Some(crate::ui::RelationState::Loading)
        ));

        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(window));
        app.refresh_details_at(4);
        assert!(app.relation_requests.contains_key(&identity));
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(dedicated));
        app.refresh_details_at(5);
        assert!(app.relation_requests.is_empty());
        assert!(app.relations.is_empty());
    }

    #[test]
    fn dedicated_pod_and_deployment_details_retain_exact_live_authority_without_lists() {
        for (kind, identity) in [
            (WorkloadKind::Pods, pod_identity("api-7f9d")),
            (WorkloadKind::Deployments, deployment_identity("api")),
        ] {
            let (mut app, state) = ready_app();
            let list = app.web_activate_workload(kind).unwrap();
            let dedicated = app
                .shell
                .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()))
                .into_iter()
                .find_map(|event| match event {
                    WorkspaceEvent::Opened(window) => Some(window),
                    _ => None,
                })
                .unwrap();
            app.reconcile_resource_streams("dev-local").unwrap();
            app.flush_outbound().unwrap();

            let watches = resource_watches(&state);
            let is_exact = |watch: &k10s_protocol::ResourceWatchSpec| {
                watch.context == identity.context
                    && watch.gvk == identity.gvk
                    && watch.namespace == identity.namespace
                    && watch.identity.as_ref().is_some_and(|pinned| {
                        pinned.name == identity.name && pinned.uid == identity.uid
                    })
            };
            let exact = watches
                .iter()
                .find(|watch| is_exact(watch))
                .expect("dedicated detail owns an exact-identity watch");
            assert_eq!(exact.context, identity.context);
            assert_eq!(exact.gvk, identity.gvk);
            assert_eq!(exact.namespace, identity.namespace);
            let exact_subscription = app
                .resource_subscriptions
                .iter()
                .find_map(|(key, retained)| {
                    (key.identity.as_ref() == Some(&identity)).then(|| retained.live.id().clone())
                })
                .unwrap();
            let list_subscription = app
                .window_subscriptions
                .get(&list)
                .and_then(|key| app.resource_subscriptions.get(key))
                .map(|retained| retained.live.id().clone())
                .unwrap();

            complete_resource_snapshot(
                &mut app,
                &list_subscription,
                9,
                vec![deployment_row(identity.clone(), 9)],
                true,
            );
            let detail = deployment_detail_fixture(&identity);
            app.details.insert(identity.clone(), detail.clone());
            app.primary_details
                .insert(identity.clone(), PrimaryDetailState::Loaded(detail));
            assert!(
                !app.mutation_authority_allows(&identity),
                "the broad List cannot grant authority before the exact snapshot"
            );
            complete_resource_snapshot(
                &mut app,
                &exact_subscription,
                10,
                vec![deployment_row(identity.clone(), 10)],
                true,
            );
            app.window_freshness_overrides.insert(
                list,
                WindowFreshness::StaleRetrying {
                    last_sync_age: "1m".into(),
                    retry_in: "1s".into(),
                    attempt: 2,
                },
            );
            assert!(
                app.mutation_authority_allows(&identity),
                "stale List authority must not poison the dedicated exact source"
            );

            app.shell
                .apply_workspace_command(WorkspaceCommand::CloseWindow(list));
            app.reconcile_resource_streams("dev-local").unwrap();
            app.flush_outbound().unwrap();
            assert!(
                resource_watches(&state).iter().any(is_exact),
                "closing the List must not retire dedicated Detail authority"
            );
            assert!(app.shell.workspace().window(dedicated).is_some());
            assert!(
                app.mutation_authority_allows(&identity),
                "the dedicated exact snapshot independently grants live authority"
            );

            complete_resource_snapshot(&mut app, &exact_subscription, 11, Vec::new(), false);
            assert_eq!(
                projected_detail_lifecycle(&app, &identity),
                Some(super::DetailLifecycle::Gone)
            );
            assert!(
                !app.mutation_authority_allows(&identity),
                "an empty exact relist revokes authority fail-closed"
            );
        }
    }

    #[test]
    fn legacy_peer_keeps_dedicated_detail_authority_fail_closed() {
        let (mut app, state) = ready_app_with_minor(3);
        let list = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let identity = pod_identity("legacy-api");
        app.shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()));
        app.reconcile_resource_streams("dev-local").unwrap();
        app.flush_outbound().unwrap();

        assert!(
            resource_watches(&state)
                .iter()
                .all(|watch| watch.identity.is_none()),
            "a peer below protocol v1.4 must never receive an exact selector"
        );
        let list_subscription = app
            .window_subscriptions
            .get(&list)
            .and_then(|key| app.resource_subscriptions.get(key))
            .map(|retained| retained.live.id().clone())
            .unwrap();
        complete_resource_snapshot(
            &mut app,
            &list_subscription,
            1,
            vec![deployment_row(identity.clone(), 1)],
            true,
        );
        let detail = deployment_detail_fixture(&identity);
        app.details.insert(identity.clone(), detail.clone());
        app.primary_details
            .insert(identity.clone(), PrimaryDetailState::Loaded(detail));
        assert!(
            !app.mutation_authority_allows(&identity),
            "broad legacy List data must not substitute for exact authority"
        );
    }

    #[test]
    fn mismatched_pinned_context_never_opens_or_grants_exact_authority() {
        let (mut app, state) = ready_app();
        let identity = ResourceIdentity {
            context: "other-cluster".into(),
            ..pod_identity("foreign-api")
        };
        app.shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()));
        app.reconcile_resource_streams("dev-local").unwrap();
        app.flush_outbound().unwrap();

        assert!(resource_watches(&state).iter().all(|watch| {
            watch
                .identity
                .as_ref()
                .is_none_or(|pinned| pinned.name != identity.name || pinned.uid != identity.uid)
        }));
        assert!(matches!(
            app.client.subscribe_resource_exact(
                "dev-local",
                "",
                "v1",
                "Pod",
                identity.namespace.clone(),
                Some(identity.clone()),
            ),
            Err(super::ClientError::InvalidState(
                "exact resource identity does not match watch selector"
            ))
        ));
        assert!(
            !app.mutation_authority_allows(&identity),
            "a restored Detail from another context stays fail-closed"
        );
    }

    #[test]
    fn relations_refresh_at_exactly_thirty_seconds_and_keep_stale_rows() {
        let (mut app, state) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        let identity = deployment_identity("api");
        app.web_select_resource(window, identity.clone());
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        assert!(app.cancel_relation_request(&identity));
        app.relations.insert(
            identity.clone(),
            crate::ui::RelationState::Loaded {
                response: std::sync::Arc::new(k10s_protocol::ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(1),
                    groups: Vec::new(),
                }),
                loaded_at_ms: 10,
                refreshing: false,
                refresh_error: None,
            },
        );
        let before_relations = state
            .borrow()
            .sent
            .iter()
            .filter_map(request_kind)
            .filter(|kind| kind == "resource.relations")
            .count();
        app.refresh_details_at(30_009);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            before_relations
        );
        app.refresh_details_at(30_010);
        assert!(matches!(
            app.relations.get(&identity),
            Some(crate::ui::RelationState::Loaded {
                refreshing: true,
                ..
            })
        ));
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.relations")
                .count(),
            before_relations + 1
        );
    }

    #[test]
    fn resource_feed_relation_projection_shares_large_response_storage() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("large-controller");
        app.relations.insert(
            identity.clone(),
            RelationState::Loaded {
                response: std::sync::Arc::new(k10s_protocol::ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(1),
                    groups: vec![k10s_protocol::RelatedGroup {
                        title: "Pods".into(),
                        gvk: GroupVersionKind::core("v1", "Pod"),
                        rows: (0..5_000)
                            .map(|index| namespace_row(&format!("related-{index}"), index))
                            .collect(),
                    }],
                }),
                loaded_at_ms: 0,
                refreshing: false,
                refresh_error: None,
            },
        );
        let original = match app.relations.get(&identity).unwrap() {
            RelationState::Loaded { response, .. } => std::sync::Arc::as_ptr(response),
            _ => unreachable!(),
        };

        let feed = app.build_resource_feed();
        let projected = match feed.relations.get(&identity).unwrap() {
            RelationState::Loaded { response, .. } => std::sync::Arc::as_ptr(response),
            _ => unreachable!(),
        };
        assert_eq!(
            original, projected,
            "frame projection must share model data"
        );
    }

    fn start_relation_request(app: &mut K10sApp, identity: &ResourceIdentity) -> PendingRequest {
        let window = pin_deployment_without_request(app, identity);
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetActiveTab(
                window,
                crate::workspace::DetailTab::Pods,
            ));
        app.refresh_details_at(1);
        app.relation_requests
            .get(identity)
            .expect("relation request starts")
            .request
            .clone()
    }

    fn relation_response_frame(
        request: &PendingRequest,
        identity: ResourceIdentity,
    ) -> ServerFrame {
        ServerFrame::response(
            request.id().clone(),
            k10s_protocol::ResourceRelationsResponse {
                identity,
                revision: BackendRevision::new(2),
                groups: Vec::new(),
            },
        )
    }

    #[test]
    fn same_name_different_uid_relation_response_never_enters_app_cache() {
        let (mut app, _) = ready_app();
        let expected = deployment_identity("same-name");
        let primary: k10s_protocol::ResourceDetailResponse =
            serde_json::from_value(serde_json::json!({
            "identity": expected,
            "revision": 1,
            "createdAt": "2026-08-21T00:00:00Z",
            "ownerReferences": [],
            "sections": [],
            "events": [],
            "capabilities": {
                "canEditYaml": true,
                "canDelete": true,
                "canScale": true,
                "canViewLogs": false,
                "canExec": false
            },
            "manifest": "kind: Deployment"
            }))
            .unwrap();
        app.details.insert(expected.clone(), primary.clone());
        let request = start_relation_request(&mut app, &expected);
        let mut wrong = expected.clone();
        wrong.uid = "replacement-uid".into();

        app.handle_event(
            server_message(&relation_response_frame(&request, wrong)),
            2,
            0,
        )
        .unwrap();
        app.refresh_details_at(2);

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(!app.client.is_pending(&request));
        assert_eq!(app.details.get(&expected), Some(&primary));
        assert!(matches!(
            app.relations.get(&expected),
            Some(RelationState::Failed(_))
        ));
        assert!(!matches!(
            app.relations.get(&expected),
            Some(RelationState::Loaded { .. })
        ));
        app.handle_resource_action(ResourceAction::RetryRelations(expected.clone()));
        app.refresh_details_at(3);
        assert!(app.relation_requests.contains_key(&expected));
        assert!(!matches!(
            app.build_resource_feed().relations.get(&expected),
            Some(RelationState::Loaded { .. })
        ));
    }

    #[test]
    fn old_generation_relation_response_is_consumed_without_cache_insertion() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("old-generation");
        let request = start_relation_request(&mut app, &identity);
        app.resource_generation = app.resource_generation.wrapping_add(1);

        app.handle_event(
            server_message(&relation_response_frame(&request, identity.clone())),
            2,
            0,
        )
        .unwrap();
        app.refresh_details_at(2);

        assert!(!app.relation_requests.contains_key(&identity));
        assert!(!app.relations.contains_key(&identity));
        assert!(!app.build_resource_feed().relations.contains_key(&identity));
    }

    #[test]
    fn retired_context_relation_response_is_consumed_without_cache_insertion() {
        let (mut app, _) = ready_app();
        let identity = deployment_identity("retired-context");
        let request = start_relation_request(&mut app, &identity);
        app.client.local_ui_mut().selected_context = Some("other-context".into());

        app.handle_event(
            server_message(&relation_response_frame(&request, identity.clone())),
            2,
            0,
        )
        .unwrap();
        app.refresh_details_at(2);
        app.refresh_details_at(3);

        assert!(!app.relation_requests.contains_key(&identity));
        assert!(!app.relations.contains_key(&identity));
        assert!(!app.build_resource_feed().relations.contains_key(&identity));
    }

    #[test]
    fn opening_visible_lists_subscribes_only_their_demand() {
        let (mut app, state) = ready_app();
        assert!(resource_watches(&state).is_empty());

        app.web_activate_workload(WorkloadKind::Pods);
        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].gvk, GroupVersionKind::core("v1", "Pod"));
        assert_eq!(watches[0].namespace.as_deref(), Some("default"));

        app.web_activate_services();
        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 2);
        assert_eq!(watches[1].gvk, GroupVersionKind::core("v1", "Service"));
        assert_eq!(watches[1].namespace.as_deref(), Some("default"));
    }

    #[test]
    fn rejected_watch_retains_its_rows_without_obscuring_a_healthy_sibling() {
        let (mut app, state) = ready_app();
        let deployments = app
            .web_activate_workload(WorkloadKind::Deployments)
            .expect("deployments window opens");
        let pods = app
            .web_activate_workload(WorkloadKind::Pods)
            .expect("pods window opens");

        let subscription_for = |app: &K10sApp, kind: &str| {
            app.resource_subscriptions
                .iter()
                .find_map(|(key, entry)| (key.gvk.kind == kind).then(|| entry.live.id().clone()))
                .expect("watch exists")
        };
        let deployment_subscription = subscription_for(&app, "Deployment");
        let pod_subscription = subscription_for(&app, "Pod");
        let row = |kind: &str, name: &str| ResourceListRow {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: if kind == "Pod" {
                    GroupVersionKind::core("v1", "Pod")
                } else {
                    GroupVersionKind {
                        group: "apps".into(),
                        version: "v1".into(),
                        kind: kind.into(),
                    }
                },
                namespace: Some("default".into()),
                name: name.into(),
                uid: format!("uid-{name}"),
            },
            revision: BackendRevision::new(7),
            labels: Default::default(),
            summary: "Ready".into(),
            created_at: String::new(),
            projection: None,
        };
        let apply_snapshot =
            |app: &mut K10sApp, subscription: &SubscriptionId, row: ResourceListRow| {
                for (kind, payload) in [
                    (
                        ServerKind::Subscribed,
                        serde_json::to_value(Subscribed).unwrap(),
                    ),
                    (
                        ServerKind::SnapshotBegin,
                        serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
                    ),
                    (
                        ServerKind::SnapshotChunk,
                        serde_json::to_value(SnapshotChunk {
                            chunk_index: 0,
                            data: serde_json::to_value(ResourceSnapshotPage {
                                revision: BackendRevision::new(7),
                                rows: vec![row.clone()],
                            })
                            .unwrap(),
                        })
                        .unwrap(),
                    ),
                    (
                        ServerKind::SnapshotEnd,
                        serde_json::to_value(SnapshotEnd {
                            checksum: "fixture".into(),
                        })
                        .unwrap(),
                    ),
                ] {
                    app.handle_event(
                        server_message(&ServerFrame {
                            kind,
                            request_id: None,
                            subscription_id: Some(subscription.clone()),
                            sequence: None,
                            payload,
                        }),
                        0,
                        0,
                    )
                    .unwrap();
                }
            };
        apply_snapshot(
            &mut app,
            &deployment_subscription,
            row("Deployment", "healthy-api"),
        );
        apply_snapshot(&mut app, &pod_subscription, row("Pod", "cached-pod"));

        let denied = ErrorFrame::new(
            ErrorCode::Unauthorized,
            "pods are forbidden",
            Retryability::AfterRefresh,
            ErrorScope::Subscription,
            "pod-watch",
        )
        .with_details(serde_json::json!({
            "user": "alice@example.com",
            "verb": "list",
            "resource": "pods",
            "scope": "--namespace=default"
        }));
        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::Error,
                request_id: None,
                subscription_id: Some(pod_subscription),
                sequence: None,
                payload: serde_json::to_value(denied).unwrap(),
            }),
            0,
            0,
        )
        .unwrap();

        let feed = app.build_resource_feed();
        assert_eq!(
            feed.window_lists[&deployments][0].identity.name,
            "healthy-api"
        );
        assert!(matches!(
            feed.window_freshness.get(&deployments),
            Some(WindowFreshness::Live { .. })
        ));
        assert_eq!(feed.window_lists[&pods][0].identity.name, "cached-pod");
        assert!(matches!(
            feed.window_freshness.get(&pods),
            Some(WindowFreshness::Forbidden { verb, resource, .. })
                if verb == "list" && resource == "pods"
        ));
        app.reconcile_selected_resource_streams();
        assert_eq!(
            resource_watches(&state).len(),
            2,
            "a rejected watch waits for explicit retry"
        );
        app.handle_resource_action(ResourceAction::RetryWindow(pods));
        assert_eq!(
            resource_watches(&state).len(),
            3,
            "retry starts one replacement watch"
        );

        app.transient_loss(100, 250);
        assert_eq!(
            app.build_resource_feed().window_lists[&pods][0]
                .identity
                .name,
            "cached-pod"
        );
        app.handle_resource_action(ResourceAction::RetryWindow(pods));
        assert_eq!(
            state.borrow().connect_count,
            2,
            "window retry during transport staleness starts transport recovery"
        );
        assert_eq!(
            app.build_resource_feed().window_lists[&pods][0]
                .identity
                .name,
            "cached-pod",
            "cached rows remain visible until a replacement snapshot succeeds"
        );
    }

    #[test]
    fn custom_resource_picker_requests_types_without_opening_a_watch() {
        let (mut app, state) = ready_app();
        app.web_activate_workload(WorkloadKind::CustomResources);

        assert!(resource_watches(&state).is_empty());
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.types")
                .count(),
            1
        );
    }

    #[test]
    fn namespace_catalog_stays_warm_after_all_namespaced_windows_close() {
        let (mut app, state) = ready_app();
        let pod = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        app.web_activate_services();

        let namespaces: Vec<_> = all_resource_watches(&state)
            .into_iter()
            .filter(|watch| watch.gvk == GroupVersionKind::core("v1", "Namespace"))
            .collect();
        assert_eq!(namespaces.len(), 1);
        assert_eq!(app.window_subscriptions.len(), 2);
        assert!(!app.window_subscriptions.contains_key(&WindowId(0)));
        let namespace_subscription = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        complete_namespace_snapshot(
            &mut app,
            &namespace_subscription,
            1,
            vec![namespace_row("warm", 1)],
        );

        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(pod));
        app.reconcile_selected_resource_streams();
        assert!(app.namespace_subscription.is_some());
        let service = app
            .workspace()
            .windows()
            .iter()
            .find(|window| matches!(window.kind, crate::workspace::WindowKind::Services))
            .unwrap()
            .id;
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(service));
        app.reconcile_selected_resource_streams();
        assert_eq!(
            app.namespace_subscription
                .as_ref()
                .map(|(_, live)| live.id()),
            Some(&namespace_subscription)
        );
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["warm".into()])
        );

        app.web_activate_workload(WorkloadKind::Deployments);

        assert_eq!(
            app.namespace_subscription
                .as_ref()
                .map(|(_, live)| live.id()),
            Some(&namespace_subscription)
        );
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["warm".into()])
        );
        assert_eq!(
            all_resource_watches(&state)
                .iter()
                .filter(|watch| watch.gvk == GroupVersionKind::core("v1", "Namespace"))
                .count(),
            1
        );
    }

    #[test]
    fn cluster_scoped_or_unselected_custom_windows_do_not_demand_namespace_catalog() {
        let (mut app, state) = ready_app();
        app.types_context = Some("dev-local".to_owned());
        app.resource_types = vec![k10s_protocol::ResourceTypeEntry {
            gvk: GroupVersionKind {
                group: "example.io".to_owned(),
                version: "v1".to_owned(),
                kind: "ClusterThing".to_owned(),
            },
            namespaced: false,
        }];
        let window = app
            .web_activate_workload(WorkloadKind::CustomResources)
            .unwrap();
        assert!(app.namespace_subscription.is_none());
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetCustomKind(
                window,
                Some("example.io/v1/ClusterThing".to_owned()),
            ));
        app.reconcile_selected_resource_streams();
        assert!(app.namespace_subscription.is_none());
        assert!(
            resource_watches(&state)
                .iter()
                .all(|watch| watch.gvk.kind != "Namespace")
        );
    }

    #[test]
    fn selected_custom_scope_starts_namespace_catalog_and_then_keeps_it_warm() {
        let (mut app, state) = ready_app();
        app.types_context = Some("dev-local".to_owned());
        app.resource_types = vec![
            k10s_protocol::ResourceTypeEntry {
                gvk: GroupVersionKind {
                    group: "example.io".into(),
                    version: "v1".into(),
                    kind: "ClusterThing".into(),
                },
                namespaced: false,
            },
            k10s_protocol::ResourceTypeEntry {
                gvk: GroupVersionKind {
                    group: "example.io".into(),
                    version: "v1".into(),
                    kind: "Widget".into(),
                },
                namespaced: true,
            },
        ];
        let window = app
            .web_activate_workload(WorkloadKind::CustomResources)
            .unwrap();
        for (selected, catalog_started) in [
            ("example.io/v1/ClusterThing", false),
            ("example.io/v1/Widget", true),
            ("example.io/v1/ClusterThing", true),
        ] {
            app.shell
                .apply_workspace_command(WorkspaceCommand::SetCustomKind(
                    window,
                    Some(selected.to_owned()),
                ));
            app.reconcile_selected_resource_streams();
            assert_eq!(app.namespace_subscription.is_some(), catalog_started);
        }
        assert_eq!(
            all_resource_watches(&state)
                .iter()
                .filter(|watch| watch.gvk == GroupVersionKind::core("v1", "Namespace"))
                .count(),
            1,
            "the namespaced selection creates exactly one catalog watch"
        );
    }

    #[test]
    fn namespace_watch_deltas_update_ready_names_without_changing_explicit_scope() {
        let (mut app, _) = ready_app();
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let explicit = NamespaceScope::Namespace("team-a".to_owned());
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                window,
                explicit.clone(),
            ));
        app.reconcile_selected_resource_streams();
        let subscription = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        complete_namespace_snapshot(&mut app, &subscription, 1, vec![namespace_row("alpha", 1)]);

        let beta = namespace_row("beta", 4);
        app.handle_event(
            server_message(&namespace_frame(
                &subscription,
                ServerKind::Event,
                4,
                serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_CHANGED.into(),
                    revision: Some("4".into()),
                    payload: serde_json::to_value(ResourceChanged {
                        identity: beta.identity.clone(),
                        row: beta.clone(),
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["alpha".into(), "beta".into()])
        );
        app.handle_event(
            server_message(&namespace_frame(
                &subscription,
                ServerKind::Event,
                5,
                serde_json::to_value(Event {
                    event_kind: k10s_protocol::RESOURCE_EVENT_GONE.into(),
                    revision: Some("5".into()),
                    payload: serde_json::to_value(ResourceGone {
                        identity: beta.identity,
                        revision: BackendRevision::new(5),
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["alpha".into()])
        );
        let scope = app
            .workspace()
            .windows()
            .iter()
            .find(|candidate| candidate.id == window)
            .and_then(|candidate| match &candidate.content {
                WindowContent::Resource(state) => Some(state.namespace_scope.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(scope, explicit);
    }

    #[test]
    fn resync_and_transport_loss_clear_ready_namespace_catalog() {
        let (mut app, _) = ready_app();
        app.web_activate_services();
        let subscription = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        complete_namespace_snapshot(&mut app, &subscription, 1, vec![namespace_row("alpha", 1)]);
        assert!(matches!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(_)
        ));

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::ResyncRequired,
                request_id: None,
                subscription_id: None,
                sequence: Some(4),
                payload: serde_json::json!({"reason": "journal unavailable"}),
            }),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Loading
        );

        complete_namespace_snapshot(&mut app, &subscription, 5, vec![namespace_row("beta", 5)]);
        app.transient_loss(100, 0);
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Loading
        );
    }

    #[test]
    fn explicit_full_resync_rebuilds_namespace_catalog_before_reusing_it() {
        let (mut app, state) = ready_app();
        let window = app.web_activate_services().unwrap();
        let original = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        complete_namespace_snapshot(&mut app, &original, 1, vec![namespace_row("old", 1)]);

        app.handle_resource_action(ResourceAction::FullResyncWindow(window));

        let replacement = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        assert_ne!(replacement, original);
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Loading
        );
        assert_eq!(
            all_resource_watches(&state)
                .iter()
                .filter(|watch| watch.gvk == GroupVersionKind::core("v1", "Namespace"))
                .count(),
            2
        );

        complete_namespace_snapshot(&mut app, &replacement, 4, vec![namespace_row("new", 4)]);
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["new".into()])
        );
    }

    #[test]
    fn accepted_resync_enters_resource_recovery_and_retires_detail_requests() {
        let (mut app, state) = ready_app();
        let identity = deployment_identity("resync-detail");
        start_relation_request(&mut app, &identity);
        let generation = app.resource_generation;
        let sent_before_resync = state.borrow().sent.len();

        app.handle_event(
            server_message(&ServerFrame {
                kind: ServerKind::ResyncRequired,
                request_id: None,
                subscription_id: None,
                sequence: Some(1),
                payload: serde_json::json!({"reason": "journal unavailable"}),
            }),
            0,
            0,
        )
        .unwrap();

        assert!(app.recovering);
        assert!(matches!(app.view, AppView::Connecting));
        assert_eq!(app.resource_generation, generation.wrapping_add(1));
        assert!(app.detail_requests.is_empty());
        assert!(app.relation_requests.is_empty());
        assert!(app.primary_details.is_empty());
        assert!(app.relations.is_empty());

        app.refresh_details_at(2);
        assert_eq!(
            state.borrow().sent[sent_before_resync..]
                .iter()
                .filter_map(request_kind)
                .filter(|kind| kind == "resource.detail" || kind == "resource.relations")
                .count(),
            0
        );
    }

    #[test]
    fn reconnect_resubscribes_namespace_once_and_waits_for_snapshot_end() {
        let (mut app, _) = ready_app();
        app.web_activate_services();
        let subscription = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        complete_namespace_snapshot(&mut app, &subscription, 1, vec![namespace_row("old", 1)]);

        app.transient_loss(100, 0);
        assert!(app.client.retry_if_due(100).unwrap());
        app.client.apply(welcome()).unwrap();
        let mut rebuilt = Vec::new();
        while let Some(frame) = app.client.take_outbound() {
            rebuilt.push(frame);
        }
        let namespace_rebuilds =
            rebuilt
                .iter()
                .filter(|frame| {
                    let Ok(k10s_protocol::ClientPayload::Subscribe(k10s_protocol::Subscribe(
                        selector,
                    ))) = frame.decode_payload()
                    else {
                        return false;
                    };
                    matches!(
                        serde_json::from_value(selector),
                        Ok(SubscriptionSelector::Resource(ref spec))
                            if spec.gvk == GroupVersionKind::core("v1", "Namespace")
                    )
                })
                .count();
        assert_eq!(namespace_rebuilds, 1, "rebuilt frames: {rebuilt:?}");
        for frame in [
            namespace_frame(
                &subscription,
                ServerKind::SnapshotBegin,
                1,
                serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            ),
            namespace_frame(
                &subscription,
                ServerKind::SnapshotChunk,
                2,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(2),
                        rows: vec![namespace_row("new", 2)],
                    })
                    .unwrap(),
                })
                .unwrap(),
            ),
        ] {
            app.client.apply(frame).unwrap();
            assert_eq!(
                app.build_resource_feed().namespace_catalog,
                NamespaceCatalogState::Loading
            );
        }
        app.client
            .apply(namespace_frame(
                &subscription,
                ServerKind::SnapshotEnd,
                3,
                serde_json::to_value(SnapshotEnd {
                    checksum: "test".into(),
                })
                .unwrap(),
            ))
            .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            NamespaceCatalogState::Ready(vec!["new".into()])
        );
    }

    #[test]
    fn namespace_catalog_waits_for_snapshot_end_and_sorts_deduplicates_names() {
        let (mut app, _) = ready_app();
        app.web_activate_workload(WorkloadKind::Pods);
        let subscription = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        let namespace_row = |name: &str, uid: &str| ResourceListRow {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: GroupVersionKind::core("v1", "Namespace"),
                namespace: None,
                name: name.into(),
                uid: uid.into(),
            },
            revision: BackendRevision::new(1),
            labels: Default::default(),
            summary: String::new(),
            created_at: String::new(),
            projection: None,
        };
        let frame = |kind, sequence, payload| ServerFrame {
            kind,
            request_id: None,
            subscription_id: Some(subscription.clone()),
            sequence: Some(sequence),
            payload,
        };
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotBegin,
                1,
                serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotChunk,
                2,
                serde_json::to_value(SnapshotChunk {
                    chunk_index: 0,
                    data: serde_json::to_value(ResourceSnapshotPage {
                        revision: BackendRevision::new(1),
                        rows: vec![
                            namespace_row("zeta", "z"),
                            namespace_row("alpha", "a"),
                            namespace_row("alpha", "a2"),
                        ],
                    })
                    .unwrap(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            crate::ui::NamespaceCatalogState::Loading
        );
        app.handle_event(
            server_message(&frame(
                ServerKind::SnapshotEnd,
                3,
                serde_json::to_value(SnapshotEnd {
                    checksum: "ok".into(),
                })
                .unwrap(),
            )),
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            crate::ui::NamespaceCatalogState::Ready(vec!["alpha".into(), "zeta".into()])
        );
    }

    #[test]
    fn exact_namespace_rejection_is_safe_guarded_and_explicitly_retryable_once() {
        let (mut app, state) = ready_app();
        app.web_activate_services();
        let rejected = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        let mut error = ErrorFrame::new(
            ErrorCode::Unauthorized,
            "namespaces are forbidden",
            Retryability::UserAction,
            ErrorScope::Subscription,
            "namespace-watch",
        );
        error.details = Some(serde_json::json!({"status": "backend raw details"}));
        let frame = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(rejected),
            sequence: Some(1),
            payload: serde_json::to_value(error).unwrap(),
        };

        app.handle_event(server_message(&frame), 0, 0).unwrap();
        assert!(app.namespace_subscription.is_none());
        assert_eq!(
            app.build_resource_feed().namespace_catalog,
            crate::ui::NamespaceCatalogState::Unavailable(SafeUiError::new(
                "namespaces are forbidden"
            ))
        );
        let before = all_resource_watches(&state).len();
        app.reconcile_selected_resource_streams();
        app.reconcile_selected_resource_streams();
        assert_eq!(all_resource_watches(&state).len(), before);

        app.handle_resource_action(ResourceAction::RetryNamespaceCatalog);
        app.reconcile_selected_resource_streams();
        assert_eq!(all_resource_watches(&state).len(), before + 1);
    }

    #[test]
    fn transport_loss_clears_rejected_namespace_catalog_when_still_demanded() {
        let (mut app, _) = ready_app();
        app.web_activate_services();
        let rejected = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        let frame = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(rejected),
            sequence: Some(1),
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::Unauthorized,
                "namespaces are forbidden",
                Retryability::UserAction,
                ErrorScope::Subscription,
                "namespace-watch",
            ))
            .unwrap(),
        };
        app.handle_event(server_message(&frame), 0, 0).unwrap();
        assert!(app.namespace_subscription.is_none());
        assert!(matches!(
            app.namespace_catalog,
            NamespaceCatalogState::Unavailable(_)
        ));

        app.transient_loss(100, 0);

        assert_eq!(app.namespace_catalog, NamespaceCatalogState::Loading);
        assert!(app.namespace_rejected_context.is_none());
    }

    #[test]
    fn all_namespaces_keeps_watch_when_context_namespace_changes() {
        let (mut app, state) = ready_app();
        let mut snapshot = app.workspace_snapshot();
        snapshot.windows.push(PersistedWindow {
            kind: PersistedWindowKind::Workload(WorkloadKind::Pods),
            title: "Pods · all namespaces".into(),
            geometry: WindowGeom::staggered(0, [800.0, 600.0]),
            z: 1,
            view: Some(PersistedListView {
                namespace_scope: NamespaceScope::AllNamespaces,
                ..Default::default()
            }),
            port_forward_view: None,
        });
        app.restore_workspace_snapshot(snapshot);
        app.reconcile_selected_resource_streams();
        let window = app
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|w| w.kind == WindowKind::Workload(WorkloadKind::Pods))
            .unwrap()
            .id;
        assert_eq!(resource_watches(&state)[0].namespace.as_deref(), None);

        let AppView::Ready { contexts, .. } = &mut app.view else {
            panic!("ready")
        };
        contexts
            .iter_mut()
            .find(|context| context.name == "dev-local")
            .unwrap()
            .namespace = Some("team-b".to_owned());
        app.reconcile_selected_resource_streams();

        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].namespace.as_deref(), None);
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| frame.kind == ClientKind::Unsubscribe)
                .count(),
            0
        );
        let key = app.window_subscriptions.get(&window).unwrap();
        assert!(matches!(
            key.scope,
            super::SubscriptionScope::Namespaced(NamespaceScope::AllNamespaces)
        ));
        assert_eq!(app.resource_subscriptions.len(), 1);
    }

    #[test]
    fn successful_resource_types_response_immediately_subscribes_selected_custom_kind() {
        let (mut app, state) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::CustomResources)
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetCustomKind(
                window,
                Some("example.io/v1/Widget".to_owned()),
            ));
        let request_id = state
            .borrow()
            .sent
            .iter()
            .find(|frame| request_kind(frame).as_deref() == Some("resource.types"))
            .and_then(|frame| frame.request_id.clone())
            .unwrap();
        let response = ServerFrame::response(
            request_id,
            k10s_protocol::ResourceTypesResponse {
                context: "dev-local".to_owned(),
                types: vec![k10s_protocol::ResourceTypeEntry {
                    gvk: GroupVersionKind {
                        group: "example.io".to_owned(),
                        version: "v1".to_owned(),
                        kind: "Widget".to_owned(),
                    },
                    namespaced: true,
                }],
            },
        );

        app.handle_event(server_message(&response), 100, 0).unwrap();
        app.finish_mutations();

        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].gvk.kind, "Widget");
    }

    #[test]
    fn equal_window_keys_share_and_last_close_unsubscribes_only_the_window_watch() {
        let (mut app, state) = ready_app();
        let first = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        let second = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        app.reconcile_selected_resource_streams();
        assert_eq!(resource_watches(&state).len(), 1);

        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(first));
        app.reconcile_selected_resource_streams();
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| frame.kind == ClientKind::Unsubscribe)
                .count(),
            0
        );
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(second));
        app.reconcile_selected_resource_streams();
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| frame.kind == ClientKind::Unsubscribe)
                .count(),
            1
        );
        assert!(app.namespace_subscription.is_some());
    }

    #[test]
    fn failed_unsubscribe_preflight_retains_app_mapping_and_client_desire() {
        let (mut app, _state) = ready_app();
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let key = app.window_subscriptions.get(&window).cloned().unwrap();
        let _extra = app
            .client
            .subscribe_resource(
                "dev-local",
                "",
                "v1",
                "ConfigMap",
                Some("default".to_owned()),
            )
            .unwrap();
        for _ in 0..255 {
            app.client.begin(Query::Bootstrap).unwrap();
        }
        assert_eq!(app.client.outbound_len(), 256);
        let desired_before = app.client.live_subscription_count();
        app.shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(window));

        assert!(app.reconcile_resource_streams("dev-local").is_err());
        assert!(app.resource_subscriptions.contains_key(&key));
        assert_eq!(app.window_subscriptions.get(&window), Some(&key));
        assert_eq!(app.client.live_subscription_count(), desired_before);
        assert_eq!(app.client.phase(), ClientPhase::Ready);
    }

    #[test]
    fn saturated_full_resync_retains_namespace_and_window_subscription_ownership() {
        let (mut app, _state) = ready_app();
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        let key = app.window_subscriptions.get(&window).cloned().unwrap();
        let namespace = app.namespace_subscription.as_ref().unwrap().1.id().clone();
        let _extra = app
            .client
            .subscribe_resource(
                "dev-local",
                "",
                "v1",
                "ConfigMap",
                Some("default".to_owned()),
            )
            .unwrap();
        for _ in 0..255 {
            app.client.begin(Query::Bootstrap).unwrap();
        }
        let desired_before = app.client.live_subscription_count();

        app.handle_resource_action(ResourceAction::FullResyncWindow(window));

        assert_eq!(
            app.namespace_subscription
                .as_ref()
                .map(|(_, live)| live.id()),
            Some(&namespace)
        );
        assert!(app.resource_subscriptions.contains_key(&key));
        assert_eq!(app.window_subscriptions.get(&window), Some(&key));
        assert_eq!(app.client.live_subscription_count(), desired_before);
    }

    #[test]
    fn failed_multi_addition_preflight_leaves_no_partial_app_or_client_state() {
        let (mut app, _state) = ready_app();
        let first = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                first,
                NamespaceScope::Namespace("a".to_owned()),
            ));
        let second = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                second,
                NamespaceScope::Namespace("b".to_owned()),
            ));
        let target_count = app.client.live_subscription_limit() - 1;
        let mut index = 0;
        while app.client.live_subscription_count() < target_count {
            app.client
                .subscribe_resource(
                    "dev-local",
                    "test.example",
                    "v1",
                    format!("Filler{index}"),
                    None,
                )
                .unwrap();
            let _ = app.client.take_outbound();
            index += 1;
        }
        let desired_before = app.client.live_subscription_count();

        assert!(app.reconcile_resource_streams("dev-local").is_err());
        assert!(app.resource_subscriptions.is_empty());
        assert!(app.window_subscriptions.is_empty());
        assert_eq!(app.client.live_subscription_count(), desired_before);
        assert_eq!(app.client.outbound_len(), 0);
    }

    #[test]
    fn namespace_scope_changes_replace_only_the_changed_reference() {
        let (mut app, state) = ready_app();
        let windows: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|namespace| {
                let id = app
                    .shell
                    .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
                        WorkloadKind::Pods,
                    ))
                    .into_iter()
                    .find_map(|event| match event {
                        WorkspaceEvent::Opened(id) => Some(id),
                        _ => None,
                    })
                    .unwrap();
                app.shell
                    .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                        id,
                        NamespaceScope::Namespace(namespace.to_owned()),
                    ));
                id
            })
            .collect();
        app.reconcile_selected_resource_streams();
        assert_eq!(resource_watches(&state).len(), 2);

        app.shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                windows[0],
                NamespaceScope::Namespace("c".to_owned()),
            ));
        app.reconcile_selected_resource_streams();
        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 3);
        assert_eq!(watches.last().unwrap().namespace.as_deref(), Some("c"));
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| frame.kind == ClientKind::Unsubscribe)
                .count(),
            1
        );
    }

    #[test]
    fn custom_gvks_are_independent_and_cluster_scoped_duplicates_share() {
        let (mut app, state) = ready_app();
        app.types_context = Some("dev-local".to_owned());
        app.resource_types = vec![
            k10s_protocol::ResourceTypeEntry {
                gvk: GroupVersionKind {
                    group: "example.io".to_owned(),
                    version: "v1".to_owned(),
                    kind: "Widget".to_owned(),
                },
                namespaced: true,
            },
            k10s_protocol::ResourceTypeEntry {
                gvk: GroupVersionKind {
                    group: "example.io".to_owned(),
                    version: "v1".to_owned(),
                    kind: "ClusterThing".to_owned(),
                },
                namespaced: false,
            },
        ];
        let mut open_custom = |key: &str, scope: NamespaceScope| {
            let id = app
                .shell
                .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
                    WorkloadKind::CustomResources,
                ))
                .into_iter()
                .find_map(|event| match event {
                    WorkspaceEvent::Opened(id) => Some(id),
                    _ => None,
                })
                .unwrap();
            app.shell
                .apply_workspace_command(WorkspaceCommand::SetCustomKind(id, Some(key.to_owned())));
            app.shell
                .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(id, scope));
        };
        open_custom(
            "example.io/v1/Widget",
            NamespaceScope::Namespace("a".to_owned()),
        );
        open_custom(
            "example.io/v1/ClusterThing",
            NamespaceScope::Namespace("a".to_owned()),
        );
        open_custom("example.io/v1/ClusterThing", NamespaceScope::AllNamespaces);
        app.reconcile_selected_resource_streams();

        let watches = resource_watches(&state);
        assert_eq!(watches.len(), 2);
        let widget = watches
            .iter()
            .find(|watch| watch.gvk.kind == "Widget")
            .unwrap();
        assert_eq!(widget.namespace.as_deref(), Some("a"));
        let cluster = watches
            .iter()
            .find(|watch| watch.gvk.kind == "ClusterThing")
            .unwrap();
        assert_eq!(cluster.namespace, None);
    }

    #[test]
    fn context_switch_round_trips_through_the_backend_before_local_commit() {
        let switched = ServerFrame::response(
            RequestId::from_u128(3),
            k10s_protocol::ContextSwitchResponse {
                current: "prod-readonly".into(),
                previous: Some("dev-local".into()),
            },
        );
        let (mut app, state) = ready_app();

        // Staging sends the switch request but moves no local state.
        app.stage_context_switch("prod-readonly", true).unwrap();
        app.flush_outbound().unwrap();
        let kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            kinds,
            ["bootstrap", "infrastructure.get", "context.switch"],
            "the staged switch is sent to the backend"
        );
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("dev-local"),
            "the selection waits for the response"
        );
        // The bootstrap committed its authoritative context into the
        // workspace, which the staged switch has not moved yet.
        assert_eq!(app.shell.workspace().context(), "dev-local");

        // Only the successful response commits the local transition.
        app.handle_event(server_message(&switched), 100, 0).unwrap();
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("prod-readonly")
        );
        assert_eq!(app.shell.workspace().context(), "prod-readonly");
        let kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            kinds,
            [
                "bootstrap",
                "infrastructure.get",
                "context.switch",
                "infrastructure.get"
            ],
            "streams and infrastructure resubscribe on the committed context"
        );
    }

    #[test]
    fn a_request_scoped_switch_rejection_keeps_the_session_and_switch_flow_alive() {
        // A third destination proves the switch flow survives the rejection.
        let mut bootstrap_payload = BootstrapResponse::fixture();
        bootstrap_payload.contexts.push(Context {
            name: "staging".into(),
            cluster: "staging-cluster".into(),
            namespace: None,
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        });
        let bootstrap = ServerFrame::response(RequestId::from_u128(1), bootstrap_payload);
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(RequestId::from_u128(3)),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(
                ErrorFrame::new(
                    ErrorCode::Conflict,
                    "context 'prod-readonly' is unavailable: plugin denied",
                    Retryability::AfterRefresh,
                    ErrorScope::Request,
                    "context-switch",
                )
                .with_details(serde_json::json!({
                    "kind": "contextUnavailable",
                    "context": "prod-readonly",
                    "reason": "plugin denied",
                })),
            )
            .unwrap(),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        app.stage_context_switch("prod-readonly", true).unwrap();
        app.flush_outbound().unwrap();

        // The rejection is projected into the switch flow itself — it never
        // becomes terminal for the session.
        app.handle_event(server_message(&rejection), 150, 0)
            .unwrap();
        assert_eq!(
            app.client.phase(),
            ClientPhase::Ready,
            "the session stays ready"
        );
        assert!(
            app.connection.is_some(),
            "the control connection stays alive"
        );
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("dev-local"),
            "the selection never moved"
        );
        assert_eq!(app.shell.workspace().context(), "dev-local");
        assert!(app.pending_switch.is_none(), "pending cleared");
        assert_eq!(
            app.failed_switch.as_deref(),
            Some("prod-readonly"),
            "the failed destination is recorded so reconciliation cannot retry-spam it"
        );
        let AppView::Ready { contexts, .. } = app.view() else {
            panic!("the session must stay ready")
        };
        let failed = contexts
            .iter()
            .find(|context| context.name == "prod-readonly")
            .expect("failed context remains visible");
        assert_eq!(
            failed.availability,
            k10s_protocol::ContextAvailability::Unavailable
        );
        assert_eq!(failed.unavailable_reason.as_deref(), Some("plugin denied"));
        assert!(
            app.bootstrap.is_some(),
            "failure queues authoritative Bootstrap"
        );

        // A NEW staged switch toward another destination still works.
        app.stage_context_switch("staging", true).unwrap();
        app.flush_outbound().unwrap();
        let kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            kinds.last(),
            Some(&"context.switch".to_owned()),
            "a fresh switch goes out after the rejection"
        );
        assert_eq!(
            app.pending_switch
                .as_ref()
                .map(|pending| pending.to.as_str()),
            Some("staging")
        );
    }

    #[test]
    fn runtime_context_unavailable_reconciles_outside_the_switch_flow() {
        let (mut app, state) = ready_app();
        app.web_activate_workload(WorkloadKind::CustomResources);
        let request_id = state
            .borrow()
            .sent
            .iter()
            .find(|frame| request_kind(frame).as_deref() == Some("resource.types"))
            .and_then(|frame| frame.request_id.clone())
            .expect("resource types request exists");
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(request_id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(
                ErrorFrame::new(
                    ErrorCode::Conflict,
                    "context unavailable",
                    Retryability::AfterRefresh,
                    ErrorScope::Request,
                    "resource-types",
                )
                .with_details(serde_json::json!({
                    "kind": "contextUnavailable",
                    "context": "dev-local",
                    "reason": "runtime plugin denied",
                })),
            )
            .unwrap(),
        };

        app.handle_event(server_message(&rejection), 150, 0)
            .expect("runtime auth failure stays request-scoped");

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(app.connection.is_some());
        assert_eq!(app.shell.workspace().context(), "dev-local");
        let AppView::Ready { contexts, .. } = app.view() else {
            panic!("the ready workspace remains visible")
        };
        let failed = contexts
            .iter()
            .find(|context| context.name == "dev-local")
            .expect("active context remains visible");
        assert_eq!(
            failed.availability,
            k10s_protocol::ContextAvailability::Unavailable
        );
        assert_eq!(
            failed.unavailable_reason.as_deref(),
            Some("runtime plugin denied")
        );
        assert!(app.bootstrap.is_some(), "authoritative refresh is queued");
    }

    #[test]
    fn background_context_unavailable_reconciles_from_bootstrap_status() {
        let (mut app, _) = ready_app();
        let subscription_id = app
            .bootstrap_subscription
            .as_ref()
            .expect("ready app subscribes to availability transitions")
            .id()
            .clone();
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(subscription_id),
            sequence: None,
            payload: serde_json::to_value(
                ErrorFrame::new(
                    ErrorCode::Conflict,
                    "context unavailable",
                    Retryability::AfterRefresh,
                    ErrorScope::Subscription,
                    "bootstrap-status",
                )
                .with_details(serde_json::json!({
                    "kind": "contextUnavailable",
                    "context": "dev-local",
                    "reason": "background plugin denied",
                })),
            )
            .unwrap(),
        };

        app.handle_event(server_message(&rejection), 150, 0)
            .expect("background auth failure stays nonterminal");

        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(app.connection.is_some());
        let AppView::Ready { contexts, .. } = app.view() else {
            panic!("the ready workspace remains visible")
        };
        let failed = contexts
            .iter()
            .find(|context| context.name == "dev-local")
            .expect("active context remains visible");
        assert_eq!(
            failed.availability,
            k10s_protocol::ContextAvailability::Unavailable
        );
        assert_eq!(
            failed.unavailable_reason.as_deref(),
            Some("background plugin denied")
        );
        assert!(app.bootstrap.is_some(), "authoritative refresh is queued");
    }

    #[test]
    fn bootstrap_status_failure_during_reconnect_waits_for_rebuilt_bootstrap() {
        let initial_bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
        let (mut app, _) = test_app(vec![
            ConnectionScript {
                events: VecDeque::from([
                    WsEvent::Opened,
                    server_message(&welcome()),
                    server_message(&initial_bootstrap),
                ]),
                overflowed: false,
            },
            ConnectionScript::default(),
        ]);
        app.poll_at(100, 0);
        let subscription_id = app
            .bootstrap_subscription
            .as_ref()
            .expect("ready app retains its status subscription")
            .id()
            .clone();

        app.transient_loss(150, 0);
        app.shell.set_external_shell_availability(
            crate::ui::ExternalShellAvailability::Available { generation: 42 },
        );
        app.external_shell_requests
            .push(crate::ui::ExternalShellTarget {
                generation: 42,
                namespace: "default".into(),
                pod: "api".into(),
                uid: "uid-api".into(),
                container: "api".into(),
                program: "/bin/sh".into(),
            });
        app.retry_now(200, 0).unwrap();
        assert_eq!(
            app.shell.external_shell_availability(),
            crate::ui::ExternalShellAvailability::Unavailable
        );
        assert!(app.external_shell_requests.is_empty());
        app.handle_event(WsEvent::Opened, 200, 0).unwrap();
        let mut resumed = welcome();
        resumed.payload = serde_json::to_value(Welcome {
            protocol: ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec![],
            session_id: SessionId::new("resumed-session"),
            server_instance_id: "resumed-server".to_owned(),
            resume_status: ResumeStatus::ResyncRequired,
        })
        .unwrap();
        app.handle_event(server_message(&resumed), 210, 0).unwrap();
        assert_eq!(app.view(), &AppView::Connecting);
        assert!(app.bootstrap.is_some(), "rebuilt Bootstrap is pending");

        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: Some(subscription_id),
            sequence: None,
            payload: serde_json::to_value(
                ErrorFrame::new(
                    ErrorCode::Conflict,
                    "context unavailable",
                    Retryability::AfterRefresh,
                    ErrorScope::Subscription,
                    "bootstrap-status",
                )
                .with_details(serde_json::json!({
                    "kind": "contextUnavailable",
                    "context": "dev-local",
                    "reason": "plugin failed during reconnect",
                })),
            )
            .unwrap(),
        };

        app.handle_event(server_message(&rejection), 220, 0)
            .expect("a retained status transition stays nonterminal during recovery");
        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(app.connection.is_some());
        assert_eq!(app.view(), &AppView::Connecting);
        assert!(
            app.bootstrap.is_some(),
            "authoritative rebuild remains pending"
        );
    }

    #[test]
    fn mismatched_switch_unavailable_details_do_not_disable_another_context() {
        let (mut app, _) = ready_app();
        app.stage_context_switch("prod-readonly", true).unwrap();
        app.flush_outbound().unwrap();
        let request_id = app
            .pending_switch
            .as_ref()
            .map(|pending| pending.request.id().clone())
            .expect("switch request is pending");
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(request_id),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(
                ErrorFrame::new(
                    ErrorCode::Conflict,
                    "context unavailable",
                    Retryability::AfterRefresh,
                    ErrorScope::Request,
                    "context-switch",
                )
                .with_details(serde_json::json!({
                    "kind": "contextUnavailable",
                    "context": "dev-local",
                    "reason": "stale response",
                })),
            )
            .unwrap(),
        };

        app.handle_event(server_message(&rejection), 150, 0)
            .expect("mismatched details remain request-scoped");

        let AppView::Ready { contexts, .. } = app.view() else {
            panic!("the ready workspace remains visible")
        };
        assert!(contexts.iter().all(|context| {
            context.availability != k10s_protocol::ContextAvailability::Unavailable
                && context.unavailable_reason.is_none()
        }));
        assert!(
            app.bootstrap.is_none(),
            "invalid details do not queue refresh"
        );
    }

    #[test]
    fn reconnect_bootstrap_is_current_marker_overrides_the_stale_local_selection() {
        fn bootstrap_marking_payload(current: &str) -> BootstrapResponse {
            let mut payload = BootstrapResponse::fixture();
            for context in &mut payload.contexts {
                context.is_current = context.name == current;
            }
            payload
        }

        let (mut app, state) = test_app(vec![
            ConnectionScript::default(),
            ConnectionScript {
                events: VecDeque::from([WsEvent::Opened, server_message(&welcome())]),
                overflowed: false,
            },
        ]);

        // First generation: bootstrap marks dev-local current.
        app.handle_event(WsEvent::Opened, 100, 0).unwrap();
        app.flush_outbound().unwrap();
        app.handle_event(server_message(&welcome()), 100, 0)
            .unwrap();
        app.handle_event(
            server_message(&ServerFrame::response(
                RequestId::from_u128(1),
                bootstrap_marking_payload("dev-local"),
            )),
            100,
            0,
        )
        .unwrap();
        app.drain_app_events();
        assert!(matches!(app.view(), AppView::Ready { .. }));
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("dev-local")
        );

        // Model a lost generation: the first bootstrap's authoritative
        // dev-local commit lives in the workspace, while a switch response
        // that died in flight left the local selection claiming staging —
        // three contexts, none agreeing.
        app.client.local_ui_mut().selected_context = Some("staging".to_owned());
        assert_eq!(app.shell.workspace().context(), "dev-local");
        let pod = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "stale-pod".into(),
            uid: "stale-pod-uid".into(),
        };
        let resource_window = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods))
            .into_iter()
            .find_map(|event| match event {
                WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .expect("resource window opens");
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(resource_window, pod));
        assert!(matches!(
            &app.shell.workspace().window(resource_window).unwrap().content,
            WindowContent::Resource(resource) if resource.selection.is_some() && resource.detail.is_some()
        ));

        // Second generation: reconnect, welcome, and the client's own fresh
        // bootstrap request, whose answer marks prod-readonly current (the
        // backend committed the switch whose response was lost).
        app.transient_loss(150, 40);
        assert_eq!(
            app.client.phase(),
            ClientPhase::Disconnected,
            "the transport died with the drift unreconciled"
        );
        assert!(app.pending_switch.is_none());
        app.poll_at(200, 0);
        app.poll_at(210, 0);
        let bootstrap_request_id = state
            .borrow()
            .sent
            .iter()
            .rev()
            .find(|frame| {
                frame.kind == ClientKind::Request
                    && request_kind(frame).as_deref() == Some("bootstrap")
            })
            .and_then(|frame| frame.request_id.clone())
            .expect("the reconnect issues a bootstrap request");
        app.handle_event(
            server_message(&ServerFrame::response(
                bootstrap_request_id,
                bootstrap_marking_payload("prod-readonly"),
            )),
            220,
            0,
        )
        .unwrap();

        assert!(matches!(app.view(), AppView::Ready { .. }));
        assert_eq!(
            app.client.local_ui().selected_context.as_deref(),
            Some("prod-readonly"),
            "the bootstrap is_current marker is authoritative over the stale local selection"
        );
        assert_eq!(
            app.shell.workspace().context(),
            "prod-readonly",
            "the authoritative context is committed into the workspace instead of waiting for another switch"
        );
        assert!(
            matches!(
                &app.shell.workspace().window(resource_window).unwrap().content,
                WindowContent::Resource(resource) if resource.selection.is_none() && resource.detail.is_none()
            ),
            "the authoritative commit clears stale selection and detail state"
        );
        assert!(app.pending_switch.is_none());
        let reconnect_events = app.drain_app_events();
        assert_eq!(
            reconnect_events
                .iter()
                .filter(|event| matches!(
                    event,
                    super::K10sAppEvent::ControlConnectionReestablished { .. }
                ))
                .count(),
            1,
            "one completed reconnect publishes exactly one typed event"
        );
        assert!(reconnect_events.iter().any(|event| matches!(
            event,
            super::K10sAppEvent::ControlConnectionReestablished { context }
                if context.as_deref() == Some("prod-readonly")
        )));
        assert!(app.drain_app_events().is_empty());
        let kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            kinds,
            [
                "bootstrap",
                "infrastructure.get",
                "bootstrap",
                "infrastructure.get"
            ],
            "streams and infrastructure resubscribe onto the marker context after reconnect"
        );
    }

    #[test]
    fn an_explicit_retry_reissues_a_failed_switch_that_passive_reconciliation_still_suppresses() {
        // A third destination proves the switch flow survives the rejection.
        let mut bootstrap_payload = BootstrapResponse::fixture();
        bootstrap_payload.contexts.push(Context {
            name: "staging".into(),
            cluster: "staging-cluster".into(),
            namespace: None,
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        });
        let bootstrap = ServerFrame::response(RequestId::from_u128(1), bootstrap_payload);
        let rejection = ServerFrame {
            kind: ServerKind::Error,
            request_id: Some(RequestId::from_u128(3)),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(ErrorFrame::new(
                ErrorCode::Internal,
                "destination refused the read path",
                Retryability::Never,
                ErrorScope::Request,
                "context-switch",
            ))
            .unwrap(),
        };
        let (mut app, state) = test_app(vec![ConnectionScript {
            events: VecDeque::from([
                WsEvent::Opened,
                server_message(&welcome()),
                server_message(&bootstrap),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);
        assert!(matches!(app.view(), AppView::Ready { .. }));
        app.stage_context_switch("prod-readonly", true).unwrap();
        app.flush_outbound().unwrap();
        app.handle_event(server_message(&rejection), 150, 0)
            .unwrap();
        assert_eq!(
            app.failed_switch.as_deref(),
            Some("prod-readonly"),
            "the failed destination is recorded"
        );
        let sent_before_failure = state.borrow().sent.len();

        // Passive mismatch reconciliation toward the failed destination must
        // stay silent: no new request leaves the client.
        app.stage_context_switch("prod-readonly", false).unwrap();
        app.flush_outbound().unwrap();
        assert_eq!(
            state.borrow().sent.len(),
            sent_before_failure,
            "a passive re-request of a failed destination is suppressed"
        );
        assert!(app.pending_switch.is_none());

        // An explicit re-pick of the SAME destination lifts the suppression
        // and issues a fresh request.
        app.stage_context_switch("prod-readonly", true).unwrap();
        app.flush_outbound().unwrap();
        let retried: Vec<_> = state.borrow().sent[sent_before_failure..]
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            retried,
            ["context.switch"],
            "an explicit retry issues a new request"
        );
        assert_eq!(
            app.pending_switch
                .as_ref()
                .map(|pending| pending.to.as_str()),
            Some("prod-readonly")
        );
        assert_eq!(app.failed_switch, None, "the failure record cleared");
    }

    /// One Deployment list window with two rows, rendered through the
    /// reference-design toolbar.
    fn toolbar_test_harness(app: K10sApp) -> Harness<'static, K10sApp> {
        toolbar_test_harness_with_size(app, egui::vec2(1_280.0, 800.0))
    }

    fn toolbar_test_harness_with_size(app: K10sApp, size: egui::Vec2) -> Harness<'static, K10sApp> {
        fn render(ui: &mut egui::Ui, app: &mut K10sApp) {
            app.render_ui(ui);
        }

        let mut harness = Harness::builder()
            .with_size(size)
            .build_ui_state(render, app);
        for _ in 0..4 {
            harness.step();
        }
        harness
    }

    fn deployment_list_app() -> (K10sApp, WindowId) {
        let (mut app, _) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        if let Some((_, subscription)) = &app.namespace_subscription {
            let ns_id = subscription.id().clone();
            complete_namespace_snapshot(&mut app, &ns_id, 1, vec![namespace_row("default", 1)]);
        }
        app.reconcile_selected_resource_streams();
        let subscription = {
            let key = app.window_subscriptions.get(&window).unwrap();
            app.resource_subscriptions
                .get(key)
                .unwrap()
                .live
                .id()
                .clone()
        };
        complete_resource_snapshot(
            &mut app,
            &subscription,
            1,
            vec![
                deployment_row(deployment_identity("alpha"), 1),
                deployment_row(deployment_identity("beta"), 1),
            ],
            true,
        );
        (app, window)
    }

    #[test]
    fn compact_deployment_toolbar_contains_primary_controls_at_640_points() {
        let (mut app, _) = deployment_list_app();
        let mut snapshot = app.workspace_snapshot();
        snapshot.free_window_resizing = true;
        let deployment = snapshot
            .windows
            .iter_mut()
            .find(|window| window.title == "Deployments")
            .expect("deployment window is persisted");
        deployment.geometry.size[0] = 640.0;
        app.restore_workspace_snapshot(snapshot);

        let mut harness = toolbar_test_harness_with_size(app, egui::vec2(640.0, 800.0));
        {
            let window = harness
                .get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");

            let search = window
                .get_by_role_and_label(egui::accesskit::Role::TextInput, "Search deployments");
            let namespace =
                window.get_by_role_and_label(egui::accesskit::Role::ComboBox, "Namespace: default");
            let status =
                window.get_by_role_and_label(egui::accesskit::Role::ComboBox, "Status: all");
            let live = window.get_by_label("Live; synced just now");
            assert!(window.query_by_label("Columns ▾").is_none());
            assert!(window.query_by_label("↻").is_none());
            let more =
                window.get_by_role_and_label(egui::accesskit::Role::Button, "More list controls");
            for (index, control) in [&search, &namespace, &status, &more, &live]
                .iter()
                .enumerate()
            {
                assert!(
                    control.rect().right() <= 640.0,
                    "primary control {index} must remain inside the visible canvas: {:?}",
                    control.rect(),
                );
                assert!(
                    (control.rect().center().y - search.rect().center().y).abs() < 1.0,
                    "primary control {index} must remain on one toolbar line"
                );
            }
            more.click();
        }
        harness.step();
        assert!(
            harness
                .query_by_role_and_label(egui::accesskit::Role::Button, "Columns ▾ ⏵")
                .is_none()
        );
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "Refresh list");
    }

    #[test]
    fn deployment_toolbar_matches_reference_layout_and_title() {
        let (app, _) = deployment_list_app();
        let harness = toolbar_test_harness(app);
        let window =
            harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");

        // The reference layout exposes primary controls directly when space permits:
        window.get_by_role_and_label(egui::accesskit::Role::TextInput, "Search deployments");
        window.get_by_role_and_label(egui::accesskit::Role::ComboBox, "Namespace: default");
        window.get_by_role_and_label(egui::accesskit::Role::ComboBox, "Status: all");
        if window
            .query_by_role_and_label(egui::accesskit::Role::Button, "More list controls")
            .is_some()
        {
            window.get_by_label("Live; synced just now");
        } else {
            window.get_by_role_and_label(egui::accesskit::Role::Button, "↻");
            window.get_by_label("Live; synced just now");
        }

        // The match line reports the result count and the age affordance.
        // With no filters active, the Reset control stays hidden.
        window.get_by_label("2 deployments");
        assert!(
            window.query_by_label("Reset").is_none(),
            "Reset only appears while filters are active"
        );
        // The standalone Live row from the old layout is gone: every "Live"
        // label belongs to the toolbar chip.
        assert!(
            window.query_all_by_label_contains("Live").all(|node| {
                let accesskit_node = node.accesskit_node();
                let text = if accesskit_node.role() == egui::accesskit::Role::Label {
                    accesskit_node.value()
                } else {
                    accesskit_node.label()
                };
                text.is_some_and(|text| text.starts_with("Live; synced "))
            }),
            "live status belongs only in the toolbar chip"
        );
    }

    #[test]
    fn deployment_window_title_tracks_namespace_scope() {
        let (app, window) = deployment_list_app();
        let harness = toolbar_test_harness(app);
        harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");

        let mut app = harness.into_state();
        app.web_set_namespace_scope(
            window,
            crate::workspace::NamespaceScope::Namespace("payments".into()),
        );

        let harness = toolbar_test_harness(app);
        harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · payments");
    }

    #[test]
    fn deployment_matchline_reflects_selection_sort_and_age_mode() {
        let (mut app, window) = deployment_list_app();
        let alpha = deployment_identity("alpha");
        app.web_select_resource(window, alpha.clone());
        app.shell.apply_workspace_command(WorkspaceCommand::SetSort(
            window,
            Some(crate::workspace::SortSpec {
                column: "namespace".into(),
                ascending: true,
            }),
        ));
        let mut snapshot = app.workspace_snapshot();
        snapshot.free_window_resizing = true;
        snapshot
            .windows
            .iter_mut()
            .find(|persisted| persisted.title == "Deployments")
            .expect("deployment window is persisted")
            .geometry
            .size[0] = 460.0;
        app.restore_workspace_snapshot(snapshot);
        app.web_select_resource(window, alpha);
        app.shell.apply_workspace_command(WorkspaceCommand::SetSort(
            window,
            Some(crate::workspace::SortSpec {
                column: "namespace".into(),
                ascending: true,
            }),
        ));

        let mut harness = toolbar_test_harness(app);
        let list_window =
            harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");
        list_window.get_by_label("2 deployments · 1 selected");
        list_window
            .get_by_role_and_label(egui::accesskit::Role::Button, "More list controls")
            .click();
        harness.step();
        harness.get_by_label("sorted by Namespace ▲ · ");
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "switch to absolute");

        // Absolute mode renders raw timestamps in the Age column and flips
        // the match-line affordance.
        let mut app = harness.into_state();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetAgeMode(
                window,
                crate::workspace::AgeMode::Absolute,
            ));
        let mut harness = toolbar_test_harness(app);
        let list_window =
            harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");
        list_window
            .get_by_role_and_label(egui::accesskit::Role::Button, "More list controls")
            .click();
        harness.step();
        harness.get_by_label("Age shown as absolute (");
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "switch to relative");
    }

    #[test]
    fn deployment_status_filter_narrows_rows_and_reset_restores_them() {
        let (mut app, _) = ready_app();
        let window = app
            .web_activate_workload(WorkloadKind::Deployments)
            .unwrap();
        if let Some((_, subscription)) = &app.namespace_subscription {
            let ns_id = subscription.id().clone();
            complete_namespace_snapshot(&mut app, &ns_id, 1, vec![namespace_row("default", 1)]);
        }
        app.reconcile_selected_resource_streams();
        let subscription = {
            let key = app.window_subscriptions.get(&window).unwrap();
            app.resource_subscriptions
                .get(key)
                .unwrap()
                .live
                .id()
                .clone()
        };
        // Distinct statuses so the filter has something to exclude.
        let alpha = deployment_row(deployment_identity("alpha"), 1);
        let mut beta = deployment_row(deployment_identity("beta"), 1);
        beta.summary = "Degraded".into();
        complete_resource_snapshot(&mut app, &subscription, 1, vec![alpha, beta], true);

        app.shell
            .apply_workspace_command(WorkspaceCommand::SetStatusFilter(
                window,
                Some("Ready".into()),
            ));
        let harness = toolbar_test_harness(app);
        let list_window =
            harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");
        list_window.get_by_label("Select resource alpha");
        assert!(
            list_window.query_by_label("Select resource beta").is_none(),
            "the status filter must exclude non-matching rows"
        );
        // An active filter keeps Reset reachable directly or through overflow.
        if list_window
            .query_by_role_and_label(egui::accesskit::Role::Button, "More list controls")
            .is_none()
        {
            list_window.get_by_role_and_label(egui::accesskit::Role::Button, "Reset");
        }
        let selector = harness
            .query_all_by_role(egui::accesskit::Role::ComboBox)
            .find(|node| {
                node.value()
                    .is_some_and(|value| value == "Status" || value.starts_with("Status: "))
            })
            .expect("the toolbar Status combobox");
        assert!(selector.value().as_deref().unwrap().contains("Status"));

        // Reset clears the filter and restores every row.
        let mut app = harness.into_state();
        app.shell
            .apply_workspace_command(WorkspaceCommand::SetStatusFilter(window, None));
        let harness = toolbar_test_harness(app);
        let list_window =
            harness.get_by_role_and_label(egui::accesskit::Role::Window, "Deployments · default");
        list_window.get_by_label("Select resource alpha");
        list_window.get_by_label("Select resource beta");
        list_window.get_by_label("2 deployments");
    }

    #[test]
    fn deployment_selected_row_reports_selection_without_disclosure_marker() {
        let (mut app, window) = deployment_list_app();
        app.web_select_resource(window, deployment_identity("alpha"));
        let harness = toolbar_test_harness(app);
        let list_window = harness.get_by_role_and_label(
            egui::accesskit::Role::Window,
            "Deployments \u{00b7} default",
        );

        // The selected row keeps its clean action affordance and reports the
        // selected state through the accessibility tree (the full-row fill is
        // the visual cue for it).
        let selected_button = list_window.get_by_role_and_label(
            egui::accesskit::Role::Button,
            "Clear selection for resource alpha",
        );
        assert_eq!(
            selected_button.accesskit_node().toggled(),
            Some(egui::accesskit::Toggled::True),
            "the selected row button is flagged toggled-on for assistive tech"
        );

        // No row in the list carries a disclosure triangle: the triangle prefix
        // was removed from the name cell, and detail opens in the detail pane.
        let triangles: Vec<String> = list_window
            .children_recursive()
            .filter_map(|node| node.accesskit_node().value())
            .filter(|value| value.contains('\u{25b6}'))
            .collect();
        assert!(
            triangles.is_empty(),
            "no row carries a disclosure triangle, got {triangles:?}"
        );
    }

    #[test]
    fn deployment_status_renders_tone_glyph_with_clean_access_label() {
        let (app, _) = deployment_list_app();
        let harness = toolbar_test_harness(app);
        let list_window = harness.get_by_role_and_label(
            egui::accesskit::Role::Window,
            "Deployments \u{00b7} default",
        );

        // The status cell paints a tone glyph (\u{25cf} for a Ready row) but the
        // accessible text stays the clean status value.
        let status_labels: Vec<Option<String>> = list_window
            .children_recursive()
            .filter(|node| node.accesskit_node().value().as_deref() == Some("Ready"))
            .map(|node| {
                node.children()
                    .find_map(|child| child.accesskit_node().value())
            })
            .collect();
        assert!(
            status_labels
                .iter()
                .any(|run| run.as_deref() == Some("\u{25cf} Ready")),
            "a Ready status renders its healthy tone glyph, got {status_labels:?}"
        );
    }
}

#[cfg(test)]
mod stream_lifecycle_tests {
    use std::sync::mpsc;

    use ewebsock::{WsEvent, WsMessage};
    use k10s_protocol::{
        BackendRevision, ContainerStateProjection, GroupVersionKind, PodContainerProjection,
        PodProjection, ResourceCapabilities, ResourceDetailResponse, ResourceIdentity,
        ResourceProjection, StreamTarget, StreamType,
    };

    use super::tests::{ready_app, test_app};
    use super::{K10sApp, LogSource};
    use crate::client::{StreamIo, StreamRoute, StreamSession};
    use crate::ui::tools::{LogsAction, LogsPhase};
    use crate::workspace::{WorkloadKind, WorkspaceCommand};

    #[derive(Debug)]
    struct ScriptStream {
        events: mpsc::Receiver<WsEvent>,
    }

    impl StreamIo for ScriptStream {
        fn try_recv(&mut self) -> Option<WsEvent> {
            self.events.try_recv().ok()
        }
        fn send_text(&mut self, _text: String) {}
    }

    fn pod(name: &str) -> ResourceIdentity {
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        }
    }

    pub(super) fn target_for(pod_name: &str) -> StreamTarget {
        target_for_container(pod_name, "app")
    }

    fn target_for_container(pod_name: &str, container: &str) -> StreamTarget {
        StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: pod_name.into(),
            uid: format!("uid-{pod_name}"),
            container: container.into(),
        }
    }

    fn log_source(target: StreamTarget, since_seconds: Option<i64>, previous: bool) -> LogSource {
        LogSource {
            target,
            since_seconds,
            previous,
        }
    }

    fn queue_logs(app: &mut K10sApp, window: crate::workspace::WindowId, target: StreamTarget) {
        let logs = &mut app.shell.stream_stores_mut().logs;
        logs.ensure(window, target.clone());
        logs.queue(
            window,
            LogsAction::OpenLogs {
                window,
                target,
                since_seconds: Some(300),
                previous: false,
            },
        );
    }

    pub(super) fn detail_with_container(
        identity: &ResourceIdentity,
        container: &str,
    ) -> ResourceDetailResponse {
        ResourceDetailResponse {
            identity: identity.clone(),
            revision: BackendRevision::new(1),
            created_at: String::new(),
            owner_references: Vec::new(),
            sections: Vec::new(),
            events: Vec::new(),
            events_condition: k10s_protocol::EventsCondition::Available,
            related: Vec::new(),
            capabilities: ResourceCapabilities {
                can_exec: true,
                ..ResourceCapabilities::default()
            },
            manifest: "SENTINEL MANIFEST: runtime tests must not parse this".into(),
            projection: Some(ResourceProjection::Pod(PodProjection {
                phase: Some("Running".into()),
                ready_containers: Some(1),
                total_containers: Some(1),
                restart_count: Some(0),
                containers: vec![PodContainerProjection {
                    name: container.into(),
                    image: None,
                    state: Some(ContainerStateProjection::Running),
                    ready: Some(true),
                    restart_count: Some(0),
                    last_termination: None,
                }],
                conditions: Vec::new(),
                node_name: None,
                pod_ip: None,
                host_ip: None,
                qos_class: None,
                priority: None,
                service_account: None,
                restart_policy: None,
                ports: Vec::new(),
                labels: Default::default(),
                annotations: Default::default(),
                created_at: None,
            })),
        }
    }

    pub(super) fn open_pod_detail(
        app: &mut K10sApp,
        pod: &ResourceIdentity,
    ) -> crate::workspace::WindowId {
        app.details
            .entry(pod.clone())
            .or_insert_with(|| detail_with_container(pod, "app"));
        let events = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods));
        let window = events
            .iter()
            .find_map(|event| match event {
                crate::workspace::WorkspaceEvent::Opened(id) => Some(*id),
                _ => None,
            })
            .expect("workload window opens");
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, pod.clone()));
        window
    }

    fn queue_logs_with_source(
        app: &mut K10sApp,
        window: crate::workspace::WindowId,
        target: StreamTarget,
        since_seconds: Option<i64>,
        previous: bool,
    ) {
        app.shell
            .stream_stores_mut()
            .logs
            .ensure(window, target.clone());
        app.shell.stream_stores_mut().logs.queue(
            window,
            LogsAction::OpenLogs {
                window,
                target,
                since_seconds,
                previous,
            },
        );
        app.process_stream_requests().unwrap();
    }

    #[test]
    fn newer_log_source_supersedes_pending_ticket_before_reversed_completion() {
        let (mut app, _state) = super::tests::ready_app();
        let pod = pod("web");
        let window = open_pod_detail(&mut app, &pod);
        let target = target_for(&pod.name);

        queue_logs_with_source(&mut app, window, target.clone(), Some(300), false);
        let old_id = app.pending_stream_tickets.keys().next().unwrap().clone();
        queue_logs_with_source(&mut app, window, target.clone(), Some(900), true);
        let new_id = app
            .pending_stream_tickets
            .keys()
            .find(|id| **id != old_id)
            .unwrap()
            .clone();

        let grant = |id, ticket: &str| {
            k10s_protocol::ServerFrame::response(
                id,
                k10s_protocol::StreamTicketResponse {
                    ticket_id: ticket.into(),
                    target: target.clone(),
                    stream_type: StreamType::Logs,
                    tty: false,
                },
            )
        };
        app.handle_event(super::tests::server_message(&grant(new_id, "new")), 1, 0)
            .unwrap();
        app.finish_stream_tickets();
        assert_eq!(
            app.log_session_sources.get(&window).unwrap().since_seconds,
            Some(900)
        );
        assert!(app.log_session_sources.get(&window).unwrap().previous);

        let _ = app.handle_event(super::tests::server_message(&grant(old_id, "old")), 2, 0);
        app.finish_stream_tickets();
        assert_eq!(
            app.log_session_sources.get(&window).unwrap().since_seconds,
            Some(900)
        );
        assert!(app.log_session_sources.get(&window).unwrap().previous);
    }

    #[test]
    fn newer_identical_log_attempt_supersedes_reversed_old_completion() {
        let (mut app, _state) = super::tests::ready_app();
        let pod = pod("same-source");
        let window = open_pod_detail(&mut app, &pod);
        let target = target_for(&pod.name);

        queue_logs_with_source(&mut app, window, target.clone(), Some(300), false);
        let old_id = app.pending_stream_tickets.keys().next().unwrap().clone();
        let old_generation = app.pending_stream_tickets[&old_id].log_generation.unwrap();
        queue_logs_with_source(&mut app, window, target.clone(), Some(300), false);
        let new_id = app
            .pending_stream_tickets
            .keys()
            .find(|id| **id != old_id)
            .unwrap()
            .clone();
        let new_generation = app.pending_stream_tickets[&new_id].log_generation.unwrap();
        assert!(new_generation > old_generation);
        assert_eq!(app.log_generations[&window], new_generation);

        let grant = |id, ticket: &str| {
            k10s_protocol::ServerFrame::response(
                id,
                k10s_protocol::StreamTicketResponse {
                    ticket_id: ticket.into(),
                    target: target.clone(),
                    stream_type: StreamType::Logs,
                    tty: false,
                },
            )
        };
        app.handle_event(super::tests::server_message(&grant(new_id, "new")), 1, 0)
            .unwrap();
        app.finish_stream_tickets();
        assert_eq!(app.log_session_generations[&window], new_generation);

        let _ = app.handle_event(super::tests::server_message(&grant(old_id, "old")), 2, 0);
        app.finish_stream_tickets();
        assert_eq!(app.log_session_generations[&window], new_generation);
    }

    #[test]
    fn failed_log_cancel_preflight_preserves_live_attempt_and_signal_projection() {
        let (mut app, _state) = super::tests::ready_app();
        let pod = pod("cancel-full");
        let window = open_pod_detail(&mut app, &pod);
        let target = target_for(&pod.name);
        queue_logs_with_source(&mut app, window, target.clone(), Some(300), false);
        let generation = app.log_generations[&window];
        let source = app.log_sources[&window].clone();

        let (tx, rx) = mpsc::channel();
        let view = app
            .shell
            .stream_stores_mut()
            .logs
            .ensure(window, target.clone());
        view.attach();
        let mut session = StreamSession::new(StreamRoute::Logs, target.clone());
        session.inject_for_test(ScriptStream { events: rx });
        app.stream_sessions
            .insert((window, StreamRoute::Logs), session);
        app.log_session_sources.insert(window, source);
        app.log_session_generations.insert(window, generation);

        super::tests::saturate_cancel_outbound(&mut app);
        let view = app.shell.stream_stores_mut().logs.get_mut(window).unwrap();
        view.connection_lost();
        assert!(view.retry());
        app.shell.stream_stores_mut().logs.queue(
            window,
            LogsAction::OpenLogs {
                window,
                target,
                since_seconds: Some(300),
                previous: false,
            },
        );
        assert!(app.process_stream_requests().is_err());
        assert_eq!(app.log_generations[&window], generation);
        assert!(
            app.stream_sessions
                .contains_key(&(window, StreamRoute::Logs))
        );

        tx.send(WsEvent::Message(WsMessage::Binary(
            k10s_protocol::encode_stream_payload(
                k10s_protocol::payload_kind::STDOUT,
                b"still-current",
            ),
        )))
        .unwrap();
        app.poll_stream_sessions();
        assert_eq!(
            app.shell
                .stream_stores()
                .logs
                .get(window)
                .unwrap()
                .export_text(),
            "still-current"
        );
    }

    #[test]
    fn previous_or_since_change_retires_same_target_log_session() {
        let (mut app, _state) = super::tests::ready_app();
        let pod = pod("web");
        let window = open_pod_detail(&mut app, &pod);
        let target = target_for(&pod.name);
        app.stream_sessions.insert(
            (window, StreamRoute::Logs),
            StreamSession::new(StreamRoute::Logs, target.clone()),
        );
        app.log_sources.insert(
            window,
            super::LogSource {
                target: target.clone(),
                since_seconds: Some(300),
                previous: false,
            },
        );
        app.log_session_sources.insert(
            window,
            super::LogSource {
                target: target.clone(),
                since_seconds: Some(300),
                previous: false,
            },
        );

        queue_logs_with_source(&mut app, window, target, Some(900), true);

        assert!(
            !app.stream_sessions
                .contains_key(&(window, StreamRoute::Logs))
        );
        let source = app.log_sources.get(&window).unwrap();
        assert_eq!(source.since_seconds, Some(900));
        assert!(source.previous);
    }

    #[test]
    fn logs_for_typed_container_attach_and_project_output() {
        let (mut app, _state) = test_app(Vec::new());
        let pod = pod("web");
        let window = open_pod_detail(&mut app, &pod);
        app.details
            .insert(pod.clone(), detail_with_container(&pod, "nginx"));
        let target = target_for_container(&pod.name, "nginx");

        assert_eq!(app.workspace_stream_target(window).as_ref(), Some(&target));

        let (tx, rx) = mpsc::channel();
        app.shell
            .stream_stores_mut()
            .logs
            .ensure(window, target.clone())
            .connect();
        let mut session = StreamSession::new(StreamRoute::Logs, target);
        session.inject_for_test(ScriptStream { events: rx });
        app.stream_sessions
            .insert((window, StreamRoute::Logs), session);

        tx.send(WsEvent::Message(WsMessage::Text(
            serde_json::to_string(&k10s_protocol::StreamServerMessage::Ready {
                stream_type: StreamType::Logs,
                tty: false,
                container: "nginx".to_owned(),
            })
            .unwrap(),
        )))
        .unwrap();
        tx.send(WsEvent::Message(WsMessage::Binary(
            k10s_protocol::encode_stream_payload(k10s_protocol::payload_kind::STDOUT, b"served"),
        )))
        .unwrap();
        app.poll_stream_sessions();

        let view = app
            .shell
            .stream_stores()
            .logs
            .get(window)
            .expect("log view exists");
        assert_eq!(view.phase(), LogsPhase::Streaming);
        assert_eq!(
            view.visible_lines().map(String::as_str).collect::<Vec<_>>(),
            vec!["served"]
        );
    }

    #[test]
    fn selected_log_container_is_authoritative_across_render_reconciliation() {
        let (mut app, _state) = test_app(Vec::new());
        let pod = pod("multi-container");
        let window = open_pod_detail(&mut app, &pod);
        let mut detail = detail_with_container(&pod, "app");
        let Some(ResourceProjection::Pod(projection)) = detail.projection.as_mut() else {
            panic!("fixture has a typed Pod projection");
        };
        projection.total_containers = Some(2);
        projection.containers.push(PodContainerProjection {
            name: "metrics".into(),
            image: None,
            state: Some(ContainerStateProjection::Running),
            ready: Some(true),
            restart_count: Some(0),
            last_termination: None,
        });
        app.details.insert(pod.clone(), detail);

        let default_target = target_for_container(&pod.name, "app");
        let selected_target = target_for_container(&pod.name, "metrics");
        app.shell
            .stream_stores_mut()
            .logs
            .ensure(window, default_target.clone())
            .select_container("metrics");
        // The next render still supplies the typed default. It must not
        // replace the user's selected container.
        app.shell
            .stream_stores_mut()
            .logs
            .ensure(window, default_target);

        assert_eq!(
            app.current_stream_target(window, StreamRoute::Logs),
            Some(selected_target.clone())
        );

        let session = StreamSession::new(StreamRoute::Logs, selected_target);
        app.stream_sessions
            .insert((window, StreamRoute::Logs), session);
        app.reconcile_sessions();
        assert!(
            app.stream_sessions
                .contains_key(&(window, StreamRoute::Logs)),
            "reconciliation retains the selected-container stream"
        );
    }

    #[test]
    fn changing_live_log_source_retires_only_that_window_and_requests_full_new_mode() {
        let (mut app, _state) = ready_app();
        let first = pod("first");
        let second = pod("second");
        let first_window = open_pod_detail(&mut app, &first);
        let second_window = open_pod_detail(&mut app, &second);
        let first_target = target_for(&first.name);
        let second_target = target_for(&second.name);

        for (window, target) in [
            (first_window, first_target.clone()),
            (second_window, second_target.clone()),
        ] {
            app.shell
                .stream_stores_mut()
                .logs
                .ensure(window, target.clone())
                .connect();
            app.stream_sessions.insert(
                (window, StreamRoute::Logs),
                StreamSession::new(StreamRoute::Logs, target.clone()),
            );
            app.log_session_sources
                .insert(window, log_source(target, Some(300), false));
        }

        let first_logs = app
            .shell
            .stream_stores_mut()
            .logs
            .get_mut(first_window)
            .expect("first log view exists");
        first_logs.select_container("metrics");
        first_logs.set_previous(true);
        first_logs.set_since_seconds(Some(900));
        app.process_stream_requests().unwrap();

        assert!(
            !app.stream_sessions
                .contains_key(&(first_window, StreamRoute::Logs)),
            "the stale live source is retired"
        );
        assert!(
            app.stream_sessions
                .contains_key(&(second_window, StreamRoute::Logs)),
            "another window's live logs are untouched"
        );
        let replacement = app
            .pending_stream_tickets
            .values()
            .find(|pending| pending.window == first_window)
            .expect("replacement ticket is pending");
        assert_eq!(
            replacement.log_source.as_ref(),
            Some(&log_source(
                target_for_container(&first.name, "metrics"),
                Some(900),
                true,
            ))
        );
    }

    #[test]
    fn changing_pending_log_source_cancels_old_ticket_and_late_reply_cannot_attach() {
        let (mut app, _state) = ready_app();
        let first = pod("first-pending");
        let second = pod("second-pending");
        let first_window = open_pod_detail(&mut app, &first);
        let second_window = open_pod_detail(&mut app, &second);
        let first_target = target_for(&first.name);
        let second_target = target_for(&second.name);
        queue_logs(&mut app, first_window, first_target.clone());
        queue_logs(&mut app, second_window, second_target);
        app.process_stream_requests().unwrap();

        let old_first = app
            .pending_stream_tickets
            .iter()
            .find(|(_, pending)| pending.window == first_window)
            .map(|(id, _)| id.clone())
            .expect("first ticket is pending");
        let other = app
            .pending_stream_tickets
            .iter()
            .find(|(_, pending)| pending.window == second_window)
            .map(|(id, _)| id.clone())
            .expect("other ticket is pending");

        let first_logs = app
            .shell
            .stream_stores_mut()
            .logs
            .get_mut(first_window)
            .expect("first log view exists");
        first_logs.select_container("metrics");
        first_logs.set_previous(true);
        first_logs.set_since_seconds(None);
        app.process_stream_requests().unwrap();

        assert!(!app.pending_stream_tickets.contains_key(&old_first));
        assert!(app.pending_stream_tickets.contains_key(&other));
        let replacement = app
            .pending_stream_tickets
            .iter()
            .find(|(_, pending)| pending.window == first_window)
            .expect("replacement ticket is pending");
        assert_ne!(replacement.0, &old_first);
        assert_eq!(
            replacement.1.log_source.as_ref(),
            Some(&log_source(
                target_for_container(&first.name, "metrics"),
                None,
                true,
            ))
        );

        let _ = app.client.apply(k10s_protocol::ServerFrame {
            kind: k10s_protocol::ServerKind::Response,
            request_id: Some(old_first),
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(k10s_protocol::StreamTicketResponse {
                ticket_id: "late-ticket".into(),
                target: first_target,
                stream_type: StreamType::Logs,
                tty: false,
            })
            .unwrap(),
        });
        app.finish_stream_tickets();
        assert!(
            !app.stream_sessions
                .contains_key(&(first_window, StreamRoute::Logs)),
            "the retired ticket cannot attach a late socket"
        );
    }
}

#[cfg(test)]
mod stream_overflow_tests {
    use ewebsock::WsEvent;

    use super::stream_lifecycle_tests::target_for;
    use super::tests::test_app;
    use crate::client::{StreamIo, StreamRoute, StreamSession, StreamSignal};
    use crate::workspace::{WorkloadKind, WorkspaceCommand};

    /// A transport whose bounded inbox overflowed: the physical socket is
    /// already closed and every queued event was discarded.
    #[derive(Debug)]
    struct OverflowedSocket;

    impl StreamIo for OverflowedSocket {
        fn try_recv(&mut self) -> Option<WsEvent> {
            None
        }
        fn send_text(&mut self, _text: String) {}
        fn overflowed(&self) -> bool {
            true
        }
    }

    #[test]
    fn inbox_overflow_projects_one_rejection_and_ends_the_session() {
        let (mut app, _state) = test_app(Vec::new());
        let events = app
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods));
        let window = events
            .iter()
            .find_map(|event| match event {
                crate::workspace::WorkspaceEvent::Opened(id) => Some(*id),
                _ => None,
            })
            .expect("workload window opens");

        // A live logs session whose inbox overflowed.
        let mut session = StreamSession::new(StreamRoute::Logs, target_for("pod-a"));
        session.inject_for_test(OverflowedSocket);
        app.stream_sessions
            .insert((window, StreamRoute::Logs), session);

        app.poll_stream_sessions();

        // The overflow is projected exactly once as terminal rejection: the
        // session is gone (non-live), so the tool cannot stay Streaming
        // behind a dead socket.
        assert!(
            !app.stream_sessions
                .contains_key(&(window, StreamRoute::Logs)),
            "an overflowed stream must be torn down"
        );
    }

    #[test]
    fn overflow_signal_is_emitted_exactly_once_and_session_goes_non_live() {
        let mut session = StreamSession::new(StreamRoute::Logs, target_for("pod-a"));
        session.inject_for_test(OverflowedSocket);
        assert!(session.is_live());

        let signals = session.poll();
        assert_eq!(
            signals,
            vec![StreamSignal::Rejected("stream inbox overflow".to_owned())]
        );

        // Terminal: later polls stay silent and the session is non-live.
        assert!(!session.is_live());
        assert!(session.poll().is_empty());
        assert!(session.poll().is_empty());
    }
}
