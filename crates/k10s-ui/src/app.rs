//! Minimal application state driven exclusively through the shared protocol client.

use web_time::Instant;

use std::collections::BTreeMap;

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_protocol::{
    ClientFrame, Context, ContextAvailability, InfrastructureRequest, RequestId,
    ResourceDetailResponse, ResourceIdentity, ResourceTypeEntry, ResourceTypesRequest, ServerFrame,
    StreamTarget,
};

use crate::client::{
    BoundedInbox, ClientConfig, ClientError, ClientPhase, ClientState, Command, ConnectTarget,
    LiveSubscription, PendingRequest, Query, QueryResult, StreamRoute, StreamSession, StreamSignal,
    TransportError, WebSocketTransport,
};
use crate::ui::ResourceFeed;
use crate::ui::RowIdentity;
use crate::ui::dialogs::DialogAction;
use crate::ui::tools::ShellPhase;
use crate::ui::{ConnectionState as ShellConnectionState, UiShell};
use crate::workspace::{
    WindowId, WorkloadKind, WorkspaceCommand, WorkspaceEvent, WorkspaceSnapshot, WorkspaceState,
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
    infrastructure_subscription: Option<LiveSubscription>,
    infrastructure_context: Option<String>,
    stream_sessions: BTreeMap<(WindowId, StreamRoute), StreamSession>,
    pending_stream_tickets: BTreeMap<RequestId, PendingStreamTicket>,
    /// Canonical resource watches retained by visible workspace demand;
    /// rebuilt automatically on reconnect by shared client recovery.
    resource_subscriptions: BTreeMap<SubscriptionKey, RetainedSubscription>,
    /// Deterministic projection from every open list window to its shared
    /// canonical subscription.
    window_subscriptions: BTreeMap<WindowId, SubscriptionKey>,
    /// Authoritative session reconstruction requested after bootstrap or
    /// reconnect; events are subscribed before this list is issued.
    port_forward_list: Option<PendingRequest>,
    pending_port_forwards: Vec<PendingRequest>,
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
    /// In-flight context switch awaiting the backend's verdict; local state
    /// moves only after it succeeds.
    pending_switch: Option<PendingSwitch>,
    /// The destination of the last failed switch, so a passive mismatch
    /// cannot retry-spam a broken context every frame.
    failed_switch: Option<String>,
    /// Backend-resolved details for every identity a window pinned, keyed by
    /// stable identity; rebuilt from `operation.status`-style queries.
    details: BTreeMap<ResourceIdentity, ResourceDetailResponse>,
    /// In-flight detail requests per identity.
    detail_requests: BTreeMap<ResourceIdentity, PendingRequest>,
    recovering: bool,
    view: AppView,
    shell: UiShell<ResourceIdentity>,
    clock_started: Instant,
    jitter_counter: u64,
}

/// A window's in-flight dedicated-stream ticket request.
#[derive(Debug)]
struct PendingStreamTicket {
    request: PendingRequest,
    route: StreamRoute,
    window: WindowId,
}

/// A window's in-flight workload mutation (scale or delete).
#[derive(Debug)]
struct PendingMutation {
    request: PendingRequest,
    window: WindowId,
}

/// A context switch sent to the backend whose response has not arrived.
#[derive(Debug)]
struct PendingSwitch {
    request: PendingRequest,
    to: String,
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
    /// Effective selector sent on the wire. Kept beside namespace intent so
    /// a Bootstrap default-namespace change replaces ContextDefault watches.
    protocol_namespace: Option<String>,
}

#[derive(Debug)]
struct RetainedSubscription {
    live: LiveSubscription,
    windows: std::collections::BTreeSet<WindowId>,
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
            infrastructure_subscription: None,
            infrastructure_context: None,
            stream_sessions: BTreeMap::new(),
            pending_stream_tickets: BTreeMap::new(),
            resource_subscriptions: BTreeMap::new(),
            window_subscriptions: BTreeMap::new(),
            port_forward_list: None,
            pending_port_forwards: Vec::new(),
            port_forward_error: None,
            port_forward_switch_prompt: None,
            port_forward_switch_after_stop: None,
            resource_types: Vec::new(),
            types_context: None,
            types_request: None,
            pending_mutations: BTreeMap::new(),
            pending_switch: None,
            failed_switch: None,
            details: BTreeMap::new(),
            detail_requests: BTreeMap::new(),
            recovering: false,
            view: AppView::Connecting,
            shell: UiShell::new(),
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

    /// Whether the authenticated server negotiated desktop Service port forwarding.
    #[must_use]
    pub fn port_forward_available(&self) -> bool {
        self.client.port_forward_available()
    }

    /// Current authoritative session snapshots used by native hosts/tests.
    #[must_use]
    pub fn port_forward_sessions(&self) -> Vec<&k10s_protocol::PortForwardSession> {
        self.client.port_forward_sessions()
    }

    /// Render the approved default-egui shell for the current connection view.
    pub fn render_ui(&mut self, ui: &mut egui::Ui) {
        self.finish_port_forward_list();
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
        let feed = self.build_resource_feed();
        let refresh = self.shell.show_with_contexts_and_resources(
            ui,
            connection,
            contexts,
            &mut self.client.local_ui_mut().selected_context,
            response.as_ref(),
            &feed,
        );
        if let Some(context) = selected_before.as_deref()
            && let Err(error) = self.reconcile_resource_streams(context)
        {
            self.terminal_failure(error.to_string());
            return;
        }
        for action in self.shell.drain_port_forward_actions() {
            self.port_forward_error = None;
            let query = match action {
                crate::ui::PortForwardAction::Start {
                    service,
                    port,
                    local_port,
                } => Query::PortForwardStart(k10s_protocol::PortForwardStartRequest {
                    service,
                    port,
                    local_port,
                }),
                crate::ui::PortForwardAction::Stop(id) => {
                    match k10s_protocol::PortForwardSessionId::try_new(id) {
                        Ok(id) => Query::PortForwardStop(id),
                        Err(_) => continue,
                    }
                }
            };
            if let Ok(request) = self.client.begin(query) {
                self.pending_port_forwards.push(request);
            }
        }
        self.pending_port_forwards.retain(|request| {
            if self.client.is_pending(request) {
                true
            } else {
                let _ = self.client.take(request.clone());
                false
            }
        });
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
                    self.pending_port_forwards.push(request);
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
                selected_after
                    .as_deref()
                    .map_or(Ok(()), |context| self.refresh_infrastructure(context))
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
        let target = self.workspace_stream_target(window).ok_or_else(|| {
            ClientError::Protocol("selected resource cannot stream logs".to_owned())
        })?;
        let stores = self.shell.stream_stores_mut();
        stores.logs.ensure(window, target.clone()).connect();
        stores.logs.queue(
            window,
            crate::ui::tools::LogsAction::OpenLogs { window, target },
        );
        self.process_stream_requests()
    }

    /// Request the selected Pod's real interactive exec stream.
    pub fn web_connect_shell(&mut self, window: WindowId) -> Result<(), ClientError> {
        let target = self
            .workspace_stream_target(window)
            .ok_or_else(|| ClientError::Protocol("selected resource cannot exec".to_owned()))?;
        let stores = self.shell.stream_stores_mut();
        stores.shells.ensure(window, target.clone()).connect();
        stores.shells.queue_connect(window, target);
        self.process_stream_requests()
    }

    /// Semantic stream state rendered by the web adapter.
    #[must_use]
    pub fn web_stream_text(&self, window: WindowId) -> (String, Vec<String>, String, Vec<String>) {
        let logs = self.shell.stream_stores().logs.get(window);
        let log_phase = logs
            .map(|logs| format!("{:?}", logs.phase()))
            .unwrap_or_else(|| "Disconnected".to_owned());
        let log_lines = logs
            .map(|logs| logs.visible_lines().cloned().collect())
            .unwrap_or_default();
        let shell = self.shell.stream_stores().shells.get(window);
        let shell_phase = shell
            .map(|shell| format!("{:?}", shell.phase()))
            .unwrap_or_else(|| "Disconnected".to_owned());
        let shell_lines = shell
            .map(|shell| shell.buffer().cloned().collect())
            .unwrap_or_default();
        (log_phase, log_lines, shell_phase, shell_lines)
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
                            self.bootstrap = self.client.take_rebuilt_bootstrap();
                            self.view = AppView::Connecting;
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
                                    .any(|request| request.id() == id)
                            }) =>
                        {
                            if let Some(id) = stream_request_id.as_ref() {
                                self.pending_port_forwards
                                    .retain(|request| request.id() != id);
                            }
                            self.port_forward_error = Some(server_error.safe_message.clone());
                        }
                        // A vanished object's detail query is dropped
                        // quietly: the pinned window keeps rendering its
                        // loading state until a newer selection arrives.
                        ClientError::Server(ref server_error)
                            if stream_request_id.as_ref().is_some_and(|id| {
                                self.detail_requests
                                    .values()
                                    .any(|request| request.id() == id)
                            }) =>
                        {
                            if let Some(id) = stream_request_id.as_ref()
                                && let Some(identity) = self
                                    .detail_requests
                                    .iter()
                                    .find(|(_, request)| request.id() == id)
                                    .map(|(identity, _)| identity.clone())
                            {
                                self.detail_requests.remove(&identity);
                            }
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
                                match entry.route {
                                    StreamRoute::Logs => {
                                        if let Some(view) = self
                                            .shell
                                            .stream_stores_mut()
                                            .logs
                                            .get_mut(entry.window)
                                        {
                                            view.fail(&reason);
                                        }
                                    }
                                    StreamRoute::Exec => {
                                        if let Some(shell) = self
                                            .shell
                                            .stream_stores_mut()
                                            .shells
                                            .get_mut(entry.window)
                                        {
                                            shell.fail(&server_error.safe_message.clone());
                                        }
                                    }
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
                    && let Some((subscription, identity, revision)) = resource_delta
                    && self
                        .client
                        .resource_list(&subscription)
                        .and_then(|state| state.revision())
                        == Some(revision)
                {
                    self.shell
                        .yaml_editors_mut()
                        .target_changed(&identity, revision);
                }
                if applied && (server_rebuild_requested || replacement_snapshot_started) {
                    // A replacement is assembled across chunks, so neither
                    // the identities in an early page nor identities removed
                    // from the new view can be projected safely page by page.
                    // Revoke server-issued authority at the accepted begin;
                    // connection_lost deliberately preserves dirty buffers.
                    self.shell.yaml_editors_mut().connection_lost();
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
                    self.view = AppView::Ready {
                        server_instance_id,
                        context_names,
                        contexts,
                    };
                    self.recovering = false;
                    if self.client.port_forward_available() {
                        let _ = self
                            .client
                            .subscribe_port_forward_sessions()
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                        self.port_forward_list = Some(
                            self.client
                                .begin(Query::PortForwardList)
                                .map_err(|error| AppEventError::Terminal(error.to_string()))?,
                        );
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
                            for event in self.shell.apply_workspace_command(
                                WorkspaceCommand::CommitContextSwitch {
                                    to: context.clone(),
                                },
                            ) {
                                self.handle_workspace_event(event);
                            }
                        }
                        // The bootstrap answer already wrote the selection,
                        // so the render-time context-change path would skip
                        // it: reconcile the resource streams here.
                        self.reconcile_resource_streams(&context)
                            .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                        self.select_infrastructure_context(&context)
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
                for event in self
                    .shell
                    .apply_workspace_command(WorkspaceCommand::CommitContextSwitch { to: current })
                {
                    self.handle_workspace_event(event);
                }
                let current = self.shell.workspace().context().to_owned();
                self.reconcile_resource_streams(&current)
                    .map_err(|error| AppEventError::Terminal(error.to_string()))?;
                self.select_infrastructure_context(&current)
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
        if let Some(QueryResult::Infrastructure(_)) = self.client.take(request) {
            self.infrastructure_request = None;
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

    fn select_infrastructure_context(&mut self, context: &str) -> Result<(), ClientError> {
        if self.infrastructure_context.as_deref() != Some(context) {
            if let Some(subscription) = self.infrastructure_subscription.take() {
                self.client.unsubscribe(&subscription)?;
            }
            self.infrastructure_subscription =
                Some(self.client.subscribe_infrastructure(context.to_owned())?);
            self.infrastructure_context = Some(context.to_owned());
        }
        self.refresh_infrastructure(context)
    }

    fn refresh_infrastructure(&mut self, context: &str) -> Result<(), ClientError> {
        if let Some(request) = self.infrastructure_request.take() {
            if self.client.is_pending(&request) {
                self.client.cancel(&request)?;
            } else {
                let _ = self.client.take(request);
            }
        }
        self.infrastructure_request = Some(self.client.begin(Query::Infrastructure(
            InfrastructureRequest {
                context: context.to_owned(),
            },
        ))?);
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
        if self.client.phase() != ClientPhase::Disconnected {
            self.client.transport_lost(now_ms, entropy);
        }
        self.bootstrap = None;
        self.infrastructure_request = None;
        // An unanswered switch request died with the transport; the selection
        // stays where it was and a retry needs a fresh user action.
        self.pending_switch = None;
        self.failed_switch = None;
        self.teardown_stream_sessions();
        self.shell.yaml_editors_mut().connection_lost();
        // Server-issued details are stale after recovery and every in-flight
        // mutation lost its response channel: dialogs reopen for a safe
        // retry (the backend deduplicates by idempotency key).
        self.details.clear();
        self.detail_requests.clear();
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

    /// Assemble the connected resource projection for one rendered frame:
    /// applied per-kind list views, Service rows, selectable types, and
    /// resolved details.
    fn build_resource_feed(&self) -> ResourceFeed {
        let mut window_lists = std::collections::HashMap::new();
        let mut window_services = std::collections::HashMap::new();
        for (window, key) in &self.window_subscriptions {
            let Some(subscription) = self.resource_subscriptions.get(key) else {
                continue;
            };
            if let Some(state) = self.client.resource_list(subscription.live.id()) {
                let rows = state.rows().cloned().collect();
                if key.gvk.group.is_empty() && key.gvk.version == "v1" && key.gvk.kind == "Service"
                {
                    window_services.insert(*window, rows);
                } else {
                    window_lists.insert(*window, rows);
                }
            }
        }
        ResourceFeed {
            lists: std::collections::HashMap::new(),
            window_lists,
            services: None,
            window_services,
            types: self.resource_types.clone(),
            details: self.details.clone().into_iter().collect(),
            port_forward_available: self.client.port_forward_available(),
            port_forward_sessions: self
                .client
                .port_forward_sessions()
                .into_iter()
                .cloned()
                .collect(),
            port_forward_error: self.port_forward_error.clone(),
        }
    }

    /// Diff the watches demanded by open workspace list windows against the
    /// retained live set. Equal canonical keys share one bounded client
    /// subscription while every window retains its own projection mapping.
    fn reconcile_resource_streams(&mut self, context: &str) -> Result<(), ClientError> {
        use crate::workspace::{WindowContent, WindowKind};

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
        for window in self.shell.workspace().windows() {
            let (gvk, scope) = match (&window.kind, &window.content) {
                (WindowKind::Services, WindowContent::Services(state)) => (
                    k10s_protocol::GroupVersionKind::core("v1", "Service"),
                    SubscriptionScope::Namespaced(state.namespace_scope.clone()),
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
                        (descriptor.gvk.clone(), scope)
                    } else {
                        let Some((group, version, kind)) = builtin_kind_gvk(*kind) else {
                            continue;
                        };
                        (
                            k10s_protocol::GroupVersionKind {
                                group: group.to_owned(),
                                version: version.to_owned(),
                                kind: kind.to_owned(),
                            },
                            SubscriptionScope::Namespaced(state.namespace_scope.clone()),
                        )
                    }
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
            };
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
            .filter(|key| !self.resource_subscriptions.contains_key(*key))
            .count();
        if self.client.phase() == ClientPhase::Ready {
            self.client
                .preflight_subscription_changes(removed.len(), additions)?;
        }
        for key in removed {
            if let Some(entry) = self.resource_subscriptions.get(&key) {
                self.client.unsubscribe(&entry.live)?;
            }
            self.resource_subscriptions.remove(&key);
        }
        if self.client.phase() != ClientPhase::Ready {
            self.window_subscriptions = window_subscriptions;
            self.resource_types.clear();
            self.types_context = None;
            self.types_request = None;
            return Ok(());
        }
        for (key, windows) in desired {
            if let Some(entry) = self.resource_subscriptions.get_mut(&key) {
                entry.windows = windows;
                continue;
            }
            let live = match self.client.subscribe_resource(
                key.context.clone(),
                key.gvk.group.clone(),
                key.gvk.version.clone(),
                key.gvk.kind.clone(),
                key.protocol_namespace.clone(),
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

    /// Drain rendering-time dialog actions into workload mutation commands.
    fn process_dialog_actions(&mut self) -> Result<(), ClientError> {
        for (window, action) in self.shell.drain_dialog_actions() {
            let command = match action {
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
                    idempotency_key,
                } => Command::Delete {
                    target,
                    propagation,
                    idempotency_key,
                },
            };
            let request = self.client.begin_command(command)?;
            self.pending_mutations
                .insert(request.id().clone(), PendingMutation { request, window });
        }
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
        self.refresh_details();
    }

    /// Issue detail queries for every identity a window pinned so the
    /// rendered feed carries backend-resolved views. Identities already
    /// resolved keep their cached response until transport loss.
    fn refresh_details(&mut self) {
        // Until bootstrap identifies the server's authoritative context, the
        // workspace may still contain selections from the lost generation.
        // Do not issue a detail read against that stale context; bootstrap
        // either commits a new context (clearing the selection) or confirms
        // the existing one before recovery ends.
        if self.recovering {
            return;
        }
        let mut desired: Vec<ResourceIdentity> = Vec::new();
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
            };
            if let Some(identity) = identity
                && !self.details.contains_key(identity)
                && !desired.iter().any(|known| known == identity)
            {
                desired.push(identity.clone());
            }
        }
        for identity in desired {
            if self.detail_requests.contains_key(&identity) {
                continue;
            }
            match self.client.begin(Query::ResourceDetail(identity.clone())) {
                Ok(request) => {
                    self.detail_requests.insert(identity, request);
                }
                Err(_) => return,
            }
        }
        // Collect completed detail responses.
        let completed: Vec<ResourceIdentity> = self
            .detail_requests
            .iter()
            .filter(|(_, request)| !self.client.is_pending(request))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in completed {
            if let Some(request) = self.detail_requests.remove(&identity)
                && let Some(QueryResult::ResourceDetail(response)) = self.client.take(request)
            {
                self.details.insert(identity, *response);
            }
        }
        self.flush_outbound()
            .map_err(|error| ClientError::Protocol(format!("{error:?}")))
            .ok();
    }

    fn reconnect_if_due(&mut self, now_ms: u64, entropy: u64) {
        if Self::terminal_phase(self.client.phase()) || self.connection.is_some() {
            return;
        }
        match self.client.retry_if_due(now_ms) {
            Ok(true) => match self.factory.connect(&self.connection_url) {
                Ok(connection) => {
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
        self.teardown_stream_sessions();
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
            })?;
        let identity = k10s_ui_row_identity(&detail.identity)?;
        Some(StreamTarget {
            context: identity.context.clone(),
            namespace: identity
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_owned()),
            pod: identity.name.clone(),
            uid: identity.uid.clone(),
            container: "app".to_owned(),
        })
    }

    /// Whether the window's workspace shell guard is currently engaged.
    fn shell_guard_connected(&self, window: WindowId) -> bool {
        self.shell
            .workspace()
            .window(window)
            .and_then(|w| match &w.content {
                crate::workspace::WindowContent::Detail(detail) => Some(detail.shell.connected),
                crate::workspace::WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .map(|detail| detail.shell.connected),
                crate::workspace::WindowContent::Services(service) => {
                    service.detail.as_ref().map(|detail| detail.shell.connected)
                }
            })
            .unwrap_or(false)
    }

    /// Release the workspace connected-shell guard once a terminal is no
    /// longer live (exit, rejection, or transport loss).
    fn release_shell_guard(&mut self, window: WindowId) {
        for event in self
            .shell
            .apply_workspace_command(WorkspaceCommand::DisconnectShell(window))
        {
            self.handle_workspace_event(event);
        }
    }

    /// Apply workspace events produced by command application. Only the
    /// focus-raising events matter at this layer, and guard-resolved switch
    /// requests are staged toward the backend as explicit user actions.
    fn handle_workspace_event(&mut self, event: WorkspaceEvent<ResourceIdentity>) {
        if let WorkspaceEvent::ContextSwitchRequested { to } = event
            && let Err(error) = self.stage_context_switch(&to, true)
        {
            self.terminal_failure(error.to_string());
        }
    }

    /// Close every dedicated stream session, release any connected-shell
    /// guards it held, and mark its tool disconnected.
    fn teardown_stream_sessions(&mut self) {
        let exec_windows: Vec<WindowId> = self
            .stream_sessions
            .keys()
            .filter(|(_, route)| *route == StreamRoute::Exec)
            .map(|(window, _)| *window)
            .collect();
        for (_, mut session) in std::mem::take(&mut self.stream_sessions) {
            session.disconnect();
        }
        for window in exec_windows {
            for event in self
                .shell
                .apply_workspace_command(WorkspaceCommand::DisconnectShell(window))
            {
                self.handle_workspace_event(event);
            }
        }
        self.pending_stream_tickets.clear();
        self.shell.stream_stores_mut().connection_lost();
    }

    /// Reconcile live sessions against the authoritative workspace state:
    /// a window whose pinned identity rebinds to another pod must never
    /// keep the old pod's socket, and a guard resolved away
    /// (DisconnectShell) closes its attached terminal. Tool phases are
    /// always kept consistent with transport ownership.
    fn reconcile_sessions(&mut self) {
        // Pass 1 (immutable): decide what must be torn down.
        let mut stale: Vec<((WindowId, StreamRoute), bool, &'static str)> = Vec::new();
        let attached_exec: std::collections::HashSet<WindowId> = {
            let stores = self.shell.stream_stores_mut();
            self.stream_sessions
                .keys()
                .filter(|(_, route)| *route == StreamRoute::Exec)
                .filter_map(|(window, _)| {
                    stores
                        .shells
                        .get_mut(*window)
                        .filter(|shell| matches!(shell.phase(), ShellPhase::Attached))
                        .map(|_| *window)
                })
                .collect()
        };
        for (key, session) in self.stream_sessions.iter() {
            let (window, route) = *key;
            let target_matches =
                self.workspace_stream_target(window).as_ref() == Some(session.target());
            if !target_matches {
                // The selection moved on while this session existed. If it
                // had already engaged the guard, release that guard too.
                let release = route == StreamRoute::Exec && self.shell_guard_connected(window);
                stale.push((*key, release, "the shell target changed"));
            } else if route == StreamRoute::Exec
                && attached_exec.contains(&window)
                && !self.shell_guard_connected(window)
            {
                // The guard was resolved away without an exit signal.
                stale.push((*key, false, "shell session closed"));
            }
        }
        // Pass 2 (mutable): tear down atomically - transport, tool phase,
        // and workspace guard together.
        for ((window, route), release_guard, reason) in stale {
            if release_guard {
                self.release_shell_guard(window);
            }
            if let Some(mut session) = self.stream_sessions.remove(&(window, route)) {
                session.disconnect();
            }
            let stores = self.shell.stream_stores_mut();
            match route {
                StreamRoute::Logs => {
                    if let Some(view) = stores.logs.get_mut(window) {
                        view.fail(reason);
                    }
                }
                StreamRoute::Exec => {
                    // Intentional teardown stays reconnectable.
                    let _ = reason;
                    if let Some(shell) = stores.shells.get_mut(window) {
                        shell.disconnect_intentional();
                    }
                }
            }
        }
    }

    fn terminal_phase(phase: ClientPhase) -> bool {
        matches!(
            phase,
            ClientPhase::WebGate | ClientPhase::UpgradeRequired | ClientPhase::Closed
        )
    }

    /// Drain rendering-time stream actions: ticket requests for new log
    /// views and explicit shell connects, plus stdin/resize forwarding into
    /// live sessions.
    fn process_stream_requests(&mut self) -> Result<(), ClientError> {
        for (window, action) in self.shell.drain_log_actions() {
            let request = self.client.begin(Query::StreamTicket {
                target: match action {
                    crate::ui::tools::LogsAction::OpenLogs { target, .. } => target,
                },
                stream_type: k10s_protocol::StreamType::Logs,
                tty: false,
            })?;
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
                },
            );
        }
        for (window, target) in self.shell.drain_shell_connects() {
            let request = self.client.begin(Query::StreamTicket {
                target: target.clone(),
                stream_type: k10s_protocol::StreamType::Exec,
                tty: true,
            })?;
            // Same explicit Connecting transition for the terminal.
            if let Some(shell) = self.shell.stream_stores_mut().shells.get_mut(window) {
                shell.connect();
            }
            self.pending_stream_tickets.insert(
                request.id().clone(),
                PendingStreamTicket {
                    request,
                    route: StreamRoute::Exec,
                    window,
                },
            );
        }
        // Forward terminal stdin/resize into live exec sessions.
        for (window, action) in self.shell.drain_shell_actions() {
            let key = (window, StreamRoute::Exec);
            if let Some(session) = self.stream_sessions.get_mut(&key)
                && session.is_live()
            {
                match action {
                    crate::ui::tools::ShellAction::Input(line) => session.send_stdin(&line),
                    crate::ui::tools::ShellAction::Resize { cols, rows } => {
                        session.send_resize(cols, rows);
                    }
                }
            }
        }
        self.flush_outbound()
            .map_err(|error| ClientError::Protocol(format!("{error:?}")))
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
            } = entry;
            if let Some(QueryResult::StreamTicket(granted)) = self.client.take(request)
                && session_open(
                    &mut self.stream_sessions,
                    window,
                    route,
                    *granted,
                    &self.connection_url,
                    &self.access_token,
                )
                .is_ok()
            {
                // The session is now live and polling.
            }
        }
    }

    /// Project dedicated-socket events into the connected tools.
    fn poll_stream_sessions(&mut self) {
        self.finish_stream_tickets();
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
                self.workspace_stream_target(window).as_ref() == Some(&bound_target);
            // Guard transitions are collected while the tool stores are
            // borrowed and applied afterwards.
            let mut guard_connected = false;
            let mut guard_released = false;
            let mut stale_handshake = false;
            // Tool projections run inside this block so the store borrow
            // ends before workspace commands are applied.
            let stores = self.shell.stream_stores_mut();
            {
                for signal in signals {
                    match signal {
                        StreamSignal::Ready { .. } => match route {
                            StreamRoute::Logs => {
                                if !target_current {
                                    continue;
                                }
                                if let Some(view) = stores.logs.get_mut(window) {
                                    view.attach();
                                }
                            }
                            StreamRoute::Exec => {
                                if !target_current {
                                    // Intentional teardown: reconnectable.
                                    if let Some(shell) = stores.shells.get_mut(window) {
                                        shell.disconnect_intentional();
                                    }
                                    stale_handshake = true;
                                    continue;
                                }
                                if let Some(shell) = stores.shells.get_mut(window) {
                                    shell.attach();
                                }
                                // The live terminal engages the workspace's
                                // connected-shell navigation guard.
                                guard_connected = true;
                            }
                        },
                        StreamSignal::Output(text) => match route {
                            StreamRoute::Logs => {
                                if let Some(view) = stores.logs.get_mut(window) {
                                    view.append(&text);
                                }
                            }
                            StreamRoute::Exec => {
                                if let Some(shell) = stores.shells.get_mut(window) {
                                    shell.apply_output(&text);
                                }
                            }
                        },
                        StreamSignal::Status(_message) => {}
                        StreamSignal::Exited(code) => {
                            if let Some(shell) = stores.shells.get_mut(window) {
                                shell.exit(code);
                            }
                            guard_released = true;
                        }
                        StreamSignal::Rejected(reason) => match route {
                            StreamRoute::Logs => {
                                if let Some(view) = stores.logs.get_mut(window) {
                                    view.fail(&reason);
                                }
                                self.stream_sessions.remove(&key);
                            }
                            StreamRoute::Exec => {
                                if let Some(shell) = stores.shells.get_mut(window) {
                                    shell.fail(&reason);
                                }
                                guard_released = true;
                                self.stream_sessions.remove(&key);
                            }
                        },
                    }
                }
            }
            if guard_connected {
                for event in self
                    .shell
                    .apply_workspace_command(WorkspaceCommand::ConnectShell(window))
                {
                    self.handle_workspace_event(event);
                }
            }
            if guard_released {
                self.release_shell_guard(window);
            }
            if stale_handshake {
                self.stream_sessions.remove(&key);
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
)> {
    let subscription = frame.subscription_id.clone()?;
    let k10s_protocol::ServerPayload::Event(event) = frame.decode_payload().ok()? else {
        return None;
    };
    match event.event_kind.as_str() {
        k10s_protocol::RESOURCE_EVENT_CHANGED => {
            let delta: k10s_protocol::ResourceChanged =
                serde_json::from_value(event.payload).ok()?;
            Some((subscription, delta.identity, delta.row.revision))
        }
        k10s_protocol::RESOURCE_EVENT_GONE => {
            let delta: k10s_protocol::ResourceGone = serde_json::from_value(event.payload).ok()?;
            Some((subscription, delta.identity, delta.revision))
        }
        _ => None,
    }
}

/// The wire identity of each built-in workload kind. Custom resources are
/// picker-driven and have no single GVK.
fn builtin_kind_gvk(kind: WorkloadKind) -> Option<(&'static str, &'static str, &'static str)> {
    match kind {
        WorkloadKind::Deployments => Some(("apps", "v1", "Deployment")),
        WorkloadKind::StatefulSets => Some(("apps", "v1", "StatefulSet")),
        WorkloadKind::DaemonSets => Some(("apps", "v1", "DaemonSet")),
        WorkloadKind::Jobs => Some(("batch", "v1", "Job")),
        WorkloadKind::CronJobs => Some(("batch", "v1", "CronJob")),
        WorkloadKind::Pods => Some(("", "v1", "Pod")),
        WorkloadKind::CustomResources => None,
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
) -> Result<(), ()> {
    let mut session = StreamSession::new(route, granted.target.clone(), granted.tty);
    session
        .open_with_ticket(connection_url, access_token, &granted.ticket_id)
        .map_err(|_| ())?;
    sessions.insert((window, route), session);
    Ok(())
}

#[derive(Debug)]
enum AppEventError {
    Transient,
    Terminal(String),
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

    use egui_kittest::{Harness, kittest::Queryable as _};
    use ewebsock::{WsEvent, WsMessage};
    use k10s_protocol::{
        BackendRevision, BootstrapResponse, ClientFrame, ClientKind, Context, ErrorCode,
        ErrorFrame, ErrorScope, Event, GroupVersionKind, ProtocolVersion, RequestId,
        ResourceChanged, ResourceIdentity, ResourceListRow, ResourceSnapshotPage, ResumeStatus,
        Retryability, ServerFrame, ServerKind, SessionId, SnapshotBegin, SnapshotChunk,
        SnapshotEnd, Subscribed, SubscriptionId, SubscriptionSelector, ValidationTicket, Welcome,
        YamlOutcome, buffer_hash,
    };

    use super::{AppConnection, AppView, ConnectionFactory, K10sApp};
    use crate::client::{ClientPhase, ConnectTarget, Query, TransportError};
    use crate::workspace::{
        NamespaceScope, WindowContent, WorkloadKind, WorkspaceCommand, WorkspaceEvent,
    };

    #[test]
    fn production_control_inbox_holds_a_large_default_snapshot_burst() {
        assert_eq!(super::CONTROL_INBOX_CAPACITY, 256);
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

    fn server_message(frame: &ServerFrame) -> WsEvent {
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

    fn welcome() -> ServerFrame {
        ServerFrame {
            kind: ServerKind::Welcome,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(Welcome {
                protocol: ProtocolVersion { major: 1, minor: 1 },
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
                server_message(&gapped_event),
            ]),
            overflowed: false,
        }]);

        app.poll_at(100, 0);

        assert!(!matches!(app.view(), AppView::Failed { .. }));
        assert_eq!(app.client.phase(), ClientPhase::Ready);
        assert!(app.connection.is_some(), "existing connection remains live");
        assert_eq!(state.borrow().connect_count, 1);
        let request_kinds: Vec<_> = state
            .borrow()
            .sent
            .iter()
            .filter(|frame| frame.kind == ClientKind::Request)
            .filter_map(request_kind)
            .collect();
        assert_eq!(
            request_kinds,
            ["bootstrap", "infrastructure.get", "bootstrap"],
            "initial bootstrap loads infrastructure; the gap adds one resync bootstrap"
        );
        assert_eq!(app.client.outbound_len(), 0);
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

    fn ready_app() -> (K10sApp, Rc<RefCell<FactoryState>>) {
        let bootstrap =
            ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
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
    fn context_default_replaces_watch_when_same_context_namespace_changes() {
        let (mut app, state) = ready_app();
        let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
        assert_eq!(
            resource_watches(&state)[0].namespace.as_deref(),
            Some("default")
        );

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
        assert_eq!(watches.len(), 2);
        assert_eq!(watches.last().unwrap().namespace.as_deref(), Some("team-b"));
        assert_eq!(
            state
                .borrow()
                .sent
                .iter()
                .filter(|frame| frame.kind == ClientKind::Unsubscribe)
                .count(),
            1
        );
        let key = app.window_subscriptions.get(&window).unwrap();
        assert!(matches!(
            key.scope,
            super::SubscriptionScope::Namespaced(NamespaceScope::ContextDefault)
        ));
        assert_eq!(app.resource_subscriptions.len(), 1);
        assert!(
            app.build_resource_feed()
                .window_lists
                .get(&window)
                .is_none(),
            "the replaced watch has no stale rows projected under the window"
        );
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
    fn equal_window_keys_share_and_last_close_unsubscribes() {
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
        app.retry_now(200, 0).unwrap();
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
}

#[cfg(test)]
mod stream_lifecycle_tests {
    use std::sync::mpsc;

    use ewebsock::{WsEvent, WsMessage};
    use k10s_protocol::{GroupVersionKind, ResourceIdentity, StreamTarget, StreamType};

    use super::K10sApp;
    use super::tests::test_app;
    use crate::client::{StreamIo, StreamRoute, StreamSession};
    use crate::ui::tools::ShellPhase;
    use crate::workspace::{DetailTab, WorkloadKind, WorkspaceCommand};

    #[derive(Debug)]
    struct ScriptStream {
        events: mpsc::Receiver<WsEvent>,
    }

    impl StreamIo for ScriptStream {
        fn try_recv(&mut self) -> Option<WsEvent> {
            self.events.try_recv().ok()
        }
        fn send_text(&mut self, _text: String) {}
        fn send_binary(&mut self, _bytes: Vec<u8>) {}
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
        StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: pod_name.into(),
            uid: format!("uid-{pod_name}"),
            container: "app".into(),
        }
    }

    fn ready_signal(tx: &mpsc::Sender<WsEvent>, container: &str) {
        tx.send(WsEvent::Message(WsMessage::Text(
            serde_json::to_string(&k10s_protocol::StreamServerMessage::Ready {
                stream_type: StreamType::Exec,
                tty: true,
                container: container.to_owned(),
            })
            .unwrap(),
        )))
        .unwrap();
    }

    pub(super) fn open_pod_detail(
        app: &mut K10sApp,
        pod: &ResourceIdentity,
    ) -> crate::workspace::WindowId {
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

    fn attach_session(
        app: &mut K10sApp,
        window: crate::workspace::WindowId,
        pod: &ResourceIdentity,
    ) -> mpsc::Sender<WsEvent> {
        let (tx, rx) = mpsc::channel();
        // Production renders the Shell tab first, creating the tool, and
        // process_stream_requests moves it to Connecting on connect; mirror
        // both here.
        {
            let stores = app.shell.stream_stores_mut();
            stores
                .shells
                .ensure(window, target_for(&pod.name))
                .connect();
        }
        let mut session = StreamSession::new(StreamRoute::Exec, target_for(&pod.name), true);
        session.inject_for_test(ScriptStream { events: rx });
        app.stream_sessions
            .insert((window, StreamRoute::Exec), session);
        tx
    }

    /// Ready attaches the terminal and engages the guard; resolving the
    /// guard closes the transport and fails the tool atomically; selection
    /// can then move on without a ghost session or a stuck guard.
    #[test]
    fn exec_ready_engages_guard_and_disconnect_resolution_closes_the_transport() {
        let (mut app, _state) = test_app(Vec::new());
        let pod_a = pod("pod-a");
        let pod_b = pod("pod-b");
        let window = open_pod_detail(&mut app, &pod_a);
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-a"))
        );

        // Connecting session becomes Ready: attach + guard.
        let tx = attach_session(&mut app, window, &pod_a);
        ready_signal(&tx, "app");
        app.poll_stream_sessions();
        assert!(
            app.shell_guard_connected(window),
            "Ready must engage the guard"
        );
        assert_eq!(
            *app.shell
                .stream_stores_mut()
                .shells
                .get_mut(window)
                .expect("terminal exists")
                .phase(),
            ShellPhase::Attached
        );

        // Navigation to pod B is blocked while the shell is connected...
        let blocked = app
            .shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, pod_b.clone()));
        assert!(
            blocked
                .iter()
                .any(|event| matches!(event, crate::workspace::WorkspaceEvent::Blocked(_)))
        );

        // ...resolving DisconnectShell commits the navigation to pod B,
        // clearing the guard...
        app.shell
            .apply_workspace_command(WorkspaceCommand::ResolveBlock(
                crate::workspace::BlockResolution::DisconnectShell { window },
            ));
        assert!(!app.shell_guard_connected(window));
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-b"))
        );
        // ...and reconciliation then closes the attached transport and
        // fails the tool atomically.
        app.poll_stream_sessions();
        assert!(!app.shell_guard_connected(window));
        assert_eq!(
            *app.shell
                .stream_stores_mut()
                .shells
                .get_mut(window)
                .expect("terminal exists")
                .phase(),
            ShellPhase::Disconnected
        );
        assert!(
            !app.stream_sessions
                .contains_key(&(window, StreamRoute::Exec)),
            "the resolved-away terminal must lose its transport"
        );

        // Navigation now succeeds and the workspace rebinds to pod B; no
        // stale session survives for the old pod.
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, pod_b.clone()));
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-b"))
        );

        // Clearing the selection and reselecting the SAME pod A must leave
        // a reconnectable terminal (not a dead Failed one): connect works
        // again and a fresh Ready re-engages the guard.
        app.shell
            .apply_workspace_command(WorkspaceCommand::ClearSelection(window));
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, pod_a.clone()));
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-a"))
        );
        let tx2 = attach_session(&mut app, window, &pod_a);
        ready_signal(&tx2, "app");
        app.poll_stream_sessions();
        assert!(
            app.shell_guard_connected(window),
            "a fresh session can engage the guard again"
        );
        assert_eq!(
            *app.shell
                .stream_stores_mut()
                .shells
                .get_mut(window)
                .expect("terminal exists")
                .phase(),
            ShellPhase::Attached
        );
    }

    /// A Ready arriving after the selection moved on is dropped: it never
    /// attaches the old pod's session nor engages the new pod's guard.
    #[test]
    fn stale_handshake_ready_never_attaches_or_guards() {
        let (mut app, _state) = test_app(Vec::new());
        let pod_a = pod("pod-a");
        let pod_b = pod("pod-b");
        let window = open_pod_detail(&mut app, &pod_a);

        // A connecting session for pod A...
        let tx = attach_session(&mut app, window, &pod_a);

        // ...and the user moves on to pod B before the handshake completes.
        app.shell
            .apply_workspace_command(WorkspaceCommand::SelectRow(window, pod_b.clone()));
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-b"))
        );

        ready_signal(&tx, "app");
        app.poll_stream_sessions();

        // The stale Ready is discarded outright.
        assert!(
            !app.stream_sessions
                .contains_key(&(window, StreamRoute::Exec)),
            "the superseded session must be dropped"
        );
        assert!(
            !app.shell_guard_connected(window),
            "pod B's guard must never engage for pod A's session"
        );
        assert_eq!(
            *app.shell
                .stream_stores_mut()
                .shells
                .get_mut(window)
                .expect("terminal exists")
                .phase(),
            ShellPhase::Disconnected
        );
        // The detail tab state stays consistent for the next connect attempt.
        assert_eq!(
            app.workspace_stream_target(window).as_ref(),
            Some(&target_for("pod-b"))
        );
        let _ = DetailTab::Shell;
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
        fn send_binary(&mut self, _bytes: Vec<u8>) {}
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
        let mut session = StreamSession::new(StreamRoute::Logs, target_for("pod-a"), false);
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
        let mut session = StreamSession::new(StreamRoute::Logs, target_for("pod-a"), false);
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
