//! Minimal application state driven exclusively through the shared protocol client.

use web_time::Instant;

use std::collections::BTreeMap;

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_protocol::{ClientFrame, InfrastructureRequest, ResourceIdentity, ServerFrame};

use crate::client::{
    BoundedInbox, ClientConfig, ClientError, ClientPhase, ClientState, ConnectTarget,
    LiveSubscription, PendingRequest, Query, QueryResult, StreamRoute, StreamSession, StreamSignal,
    TransportError, WebSocketTransport,
};
use crate::ui::{ConnectionState as ShellConnectionState, UiShell, tools::ShellPhase};
use crate::workspace::{WindowId, WorkspaceCommand, WorkspaceEvent, WorkspaceState};

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
        let (transport, inbox) = WebSocketTransport::connect(url, Options::default(), 64)?;
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
    bootstrap: Option<PendingRequest>,
    infrastructure_request: Option<PendingRequest>,
    infrastructure_subscription: Option<LiveSubscription>,
    infrastructure_context: Option<String>,
    stream_sessions: BTreeMap<(WindowId, StreamRoute), StreamSession>,
    pending_stream_tickets: BTreeMap<k10s_protocol::RequestId, PendingStreamTicket>,
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
            .field("bootstrap", &self.bootstrap)
            .field("infrastructure_request", &self.infrastructure_request)
            .field(
                "infrastructure_subscription",
                &self.infrastructure_subscription,
            )
            .field("infrastructure_context", &self.infrastructure_context)
            .field("stream_sessions", &self.stream_sessions.len())
            .field("pending_stream_tickets", &self.pending_stream_tickets.len())
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
            bootstrap: None,
            infrastructure_request: None,
            infrastructure_subscription: None,
            infrastructure_context: None,
            stream_sessions: BTreeMap::new(),
            pending_stream_tickets: BTreeMap::new(),
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

    /// Render the approved default-egui shell for the current connection view.
    pub fn render_ui(&mut self, ui: &mut egui::Ui) {
        let (connection, contexts): (ShellConnectionState, &[String]) = match &self.view {
            AppView::Connecting => (ShellConnectionState::Connecting, &[]),
            AppView::Ready { context_names, .. } => {
                (ShellConnectionState::Connected, context_names.as_slice())
            }
            AppView::Failed { .. } => (ShellConnectionState::Failed, &[]),
        };
        let selected_before = self.client.local_ui().selected_context.clone();
        let response = selected_before
            .as_deref()
            .and_then(|context| self.client.infrastructure(context))
            .cloned();
        let refresh = self.shell.show_with_infrastructure(
            ui,
            connection,
            contexts,
            &mut self.client.local_ui_mut().selected_context,
            response.as_ref(),
        );
        let selected_after = self.client.local_ui().selected_context.clone();
        let retry_requested = refresh && connection != ShellConnectionState::Connected;
        let request_result = if retry_requested {
            let now_ms =
                u64::try_from(self.clock_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.jitter_counter = self.jitter_counter.wrapping_add(1);
            let entropy = now_ms.rotate_left(17) ^ self.jitter_counter.wrapping_mul(0x9e37_79b9);
            self.retry_now(now_ms, entropy)
        } else if selected_after != selected_before {
            selected_after.as_deref().map_or(Ok(()), |context| {
                self.select_infrastructure_context(context)
            })
        } else if refresh {
            selected_after
                .as_deref()
                .map_or(Ok(()), |context| self.refresh_infrastructure(context))
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
            } => format!(
                "Server {server_instance_id}\nContexts: {}",
                context_names.join(", ")
            ),
            AppView::Failed { message } => format!("Connection failed: {message}"),
        }
    }

    fn handle_event(
        &mut self,
        event: WsEvent,
        now_ms: u64,
        entropy: u64,
    ) -> Result<(), AppEventError> {
        match event {
            WsEvent::Opened => self.flush_outbound(),
            WsEvent::Message(WsMessage::Text(text)) => {
                let frame: ServerFrame = serde_json::from_str(&text).map_err(|error| {
                    AppEventError::Terminal(format!("could not decode server frame: {error}"))
                })?;
                let stream_request_id = frame.request_id.clone();
                if let Err(error) = self.client.apply_at(frame, now_ms, entropy) {
                    match error {
                        ClientError::SequenceGap { .. } => {
                            self.bootstrap = self.client.take_rebuilt_bootstrap();
                            self.view = AppView::Connecting;
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
                        _ if self.client.phase() == ClientPhase::Disconnected => {
                            return Err(AppEventError::Transient);
                        }
                        _ => return Err(AppEventError::Terminal(error.to_string())),
                    }
                }
                self.finish_infrastructure_request();
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
                    let context_names: Vec<String> = response
                        .contexts
                        .into_iter()
                        .map(|context| context.name)
                        .collect();
                    let selected = self
                        .client
                        .local_ui()
                        .selected_context
                        .clone()
                        .filter(|selected| context_names.contains(selected))
                        .or_else(|| context_names.first().cloned());
                    self.client.local_ui_mut().selected_context = selected.clone();
                    self.view = AppView::Ready {
                        server_instance_id,
                        context_names,
                    };
                    self.recovering = false;
                    if let Some(context) = selected {
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

    fn flush_outbound(&mut self) -> Result<(), AppEventError> {
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
            Ok(connection) => self.connection = Some(connection),
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
        if self.client.phase() != ClientPhase::Disconnected {
            self.client.transport_lost(now_ms, entropy);
        }
        self.bootstrap = None;
        self.infrastructure_request = None;
        self.teardown_stream_sessions();
        self.recovering = true;
        self.view = AppView::Connecting;
    }

    fn reconnect_if_due(&mut self, now_ms: u64, entropy: u64) {
        if Self::terminal_phase(self.client.phase()) || self.connection.is_some() {
            return;
        }
        match self.client.retry_if_due(now_ms) {
            Ok(true) => match self.factory.connect(&self.connection_url) {
                Ok(connection) => self.connection = Some(connection),
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
        self.teardown_stream_sessions();
        self.view = AppView::Failed { message };
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
    /// focus-raising events matter at this layer.
    fn handle_workspace_event(&mut self, _event: WorkspaceEvent<ResourceIdentity>) {}

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

    /// Reconcile live sessions against their tools: a window whose pinned
    /// identity rebinds to another pod must never keep the old pod's
    /// socket, and a guard resolved away (DisconnectShell) closes its
    /// terminal.
    fn reconcile_sessions(&mut self) {
        // Target rebinding: compare each session's bound target with what
        // its window's tool now resolves to.
        {
            let stores = self.shell.stream_stores_mut();
            let stale: Vec<(WindowId, StreamRoute)> = self
                .stream_sessions
                .iter()
                .filter(|((window, route), session)| {
                    let bound = match route {
                        StreamRoute::Logs => stores.logs.target_of(*window),
                        StreamRoute::Exec => stores.shells.target_of(*window),
                    };
                    bound.as_ref() != Some(session.target())
                })
                .map(|(key, _)| *key)
                .collect();
            for key in stale {
                if let Some(mut session) = self.stream_sessions.remove(&key) {
                    session.disconnect();
                    match key.1 {
                        StreamRoute::Logs => {
                            if let Some(view) = stores.logs.get_mut(key.0) {
                                view.fail("the log target changed");
                            }
                        }
                        StreamRoute::Exec => {
                            if let Some(shell) = stores.shells.get_mut(key.0) {
                                shell.fail("the shell target changed");
                            }
                        }
                    }
                }
            }
        }
        // Guard resolution: a workspace that resolved DisconnectShell (or
        // lost its guarded detail) must not keep an ATTACHED terminal. A
        // session still connecting has not engaged the guard yet, so it is
        // left alone until its Ready signal arrives.
        let exec_windows: Vec<WindowId> = self
            .stream_sessions
            .range(..)
            .filter(|((_, route), _)| *route == StreamRoute::Exec)
            .map(|((window, _), _)| *window)
            .collect();
        for window in exec_windows {
            // Integrated resource windows carry their detail inside the
            // resource state; dedicated windows are Detail directly.
            let guard = self
                .shell
                .workspace()
                .window(window)
                .and_then(|w| match &w.content {
                    crate::workspace::WindowContent::Detail(detail) => Some(detail.shell.connected),
                    crate::workspace::WindowContent::Resource(resource) => resource
                        .detail
                        .as_ref()
                        .map(|detail| detail.shell.connected),
                });
            let attached = self
                .shell
                .stream_stores_mut()
                .shells
                .get_mut(window)
                .is_some_and(|shell| matches!(shell.phase(), ShellPhase::Attached));
            if attached && guard != Some(true) {
                self.release_shell_guard(window);
                if let Some(mut session) = self.stream_sessions.remove(&(window, StreamRoute::Exec))
                {
                    session.disconnect();
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
            // Guard transitions are collected while the tool stores are
            // borrowed and applied afterwards.
            let mut guard_connected = false;
            let mut guard_released = false;
            // Tool projections run inside this block so the store borrow
            // ends before workspace commands are applied.
            let stores = self.shell.stream_stores_mut();
            {
                for signal in signals {
                    match signal {
                        StreamSignal::Ready { .. } => match route {
                            StreamRoute::Logs => {
                                if let Some(view) = stores.logs.get_mut(window) {
                                    view.attach();
                                }
                            }
                            StreamRoute::Exec => {
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
        }
    }
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
        BootstrapResponse, ClientFrame, ClientKind, ErrorCode, ErrorFrame, ErrorScope,
        ProtocolVersion, RequestId, ResumeStatus, Retryability, ServerFrame, ServerKind, SessionId,
        Subscribed, SubscriptionId, Welcome,
    };

    use super::{AppConnection, AppView, ConnectionFactory, K10sApp};
    use crate::client::{ClientPhase, ConnectTarget, TransportError};

    #[derive(Debug, Default)]
    struct FactoryState {
        connect_count: usize,
        sent: Vec<ClientFrame>,
        received: usize,
        connections: VecDeque<ConnectionScript>,
    }

    #[derive(Debug, Default)]
    struct ConnectionScript {
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

    fn test_app(scripts: Vec<ConnectionScript>) -> (K10sApp, Rc<RefCell<FactoryState>>) {
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

    #[test]
    fn application_owns_the_overview_only_workspace_shell() {
        let (app, _) = test_app(Vec::new());

        assert_eq!(app.workspace().windows().len(), 1);
        assert_eq!(
            app.workspace().windows()[0].kind,
            crate::workspace::WindowKind::Overview
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
                events: VecDeque::from([WsEvent::Error("connection reset".to_owned())]),
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
        assert_eq!(app.view(), &AppView::Connecting);
        assert_eq!(state.borrow().connect_count, 1);

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
            "recovery loads bootstrap then exactly one infrastructure snapshot"
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
}
