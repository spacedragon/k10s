//! The fixed application shell rendered around the command-driven workspace.

mod command_palette;
mod detail;
pub(crate) use detail::presentation::system_time_from_rfc3339;
pub mod dialogs;
mod infrastructure;
mod launcher;
mod overview;
mod port_forward;
mod resource_table;
mod resource_window;
mod responsive_table;
mod service_window;
mod split;
mod taskbar;
mod theme;
pub mod tools;
mod top_bar;
mod window;

pub(crate) use detail::PodRuntimeProjection;
pub(crate) use port_forward::port_forward_start_authorization;

pub use port_forward::{
    LocalPortError, PortForwardModalGeneration, PortForwardRetryErrors, PortForwardStartModal,
    RETRY_LOCAL_PORT_GUIDANCE, retry_start_request,
};
pub use resource_window::{
    DetailAuthority, DetailLifecycle, NamespaceCatalogState, PrimaryDetailState, RelationState,
    ResourceFeed, RowIdentity, SafeUiError, WindowFreshness,
};
pub use service_window::{
    cluster_ip_column_label, port_compact_label, port_detail_label, ports_column_label,
};

use std::fmt::Debug;

use crate::workspace::{WindowId, WorkspaceCommand, WorkspaceEvent, WorkspaceState};

/// User-visible state of the shared control connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Failed,
}

/// Capability-local infrastructure presentation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfrastructureLoad {
    Loading,
    Available,
    Unavailable,
}

impl ConnectionState {
    fn label(self) -> &'static str {
        match self {
            Self::Connecting => "Connecting",
            Self::Connected => "Connected",
            Self::Failed => "Connection failed",
        }
    }
}

/// Where a staged context-switch request came from. The distinction lets
/// the application layer retry a recently failed destination on a fresh
/// user action while passive reconciliation stays suppressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextRequestOrigin {
    /// A fresh user action: a top-bar pick or a guard-resolved switch the
    /// user originally asked for.
    Explicit,
    /// Passive reconciliation of a selection/workspace mismatch.
    Reconcile,
}

impl ContextRequestOrigin {
    /// Whether this request came from a fresh user action.
    #[must_use]
    pub fn is_explicit(self) -> bool {
        matches!(self, Self::Explicit)
    }
}

/// Persistent UI shell state. The existing [`WorkspaceState`] remains the
/// only source of truth for window instances, geometry, focus, and content.
#[derive(Debug)]
pub struct UiShell<I> {
    workspace: WorkspaceState<I>,
    infrastructure: infrastructure::InfrastructureUiState,
    resources: resource_window::ResourceUiState,
    yaml: tools::YamlEditors,
    streams: tools::StreamStores,
    dialogs: dialogs::OperationDialogs,
    command_palette: command_palette::CommandPalette,
    /// A requested context switch awaiting backend validation; drained by
    /// the application layer, which sends the request and commits locally
    /// only after the response succeeds. The origin distinguishes a fresh
    /// user action from passive mismatch reconciliation.
    requested_context: Option<(String, ContextRequestOrigin)>,
    port_forward_start_modal: Option<PortForwardStartModal>,
    next_port_forward_modal_generation: u64,
    pending_port_forward_session_focus: Option<String>,
    port_forward_actions: Vec<PortForwardAction>,
    resource_actions: Vec<ResourceAction>,
    external_shell_availability: ExternalShellAvailability,
    launcher: launcher::LauncherState,
    traffic_history: Vec<k10s_protocol::TrafficSample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExternalShellAvailability {
    #[default]
    Unavailable,
    Available {
        generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalShellTarget {
    pub generation: u64,
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
    pub program: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceAction {
    OpenExternalShell {
        window: WindowId,
        target: ExternalShellTarget,
    },
    Restart {
        window: WindowId,
        target: k10s_protocol::ResourceIdentity,
    },
    RetryPrimary(k10s_protocol::ResourceIdentity),
    RetryRelations(k10s_protocol::ResourceIdentity),
    RetryNamespaceCatalog,
    RetryWindow(WindowId),
    FullResyncWindow(WindowId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortForwardAction {
    OpenStart {
        target: k10s_protocol::PortForwardTarget,
        remote_label: String,
        initial_local_port: u16,
    },
    Start {
        request: k10s_protocol::PortForwardStartRequest,
        generation: PortForwardModalGeneration,
    },
    Stop(String),
    Retry(k10s_protocol::PortForwardSessionId),
    FocusSession(k10s_protocol::PortForwardSessionId),
    CopyAddress(String),
}

impl<I> Default for UiShell<I>
where
    I: resource_window::RowIdentity,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I> UiShell<I>
where
    I: resource_window::RowIdentity,
{
    /// Create a shell around an Overview-only workspace.
    pub fn new() -> Self {
        Self {
            workspace: WorkspaceState::new(),
            infrastructure: infrastructure::InfrastructureUiState::default(),
            resources: resource_window::ResourceUiState::default(),
            yaml: tools::YamlEditors::default(),
            streams: tools::StreamStores::default(),
            dialogs: dialogs::OperationDialogs::default(),
            command_palette: command_palette::CommandPalette::default(),
            requested_context: None,
            port_forward_start_modal: None,
            next_port_forward_modal_generation: 1,
            pending_port_forward_session_focus: None,
            port_forward_actions: Vec::new(),
            resource_actions: Vec::new(),
            external_shell_availability: ExternalShellAvailability::Unavailable,
            launcher: launcher::LauncherState::default(),
            traffic_history: Vec::new(),
        }
    }

    /// Inspect the persistent workspace rendered by this shell.
    pub fn workspace(&self) -> &WorkspaceState<I> {
        &self.workspace
    }

    /// Replace the selected context's bounded transport history for rendering.
    pub fn set_traffic_history(
        &mut self,
        history: impl IntoIterator<Item = k10s_protocol::TrafficSample>,
    ) {
        self.traffic_history.clear();
        self.traffic_history.extend(history);
    }

    /// Whether palette search currently needs its cross-resource projections.
    pub(crate) fn command_palette_open(&self) -> bool {
        self.command_palette.is_open()
    }

    /// Apply a command initiated outside the shell's immediate-mode frame,
    /// such as a navigation-guard resolution dialog.
    pub fn apply_workspace_command(
        &mut self,
        command: WorkspaceCommand<I>,
    ) -> Vec<WorkspaceEvent<I>> {
        let mut events = self.workspace.apply(command);
        events.extend(self.replay_pending_port_forward_session_focus());
        events
    }

    /// Drain the context a switch was requested toward, if any, together
    /// with the request's origin. The caller validates it against the
    /// backend and applies [`WorkspaceCommand::CommitContextSwitch`] only
    /// after success.
    pub fn take_requested_context(&mut self) -> Option<(String, ContextRequestOrigin)> {
        self.requested_context.take()
    }

    pub fn drain_port_forward_actions(&mut self) -> Vec<PortForwardAction> {
        std::mem::take(&mut self.port_forward_actions)
    }

    /// Open the shared start dialog for one validated typed target.
    pub fn open_port_forward_start(
        &mut self,
        target: k10s_protocol::PortForwardTarget,
        remote_label: impl Into<String>,
        initial_local_port: u16,
    ) -> PortForwardModalGeneration {
        let generation = PortForwardModalGeneration(self.next_port_forward_modal_generation);
        self.next_port_forward_modal_generation = self
            .next_port_forward_modal_generation
            .wrapping_add(1)
            .max(1);
        let mut modal = PortForwardStartModal::new(target, remote_label, initial_local_port);
        modal.set_generation(generation);
        self.port_forward_start_modal = Some(modal);
        generation
    }

    /// Current shared start-dialog state.
    #[must_use]
    pub fn port_forward_start_modal(&self) -> Option<&PortForwardStartModal> {
        self.port_forward_start_modal.as_ref()
    }

    /// Mutable shared start-dialog state for application outcomes and tests.
    pub fn port_forward_start_modal_mut(&mut self) -> Option<&mut PortForwardStartModal> {
        self.port_forward_start_modal.as_mut()
    }

    /// Project a recoverable request error into the still-open dialog.
    pub fn port_forward_start_failed(&mut self, safe_message: impl Into<String>) {
        if let Some(modal) = self.port_forward_start_modal.as_mut() {
            modal.pending = false;
            modal.error = Some(safe_message.into());
        }
    }

    /// Project an error only into the dialog that originated the request.
    pub fn port_forward_start_failed_for(
        &mut self,
        generation: PortForwardModalGeneration,
        safe_message: impl Into<String>,
    ) {
        if self
            .port_forward_start_modal
            .as_ref()
            .is_some_and(|modal| modal.generation == generation)
        {
            self.port_forward_start_failed(safe_message);
        }
    }

    /// Dismiss non-persisted start state during a context transition.
    pub fn dismiss_port_forward_start(&mut self) {
        self.port_forward_start_modal = None;
    }

    /// Complete a start (including duplicate success), then open/focus the
    /// singleton manager and its returned authoritative session row.
    pub fn port_forward_start_succeeded(&mut self, session_id: &str) {
        self.port_forward_start_modal = None;
        self.focus_port_forward_session(session_id);
    }

    /// Complete one originating dialog without dismissing a different target
    /// that may have opened while the response was in flight.
    pub fn port_forward_start_succeeded_for(
        &mut self,
        generation: PortForwardModalGeneration,
        session_id: &str,
    ) {
        self.port_forward_start_completed_for(generation);
        self.focus_port_forward_session(session_id);
    }

    pub(crate) fn port_forward_start_completed_for(
        &mut self,
        generation: PortForwardModalGeneration,
    ) {
        if self
            .port_forward_start_modal
            .as_ref()
            .is_some_and(|modal| modal.generation == generation)
        {
            self.port_forward_start_modal = None;
        }
    }

    /// Open/focus the singleton manager and one authoritative session row.
    pub fn focus_port_forward_session(&mut self, session_id: &str) {
        self.pending_port_forward_session_focus = Some(session_id.to_owned());
        let _ = self.replay_pending_port_forward_session_focus();
    }

    fn replay_pending_port_forward_session_focus(&mut self) -> Vec<WorkspaceEvent<I>> {
        let Some(session_id) = self.pending_port_forward_session_focus.clone() else {
            return Vec::new();
        };
        let events = self.workspace.apply(WorkspaceCommand::ActivateLauncherItem(
            crate::workspace::LauncherItem::PortForwards,
        ));
        let window = events.iter().find_map(|event| match event {
            WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) => Some(*id),
            _ => None,
        });
        if let Some(window) = window {
            self.workspace
                .apply(WorkspaceCommand::FocusPortForwardSession(
                    window, session_id,
                ));
            self.pending_port_forward_session_focus = None;
        }
        events
    }

    pub fn drain_resource_actions(&mut self) -> Vec<ResourceAction> {
        std::mem::take(&mut self.resource_actions)
    }

    pub fn set_external_shell_availability(&mut self, availability: ExternalShellAvailability) {
        self.external_shell_availability = availability;
    }

    #[must_use]
    pub fn external_shell_availability(&self) -> ExternalShellAvailability {
        self.external_shell_availability
    }

    /// Drain the protocol actions queued by YAML editors during rendering.
    pub fn drain_yaml_actions(&mut self) -> Vec<(WindowId, tools::YamlAction)> {
        self.yaml.drain_actions()
    }

    /// Mutable access to the per-window guarded YAML editors, so the
    /// application layer can feed backend outcomes into them.
    pub fn yaml_editors_mut(&mut self) -> &mut tools::YamlEditors {
        &mut self.yaml
    }

    /// Mutable access to the connected stream tool stores.
    pub fn stream_stores_mut(&mut self) -> &mut tools::StreamStores {
        &mut self.streams
    }

    /// Read the connected stream stores for semantic adapters and tests.
    #[must_use]
    pub fn stream_stores(&self) -> &tools::StreamStores {
        &self.streams
    }

    /// Mutable access to the open operation dialogs.
    pub fn dialogs_mut(&mut self) -> &mut dialogs::OperationDialogs {
        &mut self.dialogs
    }

    /// Read operation dialogs without changing submission state.
    #[must_use]
    pub fn dialogs(&self) -> &dialogs::OperationDialogs {
        &self.dialogs
    }

    /// Drain every queued operation dialog action.
    pub fn drain_dialog_actions(&mut self) -> Vec<(WindowId, dialogs::DialogAction)> {
        self.dialogs.drain_actions()
    }

    /// Drain every queued log-view protocol action.
    pub fn drain_log_actions(&mut self) -> Vec<(WindowId, tools::LogsAction)> {
        self.streams.logs.drain_actions()
    }

    /// Render one frame. All mutations are queued while immutable workspace
    /// state is being rendered, then applied after every panel and window.
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[String],
        selected_context: &mut Option<String>,
    ) {
        self.show_with_infrastructure(ui, connection, contexts, selected_context, None);
    }

    /// Render one frame with a protocol-owned infrastructure response.
    /// Returns whether either Refresh control was activated.
    pub fn show_with_infrastructure(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[String],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
    ) -> bool {
        let load = if response.is_some() {
            InfrastructureLoad::Available
        } else {
            InfrastructureLoad::Loading
        };
        self.show_with_infrastructure_load(
            ui,
            connection,
            contexts,
            selected_context,
            response,
            load,
        )
    }

    /// Render one frame with an explicit infrastructure capability state.
    pub fn show_with_infrastructure_load(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[String],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
        load: InfrastructureLoad,
    ) -> bool {
        let feed = resource_window::ResourceFeed::default();
        self.show_with_resources_load(
            ui,
            connection,
            contexts,
            selected_context,
            response,
            &feed,
            load,
        )
    }

    /// Render one frame with a protocol-owned infrastructure response and
    /// the connected resource projections for every workload window.
    /// Returns whether either Refresh control was activated.
    pub fn show_with_resources(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[String],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
        feed: &resource_window::ResourceFeed,
    ) -> bool {
        let load = if response.is_some() {
            InfrastructureLoad::Available
        } else {
            InfrastructureLoad::Loading
        };
        self.show_with_resources_load(
            ui,
            connection,
            contexts,
            selected_context,
            response,
            feed,
            load,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn show_with_resources_load(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[String],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
        feed: &resource_window::ResourceFeed,
        load: InfrastructureLoad,
    ) -> bool {
        let contexts = contexts
            .iter()
            .map(|name| k10s_protocol::Context {
                name: name.clone(),
                cluster: String::new(),
                namespace: None,
                is_current: selected_context.as_deref() == Some(name.as_str()),
                availability: k10s_protocol::ContextAvailability::Available,
                unavailable_reason: None,
            })
            .collect::<Vec<_>>();
        self.show_with_contexts_and_resources_load(
            ui,
            connection,
            &contexts,
            selected_context,
            response,
            feed,
            load,
        )
    }

    /// Render with authoritative context availability from Bootstrap.
    pub fn show_with_contexts_and_resources(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[k10s_protocol::Context],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
        feed: &resource_window::ResourceFeed,
    ) -> bool {
        let load = if response.is_some() {
            InfrastructureLoad::Available
        } else {
            InfrastructureLoad::Loading
        };
        self.show_with_contexts_and_resources_load(
            ui,
            connection,
            contexts,
            selected_context,
            response,
            feed,
            load,
        )
    }

    /// Render with authoritative contexts and explicit infrastructure state.
    #[allow(clippy::too_many_arguments)]
    pub fn show_with_contexts_and_resources_load(
        &mut self,
        ui: &mut egui::Ui,
        connection: ConnectionState,
        contexts: &[k10s_protocol::Context],
        selected_context: &mut Option<String>,
        response: Option<&k10s_protocol::InfrastructureResponse>,
        feed: &resource_window::ResourceFeed,
        load: InfrastructureLoad,
    ) -> bool {
        theme::apply(ui.ctx());
        self.command_palette.handle_global_shortcut(ui.ctx());

        let mut queued = Vec::<WorkspaceCommand<I>>::new();
        let selected = selected_context
            .as_deref()
            .filter(|selected| {
                contexts.is_empty()
                    || contexts.iter().any(|context| {
                        context.name == *selected
                            && context.availability
                                != k10s_protocol::ContextAvailability::Unavailable
                    })
            })
            .or_else(|| {
                contexts
                    .iter()
                    .find(|context| {
                        context.availability != k10s_protocol::ContextAvailability::Unavailable
                    })
                    .map(|context| context.name.as_str())
            })
            .map(str::to_owned);

        let mut context_change = None;
        let selected_namespace = selected.as_deref().and_then(|name| {
            contexts
                .iter()
                .find(|context| context.name == name)
                .and_then(|context| context.namespace.as_deref())
        });
        let selected_response =
            response.filter(|response| Some(response.context.as_str()) == selected.as_deref());
        let mut refresh_requested = false;
        taskbar::shortcuts(ui.ctx(), &self.workspace, &mut queued);
        egui::Panel::top("k10s.top_bar")
            .resizable(false)
            .exact_size(theme::TOP_BAR_HEIGHT)
            .frame(theme::top_bar_frame())
            .show(ui, |ui| {
                let action = top_bar::show(
                    ui,
                    connection,
                    contexts,
                    selected.as_deref(),
                    self.workspace.free_window_resizing(),
                    &self.traffic_history,
                );
                context_change = action.context_change;
                refresh_requested |= action.refresh;
                if action.toggle_free_window_resizing {
                    queued.push(WorkspaceCommand::ToggleFreeWindowResizing);
                }
                let canvas = ui.ctx().content_rect();
                let canvas_size = [
                    canvas.width() - theme::LAUNCHER_WIDTH,
                    canvas.height() - theme::TOP_BAR_HEIGHT - theme::TASKBAR_HEIGHT,
                ];
                if let Some(layout) = action.layout {
                    queued.push(match layout {
                        top_bar::LayoutAction::Tile => WorkspaceCommand::Tile(canvas_size),
                        top_bar::LayoutAction::Cascade => WorkspaceCommand::Cascade(canvas_size),
                        top_bar::LayoutAction::Focus => WorkspaceCommand::ToggleFocus(canvas_size),
                    });
                }
            });

        egui::Panel::bottom("k10s.taskbar")
            .resizable(false)
            .exact_size(theme::TASKBAR_HEIGHT)
            .frame(theme::taskbar_frame())
            .show(ui, |ui| {
                taskbar::show(ui, &self.workspace, connection, &mut queued);
            });

        egui::Panel::left("k10s.launcher")
            .resizable(false)
            .exact_size(theme::LAUNCHER_WIDTH)
            .frame(theme::launcher_frame())
            .show(ui, |ui| {
                launcher::show(
                    ui,
                    &self.workspace,
                    selected_response,
                    load,
                    &mut self.launcher,
                    &mut queued,
                );
            });

        let mut resources = std::mem::take(&mut self.resources);
        ui.ctx().data_mut(|data| {
            data.insert_temp(
                egui::Id::new("k10s.external-shell-availability"),
                self.external_shell_availability,
            );
        });
        egui::CentralPanel::default()
            .frame(theme::canvas_frame())
            .show(ui, |ui| {
                refresh_requested |= window::show_canvas(
                    ui,
                    &self.workspace,
                    &mut self.infrastructure,
                    &mut resources,
                    &mut self.yaml,
                    &mut self.streams,
                    &mut self.dialogs,
                    selected_response,
                    load,
                    feed,
                    selected_namespace,
                    connection,
                    &mut self.resource_actions,
                    &mut queued,
                );
            });
        resources.retain(|id| self.workspace.window(id).is_some());
        self.resources = resources;
        self.yaml.retain(|id| self.workspace.window(id).is_some());
        self.streams
            .retain(|id| self.workspace.window(id).is_some());
        let live_windows: Vec<_> = self.workspace.windows().iter().map(|w| w.id).collect();
        self.dialogs.retain(|id| live_windows.contains(&id));
        self.dialogs
            .show(ui, connection == ConnectionState::Connected, |_, target| {
                let primary_loaded = match feed.primary_details.get(target) {
                    Some(PrimaryDetailState::Loaded(_)) => true,
                    Some(PrimaryDetailState::Loading | PrimaryDetailState::Failed(_)) => false,
                    None => feed.details.contains_key(target),
                };
                primary_loaded
                    && feed
                        .detail_authority
                        .get(target)
                        .is_some_and(resource_window::DetailAuthority::mutations_allowed)
            });

        if let Some((action, new_window)) = self.command_palette.show(ui.ctx(), contexts, feed) {
            refresh_requested |= self.activate_palette_action(ui.ctx(), action, new_window);
        }

        let port_forward_unavailable = self.port_forward_start_modal.as_ref().and_then(|modal| {
            port_forward::port_forward_start_authorization(feed, &modal.target).err()
        });
        port_forward::show(
            ui.ctx(),
            &mut self.port_forward_start_modal,
            &mut self.port_forward_actions,
            port_forward_unavailable,
        );

        let context_change = context_change
            .map(|context| (context, ContextRequestOrigin::Explicit))
            .or_else(|| {
                selected
                    .filter(|context| self.workspace.context() != context.as_str())
                    .map(|context| (context, ContextRequestOrigin::Reconcile))
            });
        if let Some((context, origin)) = context_change {
            // The request only stages: the application layer validates it
            // against the backend and commits the workspace transition after
            // the response succeeds.
            self.requested_context = Some((context, origin));
        }

        if !queued.is_empty() {
            for command in queued {
                for event in self.workspace.apply(command) {
                    match event {
                        WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) => {
                            ui.ctx().move_to_top(window::layer_id(id));
                        }
                        WorkspaceEvent::ContextSwitched { to } => {
                            *selected_context = Some(to);
                        }
                        // A guard-resolved switch request surfaces here when
                        // a queued command finally executes; route it through
                        // the same staged path as direct requests. Resolving
                        // a guard is a fresh user decision, so the origin is
                        // explicit.
                        WorkspaceEvent::ContextSwitchRequested { to } => {
                            self.requested_context = Some((to, ContextRequestOrigin::Explicit));
                        }
                        WorkspaceEvent::PortForwardStartRequested {
                            target,
                            remote_label,
                            initial_local_port,
                        } => {
                            self.port_forward_actions
                                .push(PortForwardAction::OpenStart {
                                    target,
                                    remote_label,
                                    initial_local_port,
                                });
                        }
                        WorkspaceEvent::PortForwardStopRequested(id) => {
                            self.port_forward_actions.push(PortForwardAction::Stop(id));
                        }
                        WorkspaceEvent::Closed(_)
                        | WorkspaceEvent::Blocked(_)
                        | WorkspaceEvent::YamlOwnerInUse { .. } => {}
                    }
                }
            }
            ui.ctx().request_repaint();
        }
        for event in self.replay_pending_port_forward_session_focus() {
            if let WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) = event {
                ui.ctx().move_to_top(window::layer_id(id));
            }
        }
        refresh_requested
    }

    fn activate_palette_action(
        &mut self,
        ctx: &egui::Context,
        action: command_palette::PaletteAction,
        new_window: bool,
    ) -> bool {
        use crate::workspace::{NamespaceScope, WindowContent, WindowKind};
        use command_palette::PaletteAction;

        let mut commands = Vec::new();
        match action {
            PaletteAction::Refresh => return true,
            PaletteAction::Context(to) => {
                self.requested_context = Some((to, ContextRequestOrigin::Explicit));
                return false;
            }
            PaletteAction::Namespace(namespace) => {
                let active = self
                    .workspace
                    .windows()
                    .iter()
                    .filter(|window| {
                        matches!(
                            window.content,
                            WindowContent::Resource(_) | WindowContent::Services(_)
                        )
                    })
                    .max_by_key(|window| window.z)
                    .map(|window| window.id);
                if let Some(window) = active {
                    commands.push(WorkspaceCommand::SetNamespaceScope(
                        window,
                        NamespaceScope::Namespace(namespace),
                    ));
                } else {
                    let events = self.workspace.apply(WorkspaceCommand::ActivateLauncherItem(
                        crate::workspace::LauncherItem::Workload(
                            crate::workspace::WorkloadKind::Pods,
                        ),
                    ));
                    if let Some(id) = events.iter().find_map(|event| match event {
                        WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) => Some(*id),
                        _ => None,
                    }) {
                        self.workspace.apply(WorkspaceCommand::SetNamespaceScope(
                            id,
                            NamespaceScope::Namespace(namespace),
                        ));
                        ctx.move_to_top(window::layer_id(id));
                    }
                    return false;
                }
            }
            PaletteAction::List(item) => {
                commands.push(if new_window {
                    WorkspaceCommand::AddListInstance(item)
                } else {
                    WorkspaceCommand::ActivateLauncherItem(item)
                });
            }
            PaletteAction::Resource(identity, jump) => {
                let workspace_identity = I::from_row_identity(&identity);
                let tab = command_palette::tab_for_jump(jump);
                if new_window {
                    let events = self
                        .workspace
                        .apply(WorkspaceCommand::OpenDedicatedDetail(workspace_identity));
                    if let Some(id) = events.iter().find_map(|event| match event {
                        WorkspaceEvent::Opened(id) => Some(*id),
                        _ => None,
                    }) {
                        self.workspace
                            .apply(WorkspaceCommand::SetActiveTab(id, tab));
                        ctx.move_to_top(window::layer_id(id));
                    }
                    return false;
                }

                let target_kind = match identity.gvk.kind.as_str() {
                    "Pod" => Some(WindowKind::Workload(crate::workspace::WorkloadKind::Pods)),
                    "Deployment" => Some(WindowKind::Workload(
                        crate::workspace::WorkloadKind::Deployments,
                    )),
                    "Service" => Some(WindowKind::Services),
                    _ => None,
                };
                let existing = target_kind.and_then(|kind| {
                    self.workspace
                        .windows()
                        .iter()
                        .filter(|window| window.kind == kind)
                        .max_by_key(|window| window.z)
                        .map(|window| window.id)
                });
                let window_id = if let Some(id) = existing {
                    self.workspace.apply(WorkspaceCommand::FocusWindow(id));
                    Some(id)
                } else if let Some(kind) = target_kind {
                    let item = match kind {
                        WindowKind::Workload(kind) => {
                            crate::workspace::LauncherItem::Workload(kind)
                        }
                        WindowKind::Services => crate::workspace::LauncherItem::Services,
                        _ => unreachable!(),
                    };
                    self.workspace
                        .apply(WorkspaceCommand::ActivateLauncherItem(item))
                        .iter()
                        .find_map(|event| match event {
                            WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) => Some(*id),
                            _ => None,
                        })
                } else {
                    self.workspace
                        .apply(WorkspaceCommand::OpenDedicatedDetail(
                            workspace_identity.clone(),
                        ))
                        .iter()
                        .find_map(|event| match event {
                            WorkspaceEvent::Opened(id) => Some(*id),
                            _ => None,
                        })
                };
                if let Some(id) = window_id {
                    if matches!(
                        self.workspace.window(id).map(|window| window.kind),
                        Some(WindowKind::Detail)
                    ) {
                        self.workspace
                            .apply(WorkspaceCommand::SetActiveTab(id, tab));
                    } else {
                        self.workspace
                            .apply(WorkspaceCommand::SelectRow(id, workspace_identity));
                        self.workspace
                            .apply(WorkspaceCommand::SetActiveTab(id, tab));
                    }
                    ctx.move_to_top(window::layer_id(id));
                }
                return false;
            }
        }

        for command in commands {
            for event in self.workspace.apply(command) {
                if let WorkspaceEvent::Opened(id) | WorkspaceEvent::Focused(id) = event {
                    ctx.move_to_top(window::layer_id(id));
                }
            }
        }
        false
    }
}
