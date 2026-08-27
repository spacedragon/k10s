//! The fixed application shell rendered around the command-driven workspace.

mod detail;
pub mod dialogs;
mod infrastructure;
mod launcher;
mod overview;
mod resource_table;
mod resource_window;
mod service_window;
mod split;
mod theme;
pub mod tools;
mod top_bar;
mod window;

pub use resource_window::{
    NamespaceCatalogState, PrimaryDetailState, RelationState, ResourceFeed, RowIdentity,
    SafeUiError,
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
    /// A requested context switch awaiting backend validation; drained by
    /// the application layer, which sends the request and commits locally
    /// only after the response succeeds. The origin distinguishes a fresh
    /// user action from passive mismatch reconciliation.
    requested_context: Option<(String, ContextRequestOrigin)>,
    port_forward_actions: Vec<PortForwardAction<I>>,
    resource_actions: Vec<ResourceAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceAction {
    RetryPrimary(k10s_protocol::ResourceIdentity),
    RetryRelations(k10s_protocol::ResourceIdentity),
    RetryNamespaceCatalog,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PortForwardAction<I> {
    Start {
        service: I,
        port: k10s_protocol::PortForwardPortSelector,
        local_port: u16,
    },
    Stop(String),
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
            requested_context: None,
            port_forward_actions: Vec::new(),
            resource_actions: Vec::new(),
        }
    }

    /// Inspect the persistent workspace rendered by this shell.
    pub fn workspace(&self) -> &WorkspaceState<I> {
        &self.workspace
    }

    /// Apply a command initiated outside the shell's immediate-mode frame,
    /// such as a navigation-guard resolution dialog.
    pub fn apply_workspace_command(
        &mut self,
        command: WorkspaceCommand<I>,
    ) -> Vec<WorkspaceEvent<I>> {
        self.workspace.apply(command)
    }

    /// Drain the context a switch was requested toward, if any, together
    /// with the request's origin. The caller validates it against the
    /// backend and applies [`WorkspaceCommand::CommitContextSwitch`] only
    /// after success.
    pub fn take_requested_context(&mut self) -> Option<(String, ContextRequestOrigin)> {
        self.requested_context.take()
    }

    pub fn drain_port_forward_actions(&mut self) -> Vec<PortForwardAction<I>> {
        std::mem::take(&mut self.port_forward_actions)
    }

    pub fn drain_resource_actions(&mut self) -> Vec<ResourceAction> {
        std::mem::take(&mut self.resource_actions)
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

    /// Drain every queued explicit shell-connect request.
    pub fn drain_shell_connects(&mut self) -> Vec<(WindowId, k10s_protocol::StreamTarget)> {
        self.streams.shells.drain_connects()
    }

    /// Drain every queued stdin/resize action of live terminals.
    pub fn drain_shell_actions(&mut self) -> Vec<(WindowId, tools::ShellAction)> {
        self.streams.shells.drain_actions()
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
        let mut refresh_requested = false;
        egui::Panel::top("k10s.top_bar")
            .resizable(false)
            .show(ui, |ui| {
                let action = top_bar::show(
                    ui,
                    connection,
                    contexts,
                    selected.as_deref(),
                    self.workspace.free_window_resizing(),
                );
                context_change = action.context_change;
                refresh_requested |= action.refresh;
                if action.toggle_free_window_resizing {
                    queued.push(WorkspaceCommand::ToggleFreeWindowResizing);
                }
            });

        egui::Panel::left("k10s.launcher")
            .resizable(false)
            .exact_size(176.0)
            .show(ui, |ui| {
                launcher::show(ui, &self.workspace, &mut queued);
            });

        let mut resources = std::mem::take(&mut self.resources);
        egui::CentralPanel::default().show(ui, |ui| {
            let response =
                response.filter(|response| Some(response.context.as_str()) == selected.as_deref());
            refresh_requested |= window::show_canvas(
                ui,
                &self.workspace,
                &mut self.infrastructure,
                &mut resources,
                &mut self.yaml,
                &mut self.streams,
                &mut self.dialogs,
                response,
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
            .show(ui, connection == ConnectionState::Connected);

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
                            service,
                            port,
                            local_port,
                        } => self.port_forward_actions.push(PortForwardAction::Start {
                            service,
                            port,
                            local_port,
                        }),
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
        refresh_requested
    }
}
