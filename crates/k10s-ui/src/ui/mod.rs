//! The fixed application shell rendered around the command-driven workspace.

mod infrastructure;
mod launcher;
mod overview;
mod resource_table;
mod resource_window;
mod split;
mod theme;
mod top_bar;
mod window;

pub use resource_window::ResourceFeed;

use std::fmt::Debug;

use crate::workspace::{WorkspaceCommand, WorkspaceEvent, WorkspaceState};

/// User-visible state of the shared control connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Failed,
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

/// Persistent UI shell state. The existing [`WorkspaceState`] remains the
/// only source of truth for window instances, geometry, focus, and content.
#[derive(Debug)]
pub struct UiShell<I> {
    workspace: WorkspaceState<I>,
    infrastructure: infrastructure::InfrastructureUiState,
    resources: resource_window::ResourceUiState,
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
        let feed = resource_window::ResourceFeed::default();
        self.show_with_resources(ui, connection, contexts, selected_context, response, &feed)
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
        theme::apply(ui.ctx());

        let mut queued = Vec::<WorkspaceCommand<I>>::new();
        let selected = selected_context
            .as_deref()
            .filter(|selected| {
                contexts.is_empty() || contexts.iter().any(|context| context == selected)
            })
            .or_else(|| contexts.first().map(String::as_str))
            .map(str::to_owned);

        let mut context_change = None;
        let mut refresh_requested = false;
        egui::Panel::top("k10s.top_bar")
            .resizable(false)
            .show(ui, |ui| {
                let action = top_bar::show(ui, connection, contexts, selected.as_deref());
                context_change = action.context_change;
                refresh_requested |= action.refresh;
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
                response,
                feed,
                connection,
                &mut queued,
            );
        });
        resources.retain(|id| self.workspace.window(id).is_some());
        self.resources = resources;

        let context_change = context_change
            .or_else(|| selected.filter(|context| self.workspace.context() != context.as_str()));
        if let Some(context) = context_change {
            queued.push(WorkspaceCommand::ContextSwitch { to: context });
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
