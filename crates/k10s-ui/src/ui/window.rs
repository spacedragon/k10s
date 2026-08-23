//! Standard egui windows placed on the free canvas to the launcher's right.

use egui::{Id, LayerId, Order, Pos2, Rect, Vec2};

use crate::workspace::{
    Window, WindowContent, WindowGeom, WindowId, WindowKind, WorkspaceCommand, WorkspaceState,
};

use super::{ConnectionState, infrastructure::InfrastructureUiState, resource_window};

pub(super) fn layer_id(id: WindowId) -> LayerId {
    LayerId::new(Order::Middle, Id::new(("k10s.window", id.0)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn show_canvas<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    infrastructure: &mut InfrastructureUiState,
    resources: &mut resource_window::ResourceUiState,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    feed: &resource_window::ResourceFeed,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: resource_window::RowIdentity,
{
    let canvas = ui.available_rect_before_wrap();
    let navigation_queued = queued.iter().any(|command| {
        matches!(
            command,
            WorkspaceCommand::ActivateLauncherItem(_)
                | WorkspaceCommand::AddWorkloadInstance(_)
                | WorkspaceCommand::FocusWindow(_)
        )
    });
    let mut windows: Vec<_> = workspace.windows().iter().collect();
    windows.sort_by_key(|window| window.z);

    let mut refresh_requested = false;
    for window in windows {
        refresh_requested |= show_window(
            ui,
            canvas,
            window,
            infrastructure,
            resources,
            feed,
            response,
            connection,
            queued,
        );
    }

    if !navigation_queued
        && let Some(top_layer) = ui.ctx().top_layer_id()
        && let Some(top_window) = workspace
            .windows()
            .iter()
            .find(|window| layer_id(window.id) == top_layer)
        && workspace
            .windows()
            .iter()
            .max_by_key(|window| window.z)
            .is_some_and(|window| window.id != top_window.id)
    {
        queued.push(WorkspaceCommand::FocusWindow(top_window.id));
    }
    refresh_requested
}

#[allow(clippy::too_many_arguments)]
fn show_window<I>(
    ui: &mut egui::Ui,
    canvas: Rect,
    state: &Window<I>,
    infrastructure: &mut InfrastructureUiState,
    resources: &mut resource_window::ResourceUiState,
    feed: &resource_window::ResourceFeed,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: resource_window::RowIdentity,
{
    let mut open = true;
    let position = Pos2::new(
        canvas.min.x + state.geometry.position[0],
        canvas.min.y + state.geometry.position[1],
    );
    let min_size = match state.kind {
        WindowKind::Workload(_) | WindowKind::Detail => Vec2::new(640.0, 420.0),
        WindowKind::Overview | WindowKind::Nodes | WindowKind::Storage => Vec2::new(480.0, 320.0),
    };
    let id = layer_id(state.id).id;

    // Workload windows render from a mutable clone of their list state and
    // queue the resulting commands; the workspace stays immutable during
    // rendering.
    let (mut resource_state, detail_state) = match &state.content {
        WindowContent::Resource(resource) => (Some(resource.clone()), None),
        WindowContent::Detail(detail) => (None, Some(detail.clone())),
    };

    let response = egui::Window::new(state.title.as_str())
        .id(id)
        .open(&mut open)
        .movable(true)
        .resizable(true)
        .collapsible(true)
        .default_open(!state.geometry.collapsed)
        .current_pos(position)
        .default_size(state.geometry.size)
        .min_size(min_size)
        .constrain_to(canvas)
        .show(ui.ctx(), |ui| {
            ui.set_min_size(min_size - Vec2::new(24.0, 48.0));
            if let (Some(resource), WindowKind::Workload(kind)) =
                (resource_state.as_mut(), state.kind)
            {
                super::resource_window::show(
                    ui, resources, state.id, kind, resource, feed, connection, queued,
                );
                false
            } else {
                match state.kind {
                    WindowKind::Overview => super::overview::show(ui, response, connection),
                    WindowKind::Nodes => {
                        super::infrastructure::show_nodes(ui, infrastructure, response, connection);
                        false
                    }
                    WindowKind::Storage => {
                        super::infrastructure::show_storage(
                            ui,
                            infrastructure,
                            response,
                            connection,
                        );
                        false
                    }
                    WindowKind::Workload(_) => {
                        unreachable!("workload windows render through resource_window")
                    }
                    WindowKind::Detail => {
                        // Dedicated windows render only their pinned
                        // identity; they never read the integrated
                        // selection of any list window.
                        if let Some(detail) = detail_state.as_ref() {
                            let view = detail
                                .identity
                                .as_row_identity()
                                .and_then(|identity| feed.details.get(identity));
                            super::detail::show(ui, state.id, detail, view, queued);
                        }
                        false
                    }
                }
            }
        });

    if !open {
        queued.push(WorkspaceCommand::CloseWindow(state.id));
        return false;
    }

    let Some(response) = response else {
        return false;
    };
    let collapsed = response.inner.is_none();
    let rect = response.response.rect;
    let geometry = WindowGeom {
        position: [rect.min.x - canvas.min.x, rect.min.y - canvas.min.y],
        size: if collapsed {
            state.geometry.size
        } else {
            [rect.width(), rect.height()]
        },
        collapsed,
    };
    if geometry != state.geometry {
        queued.push(WorkspaceCommand::SetGeometry(state.id, geometry));
    }
    response.inner.unwrap_or(false)
}
