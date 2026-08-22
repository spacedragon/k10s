//! Standard egui windows placed on the free canvas to the launcher's right.

use std::fmt::Debug;
use std::hash::Hash;

use egui::{Id, LayerId, Order, Pos2, Rect, Vec2};

use crate::workspace::{
    Window, WindowGeom, WindowId, WindowKind, WorkspaceCommand, WorkspaceState,
};

use super::{ConnectionState, infrastructure::InfrastructureUiState};

pub(super) fn layer_id(id: WindowId) -> LayerId {
    LayerId::new(Order::Middle, Id::new(("k10s.window", id.0)))
}

pub(super) fn show_canvas<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    infrastructure: &mut InfrastructureUiState,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: Clone + Eq + Hash + Debug,
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

fn show_window<I>(
    ui: &mut egui::Ui,
    canvas: Rect,
    state: &Window<I>,
    infrastructure: &mut InfrastructureUiState,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: Clone,
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
            match state.kind {
                WindowKind::Overview => super::overview::show(ui, response, connection),
                WindowKind::Nodes => {
                    super::infrastructure::show_nodes(ui, infrastructure, response);
                    false
                }
                WindowKind::Storage => {
                    super::infrastructure::show_storage(ui, infrastructure, response);
                    false
                }
                WindowKind::Workload(_) | WindowKind::Detail => {
                    ui.label(window_placeholder(state.kind));
                    false
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

fn window_placeholder(kind: WindowKind) -> &'static str {
    match kind {
        WindowKind::Overview | WindowKind::Nodes | WindowKind::Storage => {
            unreachable!("infrastructure windows have concrete renderers")
        }
        WindowKind::Workload(_) => "Resource list",
        WindowKind::Detail => "Resource detail",
    }
}
