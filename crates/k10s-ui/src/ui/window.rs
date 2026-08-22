//! Standard egui windows placed on the free canvas to the launcher's right.

use std::fmt::Debug;
use std::hash::Hash;

use egui::{Id, Pos2, Rect, Vec2};

use crate::workspace::{Window, WindowGeom, WindowKind, WorkspaceCommand, WorkspaceState};

pub(super) fn show_canvas<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let canvas = ui.available_rect_before_wrap();
    let mut windows: Vec<_> = workspace.windows().iter().collect();
    windows.sort_by_key(|window| window.z);

    for window in windows {
        show_window(ui, canvas, window, queued);
    }
}

fn show_window<I>(
    ui: &mut egui::Ui,
    canvas: Rect,
    state: &Window<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
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
    let id = Id::new(("k10s.window", state.id.0));

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
            ui.label(window_placeholder(state.kind));
        });

    if !open {
        queued.push(WorkspaceCommand::CloseWindow(state.id));
        return;
    }

    let Some(response) = response else {
        return;
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
}

fn window_placeholder(kind: WindowKind) -> &'static str {
    match kind {
        WindowKind::Overview => "Cluster overview",
        WindowKind::Nodes => "Node inventory",
        WindowKind::Storage => "Storage inventory",
        WindowKind::Workload(_) => "Resource list",
        WindowKind::Detail => "Resource detail",
    }
}
