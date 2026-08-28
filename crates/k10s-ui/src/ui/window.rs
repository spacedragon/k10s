//! Standard egui windows placed on the free canvas to the launcher's right.

use egui::{Id, LayerId, Order, Pos2, Rect, Stroke, StrokeKind, Vec2, pos2};

use crate::workspace::{
    Window, WindowContent, WindowGeom, WindowId, WindowKind, WorkspaceCommand, WorkspaceState,
};

use super::{
    ConnectionState, InfrastructureLoad, infrastructure::InfrastructureUiState, resource_window,
};

pub(super) fn layer_id(id: WindowId) -> LayerId {
    LayerId::new(Order::Middle, Id::new(("k10s.window", id.0)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn show_canvas<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    infrastructure: &mut InfrastructureUiState,
    resources: &mut resource_window::ResourceUiState,
    yaml: &mut super::tools::YamlEditors,
    streams: &mut super::tools::StreamStores,
    dialogs: &mut super::dialogs::OperationDialogs,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    load: InfrastructureLoad,
    feed: &resource_window::ResourceFeed,
    context_namespace: Option<&str>,
    connection: ConnectionState,
    resource_actions: &mut Vec<super::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: resource_window::RowIdentity,
{
    let canvas = ui.available_rect_before_wrap();
    paint_canvas(ui, canvas);
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
    let focused = windows.last().map(|window| window.id);

    let mut refresh_requested = false;
    for window in windows {
        refresh_requested |= show_window(
            ui,
            canvas,
            window,
            focused == Some(window.id),
            infrastructure,
            resources,
            yaml,
            streams,
            dialogs,
            feed,
            response,
            load,
            context_namespace,
            connection,
            resource_actions,
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

fn paint_canvas(ui: &egui::Ui, canvas: Rect) {
    let spacing = 20.0;
    let color = super::theme::BORDER.gamma_multiply(0.42);
    let start_x = (canvas.left() / spacing).ceil() * spacing;
    let start_y = (canvas.top() / spacing).ceil() * spacing;
    let mut y = start_y;
    while y <= canvas.bottom() {
        let mut x = start_x;
        while x <= canvas.right() {
            ui.painter().circle_filled(pos2(x, y), 0.75, color);
            x += spacing;
        }
        y += spacing;
    }
}

#[allow(clippy::too_many_arguments)]
fn show_window<I>(
    ui: &mut egui::Ui,
    canvas: Rect,
    state: &Window<I>,
    focused: bool,
    infrastructure: &mut InfrastructureUiState,
    resources: &mut resource_window::ResourceUiState,
    yaml: &mut super::tools::YamlEditors,
    streams: &mut super::tools::StreamStores,
    dialogs: &mut super::dialogs::OperationDialogs,
    feed: &resource_window::ResourceFeed,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    load: InfrastructureLoad,
    context_namespace: Option<&str>,
    connection: ConnectionState,
    resource_actions: &mut Vec<super::ResourceAction>,
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
        WindowKind::Overview | WindowKind::Nodes | WindowKind::Storage | WindowKind::Services => {
            Vec2::new(480.0, 320.0)
        }
    };
    let id = layer_id(state.id).id;

    // Workload windows render from a mutable clone of their list state and
    // queue the resulting commands; the workspace stays immutable during
    // rendering.
    let (mut resource_state, mut service_state, detail_state) = match &state.content {
        WindowContent::Resource(resource) => (Some(resource.clone()), None, None),
        WindowContent::Services(service) => (None, Some(service.clone()), None),
        WindowContent::Detail(detail) => (None, None, Some(detail.clone())),
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
                    ui,
                    resources,
                    state.id,
                    kind,
                    resource,
                    yaml,
                    streams,
                    dialogs,
                    feed,
                    context_namespace,
                    connection,
                    resource_actions,
                    queued,
                );
                false
            } else {
                match state.kind {
                    WindowKind::Overview => super::overview::show(ui, response, load, connection),
                    WindowKind::Nodes => {
                        super::infrastructure::show_nodes(
                            ui,
                            infrastructure,
                            response,
                            load,
                            connection,
                        );
                        false
                    }
                    WindowKind::Storage => {
                        super::infrastructure::show_storage(
                            ui,
                            infrastructure,
                            response,
                            load,
                            connection,
                        );
                        false
                    }
                    WindowKind::Services => {
                        if let Some(service) = service_state.as_mut() {
                            super::service_window::show(
                                ui,
                                resources,
                                state.id,
                                service,
                                feed,
                                context_namespace,
                                connection,
                                yaml,
                                streams,
                                dialogs,
                                resource_actions,
                                queued,
                            )
                        } else {
                            false
                        }
                    }
                    WindowKind::Workload(_) => {
                        unreachable!("workload windows render through resource_window")
                    }
                    WindowKind::Detail => {
                        // Dedicated windows render only their pinned
                        // identity; they never read the integrated
                        // selection of any list window.
                        if let Some(detail) = detail_state.as_ref() {
                            let identity = detail.identity.as_row_identity();
                            let primary_state =
                                identity.and_then(|identity| feed.primary_details.get(identity));
                            let view = identity.and_then(|identity| feed.details.get(identity));
                            super::detail::show(
                                ui,
                                state.id,
                                detail,
                                primary_state,
                                view,
                                false,
                                yaml,
                                streams,
                                dialogs,
                                feed,
                                None,
                                resource_actions,
                                queued,
                            );
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
    let focus_stroke = if focused {
        Stroke::new(2.0, super::theme::ACCENT)
    } else {
        Stroke::new(1.0, super::theme::BORDER)
    };
    ui.ctx().layer_painter(layer_id(state.id)).rect_stroke(
        rect,
        4.0,
        focus_stroke,
        StrokeKind::Inside,
    );
    if focused {
        let title_rule = Rect::from_min_max(
            pos2(rect.left() + 8.0, rect.top() + 24.0),
            pos2(rect.right() - 8.0, rect.top() + 26.0),
        );
        ui.ctx().layer_painter(layer_id(state.id)).rect_filled(
            title_rule,
            1.0,
            super::theme::ACCENT,
        );
    }
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
