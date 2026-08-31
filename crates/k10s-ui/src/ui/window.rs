//! Standard egui windows placed on the free canvas to the launcher's right.

use egui::{Id, LayerId, Order, Pos2, Rect, Stroke, StrokeKind, Vec2, pos2};

use crate::workspace::{
    Window, WindowContent, WindowGeom, WindowId, WindowKind, WorkspaceCommand, WorkspaceState,
};

use super::{
    ConnectionState, InfrastructureLoad, infrastructure::InfrastructureUiState, resource_window,
};

const WINDOW_CHROME_SIZE: Vec2 = Vec2::new(24.0, 48.0);

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
                | WorkspaceCommand::AddListInstance(_)
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
            workspace.free_window_resizing(),
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
    free_window_resizing: bool,
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
    let min_size = Vec2::from(state.kind.min_size());
    let id = layer_id(state.id).id;
    let layout_revision_id = id.with("layout_revision");
    let applied_layout_revision = ui
        .ctx()
        .data_mut(|data| data.get_temp::<u64>(layout_revision_id));
    let apply_layout_size =
        applied_layout_revision.is_some() && applied_layout_revision != Some(state.layout_revision);
    ui.ctx()
        .data_mut(|data| data.insert_temp(layout_revision_id, state.layout_revision));

    // Workload windows render from a mutable clone of their list state and
    // queue the resulting commands; the workspace stays immutable during
    // rendering.
    let (mut resource_state, mut service_state, detail_state) = match &state.content {
        WindowContent::Resource(resource) => (Some(resource.clone()), None, None),
        WindowContent::Services(service) => (None, Some(service.clone()), None),
        WindowContent::Detail(detail) => (None, None, Some(detail.clone())),
    };

    let mut window = egui::Window::new(state.title.as_str())
        .id(id)
        .open(&mut open)
        .movable(true)
        .resizable(true)
        .collapsible(true)
        .default_open(!state.geometry.collapsed)
        .current_pos(position)
        .default_size(state.geometry.size)
        .frame(super::theme::window_frame(focused));
    window = if free_window_resizing {
        window.min_size(Vec2::ZERO).scroll(true)
    } else {
        window.min_size(min_size)
    };
    let layout_fits_canvas = state.geometry.position[0] + state.geometry.size[0] <= canvas.width()
        && state.geometry.position[1] + state.geometry.size[1] <= canvas.height();
    if state.layout_revision == 0 || layout_fits_canvas {
        window = window.constrain_to(canvas);
    } else {
        window = window.constrain(false);
    }
    if apply_layout_size {
        // Clamp egui's persisted resize state for this frame. The next frame
        // is movable/resizable again, so layouts never disable manual edits.
        window = window.fixed_pos(position).fixed_size(state.geometry.size);
    }
    let response = window.show(ui.ctx(), |ui| {
        if !free_window_resizing {
            ui.set_min_size(min_size - WINDOW_CHROME_SIZE);
        }
        if let (Some(resource), WindowKind::Workload(kind)) = (resource_state.as_mut(), state.kind)
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
                    if let Some(detail) = detail_state.as_ref()
                        && let Some(presentation) =
                            super::detail::presentation::DetailPresentationInput::from_feed(
                                detail, feed, false, None, true,
                            )
                    {
                        super::detail::show(
                            ui,
                            state.id,
                            detail,
                            &presentation,
                            false,
                            false,
                            yaml,
                            streams,
                            dialogs,
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
    // A layout resize updates egui's persisted area/resize state during this
    // frame. Do not let the response from that transition frame overwrite
    // the command's target geometry before egui presents it next frame.
    if geometry != state.geometry && !apply_layout_size {
        queued.push(WorkspaceCommand::SetGeometry(state.id, geometry));
    }
    response.inner.unwrap_or(false)
}
