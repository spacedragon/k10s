//! Instance-addressable bottom taskbar and deterministic layout controls.

use crate::workspace::{Window, WindowContent, WindowKind, WorkspaceCommand, WorkspaceState};

use super::{ConnectionState, resource_window::RowIdentity};
use egui::{Button, WidgetInfo, WidgetType};

const TASK_WIDTH: f32 = 172.0;

fn dirty<I>(window: &Window<I>) -> bool {
    match &window.content {
        WindowContent::Resource(state) => state.detail.as_ref().is_some_and(|d| d.yaml.dirty),
        WindowContent::Services(state) => state.detail.as_ref().is_some_and(|d| d.yaml.dirty),
        WindowContent::Detail(detail) => detail.yaml.dirty,
    }
}

fn identity<I: RowIdentity>(window: &Window<I>) -> String {
    if let WindowContent::Detail(detail) = &window.content
        && let Some(id) = detail.identity.as_row_identity()
    {
        return match id.namespace.as_deref() {
            Some(namespace) => format!("{} · {namespace} / {}", id.gvk.kind, id.name),
            None => format!("{} · {}", id.gvk.kind, id.name),
        };
    }
    let scope = match &window.content {
        WindowContent::Resource(state) => Some(&state.namespace_scope),
        WindowContent::Services(state) => Some(&state.namespace_scope),
        WindowContent::Detail(_) => None,
    };
    match (window.kind, scope) {
        (WindowKind::Workload(_), Some(scope)) => {
            crate::workspace::scoped_window_title(&window.title, scope)
        }
        _ => window.title.clone(),
    }
}

fn label<I: RowIdentity>(window: &Window<I>, active: bool, connection: ConnectionState) -> String {
    if matches!(window.content, WindowContent::Detail(_)) {
        return identity(window);
    }
    let mut label = identity(window);
    if active {
        label.push_str(" · ● Active");
    }
    if dirty(window) {
        label.push_str(" · ◆ Unsaved YAML");
    }
    if connection != ConnectionState::Connected {
        label.push_str(" · ↻ Stale data");
    }
    label
}

pub(super) fn shortcuts<I: RowIdentity>(
    ctx: &egui::Context,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let keys = [
        egui::Key::Num1,
        egui::Key::Num2,
        egui::Key::Num3,
        egui::Key::Num4,
        egui::Key::Num5,
        egui::Key::Num6,
        egui::Key::Num7,
        egui::Key::Num8,
        egui::Key::Num9,
    ];
    for (index, key) in keys.into_iter().enumerate() {
        if ctx.input_mut(|input| input.consume_key(egui::Modifiers::ALT, key))
            && let Some(window) = workspace.windows().get(index)
        {
            queued.push(WorkspaceCommand::FocusWindow(window.id));
        }
    }
    if ctx.input_mut(|input| input.consume_key(egui::Modifiers::CTRL, egui::Key::Tab)) {
        queued.push(WorkspaceCommand::CycleWindow);
    }
    if ctx.input_mut(|input| input.consume_key(egui::Modifiers::CTRL, egui::Key::W))
        && let Some(window) = workspace.windows().iter().max_by_key(|window| window.z)
    {
        queued.push(WorkspaceCommand::CloseWindow(window.id));
    }
}

pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.horizontal(|ui| {
        let active = workspace
            .windows()
            .iter()
            .max_by_key(|window| window.z)
            .map(|window| window.id);
        // Base capacity on the viewport rather than the horizontal layout's
        // transient cursor so the final slot is always reserved for overflow.
        let capacity = ((ui.max_rect().width() - TASK_WIDTH) / TASK_WIDTH)
            .floor()
            .max(1.0) as usize;
        for window in workspace.windows().iter().take(capacity) {
            let text = label(window, active == Some(window.id), connection);
            let response = ui.add_sized(
                [TASK_WIDTH, ui.spacing().interact_size.y],
                Button::selectable(active == Some(window.id), &text),
            );
            response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, &text));
            if response.on_hover_text(&text).clicked() {
                queued.push(WorkspaceCommand::FocusWindow(window.id));
            }
        }
        let overflow = workspace.windows().get(capacity..).unwrap_or_default();
        if !overflow.is_empty() {
            egui::ComboBox::from_id_salt("k10s.taskbar.overflow")
                .selected_text(format!("More tasks ({})", overflow.len()))
                .show_ui(ui, |ui| {
                    for window in overflow {
                        if ui
                            .selectable_label(
                                active == Some(window.id),
                                label(window, active == Some(window.id), connection),
                            )
                            .clicked()
                        {
                            queued.push(WorkspaceCommand::FocusWindow(window.id));
                        }
                    }
                });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::WindowId;
    use crate::workspace::{DetailState, WindowGeom};
    use k10s_protocol::{GroupVersionKind, ResourceIdentity};

    #[test]
    fn pinned_task_label_is_identity_only_even_when_state_changes() {
        let mut window = Window {
            id: WindowId(1),
            kind: WindowKind::Detail,
            title: "Detail".into(),
            geometry: WindowGeom::staggered(0, [640.0, 420.0]),
            initial_geometry: false,
            layout_revision: 0,
            z: 2,
            content: WindowContent::Detail(DetailState::new(ResourceIdentity {
                context: "dev".into(),
                gvk: GroupVersionKind {
                    group: "".into(),
                    version: "v1".into(),
                    kind: "Pod".into(),
                },
                namespace: Some("payments".into()),
                name: "api-0".into(),
                uid: "1".into(),
            })),
        };
        assert_eq!(
            label(&window, true, ConnectionState::Failed),
            "Pod · payments / api-0"
        );
        if let WindowContent::Detail(detail) = &mut window.content {
            detail.yaml.dirty = true;
        }
        assert_eq!(
            label(&window, true, ConnectionState::Connecting),
            "Pod · payments / api-0"
        );
    }
}
