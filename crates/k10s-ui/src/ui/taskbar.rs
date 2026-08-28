//! Compact window switcher anchored below the free canvas.

use std::fmt::Debug;
use std::hash::Hash;

use egui::{RichText, WidgetInfo, WidgetType};

use crate::workspace::{WorkspaceCommand, WorkspaceState};

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let focused = workspace
        .windows()
        .iter()
        .max_by_key(|window| window.z)
        .map(|window| window.id);
    ui.horizontal(|ui| {
        ui.label(
            RichText::new("WINDOWS")
                .small()
                .color(super::theme::MUTED_TEXT),
        );
        for window in workspace.windows() {
            let is_focused = focused == Some(window.id);
            ui.push_id(("k10s.taskbar.window", window.id.0), |ui| {
                let visible = if is_focused {
                    format!("● {}", window.title)
                } else {
                    format!("○ {}", window.title)
                };
                let accessible = if is_focused {
                    format!("{} window, focused", window.title)
                } else {
                    format!("Focus {} window", window.title)
                };
                let button = ui.selectable_label(is_focused, visible);
                button.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Button, true, accessible.clone())
                });
                if button.on_hover_text(accessible).clicked() && !is_focused {
                    queued.push(WorkspaceCommand::FocusWindow(window.id));
                }
            });
        }
    });
}
