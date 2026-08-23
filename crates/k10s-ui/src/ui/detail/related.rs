//! The related tab: backend-resolved owner-traversal rows grouped by type.
//!
//! Clicking a related row opens a dedicated window pinned to that row's
//! identity; the UI never re-resolves relations itself.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::RelatedGroup;

use crate::workspace::{WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    groups: &[RelatedGroup],
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    if groups.iter().all(|group| group.rows.is_empty()) {
        ui.label("No related resources");
        return;
    }
    ScrollArea::vertical()
        .id_salt(("k10s.detail.related.scroll", window_id.0))
        .show(ui, |ui| {
            for group in groups {
                if group.rows.is_empty() {
                    continue;
                }
                ui.heading(egui::RichText::new(group.title.as_str()).strong());
                for row in &group.rows {
                    let label = format!("{} · {}", row.identity.name, row.summary);
                    let button = ui.button(label.clone());
                    button.widget_info(|| {
                        WidgetInfo::labeled(WidgetType::Button, true, label.clone())
                    });
                    if button.clicked() {
                        queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                            &row.identity,
                        )));
                    }
                }
                ui.separator();
            }
        });
}
