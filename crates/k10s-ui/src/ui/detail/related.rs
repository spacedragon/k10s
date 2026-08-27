//! The related tab: backend-resolved owner-traversal rows grouped by type.
//!
//! Clicking a related row opens a dedicated window pinned to that row's
//! identity; the UI never re-resolves relations itself.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::ResourceIdentity;

use crate::workspace::{WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &ResourceIdentity,
    state: Option<&crate::ui::RelationState>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    let groups = match state {
        None | Some(crate::ui::RelationState::NotRequested) => {
            ui.label("Related resources not requested");
            return;
        }
        Some(crate::ui::RelationState::Loading) => {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new());
                ui.label("Loading related resources");
            });
            return;
        }
        Some(crate::ui::RelationState::Failed(error)) => {
            ui.label(format!(
                "Related resources unavailable: {}",
                error.message()
            ));
            if ui.button("Retry related resources").clicked() {
                resource_actions.push(crate::ui::ResourceAction::RetryRelations(identity.clone()));
            }
            return;
        }
        Some(crate::ui::RelationState::Loaded {
            response,
            refreshing,
            refresh_error,
            ..
        }) => {
            if *refreshing {
                ui.label("Refreshing related resources");
            }
            if let Some(error) = refresh_error {
                ui.label(format!("Refresh failed: {}", error.message()));
                if ui.button("Retry related resources").clicked() {
                    resource_actions
                        .push(crate::ui::ResourceAction::RetryRelations(identity.clone()));
                }
            }
            response.groups.as_slice()
        }
    };
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
