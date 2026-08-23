//! The Events tab: backend-resolved deterministic event rows.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::EventRow;

use crate::workspace::WindowId;

pub(super) fn show(ui: &mut egui::Ui, window_id: WindowId, events: &[EventRow]) {
    if events.is_empty() {
        ui.label("No events reported");
        return;
    }
    ScrollArea::vertical()
        .id_salt(("k10s.detail.events.scroll", window_id.0))
        .show(ui, |ui| {
            for event in events {
                ui.horizontal(|ui| {
                    let label = format!("{} {}", event.reason, event.message);
                    ui.label(label.clone());
                    let summary = format!("×{} · last seen {}", event.count, event.last_seen);
                    let count = ui.monospace(summary.clone());
                    count.widget_info(|| {
                        WidgetInfo::labeled(WidgetType::Label, true, format!("{label} {summary}"))
                    });
                });
            }
        });
}
