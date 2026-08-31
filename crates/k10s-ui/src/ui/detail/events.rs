//! The Events tab: backend-resolved deterministic event rows.

use egui::{WidgetInfo, WidgetType};
use k10s_protocol::EventRow;

pub(super) fn show(
    ui: &mut egui::Ui,
    condition: k10s_protocol::EventsCondition,
    events: &[EventRow],
) {
    if condition == k10s_protocol::EventsCondition::Unavailable {
        ui.label("Events unavailable");
        return;
    }
    if events.is_empty() {
        ui.label("No events reported");
        return;
    }
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
}
