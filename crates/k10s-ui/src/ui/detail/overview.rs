//! The Overview tab: backend-resolved labeled sections.

use egui::{Grid, RichText};

use k10s_protocol::DetailSection;

use crate::workspace::WindowId;

pub(super) fn show(ui: &mut egui::Ui, window_id: WindowId, sections: &[DetailSection]) {
    for section in sections {
        ui.heading(RichText::new(section.title.as_str()).strong());
        Grid::new(("k10s.detail.overview.grid", window_id.0, &section.title))
            .num_columns(1)
            .striped(true)
            .min_col_width(240.0)
            .show(ui, |ui| {
                for row in &section.rows {
                    ui.label(format!("{} {}", row.label, row.value));
                    ui.end_row();
                }
            });
        ui.separator();
    }
}
