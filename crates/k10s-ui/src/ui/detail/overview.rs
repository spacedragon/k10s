//! The Overview tab: backend-resolved labeled sections.

use egui::{Grid, RichText, WidgetInfo, WidgetType};

use k10s_protocol::DetailSection;

use crate::workspace::WindowId;

pub(super) const WIDE_BODY_WIDTH: f32 = 760.0;

pub(super) fn detail_columns(width: f32, gutter: f32) -> Option<(f32, f32)> {
    if width < WIDE_BODY_WIDTH {
        return None;
    }
    let content = (width - gutter).max(0.0);
    let configuration = content / 2.35;
    Some((configuration * 1.35, configuration))
}

pub(super) fn two_column(
    ui: &mut egui::Ui,
    operational: impl FnOnce(&mut egui::Ui),
    configuration: impl FnOnce(&mut egui::Ui),
) -> bool {
    let gutter = ui.spacing().item_spacing.x;
    let Some((left, right)) = detail_columns(ui.available_width(), gutter) else {
        return false;
    };
    ui.horizontal_top(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(left, 0.0),
            egui::Layout::top_down(egui::Align::Min),
            operational,
        );
        ui.allocate_ui_with_layout(
            egui::vec2(right, 0.0),
            egui::Layout::top_down(egui::Align::Min),
            configuration,
        );
    });
    true
}

pub(super) fn long_value_text(value: &str, max_chars: usize) -> String {
    if value.is_empty() {
        "—".into()
    } else {
        crate::ui::responsive_table::middle_elide(value, max_chars)
    }
}

/// A full-width, two-line value used for images, selectors and annotations.
pub(super) fn long_value(ui: &mut egui::Ui, label: &str, value: Option<&str>) {
    let original = value.filter(|value| !value.is_empty()).unwrap_or("—");
    ui.label(RichText::new(label).weak());
    ui.horizontal(|ui| {
        let shown = long_value_text(original, 64);
        let response = ui.label(&shown);
        response.widget_info(|| {
            WidgetInfo::labeled(WidgetType::Label, true, format!("{label}: {original}"))
        });
        if shown != original {
            response.on_hover_text(original);
        }
        let copy = ui.small_button("Copy");
        copy.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, format!("Copy {label}")));
        if copy.clicked() {
            ui.ctx().copy_text(original.to_owned());
        }
    });
}

/// Keeps a two-line long value inside one parent Grid cell.
pub(super) fn long_value_cell(ui: &mut egui::Ui, label: &str, value: Option<&str>) {
    ui.vertical(|ui| long_value(ui, label, value));
}

#[cfg(test)]
mod responsive_contract_tests {
    use super::{detail_columns, long_value_text};
    use egui::accesskit::Role;
    use egui_kittest::{Harness, kittest::Queryable as _};

    #[test]
    fn exact_overview_widths_use_the_shared_breakpoint_and_ratio() {
        assert_eq!(detail_columns(640.0, 8.0), None);
        let (operational, configuration) = detail_columns(1_000.0, 8.0).unwrap();
        assert!((operational / configuration - 1.35).abs() < 0.001);
        assert!((operational + configuration + 8.0 - 1_000.0).abs() < 0.01);
    }

    #[test]
    fn long_values_keep_unicode_and_image_suffixes() {
        let image = "registry.example/团队/checkout@sha256:abcdef:v42";
        let shown = long_value_text(image, 24);
        assert!(shown.ends_with(":v42"));
        assert!(shown.contains('…'));
        assert!(shown.is_char_boundary(shown.len()));
        assert_eq!(long_value_text("", 24), "—");
    }

    #[test]
    fn actual_1000_and_640_renders_switch_ratio_and_order() {
        let mut wide = Harness::builder()
            .with_size(egui::vec2(1_000.0, 300.0))
            .build_ui(|ui| {
                assert!(super::two_column(
                    ui,
                    |ui| {
                        ui.add_sized(
                            [ui.available_width(), 20.0],
                            egui::Button::new("operational"),
                        );
                    },
                    |ui| {
                        ui.add_sized(
                            [ui.available_width(), 20.0],
                            egui::Button::new("configuration"),
                        );
                    },
                ));
            });
        wide.run();
        let operational = wide.get_by_label("operational").rect();
        let configuration = wide.get_by_label("configuration").rect();
        assert!((operational.width() / configuration.width() - 1.35).abs() < 0.01);

        let mut narrow = Harness::builder()
            .with_size(egui::vec2(640.0, 300.0))
            .build_ui(|ui| {
                assert!(!super::two_column(ui, |_| {}, |_| {}));
                ui.label("operational");
                ui.label("configuration");
                ui.label("identity");
            });
        narrow.run();
        let labels = ["operational", "configuration", "identity"].map(|label| {
            narrow
                .get_by_role_and_label(Role::Label, label)
                .rect()
                .top()
        });
        assert!(labels[0] < labels[1] && labels[1] < labels[2]);
    }

    #[test]
    fn generic_empty_sections_do_not_render_heading_or_separator() {
        let mut harness = Harness::new_ui(|ui| {
            super::generic_sections(
                ui,
                crate::workspace::WindowId(1),
                &[k10s_protocol::DetailSection {
                    title: "EMPTY SENTINEL".into(),
                    rows: vec![],
                }],
            );
        });
        harness.run();
        assert!(harness.query_by_label("EMPTY SENTINEL").is_none());
    }
}

pub(super) fn show(
    ui: &mut egui::Ui,
    window_id: WindowId,
    sections: &[DetailSection],
    identity: &k10s_protocol::ResourceIdentity,
    metrics: super::presentation::DetailMetrics<'_>,
) {
    if two_column(
        ui,
        |column| generic_status(column, metrics),
        |column| {
            generic_sections(column, window_id, sections);
            generic_identity(column, window_id, identity);
        },
    ) {
        return;
    }
    generic_status(ui, metrics);
    generic_sections(ui, window_id, sections);
    generic_identity(ui, window_id, identity);
}

fn generic_sections(ui: &mut egui::Ui, window_id: WindowId, sections: &[DetailSection]) {
    if sections.is_empty() {
        ui.label("No additional structured details");
        return;
    }
    for section in sections {
        if section.rows.is_empty() {
            continue;
        }
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

fn generic_status(ui: &mut egui::Ui, metrics: super::presentation::DetailMetrics<'_>) {
    ui.heading("STATUS");
    ui.label(format!("Status · {}", metrics.status.unwrap_or("—")));
    ui.label(format!("Age · {}", metrics.age.unwrap_or("—")));
}

fn generic_identity(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &k10s_protocol::ResourceIdentity,
) {
    ui.heading("IDENTITY");
    Grid::new(("k10s.detail.generic.identity", window_id.0)).show(ui, |ui| {
        ui.label(format!("Name · {}", identity.name));
        ui.end_row();
        ui.label(format!(
            "Namespace · {}",
            identity.namespace.as_deref().unwrap_or("—")
        ));
        ui.end_row();
        ui.label(format!(
            "UID · {}",
            if identity.uid.is_empty() {
                "—"
            } else {
                &identity.uid
            }
        ));
        ui.end_row();
        ui.label(format!("Context · {}", identity.context));
        ui.end_row();
    });
}
