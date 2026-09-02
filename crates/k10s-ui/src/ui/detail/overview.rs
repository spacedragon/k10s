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
            |ui| {
                let marker = ui
                    .allocate_response(egui::vec2(ui.available_width(), 0.0), egui::Sense::hover());
                marker.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Other, true, "Operational detail column")
                });
                operational(ui);
            },
        );
        ui.allocate_ui_with_layout(
            egui::vec2(right, 0.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                let marker = ui
                    .allocate_response(egui::vec2(ui.available_width(), 0.0), egui::Sense::hover());
                marker.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Other, true, "Configuration detail column")
                });
                configuration(ui);
            },
        );
    });
    true
}

/// Paint a local-width section boundary without adding an accessibility node.
pub(super) fn section_separator(ui: &mut egui::Ui) -> egui::Response {
    let height = ui.spacing().item_spacing.y;
    let visible_width = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width();
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(visible_width, height), egui::Sense::hover());
    if ui.is_rect_visible(rect) {
        let y = ui.painter().round_to_pixel_center(rect.center().y);
        ui.painter().hline(
            rect.x_range(),
            y,
            ui.visuals().widgets.noninteractive.bg_stroke,
        );
    }
    response
}

pub(super) fn long_value_text(value: &str, max_chars: usize) -> String {
    if value.is_empty() {
        "—".into()
    } else {
        crate::ui::responsive_table::middle_elide(value, max_chars)
    }
}

/// A full-width, two-line value used for images, selectors and annotations.
pub(super) fn long_value(ui: &mut egui::Ui, width: f32, label: &str, value: Option<&str>) {
    let available = value.filter(|value| !value.is_empty());
    let original = available.unwrap_or("—");
    ui.allocate_ui_with_layout(
        egui::vec2(width.max(1.0), 0.0),
        egui::Layout::top_down(egui::Align::Min),
        |ui| {
            ui.label(RichText::new(label).weak());
            ui.horizontal(|ui| {
                let copy_width = if available.is_some() { 48.0 } else { 0.0 };
                // Grid intrinsic sizing may report zero available width. The clip is
                // already the bounded parent cell/column viewport, so establish a
                // stable one-cell row from it before measuring the value.
                let value_width = (width - copy_width - ui.spacing().item_spacing.x).max(1.0);
                let mut limit =
                    unicode_segmentation::UnicodeSegmentation::graphemes(original, true).count();
                let mut shown = original.to_owned();
                while limit > 1
                    && ui
                        .painter()
                        .layout_no_wrap(
                            shown.clone(),
                            egui::FontId::default(),
                            ui.visuals().text_color(),
                        )
                        .size()
                        .x
                        > value_width
                {
                    limit -= 1;
                    shown = long_value_text(original, limit);
                }
                let response = ui.add_sized(
                    [value_width, ui.spacing().interact_size.y],
                    egui::Label::new(&shown).truncate(),
                );
                response.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, format!("{label}: {original}"))
                });
                if shown != original {
                    response.on_hover_text(original);
                }
                if available.is_some() {
                    let copy = ui.add_sized(
                        [copy_width, ui.spacing().interact_size.y],
                        egui::Button::new("Copy").small(),
                    );
                    copy.widget_info(|| {
                        WidgetInfo::labeled(WidgetType::Button, true, format!("Copy {label}"))
                    });
                    if copy.clicked() {
                        ui.ctx().copy_text(original.to_owned());
                    }
                }
            });
        },
    );
}

/// Keeps a two-line long value inside one parent Grid cell.
pub(super) fn long_value_cell(ui: &mut egui::Ui, width: f32, label: &str, value: Option<&str>) {
    ui.vertical(|ui| long_value(ui, width, label, value));
}

/// Render label chips and the annotation disclosure in one wrapping flow.
pub(super) fn metadata_labels_and_annotations<'a>(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    separator: &str,
    annotations: impl IntoIterator<Item = (&'a str, &'a str)>,
) {
    let labels = labels.into_iter().collect::<Vec<_>>();
    let annotations = annotations.into_iter().collect::<Vec<_>>();
    if labels.is_empty() && annotations.is_empty() {
        return;
    }
    let width = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width()
        .max(1.0);
    let expansion_id = egui::Id::new(id);
    let mut open = ui
        .ctx()
        .data_mut(|data| data.get_temp::<bool>(expansion_id))
        .unwrap_or(false);
    let mut disclosure = None;
    ui.horizontal_wrapped(|ui| {
        for (key, value) in labels {
            let full = format!("{key}{separator}{value}");
            let desired = ui
                .painter()
                .layout_no_wrap(
                    full.clone(),
                    egui::FontId::default(),
                    ui.visuals().text_color(),
                )
                .size()
                .x;
            let (rect, _) = ui.allocate_exact_size(
                egui::vec2((desired + 16.0).min(width), ui.spacing().interact_size.y),
                egui::Sense::hover(),
            );
            ui.painter().rect(
                rect,
                10.0,
                ui.visuals().faint_bg_color,
                egui::Stroke::new(1.0, ui.visuals().widgets.noninteractive.bg_stroke.color),
                egui::StrokeKind::Inside,
            );
            let response = ui.put(
                rect.shrink2(egui::vec2(7.0, 2.0)),
                egui::Label::new(&full).truncate(),
            );
            response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, full.clone()));
            response.on_hover_text(&full);
        }
        if !annotations.is_empty() {
            let arrow = if open { '▴' } else { '▾' };
            let response = ui.button(format!("Annotations {} {arrow}", annotations.len()));
            ui.ctx()
                .accesskit_node_builder(response.id, |node| node.set_expanded(open));
            disclosure = Some(response);
        }
    });
    if disclosure.is_some_and(|response| response.clicked()) {
        open = !open;
    }
    ui.ctx()
        .data_mut(|data| data.insert_temp(expansion_id, open));
    if !open {
        return;
    }
    for (key, value) in annotations {
        ui.allocate_ui_with_layout(
            egui::vec2(width, 0.0),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                let key_response = ui.add_sized(
                    [(width * 0.34).max(1.0), 0.0],
                    egui::Label::new(RichText::new(key).weak()).truncate(),
                );
                key_response
                    .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, key.to_owned()));
                key_response.on_hover_text(key);
                let full = format!("{key}: {value}");
                let response = ui.add_sized(
                    [ui.available_width().max(1.0), 0.0],
                    egui::Label::new(value).truncate(),
                );
                response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, full.clone()));
                response.on_hover_text(value);
            },
        );
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod responsive_contract_tests {
    use super::{detail_columns, long_value_text};
    use egui::accesskit::Role;
    use egui_kittest::{
        Harness,
        kittest::{NodeT as _, Queryable as _},
    };

    #[test]
    fn detail_section_separator_spans_local_width() {
        let mut harness = Harness::builder()
            .with_size(egui::vec2(420.0, 120.0))
            .build_ui(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(280.0, 0.0),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let available = ui.available_rect_before_wrap();
                        let clipped = egui::Rect::from_min_max(
                            available.min,
                            egui::pos2(available.left() + 180.0, available.bottom()),
                        );
                        ui.set_clip_rect(clipped);
                        let response = super::section_separator(ui);
                        assert!((response.rect.width() - clipped.width()).abs() <= 1.0);
                        assert!(
                            response.rect.right() <= clipped.right() + 1.0,
                            "separator must stay within the local clip: {:?} vs {:?}",
                            response.rect,
                            clipped
                        );
                        assert!(
                            response.rect.height() <= ui.spacing().item_spacing.y + 1.0,
                            "separator must fit within one standard item spacing: {:?}",
                            response.rect
                        );
                    },
                );
            });
        harness.run();
    }

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

    #[test]
    fn unavailable_long_value_never_exposes_copy() {
        let mut harness = Harness::new_ui(|ui| super::long_value(ui, 240.0, "Image", None));
        harness.run();
        harness.get_by_label("Image: —");
        assert!(
            harness
                .query_by_role_and_label(Role::Button, "Copy Image")
                .is_none()
        );
    }

    #[test]
    fn annotation_disclosure_state_is_scoped_to_resource_identity() {
        let selected = std::sync::Arc::new(std::sync::Mutex::new("pod-a"));
        let render_selected = selected.clone();
        let mut harness = Harness::new_ui(move |ui| {
            let identity = *render_selected.lock().unwrap();
            super::metadata_labels_and_annotations(
                ui,
                ("annotations", 7_u64, identity),
                [],
                "=",
                [("key", "value")],
            );
        });
        harness.run();
        harness
            .get_by_role_and_label(Role::Button, "Annotations 1 ▾")
            .click();
        harness.run_steps(2);
        harness.get_by_role_and_label(Role::Button, "Annotations 1 ▴");

        *selected.lock().unwrap() = "pod-b";
        harness.run_steps(2);
        let disclosure = harness.get_by_role_and_label(Role::Button, "Annotations 1 ▾");
        assert_eq!(
            disclosure.accesskit_node().data().is_expanded(),
            Some(false)
        );
    }

    #[test]
    fn long_common_prefix_annotation_keys_are_individually_hover_recoverable() {
        let first = "example.io/very-long-common-prefix/first-distinct-key";
        let second = "example.io/very-long-common-prefix/second-distinct-key";
        let mut harness = Harness::builder()
            .with_size(egui::vec2(280.0, 180.0))
            .build_ui(move |ui| {
                super::metadata_labels_and_annotations(
                    ui,
                    ("annotations", "resource-a"),
                    [],
                    "=",
                    [(first, "one"), (second, "two")],
                );
            });
        harness.run();
        harness
            .get_by_role_and_label(Role::Button, "Annotations 2 ▾")
            .click();
        harness.run_steps(2);

        for key in [first, second] {
            harness.get_by_label(key).hover();
            harness.run_steps(15);
            assert!(
                harness.get_all_by_label(key).count() >= 2,
                "truncated key must expose its full distinct value in a tooltip: {key}"
            );
        }
    }

    #[test]
    fn metadata_disclosure_shares_or_wraps_the_chip_flow_by_available_width() {
        let render = |width| {
            Harness::builder()
                .with_size(egui::vec2(width, 180.0))
                .build_ui(move |ui| {
                    super::metadata_labels_and_annotations(
                        ui,
                        ("annotations", width.to_bits()),
                        [("app", "web")],
                        "=",
                        [("note", "value")],
                    );
                })
        };
        let mut wide = render(520.0);
        wide.run();
        let chip = wide.get_by_label("app=web").rect();
        let disclosure = wide
            .get_by_role_and_label(Role::Button, "Annotations 1 ▾")
            .rect();
        assert!((chip.top() - disclosure.top()).abs() < 1.0);

        let mut narrow = render(150.0);
        narrow.run();
        let chip = narrow.get_by_label("app=web").rect();
        let disclosure = narrow
            .get_by_role_and_label(Role::Button, "Annotations 1 ▾")
            .rect();
        assert!(disclosure.top() > chip.top() + 1.0);
    }

    #[test]
    fn uid_empty_resource_identity_tuples_do_not_alias_annotation_state() {
        let selected = std::sync::Arc::new(std::sync::Mutex::new("pod-a"));
        let render_selected = selected.clone();
        let mut harness = Harness::new_ui(move |ui| {
            let name = *render_selected.lock().unwrap();
            super::metadata_labels_and_annotations(
                ui,
                ("ctx", "", "v1", "Pod", "default", name, ""),
                [],
                "=",
                [("note", "value")],
            );
        });
        harness.run();
        harness
            .get_by_role_and_label(Role::Button, "Annotations 1 ▾")
            .click();
        harness.run_steps(2);
        harness.get_by_role_and_label(Role::Button, "Annotations 1 ▴");
        *selected.lock().unwrap() = "pod-b";
        harness.run_steps(2);
        let disclosure = harness.get_by_role_and_label(Role::Button, "Annotations 1 ▾");
        assert_eq!(
            disclosure.accesskit_node().data().is_expanded(),
            Some(false)
        );
    }

    #[test]
    fn painted_long_values_keep_short_text_and_meaningful_complex_ends() {
        let long = "仓库/团队/e\u{301}/👨‍👩‍👧‍👦/非常非常非常长的镜像名称:v42";
        let mut harness = Harness::builder()
            .with_size(egui::vec2(360.0, 180.0))
            .build_ui(move |ui| {
                super::long_value(ui, 320.0, "Image", Some("repo/app:v1"));
                super::long_value(ui, 320.0, "Annotation", Some(long));
            });
        harness.run();
        assert!(
            harness
                .query_all_by_role(Role::TextRun)
                .filter_map(|node| node.value())
                .any(|value| value == "repo/app:v1")
        );
        let painted = harness
            .query_all_by_role(Role::TextRun)
            .filter_map(|node| node.value())
            .find(|value| value.contains('…') && value.ends_with(":v42"))
            .expect("long complex value paints a meaningful elided prefix and suffix");
        assert!(painted.starts_with('仓'));
        harness.get_by_role_and_label(Role::Button, "Copy Image");
        harness.get_by_role_and_label(Role::Button, "Copy Annotation");
    }

    #[test]
    fn duplicate_section_titles_render_independent_grids() {
        let sections = ["one", "two"].map(|value| k10s_protocol::DetailSection {
            title: "DUPLICATE".into(),
            rows: vec![k10s_protocol::DetailRow {
                label: "Value".into(),
                value: value.into(),
            }],
        });
        let mut harness = Harness::new_ui(move |ui| {
            super::generic_sections(ui, crate::workspace::WindowId(9), &sections)
        });
        harness.run();
        assert_eq!(harness.get_all_by_label("DUPLICATE").count(), 2);
        harness.get_by_label("Value one");
        harness.get_by_label("Value two");
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
    for (section_index, section) in sections.iter().enumerate() {
        if section.rows.is_empty() {
            continue;
        }
        ui.heading(RichText::new(section.title.as_str()).strong());
        Grid::new((
            "k10s.detail.overview.grid",
            window_id.0,
            section_index,
            &section.title,
        ))
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
