//! Fixed shared detail chrome: identity, vitals, controls, tabs, one body
//! scroll region, and a footer. Kind modules only supply the body content.

use egui::containers::scroll_area::ScrollBarVisibility;
use egui::{
    Align, Layout, RichText, ScrollArea, Sense, UiBuilder, WidgetInfo, WidgetText, WidgetType,
};

use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{
    DetailExpansionState, DetailFrameProjection, DetailPresentationInput, DetailPrimary,
    DetailVital, DetailVitalTone,
};
use crate::ui::resource_window::RowIdentity;

pub(crate) fn title(identity: &k10s_protocol::ResourceIdentity) -> String {
    match identity.namespace.as_deref() {
        Some(namespace) => format!("{} · {namespace} / {}", identity.gvk.kind, identity.name),
        None => format!("{} · {}", identity.gvk.kind, identity.name),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    input: &DetailPresentationInput<'_>,
    integrated: bool,
    detail_maximized: bool,
    tabs: &[DetailTab],
    queued: &mut Vec<WorkspaceCommand<I>>,
    configure: impl FnOnce(&mut DetailFrameProjection<'_>),
    mut content: impl FnMut(&mut egui::Ui, DetailPrimary<'_>, bool, &mut DetailFrameProjection<'_>),
) {
    let expansion_id = egui::Id::new(("k10s.detail.expansion", window_id.0));
    let expansion = ui
        .ctx()
        .data_mut(|data| data.get_temp::<DetailExpansionState>(expansion_id))
        .unwrap_or_default();
    let mut projection = input.frame_projection(expansion);
    configure(&mut projection);
    ui.horizontal(|ui| {
        ui.label(RichText::new(title(projection.identity)).strong().heading());
        if integrated && !input.gone {
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let maximize = ui.button(if detail_maximized {
                    "Restore split"
                } else {
                    "Maximize"
                });
                if maximize.clicked() {
                    queued.push(if detail_maximized {
                        WorkspaceCommand::RestoreDetailPane(window_id)
                    } else {
                        WorkspaceCommand::MaximizeDetailPane(window_id)
                    });
                }
                if ui.button("Pop out ↗").clicked() {
                    queued.push(WorkspaceCommand::OpenDedicatedDetail(
                        detail.identity.clone(),
                    ));
                }
            });
        }
    });
    let vitals_width = ui.available_width();
    let wide = vitals_width >= 760.0;
    let (vitals_rect, _) = ui.allocate_exact_size(
        egui::vec2(vitals_width, ui.spacing().interact_size.y),
        Sense::hover(),
    );
    let mut vitals_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.vitals", window_id.0))
            .max_rect(vitals_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    {
        let ui = &mut vitals_ui;
        for metric in &projection.visible_vitals {
            vital(ui, metric);
        }
        if wide || projection.expansion.more_vitals {
            for metric in &projection.overflow_vitals {
                vital(ui, metric);
            }
        } else if let Some(kind) = projection.vital_expansion_label
            && !projection.overflow_vitals.is_empty()
            && ui.button(format!("Show more {kind} vitals")).clicked()
        {
            projection.expansion.more_vitals = true;
        }
        if !wide
            && projection.expansion.more_vitals
            && let Some(kind) = projection.vital_expansion_label
            && ui.button(format!("Hide more {kind} vitals")).clicked()
        {
            projection.expansion.more_vitals = false;
        }
        let freshness = match projection.freshness {
            Some(crate::ui::WindowFreshness::Live { last_sync_age }) => {
                format!("Freshness · live ({last_sync_age})")
            }
            Some(crate::ui::WindowFreshness::StaleRetrying { .. }) => "Freshness · stale".into(),
            Some(crate::ui::WindowFreshness::Reconnecting { .. }) => {
                "Freshness · reconnecting".into()
            }
            Some(crate::ui::WindowFreshness::Forbidden { .. }) => "Freshness · forbidden".into(),
            Some(crate::ui::WindowFreshness::Failed { .. }) => "Freshness · failed".into(),
            Some(crate::ui::WindowFreshness::ReadyEmpty) => "Freshness · ready".into(),
            None if input.gone => "Freshness · gone".into(),
            None => "Freshness · unavailable".into(),
        };
        ui.label(freshness);
    }
    if !input.mutations_allowed
        && (projection.actions.can_scale
            || projection.actions.can_restart
            || projection.actions.can_delete)
    {
        ui.add(
            egui::Label::new(
                "Scale, restart, delete, and YAML edits are disabled until this window is live",
            )
            .wrap(),
        );
    }
    let (tab_row, _) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), ui.spacing().interact_size.y),
        Sense::hover(),
    );
    let gap = ui.spacing().item_spacing.x;
    let usable_width = (tab_row.width() - gap).max(0.0);
    let action_width = (usable_width * 0.5).clamp(96.0, 360.0).min(usable_width);
    let tab_width = usable_width - action_width;
    let tabs_rect = egui::Rect::from_min_size(tab_row.min, egui::vec2(tab_width, tab_row.height()));
    let actions_rect = egui::Rect::from_min_max(
        egui::pos2(tabs_rect.right() + gap, tab_row.top()),
        tab_row.max,
    );
    let mut tabs_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.tabs", window_id.0))
            .max_rect(tabs_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    tabs_ui.set_clip_rect(tabs_ui.clip_rect().intersect(tabs_rect));
    ScrollArea::horizontal()
        .id_salt(("k10s.detail.tabs.scroll", window_id.0))
        .scroll_bar_visibility(ScrollBarVisibility::AlwaysHidden)
        .show(&mut tabs_ui, |ui| {
            ui.horizontal(|ui| {
                for tab in tabs {
                    let active = *tab == detail.active_tab;
                    let response = ui.selectable_label(active, super::tab_label(*tab));
                    response.widget_info(|| {
                        WidgetInfo::labeled(
                            WidgetType::Button,
                            true,
                            format!("Tab {}", super::tab_label(*tab)),
                        )
                    });
                    if response.clicked() && !active {
                        queued.push(WorkspaceCommand::SetActiveTab(window_id, *tab));
                    }
                }
            });
        });
    let owner = projection.actions.verified_owner;
    let mut actions_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.actions", window_id.0))
            .max_rect(actions_rect)
            .layout(Layout::right_to_left(Align::Center)),
    );
    actions_ui.set_clip_rect(actions_ui.clip_rect().intersect(actions_rect));
    ScrollArea::horizontal()
        .id_salt(("k10s.detail.actions.scroll", window_id.0))
        .scroll_bar_visibility(ScrollBarVisibility::AlwaysHidden)
        .stick_to_right(true)
        .show(&mut actions_ui, |ui| {
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                ui.spacing_mut().item_spacing.x = 4.0;
                content(ui, input.primary, true, &mut projection);
                let namespace = projection.identity.namespace.as_deref();
                let uid = (!projection.identity.uid.is_empty())
                    .then_some(projection.identity.uid.as_str());
                if owner.is_some() || namespace.is_some() || uid.is_some() {
                    ui.menu_button("Actions", |ui| {
                        if let Some(owner) = owner {
                            let label = format!("Open owner {}", owner.name);
                            if ui.button(&label).clicked() {
                                queued.push(WorkspaceCommand::OpenDedicatedDetail(
                                    I::from_row_identity(&super::presentation::owner_identity(
                                        projection.identity,
                                        owner,
                                    )),
                                ));
                                ui.close();
                            }
                        }
                        if let Some(namespace) = namespace {
                            copy(ui, "Copy namespace", namespace);
                        }
                        if let Some(uid) = uid {
                            copy(ui, "Copy UID", uid);
                        }
                    });
                }
                copy(ui, "Copy name", &projection.identity.name);
            });
        });
    ui.separator();
    let remaining = ui.available_rect_before_wrap();
    let footer_text = RichText::new(format!(
        "Shortcuts: {} · Esc restore/close",
        projection.shortcut_labels.join(" · ")
    ))
    .weak();
    let footer_galley = WidgetText::from(footer_text.clone()).into_galley(
        ui,
        Some(egui::TextWrapMode::Wrap),
        remaining.width(),
        egui::TextStyle::Body,
    );
    let footer_height = footer_galley.size().y + ui.spacing().item_spacing.y * 2.0 + 1.0;
    let footer_top = (remaining.bottom() - footer_height).max(remaining.top());
    let body_rect =
        egui::Rect::from_min_max(remaining.min, egui::pos2(remaining.right(), footer_top));
    let footer_rect =
        egui::Rect::from_min_max(egui::pos2(remaining.left(), footer_top), remaining.max);
    ui.allocate_rect(remaining, Sense::hover());
    let mut body_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.body", window_id.0, detail.active_tab))
            .max_rect(body_rect)
            .layout(Layout::top_down(Align::Min)),
    );
    body_ui
        .ctx()
        .accesskit_node_builder(body_ui.unique_id(), |node| {
            node.set_role(egui::accesskit::Role::ScrollView);
            node.set_label("Detail body");
            node.set_bounds(egui::accesskit::Rect {
                x0: body_rect.left().into(),
                y0: body_rect.top().into(),
                x1: body_rect.right().into(),
                y1: body_rect.bottom().into(),
            });
        });
    ScrollArea::vertical()
        .id_salt(("k10s.detail.body.scroll", window_id.0, detail.active_tab))
        .max_height(body_rect.height())
        .show(&mut body_ui, |ui| {
            if input.gone {
                ui.label("This resource no longer exists");
            } else {
                content(ui, input.primary, false, &mut projection);
            }
        });
    let mut footer_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.footer", window_id.0))
            .max_rect(footer_rect)
            .layout(Layout::top_down(Align::Min)),
    );
    footer_ui.separator();
    footer_ui.add(egui::Label::new(footer_text).wrap());
    ui.ctx().data_mut(|data| {
        data.insert_temp(expansion_id, projection.expansion);
    });
}

fn vital(ui: &mut egui::Ui, vital: &DetailVital) {
    let text = match vital.shape {
        Some(shape) => format!("{} {} {}", vital.label, shape.glyph(), vital.value),
        None => format!("{} · {}", vital.label, vital.value),
    };
    ui.label(RichText::new(text).color(vital_color(ui.visuals(), vital.tone)));
}

fn vital_color(visuals: &egui::Visuals, tone: DetailVitalTone) -> egui::Color32 {
    match tone {
        DetailVitalTone::Neutral => visuals.text_color(),
        DetailVitalTone::Healthy => crate::ui::theme::HEALTHY,
        DetailVitalTone::Warning => crate::ui::theme::WARNING,
        DetailVitalTone::Danger => crate::ui::theme::DANGER,
    }
}

fn copy(ui: &mut egui::Ui, label: &str, value: &str) {
    let response = ui.button(label);
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.to_owned()));
    if response.clicked() {
        ui.ctx().copy_text(value.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use egui_kittest::{Harness, kittest::Queryable as _};
    use k10s_protocol::{GroupVersionKind, ResourceIdentity};

    use super::*;
    use crate::ui::detail::presentation::{
        DetailMetrics, DetailVital, DetailVitalShape, DetailVitalTone,
    };

    #[test]
    fn semantic_vital_shapes_are_visible_and_tones_use_theme_palette() {
        assert_eq!(DetailVitalShape::Dot.glyph(), "●");
        assert_eq!(DetailVitalShape::Triangle.glyph(), "▲");
        assert_eq!(DetailVitalShape::Cross.glyph(), "✕");

        let visuals = egui::Visuals::dark();
        assert_eq!(
            vital_color(&visuals, DetailVitalTone::Neutral),
            visuals.text_color()
        );
        assert_eq!(
            vital_color(&visuals, DetailVitalTone::Healthy),
            crate::ui::theme::HEALTHY
        );
        assert_eq!(
            vital_color(&visuals, DetailVitalTone::Warning),
            crate::ui::theme::WARNING
        );
        assert_eq!(
            vital_color(&visuals, DetailVitalTone::Danger),
            crate::ui::theme::DANGER
        );
    }

    #[test]
    fn configure_hook_changes_accessible_vital_before_body_rendering() {
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "web-0".into(),
            uid: "uid-web-0".into(),
        };
        let detail = DetailState::new(identity.clone());
        let mut harness = Harness::builder()
            .with_size(egui::vec2(900.0, 500.0))
            .build_ui(move |ui| {
                let input = DetailPresentationInput {
                    identity: &identity,
                    primary: DetailPrimary::Loading,
                    metrics: DetailMetrics {
                        status: Some("Pending"),
                        age: Some("2m"),
                    },
                    resource_metrics: None,
                    relations: None,
                    freshness: None,
                    gone: false,
                    mutations_allowed: false,
                    port_forward_available: false,
                    port_forward_sessions: &[],
                    port_forward_error: None,
                };
                show(
                    ui,
                    WindowId(77),
                    &detail,
                    &input,
                    false,
                    false,
                    &[],
                    &mut Vec::new(),
                    |projection| {
                        projection.visible_vitals = vec![DetailVital {
                            label: "Status",
                            value: "Configured".into(),
                            tone: DetailVitalTone::Danger,
                            shape: Some(DetailVitalShape::Cross),
                        }];
                    },
                    |ui, _, actions, projection| {
                        if !actions {
                            ui.label(format!(
                                "Body observed {}",
                                projection.visible_vitals[0].value
                            ));
                        }
                    },
                );
            });
        harness.run_steps(2);

        harness.get_by_label("Status ✕ Configured");
        harness.get_by_label("Body observed Configured");
        assert!(harness.query_by_label("Status · Pending").is_none());
    }
}
