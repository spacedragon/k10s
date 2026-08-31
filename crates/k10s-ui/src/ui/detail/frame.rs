//! Fixed shared detail chrome: identity, vitals, controls, tabs, one body
//! scroll region, and a footer. Kind modules only supply the body content.

use egui::{Align, Layout, RichText, ScrollArea, Sense, UiBuilder, WidgetInfo, WidgetType};

use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{
    DetailExpansionState, DetailFrameProjection, DetailPresentationInput, DetailPrimary,
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
    mut content: impl FnMut(&mut egui::Ui, DetailPrimary<'_>, bool, &mut DetailFrameProjection<'_>),
) {
    let expansion_id = egui::Id::new(("k10s.detail.expansion", window_id.0));
    let expansion = ui
        .ctx()
        .data_mut(|data| data.get_temp::<DetailExpansionState>(expansion_id))
        .unwrap_or_default();
    let mut projection = input.frame_projection(expansion);
    ui.horizontal(|ui| {
        ui.label(RichText::new(title(input.identity)).strong().heading());
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
            vital(ui, metric.label, &metric.value);
        }
        if wide || projection.expansion.more_vitals {
            for metric in &projection.overflow_vitals {
                vital(ui, metric.label, &metric.value);
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
        vital(ui, "", &freshness);
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
    let mut tabs_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.tabs", window_id.0))
            .max_rect(tab_row)
            .layout(Layout::left_to_right(Align::Center)),
    );
    for tab in tabs {
        let active = *tab == detail.active_tab;
        let response = tabs_ui.selectable_label(active, super::tab_label(*tab));
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
    let owner = projection.actions.verified_owner;
    let mut actions_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.actions", window_id.0))
            .max_rect(tab_row)
            .layout(Layout::right_to_left(Align::Center)),
    );
    {
        let ui = &mut actions_ui;
        ui.spacing_mut().item_spacing.x = 4.0;
        content(ui, input.primary, true, &mut projection);
        let namespace = input.identity.namespace.as_deref();
        let uid = (!input.identity.uid.is_empty()).then_some(input.identity.uid.as_str());
        if owner.is_some() || namespace.is_some() || uid.is_some() {
            ui.menu_button("Actions", |ui| {
                if let Some(owner) = owner {
                    let label = format!("Open owner {}", owner.name);
                    if ui.button(&label).clicked() {
                        queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                            &k10s_protocol::ResourceIdentity {
                                context: input.identity.context.clone(),
                                gvk: owner.gvk.clone(),
                                namespace: input.identity.namespace.clone(),
                                name: owner.name.clone(),
                                uid: owner.uid.clone(),
                            },
                        )));
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
        copy(ui, "Copy name", &input.identity.name);
    }
    ui.separator();
    let remaining = ui.available_rect_before_wrap();
    let footer_height =
        ui.text_style_height(&egui::TextStyle::Body) + ui.spacing().item_spacing.y * 2.0 + 1.0;
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
    footer_ui.label(
        RichText::new(format!(
            "Shortcuts: {} · Esc restore/close",
            projection.shortcut_labels.join(" · ")
        ))
        .weak(),
    );
    ui.ctx().data_mut(|data| {
        data.insert_temp(expansion_id, projection.expansion);
    });
}

fn vital(ui: &mut egui::Ui, label: &str, value: &str) {
    let text = if label.is_empty() {
        value.to_owned()
    } else {
        format!("{label} · {value}")
    };
    ui.label(text);
}

fn copy(ui: &mut egui::Ui, label: &str, value: &str) {
    let response = ui.button(label);
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.to_owned()));
    if response.clicked() {
        ui.ctx().copy_text(value.to_owned());
    }
}

/// Minimal Task-4 body hook used by typed stubs until their final renderers
/// replace only their internals. Values shown here are authoritative typed
/// projection fields, never fabricated placeholders.
pub(super) fn show_typed_stub(
    ui: &mut egui::Ui,
    kind: &str,
    labels: &std::collections::BTreeMap<String, String>,
    frame: &mut DetailFrameProjection<'_>,
) {
    ui.label(format!("{kind} structured detail renderer"));
    if let Some(metrics) = frame.resource_metrics {
        debug_assert_eq!(&metrics.identity, frame.identity);
    }
    let _relation_state_is_available = frame.relations.is_some();
    if ui.clip_rect().width() < 760.0 {
        let label = if frame.expansion.metadata {
            "Hide metadata"
        } else {
            "Show metadata"
        };
        if ui.button(label).clicked() {
            frame.expansion.metadata = !frame.expansion.metadata;
        }
        if frame.expansion.metadata {
            ui.label(format!("Context · {}", frame.identity.context));
            if let Some(namespace) = frame.identity.namespace.as_deref() {
                ui.label(format!("Namespace · {namespace}"));
            }
            if !frame.identity.uid.is_empty() {
                ui.label(format!("UID · {}", frame.identity.uid));
            }
        }
    }
    if !labels.is_empty() {
        ui.heading("Labels");
        let visible = if frame.expansion.labels {
            labels.len()
        } else {
            labels.len().min(4)
        };
        for (key, value) in labels.iter().take(visible) {
            ui.label(format!("{key}={value}"));
        }
        let hidden = labels.len().saturating_sub(4);
        if hidden > 0 {
            let label = if frame.expansion.labels {
                format!("Hide {hidden} labels")
            } else {
                format!("Show {hidden} more labels")
            };
            if ui.button(label).clicked() {
                frame.expansion.labels = !frame.expansion.labels;
            }
        }
    }
}
