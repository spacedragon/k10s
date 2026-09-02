//! Fixed shared detail chrome: identity, vitals, controls, tabs, one body
//! scroll region, and a footer. Kind modules only supply the body content.

use egui::{
    Align, Layout, RichText, ScrollArea, Sense, UiBuilder, WidgetInfo, WidgetText, WidgetType,
};

use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{
    DetailExpansionState, DetailFrameProjection, DetailFreshness, DetailPresentationInput,
    DetailPrimary, DetailVital, DetailVitalTone,
};
use crate::ui::resource_window::RowIdentity;

/// One segment of the reference action row. The frame owns the layout and
/// renders `Delete` (rightmost), the `Actions` overflow menu, and the
/// primary segment (`Restart`, `Scale`) in between; kind modules only supply
/// the individual buttons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailActionSegment {
    /// The destructive `Delete…` button, rightmost and danger-styled.
    Delete,
    /// The `Restart…` command.
    Restart,
    /// The `Scale…` command.
    Scale,
}

pub(crate) fn title(identity: &k10s_protocol::ResourceIdentity) -> String {
    match identity.namespace.as_deref() {
        Some(namespace) => format!("{} · {namespace} / {}", identity.gvk.kind, identity.name),
        None => format!("{} · {}", identity.gvk.kind, identity.name),
    }
}

fn expansion_id(
    window_id: WindowId,
    identity: &k10s_protocol::ResourceIdentity,
    tab: DetailTab,
) -> egui::Id {
    let identity_key = if identity.uid.is_empty() {
        format!(
            "{}|{}|{}|{}|{}|{}",
            identity.context,
            identity.gvk.group,
            identity.gvk.version,
            identity.gvk.kind,
            identity.namespace.as_deref().unwrap_or_default(),
            identity.name,
        )
    } else {
        identity.uid.clone()
    };
    egui::Id::new(("k10s.detail.expansion", window_id.0, identity_key, tab))
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
    mut content: impl FnMut(
        &mut egui::Ui,
        DetailPrimary<'_>,
        Option<DetailActionSegment>,
        &mut DetailFrameProjection<'_>,
    ),
) {
    let expansion_id = expansion_id(window_id, input.identity, detail.active_tab);
    let expansion = ui
        .ctx()
        .data_mut(|data| data.get_temp::<DetailExpansionState>(expansion_id))
        .unwrap_or_default();
    let mut projection = input.frame_projection(expansion);
    configure(&mut projection);
    if integrated {
        let identity = projection.identity;
        let full_title = title(identity);
        let identity_row = ui.horizontal(|ui| {
            // Reference identity hierarchy: kind in the secondary accent
            // blue, namespace muted, name emphasized. The kind label carries
            // the full-title heading semantics for the tree and screen
            // readers; the styled segments are its visual decomposition.
            let kind_label =
                ui.label(RichText::new(identity.gvk.kind.clone()).color(crate::ui::theme::ACCENT));
            ui.ctx().accesskit_node_builder(kind_label.id, |node| {
                node.set_role(egui::accesskit::Role::Heading);
                node.set_label(full_title.clone());
            });
            if let Some(namespace) = identity.namespace.as_deref() {
                ui.label(
                    RichText::new(format!(" {namespace} / ")).color(crate::ui::theme::MUTED_TEXT),
                );
            }
            ui.label(RichText::new(&identity.name).strong());
            // Freshness stays adjacent to the identity instead of owning the
            // far right of the row.
            ui.label(RichText::new(freshness_text(projection.freshness)).weak());
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let clear = ui.button("×").on_hover_text("Clear selection");
                clear.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Button, true, "Clear selection")
                });
                if clear.clicked() {
                    queued.push(WorkspaceCommand::ClearSelection(window_id));
                }
                if !input.gone {
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
                }
            });
        });
        let identity_semantics = ui.interact(
            identity_row.response.rect,
            ui.id().with(("k10s.detail.identity", window_id.0)),
            Sense::hover(),
        );
        identity_semantics
            .widget_info(|| WidgetInfo::labeled(WidgetType::Other, true, "Detail identity row"));
        ui.ctx()
            .accesskit_node_builder(identity_semantics.id, |node| {
                node.set_role(egui::accesskit::Role::GenericContainer);
                node.set_label("Detail identity row");
            });
    }
    let vitals_width = ui.available_width();
    let wide = vitals_width >= 760.0;
    let (vitals_rect, vitals_response) = ui.allocate_exact_size(
        egui::vec2(vitals_width, ui.spacing().interact_size.y),
        Sense::hover(),
    );
    vitals_response
        .widget_info(|| WidgetInfo::labeled(WidgetType::Other, true, "Detail vital strip"));
    let mut vitals_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.vitals", window_id.0))
            .max_rect(vitals_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    let mut popup_rects = None;
    let wide_count = projection.visible_vitals.len() + projection.overflow_vitals.len();
    let wide_minimum = VITAL_CHIP_SANE_MIN_WIDTH * wide_count as f32
        + ui.spacing().item_spacing.x * wide_count.saturating_sub(1) as f32;
    let show_all_vitals = wide && wide_minimum <= vitals_width;
    vitals_ui.horizontal(|ui| {
        popup_rects = show_vital_strip(ui, &mut projection, show_all_vitals);
    });
    if let Some((button_rect, popup_rect)) = popup_rects {
        let escape =
            ui.input_mut(|input| input.consume_key(egui::Modifiers::NONE, egui::Key::Escape));
        let outside_click = ui.input(|input| {
            input.pointer.any_click()
                && input.pointer.interact_pos().is_some_and(|position| {
                    !button_rect.contains(position) && !popup_rect.contains(position)
                })
        });
        if escape || outside_click {
            projection.expansion.more_vitals = false;
        }
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
    let tabs_region =
        egui::Rect::from_min_size(tab_row.min, egui::vec2(tab_width, tab_row.height()));
    let freshness_width = if integrated {
        0.0
    } else {
        tabs_region.width().min(152.0)
    };
    let tabs_rect = egui::Rect::from_min_max(
        tabs_region.min,
        egui::pos2(tabs_region.right() - freshness_width, tabs_region.bottom()),
    );
    if !integrated {
        let freshness_rect = egui::Rect::from_min_max(
            egui::pos2(tabs_rect.right(), tabs_region.top()),
            tabs_region.max,
        );
        let mut freshness_ui = ui.new_child(
            UiBuilder::new()
                .max_rect(freshness_rect)
                .layout(Layout::right_to_left(Align::Center)),
        );
        freshness_ui.set_clip_rect(freshness_ui.clip_rect().intersect(freshness_rect));
        freshness_ui.label(freshness_text(projection.freshness));
    }
    let actions_rect = egui::Rect::from_min_max(
        egui::pos2(tabs_region.right() + gap, tab_row.top()),
        tab_row.max,
    );
    let mut tabs_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.tabs", window_id.0))
            .max_rect(tabs_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    tabs_ui.set_clip_rect(tabs_ui.clip_rect().intersect(tabs_rect));
    tabs_ui.horizontal(|ui| {
        let compact = tabs_rect.width() < 300.0;
        for tab in tabs {
            if !compact || *tab == detail.active_tab {
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
        }
        if compact && tabs.len() > 1 {
            ui.menu_button("More detail tabs", |ui| {
                for tab in tabs {
                    if *tab == detail.active_tab {
                        continue;
                    }
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
                        ui.close();
                    }
                }
            });
        }
    });
    let owner = projection.actions.verified_owner;
    let mut actions_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.actions", window_id.0))
            .max_rect(actions_rect)
            .layout(Layout::right_to_left(Align::Center)),
    );
    actions_ui.set_clip_rect(actions_ui.clip_rect().intersect(actions_rect));
    actions_ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
        ui.spacing_mut().item_spacing.x = 4.0;
        // Reference order (left to right): Scale, Restart, Actions,
        // Delete. Right-to-left rendering therefore paints Delete
        // first (rightmost), the overflow menu, then the primary
        // segment.
        content(
            ui,
            input.primary,
            Some(DetailActionSegment::Delete),
            &mut projection,
        );
        let compact = actions_rect.width() < if integrated { 300.0 } else { 340.0 };
        let namespace = projection.identity.namespace.as_deref();
        let uid = (!projection.identity.uid.is_empty()).then_some(projection.identity.uid.as_str());
        ui.menu_button(
            if compact {
                "More detail actions"
            } else {
                "Actions"
            },
            |ui| {
                if compact {
                    content(
                        ui,
                        input.primary,
                        Some(DetailActionSegment::Restart),
                        &mut projection,
                    );
                    ui.separator();
                }
                // Copy name moved out of the action row into the
                // overflow menu per the reference design.
                copy(ui, "Copy name", &projection.identity.name);
                if let Some(owner) = owner {
                    let label = format!("Open owner {}", owner.name);
                    if ui.button(&label).clicked() {
                        queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                            &super::presentation::owner_identity(projection.identity, owner),
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
            },
        );
        content(
            ui,
            input.primary,
            Some(if compact {
                DetailActionSegment::Scale
            } else {
                DetailActionSegment::Restart
            }),
            &mut projection,
        );
        if !compact {
            content(
                ui,
                input.primary,
                Some(DetailActionSegment::Scale),
                &mut projection,
            );
        }
    });
    ui.separator();
    let remaining = ui.available_rect_before_wrap();
    let footer_text = RichText::new(format!(
        "{} · Esc clear selection",
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
    if uses_shared_body_scroll(detail.active_tab) {
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
    }
    let mut show_body = |ui: &mut egui::Ui| {
        if input.gone {
            ui.label("This resource no longer exists");
        } else {
            content(ui, input.primary, None, &mut projection);
        }
    };
    if uses_shared_body_scroll(detail.active_tab) {
        ScrollArea::vertical()
            .id_salt(("k10s.detail.body.scroll", window_id.0, detail.active_tab))
            .max_height(body_rect.height())
            .show(&mut body_ui, &mut show_body);
    } else {
        body_ui.set_clip_rect(body_ui.clip_rect().intersect(body_rect));
        show_body(&mut body_ui);
    }
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

const VITAL_VALUE_MAX_CHARS: usize = 24;
const VITAL_CHIP_MAX_WIDTH: f32 = 184.0;
const VITAL_CHIP_SANE_MIN_WIDTH: f32 = 64.0;

struct VitalDisplay {
    visible: String,
    accessible: String,
    elided: bool,
}

fn vital_display(vital: &DetailVital) -> VitalDisplay {
    let compact = crate::ui::responsive_table::middle_elide(&vital.value, VITAL_VALUE_MAX_CHARS);
    let elided = compact != vital.value;
    let compose = |value: &str| match vital.shape {
        Some(shape) => format!("{} {} {value}", vital.label, shape.glyph()),
        None => format!("{} · {value}", vital.label),
    };
    VitalDisplay {
        visible: compact,
        accessible: compose(&vital.value),
        elided,
    }
}

/// The prominent value text of one vital chip, with its semantic shape
/// glyph when present.
fn vital_value_text(vital: &DetailVital, visible: &str) -> String {
    match vital.shape {
        Some(shape) => format!("{} {}", shape.glyph(), visible),
        None => visible.to_owned(),
    }
}

/// Tone-tinted chip fill: dark and neutral for plain values, subtly tinted
/// toward the semantic color for healthy/warning/danger vitals.
fn chip_fill(visuals: &egui::Visuals, tone: DetailVitalTone) -> egui::Color32 {
    chip_tint(visuals.faint_bg_color, vital_color(visuals, tone), 0.22)
}

fn chip_stroke(visuals: &egui::Visuals, tone: DetailVitalTone) -> egui::Stroke {
    egui::Stroke::new(
        1.0,
        chip_tint(
            visuals.widgets.noninteractive.bg_stroke.color,
            vital_color(visuals, tone),
            0.4,
        ),
    )
}

fn chip_tint(background: egui::Color32, tint: egui::Color32, amount: f32) -> egui::Color32 {
    let mix = |channel: u8, foreground: u8| {
        (f32::from(channel) * (1.0 - amount) + f32::from(foreground) * amount).round() as u8
    };
    egui::Color32::from_rgb(
        mix(background.r(), tint.r()),
        mix(background.g(), tint.g()),
        mix(background.b(), tint.b()),
    )
}

fn vital(ui: &mut egui::Ui, vital: &DetailVital, max_width: f32) {
    let display = vital_display(vital);
    // Reference chip anatomy: a small uppercase label plus a prominent
    // value inside a bounded chip, instead of one `Label · value` string.
    let painter = ui.ctx().layer_painter(ui.layer_id());
    let label_font = egui::FontId::new(9.5, egui::FontFamily::Monospace);
    let value_font = egui::FontId::new(12.0, egui::FontFamily::Monospace);
    let label_color = crate::ui::theme::MUTED_TEXT;
    let value_color = vital_color(ui.visuals(), vital.tone);
    let label_galley = painter.layout_no_wrap(vital.label.to_uppercase(), label_font, label_color);
    let value_galley = painter.layout_no_wrap(
        vital_value_text(vital, &display.visible),
        value_font,
        value_color,
    );
    let inner_gap = 6.0;
    let padding_x = 8.0;
    let natural_width = padding_x * 2.0 + label_galley.size().x + inner_gap + value_galley.size().x;
    let chip_width = natural_width.min(max_width.min(VITAL_CHIP_MAX_WIDTH));
    let chip_height = ui.spacing().interact_size.y;
    let (chip_rect, chip_response) =
        ui.allocate_exact_size(egui::vec2(chip_width, chip_height), Sense::hover());
    chip_response
        .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, display.accessible.clone()));
    if display.elided || natural_width > chip_width + 0.1 {
        chip_response.on_hover_text(display.accessible.clone());
    }
    let mut chip_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(chip_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    chip_ui.set_clip_rect(chip_ui.clip_rect().intersect(chip_rect));
    egui::Frame::new()
        .fill(chip_fill(chip_ui.visuals(), vital.tone))
        .stroke(chip_stroke(chip_ui.visuals(), vital.tone))
        .corner_radius(3.0)
        .show(&mut chip_ui, |ui| {
            let rect = ui.max_rect();
            let label_pos = egui::pos2(
                rect.left() + padding_x,
                rect.center().y - label_galley.size().y / 2.0,
            );
            let value_pos = egui::pos2(
                rect.left() + padding_x + label_galley.size().x + inner_gap,
                rect.center().y - value_galley.size().y / 2.0,
            );
            let painter = ui.ctx().layer_painter(ui.layer_id());
            painter.galley(label_pos, label_galley.clone(), label_color);
            painter.galley(value_pos, value_galley.clone(), value_color);
        });
}

fn show_vital_strip(
    ui: &mut egui::Ui,
    projection: &mut DetailFrameProjection<'_>,
    wide: bool,
) -> Option<(egui::Rect, egui::Rect)> {
    normalize_vital_expansion(wide, &mut projection.expansion);
    let count = projection.visible_vitals.len()
        + if wide {
            projection.overflow_vitals.len()
        } else {
            0
        };
    let gaps = ui.spacing().item_spacing.x * count.saturating_sub(1) as f32;
    let max_width = if wide && count > 0 {
        ((ui.available_width() - gaps) / count as f32).min(VITAL_CHIP_MAX_WIDTH)
    } else {
        VITAL_CHIP_MAX_WIDTH
    };
    for metric in &projection.visible_vitals {
        vital(ui, metric, max_width);
    }
    if wide {
        for metric in &projection.overflow_vitals {
            vital(ui, metric, max_width);
        }
    } else if let Some(kind) = projection.vital_expansion_label
        && !projection.overflow_vitals.is_empty()
    {
        let response = ui.button(if projection.expansion.more_vitals {
            format!("Hide more {kind} vitals")
        } else {
            format!("Show more {kind} vitals")
        });
        if response.clicked() {
            projection.expansion.more_vitals = !projection.expansion.more_vitals;
        }
        if projection.expansion.more_vitals {
            let popup = egui::Area::new(ui.id().with("vital overflow popover"))
                .order(egui::Order::Foreground)
                .fixed_pos(response.rect.left_bottom())
                .show(ui.ctx(), |ui| {
                    egui::Frame::popup(ui.style()).show(ui, |ui| {
                        ui.vertical(|ui| {
                            for metric in &projection.overflow_vitals {
                                vital(ui, metric, VITAL_CHIP_MAX_WIDTH);
                            }
                            if ui.button("Dismiss vital overflow").clicked() {
                                projection.expansion.more_vitals = false;
                            }
                        });
                    });
                });
            let semantics = ui.interact(
                popup.response.rect,
                ui.id().with(("vital overflow semantics", kind)),
                Sense::hover(),
            );
            semantics.widget_info(|| {
                WidgetInfo::labeled(
                    WidgetType::Other,
                    true,
                    format!("{kind} vital overflow popover"),
                )
            });
            return Some((response.rect, popup.response.rect));
        }
    }
    None
}

fn normalize_vital_expansion(wide: bool, expansion: &mut DetailExpansionState) {
    if wide {
        expansion.more_vitals = false;
    }
}

fn freshness_text(freshness: DetailFreshness<'_>) -> String {
    match freshness {
        DetailFreshness::Loading => "Freshness · loading".into(),
        DetailFreshness::Unavailable => "Freshness · unavailable".into(),
        DetailFreshness::Gone => "Freshness · gone".into(),
        DetailFreshness::Source(crate::ui::WindowFreshness::Live { last_sync_age }) => {
            format!("Freshness · live ({last_sync_age})")
        }
        DetailFreshness::Source(crate::ui::WindowFreshness::StaleRetrying { .. }) => {
            "Freshness · stale".into()
        }
        DetailFreshness::Source(crate::ui::WindowFreshness::Reconnecting { .. }) => {
            "Freshness · reconnecting".into()
        }
        DetailFreshness::Source(crate::ui::WindowFreshness::Forbidden { .. }) => {
            "Freshness · forbidden".into()
        }
        DetailFreshness::Source(crate::ui::WindowFreshness::Failed { .. }) => {
            "Freshness · failed".into()
        }
        DetailFreshness::Source(crate::ui::WindowFreshness::ReadyEmpty) => {
            "Freshness · ready".into()
        }
    }
}

const fn uses_shared_body_scroll(tab: DetailTab) -> bool {
    matches!(tab, DetailTab::Overview | DetailTab::Events)
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
    fn only_overview_and_events_use_the_shared_body_scroll_owner() {
        for tab in [DetailTab::Overview, DetailTab::Events] {
            assert!(super::uses_shared_body_scroll(tab));
        }
        for tab in [
            DetailTab::Ports,
            DetailTab::Pods,
            DetailTab::Yaml,
            DetailTab::Logs,
            DetailTab::Shell,
        ] {
            assert!(!super::uses_shared_body_scroll(tab));
        }
    }

    #[test]
    fn vital_display_elides_long_unicode_values_without_losing_full_accessible_text() {
        let vital = DetailVital::new(
            "Rollout",
            "正在部署一个非常非常长的版本名称-with-an-equally-long-suffix",
        );
        let display = super::vital_display(&vital);
        assert!(display.visible.chars().count() <= super::VITAL_VALUE_MAX_CHARS);
        assert!(display.visible.contains('…'));
        assert_eq!(
            display.accessible,
            "Rollout · 正在部署一个非常非常长的版本名称-with-an-equally-long-suffix"
        );
        assert!(display.elided);
    }

    #[test]
    fn crossing_wide_breakpoint_clears_transient_vital_popup_state() {
        let mut expansion = DetailExpansionState {
            more_vitals: true,
            ..DetailExpansionState::default()
        };
        super::normalize_vital_expansion(true, &mut expansion);
        assert!(!expansion.more_vitals);
        super::normalize_vital_expansion(false, &mut expansion);
        assert!(
            !expansion.more_vitals,
            "returning narrow must not reopen stale popup"
        );
    }

    #[test]
    fn vital_popup_state_is_scoped_to_pinned_identity_and_tab() {
        let mut first = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "web-0".into(),
            uid: "uid-web-0".into(),
        };
        let window = WindowId(9);
        let first_overview = super::expansion_id(window, &first, DetailTab::Overview);
        first.uid = "uid-web-1".into();
        first.name = "web-1".into();
        assert_ne!(
            first_overview,
            super::expansion_id(window, &first, DetailTab::Overview)
        );
        assert_ne!(
            super::expansion_id(window, &first, DetailTab::Overview),
            super::expansion_id(window, &first, DetailTab::Events)
        );
    }

    #[test]
    fn every_freshness_state_has_one_stable_identity_label() {
        use crate::ui::WindowFreshness;

        assert_eq!(
            super::freshness_text(DetailFreshness::Loading),
            "Freshness · loading"
        );
        assert_eq!(
            super::freshness_text(DetailFreshness::Unavailable),
            "Freshness · unavailable"
        );
        assert_eq!(
            super::freshness_text(DetailFreshness::Gone),
            "Freshness · gone"
        );
        assert_eq!(
            super::freshness_text(DetailFreshness::Source(&WindowFreshness::Live {
                last_sync_age: "2s".into(),
            })),
            "Freshness · live (2s)"
        );
        assert_eq!(
            super::freshness_text(DetailFreshness::Source(&WindowFreshness::StaleRetrying {
                last_sync_age: "8s".into(),
                attempt: 2,
                retry_in: "1s".into(),
            },)),
            "Freshness · stale"
        );
    }

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
                    now: web_time::UNIX_EPOCH,
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
                    true,
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
                        if actions.is_none() {
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
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Clear selection")
            .hover();
        harness.run_steps(2);
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "Clear selection");
    }

    #[test]
    fn deployment_vitals_are_bounded_at_exact_breakpoints_and_popup_is_owned() {
        const LONG: &str = "正在部署一个非常非常长的版本名称-with-an-equally-long-suffix";
        const LONG_ASCII: &str =
            "ThisIsAnExtremelyLongReadyValueThatMustNeverEscapeTheVitalChipBoundary";
        for width in [760.0, 1000.0] {
            let identity = ResourceIdentity {
                context: "dev-local".into(),
                gvk: GroupVersionKind {
                    group: "apps".into(),
                    version: "v1".into(),
                    kind: "Deployment".into(),
                },
                namespace: Some("default".into()),
                name: "web".into(),
                uid: "uid-web".into(),
            };
            let detail = DetailState::new(identity.clone());
            let mut harness = Harness::builder()
                .with_size(egui::vec2(width + 16.0, 360.0))
                .build_ui(move |ui| {
                    let input = DetailPresentationInput {
                        identity: &identity,
                        primary: DetailPrimary::Loading,
                        metrics: DetailMetrics {
                            status: None,
                            age: None,
                        },
                        resource_metrics: None,
                        relations: None,
                        freshness: None,
                        now: web_time::UNIX_EPOCH,
                        gone: false,
                        mutations_allowed: false,
                        port_forward_available: false,
                        port_forward_sessions: &[],
                        port_forward_error: None,
                    };
                    show(
                        ui,
                        WindowId(width as u64),
                        &detail,
                        &input,
                        false,
                        false,
                        &[],
                        &mut Vec::new(),
                        |projection| {
                            projection.visible_vitals = vec![
                                DetailVital::new("Rollout", LONG),
                                DetailVital::new("Ready", LONG_ASCII),
                                DetailVital::new("Up-to-date", "3"),
                                DetailVital::new("Available", "3"),
                            ];
                            projection.overflow_vitals = vec![
                                DetailVital::new("Strategy", "RollingUpdate"),
                                DetailVital::new("Age", "2h"),
                            ];
                            projection.vital_expansion_label = Some("Deployment");
                        },
                        |_, _, _, _| {},
                    );
                });
            harness.run_steps(2);
            assert!(
                (harness.get_by_label("Detail vital strip").rect().width() - width).abs() < 0.1
            );
            let rollout_label = format!("Rollout · {LONG}");
            harness.get_by_label(&rollout_label);
            let ready_label = format!("Ready · {LONG_ASCII}");
            harness.get_by_label(&ready_label);
            harness.get_by_label("Strategy · RollingUpdate");
            let strip = harness.get_by_label("Detail vital strip").rect();
            for accessible in [
                rollout_label,
                ready_label,
                "Up-to-date · 3".to_owned(),
                "Available · 3".to_owned(),
                "Strategy · RollingUpdate".to_owned(),
                "Age · 2h".to_owned(),
            ] {
                let chip = harness.get_by_label(&accessible).rect();
                assert!(chip.width() <= super::VITAL_CHIP_MAX_WIDTH + 0.1);
                assert!(
                    strip.contains_rect(chip),
                    "required chip escaped 760pt strip"
                );
            }
            assert!(
                harness
                    .query_by_role_and_label(
                        egui::accesskit::Role::Button,
                        "Show more Deployment vitals",
                    )
                    .is_none()
            );
        }

        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let detail = DetailState::new(identity.clone());
        let mut harness = Harness::builder()
            .with_size(egui::vec2(656.0, 360.0))
            .build_ui(move |ui| {
                let input = DetailPresentationInput {
                    identity: &identity,
                    primary: DetailPrimary::Loading,
                    metrics: DetailMetrics {
                        status: None,
                        age: None,
                    },
                    resource_metrics: None,
                    relations: None,
                    freshness: None,
                    now: web_time::UNIX_EPOCH,
                    gone: false,
                    mutations_allowed: false,
                    port_forward_available: false,
                    port_forward_sessions: &[],
                    port_forward_error: None,
                };
                show(
                    ui,
                    WindowId(640),
                    &detail,
                    &input,
                    false,
                    false,
                    &[],
                    &mut Vec::new(),
                    |projection| {
                        projection.visible_vitals = vec![
                            DetailVital::new("Status", LONG),
                            DetailVital::new("Ready", "1/1"),
                            DetailVital::new("Restarts", "0"),
                            DetailVital::new("Age", "2h"),
                        ];
                        projection.overflow_vitals = vec![
                            DetailVital::new("Node", "worker-a"),
                            DetailVital::new("Pod IP", "10.0.0.2"),
                        ];
                        projection.vital_expansion_label = Some("Pod");
                    },
                    |_, _, _, _| {},
                );
            });
        harness.run_steps(2);
        let strip_height = harness.get_by_label("Detail vital strip").rect().height();
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals")
            .click();
        harness.run_steps(2);
        harness.get_by_label("Pod vital overflow popover");
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "Hide more Pod vitals");
        let node = harness.get_by_label("Node · worker-a").rect();
        let ip = harness.get_by_label("Pod IP · 10.0.0.2").rect();
        assert!(node.top() < ip.top(), "overflow declaration order changed");
        assert_eq!(
            harness.get_by_label("Detail vital strip").rect().height(),
            strip_height
        );
        harness.key_press(egui::Key::Escape);
        harness.run_steps(2);
        assert!(
            harness
                .query_by_label("Pod vital overflow popover")
                .is_none()
        );
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals");
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals")
            .click();
        harness.run_steps(2);
        harness
            .get_by_label(
                "l logs · s shell · y yaml · e events · c copy name · Esc clear selection",
            )
            .click();
        harness.run_steps(2);
        assert!(
            harness
                .query_by_label("Pod vital overflow popover")
                .is_none()
        );
        harness.get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals");
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals")
            .click();
        harness.run_steps(2);
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Dismiss vital overflow")
            .click();
        harness.run_steps(2);
        assert!(
            harness
                .query_by_label("Pod vital overflow popover")
                .is_none()
        );
    }
}
