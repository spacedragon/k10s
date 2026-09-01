//! Fixed shared detail chrome: identity, vitals, controls, tabs, one body
//! scroll region, and a footer. Kind modules only supply the body content.

use egui::containers::scroll_area::ScrollBarVisibility;
use egui::{
    Align, Layout, RichText, ScrollArea, Sense, UiBuilder, WidgetInfo, WidgetText, WidgetType,
};

use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{
    DetailExpansionState, DetailFrameProjection, DetailFreshness, DetailPresentationInput,
    DetailPrimary, DetailVital, DetailVitalTone,
};
use crate::ui::resource_window::RowIdentity;

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
    mut content: impl FnMut(&mut egui::Ui, DetailPrimary<'_>, bool, &mut DetailFrameProjection<'_>),
) {
    let expansion_id = expansion_id(window_id, input.identity, detail.active_tab);
    let expansion = ui
        .ctx()
        .data_mut(|data| data.get_temp::<DetailExpansionState>(expansion_id))
        .unwrap_or_default();
    let mut projection = input.frame_projection(expansion);
    configure(&mut projection);
    if integrated {
        let identity_row = ui.horizontal(|ui| {
            let title = title(projection.identity);
            let heading = ui.label(RichText::new(&title).strong().heading());
            ui.ctx().accesskit_node_builder(heading.id, |node| {
                node.set_role(egui::accesskit::Role::Heading);
                node.set_label(title);
            });
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let clear = ui.button("×");
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
                ui.label(freshness_text(projection.freshness));
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
    if wide && wide_minimum <= vitals_width {
        show_vital_strip(&mut vitals_ui, &mut projection, true);
    } else {
        ScrollArea::horizontal()
            .id_salt(("k10s.detail.vitals.scroll", window_id.0))
            .scroll_bar_visibility(ScrollBarVisibility::AlwaysHidden)
            .stick_to_right(true)
            .show(&mut vitals_ui, |ui| {
                ui.horizontal(|ui| {
                    popup_rects = show_vital_strip(ui, &mut projection, wide);
                });
            });
    }
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
            content(ui, input.primary, false, &mut projection);
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

fn vital(ui: &mut egui::Ui, vital: &DetailVital, max_width: f32) {
    let display = vital_display(vital);
    let visible = match vital.shape {
        Some(shape) => format!("{} {} {}", vital.label, shape.glyph(), display.visible),
        None => format!("{} · {}", vital.label, display.visible),
    };
    let text = RichText::new(visible).color(vital_color(ui.visuals(), vital.tone));
    let natural = WidgetText::from(text.clone()).into_galley(
        ui,
        Some(egui::TextWrapMode::Extend),
        f32::INFINITY,
        egui::TextStyle::Body,
    );
    let chip_width = (natural.size().x + 12.0).min(max_width.min(VITAL_CHIP_MAX_WIDTH));
    let chip_height = ui.spacing().interact_size.y;
    let (chip_rect, _) =
        ui.allocate_exact_size(egui::vec2(chip_width, chip_height), Sense::hover());
    let mut chip_ui = ui.new_child(
        UiBuilder::new()
            .max_rect(chip_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    chip_ui.set_clip_rect(chip_ui.clip_rect().intersect(chip_rect));
    egui::Frame::new()
        .fill(chip_ui.visuals().faint_bg_color)
        .stroke(chip_ui.visuals().widgets.noninteractive.bg_stroke)
        .corner_radius(4.0)
        .inner_margin(egui::Margin::symmetric(6, 1))
        .show(&mut chip_ui, |ui| {
            ui.set_width((chip_width - 12.0).max(0.0));
            let response = ui.add(egui::Label::new(text).wrap_mode(egui::TextWrapMode::Truncate));
            response.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Label, true, display.accessible.clone())
            });
            if display.elided || natural.size().x + 12.0 > chip_width {
                response.on_hover_text(display.accessible.clone());
            }
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
        let response = ui.button(format!("Show more {kind} vitals"));
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
        harness
            .get_by_role_and_label(egui::accesskit::Role::Button, "Show more Pod vitals")
            .click();
        harness.run_steps(2);
        harness
            .get_by_label(
                "Shortcuts: l logs · s shell · y yaml · e events · c copy name · Esc restore/close",
            )
            .click();
        harness.run_steps(2);
        assert!(
            harness
                .query_by_label("Pod vital overflow popover")
                .is_none()
        );
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
