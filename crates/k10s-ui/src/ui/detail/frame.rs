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
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let clear = ui.button("×").on_hover_text("Clear selection");
                clear.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Button, true, "Clear selection")
                });
                if clear.clicked() {
                    queued.push(WorkspaceCommand::ClearSelection(window_id));
                }
                if !input.gone {
                    // Icon-only: the accessible label carries the meaning
                    // so the control costs one glyph of chrome width.
                    let accessible = if detail_maximized {
                        "Restore split"
                    } else {
                        "Maximize"
                    };
                    let maximize = ui.button("⛶");
                    maximize
                        .widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible));
                    let maximize = maximize.on_hover_text(accessible);
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
                // Reference placement: the freshness badge sits at the right
                // of the identity row, just left of `Pop out`. Running text
                // (`Freshness · live (just now)`) next to the name costs the
                // row width it does not have; the full sentence stays as the
                // accessible label and the tooltip.
                let full_freshness = freshness_text(projection.freshness);
                let badge = ui.label(
                    RichText::new(format!(
                        "⟳ {}",
                        compact_freshness_text(projection.freshness).to_lowercase()
                    ))
                    .color(freshness_color(projection.freshness)),
                );
                badge.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, full_freshness.clone())
                });
                badge.on_hover_text(full_freshness);
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
    let row_width = ui.available_width();
    let visible_row_width = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width();
    let row_height = ui.spacing().interact_size.y;
    let gap = ui.spacing().item_spacing.x;
    let usable_width = (visible_row_width - gap).max(0.0);
    // Chrome budget reserved for the freshness label. The painted text is the
    // compact `● Live` form, but the budget keeps the committed thresholds so
    // the wide/compact/stacked chrome decisions do not move.
    let full_freshness_width = if integrated { 0.0 } else { 152.0 };
    let compact_freshness_width = if integrated { 0.0 } else { 64.0 };
    let action_count = usize::from(projection.actions.can_scale)
        + usize::from(projection.actions.can_restart)
        + usize::from(projection.actions.can_delete)
        + 1;
    // Budgeted from the accessible name: the `⋯`/`▾` decoration around it
    // must not push the row into the compact layout on its own.
    let wide_action_width = menu_button_width(ui, "Actions")
        + if projection.actions.can_scale {
            button_width(ui, "Scale…")
        } else {
            0.0
        }
        + if projection.actions.can_restart {
            button_width(ui, "Restart…")
        } else {
            0.0
        }
        + if projection.actions.can_delete {
            button_width(ui, "Delete…")
        } else {
            0.0
        }
        + gap * action_count.saturating_sub(1) as f32;
    let compact_action_count =
        usize::from(projection.actions.can_scale) + usize::from(projection.actions.can_delete) + 1;
    let compact_action_width = menu_button_width(ui, "More")
        + if projection.actions.can_scale {
            button_width(ui, "Scale…")
        } else {
            0.0
        }
        + if projection.actions.can_delete {
            button_width(ui, "Delete…")
        } else {
            0.0
        }
        + gap * compact_action_count.saturating_sub(1) as f32;
    let wide_tabs_width = tabs
        .iter()
        .map(|tab| {
            button_width(ui, super::tab_label(*tab))
                + if *tab == DetailTab::Pods {
                    projection
                        .pod_count
                        .map_or(0.0, |count| button_width(ui, &format!(" {count}")))
                } else {
                    0.0
                }
        })
        .sum::<f32>()
        + gap * tabs.len().saturating_sub(1) as f32
        + full_freshness_width;
    let compact_tabs_width = button_width(ui, super::tab_label(detail.active_tab))
        + menu_button_width(ui, "More")
        + gap
        + compact_freshness_width;
    let compact_chrome = wide_tabs_width + gap + wide_action_width > usable_width;
    let stacked_chrome =
        compact_chrome && compact_tabs_width + gap + compact_action_width > usable_width;
    let chrome_height = if stacked_chrome {
        row_height * 2.0 + gap
    } else {
        row_height
    };
    let (chrome_rect, _) =
        ui.allocate_exact_size(egui::vec2(row_width, chrome_height), Sense::hover());
    let visible_chrome_rect = chrome_rect.intersect(ui.clip_rect());
    let (tabs_region, actions_rect) = if stacked_chrome {
        (
            egui::Rect::from_min_size(
                visible_chrome_rect.min,
                egui::vec2(visible_chrome_rect.width(), row_height),
            ),
            egui::Rect::from_min_size(
                egui::pos2(
                    visible_chrome_rect.left(),
                    visible_chrome_rect.top() + row_height + gap,
                ),
                egui::vec2(visible_chrome_rect.width(), row_height),
            ),
        )
    } else {
        let desired_action_width = if compact_chrome {
            compact_action_width
        } else {
            wide_action_width
        };
        let reserved_tabs_width = if compact_chrome {
            compact_tabs_width
        } else {
            wide_tabs_width
        };
        let action_width = desired_action_width.min((usable_width - reserved_tabs_width).max(0.0));
        let tab_width = usable_width - action_width;
        (
            egui::Rect::from_min_size(visible_chrome_rect.min, egui::vec2(tab_width, row_height)),
            egui::Rect::from_min_max(
                egui::pos2(
                    visible_chrome_rect.left() + tab_width + gap,
                    visible_chrome_rect.top(),
                ),
                visible_chrome_rect.max,
            ),
        )
    };
    let freshness_width = if compact_chrome {
        compact_freshness_width
    } else {
        full_freshness_width
    }
    .min(tabs_region.width());
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
        let full_freshness = freshness_text(projection.freshness);
        let freshness_display = format!("● {}", compact_freshness_text(projection.freshness));
        let freshness = freshness_ui
            .label(freshness_display)
            .on_hover_text(full_freshness.clone());
        freshness
            .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, full_freshness.clone()));
    }
    let mut tabs_ui = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.detail.tabs", window_id.0))
            .max_rect(tabs_rect)
            .layout(Layout::left_to_right(Align::Center)),
    );
    tabs_ui.set_clip_rect(tabs_ui.clip_rect().intersect(tabs_rect));
    let tabs_semantics = ui.interact(
        tabs_rect,
        ui.id().with(("k10s.detail.tabs.row", window_id.0)),
        Sense::hover(),
    );
    tabs_semantics.widget_info(|| WidgetInfo::labeled(WidgetType::Other, true, "Detail tabs row"));
    tabs_ui.horizontal(|ui| {
        let compact = compact_chrome;
        for tab in tabs {
            if !compact || *tab == detail.active_tab {
                let active = *tab == detail.active_tab;
                let response = ui.selectable_label(active, tab_text(ui, *tab, &projection));
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
            let menu = ui.menu_button("More", |ui| {
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
            menu.response
                .widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, "More detail tabs"));
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
    let actions_semantics = ui.interact(
        actions_rect,
        ui.id().with(("k10s.detail.actions.row", window_id.0)),
        Sense::hover(),
    );
    actions_semantics
        .widget_info(|| WidgetInfo::labeled(WidgetType::Other, true, "Detail actions row"));
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
        let compact = compact_chrome;
        let namespace = projection.identity.namespace.as_deref();
        let uid = (!projection.identity.uid.is_empty()).then_some(projection.identity.uid.as_str());
        // The overflow marker and the disclosure arrow are part of the
        // label; `Actions` stays the accessible name.
        let menu_label: WidgetText = if compact {
            "More".into()
        } else {
            icon(action_menu_label()).into()
        };
        let action_menu = ui.menu_button(menu_label, |ui| {
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
        });
        action_menu.response.widget_info(|| {
            WidgetInfo::labeled(
                WidgetType::Button,
                true,
                if compact {
                    "More detail actions"
                } else {
                    "Actions"
                },
            )
        });
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
    let mut shortcuts: Vec<&str> = projection.shortcut_labels.to_vec();
    if projection.delete_shortcut {
        shortcuts.push("Ctrl+D delete");
    }
    shortcuts.push("Esc clear selection");
    let footer_plain = shortcuts.join(" · ");
    let footer_job = shortcut_footer_job(ui, &shortcuts);
    let footer_galley = WidgetText::from(footer_job.clone()).into_galley(
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
    let footer = footer_ui.add(egui::Label::new(footer_job).wrap());
    footer.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, footer_plain.clone()));
    ui.ctx().data_mut(|data| {
        data.insert_temp(expansion_id, projection.expansion);
    });
}

/// `**p** pods · **y** yaml …`: the key is the actionable half of a
/// shortcut hint, so it reads at full strength while the verb stays muted.
fn shortcut_footer_job(ui: &egui::Ui, shortcuts: &[&str]) -> egui::text::LayoutJob {
    let body = egui::TextStyle::Body.resolve(ui.style());
    let key_font = egui::FontId {
        family: body.family.clone(),
        size: body.size,
    };
    let key_color = crate::ui::theme::ACCENT;
    let verb_color = ui.visuals().weak_text_color();
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = f32::INFINITY;
    for (index, shortcut) in shortcuts.iter().enumerate() {
        if index > 0 {
            job.append(
                " · ",
                0.0,
                egui::TextFormat::simple(body.clone(), verb_color),
            );
        }
        match shortcut.split_once(' ') {
            Some((key, verb)) => {
                job.append(
                    key,
                    0.0,
                    egui::TextFormat {
                        font_id: key_font.clone(),
                        color: key_color,
                        ..Default::default()
                    },
                );
                job.append(
                    &format!(" {verb}"),
                    0.0,
                    egui::TextFormat::simple(body.clone(), verb_color),
                );
            }
            None => job.append(
                shortcut,
                0.0,
                egui::TextFormat::simple(body.clone(), verb_color),
            ),
        }
    }
    job
}

const VITAL_VALUE_MAX_CHARS: usize = 24;
const VITAL_CHIP_MAX_WIDTH: f32 = 184.0;
const VITAL_CHIP_SANE_MIN_WIDTH: f32 = 64.0;
/// The one-glyph overflow toggle at the end of the vital strip. `⋯` keeps
/// the strip from wrapping; the button's accessible label spells it out.
const VITAL_OVERFLOW_GLYPH: &str = "⋯";
/// The disclosure arrow of the action menu. Like `⋯`, `▾` (U+25BE) is
/// covered only by the bundled monospace family, so both are painted with
/// [`icon`] rather than the proportional button font.
const MENU_ARROW_GLYPH: &str = "▾";

/// Symbol text painted in the monospace family: the default proportional
/// fonts do not cover the geometric icons and would paint a blank box.
fn icon(text: impl Into<String>) -> RichText {
    RichText::new(text).family(egui::FontFamily::Monospace)
}

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
    chip_tint(
        crate::ui::theme::CHIP_BACKGROUND,
        vital_color(visuals, tone),
        0.22,
    )
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
    let truncated = display.elided || natural_width > chip_width + 0.1;
    match (&vital.hint, truncated) {
        (Some(hint), true) => {
            chip_response.on_hover_text(format!("{}\n{hint}", display.accessible));
        }
        (Some(hint), false) => {
            chip_response.on_hover_text(hint.clone());
        }
        (None, true) => {
            chip_response.on_hover_text(display.accessible.clone());
        }
        (None, false) => {}
    }
    if !ui.is_rect_visible(chip_rect) {
        return;
    }
    // The border and fill are painted directly. An `egui::Frame` sizes
    // itself from what its content allocates, and this chip only paints
    // galleys, so a Frame collapsed to a degenerate rectangle and the chip
    // read as plain `label · value` text.
    // The chip clip also keeps a long value from bleeding into its neighbor.
    let painter = ui
        .painter()
        .with_clip_rect(chip_rect.intersect(ui.clip_rect()));
    painter.rect(
        chip_rect,
        3.0,
        chip_fill(ui.visuals(), vital.tone),
        chip_stroke(ui.visuals(), vital.tone),
        egui::StrokeKind::Inside,
    );
    let label_pos = egui::pos2(
        chip_rect.left() + padding_x,
        chip_rect.center().y - label_galley.size().y / 2.0,
    );
    let value_pos = egui::pos2(
        chip_rect.left() + padding_x + label_galley.size().x + inner_gap,
        chip_rect.center().y - value_galley.size().y / 2.0,
    );
    painter.galley(label_pos, label_galley, label_color);
    painter.galley(value_pos, value_galley, value_color);
}

/// The painted tab label. `Pods` carries a yellow count badge once the
/// related-Pod count is known, so the tab row states the size of the thing
/// it links to.
fn tab_text(ui: &egui::Ui, tab: DetailTab, projection: &DetailFrameProjection<'_>) -> WidgetText {
    let label = super::tab_label(tab);
    let Some(count) = projection.pod_count.filter(|_| tab == DetailTab::Pods) else {
        return label.into();
    };
    let font = egui::TextStyle::Button.resolve(ui.style());
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = f32::INFINITY;
    job.append(
        label,
        0.0,
        egui::TextFormat::simple(font.clone(), ui.visuals().text_color()),
    );
    job.append(
        &format!(" {count}"),
        0.0,
        egui::TextFormat::simple(font, crate::ui::theme::WARNING),
    );
    job.into()
}

fn show_vital_strip(
    ui: &mut egui::Ui,
    projection: &mut DetailFrameProjection<'_>,
    wide: bool,
) -> Option<(egui::Rect, egui::Rect)> {
    normalize_vital_expansion(wide, &mut projection.expansion);
    let narrow_visible_count = projection.visible_vitals.len().min(2);
    let visible_count = if wide {
        projection.visible_vitals.len()
    } else {
        narrow_visible_count
    };
    let count = visible_count
        + if wide {
            projection.overflow_vitals.len()
        } else {
            0
        };
    let gaps = ui.spacing().item_spacing.x * count.saturating_sub(1) as f32;
    let max_width = if wide && count > 0 {
        ((ui.available_width() - gaps) / count as f32).min(VITAL_CHIP_MAX_WIDTH)
    } else if count > 0 {
        let overflow_width = projection.vital_expansion_label.map_or(0.0, |_| {
            WidgetText::from(icon(VITAL_OVERFLOW_GLYPH))
                .into_galley(
                    ui,
                    Some(egui::TextWrapMode::Extend),
                    f32::INFINITY,
                    egui::TextStyle::Button,
                )
                .size()
                .x
                + ui.spacing().button_padding.x * 2.0
                + ui.spacing().item_spacing.x
        });
        ((ui.available_width() - overflow_width - gaps) / count as f32)
            .clamp(VITAL_CHIP_SANE_MIN_WIDTH, VITAL_CHIP_MAX_WIDTH)
    } else {
        VITAL_CHIP_MAX_WIDTH
    };
    for metric in projection.visible_vitals.iter().take(visible_count) {
        vital(ui, metric, max_width);
    }
    if wide {
        for metric in &projection.overflow_vitals {
            vital(ui, metric, max_width);
        }
    } else if let Some(kind) = projection.vital_expansion_label
        && (projection.visible_vitals.len() > narrow_visible_count
            || !projection.overflow_vitals.is_empty())
    {
        // The strip must not wrap or clip, so the toggle stays one glyph
        // wide; the spoken label keeps the full sentence.
        let accessible = if projection.expansion.more_vitals {
            format!("Hide more {kind} vitals")
        } else {
            format!("Show more {kind} vitals")
        };
        // A ghost button: transparent fill, one muted glyph, so the
        // toggle reads as an affordance without competing with the chips.
        let response = ui.add(
            egui::Button::new(icon(VITAL_OVERFLOW_GLYPH).color(crate::ui::theme::MUTED_TEXT))
                .fill(egui::Color32::TRANSPARENT),
        );
        response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible.clone()));
        let response = response.on_hover_text(accessible);
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
                            for metric in
                                projection.visible_vitals.iter().skip(narrow_visible_count)
                            {
                                vital(ui, metric, VITAL_CHIP_MAX_WIDTH);
                            }
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

/// Live feeds read green; every recovery state keeps the muted tone so the
/// badge never claims health it does not have.
fn freshness_color(freshness: DetailFreshness<'_>) -> egui::Color32 {
    match freshness {
        DetailFreshness::Source(crate::ui::WindowFreshness::Live { .. }) => {
            crate::ui::theme::HEALTHY
        }
        DetailFreshness::Gone | DetailFreshness::Unavailable => crate::ui::theme::DANGER,
        _ => crate::ui::theme::MUTED_TEXT,
    }
}

fn compact_freshness_text(freshness: DetailFreshness<'_>) -> &'static str {
    match freshness {
        DetailFreshness::Loading => "Loading",
        DetailFreshness::Unavailable => "Unavail.",
        DetailFreshness::Gone => "Gone",
        DetailFreshness::Source(crate::ui::WindowFreshness::Live { .. }) => "Live",
        DetailFreshness::Source(crate::ui::WindowFreshness::StaleRetrying { .. }) => "Stale",
        DetailFreshness::Source(crate::ui::WindowFreshness::Reconnecting { .. }) => "Reconn.",
        DetailFreshness::Source(crate::ui::WindowFreshness::Forbidden { .. }) => "Denied",
        DetailFreshness::Source(crate::ui::WindowFreshness::Failed { .. }) => "Failed",
        DetailFreshness::Source(crate::ui::WindowFreshness::ReadyEmpty) => "Ready",
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

fn button_width(ui: &egui::Ui, label: &str) -> f32 {
    WidgetText::from(label)
        .into_galley(
            ui,
            Some(egui::TextWrapMode::Extend),
            f32::INFINITY,
            egui::TextStyle::Button,
        )
        .size()
        .x
        + ui.spacing().button_padding.x * 2.0
}

fn menu_button_width(ui: &egui::Ui, label: &str) -> f32 {
    button_width(ui, label) + ui.spacing().icon_width + ui.spacing().icon_spacing
}

/// The reference action-row overflow label: the `⋯` overflow marker and the
/// `▾` disclosure arrow are decoration around the accessible name, which
/// stays `Actions`.
fn action_menu_label() -> String {
    format!("{VITAL_OVERFLOW_GLYPH} Actions {MENU_ARROW_GLYPH}")
}

#[cfg(test)]
mod tests {
    use egui_kittest::{Harness, kittest::Queryable as _};
    use k10s_protocol::{GroupVersionKind, ResourceIdentity};

    use super::*;
    use crate::ui::detail::presentation::{
        DetailMetrics, DetailVital, DetailVitalShape, DetailVitalTone,
    };

    /// Every icon-only control carries its whole meaning in one glyph, so a
    /// glyph the bundled fonts lack paints as a blank box (as `✕` U+2715
    /// did). `Fonts::has_glyph` answers from the family's face cache and
    /// reports false negatives for plain ASCII, so coverage is probed
    /// through the advance width: a character no face in the family owns
    /// resolves to the replacement face and measures 0.
    ///
    /// The default proportional family covers far fewer symbols than the
    /// bundled monospace one, which is why the geometric icons (`⋯`, `▾`,
    /// `●`, `▲`, `⨯`) are painted in the monospace family.
    #[test]
    fn icon_only_chrome_glyphs_exist_in_the_fonts_that_paint_them() {
        let mut harness = Harness::new_ui(|ui| {
            ui.label("glyph probe");
        });
        harness.run_steps(2);
        let proportional = egui::FontId::proportional(12.0);
        let monospace = egui::FontId::monospace(12.0);
        harness.ctx.fonts_mut(|fonts| {
            let mut covered = |font: &egui::FontId, glyph: &str| {
                glyph
                    .chars()
                    .all(|character| fonts.glyph_width(font, character) > 0.0)
            };
            assert!(
                !covered(&proportional, "\u{FFFD}"),
                "probe assumes an uncovered character measures 0"
            );
            // Painted with the proportional button/label font.
            for glyph in ["⟳", "⛶", "↗", "×"] {
                assert!(
                    covered(&proportional, glyph),
                    "chrome glyph {glyph:?} is missing from the proportional font"
                );
            }
            // Painted with the monospace family: the vital chips, the
            // overflow toggle, and the action-menu affordances.
            for glyph in [
                VITAL_OVERFLOW_GLYPH,
                MENU_ARROW_GLYPH,
                DetailVitalShape::Dot.glyph(),
                DetailVitalShape::Triangle.glyph(),
                DetailVitalShape::Cross.glyph(),
            ] {
                assert!(
                    covered(&monospace, glyph),
                    "chrome glyph {glyph:?} is missing from the monospace font"
                );
            }
        });
    }

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
    fn compact_long_freshness_labels_fit_the_reserved_narrow_budget() {
        use crate::ui::WindowFreshness;

        let states = [
            DetailFreshness::Unavailable,
            DetailFreshness::Source(&WindowFreshness::Reconnecting {
                last_sync_age: "8s".into(),
                attempt: 2,
                retry_in: "1s".into(),
            }),
            DetailFreshness::Source(&WindowFreshness::Forbidden {
                user: "alice".into(),
                verb: "get".into(),
                resource: "pods".into(),
                scope: "default".into(),
            }),
        ];
        for state in states {
            let painted = super::compact_freshness_text(state);
            assert!(
                painted.chars().count() <= 8,
                "compact freshness {painted:?} exceeds the 64px text budget"
            );
            assert!(
                super::freshness_text(state).starts_with("Freshness · "),
                "full accessible freshness must remain stable"
            );
        }
    }

    #[test]
    fn semantic_vital_shapes_are_visible_and_tones_use_theme_palette() {
        assert_eq!(DetailVitalShape::Dot.glyph(), "●");
        assert_eq!(DetailVitalShape::Triangle.glyph(), "▲");
        assert_eq!(DetailVitalShape::Cross.glyph(), "⨯");

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
                            hint: None,
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

        harness.get_by_label("Status ⨯ Configured");
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
