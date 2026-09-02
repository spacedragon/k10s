//! The Overview tab: backend-resolved labeled sections.

use egui::{Grid, RichText, WidgetInfo, WidgetType};

use k10s_protocol::DetailSection;

use crate::workspace::WindowId;

pub(super) const WIDE_BODY_WIDTH: f32 = 760.0;
/// Horizontal padding inside the configuration (right) column, in points.
pub(super) const CONFIGURATION_PADDING_LEFT: i8 = 12;
pub(super) const CONFIGURATION_PADDING_RIGHT: i8 = 8;

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
    // Reference body: a fixed 1.35 : 1 split (tables left, KV right). Each
    // column clips its own content so long values never bleed across the
    // gutter, and both are painted in one row so neither pushes the other.
    ui.horizontal_top(|ui| {
        column(ui, left, "Operational detail column", false, operational);
        column(
            ui,
            right,
            "Configuration detail column",
            true,
            configuration,
        );
    });
    true
}

fn column(
    ui: &mut egui::Ui,
    width: f32,
    accessible: &'static str,
    secondary: bool,
    content: impl FnOnce(&mut egui::Ui),
) {
    ui.allocate_ui_with_layout(
        egui::vec2(width, 0.0),
        egui::Layout::top_down(egui::Align::Min),
        |ui| {
            let bounds = column_bounds(ui);
            ui.set_clip_rect(bounds);
            ui.set_min_width(width);
            ui.set_max_width(width);
            // Reserved before the content so the column ground and the
            // divider paint behind it rather than over it.
            let background = secondary.then(|| ui.painter().add(egui::Shape::Noop));
            let marker =
                ui.allocate_response(egui::vec2(ui.available_width(), 1.0), egui::Sense::hover());
            marker.widget_info(|| WidgetInfo::labeled(WidgetType::Other, true, accessible));
            if secondary {
                // Reference `.col` padding: the KV column keeps clear of the
                // divider on its left and of the window edge on its right.
                egui::Frame::new()
                    .inner_margin(egui::Margin {
                        left: CONFIGURATION_PADDING_LEFT,
                        right: CONFIGURATION_PADDING_RIGHT,
                        top: 0,
                        bottom: 0,
                    })
                    .show(ui, |ui| content(ui));
            } else {
                content(ui);
            }
            if let Some(slot) = background {
                // Reference `.col + .col`: a slightly darker ground and a
                // 1px rule in the gutter, so the KV column reads as its own
                // surface instead of continuing the tables.
                // The painter is clipped to this column, so the rule has to
                // sit on the column's own first pixel column: anything left
                // of `bounds` (in the gutter) is clipped away and invisible.
                let rect = bounds;
                let divider = ui.painter().round_to_pixel_center(rect.left() + 0.5);
                ui.painter().set(
                    slot,
                    egui::Shape::Vec(vec![
                        egui::Shape::rect_filled(rect, 0.0, crate::ui::theme::PANEL_BACKGROUND),
                        egui::Shape::line_segment(
                            [
                                egui::pos2(divider, rect.top()),
                                egui::pos2(divider, rect.bottom()),
                            ],
                            egui::Stroke::new(1.0, crate::ui::theme::SECTION_DIVIDER),
                        ),
                    ]),
                );
            }
        },
    );
}

/// The clip for one detail column: bounded horizontally by the column, but
/// vertically only by the enclosing viewport. A column allocated with a zero
/// desired height has a zero-height `max_rect`, and clipping to that hides
/// everything the column paints.
fn column_bounds(ui: &egui::Ui) -> egui::Rect {
    let clip = ui.clip_rect();
    egui::Rect::from_x_y_ranges(ui.max_rect().x_range(), clip.y_range()).intersect(clip)
}

/// Reference label column width for the `label · value` grids.
pub(super) const KV_LABEL_WIDTH: f32 = 126.0;

/// The copy affordance used everywhere in the detail. `⧉` (U+29C9), the
/// glyph in the design mockup, is absent from every font egui bundles and
/// paints as a missing-glyph box, so the console uses `⎘` (U+2398).
pub(super) const COPY_GLYPH: &str = "⎘";

/// Width reserved for one [`COPY_GLYPH`] button.
const COPY_WIDTH: f32 = 20.0;

/// Reference section header: a small, letter-spaced muted title, an optional
/// count, and the local-width rule underneath.
pub(super) fn section(ui: &mut egui::Ui, title: &str, count: Option<usize>) {
    let note = count.map(|count| count.to_string());
    section_row(ui, title, note.as_deref(), None);
}

/// Section header whose count carries its own unit (`7 revisions`).
pub(super) fn section_note(ui: &mut egui::Ui, title: &str, note: &str) {
    section_row(ui, title, Some(note), None);
}

/// Section header with a right-aligned accent link (`Open Pods tab →`).
/// Returns `true` on the frame the link was clicked.
pub(super) fn section_action(
    ui: &mut egui::Ui,
    title: &str,
    note: Option<&str>,
    action: &str,
) -> bool {
    section_row(ui, title, note, Some(action))
}

fn section_row(ui: &mut egui::Ui, title: &str, note: Option<&str>, action: Option<&str>) -> bool {
    ui.add_space(10.0);
    let accessible = match note {
        Some(note) => format!("{title} · {note}"),
        None => title.to_owned(),
    };
    let title_font = egui::FontId::new(11.0, egui::FontFamily::Monospace);
    let title_galley = ui.painter().layout_no_wrap(
        title.to_owned(),
        title_font.clone(),
        crate::ui::theme::MUTED_TEXT,
    );
    let note_galley = note.map(|note| {
        ui.painter()
            .layout_no_wrap(note.to_owned(), title_font, crate::ui::theme::FAINT_TEXT)
    });
    let gap = 8.0;
    let text_width = title_galley.size().x
        + note_galley
            .as_ref()
            .map_or(0.0, |galley| gap + galley.size().x);
    let height = title_galley
        .size()
        .y
        .max(ui.spacing().interact_size.y * 0.8);
    // With an action link the header owns the whole visible row so the link
    // can sit against the column's right edge.
    let row_width = if action.is_some() {
        ui.available_rect_before_wrap()
            .intersect(ui.clip_rect())
            .width()
            .max(text_width)
    } else {
        text_width
    };
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(row_width, height), egui::Sense::hover());
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
    if ui.is_rect_visible(rect) {
        let painter = ui.painter();
        let title_pos = egui::pos2(rect.left(), rect.center().y - title_galley.size().y / 2.0);
        painter.galley(
            title_pos,
            title_galley.clone(),
            crate::ui::theme::MUTED_TEXT,
        );
        if let Some(note_galley) = note_galley {
            let note_pos = egui::pos2(
                rect.left() + title_galley.size().x + gap,
                rect.center().y - note_galley.size().y / 2.0,
            );
            painter.galley(note_pos, note_galley, crate::ui::theme::FAINT_TEXT);
        }
    }
    let clicked = action.is_some_and(|action| {
        let mut action_ui = ui.new_child(
            egui::UiBuilder::new()
                .max_rect(rect)
                .layout(egui::Layout::right_to_left(egui::Align::Center)),
        );
        action_ui.set_clip_rect(action_ui.clip_rect().intersect(rect));
        let link = action_ui.add(
            egui::Button::new(
                RichText::new(action)
                    .font(egui::FontId::new(11.0, egui::FontFamily::Monospace))
                    .color(crate::ui::theme::ACCENT),
            )
            .frame(false),
        );
        link.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, action));
        link.clicked()
    });
    section_separator(ui);
    clicked
}

/// A value in the fixed-label KV grid, with its own elision and copy rules.
pub(super) struct KvValue<'a> {
    /// The authoritative value: accessible label, hover text, copy payload.
    pub full: &'a str,
    /// Display form when the value has a fixed shortening rule of its own
    /// (a UID keeps its first eight and last four characters). `None` fits
    /// `full` into the cell by middle elision.
    pub display: Option<String>,
    pub color: Option<egui::Color32>,
    /// Whether to paint a [`COPY_GLYPH`] button after the value.
    pub copy: bool,
}

impl<'a> KvValue<'a> {
    pub(super) fn new(full: &'a str) -> Self {
        Self {
            full,
            display: None,
            color: None,
            copy: false,
        }
    }

    pub(super) fn faint(mut self) -> Self {
        self.color = Some(crate::ui::theme::FAINT_TEXT);
        self
    }

    pub(super) fn copyable(mut self) -> Self {
        self.copy = true;
        self
    }

    pub(super) fn display(mut self, display: String) -> Self {
        self.display = Some(display);
        self
    }
}

/// Middle-elide `value` until it fits `width` in the body font. Never tail
/// truncates: the distinguishing part of an image, UID or URL is its tail.
pub(super) fn fit_middle(ui: &egui::Ui, value: &str, width: f32) -> String {
    let font = egui::TextStyle::Body.resolve(ui.style());
    let measure = |text: &str| {
        ui.painter()
            .layout_no_wrap(text.to_owned(), font.clone(), ui.visuals().text_color())
            .size()
            .x
    };
    if measure(value) <= width {
        return value.to_owned();
    }
    let mut limit = unicode_segmentation::UnicodeSegmentation::graphemes(value, true).count();
    let mut shown = value.to_owned();
    while limit > 1 && measure(&shown) > width {
        limit -= 1;
        shown = long_value_text(value, limit);
    }
    shown
}

/// `a41c7d3e…8f02`: keep the head that identifies the resource and the tail
/// that distinguishes otherwise near-identical ids.
pub(super) fn head_tail_elide(value: &str, head: usize, tail: usize) -> String {
    use unicode_segmentation::UnicodeSegmentation as _;
    let graphemes: Vec<&str> = value.graphemes(true).collect();
    if graphemes.len() <= head + tail + 1 {
        return value.to_owned();
    }
    graphemes[..head]
        .iter()
        .copied()
        .chain(std::iter::once("…"))
        .chain(graphemes[graphemes.len() - tail..].iter().copied())
        .collect()
}

/// One `label · value` row of the reference KV grid: a fixed muted label
/// column and a value that starts immediately after it.
pub(super) fn kv_row(ui: &mut egui::Ui, label: &str, value: &str) -> egui::Response {
    kv_value_row(ui, label, KvValue::new(value))
}

pub(super) fn kv_value_row(ui: &mut egui::Ui, label: &str, value: KvValue<'_>) -> egui::Response {
    let row_height = ui.spacing().interact_size.y;
    let accessible = format!("{label} · {}", value.full);
    ui.horizontal(|ui| {
        kv_label(ui, label, row_height);
        let reserved = if value.copy {
            COPY_WIDTH + ui.spacing().item_spacing.x
        } else {
            0.0
        };
        let cell = (ui.available_width() - reserved).max(1.0);
        let shown = match value.display.as_deref() {
            Some(display) => fit_middle(ui, display, cell),
            None => fit_middle(ui, value.full, cell),
        };
        let color = value.color.unwrap_or_else(|| ui.visuals().text_color());
        let response = left_aligned_label(
            ui,
            cell,
            row_height,
            egui::Label::new(RichText::new(&shown).color(color)),
        );
        response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
        if shown != value.full {
            response.clone().on_hover_text(value.full);
        }
        if value.copy {
            copy_button(ui, label, value.full);
        }
        response
    })
    .inner
}

/// Add `label` anchored to the left of an exact cell. `Ui::add_sized`
/// centers instead, which pushes short values into the middle of the
/// column and reads as an accidental right shift.
fn left_aligned_label(
    ui: &mut egui::Ui,
    width: f32,
    height: f32,
    label: egui::Label,
) -> egui::Response {
    ui.allocate_ui_with_layout(
        egui::vec2(width, height),
        egui::Layout::left_to_right(egui::Align::Center),
        |ui| {
            ui.set_min_height(height);
            ui.add(label.truncate())
        },
    )
    .inner
}

fn copy_button(ui: &mut egui::Ui, label: &str, value: &str) {
    let accessible = format!("Copy {label}");
    let response = ui.add_sized(
        [COPY_WIDTH, ui.spacing().interact_size.y],
        egui::Button::new(RichText::new(COPY_GLYPH).color(crate::ui::theme::ACCENT))
            .small()
            .frame(false),
    );
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible.clone()));
    if response.on_hover_text(accessible).clicked() {
        ui.ctx().copy_text(value.to_owned());
    }
}

/// The muted, fixed-width label cell of a KV row. Painted directly so the
/// accessible row is the value node alone (`label · value`).
fn kv_label(ui: &mut egui::Ui, label: &str, row_height: f32) {
    let (rect, _) =
        ui.allocate_exact_size(egui::vec2(KV_LABEL_WIDTH, row_height), egui::Sense::hover());
    if ui.is_rect_visible(rect) {
        let galley = ui.painter().layout(
            label.to_owned(),
            egui::TextStyle::Body.resolve(ui.style()),
            ui.visuals().weak_text_color(),
            KV_LABEL_WIDTH,
        );
        let pos = egui::pos2(rect.left(), rect.center().y - galley.size().y / 2.0);
        ui.painter()
            .with_clip_rect(rect.intersect(ui.clip_rect()))
            .galley(pos, galley, ui.visuals().weak_text_color());
    }
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
            egui::Stroke::new(1.5, crate::ui::theme::SECTION_DIVIDER),
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

/// A full-width, two-line value used for images, selectors and annotations:
/// the label on its own line, then the value starting at the column's left
/// edge with the copy control immediately after it (not floated to the far
/// right, which reads as an unrelated control).
pub(super) fn long_value(ui: &mut egui::Ui, width: f32, label: &str, value: Option<&str>) {
    let available = value.filter(|value| !value.is_empty());
    let original = available.unwrap_or("—");
    let width = width.max(1.0);
    let row_height = ui.spacing().interact_size.y;
    let font = egui::TextStyle::Body.resolve(ui.style());
    ui.allocate_ui_with_layout(
        egui::vec2(width, 0.0),
        egui::Layout::top_down(egui::Align::Min),
        |ui| {
            ui.set_max_width(width);
            // Line 1: the label alone, so the value below gets the full column.
            let label_response = ui.label(RichText::new(label).weak());
            label_response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, label));
            // Line 2: middle-elided value (the tag/tail is the part that
            // changes, so it is never cut) plus the copy control.
            ui.horizontal(|ui| {
                let reserved = if available.is_some() {
                    COPY_WIDTH + ui.spacing().item_spacing.x
                } else {
                    0.0
                };
                let value_width = (width - reserved).max(1.0);
                let shown = fit_middle(ui, original, value_width);
                let job = emphasized_tail_job(
                    &shown,
                    font.clone(),
                    ui.visuals().text_color(),
                    crate::ui::theme::TEXT,
                );
                let painted_width = ui.painter().layout_job(job.clone()).size().x;
                let response = left_aligned_label(
                    ui,
                    painted_width.min(value_width),
                    row_height,
                    egui::Label::new(job),
                );
                response.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, format!("{label}: {original}"))
                });
                if shown != original {
                    response.on_hover_text(original);
                }
                if available.is_some() {
                    copy_button(ui, label, original);
                }
            });
        },
    );
}

/// A label line followed by one `key=value` line per entry. Joining a map
/// into a single elided string hides every pair but the first, so multi-key
/// selectors and template labels get one line each, elided individually.
pub(super) fn long_value_list(
    ui: &mut egui::Ui,
    width: f32,
    label: &str,
    entries: &[(String, String)],
) {
    let pairs: Vec<String> = entries
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    match pairs.len() {
        0 => long_value(ui, width, label, None),
        // A single pair reads as one value (`app=mcp-kubernetes`).
        1 => long_value(ui, width, label, Some(&pairs[0])),
        _ => {
            let width = width.max(1.0);
            let row_height = ui.spacing().interact_size.y;
            let joined = pairs.join("\n");
            ui.allocate_ui_with_layout(
                egui::vec2(width, 0.0),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    ui.set_max_width(width);
                    ui.horizontal(|ui| {
                        let label_response = ui.label(RichText::new(label).weak());
                        label_response
                            .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, label));
                        copy_button(ui, label, &joined);
                    });
                    for pair in &pairs {
                        let shown = fit_middle(ui, pair, width);
                        let response = left_aligned_label(
                            ui,
                            width,
                            row_height,
                            egui::Label::new(RichText::new(&shown)),
                        );
                        response.widget_info(|| {
                            WidgetInfo::labeled(WidgetType::Label, true, format!("{label}: {pair}"))
                        });
                        if shown != *pair {
                            response.on_hover_text(pair);
                        }
                    }
                },
            );
        }
    }
}

/// Lay out `repo/name:tag` so the tag (the part that distinguishes revisions)
/// reads stronger than the repository path. Values without a tag are plain.
fn emphasized_tail_job(
    text: &str,
    font: egui::FontId,
    body: egui::Color32,
    emphasis: egui::Color32,
) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = f32::INFINITY;
    let split = text
        .rfind(':')
        .filter(|colon| !text[colon + 1..].contains('/') && *colon + 1 < text.len());
    match split {
        Some(colon) => {
            job.append(
                &text[..=colon],
                0.0,
                egui::TextFormat::simple(font.clone(), body),
            );
            job.append(
                &text[colon + 1..],
                0.0,
                egui::TextFormat::simple(font, emphasis),
            );
        }
        None => job.append(text, 0.0, egui::TextFormat::simple(font, body)),
    }
    job
}

/// LABELS and ANNOTATIONS as two sibling sections of chips. Either map may
/// be empty, in which case its section is not rendered at all.
pub(super) fn metadata_sections<'a>(
    ui: &mut egui::Ui,
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    label_separator: &str,
    annotations: impl IntoIterator<Item = (&'a str, &'a str)>,
) {
    let labels = labels.into_iter().collect::<Vec<_>>();
    let annotations = annotations.into_iter().collect::<Vec<_>>();
    if !labels.is_empty() {
        section(ui, "LABELS", Some(labels.len()));
        metadata_chips(ui, &labels, label_separator);
    }
    if !annotations.is_empty() {
        if !labels.is_empty() {
            section_separator(ui);
        }
        section(ui, "ANNOTATIONS", Some(annotations.len()));
        metadata_chips(ui, &annotations, ": ");
    }
}

/// Metadata chips (`key` faint, `value` in body colour) laid out in manual
/// rows: every chip is measured in the font it is painted with, a chip that
/// does not fit the remaining row starts the next one, and a chip wider than
/// the column takes a row of its own and elides, exposing the full pair on
/// hover and as its accessible name.
pub(super) fn metadata_chips(ui: &mut egui::Ui, entries: &[(&str, &str)], separator: &str) {
    let width = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width()
        .max(1.0);
    let font = egui::FontId::new(12.0, egui::FontFamily::Monospace);
    let chip_height = ui.spacing().interact_size.y - 2.0;
    let padding_x = 8.0;
    let gap = 4.0;
    // The chip key is one step fainter than a muted label so `app=` reads as
    // the qualifier and the value as the fact.
    let key_color = crate::ui::theme::FAINT_TEXT;
    let value_color = ui.visuals().text_color();
    // One gap short of the column, so a chip never exactly fills a row.
    let max_chip_width = (width - gap).max(1.0);

    struct Chip {
        full: String,
        galley: std::sync::Arc<egui::Galley>,
        width: f32,
    }
    let chips = entries
        .iter()
        .map(|(key, value)| {
            let full = format!("{key}{separator}{value}");
            let mut job = egui::text::LayoutJob::default();
            job.wrap.max_width = f32::INFINITY;
            job.append(
                &format!("{key}{separator}"),
                0.0,
                egui::TextFormat::simple(font.clone(), key_color),
            );
            job.append(
                value,
                0.0,
                egui::TextFormat::simple(font.clone(), value_color),
            );
            let galley = ui.painter().layout_job(job);
            let width = (galley.size().x + padding_x * 2.0).min(max_chip_width);
            Chip {
                full,
                galley,
                width,
            }
        })
        .collect::<Vec<_>>();

    let mut rows: Vec<Vec<&Chip>> = Vec::new();
    let mut row_used = 0.0_f32;
    for chip in &chips {
        let fits =
            rows.last().is_some_and(|row| !row.is_empty()) && row_used + gap + chip.width <= width;
        if rows.is_empty() || !fits {
            rows.push(Vec::new());
            row_used = 0.0;
        }
        if let Some(row) = rows.last_mut() {
            row.push(chip);
        }
        row_used += chip.width + gap;
    }

    let previous_spacing = ui.spacing().item_spacing;
    ui.spacing_mut().item_spacing = egui::vec2(gap, gap);
    for row in rows {
        ui.horizontal(|ui| {
            for chip in row {
                let (rect, response) = ui
                    .allocate_exact_size(egui::vec2(chip.width, chip_height), egui::Sense::hover());
                if ui.is_rect_visible(rect) {
                    ui.painter().rect(
                        rect,
                        chip_height / 2.0,
                        ui.visuals().faint_bg_color,
                        egui::Stroke::new(1.0, ui.visuals().widgets.noninteractive.bg_stroke.color),
                        egui::StrokeKind::Inside,
                    );
                    let pos = egui::pos2(
                        rect.left() + padding_x,
                        rect.center().y - chip.galley.size().y / 2.0,
                    );
                    ui.painter()
                        .with_clip_rect(rect.shrink2(egui::vec2(padding_x - 2.0, 0.0)))
                        .galley(pos, chip.galley.clone(), value_color);
                }
                let full = chip.full.clone();
                response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, full.clone()));
                response.on_hover_text(&chip.full);
            }
        });
    }
    ui.spacing_mut().item_spacing = previous_spacing;
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod responsive_contract_tests {
    use super::{detail_columns, long_value_text};
    use egui::accesskit::Role;
    use egui_kittest::{Harness, kittest::Queryable as _};

    #[test]
    fn metadata_chips_wrap_by_available_width_and_never_overflow_the_column() {
        let render = |width: f32| {
            let mut harness = Harness::builder()
                .with_size(egui::vec2(width, 200.0))
                .build_ui(move |ui| {
                    super::metadata_chips(ui, &[("app", "web"), ("tier", "frontend")], "=");
                });
            harness.run();
            harness
        };
        let wide = render(520.0);
        let first = wide.get_by_label("app=web").rect();
        let second = wide.get_by_label("tier=frontend").rect();
        assert!(
            (first.top() - second.top()).abs() < 1.0,
            "both chips share one row"
        );

        let narrow = render(150.0);
        let first = narrow.get_by_label("app=web").rect();
        let second = narrow.get_by_label("tier=frontend").rect();
        assert!(
            second.top() > first.bottom() - 1.0,
            "second chip wraps below the first"
        );
        assert!(second.right() <= 150.0 + 1.0);
    }

    #[test]
    fn oversized_metadata_chips_take_their_own_row_and_expose_the_full_pair() {
        let first = "example.io/very-long-common-prefix/first-distinct-key";
        let second = "example.io/very-long-common-prefix/second-distinct-key";
        let mut harness = Harness::builder()
            .with_size(egui::vec2(280.0, 180.0))
            .build_ui(move |ui| {
                super::metadata_chips(ui, &[(first, "one"), (second, "two")], ": ");
            });
        harness.run();
        let a = harness.get_by_label(&format!("{first}: one")).rect();
        let b = harness.get_by_label(&format!("{second}: two")).rect();
        assert!(a.right() <= 280.0 + 1.0 && b.right() <= 280.0 + 1.0);
        assert!(
            b.top() >= a.bottom() - 1.0,
            "rows must not overlap: {a:?} {b:?}"
        );
        let full = format!("{second}: two");
        harness.get_by_label(&full).hover();
        harness.run_steps(15);
        assert!(harness.get_all_by_label(&full).count() >= 2);
    }

    #[test]
    fn metadata_sections_render_labels_and_annotations_as_siblings() {
        let mut harness = Harness::builder()
            .with_size(egui::vec2(520.0, 240.0))
            .build_ui(|ui| {
                super::metadata_sections(ui, [("app", "web")], "=", [("note", "value")]);
            });
        harness.run();
        harness.get_by_label("LABELS · 1");
        harness.get_by_label("ANNOTATIONS · 1");
        harness.get_by_label("app=web");
        harness.get_by_label("note: value");
    }

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
        let padding = f32::from(super::CONFIGURATION_PADDING_LEFT)
            + f32::from(super::CONFIGURATION_PADDING_RIGHT);
        assert!((operational.width() / (configuration.width() + padding) - 1.35).abs() < 0.01);

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
