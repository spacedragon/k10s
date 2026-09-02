//! Sortable, filterable, selectable resource list table with stable
//! per-window egui IDs.
//!
//! Rows are [`ResourceListRow`]s straight from the protocol; this module
//! never duplicates authoritative data into local state.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::{ResourceListRow, ResourceProjection};
use std::borrow::Cow;
use std::collections::BTreeSet;
use web_time::SystemTime;

use super::responsive_table::RowAction;
use crate::workspace::{AgeMode, SortSpec, WindowId, WorkloadKind};

use super::responsive_table::ColumnSpec;

// Reference column hierarchy: fixed widths for the scannable columns and a
// flex Name, so the remaining width goes to Image (the column people reach
// for when checking which revision is still running).
const DEPLOYMENT_COLUMNS: [ColumnSpec; 6] = [
    ColumnSpec::required("namespace", 112.0),
    ColumnSpec::elastic("name", 150.0),
    ColumnSpec::required("ready", 60.0),
    ColumnSpec::hideable("status", 104.0, 1),
    ColumnSpec::hideable("image", 230.0, 0),
    ColumnSpec::required("created", 56.0),
];
const POD_COLUMNS: [ColumnSpec; 7] = [
    ColumnSpec::required("namespace", 150.0),
    ColumnSpec::elastic("name", 180.0),
    ColumnSpec::required("ready", 60.0),
    ColumnSpec::required("status", 104.0),
    ColumnSpec::hideable("restarts", 64.0, 1),
    ColumnSpec::hideable("node", 120.0, 0),
    ColumnSpec::required("created", 56.0),
];
const GENERIC_NAMESPACED: [ColumnSpec; 4] = [
    ColumnSpec::required("namespace", 112.0),
    ColumnSpec::elastic("name", 180.0),
    ColumnSpec::hideable("status", 120.0, 0),
    ColumnSpec::hideable("created", 56.0, 1),
];
const GENERIC_CLUSTER: [ColumnSpec; 3] = [
    ColumnSpec::elastic("name", 220.0),
    ColumnSpec::hideable("status", 120.0, 0),
    ColumnSpec::hideable("created", 56.0, 1),
];

/// Outcome of rendering one table frame.
pub(super) struct TableActions<I> {
    /// A row was clicked; carries the mapped window identity.
    pub row_action: Option<RowAction<I>>,
    /// A row was double-clicked or popped out via its context menu; the
    /// identity is cloned for a dedicated pinned detail window.
    pub popped_out: Option<I>,
    /// A sort header was clicked; carries the next sort spec.
    pub sort: Option<SortSpec>,
    /// The clear-filters button was clicked.
    pub cleared: bool,
}

impl<I> Default for TableActions<I> {
    fn default() -> Self {
        Self {
            row_action: None,
            popped_out: None,
            sort: None,
            cleared: false,
        }
    }
}

/// Whether a row matches the current search text (name, namespace, or
/// status summary). `needle` must be the already-lowercased search text;
/// the caller hoists the conversion out of the per-row loop so filtering
/// large snapshots never allocates per row.
pub(super) fn matches_search(row: &ResourceListRow, needle: &str) -> bool {
    needle.is_empty()
        || row.identity.name.to_lowercase().contains(needle)
        || row
            .identity
            .namespace
            .as_deref()
            .unwrap_or_default()
            .to_lowercase()
            .contains(needle)
        || row.summary.to_lowercase().contains(needle)
}

/// Sort rows by the spec with a deterministic name tiebreaker.
pub(super) fn sort_rows(rows: &mut [&ResourceListRow], sort: &SortSpec) {
    let column = sort.column.as_str();
    rows.sort_by(|left, right| {
        let order = match column {
            "namespace" => left
                .identity
                .namespace
                .cmp(&right.identity.namespace)
                .then_with(|| left.identity.name.cmp(&right.identity.name)),
            "status" => resource_status(left)
                .cmp(&resource_status(right))
                .then_with(|| left.identity.name.cmp(&right.identity.name)),
            "created" => left
                .created_at
                .cmp(&right.created_at)
                .then_with(|| left.identity.name.cmp(&right.identity.name)),
            _ => left.identity.name.cmp(&right.identity.name),
        };
        if sort.ascending {
            order
        } else {
            order.reverse()
        }
    });
}

/// The column layout for one workload kind; the toolbar's Columns menu and
/// the table itself share this single source of truth.
pub(super) fn column_specs(kind: WorkloadKind, namespaced: bool) -> &'static [ColumnSpec] {
    match kind {
        WorkloadKind::Deployments => &DEPLOYMENT_COLUMNS[..],
        WorkloadKind::Pods => &POD_COLUMNS[..],
        _ if namespaced => &GENERIC_NAMESPACED[..],
        _ => &GENERIC_CLUSTER[..],
    }
}

/// Render the table. Stable IDs derive from the workspace `window_id`, so
/// scroll positions and widget state never leak between windows.
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    render_time: SystemTime,
    workload_kind: WorkloadKind,
    title: &str,
    namespaced: bool,
    search: &str,
    sort: Option<&SortSpec>,
    hidden_columns: &BTreeSet<String>,
    age_mode: AgeMode,
    rows: &[&ResourceListRow],
    is_selected: impl Fn(&ResourceListRow) -> bool,
    identity_of: impl Fn(&ResourceListRow) -> I,
) -> TableActions<I>
where
    I: Clone + Send + Sync + 'static,
{
    let mut actions = TableActions::default();
    let gesture_table_id = egui::Id::new(("k10s.resource.table-gesture", window_id.0));
    actions.row_action = super::responsive_table::poll_row_action(ui.ctx(), gesture_table_id);

    if rows.is_empty() {
        if search.is_empty() {
            ui.label(format!("No {title} in this view"));
        } else {
            // The toolbar owns the "Reset" control so the label
            // stays unique per window.
            ui.label("No resources match these filters");
        }
        return actions;
    }

    // Rows are virtualized: only the visible window of rows is laid out
    // per frame, so frame cost stays bounded by the viewport rather than
    // the snapshot size. Virtual row 0 is the header; data rows follow at
    // offset 1. Row height matches what a Grid row actually
    // measures: rows contain buttons, so they are at least one interact
    // size tall, not one text line.
    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    let header_rows = 1_usize;
    // Absolute ages render full timestamps, which need a wider Age column.
    let specs: Vec<ColumnSpec> = column_specs(workload_kind, namespaced)
        .iter()
        .map(|spec| {
            if spec.key == "created" && age_mode == AgeMode::Absolute {
                spec.with_min_width(118.0)
            } else {
                *spec
            }
        })
        .collect();
    let column_spacing = ui.spacing().item_spacing.x;
    ScrollArea::both()
        .id_salt(("k10s.resource.list.scroll", window_id.0))
        .show_rows(ui, row_height, rows.len() + header_rows, |ui, range| {
            // The ScrollArea content UI exposes the actual viewport after its
            // current scrollbar allocation. Resolve here so absent, present,
            // and animated scrollbars all use the width egui really clips.
            let table_width = ui
                .available_rect_before_wrap()
                .intersect(ui.clip_rect())
                .width();
            let columns = super::responsive_table::resolve_columns(
                &specs,
                table_width,
                column_spacing,
                hidden_columns,
            );
            debug_assert_eq!(
                columns.horizontal_scroll,
                columns
                    .visible
                    .iter()
                    .map(|column| column.width)
                    .sum::<f32>()
                    + column_spacing * columns.visible.len().saturating_sub(1) as f32
                    > table_width
            );
            egui::Grid::new(("k10s.resource.table", window_id.0))
                .striped(true)
                .min_col_width(0.0)
                .show(ui, |ui| {
                    if range.start < header_rows {
                        for column in &columns.visible {
                            super::responsive_table::sized_cell(ui, column.width, false, |ui| {
                                let visible = column_title(column.key);
                                if matches!(column.key, "namespace" | "name" | "status" | "created")
                                {
                                    sort_header(ui, title, visible, column.key, sort, &mut actions);
                                } else {
                                    ui.label(visible);
                                }
                            });
                        }
                        ui.end_row();
                    }

                    for index in range.start.max(header_rows)..range.end {
                        let row = &rows[index - header_rows];
                        let selected = is_selected(row);
                        // The whole row carries the selection: a full-width
                        // fill plus a 3px left accent, painted before the
                        // cells so they render on top. No per-cell highlight
                        // and no disclosure marker.
                        let row_min = ui.cursor().min;
                        if selected {
                            let row_rect = egui::Rect::from_min_max(
                                row_min,
                                row_min + egui::vec2(table_width, row_height),
                            );
                            let painter = ui.painter();
                            painter.rect_filled(row_rect, 0.0, super::theme::SELECTED_ROW);
                            painter.rect_filled(
                                egui::Rect::from_min_max(
                                    egui::pos2(row_min.x, row_min.y),
                                    egui::pos2(row_min.x + 3.0, row_min.y + row_height),
                                ),
                                0.0,
                                super::theme::ACCENT,
                            );
                        }
                        for column in &columns.visible {
                            let numeric = matches!(column.key, "ready" | "restarts" | "created");
                            super::responsive_table::sized_cell(ui, column.width, numeric, |ui| {
                                match column.key {
                                    "namespace" => {
                                        ui.label(row.identity.namespace.as_deref().unwrap_or("—"));
                                    }
                                    "name" => {
                                        let name_button = ui.add(
                                            egui::Button::new(&row.identity.name).frame(false),
                                        );
                                        let label = super::responsive_table::row_action_label(
                                            "resource",
                                            &row.identity.name,
                                            selected,
                                        );
                                        name_button.widget_info(move || {
                                            WidgetInfo::selected(
                                                WidgetType::Button,
                                                true,
                                                selected,
                                                label.clone(),
                                            )
                                        });
                                        let popped_out = super::responsive_table::row_interaction(
                                            &name_button,
                                            gesture_table_id,
                                            identity_of(row),
                                            selected,
                                        );
                                        if popped_out.is_some() {
                                            actions.popped_out = popped_out;
                                        }
                                        name_button.context_menu(|ui| {
                                            if ui.button("Open dedicated window").clicked() {
                                                actions.popped_out = Some(identity_of(row));
                                                ui.close();
                                            }
                                        });
                                    }
                                    "status" => {
                                        status_label(ui, resource_status(row).as_ref());
                                    }
                                    "ready" => {
                                        ready_label(ui, row);
                                    }
                                    "image" => {
                                        super::responsive_table::elided_label(
                                            ui,
                                            resource_image(row),
                                            28,
                                        );
                                    }
                                    "restarts" => {
                                        right_label(ui, resource_restarts(row));
                                    }
                                    "node" => {
                                        super::responsive_table::elided_label(
                                            ui,
                                            resource_node(row),
                                            20,
                                        );
                                    }
                                    "created" => {
                                        let age = super::detail::presentation::format_age(
                                            Some(&row.created_at),
                                            render_time,
                                        );
                                        let (cell, hover) = if age_mode == AgeMode::Absolute
                                            && !row.created_at.is_empty()
                                        {
                                            (row.created_at.clone(), age)
                                        } else {
                                            (age, row.created_at.clone())
                                        };
                                        let response = ui.monospace(cell).on_hover_text(hover);
                                        response.widget_info(|| {
                                            WidgetInfo::labeled(
                                                WidgetType::Label,
                                                true,
                                                "Resource age",
                                            )
                                        });
                                    }
                                    _ => {}
                                }
                            });
                        }
                        ui.end_row();
                    }
                });
        });
    actions
}

pub(super) fn column_title(key: &str) -> &'static str {
    match key {
        "namespace" => "Namespace",
        "name" => "Name",
        "ready" => "Ready",
        "status" => "Status",
        "image" => "Image",
        "restarts" => "Restarts",
        "node" => "Node",
        "created" => "Age",
        _ => "",
    }
}
pub(super) fn resource_status(row: &ResourceListRow) -> Cow<'_, str> {
    match row.projection.as_ref() {
        Some(ResourceProjection::Pod(p)) => p
            .phase
            .as_deref()
            .map(Cow::Borrowed)
            .unwrap_or(Cow::Borrowed("—")),
        _ => Cow::Borrowed(&row.summary),
    }
}
fn ready_pair(ready: Option<u32>, desired: Option<u32>) -> String {
    match (ready, desired) {
        (Some(ready), Some(desired)) => format!("{ready}/{desired}"),
        _ => "—".into(),
    }
}
fn resource_image(row: &ResourceListRow) -> String {
    match row.projection.as_ref() {
        Some(ResourceProjection::Deployment(p)) => p
            .template_containers
            .iter()
            .filter_map(|c| c.image.clone())
            .collect::<Vec<_>>()
            .join(", "),
        _ => "—".into(),
    }
}
fn resource_restarts(row: &ResourceListRow) -> String {
    match row.projection.as_ref() {
        Some(ResourceProjection::Pod(p)) => p
            .restart_count
            .map(|n| n.to_string())
            .unwrap_or_else(|| "—".into()),
        _ => "—".into(),
    }
}
fn resource_node(row: &ResourceListRow) -> String {
    match row.projection.as_ref() {
        Some(ResourceProjection::Pod(p)) => p.node_name.clone().unwrap_or_else(|| "—".into()),
        _ => "—".into(),
    }
}
fn right_label(ui: &mut egui::Ui, value: String) {
    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
        ui.label(value);
    });
}

/// A shape and a color both carry the status signal so a wall of identical
/// ready counts stays scannable; danger beats warning beats healthy.
fn status_tone(status: &str) -> (char, egui::Color32) {
    let lowered = status.to_lowercase();
    if lowered.contains("fail")
        || lowered.contains("error")
        || lowered.contains("crashloop")
        || lowered.contains("backoff")
        || lowered.contains("terminating")
    {
        ('\u{2715}', super::theme::DANGER)
    } else if lowered.contains("progress")
        || lowered.contains("pending")
        || lowered.contains("partial")
        || is_degraded_ready_status(&lowered)
    {
        ('\u{25b2}', super::theme::WARNING)
    } else if lowered.contains("ready")
        || lowered.contains("available")
        || lowered.contains("running")
        || lowered.contains("succeeded")
        || lowered.contains("completed")
    {
        ('\u{25cf}', super::theme::HEALTHY)
    } else {
        ('\u{25cf}', super::theme::MUTED_TEXT)
    }
}

/// A "1/2 ready" style summary whose ready count is below its target.
fn is_degraded_ready_status(lowered: &str) -> bool {
    if let Some(stem) = lowered
        .find("ready")
        .or_else(|| lowered.find("container"))
        .or_else(|| lowered.find("pod"))
    {
        let head: Vec<char> = lowered[..stem].chars().collect();
        let mut ready: Option<u32> = None;
        let mut desired: Option<u32> = None;
        for ch in head.iter().rev() {
            if ch.is_ascii_digit() {
                let digit = ch.to_digit(10).unwrap_or(0);
                ready = Some(digit + ready.map(|r| r * 10).unwrap_or(0));
            } else if *ch == '/' {
                desired = ready.take();
            } else if ch.is_whitespace() || *ch == '\u{00b7}' {
                break;
            }
        }
        if let (Some(ready), Some(desired)) = (ready, desired) {
            return ready < desired;
        }
    }
    false
}

/// Render the status cell with its tone glyph and color, keeping the full
/// text accessible and on the hover tooltip.
fn status_label(ui: &mut egui::Ui, status: &str) {
    let (glyph, color) = status_tone(status);
    let text = format!("{} {}", glyph, status);
    let response = ui.colored_label(color, text);
    // The glyph is purely visual; the accessible label stays the clean status
    // text so semantic queries and screen readers never see the shape.
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, status.to_owned()));
    response.on_hover_text(status);
}

/// Render the ready/desired count, turning yellow when ready is below its
/// target so shortfalls read at a glance.
fn ready_label(ui: &mut egui::Ui, row: &ResourceListRow) {
    let (text, degraded) = match row.projection.as_ref() {
        Some(ResourceProjection::Deployment(p)) => {
            let degraded =
                matches!((p.ready_replicas, p.desired_replicas), (Some(r), Some(d)) if r < d);
            (ready_pair(p.ready_replicas, p.desired_replicas), degraded)
        }
        Some(ResourceProjection::Pod(p)) => {
            let degraded =
                matches!((p.ready_containers, p.total_containers), (Some(r), Some(t)) if r < t);
            (ready_pair(p.ready_containers, p.total_containers), degraded)
        }
        _ => ("\u{2014}".into(), false),
    };
    let color = if degraded {
        super::theme::WARNING
    } else {
        super::theme::TEXT
    };
    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
        ui.colored_label(color, text);
    });
}
#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::ready_pair;
    #[test]
    fn partial_deployment_and_pod_readiness_never_fabricate_zeroes() {
        assert_eq!(ready_pair(Some(1), None), "—");
        assert_eq!(ready_pair(None, Some(2)), "—");
        assert_eq!(ready_pair(Some(1), Some(2)), "1/2");
    }
}

fn sort_header<I>(
    ui: &mut egui::Ui,
    title: &str,
    visible: &str,
    key: &str,
    sort: Option<&SortSpec>,
    actions: &mut TableActions<I>,
) {
    let active = sort.is_some_and(|spec| spec.column == key);
    let ascending = sort.map(|spec| spec.ascending).unwrap_or(true);
    let arrow = if !active {
        "↕"
    } else if ascending {
        "↑"
    } else {
        "↓"
    };
    ui.horizontal(|ui| {
        ui.label(visible);
        let button = ui.small_button(arrow);
        let label = format!("Sort {} by {key}", title.to_lowercase());
        button.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
        if button.clicked() {
            actions.sort = Some(if active {
                SortSpec {
                    column: key.to_owned(),
                    ascending: !ascending,
                }
            } else {
                SortSpec {
                    column: key.to_owned(),
                    ascending: true,
                }
            });
        }
    });
}
