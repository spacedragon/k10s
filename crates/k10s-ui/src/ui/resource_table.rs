//! Sortable, filterable, selectable resource list table with stable
//! per-window egui IDs.
//!
//! Rows are [`ResourceListRow`]s straight from the protocol; this module
//! never duplicates authoritative data into local state.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::{ResourceListRow, ResourceProjection};

use super::responsive_table::RowAction;
use crate::workspace::{SortSpec, WindowId};

use super::responsive_table::ColumnSpec;

const DEPLOYMENT_COLUMNS: [ColumnSpec; 6] = [
    ColumnSpec::required("namespace", 112.0),
    ColumnSpec::elastic("name", 180.0),
    ColumnSpec::required("ready", 56.0),
    ColumnSpec::hideable("status", 112.0, 1),
    ColumnSpec::hideable("image", 180.0, 0),
    ColumnSpec::required("created", 56.0),
];
const POD_COLUMNS: [ColumnSpec; 7] = [
    ColumnSpec::required("namespace", 112.0),
    ColumnSpec::elastic("name", 180.0),
    ColumnSpec::required("ready", 56.0),
    ColumnSpec::required("status", 112.0),
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
            "status" => left
                .summary
                .cmp(&right.summary)
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

/// Render the table. Stable IDs derive from the workspace `window_id`, so
/// scroll positions and widget state never leak between windows.
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    title: &str,
    namespaced: bool,
    search: &str,
    sort: Option<&SortSpec>,
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
            // The toolbar owns the "Clear filters" control so the label
            // stays unique per window.
            ui.label("No resources match these filters");
        }
        return actions;
    }

    // Rows are virtualized: only the visible window of rows is laid out
    // per frame, so frame cost stays bounded by the viewport rather than
    // the snapshot size. Virtual row 0 is the sticky header; data rows
    // follow at offset 1. Row height matches what a Grid row actually
    // measures: rows contain buttons, so they are at least one interact
    // size tall, not one text line.
    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    let header_rows = 1_usize;
    let specs = match rows.first().and_then(|row| row.projection.as_ref()) {
        Some(ResourceProjection::Deployment(_)) => &DEPLOYMENT_COLUMNS[..],
        Some(ResourceProjection::Pod(_)) => &POD_COLUMNS[..],
        _ if namespaced => &GENERIC_NAMESPACED[..],
        _ => &GENERIC_CLUSTER[..],
    };
    let columns = super::responsive_table::resolve_columns(specs, ui.available_width());
    let _resolved_table_width: f32 = columns.visible.iter().map(|column| column.width).sum();
    let _horizontal_scroll = columns.horizontal_scroll;
    ScrollArea::both()
        .id_salt(("k10s.resource.list.scroll", window_id.0))
        .show_rows(ui, row_height, rows.len() + header_rows, |ui, range| {
            egui::Grid::new(("k10s.resource.table", window_id.0))
                .striped(true)
                .min_col_width(72.0)
                .show(ui, |ui| {
                    if range.start < header_rows {
                        for column in &columns.visible {
                            let visible = column_title(column.key);
                            if matches!(column.key, "namespace" | "name" | "status" | "created") {
                                sort_header(ui, title, visible, column.key, sort, &mut actions);
                            } else {
                                ui.label(visible);
                            }
                        }
                        ui.end_row();
                    }

                    for index in range.start.max(header_rows)..range.end {
                        let row = &rows[index - header_rows];
                        let selected = is_selected(row);
                        let name = if selected {
                            format!("▶ {}", row.identity.name)
                        } else {
                            format!("  {}", row.identity.name)
                        };
                        for column in &columns.visible {
                            match column.key {
                                "namespace" => {
                                    ui.label(row.identity.namespace.as_deref().unwrap_or("—"));
                                }
                                "name" => {
                                    let name_button = ui.add(
                                        egui::Button::new(if selected {
                                            egui::RichText::new(&name).strong()
                                        } else {
                                            egui::RichText::new(&name)
                                        })
                                        .selected(selected)
                                        .stroke(
                                            if selected {
                                                egui::Stroke::new(1.5, crate::ui::theme::ACCENT)
                                            } else {
                                                egui::Stroke::NONE
                                            },
                                        ),
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
                                    ui.label(resource_status(row));
                                }
                                "ready" => {
                                    right_label(ui, resource_ready(row));
                                }
                                "image" => {
                                    elided_label(ui, resource_image(row), 28);
                                }
                                "restarts" => {
                                    right_label(ui, resource_restarts(row));
                                }
                                "node" => {
                                    elided_label(ui, resource_node(row), 20);
                                }
                                "created" => {
                                    ui.monospace(
                                        row.created_at.get(..10).unwrap_or(&row.created_at),
                                    );
                                }
                                _ => {}
                            }
                        }
                        ui.end_row();
                    }
                });
        });
    actions
}

fn column_title(key: &str) -> &'static str {
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
fn resource_status(row: &ResourceListRow) -> String {
    match row.projection.as_ref() {
        Some(ResourceProjection::Pod(p)) => p.phase.clone().unwrap_or_else(|| "—".into()),
        _ => row.summary.clone(),
    }
}
fn resource_ready(row: &ResourceListRow) -> String {
    match row.projection.as_ref() {
        Some(ResourceProjection::Deployment(p)) => format!(
            "{}/{}",
            p.ready_replicas.unwrap_or(0),
            p.desired_replicas.unwrap_or(0)
        ),
        Some(ResourceProjection::Pod(p)) => format!(
            "{}/{}",
            p.ready_containers.unwrap_or(0),
            p.total_containers.unwrap_or(0)
        ),
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
fn elided_label(ui: &mut egui::Ui, value: String, max: usize) {
    let compact = super::responsive_table::middle_elide(&value, max);
    let response = ui.label(compact).on_hover_text(&value);
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, value.clone()));
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
        let header = ui.label(visible);
        if key == "created" {
            header.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, "Created"));
        }
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
