//! Sortable, filterable, selectable resource list table with stable
//! per-window egui IDs.
//!
//! Rows are [`ResourceListRow`]s straight from the protocol; this module
//! never duplicates authoritative data into local state.

use egui::{ScrollArea, WidgetInfo, WidgetType};
use k10s_protocol::ResourceListRow;

use super::responsive_table::RowAction;
use crate::workspace::{SortSpec, WindowId};

/// Column sort keys in display order.
const COLUMNS: [(&str, &str); 4] = [
    ("Namespace", "namespace"),
    ("Name", "name"),
    ("Status", "status"),
    ("Created", "created"),
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
    I: Clone,
{
    let mut actions = TableActions::default();

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
    ScrollArea::both()
        .id_salt(("k10s.resource.list.scroll", window_id.0))
        .show_rows(ui, row_height, rows.len() + header_rows, |ui, range| {
            egui::Grid::new(("k10s.resource.table", window_id.0))
                .striped(true)
                .min_col_width(72.0)
                .show(ui, |ui| {
                    if range.start < header_rows {
                        for (visible, key) in COLUMNS {
                            if visible == "Namespace" && !namespaced {
                                continue;
                            }
                            sort_header(ui, title, visible, key, sort, &mut actions);
                        }
                        ui.end_row();
                    }

                    for index in range.start.max(header_rows)..range.end {
                        let row = &rows[index - header_rows];
                        if namespaced {
                            ui.label(row.identity.namespace.as_deref().unwrap_or("—"));
                        }
                        let selected = is_selected(row);
                        let name = if selected {
                            format!("▶ {}", row.identity.name)
                        } else {
                            format!("  {}", row.identity.name)
                        };
                        let name_button = ui.add(
                            egui::Button::new(if selected {
                                egui::RichText::new(name).strong()
                            } else {
                                egui::RichText::new(name)
                            })
                            .selected(selected)
                            .stroke(if selected {
                                egui::Stroke::new(1.5, crate::ui::theme::ACCENT)
                            } else {
                                egui::Stroke::NONE
                            }),
                        );
                        let label = super::responsive_table::row_action_label(
                            "resource",
                            &row.identity.name,
                            selected,
                        );
                        name_button.widget_info(move || {
                            WidgetInfo::selected(WidgetType::Button, true, selected, label.clone())
                        });
                        let (row_action, popped_out) = super::responsive_table::row_interaction(
                            &name_button,
                            identity_of(row),
                            selected,
                        );
                        if row_action.is_some() {
                            actions.row_action = row_action;
                        }
                        if popped_out.is_some() {
                            actions.popped_out = popped_out;
                        }
                        name_button.context_menu(|ui| {
                            if ui.button("Open dedicated window").clicked() {
                                actions.popped_out = Some(identity_of(row));
                                ui.close();
                            }
                        });
                        ui.label(row.summary.clone());
                        ui.monospace(row.created_at.clone());
                        ui.end_row();
                    }
                });
        });
    actions
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
