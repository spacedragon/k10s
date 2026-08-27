//! The singleton Services window: sortable table rendered strictly from
//! normalized Service projections, plus the split integrated detail pane.
//!
//! All columns come from [`ResourceListRow::projection`]; nothing here
//! parses `summary`. Port-forward controls do not exist in this task — the
//! window is read-only.

use egui::{RichText, ScrollArea, Spinner, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{
    ResourceListRow, ServicePort, ServiceProjection, TargetPort, TransportProtocol,
};

use crate::workspace::{ServiceWindowState, SortSpec, WindowId, WorkspaceCommand};

use super::resource_window::ResourceFeed;
use super::resource_window::RowIdentity;
use super::{ConnectionState, theme};

/// Column sort keys in display order.
const COLUMNS: [(&str, &str); 6] = [
    ("Name", "name"),
    ("Namespace", "namespace"),
    ("Type", "type"),
    ("Cluster IP", "cluster_ip"),
    ("Ports", "ports"),
    ("Age", "age"),
];

/// Wire protocol of a port as a compact display label.
pub fn protocol_label(protocol: TransportProtocol) -> &'static str {
    match protocol {
        TransportProtocol::Tcp => "TCP",
        TransportProtocol::Udp => "UDP",
        TransportProtocol::Sctp => "SCTP",
    }
}

/// Compact one-port label used by the table's Ports column, such as
/// `http 80→8080/TCP` (name optional; node ports get a ` (node 31000)`
/// suffix).
pub fn port_compact_label(port: &ServicePort) -> String {
    let target = match &port.target_port {
        TargetPort::Name { name } => name.clone(),
        TargetPort::Number { number } => number.to_string(),
    };
    let mut label = String::new();
    if let Some(name) = &port.name {
        label.push_str(name);
        label.push(' ');
    }
    label.push_str(&port.service_port.to_string());
    label.push('→');
    label.push_str(&target);
    label.push('/');
    label.push_str(protocol_label(port.protocol));
    if let Some(node_port) = port.node_port {
        label.push_str(&format!(" (node {node_port})"));
    }
    label
}

/// The table's Ports column for one projection: every declared port joined
/// with `, `, or `—` when none are declared.
pub fn ports_column_label(projection: &ServiceProjection) -> String {
    if projection.ports.is_empty() {
        return "—".to_owned();
    }
    projection
        .ports
        .iter()
        .map(port_compact_label)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Structured one-port label used by the Services Ports tab and the web
/// host, such as `http · 80 → 8080 · TCP`, plus ` · appProtocol http`,
/// ` · nodePort 31000`, and ` · read-only` suffixes when they apply.
pub fn port_detail_label(port: &ServicePort) -> String {
    let target = match &port.target_port {
        TargetPort::Name { name } => name.clone(),
        TargetPort::Number { number } => number.to_string(),
    };
    let mut line = String::new();
    if let Some(name) = &port.name {
        line.push_str(name);
        line.push_str(" · ");
    }
    line.push_str(&format!("{} → {}", port.service_port, target));
    line.push_str(" · ");
    line.push_str(protocol_label(port.protocol));
    if let Some(app_protocol) = &port.app_protocol {
        line.push_str(&format!(" · appProtocol {app_protocol}"));
    }
    if let Some(node_port) = port.node_port {
        line.push_str(&format!(" · nodePort {node_port}"));
    }
    if port.protocol != TransportProtocol::Tcp {
        line.push_str(" · read-only");
    }
    line
}

/// The table's Cluster IP cell: the primary cluster IP, `Headless` for the
/// `["None"]` headless marker, `None` when no cluster IP exists at all.
pub fn cluster_ip_column_label(projection: &ServiceProjection) -> String {
    if projection.cluster_ips.len() == 1 && projection.cluster_ips[0] == "None" {
        return "Headless".to_owned();
    }
    projection
        .cluster_ips
        .first()
        .cloned()
        .unwrap_or_else(|| "None".to_owned())
}

/// The Service projection of one row, if populated.
fn service_projection(row: &ResourceListRow) -> Option<&ServiceProjection> {
    match &row.projection {
        Some(k10s_protocol::ResourceProjection::Service(projection)) => Some(projection),
        _ => None,
    }
}

fn row_type(row: &ResourceListRow) -> String {
    match service_projection(row) {
        Some(projection) => projection.service_type.clone(),
        None => "—".to_owned(),
    }
}

fn search_matches(row: &ResourceListRow, needle: &str) -> bool {
    needle.is_empty()
        || row.identity.name.to_lowercase().contains(needle)
        || row
            .identity
            .namespace
            .as_deref()
            .unwrap_or_default()
            .to_lowercase()
            .contains(needle)
        || row_type(row).to_lowercase().contains(needle)
}

/// Local table sort over the same keys the headers queue, with a
/// deterministic name tiebreaker.
fn sort_rows(rows: &mut [&ResourceListRow], sort: &SortSpec) {
    let column = sort.column.as_str();
    rows.sort_by(|left, right| {
        let order = match column {
            "namespace" => left.identity.namespace.cmp(&right.identity.namespace),
            "type" => row_type(left).cmp(&row_type(right)),
            "cluster_ip" => service_projection(left)
                .map(cluster_ip_column_label)
                .cmp(&service_projection(right).map(cluster_ip_column_label)),
            "ports" => service_projection(left)
                .map(ports_column_label)
                .cmp(&service_projection(right).map(ports_column_label)),
            "age" => left.created_at.cmp(&right.created_at),
            // "name" and any unknown key.
            _ => left.identity.name.cmp(&right.identity.name),
        };
        let order = order.then_with(|| left.identity.name.cmp(&right.identity.name));
        if sort.ascending {
            order
        } else {
            order.reverse()
        }
    });
}

/// Outcome of rendering one Services table frame.
struct TableActions<I> {
    selected: Option<I>,
    popped_out: Option<I>,
    sort: Option<SortSpec>,
}

impl<I> Default for TableActions<I> {
    fn default() -> Self {
        Self {
            selected: None,
            popped_out: None,
            sort: None,
        }
    }
}

/// Render one Services window body, queuing commands for every interaction.
///
/// Returns `false` like the other singleton windows (no refresh semantics).
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    state: &mut ServiceWindowState<I>,
    feed: &ResourceFeed,
    context_namespace: Option<&str>,
    connection: ConnectionState,
    yaml: &mut super::tools::YamlEditors,
    streams: &mut super::tools::StreamStores,
    dialogs: &mut super::dialogs::OperationDialogs,
    resource_actions: &mut Vec<super::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) -> bool
where
    I: RowIdentity,
{
    if connection != ConnectionState::Connected {
        ui.label(RichText::new("Connection stale · showing last known rows").color(theme::WARNING));
        ui.separator();
    }

    let compact_controls = ui.ctx().content_rect().width() < 700.0;
    // Keep search, scope, and detail controls reachable in narrow windows.
    // Concise labels preserve every action without collapsing editors.
    ui.horizontal(|ui| {
        let mut search = state.search.clone();
        let search_edit = ui.add(
            TextEdit::singleline(&mut search)
                .hint_text("Search services")
                .desired_width(if compact_controls { 100.0 } else { 200.0 }),
        );
        search_edit.widget_info(|| {
            WidgetInfo::labeled(WidgetType::TextEdit, true, "Search services".to_owned())
        });
        if search_edit.changed() {
            queued.push(WorkspaceCommand::SetSearch(window_id, search));
        }

        ui.label(match (&state.namespace_scope, compact_controls) {
            (crate::workspace::NamespaceScope::ContextDefault, true) => {
                format!("Default: {}", context_namespace.unwrap_or("default"))
            }
            (crate::workspace::NamespaceScope::ContextDefault, false) => format!(
                "Context default ({})",
                context_namespace.unwrap_or("default")
            ),
            (crate::workspace::NamespaceScope::Namespace(value), true) => {
                format!("NS: {value}")
            }
            (crate::workspace::NamespaceScope::Namespace(value), false) => {
                format!("Namespace: {value}")
            }
            (crate::workspace::NamespaceScope::AllNamespaces, _) => "All namespaces".to_owned(),
        });
        let mut namespace = match &state.namespace_scope {
            crate::workspace::NamespaceScope::Namespace(value) => value.clone(),
            _ => String::new(),
        };
        let namespace_edit = ui.add(
            TextEdit::singleline(&mut namespace)
                .hint_text("Namespace filter")
                .desired_width(if compact_controls { 70.0 } else { 140.0 }),
        );
        namespace_edit.widget_info(|| {
            WidgetInfo::labeled(WidgetType::TextEdit, true, "Namespace filter".to_owned())
        });
        if namespace_edit.changed() {
            let parsed = namespace.trim().to_owned();
            let scope = if parsed.is_empty() {
                crate::workspace::NamespaceScope::ContextDefault
            } else {
                crate::workspace::NamespaceScope::Namespace(parsed)
            };
            queued.push(WorkspaceCommand::SetNamespaceScope(window_id, scope));
        }
        let all_namespaces_label = if compact_controls {
            "All"
        } else {
            "All namespaces"
        };
        if ui.button(all_namespaces_label).clicked() {
            queued.push(WorkspaceCommand::SetNamespaceScope(
                window_id,
                crate::workspace::NamespaceScope::AllNamespaces,
            ));
        }

        let filters_active = !state.search.is_empty()
            || state.namespace_scope != crate::workspace::NamespaceScope::ContextDefault;
        let clear_label = if compact_controls {
            "Clear"
        } else {
            "Clear filters"
        };
        if filters_active && ui.button(clear_label).clicked() {
            queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
            queued.push(WorkspaceCommand::SetNamespaceScope(
                window_id,
                crate::workspace::NamespaceScope::ContextDefault,
            ));
        }

        let toggle_label = if state.detail_visible {
            "Hide details"
        } else {
            "Show details"
        };
        if ui.button(toggle_label).clicked() {
            queued.push(WorkspaceCommand::ToggleDetailPane(window_id));
        }
    });
    super::resource_window::show_namespace_catalog_status(
        ui,
        &feed.namespace_catalog,
        resource_actions,
    );
    ui.separator();

    let Some(rows) = feed
        .window_services
        .get(&window_id)
        .or(feed.services.as_ref())
    else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label("Loading services");
        });
        return false;
    };

    // Namespace restriction and search filter authoritative rows locally;
    // sorting happens below against the filtered set.
    let needle = state.search.to_lowercase();
    let mut filtered: Vec<&ResourceListRow> = rows
        .iter()
        .filter(|row| {
            state
                .namespace_scope
                .resolve(context_namespace)
                .is_none_or(|wanted| Some(wanted) == row.identity.namespace.as_deref())
                && search_matches(row, &needle)
        })
        .collect();
    if let Some(sort) = state.sort.as_ref() {
        sort_rows(&mut filtered, sort);
    }

    let selected: Option<&I> = state.selection.as_ref();
    let detail_row: Option<ResourceListRow> = selected.and_then(|selection| {
        rows.iter()
            .find(|row| I::from_row_identity(&row.identity) == *selection)
            .cloned()
    });
    let mut ratio = state.split_ratio;
    // A pinned identity that no longer exists among the authoritative rows
    // was deleted (or is gone behind the watch); it must never be shown as
    // merely "loading".
    let gone = state.detail.is_some() && detail_row.is_none();
    let detail_shown = state.detail_visible && state.detail.is_some();
    let detail_identity = state
        .detail
        .as_ref()
        .and_then(|detail| detail.identity.as_row_identity());
    let primary_state = detail_identity.and_then(|identity| feed.primary_details.get(identity));
    let detail_view = detail_identity.and_then(|identity| feed.details.get(identity));

    let (list_actions, _) = super::split::show_vertical(
        ui,
        &mut ratio,
        detail_shown,
        |ui| {
            show_table(
                ui,
                window_id,
                !state.search.is_empty()
                    || state.namespace_scope != crate::workspace::NamespaceScope::ContextDefault,
                state.sort.as_ref(),
                &filtered,
                |row| {
                    selected
                        .is_some_and(|selection| *selection == I::from_row_identity(&row.identity))
                },
                |row| I::from_row_identity(&row.identity),
            )
        },
        |ui| {
            if let Some(detail) = state.detail.as_ref() {
                if ui.button("Clear selection").clicked() {
                    queued.push(WorkspaceCommand::ClearSelection(window_id));
                }
                super::detail::show(
                    ui,
                    window_id,
                    detail,
                    primary_state,
                    detail_view,
                    gone,
                    yaml,
                    streams,
                    dialogs,
                    feed,
                    Some(&state.port_drafts),
                    resource_actions,
                    queued,
                );
            }
        },
    );

    if let Some(actions) = list_actions {
        if let Some(sort) = actions.sort {
            queued.push(WorkspaceCommand::SetSort(window_id, Some(sort)));
        }
        if let Some(identity) = actions.selected {
            queued.push(WorkspaceCommand::SelectRow(window_id, identity));
        }
        // Double-click and the row context menu pop a dedicated window out.
        if let Some(identity) = actions.popped_out {
            queued.push(WorkspaceCommand::OpenDedicatedDetail(identity));
        }
    }

    if ratio != state.split_ratio {
        queued.push(WorkspaceCommand::SetSplitRatio(window_id, ratio));
    }
    false
}

/// Table body of the split pane's top half, with the empty-state labels.
#[allow(clippy::too_many_arguments)]
fn show_table<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    filters_active: bool,
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
        // Distinguish "the context has no services at all" from "the
        // active filters removed everything".
        if filters_active {
            ui.label("No services match the current filters");
        } else {
            ui.label("No services");
        }
        return actions;
    }

    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    let header_rows = 1_usize;
    ScrollArea::both()
        .id_salt(("k10s.service.list.scroll", window_id.0))
        .show_rows(ui, row_height, rows.len() + header_rows, |ui, range| {
            egui::Grid::new(("k10s.service.table", window_id.0))
                .striped(true)
                .min_col_width(72.0)
                .show(ui, |ui| {
                    if range.start < header_rows {
                        for (visible, key) in COLUMNS {
                            sort_header(ui, visible, key, sort, &mut actions);
                        }
                        ui.end_row();
                    }

                    for index in range.start.max(header_rows)..range.end {
                        let row = &rows[index - header_rows];
                        service_row(ui, row, &is_selected, &identity_of, &mut actions);
                        ui.end_row();
                    }
                });
        });
    actions
}

fn service_row<I>(
    ui: &mut egui::Ui,
    row: &ResourceListRow,
    is_selected: impl Fn(&ResourceListRow) -> bool,
    identity_of: impl Fn(&ResourceListRow) -> I,
    actions: &mut TableActions<I>,
) where
    I: Clone,
{
    let name_button = if is_selected(row) {
        ui.button(egui::RichText::new(row.identity.name.clone()).strong())
    } else {
        ui.button(row.identity.name.clone())
    };
    let accessible = format!("Select service {}", row.identity.name);
    name_button
        .widget_info(move || WidgetInfo::labeled(WidgetType::Button, true, accessible.clone()));
    if name_button.clicked() {
        actions.selected = Some(identity_of(row));
    }
    if name_button.double_clicked() {
        actions.popped_out = Some(identity_of(row));
    }
    name_button.context_menu(|ui| {
        if ui.button("Open dedicated window").clicked() {
            actions.popped_out = Some(identity_of(row));
            ui.close();
        }
    });

    ui.label(row.identity.namespace.as_deref().unwrap_or("—"));

    // Every structured column renders strictly from the projection; rows
    // without one stay blank placeholders and never fall back to summary.
    match service_projection(row) {
        Some(projection) => {
            ui.label(projection.service_type.clone());
            ui.label(cluster_ip_column_label(projection));
            ui.label(ports_column_label(projection));
        }
        None => {
            ui.label("—");
            ui.label("—");
            ui.label("—");
        }
    }

    ui.monospace(row.created_at.get(..10).unwrap_or(&row.created_at));
}

fn sort_header<I>(
    ui: &mut egui::Ui,
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
        button.widget_info(|| {
            WidgetInfo::labeled(WidgetType::Button, true, format!("Sort services by {key}"))
        });
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
