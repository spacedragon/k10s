//! Nodes and Storage windows rendered from protocol infrastructure rows.

use std::cmp::Ordering;

use egui::{ProgressBar, RichText, Spinner, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{CapacityUsage, InfrastructureResponse, NodeRow};

use super::{ConnectionState, InfrastructureLoad, theme};

const GIB: f64 = 1_073_741_824.0;
const MISSING_TOOLTIP: &str = "Metric was not reported; — does not mean zero.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeSort {
    Name,
    Status,
    Roles,
    KubernetesVersion,
    Cpu,
    Memory,
    Pods,
    Age,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageTab {
    PersistentVolumeClaims,
    PersistentVolumes,
    StorageClasses,
}

/// Window-local controls. Authoritative rows remain in the protocol
/// response and are never duplicated into this state.
#[derive(Debug)]
pub(super) struct InfrastructureUiState {
    node_search: String,
    node_sort: NodeSort,
    node_sort_ascending: bool,
    storage_tab: StorageTab,
}

impl Default for InfrastructureUiState {
    fn default() -> Self {
        Self {
            node_search: String::new(),
            node_sort: NodeSort::Name,
            node_sort_ascending: true,
            storage_tab: StorageTab::PersistentVolumeClaims,
        }
    }
}

pub(super) fn show_nodes(
    ui: &mut egui::Ui,
    state: &mut InfrastructureUiState,
    response: Option<&InfrastructureResponse>,
    load: InfrastructureLoad,
    connection: ConnectionState,
) {
    if load == InfrastructureLoad::Unavailable {
        ui.label("Cluster infrastructure is not available in this build");
        return;
    }
    let Some(response) = response else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label("Loading node inventory");
        });
        return;
    };

    stale_banner(ui, response, connection);

    let ready = response
        .nodes
        .iter()
        .filter(|node| node.status == "Ready")
        .count();
    let not_ready = response.nodes.len().saturating_sub(ready);
    ui.horizontal(|ui| {
        ui.label(format!("Ready {ready}"));
        ui.separator();
        ui.label(format!("Not Ready {not_ready}"));
    });

    let search = ui.add(
        TextEdit::singleline(&mut state.node_search)
            .hint_text("Search nodes")
            .desired_width(240.0),
    );
    search.widget_info(|| WidgetInfo::labeled(WidgetType::TextEdit, true, "Search nodes"));

    let mut rows: Vec<&NodeRow> = response
        .nodes
        .iter()
        .filter(|node| {
            state.node_search.is_empty()
                || node
                    .name
                    .to_lowercase()
                    .contains(&state.node_search.to_lowercase())
                || node.roles.iter().any(|role| {
                    role.to_lowercase()
                        .contains(&state.node_search.to_lowercase())
                })
        })
        .collect();
    rows.sort_by(|left, right| {
        let order = match state.node_sort {
            NodeSort::Name => left.name.cmp(&right.name),
            NodeSort::Status => status_order(&left.status, &right.status),
            NodeSort::Roles => left.roles.cmp(&right.roles),
            NodeSort::KubernetesVersion => left.kubernetes_version.cmp(&right.kubernetes_version),
            NodeSort::Cpu => usage_order(left.cpu, right.cpu),
            NodeSort::Memory => usage_order(left.memory, right.memory),
            NodeSort::Pods => usage_order(left.pods, right.pods),
            NodeSort::Age => age_order(&left.age, &right.age),
        }
        .then_with(|| left.name.cmp(&right.name));
        if state.node_sort_ascending {
            order
        } else {
            order.reverse()
        }
    });

    if rows.is_empty() {
        if state.node_search.is_empty() {
            ui.label("No Nodes in this context");
        } else {
            ui.label("No resources match these filters");
            if ui.button("Clear filters").clicked() {
                state.node_search.clear();
            }
        }
        return;
    }

    egui::ScrollArea::both()
        .id_salt("k10s.nodes.table.scroll")
        .show(ui, |ui| {
            egui::Grid::new("k10s.nodes.table")
                .striped(true)
                .min_col_width(72.0)
                .show(ui, |ui| {
                    sort_header(ui, state, NodeSort::Name, "Name", "name");
                    sort_header(ui, state, NodeSort::Status, "Status", "status");
                    sort_header(ui, state, NodeSort::Roles, "Roles", "roles");
                    sort_header(
                        ui,
                        state,
                        NodeSort::KubernetesVersion,
                        "Kubernetes version",
                        "Kubernetes version",
                    );
                    sort_header(ui, state, NodeSort::Cpu, "CPU", "CPU");
                    sort_header(ui, state, NodeSort::Memory, "Memory", "memory");
                    sort_header(ui, state, NodeSort::Pods, "Pods", "pods");
                    sort_header(ui, state, NodeSort::Age, "Age", "age");
                    ui.end_row();

                    for node in rows {
                        ui.monospace(&node.name);
                        ui.label(&node.status);
                        ui.label(node.roles.join(", "));
                        ui.monospace(&node.kubernetes_version);
                        usage(ui, node.cpu, Quantity::Cpu, None);
                        usage(ui, node.memory, Quantity::Memory, None);
                        usage(ui, node.pods, Quantity::Pods, None);
                        ui.monospace(&node.age);
                        ui.end_row();
                    }
                });
        });
}

pub(super) fn show_storage(
    ui: &mut egui::Ui,
    state: &mut InfrastructureUiState,
    response: Option<&InfrastructureResponse>,
    load: InfrastructureLoad,
    connection: ConnectionState,
) {
    if load == InfrastructureLoad::Unavailable {
        ui.label("Cluster infrastructure is not available in this build");
        return;
    }
    let Some(response) = response else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label("Loading storage inventory");
        });
        return;
    };

    stale_banner(ui, response, connection);

    ui.horizontal(|ui| {
        for (tab, label) in [
            (StorageTab::PersistentVolumeClaims, "PersistentVolumeClaims"),
            (StorageTab::PersistentVolumes, "PersistentVolumes"),
            (StorageTab::StorageClasses, "StorageClasses"),
        ] {
            if ui
                .selectable_label(state.storage_tab == tab, label)
                .clicked()
            {
                state.storage_tab = tab;
            }
        }
    });
    ui.separator();

    egui::ScrollArea::both()
        .id_salt("k10s.storage.table.scroll")
        .show(ui, |ui| match state.storage_tab {
            StorageTab::PersistentVolumeClaims => {
                if response.storage.persistent_volume_claims.is_empty() {
                    ui.label("No PersistentVolumeClaims in this namespace");
                    return;
                }
                egui::Grid::new("k10s.storage.pvcs")
                    .striped(true)
                    .show(ui, |ui| {
                        headings(
                            ui,
                            &[
                                "Namespace",
                                "Name",
                                "Status",
                                "Capacity",
                                "Access modes",
                                "Class",
                                "Bound volume",
                                "Age",
                            ],
                        );
                        for row in &response.storage.persistent_volume_claims {
                            cells(
                                ui,
                                &[
                                    &row.namespace,
                                    &row.name,
                                    &row.status,
                                    &row.capacity,
                                    &row.access_modes.join(", "),
                                    &row.storage_class,
                                    &row.bound_volume,
                                    &row.age,
                                ],
                            );
                        }
                    });
            }
            StorageTab::PersistentVolumes => {
                if response.storage.persistent_volumes.is_empty() {
                    ui.label("No PersistentVolumes");
                    return;
                }
                egui::Grid::new("k10s.storage.pvs")
                    .striped(true)
                    .show(ui, |ui| {
                        headings(
                            ui,
                            &[
                                "Name",
                                "Status",
                                "Capacity",
                                "Access modes",
                                "Class",
                                "Bound claim",
                                "Reclaim policy",
                                "Age",
                            ],
                        );
                        for row in &response.storage.persistent_volumes {
                            cells(
                                ui,
                                &[
                                    &row.name,
                                    &row.status,
                                    &row.capacity,
                                    &row.access_modes.join(", "),
                                    &row.storage_class,
                                    &row.bound_claim,
                                    &row.reclaim_policy,
                                    &row.age,
                                ],
                            );
                        }
                    });
            }
            StorageTab::StorageClasses => {
                if response.storage.storage_classes.is_empty() {
                    ui.label("No StorageClasses");
                    return;
                }
                egui::Grid::new("k10s.storage.classes")
                    .striped(true)
                    .show(ui, |ui| {
                        headings(
                            ui,
                            &[
                                "Name",
                                "Provisioner",
                                "Reclaim policy",
                                "Binding mode",
                                "Age",
                            ],
                        );
                        for row in &response.storage.storage_classes {
                            cells(
                                ui,
                                &[
                                    &row.name,
                                    &row.provisioner,
                                    &row.reclaim_policy,
                                    &row.volume_binding_mode,
                                    &row.age,
                                ],
                            );
                        }
                    });
            }
        });
}

fn headings(ui: &mut egui::Ui, values: &[&str]) {
    for value in values {
        ui.strong(*value);
    }
    ui.end_row();
}

fn cells(ui: &mut egui::Ui, values: &[&str]) {
    for value in values {
        ui.label(*value);
    }
    ui.end_row();
}

#[derive(Clone, Copy)]
pub(super) enum Quantity {
    Cpu,
    Memory,
    Pods,
}

pub(super) fn usage(
    ui: &mut egui::Ui,
    usage: CapacityUsage,
    quantity: Quantity,
    label: Option<&str>,
) {
    let (Some(used), Some(capacity)) = (usage.used, usage.capacity) else {
        ui.label(label.map_or_else(|| "—".to_owned(), |label| format!("{label} —")))
            .on_hover_text(MISSING_TOOLTIP);
        return;
    };
    let value = match quantity {
        Quantity::Cpu => format!(
            "{:.1} / {:.1} cores",
            used as f64 / 1_000.0,
            capacity as f64 / 1_000.0
        ),
        Quantity::Memory => format!(
            "{:.1} / {:.1} GiB",
            used as f64 / GIB,
            capacity as f64 / GIB
        ),
        Quantity::Pods => format!("{used} / {capacity} pods"),
    };
    let text = label.map_or(value.clone(), |label| format!("{label} {value}"));
    let fraction = if capacity == 0 {
        0.0
    } else {
        (used as f64 / capacity as f64).clamp(0.0, 1.0) as f32
    };
    ui.add(ProgressBar::new(fraction).text(text).animate(false));
}

fn status_order(left: &str, right: &str) -> Ordering {
    let weight = |status: &str| usize::from(status == "Ready");
    weight(left).cmp(&weight(right))
}

fn usage_order(left: CapacityUsage, right: CapacityUsage) -> Ordering {
    match (left.used, right.used) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.capacity.cmp(&right.capacity),
    }
}

fn age_order(left: &str, right: &str) -> Ordering {
    match (age_seconds(left), age_seconds(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn age_seconds(age: &str) -> Option<u64> {
    for (suffix, multiplier) in [
        ("y", 365 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ] {
        if let Some(value) = age.strip_suffix(suffix) {
            return value.parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    None
}

fn stale_banner(ui: &mut egui::Ui, response: &InfrastructureResponse, connection: ConnectionState) {
    if connection != ConnectionState::Connected {
        ui.label(
            RichText::new(format!(
                "Connection stale · showing last successful update from {}",
                response.generated_at
            ))
            .color(theme::WARNING),
        );
        ui.separator();
    }
}

fn sort_header(
    ui: &mut egui::Ui,
    state: &mut InfrastructureUiState,
    sort: NodeSort,
    visible: &str,
    accessible: &str,
) {
    ui.horizontal(|ui| {
        ui.label(visible);
        let button = ui.small_button(if state.node_sort == sort {
            if state.node_sort_ascending {
                "↑"
            } else {
                "↓"
            }
        } else {
            "↕"
        });
        let label = format!("Sort nodes by {accessible}");
        button.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
        if button.clicked() {
            if state.node_sort == sort {
                state.node_sort_ascending = !state.node_sort_ascending;
            } else {
                state.node_sort = sort;
                state.node_sort_ascending = true;
            }
        }
    });
}
