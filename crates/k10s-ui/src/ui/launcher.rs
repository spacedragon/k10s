//! Searchable functional resource taxonomy backed by workspace state and
//! backend-owned launcher projections.

use crate::workspace::{LauncherItem, WorkloadKind, WorkspaceCommand, WorkspaceState};
use egui::{RichText, WidgetInfo, WidgetType};
use std::{fmt::Debug, hash::Hash};

#[derive(Debug)]
pub(super) struct LauncherState {
    filter: String,
    workloads_open: bool,
    network_open: bool,
    config_open: bool,
    storage_open: bool,
    access_open: bool,
}

impl Default for LauncherState {
    fn default() -> Self {
        Self {
            filter: String::new(),
            workloads_open: true,
            network_open: true,
            config_open: false,
            storage_open: false,
            access_open: false,
        }
    }
}

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    state: &mut LauncherState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    ui.horizontal(|ui| {
        ui.label(
            RichText::new("◈")
                .size(18.0)
                .strong()
                .color(super::theme::ACCENT),
        );
        ui.vertical(|ui| {
            ui.label(RichText::new("k10s").size(16.0).strong());
            ui.label(
                RichText::new("KUBERNETES CONSOLE")
                    .small()
                    .color(super::theme::MUTED_TEXT),
            );
        });
    });
    ui.add_space(5.0);
    ui.separator();
    ui.add_space(4.0);
    let filter =
        ui.add(egui::TextEdit::singleline(&mut state.filter).hint_text("Filter resources…"));
    filter.widget_info(|| WidgetInfo::labeled(WidgetType::TextEdit, true, "Filter resources…"));
    let query = state.filter.trim().to_ascii_lowercase();
    let matches = |label: &str| query.is_empty() || label.to_ascii_lowercase().contains(&query);
    let counts = response.map(|value| value.launcher).unwrap_or_default();
    ui.small("CLUSTER");
    if matches("Overview") {
        singleton(ui, workspace, queued, LauncherItem::Overview, "Overview");
    }
    if matches("Events") {
        resource(
            ui,
            workspace,
            queued,
            WorkloadKind::Events,
            Some((counts.events_warning, "warn")),
        );
    }
    if matches("Namespaces") {
        resource(ui, workspace, queued, WorkloadKind::Namespaces, None);
    }
    if matches("Nodes") {
        singleton(ui, workspace, queued, LauncherItem::Nodes, "Nodes");
    }
    group(
        ui,
        "Workloads",
        counts.workloads,
        &mut state.workloads_open,
        &query,
        WorkloadKind::ALL,
        workspace,
        queued,
    );
    group_with_services(
        ui,
        counts.network,
        &mut state.network_open,
        &query,
        workspace,
        queued,
    );
    group(
        ui,
        "Config",
        counts.config,
        &mut state.config_open,
        &query,
        WorkloadKind::CONFIG,
        workspace,
        queued,
    );
    group(
        ui,
        "Storage",
        counts.storage,
        &mut state.storage_open,
        &query,
        WorkloadKind::STORAGE,
        workspace,
        queued,
    );
    group(
        ui,
        "Access",
        counts.access,
        &mut state.access_open,
        &query,
        WorkloadKind::ACCESS,
        workspace,
        queued,
    );
}

#[allow(clippy::too_many_arguments)]
fn group<I, const N: usize>(
    ui: &mut egui::Ui,
    label: &'static str,
    count: u32,
    open: &mut bool,
    query: &str,
    kinds: [WorkloadKind; N],
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let visible: Vec<_> = kinds
        .into_iter()
        .filter(|kind| query.is_empty() || kind.title().to_ascii_lowercase().contains(query))
        .collect();
    if !query.is_empty() && visible.is_empty() {
        return;
    }
    let reveal = !query.is_empty();
    group_header(ui, label, count, open, reveal);
    if *open || reveal {
        ui.indent(("launcher-group", label), |ui| {
            for kind in visible {
                resource(ui, workspace, queued, kind, None);
            }
        });
    }
}

fn group_with_services<I>(
    ui: &mut egui::Ui,
    count: u32,
    open: &mut bool,
    query: &str,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let service_matches = query.is_empty() || "services".contains(query);
    let visible: Vec<_> = WorkloadKind::NETWORK
        .into_iter()
        .filter(|kind| query.is_empty() || kind.title().to_ascii_lowercase().contains(query))
        .collect();
    if !query.is_empty() && !service_matches && visible.is_empty() {
        return;
    }
    let reveal = !query.is_empty();
    group_header(ui, "Network", count, open, reveal);
    if *open || reveal {
        ui.indent("launcher-group-network", |ui| {
            if service_matches {
                singleton(ui, workspace, queued, LauncherItem::Services, "Services");
            }
            for kind in visible {
                resource(ui, workspace, queued, kind, None);
            }
        });
    }
}

fn group_header(ui: &mut egui::Ui, label: &'static str, count: u32, open: &mut bool, reveal: bool) {
    ui.horizontal(|ui| {
        let arrow = if *open || reveal { "▾" } else { "▸" };
        ui.label(arrow);
        if ui.button(label).clicked() {
            *open = !*open;
        }
        let badge = ui.monospace(count.to_string());
        badge.widget_info(|| {
            WidgetInfo::labeled(
                WidgetType::Label,
                true,
                format!("{count} {label} resources"),
            )
        });
    });
}

fn singleton<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    item: LauncherItem,
    label: &'static str,
) where
    I: Clone + Eq + Hash + Debug,
{
    let highlighted = workspace.launcher_highlight(item);
    if ui
        .selectable_label(highlighted, label)
        .on_hover_text(if highlighted {
            "Focus window"
        } else {
            "Open window"
        })
        .clicked()
    {
        queued.push(WorkspaceCommand::ActivateLauncherItem(item));
    }
}

fn resource<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    kind: WorkloadKind,
    badge: Option<(u32, &'static str)>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let label = if kind == WorkloadKind::CustomResources {
        "Custom Resources…"
    } else {
        kind.title()
    };
    let count = workspace.instance_count(kind);
    ui.horizontal(|ui| {
        let available = (ui.available_width() - 48.0).max(48.0);
        if ui
            .add_sized(
                [available, 18.0],
                egui::Button::selectable(count > 0, label),
            )
            .clicked()
        {
            queued.push(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Workload(kind),
            ));
        }
        if count > 0 {
            let text = format!(
                "{count} open {label} window{}",
                if count == 1 { "" } else { "s" }
            );
            let value = ui.monospace(count.to_string());
            value.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, text.clone()));
        } else if let Some((value, suffix)) = badge.filter(|(value, _)| *value > 0) {
            ui.label(format!("{value} {suffix}"));
        } else {
            ui.add_space(8.0);
        }
        let accessible = format!("Open another {label} window");
        let add = ui.small_button("+");
        add.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, &accessible));
        if add.on_hover_text(&accessible).clicked() {
            queued.push(WorkspaceCommand::AddWorkloadInstance(kind));
        }
    });
}
