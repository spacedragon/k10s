//! Fixed launcher backed directly by workspace highlight and count queries.

use std::fmt::Debug;
use std::hash::Hash;

use egui::{CollapsingHeader, RichText, WidgetInfo, WidgetType};

use crate::workspace::{LauncherItem, WorkloadKind, WorkspaceCommand, WorkspaceState};

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
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
    ui.label(
        RichText::new("CLUSTER")
            .small()
            .color(super::theme::MUTED_TEXT),
    );
    singleton(ui, workspace, queued, LauncherItem::Overview, "Overview");
    singleton(ui, workspace, queued, LauncherItem::Nodes, "Nodes");
    singleton(ui, workspace, queued, LauncherItem::Storage, "Storage");

    ui.add_space(3.0);
    CollapsingHeader::new(RichText::new("Network").strong())
        .id_salt("k10s.launcher.network")
        .default_open(true)
        .show(ui, |ui| {
            singleton(ui, workspace, queued, LauncherItem::Services, "Services");
        });
    CollapsingHeader::new(RichText::new("Workloads").strong())
        .id_salt("k10s.launcher.workloads")
        .default_open(true)
        .show(ui, |ui| {
            for kind in WorkloadKind::ALL {
                workload(ui, workspace, queued, kind);
            }
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
    ui.push_id(("k10s.launcher.singleton", label), |ui| {
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
    });
}

fn workload<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    kind: WorkloadKind,
) where
    I: Clone + Eq + Hash + Debug,
{
    let label = launcher_label(kind);
    let count = workspace.instance_count(kind);
    ui.push_id(("k10s.launcher.workload", kind), |ui| {
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
                let count_text = if count == 1 {
                    format!("1 open {label} window")
                } else {
                    format!("{count} open {label} windows")
                };
                let badge = ui.monospace(count.to_string());
                badge.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, count_text.clone())
                });
            } else {
                ui.add_space(8.0);
            }

            ui.push_id("add", |ui| {
                let accessible_label = format!("Open another {label} window");
                let add = ui.small_button("+");
                add.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Button, true, &accessible_label)
                });
                if add.on_hover_text(&accessible_label).clicked() {
                    queued.push(WorkspaceCommand::AddWorkloadInstance(kind));
                }
            });
        });
    });
}

fn launcher_label(kind: WorkloadKind) -> &'static str {
    match kind {
        WorkloadKind::CustomResources => "Custom Resources…",
        _ => kind.title(),
    }
}
