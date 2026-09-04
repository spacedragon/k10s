//! Searchable functional resource taxonomy backed by workspace state and
//! backend-owned launcher projections.

use super::InfrastructureLoad;
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

#[derive(Debug, Clone, Copy)]
pub(super) struct PortForwardInventory {
    available: bool,
    live: Option<usize>,
}

impl PortForwardInventory {
    pub(super) fn new(available: bool, live: Option<usize>) -> Self {
        Self { available, live }
    }
}

pub(super) fn show<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    response: Option<&k10s_protocol::InfrastructureResponse>,
    load: InfrastructureLoad,
    port_forwards: PortForwardInventory,
    state: &mut LauncherState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing.x = 6.0;
        let (rect, response) = ui.allocate_exact_size(egui::vec2(16.0, 16.0), egui::Sense::hover());
        response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, "⬡"));
        let center = rect.center();
        let radius = 6.5;
        let points: Vec<egui::Pos2> = (0..6)
            .map(|i| {
                let angle = std::f32::consts::PI / 3.0 * i as f32 - std::f32::consts::FRAC_PI_2;
                egui::pos2(
                    center.x + radius * angle.cos(),
                    center.y + radius * angle.sin(),
                )
            })
            .collect();
        ui.painter().add(egui::Shape::closed_line(
            points,
            egui::Stroke::new(1.5, egui::Color32::WHITE),
        ));
        ui.label(
            RichText::new("k10s")
                .size(16.0)
                .strong()
                .color(egui::Color32::WHITE),
        )
        .on_hover_text(format!("k10s v{}", env!("CARGO_PKG_VERSION")));
    });
    ui.label(
        RichText::new("Kubernetes console")
            .size(11.0)
            .color(super::theme::MUTED_TEXT),
    );
    ui.add_space(8.0);
    let filter = ui.add(
        egui::TextEdit::singleline(&mut state.filter)
            .hint_text("Filter resources…")
            .desired_width(ui.available_width()),
    );
    filter.widget_info(|| WidgetInfo::labeled(WidgetType::TextEdit, true, "Filter resources…"));
    ui.add_space(8.0);
    let query = state.filter.trim().to_ascii_lowercase();
    let matches = |label: &str| query.is_empty() || label.to_ascii_lowercase().contains(&query);
    let inventory = Inventory::new(load, response);
    egui::ScrollArea::vertical()
        .id_salt("resource-launcher")
        .auto_shrink([false, false])
        .show(ui, |ui| {
            ui.label(
                RichText::new("CLUSTER")
                    .size(10.0)
                    .color(super::theme::FAINT_TEXT),
            );
            ui.add_space(2.0);
            if matches("Overview") {
                cluster_item(
                    ui,
                    workspace,
                    queued,
                    LauncherItem::Overview,
                    "Overview",
                    None,
                );
            }
            if matches("Events") {
                cluster_item(
                    ui,
                    workspace,
                    queued,
                    LauncherItem::Workload(WorkloadKind::Events),
                    "Events",
                    Some(inventory.warning(response.map(|value| value.launcher.events_warning))),
                );
            }
            if matches("Namespaces") {
                cluster_item(
                    ui,
                    workspace,
                    queued,
                    LauncherItem::Workload(WorkloadKind::Namespaces),
                    "Namespaces",
                    None,
                );
            }
            if matches("Nodes") {
                cluster_item(ui, workspace, queued, LauncherItem::Nodes, "Nodes", None);
            }
            group(
                ui,
                "Workloads",
                inventory.count(response.map(|value| value.launcher.workloads)),
                &mut state.workloads_open,
                &query,
                WorkloadKind::ALL,
                workspace,
                queued,
            );
            group_with_services(
                ui,
                inventory.count(response.map(|value| value.launcher.network)),
                &mut state.network_open,
                &query,
                port_forwards,
                workspace,
                queued,
            );
            group(
                ui,
                "Config",
                inventory.count(response.map(|value| value.launcher.config)),
                &mut state.config_open,
                &query,
                WorkloadKind::CONFIG,
                workspace,
                queued,
            );
            group(
                ui,
                "Storage",
                inventory.count(response.map(|value| value.launcher.storage)),
                &mut state.storage_open,
                &query,
                WorkloadKind::STORAGE,
                workspace,
                queued,
            );
            group(
                ui,
                "Access",
                inventory.count(response.map(|value| value.launcher.access)),
                &mut state.access_open,
                &query,
                WorkloadKind::ACCESS,
                workspace,
                queued,
            );
        });
}

#[allow(clippy::too_many_arguments)]
fn group<I, const N: usize>(
    ui: &mut egui::Ui,
    label: &'static str,
    badge: InventoryBadge,
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
    group_header(ui, label, badge, open, reveal);
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
    badge: InventoryBadge,
    open: &mut bool,
    query: &str,
    port_forwards: PortForwardInventory,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let service_matches = query.is_empty() || "services".contains(query);
    let port_forward_matches =
        port_forwards.available && (query.is_empty() || "port forwards".contains(query));
    let visible: Vec<_> = WorkloadKind::NETWORK
        .into_iter()
        .filter(|kind| query.is_empty() || kind.title().to_ascii_lowercase().contains(query))
        .collect();
    if !query.is_empty() && !service_matches && !port_forward_matches && visible.is_empty() {
        return;
    }
    let reveal = !query.is_empty();
    group_header(ui, "Network", badge, open, reveal);
    if *open || reveal {
        ui.indent("launcher-group-network", |ui| {
            if service_matches {
                cluster_item(
                    ui,
                    workspace,
                    queued,
                    LauncherItem::Services,
                    "Services",
                    None,
                );
            }
            if port_forward_matches {
                singleton_with_count(
                    ui,
                    workspace,
                    queued,
                    LauncherItem::PortForwards,
                    "Port Forwards",
                    port_forwards.live,
                );
            }
            for kind in visible {
                resource(ui, workspace, queued, kind, None);
            }
        });
    }
}

fn horizontal_row<R>(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui) -> R) -> R {
    ui.horizontal(|ui| {
        let initial_size = egui::vec2(ui.available_width(), ui.spacing().interact_size.y);
        let layout =
            egui::Layout::left_to_right(egui::Align::Center).with_main_align(egui::Align::Min);
        ui.allocate_ui_with_layout(initial_size, layout, |ui| {
            ui.spacing_mut().item_spacing = egui::vec2(4.0, 0.0);
            ui.spacing_mut().button_padding = egui::vec2(6.0, 2.0);
            add_contents(ui)
        })
        .inner
    })
    .inner
}

fn singleton_with_count<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    item: LauncherItem,
    label: &'static str,
    count: Option<usize>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let highlighted = workspace.launcher_highlight(item);
    horizontal_row(ui, |ui| {
        let button_width = (ui.available_width() - 32.0).max(40.0);

        let text_color = if highlighted {
            egui::Color32::WHITE
        } else {
            super::theme::TEXT
        };
        let fill = if highlighted {
            super::theme::ACCENT_DARK
        } else {
            egui::Color32::TRANSPARENT
        };

        let btn = egui::Button::new(RichText::new(label).color(text_color))
            .min_size(egui::vec2(button_width, 20.0))
            .fill(fill)
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(4))
            .frame(highlighted)
            .selected(highlighted);

        let button = ui.add(btn);
        if button
            .on_hover_text(if highlighted {
                "Focus window"
            } else {
                "Open window"
            })
            .clicked()
        {
            queued.push(WorkspaceCommand::ActivateLauncherItem(item));
        }
        if let Some(count) = count {
            let badge = ui.monospace(count.to_string());
            let accessible = format!("{count} live Port Forwards");
            badge.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
        }
    });
}

fn group_header(
    ui: &mut egui::Ui,
    label: &'static str,
    badge: InventoryBadge,
    open: &mut bool,
    reveal: bool,
) {
    horizontal_row(ui, |ui| {
        let arrow = if *open || reveal { "▼" } else { "►" };
        let arrow_label = ui.add(
            egui::Label::new(
                RichText::new(arrow)
                    .size(11.0)
                    .color(super::theme::MUTED_TEXT),
            )
            .sense(egui::Sense::click()),
        );

        let width = (ui.available_width() - 32.0).max(30.0);
        let btn_text = RichText::new(label)
            .size(12.0)
            .strong()
            .color(super::theme::TEXT);
        let btn = egui::Button::new(btn_text)
            .min_size(egui::vec2(width, 20.0))
            .frame(false);

        let response = ui.add(btn);
        if response.clicked() || arrow_label.clicked() {
            *open = !*open;
        }

        if !*open && !reveal {
            inventory_badge(ui, badge, label);
        } else {
            let accessible = match badge {
                InventoryBadge::Loading => format!("Loading {label} inventory"),
                InventoryBadge::Count(count) => format!("{count} {label} resources"),
                InventoryBadge::Warning(count) => format!("{count} warning {label} resources"),
                InventoryBadge::Unavailable => format!("{label} inventory unavailable"),
            };
            let hidden_label = ui.add(egui::Label::new(""));
            hidden_label
                .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
        }
    });
}

fn cluster_item<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    item: LauncherItem,
    label: &'static str,
    badge: Option<InventoryBadge>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let highlighted = workspace.launcher_highlight(item);
    let row_height = 20.0;

    horizontal_row(ui, |ui| {
        let button_width = if badge.is_some() {
            (ui.available_width() - 32.0).max(40.0)
        } else {
            ui.available_width()
        };

        let text_color = if highlighted {
            egui::Color32::WHITE
        } else {
            super::theme::TEXT
        };
        let fill = if highlighted {
            super::theme::ACCENT_DARK
        } else {
            egui::Color32::TRANSPARENT
        };

        let btn = egui::Button::new(RichText::new(label).color(text_color))
            .min_size(egui::vec2(button_width, row_height))
            .fill(fill)
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(4))
            .frame(highlighted)
            .selected(highlighted);

        let response = ui.add(btn);
        if response
            .on_hover_text(if highlighted {
                "Focus window"
            } else {
                "Open window"
            })
            .clicked()
        {
            queued.push(WorkspaceCommand::ActivateLauncherItem(item));
        }

        if let LauncherItem::Workload(kind) = item {
            let count = workspace.instance_count(kind);
            if count > 0 {
                let text = format!(
                    "{count} open {label} window{}",
                    if count == 1 { "" } else { "s" }
                );
                let value = ui.add(egui::Label::new(""));
                value.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, text.clone()));
            }
        }

        if let Some(badge) = badge {
            cluster_badge(ui, badge, label);
        }
    });
}

fn cluster_badge(ui: &mut egui::Ui, badge: InventoryBadge, label: &'static str) {
    match badge {
        InventoryBadge::Warning(count) if count > 0 => {
            let accessible = format!("{count} warning {label} resources");
            let badge_btn = egui::Button::new(
                RichText::new(count.to_string())
                    .size(10.0)
                    .strong()
                    .color(egui::Color32::from_rgb(246, 200, 95)),
            )
            .fill(egui::Color32::from_rgb(102, 77, 26))
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(8))
            .min_size(egui::vec2(18.0, 16.0));

            let res = ui.add(badge_btn);
            res.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
        }
        _ => {
            inventory_badge(ui, badge, label);
        }
    }
}

fn resource<I>(
    ui: &mut egui::Ui,
    workspace: &WorkspaceState<I>,
    queued: &mut Vec<WorkspaceCommand<I>>,
    kind: WorkloadKind,
    badge: Option<InventoryBadge>,
) where
    I: Clone + Eq + Hash + Debug,
{
    let label = if kind == WorkloadKind::CustomResources {
        "Custom Resources…"
    } else {
        kind.title()
    };
    let count = workspace.instance_count(kind);
    let highlighted = count > 0;

    horizontal_row(ui, |ui| {
        let button_width = (ui.available_width() - 54.0).max(40.0);

        let text_color = if highlighted {
            egui::Color32::WHITE
        } else {
            super::theme::TEXT
        };
        let fill = if highlighted {
            super::theme::ACCENT_DARK
        } else {
            egui::Color32::TRANSPARENT
        };

        let btn = egui::Button::new(RichText::new(label).color(text_color))
            .min_size(egui::vec2(button_width, 20.0))
            .fill(fill)
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(4))
            .frame(highlighted)
            .selected(highlighted);

        let response = ui.add(btn);
        if response.clicked() {
            queued.push(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Workload(kind),
            ));
        }

        if count > 0 {
            let text = format!(
                "{count} open {label} window{}",
                if count == 1 { "" } else { "s" }
            );
            let count_btn = egui::Button::new(
                RichText::new(count.to_string())
                    .size(10.0)
                    .strong()
                    .color(egui::Color32::from_rgb(217, 241, 252)),
            )
            .fill(egui::Color32::from_rgb(13, 64, 88))
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(8))
            .min_size(egui::vec2(18.0, 16.0));

            let value = ui.add(count_btn);
            value.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, text.clone()));
        } else if let Some(badge) = badge {
            inventory_badge(ui, badge, label);
        } else {
            ui.add_space(20.0);
        }

        let accessible = format!("Open another {label} window");
        let add_btn = egui::Button::new(RichText::new("+").size(12.0).color(super::theme::TEXT))
            .fill(super::theme::CONTROL_BACKGROUND)
            .stroke(egui::Stroke::NONE)
            .corner_radius(egui::CornerRadius::same(3))
            .min_size(egui::vec2(18.0, 18.0));

        let add = ui.add(add_btn);
        add.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, &accessible));
        if add.on_hover_text(&accessible).clicked() {
            queued.push(WorkspaceCommand::AddWorkloadInstance(kind));
        }
    });
}

#[derive(Debug, Clone, Copy)]
enum InventoryBadge {
    Loading,
    Count(u32),
    Warning(u32),
    Unavailable,
}

#[derive(Debug, Clone, Copy)]
struct Inventory(InfrastructureLoad);

impl Inventory {
    fn new(
        load: InfrastructureLoad,
        response: Option<&k10s_protocol::InfrastructureResponse>,
    ) -> Self {
        Self(
            if load == InfrastructureLoad::Available && response.is_none() {
                InfrastructureLoad::Loading
            } else {
                load
            },
        )
    }

    fn count(self, value: Option<u32>) -> InventoryBadge {
        self.badge(value, false)
    }

    fn warning(self, value: Option<u32>) -> InventoryBadge {
        self.badge(value, true)
    }

    fn badge(self, value: Option<u32>, warning: bool) -> InventoryBadge {
        match self.0 {
            InfrastructureLoad::Loading => InventoryBadge::Loading,
            InfrastructureLoad::Unavailable => InventoryBadge::Unavailable,
            InfrastructureLoad::Available => {
                let value = value.unwrap_or_default();
                if warning && value > 0 {
                    InventoryBadge::Warning(value)
                } else {
                    InventoryBadge::Count(value)
                }
            }
        }
    }
}

fn inventory_badge(ui: &mut egui::Ui, badge: InventoryBadge, label: &'static str) {
    let (visible, accessible) = match badge {
        InventoryBadge::Loading => ("…".to_owned(), format!("Loading {label} inventory")),
        InventoryBadge::Count(count) => (count.to_string(), format!("{count} {label} resources")),
        InventoryBadge::Warning(count) => (
            count.to_string(),
            format!("{count} warning {label} resources"),
        ),
        InventoryBadge::Unavailable => ("—".to_owned(), format!("{label} inventory unavailable")),
    };
    let response = ui.label(
        RichText::new(visible)
            .size(11.0)
            .color(super::theme::FAINT_TEXT),
    );
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
}
