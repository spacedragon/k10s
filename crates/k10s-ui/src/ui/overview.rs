//! Overview window rendered exclusively from one protocol response.

use egui::{Color32, ProgressBar, RichText, WidgetInfo, WidgetType};
use k10s_protocol::{CapacityUsage, HealthLevel, InfrastructureResponse, MetricsCondition};

use super::{ConnectionState, theme};

const GIB: f64 = 1_073_741_824.0;
const MISSING_TOOLTIP: &str = "Metric was not reported; — does not mean zero.";

pub(super) fn show(
    ui: &mut egui::Ui,
    response: Option<&InfrastructureResponse>,
    connection: ConnectionState,
) -> bool {
    let Some(response) = response else {
        ui.horizontal(|ui| {
            ui.label("◌");
            ui.label("Loading cluster overview");
        });
        return false;
    };

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

    ui.horizontal_wrapped(|ui| {
        ui.monospace(format!("{} nodes", response.totals.nodes));
        ui.separator();
        ui.monospace(format!("{} pods", response.totals.pods));
        ui.separator();
        ui.monospace(format!("{} workloads", response.totals.workloads));
        ui.separator();
        ui.monospace(format!(
            "{} GiB persistent storage",
            response.totals.persistent_storage_bytes / 1_073_741_824
        ));
    });
    ui.separator();

    cluster_progress(ui, "CPU", response.cluster_cpu, Quantity::Cpu);
    cluster_progress(ui, "Memory", response.cluster_memory, Quantity::Memory);
    cluster_progress(ui, "Pod capacity", response.pod_capacity, Quantity::Pods);

    ui.separator();
    ui.horizontal_wrapped(|ui| {
        for bucket in &response.workload_health {
            let color = health_color(bucket.level, ui.visuals().error_fg_color);
            ui.label(RichText::new(format!("● {} {}", bucket.label, bucket.count)).color(color));
        }
    });

    ui.separator();
    ui.heading("Needs attention");
    if response.attention.is_empty() {
        ui.label("No unhealthy or pending resources");
    } else {
        egui::Grid::new("k10s.overview.attention")
            .striped(true)
            .show(ui, |ui| {
                for heading in ["Namespace", "Kind", "Name", "Status", "Reason"] {
                    ui.strong(heading);
                }
                ui.end_row();
                for row in &response.attention {
                    ui.label(row.namespace.as_deref().unwrap_or("Cluster-scoped"));
                    ui.label(&row.kind);
                    ui.monospace(&row.name);
                    ui.label(&row.status);
                    ui.label(&row.reason);
                    ui.end_row();
                }
            });
    }

    ui.separator();
    let metrics_label = match response.metrics.condition {
        MetricsCondition::Forbidden => {
            format!(
                "Metrics: {} · RBAC forbidden",
                response.metrics.availability
            )
        }
        MetricsCondition::Stale => {
            format!("Metrics: {} · stale", response.metrics.availability)
        }
        MetricsCondition::Fresh | MetricsCondition::Partial => {
            format!("Metrics: {}", response.metrics.availability)
        }
    };
    ui.label(metrics_label);
    ui.label(&response.metrics.detail);
    ui.label(format!("Source: {}", response.metrics.source));
    ui.label(format!(
        "Source updated: {}",
        response.metrics.source_updated_at.as_deref().unwrap_or("—")
    ));

    ui.horizontal(|ui| {
        let refresh = ui.button("Refresh");
        refresh.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, "Refresh overview"));
        ui.label(format!("Last updated: {}", response.generated_at));
        refresh.clicked()
    })
    .inner
}

#[derive(Clone, Copy)]
enum Quantity {
    Cpu,
    Memory,
    Pods,
}

fn cluster_progress(ui: &mut egui::Ui, label: &str, usage: CapacityUsage, quantity: Quantity) {
    let (Some(used), Some(capacity)) = (usage.used, usage.capacity) else {
        ui.label(format!("{label} —"))
            .on_hover_text(MISSING_TOOLTIP);
        return;
    };
    let text = match quantity {
        Quantity::Cpu => format!(
            "{label} {:.1} / {:.1} cores",
            used as f64 / 1_000.0,
            capacity as f64 / 1_000.0
        ),
        Quantity::Memory => format!(
            "{label} {:.1} / {:.1} GiB",
            used as f64 / GIB,
            capacity as f64 / GIB
        ),
        Quantity::Pods => format!("{label} {used} / {capacity} pods"),
    };
    ui.add(
        ProgressBar::new(fraction(used, capacity))
            .text(text)
            .animate(false),
    );
}

fn fraction(used: u64, capacity: u64) -> f32 {
    if capacity == 0 {
        0.0
    } else {
        (used as f64 / capacity as f64).clamp(0.0, 1.0) as f32
    }
}

fn health_color(level: HealthLevel, error: Color32) -> Color32 {
    match level {
        HealthLevel::Healthy => theme::HEALTHY,
        HealthLevel::Warning => theme::WARNING,
        HealthLevel::Failure => error,
    }
}
