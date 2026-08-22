//! Overview window rendered exclusively from one protocol response.

use egui::{Color32, RichText, Spinner, WidgetInfo, WidgetType};
use k10s_protocol::{HealthLevel, InfrastructureResponse, MetricsCondition};

use super::{
    ConnectionState,
    infrastructure::{Quantity, usage},
    theme,
};

pub(super) fn show(
    ui: &mut egui::Ui,
    response: Option<&InfrastructureResponse>,
    connection: ConnectionState,
) -> bool {
    let Some(response) = response else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
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

    usage(ui, response.cluster_cpu, Quantity::Cpu, Some("CPU"));
    usage(
        ui,
        response.cluster_memory,
        Quantity::Memory,
        Some("Memory"),
    );
    usage(
        ui,
        response.pod_capacity,
        Quantity::Pods,
        Some("Pod capacity"),
    );

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
        let (visible, accessible) = if connection == ConnectionState::Connected {
            ("Refresh", "Refresh overview")
        } else {
            ("Retry", "Retry connection")
        };
        let refresh = ui.button(visible);
        refresh.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible));
        ui.label(format!("Last updated: {}", response.generated_at));
        refresh.clicked()
    })
    .inner
}

fn health_color(level: HealthLevel, error: Color32) -> Color32 {
    match level {
        HealthLevel::Healthy => theme::HEALTHY,
        HealthLevel::Warning => theme::WARNING,
        HealthLevel::Failure => error,
    }
}
