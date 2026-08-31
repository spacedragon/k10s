//! Cluster overview rendered exclusively from one protocol response.

use std::hash::{Hash, Hasher};

use egui::{Color32, Frame, Margin, RichText, Spinner, Stroke, WidgetInfo, WidgetType};
use k10s_protocol::{HealthLevel, InfrastructureResponse, MetricsCondition};

use super::{
    ConnectionState, InfrastructureLoad,
    infrastructure::{Quantity, usage},
    theme,
};

#[derive(Clone, PartialEq)]
struct LayoutMeasurementKey {
    width_quarters: i32,
    body_font: egui::FontId,
    monospace_font: egui::FontId,
    interact_height_bits: u32,
    item_spacing_x_bits: u32,
    item_spacing_y_bits: u32,
    scroll_width_bits: u32,
    content_hash: u64,
}

#[derive(Clone)]
struct LayoutMeasurement {
    key: LayoutMeasurementKey,
    height: f32,
}

pub(super) fn show(
    ui: &mut egui::Ui,
    response: Option<&InfrastructureResponse>,
    load: InfrastructureLoad,
    connection: ConnectionState,
) -> bool {
    if load == InfrastructureLoad::Unavailable {
        ui.label("Cluster overview is not available in this build");
        let refresh = ui.button("Refresh");
        refresh.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, "Refresh overview"));
        return refresh.clicked();
    }
    let Some(response) = response else {
        ui.centered_and_justified(|ui| {
            ui.horizontal(|ui| {
                ui.add(Spinner::new());
                ui.label("Loading cluster overview");
            })
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

    let content_height = ui
        .available_height()
        .min(ui.clip_rect().bottom() - ui.cursor().top())
        .max(0.0);
    let content_width = ui.available_width();
    let footer_height = current_metrics_footer_height(ui, response, content_width);
    let panel_chrome_height = attention_panel_chrome_height(ui);
    let measured_fixed_height =
        current_fixed_content_height(ui, response, connection, content_width);
    // Account for the explicit gap plus egui's gap between the panel and footer.
    let available_panel_height = content_height - measured_fixed_height - 16.0 - footer_height;

    if available_panel_height >= panel_chrome_height + 48.0 {
        let fixed_top = ui.cursor().top();
        let refresh_requested = fixed_content(ui, response, connection);
        let fixed_height = ui.cursor().top() - fixed_top;
        let available_panel_height = content_height - fixed_height - 16.0 - footer_height;
        let inner_height = if available_panel_height >= panel_chrome_height + 96.0 {
            available_panel_height - panel_chrome_height
        } else {
            (available_panel_height - panel_chrome_height).max(48.0)
        };
        attention_panel(ui, response, inner_height);
        ui.add_space(8.0);
        metrics_footer(ui, response);
        refresh_requested
    } else {
        let mut refresh_requested = false;
        egui::ScrollArea::vertical()
            .id_salt("k10s.overview.compact.scroll")
            .max_height(content_height)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.set_width(content_width - ui.style().spacing.scroll.allocated_width());
                refresh_requested = fixed_content(ui, response, connection);
                attention_panel(ui, response, 48.0);
                ui.add_space(8.0);
                metrics_footer(ui, response);
            });
        refresh_requested
    }
}

fn current_fixed_content_height(
    ui: &egui::Ui,
    response: &InfrastructureResponse,
    connection: ConnectionState,
    width: f32,
) -> f32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::mem::discriminant(&connection).hash(&mut hasher);
    response.generated_at.hash(&mut hasher);
    response.totals.nodes.hash(&mut hasher);
    response.totals.pods.hash(&mut hasher);
    response.totals.workloads.hash(&mut hasher);
    response.totals.persistent_storage_bytes.hash(&mut hasher);
    for usage in [
        response.cluster_cpu,
        response.cluster_memory,
        response.pod_capacity,
    ] {
        usage.used.hash(&mut hasher);
        usage.capacity.hash(&mut hasher);
    }
    for bucket in &response.workload_health {
        bucket.label.hash(&mut hasher);
        bucket.count.hash(&mut hasher);
    }
    let key = layout_measurement_key(ui, width, hasher.finish());
    let measurement_id = ui.make_persistent_id("k10s.overview.current-fixed-height");
    if let Some(measurement) = ui
        .ctx()
        .data(|data| data.get_temp::<LayoutMeasurement>(measurement_id))
        && measurement.key == key
    {
        return measurement.height;
    }

    let measured_width = key.width_quarters as f32 / 4.0;
    let height = measure_isolated(ui, measured_width, |measure_ui| {
        fixed_content(measure_ui, response, connection);
    });
    ui.ctx().data_mut(|data| {
        data.insert_temp(measurement_id, LayoutMeasurement { key, height });
    });
    height
}

fn current_metrics_footer_height(
    ui: &egui::Ui,
    response: &InfrastructureResponse,
    width: f32,
) -> f32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metrics_label(response).hash(&mut hasher);
    response.metrics.detail.hash(&mut hasher);
    response.metrics.source.hash(&mut hasher);
    response.metrics.source_updated_at.hash(&mut hasher);
    let key = layout_measurement_key(ui, width, hasher.finish());
    let measurement_id = ui.make_persistent_id("k10s.overview.current-footer-height");
    if let Some(measurement) = ui
        .ctx()
        .data(|data| data.get_temp::<LayoutMeasurement>(measurement_id))
        && measurement.key == key
    {
        return measurement.height;
    }

    let measured_width = key.width_quarters as f32 / 4.0;
    let height = measure_isolated(ui, measured_width, |measure_ui| {
        metrics_footer(measure_ui, response);
    });
    ui.ctx().data_mut(|data| {
        data.insert_temp(measurement_id, LayoutMeasurement { key, height });
    });
    height
}

fn layout_measurement_key(ui: &egui::Ui, width: f32, content_hash: u64) -> LayoutMeasurementKey {
    let body_font = ui
        .style()
        .text_styles
        .get(&egui::TextStyle::Body)
        .cloned()
        .unwrap_or_else(|| egui::FontId::proportional(14.0));
    let monospace_font = ui
        .style()
        .text_styles
        .get(&egui::TextStyle::Monospace)
        .cloned()
        .unwrap_or_else(|| egui::FontId::monospace(14.0));
    LayoutMeasurementKey {
        width_quarters: (width * 4.0).floor() as i32,
        body_font,
        monospace_font,
        interact_height_bits: ui.spacing().interact_size.y.to_bits(),
        item_spacing_x_bits: ui.spacing().item_spacing.x.to_bits(),
        item_spacing_y_bits: ui.spacing().item_spacing.y.to_bits(),
        scroll_width_bits: ui.style().spacing.scroll.allocated_width().to_bits(),
        content_hash,
    }
}

fn measure_isolated(ui: &egui::Ui, width: f32, contents: impl Fn(&mut egui::Ui)) -> f32 {
    let context = egui::Context::default();
    context.set_style_of(egui::Theme::Dark, ui.style().as_ref().clone());
    context.set_style_of(egui::Theme::Light, ui.style().as_ref().clone());
    let input = egui::RawInput {
        screen_rect: Some(egui::Rect::from_min_size(
            egui::Pos2::ZERO,
            egui::vec2(width, 10_000.0),
        )),
        ..Default::default()
    };
    let mut height = 0.0;
    for _ in 0..2 {
        let mut output = context.run_ui(input.clone(), |measure_ui| {
            measure_ui.set_max_width(width);
            measure_ui.set_width(width);
            let top = measure_ui.cursor().top();
            contents(measure_ui);
            height = measure_ui.cursor().top() - top;
        });
        output.textures_delta.clear();
    }
    height
}

fn fixed_content(
    ui: &mut egui::Ui,
    response: &InfrastructureResponse,
    connection: ConnectionState,
) -> bool {
    let mut refresh_requested = false;
    ui.horizontal(|ui| {
        let (visible, accessible) = if connection == ConnectionState::Connected {
            ("Refresh", "Refresh overview")
        } else {
            ("Retry", "Retry connection")
        };
        let refresh = ui.button(visible);
        refresh.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible));
        refresh_requested = refresh.clicked();
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.weak(format!("Last updated: {}", response.generated_at));
        });
    });
    ui.add_space(6.0);

    summary(ui, response);
    ui.add_space(8.0);
    if ui.available_width() >= 600.0 {
        ui.columns(2, |columns| {
            capacity_panel(&mut columns[0], response);
            health_panel(&mut columns[1], response);
        });
    } else {
        capacity_panel(ui, response);
        ui.add_space(8.0);
        health_panel(ui, response);
    }
    ui.add_space(8.0);
    refresh_requested
}

fn panel() -> Frame {
    Frame::new()
        .fill(Color32::from_gray(29))
        .stroke(Stroke::new(1.0, Color32::from_gray(57)))
        .corner_radius(4.0)
        .inner_margin(Margin::same(9))
}

fn summary(ui: &mut egui::Ui, response: &InfrastructureResponse) {
    ui.columns(4, |columns| {
        let values = [
            format!("{} nodes", response.totals.nodes),
            format!("{} pods", response.totals.pods),
            format!("{} workloads", response.totals.workloads),
            format!(
                "{} persistent storage",
                storage_size(response.totals.persistent_storage_bytes)
            ),
        ];
        for (column, value) in columns.iter_mut().zip(values) {
            panel().show(column, |ui| {
                ui.set_min_width(ui.available_width());
                ui.label(RichText::new(value).monospace().size(15.0));
            });
        }
    });
}

fn capacity_panel(ui: &mut egui::Ui, response: &InfrastructureResponse) {
    panel().show(ui, |ui| {
        ui.set_min_width(ui.available_width());
        ui.strong("Cluster capacity");
        ui.add_space(4.0);
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
    });
}

fn health_panel(ui: &mut egui::Ui, response: &InfrastructureResponse) {
    panel().show(ui, |ui| {
        ui.set_min_width(ui.available_width());
        ui.strong("Workload health");
        ui.add_space(4.0);
        if response.workload_health.is_empty() {
            ui.weak("No workload health data");
        } else {
            egui::Grid::new("k10s.overview.health")
                .num_columns(2)
                .spacing([16.0, 6.0])
                .show(ui, |ui| {
                    for (index, bucket) in response.workload_health.iter().enumerate() {
                        let color = health_color(bucket.level, ui.visuals().error_fg_color);
                        ui.label(
                            RichText::new(format!("● {} {}", bucket.label, bucket.count))
                                .color(color),
                        );
                        if index % 2 == 1 {
                            ui.end_row();
                        }
                    }
                });
        }
    });
}

fn attention_panel_chrome_height(ui: &egui::Ui) -> f32 {
    let frame = panel();
    let heading = egui::WidgetText::from(RichText::new("Needs attention").strong()).into_galley(
        ui,
        Some(egui::TextWrapMode::Extend),
        f32::INFINITY,
        egui::TextStyle::Body,
    );
    frame.total_margin().sum().y
        + heading.size().y.max(ui.spacing().interact_size.y)
        + 4.0
        + ui.style().spacing.scroll.allocated_width()
}

fn attention_panel(ui: &mut egui::Ui, response: &InfrastructureResponse, inner_scroll_height: f32) {
    let frame = panel();
    let frame_height = frame.total_margin().sum().y;
    let panel_height = attention_panel_chrome_height(ui) + inner_scroll_height;
    frame.show(ui, |ui| {
        ui.set_min_width(ui.available_width());
        ui.set_height(panel_height - frame_height);
        ui.strong("Needs attention");
        ui.add_space(4.0);
        if response.attention.is_empty() {
            ui.label("No unhealthy or pending resources");
        } else {
            egui::ScrollArea::both()
                .id_salt("k10s.overview.attention.scroll")
                .max_height(inner_scroll_height)
                .show(ui, |ui| {
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
                });
        }
    });
}

fn metrics_label(response: &InfrastructureResponse) -> String {
    match response.metrics.condition {
        MetricsCondition::Forbidden => format!(
            "Metrics: {} · RBAC forbidden",
            response.metrics.availability
        ),
        MetricsCondition::Stale => format!("Metrics: {} · stale", response.metrics.availability),
        MetricsCondition::Fresh | MetricsCondition::Partial => {
            format!("Metrics: {}", response.metrics.availability)
        }
    }
}

fn metrics_footer(ui: &mut egui::Ui, response: &InfrastructureResponse) {
    let metrics_label = metrics_label(response);
    ui.label(metrics_label)
        .on_hover_text(&response.metrics.detail);
    ui.horizontal_wrapped(|ui| {
        ui.weak(&response.metrics.detail);
        ui.separator();
        ui.weak(format!("Source: {}", response.metrics.source));
        ui.separator();
        ui.weak(format!(
            "Source updated: {}",
            response.metrics.source_updated_at.as_deref().unwrap_or("—")
        ));
    });
}

fn storage_size(bytes: u64) -> String {
    const GIB: u64 = 1_073_741_824;
    const TIB: u64 = 1_099_511_627_776;
    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else {
        format!("{} GiB", bytes / GIB)
    }
}

fn health_color(level: HealthLevel, error: Color32) -> Color32 {
    match level {
        HealthLevel::Healthy => theme::HEALTHY,
        HealthLevel::Warning => theme::WARNING,
        HealthLevel::Failure => error,
    }
}

#[cfg(test)]
mod tests {
    use super::storage_size;
    #[test]
    fn storage_totals_use_readable_binary_units() {
        assert_eq!(storage_size(60 * 1_073_741_824), "60 GiB");
        assert_eq!(storage_size(2 * 1_099_511_627_776), "2.0 TiB");
    }
}
