//! Compact global menus, connection state, refresh, and context selection.

use egui::{
    ComboBox, Label, Layout, MenuBar, RichText, Sense, ViewportCommand, WidgetInfo, WidgetType,
};
use k10s_protocol::{Context, ContextAvailability};

use super::{ConnectionState, theme};

pub(super) struct TopBarAction {
    pub(super) context_change: Option<String>,
    pub(super) refresh: bool,
    pub(super) toggle_free_window_resizing: bool,
    pub(super) layout: Option<LayoutAction>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LayoutAction {
    Tile,
    Cascade,
    Focus,
}

pub(super) fn show(
    ui: &mut egui::Ui,
    connection: ConnectionState,
    contexts: &[Context],
    selected_context: Option<&str>,
    free_window_resizing: bool,
    traffic: &[k10s_protocol::TrafficSample],
) -> TopBarAction {
    let mut context_change = None;
    let mut refresh = false;
    let mut toggle_free_window_resizing = false;
    let mut layout = None;
    let compact = ui.available_width() < 760.0;

    MenuBar::new().ui(ui, |ui| {
        ui.push_id("k10s.top_bar.menus", |ui| {
            ui.menu_button("File", |ui| {
                if ui.button("Exit").clicked() {
                    ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                    ui.close();
                }
            });
            ui.menu_button("View", |ui| {
                let mut enabled = free_window_resizing;
                if ui.checkbox(&mut enabled, "Free window resizing").clicked() {
                    toggle_free_window_resizing = true;
                    ui.close();
                }

                if ui.button("Minimize").clicked() {
                    ui.ctx().send_viewport_cmd(ViewportCommand::Minimized(true));
                    ui.close();
                }

                let fullscreen = ui
                    .ctx()
                    .input(|input| input.viewport().fullscreen.unwrap_or(false));
                let fullscreen_label = if fullscreen {
                    "Exit full screen"
                } else {
                    "Enter full screen"
                };
                if ui.button(fullscreen_label).clicked() {
                    ui.ctx()
                        .send_viewport_cmd(ViewportCommand::Fullscreen(!fullscreen));
                    ui.close();
                }
            });
            ui.menu_button("Window", |ui| {
                if ui.button("Tile windows").clicked() {
                    layout = Some(LayoutAction::Tile);
                    ui.close();
                }
                if ui.button("Cascade windows").clicked() {
                    layout = Some(LayoutAction::Cascade);
                    ui.close();
                }
                if ui.button("Focus active window").clicked() {
                    layout = Some(LayoutAction::Focus);
                    ui.close();
                }
            });
            ui.menu_button("Help", |ui| {
                ui.hyperlink_to(
                    "Documentation",
                    "https://github.com/spacedragon/k10s#readme",
                );
                ui.menu_button("Keyboard shortcuts", |ui| {
                    ui.label("Command palette: : or Ctrl+K");
                    ui.label("Refresh resources: Ctrl+R");
                    ui.label("Close window: use the window close button");
                });
                ui.menu_button("About k10s", |ui| {
                    ui.label(format!("k10s {}", env!("CARGO_PKG_VERSION")));
                    ui.label("A Kubernetes desktop dashboard");
                });
            });
        });

        ui.separator();
        ui.push_id("k10s.top_bar.layout", |ui| {
            for (label, action, help) in [
                ("Tile", LayoutAction::Tile, "Tile all workspace windows"),
                (
                    if compact { "Stack" } else { "Cascade" },
                    LayoutAction::Cascade,
                    "Cascade workspace windows",
                ),
                (
                    "Focus",
                    LayoutAction::Focus,
                    "Focus the active window or restore the layout",
                ),
            ] {
                if ui.button(label).on_hover_text(help).clicked() {
                    layout = Some(action);
                }
            }
        });

        ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
            show_traffic(ui, connection, traffic, compact);

            ui.push_id("k10s.top_bar.context", |ui| {
                ui.add_enabled_ui(!contexts.is_empty(), |ui| {
                    let selected_text = selected_context.unwrap_or("No contexts");
                    let compact_text = compact.then(|| {
                        let mut chars = selected_text.chars();
                        let prefix = chars.by_ref().take(9).collect::<String>();
                        if chars.next().is_some() {
                            format!("{prefix}...")
                        } else {
                            prefix
                        }
                    });
                    let combo = if compact {
                        ComboBox::from_id_salt("selector")
                    } else {
                        ComboBox::new("selector", "Kubernetes context")
                    };
                    let response = combo
                        .selected_text(compact_text.as_deref().unwrap_or(selected_text))
                        .width(if compact { 96.0 } else { 180.0 })
                        .show_ui(ui, |ui| {
                            for context in contexts {
                                if context.availability == ContextAvailability::Unavailable {
                                    let reason = context
                                        .unavailable_reason
                                        .as_deref()
                                        .unwrap_or("credential plugin is unavailable");
                                    ui.add(
                                        Label::new(
                                            RichText::new(&context.name)
                                                .color(ui.visuals().weak_text_color()),
                                        )
                                        .sense(Sense::hover()),
                                    )
                                    .on_hover_text(reason);
                                } else if ui
                                    .selectable_label(
                                        selected_context == Some(context.name.as_str()),
                                        &context.name,
                                    )
                                    .clicked()
                                {
                                    context_change = Some(context.name.clone());
                                    ui.close();
                                }
                            }
                        })
                        .response;
                    response.widget_info(|| {
                        WidgetInfo::labeled(WidgetType::ComboBox, true, "Kubernetes context")
                    });
                    response.on_hover_text(selected_text);
                });
            });

            ui.push_id("k10s.top_bar.refresh", |ui| {
                let label = if connection == ConnectionState::Connected {
                    "Refresh"
                } else {
                    "Retry"
                };
                let response = ui.button(if compact { "↻" } else { label });
                response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label));
                refresh = response
                    .on_hover_text(if connection == ConnectionState::Connected {
                        "Refresh all resources"
                    } else {
                        "Retry the control connection"
                    })
                    .clicked();
            });

            ui.separator();
            let version = if compact {
                format!("v{}", env!("CARGO_PKG_VERSION"))
            } else {
                format!("k10s v{}", env!("CARGO_PKG_VERSION"))
            };
            ui.label(version).on_hover_text("Application version");

            ui.separator();
            ui.label(connection.label());
            let color = match connection {
                ConnectionState::Connecting => theme::CONNECTING,
                ConnectionState::Connected => theme::HEALTHY,
                ConnectionState::Failed => ui.visuals().error_fg_color,
            };
            let dot = ui.label(RichText::new("●").color(color));
            dot.widget_info(|| {
                WidgetInfo::labeled(
                    WidgetType::Label,
                    true,
                    format!("Connection status: {}", connection.label()),
                )
            });
        });
    });

    TopBarAction {
        context_change,
        refresh,
        toggle_free_window_resizing,
        layout,
    }
}

fn show_traffic(
    ui: &mut egui::Ui,
    connection: ConnectionState,
    history: &[k10s_protocol::TrafficSample],
    compact: bool,
) {
    let latest = history.last();
    let (down, up) = latest
        .map(|sample| {
            (
                sample.download_bytes_per_second,
                sample.upload_bytes_per_second,
            )
        })
        .unwrap_or_default();
    let label = if compact {
        format!("↓{} ↑{}", format_rate(down), format_rate(up))
    } else {
        format!("↓ {}/s   ↑ {}/s", format_bytes(down), format_bytes(up))
    };
    let status = if connection != ConnectionState::Connected {
        "stale"
    } else if latest.is_none() {
        "waiting"
    } else if down == 0 && up == 0 {
        "idle"
    } else {
        "live"
    };
    let response = ui
        .horizontal(|ui| {
            let color = match status {
                "live" => theme::HEALTHY,
                "waiting" => theme::CONNECTING,
                _ => ui.visuals().weak_text_color(),
            };
            ui.label(RichText::new("●").color(color));
            ui.label(RichText::new(label).monospace());
            if !compact && history.len() > 1 {
                sparkline(ui, history);
            }
        })
        .response;
    response.widget_info(|| {
        WidgetInfo::labeled(
            WidgetType::Other,
            true,
            format!("Cluster traffic: {status}"),
        )
    });
    let detail = latest.map_or_else(
        || "Waiting for the first Kubernetes API traffic sample".to_owned(),
        |sample| format!(
            "Kubernetes API traffic ({status})\nDownloaded: {}\nUploaded: {}\nRequests: {}\nActive requests: {}\nCounts traffic between this server and the selected cluster; payload content is never recorded.",
            format_bytes(sample.downloaded_bytes_total),
            format_bytes(sample.uploaded_bytes_total),
            sample.requests_total,
            sample.active_requests,
        ),
    );
    response.on_hover_text(detail);
}

fn sparkline(ui: &mut egui::Ui, history: &[k10s_protocol::TrafficSample]) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(68.0, 20.0), Sense::hover());
    let max = history
        .iter()
        .flat_map(|sample| {
            [
                sample.download_bytes_per_second,
                sample.upload_bytes_per_second,
            ]
        })
        .max()
        .unwrap_or(1)
        .max(1) as f32;
    let draw = |values: Vec<u64>, color: egui::Color32| {
        let count = values.len().max(2);
        let points = values.into_iter().enumerate().map(|(index, value)| {
            let x = rect.left() + rect.width() * index as f32 / (count - 1) as f32;
            let y = rect.bottom() - rect.height() * value as f32 / max;
            egui::pos2(x, y)
        });
        ui.painter().add(egui::Shape::line(
            points.collect(),
            egui::Stroke::new(1.25, color),
        ));
    };
    draw(
        history
            .iter()
            .map(|sample| sample.download_bytes_per_second)
            .collect(),
        theme::HEALTHY,
    );
    draw(
        history
            .iter()
            .map(|sample| sample.upload_bytes_per_second)
            .collect(),
        theme::CONNECTING,
    );
}

fn format_rate(bytes: u64) -> String {
    if bytes < 1_000 {
        format!("{bytes}B")
    } else if bytes < 1_000_000 {
        format!("{:.0}K", bytes as f64 / 1_000.0)
    } else {
        format!("{:.1}M", bytes as f64 / 1_000_000.0)
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1_000.0 && unit < UNITS.len() - 1 {
        value /= 1_000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::{format_bytes, format_rate};

    #[test]
    fn traffic_units_stay_compact_and_readable() {
        assert_eq!(format_rate(999), "999B");
        assert_eq!(format_rate(1_250), "1K");
        assert_eq!(format_rate(1_250_000), "1.2M");
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_500_000), "1.5 MB");
    }
}
