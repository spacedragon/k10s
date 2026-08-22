//! Compact global menus, connection state, refresh, and context selection.

use egui::{ComboBox, Layout, MenuBar, RichText, WidgetInfo, WidgetType};

use super::{ConnectionState, theme};

pub(super) struct TopBarAction {
    pub(super) context_change: Option<String>,
    pub(super) refresh: bool,
}

pub(super) fn show(
    ui: &mut egui::Ui,
    connection: ConnectionState,
    contexts: &[String],
    selected_context: Option<&str>,
) -> TopBarAction {
    let mut context_change = None;
    let mut refresh = false;

    MenuBar::new().ui(ui, |ui| {
        ui.push_id("k10s.top_bar.menus", |ui| {
            ui.menu_button("File", |_| {});
            ui.menu_button("View", |_| {});
            ui.menu_button("Help", |_| {});
        });

        ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
            ui.push_id("k10s.top_bar.context", |ui| {
                ui.add_enabled_ui(!contexts.is_empty(), |ui| {
                    let selected_text = selected_context.unwrap_or("No contexts");
                    let response = ComboBox::new("selector", "Kubernetes context")
                        .selected_text(selected_text)
                        .width(220.0)
                        .show_ui(ui, |ui| {
                            for context in contexts {
                                if ui
                                    .selectable_label(selected_context == Some(context), context)
                                    .clicked()
                                {
                                    context_change = Some(context.clone());
                                    ui.close();
                                }
                            }
                        })
                        .response;
                    response.on_hover_text(selected_text);
                });
            });

            ui.push_id("k10s.top_bar.refresh", |ui| {
                refresh = ui
                    .button("Refresh")
                    .on_hover_text("Refresh all resources")
                    .clicked();
            });

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
    }
}
