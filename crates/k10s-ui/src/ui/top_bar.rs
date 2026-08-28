//! Compact global menus, connection state, refresh, and context selection.

use egui::{
    ComboBox, Label, Layout, MenuBar, RichText, Sense, ViewportCommand, WidgetInfo, WidgetType,
};
use k10s_protocol::{Context, ContextAvailability};

use super::{ConnectionState, theme};

pub(super) struct TopBarAction {
    pub(super) context_change: Option<String>,
    pub(super) refresh: bool,
}

pub(super) fn show(
    ui: &mut egui::Ui,
    connection: ConnectionState,
    contexts: &[Context],
    selected_context: Option<&str>,
) -> TopBarAction {
    let mut context_change = None;
    let mut refresh = false;

    MenuBar::new().ui(ui, |ui| {
        ui.push_id("k10s.top_bar.menus", |ui| {
            ui.menu_button("File", |ui| {
                if ui.button("Exit").clicked() {
                    ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                    ui.close();
                }
            });
            ui.menu_button("View", |ui| {
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

        ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
            ui.push_id("k10s.top_bar.context", |ui| {
                ui.add_enabled_ui(!contexts.is_empty(), |ui| {
                    let selected_text = selected_context.unwrap_or("No contexts");
                    let response = ComboBox::new("selector", "Kubernetes context")
                        .selected_text(selected_text)
                        .width(220.0)
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
                    response.on_hover_text(selected_text);
                });
            });

            ui.push_id("k10s.top_bar.refresh", |ui| {
                let label = if connection == ConnectionState::Connected {
                    "Refresh"
                } else {
                    "Retry"
                };
                refresh = ui
                    .button(label)
                    .on_hover_text(if connection == ConnectionState::Connected {
                        "Refresh all resources"
                    } else {
                        "Retry the control connection"
                    })
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
