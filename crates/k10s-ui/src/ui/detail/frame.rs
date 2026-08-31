//! Fixed shared detail chrome: identity, vitals, controls, tabs, one body
//! scroll region, and a footer. Kind modules only supply the body content.

use egui::{RichText, ScrollArea, WidgetInfo, WidgetType};

use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{DetailPresentationInput, DetailPrimary};
use crate::ui::resource_window::RowIdentity;

pub(super) fn title(identity: &k10s_protocol::ResourceIdentity) -> String {
    match identity.namespace.as_deref() {
        Some(namespace) => format!("{} · {namespace} / {}", identity.gvk.kind, identity.name),
        None => format!("{} · cluster / {}", identity.gvk.kind, identity.name),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    input: &DetailPresentationInput<'_>,
    integrated: bool,
    detail_maximized: bool,
    tabs: &[DetailTab],
    queued: &mut Vec<WorkspaceCommand<I>>,
    body: impl FnOnce(&mut egui::Ui, DetailPrimary<'_>),
) {
    ui.label(RichText::new(title(input.identity)).strong().heading());
    // Retain the terse identity fields as individually accessible labels for
    // keyboard and assistive-technology users.
    ui.horizontal(|ui| {
        ui.label("Details");
        ui.label(format!("Kind {}", input.identity.gvk.kind));
        match input.identity.namespace.as_deref() {
            Some(namespace) => ui.label(format!("Namespace {namespace}")),
            None => ui.label("Scope Cluster-scoped"),
        };
        ui.label(format!("Context {}", input.identity.context));
        if !input.identity.uid.is_empty() {
            ui.label(format!("UID {}", input.identity.uid));
        }
    });
    ui.horizontal_wrapped(|ui| {
        vital(ui, "Status", input.metrics.status.unwrap_or("—"));
        vital(ui, "Age", input.metrics.age.unwrap_or("—"));
        let freshness = match input.freshness {
            Some(crate::ui::WindowFreshness::Live { last_sync_age }) => {
                format!("Freshness · live ({last_sync_age})")
            }
            Some(crate::ui::WindowFreshness::StaleRetrying { .. }) => "Freshness · stale".into(),
            Some(crate::ui::WindowFreshness::Reconnecting { .. }) => {
                "Freshness · reconnecting".into()
            }
            Some(crate::ui::WindowFreshness::Forbidden { .. }) => "Freshness · forbidden".into(),
            Some(crate::ui::WindowFreshness::Failed { .. }) => "Freshness · failed".into(),
            Some(crate::ui::WindowFreshness::ReadyEmpty) => "Freshness · ready".into(),
            None if input.gone => "Freshness · gone".into(),
            None => "Freshness · loading".into(),
        };
        vital(ui, "", &freshness);
        if let Some(age) = input.metrics.age {
            ui.label(format!("Created {age}"));
        }
        copy(ui, "Copy name", &input.identity.name);
        if let Some(namespace) = input.identity.namespace.as_deref() {
            copy(ui, "Copy namespace", namespace);
        }
        if !input.identity.uid.is_empty() {
            copy(ui, "Copy UID", &input.identity.uid);
        }
        let owner = match input.primary {
            DetailPrimary::Loaded(view) => {
                view.owner_references.iter().find(|owner| owner.controller)
            }
            DetailPrimary::Loading | DetailPrimary::Failed(_) => None,
        };
        if owner.is_some() {
            ui.menu_button("More", |ui| {
                if let Some(owner) = owner {
                    let label = format!("Open owner {}", owner.name);
                    if ui.button(&label).clicked() {
                        queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                            &k10s_protocol::ResourceIdentity {
                                context: input.identity.context.clone(),
                                gvk: owner.gvk.clone(),
                                namespace: input.identity.namespace.clone(),
                                name: owner.name.clone(),
                                uid: owner.uid.clone(),
                            },
                        )));
                        ui.close();
                    }
                }
            });
        }
        if integrated && !input.gone {
            let pop_out = ui.button("Pop out ↗");
            if pop_out.clicked() {
                queued.push(WorkspaceCommand::OpenDedicatedDetail(
                    detail.identity.clone(),
                ));
            }
            let maximize = ui.button(if detail_maximized {
                "Restore split"
            } else {
                "Maximize"
            });
            if maximize.clicked() {
                queued.push(if detail_maximized {
                    WorkspaceCommand::RestoreDetailPane(window_id)
                } else {
                    WorkspaceCommand::MaximizeDetailPane(window_id)
                });
            }
        }
        for tab in tabs {
            let active = *tab == detail.active_tab;
            let response = ui.selectable_label(active, super::tab_label(*tab));
            response.widget_info(|| {
                WidgetInfo::labeled(
                    WidgetType::Button,
                    true,
                    format!("Tab {}", super::tab_label(*tab)),
                )
            });
            if response.clicked() && !active {
                queued.push(WorkspaceCommand::SetActiveTab(window_id, *tab));
            }
        }
    });
    ui.separator();
    ScrollArea::vertical()
        .id_salt(("k10s.detail.body.scroll", window_id.0, detail.active_tab))
        .show(ui, |ui| {
            if input.gone {
                ui.label("This resource no longer exists");
            } else {
                body(ui, input.primary);
            }
        });
    ui.separator();
    ui.label(
        RichText::new(
            "Shortcuts: l Logs · p Pods · s Shell · y YAML · e Events · Esc restore/close",
        )
        .weak(),
    );
}

fn vital(ui: &mut egui::Ui, label: &str, value: &str) {
    let text = if label.is_empty() {
        value.to_owned()
    } else {
        format!("{label} · {value}")
    };
    ui.label(text);
}

fn copy(ui: &mut egui::Ui, label: &str, value: &str) {
    let response = ui.button(label);
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.to_owned()));
    if response.clicked() {
        ui.ctx().copy_text(value.to_owned());
    }
}
