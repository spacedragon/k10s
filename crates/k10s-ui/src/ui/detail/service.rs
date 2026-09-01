//! The core/v1 Service detail body: Overview rows from the normalized
//! projection, structured read-only Ports, events, and the guarded YAML
//! workflow.
//!
//! This panel is deliberately action-free beyond YAML editing: no Scale,
//! Delete, logs, or exec, and no port-forward controls — those arrive with
//! the desktop capability in a later task.

use egui::{Grid, RichText, WidgetType};
use k10s_protocol::{ResourceDetailResponse, ServiceProjection};

use crate::ui::tools;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    view: &ResourceDetailResponse,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    yaml: &mut tools::YamlEditors,
    port_drafts: Option<&std::collections::BTreeMap<String, String>>,
    _resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    let projection = projection_of(view);
    match detail.active_tab {
        DetailTab::Overview => overview_tab(ui, window_id, projection, presentation),
        DetailTab::Ports => ports_tab(
            ui,
            window_id,
            &detail.identity,
            projection,
            presentation,
            port_drafts,
            queued,
        ),
        DetailTab::Events => super::events::show(ui, view.events_condition, &view.events),
        DetailTab::Yaml => {
            if !view.capabilities.can_edit_yaml {
                ui.label("This kind cannot be edited");
            } else {
                tools::yaml::show(
                    ui,
                    window_id,
                    yaml,
                    detail.identity.as_row_identity(),
                    Some(view.manifest.as_str()),
                    presentation.mutations_allowed,
                    queued,
                );
            }
        }
        // Services never offer workload tabs; a stale command targeting one
        // of them renders nothing rather than generic content.
        _ => {}
    }
}

/// The Service projection of a detail response, if populated.
fn projection_of(view: &ResourceDetailResponse) -> Option<&ServiceProjection> {
    match &view.projection {
        Some(k10s_protocol::ResourceProjection::Service(projection)) => Some(projection),
        _ => None,
    }
}

/// Overview tab: every field comes from the projection only when present.
fn overview_tab(
    ui: &mut egui::Ui,
    window_id: WindowId,
    projection: Option<&ServiceProjection>,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    let Some(projection) = projection else {
        show_unavailable(ui, window_id, presentation);
        return;
    };
    if super::overview::two_column(
        ui,
        |column| service_operational(column, window_id, projection),
        |column| {
            service_configuration(column, window_id, projection);
            service_identity(column, window_id, presentation);
        },
    ) {
        return;
    }
    service_operational(ui, window_id, projection);
    if service_configuration(ui, window_id, projection) {
        ui.separator();
    }
    service_identity(ui, window_id, presentation);
}

pub(super) fn show_unavailable(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    if super::overview::two_column(
        ui,
        |column| {
            column.heading("OPERATIONAL");
            column.label("Structured details unavailable");
        },
        |column| {
            column.heading("CONFIGURATION");
            column.label("Structured details unavailable");
            service_identity(column, window_id, presentation);
        },
    ) {
        return;
    }
    ui.heading("OPERATIONAL");
    ui.label("Structured details unavailable");
    ui.heading("CONFIGURATION");
    ui.label("Structured details unavailable");
    service_identity(ui, window_id, presentation);
}

fn service_operational(ui: &mut egui::Ui, window_id: WindowId, projection: &ServiceProjection) {
    ui.heading("PORTS");
    if projection.ports.is_empty() {
        ui.label("No declared ports");
    } else {
        for port in &projection.ports {
            ui.label(crate::ui::port_detail_label(port));
        }
    }
    ui.heading("STATUS");
    Grid::new(("k10s.detail.service.overview.grid", window_id.0))
        .num_columns(1)
        .striped(true)
        .min_col_width(240.0)
        .show(ui, |ui| {
            overview_row(ui, "Type", &projection.service_type);
            overview_row(
                ui,
                "Cluster IPs",
                &if projection.cluster_ips.is_empty() {
                    "—".to_owned()
                } else {
                    projection.cluster_ips.join(", ")
                },
            );
            if let Some(value) = &projection.external_name {
                overview_row(ui, "External name", value);
            }
        });
}

fn service_configuration(
    ui: &mut egui::Ui,
    window_id: WindowId,
    projection: &ServiceProjection,
) -> bool {
    let mut painted = false;
    let selector = projection
        .selector
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(", ");
    if !selector.is_empty() {
        ui.heading("SELECTORS");
        super::overview::long_value(ui, "Selector", Some(&selector));
        painted = true;
    }
    if projection.session_affinity.is_none()
        && projection.external_traffic_policy.is_none()
        && projection.internal_traffic_policy.is_none()
    {
        return painted;
    }
    ui.heading("TRAFFIC & SESSION");
    painted = true;
    Grid::new(("k10s.detail.service.traffic", window_id.0)).show(ui, |ui| {
        if let Some(value) = projection.session_affinity.as_deref() {
            overview_row(ui, "Session affinity", nonempty(value));
        }
        if let Some(value) = projection.external_traffic_policy.as_deref() {
            overview_row(ui, "External policy", nonempty(value));
        }
        if let Some(value) = projection.internal_traffic_policy.as_deref() {
            overview_row(ui, "Internal policy", nonempty(value));
        }
    });
    painted
}

fn nonempty(value: &str) -> &str {
    if value.is_empty() { "—" } else { value }
}

fn service_identity(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    ui.heading("IDENTITY");
    Grid::new(("k10s.detail.service.identity", window_id.0)).show(ui, |ui| {
        overview_row(ui, "Name", &presentation.identity.name);
        overview_row(
            ui,
            "Namespace",
            presentation.identity.namespace.as_deref().unwrap_or("—"),
        );
        overview_row(
            ui,
            "UID",
            if presentation.identity.uid.is_empty() {
                "—"
            } else {
                &presentation.identity.uid
            },
        );
        overview_row(ui, "Context", &presentation.identity.context);
    });
}

fn overview_row(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.label(format!("{label} {value}"));
    ui.end_row();
}

/// Ports tab: one structured read-only line per declared port. UDP/SCTP
/// entries are labelled read-only; TCP entries expose no controls yet.
fn ports_tab<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &I,
    projection: Option<&ServiceProjection>,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    port_drafts: Option<&std::collections::BTreeMap<String, String>>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let Some(projection) = projection else {
        ui.label("No structured projection available");
        return;
    };
    if projection.ports.is_empty() {
        ui.label("No declared ports");
        return;
    }
    if let Some(error) = presentation.port_forward_error {
        ui.label(RichText::new(error).color(crate::ui::theme::WARNING));
    }
    for port in &projection.ports {
        let line = crate::ui::port_detail_label(port);
        let label = ui.label(RichText::new(line.clone()).strong());
        label.widget_info(|| {
            egui::WidgetInfo::labeled(WidgetType::Label, true, format!("Port {line}"))
        });
        if port.protocol != k10s_protocol::TransportProtocol::Tcp {
            continue;
        }
        let Some(service) = identity.as_row_identity() else {
            continue;
        };
        let session = presentation.port_forward_sessions.iter().find(|session| {
            session.service.uid == service.uid && session.service_port == port.service_port
        });
        if let Some(session) = session {
            ui.label(format!(
                "{} · {}:{} · {:?}",
                session.local_addr, session.pod.name, session.pod_port, session.state
            ));
            if ui.button("Copy address").clicked() {
                ui.ctx().copy_text(session.local_addr.clone());
            }
            if ui.button("Stop").clicked() {
                queued.push(WorkspaceCommand::StopPortForward(
                    session.id.as_str().to_owned(),
                ));
            }
        } else if presentation.port_forward_available {
            let draft_key = crate::workspace::ServiceWindowState::<I>::port_draft_key(
                &service.uid,
                port.service_port,
            );
            let mut draft = port_drafts
                .and_then(|drafts| drafts.get(&draft_key))
                .cloned()
                .unwrap_or_default();
            let edit = ui.add(
                egui::TextEdit::singleline(&mut draft)
                    .hint_text("Local port (blank = automatic)")
                    .desired_width(180.0),
            );
            if edit.changed() {
                queued.push(WorkspaceCommand::SetServicePortDraft(
                    window_id,
                    draft_key,
                    draft.clone(),
                ));
            }
            let local_port = if draft.trim().is_empty() || draft.trim() == "0" {
                Ok(0)
            } else {
                draft
                    .trim()
                    .parse::<u16>()
                    .ok()
                    .filter(|port| *port != 0)
                    .ok_or(())
            };
            if local_port.is_err() {
                ui.label(
                    RichText::new("Enter a port from 1 to 65535").color(crate::ui::theme::WARNING),
                );
            }
            if ui
                .add_enabled(local_port.is_ok(), egui::Button::new("Start"))
                .clicked()
            {
                let selector = port.name.clone().map_or(
                    k10s_protocol::PortForwardPortSelector::Number {
                        number: port.service_port,
                    },
                    |name| k10s_protocol::PortForwardPortSelector::Name { name },
                );
                queued.push(WorkspaceCommand::StartPortForward {
                    service: identity.clone(),
                    port: selector,
                    local_port: local_port.unwrap_or(0),
                });
            }
        } else {
            ui.label("Port forwarding is available in the desktop application");
        }
    }
}
