//! Typed, feed-independent Pod Overview presentation.

use egui::{RichText, WidgetInfo, WidgetType, accesskit::Role};
use k10s_protocol::{
    ContainerStateProjection, ContainerTerminationProjection, EventsCondition, MetricsAvailability,
    OwnerReference, PodContainerPort, PodProjection, ResourceDetailResponse, ResourceIdentity,
    ResourceProjection, TransportProtocol,
};

use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use super::presentation::{
    DetailFrameProjection, DetailPresentationInput, DetailPrimary, DetailVitalShape,
    DetailVitalTone, owner_identity,
};

const UNAVAILABLE: &str = "—";

/// Configure Pod-owned shared chrome before the frame paints its vitals.
pub(super) fn configure_frame(
    input: &DetailPresentationInput<'_>,
    frame: &mut DetailFrameProjection<'_>,
) {
    let status = PodDetailProjection::from_input(input).map(|pod| pod.status);
    let Some(vital) = frame
        .visible_vitals
        .iter_mut()
        .find(|vital| vital.label == "Status")
    else {
        return;
    };
    match status {
        Some(status) => {
            vital.value = status.text;
            vital.tone = status.tone;
            vital.shape = Some(status.shape);
        }
        None => {
            vital.tone = DetailVitalTone::Neutral;
            vital.shape = Some(DetailVitalShape::Dot);
        }
    }
}

pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    _detail: &DetailState<I>,
    input: &DetailPresentationInput<'_>,
    frame: &mut DetailFrameProjection<'_>,
    runtime_actions: &mut Vec<super::DetailRuntimeAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let Some(pod) = PodDetailProjection::from_input(input) else {
        ui.label("Structured details unavailable");
        return;
    };

    let mut operational_queued = Vec::new();
    let mut metadata_queued = Vec::new();
    if super::overview::two_column(
        ui,
        |column| {
            show_operational(
                column,
                window_id,
                &pod,
                runtime_actions,
                &mut operational_queued,
            )
        },
        |column| show_metadata(column, window_id, &pod, frame, &mut metadata_queued),
    ) {
        queued.extend(operational_queued);
        queued.extend(metadata_queued);
        // Both responsive columns were painted by the shared 1.35:1 layout.
    } else {
        show_operational(ui, window_id, &pod, runtime_actions, queued);
        let label = if frame.expansion.metadata {
            "Hide Pod metadata"
        } else {
            "Show Pod metadata"
        };
        if ui.button(label).clicked() {
            frame.expansion.metadata = !frame.expansion.metadata;
        }
        if frame.expansion.metadata {
            show_metadata(ui, window_id, &pod, frame, queued);
        }
    }
}

#[derive(Clone)]
struct PodDetailProjection {
    identity: ResourceIdentity,
    status: StatusProjection,
    failure: Option<FailureProjection>,
    containers: Vec<ContainerProjection>,
    conditions: Vec<ConditionProjection>,
    events_condition: EventsCondition,
    events: Vec<EventProjection>,
    owner: Option<OwnerReference>,
    namespace: String,
    name: String,
    node_name: String,
    qos_class: String,
    priority: String,
    service_account: String,
    restart_policy: String,
    pod_ip: String,
    host_ip: String,
    ports: Vec<PodContainerPort>,
    port_forward_capability: bool,
    port_forward_authority: bool,
    labels: Vec<(String, String)>,
    annotations: Vec<(String, String)>,
    created_at: String,
    uid: String,
    context: String,
}

/// Runtime source data for Pod Logs and Shell tabs. This projection is
/// deliberately typed-only: manifests and display sections are never
/// interpreted as runtime authority.
pub(crate) struct PodRuntimeProjection {
    containers: Vec<String>,
    default_previous: bool,
}

impl PodRuntimeProjection {
    pub(crate) fn from_view(
        identity: &ResourceIdentity,
        view: &ResourceDetailResponse,
    ) -> Option<Self> {
        if view.identity != *identity {
            return None;
        }
        let Some(ResourceProjection::Pod(pod)) = view.projection.as_ref() else {
            return None;
        };
        let default = pod
            .containers
            .iter()
            .find(|container| !container.name.is_empty())?;
        let default_previous =
            failure_projection(default).is_some() && default.last_termination.is_some();
        let containers = pod
            .containers
            .iter()
            .filter(|container| !container.name.is_empty())
            .map(|container| container.name.clone())
            .collect();
        Some(Self {
            containers,
            default_previous,
        })
    }

    pub(crate) fn containers(&self) -> &[String] {
        &self.containers
    }

    pub(crate) fn default_container(&self) -> &str {
        &self.containers[0]
    }

    pub(crate) fn default_previous(&self) -> bool {
        self.default_previous
    }

    pub(crate) fn contains(&self, container: &str) -> bool {
        self.containers
            .iter()
            .any(|candidate| candidate == container)
    }
}

impl PodDetailProjection {
    fn from_input(input: &DetailPresentationInput<'_>) -> Option<Self> {
        let DetailPrimary::Loaded(view) = input.primary else {
            return None;
        };
        if view.identity != *input.identity {
            return None;
        }
        let Some(ResourceProjection::Pod(pod)) = view.projection.as_ref() else {
            return None;
        };

        let status = status_projection(pod);
        let containers = pod
            .containers
            .iter()
            .map(|container| ContainerProjection {
                name: present(&container.name),
                image: optional(container.image.as_deref()),
                state: container_state(container.state.as_ref()),
                ready: container.ready.map_or_else(
                    || UNAVAILABLE.into(),
                    |ready| if ready { "Yes" } else { "No" }.into(),
                ),
                restarts: number(container.restart_count),
                metrics: container_metrics(input, &container.name),
            })
            .collect();
        let failure = pod.containers.iter().find_map(failure_projection);
        let mut labels = pod
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        labels.sort_by(|left, right| left.0.cmp(&right.0));
        let mut annotations = pod
            .annotations
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        annotations.sort_by(|left, right| left.0.cmp(&right.0));

        Some(Self {
            identity: input.identity.clone(),
            status,
            failure,
            containers,
            conditions: pod
                .conditions
                .iter()
                .map(|condition| ConditionProjection {
                    condition_type: present(&condition.condition_type),
                    status: present(&condition.status),
                    reason: optional(condition.reason.as_deref()),
                    message: optional(condition.message.as_deref()),
                    last_transition: super::presentation::format_age(
                        condition.last_transition_time.as_deref(),
                        input.now,
                    ),
                })
                .collect(),
            events_condition: view.events_condition,
            events: view
                .events
                .iter()
                .map(|event| EventProjection {
                    reason: present(&event.reason),
                    message: present(&event.message),
                    count: event.count,
                    last_seen: present(&event.last_seen),
                })
                .collect(),
            owner: input.verified_owner().cloned(),
            namespace: optional(input.identity.namespace.as_deref()),
            name: present(&input.identity.name),
            node_name: optional(pod.node_name.as_deref()),
            qos_class: optional(pod.qos_class.as_deref()),
            priority: pod
                .priority
                .map_or_else(|| UNAVAILABLE.into(), |priority| priority.to_string()),
            service_account: optional(pod.service_account.as_deref()),
            restart_policy: optional(pod.restart_policy.as_deref()),
            pod_ip: optional(pod.pod_ip.as_deref()),
            host_ip: optional(pod.host_ip.as_deref()),
            ports: pod.ports.clone(),
            port_forward_capability: input.port_forward_capability,
            port_forward_authority: input.mutations_allowed,
            labels,
            annotations,
            created_at: optional(pod.created_at.as_deref()),
            uid: present(&input.identity.uid),
            context: present(&input.identity.context),
        })
    }
}

#[derive(Clone)]
struct StatusProjection {
    text: String,
    tone: DetailVitalTone,
    shape: DetailVitalShape,
}

#[derive(Clone)]
struct FailureProjection {
    container: String,
    reason: String,
    last_exit: Option<ContainerTerminationProjection>,
    terminated: bool,
    supports_previous_logs: bool,
}

#[derive(Clone)]
struct ContainerProjection {
    name: String,
    image: String,
    state: String,
    ready: String,
    restarts: String,
    metrics: String,
}

#[derive(Clone)]
struct ConditionProjection {
    condition_type: String,
    status: String,
    reason: String,
    message: String,
    last_transition: String,
}

#[derive(Clone)]
struct EventProjection {
    reason: String,
    message: String,
    count: u32,
    last_seen: String,
}

fn status_projection(pod: &PodProjection) -> StatusProjection {
    if let Some(failure) = pod.containers.iter().find_map(failure_projection) {
        return StatusProjection {
            text: failure.reason,
            tone: DetailVitalTone::Danger,
            shape: if failure.terminated {
                DetailVitalShape::Cross
            } else {
                DetailVitalShape::Triangle
            },
        };
    }
    let phase = optional(pod.phase.as_deref());
    match pod.phase.as_deref() {
        Some("Running" | "Succeeded") => StatusProjection {
            text: phase,
            tone: DetailVitalTone::Healthy,
            shape: DetailVitalShape::Dot,
        },
        Some("Failed") => StatusProjection {
            text: phase,
            tone: DetailVitalTone::Danger,
            shape: DetailVitalShape::Cross,
        },
        Some("Pending" | "Unknown") => StatusProjection {
            text: phase,
            tone: DetailVitalTone::Warning,
            shape: DetailVitalShape::Triangle,
        },
        _ => StatusProjection {
            text: phase,
            tone: DetailVitalTone::Neutral,
            shape: DetailVitalShape::Dot,
        },
    }
}

fn failure_projection(
    container: &k10s_protocol::PodContainerProjection,
) -> Option<FailureProjection> {
    match container.state.as_ref()? {
        ContainerStateProjection::Waiting {
            reason: Some(reason),
        } if !reason.is_empty() => Some(FailureProjection {
            container: present(&container.name),
            reason: reason.clone(),
            last_exit: container.last_termination.clone(),
            terminated: false,
            supports_previous_logs: container.last_termination.is_some(),
        }),
        ContainerStateProjection::Terminated(termination) if termination.exit_code != 0 => {
            termination
                .reason
                .as_deref()
                .filter(|reason| !reason.is_empty())
                .map(|reason| FailureProjection {
                    container: present(&container.name),
                    reason: present(reason),
                    last_exit: Some(termination.clone()),
                    terminated: true,
                    supports_previous_logs: container.last_termination.is_some(),
                })
        }
        ContainerStateProjection::Running
        | ContainerStateProjection::Waiting { .. }
        | ContainerStateProjection::Terminated(_) => None,
    }
}

fn container_state(state: Option<&ContainerStateProjection>) -> String {
    match state {
        Some(ContainerStateProjection::Running) => "Running".into(),
        Some(ContainerStateProjection::Waiting { reason }) => {
            format!("Waiting · {}", optional(reason.as_deref()))
        }
        Some(ContainerStateProjection::Terminated(termination)) => {
            format!("Terminated · {}", optional(termination.reason.as_deref()))
        }
        None => UNAVAILABLE.into(),
    }
}

fn container_metrics(input: &DetailPresentationInput<'_>, name: &str) -> String {
    let Some(response) = input
        .resource_metrics
        .filter(|response| response.identity == *input.identity)
    else {
        return format!("{UNAVAILABLE} / {UNAVAILABLE}");
    };
    let mut matches = response
        .containers
        .iter()
        .filter(|sample| sample.name == name);
    let Some(sample) = matches.next() else {
        return format!("{UNAVAILABLE} / {UNAVAILABLE}");
    };
    if matches.next().is_some() || sample.metrics.availability != MetricsAvailability::Available {
        return format!("{UNAVAILABLE} / {UNAVAILABLE}");
    }
    match (sample.metrics.cpu_millicores, sample.metrics.memory_bytes) {
        (Some(cpu), Some(memory)) => format!("{cpu}m / {}Mi", memory / 1_048_576),
        _ => format!("{UNAVAILABLE} / {UNAVAILABLE}"),
    }
}

fn show_operational<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    pod: &PodDetailProjection,
    runtime_actions: &mut Vec<super::DetailRuntimeAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if let Some(failure) = &pod.failure {
        section(ui, "WHY IT'S FAILING");
        let exit = failure.last_exit.as_ref().map_or_else(
            || UNAVAILABLE.into(),
            |exit| {
                format!(
                    "exit {} · {}",
                    exit.exit_code,
                    optional(exit.reason.as_deref())
                )
            },
        );
        ui.label(format!(
            "{} · {} · {}",
            failure.container, failure.reason, exit
        ));
        if failure.supports_previous_logs && ui.button("Previous logs").clicked() {
            runtime_actions.push(super::DetailRuntimeAction::PreviousLogs {
                window: window_id,
                container: failure.container.clone(),
            });
            queued.push(WorkspaceCommand::SetActiveTab(window_id, DetailTab::Logs));
        }
    }

    section(ui, &format!("CONTAINERS · {}", pod.containers.len()));
    table_region(ui, window_id, "containers", "Pod containers table", |ui| {
        let available = (ui.available_width() - ui.spacing().item_spacing.x * 5.0).max(1.0);
        let widths = [0.15, 0.31, 0.20, 0.08, 0.11, 0.15].map(|part| available * part);
        ui.spacing_mut().item_spacing.y = 10.0;
        ui.horizontal(|ui| {
            for (heading, width) in ["NAME", "IMAGE", "STATE", "READY", "RESTARTS", "CPU / MEM"]
                .into_iter()
                .zip(widths)
            {
                table_cell(ui, width, heading, None, None);
            }
        });
        ui.separator();
        for container in &pod.containers {
            ui.horizontal(|ui| {
                table_cell(ui, widths[0], &container.name, None, None);
                table_cell(
                    ui,
                    widths[1],
                    &container.image,
                    Some(format!("Image: {}", container.image)),
                    Some(crate::ui::theme::MUTED_TEXT),
                );
                let state_color = if container.state == "Running" {
                    crate::ui::theme::HEALTHY
                } else if container.state.starts_with("Terminated") {
                    crate::ui::theme::MUTED_TEXT
                } else {
                    crate::ui::theme::WARNING
                };
                table_cell(
                    ui,
                    widths[2],
                    &format!("● {}", container.state),
                    Some(container.state.clone()),
                    Some(state_color),
                );
                let ready = match container.ready.as_str() {
                    "Yes" => "✓",
                    "No" => "⨯",
                    _ => UNAVAILABLE,
                };
                table_cell(ui, widths[3], ready, Some(container.ready.clone()), None);
                table_cell(ui, widths[4], &container.restarts, None, None);
                table_cell(
                    ui,
                    widths[5],
                    &container.metrics,
                    None,
                    Some(crate::ui::theme::MUTED_TEXT),
                );
            });
        }
    });

    section(ui, &format!("PORTS · {}", pod.ports.len()));
    if pod.ports.is_empty() {
        ui.label("No declared ports");
    } else {
        table_region(ui, window_id, "ports", "Pod ports table", |ui| {
            let available = (ui.available_width() - ui.spacing().item_spacing.x * 4.0).max(1.0);
            let widths = [0.18, 0.22, 0.14, 0.14, 0.32].map(|part| available * part);
            ui.horizontal(|ui| {
                for (heading, width) in ["NAME", "CONTAINER", "PORT", "PROTOCOL", "ACTION"]
                    .into_iter()
                    .zip(widths)
                {
                    table_cell(ui, width, heading, None, None);
                }
            });
            ui.separator();
            for (index, port) in pod.ports.iter().enumerate() {
                ui.push_id(
                    (
                        "k10s.detail.pod.port",
                        window_id.0,
                        index,
                        &port.container_name,
                        port.container_port,
                    ),
                    |ui| {
                        let name = optional(port.name.as_deref());
                        let container = present(&port.container_name);
                        let protocol = protocol_label(port.protocol);
                        let accessible = format!(
                            "Port {name} · {container} · {} · {protocol}",
                            port.container_port
                        );
                        let row = ui.horizontal(|ui| {
                            table_cell(ui, widths[0], &name, None, None);
                            table_cell(ui, widths[1], &container, None, None);
                            table_cell(
                                ui,
                                widths[2],
                                &port.container_port.to_string(),
                                None,
                                None,
                            );
                            table_cell(ui, widths[3], protocol, None, None);
                            ui.allocate_ui_with_layout(
                                egui::vec2(widths[4], ui.spacing().interact_size.y),
                                egui::Layout::left_to_right(egui::Align::Center),
                                |ui| {
                                    if port.protocol != TransportProtocol::Tcp {
                                        return;
                                    }
                                    if pod.port_forward_capability {
                                        let action_name = port.name.as_deref().map_or_else(
                                            || {
                                                format!(
                                                    "Port Forward unnamed port on container {} port {}",
                                                    port.container_name, port.container_port
                                                )
                                            },
                                            |name| {
                                                format!(
                                                    "Port Forward {name} on container {} port {}",
                                                    port.container_name, port.container_port
                                                )
                                            },
                                        );
                                        let action = ui.add_enabled(
                                            pod.port_forward_authority
                                                && !port.container_name.is_empty()
                                                && port.container_port != 0,
                                            egui::Button::new("Port Forward"),
                                        );
                                        action.widget_info(|| {
                                            WidgetInfo::labeled(
                                                WidgetType::Button,
                                                true,
                                                action_name.clone(),
                                            )
                                        });
                                        if action.clicked() {
                                            queued.push(WorkspaceCommand::StartPortForward {
                                                target: k10s_protocol::PortForwardTarget::Pod {
                                                    identity: pod.identity.clone(),
                                                    container_name: port.container_name.clone(),
                                                    remote_port: port.container_port,
                                                },
                                                remote_label: format!(
                                                    "{name} · {container} · {} · {protocol}",
                                                    port.container_port
                                                ),
                                                initial_local_port: port.container_port,
                                            });
                                        }
                                    } else {
                                        ui.add(
                                            egui::Label::new(
                                                "Port forwarding is available in the desktop application",
                                            )
                                            .truncate(),
                                        )
                                        .on_hover_text(
                                            "Port forwarding is available in the desktop application",
                                        );
                                    }
                                },
                            );
                        });
                        row.response.widget_info(|| {
                            WidgetInfo::labeled(WidgetType::Label, true, accessible.clone())
                        });
                        ui.separator();
                    },
                );
            }
        });
        if pod.port_forward_capability
            && !pod.port_forward_authority
            && pod
                .ports
                .iter()
                .any(|port| port.protocol == TransportProtocol::Tcp)
        {
            ui.label(
                RichText::new(crate::ui::port_forward::PORT_FORWARD_AUTHORITY_UNAVAILABLE)
                    .color(crate::ui::theme::WARNING),
            );
        }
    }

    section(ui, "CONDITIONS");
    if pod.conditions.is_empty() {
        ui.label("No conditions reported");
    } else {
        table_region(ui, window_id, "conditions", "Pod conditions table", |ui| {
            for condition in &pod.conditions {
                let tone = if condition.status == "True" {
                    crate::ui::theme::HEALTHY
                } else {
                    crate::ui::theme::DANGER
                };
                let available = ui.available_width() - ui.spacing().item_spacing.x * 2.0;
                let row = ui.horizontal(|ui| {
                    table_cell(
                        ui,
                        (available - 160.0).max(180.0),
                        &format!("● {}", condition.condition_type),
                        Some(condition.condition_type.clone()),
                        Some(tone),
                    );
                    table_cell(
                        ui,
                        70.0,
                        &condition.reason,
                        None,
                        Some(crate::ui::theme::MUTED_TEXT),
                    )
                    .on_hover_text(&condition.message);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(RichText::new(&condition.last_transition).weak());
                    });
                });
                row.response.widget_info(|| {
                    WidgetInfo::labeled(
                        WidgetType::Label,
                        true,
                        format!("{}: {}", condition.condition_type, condition.status),
                    )
                });
                ui.separator();
            }
        });
    }

    section(ui, "RECENT EVENTS");
    match pod.events_condition {
        EventsCondition::Unavailable => {
            ui.label("Events unavailable");
        }
        EventsCondition::Available if pod.events.is_empty() => {
            ui.label("No recent events");
        }
        EventsCondition::Available => {
            for event in &pod.events {
                let full = format!(
                    "{} · {} · ×{} · {}",
                    event.reason, event.message, event.count, event.last_seen
                );
                let row = ui.horizontal(|ui| {
                    ui.label(RichText::new("●").color(crate::ui::theme::HEALTHY));
                    ui.vertical(|ui| {
                        ui.label(RichText::new(&event.message).strong());
                        ui.label(
                            RichText::new(format!("{} · ×{}", event.reason, event.count)).weak(),
                        );
                    });
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                        ui.label(RichText::new(&event.last_seen).weak());
                    });
                });
                row.response
                    .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, full.clone()));
                ui.separator();
            }
        }
    }
}

fn table_cell(
    ui: &mut egui::Ui,
    width: f32,
    value: &str,
    accessible: Option<String>,
    color: Option<egui::Color32>,
) -> egui::Response {
    let text = color.map_or_else(
        || RichText::new(value),
        |color| RichText::new(value).color(color),
    );
    let height = ui.spacing().interact_size.y;
    let response = ui
        .allocate_ui_with_layout(
            egui::vec2(width, height),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                ui.set_min_size(egui::vec2(width, height));
                ui.add(egui::Label::new(text).truncate().halign(egui::Align::Min))
            },
        )
        .inner;
    if let Some(accessible) = accessible {
        response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, accessible.clone()));
        response.on_hover_text(value)
    } else {
        response
    }
}

fn table_region(
    ui: &mut egui::Ui,
    window_id: WindowId,
    name: &'static str,
    accessible_label: &'static str,
    content: impl FnOnce(&mut egui::Ui),
) {
    let table = ui.push_id(("k10s.detail.pod.table", name, window_id.0), content);
    let rect = table.response.rect;
    ui.ctx().accesskit_node_builder(table.response.id, |node| {
        node.set_role(Role::Table);
        node.set_label(accessible_label);
        node.set_bounds(egui::accesskit::Rect {
            x0: rect.left().into(),
            y0: rect.top().into(),
            x1: rect.right().into(),
            y1: rect.bottom().into(),
        });
    });
}

fn show_metadata<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    pod: &PodDetailProjection,
    frame: &mut DetailFrameProjection<'_>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if let Some(owner) = &pod.owner {
        section(ui, "OWNER CHAIN");
        let owner_label = format!(
            "Open owner {}/{}/{}",
            owner.gvk.kind, pod.namespace, owner.name
        );
        let owner_link = ui
            .add(
                egui::Button::new(RichText::new(&owner_label).color(crate::ui::theme::ACCENT))
                    .frame(false),
            )
            .on_hover_cursor(egui::CursorIcon::PointingHand)
            .on_hover_text(&owner_label);
        if owner_link.clicked() {
            queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                &owner_identity(frame.identity, owner),
            )));
        }
        ui.label(RichText::new(format!("this Pod · {}/{}", pod.namespace, pod.name)).weak());
    }

    section(ui, "PLACEMENT");
    metadata_grid(
        ui,
        ("k10s.detail.pod.placement", window_id.0),
        &[
            ("Node", &pod.node_name),
            ("QoS class", &pod.qos_class),
            ("Priority", &pod.priority),
            ("Service account", &pod.service_account),
            ("Restart policy", &pod.restart_policy),
        ],
    );

    section(ui, "NETWORK");
    metadata_grid(
        ui,
        ("k10s.detail.pod.network", window_id.0),
        &[("Pod IP", &pod.pod_ip), ("Host IP", &pod.host_ip)],
    );

    if !pod.labels.is_empty() || !pod.annotations.is_empty() {
        ui.add_space(10.0);
        super::overview::metadata_sections(
            ui,
            pod.labels
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
            "=",
            pod.annotations
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }

    section(ui, "IDENTITY");
    metadata_grid(
        ui,
        ("k10s.detail.pod.identity", window_id.0),
        &[
            ("Created", &pod.created_at),
            ("UID", &pod.uid),
            ("Context", &pod.context),
        ],
    );
}

fn section(ui: &mut egui::Ui, title: &str) {
    ui.add_space(10.0);
    ui.label(RichText::new(title).size(11.0).strong().weak());
    super::overview::section_separator(ui);
}

fn metadata_grid(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    rows: &[(&str, &String)],
) {
    ui.push_id(id, |ui| {
        for (label, value) in rows {
            ui.horizontal(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(140.0, 24.0),
                    egui::Layout::left_to_right(egui::Align::Center),
                    |ui| {
                        ui.set_min_size(egui::vec2(140.0, 24.0));
                        ui.label(RichText::new(*label).weak());
                    },
                );
                ui.allocate_ui_with_layout(
                    egui::vec2(ui.available_width(), 24.0),
                    egui::Layout::left_to_right(egui::Align::Center),
                    |ui| {
                        ui.set_min_size(egui::vec2(ui.available_width(), 24.0));
                        ui.add(egui::Label::new(value.as_str()).truncate())
                            .on_hover_text(value.as_str());
                    },
                );
            });
        }
    });
}

fn protocol_label(protocol: TransportProtocol) -> &'static str {
    match protocol {
        TransportProtocol::Tcp => "TCP",
        TransportProtocol::Udp => "UDP",
        TransportProtocol::Sctp => "SCTP",
    }
}

fn optional(value: Option<&str>) -> String {
    value.map(present).unwrap_or_else(|| UNAVAILABLE.into())
}

fn present(value: &str) -> String {
    if value.is_empty() {
        UNAVAILABLE.into()
    } else {
        value.into()
    }
}

fn number(value: Option<u32>) -> String {
    value.map_or_else(|| UNAVAILABLE.into(), |value| value.to_string())
}
