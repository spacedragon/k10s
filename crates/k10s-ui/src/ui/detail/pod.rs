//! Typed, feed-independent Pod Overview presentation.

use egui::{Grid, RichText, Stroke};
use k10s_protocol::{
    ContainerStateProjection, ContainerTerminationProjection, EventsCondition, MetricsAvailability,
    OwnerReference, PodContainerPort, PodProjection, ResourceProjection, TransportProtocol,
};

use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, WindowId, WorkspaceCommand};

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
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let Some(pod) = PodDetailProjection::from_input(input) else {
        ui.label("Structured details unavailable");
        return;
    };

    if ui.available_width() >= 760.0 {
        ui.columns(2, |columns| {
            show_operational(&mut columns[0], window_id, &pod);
            show_metadata(&mut columns[1], window_id, &pod, frame, queued);
        });
    } else {
        show_operational(ui, window_id, &pod);
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
    ports: Vec<String>,
    labels: Vec<(String, String)>,
    annotations: Vec<(String, String)>,
    created_at: String,
    uid: String,
    context: String,
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
                last_exit: termination(container.last_termination.as_ref()),
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
                    last_transition: optional(condition.last_transition_time.as_deref()),
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
            ports: pod.ports.iter().map(format_port).collect(),
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
}

#[derive(Clone)]
struct ContainerProjection {
    name: String,
    image: String,
    state: String,
    ready: String,
    restarts: String,
    last_exit: String,
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
        }),
        ContainerStateProjection::Terminated(termination) if termination.exit_code != 0 => {
            Some(FailureProjection {
                container: present(&container.name),
                reason: termination
                    .reason
                    .as_deref()
                    .map(present)
                    .unwrap_or_else(|| format!("Exit {}", termination.exit_code)),
                last_exit: Some(termination.clone()),
                terminated: true,
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

fn termination(termination: Option<&ContainerTerminationProjection>) -> String {
    termination.map_or_else(
        || UNAVAILABLE.into(),
        |termination| {
            format!(
                "{} · {}",
                termination.exit_code,
                optional(termination.reason.as_deref())
            )
        },
    )
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

fn show_operational(ui: &mut egui::Ui, window_id: WindowId, pod: &PodDetailProjection) {
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
    }

    section(ui, &format!("CONTAINERS · {}", pod.containers.len()));
    Grid::new(("k10s.detail.pod.containers", window_id.0))
        .striped(true)
        .show(ui, |ui| {
            for heading in [
                "NAME",
                "IMAGE",
                "STATE",
                "READY",
                "RESTARTS",
                "LAST EXIT",
                "CPU / MEM",
            ] {
                ui.label(RichText::new(heading).weak());
            }
            ui.end_row();
            for container in &pod.containers {
                ui.label(&container.name);
                ui.label(&container.image);
                ui.label(&container.state);
                ui.label(&container.ready);
                ui.label(&container.restarts);
                ui.label(&container.last_exit);
                ui.label(&container.metrics);
                ui.end_row();
            }
        });

    section(ui, "CONDITIONS");
    if pod.conditions.is_empty() {
        ui.label("No conditions reported");
    } else {
        Grid::new(("k10s.detail.pod.conditions", window_id.0))
            .striped(true)
            .show(ui, |ui| {
                for heading in ["TYPE", "STATUS", "REASON", "MESSAGE", "LAST TRANSITION"] {
                    ui.label(RichText::new(heading).weak());
                }
                ui.end_row();
                for condition in &pod.conditions {
                    ui.label(&condition.condition_type);
                    ui.label(&condition.status);
                    ui.label(&condition.reason);
                    ui.label(&condition.message);
                    ui.label(&condition.last_transition);
                    ui.end_row();
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
                ui.label(format!(
                    "{} · {} · ×{} · {}",
                    event.reason, event.message, event.count, event.last_seen
                ));
            }
        }
    }
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
        if ui.button(owner_label).clicked() {
            queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                &owner_identity(frame.identity, owner),
            )));
        }
        ui.label(format!("this Pod · {}/{}", pod.namespace, pod.name));
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
    let ports = if pod.ports.is_empty() {
        UNAVAILABLE.into()
    } else {
        pod.ports.join(" · ")
    };
    metadata_grid(
        ui,
        ("k10s.detail.pod.network", window_id.0),
        &[
            ("Pod IP", &pod.pod_ip),
            ("Host IP", &pod.host_ip),
            ("Ports", &ports),
        ],
    );

    section(ui, &format!("LABELS · {}", pod.labels.len()));
    let visible_labels = if frame.expansion.labels {
        pod.labels.len()
    } else {
        pod.labels.len().min(4)
    };
    ui.horizontal_wrapped(|ui| {
        for (key, value) in pod.labels.iter().take(visible_labels) {
            chip(ui, &format!("{key}={value}"));
        }
    });
    if visible_labels < pod.labels.len() {
        if ui
            .button(format!(
                "Show {} more labels",
                pod.labels.len() - visible_labels
            ))
            .clicked()
        {
            frame.expansion.labels = true;
        }
    } else if pod.labels.len() > 4 && ui.button("Show fewer labels").clicked() {
        frame.expansion.labels = false;
    }

    egui::CollapsingHeader::new(format!("ANNOTATIONS · {}", pod.annotations.len()))
        .id_salt(("k10s.detail.pod.annotations", window_id.0))
        .default_open(false)
        .show(ui, |ui| {
            if pod.annotations.is_empty() {
                ui.label("No annotations");
            } else {
                let rows = pod
                    .annotations
                    .iter()
                    .map(|(key, value)| (key.as_str(), value))
                    .collect::<Vec<_>>();
                metadata_grid(ui, ("k10s.detail.pod.annotation-rows", window_id.0), &rows);
            }
        });

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
    ui.add_space(ui.spacing().item_spacing.y);
    ui.label(RichText::new(title).strong());
    ui.separator();
}

fn metadata_grid(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash + std::fmt::Debug,
    rows: &[(&str, &String)],
) {
    Grid::new(id).show(ui, |ui| {
        for (label, value) in rows {
            ui.label(RichText::new(*label).weak());
            ui.label(value.as_str());
            ui.end_row();
        }
    });
}

fn chip(ui: &mut egui::Ui, text: &str) {
    egui::Frame::new()
        .fill(ui.visuals().faint_bg_color)
        .stroke(Stroke::new(
            1.0,
            ui.visuals().widgets.noninteractive.bg_stroke.color,
        ))
        .corner_radius(10)
        .inner_margin(egui::Margin::symmetric(7, 2))
        .show(ui, |ui| {
            ui.label(text);
        });
}

fn format_port(port: &PodContainerPort) -> String {
    let protocol = match port.protocol {
        TransportProtocol::Tcp => "TCP",
        TransportProtocol::Udp => "UDP",
        TransportProtocol::Sctp => "SCTP",
    };
    let name = port
        .name
        .as_deref()
        .map_or_else(String::new, |name| format!(" {name}"));
    let host = port
        .host_port
        .map_or_else(String::new, |host| format!(" host:{host}"));
    format!(
        "{} {}/{protocol}{name}{host}",
        present(&port.container_name),
        port.container_port
    )
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
