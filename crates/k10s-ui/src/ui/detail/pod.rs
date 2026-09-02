//! Typed, feed-independent Pod Overview presentation.

use egui::{Grid, RichText, ScrollArea, accesskit::Role};
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
    supports_previous_logs: bool,
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
    horizontal_table(ui, window_id, "containers", "Pod containers table", |ui| {
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
                    super::overview::long_value_cell(ui, 220.0, "Image", Some(&container.image));
                    ui.label(&container.state);
                    ui.label(&container.ready);
                    ui.label(&container.restarts);
                    ui.label(&container.last_exit);
                    ui.label(&container.metrics);
                    ui.end_row();
                }
            });
    });

    section(ui, "CONDITIONS");
    if pod.conditions.is_empty() {
        ui.label("No conditions reported");
    } else {
        horizontal_table(ui, window_id, "conditions", "Pod conditions table", |ui| {
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

fn horizontal_table(
    ui: &mut egui::Ui,
    window_id: WindowId,
    name: &'static str,
    accessible_label: &'static str,
    content: impl FnOnce(&mut egui::Ui),
) {
    let table = ui.push_id(("k10s.detail.pod.table", name, window_id.0), |ui| {
        ScrollArea::horizontal()
            .id_salt(("k10s.detail.pod.table.scroll", name, window_id.0))
            .auto_shrink([false, true])
            .show(ui, content);
    });
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

    if !pod.labels.is_empty() || !pod.annotations.is_empty() {
        if pod.labels.is_empty() {
            super::overview::section_separator(ui);
        } else {
            section(ui, &format!("LABELS · {}", pod.labels.len()));
        }
        let identity = frame.identity;
        super::overview::metadata_labels_and_annotations(
            ui,
            (
                "k10s.detail.pod.annotations",
                window_id.0,
                identity.context.as_str(),
                identity.gvk.group.as_str(),
                identity.gvk.version.as_str(),
                identity.gvk.kind.as_str(),
                identity.namespace.as_deref(),
                identity.name.as_str(),
                identity.uid.as_str(),
            ),
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
