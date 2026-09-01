//! Typed apps/v1 Deployment detail presentation.
//!
//! The renderer consumes only the frozen detail presentation input and typed
//! resource projections. Display summaries, generic detail sections, and YAML
//! are deliberately outside this module's data path.

use egui::{Grid, RichText, ScrollArea, WidgetInfo, WidgetType, accesskit::Role};
use k10s_protocol::{
    ContainerStateProjection, DeploymentProjection, EventRow, EventsCondition, GroupVersionKind,
    PodProjection, ReplicaSetProjection, ResourceIdentity, ResourceListRow, ResourceProjection,
};

use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, WindowId, WorkspaceCommand};
use web_time::{SystemTime, UNIX_EPOCH};

use super::presentation::{
    DetailFrameProjection, DetailPresentationInput, DetailPrimary, DetailVitalShape,
    DetailVitalTone,
};

const POD_GROUP: (&str, &str, &str) = ("", "v1", "Pod");
const REPLICA_SET_GROUP: (&str, &str, &str) = ("apps", "v1", "ReplicaSet");

/// Configure Deployment-owned shared chrome before the frame paints vitals.
pub(super) fn configure_frame(
    input: &DetailPresentationInput<'_>,
    frame: &mut DetailFrameProjection<'_>,
) {
    let Some(deployment) = deployment_of(input) else {
        for vital in frame
            .visible_vitals
            .iter_mut()
            .chain(frame.overflow_vitals.iter_mut())
        {
            if matches!(
                vital.label,
                "Rollout" | "Ready" | "Up-to-date" | "Available" | "Strategy" | "Age"
            ) {
                vital.value = "—".into();
                vital.shape = None;
                vital.tone = DetailVitalTone::Neutral;
            }
        }
        return;
    };
    let rollout = rollout(deployment);
    if let Some(vital) = frame
        .visible_vitals
        .iter_mut()
        .find(|vital| vital.label == "Rollout")
    {
        vital.value = rollout.text;
        vital.shape = rollout.shape;
        vital.tone = rollout.tone;
    }
}

/// Compatibility entry point for the frozen frame call site.
///
/// PR A's follow-up freeze passes the real resource-action queue to
/// [`show_with_actions`]. Until that shared seam lands, rendering remains
/// complete but relation retry clicks cannot escape this compatibility call.
pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    input: &DetailPresentationInput<'_>,
    frame: &mut DetailFrameProjection<'_>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    show_with_actions(
        ui,
        window_id,
        detail,
        input,
        frame,
        resource_actions,
        queued,
    );
}

/// Final Deployment body entry point, including independently retryable
/// relation state. The shared router only needs to forward its existing
/// resource-action queue here.
#[allow(clippy::too_many_arguments)]
pub(super) fn show_with_actions<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    _detail: &DetailState<I>,
    input: &DetailPresentationInput<'_>,
    frame: &mut DetailFrameProjection<'_>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let Some(projection) = DeploymentDetailProjection::from_input(input) else {
        ui.label("Structured details unavailable");
        return;
    };

    if super::overview::two_column(
        ui,
        |column| {
            operational_column(
                column,
                window_id,
                input.identity,
                &projection,
                input.now,
                resource_actions,
                queued,
            );
        },
        |column| metadata_column(column, window_id, input.identity, &projection, frame),
    ) {
        // Both responsive columns were painted by the shared 1.35:1 layout.
    } else {
        operational_column(
            ui,
            window_id,
            input.identity,
            &projection,
            input.now,
            resource_actions,
            queued,
        );
        ui.separator();
        if frame.expansion.metadata {
            if ui.button("Hide Deployment metadata").clicked() {
                frame.expansion.metadata = false;
            }
            metadata_column(ui, window_id, input.identity, &projection, frame);
        } else if ui.button("Show Deployment metadata").clicked() {
            frame.expansion.metadata = true;
        }
    }
}

struct DeploymentDetailProjection<'a> {
    deployment: &'a DeploymentProjection,
    events_condition: EventsCondition,
    events: &'a [EventRow],
    relations: DeploymentRelations<'a>,
}

impl<'a> DeploymentDetailProjection<'a> {
    fn from_input(input: &'a DetailPresentationInput<'a>) -> Option<Self> {
        let DetailPrimary::Loaded(view) = input.primary else {
            return None;
        };
        let deployment = deployment_of(input)?;
        Some(Self {
            deployment,
            events_condition: view.events_condition,
            events: &view.events,
            relations: DeploymentRelations::from_input(input),
        })
    }
}

enum DeploymentRelations<'a> {
    NotRequested,
    Loading,
    Failed(&'a crate::ui::SafeUiError),
    IdentityMismatch,
    Loaded {
        pods: Vec<&'a ResourceListRow>,
        history: Vec<ReplicaSetHistory<'a>>,
        refreshing: bool,
        refresh_error: Option<&'a crate::ui::SafeUiError>,
    },
}

impl<'a> DeploymentRelations<'a> {
    fn from_input(input: &'a DetailPresentationInput<'a>) -> Self {
        let Some(state) = input.relations else {
            return Self::NotRequested;
        };
        match state {
            crate::ui::RelationState::NotRequested => Self::NotRequested,
            crate::ui::RelationState::Loading => Self::Loading,
            crate::ui::RelationState::Failed(error) => Self::Failed(error),
            crate::ui::RelationState::Loaded {
                response,
                refreshing,
                refresh_error,
                ..
            } => {
                if response.identity != *input.identity {
                    return Self::IdentityMismatch;
                }
                let mut pods = Vec::new();
                let mut history = Vec::new();
                for group in &response.groups {
                    if exact_gvk(&group.gvk, POD_GROUP) {
                        pods.extend(
                            group
                                .rows
                                .iter()
                                .filter(|row| exact_related_row(input.identity, row, POD_GROUP)),
                        );
                    } else if exact_gvk(&group.gvk, REPLICA_SET_GROUP) {
                        history.extend(group.rows.iter().filter_map(|row| {
                            if !exact_related_row(input.identity, row, REPLICA_SET_GROUP) {
                                return None;
                            }
                            match row.projection.as_ref() {
                                Some(ResourceProjection::ReplicaSet(replica_set)) => {
                                    Some(ReplicaSetHistory { row, replica_set })
                                }
                                _ => None,
                            }
                        }));
                    }
                }
                history.sort_by(|left, right| {
                    right
                        .replica_set
                        .revision
                        .cmp(&left.replica_set.revision)
                        .then_with(|| left.row.identity.name.cmp(&right.row.identity.name))
                });
                Self::Loaded {
                    pods,
                    history,
                    refreshing: *refreshing,
                    refresh_error: refresh_error.as_ref(),
                }
            }
        }
    }
}

struct ReplicaSetHistory<'a> {
    row: &'a ResourceListRow,
    replica_set: &'a ReplicaSetProjection,
}

struct RolloutVital {
    text: String,
    tone: DetailVitalTone,
    shape: Option<DetailVitalShape>,
}

fn rollout(deployment: &DeploymentProjection) -> RolloutVital {
    let failed = deployment.conditions.iter().find(|condition| {
        (condition.condition_type == "Progressing" && condition.status == "False")
            || (condition.condition_type == "ReplicaFailure" && condition.status == "True")
    });
    if let Some(condition) = failed {
        return RolloutVital {
            text: condition.reason.as_deref().unwrap_or("Failed").to_owned(),
            tone: DetailVitalTone::Danger,
            shape: Some(DetailVitalShape::Cross),
        };
    }

    if let Some(condition) = deployment
        .conditions
        .iter()
        .find(|condition| condition.condition_type == "Progressing" && condition.status == "True")
    {
        if condition.reason.as_deref() == Some("NewReplicaSetAvailable") {
            return RolloutVital {
                text: "NewReplicaSetAvailable".into(),
                tone: DetailVitalTone::Healthy,
                shape: Some(DetailVitalShape::Dot),
            };
        }
        return RolloutVital {
            text: condition
                .reason
                .as_deref()
                .unwrap_or("Progressing")
                .to_owned(),
            tone: DetailVitalTone::Warning,
            shape: Some(DetailVitalShape::Triangle),
        };
    }

    if complete_replica_counts(deployment) {
        return RolloutVital {
            text: "Complete".into(),
            tone: DetailVitalTone::Healthy,
            shape: Some(DetailVitalShape::Dot),
        };
    }

    if let Some(condition) = deployment
        .conditions
        .iter()
        .find(|condition| condition.condition_type == "Available" && condition.status == "False")
    {
        return RolloutVital {
            text: condition
                .reason
                .as_deref()
                .unwrap_or("Progressing")
                .to_owned(),
            tone: DetailVitalTone::Warning,
            shape: Some(DetailVitalShape::Triangle),
        };
    }

    RolloutVital {
        text: "—".into(),
        tone: DetailVitalTone::Neutral,
        shape: None,
    }
}

fn complete_replica_counts(deployment: &DeploymentProjection) -> bool {
    let Some(desired) = deployment.desired_replicas else {
        return false;
    };
    deployment.ready_replicas == Some(desired)
        && deployment.updated_replicas == Some(desired)
        && deployment.available_replicas == Some(desired)
}

fn operational_column<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &ResourceIdentity,
    projection: &DeploymentDetailProjection<'_>,
    now: SystemTime,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    match &projection.relations {
        DeploymentRelations::NotRequested => {
            ui.label("Related resources not requested");
        }
        DeploymentRelations::Loading => {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new());
                ui.label("Loading related resources");
            });
        }
        DeploymentRelations::Failed(error) => {
            ui.label(format!(
                "Related resources unavailable: {}",
                error.message()
            ));
            relation_retry(ui, identity, resource_actions);
        }
        DeploymentRelations::IdentityMismatch => {
            ui.label("Related resources unavailable for this deployment");
        }
        DeploymentRelations::Loaded {
            pods,
            history,
            refreshing,
            refresh_error,
        } => {
            if *refreshing {
                ui.label("Refreshing related resources");
            }
            if let Some(error) = refresh_error {
                ui.label(format!("Refresh failed: {}", error.message()));
                relation_retry(ui, identity, resource_actions);
            }
            pods_table(ui, window_id, pods, now, queued);
            ui.separator();
            rollout_history(ui, window_id, history, now);
        }
    }
    ui.separator();
    rollout_events(ui, projection.events_condition, projection.events);
}

fn relation_retry(
    ui: &mut egui::Ui,
    identity: &ResourceIdentity,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
) {
    if ui.button("Retry related resources").clicked() {
        resource_actions.push(crate::ui::ResourceAction::RetryRelations(identity.clone()));
    }
}

/// A fixed-width table cell that elides overflowing text (left-anchored, like
/// the reference `text-overflow:ellipsis`) and exposes the full value on hover.
fn elided_cell(ui: &mut egui::Ui, width: f32, value: &str) {
    elided_cell_toned(ui, width, value, None);
}

fn elided_cell_toned(ui: &mut egui::Ui, width: f32, value: &str, color: Option<egui::Color32>) {
    let width = width.max(1.0);
    let row_height = ui.spacing().interact_size.y;
    let color = color.unwrap_or(ui.visuals().text_color());
    let content = ui
        .painter()
        .layout_no_wrap(value.to_owned(), egui::FontId::default(), color)
        .size()
        .x;
    let response = ui.add_sized(
        [width, row_height],
        egui::Label::new(RichText::new(value).color(color)).truncate(),
    );
    if content > width {
        response.on_hover_text(value);
    }
}

/// The Pod status text and tone color for the PODS table cell, mirroring the
/// container/phase precedence of the previous inline rendering.
fn pod_status_text(pod: Option<&PodProjection>) -> (String, egui::Color32) {
    let Some(pod) = pod else {
        return ("—".into(), egui::Color32::TRANSPARENT);
    };
    if let Some((shape, text, color)) = pod.containers.iter().find_map(|container| match container
        .state
        .as_ref()?
    {
        ContainerStateProjection::Waiting { reason } => Some((
            "▲",
            reason
                .as_deref()
                .filter(|reason| !reason.is_empty())
                .unwrap_or("Waiting")
                .to_owned(),
            crate::ui::theme::WARNING,
        )),
        ContainerStateProjection::Terminated(termination) if termination.exit_code != 0 => {
            let text = termination
                .reason
                .as_deref()
                .filter(|reason| !reason.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Exit {}", termination.exit_code));
            Some(("✕", text, crate::ui::theme::DANGER))
        }
        ContainerStateProjection::Running | ContainerStateProjection::Terminated(_) => None,
    }) {
        return (format!("{shape} {text}"), color);
    }
    let Some(phase) = pod.phase.as_deref() else {
        return ("—".into(), egui::Color32::TRANSPARENT);
    };
    let (shape, color) = match phase {
        "Running" | "Succeeded" => ("●", crate::ui::theme::HEALTHY),
        "Failed" => ("✕", crate::ui::theme::DANGER),
        _ => ("▲", crate::ui::theme::WARNING),
    };
    (format!("{shape} {phase}"), color)
}

fn pods_table<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    pods: &[&ResourceListRow],
    now: SystemTime,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.heading(format!("PODS · {}", pods.len()));
    if pods.is_empty() {
        ui.label("No related Pods");
        return;
    }
    // Reference column widths; NAME flexes to fill the remaining space.
    const READY: f32 = 52.0;
    const STATUS: f32 = 92.0;
    const RESTARTS: f32 = 60.0;
    const NODE: f32 = 120.0;
    const AGE: f32 = 42.0;
    horizontal_table(ui, window_id, "pods", "Deployment Pods table", |ui| {
        let spacing = ui.spacing().item_spacing.x;
        let available = ui.clip_rect().width().max(200.0);
        let name_width =
            (available - READY - STATUS - RESTARTS - NODE - AGE - spacing * 5.0).max(60.0);
        // Header row: fixed columns match the body so values never drift.
        ui.horizontal(|ui| {
            let mut header = |label: &str, width: f32| {
                ui.add_sized(
                    [width, ui.spacing().interact_size.y],
                    egui::Label::new(RichText::new(label).strong().weak()),
                );
            };
            header("Name", name_width);
            header("Ready", READY);
            header("Status", STATUS);
            header("Restarts", RESTARTS);
            header("Node", NODE);
            header("Age", AGE);
        });
        ui.separator();
        for row in pods {
            let pod = match row.projection.as_ref() {
                Some(ResourceProjection::Pod(pod)) => Some(pod),
                _ => None,
            };
            ui.horizontal(|ui| {
                let name = row.identity.name.clone();
                let label = format!("Pod · {}", name);
                let open = ui.add_sized(
                    [name_width, ui.spacing().interact_size.y],
                    egui::Button::new(name.clone()).truncate(),
                );
                open.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
                if open.clicked() {
                    queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                        &row.identity,
                    )));
                }
                elided_cell(
                    ui,
                    READY,
                    &pod.and_then(|pod| pair(pod.ready_containers, pod.total_containers))
                        .unwrap_or_else(|| "—".into()),
                );
                // Status cell keeps its tone dot; the label stays fixed-width.
                let (status_text, status_color) = pod_status_text(pod);
                elided_cell_toned(ui, STATUS, &status_text, Some(status_color));
                elided_cell(ui, RESTARTS, &number(pod.and_then(|pod| pod.restart_count)));
                elided_cell(
                    ui,
                    NODE,
                    value(pod.and_then(|pod| pod.node_name.as_deref())),
                );
                elided_cell(
                    ui,
                    AGE,
                    &format_age(pod.and_then(|pod| pod.created_at.as_deref()), now),
                );
            });
        }
    });
}

/// The short image tag (after the final ':') for the IMAGE TAG column.
fn image_tag(images: &[k10s_protocol::ContainerImageProjection]) -> String {
    let Some(image) = images.first().and_then(|image| image.image.as_deref()) else {
        return "—".into();
    };
    image
        .rsplit_once(':')
        .map(|(_, tag)| tag.to_owned())
        .unwrap_or_else(|| image.to_owned())
}

fn rollout_history(
    ui: &mut egui::Ui,
    window_id: WindowId,
    history: &[ReplicaSetHistory<'_>],
    now: SystemTime,
) {
    ui.heading("ROLLOUT HISTORY");
    if history.is_empty() {
        ui.label("No rollout history");
        return;
    }
    // Reference column widths; REPLICASET flexes to fill the remaining space.
    const REV: f32 = 96.0;
    const IMAGE_TAG: f32 = 86.0;
    const WHEN: f32 = 70.0;
    horizontal_table(
        ui,
        window_id,
        "history",
        "Deployment rollout history table",
        |ui| {
            let spacing = ui.spacing().item_spacing.x;
            let available = ui.clip_rect().width().max(200.0);
            let replica_set_width = (available - REV - IMAGE_TAG - WHEN - spacing * 3.0).max(60.0);
            ui.horizontal(|ui| {
                let mut header = |label: &str, width: f32| {
                    ui.add_sized(
                        [width, ui.spacing().interact_size.y],
                        egui::Label::new(RichText::new(label).strong().weak()),
                    );
                };
                header("Rev", REV);
                header("ReplicaSet", replica_set_width);
                header("Image tag", IMAGE_TAG);
                header("When", WHEN);
            });
            ui.separator();
            for (index, history) in history.iter().enumerate() {
                let is_current = index == 0;
                let mut revision = format!("{}", history.replica_set.revision);
                if is_current {
                    revision.push_str(" current");
                }
                ui.horizontal(|ui| {
                    elided_cell(ui, REV, &revision);
                    elided_cell(ui, replica_set_width, &history.row.identity.name);
                    elided_cell(ui, IMAGE_TAG, &image_tag(&history.replica_set.images));
                    elided_cell(
                        ui,
                        WHEN,
                        &format_age(history.replica_set.created_at.as_deref(), now),
                    );
                });
            }
        },
    );
}

fn horizontal_table(
    ui: &mut egui::Ui,
    window_id: WindowId,
    name: &'static str,
    accessible_label: &'static str,
    content: impl FnOnce(&mut egui::Ui),
) {
    let table = ui.push_id(("k10s.detail.deployment.table", name, window_id.0), |ui| {
        ScrollArea::horizontal()
            .id_salt(("k10s.detail.deployment.table.scroll", name, window_id.0))
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

fn format_age(created_at: Option<&str>, now: SystemTime) -> String {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;
    const WEEK: i64 = 7 * DAY;

    let Some(created_at) = created_at else {
        return "—".into();
    };
    let Ok(created_at) = created_at.parse::<jiff::Timestamp>() else {
        return "—".into();
    };
    let Ok(now_since_epoch) = now.duration_since(UNIX_EPOCH) else {
        return "—".into();
    };
    let Ok(now_seconds) = i64::try_from(now_since_epoch.as_secs()) else {
        return "—".into();
    };
    let Ok(now) = jiff::Timestamp::new(now_seconds, now_since_epoch.subsec_nanos() as i32) else {
        return "—".into();
    };
    let age = now.duration_since(created_at);
    if age.is_negative() {
        return "—".into();
    }
    let age = age.as_secs();
    if age >= WEEK {
        return format!("{}d", age / DAY);
    }
    if age >= DAY {
        let days = age / DAY;
        let hours = age % DAY / HOUR;
        return if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d {hours}h")
        };
    }
    if age >= HOUR {
        let hours = age / HOUR;
        let minutes = age % HOUR / MINUTE;
        return if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        };
    }
    if age >= MINUTE {
        return format!("{}m", age / MINUTE);
    }
    format!("{}s", age)
}

fn rollout_events(ui: &mut egui::Ui, condition: EventsCondition, events: &[EventRow]) {
    // An unavailable events feed is a distinct, explicit error state.
    if condition == EventsCondition::Unavailable {
        ui.label(RichText::new("Rollout events unavailable").weak());
        return;
    }
    // Empty rollout events collapse to one muted line (no reserved heading),
    // matching the reference where absent events cost no section of their own.
    if events.is_empty() {
        ui.label(RichText::new("No rollout events in the last 24h.").weak());
        return;
    }
    ui.heading("RECENT ROLLOUT EVENTS");
    for event in events.iter().take(5) {
        ui.label(format!(
            "{} · {} · ×{} · {}",
            event.reason, event.message, event.count, event.last_seen
        ));
    }
}

fn metadata_column(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &ResourceIdentity,
    projection: &DeploymentDetailProjection<'_>,
    frame: &mut DetailFrameProjection<'_>,
) {
    template(ui, window_id, projection.deployment);
    ui.separator();
    managed_by(ui, window_id, projection.deployment);
    ui.separator();
    labels(ui, projection.deployment, frame);
    ui.separator();
    annotations(ui, window_id, projection.deployment);
    ui.separator();
    identity_section(ui, window_id, identity, projection.deployment);
}

fn template(ui: &mut egui::Ui, window_id: WindowId, deployment: &DeploymentProjection) {
    ui.heading("TEMPLATE");
    // The long values (Image, Selector) use the full column width so the
    // elided code text has room and the copy control never overlaps.
    let width = ui.available_width().max(120.0);
    Grid::new(("k10s.detail.deployment.template", window_id.0))
        .num_columns(1)
        .striped(true)
        .show(ui, |ui| {
            if deployment.template_containers.is_empty() {
                row(ui, "Images", "—");
            } else {
                for container in &deployment.template_containers {
                    super::overview::long_value(
                        ui,
                        width,
                        &format!("Image ({})", container.name),
                        container.image.as_deref(),
                    );
                    ui.end_row();
                }
            }
            row(
                ui,
                "Replicas",
                &pair(deployment.available_replicas, deployment.desired_replicas)
                    .map_or_else(|| "—".into(), |pair| format!("{pair} available")),
            );
            row(ui, "Max surge", value(deployment.max_surge.as_deref()));
            row(
                ui,
                "Max unavailable",
                value(deployment.max_unavailable.as_deref()),
            );
            let selector = map_list(&deployment.selector);
            super::overview::long_value(ui, width, "Selector", Some(&selector));
            ui.end_row();
            row(
                ui,
                "Template labels",
                &map_list(&deployment.template_labels),
            );
            row(
                ui,
                "Template annotations",
                &map_list(&deployment.template_annotations),
            );
        });
}

fn managed_by(ui: &mut egui::Ui, window_id: WindowId, deployment: &DeploymentProjection) {
    ui.heading("MANAGED BY");
    Grid::new(("k10s.detail.deployment.manager", window_id.0))
        .num_columns(1)
        .striped(true)
        .show(ui, |ui| {
            row(
                ui,
                "Manager",
                value(
                    deployment
                        .labels
                        .get("app.kubernetes.io/managed-by")
                        .map(String::as_str),
                ),
            );
            if let Some(release) = deployment.annotations.get("meta.helm.sh/release-name") {
                row(ui, "Helm release", release);
            }
            if let Some(namespace) = deployment.annotations.get("meta.helm.sh/release-namespace") {
                row(ui, "Helm namespace", namespace);
            }
            if let Some(chart) = deployment.labels.get("helm.sh/chart") {
                row(ui, "Chart", chart);
            }
        });
}

/// A single label chip: key + value with a bounded, tinted fill, matching the
/// reference `.chip`. The inner label carries the `key: value` accessible name
/// so the chip stays queryable even though the visible text omits the colon.
fn label_chip(ui: &mut egui::Ui, key: &str, value: &str) -> egui::Response {
    // Truncate visible text so wide k8s keys don't overflow the wrap row;
    // the full `key: value` stays the accessible name and hover text.
    let full = format!("{}: {}", key, value);
    let text = crate::ui::responsive_table::middle_elide(&full, 18);
    let accessible = full.clone();
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(35, 35, 35))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(59, 59, 59)))
        .corner_radius(11.0)
        .inner_margin(egui::Margin::symmetric(8, 2))
        .show(ui, |ui| {
            let response = ui.label(RichText::new(text).small());
            let response = response.on_hover_text(&accessible);
            response.widget_info(|| {
                egui::WidgetInfo::labeled(egui::WidgetType::Label, true, accessible.clone())
            });
            response
        })
        .inner
}

/// Estimate a label chip's painted width (text + padding + border) without
/// rendering it, so chips can be grouped into rows that respect the column
/// width.
fn label_chip_width(ui: &mut egui::Ui, key: &str, value: &str) -> f32 {
    let full = format!("{}: {}", key, value);
    let text = crate::ui::responsive_table::middle_elide(&full, 18);
    let text_width = ui
        .painter()
        .layout_no_wrap(text, egui::FontId::proportional(12.0), egui::Color32::WHITE)
        .size()
        .x;
    // inner_margin symmetric(8,2) => 16px horizontal padding, +2px border
    text_width + 16.0 + 2.0
}

/// Render label chips in manually-wrapped rows. egui's built-in wrap only
/// triggers when a single chip exceeds the full row width, so with many long
/// k8s keys chips would overflow instead of wrapping; this groups them by width.
fn render_label_chips(ui: &mut egui::Ui, deployment: &DeploymentProjection, visible: usize) {
    let available = ui.available_width();
    let spacing = ui.spacing().item_spacing.x;
    let chips: Vec<(String, String)> = deployment
        .labels
        .iter()
        .take(visible)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut rows: Vec<Vec<(String, String)>> = Vec::new();
    let mut current: Vec<(String, String)> = Vec::new();
    let mut row_width = 0.0;
    for chip in &chips {
        let chip_width = label_chip_width(ui, &chip.0, &chip.1);
        let needed = if current.is_empty() { 0.0 } else { spacing } + chip_width;
        if !current.is_empty() && row_width + needed > available {
            rows.push(std::mem::take(&mut current));
            row_width = 0.0;
        }
        row_width += needed;
        current.push((chip.0.clone(), chip.1.clone()));
    }
    if !current.is_empty() {
        rows.push(current);
    }
    for row in rows {
        ui.horizontal(|ui| {
            for (key, value) in row {
                label_chip(ui, &key, &value);
            }
        });
    }
}

fn labels(
    ui: &mut egui::Ui,
    deployment: &DeploymentProjection,
    frame: &mut DetailFrameProjection<'_>,
) {
    ui.heading(format!("LABELS · {}", deployment.labels.len()));
    let total = deployment.labels.len();
    let visible = if frame.expansion.labels {
        total
    } else {
        total.min(4)
    };
    // egui's horizontal_wrapped only wraps when an item is wider than the whole
    // available width, not when it would overflow the current row. With long k8s
    // keys, chips accumulate past the row width without wrapping and get
    // stretched vertically, pushing later controls below the fold. So we wrap
    // manually: start a new row once adding the next chip would overflow.
    render_label_chips(ui, deployment, visible);
    let hidden = total.saturating_sub(visible);
    if !frame.expansion.labels && hidden > 0 {
        if ui.button(format!("Show {hidden} more labels")).clicked() {
            frame.expansion.labels = true;
        }
    } else if frame.expansion.labels && total > 4 {
        let extra = total - 4;
        if ui.button(format!("Hide {extra} labels")).clicked() {
            frame.expansion.labels = false;
        }
    }
}

fn annotations(ui: &mut egui::Ui, window_id: WindowId, deployment: &DeploymentProjection) {
    let expansion_id = egui::Id::new(("k10s.detail.deployment.annotations", window_id.0));
    let mut expanded = ui
        .ctx()
        .data_mut(|data| data.get_temp::<bool>(expansion_id))
        .unwrap_or(false);
    ui.heading(format!("ANNOTATIONS · {}", deployment.annotations.len()));
    if expanded {
        for (key, value) in &deployment.annotations {
            super::overview::long_value(ui, ui.available_width(), key, Some(value));
        }
        if ui.button("Hide annotations").clicked() {
            expanded = false;
        }
    } else if ui
        .button(format!("Show {} annotations", deployment.annotations.len()))
        .clicked()
    {
        expanded = true;
    }
    ui.ctx()
        .data_mut(|data| data.insert_temp(expansion_id, expanded));
}

fn identity_section(
    ui: &mut egui::Ui,
    window_id: WindowId,
    identity: &ResourceIdentity,
    deployment: &DeploymentProjection,
) {
    ui.heading("IDENTITY");
    Grid::new(("k10s.detail.deployment.identity", window_id.0))
        .num_columns(1)
        .striped(true)
        .show(ui, |ui| {
            row(ui, "Name", &identity.name);
            row(ui, "Namespace", value(identity.namespace.as_deref()));
            row(ui, "Created", value(deployment.created_at.as_deref()));
            row(
                ui,
                "UID",
                value((!identity.uid.is_empty()).then_some(&identity.uid)),
            );
            row(ui, "Context", &identity.context);
        });
}

fn row(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.label(format!("{label} · {value}"));
    ui.end_row();
}

fn deployment_of<'a>(input: &'a DetailPresentationInput<'a>) -> Option<&'a DeploymentProjection> {
    let DetailPrimary::Loaded(view) = input.primary else {
        return None;
    };
    if view.identity != *input.identity {
        return None;
    }
    match view.projection.as_ref() {
        Some(ResourceProjection::Deployment(deployment)) => Some(deployment),
        _ => None,
    }
}

fn exact_related_row(
    deployment: &ResourceIdentity,
    row: &ResourceListRow,
    expected_gvk: (&str, &str, &str),
) -> bool {
    row.identity.context == deployment.context
        && row.identity.namespace == deployment.namespace
        && exact_gvk(&row.identity.gvk, expected_gvk)
}

fn exact_gvk(gvk: &GroupVersionKind, expected: (&str, &str, &str)) -> bool {
    gvk.group == expected.0 && gvk.version == expected.1 && gvk.kind == expected.2
}

fn pair(left: Option<u32>, right: Option<u32>) -> Option<String> {
    Some(format!("{}/{}", left?, right?))
}

fn number(value: Option<u32>) -> String {
    value.map_or_else(|| "—".into(), |value| value.to_string())
}

fn value(value: Option<&str>) -> &str {
    value.unwrap_or("—")
}

fn map_list(values: &std::collections::BTreeMap<String, String>) -> String {
    if values.is_empty() {
        return "—".into();
    }
    values
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod shared_seam_tests {
    use super::super::overview::detail_columns;

    #[test]
    fn deployment_body_breakpoint_is_exactly_760_points() {
        assert!(detail_columns(759.0, 8.0).is_none());
        assert!(detail_columns(760.0, 8.0).is_some());
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use egui_kittest::{Harness, kittest::Queryable as _};
    use k10s_protocol::{
        BackendRevision, DeploymentProjection, EventsCondition, GroupVersionKind, PodProjection,
        RelatedGroup, ResourceCapabilities, ResourceDetailResponse, ResourceIdentity,
        ResourceListRow, ResourceProjection, ResourceRelationsResponse,
    };

    use super::super::presentation::{DetailMetrics, DetailPresentationInput, DetailPrimary};
    use crate::ui::RelationState;
    use crate::workspace::{DetailState, WindowId};

    #[test]
    fn show_contract_accepts_the_shared_resource_action_queue() {
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let detail = DetailState::new(identity.clone());
        let mut harness = Harness::builder().build_ui(move |ui| {
            let input = DetailPresentationInput {
                identity: &identity,
                primary: DetailPrimary::Loading,
                metrics: DetailMetrics {
                    status: None,
                    age: None,
                },
                resource_metrics: None,
                relations: None,
                freshness: None,
                now: web_time::UNIX_EPOCH,
                gone: false,
                mutations_allowed: false,
                port_forward_available: false,
                port_forward_sessions: &[],
                port_forward_error: None,
            };
            let mut frame = input.frame_projection(Default::default());
            let mut queued = Vec::new();
            let mut resource_actions = Vec::new();
            super::show(
                ui,
                WindowId(9),
                &detail,
                &input,
                &mut frame,
                &mut resource_actions,
                &mut queued,
            );
            assert!(resource_actions.is_empty());
        });
        harness.run();
    }

    #[test]
    fn pod_age_uses_the_injected_clock_and_typed_rfc3339_creation_time_only() {
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let view = ResourceDetailResponse {
            identity: identity.clone(),
            revision: BackendRevision::new(1),
            created_at: "generic-created-at-must-not-render".into(),
            owner_references: Vec::new(),
            sections: Vec::new(),
            events_condition: EventsCondition::Available,
            events: Vec::new(),
            related: Vec::new(),
            capabilities: ResourceCapabilities::default(),
            manifest: String::new(),
            projection: Some(ResourceProjection::Deployment(DeploymentProjection {
                desired_replicas: None,
                ready_replicas: None,
                updated_replicas: None,
                available_replicas: None,
                strategy: None,
                selector: BTreeMap::new(),
                max_surge: None,
                max_unavailable: None,
                conditions: Vec::new(),
                template_containers: Vec::new(),
                template_labels: BTreeMap::new(),
                template_annotations: BTreeMap::new(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                created_at: None,
            })),
        };
        let relations = RelationState::Loaded {
            response: Arc::new(ResourceRelationsResponse {
                identity: identity.clone(),
                revision: BackendRevision::new(2),
                groups: vec![RelatedGroup {
                    title: "Pods".into(),
                    gvk: GroupVersionKind::core("v1", "Pod"),
                    rows: vec![ResourceListRow {
                        identity: ResourceIdentity {
                            context: "dev-local".into(),
                            gvk: GroupVersionKind::core("v1", "Pod"),
                            namespace: Some("default".into()),
                            name: "web-0".into(),
                            uid: "uid-web-0".into(),
                        },
                        revision: BackendRevision::new(2),
                        labels: BTreeMap::new(),
                        summary: "row-summary-must-not-render".into(),
                        created_at: "row-created-at-must-not-render".into(),
                        projection: Some(ResourceProjection::Pod(PodProjection {
                            phase: None,
                            ready_containers: None,
                            total_containers: None,
                            restart_count: None,
                            containers: Vec::new(),
                            conditions: Vec::new(),
                            node_name: None,
                            pod_ip: None,
                            host_ip: None,
                            qos_class: None,
                            priority: None,
                            service_account: None,
                            restart_policy: None,
                            ports: Vec::new(),
                            labels: BTreeMap::new(),
                            annotations: BTreeMap::new(),
                            created_at: Some("1970-02-01T23:42:00Z".into()),
                        })),
                    }],
                }],
            }),
            loaded_at_ms: 0,
            refreshing: false,
            refresh_error: None,
        };
        let detail = DetailState::new(identity.clone());
        let mut harness = Harness::builder().build_ui(move |ui| {
            let input = DetailPresentationInput {
                identity: &identity,
                primary: DetailPrimary::Loaded(&view),
                metrics: DetailMetrics {
                    status: None,
                    age: None,
                },
                resource_metrics: None,
                relations: Some(&relations),
                freshness: None,
                now: web_time::UNIX_EPOCH + Duration::from_secs(32 * 24 * 60 * 60),
                gone: false,
                mutations_allowed: false,
                port_forward_available: false,
                port_forward_sessions: &[],
                port_forward_error: None,
            };
            let mut frame = input.frame_projection(Default::default());
            let mut queued = Vec::new();
            let mut resource_actions = Vec::new();
            super::show(
                ui,
                WindowId(10),
                &detail,
                &input,
                &mut frame,
                &mut resource_actions,
                &mut queued,
            );
        });
        harness.run();

        harness.get_by_label("18m");
        assert!(harness.query_by_label("1970-02-01T23:42:00Z").is_none());
        assert!(
            harness
                .query_by_label("row-created-at-must-not-render")
                .is_none()
        );
        assert!(
            harness
                .query_by_label("row-summary-must-not-render")
                .is_none()
        );
    }
}
