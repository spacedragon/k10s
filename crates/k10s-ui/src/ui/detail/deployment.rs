//! Typed apps/v1 Deployment detail presentation.
//!
//! The renderer consumes only the frozen detail presentation input and typed
//! resource projections. Display summaries, generic detail sections, and YAML
//! are deliberately outside this module's data path.

use egui::{RichText, ScrollArea, WidgetInfo, WidgetType, accesskit::Role};
use k10s_protocol::{
    ContainerStateProjection, DeploymentProjection, EventRow, EventsCondition, GroupVersionKind,
    PodProjection, ReplicaSetProjection, ResourceIdentity, ResourceListRow, ResourceProjection,
};

use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};
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
    frame.vitals_in_footer = true;
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
    // The Pods tab badge states how many Pods that tab will show.
    frame.pod_count = related_pod_count(input);
    let rollout = rollout(deployment);
    if let Some(vital) = frame
        .visible_vitals
        .iter_mut()
        .find(|vital| vital.label == "Rollout")
    {
        vital.value = rollout.text;
        vital.shape = rollout.shape;
        vital.tone = rollout.tone;
        vital.hint = rollout.hint;
    }
}

/// The number of Pods this Deployment's relation feed has resolved, or
/// `None` while the feed is loading, failed, or bound to another identity.
fn related_pod_count(input: &DetailPresentationInput<'_>) -> Option<usize> {
    let Some(crate::ui::RelationState::Loaded { response, .. }) = input.relations else {
        return None;
    };
    if response.identity != *input.identity {
        return None;
    }
    Some(
        response
            .groups
            .iter()
            .filter(|group| exact_gvk(&group.gvk, POD_GROUP))
            .flat_map(|group| group.rows.iter())
            .filter(|row| exact_related_row(input.identity, row, POD_GROUP))
            .count(),
    )
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
        |column| {
            metadata_column(
                column,
                window_id,
                input.identity,
                &projection,
                input.now,
                frame,
            );
        },
    ) {
        // Both responsive columns were painted by the shared 1.35:1 layout.
    } else {
        // Keep the narrow-mode disclosure in the initially visible control
        // region. Operational tables can be taller than the viewport; placing
        // this after them made the only route to metadata start below the
        // window clip at browser-sized viewports.
        let metadata_expanded = frame.expansion.metadata;
        let disclosure_clicked = if metadata_expanded {
            ui.button("Hide Deployment metadata").clicked()
        } else {
            ui.button("Show Deployment metadata").clicked()
        };
        if disclosure_clicked {
            frame.expansion.metadata = !metadata_expanded;
        }
        ui.separator();
        operational_column(
            ui,
            window_id,
            input.identity,
            &projection,
            input.now,
            resource_actions,
            queued,
        );
        if frame.expansion.metadata {
            ui.separator();
            metadata_column(ui, window_id, input.identity, &projection, input.now, frame);
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
    /// Condition reason/message; belongs in the chip tooltip, not the chip.
    hint: Option<String>,
}

/// `Progressing · NewReplicaSetAvailable: Deployment "x" has successfully
/// progressed.` — the raw condition, kept for hover.
fn condition_hint(condition: &k10s_protocol::ResourceConditionProjection) -> Option<String> {
    let reason = condition
        .reason
        .as_deref()
        .filter(|reason| !reason.is_empty());
    let message = condition
        .message
        .as_deref()
        .filter(|message| !message.is_empty());
    match (reason, message) {
        (None, None) => None,
        (Some(reason), None) => Some(format!("{} · {reason}", condition.condition_type)),
        (None, Some(message)) => Some(format!("{} · {message}", condition.condition_type)),
        (Some(reason), Some(message)) => Some(format!(
            "{} · {reason}: {message}",
            condition.condition_type
        )),
    }
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
            hint: condition_hint(condition),
        };
    }

    if let Some(condition) = deployment
        .conditions
        .iter()
        .find(|condition| condition.condition_type == "Progressing" && condition.status == "True")
    {
        if condition.reason.as_deref() == Some("NewReplicaSetAvailable") {
            // The reason is implementation vocabulary; the chip says what it
            // means and keeps the reason for hover.
            return RolloutVital {
                text: "Complete".into(),
                tone: DetailVitalTone::Healthy,
                shape: Some(DetailVitalShape::Dot),
                hint: condition_hint(condition),
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
            hint: condition_hint(condition),
        };
    }

    if complete_replica_counts(deployment) {
        return RolloutVital {
            text: "Complete".into(),
            tone: DetailVitalTone::Healthy,
            shape: Some(DetailVitalShape::Dot),
            hint: None,
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
            hint: condition_hint(condition),
        };
    }

    RolloutVital {
        text: "—".into(),
        tone: DetailVitalTone::Neutral,
        shape: None,
        hint: None,
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
    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    let color = color.unwrap_or(ui.visuals().text_color());
    let content = ui
        .painter()
        .layout_no_wrap(value.to_owned(), egui::FontId::default(), color)
        .size()
        .x;
    let response = ui.add_sized(
        [width, row_height],
        egui::Label::new(
            RichText::new(value)
                .text_style(egui::TextStyle::Body)
                .color(color),
        )
        .truncate(),
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
            Some(("⨯", text, crate::ui::theme::DANGER))
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
        "Failed" => ("⨯", crate::ui::theme::DANGER),
        _ => ("▲", crate::ui::theme::WARNING),
    };
    (format!("{shape} {phase}"), color)
}

/// Table header cell: small uppercase muted text, aligned exactly like the
/// column's own cells (never centered).
fn header_cell(ui: &mut egui::Ui, width: f32, height: f32, align: egui::Align, label: &str) {
    let font = egui::FontId::new(10.0, egui::FontFamily::Monospace);
    aligned_label_cell_in(
        ui,
        width,
        height,
        align,
        label,
        egui::Label::new(
            RichText::new(label.to_uppercase())
                .font(font.clone())
                .color(crate::ui::theme::MUTED_TEXT),
        ),
        font,
    );
}

/// The width the table may paint into: bounded by the enclosing clip *and*
/// by what is still free after the cursor, so the last column is never cut
/// off at the column or window edge.
fn table_content_width(ui: &egui::Ui) -> f32 {
    ui.available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width()
}

fn pods_table<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    pods: &[&ResourceListRow],
    now: SystemTime,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    // The section header doubles as the route to the full Pods tab.
    if super::overview::section_action(ui, "PODS", Some(&pods.len().to_string()), "Open Pods tab →")
    {
        queued.push(WorkspaceCommand::SetActiveTab(window_id, DetailTab::Pods));
    }
    if pods.is_empty() {
        ui.label(RichText::new("No related Pods").weak());
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
        let fixed = READY + STATUS + RESTARTS + NODE + AGE + spacing * 5.0;
        let semantic_minimum = 60.0 + fixed;
        let table_width = table_content_width(ui).max(semantic_minimum);
        let name_width = table_width - fixed;
        let row_height = ui
            .spacing()
            .interact_size
            .y
            .max(ui.text_style_height(&egui::TextStyle::Body));
        // Header row: fixed columns match the body so values never drift.
        ui.horizontal(|ui| {
            header_cell(ui, name_width, row_height, egui::Align::Min, "Name");
            header_cell(ui, READY, row_height, egui::Align::Max, "Ready");
            header_cell(ui, STATUS, row_height, egui::Align::Min, "Status");
            header_cell(ui, RESTARTS, row_height, egui::Align::Max, "Restarts");
            header_cell(ui, NODE, row_height, egui::Align::Min, "Node");
            header_cell(ui, AGE, row_height, egui::Align::Max, "Age");
        });
        ui.separator();
        for row in pods {
            let pod = match row.projection.as_ref() {
                Some(ResourceProjection::Pod(pod)) => Some(pod),
                _ => None,
            };
            // Reserve the row background slot before the cells so a hover
            // highlight paints behind the text instead of over it.
            let background = ui.painter().add(egui::Shape::Noop);
            let cells = ui.horizontal(|ui| {
                // Plain text, not a framed button: the whole row is the
                // affordance, so the name must not look like a control.
                elided_cell(ui, name_width, &row.identity.name);
                right_aligned_cell(
                    ui,
                    READY,
                    &pod.and_then(|pod| pair(pod.ready_containers, pod.total_containers))
                        .unwrap_or_else(|| "—".into()),
                );
                // Status cell keeps its tone dot; the label stays fixed-width.
                let (status_text, status_color) = pod_status_text(pod);
                elided_cell_toned(ui, STATUS, &status_text, Some(status_color));
                right_aligned_cell(ui, RESTARTS, &number(pod.and_then(|pod| pod.restart_count)));
                elided_cell(
                    ui,
                    NODE,
                    value(pod.and_then(|pod| pod.node_name.as_deref())),
                );
                right_aligned_cell(
                    ui,
                    AGE,
                    &format_age(pod.and_then(|pod| pod.created_at.as_deref()), now),
                );
            });
            let row_rect = egui::Rect::from_min_size(
                cells.response.rect.min,
                egui::vec2(table_width, cells.response.rect.height()),
            );
            let label = format!("Pod · {}", row.identity.name);
            let open = ui.interact(
                row_rect,
                ui.id().with((
                    "k10s.detail.deployment.pod-row",
                    &row.identity.name,
                    &row.identity.uid,
                )),
                egui::Sense::click(),
            );
            open.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
            if open.hovered() {
                ui.painter().set(
                    background,
                    egui::Shape::rect_filled(
                        row_rect,
                        0.0,
                        ui.visuals().widgets.hovered.weak_bg_fill,
                    ),
                );
            }
            if open.clicked() {
                queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                    &row.identity,
                )));
            }
        }
    });
}

fn right_aligned_cell(ui: &mut egui::Ui, width: f32, value: &str) {
    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    aligned_label_cell(
        ui,
        width,
        row_height,
        egui::Align::Max,
        value,
        egui::Label::new(RichText::new(value).text_style(egui::TextStyle::Body)),
    );
}

fn aligned_label_cell(
    ui: &mut egui::Ui,
    width: f32,
    height: f32,
    align: egui::Align,
    text: &str,
    label: egui::Label,
) {
    let font = egui::TextStyle::Body.resolve(ui.style());
    aligned_label_cell_in(ui, width, height, align, text, label, font);
}

/// `aligned_label_cell`, measuring the right-aligned offset in `font` so a
/// header painted smaller than the body still lines up with its column.
fn aligned_label_cell_in(
    ui: &mut egui::Ui,
    width: f32,
    height: f32,
    align: egui::Align,
    text: &str,
    label: egui::Label,
    font: egui::FontId,
) {
    let text_width = ui
        .painter()
        .layout_no_wrap(text.to_owned(), font, ui.visuals().text_color())
        .size()
        .x;
    let overflowing = text_width > width;
    // `add_sized` centers its widget, so left-aligned cells lay the label out
    // in an explicit left-to-right cell instead; right-aligned cells keep
    // padding the text to the cell's right edge.
    let response = if align == egui::Align::Max {
        let label_width = text_width.min(width);
        ui.add_space(width - label_width);
        ui.add_sized([label_width, height], label.truncate())
    } else {
        ui.allocate_ui_with_layout(
            egui::vec2(width, height),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                ui.set_min_size(egui::vec2(width, height));
                ui.add(label.truncate().halign(egui::Align::Min))
            },
        )
        .inner
    };
    response.widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, text));
    if overflowing {
        response.on_hover_text(text);
    }
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

/// `9` in normal text with a weak `current` qualifier: the number is the
/// fact, the qualifier is context, so they must not read as one value.
fn revision_cell(ui: &mut egui::Ui, width: f32, revision: u64, current: bool) {
    let row_height = ui
        .spacing()
        .interact_size
        .y
        .max(ui.text_style_height(&egui::TextStyle::Body));
    let font = egui::TextStyle::Body.resolve(ui.style());
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = f32::INFINITY;
    job.append(
        &revision.to_string(),
        0.0,
        egui::TextFormat::simple(font.clone(), ui.visuals().text_color()),
    );
    let accessible = if current {
        job.append(
            " current",
            0.0,
            egui::TextFormat::simple(font, crate::ui::theme::FAINT_TEXT),
        );
        format!("{revision} current")
    } else {
        revision.to_string()
    };
    aligned_label_cell(
        ui,
        width,
        row_height,
        egui::Align::Min,
        &accessible,
        egui::Label::new(job),
    );
}

fn rollout_history(
    ui: &mut egui::Ui,
    window_id: WindowId,
    history: &[ReplicaSetHistory<'_>],
    now: SystemTime,
) {
    let note = if history.len() == 1 {
        "1 revision".to_owned()
    } else {
        format!("{} revisions", history.len())
    };
    super::overview::section_note(ui, "ROLLOUT HISTORY", &note);
    if history.is_empty() {
        ui.label(RichText::new("No rollout history").weak());
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
            let available = table_content_width(ui).max(200.0);
            let replica_set_width = (available - REV - IMAGE_TAG - WHEN - spacing * 3.0).max(60.0);
            let row_height = ui
                .spacing()
                .interact_size
                .y
                .max(ui.text_style_height(&egui::TextStyle::Body));
            ui.horizontal(|ui| {
                header_cell(ui, REV, row_height, egui::Align::Min, "Rev");
                header_cell(
                    ui,
                    replica_set_width,
                    row_height,
                    egui::Align::Min,
                    "ReplicaSet",
                );
                header_cell(ui, IMAGE_TAG, row_height, egui::Align::Min, "Image tag");
                header_cell(ui, WHEN, row_height, egui::Align::Max, "When");
            });
            ui.separator();
            for (index, history) in history.iter().enumerate() {
                let is_current = index == 0;
                ui.horizontal(|ui| {
                    revision_cell(ui, REV, history.replica_set.revision, is_current);
                    // The ReplicaSet hash is context, not the fact being
                    // read; the revision and tag carry that.
                    elided_cell_toned(
                        ui,
                        replica_set_width,
                        &history.row.identity.name,
                        Some(crate::ui::theme::FAINT_TEXT),
                    );
                    elided_cell(ui, IMAGE_TAG, &image_tag(&history.replica_set.images));
                    right_aligned_cell(
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
    super::overview::section(ui, "RECENT ROLLOUT EVENTS", None);
    for event in events.iter().take(5) {
        ui.label(format!(
            "{} · {} · ×{} · {}",
            event.reason, event.message, event.count, event.last_seen
        ));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConfigurationSection {
    Template,
    LabelsAnnotations,
    Identity,
}

/// Reference right column: TEMPLATE, then LABELS (with the annotations
/// disclosure) only when there is something to show, then IDENTITY. The
/// manager (Helm/Argo) is one IDENTITY row, not a section of its own.
fn configuration_section_sequence(
    has_labels: bool,
    has_annotations: bool,
) -> Vec<ConfigurationSection> {
    let mut sections = vec![ConfigurationSection::Template];
    if has_labels || has_annotations {
        sections.push(ConfigurationSection::LabelsAnnotations);
    }
    sections.push(ConfigurationSection::Identity);
    sections
}

fn metadata_column(
    ui: &mut egui::Ui,
    _window_id: WindowId,
    identity: &ResourceIdentity,
    projection: &DeploymentDetailProjection<'_>,
    now: SystemTime,
    _frame: &mut DetailFrameProjection<'_>,
) {
    let deployment = projection.deployment;
    let sections = configuration_section_sequence(
        !deployment.labels.is_empty(),
        !deployment.annotations.is_empty(),
    );
    for section in sections {
        match section {
            ConfigurationSection::Template => template(ui, deployment),
            ConfigurationSection::LabelsAnnotations => {
                super::overview::metadata_sections(
                    ui,
                    deployment
                        .labels
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                    ": ",
                    deployment
                        .annotations
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_str())),
                );
            }
            ConfigurationSection::Identity => identity_section(ui, identity, deployment, now),
        }
    }
}

fn template(ui: &mut egui::Ui, deployment: &DeploymentProjection) {
    super::overview::section(ui, "TEMPLATE", None);
    // Long values (Image, Selector, template annotations) take the full
    // column as two-line rows; short facts share the fixed-label KV grid.
    let width = ui.available_width().max(120.0);
    if deployment.template_containers.is_empty() {
        super::overview::long_value(ui, width, "Image", None);
    } else {
        for container in &deployment.template_containers {
            super::overview::long_value(
                ui,
                width,
                &format!("Image ({})", container.name),
                container.image.as_deref(),
            );
        }
    }
    super::overview::kv_row(ui, "Replicas", &replicas_summary(deployment));
    if let Some(rolling_update) = rolling_update_summary(deployment) {
        super::overview::kv_row(ui, "Rolling update", &rolling_update);
    }
    // One `key=value` per line: joining a map and middle-eliding it hides
    // every pair but the first, and a one-key selector must read exactly
    // as `app=mcp-kubernetes`.
    super::overview::long_value_list(ui, width, "Selector", &map_pairs(&deployment.selector));
    // Template labels normally repeat the selector, and often the object's
    // own labels; only render what they actually add.
    if !deployment.template_labels.is_empty()
        && deployment.template_labels != deployment.selector
        && deployment.template_labels != deployment.labels
    {
        super::overview::long_value_list(
            ui,
            width,
            "Template labels",
            &map_pairs(&deployment.template_labels),
        );
    }
    if !deployment.template_annotations.is_empty() {
        super::overview::long_value_list(
            ui,
            width,
            "Template annotations",
            &map_pairs(&deployment.template_annotations),
        );
    }
}

/// `3 desired · 3 available`, or `—` when the counts are unknown.
fn replicas_summary(deployment: &DeploymentProjection) -> String {
    match (deployment.desired_replicas, deployment.available_replicas) {
        (Some(desired), Some(available)) => format!("{desired} desired · {available} available"),
        (Some(desired), None) => format!("{desired} desired"),
        (None, Some(available)) => format!("{available} available"),
        (None, None) => "—".into(),
    }
}

/// `surge 25% · unavailable 1`; `None` when neither parameter is known, so
/// the row does not render as a dash.
fn rolling_update_summary(deployment: &DeploymentProjection) -> Option<String> {
    let parts: Vec<String> = [
        deployment
            .max_surge
            .as_deref()
            .map(|surge| format!("surge {surge}")),
        deployment
            .max_unavailable
            .as_deref()
            .map(|unavailable| format!("unavailable {unavailable}")),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!parts.is_empty()).then(|| parts.join(" · "))
}

/// `Helm · release checkout-prod (payments) · chart checkout-1.2.0`, or `—`.
/// Name and namespace are not repeated here: the detail header carries them.
fn managed_by_summary(deployment: &DeploymentProjection) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(manager) = deployment.labels.get("app.kubernetes.io/managed-by") {
        parts.push(manager.clone());
    }
    match (
        deployment.annotations.get("meta.helm.sh/release-name"),
        deployment.annotations.get("meta.helm.sh/release-namespace"),
    ) {
        (Some(release), Some(namespace)) => parts.push(format!("release {release} ({namespace})")),
        (Some(release), None) => parts.push(format!("release {release}")),
        (None, Some(namespace)) => parts.push(format!("release namespace {namespace}")),
        (None, None) => {}
    }
    if let Some(chart) = deployment.labels.get("helm.sh/chart") {
        parts.push(format!("chart {chart}"));
    }
    if parts.is_empty() {
        "—".into()
    } else {
        parts.join(" · ")
    }
}

/// `2026-08-01 08:00 · 32d ago` when the timestamp parses, else the raw value.
fn created_summary(created_at: Option<&str>, now: SystemTime) -> String {
    let Some(created_at) = created_at.filter(|value| !value.is_empty()) else {
        return "—".into();
    };
    let Ok(timestamp) = created_at.parse::<jiff::Timestamp>() else {
        return created_at.to_owned();
    };
    let absolute = timestamp.strftime("%Y-%m-%d %H:%M").to_string();
    let relative = format_age(Some(created_at), now);
    if relative == "—" {
        absolute
    } else {
        format!("{absolute} · {relative} ago")
    }
}

fn identity_section(
    ui: &mut egui::Ui,
    identity: &ResourceIdentity,
    deployment: &DeploymentProjection,
    now: SystemTime,
) {
    super::overview::section(ui, "IDENTITY", None);
    super::overview::kv_row(
        ui,
        "Created",
        &created_summary(deployment.created_at.as_deref(), now),
    );
    // A UID is never read in full: keep the head that identifies it and the
    // tail that separates near-identical ids, with copy and hover for the
    // rest.
    let uid = if identity.uid.is_empty() {
        super::overview::kv_row(ui, "UID", "—")
    } else {
        super::overview::kv_value_row(
            ui,
            "UID",
            super::overview::KvValue::new(&identity.uid)
                .display(super::overview::head_tail_elide(&identity.uid, 8, 4))
                .faint()
                .copyable(),
        )
    };
    if !identity.uid.is_empty() {
        uid.context_menu(|ui| {
            if ui.button("Copy UID").clicked() {
                ui.ctx().copy_text(identity.uid.clone());
                ui.close();
            }
        });
    }
    // Middle elision keeps the chart version, which is the part of a
    // manager summary that actually changes between releases.
    super::overview::kv_value_row(
        ui,
        "Managed by",
        super::overview::KvValue::new(&managed_by_summary(deployment)).faint(),
    );
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

fn map_pairs(values: &std::collections::BTreeMap<String, String>) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod shared_seam_tests {
    use super::super::overview::detail_columns;
    use super::{ConfigurationSection, configuration_section_sequence};

    #[test]
    fn deployment_body_breakpoint_is_exactly_760_points() {
        assert!(detail_columns(759.0, 8.0).is_none());
        assert!(detail_columns(760.0, 8.0).is_some());
    }

    #[test]
    fn configuration_sections_include_only_non_empty_rendered_regions() {
        use ConfigurationSection::{Identity, LabelsAnnotations, Template};

        let cases = [
            (true, true, vec![Template, LabelsAnnotations, Identity]),
            (true, false, vec![Template, LabelsAnnotations, Identity]),
            (false, true, vec![Template, LabelsAnnotations, Identity]),
            (false, false, vec![Template, Identity]),
        ];
        for (labels, annotations, expected) in cases {
            assert_eq!(
                configuration_section_sequence(labels, annotations),
                expected
            );
        }
    }

    #[test]
    fn identity_summaries_collapse_manager_and_creation_facts() {
        use k10s_protocol::DeploymentProjection;
        use std::collections::BTreeMap;

        let mut deployment = DeploymentProjection {
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
        };
        assert_eq!(super::managed_by_summary(&deployment), "—");
        assert_eq!(super::replicas_summary(&deployment), "—");
        assert_eq!(super::rolling_update_summary(&deployment), None);
        deployment.labels = BTreeMap::from([
            ("app.kubernetes.io/managed-by".to_owned(), "Helm".to_owned()),
            ("helm.sh/chart".to_owned(), "checkout-1.2.0".to_owned()),
        ]);
        deployment.annotations = BTreeMap::from([
            (
                "meta.helm.sh/release-name".to_owned(),
                "checkout-prod".to_owned(),
            ),
            (
                "meta.helm.sh/release-namespace".to_owned(),
                "payments".to_owned(),
            ),
        ]);
        deployment.desired_replicas = Some(3);
        deployment.available_replicas = Some(2);
        deployment.max_surge = Some("25%".to_owned());
        assert_eq!(
            super::managed_by_summary(&deployment),
            "Helm · release checkout-prod (payments) · chart checkout-1.2.0"
        );
        assert_eq!(
            super::replicas_summary(&deployment),
            "3 desired · 2 available"
        );
        assert_eq!(
            super::rolling_update_summary(&deployment).as_deref(),
            Some("surge 25%")
        );
        let now = web_time::UNIX_EPOCH + std::time::Duration::from_secs(86_400 * 8);
        assert_eq!(
            super::created_summary(Some("1970-01-01T00:00:00Z"), now),
            "1970-01-01 00:00 · 8d ago"
        );
        assert_eq!(
            super::created_summary(Some("not-a-timestamp"), now),
            "not-a-timestamp"
        );
        assert_eq!(super::created_summary(None, now), "—");
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
                port_forward_capability: false,
                port_forward_list_state: crate::ui::PortForwardListState::Ready,
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
                port_forward_capability: false,
                port_forward_list_state: crate::ui::PortForwardListState::Ready,
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
