//! Kind-specific detail views rendered identically in the integrated pane
//! and in dedicated pinned windows.
//!
//! Every byte comes from protocol payloads: identity header from
//! [`ResourceIdentity`], sections/events/related rows from the backend-
//! resolved [`ResourceDetailResponse`]. Tabs and actions are chosen by the
//! object's kind; the active tab lives in the workspace's [`DetailState`],
//! so two views of any resource stay independent.

mod deployment;
mod events;
pub(super) mod frame;
mod overview;
mod pod;
pub(super) mod presentation;
mod related;
mod service;

pub(crate) use pod::PodRuntimeProjection;

use k10s_protocol::{GroupVersionKind, ResourceDetailResponse, WorkloadKind};

use crate::ui::dialogs;
use crate::ui::tools;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

/// Exact tab set per kind. Pods expose runtime tools, controllers expose
/// their resolved related workloads, Services expose their structured
/// ports, everything else keeps the common core.
#[must_use]
pub fn tabs_for_kind(gvk: &GroupVersionKind) -> &'static [DetailTab] {
    if is_service_gvk(gvk) {
        return &[
            DetailTab::Overview,
            DetailTab::Endpoints,
            DetailTab::Pods,
            DetailTab::Events,
            DetailTab::Yaml,
        ];
    }
    match WorkloadKind::from_gvk(gvk) {
        Some(WorkloadKind::Pod) => &[
            DetailTab::Overview,
            DetailTab::Events,
            DetailTab::Yaml,
            DetailTab::Logs,
        ],
        Some(WorkloadKind::Deployment | WorkloadKind::ReplicaSet | WorkloadKind::StatefulSet) => &[
            DetailTab::Overview,
            DetailTab::Pods,
            DetailTab::Events,
            DetailTab::Yaml,
            DetailTab::Logs,
        ],
        Some(WorkloadKind::DaemonSet | WorkloadKind::Job | WorkloadKind::CronJob) => &[
            DetailTab::Overview,
            DetailTab::Pods,
            DetailTab::Events,
            DetailTab::Yaml,
        ],
        None => &[DetailTab::Overview, DetailTab::Events, DetailTab::Yaml],
    }
}

fn tab_label(tab: DetailTab) -> &'static str {
    match tab {
        DetailTab::Overview => "Overview",
        DetailTab::Endpoints => "Endpoints",
        DetailTab::Pods => "Pods",
        DetailTab::Yaml => "YAML",
        DetailTab::Events => "Events",
        DetailTab::Logs => "Logs",
    }
}

fn shortcut_tab(key: egui::Key) -> Option<DetailTab> {
    match key {
        egui::Key::L => Some(DetailTab::Logs),
        egui::Key::P => Some(DetailTab::Pods),
        egui::Key::Y => Some(DetailTab::Yaml),
        egui::Key::E => Some(DetailTab::Events),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailShortcut {
    Tab(DetailTab),
    CopyName,
    OpenOwner,
    /// `Ctrl+D`: open the delete confirmation dialog.
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DetailRuntimeAction {
    PreviousLogs { window: WindowId, container: String },
}

/// Whether this detail may advertise and honor `Ctrl+D`: the backend
/// reports the resource as deletable and the window is live.
pub(super) fn delete_shortcut_available(
    presentation: &presentation::DetailPresentationInput<'_>,
) -> bool {
    presentation.mutations_allowed
        && matches!(
            presentation.primary,
            presentation::DetailPrimary::Loaded(view) if view.capabilities.can_delete
        )
}

fn shortcut_for_key(
    key: egui::Key,
    tabs: &[DetailTab],
    has_verified_owner: bool,
) -> Option<DetailShortcut> {
    match key {
        egui::Key::C => Some(DetailShortcut::CopyName),
        egui::Key::O if has_verified_owner => Some(DetailShortcut::OpenOwner),
        _ => shortcut_tab(key)
            .filter(|tab| tabs.contains(tab))
            .map(DetailShortcut::Tab),
    }
}

fn shortcut_labels_for(
    gvk: &GroupVersionKind,
    has_verified_owner: bool,
) -> &'static [&'static str] {
    const POD: &[&str] = &["l logs", "y yaml", "e events", "c copy name"];
    const POD_OWNER: &[&str] = &["l logs", "y yaml", "e events", "c copy name", "o owner"];
    const CONTROLLER: &[&str] = &["p pods", "l logs", "y yaml", "e events", "c copy name"];
    const CONTROLLER_OWNER: &[&str] = &[
        "p pods",
        "l logs",
        "y yaml",
        "e events",
        "c copy name",
        "o owner",
    ];
    const GENERIC: &[&str] = &["y yaml", "e events", "c copy name"];
    const GENERIC_OWNER: &[&str] = &["y yaml", "e events", "c copy name", "o owner"];

    let tabs = tabs_for_kind(gvk);
    if is_service_gvk(gvk) {
        if has_verified_owner {
            GENERIC_OWNER
        } else {
            GENERIC
        }
    } else if tabs.contains(&DetailTab::Logs)
        && WorkloadKind::from_gvk(gvk) == Some(WorkloadKind::Pod)
    {
        if has_verified_owner { POD_OWNER } else { POD }
    } else if tabs.contains(&DetailTab::Pods) {
        if has_verified_owner {
            CONTROLLER_OWNER
        } else {
            CONTROLLER
        }
    } else if has_verified_owner {
        GENERIC_OWNER
    } else {
        GENERIC
    }
}

/// Whether this GVK is exactly core/v1 `Service`.
pub(super) fn is_service_gvk(gvk: &GroupVersionKind) -> bool {
    gvk.group.is_empty() && gvk.version == "v1" && gvk.kind == "Service"
}

/// Render one detail view bound to the stable identity inside `detail`.
///
/// `view` is the backend-resolved response for that identity, when it has
/// arrived yet; until then the header still renders from the pinned
/// identity and the content area shows a loading state. All interactions
/// are queued as workspace commands or stream actions.
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    presentation: &presentation::DetailPresentationInput<'_>,
    shortcut_owner: bool,
    integrated: bool,
    detail_maximized: bool,
    yaml: &mut tools::YamlEditors,
    streams: &mut tools::StreamStores,
    dialogs: &mut dialogs::OperationDialogs,
    service_port_drafts: Option<&std::collections::BTreeMap<String, String>>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    if detail.identity.as_row_identity().is_none() {
        return;
    }
    if shortcut_owner && !ui.ctx().memory(|memory| memory.focused().is_some()) {
        let tabs = tabs_for_kind(&detail_identity_gvk(detail));
        let verified_owner = presentation.verified_owner();
        let shortcut = ui.input(|input| {
            // `Ctrl+D` only opens the delete confirmation dialog, so the
            // keyboard can never destroy a resource on its own.
            if delete_shortcut_available(presentation)
                && input.modifiers.command_only()
                && input.key_pressed(egui::Key::D)
            {
                return Some(DetailShortcut::Delete);
            }
            [
                egui::Key::L,
                egui::Key::P,
                egui::Key::Y,
                egui::Key::E,
                egui::Key::C,
                egui::Key::O,
            ]
            .into_iter()
            .find(|key| input.key_pressed(*key))
            .and_then(|key| shortcut_for_key(key, tabs, verified_owner.is_some()))
        });
        match shortcut {
            Some(DetailShortcut::Tab(tab)) => {
                queued.push(WorkspaceCommand::SetActiveTab(window_id, tab));
            }
            Some(DetailShortcut::CopyName) => {
                ui.ctx().copy_text(presentation.identity.name.clone());
            }
            Some(DetailShortcut::Delete) => {
                dialogs.open_delete(window_id, presentation.identity.clone());
            }
            Some(DetailShortcut::OpenOwner) => {
                if let Some(owner) = verified_owner {
                    queued.push(WorkspaceCommand::OpenDedicatedDetail(I::from_row_identity(
                        &presentation::owner_identity(presentation.identity, owner),
                    )));
                }
            }
            None => {}
        }
    }

    let mut body_queued = Vec::new();
    let mut runtime_actions = Vec::new();
    let shell_container_count = external_shell_container_count(ui, presentation);
    frame::show(
        ui,
        window_id,
        detail,
        presentation,
        integrated,
        detail_maximized,
        tabs_for_kind(&detail_identity_gvk(detail)),
        queued,
        |frame| {
            let gvk = detail_identity_gvk(detail);
            if gvk.group.is_empty() && gvk.version == "v1" && gvk.kind == "Pod" {
                pod::configure_frame(presentation, frame);
                frame.actions.shell_container_count = shell_container_count;
            } else if gvk.group == "apps" && gvk.version == "v1" && gvk.kind == "Deployment" {
                deployment::configure_frame(presentation, frame);
            } else if is_service_gvk(&gvk) {
                service::configure_frame(presentation, frame, detail.active_tab);
            }
        },
        |ui, primary, actions, frame| {
            let view = match primary {
                presentation::DetailPrimary::Loading => {
                    if actions.is_none() {
                        ui.horizontal(|ui| {
                            ui.add(egui::Spinner::new());
                            ui.label("Loading details");
                        });
                        if is_service_gvk(&detail_identity_gvk(detail)) {
                            service::show_unavailable(ui, window_id, presentation);
                        }
                    }
                    return;
                }
                presentation::DetailPrimary::Failed(error) => {
                    if actions.is_none() {
                        ui.label(format!("Details unavailable: {}", error.message()));
                    }
                    if actions.is_none() && ui.button("Retry details").clicked() {
                        resource_actions.push(crate::ui::ResourceAction::RetryPrimary(
                            presentation.identity.clone(),
                        ));
                    }
                    if actions.is_none() && is_service_gvk(&detail_identity_gvk(detail)) {
                        service::show_unavailable(ui, window_id, presentation);
                    }
                    return;
                }
                presentation::DetailPrimary::Loaded(view) => view,
            };
            if let Some(segment) = actions {
                if presentation.gone {
                    return;
                }
                if is_service_gvk(&detail_identity_gvk(detail)) {
                    match segment {
                        frame::DetailActionSegment::Delete => {
                            show_delete_action(ui, window_id, presentation, frame, view, dialogs)
                        }
                        frame::DetailActionSegment::PortForward => {
                            let btn = ui.button("Port-forward…");
                            if btn.clicked() && detail.active_tab != DetailTab::Endpoints {
                                body_queued.push(WorkspaceCommand::SetActiveTab(
                                    window_id,
                                    DetailTab::Endpoints,
                                ));
                            }
                        }
                        _ => {}
                    }
                } else {
                    match segment {
                        frame::DetailActionSegment::Delete => {
                            show_delete_action(ui, window_id, presentation, frame, view, dialogs)
                        }
                        frame::DetailActionSegment::Restart => show_restart_action(
                            ui,
                            window_id,
                            presentation,
                            frame,
                            resource_actions,
                        ),
                        frame::DetailActionSegment::Scale => {
                            show_scale_action(ui, window_id, presentation, frame, view, dialogs)
                        }
                        frame::DetailActionSegment::Shell => show_external_shell_action(
                            ui,
                            window_id,
                            presentation,
                            resource_actions,
                        ),
                        frame::DetailActionSegment::PortForward => {}
                    }
                }
                return;
            }
            if is_service_gvk(&detail_identity_gvk(detail)) {
                service::show(
                    ui,
                    window_id,
                    detail,
                    view,
                    presentation,
                    yaml,
                    service_port_drafts,
                    resource_actions,
                    &mut body_queued,
                );
            } else if matches!(detail_identity_gvk(detail).kind.as_str(), "Pod")
                && detail_identity_gvk(detail).group.is_empty()
                && detail_identity_gvk(detail).version == "v1"
            {
                if detail.active_tab == DetailTab::Overview {
                    pod::show(
                        ui,
                        window_id,
                        detail,
                        presentation,
                        frame,
                        &mut runtime_actions,
                        &mut body_queued,
                    );
                } else {
                    show_generic_body(
                        ui,
                        window_id,
                        detail,
                        presentation,
                        view,
                        yaml,
                        streams,
                        dialogs,
                        resource_actions,
                        &mut body_queued,
                    );
                }
            } else if matches!(detail_identity_gvk(detail).kind.as_str(), "Deployment")
                && detail_identity_gvk(detail).group == "apps"
                && detail_identity_gvk(detail).version == "v1"
            {
                if detail.active_tab == DetailTab::Overview {
                    deployment::show(
                        ui,
                        window_id,
                        detail,
                        presentation,
                        frame,
                        resource_actions,
                        &mut body_queued,
                    );
                } else {
                    show_generic_body(
                        ui,
                        window_id,
                        detail,
                        presentation,
                        view,
                        yaml,
                        streams,
                        dialogs,
                        resource_actions,
                        &mut body_queued,
                    );
                }
            } else {
                show_generic_body(
                    ui,
                    window_id,
                    detail,
                    presentation,
                    view,
                    yaml,
                    streams,
                    dialogs,
                    resource_actions,
                    &mut body_queued,
                );
            }
        },
    );
    for action in runtime_actions {
        match action {
            DetailRuntimeAction::PreviousLogs { window, container } => {
                if let presentation::DetailPrimary::Loaded(view) = presentation.primary
                    && let Some(runtime) =
                        pod::PodRuntimeProjection::from_view(presentation.identity, view)
                    && runtime.contains(&container)
                    && let Some(target) = stream_target(detail, &container)
                {
                    let logs = streams.logs.ensure(window, target);
                    logs.select_container(&container);
                    logs.set_previous(true);
                }
            }
        }
    }
    queued.extend(body_queued);
}

fn show_external_shell_action(
    ui: &mut egui::Ui,
    window: WindowId,
    presentation: &presentation::DetailPresentationInput<'_>,
    actions: &mut Vec<crate::ui::ResourceAction>,
) {
    let availability = ui.ctx().data(|data| {
        data.get_temp::<crate::ui::ExternalShellAvailability>(egui::Id::new(
            "k10s.external-shell-availability",
        ))
    });
    if !presentation.mutations_allowed {
        return;
    }
    let identity = presentation.identity;
    let presentation::DetailPrimary::Loaded(view) = presentation.primary else {
        return;
    };
    let Some(runtime) = pod::PodRuntimeProjection::from_view(identity, view) else {
        return;
    };
    let availability = availability.unwrap_or_default();
    let containers = runtime.containers();
    let Some(first) = containers.first() else {
        return;
    };
    if build_external_shell_target(availability, identity, containers, Some(first)).is_none() {
        return;
    }

    let mut open = |container: &str| {
        let Some(target) =
            build_external_shell_target(availability, identity, containers, Some(container))
        else {
            return;
        };
        actions.push(crate::ui::ResourceAction::OpenExternalShell { window, target });
    };
    if containers.len() == 1 {
        let button = ui
            .button("Open shell")
            .on_hover_text("Open an interactive kubectl shell in your system terminal");
        if button.clicked() {
            open(first);
        }
    } else {
        let menu = ui.menu_button("Open shell", |ui| {
            for container in containers {
                let button = ui.button(container);
                button.widget_info(|| {
                    egui::WidgetInfo::labeled(
                        egui::WidgetType::Button,
                        true,
                        format!("Open shell: {container}"),
                    )
                });
                if button.clicked() {
                    open(container);
                    ui.close();
                }
            }
        });
        menu.response.widget_info(|| {
            egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Open shell")
        });
    }
}

fn external_shell_container_count(
    ui: &egui::Ui,
    presentation: &presentation::DetailPresentationInput<'_>,
) -> usize {
    if !presentation.mutations_allowed {
        return 0;
    }
    let availability = ui.ctx().data(|data| {
        data.get_temp::<crate::ui::ExternalShellAvailability>(egui::Id::new(
            "k10s.external-shell-availability",
        ))
    });
    let presentation::DetailPrimary::Loaded(view) = presentation.primary else {
        return 0;
    };
    let Some(runtime) = pod::PodRuntimeProjection::from_view(presentation.identity, view) else {
        return 0;
    };
    usize::from(
        build_external_shell_target(
            availability.unwrap_or_default(),
            presentation.identity,
            runtime.containers(),
            runtime.containers().first().map(String::as_str),
        )
        .is_some(),
    ) * runtime.containers().len()
}

fn build_external_shell_target(
    availability: crate::ui::ExternalShellAvailability,
    identity: &k10s_protocol::ResourceIdentity,
    containers: &[String],
    selected: Option<&str>,
) -> Option<crate::ui::ExternalShellTarget> {
    let crate::ui::ExternalShellAvailability::Available { generation } = availability else {
        return None;
    };
    if !identity.gvk.group.is_empty()
        || identity.gvk.version != "v1"
        || identity.gvk.kind != "Pod"
        || identity.namespace.as_deref().is_none_or(str::is_empty)
        || identity.name.is_empty()
        || identity.uid.is_empty()
    {
        return None;
    }
    let container = selected
        .filter(|selected| containers.iter().any(|container| container == selected))
        .or_else(|| containers.first().map(String::as_str))?;
    if container.is_empty() {
        return None;
    }
    Some(crate::ui::ExternalShellTarget {
        generation,
        namespace: identity.namespace.clone()?,
        pod: identity.name.clone(),
        uid: identity.uid.clone(),
        container: container.to_owned(),
        program: "/bin/sh".to_owned(),
    })
}

#[allow(clippy::too_many_arguments)]
fn show_generic_body<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    presentation: &presentation::DetailPresentationInput<'_>,
    view: &ResourceDetailResponse,
    yaml: &mut tools::YamlEditors,
    streams: &mut tools::StreamStores,
    _dialogs: &mut dialogs::OperationDialogs,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    match detail.active_tab {
        DetailTab::Overview => overview::show(
            ui,
            window_id,
            &view.sections,
            presentation.identity,
            presentation.metrics,
        ),
        DetailTab::Pods => related::show(
            ui,
            presentation.identity,
            presentation.relations,
            resource_actions,
            queued,
        ),
        DetailTab::Events => events::show(ui, view.events_condition, &view.events),
        DetailTab::Endpoints => {}
        DetailTab::Yaml => {
            let mutations_allowed =
                presentation.mutations_allowed && view.capabilities.can_edit_yaml;
            tools::yaml::show(
                ui,
                window_id,
                yaml,
                Some(presentation.identity),
                Some(view.manifest.as_str()),
                mutations_allowed,
                queued,
            );
        }
        DetailTab::Logs => {
            if WorkloadKind::from_gvk(&presentation.identity.gvk) == Some(WorkloadKind::Pod) {
                let Some(runtime) =
                    pod::PodRuntimeProjection::from_view(presentation.identity, view)
                else {
                    ui.label("Pod runtime details unavailable");
                    return;
                };
                tools::logs::show(
                    ui,
                    window_id,
                    &mut streams.logs,
                    stream_target(detail, runtime.default_container()),
                    runtime.containers(),
                    runtime.default_previous(),
                );
            } else {
                let targets = aggregate_log_targets(presentation.identity, presentation.relations);
                tools::logs::show_aggregate(ui, window_id, &mut streams.logs, &targets);
            }
        }
    }
}

fn aggregate_log_targets(
    owner: &k10s_protocol::ResourceIdentity,
    relations: Option<&crate::ui::RelationState>,
) -> Vec<k10s_protocol::StreamTarget> {
    let Some(crate::ui::RelationState::Loaded { response, .. }) = relations else {
        return Vec::new();
    };
    let mut targets = response
        .groups
        .iter()
        .filter(|group| {
            group.gvk.group.is_empty() && group.gvk.version == "v1" && group.gvk.kind == "Pod"
        })
        .flat_map(|group| &group.rows)
        .flat_map(|row| {
            let containers = match &row.projection {
                Some(k10s_protocol::ResourceProjection::Pod(pod)) => pod
                    .containers
                    .iter()
                    .map(|container| container.name.as_str())
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            };
            containers
                .into_iter()
                .map(move |container| k10s_protocol::StreamTarget {
                    context: owner.context.clone(),
                    namespace: row
                        .identity
                        .namespace
                        .clone()
                        .unwrap_or_else(|| "default".to_owned()),
                    pod: row.identity.name.clone(),
                    uid: row.identity.uid.clone(),
                    container: container.to_owned(),
                })
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        (&left.namespace, &left.pod, &left.container).cmp(&(
            &right.namespace,
            &right.pod,
            &right.container,
        ))
    });
    targets
}

/// The destructive `Delete…` button, rendered rightmost in the reference
/// action row and styled as a danger control.
fn show_delete_action(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &presentation::DetailPresentationInput<'_>,
    frame: &presentation::DetailFrameProjection<'_>,
    _view: &ResourceDetailResponse,
    dialogs: &mut dialogs::OperationDialogs,
) {
    if frame.actions.can_delete {
        let danger = ui.add_enabled(
            presentation.mutations_allowed,
            egui::Button::new(egui::RichText::new("Delete…").color(crate::ui::theme::DANGER))
                .fill(egui::Color32::from_rgb(48, 28, 28))
                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(125, 65, 65))),
        );
        danger.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Delete…"));
        if danger.clicked() {
            dialogs.open_delete(window_id, presentation.identity.clone());
        }
    }
}

fn show_restart_action(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &presentation::DetailPresentationInput<'_>,
    frame: &presentation::DetailFrameProjection<'_>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
) {
    if frame.actions.can_restart {
        let restart = ui.add_enabled(
            presentation.mutations_allowed,
            egui::Button::new("Restart…"),
        );
        restart
            .widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Restart…"));
        if restart.clicked() {
            resource_actions.push(crate::ui::ResourceAction::Restart {
                window: window_id,
                target: presentation.identity.clone(),
            });
        }
    }
}

fn show_scale_action(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &presentation::DetailPresentationInput<'_>,
    frame: &presentation::DetailFrameProjection<'_>,
    view: &ResourceDetailResponse,
    dialogs: &mut dialogs::OperationDialogs,
) {
    if frame.actions.can_scale {
        let scale = ui.add_enabled(presentation.mutations_allowed, egui::Button::new("Scale…"));
        scale.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Scale…"));
        if scale.clicked() {
            dialogs.open_scale(
                window_id,
                presentation.identity.clone(),
                suggested_replicas(view),
            );
        }
    }
}

fn suggested_replicas(view: &ResourceDetailResponse) -> Option<u32> {
    match view.projection.as_ref() {
        Some(k10s_protocol::ResourceProjection::Deployment(deployment)) => {
            deployment.desired_replicas
        }
        Some(_) => None,
        None => status_summary(view).and_then(summary_replicas),
    }
}

/// Best-effort current desired replica count from a status summary such as
/// `20/20 ready`, used as the pre-filled value of the scale dialog.
fn summary_replicas(summary: &str) -> Option<u32> {
    let (desired, _) = summary.split_once('/')?;
    desired.trim().parse::<u32>().ok()
}

/// The backend-asserted status summary of a detail response, if present.
fn status_summary(view: &ResourceDetailResponse) -> Option<&str> {
    view.sections
        .iter()
        .find(|section| section.title == "Overview")
        .and_then(|section| {
            section
                .rows
                .iter()
                .find(|row| row.label == "Status")
                .map(|row| row.value.as_str())
        })
}

/// Resolve a pod/container stream target from the pinned identity. Only pod
/// identities can stream; anything else yields no target.
fn stream_target<I>(detail: &DetailState<I>, container: &str) -> Option<k10s_protocol::StreamTarget>
where
    I: RowIdentity,
{
    let identity = detail.identity.as_row_identity()?;
    if !identity.gvk.group.is_empty()
        || identity.gvk.version != "v1"
        || identity.gvk.kind != "Pod"
        || container.is_empty()
    {
        return None;
    }
    Some(k10s_protocol::StreamTarget {
        context: identity.context.clone(),
        namespace: identity
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_owned()),
        pod: identity.name.clone(),
        uid: identity.uid.clone(),
        container: container.to_owned(),
    })
}

fn detail_identity_gvk<I>(detail: &DetailState<I>) -> GroupVersionKind
where
    I: RowIdentity,
{
    detail.identity.as_row_identity().map_or_else(
        || GroupVersionKind {
            group: String::new(),
            version: String::new(),
            kind: String::new(),
        },
        |identity| identity.gvk.clone(),
    )
}

#[cfg(test)]
mod external_shell_tests {
    use super::build_external_shell_target;
    use crate::ui::ExternalShellAvailability;
    use k10s_protocol::{GroupVersionKind, ResourceIdentity};

    fn pod() -> ResourceIdentity {
        ResourceIdentity {
            context: "dev".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "api".into(),
            uid: "uid-api".into(),
        }
    }

    #[test]
    fn external_shell_target_requires_exact_complete_pod_and_container() {
        let available = ExternalShellAvailability::Available { generation: 7 };
        let containers = vec!["app".to_owned(), "metrics".to_owned()];
        let expected = build_external_shell_target(available, &pod(), &containers, Some("metrics"))
            .expect("exact target");
        assert_eq!(expected.container, "metrics");
        assert_eq!(expected.generation, 7);
        assert_eq!(expected.namespace, "default");
        assert_eq!(expected.pod, "api");
        assert_eq!(expected.uid, "uid-api");
        assert_eq!(expected.program, "/bin/sh");

        assert_eq!(
            build_external_shell_target(available, &pod(), &containers, None)
                .unwrap()
                .container,
            "app"
        );
        assert!(
            build_external_shell_target(
                ExternalShellAvailability::Unavailable,
                &pod(),
                &containers,
                None
            )
            .is_none()
        );
        let mut invalid = pod();
        invalid.gvk.group = "apps".into();
        assert!(build_external_shell_target(available, &invalid, &containers, None).is_none());
        invalid = pod();
        invalid.gvk.version = "v1beta1".into();
        assert!(build_external_shell_target(available, &invalid, &containers, None).is_none());
        invalid = pod();
        invalid.namespace = None;
        assert!(build_external_shell_target(available, &invalid, &containers, None).is_none());
        invalid = pod();
        invalid.name.clear();
        assert!(build_external_shell_target(available, &invalid, &containers, None).is_none());
        invalid = pod();
        invalid.uid.clear();
        assert!(build_external_shell_target(available, &invalid, &containers, None).is_none());
        assert!(build_external_shell_target(available, &pod(), &[], None).is_none());
        assert_eq!(
            build_external_shell_target(available, &pod(), &containers, Some("missing"))
                .unwrap()
                .container,
            "app"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{DetailShortcut, shortcut_for_key, shortcut_tab, tabs_for_kind};
    use crate::workspace::DetailTab;
    use k10s_protocol::GroupVersionKind;

    fn apps(kind: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: kind.into(),
        }
    }

    #[test]
    fn aggregate_logs_are_limited_to_requested_controller_kinds() {
        for kind in ["Deployment", "ReplicaSet", "StatefulSet"] {
            assert!(
                tabs_for_kind(&apps(kind)).contains(&DetailTab::Logs),
                "{kind}"
            );
        }
        assert!(!tabs_for_kind(&apps("DaemonSet")).contains(&DetailTab::Logs));
    }

    #[test]
    fn detail_shortcuts_map_to_investigation_tabs() {
        assert_eq!(shortcut_tab(egui::Key::L), Some(DetailTab::Logs));
        assert_eq!(shortcut_tab(egui::Key::P), Some(DetailTab::Pods));
        assert_eq!(shortcut_tab(egui::Key::S), None);
        assert_eq!(shortcut_tab(egui::Key::Y), Some(DetailTab::Yaml));
        assert_eq!(shortcut_tab(egui::Key::E), Some(DetailTab::Events));
        assert_eq!(shortcut_tab(egui::Key::Enter), None);

        let pod_tabs = [
            DetailTab::Overview,
            DetailTab::Events,
            DetailTab::Yaml,
            DetailTab::Logs,
        ];
        assert_eq!(
            shortcut_for_key(egui::Key::C, &pod_tabs, false),
            Some(DetailShortcut::CopyName)
        );
        assert_eq!(shortcut_for_key(egui::Key::P, &pod_tabs, false), None);
        assert_eq!(shortcut_for_key(egui::Key::O, &pod_tabs, false), None);
        assert_eq!(
            shortcut_for_key(egui::Key::O, &pod_tabs, true),
            Some(DetailShortcut::OpenOwner)
        );
    }
}
