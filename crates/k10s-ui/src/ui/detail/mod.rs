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
mod frame;
mod overview;
mod pod;
pub(super) mod presentation;
mod related;
mod service;

use k10s_protocol::{GroupVersionKind, ResourceDetailResponse, WorkloadKind};
use serde::Deserialize;

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
            DetailTab::Ports,
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
            DetailTab::Shell,
        ],
        Some(
            WorkloadKind::Deployment
            | WorkloadKind::ReplicaSet
            | WorkloadKind::StatefulSet
            | WorkloadKind::DaemonSet
            | WorkloadKind::Job
            | WorkloadKind::CronJob,
        ) => &[
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
        DetailTab::Ports => "Ports",
        DetailTab::Pods => "Pods",
        DetailTab::Yaml => "YAML",
        DetailTab::Events => "Events",
        DetailTab::Logs => "Logs",
        DetailTab::Shell => "Shell",
    }
}

fn shortcut_tab(key: egui::Key) -> Option<DetailTab> {
    match key {
        egui::Key::L => Some(DetailTab::Logs),
        egui::Key::P => Some(DetailTab::Pods),
        egui::Key::S => Some(DetailTab::Shell),
        egui::Key::Y => Some(DetailTab::Yaml),
        egui::Key::E => Some(DetailTab::Events),
        _ => None,
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
    if !ui.ctx().memory(|memory| memory.focused().is_some()) {
        let shortcut = ui.input(|input| {
            [
                egui::Key::L,
                egui::Key::P,
                egui::Key::S,
                egui::Key::Y,
                egui::Key::E,
            ]
            .into_iter()
            .find(|key| input.key_pressed(*key))
            .and_then(shortcut_tab)
        });
        if let Some(tab) =
            shortcut.filter(|tab| tabs_for_kind(&detail_identity_gvk(detail)).contains(tab))
        {
            queued.push(WorkspaceCommand::SetActiveTab(window_id, tab));
        }
    }

    let mut body_queued = Vec::new();
    frame::show(
        ui,
        window_id,
        detail,
        presentation,
        integrated,
        detail_maximized,
        tabs_for_kind(&detail_identity_gvk(detail)),
        queued,
        |ui, primary| {
            let view = match primary {
                presentation::DetailPrimary::Loading => {
                    ui.horizontal(|ui| {
                        ui.add(egui::Spinner::new());
                        ui.label("Loading details");
                    });
                    return;
                }
                presentation::DetailPrimary::Failed(error) => {
                    ui.label(format!("Details unavailable: {}", error.message()));
                    if ui.button("Retry details").clicked() {
                        resource_actions.push(crate::ui::ResourceAction::RetryPrimary(
                            presentation.identity.clone(),
                        ));
                    }
                    return;
                }
                presentation::DetailPrimary::Loaded(view) => view,
            };
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
                pod::show(ui);
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
            } else if matches!(detail_identity_gvk(detail).kind.as_str(), "Deployment")
                && detail_identity_gvk(detail).group == "apps"
                && detail_identity_gvk(detail).version == "v1"
            {
                deployment::show(ui);
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
    queued.extend(body_queued);
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
    dialogs: &mut dialogs::OperationDialogs,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.horizontal_wrapped(|ui| {
        if view.capabilities.can_scale {
            let scale = ui.add_enabled(presentation.mutations_allowed, egui::Button::new("Scale"));
            scale.widget_info(|| {
                egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Scale workload")
            });
            if scale.clicked() {
                dialogs.open_scale(
                    window_id,
                    presentation.identity.clone(),
                    status_summary(view).and_then(summary_replicas),
                );
            }
        }
        if view.capabilities.can_restart {
            let restart =
                ui.add_enabled(presentation.mutations_allowed, egui::Button::new("Restart"));
            restart.widget_info(|| {
                egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Restart workload")
            });
        }
        if view.capabilities.can_delete {
            let delete =
                ui.add_enabled(presentation.mutations_allowed, egui::Button::new("Delete"));
            delete.widget_info(|| {
                egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Delete resource")
            });
            if delete.clicked() {
                dialogs.open_delete(window_id, presentation.identity.clone());
            }
        }
        if view.capabilities.can_view_logs {
            action_button(ui, "View logs", "View logs");
        }
        if view.capabilities.can_exec {
            action_button(ui, "Exec shell", "Exec shell");
        }
        if view.capabilities.can_edit_yaml
            && ui
                .add_enabled(
                    presentation.mutations_allowed,
                    egui::Button::new("Edit YAML"),
                )
                .clicked()
        {
            queued.push(WorkspaceCommand::SetActiveTab(window_id, DetailTab::Yaml));
        }
    });
    if !presentation.mutations_allowed {
        ui.label("Scale, delete, and YAML edits are disabled until this window is live");
    }
    ui.separator();
    match detail.active_tab {
        DetailTab::Overview => overview::show(ui, window_id, &view.sections),
        DetailTab::Pods => related::show(
            ui,
            presentation.identity,
            presentation.relations,
            resource_actions,
            queued,
        ),
        DetailTab::Events => events::show(ui, view.events_condition, &view.events),
        DetailTab::Ports => {}
        DetailTab::Yaml => {
            if !view.capabilities.can_edit_yaml {
                ui.label("This kind cannot be edited");
            } else {
                tools::yaml::show(
                    ui,
                    window_id,
                    yaml,
                    Some(presentation.identity),
                    Some(view.manifest.as_str()),
                    presentation.mutations_allowed,
                    queued,
                );
            }
        }
        DetailTab::Logs => {
            let containers = pod_containers(&view.manifest);
            tools::logs::show(
                ui,
                window_id,
                &mut streams.logs,
                stream_target(detail, view),
                &containers,
                status_summary(view).is_some_and(|status| status.contains("CrashLoopBackOff")),
            );
        }
        DetailTab::Shell => tools::shell::show(
            ui,
            window_id,
            &mut streams.shells,
            stream_target(detail, view),
        ),
    }
}

/// The default pod container streamed by the connected tools.
const DEFAULT_CONTAINER: &str = "app";

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
fn stream_target<I>(
    detail: &DetailState<I>,
    view: &ResourceDetailResponse,
) -> Option<k10s_protocol::StreamTarget>
where
    I: RowIdentity,
{
    let identity = detail.identity.as_row_identity()?;
    Some(k10s_protocol::StreamTarget {
        context: identity.context.clone(),
        namespace: identity
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_owned()),
        pod: identity.name.clone(),
        uid: identity.uid.clone(),
        container: pod_container(&view.manifest).unwrap_or_else(|| DEFAULT_CONTAINER.to_owned()),
    })
}

/// Resolve the default exec/logs container from the authoritative Pod
/// manifest. Kubernetes does not prescribe an `app` container name, so the
/// first regular container is the only generally valid implicit selection.
pub(crate) fn pod_container(manifest: &str) -> Option<String> {
    pod_containers(manifest).into_iter().next()
}

pub(crate) fn pod_containers(manifest: &str) -> Vec<String> {
    #[derive(Deserialize)]
    struct Manifest {
        spec: Spec,
    }

    #[derive(Deserialize)]
    struct Spec {
        containers: Vec<Container>,
    }

    #[derive(Deserialize)]
    struct Container {
        name: String,
    }

    serde_yaml::from_str::<Manifest>(manifest)
        .map(|manifest| {
            manifest
                .spec
                .containers
                .into_iter()
                .map(|container| container.name)
                .filter(|name| !name.is_empty())
                .collect()
        })
        .unwrap_or_default()
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

fn action_button(ui: &mut egui::Ui, label: &str, accessible: &str) {
    let button = ui.button(label);
    button.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, true, accessible.to_owned())
    });
}

#[cfg(test)]
mod tests {
    use super::{pod_container, shortcut_tab};
    use crate::workspace::DetailTab;

    #[test]
    fn detail_shortcuts_map_to_investigation_tabs() {
        assert_eq!(shortcut_tab(egui::Key::L), Some(DetailTab::Logs));
        assert_eq!(shortcut_tab(egui::Key::P), Some(DetailTab::Pods));
        assert_eq!(shortcut_tab(egui::Key::S), Some(DetailTab::Shell));
        assert_eq!(shortcut_tab(egui::Key::Y), Some(DetailTab::Yaml));
        assert_eq!(shortcut_tab(egui::Key::E), Some(DetailTab::Events));
        assert_eq!(shortcut_tab(egui::Key::Enter), None);
    }

    #[test]
    fn pod_container_reads_first_regular_container_from_yaml_manifest() {
        let manifest = r#"
spec:
  initContainers:
    - name: setup
  containers:
    - name: postgres
    - name: metrics
"#;

        assert_eq!(pod_container(manifest).as_deref(), Some("postgres"));
    }

    #[test]
    fn pod_container_rejects_missing_or_empty_container_names() {
        assert_eq!(pod_container("spec:\n  containers: []\n"), None);
        assert_eq!(
            pod_container("spec:\n  containers:\n    - name: ''\n"),
            None
        );
    }
}
