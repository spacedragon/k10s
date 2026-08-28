//! Kind-specific detail views rendered identically in the integrated pane
//! and in dedicated pinned windows.
//!
//! Every byte comes from protocol payloads: identity header from
//! [`ResourceIdentity`], sections/events/related rows from the backend-
//! resolved [`ResourceDetailResponse`]. Tabs and actions are chosen by the
//! object's kind; the active tab lives in the workspace's [`DetailState`],
//! so two views of any resource stay independent.

mod events;
mod overview;
mod related;
mod service;

use egui::{RichText, Spinner};
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
    primary_state: Option<&crate::ui::PrimaryDetailState>,
    view: Option<&ResourceDetailResponse>,
    gone: bool,
    integrated: bool,
    detail_maximized: bool,
    yaml: &mut tools::YamlEditors,
    streams: &mut tools::StreamStores,
    dialogs: &mut dialogs::OperationDialogs,
    feed: &crate::ui::ResourceFeed,
    service_port_drafts: Option<&std::collections::BTreeMap<String, String>>,
    mutations_allowed: bool,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    // Services render through their dedicated read-only body; the generic
    // workload path stays untouched for every other kind.
    if is_service_gvk(&detail_identity_gvk(detail)) {
        service::show(
            ui,
            window_id,
            detail,
            primary_state,
            view,
            gone,
            yaml,
            feed,
            service_port_drafts,
            mutations_allowed,
            resource_actions,
            queued,
        );
        return;
    }

    // A gone resource renders only its pinned identity header plus the
    // gone message: no cached response may resurrect Scale/Delete/YAML or
    // stream controls for an object the authoritative rows dropped.
    if gone && let Some(identity) = detail.identity.as_row_identity() {
        show_header(ui, identity, None);
        ui.label(RichText::new("This resource no longer exists").color(crate::ui::theme::WARNING));
        return;
    }

    let Some(identity) = detail.identity.as_row_identity() else {
        return;
    };
    ui.horizontal(|ui| {
        ui.heading(RichText::new("Details").strong());
        show_header(ui, identity, view);
        if integrated {
            if ui.button("Pop out ↗").clicked() {
                queued.push(WorkspaceCommand::OpenDedicatedDetail(
                    detail.identity.clone(),
                ));
            }
            let maximize_label = if detail_maximized {
                "Restore split"
            } else {
                "Maximize"
            };
            if ui.button(maximize_label).clicked() {
                queued.push(if detail_maximized {
                    WorkspaceCommand::RestoreDetailPane(window_id)
                } else {
                    WorkspaceCommand::MaximizeDetailPane(window_id)
                });
            }
        }
    });
    ui.horizontal_wrapped(|ui| {
        for tab in tabs_for_kind(&detail_identity_gvk(detail)) {
            let active = *tab == detail.active_tab;
            let label = if active {
                RichText::new(tab_label(*tab)).strong()
            } else {
                RichText::new(tab_label(*tab))
            };
            let button = ui.button(label);
            button.widget_info(|| {
                egui::WidgetInfo::labeled(
                    egui::WidgetType::Button,
                    true,
                    format!("Tab {}", tab_label(*tab)),
                )
            });
            if button.clicked() && !active {
                queued.push(WorkspaceCommand::SetActiveTab(window_id, *tab));
            }
        }
    });
    ui.separator();

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

    let view = match primary_state {
        Some(crate::ui::PrimaryDetailState::Loaded(view)) => Some(view),
        Some(crate::ui::PrimaryDetailState::Failed(error)) => {
            ui.label(format!("Details unavailable: {}", error.message()));
            if ui.button("Retry details").clicked() {
                resource_actions.push(crate::ui::ResourceAction::RetryPrimary(identity.clone()));
            }
            None
        }
        Some(crate::ui::PrimaryDetailState::Loading) => None,
        None => view,
    };
    let Some(view) = view else {
        if !matches!(
            primary_state,
            Some(crate::ui::PrimaryDetailState::Failed(_))
        ) {
            ui.horizontal(|ui| {
                ui.add(Spinner::new());
                ui.label("Loading details");
            });
        }
        return;
    };

    ui.horizontal(|ui| {
        let capabilities = view.capabilities;
        let identity = detail.identity.as_row_identity();
        if capabilities.can_scale {
            let scale = ui.add_enabled(mutations_allowed, egui::Button::new("Scale"));
            scale.widget_info(|| {
                egui::WidgetInfo::labeled(
                    egui::WidgetType::Button,
                    true,
                    "Scale workload".to_owned(),
                )
            });
            if scale.clicked()
                && let Some(identity) = identity
            {
                dialogs.open_scale(
                    window_id,
                    identity.clone(),
                    status_summary(view).and_then(summary_replicas),
                );
            }
        }
        if capabilities.can_delete && identity.is_some() {
            let delete = ui.add_enabled(mutations_allowed, egui::Button::new("Delete"));
            delete.widget_info(|| {
                egui::WidgetInfo::labeled(
                    egui::WidgetType::Button,
                    true,
                    "Delete resource".to_owned(),
                )
            });
            if delete.clicked()
                && let Some(identity) = identity
            {
                dialogs.open_delete(window_id, identity.clone());
            }
        }
        if capabilities.can_view_logs {
            action_button(ui, "View logs", "View logs");
        }
        if capabilities.can_exec {
            action_button(ui, "Exec shell", "Exec shell");
        }
        if capabilities.can_edit_yaml
            && ui
                .add_enabled(mutations_allowed, egui::Button::new("Edit YAML"))
                .clicked()
        {
            queued.push(WorkspaceCommand::SetActiveTab(window_id, DetailTab::Yaml));
        }
    });
    if !mutations_allowed {
        ui.label("Scale, delete, and YAML edits are disabled until this window is live");
    }
    ui.separator();

    match detail.active_tab {
        DetailTab::Overview => overview::show(ui, window_id, &view.sections),
        DetailTab::Pods => related::show(
            ui,
            window_id,
            identity,
            feed.relations.get(identity),
            resource_actions,
            queued,
        ),
        DetailTab::Events => events::show(ui, window_id, view.events_condition, &view.events),
        // Only Service details expose Ports; the generic body renders
        // nothing for it rather than falling back to another tab.
        DetailTab::Ports => {}
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
                    mutations_allowed,
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
        DetailTab::Shell => {
            tools::shell::show(
                ui,
                window_id,
                &mut streams.shells,
                stream_target(detail, view),
            );
        }
    }
    ui.separator();
    ui.label(
        RichText::new(
            "Shortcuts: l Logs · p Pods · s Shell · y YAML · e Events · Esc restore/close",
        )
        .weak(),
    );
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

/// Identity header: the pinned identity exactly as the backend asserts it.
pub(super) fn show_header(
    ui: &mut egui::Ui,
    identity: &k10s_protocol::ResourceIdentity,
    view: Option<&ResourceDetailResponse>,
) {
    ui.vertical(|ui| {
        ui.heading(RichText::new(format!("{} / {}", identity.gvk.kind, identity.name)).strong());
        ui.horizontal(|ui| {
            ui.label(format!("Kind {}", identity.gvk.kind));
            match identity.namespace.as_deref() {
                Some(namespace) => ui.label(format!("Namespace {namespace}")),
                None => ui.label("Scope Cluster-scoped"),
            };
            ui.label(format!("Context {}", identity.context));
        });
        ui.horizontal(|ui| {
            ui.monospace(format!("UID {}", identity.uid));
            if let Some(view) = view {
                ui.monospace(format!("Created {}", view.created_at));
                ui.label(RichText::new("Fresh · live").color(crate::ui::theme::HEALTHY));
            } else {
                ui.label(RichText::new("Refreshing…").weak());
            }
        });
    });
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
