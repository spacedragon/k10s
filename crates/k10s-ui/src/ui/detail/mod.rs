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

use egui::{RichText, Spinner};
use k10s_protocol::{GroupVersionKind, ResourceDetailResponse, WorkloadKind};

use crate::ui::tools;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

/// Exact tab set per kind. Pods expose runtime tools, controllers expose
/// their resolved related workloads, everything else keeps the common core.
#[must_use]
pub fn tabs_for_kind(gvk: &GroupVersionKind) -> &'static [DetailTab] {
    match WorkloadKind::from_gvk(gvk) {
        Some(WorkloadKind::Pod) => &[
            DetailTab::Overview,
            DetailTab::Events,
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
        DetailTab::Pods => "Pods",
        DetailTab::Yaml => "YAML",
        DetailTab::Events => "Events",
        DetailTab::Logs => "Logs",
        DetailTab::Shell => "Shell",
    }
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
    view: Option<&ResourceDetailResponse>,
    yaml: &mut tools::YamlEditors,
    streams: &mut tools::StreamStores,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    ui.horizontal(|ui| {
        ui.heading(RichText::new("Details").strong());
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

    // The header renders from the pinned identity alone, so a dedicated
    // window never follows a later integrated selection.
    let Some(identity) = detail.identity.as_row_identity() else {
        return;
    };
    show_header(ui, identity, view);

    let Some(view) = view else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label("Loading details");
        });
        return;
    };

    ui.horizontal(|ui| {
        let capabilities = view.capabilities;
        if capabilities.can_scale {
            action_button(ui, "Scale", "Scale workload");
        }
        if capabilities.can_view_logs {
            action_button(ui, "View logs", "View logs");
        }
        if capabilities.can_exec {
            action_button(ui, "Exec shell", "Exec shell");
        }
        if capabilities.can_edit_yaml && ui.button("Edit YAML").clicked() {
            queued.push(WorkspaceCommand::BeginYamlEdit(window_id));
        }
    });
    ui.separator();

    match detail.active_tab {
        DetailTab::Overview => overview::show(ui, window_id, &view.sections),
        DetailTab::Pods => related::show(ui, window_id, &view.related, queued),
        DetailTab::Events => events::show(ui, window_id, &view.events),
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
                    queued,
                );
            }
        }
        DetailTab::Logs => {
            tools::logs::show(ui, window_id, &mut streams.logs, stream_target(detail));
        }
        DetailTab::Shell => {
            tools::shell::show(ui, window_id, &mut streams.shells, stream_target(detail));
        }
    }
}

/// The default pod container streamed by the connected tools.
const DEFAULT_CONTAINER: &str = "app";

/// Resolve a pod/container stream target from the pinned identity. Only pod
/// identities can stream; anything else yields no target.
fn stream_target<I>(detail: &DetailState<I>) -> Option<k10s_protocol::StreamTarget>
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
        container: DEFAULT_CONTAINER.to_owned(),
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

/// Identity header: the pinned identity exactly as the backend asserts it.
fn show_header(
    ui: &mut egui::Ui,
    identity: &k10s_protocol::ResourceIdentity,
    view: Option<&ResourceDetailResponse>,
) {
    ui.heading(RichText::new(identity.name.as_str()).strong());
    ui.label(format!("Kind {}", identity.gvk.kind));
    match identity.namespace.as_deref() {
        Some(namespace) => {
            ui.label(format!("Namespace {namespace}"));
        }
        None => {
            ui.label("Scope Cluster-scoped");
        }
    }
    ui.monospace(format!("UID {}", identity.uid));
    if let Some(view) = view {
        ui.monospace(format!("Created {}", view.created_at));
    }
}

fn action_button(ui: &mut egui::Ui, label: &str, accessible: &str) {
    let button = ui.button(label);
    button.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, true, accessible.to_owned())
    });
}
