//! Connected workload list windows: sortable table, searchable GVK picker
//! for custom resources, and the split integrated detail pane.
//!
//! All rows come from [`ResourceFeed`] projections of protocol payloads;
//! nothing here reads backend or fake state. Window-local control state is
//! either command-driven workspace state (search, namespace, sort, split,
//! selection) or this module's small scratch state (picker search).

use std::collections::HashMap;

use egui::{RichText, Spinner, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{
    GroupVersionKind, ResourceDetailResponse, ResourceIdentity, ResourceListRow, ResourceTypeEntry,
};

use crate::workspace::{ResourceWindowState, WindowId, WorkloadKind, WorkspaceCommand};

use super::{ConnectionState, theme};

/// Protocol rows and selectable types for one rendered frame.
///
/// The application builds this from its client state; windows render it
/// read-only. An absent list entry means the window is still loading.
#[derive(Debug, Clone, Default)]
pub struct ResourceFeed {
    /// Rows per workload kind for the selected context.
    pub lists: HashMap<WorkloadKind, Vec<ResourceListRow>>,
    /// Types offered by the searchable GVK picker.
    pub types: Vec<ResourceTypeEntry>,
    /// Backend-resolved detail responses keyed by stable identity. Both the
    /// integrated pane and dedicated windows look their view up here.
    pub details: HashMap<ResourceIdentity, ResourceDetailResponse>,
}

/// Maps a protocol row identity onto the shell's workspace identity type.
///
/// Production instantiates the shell with [`ResourceIdentity`] itself;
/// static prototypes may map every row onto `()`.
pub trait RowIdentity: Clone + Eq + std::hash::Hash + std::fmt::Debug {
    /// Convert one protocol identity into this workspace's identity type.
    #[must_use]
    fn from_row_identity(identity: &ResourceIdentity) -> Self;

    /// Recover the protocol identity this workspace identity refers to, so
    /// pinned detail views can look up their backend-resolved response.
    #[must_use]
    fn as_row_identity(&self) -> Option<&ResourceIdentity> {
        None
    }
}

impl RowIdentity for ResourceIdentity {
    fn from_row_identity(identity: &ResourceIdentity) -> Self {
        identity.clone()
    }

    fn as_row_identity(&self) -> Option<&ResourceIdentity> {
        Some(self)
    }
}

#[allow(clippy::fallible_impl_from)]
impl RowIdentity for () {
    fn from_row_identity(_: &ResourceIdentity) -> Self {}
}

/// Per-window scratch state that does not belong in workspace commands.
#[derive(Debug, Default)]
pub(super) struct ResourceUiState {
    picker_search: HashMap<WindowId, String>,
}

impl ResourceUiState {
    /// Drop scratch entries for closed windows.
    pub(super) fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.picker_search.retain(|id, _| live(*id));
    }
}

/// Canonical key format shared by commands and picker entries.
fn type_key(gvk: &GroupVersionKind) -> String {
    format!("{}/{}/{}", gvk.group, gvk.version, gvk.kind)
}

/// Human label of a picker entry; core-group kinds drop the group prefix.
fn type_label(entry: &ResourceTypeEntry) -> String {
    if entry.gvk.group.is_empty() {
        format!("{} {}", entry.gvk.version, entry.gvk.kind)
    } else {
        format!(
            "{}/{} {}",
            entry.gvk.group, entry.gvk.version, entry.gvk.kind
        )
    }
}

fn gvk(group: &str, version: &str, kind: &str) -> GroupVersionKind {
    GroupVersionKind {
        group: group.to_owned(),
        version: version.to_owned(),
        kind: kind.to_owned(),
    }
}

/// Map a built-in launcher kind onto its wire GVK; custom resources pick
/// their own type instead.
fn builtin_gvk(kind: WorkloadKind) -> Option<GroupVersionKind> {
    match kind {
        WorkloadKind::Deployments => Some(gvk("apps", "v1", "Deployment")),
        WorkloadKind::Pods => Some(GroupVersionKind::core("v1", "Pod")),
        WorkloadKind::StatefulSets => Some(gvk("apps", "v1", "StatefulSet")),
        WorkloadKind::DaemonSets => Some(gvk("apps", "v1", "DaemonSet")),
        WorkloadKind::Jobs => Some(gvk("batch", "v1", "Job")),
        WorkloadKind::CronJobs => Some(gvk("batch", "v1", "CronJob")),
        WorkloadKind::CustomResources => None,
    }
}

/// Render one workload list window body, queuing commands for every
/// interaction.
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    scratch: &mut ResourceUiState,
    window_id: WindowId,
    kind: WorkloadKind,
    state: &mut ResourceWindowState<I>,
    feed: &ResourceFeed,
    connection: ConnectionState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    let title = kind.title();
    if connection != ConnectionState::Connected {
        ui.label(RichText::new("Connection stale · showing last known rows").color(theme::WARNING));
        ui.separator();
    }

    // Resolve which type this window displays. Custom resources must pick
    // one through the searchable GVK picker first.
    let selected_type = match kind {
        WorkloadKind::CustomResources => state
            .custom_kind
            .as_deref()
            .and_then(|key| feed.types.iter().find(|entry| type_key(&entry.gvk) == *key))
            .map(|entry| (entry.gvk.clone(), entry.namespaced)),
        _ => builtin_gvk(kind).map(|gvk| (gvk, true)),
    };
    let Some((_, namespaced)) = selected_type else {
        show_picker(ui, scratch, window_id, feed, queued);
        return;
    };

    ui.horizontal(|ui| {
        let search_hint = format!("Search {}", title.to_lowercase());
        let mut search = state.search.clone();
        let search_edit = ui.add(
            TextEdit::singleline(&mut search)
                .hint_text(search_hint.clone())
                .desired_width(200.0),
        );
        search_edit.widget_info(move || {
            WidgetInfo::labeled(WidgetType::TextEdit, true, search_hint.clone())
        });
        if search_edit.changed() {
            queued.push(WorkspaceCommand::SetSearch(window_id, search));
        }

        if namespaced {
            let mut namespace = state.namespace.clone().unwrap_or_default();
            let namespace_edit = ui.add(
                TextEdit::singleline(&mut namespace)
                    .hint_text("Namespace filter")
                    .desired_width(140.0),
            );
            namespace_edit.widget_info(|| {
                WidgetInfo::labeled(WidgetType::TextEdit, true, "Namespace filter".to_owned())
            });
            if namespace_edit.changed() {
                let parsed = namespace.trim().to_owned();
                queued.push(WorkspaceCommand::SetNamespace(
                    window_id,
                    (!parsed.is_empty()).then_some(parsed),
                ));
            }
        }

        if kind == WorkloadKind::CustomResources && ui.button("Change resource type").clicked() {
            queued.push(WorkspaceCommand::SetCustomKind(window_id, None));
        }

        let toggle_label = if state.detail_visible {
            "Hide details"
        } else {
            "Show details"
        };
        let filters_active = !state.search.is_empty() || state.namespace.is_some();
        if filters_active && ui.button("Clear filters").clicked() {
            queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
            queued.push(WorkspaceCommand::SetNamespace(window_id, None));
        }

        if ui.button(toggle_label).clicked() {
            queued.push(WorkspaceCommand::ToggleDetailPane(window_id));
        }
    });
    ui.separator();

    let Some(rows) = feed.lists.get(&kind) else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label(format!("Loading {}", title.to_lowercase()));
        });
        return;
    };

    // The namespace restriction filters authoritative rows locally; the
    // search text filter lives in the table module.
    let filtered: Vec<&ResourceListRow> = rows
        .iter()
        .filter(|row| {
            state
                .namespace
                .as_deref()
                .is_none_or(|wanted| Some(wanted) == row.identity.namespace.as_deref())
                && super::resource_table::matches_search(row, &state.search)
        })
        .collect();
    let mut sorted = filtered;
    if let Some(sort) = state.sort.as_ref() {
        super::resource_table::sort_rows(&mut sorted, sort);
    }

    let selected: Option<&I> = state.selection.as_ref();
    let detail_row: Option<ResourceListRow> = selected.and_then(|selection| {
        rows.iter()
            .find(|row| I::from_row_identity(&row.identity) == *selection)
            .cloned()
    });
    let mut ratio = state.split_ratio;
    let detail_shown = state.detail_visible && detail_row.is_some();

    // The integrated pane renders the pinned detail state; the backend
    // response is looked up by that identity alone.
    let detail_view = state
        .detail
        .as_ref()
        .and_then(|detail| detail.identity.as_row_identity())
        .and_then(|identity| feed.details.get(identity));

    let (list_actions, _) = super::split::show_vertical(
        ui,
        &mut ratio,
        detail_shown,
        |ui| {
            super::resource_table::show(
                ui,
                window_id,
                title,
                namespaced,
                &state.search,
                state.sort.as_ref(),
                &sorted,
                |row| {
                    selected
                        .is_some_and(|selection| *selection == I::from_row_identity(&row.identity))
                },
                |row| I::from_row_identity(&row.identity),
            )
        },
        |ui| {
            if let Some(detail) = state.detail.as_ref() {
                if ui.button("Clear selection").clicked() {
                    queued.push(WorkspaceCommand::ClearSelection(window_id));
                }
                super::detail::show(ui, window_id, detail, detail_view, queued);
            }
        },
    );

    if let Some(actions) = list_actions {
        if actions.cleared {
            queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
            queued.push(WorkspaceCommand::SetNamespace(window_id, None));
        }
        if let Some(sort) = actions.sort {
            queued.push(WorkspaceCommand::SetSort(window_id, Some(sort)));
        }
        if let Some(identity) = actions.selected {
            queued.push(WorkspaceCommand::SelectRow(window_id, identity));
        }
        // Double-click and the row context menu pop a dedicated window out;
        // it clones the stable identity at open time and never follows this
        // window's later selection.
        if let Some(identity) = actions.popped_out {
            queued.push(WorkspaceCommand::OpenDedicatedDetail(identity));
        }
    }

    if ratio != state.split_ratio {
        queued.push(WorkspaceCommand::SetSplitRatio(window_id, ratio));
    }
}

/// The searchable GVK picker shown by custom-resources windows before a
/// type is picked. Cluster-scoped entries are labelled explicitly because
/// they ignore namespace filtering everywhere else.
fn show_picker<I>(
    ui: &mut egui::Ui,
    scratch: &mut ResourceUiState,
    window_id: WindowId,
    feed: &ResourceFeed,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.label("Pick a resource type");
    ui.separator();

    let search = scratch.picker_search.entry(window_id).or_default();
    let search_edit = ui.add(
        TextEdit::singleline(search)
            .hint_text("Search resource types")
            .desired_width(280.0),
    );
    search_edit.widget_info(|| {
        WidgetInfo::labeled(
            WidgetType::TextEdit,
            true,
            "Search resource types".to_owned(),
        )
    });

    egui::ScrollArea::vertical()
        .id_salt(("k10s.resource.picker.scroll", window_id.0))
        .show(ui, |ui| {
            let needle = scratch
                .picker_search
                .get(&window_id)
                .cloned()
                .unwrap_or_default();
            for entry in &feed.types {
                let label = type_label(entry);
                let visible = needle.is_empty()
                    || label.to_lowercase().contains(&needle.to_lowercase())
                    || entry
                        .gvk
                        .kind
                        .to_lowercase()
                        .contains(&needle.to_lowercase());
                if !visible {
                    continue;
                }
                let suffix = if entry.namespaced {
                    ""
                } else {
                    " · cluster-scoped"
                };
                let button = ui.button(format!("{label}{suffix}"));
                button.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
                if button.clicked() {
                    queued.push(WorkspaceCommand::SetCustomKind(
                        window_id,
                        Some(type_key(&entry.gvk)),
                    ));
                }
            }
        });
}
