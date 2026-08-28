//! Connected workload list windows: sortable table, searchable GVK picker
//! for custom resources, and the split integrated detail pane.
//!
//! All rows come from [`ResourceFeed`] projections of protocol payloads;
//! nothing here reads backend or fake state. Window-local control state is
//! either command-driven workspace state (search, namespace, sort, split,
//! selection) or this module's small scratch state (picker search).

use std::collections::HashMap;

use egui::{ComboBox, RichText, Spinner, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{
    GroupVersionKind, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceRelationsResponse, ResourceTypeEntry,
};

use crate::workspace::{DetailTab, ResourceWindowState, WindowId, WorkloadKind, WorkspaceCommand};

use super::{ConnectionState, theme};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeUiError(String);

impl SafeUiError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

/// Window-local lifecycle for a resource watch. Values are presentation-ready
/// so deterministic fixtures never depend on wall-clock time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFreshness {
    Live {
        last_sync_age: String,
    },
    StaleRetrying {
        last_sync_age: String,
        retry_in: String,
        attempt: u32,
    },
    Forbidden {
        user: String,
        verb: String,
        resource: String,
        scope: String,
    },
    Failed {
        message: String,
    },
    ReadyEmpty,
}

impl WindowFreshness {
    #[must_use]
    pub fn mutations_allowed(&self) -> bool {
        matches!(self, Self::Live { .. } | Self::ReadyEmpty)
    }
}

/// Authoritative lifecycle of the shared core/v1 Namespace catalog.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NamespaceCatalogState {
    #[default]
    NotDemanded,
    Loading,
    Ready(Vec<String>),
    Unavailable(SafeUiError),
}

pub(super) fn show_namespace_catalog_status(
    ui: &mut egui::Ui,
    state: &NamespaceCatalogState,
    resource_actions: &mut Vec<super::ResourceAction>,
) {
    match state {
        NamespaceCatalogState::Loading => {
            ui.horizontal(|ui| {
                ui.add(Spinner::new());
                ui.label("Loading namespaces");
            });
        }
        NamespaceCatalogState::Unavailable(error) => {
            ui.horizontal(|ui| {
                ui.label(format!("Namespaces unavailable: {}", error.message()));
                if ui.button("Retry namespaces").clicked() {
                    resource_actions.push(super::ResourceAction::RetryNamespaceCatalog);
                }
            });
        }
        NamespaceCatalogState::NotDemanded | NamespaceCatalogState::Ready(_) => {}
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // Public projection intentionally mirrors the protocol value.
pub enum PrimaryDetailState {
    Loading,
    Loaded(ResourceDetailResponse),
    Failed(SafeUiError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Public projection intentionally mirrors the protocol value.
pub enum RelationState {
    NotRequested,
    Loading,
    Loaded {
        response: std::sync::Arc<ResourceRelationsResponse>,
        loaded_at_ms: u64,
        refreshing: bool,
        refresh_error: Option<SafeUiError>,
    },
    Failed(SafeUiError),
}

/// Protocol rows and selectable types for one rendered frame.
///
/// The application builds this from its client state; windows render it
/// read-only. An absent list entry means the window is still loading.
/// External fixtures should construct it through [`ResourceFeed::default`]
/// (or struct update syntax with that default) and set only the public
/// projections they need. This is the intentional construction boundary as
/// lifecycle projections evolve during the crate's pre-1.0 API.
#[derive(Debug, Clone, Default)]
pub struct ResourceFeed {
    /// Lifecycle of each open list window. Missing entries retain the legacy
    /// inference from connection and row state for compatibility.
    pub window_freshness: HashMap<WindowId, WindowFreshness>,
    /// Shared Namespace candidates for every namespaced list window.
    pub namespace_catalog: NamespaceCatalogState,
    /// Legacy kind-keyed fixture input. Production uses `window_lists` so
    /// same-kind windows with different scopes never collapse.
    pub lists: HashMap<WorkloadKind, Vec<ResourceListRow>>,
    /// Rows per open workload window.
    pub window_lists: HashMap<WindowId, Vec<ResourceListRow>>,
    /// Core/v1 Service rows for the selected context, carrying structured
    /// projections; `None` while the Services watch is still loading.
    pub services: Option<Vec<ResourceListRow>>,
    /// Production Service rows per open window.
    pub window_services: HashMap<WindowId, Vec<ResourceListRow>>,
    /// Types offered by the searchable GVK picker.
    pub types: Vec<ResourceTypeEntry>,
    /// Backend-resolved detail responses keyed by stable identity. Both the
    /// integrated pane and dedicated windows look their view up here.
    pub details: HashMap<ResourceIdentity, ResourceDetailResponse>,
    /// Explicit primary-detail lifecycle. New production projections use
    /// this map; `details` remains a compatibility input for static fixtures.
    pub primary_details: HashMap<ResourceIdentity, PrimaryDetailState>,
    /// Independently loaded controller relations, keyed by exact identity.
    pub relations: HashMap<ResourceIdentity, RelationState>,
    pub port_forward_available: bool,
    pub port_forward_sessions: Vec<k10s_protocol::PortForwardSession>,
    pub port_forward_error: Option<String>,
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

pub(super) fn show_window_freshness(
    ui: &mut egui::Ui,
    window_id: WindowId,
    freshness: &WindowFreshness,
    resource_actions: &mut Vec<super::ResourceAction>,
) {
    match freshness {
        WindowFreshness::Live { last_sync_age } => {
            ui.label(
                RichText::new(format!("● Live · synced {last_sync_age}")).color(theme::HEALTHY),
            );
        }
        WindowFreshness::ReadyEmpty => {
            ui.label("◇ Ready · no resources");
            if ui.button("Refresh list").clicked() {
                resource_actions.push(super::ResourceAction::FullResyncWindow(window_id));
            }
        }
        WindowFreshness::StaleRetrying {
            last_sync_age,
            retry_in,
            attempt,
        } => {
            ui.label(
                RichText::new(format!(
                    "▲ Stale · last sync {last_sync_age} · retry in {retry_in} · attempt {attempt}"
                ))
                .color(theme::WARNING),
            );
            ui.label("Mutations disabled while this window is stale");
            ui.horizontal(|ui| {
                if ui.button("Retry now").clicked() {
                    resource_actions.push(super::ResourceAction::RetryWindow(window_id));
                }
                if ui.button("Full resync").clicked() {
                    resource_actions.push(super::ResourceAction::FullResyncWindow(window_id));
                }
            });
        }
        WindowFreshness::Forbidden {
            user,
            verb,
            resource,
            scope,
        } => {
            ui.label(
                RichText::new(format!(
                    "■ Forbidden · user {user} cannot {verb} {resource} in {scope}"
                ))
                .color(egui::Color32::LIGHT_RED),
            );
            ui.label("Ask a cluster administrator to grant this permission, then retry.");
            let command = format!("kubectl auth can-i {verb} {resource} --as={user} {scope}");
            ui.horizontal(|ui| {
                ui.monospace(&command);
                if ui.button("Copy auth can-i command").clicked() {
                    ui.ctx().copy_text(command);
                }
                if ui.button("Retry now").clicked() {
                    resource_actions.push(super::ResourceAction::RetryWindow(window_id));
                }
            });
        }
        WindowFreshness::Failed { message } => {
            ui.label(
                RichText::new(format!("✕ Failed · {message}")).color(egui::Color32::LIGHT_RED),
            );
            ui.horizontal(|ui| {
                if ui.button("Retry now").clicked() {
                    resource_actions.push(super::ResourceAction::RetryWindow(window_id));
                }
                if ui.button("Full resync").clicked() {
                    resource_actions.push(super::ResourceAction::FullResyncWindow(window_id));
                }
            });
        }
    }
    ui.separator();
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
    namespace_search: HashMap<WindowId, String>,
}

impl ResourceUiState {
    /// Drop scratch entries for closed windows.
    pub(super) fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.picker_search.retain(|id, _| live(*id));
        self.namespace_search.retain(|id, _| live(*id));
    }
}

/// Render the one shared namespace selector used by every namespaced list.
pub(super) fn show_namespace_combobox<I>(
    ui: &mut egui::Ui,
    scratch: &mut ResourceUiState,
    window_id: WindowId,
    scope: &crate::workspace::NamespaceScope,
    catalog: &NamespaceCatalogState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let selected = match scope {
        crate::workspace::NamespaceScope::Namespace(value) => value.as_str(),
        crate::workspace::NamespaceScope::ContextDefault
        | crate::workspace::NamespaceScope::AllNamespaces => "All namespaces",
    };
    let missing = matches!(scope, crate::workspace::NamespaceScope::Namespace(value)
        if matches!(catalog, NamespaceCatalogState::Ready(values) if !values.contains(value)));
    let selected_text = if matches!(catalog, NamespaceCatalogState::NotDemanded) {
        "Namespace catalog not requested".to_owned()
    } else if missing {
        format!("{selected} · namespace no longer exists")
    } else {
        selected.to_owned()
    };
    let enabled = matches!(catalog, NamespaceCatalogState::Ready(_));
    ui.add_enabled_ui(enabled, |ui| {
        ComboBox::new(("namespace", window_id.0), "Namespace")
            .selected_text(selected_text)
            .width(150.0)
            .show_ui(ui, |ui| {
                let search = scratch.namespace_search.entry(window_id).or_default();
                let response = ui.add(
                    TextEdit::singleline(search)
                        .hint_text("Search namespaces")
                        .desired_width(150.0),
                );
                response.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::TextEdit, true, "Search namespaces")
                });
                if !response.has_focus() {
                    response.request_focus();
                }
                let needle = search.to_lowercase();
                if "all namespaces".contains(&needle)
                    && ui
                        .selectable_label(
                            matches!(scope, crate::workspace::NamespaceScope::AllNamespaces),
                            "All namespaces",
                        )
                        .clicked()
                {
                    queued.push(WorkspaceCommand::SetNamespaceScope(
                        window_id,
                        crate::workspace::NamespaceScope::AllNamespaces,
                    ));
                    search.clear();
                    ui.close();
                }
                if let NamespaceCatalogState::Ready(values) = catalog {
                    if values.is_empty() {
                        ui.label("No namespaces found");
                    }
                    for namespace in values
                        .iter()
                        .filter(|value| value.to_lowercase().contains(&needle))
                    {
                        if ui
                            .selectable_label(
                                matches!(scope, crate::workspace::NamespaceScope::Namespace(value) if value == namespace),
                                namespace,
                            )
                            .clicked()
                        {
                            queued.push(WorkspaceCommand::SetNamespaceScope(
                                window_id,
                                crate::workspace::NamespaceScope::Namespace(namespace.clone()),
                            ));
                            search.clear();
                            ui.close();
                        }
                    }
                }
            });
    });
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
    yaml: &mut super::tools::YamlEditors,
    streams: &mut super::tools::StreamStores,
    dialogs: &mut super::dialogs::OperationDialogs,
    feed: &ResourceFeed,
    context_namespace: Option<&str>,
    connection: ConnectionState,
    resource_actions: &mut Vec<super::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    let title = kind.title();
    let fallback_freshness =
        (connection != ConnectionState::Connected).then(|| WindowFreshness::StaleRetrying {
            last_sync_age: "unknown".into(),
            retry_in: "pending".into(),
            attempt: 1,
        });
    let effective_freshness = feed
        .window_freshness
        .get(&window_id)
        .or(fallback_freshness.as_ref());
    if let Some(freshness) = effective_freshness {
        show_window_freshness(ui, window_id, freshness, resource_actions);
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

    let compact_controls = ui.ctx().content_rect().width() < 700.0;
    // Scope controls must remain reachable in the supported compact web
    // viewport. Concise labels and useful-but-smaller editors keep the
    // established single-row layout intact.
    ui.horizontal(|ui| {
        let search_hint = format!("Search {}", title.to_lowercase());
        let mut search = state.search.clone();
        let search_edit = ui.add(
            TextEdit::singleline(&mut search)
                .hint_text(search_hint.clone())
                .desired_width(if compact_controls { 100.0 } else { 200.0 }),
        );
        search_edit.widget_info(move || {
            WidgetInfo::labeled(WidgetType::TextEdit, true, search_hint.clone())
        });
        if search_edit.changed() {
            queued.push(WorkspaceCommand::SetSearch(window_id, search));
        }

        if namespaced {
            show_namespace_combobox(
                ui,
                scratch,
                window_id,
                &state.namespace_scope,
                &feed.namespace_catalog,
                queued,
            );
        }

        if kind == WorkloadKind::CustomResources && ui.button("Change resource type").clicked() {
            queued.push(WorkspaceCommand::SetCustomKind(window_id, None));
        }

        let toggle_label = if state.detail_visible {
            "Hide details"
        } else {
            "Show details"
        };
        let filters_active = !state.search.is_empty()
            || (namespaced
                && state.namespace_scope != crate::workspace::NamespaceScope::AllNamespaces);
        let clear_label = if compact_controls {
            "Clear"
        } else {
            "Clear filters"
        };
        if filters_active && ui.button(clear_label).clicked() {
            queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
            if namespaced {
                queued.push(WorkspaceCommand::SetNamespaceScope(
                    window_id,
                    crate::workspace::NamespaceScope::AllNamespaces,
                ));
            }
        }

        if ui.button(toggle_label).clicked() {
            queued.push(WorkspaceCommand::ToggleDetailPane(window_id));
        }
    });
    if namespaced {
        show_namespace_catalog_status(ui, &feed.namespace_catalog, resource_actions);
    }
    ui.separator();

    let Some(rows) = feed
        .window_lists
        .get(&window_id)
        .or_else(|| feed.lists.get(&kind))
    else {
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label(format!("Loading {}", title.to_lowercase()));
        });
        return;
    };

    if rows.is_empty() && effective_freshness.is_none() {
        show_window_freshness(
            ui,
            window_id,
            &WindowFreshness::ReadyEmpty,
            resource_actions,
        );
    }

    // The namespace restriction filters authoritative rows locally; the
    // search text filter lives in the table module.
    let needle = state.search.to_lowercase();
    let filtered: Vec<&ResourceListRow> = rows
        .iter()
        .filter(|row| {
            (!namespaced
                || state
                    .namespace_scope
                    .resolve(context_namespace)
                    .is_none_or(|wanted| Some(wanted) == row.identity.namespace.as_deref()))
                && super::resource_table::matches_search(row, &needle)
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
    // A pinned identity that no longer exists among the authoritative rows
    // was deleted (or is gone behind the watch); it must never be shown as
    // merely "loading".
    let gone = state.detail.is_some() && detail_row.is_none();
    let detail_shown = state.detail_visible && state.detail.is_some();

    // The integrated pane renders the pinned detail state; the backend
    // response is looked up by that identity alone.
    let detail_identity = state
        .detail
        .as_ref()
        .and_then(|detail| detail.identity.as_row_identity());
    let primary_state = detail_identity.and_then(|identity| feed.primary_details.get(identity));
    let detail_view = detail_identity.and_then(|identity| feed.details.get(identity));

    let available_height = ui.available_height();
    let (list_actions, _) = super::split::show_vertical(
        ui,
        &mut ratio,
        detail_shown,
        state.prior_split_ratio.is_some(),
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
                super::detail::show(
                    ui,
                    window_id,
                    detail,
                    primary_state,
                    detail_view,
                    gone,
                    state.prior_split_ratio.is_some(),
                    yaml,
                    streams,
                    dialogs,
                    feed,
                    None,
                    effective_freshness.is_none_or(WindowFreshness::mutations_allowed),
                    resource_actions,
                    queued,
                );
            }
        },
    );

    if detail_shown {
        let auto_focus =
            state.detail.as_ref().is_some_and(|detail| {
                matches!(detail.active_tab, DetailTab::Logs | DetailTab::Shell)
            }) && super::split::pane_heights(available_height, ratio, true).1
                < super::split::DETAIL_PANE_MIN + 80.0;
        if auto_focus && state.prior_split_ratio.is_none() {
            queued.push(WorkspaceCommand::MaximizeDetailPane(window_id));
        }
        if ui.input(|input| input.key_pressed(egui::Key::Escape)) {
            let find_active = state
                .detail
                .as_ref()
                .is_some_and(|detail| detail.active_tab == DetailTab::Logs)
                && streams
                    .logs
                    .get(window_id)
                    .and_then(|logs| logs.find())
                    .is_some();
            if find_active {
                if let Some(logs) = streams.logs.get_mut(window_id) {
                    logs.set_find(None);
                }
            } else if state.prior_split_ratio.is_some() {
                queued.push(WorkspaceCommand::RestoreDetailPane(window_id));
            } else {
                queued.push(WorkspaceCommand::ClearSelection(window_id));
            }
        }
    }

    if let Some(actions) = list_actions {
        if actions.cleared {
            queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
            if namespaced {
                queued.push(WorkspaceCommand::SetNamespaceScope(
                    window_id,
                    crate::workspace::NamespaceScope::AllNamespaces,
                ));
            }
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

    if let Some(identity) = state.selection.clone()
        && ui.input(|input| input.key_pressed(egui::Key::Enter))
    {
        if ui.input(|input| input.modifiers.any()) {
            queued.push(WorkspaceCommand::OpenDedicatedDetail(identity));
        } else if !state.detail_visible {
            queued.push(WorkspaceCommand::ToggleDetailPane(window_id));
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
