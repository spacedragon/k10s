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
use web_time::SystemTime;

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
    Reconnecting {
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

fn recovery_button(
    ui: &mut egui::Ui,
    label: &str,
    enabled: bool,
    unavailable_reason: &str,
) -> bool {
    let response = ui.add_enabled(enabled, egui::Button::new(label));
    response.widget_info(|| {
        egui::WidgetInfo::labeled(egui::WidgetType::Button, true, label.to_owned())
    });
    if enabled {
        response.clicked()
    } else {
        response
            .on_disabled_hover_text(unavailable_reason)
            .clicked()
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

/// UI-owned freshness and mutation authority for one exact pinned identity.
/// Dedicated Detail windows consume only this projection and never infer
/// authority by searching arbitrary list windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailLifecycle {
    Present,
    Gone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetailAuthority {
    pub freshness: WindowFreshness,
    pub lifecycle: DetailLifecycle,
}

/// Authority state of the global port-forward session reconstruction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PortForwardListState {
    /// The first authoritative list has not completed yet.
    Loading,
    /// A reconnected transport is rebuilding the authoritative list.
    Reconstructing,
    /// The latest list completed; an empty session vector is authoritative.
    #[default]
    Ready,
}

impl DetailAuthority {
    #[must_use]
    pub fn mutations_allowed(&self) -> bool {
        self.lifecycle == DetailLifecycle::Present && self.freshness.mutations_allowed()
    }
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
    /// One clock sample shared by every relative-age cell in a rendered frame.
    /// Production derives it from the backend snapshot timestamp; deterministic
    /// fixtures pin it explicitly.
    pub render_time: Option<SystemTime>,
    /// Lifecycle of each open list window. Missing entries retain the legacy
    /// inference from connection and row state for compatibility.
    pub window_freshness: HashMap<WindowId, WindowFreshness>,
    /// Exact-identity authority for dedicated Detail windows. Missing means
    /// unavailable and mutations fail closed.
    pub detail_authority: HashMap<ResourceIdentity, DetailAuthority>,
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
    /// Exact identity-matched resource and container metrics for detail panes.
    pub metrics: HashMap<ResourceIdentity, k10s_protocol::ResourceMetricsResponse>,
    /// Whether the negotiated server accepts Service port-forward targets.
    pub port_forward_available: bool,
    /// Whether the negotiated server accepts Pod port-forward targets.
    pub pod_port_forward_available: bool,
    pub port_forward_list_state: PortForwardListState,
    pub port_forward_sessions: Vec<k10s_protocol::PortForwardSession>,
    /// Application-owned safe retry errors keyed to their authoritative row.
    pub port_forward_retry_errors:
        std::collections::BTreeMap<k10s_protocol::PortForwardSessionId, String>,
    pub port_forward_error: Option<String>,
}

/// Maps a protocol row identity onto the shell's workspace identity type.
///
/// Production instantiates the shell with [`ResourceIdentity`] itself;
/// static prototypes may map every row onto `()`.
pub trait RowIdentity:
    Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static
{
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
        WindowFreshness::Reconnecting {
            last_sync_age,
            retry_in,
            attempt,
        } => {
            ui.label(
                RichText::new(format!(
                    "[~] Reconnecting · last sync {last_sync_age} · retry in {retry_in} · attempt {attempt}"
                ))
                .strong()
                .color(theme::CONNECTING),
            );
            ui.label("Mutations are disabled; recovery controls unlock after reconnecting.");
            ui.horizontal(|ui| {
                recovery_button(ui, "Retry now", false, "Reconnect is already in progress");
                recovery_button(
                    ui,
                    "Full resync",
                    false,
                    "Full resync is unavailable until the transport reconnects",
                );
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
                RichText::new(format!("⨯ Failed · {message}")).color(egui::Color32::LIGHT_RED),
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
    compact: bool,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let selected = match scope {
        crate::workspace::NamespaceScope::Namespace(value) => value.as_str(),
        crate::workspace::NamespaceScope::ContextDefault => {
            if let NamespaceCatalogState::Ready(values) = catalog {
                values
                    .first()
                    .map(String::as_str)
                    .unwrap_or("All namespaces")
            } else {
                "loading..."
            }
        }
        crate::workspace::NamespaceScope::AllNamespaces => "All namespaces",
    };
    let missing = matches!(scope, crate::workspace::NamespaceScope::Namespace(value)
        if matches!(catalog, NamespaceCatalogState::Ready(values) if !values.contains(value)));
    // The control carries its own label (`Namespace: … ▾`), so the status
    // text stays short and never repeats the prefix.
    let full_selected_text = if matches!(catalog, NamespaceCatalogState::NotDemanded) {
        "not requested".to_owned()
    } else if missing {
        format!("{selected} · no longer exists")
    } else {
        selected.to_owned()
    };
    let label_text = if compact {
        "Namespace".to_owned()
    } else {
        format!("Namespace: {full_selected_text}")
    };
    let enabled = matches!(catalog, NamespaceCatalogState::Ready(_));
    ui.add_enabled_ui(enabled, |ui| {
        // The label lives inside the control (`Namespace: all ▾`) instead of
        // floating next to it.
        let response = ComboBox::from_id_salt(("namespace", window_id.0))
            .selected_text(label_text)
            .width(70.0)
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
                    for (idx, namespace) in values
                        .iter()
                        .filter(|value| value.to_lowercase().contains(&needle))
                        .enumerate()
                    {
                        let is_active = match scope {
                            crate::workspace::NamespaceScope::Namespace(value) => {
                                value == namespace
                            }
                            crate::workspace::NamespaceScope::ContextDefault => idx == 0,
                            crate::workspace::NamespaceScope::AllNamespaces => false,
                        };
                        if ui.selectable_label(is_active, namespace).clicked() {
                            queued.push(WorkspaceCommand::SetNamespaceScope(
                                window_id,
                                crate::workspace::NamespaceScope::Namespace(namespace.clone()),
                            ));
                            search.clear();
                            ui.close();
                        }
                    }
                }
            })
            .response;
        response.widget_info(|| {
            WidgetInfo::labeled(
                WidgetType::ComboBox,
                enabled,
                format!("Namespace: {full_selected_text}"),
            )
        });
    });
}

/// Compact toolbar control that filters the list by its status column.
/// The label carries its own prefix (`Status: all`) so the row never shows
/// a floating label next to the control.
fn show_status_combobox<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    status_filter: &Option<String>,
    rows: &[ResourceListRow],
    compact: bool,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let statuses: Vec<String> = rows
        .iter()
        .map(|row| super::resource_table::resource_status(row).into_owned())
        .filter(|status| !status.is_empty() && status != "—")
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let selected = status_filter.as_deref().unwrap_or("all");
    let selected_text = if compact {
        "Status".to_owned()
    } else {
        format!("Status: {selected}")
    };
    let response = ComboBox::from_id_salt(("status", window_id.0))
        .selected_text(selected_text)
        .width(65.0)
        .show_ui(ui, |ui| {
            if ui
                .selectable_label(status_filter.is_none(), "all")
                .clicked()
            {
                queued.push(WorkspaceCommand::SetStatusFilter(window_id, None));
                ui.close();
            }
            for status in &statuses {
                if ui
                    .selectable_label(status_filter.as_deref() == Some(status.as_str()), status)
                    .clicked()
                {
                    queued.push(WorkspaceCommand::SetStatusFilter(
                        window_id,
                        Some(status.clone()),
                    ));
                    ui.close();
                }
            }
        })
        .response;
    response.widget_info(|| {
        WidgetInfo::labeled(WidgetType::ComboBox, true, format!("Status: {selected}"))
    });
}

/// `reversed` emits the pieces back-to-front, which is what a
/// right-to-left layout needs to end up reading left-to-right while the
/// whole group stays anchored to the table's right edge.
fn show_match_details<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    state: &ResourceWindowState<I>,
    reversed: bool,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    // The line is one sentence, so the pieces carry their own spacing and
    // the layout adds none: standard control gaps and button padding put
    // stray spaces around the parentheses.
    ui.spacing_mut().item_spacing.x = 0.0;
    ui.spacing_mut().button_padding.x = 0.0;
    let (age_text, switch_label, next_mode) = match state.age_mode {
        crate::workspace::AgeMode::Relative => (
            "Age shown as relative",
            "switch to absolute",
            crate::workspace::AgeMode::Absolute,
        ),
        crate::workspace::AgeMode::Absolute => (
            "Age shown as absolute",
            "switch to relative",
            crate::workspace::AgeMode::Relative,
        ),
    };
    let sort_note = state.sort.as_ref().map(|sort| {
        format!(
            "sorted by {} {} · ",
            super::resource_table::column_title(&sort.column),
            if sort.ascending { "▲" } else { "▼" }
        )
    });
    let show_sort = |ui: &mut egui::Ui| {
        if let Some(note) = sort_note.as_ref() {
            ui.label(RichText::new(note).color(theme::MUTED_TEXT));
        }
    };
    let show_prefix = |ui: &mut egui::Ui| {
        ui.label(RichText::new(format!("{age_text} (")).color(theme::MUTED_TEXT));
    };
    let show_link = |ui: &mut egui::Ui| -> bool {
        ui.add(
            egui::Button::new(RichText::new(switch_label).color(theme::ACCENT))
                .frame(false)
                .wrap_mode(egui::TextWrapMode::Extend),
        )
        .clicked()
    };
    let show_suffix = |ui: &mut egui::Ui| {
        ui.label(RichText::new(")").color(theme::MUTED_TEXT));
    };
    let switched = if reversed {
        show_suffix(ui);
        let switched = show_link(ui);
        show_prefix(ui);
        show_sort(ui);
        switched
    } else {
        show_sort(ui);
        show_prefix(ui);
        let switched = show_link(ui);
        show_suffix(ui);
        switched
    };
    if switched {
        queued.push(WorkspaceCommand::SetAgeMode(window_id, next_mode));
    }
}

#[allow(clippy::too_many_arguments)]
fn show_secondary_controls<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    kind: WorkloadKind,
    namespaced: bool,
    filters_active: bool,
    compact_controls: bool,
    resource_actions: &mut Vec<super::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if kind == WorkloadKind::CustomResources && ui.button("Change resource type").clicked() {
        queued.push(WorkspaceCommand::SetCustomKind(window_id, None));
    }
    let refresh = if compact_controls {
        ui.button("Refresh list")
    } else {
        ui.button("↻").on_hover_text("Refresh list")
    };
    if refresh.clicked() {
        resource_actions.push(super::ResourceAction::FullResyncWindow(window_id));
    }
    if filters_active && ui.button("Reset").clicked() {
        queued.push(WorkspaceCommand::SetSearch(window_id, String::new()));
        if namespaced {
            queued.push(WorkspaceCommand::SetNamespaceScope(
                window_id,
                crate::workspace::NamespaceScope::AllNamespaces,
            ));
        }
        queued.push(WorkspaceCommand::SetStatusFilter(window_id, None));
    }
}

fn direct_toolbar_width(
    available_width: f32,
    namespaced: bool,
    custom_resource: bool,
    filters_active: bool,
    shows_freshness: bool,
) -> f32 {
    let search = if available_width < 760.0 {
        180.0
    } else {
        200.0
    };
    // The full `Namespace: All namespaces` selector is about 205 points with
    // the current monospace theme. Budget its painted width so a borderline
    // toolbar selects the compact layout instead of wrapping and oscillating
    // between two measured search widths on consecutive frames.
    let namespace = if namespaced { 210.0 } else { 0.0 };
    let custom_type = if custom_resource { 160.0 } else { 0.0 };
    let reset = if filters_active { 52.0 } else { 0.0 };
    let freshness = if shows_freshness { 115.0 } else { 0.0 };
    // Search, namespace, status, custom type, refresh, reset, and
    // freshness, plus one standard inter-control gap for each visible item.
    let controls = 1
        + usize::from(namespaced)
        + 1
        + usize::from(custom_resource)
        + 1
        + usize::from(filters_active)
        + usize::from(shows_freshness);
    search
        + namespace
        + 105.0
        + custom_type
        + 30.0
        + reset
        + freshness
        + (controls.saturating_sub(1) as f32 * 8.0)
        + 16.0
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
        WorkloadKind::Events => Some(GroupVersionKind::core("v1", "Event")),
        WorkloadKind::Namespaces => Some(GroupVersionKind::core("v1", "Namespace")),
        WorkloadKind::Deployments => Some(gvk("apps", "v1", "Deployment")),
        WorkloadKind::Pods => Some(GroupVersionKind::core("v1", "Pod")),
        WorkloadKind::StatefulSets => Some(gvk("apps", "v1", "StatefulSet")),
        WorkloadKind::DaemonSets => Some(gvk("apps", "v1", "DaemonSet")),
        WorkloadKind::Jobs => Some(gvk("batch", "v1", "Job")),
        WorkloadKind::CronJobs => Some(gvk("batch", "v1", "CronJob")),
        WorkloadKind::CustomResources => None,
        WorkloadKind::Ingresses => Some(gvk("networking.k8s.io", "v1", "Ingress")),
        WorkloadKind::Endpoints => Some(GroupVersionKind::core("v1", "Endpoints")),
        WorkloadKind::NetworkPolicies => Some(gvk("networking.k8s.io", "v1", "NetworkPolicy")),
        WorkloadKind::ConfigMaps => Some(GroupVersionKind::core("v1", "ConfigMap")),
        WorkloadKind::Secrets => Some(GroupVersionKind::core("v1", "Secret")),
        WorkloadKind::PersistentVolumeClaims => {
            Some(GroupVersionKind::core("v1", "PersistentVolumeClaim"))
        }
        WorkloadKind::PersistentVolumes => Some(GroupVersionKind::core("v1", "PersistentVolume")),
        WorkloadKind::StorageClasses => Some(gvk("storage.k8s.io", "v1", "StorageClass")),
        WorkloadKind::ServiceAccounts => Some(GroupVersionKind::core("v1", "ServiceAccount")),
        WorkloadKind::Roles => Some(gvk("rbac.authorization.k8s.io", "v1", "Role")),
        WorkloadKind::RoleBindings => Some(gvk("rbac.authorization.k8s.io", "v1", "RoleBinding")),
    }
}

/// Render one workload list window body, queuing commands for every
/// interaction.
#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    scratch: &mut ResourceUiState,
    window_id: WindowId,
    focused: bool,
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
    let title_lower = title.to_lowercase();
    let fallback_freshness =
        (connection != ConnectionState::Connected).then(|| WindowFreshness::Reconnecting {
            last_sync_age: "unknown".into(),
            retry_in: "pending".into(),
            attempt: 1,
        });
    let effective_freshness = feed
        .window_freshness
        .get(&window_id)
        .or(fallback_freshness.as_ref());

    // Resolve which type this window displays. Custom resources must pick
    // one through the searchable GVK picker first.
    let selected_type = match kind {
        WorkloadKind::CustomResources => state
            .custom_kind
            .as_deref()
            .and_then(|key| feed.types.iter().find(|entry| type_key(&entry.gvk) == *key))
            .map(|entry| (entry.gvk.clone(), entry.namespaced)),
        _ => builtin_gvk(kind).map(|gvk| (gvk, kind.namespaced())),
    };
    let Some((_, namespaced)) = selected_type else {
        // The GVK picker has no list toolbar yet; keep the full freshness
        // block above it so recovery controls stay reachable.
        if let Some(freshness) = effective_freshness {
            show_window_freshness(ui, window_id, freshness, resource_actions);
        }
        show_picker(ui, scratch, window_id, feed, queued);
        return;
    };

    let rows_opt = feed
        .window_lists
        .get(&window_id)
        .or_else(|| feed.lists.get(&kind));

    // A window can be much narrower than the app canvas, so use this list's
    // local control budget rather than the global content width or a fixed
    // breakpoint. Secondary controls overflow before they would wrap/clip.
    let filters_active = !state.search.is_empty()
        || (namespaced && state.namespace_scope != crate::workspace::NamespaceScope::AllNamespaces)
        || state.status_filter.is_some();
    let shows_freshness = effective_freshness.is_some_and(|freshness| {
        matches!(
            freshness,
            WindowFreshness::Live { .. } | WindowFreshness::ReadyEmpty
        )
    });
    let clipped_width = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width();
    let available_width = clipped_width.min(ui.ctx().content_rect().right() - ui.cursor().left());
    let toolbar_width = direct_toolbar_width(
        available_width,
        namespaced,
        kind == WorkloadKind::CustomResources,
        filters_active,
        shows_freshness,
    );
    let compact_controls = available_width < toolbar_width;
    // The search field is the only elastic control in the filter row, so it
    // absorbs whatever width the fixed controls leave over. The estimate in
    // `direct_toolbar_width` is deliberately conservative, so the real
    // width of everything beside the field is measured from the painted row
    // and reused next frame; the fixed point is stable because growing the
    // field never changes the fixed controls.
    let fixed_width_id = egui::Id::new(("k10s.resource.filters.fixed-width", window_id.0));
    let measured_fixed: Option<f32> = ui.data(|data| data.get_temp(fixed_width_id));
    let search_width = if compact_controls {
        let compact_fixed = (if namespaced { 76.0 } else { 0.0 })
            + 70.0 // status
            + 50.0 // more
            + (if shows_freshness { 36.0 } else { 0.0 })
            + 16.0;
        (available_width - compact_fixed).max(80.0)
    } else {
        let fixed_estimate = (if namespaced { 210.0 } else { 0.0 })
            + 105.0 // status
            + (if kind == WorkloadKind::CustomResources { 160.0 } else { 0.0 })
            + 30.0 // refresh
            + (if filters_active { 52.0 } else { 0.0 })
            + (if shows_freshness { 115.0 } else { 0.0 })
            + 36.0;
        match measured_fixed {
            // The margin keeps a rounding error from wrapping the row.
            Some(fixed) => (available_width - fixed - 12.0).max(120.0),
            None => (available_width - fixed_estimate - 12.0).max(120.0),
        }
    };
    let empty_rows: [ResourceListRow; 0] = [];
    let rows_for_status = rows_opt.map_or(&empty_rows[..], |r| r.as_slice());
    let filter_row = ui.horizontal_wrapped(|ui| {
        if compact_controls {
            ui.spacing_mut().item_spacing.x = 0.0;
        }
        let search_hint = format!("⌕  Search {title_lower}…");
        let search_label = format!("Search {title_lower}");
        let mut search = state.search.clone();
        let search_edit = ui.add(
            TextEdit::singleline(&mut search)
                .hint_text(search_hint)
                .desired_width(search_width),
        );
        let search_rect = search_edit.rect;
        search_edit.widget_info(move || {
            WidgetInfo::labeled(WidgetType::TextEdit, true, search_label.clone())
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
                compact_controls,
                queued,
            );
        }

        show_status_combobox(
            ui,
            window_id,
            &state.status_filter,
            rows_for_status,
            compact_controls,
            queued,
        );

        if compact_controls {
            let menu = ui.menu_button("More", |ui| {
                show_match_details(ui, window_id, state, false, queued);
                ui.separator();
                show_secondary_controls(
                    ui,
                    window_id,
                    kind,
                    namespaced,
                    filters_active,
                    compact_controls,
                    resource_actions,
                    queued,
                );
            });
            menu.response.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Button, true, "More list controls")
            });
        } else {
            show_secondary_controls(
                ui,
                window_id,
                kind,
                namespaced,
                filters_active,
                compact_controls,
                resource_actions,
                queued,
            );
        }

        if let Some(freshness) = effective_freshness {
            match freshness {
                WindowFreshness::Live { last_sync_age } => {
                    let label = if compact_controls {
                        "● Live".to_owned()
                    } else {
                        format!("● Live · {last_sync_age}")
                    };
                    let text = RichText::new(label).color(theme::HEALTHY);
                    let response = ui.label(if compact_controls {
                        text.size(6.0)
                    } else {
                        text
                    });
                    response.widget_info(|| {
                        WidgetInfo::labeled(
                            WidgetType::Label,
                            true,
                            format!("Live; synced {last_sync_age}"),
                        )
                    });
                }
                WindowFreshness::ReadyEmpty => {
                    ui.label(RichText::new("◇ Ready · no resources").weak());
                }
                _ => {}
            }
        }
        search_rect
    });
    if !compact_controls {
        // Everything beside the field, measured where it actually landed.
        let row = filter_row.response.rect;
        let search_rect = filter_row.inner;
        let fixed = (search_rect.left() - row.left()) + (row.right() - search_rect.right());
        ui.data_mut(|data| data.insert_temp(fixed_width_id, fixed.max(0.0)));
    }

    if namespaced {
        show_namespace_catalog_status(ui, &feed.namespace_catalog, resource_actions);
    }
    // Non-live freshness keeps its recovery block; live/empty states are
    // already in the toolbar, so only one separator is painted.
    let recovery_shown = effective_freshness.is_some_and(|freshness| {
        if matches!(
            freshness,
            WindowFreshness::Live { .. } | WindowFreshness::ReadyEmpty
        ) {
            false
        } else {
            show_window_freshness(ui, window_id, freshness, resource_actions);
            true
        }
    });

    let Some(rows) = rows_opt else {
        if !recovery_shown {
            ui.separator();
        }
        ui.horizontal(|ui| {
            ui.add(Spinner::new());
            ui.label(format!("Loading {title_lower}"));
        });
        return;
    };

    // The namespace restriction, status filter, and search text all filter
    // authoritative rows locally; the match line below reports the result.
    let needle = state.search.to_lowercase();
    let status_filter = state.status_filter.as_deref();
    let filtered: Vec<&ResourceListRow> = rows
        .iter()
        .filter(|row| {
            (!namespaced
                || state
                    .namespace_scope
                    .resolve(context_namespace)
                    .is_none_or(|wanted| Some(wanted) == row.identity.namespace.as_deref()))
                && status_filter.is_none_or(|wanted| {
                    super::resource_table::resource_status(row).as_ref() == wanted
                })
                && super::resource_table::matches_search(row, &needle)
        })
        .collect();
    let mut sorted = filtered;
    if let Some(sort) = state.sort.as_ref() {
        super::resource_table::sort_rows(&mut sorted, sort);
    }

    // Compact match line: result count, selection, active sort, and the
    // relative/absolute age affordance.
    ui.horizontal_wrapped(|ui| {
        let count = sorted.len();
        ui.label(
            RichText::new(format!(
                "{count} {title_lower}{}",
                if state.selection.is_some() {
                    " · 1 selected"
                } else {
                    ""
                }
            ))
            .color(theme::MUTED_TEXT),
        );
        if compact_controls {
            // Sort and age are available from the toolbar overflow.
        } else {
            // The group is emitted back-to-front in a right-to-left layout,
            // so it reads left-to-right while hugging the table's right
            // edge. A nested left-to-right child would claim the whole
            // remaining width and jam the text back against the count.
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                show_match_details(ui, window_id, state, true, queued);
            });
        }
    });

    if !recovery_shown {
        if rows.is_empty() && effective_freshness.is_none() {
            show_window_freshness(
                ui,
                window_id,
                &WindowFreshness::ReadyEmpty,
                resource_actions,
            );
        } else {
            ui.separator();
        }
    }

    if sorted.is_empty() && !rows.is_empty() {
        egui::Frame::NONE
            .fill(theme::STATUS_BACKGROUND)
            .stroke(egui::Stroke::new(1.0, theme::ACCENT))
            .corner_radius(egui::CornerRadius::same(3))
            .inner_margin(egui::Margin::symmetric(8, 4))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("[?]").strong().color(theme::ACCENT));
                    ui.strong("Filtered empty");
                });
                ui.label("Resources exist, but none match the active filters.");
                ui.label("Use Reset in the toolbar to restore all rows.");
            });
        ui.separator();
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
    // Workload details are a contextual bottom panel: selecting a resource
    // opens it and clearing the selection removes it. `detail_visible` is
    // retained only for backwards-compatible workspace snapshots.
    let detail_shown = state.detail.is_some();

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
                feed.render_time.unwrap_or_else(SystemTime::now),
                kind,
                title,
                namespaced,
                &state.search,
                state.sort.as_ref(),
                &state.hidden_columns,
                state.age_mode,
                &sorted,
                |row| {
                    selected
                        .is_some_and(|selection| *selection == I::from_row_identity(&row.identity))
                },
                |row| I::from_row_identity(&row.identity),
            )
        },
        |ui| {
            if let Some(detail) = state.detail.as_ref()
                && let Some(presentation) =
                    super::detail::presentation::DetailPresentationInput::from_feed(
                        detail,
                        feed,
                        gone,
                        effective_freshness,
                        effective_freshness.is_some_and(WindowFreshness::mutations_allowed),
                    )
            {
                super::detail::show(
                    ui,
                    window_id,
                    detail,
                    &presentation,
                    focused,
                    true,
                    state.prior_split_ratio.is_some(),
                    yaml,
                    streams,
                    dialogs,
                    None,
                    resource_actions,
                    queued,
                );
            }
        },
    );

    if detail_shown && focused {
        let auto_focus = state
            .detail
            .as_ref()
            .is_some_and(|detail| detail.active_tab == DetailTab::Logs)
            && super::split::pane_heights(available_height, ratio, true).1
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
        if let Some(action) = actions.row_action {
            queued.push(action.into_command(window_id));
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
        && !ui.ctx().egui_wants_keyboard_input()
        && ui.input(|input| input.modifiers.any())
        && !gone
    {
        queued.push(WorkspaceCommand::OpenDedicatedDetail(identity));
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

#[cfg(test)]
mod taxonomy_tests {
    use super::*;

    #[test]
    fn every_named_taxonomy_entry_maps_directly_to_its_gvk_and_scope() {
        let expected = [
            (WorkloadKind::Events, "", "Event", true),
            (WorkloadKind::Namespaces, "", "Namespace", false),
            (
                WorkloadKind::Ingresses,
                "networking.k8s.io",
                "Ingress",
                true,
            ),
            (WorkloadKind::Endpoints, "", "Endpoints", true),
            (
                WorkloadKind::NetworkPolicies,
                "networking.k8s.io",
                "NetworkPolicy",
                true,
            ),
            (WorkloadKind::ConfigMaps, "", "ConfigMap", true),
            (WorkloadKind::Secrets, "", "Secret", true),
            (
                WorkloadKind::PersistentVolumeClaims,
                "",
                "PersistentVolumeClaim",
                true,
            ),
            (
                WorkloadKind::PersistentVolumes,
                "",
                "PersistentVolume",
                false,
            ),
            (
                WorkloadKind::StorageClasses,
                "storage.k8s.io",
                "StorageClass",
                false,
            ),
            (WorkloadKind::ServiceAccounts, "", "ServiceAccount", true),
            (
                WorkloadKind::Roles,
                "rbac.authorization.k8s.io",
                "Role",
                true,
            ),
            (
                WorkloadKind::RoleBindings,
                "rbac.authorization.k8s.io",
                "RoleBinding",
                true,
            ),
        ];
        for (kind, group, wire_kind, namespaced) in expected {
            let gvk = builtin_gvk(kind).expect("named entries bypass the custom picker");
            assert_eq!(
                (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()),
                (group, "v1", wire_kind)
            );
            assert_eq!(kind.namespaced(), namespaced);
        }
    }

    #[test]
    fn toolbar_budget_can_overflow_secondary_controls_above_the_old_breakpoint() {
        let required = direct_toolbar_width(700.0, true, true, true, true);
        assert!(
            required > 700.0,
            "a namespaced custom list with active filters needs overflow even at 700 points"
        );
        assert!(
            required < 1_400.0,
            "the same controls return directly once their measured budget fits"
        );
    }
}
