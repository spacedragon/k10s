//! Serializable workspace snapshots for desktop session persistence.
//!
//! A snapshot captures everything that can be restored without live cluster
//! data: which windows are open, their geometry and z-order, and the per-list
//! view settings (namespace filter, search, filters, sort, split). Row
//! selections, dedicated detail windows, YAML buffers, shells, dialogs, and
//! navigation guards are deliberately excluded — they pin resource identities
//! that may no longer exist after a restart.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::resource::{NamespaceScope, ResourceWindowState, SortSpec};
use super::service::ServiceWindowState;
use super::window::{WindowGeom, WindowKind, WorkloadKind};
use super::{Window, WindowContent, WindowId, WorkspaceState};

/// Snapshot format version written to and read from the desktop state file.
pub const SNAPSHOT_VERSION: u32 = 3;

/// Upper bound for persisted allocation counters. Real workspaces hand out
/// a handful of ids per session; values near this ceiling are corruption,
/// not history, and accepting them would overflow on the next open/focus
/// increment in any build.
pub const COUNTER_LIMIT: u64 = u32::MAX as u64;

/// Persisted window kind. `WindowKind::Detail` has no representation on
/// purpose: dedicated windows pin a live resource identity and are never
/// restored (see [`WorkspaceState::snapshot`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedWindowKind {
    Overview,
    Nodes,
    Storage,
    Services,
    Workload(WorkloadKind),
}

/// Per-list view settings that survive a restart for one window. Selection
/// and detail state are intentionally absent (see module docs).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedListView {
    pub namespace_scope: NamespaceScope,
    pub search: String,
    /// Key/value list filters (for example `phase` → `Running`).
    pub filters: BTreeMap<String, String>,
    pub sort: Option<SortSpec>,
    /// Fraction of the window height given to the list pane.
    #[serde(default = "default_split_ratio")]
    pub split_ratio: f32,
    /// Legacy compatibility input. Deserialized values are accepted, but
    /// live restoration and subsequent serialization normalize this to true.
    #[serde(default = "default_true")]
    pub detail_visible: bool,
    /// Selected type of a custom-resources window (`group/version/kind`).
    pub custom_kind: Option<String>,
}

fn default_split_ratio() -> f32 {
    0.5
}

fn default_true() -> bool {
    true
}

/// Sanitize one persisted split ratio; non-finite or out-of-unit-interval
/// values mean the file was tampered with (or came from a different app):
/// fall back to the default instead of rendering a degenerate split.
fn sanitized_split_ratio(ratio: f32) -> f32 {
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        ratio
    } else {
        0.5
    }
}

impl PersistedListView {
    /// View settings extracted from a live list window; selection and detail
    /// state are dropped on purpose.
    fn from_resource<I>(resource: &ResourceWindowState<I>) -> Self {
        Self {
            namespace_scope: resource.namespace_scope.clone(),
            search: resource.search.clone(),
            filters: resource.filters.clone(),
            sort: resource.sort.clone(),
            split_ratio: resource.split_ratio,
            detail_visible: true,
            custom_kind: resource.custom_kind.clone(),
        }
    }

    /// View settings extracted from a live Services window; selection,
    /// detail, and port drafts are dropped on purpose.
    fn from_service<I>(service: &ServiceWindowState<I>) -> Self {
        Self {
            namespace_scope: service.namespace_scope.clone(),
            search: service.search.clone(),
            // The Services window has no key/value filters or GVK picker.
            filters: BTreeMap::new(),
            sort: service.sort.clone(),
            split_ratio: service.split_ratio,
            detail_visible: true,
            custom_kind: None,
        }
    }

    /// Rebuild a fresh list state from persisted view settings. Selection and
    /// detail start empty; the row re-resolves against live data on connect.
    fn into_resource<I>(self) -> ResourceWindowState<I> {
        let split_ratio = sanitized_split_ratio(self.split_ratio);
        ResourceWindowState {
            namespace_scope: self.namespace_scope.into_live(),
            search: self.search,
            filters: self.filters,
            sort: self.sort,
            split_ratio,
            detail_visible: true,
            custom_kind: self.custom_kind,
            ..Default::default()
        }
    }

    /// Rebuild fresh Services state from persisted view settings; selection,
    /// detail, and port drafts start empty.
    fn into_service<I>(self) -> ServiceWindowState<I> {
        let split_ratio = sanitized_split_ratio(self.split_ratio);
        ServiceWindowState {
            namespace_scope: self.namespace_scope.into_live(),
            search: self.search,
            sort: self.sort,
            split_ratio,
            detail_visible: true,
            ..Default::default()
        }
    }
}

/// One persisted window: geometry, z-order, and view settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedWindow {
    pub kind: PersistedWindowKind,
    pub title: String,
    pub geometry: WindowGeom,
    /// Z-order; higher means raised on restore.
    #[serde(default)]
    pub z: u64,
    /// List view settings; present for list windows only.
    pub view: Option<PersistedListView>,
}

/// A complete persistable workspace snapshot. Written by the desktop app and
/// validated here so the shared state module stays the single authority on
/// what a snapshot may contain.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkspaceSnapshot {
    /// Bumped whenever the persisted layout is incompatible; mismatched
    /// snapshots are rejected wholesale by [`WorkspaceState::from_snapshot`].
    pub version: u32,
    /// Next window id to hand out so ids never collide with restored ones.
    pub next_id: u64,
    /// Next z-order value to hand out above every restored window.
    pub next_z: u64,
    /// Whether workspace windows may be resized without preserving aspect ratio.
    pub free_window_resizing: bool,
    /// The restorable windows in their persisted order.
    pub windows: Vec<PersistedWindow>,
}

/// A decoded snapshot plus provenance used by desktop persistence to rewrite
/// migrated files without treating the normalized value as already saved.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedWorkspaceSnapshot {
    pub snapshot: WorkspaceSnapshot,
    pub migrated_from: Option<u32>,
}

#[derive(Deserialize)]
struct VersionEnvelope {
    version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1Snapshot {
    #[serde(rename = "version")]
    _version: u32,
    next_id: u64,
    next_z: u64,
    windows: Vec<V1Window>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1Window {
    kind: PersistedWindowKind,
    title: String,
    geometry: WindowGeom,
    #[serde(default)]
    z: u64,
    view: Option<V1ListView>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1ListView {
    namespace: Option<String>,
    search: String,
    filters: BTreeMap<String, String>,
    sort: Option<SortSpec>,
    #[serde(default = "default_split_ratio")]
    split_ratio: f32,
    #[serde(default = "default_true")]
    detail_visible: bool,
    custom_kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2Snapshot {
    #[serde(rename = "version")]
    _version: u32,
    next_id: u64,
    next_z: u64,
    windows: Vec<V2Window>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V3Snapshot {
    #[serde(rename = "version")]
    _version: u32,
    next_id: u64,
    next_z: u64,
    free_window_resizing: bool,
    windows: Vec<V3Window>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2Window {
    kind: PersistedWindowKind,
    title: String,
    geometry: WindowGeom,
    #[serde(default)]
    z: u64,
    view: Option<V2ListView>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V3Window {
    kind: PersistedWindowKind,
    title: String,
    geometry: WindowGeom,
    #[serde(default)]
    z: u64,
    view: Option<V3ListView>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2ListView {
    namespace_scope: NamespaceScope,
    search: String,
    filters: BTreeMap<String, String>,
    sort: Option<SortSpec>,
    #[serde(default = "default_split_ratio")]
    split_ratio: f32,
    #[serde(default = "default_true")]
    detail_visible: bool,
    custom_kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V3ListView {
    namespace_scope: NamespaceScope,
    search: String,
    filters: BTreeMap<String, String>,
    sort: Option<SortSpec>,
    #[serde(default = "default_split_ratio")]
    split_ratio: f32,
    #[serde(default = "default_true")]
    detail_visible: bool,
    custom_kind: Option<String>,
}

impl<'de> Deserialize<'de> for LoadedWorkspaceSnapshot {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let envelope: VersionEnvelope =
            serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
        let (next_id, next_z, free_window_resizing, windows, migrated_from) = match envelope.version
        {
            1 => {
                let raw: V1Snapshot =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                let windows = raw
                    .windows
                    .into_iter()
                    .map(|window| PersistedWindow {
                        kind: window.kind,
                        title: window.title,
                        geometry: window.geometry,
                        z: window.z,
                        view: window.view.map(|view| PersistedListView {
                            namespace_scope: view
                                .namespace
                                .map(NamespaceScope::Namespace)
                                .unwrap_or(NamespaceScope::AllNamespaces),
                            search: view.search,
                            filters: view.filters,
                            sort: view.sort,
                            split_ratio: view.split_ratio,
                            detail_visible: view.detail_visible,
                            custom_kind: view.custom_kind,
                        }),
                    })
                    .collect();
                (raw.next_id, raw.next_z, false, windows, Some(1))
            }
            2 => {
                let raw: V2Snapshot =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                let windows = raw
                    .windows
                    .into_iter()
                    .map(|window| PersistedWindow {
                        kind: window.kind,
                        title: window.title,
                        geometry: window.geometry,
                        z: window.z,
                        view: window.view.map(|view| PersistedListView {
                            namespace_scope: view.namespace_scope,
                            search: view.search,
                            filters: view.filters,
                            sort: view.sort,
                            split_ratio: view.split_ratio,
                            detail_visible: view.detail_visible,
                            custom_kind: view.custom_kind,
                        }),
                    })
                    .collect();
                (raw.next_id, raw.next_z, false, windows, Some(2))
            }
            SNAPSHOT_VERSION => {
                let raw: V3Snapshot =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                let windows = raw
                    .windows
                    .into_iter()
                    .map(|window| PersistedWindow {
                        kind: window.kind,
                        title: window.title,
                        geometry: window.geometry,
                        z: window.z,
                        view: window.view.map(|view| PersistedListView {
                            namespace_scope: view.namespace_scope,
                            search: view.search,
                            filters: view.filters,
                            sort: view.sort,
                            split_ratio: view.split_ratio,
                            detail_visible: view.detail_visible,
                            custom_kind: view.custom_kind,
                        }),
                    })
                    .collect();
                (
                    raw.next_id,
                    raw.next_z,
                    raw.free_window_resizing,
                    windows,
                    None,
                )
            }
            version => {
                return Err(serde::de::Error::custom(format!(
                    "unsupported workspace snapshot version {version}"
                )));
            }
        };
        Ok(Self {
            snapshot: WorkspaceSnapshot {
                version: SNAPSHOT_VERSION,
                next_id,
                next_z,
                free_window_resizing,
                windows,
            },
            migrated_from,
        })
    }
}

impl<'de> Deserialize<'de> for WorkspaceSnapshot {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(LoadedWorkspaceSnapshot::deserialize(deserializer)?.snapshot)
    }
}

impl WorkspaceSnapshot {
    /// Whether this snapshot matches the format version this build reads and
    /// writes; mismatched snapshots must be ignored rather than misread.
    #[must_use]
    pub fn is_current_version(&self) -> bool {
        self.version == SNAPSHOT_VERSION
    }
}

impl PersistedWindow {
    /// The live window kind, `None` when this entry cannot be restored
    /// (defensive against hand-edited or stale files).
    fn restorable_kind(&self) -> Option<WindowKind> {
        match self.kind {
            PersistedWindowKind::Overview => Some(WindowKind::Overview),
            PersistedWindowKind::Nodes => Some(WindowKind::Nodes),
            PersistedWindowKind::Storage => Some(WindowKind::Storage),
            PersistedWindowKind::Services => Some(WindowKind::Services),
            PersistedWindowKind::Workload(kind) => Some(WindowKind::Workload(kind)),
        }
    }

    /// Whether the persisted geometry is finite and plausible; entries that
    /// fail this check are skipped on restore.
    fn has_sane_geometry(&self) -> bool {
        self.geometry.position.iter().all(|value| value.is_finite())
            && self
                .geometry
                .size
                .iter()
                .all(|value| value.is_finite() && *value > 0.0)
    }
}

/// Default window size for one freshly opened list window, mirroring the
/// interactive sizes in [`WorkspaceState`]; a degenerate persisted geometry
/// degrades to this normal first-launch layout instead of rendering empty.
fn default_size_for(kind: WindowKind) -> [f32; 2] {
    match kind {
        WindowKind::Overview | WindowKind::Nodes | WindowKind::Storage | WindowKind::Services => {
            [840.0, 560.0]
        }
        _ => [700.0, 480.0],
    }
}

impl<I> WorkspaceState<I>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    /// Snapshot the workspace for persistence. Detail windows and every
    /// selection/detail/YAML/shell/dialog state are excluded (see module docs).
    #[must_use]
    pub fn snapshot(&self) -> WorkspaceSnapshot {
        let mut windows = Vec::new();
        for window in &self.windows {
            // The invariant behind the workspace commands: non-detail kinds
            // hold list bodies and detail kinds pin a live identity. Anything
            // else is skipped defensively instead of persisted.
            let (kind, view) = match (&window.kind, &window.content) {
                (WindowKind::Overview, WindowContent::Resource(resource)) => (
                    PersistedWindowKind::Overview,
                    Some(PersistedListView::from_resource(resource)),
                ),
                (WindowKind::Nodes, WindowContent::Resource(resource)) => (
                    PersistedWindowKind::Nodes,
                    Some(PersistedListView::from_resource(resource)),
                ),
                (WindowKind::Storage, WindowContent::Resource(resource)) => (
                    PersistedWindowKind::Storage,
                    Some(PersistedListView::from_resource(resource)),
                ),
                (WindowKind::Services, WindowContent::Services(service)) => (
                    PersistedWindowKind::Services,
                    Some(PersistedListView::from_service(service)),
                ),
                (WindowKind::Workload(w), WindowContent::Resource(resource)) => (
                    PersistedWindowKind::Workload(*w),
                    Some(PersistedListView::from_resource(resource)),
                ),
                _ => continue,
            };
            windows.push(PersistedWindow {
                kind,
                title: window.title.clone(),
                geometry: window.geometry,
                z: window.z,
                view,
            });
        }
        WorkspaceSnapshot {
            version: SNAPSHOT_VERSION,
            next_id: self.next_id,
            next_z: self.next_z,
            free_window_resizing: self.free_window_resizing,
            windows,
        }
    }

    /// Build a workspace from one persisted snapshot. Returns `None` when the
    /// snapshot is not on the current format version; individual malformed or
    /// unrestoreable entries are skipped defensively while healthy ones land.
    ///
    /// Restored windows receive fresh ids (ids are local handles, never
    /// referenced across sessions) and keep their persisted z-order verbatim,
    /// so a healthy file round-trips unchanged on relaunch. `next_id`/`next_z`
    /// continue strictly above everything restored so no id or z is reused.
    #[must_use]
    pub fn from_snapshot(snapshot: &WorkspaceSnapshot) -> Option<Self> {
        if !snapshot.is_current_version() || Self::counters_overflow(snapshot) {
            return None;
        }

        // Keep the file's entry order verbatim (rendering and stacking read
        // z, not vec position) so a healthy file round-trips unchanged on
        // relaunch. Entries with out-of-range or unrestorable fields are
        // dropped: they would overflow future increments or misrender.
        let restorable: Vec<&PersistedWindow> = snapshot
            .windows
            .iter()
            .filter(|window| {
                window.restorable_kind().is_some()
                    && window.has_sane_geometry()
                    && window.z <= COUNTER_LIMIT
            })
            .collect();

        let mut state = Self {
            windows: Vec::new(),
            free_window_resizing: snapshot.free_window_resizing,
            next_id: 1,
            next_z: 0,
            context: String::new(),
            yaml_owner: std::collections::HashMap::new(),
            pending: None,
            layout_checkpoint: None,
            next_layout_revision: 0,
        };

        for window in &restorable {
            let kind = window.restorable_kind().expect("filtered above");
            // Below any rendered minimum; egui would expand it anyway, so
            // keep the restored layout self-consistent with what renders.
            let size = if window.geometry.size[0] < 1.0 || window.geometry.size[1] < 1.0 {
                default_size_for(kind)
            } else {
                window.geometry.size
            };
            let geometry = WindowGeom {
                position: [window.geometry.position[0], window.geometry.position[1]],
                size,
                collapsed: window.geometry.collapsed,
            };

            let id = WindowId(state.next_id);
            state.next_id += 1;
            // Each kind gets the body shape it was opened with; view settings
            // rehydrate only what that body can hold.
            let content = match kind {
                WindowKind::Services => match &window.view {
                    Some(view) => WindowContent::Services(view.clone().into_service()),
                    None => WindowContent::Services(ServiceWindowState::default()),
                },
                _ => match &window.view {
                    Some(view) => WindowContent::Resource(view.clone().into_resource()),
                    None => WindowContent::Resource(ResourceWindowState::default()),
                },
            };
            state.windows.push(Window {
                id,
                kind,
                title: window.title.clone(),
                geometry,
                layout_revision: 0,
                z: window.z,
                content,
            });
        }

        // Continue allocation strictly above everything the file claims, so
        // ids and z never collide with what an older session handed out.
        state.next_id = state.next_id.max(snapshot.next_id);
        let highest_restored_z = restorable.iter().map(|window| window.z).max().unwrap_or(0);
        state.next_z = state
            .next_z
            .max(snapshot.next_z)
            .max(highest_restored_z + 1);

        Some(state)
    }

    /// Whether either persisted allocation counter is close enough to the
    /// u64 ceiling that a normal open/focus increment would overflow.
    fn counters_overflow(snapshot: &WorkspaceSnapshot) -> bool {
        snapshot.next_id > COUNTER_LIMIT || snapshot.next_z > COUNTER_LIMIT
    }
}
