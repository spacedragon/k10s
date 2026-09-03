//! Command-driven workspace state.
//!
//! Pure Rust data shared by the native and web targets: open windows,
//! focus order, per-window resource state, pinned dedicated details, and
//! the navigation guards. No egui initialization is required to exercise
//! any behavior here — the UI shell (Task 3) renders this state and applies
//! queued [`WorkspaceCommand`]s after each frame.
//!
//! The state is generic over the resource identity type `I` so this module
//! stays independent of the protocol crate; production code instantiates it
//! with the protocol `ResourceIdentity`.

mod detail;
mod guard;
mod port_forward;
mod resource;
mod snapshot;

mod service;
mod window;

pub use detail::{DetailState, DetailTab, YamlState};
pub use guard::{BlockReason, BlockResolution, Blocker, PendingNavigation};
pub use port_forward::PortForwardWindowState;
pub use resource::{AgeMode, NamespaceScope, ResourceWindowState, SortSpec};
pub use snapshot::{
    COUNTER_LIMIT, LoadedWorkspaceSnapshot, PersistedListView, PersistedPortForwardView,
    PersistedWindow, PersistedWindowKind, SNAPSHOT_VERSION, WorkspaceSnapshot,
};

pub use service::ServiceWindowState;
pub use window::{Window, WindowContent, WindowGeom, WindowId, WindowKind, WorkloadKind};

use std::collections::HashMap;

/// Items in the fixed left launcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LauncherItem {
    Overview,
    Nodes,
    Storage,
    Services,
    PortForwards,
    Workload(WorkloadKind),
}

/// Commands applied to the workspace state. Destructive navigations (row
/// selection, selection clearing, window close, context switch) are blocked
/// while the affected detail has a dirty YAML buffer;
/// the blocking command parks as a [`PendingNavigation`] and commits only
/// after every blocker is resolved via [`WorkspaceCommand::ResolveBlock`].
#[derive(Debug, Clone, PartialEq)]
pub enum WorkspaceCommand<I> {
    /// Replace the entire workspace with a restored snapshot. Used by native
    /// desktop hosts at startup to reopen the user's previous session.
    RestoreSnapshot(WorkspaceSnapshot),
    /// Launcher click: open the first instance or focus/raise the existing
    /// one (singleton) or the most recently used one (workload).
    ActivateLauncherItem(LauncherItem),
    /// Launcher `+`: open another independent workload instance.
    AddWorkloadInstance(WorkloadKind),
    /// Toggle whether workspace windows may be resized freely.
    ToggleFreeWindowResizing,
    /// Command-palette modified activation: open another independent list.
    AddListInstance(LauncherItem),
    /// Raise a window above the others.
    FocusWindow(WindowId),
    CloseWindow(WindowId),
    SetGeometry(WindowId, WindowGeom),
    /// Arrange all windows in a deterministic grid inside `[width, height]`.
    Tile([f32; 2]),
    /// Toggle a deterministic cascade, restoring the prior geometry on repeat.
    Cascade([f32; 2]),
    /// Toggle focus mode for the active window, restoring all prior geometry.
    ToggleFocus([f32; 2]),
    /// Focus the next window in registry MRU order.
    CycleWindow,
    SetNamespaceScope(WindowId, NamespaceScope),
    /// Set or clear the toolbar status filter for one list window.
    SetStatusFilter(WindowId, Option<String>),
    /// Toggle a user-hidden column through the toolbar Columns menu.
    ToggleColumnVisibility(WindowId, String),
    /// Switch the Age column between relative and absolute rendering.
    SetAgeMode(WindowId, AgeMode),
    SetSearch(WindowId, String),
    SetServicePortDraft(WindowId, String, String),
    SetFilter(WindowId, String, String),
    SetSort(WindowId, Option<SortSpec>),
    SetPortForwardSort(WindowId, Option<SortSpec>),
    FocusPortForwardSession(WindowId, String),
    ClearPortForwardSessionFocus(WindowId),
    SetSplitRatio(WindowId, f32),
    /// Give the integrated detail the full content area while remembering
    /// the user's current split for an explicit restore.
    MaximizeDetailPane(WindowId),
    /// Return from a focused detail view to the remembered split.
    RestoreDetailPane(WindowId),
    /// Legacy compatibility command. It only normalizes `detail_visible` to
    /// true and cannot hide a selected integrated Detail.
    ToggleDetailPane(WindowId),
    /// Pick (or clear) the resource type of a custom-resources window. The
    /// key is the canonical `group/version/kind` string of a picker entry.
    SetCustomKind(WindowId, Option<String>),
    /// Single-click row selection; updates the integrated detail pane.
    SelectRow(WindowId, I),
    ClearSelection(WindowId),
    /// Double-click / context-menu: a dedicated window pinned to `I`.
    OpenDedicatedDetail(I),
    SetActiveTab(WindowId, DetailTab),
    BeginYamlEdit(WindowId),
    DiscardYaml(WindowId),
    StartPortForward {
        target: k10s_protocol::PortForwardTarget,
        remote_label: String,
        initial_local_port: u16,
    },
    StopPortForward(String),
    /// Global context switch. Preserves window kinds, geometry, filters,
    /// and splits; clears selections and closes pinned detail windows.
    ///
    /// Requesting a switch only validates local navigation guards; the
    /// workspace state moves solely through [`WorkspaceCommand::
    /// CommitContextSwitch`] once the backend confirmed the destination.
    ContextSwitch {
        to: String,
    },
    /// Commit a backend-validated context switch locally. Never send this
    /// before the switch request succeeded.
    CommitContextSwitch {
        to: String,
    },
    ResolveBlock(BlockResolution),
}

/// Observable outcomes of a command.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkspaceEvent<I> {
    Opened(WindowId),
    Closed(WindowId),
    Focused(WindowId),
    /// The navigation did not execute; see [`PendingNavigation`].
    Blocked(PendingNavigation<I>),
    /// Another window already owns the writable YAML buffer for this
    /// identity; this view stays read-only.
    YamlOwnerInUse {
        owner: WindowId,
    },
    /// The switch request cleared every local navigation guard and now waits
    /// for the backend to validate the destination. The workspace state is
    /// untouched until the application layer commits.
    ContextSwitchRequested {
        to: String,
    },
    /// A context switch committed: the workspace now serves `to`. Emitted
    /// by [`WorkspaceCommand::CommitContextSwitch`]; a blocked and later
    /// resolved switch emits it once its commit runs. `Cancel` never
    /// emits it.
    ContextSwitched {
        to: String,
    },
    PortForwardStartRequested {
        target: k10s_protocol::PortForwardTarget,
        remote_label: String,
        initial_local_port: u16,
    },
    PortForwardStopRequested(String),
}

/// The complete workspace state.
#[derive(Debug, Clone)]
pub struct WorkspaceState<I> {
    windows: Vec<Window<I>>,
    free_window_resizing: bool,
    next_id: u64,
    next_z: u64,
    /// Active context; empty until the first switch commits.
    context: String,
    /// Writable-YAML ownership: at most one editor per resource identity.
    yaml_owner: HashMap<I, WindowId>,
    pending: Option<PendingNavigation<I>>,
    layout_checkpoint: Option<Vec<(WindowId, WindowGeom)>>,
    /// Monotonic live-render token; intentionally not persisted.
    next_layout_revision: u64,
}

impl<I> Default for WorkspaceState<I>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I> WorkspaceState<I>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    /// A fresh workspace with the Overview window open — the only window on
    /// first launch.
    pub fn new() -> Self {
        let mut state = Self {
            windows: Vec::new(),
            free_window_resizing: false,
            next_id: 1,
            next_z: 1,
            context: String::new(),
            yaml_owner: HashMap::new(),
            pending: None,
            layout_checkpoint: None,
            next_layout_revision: 0,
        };
        state.open_singleton(WindowKind::Overview);
        state
    }

    pub fn windows(&self) -> &[Window<I>] {
        &self.windows
    }

    /// Whether workspace windows may be resized freely.
    #[must_use]
    pub fn free_window_resizing(&self) -> bool {
        self.free_window_resizing
    }

    pub fn window(&self, id: WindowId) -> Option<&Window<I>> {
        self.windows.iter().find(|window| window.id == id)
    }

    /// The active context; empty until the first context switch commits.
    pub fn context(&self) -> &str {
        &self.context
    }

    /// The navigation currently waiting on guard resolution, if any.
    pub fn pending(&self) -> Option<&PendingNavigation<I>> {
        self.pending.as_ref()
    }

    /// The window holding the writable YAML buffer for `identity`, if any.
    pub fn yaml_owner(&self, identity: &I) -> Option<WindowId> {
        self.yaml_owner.get(identity).copied()
    }

    /// The list-window-local resource state of one window, if it is a
    /// resource list.
    pub fn resource_state(&self, id: WindowId) -> Option<&ResourceWindowState<I>> {
        match self.window(id).map(|window| &window.content) {
            Some(WindowContent::Resource(resource)) => Some(resource),
            _ => None,
        }
    }

    /// The window-local state of the singleton Services window.
    pub fn service_state(&self, id: WindowId) -> Option<&ServiceWindowState<I>> {
        match self.window(id).map(|window| &window.content) {
            Some(WindowContent::Services(service)) => Some(service),
            _ => None,
        }
    }

    /// Mutable Services window state, for callers outside the command
    /// vocabulary (port-forward drafts).
    pub fn service_state_mut(&mut self, id: WindowId) -> Option<&mut ServiceWindowState<I>> {
        match self.window_mut(id).map(|window| &mut window.content) {
            Some(WindowContent::Services(service)) => Some(service),
            _ => None,
        }
    }

    /// View state of the singleton Port Forwards window.
    pub fn port_forward_state(&self, id: WindowId) -> Option<&PortForwardWindowState> {
        match self.window(id).map(|window| &window.content) {
            Some(WindowContent::PortForwards(state)) => Some(state),
            _ => None,
        }
    }

    /// Number of open list-window instances for a workload kind.
    pub fn instance_count(&self, kind: WorkloadKind) -> usize {
        self.windows
            .iter()
            .filter(|window| window.kind == WindowKind::Workload(kind))
            .count()
    }

    /// Whether a launcher item is highlighted (window open for singletons,
    /// at least one instance for workloads).
    pub fn launcher_highlight(&self, item: LauncherItem) -> bool {
        match item {
            LauncherItem::Overview => self.has_kind(WindowKind::Overview),
            LauncherItem::Nodes => self.has_kind(WindowKind::Nodes),
            LauncherItem::Storage => self.has_kind(WindowKind::Storage),
            LauncherItem::Services => self.has_kind(WindowKind::Services),
            LauncherItem::PortForwards => self.has_kind(WindowKind::PortForwards),
            LauncherItem::Workload(kind) => self.instance_count(kind) > 0,
        }
    }

    /// Apply one command and return the observable events.
    ///
    /// While a pending navigation waits on guard resolution, every other
    /// command is held back until the user resolves or cancels it.
    pub fn apply(&mut self, command: WorkspaceCommand<I>) -> Vec<WorkspaceEvent<I>> {
        if let WorkspaceCommand::ResolveBlock(resolution) = command {
            return self.resolve(resolution);
        }
        if self.pending.is_some() {
            return Vec::new();
        }
        self.dispatch(command)
    }

    fn has_kind(&self, kind: WindowKind) -> bool {
        self.windows.iter().any(|window| window.kind == kind)
    }

    // -- dispatch ----------------------------------------------------------

    fn dispatch(&mut self, command: WorkspaceCommand<I>) -> Vec<WorkspaceEvent<I>> {
        match command {
            // A restore replaces the entire workspace. `apply` already holds
            // it back while a navigation guard is pending; mismatched or
            // malformed snapshots leave the current state untouched.
            WorkspaceCommand::RestoreSnapshot(snapshot) => {
                if let Some(mut restored) = Self::from_snapshot(&snapshot) {
                    let revision = self.next_layout_revision.wrapping_add(1);
                    restored.next_layout_revision = revision;
                    for window in &mut restored.windows {
                        window.layout_revision = revision;
                    }
                    *self = restored;
                }
                Vec::new()
            }
            WorkspaceCommand::ActivateLauncherItem(item) => self.activate(item),
            WorkspaceCommand::AddWorkloadInstance(kind) => {
                let id = self.open_workload(kind);
                vec![WorkspaceEvent::Opened(id)]
            }
            WorkspaceCommand::ToggleFreeWindowResizing => {
                self.free_window_resizing = !self.free_window_resizing;
                Vec::new()
            }
            WorkspaceCommand::AddListInstance(item) => {
                let id = match item {
                    LauncherItem::Workload(kind) => self.open_workload(kind),
                    LauncherItem::Services => self.open_singleton(WindowKind::Services),
                    _ => return self.activate(item),
                };
                vec![WorkspaceEvent::Opened(id)]
            }
            WorkspaceCommand::FocusWindow(id) => self.focus(id),
            WorkspaceCommand::CloseWindow(id) => self.close_window(id),
            WorkspaceCommand::SetGeometry(id, geometry) => {
                if let Some(window) = self.window_mut(id) {
                    window.geometry = geometry;
                    window.initial_geometry = false;
                }
                Vec::new()
            }
            WorkspaceCommand::Tile(size) => {
                self.layout_checkpoint = None;
                self.tile(size);
                Vec::new()
            }
            WorkspaceCommand::Cascade(size) => {
                self.toggle_layout(|window, index, _count| {
                    WindowGeom::cascade(index, size, window.kind.min_size())
                });
                Vec::new()
            }
            WorkspaceCommand::ToggleFocus(size) => {
                if self.restore_layout() {
                    return Vec::new();
                }
                self.save_layout();
                if let Some(active) = self
                    .windows
                    .iter()
                    .max_by_key(|window| window.z)
                    .map(|w| w.id)
                {
                    let revision = self.bump_layout_revision();
                    if let Some(window) = self.window_mut(active) {
                        window.geometry = WindowGeom::focused(size, window.kind.min_size());
                        window.layout_revision = revision;
                    }
                }
                Vec::new()
            }
            WorkspaceCommand::CycleWindow => self.cycle_window(),
            WorkspaceCommand::SetNamespaceScope(id, scope) => self.set_namespace_scope(id, scope),
            WorkspaceCommand::SetStatusFilter(id, filter) => {
                self.with_resource_mut(id, |resource| resource.status_filter = filter);
                Vec::new()
            }
            WorkspaceCommand::ToggleColumnVisibility(id, column) => {
                self.with_resource_mut(id, |resource| {
                    if !resource.hidden_columns.remove(&column) {
                        resource.hidden_columns.insert(column);
                    }
                });
                Vec::new()
            }
            WorkspaceCommand::SetAgeMode(id, mode) => {
                self.with_resource_mut(id, |resource| resource.age_mode = mode);
                Vec::new()
            }
            WorkspaceCommand::SetSearch(id, search) => {
                self.with_resource_mut(id, |resource| resource.search = search.clone());
                self.with_service_mut(id, |service| service.search = search);
                Vec::new()
            }
            WorkspaceCommand::SetServicePortDraft(id, key, value) => {
                self.with_service_mut(id, |service| {
                    service.port_drafts.insert(key, value);
                });
                Vec::new()
            }
            WorkspaceCommand::SetFilter(id, key, value) => {
                self.with_resource_mut(id, |resource| {
                    resource.filters.insert(key, value);
                });
                Vec::new()
            }
            WorkspaceCommand::SetSort(id, sort) => {
                self.with_resource_mut(id, |resource| resource.sort = sort.clone());
                self.with_service_mut(id, |service| service.sort = sort);
                Vec::new()
            }
            WorkspaceCommand::SetPortForwardSort(id, sort) => {
                self.with_port_forward_mut(id, |state| state.sort = sort);
                Vec::new()
            }
            WorkspaceCommand::FocusPortForwardSession(id, session_id) => {
                self.with_port_forward_mut(id, |state| {
                    state.focused_session = Some(session_id);
                });
                Vec::new()
            }
            WorkspaceCommand::ClearPortForwardSessionFocus(id) => {
                self.with_port_forward_mut(id, |state| state.focused_session = None);
                Vec::new()
            }
            WorkspaceCommand::SetSplitRatio(id, ratio) => {
                let clamped = ratio.clamp(0.0, 1.0);
                self.with_resource_mut(id, move |resource| resource.split_ratio = clamped);
                self.with_service_mut(id, move |service| service.split_ratio = clamped);
                Vec::new()
            }
            WorkspaceCommand::MaximizeDetailPane(id) => {
                self.with_resource_mut(id, |resource| {
                    resource
                        .prior_split_ratio
                        .get_or_insert(resource.split_ratio);
                });
                self.with_service_mut(id, |service| {
                    service.prior_split_ratio.get_or_insert(service.split_ratio);
                });
                Vec::new()
            }
            WorkspaceCommand::RestoreDetailPane(id) => {
                self.with_resource_mut(id, |resource| {
                    if let Some(ratio) = resource.prior_split_ratio.take() {
                        resource.split_ratio = ratio;
                    }
                });
                self.with_service_mut(id, |service| {
                    if let Some(ratio) = service.prior_split_ratio.take() {
                        service.split_ratio = ratio;
                    }
                });
                Vec::new()
            }
            WorkspaceCommand::ToggleDetailPane(id) => {
                self.with_resource_mut(id, |resource| {
                    resource.detail_visible = true;
                });
                self.with_service_mut(id, |service| {
                    service.detail_visible = true;
                });
                Vec::new()
            }
            WorkspaceCommand::SetCustomKind(id, kind) => {
                self.with_resource_mut(id, |resource| {
                    resource.custom_kind = kind;
                });
                Vec::new()
            }
            WorkspaceCommand::SelectRow(id, identity) => self.select_row(id, identity),
            WorkspaceCommand::ClearSelection(id) => self.clear_selection(id),
            WorkspaceCommand::OpenDedicatedDetail(identity) => {
                let id = self.open_detail(identity);
                vec![WorkspaceEvent::Opened(id)]
            }
            WorkspaceCommand::SetActiveTab(id, tab) => {
                self.with_detail_mut(id, |detail| detail.active_tab = tab);
                Vec::new()
            }
            WorkspaceCommand::BeginYamlEdit(id) => self.begin_yaml_edit(id),
            WorkspaceCommand::DiscardYaml(id) => {
                self.discard_yaml(id);
                Vec::new()
            }
            WorkspaceCommand::StartPortForward {
                target,
                remote_label,
                initial_local_port,
            } => vec![WorkspaceEvent::PortForwardStartRequested {
                target,
                remote_label,
                initial_local_port,
            }],
            WorkspaceCommand::StopPortForward(id) => {
                vec![WorkspaceEvent::PortForwardStopRequested(id)]
            }
            WorkspaceCommand::ContextSwitch { to } => self.context_switch(to),
            WorkspaceCommand::CommitContextSwitch { to } => self.commit_context_switch(to),
            WorkspaceCommand::ResolveBlock(_) => Vec::new(), // handled in `apply`
        }
    }

    // -- launcher and windows ---------------------------------------------

    fn activate(&mut self, item: LauncherItem) -> Vec<WorkspaceEvent<I>> {
        match item {
            LauncherItem::Overview => self.activate_singleton(WindowKind::Overview),
            LauncherItem::Nodes => self.activate_singleton(WindowKind::Nodes),
            LauncherItem::Storage => self.activate_singleton(WindowKind::Storage),
            LauncherItem::Services => self.activate_singleton(WindowKind::Services),
            LauncherItem::PortForwards => self.activate_singleton(WindowKind::PortForwards),
            LauncherItem::Workload(kind) => {
                let mru = self
                    .windows
                    .iter()
                    .filter(|window| window.kind == WindowKind::Workload(kind))
                    .max_by_key(|window| window.z)
                    .map(|window| window.id);
                match mru {
                    Some(id) => self.focus(id),
                    None => {
                        let id = self.open_workload(kind);
                        vec![WorkspaceEvent::Opened(id)]
                    }
                }
            }
        }
    }

    fn activate_singleton(&mut self, kind: WindowKind) -> Vec<WorkspaceEvent<I>> {
        let existing = self
            .windows
            .iter()
            .filter(|window| window.kind == kind)
            .max_by_key(|window| window.z)
            .map(|window| window.id);
        match existing {
            Some(id) => self.focus(id),
            None => {
                let id = self.open_singleton(kind);
                vec![WorkspaceEvent::Opened(id)]
            }
        }
    }

    fn focus(&mut self, id: WindowId) -> Vec<WorkspaceEvent<I>> {
        let Some(index) = self.windows.iter().position(|window| window.id == id) else {
            return Vec::new();
        };
        self.next_z += 1;
        self.windows[index].z = self.next_z;
        vec![WorkspaceEvent::Focused(id)]
    }

    fn cycle_window(&mut self) -> Vec<WorkspaceEvent<I>> {
        let mut ids: Vec<_> = self
            .windows
            .iter()
            .map(|window| (window.z, window.id))
            .collect();
        ids.sort_by_key(|(z, _)| std::cmp::Reverse(*z));
        match ids.as_slice() {
            [] => Vec::new(),
            [_] => self.focus(ids[0].1),
            _ => self.focus(ids[1].1),
        }
    }

    fn save_layout(&mut self) {
        self.layout_checkpoint = Some(
            self.windows
                .iter()
                .map(|window| (window.id, window.geometry))
                .collect(),
        );
    }

    fn bump_layout_revision(&mut self) -> u64 {
        self.next_layout_revision = self.next_layout_revision.wrapping_add(1);
        self.next_layout_revision
    }

    fn restore_layout(&mut self) -> bool {
        let Some(saved) = self.layout_checkpoint.take() else {
            return false;
        };
        let revision = self.bump_layout_revision();
        for (id, geometry) in saved {
            if let Some(window) = self.window_mut(id) {
                window.geometry = geometry;
                window.layout_revision = revision;
            }
        }
        true
    }

    fn toggle_layout(&mut self, geometry: impl Fn(&Window<I>, usize, usize) -> WindowGeom) {
        if self.restore_layout() {
            return;
        }
        self.save_layout();
        let count = self.windows.len();
        let revision = self.bump_layout_revision();
        for (index, window) in self.windows.iter_mut().enumerate() {
            window.geometry = geometry(window, index, count);
            window.layout_revision = revision;
        }
    }

    fn tile(&mut self, size: [f32; 2]) {
        let count = self.windows.len();
        if count == 0 {
            return;
        }
        let minimum = self
            .windows
            .iter()
            .fold([0.0_f32, 0.0_f32], |minimum, window| {
                let window_minimum = window.kind.min_size();
                [
                    minimum[0].max(window_minimum[0]),
                    minimum[1].max(window_minimum[1]),
                ]
            });
        let revision = self.bump_layout_revision();
        for (index, window) in self.windows.iter_mut().enumerate() {
            window.geometry = WindowGeom::tiled(index, count, size, minimum);
            window.layout_revision = revision;
        }
    }

    fn open_singleton(&mut self, kind: WindowKind) -> WindowId {
        let size = match kind {
            WindowKind::Overview
            | WindowKind::Nodes
            | WindowKind::Storage
            | WindowKind::Services
            | WindowKind::PortForwards => [840.0, 560.0],
            _ => [700.0, 480.0],
        };
        let content = match kind {
            // Services is a singleton list window with its own state shape;
            // every other singleton renders a generic resource list.
            WindowKind::Services => WindowContent::Services(ServiceWindowState::default()),
            WindowKind::PortForwards => {
                WindowContent::PortForwards(PortForwardWindowState::default())
            }
            _ => WindowContent::Resource(ResourceWindowState::default()),
        };
        self.push_window(kind, kind.title().to_owned(), size, content)
    }

    fn open_workload(&mut self, kind: WorkloadKind) -> WindowId {
        let window_kind = WindowKind::Workload(kind);
        self.push_window(
            window_kind,
            kind.title().to_owned(),
            [700.0, 480.0],
            WindowContent::Resource(ResourceWindowState::default()),
        )
    }

    fn open_detail(&mut self, identity: I) -> WindowId {
        self.push_window(
            WindowKind::Detail,
            "Detail".to_owned(),
            [640.0, 520.0],
            WindowContent::Detail(DetailState::new(identity)),
        )
    }

    fn push_window(
        &mut self,
        kind: WindowKind,
        title: String,
        size: [f32; 2],
        content: WindowContent<I>,
    ) -> WindowId {
        let id = WindowId(self.next_id);
        self.next_id += 1;
        let index = self.windows.len();
        let geometry = WindowGeom::staggered(index, size);
        self.next_z += 1;
        let z = self.next_z;
        self.windows.push(Window {
            id,
            kind,
            title,
            geometry,
            initial_geometry: true,
            layout_revision: 0,
            z,
            content,
        });
        id
    }

    fn close_window(&mut self, id: WindowId) -> Vec<WorkspaceEvent<I>> {
        if self.window(id).is_none() {
            return Vec::new();
        }
        let blockers = self.blockers_for(id);
        if !blockers.is_empty() {
            return self.block(WorkspaceCommand::CloseWindow(id), blockers);
        }
        self.remove_window(id);
        vec![WorkspaceEvent::Closed(id)]
    }

    fn remove_window(&mut self, id: WindowId) {
        self.windows.retain(|window| window.id != id);
        let owned: Vec<I> = self
            .yaml_owner
            .iter()
            .filter(|&(_, &owner)| owner == id)
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in owned {
            self.yaml_owner.remove(&identity);
        }
    }

    // -- navigation guards -------------------------------------------------

    fn block(
        &mut self,
        command: WorkspaceCommand<I>,
        blockers: Vec<Blocker>,
    ) -> Vec<WorkspaceEvent<I>> {
        debug_assert!(!blockers.is_empty());
        let pending = PendingNavigation { command, blockers };
        self.pending = Some(pending.clone());
        vec![WorkspaceEvent::Blocked(pending)]
    }

    fn blockers_for(&self, id: WindowId) -> Vec<Blocker> {
        let Some(window) = self.window(id) else {
            return Vec::new();
        };
        match &window.content {
            WindowContent::Resource(resource) => resource
                .detail
                .as_ref()
                .map(|detail| detail.blockers(id))
                .unwrap_or_default(),
            WindowContent::Services(service) => service
                .detail
                .as_ref()
                .map(|detail| detail.blockers(id))
                .unwrap_or_default(),
            WindowContent::Detail(detail) => detail.blockers(id),
            WindowContent::PortForwards(_) => Vec::new(),
        }
    }

    fn resolve(&mut self, resolution: BlockResolution) -> Vec<WorkspaceEvent<I>> {
        let Some(mut pending) = self.pending.take() else {
            return Vec::new();
        };
        match resolution {
            BlockResolution::Cancel => Vec::new(),
            BlockResolution::DiscardYaml { window } => {
                self.discard_yaml(window);
                pending.blockers.retain(|blocker| {
                    !(blocker.window == window && blocker.reason == BlockReason::DirtyYaml)
                });
                self.finish_pending(pending)
            }
        }
    }

    fn finish_pending(&mut self, pending: PendingNavigation<I>) -> Vec<WorkspaceEvent<I>> {
        if pending.blockers.is_empty() {
            self.execute(pending.command)
        } else {
            self.pending = Some(pending);
            Vec::new()
        }
    }

    /// Run a previously blocked navigation. All its blockers are resolved,
    /// so it must commit without blocking again.
    fn execute(&mut self, command: WorkspaceCommand<I>) -> Vec<WorkspaceEvent<I>> {
        match command {
            WorkspaceCommand::SelectRow(id, identity) => self.select_row(id, identity),
            WorkspaceCommand::ClearSelection(id) => self.clear_selection(id),
            WorkspaceCommand::CloseWindow(id) => self.close_window(id),
            WorkspaceCommand::ContextSwitch { to } => self.context_switch(to),
            WorkspaceCommand::SetNamespaceScope(id, scope) => self.set_namespace_scope(id, scope),
            other => self.dispatch(other),
        }
    }

    fn set_namespace_scope(
        &mut self,
        id: WindowId,
        scope: NamespaceScope,
    ) -> Vec<WorkspaceEvent<I>> {
        let current = self
            .resource_state(id)
            .map(|s| &s.namespace_scope)
            .or_else(|| self.service_state(id).map(|s| &s.namespace_scope));
        if current.is_none_or(|current| current == &scope) {
            return Vec::new();
        }
        let blockers = self.blockers_for(id);
        if !blockers.is_empty() {
            return self.block(WorkspaceCommand::SetNamespaceScope(id, scope), blockers);
        }
        self.with_resource_mut(id, |state| {
            state.namespace_scope = scope.clone();
            state.selection = None;
            state.detail = None;
        });
        self.with_service_mut(id, |state| {
            state.namespace_scope = scope;
            state.selection = None;
            state.detail = None;
        });
        Vec::new()
    }

    // -- selections ----------------------------------------------------------

    fn select_row(&mut self, id: WindowId, identity: I) -> Vec<WorkspaceEvent<I>> {
        let Some(window) = self.window(id) else {
            return Vec::new();
        };
        let already_selected = match &window.content {
            WindowContent::Resource(resource) => resource.selection.as_ref() == Some(&identity),
            WindowContent::Services(service) => service.selection.as_ref() == Some(&identity),
            WindowContent::Detail(_) => return Vec::new(),
            WindowContent::PortForwards(_) => return Vec::new(),
        };
        if already_selected {
            return Vec::new();
        }
        let blockers = self.blockers_for(id);
        if !blockers.is_empty() {
            return self.block(WorkspaceCommand::SelectRow(id, identity), blockers);
        }
        let window = self.window_mut(id).expect("window checked above");
        match &mut window.content {
            WindowContent::Resource(resource) => {
                resource.selection = Some(identity.clone());
                resource.detail = Some(DetailState::new(identity));
            }
            WindowContent::Services(service) => {
                service.selection = Some(identity.clone());
                service.detail = Some(DetailState::new(identity));
            }
            WindowContent::Detail(_) => {}
            WindowContent::PortForwards(_) => {}
        }
        Vec::new()
    }

    fn clear_selection(&mut self, id: WindowId) -> Vec<WorkspaceEvent<I>> {
        let Some(window) = self.window(id) else {
            return Vec::new();
        };
        let has_selection = match &window.content {
            WindowContent::Resource(resource) => resource.selection.is_some(),
            WindowContent::Services(service) => service.selection.is_some(),
            WindowContent::Detail(_) => false,
            WindowContent::PortForwards(_) => false,
        };
        if !has_selection {
            return Vec::new();
        }
        let blockers = self.blockers_for(id);
        if !blockers.is_empty() {
            return self.block(WorkspaceCommand::ClearSelection(id), blockers);
        }
        let window = self.window_mut(id).expect("window checked above");
        match &mut window.content {
            WindowContent::Resource(resource) => {
                resource.selection = None;
                resource.detail = None;
            }
            WindowContent::Services(service) => {
                service.selection = None;
                service.detail = None;
            }
            WindowContent::Detail(_) => {}
            WindowContent::PortForwards(_) => {}
        }
        Vec::new()
    }

    // -- context switch -------------------------------------------------------

    /// Navigation guards a context switch would hit right now, without
    /// moving any state.
    pub fn context_switch_blockers(&self) -> Vec<Blocker> {
        self.windows
            .iter()
            .flat_map(|window| match &window.content {
                WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .map(|detail| detail.blockers(window.id))
                    .unwrap_or_default(),
                WindowContent::Services(service) => service
                    .detail
                    .as_ref()
                    .map(|detail| detail.blockers(window.id))
                    .unwrap_or_default(),
                WindowContent::Detail(detail) => detail.blockers(window.id),
                WindowContent::PortForwards(_) => Vec::new(),
            })
            .collect()
    }

    fn context_switch(&mut self, to: String) -> Vec<WorkspaceEvent<I>> {
        let blockers: Vec<Blocker> = self.context_switch_blockers();
        if !blockers.is_empty() {
            return self.block(WorkspaceCommand::ContextSwitch { to }, blockers);
        }

        // The switch is only requested here: the backend must validate the
        // destination before any local state moves, so the application layer
        // sends the request and commits through `CommitContextSwitch`.
        vec![WorkspaceEvent::ContextSwitchRequested { to }]
    }

    fn commit_context_switch(&mut self, to: String) -> Vec<WorkspaceEvent<I>> {
        let mut events = Vec::new();
        let dedicated: Vec<WindowId> = self
            .windows
            .iter()
            .filter(|window| matches!(window.content, WindowContent::Detail(_)))
            .map(|window| window.id)
            .collect();
        for id in dedicated {
            self.remove_window(id);
            events.push(WorkspaceEvent::Closed(id));
        }
        for window in &mut self.windows {
            match &mut window.content {
                WindowContent::Resource(resource) => {
                    resource.selection = None;
                    resource.detail = None;
                }
                WindowContent::Services(service) => {
                    service.selection = None;
                    service.detail = None;
                }
                WindowContent::Detail(_) => {}
                WindowContent::PortForwards(_) => {}
            }
        }
        // Every dirty buffer was resolved before the commit could run.
        self.yaml_owner.clear();
        // The switch is observable: state records the target and the caller
        // learns it through the commit event.
        self.context = to.clone();
        events.push(WorkspaceEvent::ContextSwitched { to });
        events
    }

    // -- YAML state -----------------------------------------------------------

    fn detail_identity(window: &Window<I>) -> Option<&I> {
        match &window.content {
            WindowContent::Resource(resource) => {
                resource.detail.as_ref().map(|detail| &detail.identity)
            }
            WindowContent::Services(service) => {
                service.detail.as_ref().map(|detail| &detail.identity)
            }
            WindowContent::Detail(detail) => Some(&detail.identity),
            WindowContent::PortForwards(_) => None,
        }
    }

    fn detail_is_dirty(window: &Window<I>) -> bool {
        match &window.content {
            WindowContent::Resource(resource) => resource
                .detail
                .as_ref()
                .is_some_and(|detail| detail.yaml.dirty),
            WindowContent::Services(service) => service
                .detail
                .as_ref()
                .is_some_and(|detail| detail.yaml.dirty),
            WindowContent::Detail(detail) => detail.yaml.dirty,
            WindowContent::PortForwards(_) => false,
        }
    }

    fn begin_yaml_edit(&mut self, id: WindowId) -> Vec<WorkspaceEvent<I>> {
        let Some(window) = self.window(id) else {
            return Vec::new();
        };
        let Some(identity) = Self::detail_identity(window).cloned() else {
            return Vec::new();
        };
        if Self::detail_is_dirty(window) {
            return Vec::new();
        }
        if let Some(&owner) = self.yaml_owner.get(&identity)
            && owner != id
        {
            return vec![WorkspaceEvent::YamlOwnerInUse { owner }];
        }
        self.with_detail_mut(id, |detail| detail.yaml.dirty = true);
        self.yaml_owner.insert(identity, id);
        Vec::new()
    }

    fn discard_yaml(&mut self, id: WindowId) {
        let identity = match self.window(id) {
            Some(window) => match &window.content {
                WindowContent::Resource(resource) => resource
                    .detail
                    .as_ref()
                    .filter(|detail| detail.yaml.dirty)
                    .map(|detail| detail.identity.clone()),
                WindowContent::Services(service) => service
                    .detail
                    .as_ref()
                    .filter(|detail| detail.yaml.dirty)
                    .map(|detail| detail.identity.clone()),
                WindowContent::Detail(detail) if detail.yaml.dirty => Some(detail.identity.clone()),
                _ => None,
            },
            None => None,
        };
        if let Some(identity) = identity {
            self.with_detail_mut(id, |detail| detail.yaml.dirty = false);
            self.yaml_owner.remove(&identity);
        }
    }

    // -- small accessors --------------------------------------------------------

    fn window_mut(&mut self, id: WindowId) -> Option<&mut Window<I>> {
        self.windows.iter_mut().find(|window| window.id == id)
    }

    fn with_resource_mut(
        &mut self,
        id: WindowId,
        mutate: impl FnOnce(&mut ResourceWindowState<I>),
    ) {
        if let Some(window) = self.window_mut(id)
            && let WindowContent::Resource(resource) = &mut window.content
        {
            mutate(resource);
        }
    }

    fn with_service_mut(&mut self, id: WindowId, mutate: impl FnOnce(&mut ServiceWindowState<I>)) {
        if let Some(window) = self.window_mut(id)
            && let WindowContent::Services(service) = &mut window.content
        {
            mutate(service);
        }
    }

    fn with_detail_mut(&mut self, id: WindowId, mutate: impl FnOnce(&mut DetailState<I>)) {
        if let Some(window) = self.window_mut(id) {
            match &mut window.content {
                WindowContent::Resource(resource) => {
                    if let Some(detail) = &mut resource.detail {
                        mutate(detail);
                    }
                }
                WindowContent::Services(service) => {
                    if let Some(detail) = &mut service.detail {
                        mutate(detail);
                    }
                }
                WindowContent::Detail(detail) => mutate(detail),
                WindowContent::PortForwards(_) => {}
            }
        }
    }

    fn with_port_forward_mut(
        &mut self,
        id: WindowId,
        mutate: impl FnOnce(&mut PortForwardWindowState),
    ) {
        if let Some(window) = self.window_mut(id)
            && let WindowContent::PortForwards(state) = &mut window.content
        {
            mutate(state);
        }
    }
}

/// Window-title text for a list window: its base title plus the active
/// namespace scope, matching the reference design (`Deployments · all
/// namespaces`). Shared by the window chrome and the taskbar so both never
/// drift apart.
#[must_use]
pub fn scoped_window_title(title: &str, scope: &NamespaceScope) -> String {
    match scope {
        NamespaceScope::Namespace(namespace) => format!("{title} · {namespace}"),
        NamespaceScope::ContextDefault | NamespaceScope::AllNamespaces => {
            format!("{title} · all namespaces")
        }
    }
}
