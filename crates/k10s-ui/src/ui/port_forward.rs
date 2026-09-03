//! Shared port-forward start dialog and non-authoritative presentation state.

use std::collections::BTreeMap;

use egui::{RichText, ScrollArea, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{
    PortForwardSession, PortForwardSessionId, PortForwardSessionState, PortForwardStartRequest,
    PortForwardTarget,
};

use super::PortForwardAction;
use crate::workspace::{PortForwardWindowState, SortSpec, WindowId, WorkspaceCommand};

const MANAGEMENT_COLUMNS: [super::responsive_table::ColumnSpec; 6] = [
    super::responsive_table::ColumnSpec::elastic("target", 140.0),
    super::responsive_table::ColumnSpec::required("namespace", 100.0),
    super::responsive_table::ColumnSpec::required("remote", 220.0),
    super::responsive_table::ColumnSpec::required("local", 132.0),
    super::responsive_table::ColumnSpec::required("status", 82.0),
    super::responsive_table::ColumnSpec::required("actions", 164.0),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagementColumn {
    Target,
    Namespace,
    Remote,
    Local,
    Status,
    Actions,
}

impl ManagementColumn {
    fn from_key(key: &str) -> Option<Self> {
        match key {
            "target" => Some(Self::Target),
            "namespace" => Some(Self::Namespace),
            "remote" => Some(Self::Remote),
            "local" => Some(Self::Local),
            "status" => Some(Self::Status),
            "actions" => Some(Self::Actions),
            _ => None,
        }
    }

    const fn key(self) -> &'static str {
        match self {
            Self::Target => "target",
            Self::Namespace => "namespace",
            Self::Remote => "remote",
            Self::Local => "local",
            Self::Status => "status",
            Self::Actions => "actions",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Target => "Target",
            Self::Namespace => "Namespace",
            Self::Remote => "Remote",
            Self::Local => "Local address",
            Self::Status => "Status",
            Self::Actions => "Actions",
        }
    }
}

struct TargetPresentation<'a> {
    kind: &'static str,
    name: &'a str,
    namespace: &'a str,
    target: String,
    remote: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionAction {
    Stop,
    DisabledStop,
    Retry,
    None,
}

struct SessionPresentation {
    label: &'static str,
    color: egui::Color32,
    rank: u8,
    action: SessionAction,
    live: bool,
}

pub(crate) const PORT_FORWARD_AUTHORITY_UNAVAILABLE: &str =
    "Port forwarding requires live, matching resource details";

/// Revalidate the exact target against the current capability and resource
/// feed. Both the modal and the application dispatch boundary use this
/// function so a render-time decision can never outlive its authority.
pub(crate) fn port_forward_start_authorization(
    feed: &super::ResourceFeed,
    target: &PortForwardTarget,
) -> Result<(), &'static str> {
    if target.validate().is_err() {
        return Err("The selected port-forward target is no longer valid");
    }
    let (identity, capability, capability_error) = match target {
        PortForwardTarget::Service { identity, .. } => (
            identity,
            feed.port_forward_available,
            "Service port forwarding is unavailable on this connection",
        ),
        PortForwardTarget::Pod { identity, .. } => (
            identity,
            feed.pod_port_forward_available,
            "Pod port forwarding is unavailable on this connection",
        ),
    };
    if !capability {
        return Err(capability_error);
    }
    let exact_loaded = match feed.primary_details.get(identity) {
        Some(super::PrimaryDetailState::Loaded(view)) => view.identity == *identity,
        Some(super::PrimaryDetailState::Loading | super::PrimaryDetailState::Failed(_)) => false,
        None => feed
            .details
            .get(identity)
            .is_some_and(|view| view.identity == *identity),
    };
    if !exact_loaded
        || !feed
            .detail_authority
            .get(identity)
            .is_some_and(super::DetailAuthority::mutations_allowed)
    {
        return Err(PORT_FORWARD_AUTHORITY_UNAVAILABLE);
    }
    Ok(())
}

/// Invalid local-port input in the shared start dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalPortError;

/// One non-persisted opening of the shared start dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortForwardModalGeneration(pub(super) u64);

/// Shared start-dialog state for either an exact Pod or Service target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardStartModal {
    pub target: PortForwardTarget,
    pub remote_label: String,
    pub local_port_draft: String,
    pub pending: bool,
    pub error: Option<String>,
    /// Correlates in-flight outcomes to this exact opening of the dialog.
    pub generation: PortForwardModalGeneration,
}

impl PortForwardStartModal {
    /// Create a dialog with the desired initial local port.
    #[must_use]
    pub fn new(
        target: PortForwardTarget,
        remote_label: impl Into<String>,
        initial_local_port: u16,
    ) -> Self {
        Self {
            target,
            remote_label: remote_label.into(),
            local_port_draft: initial_local_port.to_string(),
            pending: false,
            error: None,
            generation: PortForwardModalGeneration(0),
        }
    }

    pub(super) fn set_generation(&mut self, generation: PortForwardModalGeneration) {
        self.generation = generation;
    }

    /// Parse the requested loopback port. Blank and `0` request automatic
    /// assignment; every other value must fit in `1..=65535`.
    pub fn requested_port(&self) -> Result<u16, LocalPortError> {
        let draft = self.local_port_draft.trim();
        if draft.is_empty() {
            return Ok(0);
        }
        draft.parse::<u16>().map_err(|_| LocalPortError)
    }

    /// Whether the dialog can submit its current draft.
    #[must_use]
    pub fn can_start(&self) -> bool {
        !self.pending && self.requested_port().is_ok()
    }
}

/// Exact guidance shown when retrying a retained failed session cannot
/// reclaim its explicitly requested local port.
pub const RETRY_LOCAL_PORT_GUIDANCE: &str =
    "Local port is in use; start a new forward from the Pod or Service with another port.";

/// Application-owned presentation errors layered over authoritative session
/// snapshots. Values retain the revision they describe so newer authority
/// always clears stale local presentation.
#[derive(Debug, Default)]
pub struct PortForwardRetryErrors {
    errors: BTreeMap<PortForwardSessionId, (u64, String)>,
}

impl PortForwardRetryErrors {
    /// Store one safe retry rejection on its originating failed row.
    pub fn record(&mut self, session: &PortForwardSession, message: impl Into<String>) {
        self.errors
            .insert(session.id.clone(), (session.revision, message.into()));
    }

    /// Store the retry-only local-port conflict guidance for a failed row.
    pub fn local_port_conflict(&mut self, session: &PortForwardSession) {
        self.record(session, RETRY_LOCAL_PORT_GUIDANCE);
    }

    /// Remove an overlay after a later retry succeeds.
    pub fn retry_succeeded(&mut self, session_id: &PortForwardSessionId) {
        self.errors.remove(session_id);
    }

    /// Drop overlays whose session expired or whose authoritative revision
    /// changed. The supplied feed is only inspected and is never modified.
    pub fn reconcile(&mut self, sessions: &[PortForwardSession]) {
        self.errors.retain(|id, (revision, _)| {
            sessions
                .iter()
                .any(|session| &session.id == id && session.revision == *revision)
        });
    }

    /// Presentation error for one retained session.
    #[must_use]
    pub fn get(&self, session_id: &PortForwardSessionId) -> Option<&str> {
        self.errors
            .get(session_id)
            .map(|(_, message)| message.as_str())
    }

    /// Clear all connection-generation-local overlays.
    pub fn clear(&mut self) {
        self.errors.clear();
    }
}

/// Build a retry from the retained failed snapshot, never from display text.
pub fn retry_start_request(
    session: &PortForwardSession,
) -> Result<PortForwardStartRequest, &'static str> {
    if session.state != PortForwardSessionState::Failed {
        return Err("only failed port-forward sessions can be retried");
    }
    PortForwardStartRequest::try_target(session.target.clone(), session.requested_local_port)
}

pub(super) fn show_start_modal(
    ctx: &egui::Context,
    modal: &mut Option<PortForwardStartModal>,
    actions: &mut Vec<PortForwardAction>,
    unavailable_reason: Option<&'static str>,
) {
    let Some(state) = modal.as_mut() else {
        return;
    };
    let mut cancel = false;
    egui::Modal::new(egui::Id::new("k10s.port_forward.start_modal")).show(ctx, |ui| {
        ui.heading("Start port forward");
        ui.label(&state.remote_label);
        target_details(ui, &state.target);

        let edit = ui.add(
            TextEdit::singleline(&mut state.local_port_draft)
                .id_source("k10s.port_forward.local_port")
                .hint_text("Blank or 0 = automatic")
                .desired_width(180.0),
        );
        edit.widget_info(|| {
            WidgetInfo::labeled(WidgetType::TextEdit, true, "Local port".to_owned())
        });
        if state.requested_port().is_err() {
            ui.label(RichText::new("Enter a port from 0 to 65535").color(super::theme::WARNING));
        }
        if let Some(error) = &state.error {
            ui.label(RichText::new(error).color(super::theme::WARNING));
        }
        if let Some(reason) = unavailable_reason {
            ui.label(RichText::new(reason).color(super::theme::WARNING));
        }
        if state.pending {
            ui.add(egui::Spinner::new());
        }

        ui.horizontal(|ui| {
            let start = ui.add_enabled(
                state.can_start() && unavailable_reason.is_none(),
                egui::Button::new("Start"),
            );
            start.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Button, true, "Start port forward".to_owned())
            });
            if unavailable_reason.is_none()
                && start.clicked()
                && let Ok(local_port) = state.requested_port()
            {
                match PortForwardStartRequest::try_target(state.target.clone(), local_port) {
                    Ok(request) => {
                        state.pending = true;
                        state.error = None;
                        actions.push(PortForwardAction::Start {
                            request,
                            generation: state.generation,
                        });
                    }
                    Err(_) => {
                        state.error =
                            Some("The selected port-forward target is no longer valid".into());
                    }
                }
            }

            let cancel_button = ui.button("Cancel");
            cancel_button.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Button, true, "Cancel port forward".to_owned())
            });
            cancel |= cancel_button.clicked();
        });
    });
    if cancel {
        *modal = None;
    }
}

pub(super) fn show_manager<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    state: &PortForwardWindowState,
    feed: &super::ResourceFeed,
    connection: super::ConnectionState,
    actions: &mut Vec<PortForwardAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    match connection {
        super::ConnectionState::Failed => {
            ui.label("Disconnected. Existing port-forward sessions are unavailable.");
            clear_focus_if_needed(window_id, state, queued);
            return;
        }
        super::ConnectionState::Connecting => {
            ui.label("Reconnecting to port-forward sessions…");
            clear_focus_if_needed(window_id, state, queued);
            return;
        }
        super::ConnectionState::Connected => {}
    }
    if !feed.port_forward_available && !feed.pod_port_forward_available {
        ui.label("Port forwarding is unavailable on this connection.");
        clear_focus_if_needed(window_id, state, queued);
        return;
    }
    match feed.port_forward_list_state {
        super::PortForwardListState::Loading => {
            ui.label("Loading port-forward sessions…");
            return;
        }
        super::PortForwardListState::Reconstructing => {
            ui.label("Reconstructing port-forward sessions…");
            return;
        }
        super::PortForwardListState::Ready => {}
    }
    if let Some(error) = &feed.port_forward_error {
        ui.label(RichText::new(error).color(super::theme::WARNING));
    }
    if feed.port_forward_sessions.is_empty() {
        ui.label("No port forwards yet. Start one from Pod Ports or Service Ports.");
        clear_focus_if_needed(window_id, state, queued);
        return;
    }

    let mut sessions = feed.port_forward_sessions.iter().collect::<Vec<_>>();
    sort_sessions(&mut sessions, state.sort.as_ref());
    let spacing = ui.spacing().item_spacing.x;
    let available = ui
        .available_rect_before_wrap()
        .intersect(ui.clip_rect())
        .width();
    let columns = super::responsive_table::resolve_columns(
        &MANAGEMENT_COLUMNS,
        available,
        spacing,
        &std::collections::BTreeSet::new(),
    );
    let focus = state.focused_session.as_deref();
    ScrollArea::both()
        .id_salt(("k10s.port-forward.sessions", window_id.0))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                for column in &columns.visible {
                    if let Some(column_kind) = ManagementColumn::from_key(column.key) {
                        super::responsive_table::sized_cell(ui, column.width, false, |ui| {
                            sort_header(ui, window_id, column_kind, state.sort.as_ref(), queued);
                        });
                    }
                }
            });
            ui.separator();
            for session in sessions {
                let focused = focus == Some(session.id.as_str());
                ui.push_id(session.id.as_str(), |ui| {
                    let response = egui::Frame::new()
                        .fill(if focused {
                            super::theme::SELECTED_ROW
                        } else if session.state == PortForwardSessionState::Stopped {
                            ui.visuals().faint_bg_color
                        } else {
                            egui::Color32::TRANSPARENT
                        })
                        .inner_margin(egui::Margin::symmetric(4, 5))
                        .show(ui, |ui| {
                            if session.state == PortForwardSessionState::Stopped {
                                ui.visuals_mut().override_text_color =
                                    Some(super::theme::MUTED_TEXT);
                            }
                            ui.horizontal(|ui| {
                                for column in &columns.visible {
                                    if let Some(column_kind) =
                                        ManagementColumn::from_key(column.key)
                                    {
                                        super::responsive_table::sized_cell(
                                            ui,
                                            column.width,
                                            false,
                                            |ui| {
                                                session_cell(ui, column_kind, session, actions);
                                            },
                                        );
                                    }
                                }
                            });
                            session_messages(
                                ui,
                                session,
                                feed.port_forward_retry_errors
                                    .get(&session.id)
                                    .map(String::as_str),
                            );
                        })
                        .response;
                    let row_label = format!("Port forward session {}", session.id);
                    response.widget_info(|| {
                        WidgetInfo::labeled(WidgetType::Other, true, row_label.clone())
                    });
                    if focused {
                        response.scroll_to_me(Some(egui::Align::Center));
                    }
                });
            }
        });
    if focus.is_some() {
        queued.push(WorkspaceCommand::ClearPortForwardSessionFocus(window_id));
    }
}

fn clear_focus_if_needed<I>(
    window_id: WindowId,
    state: &PortForwardWindowState,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if state.focused_session.is_some() {
        queued.push(WorkspaceCommand::ClearPortForwardSessionFocus(window_id));
    }
}

fn session_cell(
    ui: &mut egui::Ui,
    column: ManagementColumn,
    session: &PortForwardSession,
    actions: &mut Vec<PortForwardAction>,
) {
    match column {
        ManagementColumn::Target => {
            let value = target_presentation(session).target;
            let color = (session.state == PortForwardSessionState::Stopped)
                .then_some(super::theme::MUTED_TEXT);
            ui.vertical(|ui| {
                let response = ui.add(
                    egui::Label::new(color.map_or_else(
                        || RichText::new(&value),
                        |color| RichText::new(&value).color(color),
                    ))
                    .truncate(),
                );
                response
                    .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, value.clone()));
            });
        }
        ManagementColumn::Namespace => {
            ui.label(target_presentation(session).namespace);
        }
        ManagementColumn::Remote => {
            super::responsive_table::elided_label(ui, target_presentation(session).remote, 30);
        }
        ManagementColumn::Local => {
            ui.monospace(if session.local_addr.is_empty() {
                "—"
            } else {
                session.local_addr.as_str()
            });
        }
        ManagementColumn::Status => {
            let presentation = session_presentation(session.state);
            ui.label(RichText::new(presentation.label).color(presentation.color));
        }
        ManagementColumn::Actions => session_actions(ui, session, actions),
    }
}

fn session_messages(ui: &mut egui::Ui, session: &PortForwardSession, retry_error: Option<&str>) {
    for message in session
        .failure
        .as_ref()
        .map(|failure| failure.message.as_str())
        .into_iter()
        .chain(retry_error)
    {
        ui.add(
            egui::Label::new(RichText::new(message).color(super::theme::WARNING))
                .wrap()
                .selectable(true),
        );
    }
}

fn session_actions(
    ui: &mut egui::Ui,
    session: &PortForwardSession,
    actions: &mut Vec<PortForwardAction>,
) {
    let presentation = session_presentation(session.state);
    if presentation.action == SessionAction::None {
        return;
    }
    if !session.local_addr.is_empty() {
        let copy = ui.small_button("Copy");
        let label = format!("Copy address for {}", session.id);
        copy.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
        if copy.clicked() {
            actions.push(PortForwardAction::CopyAddress(session.local_addr.clone()));
        }
    }
    match presentation.action {
        SessionAction::Stop => {
            let stop = ui.small_button("Stop");
            let label = format!("Stop port forward {}", session.id);
            stop.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
            if stop.clicked() {
                actions.push(PortForwardAction::Stop(session.id.to_string()));
            }
        }
        SessionAction::DisabledStop => {
            let stop = ui.add_enabled(false, egui::Button::new("Stop"));
            let label = format!("Stop port forward {}", session.id);
            stop.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
        }
        SessionAction::Retry => {
            let retry = ui.small_button("Retry");
            let label = format!("Retry port forward {}", session.id);
            retry.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, label.clone()));
            if retry.clicked() {
                actions.push(PortForwardAction::Retry(session.id.clone()));
            }
        }
        SessionAction::None => {}
    }
}

fn sort_header<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    column: ManagementColumn,
    sort: Option<&SortSpec>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if column == ManagementColumn::Actions {
        ui.label(column.title());
        return;
    }
    let active = sort.is_some_and(|sort| sort.column == column.key());
    let ascending = sort.map(|sort| sort.ascending).unwrap_or(true);
    ui.horizontal(|ui| {
        ui.label(column.title());
        let button = ui.small_button(if active {
            if ascending { "↑" } else { "↓" }
        } else {
            "↕"
        });
        let accessible = format!("Sort port forwards by {}", column.key());
        button.widget_info(|| WidgetInfo::labeled(WidgetType::Button, true, accessible.clone()));
        if button.clicked() {
            queued.push(WorkspaceCommand::SetPortForwardSort(
                window_id,
                Some(SortSpec {
                    column: column.key().to_owned(),
                    ascending: if active { !ascending } else { true },
                }),
            ));
        }
    });
}

fn sort_sessions(sessions: &mut [&PortForwardSession], sort: Option<&SortSpec>) {
    let column = sort
        .and_then(|sort| ManagementColumn::from_key(&sort.column))
        .unwrap_or(ManagementColumn::Target);
    let ascending = sort.map(|sort| sort.ascending).unwrap_or(true);
    sessions.sort_by(|left, right| {
        let left_target = target_presentation(left);
        let right_target = target_presentation(right);
        let order = match column {
            ManagementColumn::Namespace => left_target.namespace.cmp(right_target.namespace),
            ManagementColumn::Remote => left_target.remote.cmp(&right_target.remote),
            ManagementColumn::Local => left.local_addr.cmp(&right.local_addr),
            ManagementColumn::Status => session_presentation(left.state)
                .rank
                .cmp(&session_presentation(right.state).rank),
            ManagementColumn::Target | ManagementColumn::Actions => {
                (left_target.kind, left_target.name).cmp(&(right_target.kind, right_target.name))
            }
        }
        .then_with(|| left.id.cmp(&right.id));
        if ascending { order } else { order.reverse() }
    });
}

fn target_presentation(session: &PortForwardSession) -> TargetPresentation<'_> {
    match &session.target {
        PortForwardTarget::Service { identity, port } => {
            let remote = port_selector_label(port);
            TargetPresentation {
                kind: "Service",
                name: &identity.name,
                namespace: identity.namespace.as_deref().unwrap_or("—"),
                target: format!("Service {}", identity.name),
                remote: format!(
                    "port {} · backing Pod {}:{}",
                    remote, session.pod.name, session.pod_port
                ),
            }
        }
        PortForwardTarget::Pod {
            identity,
            container_name,
            remote_port,
        } => {
            let remote = remote_port.to_string();
            TargetPresentation {
                kind: "Pod",
                name: &identity.name,
                namespace: identity.namespace.as_deref().unwrap_or("—"),
                target: format!("Pod {}", identity.name),
                remote: format!("container {} · port {}", container_name, remote),
            }
        }
    }
}

fn port_selector_label(selector: &k10s_protocol::PortForwardPortSelector) -> String {
    match selector {
        k10s_protocol::PortForwardPortSelector::Number { number } => number.to_string(),
        k10s_protocol::PortForwardPortSelector::Name { name } => name.clone(),
    }
}

fn session_presentation(state: PortForwardSessionState) -> SessionPresentation {
    match state {
        PortForwardSessionState::Starting => SessionPresentation {
            label: "Starting",
            color: super::theme::CONNECTING,
            rank: 0,
            action: SessionAction::Stop,
            live: true,
        },
        PortForwardSessionState::Active => SessionPresentation {
            label: "Active",
            color: super::theme::HEALTHY,
            rank: 1,
            action: SessionAction::Stop,
            live: true,
        },
        PortForwardSessionState::Stopping => SessionPresentation {
            label: "Stopping",
            color: super::theme::WARNING,
            rank: 2,
            action: SessionAction::DisabledStop,
            live: true,
        },
        PortForwardSessionState::Failed => SessionPresentation {
            label: "Failed",
            color: super::theme::DANGER,
            rank: 3,
            action: SessionAction::Retry,
            live: false,
        },
        PortForwardSessionState::Stopped => SessionPresentation {
            label: "Stopped",
            color: super::theme::MUTED_TEXT,
            rank: 4,
            action: SessionAction::None,
            live: false,
        },
    }
}

pub(super) fn is_live_session_state(state: PortForwardSessionState) -> bool {
    session_presentation(state).live
}

fn target_details(ui: &mut egui::Ui, target: &PortForwardTarget) {
    let (identity, detail) = match target {
        PortForwardTarget::Service { identity, .. } => (identity, "Service".to_owned()),
        PortForwardTarget::Pod {
            identity,
            container_name,
            remote_port,
        } => (
            identity,
            format!("Pod container {container_name} · remote port {remote_port}"),
        ),
    };
    ui.label(format!("Context: {}", identity.context));
    ui.label(format!(
        "Namespace: {}",
        identity.namespace.as_deref().unwrap_or("—")
    ));
    ui.label(format!("{detail}: {}", identity.name));
}
