//! Shared port-forward start dialog and non-authoritative presentation state.

use std::collections::BTreeMap;

use egui::{RichText, TextEdit, WidgetInfo, WidgetType};
use k10s_protocol::{
    PortForwardSession, PortForwardSessionId, PortForwardSessionState, PortForwardStartRequest,
    PortForwardTarget,
};

use super::PortForwardAction;

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
    /// Store the retry-only local-port conflict guidance for a failed row.
    pub fn local_port_conflict(&mut self, session: &PortForwardSession) {
        self.errors.insert(
            session.id.clone(),
            (session.revision, RETRY_LOCAL_PORT_GUIDANCE.to_owned()),
        );
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

pub(super) fn show(
    ctx: &egui::Context,
    modal: &mut Option<PortForwardStartModal>,
    actions: &mut Vec<PortForwardAction>,
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
        if state.pending {
            ui.add(egui::Spinner::new());
        }

        ui.horizontal(|ui| {
            let start = ui.add_enabled(state.can_start(), egui::Button::new("Start"));
            start.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Button, true, "Start port forward".to_owned())
            });
            if start.clicked()
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
