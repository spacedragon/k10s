//! The guarded YAML editing workflow.
//!
//! The editor is a pure state machine: read-only by default, one writable
//! buffer per window, review with a line diff, and an Apply action gated on
//! a backend-issued validation ticket. The UI never fabricates authoritative
//! success — every ticket, diagnostic, and conflict comes from the backend
//! through the protocol client, and any of connection loss, target drift,
//! or further editing invalidates the ticket while preserving the user's
//! dirty buffer.

use std::collections::HashMap;

use egui::{Color32, RichText, TextEdit};
use k10s_protocol::{
    BackendRevision, ResourceIdentity, ValidationTicket, YamlApplyRequest, YamlDiagnostic,
    YamlOutcome, buffer_hash,
};

use crate::workspace::{WindowId, WorkspaceCommand};

/// Read-only/edit/review phases of one guarded YAML editor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YamlPhase {
    /// The backend manifest renders read-only.
    ReadOnly,
    /// The writable buffer is being edited.
    Editing,
    /// The buffer is under review with its computed diff and validation
    /// state.
    Reviewing,
}

/// One line of the original-to-buffer diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffLine {
    /// How the line changed.
    pub kind: DiffKind,
    /// Line text without trailing newline.
    pub text: String,
}

/// How a diff line changed relative to the original.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffKind {
    /// Present in both texts.
    Unchanged,
    /// Only present in the edited buffer.
    Added,
    /// Only present in the original.
    Removed,
}

/// Protocol actions the editor needs the application to perform. Local
/// transitions never leave the state machine.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum YamlAction {
    /// Validate the current buffer; a valid outcome issues a ticket.
    Validate {
        /// Window that owns the editor.
        window: WindowId,
        /// Context the manifest targets.
        context: String,
        /// The exact buffer text.
        yaml: String,
    },
    /// Apply through the validated ticket (gated by [`YamlEditor::can_apply`]).
    Apply {
        /// Window that owns the editor.
        window: WindowId,
        /// The apply request built from the live ticket.
        request: YamlApplyRequest,
    },
}

/// One guarded YAML editor bound to a stable resource identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YamlEditor {
    target: ResourceIdentity,
    original: String,
    buffer: String,
    phase: YamlPhase,
    diff: Vec<DiffLine>,
    ticket: Option<ValidationTicket>,
    diagnostics: Vec<YamlDiagnostic>,
    conflict_message: Option<String>,
    disruption_acknowledged: bool,
}

impl YamlEditor {
    /// Create a read-only editor for `target` seeded with the
    /// backend-authored manifest.
    #[must_use]
    pub fn for_target(target: ResourceIdentity, original: &str) -> Self {
        Self {
            target,
            original: original.to_owned(),
            buffer: original.to_owned(),
            phase: YamlPhase::ReadOnly,
            diff: Vec::new(),
            ticket: None,
            diagnostics: Vec::new(),
            conflict_message: None,
            disruption_acknowledged: false,
        }
    }

    /// Identity this editor's changes apply to.
    #[must_use]
    pub fn target(&self) -> &ResourceIdentity {
        &self.target
    }

    /// Current phase.
    #[must_use]
    pub fn phase(&self) -> YamlPhase {
        self.phase
    }

    /// The current buffer; the untouched value while read-only.
    #[must_use]
    pub fn buffer(&self) -> &str {
        &self.buffer
    }

    /// Whether the buffer differs from the backend-authored original.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.buffer != self.original || self.phase != YamlPhase::ReadOnly
    }

    /// The live validation ticket, when one is still valid.
    #[must_use]
    pub fn ticket(&self) -> Option<&ValidationTicket> {
        self.ticket.as_ref()
    }

    /// Diagnostics from the latest failed validation.
    #[must_use]
    pub fn diagnostics(&self) -> &[YamlDiagnostic] {
        &self.diagnostics
    }

    /// Message from the latest conflict outcome.
    #[must_use]
    pub fn conflict_message(&self) -> Option<&str> {
        self.conflict_message.as_deref()
    }

    /// Whether the validated change restarts existing pods and has not been
    /// acknowledged yet.
    #[must_use]
    pub fn has_disruption_warning(&self) -> bool {
        self.ticket().is_some_and(|ticket| ticket.disruptive) && !self.disruption_acknowledged
    }

    /// Computed original-to-buffer diff; only meaningful while reviewing.
    #[must_use]
    pub fn diff(&self) -> &[DiffLine] {
        &self.diff
    }

    /// Enter the editable phase. Read-only editors keep their original.
    pub fn begin_edit(&mut self) {
        if self.phase == YamlPhase::ReadOnly {
            self.phase = YamlPhase::Editing;
        }
    }

    /// Replace the writable buffer. Any previously issued ticket becomes
    /// stale because it binds a different hash.
    pub fn set_buffer(&mut self, buffer: impl Into<String>) {
        if self.phase == YamlPhase::ReadOnly {
            return;
        }
        self.buffer = buffer.into();
        self.invalidate_ticket();
    }

    /// Move to the review phase and compute the diff against the original.
    pub fn review(&mut self) {
        if self.phase == YamlPhase::ReadOnly {
            return;
        }
        // A ticket stays valid across edit↔review round trips exactly as
        // long as the buffer keeps matching its hash.
        let still_valid = self
            .ticket
            .as_ref()
            .is_some_and(|ticket| ticket.buffer_hash == buffer_hash(&self.buffer));
        if !still_valid {
            self.invalidate_ticket();
        }
        self.diff = diff_lines(&self.original, &self.buffer);
        self.phase = YamlPhase::Reviewing;
    }

    /// Leave the review phase with the buffer intact.
    pub fn edit_again(&mut self) {
        if self.phase == YamlPhase::Reviewing {
            self.phase = YamlPhase::Editing;
        }
    }

    /// Discard every local change and return to the read-only original.
    pub fn discard(&mut self) {
        self.buffer = self.original.clone();
        self.diff = Vec::new();
        self.invalidate_ticket();
        self.phase = YamlPhase::ReadOnly;
        self.disruption_acknowledged = false;
    }

    /// Apply one backend validation outcome.
    ///
    /// Valid outcomes install the ticket; schema errors surface their
    /// diagnostics; conflicts drop the ticket but never destroy the buffer.
    /// A ticket issued for another identity is ignored outright.
    pub fn apply_outcome(&mut self, outcome: &YamlOutcome) {
        match outcome {
            YamlOutcome::Valid { ticket } => {
                if ticket.target != self.target {
                    return;
                }
                let matches_buffer = ticket.buffer_hash == buffer_hash(&self.buffer);
                self.ticket = matches_buffer.then(|| ticket.clone());
                self.diagnostics = Vec::new();
                self.conflict_message = None;
                self.disruption_acknowledged = false;
            }
            YamlOutcome::Invalid { diagnostics } => {
                self.invalidate_ticket();
                self.diagnostics = diagnostics.clone();
                self.conflict_message = None;
            }
            YamlOutcome::Conflict { message } => {
                // Conflict preservation: the user's work stays; only the
                // server-issued authority is dropped.
                self.invalidate_ticket();
                self.conflict_message = Some(message.clone());
            }
        }
    }

    /// Acknowledge the disruption warning of the live ticket.
    pub fn acknowledge_disruption(&mut self) {
        self.disruption_acknowledged = true;
    }

    /// Drop the ticket because the target advanced. The dirty buffer is
    /// preserved; equal or older revisions are no-ops.
    pub fn on_target_revision(&mut self, revision: BackendRevision) {
        let stale = self
            .ticket
            .as_ref()
            .is_some_and(|ticket| ticket.resource_revision < revision);
        if stale {
            self.invalidate_ticket();
        }
    }

    /// Drop the ticket after transport loss. The dirty buffer is preserved:
    /// reconnection must never destroy unsaved user work.
    pub fn connection_lost(&mut self) {
        self.invalidate_ticket();
    }

    /// Whether every Apply gate currently passes.
    #[must_use]
    pub fn can_apply(&self) -> bool {
        let Some(ticket) = self.ticket() else {
            return false;
        };
        ticket.buffer_hash == buffer_hash(&self.buffer)
            && (!ticket.disruptive || self.disruption_acknowledged)
    }

    /// Build the apply request from the live ticket, consuming nothing but
    /// requiring every gate to pass.
    #[must_use]
    pub fn take_apply_request(&mut self) -> Option<YamlApplyRequest> {
        if !self.can_apply() {
            return None;
        }
        let ticket = self.ticket()?;
        Some(YamlApplyRequest {
            context: self.target.context.clone(),
            ticket_id: ticket.id.clone(),
            target: self.target.clone(),
            buffer_hash: ticket.buffer_hash.clone(),
            yaml: self.buffer.clone(),
        })
    }

    fn invalidate_ticket(&mut self) {
        self.ticket = None;
        self.disruption_acknowledged = false;
    }
}

/// Per-window editors plus the protocol actions queued during rendering.
///
/// Owned by the UI shell; the application drains the actions each frame and
/// feeds outcomes back into the editors.
#[derive(Debug, Default)]
pub struct YamlEditors {
    editors: HashMap<WindowId, YamlEditor>,
    actions: Vec<(WindowId, YamlEditorActionInternal)>,
}

/// Internal alias so the map can queue actions alongside editors.
type YamlEditorActionInternal = YamlAction;

impl YamlEditors {
    /// Open (or replace) the editor for `window`, seeded with the
    /// backend-authored manifest.
    pub fn open(
        &mut self,
        window: WindowId,
        target: ResourceIdentity,
        manifest: &str,
    ) -> &mut YamlEditor {
        self.editors
            .insert(window, YamlEditor::for_target(target, manifest));
        self.editors.get_mut(&window).expect("editor just inserted")
    }

    /// Editor access for rendering.
    #[must_use]
    pub fn get(&self, window: WindowId) -> Option<&YamlEditor> {
        self.editors.get(&window)
    }

    /// Mutable editor access for rendering interactions.
    #[must_use]
    pub fn get_mut(&mut self, window: WindowId) -> Option<&mut YamlEditor> {
        self.editors.get_mut(&window)
    }

    /// Queue one protocol action produced during rendering.
    pub fn queue(&mut self, window: WindowId, action: YamlAction) {
        self.actions.push((window, action));
    }

    /// Drain every queued protocol action with its owning window.
    pub fn drain_actions(&mut self) -> Vec<(WindowId, YamlAction)> {
        std::mem::take(&mut self.actions)
    }

    /// Notify every editor that the transport was lost: tickets die, dirty
    /// buffers survive.
    pub fn connection_lost(&mut self) {
        for editor in self.editors.values_mut() {
            editor.connection_lost();
        }
    }

    /// Notify every editor bound to `identity` that its target advanced.
    pub fn target_changed(&mut self, identity: &ResourceIdentity, revision: BackendRevision) {
        for editor in self.editors.values_mut() {
            if editor.target() == identity {
                editor.on_target_revision(revision);
            }
        }
    }

    /// Drop entries for closed windows.
    pub fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.editors.retain(|id, _| live(*id));
    }
}

/// Render the YAML tab content for one detail view.
///
/// Interactions either mutate the editor directly (local transitions), queue
/// workspace commands (guards stay authoritative in the workspace), or queue
/// protocol actions for the application layer.
pub(crate) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    editors: &mut YamlEditors,
    identity: Option<&ResourceIdentity>,
    view_manifest: Option<&str>,
    mutations_allowed: bool,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: crate::ui::resource_window::RowIdentity,
{
    let Some(identity) = identity else {
        return;
    };
    let Some(manifest) = view_manifest else {
        ui.horizontal(|ui| {
            ui.label("Loading YAML");
        });
        return;
    };

    // Lazily open the editor so the read-only view always exists.
    if editors.get(window_id).is_none() {
        editors.open(window_id, identity.clone(), manifest);
    }
    let mut queued_action: Option<YamlAction> = None;
    {
        let editor = editors.get_mut(window_id).expect("editor just ensured");

        match editor.phase() {
            YamlPhase::ReadOnly => {
                ui.label(RichText::new("Read-only").weak());
                ui.vertical(|ui| {
                    ui.add(egui::Label::new(RichText::new(manifest).monospace()).wrap());
                });
                let edit = ui.add_enabled(mutations_allowed, egui::Button::new("Edit YAML"));
                edit.widget_info(|| {
                    egui::WidgetInfo::labeled(
                        egui::WidgetType::Button,
                        true,
                        "Edit YAML".to_owned(),
                    )
                });
                if edit.clicked() {
                    editor.begin_edit();
                    queued.push(WorkspaceCommand::BeginYamlEdit(window_id));
                }
            }
            YamlPhase::Editing => {
                let mut buffer = editor.buffer().to_owned();
                ui.vertical(|ui| {
                    ui.add_sized(
                        [ui.available_width(), ui.available_height() - 40.0],
                        TextEdit::multiline(&mut buffer).code_editor(),
                    );
                });
                editor.set_buffer(buffer);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(mutations_allowed, egui::Button::new("Review changes"))
                        .clicked()
                    {
                        editor.review();
                    }
                    if ui.button("Discard").clicked() {
                        editor.discard();
                        queued.push(WorkspaceCommand::DiscardYaml(window_id));
                    }
                });
            }
            YamlPhase::Reviewing => {
                show_validation_panel(ui, editor);
                ui.separator();
                ui.vertical(|ui| {
                    for line in editor.diff() {
                        let (color, prefix) = match line.kind {
                            DiffKind::Added => (Color32::from_rgb(0x2e, 0xa0, 0x43), "+"),
                            DiffKind::Removed => (Color32::from_rgb(0xc0, 0x39, 0x2b), "-"),
                            DiffKind::Unchanged => (Color32::GRAY, " "),
                        };
                        ui.label(
                            RichText::new(format!("{prefix} {}", line.text))
                                .monospace()
                                .color(color),
                        );
                    }
                });
                ui.separator();
                ui.horizontal(|ui| {
                    if editor.has_disruption_warning() {
                        let ack = ui.button("Acknowledge restart");
                        ack.widget_info(|| {
                            egui::WidgetInfo::labeled(
                                egui::WidgetType::Button,
                                true,
                                "Acknowledge restart".to_owned(),
                            )
                        });
                        if ack.clicked() {
                            editor.acknowledge_disruption();
                        }
                    }
                    let apply_enabled = mutations_allowed && editor.can_apply();
                    let apply = ui.add_enabled(apply_enabled, egui::Button::new("Apply"));
                    apply.widget_info(|| {
                        egui::WidgetInfo::labeled(
                            egui::WidgetType::Button,
                            apply_enabled,
                            format!(
                                "Apply{}",
                                if apply_enabled {
                                    ""
                                } else {
                                    " (validation required)"
                                }
                            ),
                        )
                    });
                    if apply.clicked()
                        && let Some(request) = editor.take_apply_request()
                    {
                        queued_action = Some(YamlAction::Apply {
                            window: window_id,
                            request,
                        });
                    }
                    if ui
                        .add_enabled(mutations_allowed, egui::Button::new("Validate"))
                        .clicked()
                    {
                        queued_action = Some(YamlAction::Validate {
                            window: window_id,
                            context: editor.target().context.clone(),
                            yaml: editor.buffer().to_owned(),
                        });
                    }
                    if ui.button("Back to edit").clicked() {
                        editor.edit_again();
                    }
                    if ui.button("Discard").clicked() {
                        editor.discard();
                        queued.push(WorkspaceCommand::DiscardYaml(window_id));
                    }
                });
            }
        }
        if !mutations_allowed {
            ui.label("YAML validation and apply are disabled until this window is live");
        }
    }
    if let Some(action) = queued_action {
        editors.queue(window_id, action);
    }
}

/// Ticket, diagnostics, and conflict status above the diff.
fn show_validation_panel(ui: &mut egui::Ui, editor: &YamlEditor) {
    if let Some(message) = editor.conflict_message() {
        ui.label(
            RichText::new(format!("Conflict: {message}"))
                .color(Color32::from_rgb(0xc0, 0x39, 0x2b)),
        );
    }
    if !editor.diagnostics().is_empty() {
        for diagnostic in editor.diagnostics() {
            ui.label(
                RichText::new(format!("Line {}: {}", diagnostic.line, diagnostic.message))
                    .color(Color32::from_rgb(0xd3, 0x8b, 0x26)),
            );
        }
    }
    if let Some(ticket) = editor.ticket() {
        ui.label(format!(
            "Validated against {} at revision {}",
            ticket.target.name, ticket.resource_revision
        ));
    }
    if editor.has_disruption_warning() {
        ui.label(
            RichText::new("Warning: applying this change restarts existing pods")
                .color(Color32::from_rgb(0xd3, 0x8b, 0x26)),
        );
    }
    if editor.ticket().is_none()
        && editor.diagnostics().is_empty()
        && editor.conflict_message().is_none()
    {
        ui.label(RichText::new("Not validated").weak());
    }
}

/// Deterministic set-based line diff. Good enough for prototype review;
/// ordering follows the original first, then additions.
#[must_use]
fn diff_lines(original: &str, buffer: &str) -> Vec<DiffLine> {
    use std::collections::HashSet;

    let original_set: HashSet<&str> = original.lines().collect();
    let buffer_set: HashSet<&str> = buffer.lines().collect();

    let mut diff: Vec<DiffLine> = original
        .lines()
        .map(|line| DiffLine {
            kind: if buffer_set.contains(line) {
                DiffKind::Unchanged
            } else {
                DiffKind::Removed
            },
            text: line.to_owned(),
        })
        .collect();
    diff.extend(
        buffer
            .lines()
            .filter(|line| !original_set.contains(*line))
            .map(|line| DiffLine {
                kind: DiffKind::Added,
                text: line.to_owned(),
            }),
    );
    diff.retain(|line| line.kind != DiffKind::Unchanged || !line.text.trim().is_empty());
    diff
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_marks_removals_additions_and_keeps_order() {
        let diff = diff_lines("a\nb\nc\n", "a\nB\nc\n");
        assert_eq!(
            diff,
            vec![
                DiffLine {
                    kind: DiffKind::Unchanged,
                    text: "a".into()
                },
                DiffLine {
                    kind: DiffKind::Removed,
                    text: "b".into()
                },
                DiffLine {
                    kind: DiffKind::Unchanged,
                    text: "c".into()
                },
                DiffLine {
                    kind: DiffKind::Added,
                    text: "B".into()
                },
            ]
        );
    }
}
