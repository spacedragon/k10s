//! Operation confirmation dialogs: scale and delete workflows rendered as
//! small modal windows above the canvas.
//!
//! The dialog state is pure and testable; rendering only queues actions.
//! The application layer drains [`DialogAction`]s, submits them through the
//! shared client's command path (every action carries an idempotency key),
//! and reports the accepted operation or failure back to the originating
//! dialog. Disabled controls always carry a safe human-readable reason.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use egui::RichText;
use k10s_protocol::{DeletePropagation, OperationId, ResourceIdentity};

use crate::workspace::WindowId;

/// Authoritative preflight outcome shown by every destructive confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestructivePreflight {
    /// No exact-target server result has been received yet.
    Pending,
    /// The API server accepted the dry-run and returned an impact estimate.
    Ready {
        impact: String,
        dry_run: String,
        resource_version: String,
    },
    /// RBAC denied the dry-run.
    Forbidden(String),
    /// The target changed after the view was loaded.
    Conflict(String),
    /// The API server rejected or could not complete the dry-run.
    DryRunFailed(String),
}

impl DestructivePreflight {
    /// Deterministic fake-mode fixtures covering every safety outcome.
    #[must_use]
    pub fn fake_success() -> Self {
        Self::Ready {
            impact: "Deletes this object; dependents follow the selected propagation policy."
                .into(),
            resource_version: "fake-revision".into(),
            dry_run: "Passed — the API server accepted the delete dry-run.".into(),
        }
    }

    /// Fake RBAC denial fixture.
    #[must_use]
    pub fn fake_forbidden() -> Self {
        Self::Forbidden("Forbidden — delete is not allowed in this context.".into())
    }

    /// Fake stale-resource conflict fixture.
    #[must_use]
    pub fn fake_conflict() -> Self {
        Self::Conflict("Conflict — the resource changed; refresh before deleting.".into())
    }

    /// Fake server dry-run failure fixture.
    #[must_use]
    pub fn fake_dry_run_failure() -> Self {
        Self::DryRunFailed("Dry-run failed — the API server rejected this delete.".into())
    }

    fn blocking_reason(&self) -> Option<&str> {
        match self {
            Self::Pending => Some("waiting for authoritative server dry-run"),
            Self::Ready { .. } => None,
            Self::Forbidden(reason) | Self::Conflict(reason) | Self::DryRunFailed(reason) => {
                Some(reason)
            }
        }
    }
}

/// One queued mutation request from a dialog, drained by the application
/// layer after rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialogAction {
    /// Request an authoritative delete dry-run for this exact target/policy.
    RequestDeletePreflight {
        target: ResourceIdentity,
        propagation: DeletePropagation,
    },
    /// Scale the target to the requested replica count.
    SubmitScale {
        /// Exact target identity including its immutable UID.
        target: ResourceIdentity,
        /// Desired replica count.
        replicas: u32,
        /// Idempotency key for safe retries.
        idempotency_key: String,
    },
    /// Delete the target with the selected propagation mode.
    SubmitDelete {
        /// Exact target identity including its immutable UID.
        target: ResourceIdentity,
        /// How dependents are handled.
        propagation: DeletePropagation,
        /// Resource version authorized by the successful dry-run.
        resource_version: String,
        /// Idempotency key for safe retries.
        idempotency_key: String,
    },
}

/// Lifecycle phase of one open dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogPhase {
    /// Waiting for user input or a valid submission.
    AwaitingInput,
    /// The submission was drained by the application layer.
    Submitted,
}

/// Deterministic idempotency keys for dialog submissions. A process-wide
/// counter keeps repeated dialogs for the same target distinct.
fn next_dialog_key(prefix: &str, target: &ResourceIdentity) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!(
        "{prefix}:{}:{}:{counter}",
        target.context,
        target.name,
        counter = COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// The replica-count bounds accepted by the fake-backed prototype.
const MIN_REPLICAS: i64 = 0;
const MAX_REPLICAS: i64 = 999;

fn replicas_reason(input: &str) -> Option<&'static str> {
    match input.trim().parse::<i64>() {
        Ok(value) if (MIN_REPLICAS..=MAX_REPLICAS).contains(&value) => None,
        Ok(_) => Some("replicas must be a whole number between 0 and 999"),
        Err(_) => Some("replicas must be a whole number between 0 and 999"),
    }
}

/// Modal confirmation dialog for scaling one exact workload object.
#[derive(Debug, Clone)]
pub struct ScaleDialog {
    target: ResourceIdentity,
    input: String,
    idempotency_key: String,
    phase: DialogPhase,
    connected: bool,
    submitted_operation: Option<OperationId>,
    failure: Option<String>,
}

impl ScaleDialog {
    /// Open a scale dialog for `target`, optionally pre-filling the desired
    /// count from the row summary.
    #[must_use]
    pub fn for_target(target: ResourceIdentity, suggested_replicas: Option<u32>) -> Self {
        Self {
            input: suggested_replicas
                .map(|count| count.to_string())
                .unwrap_or_default(),
            idempotency_key: next_dialog_key("scale", &target),
            target,
            phase: DialogPhase::AwaitingInput,
            connected: true,
            submitted_operation: None,
            failure: None,
        }
    }

    /// The exact target this dialog mutates.
    #[must_use]
    pub fn target(&self) -> &ResourceIdentity {
        &self.target
    }

    /// Mutable input buffer for the replica count field.
    fn input_buffer(&mut self) -> &mut String {
        &mut self.input
    }

    /// Update the desired replica count text.
    pub fn set_input(&mut self, text: impl Into<String>) {
        self.input = text.into();
    }

    /// Why submission is disabled right now, if it is. Disconnection wins
    /// over validation so users are never told to fix numbers that cannot
    /// be submitted anyway.
    #[must_use]
    pub fn disabled_reason(&self) -> Option<&'static str> {
        if !self.connected {
            return Some("not connected");
        }
        if matches!(self.phase, DialogPhase::Submitted) && self.failure.is_none() {
            return Some("already submitted");
        }
        replicas_reason(&self.input)
    }

    /// Whether a submission may be drained right now.
    #[must_use]
    pub fn can_submit(&self) -> bool {
        self.disabled_reason().is_none()
    }

    /// Take the queued submission exactly once. Consuming moves the dialog
    /// to [`DialogPhase::Submitted`].
    pub fn take_action(&mut self) -> Option<DialogAction> {
        if !self.can_submit() {
            return None;
        }
        let replicas = self.input.trim().parse::<u32>().ok()?;
        self.phase = DialogPhase::Submitted;
        Some(DialogAction::SubmitScale {
            target: self.target.clone(),
            replicas,
            idempotency_key: self.idempotency_key.clone(),
        })
    }

    /// Notify the dialog that the transport was lost.
    pub fn connection_lost(&mut self) {
        self.connected = false;
    }

    /// Notify the dialog that the transport is available again.
    pub fn reconnected(&mut self) {
        self.connected = true;
    }

    /// Current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> DialogPhase {
        self.phase
    }

    /// Report an accepted operation for the drained submission. Any earlier
    /// failure is cleared: the dialog is done.
    pub fn operation_accepted(&mut self, operation_id: OperationId) {
        self.submitted_operation = Some(operation_id);
        self.failure = None;
        self.phase = DialogPhase::Submitted;
    }

    /// Report a failed submission and reopen the dialog for a corrected
    /// retry with the same idempotency key.
    pub fn operation_failed(&mut self, reason: impl Into<String>) {
        self.failure = Some(reason.into());
        self.phase = DialogPhase::AwaitingInput;
    }

    /// The accepted operation, once known.
    #[must_use]
    pub fn submitted_operation(&self) -> Option<OperationId> {
        self.submitted_operation.clone()
    }

    /// The safe failure reason, if the last attempt failed.
    #[must_use]
    pub fn failure_message(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// Whether a failed attempt can be retried through this dialog.
    #[must_use]
    pub fn can_resubmit(&self) -> bool {
        self.failure.is_some()
    }
}

/// Modal confirmation dialog for deleting one exact object. Deletion is
/// gated on typing the resource name and carries an explicit propagation
/// mode.
#[derive(Debug, Clone)]
pub struct DeleteDialog {
    target: ResourceIdentity,
    propagation: DeletePropagation,
    confirmation: String,
    idempotency_key: String,
    connected: bool,
    stale_reason: Option<String>,
    preflight: DestructivePreflight,
    consumed: bool,
    submitted_operation: Option<OperationId>,
    failure: Option<String>,
}

impl DeleteDialog {
    /// Open a delete dialog for `target`.
    #[must_use]
    pub fn for_target(target: ResourceIdentity) -> Self {
        Self {
            idempotency_key: next_dialog_key("delete", &target),
            propagation: DeletePropagation::Background,
            confirmation: String::new(),
            target,
            connected: true,
            stale_reason: None,
            preflight: DestructivePreflight::Pending,
            consumed: false,
            submitted_operation: None,
            failure: None,
        }
    }

    /// The exact target this dialog deletes.
    #[must_use]
    pub fn target(&self) -> &ResourceIdentity {
        &self.target
    }

    /// Mutable input buffer for the typed confirmation field.
    fn confirmation_buffer(&mut self) -> &mut String {
        &mut self.confirmation
    }

    /// Update the typed confirmation text.
    pub fn set_confirmation(&mut self, text: impl Into<String>) {
        self.confirmation = text.into();
    }

    /// Select the propagation mode.
    pub fn set_propagation(&mut self, propagation: DeletePropagation) {
        self.propagation = propagation;
        self.preflight = DestructivePreflight::Pending;
    }

    /// Currently selected propagation mode.
    #[must_use]
    pub fn propagation(&self) -> DeletePropagation {
        self.propagation
    }

    /// Replace the authoritative impact and server dry-run result.
    pub fn set_preflight(&mut self, preflight: DestructivePreflight) {
        self.preflight = preflight;
    }

    /// Mark the displayed target data stale and revoke submission authority.
    pub fn mark_stale(&mut self, reason: impl Into<String>) {
        self.stale_reason = Some(reason.into());
        self.preflight = DestructivePreflight::Pending;
    }

    /// Clear a stale-data block, returning whether a fresh preflight is needed.
    pub fn clear_stale(&mut self) -> bool {
        self.stale_reason.take().is_some()
    }

    /// Equivalent command for the exact target and selected propagation.
    #[must_use]
    pub fn kubectl_command(&self) -> String {
        let namespace = self
            .target
            .namespace
            .as_ref()
            .map(|namespace| format!(" --namespace {}", shell_quote(namespace)))
            .unwrap_or_default();
        let propagation = match self.propagation {
            DeletePropagation::Background => "Background",
            DeletePropagation::Foreground => "Foreground",
            DeletePropagation::Orphan => "Orphan",
        };
        format!(
            "kubectl --context {} delete {} {}{} --cascade={} --wait=false",
            shell_quote(&self.target.context),
            shell_quote(&self.target.gvk.kind.to_ascii_lowercase()),
            shell_quote(&self.target.name),
            namespace,
            propagation.to_ascii_lowercase()
        )
    }

    /// Why submission is disabled right now, if it is.
    #[must_use]
    pub fn disabled_reason(&self) -> Option<&str> {
        if !self.connected {
            return Some("not connected");
        }
        if self.stale_reason.is_some() {
            return self.stale_reason.as_deref();
        }
        if let Some(reason) = self.preflight.blocking_reason() {
            return Some(reason);
        }
        if self.consumed {
            return Some("already submitted");
        }
        if self.confirmation != self.target.name {
            return Some("type the resource name to confirm deletion");
        }
        None
    }

    /// Whether a submission may be drained right now.
    #[must_use]
    pub fn can_submit(&self) -> bool {
        self.disabled_reason().is_none()
    }

    /// Take the queued submission exactly once.
    pub fn take_action(&mut self) -> Option<DialogAction> {
        if !self.can_submit() {
            return None;
        }
        self.consumed = true;
        let DestructivePreflight::Ready {
            resource_version, ..
        } = &self.preflight
        else {
            unreachable!("submission is gated on a ready preflight")
        };
        Some(DialogAction::SubmitDelete {
            target: self.target.clone(),
            propagation: self.propagation,
            resource_version: resource_version.clone(),
            idempotency_key: self.idempotency_key.clone(),
        })
    }

    /// Notify the dialog that the transport was lost.
    pub fn connection_lost(&mut self) {
        self.connected = false;
        self.preflight = DestructivePreflight::Pending;
    }

    /// Notify the dialog that the transport is available again.
    pub fn reconnected(&mut self) {
        self.connected = true;
    }

    /// Report an accepted operation for the drained submission. Any earlier
    /// failure is cleared: the dialog is done.
    pub fn operation_accepted(&mut self, operation_id: OperationId) {
        self.submitted_operation = Some(operation_id);
        self.failure = None;
    }

    /// Report a failed submission and reopen the dialog for a corrected
    /// retry with the same idempotency key.
    pub fn operation_failed(&mut self, reason: impl Into<String>) {
        self.failure = Some(reason.into());
        self.consumed = false;
    }

    /// The safe failure reason, if the last attempt failed.
    #[must_use]
    pub fn failure_message(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// The accepted operation, once known.
    #[must_use]
    pub fn submitted_operation(&self) -> Option<OperationId> {
        self.submitted_operation.clone()
    }
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        })
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

/// Which dialog is active on a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveDialogKind {
    /// A scale confirmation is open.
    Scale,
    /// A delete confirmation is open.
    Delete,
}

/// Mutable access to the dialog active on a window.
#[derive(Debug)]
pub enum DialogHandle<'a> {
    /// A scale confirmation.
    Scale(&'a mut ScaleDialog),
    /// A delete confirmation.
    Delete(&'a mut DeleteDialog),
}

impl DialogHandle<'_> {
    /// Report an accepted operation to whichever dialog is active.
    pub fn operation_accepted(&mut self, operation_id: OperationId) {
        match self {
            Self::Scale(dialog) => dialog.operation_accepted(operation_id),
            Self::Delete(dialog) => dialog.operation_accepted(operation_id),
        }
    }

    /// Report a failed submission to whichever dialog is active.
    pub fn operation_failed(&mut self, reason: impl Into<String>) {
        match self {
            Self::Scale(dialog) => dialog.operation_failed(reason),
            Self::Delete(dialog) => dialog.operation_failed(reason),
        }
    }
}

/// Per-window store of open operation dialogs plus the actions queued by
/// rendering. Owned by the shell; drained by the application layer.
#[derive(Debug, Default)]
pub struct OperationDialogs {
    windows: HashMap<WindowId, ActiveDialog>,
    actions: Vec<(WindowId, DialogAction)>,
}

/// The dialog open on one window.
#[derive(Debug)]
enum ActiveDialog {
    Scale(ScaleDialog),
    Delete(DeleteDialog),
}

impl ActiveDialog {
    fn target(&self) -> &ResourceIdentity {
        match self {
            Self::Scale(dialog) => dialog.target(),
            Self::Delete(dialog) => dialog.target(),
        }
    }
}

impl OperationDialogs {
    /// Open (or replace) the scale dialog on `window`.
    pub fn open_scale(
        &mut self,
        window: WindowId,
        target: ResourceIdentity,
        suggested_replicas: Option<u32>,
    ) {
        self.windows.insert(
            window,
            ActiveDialog::Scale(ScaleDialog::for_target(target, suggested_replicas)),
        );
    }

    /// Open (or replace) the delete dialog on `window`.
    pub fn open_delete(&mut self, window: WindowId, target: ResourceIdentity) {
        self.actions.push((
            window,
            DialogAction::RequestDeletePreflight {
                target: target.clone(),
                propagation: DeletePropagation::Background,
            },
        ));
        self.windows.insert(
            window,
            ActiveDialog::Delete(DeleteDialog::for_target(target)),
        );
    }

    /// Which dialog, if any, is open on `window`.
    #[must_use]
    pub fn active(&self, window: WindowId) -> Option<ActiveDialogKind> {
        match self.windows.get(&window) {
            Some(ActiveDialog::Scale(_)) => Some(ActiveDialogKind::Scale),
            Some(ActiveDialog::Delete(_)) => Some(ActiveDialogKind::Delete),
            None => None,
        }
    }

    /// Exact identities currently retained by open mutation dialogs.
    pub fn targets(&self) -> impl Iterator<Item = &ResourceIdentity> {
        self.windows.values().map(ActiveDialog::target)
    }

    /// Mutable access to the dialog open on `window`.
    #[must_use]
    pub fn active_mut(&mut self, window: WindowId) -> Option<DialogHandle<'_>> {
        match self.windows.get_mut(&window) {
            Some(ActiveDialog::Scale(dialog)) => Some(DialogHandle::Scale(dialog)),
            Some(ActiveDialog::Delete(dialog)) => Some(DialogHandle::Delete(dialog)),
            None => None,
        }
    }

    /// Close the dialog on `window`, if any.
    pub fn close(&mut self, window: WindowId) {
        self.windows.remove(&window);
    }

    /// Submit the dialog active on `window`, if it allows a submission
    /// right now. Queues its action for draining. Rendering and pure-state
    /// callers share this path.
    pub fn submit_active(&mut self, window: WindowId) {
        let action = match self.windows.get_mut(&window) {
            Some(ActiveDialog::Scale(dialog)) => dialog.take_action(),
            Some(ActiveDialog::Delete(dialog)) => dialog.take_action(),
            None => None,
        };
        if let Some(action) = action {
            self.actions.push((window, action));
        }
    }

    fn request_delete_preflight(&mut self, window: WindowId) {
        if let Some(ActiveDialog::Delete(dialog)) = self.windows.get(&window) {
            self.actions.push((
                window,
                DialogAction::RequestDeletePreflight {
                    target: dialog.target.clone(),
                    propagation: dialog.propagation,
                },
            ));
        }
    }

    /// Drop entries for closed windows.
    pub fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.windows.retain(|window, _| live(*window));
    }

    /// Notify every open dialog that the transport was lost.
    pub fn connection_lost(&mut self) {
        self.set_connected(false);
    }

    /// Drive every dialog's connectivity flag from the shell each frame, so
    /// a dialog opened before or across a reconnect always reflects the
    /// current transport state.
    pub fn set_connected(&mut self, connected: bool) {
        let mut refresh = Vec::new();
        for (window, dialog) in &mut self.windows {
            match dialog {
                ActiveDialog::Scale(scale) => {
                    if connected {
                        scale.reconnected();
                    } else {
                        scale.connection_lost();
                    }
                }
                ActiveDialog::Delete(delete) => {
                    if connected {
                        if !delete.connected {
                            refresh.push((*window, delete.target.clone(), delete.propagation));
                        }
                        delete.reconnected();
                    } else {
                        delete.connection_lost();
                    }
                }
            }
        }
        self.actions
            .extend(refresh.into_iter().map(|(window, target, propagation)| {
                (
                    window,
                    DialogAction::RequestDeletePreflight {
                        target,
                        propagation,
                    },
                )
            }));
    }

    /// Drain every queued dialog action for submission.
    pub fn drain_actions(&mut self) -> Vec<(WindowId, DialogAction)> {
        std::mem::take(&mut self.actions)
    }

    /// Render every open dialog. Queues actions; never blocks.
    pub fn show(
        &mut self,
        ui: &mut egui::Ui,
        connected: bool,
        mut mutations_allowed: impl FnMut(WindowId, &ResourceIdentity) -> bool,
    ) {
        let windows: Vec<WindowId> = self.windows.keys().copied().collect();
        let mut refresh = Vec::new();
        for window in windows {
            let Some(dialog) = self.windows.get_mut(&window) else {
                continue;
            };
            let mutations_allowed = mutations_allowed(window, dialog.target());
            match dialog {
                ActiveDialog::Scale(scale) => {
                    if connected && mutations_allowed {
                        scale.reconnected()
                    } else {
                        scale.connection_lost()
                    }
                }
                ActiveDialog::Delete(delete) => {
                    if !connected {
                        delete.connection_lost()
                    } else if mutations_allowed {
                        if !delete.connected || delete.clear_stale() {
                            refresh.push((window, delete.target.clone(), delete.propagation));
                        }
                        delete.reconnected()
                    } else {
                        delete.mark_stale("stale data - refresh the resource before deleting")
                    }
                }
            }
            let mut close_requested = false;
            let mut submit_requested = false;
            let mut preflight_requested = false;
            let operation_window = egui::Window::new(dialog_title(dialog))
                .id(egui::Id::new(("k10s.operation-dialog", window.0)))
                .collapsible(false)
                .resizable(false);
            let operation_window = if matches!(dialog, ActiveDialog::Delete(_)) {
                operation_window
                    .default_width(520.0)
                    .max_width(ui.available_width().min(620.0))
            } else {
                operation_window
            };
            operation_window.show(ui, |ui| match dialog {
                ActiveDialog::Scale(scale) => {
                    render_scale(ui, scale, &mut submit_requested, &mut close_requested);
                }
                ActiveDialog::Delete(delete) => {
                    render_delete(
                        ui,
                        delete,
                        &mut submit_requested,
                        &mut preflight_requested,
                        &mut close_requested,
                    );
                }
            });
            if submit_requested {
                self.submit_active(window);
                ui.ctx().request_repaint();
            }
            if preflight_requested {
                self.request_delete_preflight(window);
            }
            if close_requested {
                self.windows.remove(&window);
                ui.ctx().request_repaint();
            }
        }
        self.actions
            .extend(refresh.into_iter().map(|(window, target, propagation)| {
                (
                    window,
                    DialogAction::RequestDeletePreflight {
                        target,
                        propagation,
                    },
                )
            }));
    }
}

fn dialog_title(dialog: &ActiveDialog) -> &'static str {
    match dialog {
        ActiveDialog::Scale(_) => "Scale workload",
        ActiveDialog::Delete(_) => "Delete resource",
    }
}

fn render_scale(ui: &mut egui::Ui, dialog: &mut ScaleDialog, submit: &mut bool, close: &mut bool) {
    ui.label(format!("Set replicas for {}", dialog.target().name));
    ui.add_space(4.0);
    let field = ui.text_edit_singleline(dialog.input_buffer());
    field.widget_info(|| {
        egui::WidgetInfo::labeled(
            egui::WidgetType::TextEdit,
            true,
            "Desired replicas".to_owned(),
        )
    });
    if let Some(reason) = dialog.disabled_reason() {
        ui.label(RichText::new(reason).weak());
    } else {
        ui.label("Replicas will be applied through a background operation.");
    }
    if let Some(failure) = dialog.failure_message() {
        ui.label(RichText::new(format!("Failed: {failure}")).color(ui.visuals().error_fg_color));
    }
    if let Some(operation_id) = dialog.submitted_operation() {
        ui.label(format!("Submitted operation {}", operation_id.as_str()));
    }
    ui.add_space(6.0);
    ui.horizontal(|ui| {
        if ui.button("Cancel").clicked() {
            *close = true;
        }
        let button = ui.add_enabled(dialog.can_submit(), egui::Button::new("Apply scale"));
        button.widget_info(|| {
            egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Apply scale".to_owned())
        });
        if button.clicked() {
            *submit = true;
        }
    });
}

fn render_delete(
    ui: &mut egui::Ui,
    dialog: &mut DeleteDialog,
    submit: &mut bool,
    request_preflight: &mut bool,
    close: &mut bool,
) {
    ui.heading("WARNING — Destructive action");
    ui.label("Complete each safety check in order. Nothing runs until all five pass.");
    ui.strong("1  Scope");
    egui::Grid::new("delete-scope")
        .num_columns(2)
        .show(ui, |ui| {
            let target = dialog.target();
            let gvk = if target.gvk.group.is_empty() {
                format!("{}/{}", target.gvk.version, target.gvk.kind)
            } else {
                format!(
                    "{}/{}/{}",
                    target.gvk.group, target.gvk.version, target.gvk.kind
                )
            };
            for (label, value) in [
                ("Context", target.context.as_str()),
                (
                    "Namespace",
                    target.namespace.as_deref().unwrap_or("Cluster-scoped"),
                ),
                ("GVK", gvk.as_str()),
                ("Name", target.name.as_str()),
                ("UID", target.uid.as_str()),
            ] {
                ui.strong(label);
                ui.label(value);
                ui.end_row();
            }
        });
    ui.add_space(4.0);
    ui.strong("2  Impact");
    if let DestructivePreflight::Ready { impact, .. } = &dialog.preflight {
        ui.label(impact);
    } else {
        ui.label("Deletes this exact object using the selected dependent policy.");
    }
    ui.horizontal(|ui| {
        ui.label("Propagation");
        for (mode, label) in [
            (DeletePropagation::Background, "Background"),
            (DeletePropagation::Foreground, "Foreground"),
            (DeletePropagation::Orphan, "Orphan"),
        ] {
            if ui.radio(dialog.propagation() == mode, label).clicked() {
                dialog.set_propagation(mode);
                *request_preflight = true;
            }
        }
    });
    ui.label(format!("Propagation policy: {:?}", dialog.propagation()));
    ui.strong("3  Server dry run");
    match &dialog.preflight {
        DestructivePreflight::Pending => {
            ui.label("[PENDING] Waiting for authoritative server dry-run.");
        }
        DestructivePreflight::Ready { dry_run, .. } => {
            ui.label(
                RichText::new(format!("[PASS] Server dry-run: {dry_run}"))
                    .color(egui::Color32::GREEN),
            );
        }
        DestructivePreflight::Forbidden(reason) => {
            ui.label(
                RichText::new(format!("[BLOCKED] Forbidden: {reason}"))
                    .color(ui.visuals().error_fg_color),
            );
        }
        DestructivePreflight::Conflict(reason) => {
            ui.label(
                RichText::new(format!("[STALE] Conflict: {reason}")).color(egui::Color32::YELLOW),
            );
        }
        DestructivePreflight::DryRunFailed(reason) => {
            ui.label(
                RichText::new(format!("[FAILED] Server dry-run failed: {reason}"))
                    .color(ui.visuals().error_fg_color),
            );
        }
    }
    ui.strong("4  Typed confirmation");
    ui.label(format!("Type {} to confirm.", dialog.target().name));
    let field = ui.text_edit_singleline(dialog.confirmation_buffer());
    // A single-line editor surrenders focus while handling Enter, so
    // `lost_focus` is part of the same safe, field-owned activation gesture.
    let confirmation_focused = field.has_focus() || field.lost_focus();
    field.widget_info(|| {
        egui::WidgetInfo::labeled(
            egui::WidgetType::TextEdit,
            true,
            "Confirm deletion".to_owned(),
        )
    });
    let command = dialog.kubectl_command();
    ui.strong("5  Exact command");
    ui.label(
        "Equivalent kubectl command (shown for review; the client submits the protocol operation)",
    );
    ui.vertical(|ui| {
        ui.monospace(&command);
        if ui.button("Copy command").clicked() {
            ui.ctx().copy_text(command);
        }
    });
    if let Some(reason) = dialog.disabled_reason() {
        ui.label(RichText::new(reason).weak());
    }
    if let Some(failure) = dialog.failure_message() {
        ui.label(RichText::new(format!("Failed: {failure}")).color(ui.visuals().error_fg_color));
    }
    if let Some(operation_id) = dialog.submitted_operation() {
        ui.label(format!("Submitted operation {}", operation_id.as_str()));
    }
    ui.add_space(6.0);
    ui.horizontal(|ui| {
        if ui.button("Cancel").clicked() {
            *close = true;
        }
        let button = ui.add_enabled(dialog.can_submit(), egui::Button::new("Delete"));
        button.widget_info(|| {
            egui::WidgetInfo::labeled(egui::WidgetType::Button, true, "Confirm delete".to_owned())
        });
        if button.clicked() {
            *submit = true;
        }
    });
    if confirmation_focused
        && ui.input(|input| input.key_pressed(egui::Key::Enter))
        && dialog.can_submit()
    {
        *submit = true;
    }
}
