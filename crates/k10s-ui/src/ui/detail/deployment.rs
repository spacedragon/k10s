//! Placeholder for the frozen Deployment detail body contract.
use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, WindowId, WorkspaceCommand};

pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    _window_id: WindowId,
    _detail: &DetailState<I>,
    _input: &super::presentation::DetailPresentationInput<'_>,
    _queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.label("Structured details unavailable");
}
