//! Placeholder for the frozen Deployment detail body contract.
use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, WindowId, WorkspaceCommand};

/// Configure Deployment-owned shared chrome before the frame paints its vitals.
pub(super) fn configure_frame(
    _input: &super::presentation::DetailPresentationInput<'_>,
    frame: &mut super::presentation::DetailFrameProjection<'_>,
) {
    if let Some(rollout) = frame
        .visible_vitals
        .iter_mut()
        .find(|vital| vital.label == "Rollout")
    {
        rollout.shape = Some(super::presentation::DetailVitalShape::Dot);
    }
}

pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    _window_id: WindowId,
    _detail: &DetailState<I>,
    _input: &super::presentation::DetailPresentationInput<'_>,
    _frame: &mut super::presentation::DetailFrameProjection<'_>,
    _queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.label("Structured details unavailable");
}
