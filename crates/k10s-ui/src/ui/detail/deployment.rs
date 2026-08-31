//! Placeholder for the frozen Deployment detail body contract.
use crate::ui::resource_window::RowIdentity;
use crate::workspace::{DetailState, WindowId, WorkspaceCommand};

pub(super) fn show<I: RowIdentity>(
    ui: &mut egui::Ui,
    _window_id: WindowId,
    _detail: &DetailState<I>,
    _input: &super::presentation::DetailPresentationInput<'_>,
    frame: &mut super::presentation::DetailFrameProjection<'_>,
    _queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let super::presentation::DetailPrimary::Loaded(view) = _input.primary else {
        return;
    };
    let Some(k10s_protocol::ResourceProjection::Deployment(projection)) = &view.projection else {
        return;
    };
    super::frame::show_typed_stub(ui, "Deployment", &projection.labels, frame);
}
