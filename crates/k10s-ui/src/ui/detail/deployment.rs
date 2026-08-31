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
    _resource_actions: &mut Vec<crate::ui::ResourceAction>,
    _queued: &mut Vec<WorkspaceCommand<I>>,
) {
    ui.label("Structured details unavailable");
}

#[cfg(test)]
mod tests {
    use egui_kittest::Harness;
    use k10s_protocol::{GroupVersionKind, ResourceIdentity};

    use super::super::presentation::{DetailMetrics, DetailPresentationInput, DetailPrimary};
    use crate::workspace::{DetailState, WindowId};

    #[test]
    fn show_contract_accepts_the_shared_resource_action_queue() {
        let identity = ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
        };
        let detail = DetailState::new(identity.clone());
        let mut harness = Harness::builder().build_ui(move |ui| {
            let input = DetailPresentationInput {
                identity: &identity,
                primary: DetailPrimary::Loading,
                metrics: DetailMetrics {
                    status: None,
                    age: None,
                },
                resource_metrics: None,
                relations: None,
                freshness: None,
                now: web_time::UNIX_EPOCH,
                gone: false,
                mutations_allowed: false,
                port_forward_available: false,
                port_forward_sessions: &[],
                port_forward_error: None,
            };
            let mut frame = input.frame_projection(Default::default());
            let mut queued = Vec::new();
            let mut resource_actions = Vec::new();
            super::show(
                ui,
                WindowId(9),
                &detail,
                &input,
                &mut frame,
                &mut resource_actions,
                &mut queued,
            );
            assert!(resource_actions.is_empty());
        });
        harness.run();
    }
}
