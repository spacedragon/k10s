/// Unambiguous result of activating a row in a selectable list.
pub(super) enum RowAction<I> {
    Select(I),
    ClearSelection,
}

impl<I> RowAction<I> {
    pub(super) fn for_row(identity: I, selected: bool) -> Self {
        if selected {
            Self::ClearSelection
        } else {
            Self::Select(identity)
        }
    }

    pub(super) fn into_command(
        self,
        window_id: crate::workspace::WindowId,
    ) -> crate::workspace::WorkspaceCommand<I> {
        match self {
            Self::Select(identity) => {
                crate::workspace::WorkspaceCommand::SelectRow(window_id, identity)
            }
            Self::ClearSelection => crate::workspace::WorkspaceCommand::ClearSelection(window_id),
        }
    }
}

pub(super) fn row_action_label(kind: &str, name: &str, selected: bool) -> String {
    if selected {
        format!("Clear selection for {kind} {name}")
    } else {
        format!("Select {kind} {name}")
    }
}

/// Resolve one row response with double-click priority. The first-click
/// origin lets callers undo a provisional selection (or cancel its guard)
/// when the second click promotes the gesture to a dedicated-detail action.
pub(super) fn row_interaction<I: Clone>(
    response: &egui::Response,
    identity: I,
    selected: bool,
) -> (Option<RowAction<I>>, Option<I>, bool) {
    let origin_id = response.id.with("k10s.row.click-origin");
    if response.double_clicked() {
        let began_selected = response
            .ctx
            .data_mut(|data| data.remove_temp::<bool>(origin_id))
            .unwrap_or(selected);
        (
            (!began_selected).then_some(RowAction::ClearSelection),
            Some(identity),
            true,
        )
    } else if response.clicked() {
        response
            .ctx
            .data_mut(|data| data.insert_temp(origin_id, selected));
        (Some(RowAction::for_row(identity, selected)), None, false)
    } else {
        (None, None, false)
    }
}
