/// Unambiguous result of activating a row in a selectable list.
pub(super) enum RowAction<I> {
    Select(I),
    ClearSelection,
}

impl<I> RowAction<I> {
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

/// Resolve one row response with double-click priority. Clearing an already
/// selected row waits for egui's configured double-click interval; selecting
/// a different row remains immediate.
pub(super) fn row_interaction<I: Clone>(
    response: &egui::Response,
    identity: I,
    selected: bool,
) -> (Option<RowAction<I>>, Option<I>) {
    let pending_id = response.id.with("k10s.row.pending-action");
    if response.double_clicked() {
        let began_selected = response
            .ctx
            .data_mut(|data| data.remove_temp::<(bool, f64)>(pending_id))
            .map(|pending| pending.0)
            .unwrap_or(selected);
        (
            (!began_selected).then_some(RowAction::ClearSelection),
            Some(identity),
        )
    } else if response.clicked() {
        let now = response.ctx.input(|input| input.time);
        let delay = response
            .ctx
            .options(|options| options.input_options.max_double_click_delay);
        response
            .ctx
            .data_mut(|data| data.insert_temp(pending_id, (selected, now)));
        response
            .ctx
            .request_repaint_after(std::time::Duration::from_secs_f64(delay));
        ((!selected).then_some(RowAction::Select(identity)), None)
    } else {
        let now = response.ctx.input(|input| input.time);
        let delay = response
            .ctx
            .options(|options| options.input_options.max_double_click_delay);
        let pending = response
            .ctx
            .data_mut(|data| data.get_temp::<(bool, f64)>(pending_id));
        if let Some((began_selected, clicked_at)) = pending
            && now - clicked_at >= delay
        {
            response
                .ctx
                .data_mut(|data| data.remove_temp::<(bool, f64)>(pending_id));
            (began_selected.then_some(RowAction::ClearSelection), None)
        } else {
            (None, None)
        }
    }
}
