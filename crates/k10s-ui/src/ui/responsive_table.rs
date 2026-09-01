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

#[derive(Clone, Copy)]
struct PendingRowGesture {
    row_id: egui::Id,
    clear_selection: bool,
    deadline: f64,
}

impl Default for PendingRowGesture {
    fn default() -> Self {
        Self {
            row_id: egui::Id::NULL,
            clear_selection: false,
            deadline: 0.0,
        }
    }
}

/// Resolve one row response against one table-scoped pending gesture. Every
/// single click waits for egui's configured double-click interval.
pub(super) fn row_interaction<I: Clone>(
    response: &egui::Response,
    table_id: egui::Id,
    row_id: egui::Id,
    identity: I,
    selected: bool,
) -> (Option<RowAction<I>>, Option<I>) {
    let pending_id = table_id.with("k10s.table.pending-row-action");
    if response.double_clicked() {
        response
            .ctx
            .data_mut(|data| data.remove_temp::<PendingRowGesture>(pending_id));
        (None, Some(identity))
    } else if response.clicked() {
        let now = response.ctx.input(|input| input.time);
        let delay = response
            .ctx
            .options(|options| options.input_options.max_double_click_delay);
        response.ctx.data_mut(|data| {
            data.insert_temp(
                pending_id,
                PendingRowGesture {
                    row_id,
                    clear_selection: selected,
                    deadline: now + delay,
                },
            );
        });
        response
            .ctx
            .request_repaint_after(std::time::Duration::from_secs_f64(delay));
        (None, None)
    } else {
        let (now, another_click) = response
            .ctx
            .input(|input| (input.time, input.pointer.any_click()));
        let pending = response
            .ctx
            .data_mut(|data| data.get_temp::<PendingRowGesture>(pending_id));
        if let Some(pending) = pending
            && pending.row_id == row_id
            && now >= pending.deadline
            && !another_click
        {
            response
                .ctx
                .data_mut(|data| data.remove_temp::<PendingRowGesture>(pending_id));
            let action = if pending.clear_selection {
                RowAction::ClearSelection
            } else {
                RowAction::Select(identity)
            };
            (Some(action), None)
        } else {
            (None, None)
        }
    }
}
