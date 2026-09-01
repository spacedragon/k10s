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

#[derive(Clone)]
struct PendingRowGesture<I> {
    identity: I,
    clear_selection: bool,
    deadline: f64,
}

fn pending_id(table_id: egui::Id) -> egui::Id {
    table_id.with("k10s.table.pending-row-action")
}

/// Consume a due gesture independently of which rows virtualization renders.
pub(super) fn poll_row_action<I>(ctx: &egui::Context, table_id: egui::Id) -> Option<RowAction<I>>
where
    I: Clone + Send + Sync + 'static,
{
    let pending_id = pending_id(table_id);
    let (now, another_click) = ctx.input(|input| (input.time, input.pointer.any_click()));
    let pending = ctx.data_mut(|data| data.get_temp::<PendingRowGesture<I>>(pending_id));
    if let Some(pending) = pending
        && now >= pending.deadline
        && !another_click
    {
        ctx.data_mut(|data| data.remove::<PendingRowGesture<I>>(pending_id));
        Some(if pending.clear_selection {
            RowAction::ClearSelection
        } else {
            RowAction::Select(pending.identity)
        })
    } else {
        None
    }
}

/// Arm, replace, or cancel the table-scoped gesture from one row response.
/// Every single click waits for egui's configured double-click interval.
pub(super) fn row_interaction<I>(
    response: &egui::Response,
    table_id: egui::Id,
    identity: I,
    selected: bool,
) -> Option<I>
where
    I: Clone + Send + Sync + 'static,
{
    let pending_id = pending_id(table_id);
    if response.double_clicked() {
        response
            .ctx
            .data_mut(|data| data.remove::<PendingRowGesture<I>>(pending_id));
        Some(identity)
    } else if response.clicked() {
        let now = response.ctx.input(|input| input.time);
        let delay = response
            .ctx
            .options(|options| options.input_options.max_double_click_delay);
        response.ctx.data_mut(|data| {
            data.insert_temp(
                pending_id,
                PendingRowGesture {
                    identity,
                    clear_selection: selected,
                    deadline: now + delay,
                },
            );
        });
        response
            .ctx
            .request_repaint_after(std::time::Duration::from_secs_f64(delay));
        None
    } else {
        None
    }
}
