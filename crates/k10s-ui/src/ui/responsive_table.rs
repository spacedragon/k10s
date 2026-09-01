/// Unambiguous result of activating a row in a selectable list.
pub(super) enum RowAction<I> {
    Select(I),
    ClearSelection,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ColumnSpec {
    pub key: &'static str,
    pub min_width: f32,
    pub elastic: bool,
    hide_priority: Option<u8>,
}

impl ColumnSpec {
    pub const fn required(key: &'static str, min_width: f32) -> Self {
        Self {
            key,
            min_width,
            elastic: false,
            hide_priority: None,
        }
    }

    pub const fn elastic(key: &'static str, min_width: f32) -> Self {
        Self {
            key,
            min_width,
            elastic: true,
            hide_priority: None,
        }
    }

    pub const fn hideable(key: &'static str, min_width: f32, hide_priority: u8) -> Self {
        Self {
            key,
            min_width,
            elastic: false,
            hide_priority: Some(hide_priority),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedColumn {
    pub key: &'static str,
    pub width: f32,
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedColumns {
    pub visible: Vec<ResolvedColumn>,
    pub horizontal_scroll: bool,
}

impl ResolvedColumns {
    #[cfg(test)]
    pub fn contains(&self, key: &str) -> bool {
        self.visible.iter().any(|column| column.key == key)
    }
    #[cfg(test)]
    pub fn width(&self, key: &str) -> Option<f32> {
        self.visible
            .iter()
            .find(|column| column.key == key)
            .map(|column| column.width)
    }
    #[cfg(test)]
    fn visible_keys(&self) -> Vec<&'static str> {
        self.visible.iter().map(|column| column.key).collect()
    }
}

pub(super) fn resolve_columns(specs: &[ColumnSpec], available_width: f32) -> ResolvedColumns {
    let mut visible = vec![true; specs.len()];
    let mut total: f32 = specs.iter().map(|column| column.min_width).sum();
    let mut hideable: Vec<_> = specs
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.hide_priority.map(|priority| (priority, index)))
        .collect();
    hideable.sort_by_key(|&(priority, index)| (priority, index));
    for (_, index) in hideable {
        if total <= available_width {
            break;
        }
        visible[index] = false;
        total -= specs[index].min_width;
    }
    let horizontal_scroll = total > available_width;
    let elastic_count = specs
        .iter()
        .enumerate()
        .filter(|(index, column)| visible[*index] && column.elastic)
        .count();
    let extra = if horizontal_scroll || elastic_count == 0 {
        0.0
    } else {
        (available_width - total).max(0.0) / elastic_count as f32
    };
    let visible = specs
        .iter()
        .enumerate()
        .filter(|(index, _)| visible[*index])
        .map(|(_, column)| ResolvedColumn {
            key: column.key,
            width: column.min_width + if column.elastic { extra } else { 0.0 },
        })
        .collect();
    ResolvedColumns {
        visible,
        horizontal_scroll,
    }
}

/// Unicode-safe compact representation retaining both identifying ends.
pub(super) fn middle_elide(value: &str, max_chars: usize) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= max_chars {
        return value.to_owned();
    }
    if max_chars <= 1 {
        return "…".chars().take(max_chars).collect();
    }
    let tagged_suffix = value.rfind(':').map(|byte| value[byte..].chars().count());
    let suffix = tagged_suffix
        .filter(|count| *count < max_chars - 1)
        .unwrap_or((max_chars - 1) / 2);
    let prefix = max_chars - 1 - suffix;
    chars[..prefix]
        .iter()
        .chain(std::iter::once(&'…'))
        .chain(chars[chars.len() - suffix..].iter())
        .collect()
}

/// Paint a resolved cell at its exact content width; the surrounding Grid
/// continues to own the standard inter-column padding.
pub(super) fn sized_cell(
    ui: &mut egui::Ui,
    width: f32,
    right_aligned: bool,
    add_contents: impl FnOnce(&mut egui::Ui),
) {
    let layout = if right_aligned {
        egui::Layout::right_to_left(egui::Align::Center)
    } else {
        egui::Layout::left_to_right(egui::Align::Center)
    };
    ui.allocate_ui_with_layout(
        egui::vec2(width, ui.spacing().interact_size.y),
        layout,
        add_contents,
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    const SERVICE: [ColumnSpec; 6] = [
        ColumnSpec::required("namespace", 112.0),
        ColumnSpec::elastic("name", 180.0),
        ColumnSpec::hideable("type", 88.0, 1),
        ColumnSpec::hideable("cluster_ip", 120.0, 0),
        ColumnSpec::elastic("ports", 180.0),
        ColumnSpec::required("age", 56.0),
    ];

    #[test]
    fn resolver_hides_in_priority_order_and_restores_deterministically() {
        let wide = resolve_columns(&SERVICE, 1_000.0);
        assert_eq!(
            wide.visible_keys(),
            vec!["namespace", "name", "type", "cluster_ip", "ports", "age"]
        );
        assert!(!wide.horizontal_scroll);

        let compact = resolve_columns(&SERVICE, 640.0);
        assert!(!compact.contains("cluster_ip"));
        assert!(compact.contains("type"));

        let restored = resolve_columns(&SERVICE, 1_000.0);
        assert_eq!(restored.visible_keys(), wide.visible_keys());
    }

    #[test]
    fn resolver_never_compresses_minima_and_reports_overflow() {
        let required = [
            ColumnSpec::required("namespace", 400.0),
            ColumnSpec::elastic("name", 300.0),
        ];
        let resolved = resolve_columns(&required, 640.0);
        assert!(resolved.horizontal_scroll);
        assert_eq!(resolved.width("namespace"), Some(400.0));
        assert_eq!(resolved.width("name"), Some(300.0));
    }

    #[test]
    fn adapter_fixtures_fit_wide_and_apply_exact_compact_priorities() {
        let deployment = [
            ColumnSpec::required("namespace", 112.0),
            ColumnSpec::elastic("name", 180.0),
            ColumnSpec::required("ready", 56.0),
            ColumnSpec::hideable("status", 112.0, 1),
            ColumnSpec::hideable("image", 180.0, 0),
            ColumnSpec::required("age", 56.0),
        ];
        let pod = [
            ColumnSpec::required("namespace", 112.0),
            ColumnSpec::elastic("name", 180.0),
            ColumnSpec::required("ready", 56.0),
            ColumnSpec::required("status", 112.0),
            ColumnSpec::hideable("restarts", 64.0, 1),
            ColumnSpec::hideable("node", 120.0, 0),
            ColumnSpec::required("age", 56.0),
        ];
        for fixture in [&deployment[..], &pod[..], &SERVICE[..]] {
            assert_eq!(
                resolve_columns(fixture, 1_000.0).visible.len(),
                fixture.len()
            );
        }
        assert!(!resolve_columns(&deployment, 640.0).contains("image"));
        assert!(resolve_columns(&deployment, 640.0).contains("status"));
        assert!(!resolve_columns(&pod, 640.0).contains("node"));
        assert!(resolve_columns(&pod, 640.0).contains("restarts"));
    }

    #[test]
    fn middle_elision_preserves_image_tag_and_unicode_boundaries() {
        let image = "ghcr.io/containers/kubernetes-mcp:v0.3.1";
        let compact = middle_elide(image, 24);
        assert!(compact.ends_with(":v0.3.1"));
        assert!(compact.chars().count() <= 24);
        assert_eq!(middle_elide("部署镜像:v1", 6), "部署…:v1");
    }
}
