//! Vertical split layout for resource windows.
//!
//! The list pane keeps a hard minimum height and the integrated detail
//! pane keeps a larger one; extreme split ratios clamp to those minima so
//! neither pane can ever collapse while both are visible.

use egui::{Align, Layout, UiBuilder, Vec2};

/// Hard minimum height of the resource list pane.
pub(super) const LIST_PANE_MIN: f32 = 120.0;
/// Hard minimum height of the integrated detail pane.
pub(super) const DETAIL_PANE_MIN: f32 = 180.0;
/// Height of the draggable separator between panes.
const SEPARATOR_HEIGHT: f32 = 8.0;

/// Compute `(list_height, detail_height)` for `total` available points.
///
/// The desired list share is `ratio * total`, clamped so that a visible
/// detail pane never drops below [`DETAIL_PANE_MIN`] and the list pane
/// never drops below [`LIST_PANE_MIN`]. When the total cannot fit both
/// minima the list takes everything (the window minimum size prevents this
/// in practice).
pub(super) fn pane_heights(total: f32, ratio: f32, detail_visible: bool) -> (f32, f32) {
    if !detail_visible {
        return (total, 0.0);
    }
    let max_list = (total - DETAIL_PANE_MIN - SEPARATOR_HEIGHT).max(0.0);
    let min_list = LIST_PANE_MIN.min(max_list.max(LIST_PANE_MIN));
    if total < LIST_PANE_MIN + DETAIL_PANE_MIN + SEPARATOR_HEIGHT {
        return (total, 0.0);
    }
    let desired = total * ratio.clamp(0.0, 1.0);
    let list = desired.clamp(min_list, max_list);
    (list, total - list - SEPARATOR_HEIGHT)
}

/// Render the vertical split: `top` (the list) and, when visible, `bottom`
/// (the detail) with a draggable separator between them. Returns the
/// closures' results, `(None, None)` when the detail pane is hidden.
pub(super) fn show_vertical<R, S>(
    ui: &mut egui::Ui,
    ratio: &mut f32,
    detail_visible: bool,
    detail_maximized: bool,
    top: impl FnOnce(&mut egui::Ui) -> R,
    bottom: impl FnOnce(&mut egui::Ui) -> S,
) -> (Option<R>, Option<S>) {
    let total = ui.available_size().y;
    let pane_item_spacing = ui.spacing().item_spacing;
    // The split owns every point in `total`; parent-layout spacing between
    // its exact allocations would otherwise be added outside that budget.
    ui.spacing_mut().item_spacing.y = 0.0;
    if detail_visible && detail_maximized {
        let width = ui.available_size().x;
        let bottom_result = show_fixed_pane(ui, "detail", width, total, pane_item_spacing, bottom);
        ui.spacing_mut().item_spacing = pane_item_spacing;
        return (None, Some(bottom_result));
    }
    let (list_height, detail_height) = pane_heights(total, *ratio, detail_visible);
    let width = ui.available_size().x;

    let top_result = show_fixed_pane(ui, "list", width, list_height, pane_item_spacing, top);

    let mut bottom_result = None;
    if detail_visible && detail_height > 0.0 {
        let (_, separator) =
            ui.allocate_exact_size(Vec2::new(width, SEPARATOR_HEIGHT), egui::Sense::drag());
        if separator.dragged() {
            let delta = separator.drag_delta().y / total.max(1.0);
            *ratio = (*ratio + delta).clamp(0.05, 0.95);
        }
        if separator.hovered() || separator.dragged() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeVertical);
        }

        bottom_result = Some(show_fixed_pane(
            ui,
            "detail",
            width,
            detail_height,
            pane_item_spacing,
            bottom,
        ));
    }
    ui.spacing_mut().item_spacing = pane_item_spacing;
    (Some(top_result), bottom_result)
}

/// Render a pane inside an exact parent allocation.
///
/// `Ui::allocate_ui_with_layout` expands its parent allocation when child
/// content reports a larger minimum. That is useful for flowing layouts, but
/// a split pane must instead keep the outer window fixed and let its own
/// scroll regions handle overflow.
fn show_fixed_pane<R>(
    ui: &mut egui::Ui,
    id_salt: &'static str,
    width: f32,
    height: f32,
    item_spacing: Vec2,
    content: impl FnOnce(&mut egui::Ui) -> R,
) -> R {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::hover());
    let mut pane = ui.new_child(
        UiBuilder::new()
            .id_salt(("k10s.split.pane", id_salt))
            .max_rect(rect)
            .layout(Layout::top_down_justified(Align::Min)),
    );
    pane.spacing_mut().item_spacing = item_spacing;
    pane.set_clip_rect(rect.intersect(ui.clip_rect()));
    content(&mut pane)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hidden_detail_gives_everything_to_the_list() {
        assert_eq!(pane_heights(400.0, 0.5, false), (400.0, 0.0));
    }

    #[test]
    fn extreme_ratios_clamp_to_the_pane_minima() {
        let total = 360.0;
        let (list, detail) = pane_heights(total, 0.0, true);
        assert_eq!(list, LIST_PANE_MIN);
        assert!(detail >= DETAIL_PANE_MIN);

        let (list, detail) = pane_heights(total, 1.0, true);
        assert_eq!(detail, DETAIL_PANE_MIN);
        assert!(list >= LIST_PANE_MIN);
        assert!((list + detail + SEPARATOR_HEIGHT - total).abs() < f32::EPSILON);
    }

    #[test]
    fn balanced_ratios_split_proportionally_between_the_minima() {
        let total = 600.0;
        let (list, detail) = pane_heights(total, 0.5, true);
        assert_eq!(list, 300.0);
        assert_eq!(detail, 300.0 - SEPARATOR_HEIGHT);
    }
}
