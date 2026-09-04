#![allow(dead_code)]
//! Shared helpers for the UI integration tests.
//!
//! The List + Detail redesign (#193) moved the active namespace scope into
//! the workload window title (`Deployments · all namespaces`) and folded the
//! selector labels into their controls (`Namespace: all ▾`). These helpers
//! keep the per-test queries aligned with that layout in one place.

/// Workload windows carry their active scope in the title
/// (`Deployments · all namespaces`), so tests address them by kind. The
/// taskbar repeats the same title on its buttons, hence the role filter.
use egui_kittest::kittest::Queryable as _;

pub fn workload_window<'n, T>(
    harness: &'n egui_kittest::Harness<T>,
    kind_title: &str,
) -> egui_kittest::Node<'n> {
    use egui_kittest::kittest::NodeT as _;
    // The query API borrows the filter with the tree's lifetime, so test
    // helpers leak the (tiny) search string instead of fighting lifetimes.
    let needle: &'static str = Box::leak(format!("{kind_title} ·").into_boxed_str());
    harness
        .query_all_by_label_contains(needle)
        .find(|node| node.accesskit_node().role() == egui::accesskit::Role::Window)
        .unwrap_or_else(|| panic!("no workload window titled {kind_title:?} · scope"))
}

/// Like [`workload_window`], but for every matching instance.
pub fn workload_window_all<'n, T>(
    harness: &'n egui_kittest::Harness<T>,
    kind_title: &str,
) -> impl DoubleEndedIterator<Item = egui_kittest::Node<'n>> + 'n {
    use egui_kittest::kittest::NodeT as _;
    let needle: &'static str = Box::leak(format!("{kind_title} ·").into_boxed_str());
    harness
        .query_all_by_label_contains(needle)
        .filter(move |node| node.accesskit_node().role() == egui::accesskit::Role::Window)
}

/// The namespace selector carries its own label inside the control
/// (`Namespace: all ▾`), so tests address it by that value prefix. Pass
/// `harness.root()` or a window node.
pub fn namespace_combobox<'n>(node: egui_kittest::Node<'n>) -> egui_kittest::Node<'n> {
    use egui_kittest::kittest::NodeT as _;
    node.query_all_by_role(egui::accesskit::Role::ComboBox)
        .find(|node| {
            node.accesskit_node()
                .label()
                .is_some_and(|label| label.starts_with("Namespace: "))
                || node
                    .value()
                    .is_some_and(|value| value == "Namespace" || value.starts_with("Namespace: "))
        })
        .expect("the toolbar Namespace combobox")
}
