//! Per-list-window resource state: filters, sort, selection, and the
//! integrated detail pane.

use std::collections::BTreeMap;

use super::detail::DetailState;

/// List sorting specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    pub column: String,
    pub ascending: bool,
}

/// State owned by one resource list window (Overview, Nodes, Storage, and
/// every workload instance). Two windows of the same kind keep fully
/// independent namespace, search, filters, sort, split, and selection state.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceWindowState<I> {
    /// Local namespace filter; `None` means "all namespaces".
    pub namespace: Option<String>,
    pub search: String,
    /// Key/value list filters (for example `phase` → `Running`).
    pub filters: BTreeMap<String, String>,
    pub sort: Option<SortSpec>,
    /// Selected row identity; owns the integrated detail below.
    pub selection: Option<I>,
    /// Fraction of the window height given to the list pane, clamped to
    /// the unit interval. Pane minima are enforced by the renderer.
    pub split_ratio: f32,
    /// Whether the detail pane is visible; hiding keeps the detail state.
    pub detail_visible: bool,
    /// Integrated detail state, present exactly when `selection` is.
    pub detail: Option<DetailState<I>>,
}

impl<I> Default for ResourceWindowState<I> {
    fn default() -> Self {
        Self {
            namespace: None,
            search: String::new(),
            filters: BTreeMap::new(),
            sort: None,
            selection: None,
            split_ratio: 0.5,
            detail_visible: true,
            detail: None,
        }
    }
}
