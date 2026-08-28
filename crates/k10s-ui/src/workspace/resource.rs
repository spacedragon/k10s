//! Per-list-window resource state: filters, sort, selection, and the
//! integrated detail pane.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::detail::DetailState;

/// Namespace intent for a namespaced list. `ContextDefault` remains solely
/// for decoding legacy snapshots and is normalized before entering live state.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum NamespaceScope {
    ContextDefault,
    Namespace(String),
    #[default]
    AllNamespaces,
}

impl NamespaceScope {
    pub(crate) fn into_live(self) -> Self {
        match self {
            Self::ContextDefault => Self::AllNamespaces,
            scope => scope,
        }
    }

    #[must_use]
    pub fn resolve<'a>(&'a self, context_namespace: Option<&'a str>) -> Option<&'a str> {
        match self {
            Self::ContextDefault => Some(context_namespace.unwrap_or("default")),
            Self::Namespace(namespace) => Some(namespace),
            Self::AllNamespaces => None,
        }
    }
}

/// List sorting specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortSpec {
    pub column: String,
    pub ascending: bool,
}

/// State owned by one resource list window (Overview, Nodes, Storage, and
/// every workload instance). Two windows of the same kind keep fully
/// independent namespace, search, filters, sort, split, and selection state.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceWindowState<I> {
    pub namespace_scope: NamespaceScope,
    pub search: String,
    /// Key/value list filters (for example `phase` → `Running`).
    pub filters: BTreeMap<String, String>,
    pub sort: Option<SortSpec>,
    /// Selected row identity; owns the integrated detail below.
    pub selection: Option<I>,
    /// Fraction of the window height given to the list pane, clamped to
    /// the unit interval. Pane minima are enforced by the renderer.
    pub split_ratio: f32,
    /// Split to restore after a focused, detail-only view. This is transient
    /// interaction state and is intentionally omitted from snapshots.
    pub prior_split_ratio: Option<f32>,
    /// Whether the detail pane is visible; hiding keeps the detail state.
    pub detail_visible: bool,
    /// Selected type of a custom-resources window, as a canonical
    /// `group/version/kind` key; `None` shows the GVK picker.
    pub custom_kind: Option<String>,
    /// Integrated detail state, present exactly when `selection` is.
    pub detail: Option<DetailState<I>>,
}

impl<I> Default for ResourceWindowState<I> {
    fn default() -> Self {
        Self {
            namespace_scope: NamespaceScope::AllNamespaces,
            search: String::new(),
            filters: BTreeMap::new(),
            sort: None,
            selection: None,
            split_ratio: 0.5,
            prior_split_ratio: None,
            detail_visible: true,
            detail: None,
            custom_kind: None,
        }
    }
}
