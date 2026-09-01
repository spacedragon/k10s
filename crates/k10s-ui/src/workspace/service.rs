//! Per-Services-window state: filters, sort, selection, the integrated
//! detail pane, and local-port drafts keyed by Service port.
//!
//! Active port-forward sessions are authoritative client state owned by the
//! connection layer; this struct holds only window-local UI data.

use std::collections::BTreeMap;

use super::detail::DetailState;
use super::resource::{NamespaceScope, SortSpec};

/// State owned by the singleton Services window. It follows the same
/// command-driven shape as workload list windows so guards, geometry, and
/// context switching behave identically.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceWindowState<I> {
    pub namespace_scope: NamespaceScope,
    pub search: String,
    pub sort: Option<SortSpec>,
    /// Selected row identity; owns the integrated detail below.
    pub selection: Option<I>,
    /// Fraction of the window height given to the list pane, clamped to
    /// the unit interval. Pane minima are enforced by the renderer.
    pub split_ratio: f32,
    /// Split to restore after a focused, detail-only view.
    pub prior_split_ratio: Option<f32>,
    /// Legacy snapshot compatibility only. Runtime visibility is derived
    /// from `detail`; persisted false values are ignored and normalized true.
    pub detail_visible: bool,
    /// Integrated detail state, present exactly when `selection` is.
    pub detail: Option<DetailState<I>>,
    /// Draft text of the local-port input per Service port, keyed by
    /// `<identity.uid>/<service_port>`. Blank or `0` asks for an OS-assigned
    /// port; the input survives window close and context switches.
    pub port_drafts: BTreeMap<String, String>,
}

impl<I> Default for ServiceWindowState<I> {
    fn default() -> Self {
        Self {
            namespace_scope: NamespaceScope::AllNamespaces,
            search: String::new(),
            sort: None,
            selection: None,
            split_ratio: 0.5,
            prior_split_ratio: None,
            detail_visible: true,
            detail: None,
            port_drafts: BTreeMap::new(),
        }
    }
}

impl<I> ServiceWindowState<I> {
    /// Stable key of one Service port inside [`Self::port_drafts`].
    #[must_use]
    pub fn port_draft_key(uid: &str, service_port: u16) -> String {
        format!("{uid}/{service_port}")
    }
}
