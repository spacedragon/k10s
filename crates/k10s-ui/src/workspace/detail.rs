//! Per-detail state: active tab and YAML edit buffer.
//!
//! Kept intentionally small in this task; kind-specific tabs (Task 6), the
//! guarded YAML workflow (Task 7) and log sessions (Task 8) extend
//! these fields without changing the guard contract.

use super::guard::{BlockReason, Blocker};
use super::window::WindowId;

/// Tabs shown in the integrated detail pane and dedicated detail windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetailTab {
    Overview,
    /// Structured Service endpoints and EndpointSlices.
    Endpoints,
    Pods,
    Yaml,
    Events,
    Logs,
}

/// Guarded YAML edit buffer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct YamlState {
    /// `Edit` marks the detail dirty; the buffer must be reviewed,
    /// discarded, or applied before destructive navigation is allowed.
    pub dirty: bool,
}

/// State of one detail view: the integrated pane of a resource window or a
/// dedicated detail window pinned to `identity`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetailState<I> {
    /// Stable identity this detail is bound to. Dedicated windows never
    /// follow later integrated selections.
    pub identity: I,
    pub active_tab: DetailTab,
    pub yaml: YamlState,
}

impl<I> DetailState<I> {
    pub fn new(identity: I) -> Self {
        Self {
            identity,
            active_tab: DetailTab::Overview,
            yaml: YamlState::default(),
        }
    }

    /// Every reason this detail state blocks destructive navigation.
    pub fn blockers(&self, window: WindowId) -> Vec<Blocker> {
        let mut blockers = Vec::new();
        if self.yaml.dirty {
            blockers.push(Blocker {
                window,
                reason: BlockReason::DirtyYaml,
            });
        }
        blockers
    }
}
