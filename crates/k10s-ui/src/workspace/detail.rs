//! Per-detail state: active tab, YAML edit buffer, and shell session.
//!
//! Kept intentionally small in this task; kind-specific tabs (Task 6), the
//! guarded YAML workflow (Task 7), and log/shell sessions (Task 8) extend
//! these fields without changing the guard contract.

use super::guard::{BlockReason, Blocker};
use super::window::WindowId;

/// Tabs shown in the integrated detail pane and dedicated detail windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetailTab {
    Overview,
    /// Structured Service ports (and later port-forward controls).
    Ports,
    Pods,
    Yaml,
    Events,
    Logs,
    Shell,
}

/// Guarded YAML edit buffer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct YamlState {
    /// `Edit` marks the detail dirty; the buffer must be reviewed,
    /// discarded, or applied before destructive navigation is allowed.
    pub dirty: bool,
}

/// Shell session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShellState {
    /// Whether an exec session is currently connected.
    pub connected: bool,
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
    pub shell: ShellState,
}

impl<I> DetailState<I> {
    pub fn new(identity: I) -> Self {
        Self {
            identity,
            active_tab: DetailTab::Overview,
            yaml: YamlState::default(),
            shell: ShellState::default(),
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
        if self.shell.connected {
            blockers.push(Blocker {
                window,
                reason: BlockReason::ConnectedShell,
            });
        }
        blockers
    }
}
