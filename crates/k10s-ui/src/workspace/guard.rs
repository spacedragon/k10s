//! Navigation guards: pending navigations blocked by dirty YAML or a
//! connected shell, plus the resolutions that release them.

use super::WorkspaceCommand;
use super::window::WindowId;

/// Why a navigation is blocked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockReason {
    /// The detail holds an unsaved YAML edit buffer.
    DirtyYaml,
    /// The detail has a connected shell session.
    ConnectedShell,
}

/// One blocking detail state, keyed by its window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Blocker {
    pub window: WindowId,
    pub reason: BlockReason,
}

/// A navigation that waits until every blocker is resolved. The command is
/// committed exactly once, only after all blockers have been released;
/// `Cancel` drops it and preserves the current selection, window, and
/// context.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingNavigation<I> {
    pub command: WorkspaceCommand<I>,
    pub blockers: Vec<Blocker>,
}

/// How the user resolves blockers on the pending navigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockResolution {
    /// Discard the dirty YAML buffer of `window`.
    DiscardYaml { window: WindowId },
    /// Disconnect the shell session of `window`.
    DisconnectShell { window: WindowId },
    /// Abort the navigation entirely; nothing changes.
    Cancel,
}
