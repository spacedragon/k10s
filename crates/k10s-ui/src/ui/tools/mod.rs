//! Connected tool workflows rendered inside detail views: the guarded YAML
//! editor, the connected log viewer, and the exec terminal.

pub mod logs;
pub mod shell;
pub mod yaml;

use crate::workspace::WindowId;

pub use logs::{
    DEFAULT_TAIL_CAPACITY, LogsAction, LogsPhase, LogsTool, LogsViews, MAX_LINE_CHARS,
    TRUNCATION_MARKER,
};
pub use shell::{ShellAction, ShellPhase, ShellSessions, ShellTool};
pub use yaml::{DiffKind, DiffLine, YamlAction, YamlEditor, YamlEditors, YamlPhase};

/// The connected stream tool stores threaded through the UI shell: per-
/// window log views and terminal sessions. Owned by the application layer,
/// which drains rendering-time actions and projects stream signals back.
#[derive(Debug, Default)]
pub struct StreamStores {
    /// Connected log viewers.
    pub logs: LogsViews,
    /// Interactive terminal sessions.
    pub shells: ShellSessions,
}

impl StreamStores {
    /// Drop entries for closed windows.
    pub fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.logs.retain(&live);
        self.shells.retain(live);
    }

    /// Notify every store that the transport was lost.
    pub fn connection_lost(&mut self) {
        self.logs.connection_lost();
        self.shells.connection_lost();
    }
}
