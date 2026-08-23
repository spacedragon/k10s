//! Connected tool workflows rendered inside detail views: the guarded YAML
//! editor, the connected log viewer, and the exec terminal.

pub mod logs;
pub mod shell;
pub mod yaml;

pub use logs::{LogsPhase, LogsTool, MAX_LINE_CHARS, TRUNCATION_MARKER};
pub use shell::{ShellAction, ShellPhase, ShellTool};
pub use yaml::{DiffKind, DiffLine, YamlAction, YamlEditor, YamlEditors, YamlPhase};
