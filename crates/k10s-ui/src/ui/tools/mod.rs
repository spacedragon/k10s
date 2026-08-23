//! Connected tool workflows rendered inside detail views: the guarded YAML
//! editor lives here; log and shell tools follow in a later task.

pub mod yaml;

pub use yaml::{DiffKind, DiffLine, YamlAction, YamlEditor, YamlEditors, YamlPhase};
