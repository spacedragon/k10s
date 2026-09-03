//! Non-authoritative view state for the singleton Port Forwards window.

use super::SortSpec;

/// Presentation preferences for the global port-forward session feed.
///
/// Session snapshots stay in the client state and are deliberately absent
/// here so reconnect/list reconstruction remains authoritative.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PortForwardWindowState {
    pub sort: Option<SortSpec>,
    pub focused_session: Option<String>,
}
