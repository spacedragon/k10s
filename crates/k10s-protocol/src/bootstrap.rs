//! Bootstrap payload types for the k10s control protocol.

use serde::{Deserialize, Serialize};

/// Negotiated protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolVersion {
    /// Major version number.
    pub major: u16,
    /// Minor version number.
    pub minor: u16,
}

/// Server identification info.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfo {
    /// Unique server instance identifier.
    pub instance_id: String,
    /// Server build version.
    pub version: String,
}

/// The bootstrap response payload returned in a `response` frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapResponse {
    /// The negotiated protocol version.
    pub protocol: ProtocolVersion,
    /// The negotiated capability set.
    pub capabilities: Vec<String>,
    /// Server identification, added in protocol v1.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerInfo>,
}

impl BootstrapResponse {
    /// Return a deterministic fixture value used by golden tests.
    #[must_use]
    pub fn fixture() -> Self {
        Self {
            protocol: ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec!["logs.tail".into(), "exec.attach".into()],
            server: Some(ServerInfo {
                instance_id: "instance-1".into(),
                version: "0.1.0".into(),
            }),
        }
    }
}
