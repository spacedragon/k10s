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

/// Safe context metadata exposed to the UI.
///
/// Never exposes credentials or raw kubeconfig.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    /// Context name.
    pub name: String,
    /// Cluster name.
    pub cluster: String,
    /// Default namespace, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Whether this is the current context.
    pub is_current: bool,
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
    /// Safe context metadata, added in protocol v1.1.
    #[serde(default)]
    pub contexts: Vec<Context>,
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
            contexts: vec![
                Context {
                    name: "dev-local".into(),
                    cluster: "dev-cluster".into(),
                    namespace: Some("default".into()),
                    is_current: true,
                },
                Context {
                    name: "prod-readonly".into(),
                    cluster: "prod-cluster".into(),
                    namespace: Some("default".into()),
                    is_current: false,
                },
            ],
        }
    }
}
