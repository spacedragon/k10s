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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContextAvailability {
    /// The credential path has not been exercised yet.
    Unknown,
    /// The context has a usable client or needs no exec credential helper.
    #[default]
    Available,
    /// The context's exec credential helper failed and must be retried explicitly.
    Unavailable,
}

/// Safe context metadata exposed to the UI.
///
/// The custom serde implementation keeps availability and its optional reason
/// consistent in both wire directions while accepting legacy four-field peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    /// Context name.
    pub name: String,
    /// Cluster name.
    pub cluster: String,
    /// Default namespace, if set.
    pub namespace: Option<String>,
    /// Whether this is the current context.
    pub is_current: bool,
    /// Current credential availability.
    pub availability: ContextAvailability,
    /// Safe, bounded operator-facing reason when unavailable.
    pub unavailable_reason: Option<String>,
}

impl Serialize for Context {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct WireContext<'a> {
            name: &'a str,
            cluster: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            namespace: &'a Option<String>,
            is_current: bool,
            availability: ContextAvailability,
            #[serde(skip_serializing_if = "Option::is_none", rename = "unavailableReason")]
            unavailable_reason: Option<&'a str>,
        }

        WireContext {
            name: &self.name,
            cluster: &self.cluster,
            namespace: &self.namespace,
            is_current: self.is_current,
            availability: self.availability,
            unavailable_reason: matches!(self.availability, ContextAvailability::Unavailable)
                .then(|| self.unavailable_reason.as_deref())
                .flatten(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireContext {
            name: String,
            cluster: String,
            #[serde(default)]
            namespace: Option<String>,
            is_current: bool,
            #[serde(default)]
            availability: ContextAvailability,
            #[serde(default, rename = "unavailableReason")]
            unavailable_reason: Option<String>,
        }

        let wire = WireContext::deserialize(deserializer)?;
        let unavailable_reason = match wire.availability {
            ContextAvailability::Unavailable => Some(
                wire.unavailable_reason
                    .unwrap_or_else(|| "credential plugin is unavailable".into()),
            ),
            ContextAvailability::Unknown | ContextAvailability::Available => None,
        };
        Ok(Self {
            name: wire.name,
            cluster: wire.cluster,
            namespace: wire.namespace,
            is_current: wire.is_current,
            availability: wire.availability,
            unavailable_reason,
        })
    }
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
            protocol: ProtocolVersion { major: 1, minor: 2 },
            capabilities: vec!["logs.tail".into()],
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
                    availability: ContextAvailability::Available,
                    unavailable_reason: None,
                },
                Context {
                    name: "prod-readonly".into(),
                    cluster: "prod-cluster".into(),
                    namespace: Some("default".into()),
                    is_current: false,
                    availability: ContextAvailability::Available,
                    unavailable_reason: None,
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};

    use super::{Context, ContextAvailability};

    fn context(availability: ContextAvailability, reason: Option<&str>) -> Context {
        Context {
            name: "context-a".into(),
            cluster: "cluster-a".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability,
            unavailable_reason: reason.map(str::to_owned),
        }
    }

    #[test]
    fn context_availability_round_trips() {
        for (availability, reason) in [
            (ContextAvailability::Unknown, None),
            (ContextAvailability::Available, None),
            (ContextAvailability::Unavailable, Some("plugin failed")),
        ] {
            let original = context(availability, reason);
            let encoded = serde_json::to_string(&original).expect("context serializes");
            let decoded: Context = serde_json::from_str(&encoded).expect("context deserializes");
            assert_eq!(decoded.availability, availability);
            assert_eq!(decoded.unavailable_reason.as_deref(), reason);
        }
    }

    #[test]
    fn legacy_context_defaults_to_available() {
        let decoded: Context = serde_json::from_value(json!({
            "name": "legacy",
            "cluster": "legacy-cluster",
            "namespace": "default",
            "isCurrent": true
        }))
        .expect("legacy context deserializes");

        assert_eq!(decoded.availability, ContextAvailability::Available);
        assert_eq!(decoded.unavailable_reason, None);
    }

    #[test]
    fn old_peer_ignores_context_availability() {
        #[derive(Debug, Deserialize, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct LegacyContext {
            name: String,
            cluster: String,
            namespace: Option<String>,
            is_current: bool,
        }

        let encoded = serde_json::to_value(context(
            ContextAvailability::Unavailable,
            Some("plugin failed"),
        ))
        .expect("new context serializes");
        let legacy: LegacyContext =
            serde_json::from_value(encoded).expect("old peer ignores new fields");
        assert_eq!(
            legacy,
            LegacyContext {
                name: "context-a".into(),
                cluster: "cluster-a".into(),
                namespace: Some("default".into()),
                is_current: true,
            }
        );
    }

    #[test]
    fn context_reason_is_normalized() {
        for availability in [ContextAvailability::Unknown, ContextAvailability::Available] {
            let encoded = serde_json::to_value(context(availability, Some("must disappear")))
                .expect("context serializes");
            assert_eq!(encoded.get("unavailableReason"), None);

            let mut malformed = encoded;
            malformed["unavailableReason"] = Value::String("must disappear".into());
            let decoded: Context = serde_json::from_value(malformed).expect("context deserializes");
            assert_eq!(decoded.unavailable_reason, None);
        }

        let decoded: Context = serde_json::from_value(json!({
            "name": "broken",
            "cluster": "cluster-a",
            "namespace": null,
            "isCurrent": false,
            "availability": "unavailable"
        }))
        .expect("unavailable context deserializes");
        assert_eq!(
            decoded.unavailable_reason.as_deref(),
            Some("credential plugin is unavailable")
        );
    }
}
