//! Bounded, loopback-only Service port-forward lifecycle contracts.
//!
//! Port forwarding is a server-owned session feature: the UI asks the
//! embedded server to forward one declared TCP Service port to exactly one
//! ready backing Pod. Every payload in this module is normalized and safe;
//! Kubernetes client types and socket handles never appear here.

use serde::{Deserialize, Serialize};

use crate::error::{ErrorScope, Retryability};
use crate::resource::ResourceIdentity;

/// Capability advertised by servers that accept port-forward requests.
///
/// The desktop embedded server enables it; standalone and web deployments
/// never advertise it. Capability absence is not the security boundary: the
/// server rejects every port-forward request when the feature is disabled.
pub const CAPABILITY_SERVICE_PORT_FORWARD: &str = "service.portForward";

/// Request kind starting one bounded port-forward session.
pub const REQUEST_PORT_FORWARD_START: &str = "portForward.start";
/// Request kind stopping one session by id; idempotent.
pub const REQUEST_PORT_FORWARD_STOP: &str = "portForward.stop";
/// Request kind listing every retained session of this server instance.
pub const REQUEST_PORT_FORWARD_LIST: &str = "portForward.list";

/// Envelope event kind carrying a [`PortForwardSessionEvent`].
pub const PORT_FORWARD_EVENT_SESSION: &str = "portForward.session";

/// Which declared Service port a start request forwards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PortForwardPortSelector {
    /// Select by declared Service port name.
    Name {
        /// Declared Service port name.
        name: String,
    },
    /// Select by Service port number.
    Number {
        /// Declared Service port number.
        number: u16,
    },
}

/// Request payload for `portForward.start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardStartRequest {
    /// Exact core/v1 Service identity including its immutable UID.
    pub service: ResourceIdentity,
    /// Which declared Service port to forward.
    pub port: PortForwardPortSelector,
    /// Requested local loopback port; `0` lets the OS assign one.
    pub local_port: u16,
}

impl PortForwardStartRequest {
    /// Validate the request shape before it reaches the backend.
    ///
    /// Only exact core/v1 Service identities with a namespace and non-empty
    /// UID are accepted; anything else is rejected without side effects.
    pub fn validate(&self) -> Result<(), &'static str> {
        let gvk = &self.service.gvk;
        if !(gvk.group.is_empty() && gvk.version == "v1" && gvk.kind == "Service") {
            return Err("port forwarding requires an exact core/v1 Service identity");
        }
        if self.service.namespace.as_deref().unwrap_or("").is_empty() {
            return Err("the Service identity must carry a namespace");
        }
        if self.service.uid.is_empty() {
            return Err("the Service identity must carry a UID");
        }
        if matches!(
            &self.port,
            PortForwardPortSelector::Name { name } if name.is_empty()
        ) {
            return Err("a named port selector must not be empty");
        }
        Ok(())
    }
}

/// Opaque identifier of one port-forward session.
///
/// Session IDs are random values scoped to the authenticated server
/// instance; empty strings are rejected on decode.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct PortForwardSessionId(String);

impl PortForwardSessionId {
    /// Construct an identifier, rejecting empty values.
    pub fn try_new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() {
            return Err("session id must not be empty".into());
        }
        Ok(Self(value))
    }

    /// Return the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PortForwardSessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PortForwardSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Lifecycle state of one port-forward session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PortForwardSessionState {
    /// The request was accepted; resolution or binding is in flight.
    Starting,
    /// The loopback listener is bound and accepting connections.
    Active,
    /// Stop was requested; the listener is draining.
    Stopping,
    /// The session ended cleanly or by explicit stop; terminal.
    Stopped,
    /// The pinned target became unusable; terminal until retried.
    Failed,
}

/// Safe failure categories used on terminal [`PortForwardSession`] snapshots
/// and typed errors. Message text is always sanitized; raw Kubernetes errors
/// never cross the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PortForwardFailureCategory {
    /// No ready endpoint exists for the requested Service port.
    UnavailableEndpoint,
    /// Kubernetes authorization denied a required call.
    Forbidden,
    /// The requested local port is already occupied.
    LocalPortInUse,
    /// The Service or Pod identity changed or disappeared.
    VanishedResource,
    /// The Service type or port cannot be forwarded.
    UnsupportedService,
    /// A context switch invalidated the request; safe to retry after it.
    ContextTransition,
    /// The control transport closed before completion.
    TransportClosed,
}

impl PortForwardFailureCategory {
    /// Map a category onto the protocol error code used when a request fails
    /// before any snapshot exists.
    #[must_use]
    pub const fn retryability(self) -> Retryability {
        match self {
            Self::UnavailableEndpoint => Retryability::UserAction,
            Self::Forbidden => Retryability::Never,
            Self::LocalPortInUse => Retryability::UserAction,
            Self::VanishedResource => Retryability::AfterRefresh,
            Self::UnsupportedService => Retryability::Never,
            Self::ContextTransition => Retryability::AfterRefresh,
            Self::TransportClosed => Retryability::AfterReconnect,
        }
    }

    /// Return the error scope carried alongside pre-session failures.
    #[must_use]
    pub const fn scope() -> ErrorScope {
        ErrorScope::Request
    }
}

/// Safe failure detail attached to a `Failed` session snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardFailure {
    /// Stable category driving UI affordances such as Retry.
    pub category: PortForwardFailureCategory,
    /// Short sanitized reason safe to display.
    pub message: String,
}

/// Identity of the selected backing Pod of a session.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardPodTarget {
    /// Pod namespace, equal to the Service namespace.
    pub namespace: String,
    /// Pod name.
    pub name: String,
    /// Immutable Pod UID verified at selection time.
    pub uid: String,
}

/// Complete snapshot of one port-forward session.
///
/// Snapshots are authoritative server state: clients render them verbatim
/// and never infer state from button clicks. The revision is monotonic per
/// manager so events may be coalesced or replayed safely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardSession {
    /// Opaque session identifier scoped to this server instance.
    pub id: PortForwardSessionId,
    /// Exact Service being forwarded.
    pub service: ResourceIdentity,
    /// Declared Service port number being forwarded.
    pub service_port: u16,
    /// Selected backing Pod.
    pub pod: PortForwardPodTarget,
    /// Resolved numeric target port on the Pod.
    pub pod_port: u16,
    /// Bound local address, always within `127.0.0.1`.
    pub local_addr: String,
    /// Current lifecycle state.
    pub state: PortForwardSessionState,
    /// Safe failure detail, present only while `Failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<PortForwardFailure>,
    /// Monotonic snapshot revision of this session.
    pub revision: u64,
}

/// Response payload for `portForward.start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardStartResponse {
    /// Snapshot of the accepted session.
    pub session: PortForwardSession,
}

/// Request payload for `portForward.stop`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardStopRequest {
    /// Identifier of the session to stop.
    pub session_id: PortForwardSessionId,
}

/// Response payload for `portForward.stop`.
///
/// Stop is idempotent: stopping an unknown or already-terminal session
/// succeeds with no snapshot instead of an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardStopResponse {
    /// Final snapshot of the stopped session, when still retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<PortForwardSession>,
}

/// Request payload for `portForward.list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardListRequest {}

/// Response payload for `portForward.list`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardListResponse {
    /// Manager-global revision covered by this reconstruction snapshot.
    /// Clients reject responses older than an already-applied event.
    #[serde(default)]
    pub revision: u64,
    /// Every retained session owned by this server instance.
    pub sessions: Vec<PortForwardSession>,
}

/// One complete session snapshot with its monotonic revision.
///
/// Emitted on the `portForward.sessions` subscription whenever a session
/// changes; receivers apply any event whose revision is newer than what they
/// have already applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardSessionEvent {
    /// Monotonic revision of this snapshot.
    pub revision: u64,
    /// The complete session snapshot as of `revision`.
    pub session: PortForwardSession,
}
