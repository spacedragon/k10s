//! Bounded, loopback-only port-forward lifecycle contracts.
//!
//! Port forwarding is a server-owned session feature: the UI asks the
//! embedded server to forward either one declared TCP Service port or one
//! declared TCP Pod container port. Every payload in this module is normalized
//! and safe; Kubernetes client types and socket handles never appear here.

use serde::{Deserialize, Serialize};

use crate::error::{ErrorScope, Retryability};
use crate::resource::ResourceIdentity;

/// Capability advertised by servers that accept Service port-forward requests.
///
/// The desktop embedded server enables it; standalone and web deployments
/// never advertise it. Capability absence is not the security boundary: the
/// server rejects every port-forward request when the feature is disabled.
pub const CAPABILITY_SERVICE_PORT_FORWARD: &str = "service.portForward";
/// Capability advertised by servers that accept Pod port-forward requests.
pub const CAPABILITY_POD_PORT_FORWARD: &str = "pod.portForward";

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

/// Exact Kubernetes source targeted by one port-forward session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PortForwardTarget {
    /// Resolve a declared Service port to one ready backing Pod.
    Service {
        /// Exact core/v1 Service identity including its immutable UID.
        identity: ResourceIdentity,
        /// Which declared Service port to forward.
        port: PortForwardPortSelector,
    },
    /// Forward one declared port on an exact Pod container.
    Pod {
        /// Exact core/v1 Pod identity including its immutable UID.
        identity: ResourceIdentity,
        /// Name of the regular Pod container declaring the port.
        container_name: String,
        /// Declared numeric TCP container port.
        remote_port: u16,
    },
}

impl PortForwardTarget {
    /// Validate the target shape before it reaches the backend.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Service { identity, port } => {
                validate_identity(identity, IdentityKind::Service)?;
                if matches!(port, PortForwardPortSelector::Name { name } if name.is_empty()) {
                    return Err("a named port selector must not be empty");
                }
            }
            Self::Pod {
                identity,
                container_name,
                remote_port,
            } => {
                validate_identity(identity, IdentityKind::Pod)?;
                if container_name.is_empty() {
                    return Err("the Pod target must name a container");
                }
                if *remote_port == 0 {
                    return Err("the Pod target remote port must be greater than zero");
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum IdentityKind {
    Service,
    Pod,
}

fn validate_identity(
    identity: &ResourceIdentity,
    expected: IdentityKind,
) -> Result<(), &'static str> {
    let (kind, wrong_kind, missing_namespace, missing_uid) = match expected {
        IdentityKind::Service => (
            "Service",
            "port forwarding requires an exact core/v1 Service identity",
            "the Service identity must carry a namespace",
            "the Service identity must carry a UID",
        ),
        IdentityKind::Pod => (
            "Pod",
            "port forwarding requires an exact core/v1 Pod identity",
            "the Pod identity must carry a namespace",
            "the Pod identity must carry a UID",
        ),
    };
    let gvk = &identity.gvk;
    if !(gvk.group.is_empty() && gvk.version == "v1" && gvk.kind == kind) {
        return Err(wrong_kind);
    }
    if identity.namespace.as_deref().unwrap_or("").is_empty() {
        return Err(missing_namespace);
    }
    if identity.uid.is_empty() {
        return Err(missing_uid);
    }
    Ok(())
}

/// Request payload for `portForward.start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardStartRequest {
    target: PortForwardTarget,
    local_port: u16,
}

impl PortForwardStartRequest {
    /// Construct a Service request that retains the legacy Service wire shape.
    pub fn try_service(
        identity: ResourceIdentity,
        port: PortForwardPortSelector,
        local_port: u16,
    ) -> Result<Self, &'static str> {
        Self::try_target(PortForwardTarget::Service { identity, port }, local_port)
    }

    /// Construct and validate a request for either supported target kind.
    pub fn try_target(target: PortForwardTarget, local_port: u16) -> Result<Self, &'static str> {
        target.validate()?;
        Ok(Self { target, local_port })
    }

    /// Return the exact requested target.
    #[must_use]
    pub const fn target(&self) -> &PortForwardTarget {
        &self.target
    }

    /// Return the requested local loopback port; `0` lets the OS assign one.
    #[must_use]
    pub const fn local_port(&self) -> u16 {
        self.local_port
    }

    /// Consume the request into its target and requested local port.
    #[must_use]
    pub fn into_parts(self) -> (PortForwardTarget, u16) {
        (self.target, self.local_port)
    }

    /// Validate the request shape before it reaches the backend.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.target.validate()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyServiceStartRequestRef<'a> {
    service: &'a ResourceIdentity,
    port: &'a PortForwardPortSelector,
    local_port: u16,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetStartRequestRef<'a> {
    target: &'a PortForwardTarget,
    local_port: u16,
}

impl Serialize for PortForwardStartRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.target {
            PortForwardTarget::Service { identity, port } => LegacyServiceStartRequestRef {
                service: identity,
                port,
                local_port: self.local_port,
            }
            .serialize(serializer),
            PortForwardTarget::Pod { .. } => TargetStartRequestRef {
                target: &self.target,
                local_port: self.local_port,
            }
            .serialize(serializer),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StartRequestWire {
    LegacyService {
        service: ResourceIdentity,
        port: PortForwardPortSelector,
        #[serde(rename = "localPort")]
        local_port: u16,
    },
    Target {
        target: PortForwardTarget,
        #[serde(rename = "localPort")]
        local_port: u16,
    },
}

impl<'de> Deserialize<'de> for PortForwardStartRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match StartRequestWire::deserialize(deserializer)? {
            StartRequestWire::LegacyService {
                service,
                port,
                local_port,
            } => Self::try_service(service, port, local_port),
            StartRequestWire::Target { target, local_port } => Self::try_target(target, local_port),
        }
        .map_err(serde::de::Error::custom)
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

/// Identity of the resolved Pod of a session.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardPodTarget {
    /// Pod namespace, equal to the source Service or Pod namespace.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortForwardSession {
    /// Opaque session identifier scoped to this server instance.
    pub id: PortForwardSessionId,
    /// Exact Service or Pod source being forwarded.
    pub target: PortForwardTarget,
    /// Requested local port, retaining `0` when automatic assignment was used.
    pub requested_local_port: u16,
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeneralizedSessionWire {
    id: PortForwardSessionId,
    target: PortForwardTarget,
    requested_local_port: u16,
    pod: PortForwardPodTarget,
    pod_port: u16,
    local_addr: String,
    state: PortForwardSessionState,
    #[serde(default)]
    failure: Option<PortForwardFailure>,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyServiceSessionWire {
    id: PortForwardSessionId,
    service: ResourceIdentity,
    service_port: u16,
    pod: PortForwardPodTarget,
    pod_port: u16,
    local_addr: String,
    state: PortForwardSessionState,
    #[serde(default)]
    failure: Option<PortForwardFailure>,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SessionWire {
    Generalized(GeneralizedSessionWire),
    LegacyService(LegacyServiceSessionWire),
}

impl From<GeneralizedSessionWire> for PortForwardSession {
    fn from(wire: GeneralizedSessionWire) -> Self {
        Self {
            id: wire.id,
            target: wire.target,
            requested_local_port: wire.requested_local_port,
            pod: wire.pod,
            pod_port: wire.pod_port,
            local_addr: wire.local_addr,
            state: wire.state,
            failure: wire.failure,
            revision: wire.revision,
        }
    }
}

impl TryFrom<LegacyServiceSessionWire> for PortForwardSession {
    type Error = std::net::AddrParseError;

    fn try_from(wire: LegacyServiceSessionWire) -> Result<Self, Self::Error> {
        let requested_local_port = wire.local_addr.parse::<std::net::SocketAddr>()?.port();
        Ok(Self {
            id: wire.id,
            target: PortForwardTarget::Service {
                identity: wire.service,
                port: PortForwardPortSelector::Number {
                    number: wire.service_port,
                },
            },
            requested_local_port,
            pod: wire.pod,
            pod_port: wire.pod_port,
            local_addr: wire.local_addr,
            state: wire.state,
            failure: wire.failure,
            revision: wire.revision,
        })
    }
}

impl<'de> Deserialize<'de> for PortForwardSession {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match SessionWire::deserialize(deserializer)? {
            SessionWire::Generalized(wire) => Ok(wire.into()),
            SessionWire::LegacyService(wire) => {
                Self::try_from(wire).map_err(serde::de::Error::custom)
            }
        }
    }
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
