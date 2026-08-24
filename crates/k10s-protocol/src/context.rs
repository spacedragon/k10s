//! Context-switch and advisory-permission payloads for the k10s control
//! protocol.

use serde::{Deserialize, Serialize};

/// Request kind: switch the backend's current Kubernetes context.
pub const REQUEST_CONTEXT_SWITCH: &str = "context.switch";
/// Request kind: project advisory RBAC capabilities of one context.
pub const REQUEST_CONTEXT_PERMISSIONS: &str = "context.permissions";

/// Payload describing a context-switch request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextSwitchRequest {
    /// Destination context name.
    pub to: String,
}

/// Response payload for a committed context switch.
///
/// The switch is prepare-then-commit: a response only ever reports a
/// destination whose read path validated successfully.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextSwitchResponse {
    /// Context that is current after the commit.
    pub current: String,
    /// Context that lost the current marker, when one existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

/// One requested advisory permission check.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionProbe {
    /// Kubernetes verb to review, such as `list` or `delete`.
    pub verb: String,
    /// Resource plural to review, such as `pods`.
    pub resource: String,
    /// Namespace restriction, when reviewed within one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Payload describing an advisory permission projection request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPermissionsRequest {
    /// Context whose authorization is projected.
    pub context: String,
    /// Verb/resource/namespace checks to review.
    pub probes: Vec<PermissionProbe>,
}

/// What authorization reported for one probe.
///
/// The values are advisory metadata only: they hint at what later operations
/// are expected to be allowed and are never enforced client-side. Unknown is
/// distinct from denied — it means the review itself could not be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionOutcome {
    /// The review reported the action as allowed.
    Allowed,
    /// The review reported the action as denied.
    Denied,
    /// The review could not answer (rejected, unreachable, or errored).
    Unknown,
}

/// One answered advisory permission check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionCheck {
    /// Kubernetes verb that was reviewed.
    pub verb: String,
    /// Resource plural that was reviewed.
    pub resource: String,
    /// Namespace restriction, when reviewed within one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// What authorization reported.
    pub outcome: PermissionOutcome,
}

/// Response payload for an advisory permission projection.
///
/// Carries no raw Kubernetes review text: only normalized outcomes cross the
/// wire, with unknown states kept distinct from denied ones.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPermissionsResponse {
    /// Context the checks were reviewed against.
    pub context: String,
    /// One answered check per distinct probe, in request order.
    pub checks: Vec<PermissionCheck>,
}
