//! Guarded YAML validation and apply payloads for the k10s control protocol.
//!
//! Validation and ticket issuance are queries; applying a validated ticket
//! is a separate command that returns an [`OperationId`]. A ticket binds one
//! exact buffer (by content hash) to one target identity at one backend
//! revision, so the server can reject every stale or tampered apply without
//! trusting any client-side claim.

use serde::{Deserialize, Serialize};

use crate::ids::OperationId;
use crate::resource::{BackendRevision, ResourceIdentity};

/// Request kind carrying a [`YamlValidateRequest`] payload.
pub const REQUEST_YAML_VALIDATE: &str = "yaml.validate";
/// Request kind carrying a [`YamlApplyRequest`] payload.
pub const REQUEST_YAML_APPLY: &str = "yaml.apply";

/// Deterministic content hash of one YAML edit buffer.
///
/// The buffer identity is an authorization boundary between validation and
/// apply, so it uses a collision-resistant digest (SHA-256) with the
/// algorithm tagged in the encoding. Both the client and the backend
/// compute this over the exact bytes, so a ticket can only ever apply to
/// the buffer that was validated.
#[must_use]
pub fn buffer_hash(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(input.as_bytes());
    let mut encoded = String::with_capacity(8 + 2 * digest.len());
    encoded.push_str("sha-256:");
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

/// Request payload validating a YAML manifest without submitting it.
///
/// The backend parses and dry-runs the manifest deterministically and, when
/// it is applicable, issues a single-use [`ValidationTicket`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YamlValidateRequest {
    /// Context the manifest targets.
    pub context: String,
    /// The exact YAML text to validate.
    pub yaml: String,
}

/// One deterministic schema or dry-run diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YamlDiagnostic {
    /// One-based line number the diagnostic refers to.
    pub line: u32,
    /// Safe human-readable explanation.
    pub message: String,
}

/// A backend-issued, single-use proof that one buffer was validated.
///
/// Tickets bind the exact buffer hash to the target identity and the backend
/// revision observed at validation time. Any later mutation of the target —
/// or of the buffer — invalidates the ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationTicket {
    /// Opaque ticket identifier.
    pub id: String,
    /// Identity of the object this change applies to.
    pub target: ResourceIdentity,
    /// Backend revision the validation is bound to.
    pub resource_revision: BackendRevision,
    /// Content hash of the validated buffer.
    pub buffer_hash: String,
    /// Whether applying this change restarts existing workload pods.
    pub disruptive: bool,
}

/// Outcome of a guarded YAML validation.
// The valid variant legitimately carries the full ticket while the others
// stay small; boxing would only add indirection on the hot path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "camelCase")]
pub enum YamlOutcome {
    /// The manifest is applicable; a single-use ticket was issued.
    Valid {
        /// The issued ticket.
        ticket: ValidationTicket,
    },
    /// Schema or dry-run errors were found.
    Invalid {
        /// Deterministic diagnostics in document order.
        diagnostics: Vec<YamlDiagnostic>,
    },
    /// The target moved on since validation; the user's buffer is kept but
    /// must be revalidated.
    Conflict {
        /// Safe human-readable conflict reason.
        message: String,
    },
}

/// Command payload applying a previously validated buffer.
///
/// Every field is re-checked by the backend before the apply runs; the
/// client never fabricates authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YamlApplyRequest {
    /// Context the ticket was issued for.
    pub context: String,
    /// The single-use ticket ID.
    pub ticket_id: String,
    /// Identity the ticket binds to.
    pub target: ResourceIdentity,
    /// Content hash the ticket binds to.
    pub buffer_hash: String,
    /// The exact validated YAML text.
    pub yaml: String,
}

/// Response payload for an accepted apply command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationAccepted {
    /// Identifier of the background operation performing the apply.
    pub operation_id: OperationId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_hash_is_deterministic_and_content_sensitive() {
        let first = buffer_hash("kind: Deployment\n");
        let second = buffer_hash("kind: Deployment\n");
        assert_eq!(first, second);
        assert_ne!(first, buffer_hash("kind: Deployment"));
        assert!(first.starts_with("sha-256:"));
    }

    #[test]
    fn outcomes_round_trip_through_json() {
        let outcome = YamlOutcome::Valid {
            ticket: ValidationTicket {
                id: "ticket-1".into(),
                target: ResourceIdentity {
                    context: "dev".into(),
                    gvk: crate::GroupVersionKind::core("v1", "Pod"),
                    namespace: Some("default".into()),
                    name: "p".into(),
                    uid: "uid-p".into(),
                },
                resource_revision: BackendRevision::new(7),
                buffer_hash: buffer_hash("yaml"),
                disruptive: true,
            },
        };
        let value = serde_json::to_value(&outcome).unwrap();
        assert_eq!(value["outcome"], serde_json::json!("valid"));
        assert_eq!(
            serde_json::from_value::<YamlOutcome>(value).unwrap(),
            outcome
        );
    }
}
