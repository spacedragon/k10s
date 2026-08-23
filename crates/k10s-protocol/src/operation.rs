//! Guarded YAML validation and apply payloads for the k10s control protocol.
//!
//! Validation and ticket issuance are queries; applying a validated ticket
//! is a separate command that returns an [`OperationId`]. A ticket binds one
//! exact buffer (by content hash) to one target identity at one backend
//! revision, so the server can reject every stale or tampered apply without
//! trusting any client-side claim.

use serde::{Deserialize, Serialize};

use crate::ids::OperationId;
use crate::resource::{BackendRevision, GroupVersionKind, ResourceIdentity};

/// Request kind carrying a [`YamlValidateRequest`] payload.
pub const REQUEST_YAML_VALIDATE: &str = "yaml.validate";
/// Request kind carrying a [`YamlApplyRequest`] payload.
pub const REQUEST_YAML_APPLY: &str = "yaml.apply";
/// Request kind carrying a [`ScaleRequest`] payload.
pub const REQUEST_WORKLOAD_SCALE: &str = "workload.scale";
/// Request kind carrying a [`DeleteRequest`] payload.
pub const REQUEST_WORKLOAD_DELETE: &str = "workload.delete";
/// Request kind carrying an [`OperationStatusRequest`] payload.
pub const REQUEST_OPERATION_STATUS: &str = "operation.status";

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

// ---------------------------------------------------------------------------
// Workload mutations and operation status queries
// ---------------------------------------------------------------------------

/// Command payload scaling one exact workload object.
///
/// The target identity includes the immutable UID so a stale client can
/// never scale an object that was deleted and recreated under the same
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScaleRequest {
    /// Context the workload lives in.
    pub context: String,
    /// Type of the workload.
    pub gvk: GroupVersionKind,
    /// Namespace, absent for cluster-scoped workloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Object name.
    pub name: String,
    /// Immutable server-assigned identifier of the exact target.
    pub uid: String,
    /// Desired replica count.
    pub replicas: u32,
}

/// How dependents are handled when an object is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeletePropagation {
    /// Dependents are garbage-collected after the owner disappears.
    Background,
    /// The owner disappears only after every dependent was removed.
    Foreground,
    /// Dependents are orphaned and left running.
    Orphan,
}

/// Command payload deleting one exact object with an explicit propagation
/// mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRequest {
    /// Exact identity of the object to delete, including its UID.
    pub identity: ResourceIdentity,
    /// How dependents are handled.
    pub propagation: DeletePropagation,
}

/// Deterministic progress of a running background operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationProgress {
    /// Completed steps.
    pub completed: u32,
    /// Total steps.
    pub total: u32,
}

/// Query payload asking the backend for the current state of specific
/// operations by ID. Used after reconnects to refresh every nonterminal
/// operation before any retry is allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationStatusRequest {
    /// Operation IDs to look up.
    pub operation_ids: Vec<OperationId>,
}

/// One answered operation in an [`OperationStatusResponse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationSnapshotEntry {
    /// The looked-up operation.
    pub operation_id: OperationId,
    /// Current backend-observed status.
    pub status: crate::envelope::OperationStatus,
    /// Progress data, when still meaningful.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<OperationProgress>,
}

/// Response payload answering [`OperationStatusRequest`]. IDs that the
/// backend does not know (expired or evicted) are simply absent; clients
/// derive an `Unknown` state for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationStatusResponse {
    /// Entries for every requested ID the backend still knows.
    pub operations: Vec<OperationSnapshotEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::OperationStatus;

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

    #[test]
    fn mutation_requests_round_trip_with_their_exact_scope_identity() {
        let scale = ScaleRequest {
            context: "dev".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
            replicas: 3,
        };
        let value = serde_json::to_value(&scale).unwrap();
        assert_eq!(value["uid"], "uid-web", "the UID stays on the wire");
        assert_eq!(value["replicas"], 3);
        assert_eq!(
            serde_json::from_value::<ScaleRequest>(value).unwrap(),
            scale
        );

        for (mode, wire) in [
            (DeletePropagation::Background, "background"),
            (DeletePropagation::Foreground, "foreground"),
            (DeletePropagation::Orphan, "orphan"),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), serde_json::json!(wire));
            assert_eq!(
                serde_json::from_value::<DeletePropagation>(serde_json::json!(wire)).unwrap(),
                mode
            );
        }

        let delete = DeleteRequest {
            identity: ResourceIdentity {
                context: "dev".into(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                namespace: Some("default".into()),
                name: "p".into(),
                uid: "uid-p".into(),
            },
            propagation: DeletePropagation::Foreground,
        };
        let value = serde_json::to_value(&delete).unwrap();
        assert_eq!(value["propagation"], "foreground");
        assert_eq!(value["identity"]["uid"], "uid-p");
    }

    #[test]
    fn operation_status_payloads_round_trip() {
        let request = OperationStatusRequest {
            operation_ids: vec![OperationId::new("op-1"), OperationId::new("op-2")],
        };
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["operationIds"], serde_json::json!(["op-1", "op-2"]));

        let response = OperationStatusResponse {
            operations: vec![OperationSnapshotEntry {
                operation_id: OperationId::new("op-1"),
                status: OperationStatus::Running,
                progress: Some(OperationProgress {
                    completed: 1,
                    total: 3,
                }),
            }],
        };
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["operations"][0]["status"], "running");
        assert_eq!(value["operations"][0]["progress"]["completed"], 1);
        assert_eq!(
            serde_json::from_value::<OperationStatusResponse>(value).unwrap(),
            response
        );
    }
}
