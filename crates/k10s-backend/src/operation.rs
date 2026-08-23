//! Guarded YAML validation types and kernel mapping.
//!
//! The adapter parses and dry-runs manifests deterministically; this module
//! owns the backend-side data model, a minimal deterministic manifest
//! reader, and the kernel-facing result that maps onto the protocol
//! [`YamlOutcome`](k10s_protocol::YamlOutcome) payload.

use k10s_protocol::{BackendRevision, ResourceIdentity, YamlDiagnostic, YamlOutcome};

use crate::port::{Gvk, ResourceRecord};

/// Backend-owned validation ticket before protocol mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ticket {
    /// Opaque ticket identifier.
    pub id: String,
    /// Target the change applies to.
    pub target: crate::port::ResourceRef,
    /// Backend revision the validation is bound to.
    pub resource_revision: u64,
    /// Backend revision at which the ticket was issued; drives expiry.
    pub issued_revision: u64,
    /// Content hash of the validated buffer.
    pub buffer_hash: String,
    /// Whether applying restarts existing workload pods.
    pub disruptive: bool,
}

/// How dependents are handled when an object is deleted. Mirrors the
/// protocol propagation modes without leaking wire types across the port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Propagation {
    /// Dependents are garbage-collected after the owner disappears.
    Background,
    /// The owner disappears only after every dependent was removed.
    Foreground,
    /// Dependents are orphaned and left running.
    Orphan,
}

/// Backend-owned lifecycle state of one background operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationState {
    /// Waiting to run.
    Pending,
    /// Currently running.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Completed with an error.
    Failed,
    /// Cancelled before completion.
    Cancelled,
}

impl OperationState {
    /// Whether this state ends an operation's lifecycle.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Map to the protocol-facing status.
    #[must_use]
    pub fn wire(self) -> k10s_protocol::OperationStatus {
        match self {
            Self::Pending => k10s_protocol::OperationStatus::Pending,
            Self::Running => k10s_protocol::OperationStatus::Running,
            Self::Succeeded => k10s_protocol::OperationStatus::Succeeded,
            Self::Failed => k10s_protocol::OperationStatus::Failed,
            Self::Cancelled => k10s_protocol::OperationStatus::Cancelled,
        }
    }
}

/// One backend-observed operation state change, delivered to subscribers
/// and answerable through status queries until eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEvent {
    /// Operation identifier.
    pub id: String,
    /// Current lifecycle state.
    pub state: OperationState,
    /// Deterministic progress as `(completed, total)` when running.
    pub progress: Option<(u32, u32)>,
    /// Safe human-readable detail, set for terminal failures.
    pub detail: Option<String>,
}

/// The retained record of one operation behind status queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRecord {
    /// Operation identifier.
    pub id: String,
    /// Current lifecycle state.
    pub state: OperationState,
    /// Progress as `(completed, total)` when running.
    pub progress: Option<(u32, u32)>,
    /// Safe detail, set for terminal failures.
    pub detail: Option<String>,
}

/// Answer to a status query: records for every requested ID still known.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperationStatusData {
    /// Records for every requested ID the adapter still knows.
    pub operations: Vec<OperationRecord>,
}

/// Backend-owned validation outcome before protocol mapping.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeData {
    /// The manifest is applicable; a ticket was issued.
    Valid { ticket: Ticket },
    /// Schema or dry-run errors were found.
    Invalid { diagnostics: Vec<YamlDiagnostic> },
}

/// Backend-owned result of one validation query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YamlValidationData {
    /// Context the manifest targeted.
    pub context: String,
    /// Deterministic outcome.
    pub outcome: OutcomeData,
}

/// Kernel-mapped validation result carrying the exact wire payload.
#[derive(Debug, Clone)]
pub struct YamlValidateResult {
    payload: YamlOutcome,
}

impl YamlValidateResult {
    /// Map backend-owned validation data into the protocol-facing payload.
    #[must_use]
    pub fn new(data: YamlValidationData) -> Self {
        let payload = match data.outcome {
            OutcomeData::Valid { ticket } => YamlOutcome::Valid {
                ticket: k10s_protocol::ValidationTicket {
                    id: ticket.id,
                    target: map_identity(&ticket.target),
                    resource_revision: BackendRevision::new(ticket.resource_revision),
                    buffer_hash: ticket.buffer_hash,
                    disruptive: ticket.disruptive,
                },
            },
            OutcomeData::Invalid { diagnostics } => YamlOutcome::Invalid { diagnostics },
        };
        Self { payload }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> YamlOutcome {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("YamlOutcome must serialize")
    }
}

/// Map a backend resource reference into its protocol identity.
#[must_use]
fn map_identity(reference: &crate::port::ResourceRef) -> ResourceIdentity {
    ResourceIdentity {
        context: reference.context.clone(),
        gvk: crate::kernel::map_gvk(&reference.gvk),
        namespace: reference.namespace.clone(),
        name: reference.name.clone(),
        uid: reference.uid.clone(),
    }
}

// ---------------------------------------------------------------------------
// Minimal deterministic manifest model and parser
// ---------------------------------------------------------------------------

/// One parsed manifest field with its source line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Field {
    pub(crate) value: Option<String>,
    pub(crate) line: u32,
}

/// The subset of a Kubernetes manifest the deterministic fake understands.
///
/// Top-level scalar keys (`apiVersion`, `kind`) plus single nested mappings
/// (`metadata`, `spec`) are supported; anything else is reported as an
/// unsupported-syntax diagnostic so no input is silently ignored.
#[derive(Debug, Default)]
pub(crate) struct Manifest {
    pub(crate) api_version: Option<Field>,
    pub(crate) kind: Option<Field>,
    pub(crate) metadata_name: Option<Field>,
    pub(crate) metadata_namespace: Option<String>,
    unsupported: Vec<YamlDiagnostic>,
}

impl Manifest {
    /// All schema diagnostics in document order, including missing required
    /// fields.
    pub(crate) fn diagnostics(&self) -> Vec<YamlDiagnostic> {
        let mut diagnostics = self.unsupported.clone();
        let missing: [(&str, u32); 3] = [("apiVersion", 1), ("kind", 1), ("metadata.name", 2)];
        let present = [
            self.api_version.is_some(),
            self.kind.is_some(),
            self.metadata_name.is_some(),
        ];
        for ((field, line), exists) in missing.iter().zip(present.iter()) {
            if !exists {
                diagnostics.push(YamlDiagnostic {
                    line: *line,
                    message: format!("missing required field {field}"),
                });
            }
        }
        diagnostics.sort();
        diagnostics
    }

    /// Resolve the parsed fields onto a group/version/kind.
    ///
    /// Unknown apiVersion/kind pairs become schema diagnostics instead of a
    /// hard error so callers can batch every problem into one response.
    pub(crate) fn resolve_gvk(&self) -> Result<Gvk, Vec<YamlDiagnostic>> {
        let mut problems = self.diagnostics();
        if !problems.is_empty() {
            return Err(problems);
        }
        let Some(kind) = self.kind.as_ref().map(|field| field.value.clone()) else {
            return Err(problems);
        };
        let Some(api_version) = self.api_version.as_ref().map(|field| field.value.clone()) else {
            return Err(problems);
        };
        let (kind, api_version) = (kind.unwrap_or_default(), api_version.unwrap_or_default());
        let known = match api_version.as_str() {
            "v1" => matches!(
                kind.as_str(),
                "Pod"
                    | "Node"
                    | "Namespace"
                    | "ConfigMap"
                    | "Secret"
                    | "Service"
                    | "ServiceAccount"
                    | "PersistentVolumeClaim"
                    | "PersistentVolume"
            ),
            "apps/v1" => matches!(
                kind.as_str(),
                "Deployment" | "ReplicaSet" | "StatefulSet" | "DaemonSet"
            ),
            "batch/v1" => matches!(kind.as_str(), "Job" | "CronJob"),
            "monitoring.example.com/v1" => matches!(kind.as_str(), "Dashboard"),
            _ => false,
        };
        if !known {
            problems.push(YamlDiagnostic {
                line: self.kind.as_ref().map_or(1, |field| field.line),
                message: format!("unknown kind {kind} in {api_version}"),
            });
            return Err(problems);
        }
        let (group, version) = match api_version.split_once('/') {
            Some((group, version)) => (group.to_owned(), version.to_owned()),
            None => (String::new(), api_version.clone()),
        };
        Ok(Gvk::new(group, version, kind))
    }
}

/// Parse the deterministic manifest subset of `yaml`.
#[must_use]
pub(crate) fn parse_manifest(yaml: &str) -> Manifest {
    let mut manifest = Manifest::default();
    // Active top-level section: `Some("metadata")` while inside its block.
    let mut section: Option<String> = None;
    for (index, raw_line) in yaml.lines().enumerate() {
        let line_number = index as u32 + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indented = raw_line.starts_with(' ') || raw_line.starts_with('\t');
        let Some((key, value)) = split_key_value(trimmed) else {
            manifest.unsupported.push(YamlDiagnostic {
                line: line_number,
                message: format!("unsupported manifest syntax: {trimmed}"),
            });
            continue;
        };
        if !indented {
            section = None;
            match key {
                "apiVersion" => {
                    manifest.api_version = Some(Field {
                        value,
                        line: line_number,
                    })
                }
                "kind" => {
                    manifest.kind = Some(Field {
                        value,
                        line: line_number,
                    })
                }
                "metadata" | "spec" if value.is_none() => section = Some(key.to_owned()),
                _ => manifest.unsupported.push(YamlDiagnostic {
                    line: line_number,
                    message: format!("unsupported manifest key: {key}"),
                }),
            }
        } else if section.as_deref() == Some("metadata") {
            match key {
                "name" => {
                    manifest.metadata_name = Some(Field {
                        value,
                        line: line_number,
                    })
                }
                "namespace" => manifest.metadata_namespace = value,
                _ => {}
            }
        }
        // Spec content is accepted but not interpreted by the prototype.
    }
    manifest
}

/// Split `key: value`, returning `None` for non-mapping lines.
fn split_key_value(trimmed: &str) -> Option<(&str, Option<String>)> {
    let (key, rest) = trimmed.split_once(':')?;
    let key = key.trim();
    if key.is_empty() || key.contains(' ') || key.contains('\t') {
        return None;
    }
    let value = rest.trim();
    let value = if value.is_empty() {
        None
    } else {
        Some(value.trim_matches('"').trim_matches('\'').to_owned())
    };
    Some((key, value))
}

/// Whether applying a change to this kind restarts existing pods.
#[must_use]
pub(crate) fn is_disruptive_kind(gvk: &Gvk) -> bool {
    matches!(
        (gvk.group.as_str(), gvk.kind.as_str()),
        ("apps", "Deployment") | ("apps", "StatefulSet") | ("apps", "DaemonSet")
    )
}

/// Build the authoritative read-only manifest text shown by detail views.
#[must_use]
pub(crate) fn manifest_for(record: &ResourceRecord) -> String {
    let reference = &record.reference;
    let api_version = if reference.gvk.group.is_empty() {
        reference.gvk.version.clone()
    } else {
        format!("{}/{}", reference.gvk.group, reference.gvk.version)
    };
    let mut manifest = format!(
        "apiVersion: {api_version}\nkind: {}\nmetadata:\n  name: {}\n",
        reference.gvk.kind, reference.name
    );
    if let Some(namespace) = &reference.namespace {
        manifest.push_str(&format!("  namespace: {namespace}\n"));
    }
    manifest
}

/// Kernel-mapped operation status result carrying the exact wire payload.
#[derive(Debug, Clone)]
pub struct OperationStatusResult {
    payload: k10s_protocol::OperationStatusResponse,
}

impl OperationStatusResult {
    /// Map backend-owned status data into the protocol-facing payload.
    /// IDs the adapter no longer knows are simply absent so clients derive
    /// an `Unknown` state for them.
    #[must_use]
    pub fn new(data: OperationStatusData) -> Self {
        let payload = k10s_protocol::OperationStatusResponse {
            operations: data
                .operations
                .into_iter()
                .map(|record| k10s_protocol::OperationSnapshotEntry {
                    operation_id: k10s_protocol::OperationId::new(record.id),
                    status: record.state.wire(),
                    progress: record.progress.map(|(completed, total)| {
                        k10s_protocol::OperationProgress { completed, total }
                    }),
                })
                .collect(),
        };
        Self { payload }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::OperationStatusResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("OperationStatusResponse must serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_the_supported_subset_and_reports_the_rest() {
        let manifest = parse_manifest(
            "apiVersion: apps/v1\n# comment\nkind: Deployment\nmetadata:\n  name: web\n  namespace: default\nspec:\n  replicas: 3\n",
        );
        assert_eq!(
            manifest.api_version.as_ref().unwrap().value.as_deref(),
            Some("apps/v1")
        );
        assert_eq!(
            manifest.kind.as_ref().unwrap().value.as_deref(),
            Some("Deployment")
        );
        assert_eq!(
            manifest.metadata_name.as_ref().unwrap().value.as_deref(),
            Some("web")
        );
        assert_eq!(manifest.metadata_namespace.as_deref(), Some("default"));
        assert!(manifest.diagnostics().is_empty());
        assert!(manifest.resolve_gvk().is_ok());

        let broken = parse_manifest("- just a list\ngood: no\n");
        assert_eq!(broken.diagnostics().len(), 5);
    }
}
