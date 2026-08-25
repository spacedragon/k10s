//! Authoritative validation against the selected Kubernetes API server.

use k10s_protocol::YamlDiagnostic;
use kube::ResourceExt;
use kube::api::{Patch, PatchParams};

use crate::operation::{OutcomeData, Ticket, YamlValidationData, is_disruptive_kind};
use crate::port::{BackendError, QueryResult, ResourceRef};

use super::KubeAdapter;
use super::watch::dynamic_api;

impl KubeAdapter {
    pub(super) async fn validate_apply(
        &self,
        context: String,
        yaml: String,
    ) -> Result<QueryResult, BackendError> {
        if !self.knows_context(&context) {
            return Err(BackendError::NotFound);
        }
        let parsed = match crate::validation::yaml::parse(&yaml) {
            Ok(parsed) => parsed,
            Err(diagnostics) => return Ok(invalid(context, diagnostics)),
        };
        let catalog = self.catalog_for(&context).await?;
        let Some(descriptor) = catalog.types.iter().find(|entry| entry.gvk == parsed.gvk) else {
            return Ok(invalid(
                context,
                vec![diagnostic(
                    "schema is unavailable for this apiVersion and kind",
                )],
            ));
        };
        if descriptor.namespaced != parsed.namespace.is_some() {
            return Ok(invalid(
                context,
                vec![diagnostic(if descriptor.namespaced {
                    "metadata.namespace is required for this resource"
                } else {
                    "metadata.namespace is not allowed for this cluster-scoped resource"
                })],
            ));
        }

        let client = self.cluster_client(&context).await?;
        let api = dynamic_api(
            client,
            parsed.gvk.clone(),
            descriptor.plural.clone(),
            descriptor.namespaced,
            parsed.namespace.clone(),
        );
        let current = match api.get(&parsed.name).await {
            Ok(object) => object,
            Err(kube::Error::Api(status)) if status.code == 404 => {
                return Err(BackendError::NotFound);
            }
            Err(kube::Error::Api(status)) if status.code == 403 => {
                return Err(BackendError::Forbidden);
            }
            Err(error) => {
                if let Some(unavailable) = super::auth::context_unavailable(&error) {
                    return Err(unavailable);
                }
                return Err(BackendError::Internal(
                    "kubernetes api unreachable during YAML validation".into(),
                ));
            }
        };
        if current.uid().as_deref() != Some(parsed.uid.as_str()) {
            return Err(BackendError::Conflict(
                "the object UID no longer matches the edited document".into(),
            ));
        }
        if current.resource_version().as_deref() != Some(parsed.resource_version.as_str()) {
            return Err(BackendError::Conflict(
                "the object resourceVersion no longer matches the edited document".into(),
            ));
        }

        // Kubernetes defaults field validation to Warn, which can silently
        // drop an unknown field while still accepting the dry-run. Tickets
        // are authoritative, so fail closed on every unknown/duplicate field.
        let params = PatchParams::apply("k10s").dry_run().validation_strict();
        if let Err(error) = api
            .patch(&parsed.name, &params, &Patch::Apply(&parsed.object))
            .await
        {
            if let Some(unavailable) = super::auth::context_unavailable(&error) {
                return Err(unavailable);
            }
            return match error {
                kube::Error::Api(status) if status.code == 403 => Err(BackendError::Forbidden),
                kube::Error::Api(status) if status.code == 404 => Err(BackendError::NotFound),
                kube::Error::Api(status) if status.code == 409 => Err(BackendError::Conflict(
                    "the object changed while the YAML was being validated".into(),
                )),
                kube::Error::Api(_) => Ok(invalid(
                    context,
                    vec![diagnostic(
                        "the api server rejected the server-side dry-run",
                    )],
                )),
                _ => Err(BackendError::Internal(
                    "kubernetes api unreachable during YAML dry-run".into(),
                )),
            };
        }

        let revision = self.watches.next_revision();
        let ticket = Ticket {
            id: String::new(),
            target: ResourceRef {
                context: context.clone(),
                gvk: parsed.gvk.clone(),
                namespace: parsed.namespace,
                name: parsed.name,
                uid: parsed.uid,
            },
            resource_revision: revision,
            opaque_resource_version: Some(parsed.resource_version),
            issued_revision: revision,
            buffer_hash: k10s_protocol::buffer_hash(&yaml),
            disruptive: is_disruptive_kind(&parsed.gvk),
        };
        let ticket = self
            .validation_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .issue(ticket);
        Ok(QueryResult::YamlValidation(YamlValidationData {
            context,
            outcome: OutcomeData::Valid { ticket },
        }))
    }
}

fn invalid(context: String, diagnostics: Vec<YamlDiagnostic>) -> QueryResult {
    QueryResult::YamlValidation(YamlValidationData {
        context,
        outcome: OutcomeData::Invalid { diagnostics },
    })
}

fn diagnostic(message: &str) -> YamlDiagnostic {
    YamlDiagnostic {
        line: 1,
        message: message.into(),
    }
}
