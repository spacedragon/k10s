//! Exact-identity Kubernetes mutations driven by the shared operation engine.

use kube::ResourceExt;
use kube::api::{DeleteParams, Patch, PatchParams, Preconditions, PropagationPolicy};
use serde_json::json;

use crate::operation::{AcceptOutcome, Propagation, Ticket};
use crate::port::{BackendError, Command, OperationId, ResourceRef};

use super::KubeAdapter;
use super::watch::dynamic_api;

impl KubeAdapter {
    pub(super) async fn execute_mutation(
        &self,
        command: Command,
    ) -> Result<OperationId, BackendError> {
        match command {
            Command::Scale {
                context,
                gvk,
                namespace,
                name,
                uid,
                replicas,
                idempotency_key,
            } => {
                if replicas > i32::MAX as u32 {
                    return Err(BackendError::Conflict(
                        "replicas exceed the Kubernetes scale range".into(),
                    ));
                }
                let target = ResourceRef {
                    context,
                    gvk,
                    namespace,
                    name,
                    uid,
                };
                let (api, current, descriptor) = self.mutation_target(&target).await?;
                if !descriptor.supports_scale || !descriptor.supports_patch {
                    return Err(BackendError::unsupported("workload.scale"));
                }
                let resource_version = current.resource_version().ok_or_else(|| {
                    BackendError::Conflict("the target has no resourceVersion".into())
                })?;
                let patch = json!({"metadata":{"resourceVersion":resource_version},"spec":{"replicas":replicas}});
                let fingerprint = format!("scale/{}/{replicas}", target.coalescing_key());
                let scope = target.coalescing_key();
                self.spawn_mutation(idempotency_key, fingerprint, scope, async move {
                    api.patch_subresource(
                        "scale",
                        &target.name,
                        &PatchParams::default(),
                        &Patch::Merge(&patch),
                    )
                    .await
                    .map(|_| ())
                })
            }
            Command::Restart {
                target,
                idempotency_key,
            } => {
                if !matches!(
                    (target.gvk.group.as_str(), target.gvk.kind.as_str()),
                    ("apps", "Deployment" | "StatefulSet" | "DaemonSet")
                ) {
                    return Err(BackendError::unsupported("workload.restart"));
                }
                let (api, current, descriptor) = self.mutation_target(&target).await?;
                if !descriptor.supports_patch {
                    return Err(BackendError::unsupported("workload.restart"));
                }
                let resource_version = current.resource_version().ok_or_else(|| {
                    BackendError::Conflict("the target has no resourceVersion".into())
                })?;
                let patch = json!({"metadata":{"resourceVersion":resource_version},"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":crate::runtime::now_rfc3339()}}}}});
                let fingerprint = format!("restart/{}", target.coalescing_key());
                let scope = target.coalescing_key();
                self.spawn_mutation(idempotency_key, fingerprint, scope, async move {
                    api.patch(&target.name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                        .map(|_| ())
                })
            }
            Command::CreateJob {
                source,
                idempotency_key,
            } => self.create_job(source, idempotency_key).await,
            Command::SetCronJobSuspended {
                target,
                suspended,
                idempotency_key,
            } => {
                self.set_cronjob_suspended(target, suspended, idempotency_key)
                    .await
            }
            Command::Delete {
                target,
                propagation,
                idempotency_key,
            } => {
                let (api, current, descriptor) = self.mutation_target(&target).await?;
                if !descriptor.supports_delete {
                    return Err(BackendError::unsupported("workload.delete"));
                }
                let resource_version = current.resource_version().ok_or_else(|| {
                    BackendError::Conflict("the target has no resourceVersion".into())
                })?;
                let policy = match propagation {
                    Propagation::Background => PropagationPolicy::Background,
                    Propagation::Foreground => PropagationPolicy::Foreground,
                    Propagation::Orphan => PropagationPolicy::Orphan,
                };
                let params = DeleteParams {
                    propagation_policy: Some(policy),
                    preconditions: Some(Preconditions {
                        uid: Some(target.uid.clone()),
                        resource_version: Some(resource_version),
                    }),
                    ..DeleteParams::default()
                };
                let fingerprint = format!("delete/{}/{propagation:?}", target.coalescing_key());
                let scope = target.coalescing_key();
                self.spawn_mutation(idempotency_key, fingerprint, scope, async move {
                    api.delete(&target.name, &params).await.map(|_| ())
                })
            }
            Command::Apply {
                context,
                yaml,
                idempotency_key,
                ticket_id,
                buffer_hash,
                target,
            } => {
                let fingerprint = format!("apply/{}/{}", target.coalescing_key(), buffer_hash);
                if let Some(id) = self.operations.replay(&idempotency_key, &fingerprint)? {
                    return Ok(id);
                }
                let ticket = self.inspect_ticket(&ticket_id)?;
                validate_ticket(&ticket, &context, &yaml, &buffer_hash, &target)?;
                let parsed = crate::validation::yaml::parse(&yaml).map_err(|_| {
                    BackendError::Conflict("the validated YAML can no longer be parsed".into())
                })?;
                let descriptor = self.descriptor_for(&context, &target.gvk).await?;
                if !descriptor.supports_patch {
                    return Err(BackendError::unsupported("yaml.apply"));
                }
                let client = self.cluster_client(&context).await?;
                let api = dynamic_api(
                    client,
                    target.gvk.clone(),
                    descriptor.plural,
                    descriptor.namespaced,
                    target.namespace.clone(),
                );
                let outcome = self.operations.accept_scoped(
                    &idempotency_key,
                    &fingerprint,
                    &target.coalescing_key(),
                )?;
                if let AcceptOutcome::Replayed(id) = outcome {
                    return Ok(id);
                }
                let id = outcome.operation_id().clone();
                // Only a fresh accepted operation consumes the single-use ticket.
                self.validation_tickets
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take(&ticket_id)?;
                self.operations.running(id.as_str(), None)?;
                let operations = self.operations.clone();
                let task_id = id.clone();
                tokio::spawn(async move {
                    let params = PatchParams::apply("k10s").validation_strict();
                    finish(
                        &operations,
                        task_id.as_str(),
                        api.patch(&target.name, &params, &Patch::Apply(&parsed.object))
                            .await
                            .map(|_| ()),
                    );
                });
                Ok(id)
            }
        }
    }

    pub(super) async fn mutation_target(
        &self,
        target: &ResourceRef,
    ) -> Result<
        (
            kube::Api<kube::core::DynamicObject>,
            kube::core::DynamicObject,
            crate::port::ApiResourceDescriptor,
        ),
        BackendError,
    > {
        let descriptor = self.descriptor_for(&target.context, &target.gvk).await?;
        if descriptor.namespaced != target.namespace.is_some() {
            return Err(BackendError::NotFound);
        }
        let client = self.cluster_client(&target.context).await?;
        let api = dynamic_api(
            client,
            target.gvk.clone(),
            descriptor.plural.clone(),
            descriptor.namespaced,
            target.namespace.clone(),
        );
        let current = api.get(&target.name).await.map_err(pre_submit_error)?;
        if current.uid().as_deref() != Some(target.uid.as_str()) {
            return Err(BackendError::Conflict(
                "the target UID no longer matches the current object".into(),
            ));
        }
        Ok((api, current, descriptor))
    }

    fn inspect_ticket(&self, id: &str) -> Result<Ticket, BackendError> {
        self.validation_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .inspect(id)
    }

    pub(super) fn spawn_mutation<F>(
        &self,
        key: String,
        fingerprint: String,
        scope: String,
        future: F,
    ) -> Result<OperationId, BackendError>
    where
        F: std::future::Future<Output = Result<(), kube::Error>> + Send + 'static,
    {
        let outcome = self.operations.accept_scoped(&key, &fingerprint, &scope)?;
        if let AcceptOutcome::Replayed(id) = outcome {
            return Ok(id);
        }
        let id = outcome.operation_id().clone();
        self.operations.running(id.as_str(), None)?;
        let operations = self.operations.clone();
        let task_id = id.clone();
        tokio::spawn(async move {
            finish(&operations, task_id.as_str(), future.await);
        });
        Ok(id)
    }
}

fn validate_ticket(
    ticket: &Ticket,
    context: &str,
    yaml: &str,
    buffer_hash: &str,
    target: &ResourceRef,
) -> Result<(), BackendError> {
    if ticket.target != *target
        || ticket.target.context != context
        || ticket.buffer_hash != buffer_hash
        || k10s_protocol::buffer_hash(yaml) != buffer_hash
    {
        return Err(BackendError::Conflict(
            "the validation ticket does not match this exact target and buffer".into(),
        ));
    }
    Ok(())
}

fn pre_submit_error(error: kube::Error) -> BackendError {
    match error {
        kube::Error::Api(status) if status.code == 404 => BackendError::NotFound,
        kube::Error::Api(status) if status.code == 403 => BackendError::Forbidden,
        kube::Error::Api(status) if status.code == 409 => {
            BackendError::Conflict("the target changed before submission".into())
        }
        _ => BackendError::Internal("kubernetes api unreachable before mutation submission".into()),
    }
}

fn finish(engine: &crate::operation::OperationEngine, id: &str, result: Result<(), kube::Error>) {
    match result {
        Ok(()) => {
            let _ = engine.succeeded(id);
        }
        Err(kube::Error::Api(status)) => {
            let detail = match status.code {
                403 => "the mutation was forbidden",
                404 => "the target no longer exists",
                409 => "the target changed during submission",
                _ => "the api server rejected the mutation",
            };
            let _ = engine.failed(id, detail);
        }
        Err(_) => {
            let _ = engine.outcome_unknown(id);
        }
    }
}
