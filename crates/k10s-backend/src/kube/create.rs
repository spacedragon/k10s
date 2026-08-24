//! Kubernetes-native Job creation and CronJob scheduling mutations.

use kube::ResourceExt;
use kube::api::{Patch, PatchParams, PostParams};
use serde_json::{Value, json};

use crate::port::{BackendError, Gvk, OperationId, ResourceRef};

use super::KubeAdapter;
use super::watch::dynamic_api;

impl KubeAdapter {
    pub(super) async fn create_job(
        &self,
        source: ResourceRef,
        key: String,
    ) -> Result<OperationId, BackendError> {
        let fingerprint = format!("create-job/{}", source.exact_identity_key());
        if let Some(id) = self.operations.replay(&key, &fingerprint)? {
            return Ok(id);
        }
        if source.namespace.is_none()
            || source.gvk.group != "batch"
            || source.gvk.version != "v1"
            || !matches!(source.gvk.kind.as_str(), "Job" | "CronJob")
        {
            return Err(BackendError::unsupported("job.create"));
        }
        let (_, current, _) = self.mutation_target(&source).await?;
        let value = serde_json::to_value(current)
            .map_err(|_| BackendError::Internal("could not normalize job source".into()))?;
        let spec = if source.gvk.kind == "CronJob" {
            value.pointer("/spec/jobTemplate/spec")
        } else {
            value.get("spec")
        }
        .cloned()
        .ok_or_else(|| BackendError::Conflict("the source has no Job template".into()))?;
        let object = serde_json::from_value(json!({
            "apiVersion":"batch/v1", "kind":"Job",
            "metadata":{"generateName":generated_prefix(&source.name), "namespace":source.namespace},
            "spec":clean_job_spec(spec)
        })).map_err(|_| BackendError::Internal("could not build Job submission".into()))?;
        let descriptor = self
            .descriptor_for(&source.context, &Gvk::new("batch", "v1", "Job"))
            .await?;
        if !descriptor.namespaced || !descriptor.supports_create {
            return Err(BackendError::unsupported("job.create"));
        }
        let client = self.cluster_client(&source.context).await?;
        let api = dynamic_api(
            client,
            descriptor.gvk,
            descriptor.plural,
            true,
            source.namespace.clone(),
        );
        let scope = source.coalescing_key();
        self.spawn_mutation(key, fingerprint, scope, async move {
            api.create(&PostParams::default(), &object)
                .await
                .map(|_| ())
        })
    }

    pub(super) async fn set_cronjob_suspended(
        &self,
        target: ResourceRef,
        suspended: bool,
        key: String,
    ) -> Result<OperationId, BackendError> {
        let fingerprint = format!(
            "cronjob-suspend/{}/{}",
            target.exact_identity_key(),
            suspended
        );
        if let Some(id) = self.operations.replay(&key, &fingerprint)? {
            return Ok(id);
        }
        if target.gvk != Gvk::new("batch", "v1", "CronJob") {
            return Err(BackendError::unsupported("cronjob.suspend"));
        }
        let (api, current, descriptor) = self.mutation_target(&target).await?;
        if !descriptor.supports_patch {
            return Err(BackendError::unsupported("cronjob.suspend"));
        }
        let rv = current
            .resource_version()
            .ok_or_else(|| BackendError::Conflict("the target has no resourceVersion".into()))?;
        let patch = json!({"metadata":{"resourceVersion":rv},"spec":{"suspend":suspended}});
        let scope = target.coalescing_key();
        self.spawn_mutation(key, fingerprint, scope, async move {
            api.patch(&target.name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map(|_| ())
        })
    }
}

fn clean_job_spec(mut spec: Value) -> Value {
    let Some(map) = spec.as_object_mut() else {
        return spec;
    };
    map.remove("selector");
    map.remove("manualSelector");
    if let Some(labels) = map
        .get_mut("template")
        .and_then(|v| v.get_mut("metadata"))
        .and_then(|v| v.get_mut("labels"))
        .and_then(Value::as_object_mut)
    {
        for key in [
            "controller-uid",
            "job-name",
            "batch.kubernetes.io/controller-uid",
            "batch.kubernetes.io/job-name",
        ] {
            labels.remove(key);
        }
    }
    spec
}

fn generated_prefix(name: &str) -> String {
    let mut prefix: String = name.chars().take(57).collect();
    prefix.push('-');
    prefix
}
