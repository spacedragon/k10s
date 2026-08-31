//! Structured Deployment and ReplicaSet projections from typed Kubernetes data.
//!
//! These projections retain only fields already represented by the backend
//! port. They never inspect manifest text or list summaries.

use k8s_openapi::api::apps::v1::{Deployment, DeploymentCondition, ReplicaSet};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::port::{
    ContainerImageProjection, DeploymentProjection, ReplicaSetProjection,
    ResourceConditionProjection, ResourceProjection,
};

/// Project an apps/v1 Deployment from its typed metadata, spec, and status.
pub(super) fn deployment_projection(deployment: &Deployment) -> ResourceProjection {
    let spec = deployment.spec.as_ref();
    let status = deployment.status.as_ref();
    let strategy = spec.and_then(|spec| spec.strategy.as_ref());
    let rolling_update = strategy.and_then(|strategy| strategy.rolling_update.as_ref());

    ResourceProjection::Deployment(DeploymentProjection {
        desired_replicas: spec.and_then(|spec| nonnegative(spec.replicas)),
        ready_replicas: status.and_then(|status| nonnegative(status.ready_replicas)),
        updated_replicas: status.and_then(|status| nonnegative(status.updated_replicas)),
        available_replicas: status.and_then(|status| nonnegative(status.available_replicas)),
        strategy: strategy.and_then(|strategy| strategy.type_.clone()),
        selector: spec
            .and_then(|spec| spec.selector.match_labels.clone())
            .unwrap_or_default(),
        max_surge: rolling_update
            .and_then(|rolling_update| rolling_update.max_surge.as_ref())
            .map(int_or_string),
        max_unavailable: rolling_update
            .and_then(|rolling_update| rolling_update.max_unavailable.as_ref())
            .map(int_or_string),
        conditions: deployment_conditions(status.and_then(|status| status.conditions.as_deref())),
        template_containers: spec
            .and_then(|spec| spec.template.spec.as_ref())
            .map(|template| {
                template
                    .containers
                    .iter()
                    .map(|container| ContainerImageProjection {
                        name: container.name.clone(),
                        image: container.image.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        template_labels: spec
            .and_then(|spec| spec.template.metadata.as_ref())
            .and_then(|metadata| metadata.labels.clone())
            .unwrap_or_default(),
        template_annotations: spec
            .and_then(|spec| spec.template.metadata.as_ref())
            .and_then(|metadata| metadata.annotations.clone())
            .unwrap_or_default(),
        labels: deployment.metadata.labels.clone().unwrap_or_default(),
        annotations: deployment.metadata.annotations.clone().unwrap_or_default(),
        created_at: deployment
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|time| time.0.to_string()),
    })
}

/// Project a ReplicaSet only when its Deployment revision annotation is valid.
///
/// A ReplicaSet without that authoritative annotation remains a valid row but
/// must not be guessed into a rollout-history entry.
pub(super) fn replica_set_projection(replica_set: &ReplicaSet) -> Option<ResourceProjection> {
    let revision = replica_set
        .metadata
        .annotations
        .as_ref()?
        .get("deployment.kubernetes.io/revision")?
        .parse()
        .ok()?;

    Some(ResourceProjection::ReplicaSet(ReplicaSetProjection {
        revision,
        replicas: replica_set
            .spec
            .as_ref()
            .and_then(|spec| nonnegative(spec.replicas)),
        ready_replicas: replica_set
            .status
            .as_ref()
            .and_then(|status| nonnegative(status.ready_replicas)),
        created_at: replica_set
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|time| time.0.to_string()),
    }))
}

/// Convert a Kubernetes count only when it is representable on the port.
fn nonnegative(value: Option<i32>) -> Option<u32> {
    value.and_then(|value| u32::try_from(value).ok())
}

/// Preserve either Kubernetes `IntOrString` representation without a guess.
fn int_or_string(value: &IntOrString) -> String {
    match value {
        IntOrString::Int(value) => value.to_string(),
        IntOrString::String(value) => value.clone(),
    }
}

/// Map and sort Deployment conditions into the deterministic backend shape.
fn deployment_conditions(
    conditions: Option<&[DeploymentCondition]>,
) -> Vec<ResourceConditionProjection> {
    let mut projections: Vec<_> = conditions
        .unwrap_or_default()
        .iter()
        .map(|condition| ResourceConditionProjection {
            condition_type: condition.type_.clone(),
            status: condition.status.clone(),
            reason: condition.reason.clone(),
            message: condition.message.clone(),
            last_transition_time: condition
                .last_transition_time
                .as_ref()
                .map(|time| time.0.to_string()),
        })
        .collect();
    projections.sort_by(|left, right| {
        (
            &left.condition_type,
            &left.status,
            &left.reason,
            &left.message,
            &left.last_transition_time,
        )
            .cmp(&(
                &right.condition_type,
                &right.status,
                &right.reason,
                &right.message,
                &right.last_transition_time,
            ))
    });
    projections
}
