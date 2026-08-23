//! Normalization of Kubernetes objects into backend list view models.
//!
//! Typed built-in kinds get dedicated summarizers over k8s-openapi structs;
//! everything else — CRDs and unknown built-ins — normalizes through the
//! standard-metadata path (identity, labels, owners, creation timestamp)
//! with an empty summary rather than a guessed one. Quantities are never
//! parsed into display guesses and timestamps are carried through exactly
//! as the cluster reported them.
//!
//! The output is a [`WatchRow`] view model only: no raw object payload,
//! spec, or status ever survives normalization into the cache or wire.

use serde::de::DeserializeOwned;

use crate::port::{Gvk, OwnerRef, ResourceRef};
use crate::runtime::supervisor::WatchRow;

/// Normalize one cluster object into its list view-model row.
///
/// `namespaced` and `namespace` describe the selection the object was listed
/// under; they only fill in the namespace when the object's own metadata
/// omits it (recorded data can be loose there).
pub(crate) fn normalize_row(
    context: &str,
    gvk: &Gvk,
    namespaced: bool,
    namespace: Option<&str>,
    object: &kube::core::DynamicObject,
) -> WatchRow {
    use kube::ResourceExt;

    let name = object.name_any();
    let uid = object.uid().unwrap_or_else(|| {
        // Server-assigned UIDs are always present on real clusters; the
        // deterministic fallback only covers degenerate recorded data.
        format!("uid-{}-{}", gvk.kind.to_lowercase(), name)
    });
    let namespace = object
        .namespace()
        .or_else(|| namespaced.then(|| namespace.map(str::to_owned)).flatten());
    let owner_references: Vec<OwnerRef> = object
        .owner_references()
        .iter()
        .map(|owner| {
            let (group, version) = split_api_version(&owner.api_version);
            OwnerRef {
                gvk: Gvk::new(group, version, owner.kind.clone()),
                name: owner.name.clone(),
                uid: owner.uid.clone(),
                controller: owner.controller.unwrap_or(false),
            }
        })
        .collect();

    WatchRow {
        reference: ResourceRef {
            context: context.to_owned(),
            gvk: gvk.clone(),
            namespace,
            name,
            uid,
        },
        labels: object.labels().clone(),
        summary: summarize(gvk, object),
        created_at: object
            .creation_timestamp()
            .map(|time| time.0.to_string())
            .unwrap_or_default(),
        owner_references,
    }
}

/// Per-kind status summary, derived from typed fields only.
fn summarize(gvk: &Gvk, object: &kube::core::DynamicObject) -> String {
    match (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()) {
        ("apps", "v1", "Deployment") => {
            typed(object, |d: &k8s_openapi::api::apps::v1::Deployment| {
                replica_summary(
                    d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1),
                    d.status.as_ref().and_then(|s| s.ready_replicas),
                )
            })
        }
        ("apps", "v1", "StatefulSet") => {
            typed(object, |s: &k8s_openapi::api::apps::v1::StatefulSet| {
                replica_summary(
                    s.spec.as_ref().and_then(|spec| spec.replicas).unwrap_or(1),
                    s.status.as_ref().and_then(|status| status.ready_replicas),
                )
            })
        }
        ("apps", "v1", "DaemonSet") => {
            typed(object, |d: &k8s_openapi::api::apps::v1::DaemonSet| {
                let status = d.status.as_ref();
                replica_summary(
                    status.map_or(0, |s| s.desired_number_scheduled),
                    status.map(|s| s.number_ready),
                )
            })
        }
        ("batch", "v1", "Job") => typed(object, |j: &k8s_openapi::api::batch::v1::Job| {
            job_summary(j)
        }),
        ("batch", "v1", "CronJob") => typed(object, |c: &k8s_openapi::api::batch::v1::CronJob| {
            cronjob_summary(c)
        }),
        ("", "v1", "Pod") => typed(object, |p: &k8s_openapi::api::core::v1::Pod| pod_summary(p)),
        ("", "v1", "Node") => typed(object, |n: &k8s_openapi::api::core::v1::Node| {
            node_summary(n)
        }),
        ("", "v1", "PersistentVolumeClaim") => typed(
            object,
            |p: &k8s_openapi::api::core::v1::PersistentVolumeClaim| {
                p.status
                    .as_ref()
                    .and_then(|s| s.phase.clone())
                    .unwrap_or_else(|| "Pending".into())
            },
        ),
        ("", "v1", "PersistentVolume") => typed(
            object,
            |p: &k8s_openapi::api::core::v1::PersistentVolume| {
                p.status
                    .as_ref()
                    .and_then(|s| s.phase.clone())
                    .unwrap_or_else(|| "Pending".into())
            },
        ),
        // StorageClasses carry no meaningful phase; empty is honest.
        _ => String::new(),
    }
}

/// Deserialize a full object JSON into its typed form and derive a summary;
/// any shape mismatch yields an empty summary instead of a guess.
fn typed<K, F>(object: &kube::core::DynamicObject, summarize: F) -> String
where
    K: DeserializeOwned,
    F: FnOnce(&K) -> String,
{
    serde_json::to_value(object)
        .ok()
        .and_then(|value| serde_json::from_value::<K>(value).ok())
        .map(|typed| summarize(&typed))
        .unwrap_or_default()
}

/// `ready/desired ready`, clamped to non-negative counts.
fn replica_summary(desired: i32, ready: Option<i32>) -> String {
    let ready = ready.unwrap_or(0).max(0);
    format!("{}/{} ready", ready, desired.max(0))
}

fn job_summary(job: &k8s_openapi::api::batch::v1::Job) -> String {
    let conditions = job
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default();
    for condition in conditions {
        if condition.status != "True" {
            continue;
        }
        match condition.type_.as_str() {
            "Complete" => return "Complete".into(),
            "Failed" => return "Failed".into(),
            _ => {}
        }
    }
    if job.status.as_ref().and_then(|s| s.active).unwrap_or(0) > 0 {
        "Running".into()
    } else {
        String::new()
    }
}

fn cronjob_summary(cronjob: &k8s_openapi::api::batch::v1::CronJob) -> String {
    if cronjob.spec.suspend == Some(true) {
        return "Suspended".into();
    }
    if cronjob
        .status
        .as_ref()
        .and_then(|status| status.active.as_deref())
        .is_some_and(|active| !active.is_empty())
    {
        return "Running".into();
    }
    String::new()
}

fn pod_summary(pod: &k8s_openapi::api::core::v1::Pod) -> String {
    let status = match pod.status.as_ref() {
        Some(status) => status,
        None => return "Unknown".into(),
    };
    let statuses = status
        .init_container_statuses
        .iter()
        .flatten()
        .chain(status.container_statuses.iter().flatten());
    for status in statuses {
        let waiting_reason = status
            .state
            .as_ref()
            .and_then(|state| state.waiting.as_ref())
            .and_then(|waiting| waiting.reason.as_deref());
        if waiting_reason == Some("CrashLoopBackOff") {
            return "CrashLoopBackOff".into();
        }
    }
    pod.status
        .as_ref()
        .and_then(|status| status.phase.clone())
        .unwrap_or_else(|| "Unknown".into())
}

fn node_summary(node: &k8s_openapi::api::core::v1::Node) -> String {
    let conditions = node
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_deref())
        .unwrap_or_default();
    for condition in conditions {
        if condition.type_ != "Ready" {
            continue;
        }
        return match condition.status.as_str() {
            "True" => "Ready".into(),
            "False" => "NotReady".into(),
            _ => "Unknown".into(),
        };
    }
    "Unknown".into()
}

fn split_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((group, version)) => (group.to_owned(), version.to_owned()),
        None => (String::new(), api_version.to_owned()),
    }
}
