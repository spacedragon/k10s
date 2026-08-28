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

use crate::port::{
    Gvk, OwnerRef, ResourceProjection, ResourceRef, ServicePort, ServiceProjection, TargetPort,
    TransportProtocol,
};
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
        projection: project(gvk, object),
    }
}

/// Per-kind structured projection, derived from typed fields only.
fn project(gvk: &Gvk, object: &kube::core::DynamicObject) -> Option<ResourceProjection> {
    match (gvk.group.as_str(), gvk.version.as_str(), gvk.kind.as_str()) {
        ("", "v1", "Service") => typed_object(object, |s: &k8s_openapi::api::core::v1::Service| {
            service_projection(s)
        }),
        _ => None,
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
        ("", "v1", "Service") => typed(object, |s: &k8s_openapi::api::core::v1::Service| {
            service_summary(s)
        }),
        ("", "v1", "Node") => typed(object, |n: &k8s_openapi::api::core::v1::Node| {
            node_summary(n)
        }),
        ("", "v1", "Event") => typed(object, |event: &k8s_openapi::api::core::v1::Event| {
            event.type_.clone().unwrap_or_default()
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
    typed_object(object, summarize).unwrap_or_default()
}

/// Deserialize a full object JSON into its typed form and derive an optional
/// value; any shape mismatch yields `None` instead of a guess.
fn typed_object<K, F, T>(object: &kube::core::DynamicObject, derive: F) -> Option<T>
where
    K: DeserializeOwned,
    F: FnOnce(&K) -> T,
{
    serde_json::to_value(object)
        .ok()
        .and_then(|value| serde_json::from_value::<K>(value).ok())
        .map(|typed| derive(&typed))
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

/// Service summary: the Service type, or the external name for
/// ExternalName Services.
fn service_summary(service: &k8s_openapi::api::core::v1::Service) -> String {
    match service.spec.as_ref() {
        Some(spec) if spec.type_.as_deref() == Some("ExternalName") => spec
            .external_name
            .clone()
            .unwrap_or_else(|| "ExternalName".into()),
        Some(spec) => spec.type_.clone().unwrap_or_else(|| "ClusterIP".to_owned()),
        None => String::new(),
    }
}

/// Build the normalized Service projection from typed fields only.
///
/// Every declared port is projected, including UDP and SCTP entries. An
/// omitted `targetPort` normalizes to the Service port number so the
/// defaulted case is explicit on the wire.
fn service_projection(service: &k8s_openapi::api::core::v1::Service) -> ResourceProjection {
    let spec = service.spec.as_ref();
    let ports = spec
        .map(|spec| spec.ports.iter().flatten().map(service_port).collect())
        .unwrap_or_default();
    ResourceProjection::Service(ServiceProjection {
        service_type: spec
            .and_then(|spec| spec.type_.clone())
            .unwrap_or_else(|| "ClusterIP".into()),
        cluster_ips: spec
            .and_then(|spec| spec.cluster_ips.clone())
            .unwrap_or_default(),
        selector: spec
            .and_then(|spec| spec.selector.clone())
            .unwrap_or_default(),
        external_name: spec.and_then(|spec| spec.external_name.clone()),
        session_affinity: spec.and_then(|spec| spec.session_affinity.clone()),
        external_traffic_policy: spec.and_then(|spec| spec.external_traffic_policy.clone()),
        internal_traffic_policy: spec.and_then(|spec| spec.internal_traffic_policy.clone()),
        ports,
    })
}

/// Project one declared Service port.
fn service_port(port: &k8s_openapi::api::core::v1::ServicePort) -> ServicePort {
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
    let target_port = port.target_port.as_ref().map(|target| match target {
        IntOrString::Int(number) => TargetPort::Number(u16::try_from(*number).unwrap_or(0)),
        IntOrString::String(name) => match name.parse::<u16>() {
            Ok(number) => TargetPort::Number(number),
            Err(_) => TargetPort::Name(name.clone()),
        },
    });
    // Kubernetes defaults an omitted targetPort to the Service port.
    let target_port =
        target_port.unwrap_or_else(|| TargetPort::Number(u16::try_from(port.port).unwrap_or(0)));
    ServicePort {
        name: port.name.clone(),
        service_port: u16::try_from(port.port).unwrap_or(0),
        target_port,
        node_port: port.node_port.and_then(|p| u16::try_from(p).ok()),
        protocol: transport_protocol(port.protocol.as_deref()),
        app_protocol: port.app_protocol.clone(),
    }
}

/// Map a Kubernetes port protocol; the omitted value means TCP.
fn transport_protocol(protocol: Option<&str>) -> TransportProtocol {
    match protocol {
        Some("UDP") => TransportProtocol::Udp,
        Some("SCTP") => TransportProtocol::Sctp,
        _ => TransportProtocol::Tcp,
    }
}

fn split_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((group, version)) => (group.to_owned(), version.to_owned()),
        None => (String::new(), api_version.to_owned()),
    }
}
