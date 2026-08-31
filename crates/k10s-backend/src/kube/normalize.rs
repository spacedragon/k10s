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
    ContainerStateProjection, ContainerTerminationProjection, Gvk, OwnerRef, PodContainerPort,
    PodContainerProjection, PodProjection, ResourceConditionProjection, ResourceProjection,
    ResourceRef, ServicePort, ServiceProjection, TargetPort, TransportProtocol,
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
        ("", "v1", "Pod") => typed_object(object, |pod: &k8s_openapi::api::core::v1::Pod| {
            pod_projection(pod)
        }),
        ("", "v1", "Service") => typed_object(object, |s: &k8s_openapi::api::core::v1::Service| {
            service_projection(s)
        }),
        _ => None,
    }
}

/// Build the normalized Pod projection from Kubernetes metadata, spec, and
/// status fields only. Container statuses join declared containers by exact
/// name; summaries and manifest text never participate in this projection.
fn pod_projection(pod: &k8s_openapi::api::core::v1::Pod) -> ResourceProjection {
    use std::collections::{BTreeMap, BTreeSet};

    let spec = pod.spec.as_ref();
    let status = pod.status.as_ref();
    let declared = spec.map(|spec| spec.containers.as_slice());
    let statuses = status
        .and_then(|status| status.container_statuses.as_deref())
        .unwrap_or_default();
    let mut status_by_name: BTreeMap<_, _> = statuses
        .iter()
        .map(|container| (container.name.as_str(), container))
        .collect();
    let status_names_unique = status_by_name.len() == statuses.len();
    let declared_names_unique = declared.is_none_or(|containers| {
        containers
            .iter()
            .map(|container| container.name.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == containers.len()
    });
    let can_join_statuses = status_names_unique && declared_names_unique;
    if !can_join_statuses {
        // Duplicate names make an exact status join ambiguous. Withhold
        // per-container status rather than retaining an arbitrary duplicate.
        status_by_name.clear();
    }
    let containers = declared
        .unwrap_or_default()
        .iter()
        .map(|container| {
            let status = status_by_name.get(container.name.as_str()).copied();
            PodContainerProjection {
                name: container.name.clone(),
                image: container.image.clone(),
                state: status
                    .and_then(|status| status.state.as_ref())
                    .and_then(container_state),
                ready: status.map(|status| status.ready),
                restart_count: status.and_then(|status| u32::try_from(status.restart_count).ok()),
                last_termination: status
                    .and_then(|status| status.last_state.as_ref())
                    .and_then(|state| state.terminated.as_ref())
                    .map(termination),
            }
        })
        .collect();
    let statuses_complete = can_join_statuses
        && declared.is_some_and(|declared| {
            declared
                .iter()
                .all(|container| status_by_name.contains_key(container.name.as_str()))
        });
    let ready_containers = statuses_complete
        .then(|| {
            declared
                .unwrap_or_default()
                .iter()
                .filter(|container| {
                    status_by_name
                        .get(container.name.as_str())
                        .is_some_and(|status| status.ready)
                })
                .count()
        })
        .and_then(|count| u32::try_from(count).ok());
    let restart_count = statuses_complete
        .then(|| {
            declared
                .unwrap_or_default()
                .iter()
                .try_fold(0u32, |total, container| {
                    let status = status_by_name.get(container.name.as_str())?;
                    total.checked_add(u32::try_from(status.restart_count).ok()?)
                })
        })
        .flatten();
    let mut conditions: Vec<_> = status
        .and_then(|status| status.conditions.as_deref())
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
    conditions.sort_by(|left, right| {
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

    ResourceProjection::Pod(PodProjection {
        phase: status.and_then(|status| status.phase.clone()),
        ready_containers,
        total_containers: declared.and_then(|containers| u32::try_from(containers.len()).ok()),
        restart_count,
        containers,
        conditions,
        node_name: spec.and_then(|spec| spec.node_name.clone()),
        pod_ip: status.and_then(|status| status.pod_ip.clone()),
        host_ip: status.and_then(|status| status.host_ip.clone()),
        qos_class: status.and_then(|status| status.qos_class.clone()),
        priority: spec.and_then(|spec| spec.priority),
        service_account: spec.and_then(|spec| spec.service_account_name.clone()),
        restart_policy: spec.and_then(|spec| spec.restart_policy.clone()),
        ports: declared
            .unwrap_or_default()
            .iter()
            .flat_map(|container| {
                container
                    .ports
                    .iter()
                    .flatten()
                    .filter_map(move |port| pod_container_port(&container.name, port))
            })
            .collect(),
        labels: pod.metadata.labels.clone().unwrap_or_default(),
        annotations: pod.metadata.annotations.clone().unwrap_or_default(),
        created_at: pod
            .metadata
            .creation_timestamp
            .as_ref()
            .map(|time| time.0.to_string()),
    })
}

/// Normalize one current container lifecycle state.
fn container_state(
    state: &k8s_openapi::api::core::v1::ContainerState,
) -> Option<ContainerStateProjection> {
    if state.running.is_some() {
        Some(ContainerStateProjection::Running)
    } else if let Some(waiting) = state.waiting.as_ref() {
        Some(ContainerStateProjection::Waiting {
            reason: waiting.reason.clone(),
        })
    } else {
        state
            .terminated
            .as_ref()
            .map(termination)
            .map(ContainerStateProjection::Terminated)
    }
}

/// Normalize a Kubernetes container termination without interpreting it.
fn termination(
    terminated: &k8s_openapi::api::core::v1::ContainerStateTerminated,
) -> ContainerTerminationProjection {
    ContainerTerminationProjection {
        exit_code: terminated.exit_code,
        reason: terminated.reason.clone(),
    }
}

/// Normalize a declared port only when Kubernetes supplied a valid container
/// port. Host port `0` remains explicit when the API supplied it.
fn pod_container_port(
    container_name: &str,
    port: &k8s_openapi::api::core::v1::ContainerPort,
) -> Option<PodContainerPort> {
    let container_port = u16::try_from(port.container_port)
        .ok()
        .filter(|port| *port > 0)?;
    Some(PodContainerPort {
        container_name: container_name.to_owned(),
        name: port.name.clone(),
        container_port,
        host_port: port.host_port.and_then(|port| u16::try_from(port).ok()),
        protocol: transport_protocol(port.protocol.as_deref()),
    })
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
        if let Some(reason) = waiting_reason.filter(|reason| !reason.is_empty()) {
            return reason.to_owned();
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
