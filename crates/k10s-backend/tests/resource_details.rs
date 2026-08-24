//! On-demand detail reads for the real adapter: exact-identity GET,
//! resource-gone semantics, controller-UID owner traversal, tailored detail
//! fields, newest-first normalized events across both Kubernetes Event API
//! variants, and YAML bound to UID/resourceVersion.
//!
//! Every test drives a real kube-rs client against a recorded tower-level
//! API server; no live cluster is contacted.

use std::time::Duration;

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, BackendKernel, ContextInfo, Gvk, KernelQueryResult, KubeAdapter,
    KubernetesAccess, Query, ResourceRef,
};
use serde_json::json;

const CONTEXT: &str = "detail-mock";
const NS: &str = "default";

/// One adapter around a fresh recorded server sharing the standard
/// discovery surface.
fn adapter_for(server: &RecordedApiServer) -> KubeAdapter {
    let client = server.clone().into_client(NS);
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some(NS.into()),
            is_current: true,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds around the recorded server")
}

fn kernel(server: &RecordedApiServer) -> BackendKernel {
    BackendKernel::new(adapter_for(server))
}

fn reference(gvk: Gvk, name: &str, uid: &str) -> ResourceRef {
    ResourceRef {
        context: CONTEXT.into(),
        gvk,
        namespace: Some(NS.into()),
        name: name.into(),
        uid: uid.into(),
    }
}

fn deployments_gvk() -> Gvk {
    Gvk::new("apps", "v1", "Deployment")
}

fn pods_gvk() -> Gvk {
    Gvk::core("v1", "Pod")
}

async fn detail(
    kernel: &BackendKernel,
    reference: ResourceRef,
) -> Result<k10s_protocol::ResourceDetailResponse, BackendError> {
    match kernel.query(Query::ResourceDetail { reference }).await {
        Ok(KernelQueryResult::ResourceDetail(result)) => Ok(result.wire_payload()),
        Ok(other) => panic!("kernel must map the detail into its wire payload, got {other:?}"),
        Err(error) => Err(error),
    }
}

fn overview_value(detail: &k10s_protocol::ResourceDetailResponse, label: &str) -> String {
    detail.sections[0]
        .rows
        .iter()
        .find(|row| row.label == label)
        .unwrap_or_else(|| panic!("overview carries a {label} row"))
        .value
        .clone()
}

/// A three-ready deployment with labels, owners of its own absent, and an
/// explicit resourceVersion so the YAML binding can be asserted.
fn recorded_deployment() -> String {
    json!({
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "web",
            "namespace": NS,
            "uid": "uid-web",
            "resourceVersion": "41",
            "creationTimestamp": "2026-08-21T00:00:00Z",
            "labels": {"app": "web"},
        },
        "spec": {"replicas": 3},
        "status": {"readyReplicas": 3},
    })
    .to_string()
}

#[tokio::test]
async fn exact_identity_get_returns_tailored_detail_fields() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &recorded_deployment(),
    );
    let kernel = kernel(&server);

    let detail = detail(&kernel, reference(deployments_gvk(), "web", "uid-web"))
        .await
        .expect("exact identity resolves");

    // Identity header echoes the request exactly.
    assert_eq!(detail.identity.name, "web");
    assert_eq!(detail.identity.gvk.kind, "Deployment");
    assert_eq!(detail.identity.namespace.as_deref(), Some(NS));
    assert_eq!(detail.identity.uid, "uid-web");

    // Tailored fields come from the object itself, never guesses.
    assert_eq!(detail.created_at, "2026-08-21T00:00:00Z");
    assert!(
        detail.revision.get() >= 1,
        "detail carries a backend revision"
    );
    assert_eq!(overview_value(&detail, "Status"), "3/3 ready");
    let labels = detail
        .sections
        .iter()
        .find(|section| section.title == "Labels")
        .expect("labels surface on the detail");
    assert!(
        labels
            .rows
            .iter()
            .any(|row| row.label == "app" && row.value == "web")
    );
    assert!(detail.capabilities.can_scale, "deployments are scalable");

    hit_once(&server, "/apis/apps/v1/namespaces/default/deployments/web");
}

#[tokio::test]
async fn missing_resource_is_typed_not_found() {
    let server = RecordedApiServer::standard();
    let kernel = kernel(&server);

    let error = detail(&kernel, reference(deployments_gvk(), "ghost", "uid-ghost"))
        .await
        .expect_err("a vanished object is not found");
    assert_eq!(error, BackendError::NotFound);
}

#[tokio::test]
async fn stale_uid_get_is_rejected_as_not_found() {
    let server = RecordedApiServer::standard();
    // The object exists under this name, but the caller's UID is from a
    // past life of a delete/recreate cycle.
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &recorded_deployment(),
    );
    let kernel = kernel(&server);

    let error = detail(
        &kernel,
        reference(deployments_gvk(), "web", "uid-from-a-past-life"),
    )
    .await
    .expect_err("a stale UID must never resolve by reused name");
    assert_eq!(error, BackendError::NotFound);
}

#[tokio::test]
async fn cluster_scoped_node_detail_reads_by_exact_identity() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/api/v1/nodes/ip-10-0-0-5",
        200,
        &json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": {
                "name": "ip-10-0-0-5",
                "uid": "uid-node",
                "resourceVersion": "77",
                "creationTimestamp": "2026-08-20T09:00:00Z",
            },
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        })
        .to_string(),
    );
    let kernel = kernel(&server);

    let node_ref = ResourceRef {
        context: CONTEXT.into(),
        gvk: Gvk::core("v1", "Node"),
        namespace: None,
        name: "ip-10-0-0-5".into(),
        uid: "uid-node".into(),
    };
    let detail = detail(&kernel, node_ref).await.expect("node resolves");
    assert_eq!(overview_value(&detail, "Status"), "Ready");
    assert_eq!(
        overview_value(&detail, "Scope"),
        "Cluster-scoped",
        "cluster-scoped objects report their scope"
    );

    hit_once(&server, "/api/v1/nodes/ip-10-0-0-5");
}

/// Controller-UID traversal: only objects whose *controller* owner UID chain
/// reaches the target resolve. Labels never decide ownership, and a reused
/// name with a different UID stays unrelated.
#[tokio::test]
async fn relations_traverse_controller_uids_not_labels_or_reused_names() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &recorded_deployment(),
    );
    server.set_response(
        "/apis/apps/v1/namespaces/default/replicasets",
        200,
        &json!({
            "kind": "ReplicaSetList",
            "apiVersion": "apps/v1",
            "metadata": {"resourceVersion": "42"},
            "items": [{
                "metadata": {
                    "name": "web-frontend-7d9f8",
                    "namespace": NS,
                    "uid": "uid-rs",
                    "creationTimestamp": "2026-08-21T00:01:00Z",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web",
                        "uid": "uid-web",
                        "controller": true,
                    }],
                },
            }]
        })
        .to_string(),
    );
    server.set_response(
        "/api/v1/namespaces/default/pods",
        200,
        &json!({
            "kind": "PodList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "43"},
            "items": [
                {
                    "metadata": {
                        "name": "pod-a",
                        "namespace": NS,
                        "uid": "uid-pod-a",
                        "creationTimestamp": "2026-08-21T00:02:00Z",
                        "labels": {"app": "web"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "web-frontend-7d9f8",
                            "uid": "uid-rs",
                            "controller": true,
                        }],
                    },
                    "status": {"phase": "Running"},
                },
                {
                    "metadata": {
                        "name": "pod-label-only",
                        "namespace": NS,
                        "uid": "uid-pod-label",
                        "creationTimestamp": "2026-08-21T00:02:00Z",
                        "labels": {"app": "web"},
                    },
                    "status": {"phase": "Running"},
                },
                {
                    "metadata": {
                        "name": "pod-non-controller",
                        "namespace": NS,
                        "uid": "uid-pod-nonctl",
                        "creationTimestamp": "2026-08-21T00:02:00Z",
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "Deployment",
                            "name": "web",
                            "uid": "uid-web",
                            "controller": false,
                        }],
                    },
                    "status": {"phase": "Running"},
                },
                {
                    "metadata": {
                        "name": "pod-stale-owner-name",
                        "namespace": NS,
                        "uid": "uid-pod-stale",
                        "creationTimestamp": "2026-08-21T00:02:00Z",
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "web-frontend-7d9f8",
                            "uid": "uid-rs-recreated",
                            "controller": true,
                        }],
                    },
                    "status": {"phase": "Running"},
                }
            ]
        })
        .to_string(),
    );
    let kernel = kernel(&server);

    let detail = detail(&kernel, reference(deployments_gvk(), "web", "uid-web"))
        .await
        .expect("deployment resolves");

    let rs_group = detail
        .related
        .iter()
        .find(|group| group.gvk.kind == "ReplicaSet")
        .expect("the replicaset resolves through its controller UID");
    assert_eq!(rs_group.rows.len(), 1);
    assert_eq!(rs_group.rows[0].identity.name, "web-frontend-7d9f8");
    assert_eq!(rs_group.rows[0].identity.uid, "uid-rs");

    let pod_group = detail
        .related
        .iter()
        .find(|group| group.gvk.kind == "Pod")
        .expect("pods resolve transitively through the replicaset");
    let names: Vec<_> = pod_group
        .rows
        .iter()
        .map(|row| row.identity.name.clone())
        .collect();
    assert_eq!(
        names,
        vec!["pod-a"],
        "only the controller-owned pod resolves"
    );
}

/// Events normalize both Kubernetes Event API variants into one protocol
/// shape and arrive newest-first.
#[tokio::test]
async fn events_normalize_both_api_variants_newest_first() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/api/v1/namespaces/default/pods/web",
        200,
        &json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "web",
                "namespace": NS,
                "uid": "uid-pod",
                "resourceVersion": "44",
                "creationTimestamp": "2026-08-21T00:00:00Z",
            },
            "status": {"phase": "Running"},
        })
        .to_string(),
    );
    // Core/v1 events, deliberately out of chronological order in the cut.
    server.set_response(
        "/api/v1/namespaces/default/events",
        200,
        &json!({
            "kind": "EventList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "45"},
            "items": [
                {
                    "metadata": {"name": "web.1", "namespace": NS, "uid": "uid-ev-1"},
                    "involvedObject": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-pod"},
                    "reason": "Started",
                    "message": "Started container",
                    "count": 3,
                    "lastTimestamp": "2026-08-21T00:03:00Z",
                },
                {
                    "metadata": {"name": "web.2", "namespace": NS, "uid": "uid-ev-2"},
                    "involvedObject": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-other-pod"},
                    "reason": "Unrelated",
                    "message": "Another object's event",
                    "count": 1,
                    "lastTimestamp": "2026-08-21T23:59:59Z",
                },
                {
                    "metadata": {"name": "web.3", "namespace": NS, "uid": "uid-ev-3"},
                    "involvedObject": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-pod"},
                    "reason": "Killing",
                    "message": "Stopping container",
                    "count": 1,
                    "lastTimestamp": "2026-08-21T00:01:00Z",
                }
            ]
        })
        .to_string(),
    );
    // events.k8s.io/v1 variant for the same pod.
    server.set_response(
        "/apis/events.k8s.io/v1/namespaces/default/events",
        200,
        &json!({
            "kind": "EventList",
            "apiVersion": "events.k8s.io/v1",
            "metadata": {"resourceVersion": "46"},
            "items": [{
                "metadata": {"name": "web.x1", "namespace": NS, "uid": "uid-ev-x1"},
                "regarding": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-pod"},
                "reason": "CrashLoopBackOff",
                "note": "Back-off restarting failed container",
                "series": {"count": 2, "lastObservedTime": "2026-08-21T00:07:30.000000Z"},
                "eventTime": "2026-08-21T00:07:00.000000Z",
            }]
        })
        .to_string(),
    );
    let kernel = kernel(&server);

    let detail = detail(&kernel, reference(pods_gvk(), "web", "uid-pod"))
        .await
        .expect("pod resolves");

    let reasons: Vec<(&str, u32, &str)> = detail
        .events
        .iter()
        .map(|event| (event.reason.as_str(), event.count, event.last_seen.as_str()))
        .collect();
    assert_eq!(
        reasons,
        vec![
            ("CrashLoopBackOff", 2, "2026-08-21T00:07:30.000000Z"),
            ("Started", 3, "2026-08-21T00:03:00Z"),
            ("Killing", 1, "2026-08-21T00:01:00Z"),
        ],
        "both event variants merge into one newest-first projection"
    );
}

/// core/v1 and events.k8s.io/v1 mirror the same persisted Event store: one
/// Event UID served through both endpoints surfaces exactly one row.
#[tokio::test]
async fn duplicate_event_uid_across_both_variants_is_emitted_once() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/api/v1/namespaces/default/pods/web",
        200,
        &json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "web",
                "namespace": NS,
                "uid": "uid-pod",
                "resourceVersion": "44",
                "creationTimestamp": "2026-08-21T00:00:00Z",
            },
            "status": {"phase": "Running"},
        })
        .to_string(),
    );
    server.set_response(
        "/api/v1/namespaces/default/events",
        200,
        &json!({
            "kind": "EventList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "45"},
            "items": [{
                "metadata": {"name": "web.1", "namespace": NS, "uid": "uid-ev-shared"},
                "involvedObject": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-pod"},
                "reason": "Started",
                "message": "Started container",
                "count": 3,
                "lastTimestamp": "2026-08-21T00:03:00Z",
            }]
        })
        .to_string(),
    );
    // The same persisted Event again through the dedicated API under its
    // alternative field spellings.
    server.set_response(
        "/apis/events.k8s.io/v1/namespaces/default/events",
        200,
        &json!({
            "kind": "EventList",
            "apiVersion": "events.k8s.io/v1",
            "metadata": {"resourceVersion": "46"},
            "items": [{
                "metadata": {"name": "web.x1", "namespace": NS, "uid": "uid-ev-shared"},
                "regarding": {"kind": "Pod", "name": "web", "namespace": NS, "uid": "uid-pod"},
                "reason": "Started",
                "note": "Started container",
                "series": {"count": 3, "lastObservedTime": "2026-08-21T00:03:00Z"},
                "eventTime": "2026-08-21T00:03:00Z",
            }]
        })
        .to_string(),
    );
    let kernel = kernel(&server);

    let detail = detail(&kernel, reference(pods_gvk(), "web", "uid-pod"))
        .await
        .expect("pod resolves");

    assert_eq!(detail.events.len(), 1, "one persisted Event, one row");
    assert_eq!(detail.events[0].reason, "Started");
    assert_eq!(detail.events[0].message, "Started container");
    assert_eq!(detail.events[0].count, 3);
    assert_eq!(detail.events[0].last_seen, "2026-08-21T00:03:00Z");
}

/// The rendered YAML comes from the fetched object and is bound to its UID
/// and opaque resourceVersion, so guarded edits can detect drift.
#[tokio::test]
async fn yaml_manifest_is_bound_to_uid_and_resource_version() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &recorded_deployment(),
    );
    let kernel = kernel(&server);

    let detail = detail(&kernel, reference(deployments_gvk(), "web", "uid-web"))
        .await
        .expect("exact identity resolves");

    assert!(
        detail.manifest.starts_with("apiVersion: apps/v1"),
        "manifest renders the fetched object: {}",
        detail.manifest
    );
    assert!(detail.manifest.contains("kind: Deployment"));
    assert!(
        detail.manifest.contains("uid-web"),
        "manifest binds to the object UID: {}",
        detail.manifest
    );
    assert!(
        detail.manifest.contains("resourceVersion") && detail.manifest.contains("41"),
        "manifest binds to the opaque resourceVersion: {}",
        detail.manifest
    );
}

/// Relations on a vanished or stale-identity target keep the typed
/// not-found instead of guessing empty relations.
#[tokio::test]
async fn relations_on_a_vanished_target_are_not_found() {
    let server = RecordedApiServer::standard();
    let adapter = adapter_for(&server);
    let error = adapter
        .query(Query::ResourceRelations {
            reference: reference(deployments_gvk(), "ghost", "uid-ghost"),
        })
        .await
        .expect_err("vanished targets are typed not-founds");
    assert_eq!(error, BackendError::NotFound);
}

/// Relation traversal shares the caller's detail deadline: a relation sweep
/// that stalls past the deadline returns Timeout instead of waiting on
/// unbounded cluster I/O after the detail read already succeeded.
#[tokio::test]
async fn slow_relations_traversal_returns_timeout_at_the_deadline() {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &recorded_deployment(),
    );
    server.set_hanging_path("/apis/apps/v1/namespaces/default/replicasets");
    let kernel = kernel(&server);

    let error = kernel
        .query_with_deadline(
            Query::ResourceDetail {
                reference: reference(deployments_gvk(), "web", "uid-web"),
            },
            Some(Duration::from_millis(500)),
        )
        .await
        .expect_err("relation traversal past the deadline must be cancelled");
    assert_eq!(error, BackendError::Timeout);
    assert!(
        server.hit_count("/apis/apps/v1/namespaces/default/replicasets") >= 1,
        "the deadline cut a relation sweep that had already begun"
    );
}

fn hit_once(server: &RecordedApiServer, path: &str) {
    assert!(
        server.hit_count(path) >= 1,
        "{path} is read on demand for the detail"
    );
}
