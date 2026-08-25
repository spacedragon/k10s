//! Recorded-API coverage for exact Service-to-Pod port-forward resolution.
//!
//! Every test drives the real kube-rs client against a recorded tower-level
//! API server: no live cluster. The contract under test is the designed
//! resolution policy — exact Service UID binding, EndpointSlice owner
//! scoping, deterministic ready-Pod selection with Pod UID revalidation,
//! and sanitized typed rejections.

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, ContextInfo, KubeAdapter, PortForwardConnector, PortForwardPortSelection,
    PortForwardRequest, RejectionCategory, ResolvedPortForward,
};

const CONTEXT: &str = "pf-cluster";
const NS: &str = "default";
const SERVICE_UID: &str = "uid-svc-current";

fn request(port: PortForwardPortSelection) -> PortForwardRequest {
    PortForwardRequest {
        context: CONTEXT.into(),
        namespace: NS.into(),
        service_name: "web".into(),
        service_uid: SERVICE_UID.into(),
        port,
    }
}

fn connector_for(server: &RecordedApiServer) -> PortForwardConnector {
    let client = server.clone().into_client(NS);
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some(NS.into()),
            is_current: true,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds around the recorded server");
    adapter.port_forward_connector()
}

fn category_of(error: &BackendError) -> Option<RejectionCategory> {
    match error {
        BackendError::PortForward { category, .. } => Some(*category),
        _ => None,
    }
}

/// A ClusterIP TCP Service with one named port.
fn service_json() -> String {
    serde_json::json!({
        "kind": "Service", "apiVersion": "v1",
        "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID,
                     "resourceVersion": "41", "creationTimestamp": "2026-08-21T00:00:00Z"},
        "spec": {"type": "ClusterIP", "clusterIP": "10.96.0.10",
                 "selector": {"app": "web"},
                 "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]}
    })
    .to_string()
}

/// One EndpointSlice owned by the current Service UID carrying one endpoint.
fn slice_json(name: &str, uid: &str, owner_uid: &str, endpoints: serde_json::Value) -> String {
    let owner_references = if owner_uid.is_empty() {
        serde_json::json!([])
    } else {
        serde_json::json!([{
            "apiVersion": "v1", "kind": "Service", "name": "web",
            "uid": owner_uid, "controller": true,
        }])
    };
    serde_json::json!({
        "kind": "EndpointSlice", "apiVersion": "discovery.k8s.io/v1",
        "metadata": {
            "name": name, "namespace": NS, "uid": uid,
            "labels": {"kubernetes.io/service-name": "web"},
            "ownerReferences": owner_references,
        },
        "addressType": "IPv4",
        "ports": [{"name": "http", "port": 80, "protocol": "TCP"}],
        "endpoints": endpoints,
    })
    .to_string()
}

fn ready_pod_endpoint(pod_name: &str, pod_uid: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!([{
        "addresses": ["10.244.0.5"],
        "conditions": {"ready": true},
        "targetRef": {"kind": "Pod", "name": pod_name, "uid": pod_uid,
                      "namespace": namespace}
    }])
}

/// Install the happy-path fixtures: Service plus one owned slice with one
/// ready same-namespace Pod, and the matching Pod object.
fn install_happy_path(server: &RecordedApiServer) {
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &service_json(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [serde_json::from_str::<serde_json::Value>(&slice_json(
                "web-abc",
                "uid-slice-1",
                SERVICE_UID,
                ready_pod_endpoint("web-7d9f8-b", "uid-pod-b", NS),
            ))
            .unwrap()]
        })
        .to_string(),
    );
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/web-7d9f8-b"),
        200,
        &serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "web-7d9f8-b", "namespace": NS, "uid": "uid-pod-b",
                         "creationTimestamp": "2026-08-21T00:00:00Z"}
        })
        .to_string(),
    );
}

#[tokio::test]
async fn exact_service_identity_resolves_to_the_deterministic_ready_pod() {
    let server = RecordedApiServer::standard();
    install_happy_path(&server);
    let connector = connector_for(&server);

    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("the exact identity resolves");
    assert_eq!(
        resolved,
        ResolvedPortForward {
            context: CONTEXT.into(),
            namespace: NS.into(),
            service_uid: SERVICE_UID.into(),
            service_port: 80,
            pod_name: "web-7d9f8-b".into(),
            pod_uid: "uid-pod-b".into(),
            pod_port: 8_080,
        },
        "only backend-owned identifiers cross the seam"
    );

    // The EndpointSlice listing is scoped by the service-name label.
    let uris = server.request_uris("/apis/discovery.k8s.io/v1/namespaces/default/endpointslices");
    assert!(
        uris.iter()
            .any(|uri| uri.contains("kubernetes.io%2Fservice-name%3Dweb")
                || uri.contains("kubernetes.io/service-name=web")),
        "slice listing carries the service-name label selector: {uris:?}"
    );

    // Numeric selection by Service port number works identically.
    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Number(80)))
        .await
        .expect("numeric selection resolves");
    assert_eq!(resolved.pod_port, 8_080);
}

/// A named selection keeps the DECLARED Service port identity on the
/// resolved target even though the pod port differs.
#[tokio::test]
async fn named_selection_keeps_declared_service_port_identity() {
    let server = RecordedApiServer::standard();
    install_happy_path(&server);
    let connector = connector_for(&server);

    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("named selection resolves");
    assert_eq!(resolved.service_port, 80, "declared identity preserved");
    assert_eq!(resolved.pod_port, 8_080);

    // Numeric selection reports the same declared port.
    let numeric = connector
        .resolve_service_port(request(PortForwardPortSelection::Number(80)))
        .await
        .expect("numeric selection resolves");
    assert_eq!(numeric.service_port, 80);
    assert_eq!(numeric.pod_name, resolved.pod_name);
}

/// A named targetPort resolves against the selected Pod's declared TCP
/// container ports.
#[tokio::test]
async fn named_target_ports_resolve_through_pod_container_ports() {
    let server = RecordedApiServer::standard();
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &serde_json::json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
            "spec": {"type": "ClusterIP",
                     "ports": [{"name": "http", "port": 80, "targetPort": "web-http", "protocol": "TCP"}]}
        })
        .to_string(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [serde_json::from_str::<serde_json::Value>(&slice_json(
                "web-abc", "uid-slice-1", SERVICE_UID,
                ready_pod_endpoint("pod-a", "uid-pod-a", NS),
            ))
            .unwrap()]
        })
        .to_string(),
    );
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/pod-a"),
        200,
        &serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "pod-a", "namespace": NS, "uid": "uid-pod-a"},
            "spec": {"containers": [{
                "name": "app",
                "ports": [{"name": "web-http", "containerPort": 8080, "protocol": "TCP"},
                          {"name": "web-http", "containerPort": 9999, "protocol": "UDP"}]
            }]}
        })
        .to_string(),
    );
    let connector = connector_for(&server);
    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("named target port resolves through the pod");
    assert_eq!(resolved.service_port, 80);
    assert_eq!(
        resolved.pod_port, 8_080,
        "the TCP container port is selected over the UDP one"
    );

    // An omitted targetPort defaults to the Service port number.
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &serde_json::json!({
            "kind": "Service", "apiVersion": "v1",
            "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
            "spec": {"type": "ClusterIP",
                     "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}]}
        })
        .to_string(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [{
                "kind": "EndpointSlice", "apiVersion": "discovery.k8s.io/v1",
                "metadata": {"name": "web-abc", "namespace": NS, "uid": "uid-slice-2",
                    "labels": {"kubernetes.io/service-name": "web"},
                    "ownerReferences": [{"apiVersion": "v1", "kind": "Service",
                                         "name": "web", "uid": SERVICE_UID, "controller": true}]},
                "addressType": "IPv4",
                "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}],
                "endpoints": [{"addresses": ["10.244.0.5"], "conditions": {"ready": true},
                               "targetRef": {"kind": "Pod", "name": "pod-a", "uid": "uid-pod-a",
                                             "namespace": NS}}]
            }]
        })
        .to_string(),
    );
    let defaulted = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("defaulted target port resolves");
    assert_eq!(defaulted.pod_port, 8_080);
}

#[tokio::test]
async fn recreated_services_and_stale_slices_are_rejected_or_skipped() {
    let server = RecordedApiServer::standard();
    // Mixed set: stale slice owned by the previous Service lifetime, an
    // ownerless hand-crafted slice, and the current owned slice.
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &service_json(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [
                serde_json::from_str::<serde_json::Value>(&slice_json(
                    "web-stale", "uid-slice-old", "uid-svc-previous",
                    ready_pod_endpoint("stale-pod", "uid-pod-stale", NS),
                )).unwrap(),
                serde_json::from_str::<serde_json::Value>(&slice_json(
                    "web-hand", "uid-slice-hand", "",
                    ready_pod_endpoint("hand-pod", "uid-pod-hand", NS),
                )).unwrap(),
                serde_json::from_str::<serde_json::Value>(&slice_json(
                    "web-cur", "uid-slice-new", SERVICE_UID,
                    ready_pod_endpoint("cur-pod", "uid-pod-cur", NS),
                )).unwrap()
            ]
        })
        .to_string(),
    );
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/cur-pod"),
        200,
        &serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "cur-pod", "namespace": NS, "uid": "uid-pod-cur",
                         "creationTimestamp": "2026-08-21T00:00:00Z"}
        })
        .to_string(),
    );
    let connector = connector_for(&server);

    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("only the owned slice survives");
    assert_eq!(resolved.pod_name, "cur-pod");
    assert_eq!(resolved.pod_uid, "uid-pod-cur");

    // A request carrying a stale Service UID never resolves even though the
    // name still exists.
    let stale = PortForwardRequest {
        service_uid: "uid-svc-previous".into(),
        ..request(PortForwardPortSelection::Name("http".into()))
    };
    let error = connector
        .resolve_service_port(stale)
        .await
        .expect_err("recreated services reject stale identities");
    assert_eq!(
        category_of(&error),
        Some(RejectionCategory::VanishedResource)
    );
}

#[tokio::test]
async fn candidates_sort_deterministically_and_pods_revalidate_by_uid() {
    let server = RecordedApiServer::standard();
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &service_json(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [serde_json::from_str::<serde_json::Value>(&slice_json(
                "web-abc", "uid-slice-1", SERVICE_UID,
                serde_json::json!([
                    {"addresses": ["10.244.0.6"], "conditions": {"ready": true},
                     "targetRef": {"kind": "Pod", "name": "web-z", "uid": "uid-z", "namespace": NS}},
                    {"addresses": ["10.244.0.2"], "conditions": {"ready": false},
                     "targetRef": {"kind": "Pod", "name": "web-a-not-ready", "uid": "uid-nr", "namespace": NS}},
                    {"addresses": ["10.244.0.4"], "conditions": {"ready": true},
                     "targetRef": {"kind": "Pod", "name": "web-b", "uid": "uid-b", "namespace": NS}},
                    {"addresses": ["10.244.0.3"], "conditions": {},
                     "targetRef": {"kind": "Pod", "name": "web-c-default-ready", "uid": "uid-c", "namespace": "other"}},
                    {"addresses": ["10.244.0.5"], "conditions": {"ready": true},
                     "targetRef": {"kind": "Node", "name": "node-1", "uid": "uid-node", "namespace": NS}}
                ]),
            ))
            .unwrap()]
        })
        .to_string(),
    );
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/web-b"),
        200,
        &serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "web-b", "namespace": NS, "uid": "uid-replaced",
                         "creationTimestamp": "2026-08-21T00:00:00Z"}
        })
        .to_string(),
    );
    let connector = connector_for(&server);

    // Not-ready, cross-namespace, and non-Pod targets are skipped; the first
    // sorted candidate is web-b — whose live UID no longer matches.
    let error = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect_err("a replaced Pod must not resolve");
    assert_eq!(
        category_of(&error),
        Some(RejectionCategory::UnavailableEndpoint)
    );

    // With the correct live UID the same selection resolves.
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/web-b"),
        200,
        &serde_json::json!({
            "kind": "Pod", "apiVersion": "v1",
            "metadata": {"name": "web-b", "namespace": NS, "uid": "uid-b",
                         "creationTimestamp": "2026-08-21T00:00:00Z"}
        })
        .to_string(),
    );
    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("the verified Pod resolves");
    assert_eq!(resolved.pod_name, "web-b");
    assert_eq!(resolved.pod_uid, "uid-b");
}

#[tokio::test]
async fn unsupported_and_unavailable_targets_reject_with_typed_categories() {
    struct Case {
        label: &'static str,
        service: serde_json::Value,
        expected: RejectionCategory,
    }
    let cases = [
        Case {
            label: "udp-port",
            service: serde_json::json!({
                "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
                "spec": {"type": "ClusterIP",
                         "ports": [{"name": "dns", "port": 53, "protocol": "UDP"}]}
            }),
            expected: RejectionCategory::UnsupportedService,
        },
        Case {
            label: "sctp-port",
            service: serde_json::json!({
                "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
                "spec": {"type": "ClusterIP",
                         "ports": [{"name": "signaling", "port": 5060, "protocol": "SCTP"}]}
            }),
            expected: RejectionCategory::UnsupportedService,
        },
        Case {
            label: "external-name",
            service: serde_json::json!({
                "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
                "spec": {"type": "ExternalName", "externalName": "example.com"}
            }),
            expected: RejectionCategory::UnsupportedService,
        },
        Case {
            label: "missing-port",
            service: serde_json::json!({
                "metadata": {"name": "web", "namespace": NS, "uid": SERVICE_UID},
                "spec": {"type": "ClusterIP",
                         "ports": [{"name": "http", "port": 80, "protocol": "TCP"}]}
            }),
            expected: RejectionCategory::UnsupportedService,
        },
    ];
    for case in &cases {
        let server = RecordedApiServer::standard();
        let mut body = case.service.clone();
        body["kind"] = serde_json::json!("Service");
        body["apiVersion"] = serde_json::json!("v1");
        server.set_response(
            &format!("/api/v1/namespaces/{NS}/services/web"),
            200,
            &body.to_string(),
        );
        let connector = connector_for(&server);
        let selection = match case.label {
            "missing-port" => PortForwardPortSelection::Number(9_000),
            _ => PortForwardPortSelection::Name("dns".into()),
        };
        let error = match connector.resolve_service_port(request(selection)).await {
            Err(error) => error,
            Ok(_) => panic!("{}: expected rejection", case.label),
        };
        assert_eq!(
            category_of(&error),
            Some(case.expected),
            "{}: wrong rejection category",
            case.label
        );
    }

    // No ready endpoint anywhere: Start fails without side effects.
    let server = RecordedApiServer::standard();
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &service_json(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({"kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1", "items": []})
            .to_string(),
    );
    let connector = connector_for(&server);
    let error = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect_err("no slices means no endpoints");
    assert_eq!(
        category_of(&error),
        Some(RejectionCategory::UnavailableEndpoint)
    );

    // Endpoints without a Pod targetRef are skipped entirely.
    let server = RecordedApiServer::standard();
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/services/web"),
        200,
        &service_json(),
    );
    server.set_response(
        &format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        200,
        &serde_json::json!({
            "kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
            "items": [serde_json::from_str::<serde_json::Value>(&slice_json(
                "web-abc", "uid-slice-1", SERVICE_UID,
                serde_json::json!([
                    {"addresses": ["10.1.2.3"], "conditions": {"ready": true}}
                ]),
            ))
            .unwrap()]
        })
        .to_string(),
    );
    let connector = connector_for(&server);
    let error = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect_err("endpointless slices cannot back a forward");
    assert_eq!(
        category_of(&error),
        Some(RejectionCategory::UnavailableEndpoint)
    );
}

#[tokio::test]
async fn forbidden_api_calls_surface_sanitized_failures() {
    for path in [
        format!("/api/v1/namespaces/{NS}/services/web"),
        format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices"),
        format!("/api/v1/namespaces/{NS}/pods/web-7d9f8-b"),
    ] {
        let server = RecordedApiServer::standard();
        install_happy_path(&server);
        server.set_method_response(
            "GET",
            &path,
            403,
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden: pods/portforward","reason":"Forbidden","code":403}"#,
        );
        let connector = connector_for(&server);
        let error = connector
            .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
            .await
            .expect_err("forbidden calls never resolve");
        assert_eq!(
            category_of(&error),
            Some(RejectionCategory::Forbidden),
            "{path}: expected a typed forbidden failure"
        );
    }
}

#[tokio::test]
async fn connect_opens_a_stream_through_the_backend_only() {
    // The recorded API server cannot complete a WebSocket port-forward
    // upgrade; connect must fail safely without leaking internals.
    let server = RecordedApiServer::standard();
    install_happy_path(&server);
    let connector = connector_for(&server);
    let resolved = connector
        .resolve_service_port(request(PortForwardPortSelection::Name("http".into())))
        .await
        .expect("resolution succeeds");
    let error = connector
        .connect(&resolved)
        .await
        .expect_err("a recorded server cannot upgrade");
    assert!(
        matches!(
            error,
            BackendError::PortForward {
                category: RejectionCategory::TransportClosed | RejectionCategory::Forbidden,
                ..
            } | BackendError::Internal(_)
        ),
        "sanitized failure, got {error:?}"
    );

    // The fake seam proves connect succeeds without a cluster.
    let fake = std::sync::Arc::new(k10s_backend::FakePortForwardSeam::new());
    let connector = k10s_backend::PortForwardConnector::new(fake);
    let resolved = ResolvedPortForward {
        context: CONTEXT.into(),
        namespace: NS.into(),
        service_uid: SERVICE_UID.into(),
        service_port: 80,
        pod_name: "web-1".into(),
        pod_uid: "uid-pod".into(),
        pod_port: 8_080,
    };
    let stream = connector
        .connect(&resolved)
        .await
        .expect("fake streams open");
    // The stream is an opaque boxed transport; it never serializes.
    assert_eq!(
        format!("{stream:?}"),
        "PortForwardStream",
        "only the opaque handle leaves the backend"
    );
}
