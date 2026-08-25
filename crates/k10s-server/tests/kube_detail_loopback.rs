//! Kubernetes detail loopback: the real kube-rs adapter, driven by a
//! recorded tower-level API server (no cluster), serves exact-identity
//! resource details with backend-resolved related rows, normalized events,
//! UID/resourceVersion-bound YAML, and typed not-found semantics over a live
//! control socket — the same protocol shapes the fake adapter produces.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{GroupVersionKind, ResourceDetailResponse, ServerFrame, ServerKind};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CONTEXT: &str = "detail-loopback";

fn deployments_gvk() -> GroupVersionKind {
    GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    }
}

/// The recorded cut: a deployment, its replicaset and pod (controller-UID
/// chained), and one core/v1 event.
fn recorded_server() -> RecordedApiServer {
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        &json!({
            "kind": "Deployment",
            "apiVersion": "apps/v1",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "uid-kube-web",
                "resourceVersion": "41",
                "creationTimestamp": "2026-08-21T00:00:00Z",
                "labels": {"app": "web"},
            },
            "spec": {"replicas": 2},
            "status": {"readyReplicas": 2},
        })
        .to_string(),
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
                    "name": "web-rs",
                    "namespace": "default",
                    "uid": "uid-kube-rs",
                    "creationTimestamp": "2026-08-21T00:01:00Z",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web",
                        "uid": "uid-kube-web",
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
            "items": [{
                "metadata": {
                    "name": "web-rs-aaaa1",
                    "namespace": "default",
                    "uid": "uid-kube-pod",
                    "creationTimestamp": "2026-08-21T00:02:00Z",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "web-rs",
                        "uid": "uid-kube-rs",
                        "controller": true,
                    }],
                },
                "status": {"phase": "Running"},
            }]
        })
        .to_string(),
    );
    server.set_response(
        "/api/v1/namespaces/default/events",
        200,
        &json!({
            "kind": "EventList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "44"},
            "items": [{
                "metadata": {"name": "ev.1", "namespace": "default", "uid": "uid-ev"},
                "involvedObject": {"kind": "Deployment", "name": "web", "namespace": "default", "uid": "uid-kube-web"},
                "reason": "ScalingReplicaSet",
                "message": "Scaled up replica set web-rs to 2",
                "count": 1,
                "lastTimestamp": "2026-08-21T00:01:00Z",
            }]
        })
        .to_string(),
    );
    server
}

async fn spawn_server() -> (k10s_server::ServerHandle, RecordedApiServer) {
    let server = recorded_server();
    let client = server.clone().into_client("default");
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds around the recorded server");
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "kube-detail-server"),
    )
    .await
    .unwrap();
    (handle, server)
}

async fn connect_authenticated(server: &k10s_server::ServerHandle) -> Ws {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"hello",
            "payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Welcome);
    ws
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn request_detail(ws: &mut Ws, request_id: &str, uid: &str) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {"kind": "resource.detail", "payload": {
                "identity": {
                    "context": CONTEXT,
                    "gvk": deployments_gvk(),
                    "namespace": "default",
                    "name": "web",
                    "uid": uid,
                }
            }}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn kube_detail_serves_traversal_events_and_yaml_over_the_socket() {
    let (server, _recorded) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    request_detail(&mut ws, "detail-kube", "uid-kube-web").await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), "detail-kube");
    let detail: ResourceDetailResponse = frame.decode_response_payload().unwrap();

    // Identity header echoes the exact identity.
    assert_eq!(detail.identity.name, "web");
    assert_eq!(detail.identity.uid, "uid-kube-web");
    assert_eq!(detail.identity.gvk, deployments_gvk());
    assert_eq!(detail.created_at, "2026-08-21T00:00:00Z");
    assert!(detail.capabilities.can_scale);

    // Tailored status from the recorded object.
    let status = detail.sections[0]
        .rows
        .iter()
        .find(|row| row.label == "Status")
        .map(|row| row.value.clone())
        .unwrap_or_default();
    assert_eq!(status, "2/2 ready");

    // Controller-UID traversal resolves the replicaset and its pod.
    assert!(
        detail
            .related
            .iter()
            .any(|group| group.gvk.kind == "ReplicaSet"
                && group.rows.iter().any(|row| row.identity.name == "web-rs"))
    );
    assert!(detail.related.iter().any(|group| {
        group.gvk.kind == "Pod"
            && group
                .rows
                .iter()
                .any(|row| row.identity.name == "web-rs-aaaa1")
    }));

    // Normalized events ride on the same response.
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.reason == "ScalingReplicaSet")
    );

    // YAML is rendered by the backend and bound to the UID.
    assert!(detail.manifest.contains("uid-kube-web"));
    assert!(detail.manifest.contains("resourceVersion"));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn kube_detail_rejects_stale_uid_as_not_found() {
    let (server, _recorded) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    request_detail(&mut ws, "detail-stale", "uid-from-a-past-life").await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Error);
    assert_eq!(frame.payload["code"], json!("notFound"));

    server.shutdown().await.unwrap();
}

/// A Service detail served by the real adapter over the socket carries the
/// structured projection with defaulted target ports and traffic policies.
#[tokio::test]
async fn kube_service_detail_carries_projection_over_the_socket() {
    let (handle, recorded) = spawn_server().await;
    recorded.set_response(
        "/api/v1/namespaces/default/services/web",
        200,
        &json!({
            "kind": "Service",
            "apiVersion": "v1",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "uid-kube-svc-web",
                "resourceVersion": "44",
                "creationTimestamp": "2026-08-21T00:00:00Z",
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.0.10",
                "clusterIPs": ["10.96.0.10"],
                "selector": {"app": "web"},
                "ports": [
                    {"name": "http", "port": 80, "protocol": "TCP"},
                    {"name": "metrics", "port": 9100, "targetPort": 9100, "protocol": "UDP"},
                ],
            },
        })
        .to_string(),
    );
    let mut ws = connect_authenticated(&handle).await;
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": "svc-detail-1",
            "payload": {"kind": "resource.detail", "payload": {
                "identity": {
                    "context": CONTEXT,
                    "gvk": {"group": "", "version": "v1", "kind": "Service"},
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-kube-svc-web",
                }
            }}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response);
    let detail: ResourceDetailResponse = frame.decode_response_payload().unwrap();
    assert_eq!(detail.identity.name, "web");
    let Some(k10s_protocol::ResourceProjection::Service(projection)) = &detail.projection else {
        panic!("service detail carries a Service projection")
    };
    assert_eq!(projection.service_type, "ClusterIP");
    assert_eq!(projection.cluster_ips, ["10.96.0.10"]);
    assert_eq!(
        projection.selector.get("app").map(String::as_str),
        Some("web")
    );
    assert_eq!(projection.ports.len(), 2);
    assert_eq!(
        projection.ports[0].target_port,
        k10s_protocol::TargetPort::Number { number: 80 },
        "omitted targetPort normalizes to the Service port over the wire"
    );
    assert_eq!(
        projection.ports[1].protocol,
        k10s_protocol::TransportProtocol::Udp
    );

    handle.shutdown().await.unwrap();
}
