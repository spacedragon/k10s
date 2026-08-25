//! Discovery loopback: the real kube-rs adapter, backed by a recorded tower
//! Service, serves normalized resource-types payloads over a live control
//! socket. No cluster is dialed and no kube types reach the wire.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{ResourceTypesRequest, ResourceTypesResponse, ServerFrame, ServerKind};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn recorded_kube_kernel() -> (BackendKernel, RecordedApiServer) {
    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "loopback-cluster".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("loopback-cluster", client)],
    )
    .expect("adapter builds around the recorded server");
    let kernel = BackendKernel::new_with_instance_id(adapter, "discovery-server");
    (kernel, server)
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

async fn send_request(ws: &mut Ws, request_id: &str, kind: &str, payload: Value) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {"kind": kind, "payload": payload}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn receive_response(ws: &mut Ws, request_id: &str) -> ServerFrame {
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame
}

#[tokio::test]
async fn discovery_catalog_flows_over_the_real_control_socket() {
    let (kernel, _server) = recorded_kube_kernel();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        kernel,
    )
    .await
    .unwrap();
    let mut ws = connect_authenticated(&server).await;

    send_request(
        &mut ws,
        "types-1",
        "resource.types",
        serde_json::to_value(ResourceTypesRequest {
            context: "loopback-cluster".into(),
        })
        .unwrap(),
    )
    .await;
    let response = receive_response(&mut ws, "types-1").await;
    let types: ResourceTypesResponse = response.decode_response_payload().unwrap();

    assert_eq!(types.context, "loopback-cluster");
    assert!(!types.types.is_empty(), "recorded discovery is non-empty");

    // Normalized built-ins and the CRD arrive through one seam.
    let deployments = types
        .types
        .iter()
        .find(|entry| entry.gvk.kind == "Deployment")
        .expect("deployment listed");
    assert_eq!(deployments.gvk.group, "apps");
    assert!(deployments.namespaced);

    let nodes = types
        .types
        .iter()
        .find(|entry| entry.gvk.kind == "Node")
        .expect("node listed");
    assert!(!nodes.namespaced, "cluster-scoped on the wire");

    let gadgets = types
        .types
        .iter()
        .find(|entry| entry.gvk.kind == "Gadget")
        .expect("CRD listed");
    assert_eq!(gadgets.gvk.group, "k10s.example.com");

    // Raw discovery vocabulary must never cross the seam.
    let wire = serde_json::to_string(&types).unwrap();
    for marker in ["APIResourceList", "verbs", "subresources", "singularName"] {
        assert!(
            !wire.contains(marker),
            "raw discovery term leaked to the wire: {marker}"
        );
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_context_stays_a_typed_error_over_the_socket() {
    let (kernel, _server) = recorded_kube_kernel();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        kernel,
    )
    .await
    .unwrap();
    let mut ws = connect_authenticated(&server).await;

    send_request(
        &mut ws,
        "types-unknown",
        "resource.types",
        serde_json::to_value(ResourceTypesRequest {
            context: "missing-context".into(),
        })
        .unwrap(),
    )
    .await;
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.kind, ServerKind::Error);
    assert_eq!(
        error.request_id.as_ref().map(|id| id.as_str()),
        Some("types-unknown")
    );
    assert_eq!(error.payload["code"], json!("notFound"));

    server.shutdown().await.unwrap();
}
