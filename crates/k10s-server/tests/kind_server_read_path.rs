//! Ignored real-control-socket smoke over the ephemeral kind cluster.

use std::path::PathBuf;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, KubeAdapter};
use k10s_protocol::{
    BootstrapResponse, ContextPermissionsRequest, ContextPermissionsResponse, GroupVersionKind,
    MetricsAvailability, PermissionOutcome, PermissionProbe, ResourceDetailResponse,
    ResourceListRequest, ResourceListResponse, ResourceMetricsResponse, ResourceRefRequest,
    ResourceTypesRequest, ResourceTypesResponse, ServerFrame, ServerKind,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn kubeconfig() -> PathBuf {
    std::env::var_os("K10S_KIND_KUBECONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/.kubeconfig")
        })
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(30), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket remains open")
        .expect("socket remains healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn send_request(ws: &mut Ws, request_id: &str, kind: &str, payload: Value) -> ServerFrame {
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
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame
}

#[tokio::test]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn standalone_control_socket_serves_live_kind_data_not_fake_fixtures() {
    let adapter = KubeAdapter::from_kubeconfig(Some(&kubeconfig())).unwrap();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "kind-secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "kind-server"),
    )
    .await
    .unwrap();

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
            "payload":{
                "protocolMajor":1,
                "protocolMinor":1,
                "capabilities":[],
                "accessToken":"kind-secret"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Welcome);

    let bootstrap = send_request(&mut ws, "bootstrap", "bootstrap", json!({})).await;
    let bootstrap: BootstrapResponse = bootstrap.decode_response_payload().unwrap();
    assert!(
        bootstrap
            .contexts
            .iter()
            .any(|context| context.name == "k10s-limited")
    );
    assert!(
        bootstrap
            .contexts
            .iter()
            .all(|context| context.name != "dev-local")
    );

    let types = send_request(
        &mut ws,
        "types",
        "resource.types",
        serde_json::to_value(ResourceTypesRequest {
            context: "k10s-limited".into(),
        })
        .unwrap(),
    )
    .await;
    let types: ResourceTypesResponse = types.decode_response_payload().unwrap();
    assert!(types.types.iter().any(|entry| entry.gvk.kind == "Widget"));

    let list = send_request(
        &mut ws,
        "pods",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "k10s-limited".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("k10s-read".into()),
        })
        .unwrap(),
    )
    .await;
    let list: ResourceListResponse = list.decode_response_payload().unwrap();
    assert_eq!(list.rows.len(), 2);
    assert!(
        list.rows
            .iter()
            .all(|row| row.identity.context == "k10s-limited")
    );

    let metrics = send_request(
        &mut ws,
        "metrics",
        "resource.metrics",
        serde_json::to_value(ResourceRefRequest {
            identity: list.rows[0].identity.clone(),
        })
        .unwrap(),
    )
    .await;
    let metrics: ResourceMetricsResponse = metrics.decode_response_payload().unwrap();
    assert_eq!(
        metrics.metrics.availability,
        MetricsAvailability::Unavailable
    );
    assert!(metrics.metrics.cpu_millicores.is_none());
    assert!(metrics.metrics.memory_bytes.is_none());

    let deployments = send_request(
        &mut ws,
        "deployments",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "k10s-limited".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("k10s-read".into()),
        })
        .unwrap(),
    )
    .await;
    let deployments: ResourceListResponse = deployments.decode_response_payload().unwrap();
    let deployment = deployments
        .rows
        .iter()
        .find(|row| row.identity.name == "read-path-web")
        .unwrap()
        .identity
        .clone();
    let detail = send_request(
        &mut ws,
        "detail",
        "resource.detail",
        serde_json::to_value(ResourceRefRequest {
            identity: deployment,
        })
        .unwrap(),
    )
    .await;
    let detail: ResourceDetailResponse = detail.decode_response_payload().unwrap();
    assert!(detail.manifest.contains("read-path-web"));
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.reason == "FixtureReady")
    );
    assert!(detail.related.iter().any(|group| group.gvk.kind == "Pod"));

    let permissions = send_request(
        &mut ws,
        "permissions",
        "context.permissions",
        serde_json::to_value(ContextPermissionsRequest {
            context: "k10s-limited".into(),
            probes: vec![
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-read".into()),
                },
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-forbidden".into()),
                },
            ],
        })
        .unwrap(),
    )
    .await;
    let permissions: ContextPermissionsResponse = permissions.decode_response_payload().unwrap();
    assert_eq!(permissions.checks[0].outcome, PermissionOutcome::Allowed);
    assert_eq!(permissions.checks[1].outcome, PermissionOutcome::Denied);

    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": "forbidden",
            "payload": {
                "kind": "resource.list",
                "payload": {
                    "context": "k10s-limited",
                    "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                    "namespace": "k10s-forbidden"
                }
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Error);

    server.shutdown().await.unwrap();
}
