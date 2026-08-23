//! End-to-end detail contract: kind-specific detail payloads with
//! backend-resolved events, Deployment → ReplicaSet → Pod controller-UID
//! traversal, stale-UID rejection, and the client-state seam over a real
//! control socket with the deterministic fake adapter.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    GroupVersionKind, ResourceDetailResponse, ResourceIdentity, ResourceListRequest,
    ResourceListResponse, ResourceRefRequest, ServerFrame, ServerKind,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn gvk(kind: &str) -> GroupVersionKind {
    match kind {
        "Pod" => GroupVersionKind::core("v1", "Pod"),
        other => GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: other.into(),
        },
    }
}

/// The deterministic fake UID for one dev-local object.
fn uid(kind: &str, name: &str) -> String {
    format!("uid-dev-local-{}-default-{name}", kind.to_lowercase())
}

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "detail-server"),
    )
    .await
    .unwrap();
    (handle, fake)
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

async fn receive_detail(ws: &mut Ws, request_id: &str) -> ResourceDetailResponse {
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame.decode_response_payload().unwrap()
}

async fn request_detail(ws: &mut Ws, request_id: &str, identity: ResourceIdentity) {
    send_request(
        ws,
        request_id,
        "resource.detail",
        serde_json::to_value(ResourceRefRequest { identity }).unwrap(),
    )
    .await;
}

#[tokio::test]
async fn deployment_detail_traverses_replicasets_and_pods_by_controller_uid() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // Fetch the advertised deployment identity from a normalized list so
    // the UID is authoritative rather than assumed.
    send_request(
        &mut ws,
        "list",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "dev-local".into(),
            gvk: gvk("Deployment"),
            namespace: Some("default".into()),
        })
        .unwrap(),
    )
    .await;
    let frame = receive_frame(&mut ws).await;
    let list: ResourceListResponse = frame.decode_response_payload().unwrap();
    let web_frontend = list
        .rows
        .iter()
        .find(|row| row.identity.name == "web-frontend")
        .expect("web-frontend deployment exists")
        .identity
        .clone();

    request_detail(&mut ws, "detail-deploy", web_frontend).await;
    let detail = receive_detail(&mut ws, "detail-deploy").await;

    // Identity header fields are echoed exactly.
    assert_eq!(detail.identity.name, "web-frontend");
    assert_eq!(detail.identity.gvk.kind, "Deployment");
    assert_eq!(detail.identity.namespace.as_deref(), Some("default"));
    assert!(!detail.created_at.is_empty());
    assert!(detail.capabilities.can_scale);

    // Overview sections carry the identity header rows.
    let overview = &detail.sections[0];
    assert_eq!(overview.title, "Overview");
    assert!(
        overview
            .rows
            .iter()
            .any(|row| row.label == "Name" && row.value == "web-frontend")
    );

    // Controller-UID traversal resolves the replicaset AND its pods.
    let rs_group = detail
        .related
        .iter()
        .find(|group| group.gvk.kind == "ReplicaSet")
        .expect("deployment relates to its replicaset");
    assert_eq!(rs_group.title, "ReplicaSets");
    assert_eq!(
        rs_group
            .rows
            .iter()
            .map(|row| row.identity.name.as_str())
            .collect::<Vec<_>>(),
        vec!["web-frontend-7d9f8"]
    );
    let pod_group = detail
        .related
        .iter()
        .find(|group| group.gvk.kind == "Pod")
        .expect("deployment traversal reaches pods transitively");
    assert_eq!(pod_group.rows.len(), 20, "all twenty replica pods resolve");
    assert!(
        pod_group
            .rows
            .iter()
            .all(|row| row.identity.uid == uid("Pod", row.identity.name.as_str()))
    );

    // Backend-resolved events arrive on the same response.
    assert!(!detail.events.is_empty());
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.reason == "Started" && event.count >= 1)
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn replicaset_detail_resolves_its_pods_without_intermediate_layers() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    request_detail(
        &mut ws,
        "detail-rs",
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: gvk("ReplicaSet"),
            namespace: Some("default".into()),
            name: "web-frontend-7d9f8".into(),
            uid: uid("ReplicaSet", "web-frontend-7d9f8"),
        },
    )
    .await;
    let detail = receive_detail(&mut ws, "detail-rs").await;

    assert!(
        !detail.capabilities.can_scale,
        "replicasets are not directly scalable"
    );
    let groups: Vec<&str> = detail
        .related
        .iter()
        .map(|group| group.gvk.kind.as_str())
        .collect();
    assert_eq!(groups, vec!["Pod"], "a replicaset has no deeper layer");
    assert_eq!(detail.related[0].rows.len(), 20);
}

#[tokio::test]
async fn pod_detail_carries_events_and_runtime_capabilities() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    request_detail(
        &mut ws,
        "detail-pod",
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: gvk("Pod"),
            namespace: Some("default".into()),
            name: "db-postgres-0".into(),
            uid: uid("Pod", "db-postgres-0"),
        },
    )
    .await;
    let detail = receive_detail(&mut ws, "detail-pod").await;

    assert!(detail.capabilities.can_view_logs);
    assert!(detail.capabilities.can_exec);
    assert!(
        !detail.capabilities.can_scale,
        "pods are never scaled directly"
    );
    assert!(detail.related.is_empty(), "pods own nothing");
    assert!(
        detail.events.iter().any(|event| event.reason == "Started"),
        "{:?}",
        detail.events
    );
}

#[tokio::test]
async fn stale_uid_detail_is_rejected_as_not_found() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    request_detail(
        &mut ws,
        "detail-stale",
        ResourceIdentity {
            context: "dev-local".into(),
            gvk: gvk("ReplicaSet"),
            namespace: Some("default".into()),
            name: "web-frontend-7d9f8".into(),
            uid: "uid-from-a-past-life".into(),
        },
    )
    .await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Error);
    assert_eq!(frame.payload["code"], json!("notFound"));

    server.shutdown().await.unwrap();
}

/// Prove the shared UI client state can drive the same query end to end.
#[tokio::test]
async fn client_state_seam_resolves_the_full_detail_payload() {
    use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget, Query, QueryResult};

    let (server, _fake) = spawn_server().await;
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let (mut socket, _) = connect_async(&url).await.unwrap();
    let mut client = ClientState::new(ClientConfig::default());

    client
        .connect(ConnectTarget::new(url.clone(), "secret"))
        .unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&client.take_outbound().unwrap())
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    client.apply(receive_frame(&mut socket).await).unwrap();
    assert_eq!(client.phase(), k10s_ui::client::ClientPhase::Ready);

    let request = client
        .begin(Query::ResourceDetail(ResourceIdentity {
            context: "dev-local".into(),
            gvk: gvk("Deployment"),
            namespace: Some("default".into()),
            name: "web-frontend".into(),
            uid: uid("Deployment", "web-frontend"),
        }))
        .unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&client.take_outbound().unwrap())
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();

    let frame = receive_frame(&mut socket).await;
    client.apply(frame).unwrap();
    match client.take(request).expect("detail response completes") {
        QueryResult::ResourceDetail(detail) => {
            assert_eq!(detail.identity.name, "web-frontend");
            assert!(
                detail.related.iter().any(|group| group.gvk.kind == "Pod"),
                "the client receives backend-resolved related rows"
            );
        }
        other => panic!("expected detail result, got {other:?}"),
    }

    drop(client);
    server.shutdown().await.unwrap();
}
