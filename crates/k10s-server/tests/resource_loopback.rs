//! End-to-end resource contract: normalized list, detail, metrics, snapshot
//! streaming, monotonic revisions, and resource-gone deltas over a real
//! control socket with the deterministic fake adapter.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    GroupVersionKind, ResourceChanged, ResourceDetailResponse, ResourceGone, ResourceIdentity,
    ResourceListRequest, ResourceListResponse, ResourceRefRequest, ResourceSnapshotPage,
    ServerFrame, ServerKind, ServerPayload, SnapshotBegin, SnapshotChunk, SnapshotEnd,
    WorkloadKind,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn deployments() -> GroupVersionKind {
    GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    }
}

fn replicasets() -> GroupVersionKind {
    GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "ReplicaSet".into(),
    }
}

fn backend_gvk(gvk: &GroupVersionKind) -> k10s_backend::Gvk {
    k10s_backend::Gvk {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
    }
}

async fn spawn_server_with_fake() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "resource-server"),
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

async fn receive_response(ws: &mut Ws, request_id: &str) -> ServerFrame {
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame
}

#[tokio::test]
async fn resource_list_detail_and_metrics_flow_through_the_real_socket() {
    let (server, _fake) = spawn_server_with_fake().await;
    let mut ws = connect_authenticated(&server).await;

    // Normalized list: deployments on dev-local.
    send_request(
        &mut ws,
        "list-1",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "dev-local".into(),
            gvk: deployments(),
            namespace: Some("default".into()),
        })
        .unwrap(),
    )
    .await;
    let response = receive_response(&mut ws, "list-1").await;
    let list: ResourceListResponse = response.decode_response_payload().unwrap();
    let names: Vec<_> = list
        .rows
        .iter()
        .map(|row| row.identity.name.as_str())
        .collect();
    assert_eq!(names, vec!["api-server", "web-frontend"]);
    assert!(list.revision.get() > 0);
    assert!(list.capabilities.can_scale, "deployments are scalable");
    assert!(!list.capabilities.can_view_logs);
    assert!(
        list.rows
            .iter()
            .all(|row| row.identity.gvk == deployments())
    );
    assert_eq!(list.rows[0].identity.scope().to_string(), "namespaced");
    assert!(!list.generated_at.is_empty());

    // Detail: replicaset carries a controller owner reference to its
    // deployment plus kind-specific sections.
    send_request(
        &mut ws,
        "detail-1",
        "resource.detail",
        serde_json::to_value(ResourceRefRequest {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: replicasets(),
                namespace: Some("default".into()),
                name: "web-frontend-7d9f8".into(),
                uid: "whatever".into(),
            },
        })
        .unwrap(),
    )
    .await;
    let response = receive_response(&mut ws, "detail-1").await;
    let detail: ResourceDetailResponse = response.decode_response_payload().unwrap();
    let owner = detail
        .owner_references
        .iter()
        .find(|owner| owner.name == "web-frontend")
        .expect("replicaset owned by deployment");
    assert!(owner.controller);
    assert_eq!(owner.gvk.kind, "Deployment");
    assert_eq!(detail.identity.name, "web-frontend-7d9f8");
    assert!(!detail.sections.is_empty());
    assert!(
        !detail.capabilities.can_scale,
        "replicasets are not directly scalable"
    );

    // Metrics tri-state across pods of the same namespace.
    for (name, availability, cpu, memory) in [
        ("web-frontend-7d9f8-00001", "available", true, true),
        ("api-server-5cc4d-qw8rt", "partial", true, false),
        ("db-postgres-0", "unavailable", false, false),
    ] {
        send_request(
            &mut ws,
            &format!("metrics-{name}"),
            "resource.metrics",
            serde_json::to_value(ResourceRefRequest {
                identity: ResourceIdentity {
                    context: "dev-local".into(),
                    gvk: GroupVersionKind::core("v1", "Pod"),
                    namespace: Some("default".into()),
                    name: name.into(),
                    uid: "whatever".into(),
                },
            })
            .unwrap(),
        )
        .await;
        let response = receive_response(&mut ws, &format!("metrics-{name}")).await;
        assert_eq!(
            response.payload["metrics"]["availability"],
            json!(availability)
        );
        assert_eq!(response.payload["metrics"]["cpuMillicores"].is_u64(), cpu);
        assert_eq!(response.payload["metrics"]["memoryBytes"].is_u64(), memory);
        assert_eq!(response.payload["identity"]["name"], json!(name));
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_watch_streams_chunked_snapshot_then_deltas() {
    let (server, fake) = spawn_server_with_fake().await;
    let mut ws = connect_authenticated(&server).await;

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"res-1",
            "payload":{
                "kind":"resource",
                "context":"dev-local",
                "gvk":{"group":"","version":"v1","kind":"Pod"},
                "namespace":"default"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let subscribed = receive_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);
    assert_eq!(
        subscribed.subscription_id.as_ref().map(|id| id.as_str()),
        Some("res-1")
    );
    let subscribed_sequence = subscribed.sequence.unwrap();

    let begin = receive_frame(&mut ws).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    assert_eq!(
        begin.subscription_id.as_ref().map(|id| id.as_str()),
        Some("res-1")
    );
    let begin_payload: SnapshotBegin = serde_json::from_value(begin.payload).unwrap();
    assert_eq!(
        begin_payload.total_chunks, 2,
        "22 pods chunk into two bounded pages"
    );

    let mut rows = Vec::new();
    let mut snapshot_revision = 0_u64;
    for expected_chunk in 0..begin_payload.total_chunks {
        let chunk = receive_frame(&mut ws).await;
        assert_eq!(chunk.kind, ServerKind::SnapshotChunk);
        let chunk_payload: SnapshotChunk = serde_json::from_value(chunk.payload).unwrap();
        assert_eq!(chunk_payload.chunk_index, expected_chunk);
        let page: ResourceSnapshotPage = serde_json::from_value(chunk_payload.data).unwrap();
        snapshot_revision = snapshot_revision.max(page.revision.get());
        rows.extend(page.rows);
    }

    let end = receive_frame(&mut ws).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);
    let end_sequence = end.sequence.unwrap();
    let end_payload: SnapshotEnd = serde_json::from_value(end.payload).unwrap();
    assert!(!end_payload.checksum.is_empty());

    assert_eq!(rows.len(), 22, "fake dev-local serves every default pod");
    assert!(
        rows.windows(2).all(|pair| pair[0] <= pair[1]),
        "snapshot rows arrive sorted"
    );
    assert_eq!(rows[0].identity.gvk.kind, "Pod");
    assert_eq!(
        WorkloadKind::from_gvk(&rows[0].identity.gvk),
        Some(WorkloadKind::Pod)
    );
    for frame_sequence in [subscribed_sequence, begin.sequence.unwrap(), end_sequence] {
        assert!(frame_sequence >= subscribed_sequence);
    }

    // Mutate behind the adapter: delete one pod -> resource.gone delta.
    assert!(fake.delete_resource(
        "dev-local",
        &backend_gvk(&GroupVersionKind::core("v1", "Pod")),
        Some("default"),
        "web-frontend-7d9f8-00003",
    ));
    let gone = receive_frame(&mut ws).await;
    assert_eq!(gone.kind, ServerKind::Event);
    let gone_sequence = gone.sequence.unwrap();
    assert!(gone_sequence > end_sequence, "monotonic sequence");
    let event = match gone.decode_payload().unwrap() {
        ServerPayload::Event(event) => event,
        other => panic!("expected event, got {other:?}"),
    };
    assert_eq!(event.event_kind, "resource.gone");
    let gone_payload: ResourceGone = serde_json::from_value(event.payload).unwrap();
    assert_eq!(gone_payload.identity.name, "web-frontend-7d9f8-00003");
    assert!(gone_payload.revision.get() > snapshot_revision);

    // Touch a watched pod -> resource.changed delta carrying the full row.
    let touched_revision = fake.touch_resource(
        "dev-local",
        &backend_gvk(&GroupVersionKind::core("v1", "Pod")),
        Some("default"),
        "web-frontend-7d9f8-00001",
    );
    assert!(touched_revision.is_some());
    let touched_revision = touched_revision.unwrap();
    let changed = receive_frame(&mut ws).await;
    assert_eq!(changed.kind, ServerKind::Event);
    let event = match changed.decode_payload().unwrap() {
        ServerPayload::Event(event) => event,
        other => panic!("expected event, got {other:?}"),
    };
    assert_eq!(event.event_kind, "resource.changed");
    assert_eq!(
        event.revision.as_deref(),
        Some(touched_revision.to_string()).as_deref()
    );
    let changed_payload: ResourceChanged = serde_json::from_value(event.payload).unwrap();
    assert_eq!(changed_payload.identity.name, "web-frontend-7d9f8-00001");
    assert_eq!(
        changed_payload.row.revision,
        k10s_protocol::BackendRevision::new(touched_revision)
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_subscription_kinds_stay_rejected_over_the_socket() {
    let (server, _fake) = spawn_server_with_fake().await;
    let mut ws = connect_authenticated(&server).await;
    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"res-x",
            "payload":{"kind":"galaxyStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.kind, ServerKind::Error);
    assert_eq!(error.subscription_id.unwrap().as_str(), "res-x");
    server.shutdown().await.unwrap();
}
