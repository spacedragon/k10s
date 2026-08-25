use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{
    OperationStatus, OperationStatusRequest, OperationStatusResponse, OperationUpdate, ServerFrame,
    ServerKind,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(server: &k10s_server::ServerHandle) -> Ws {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive(&mut ws).await.kind, ServerKind::Welcome);
    ws
}

async fn receive(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

#[tokio::test]
async fn real_engine_publishes_p0_updates_and_answers_status_after_reconnect() {
    let recorded = RecordedApiServer::standard();
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "recorded".into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("recorded", recorded.into_client("default"))],
    )
    .unwrap();
    let operations = adapter.operation_engine();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "operation-recovery"),
    )
    .await
    .unwrap();
    let mut first = connect(&server).await;

    let accepted = operations.accept("key", "scale/default/web/3").unwrap();
    let id = accepted.operation_id().as_str().to_owned();
    let frame = receive(&mut first).await;
    assert_eq!(frame.kind, ServerKind::OperationUpdate);
    let pending: OperationUpdate = serde_json::from_value(frame.payload).unwrap();
    assert_eq!(pending.operation_id.as_str(), id);
    assert_eq!(pending.status, OperationStatus::Pending);
    operations.running(&id, Some((1, 2))).unwrap();
    let running: OperationUpdate =
        serde_json::from_value(receive(&mut first).await.payload).unwrap();
    assert_eq!(running.status, OperationStatus::Running);

    first.close(None).await.unwrap();
    let mut reconnected = connect(&server).await;
    // A late subscription immediately publishes the retained nonterminal cut.
    let snapshot: OperationUpdate =
        serde_json::from_value(receive(&mut reconnected).await.payload).unwrap();
    assert_eq!(snapshot.operation_id.as_str(), id);
    assert_eq!(snapshot.status, OperationStatus::Running);

    reconnected
        .send(Message::Text(
            json!({
                "kind":"request","requestId":"status-after-reconnect",
                "payload":{"kind":"operation.status","payload":serde_json::to_value(OperationStatusRequest {
                    operation_ids: vec![k10s_protocol::OperationId::new(id.clone())]
                }).unwrap()}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let response = loop {
        let frame = receive(&mut reconnected).await;
        if frame.kind == ServerKind::Response {
            break frame;
        }
    };
    let status: OperationStatusResponse = response.decode_response_payload().unwrap();
    assert_eq!(status.operations[0].status, OperationStatus::Running);

    operations.outcome_unknown(&id).unwrap();
    let unknown: OperationUpdate =
        serde_json::from_value(receive(&mut reconnected).await.payload).unwrap();
    assert_eq!(unknown.status, OperationStatus::OutcomeUnknown);
    server.shutdown().await.unwrap();
}
