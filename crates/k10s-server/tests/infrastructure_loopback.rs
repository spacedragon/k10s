//! Infrastructure data must traverse fake adapter -> backend port/kernel ->
//! real control socket -> shared client state. Metrics updates use the same
//! bounded, context-coalesced P2 scheduler as resource deltas.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes, FakeMetricsScenario};
use k10s_protocol::{
    ClientFrame, ClientKind, ClientPayload, InfrastructureRequest, InfrastructureResponse,
    MetricsAvailability, MetricsCondition, ServerFrame, ServerKind,
};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget, Query, QueryResult};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn send_client_frame(ws: &mut Ws, frame: ClientFrame) {
    ws.send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
        .unwrap();
}

async fn connected_client(server: &k10s_server::ServerHandle) -> (Ws, ClientState) {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH),
            "secret",
        ))
        .unwrap();
    send_client_frame(&mut ws, client.take_outbound().unwrap()).await;
    client.apply(receive_frame(&mut ws).await).unwrap();
    (ws, client)
}

#[tokio::test]
async fn infrastructure_query_round_trips_through_real_socket_and_client_response() {
    let fake = FakeKubernetes::standard();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake, "infrastructure-server"),
    )
    .await
    .unwrap();
    let (mut ws, mut client) = connected_client(&server).await;

    let request = client
        .begin(Query::Infrastructure(InfrastructureRequest {
            context: "dev-local".into(),
        }))
        .unwrap();
    let outbound = client.take_outbound().unwrap();
    let ClientPayload::Request(payload) = outbound.decode_payload().unwrap() else {
        panic!("expected request");
    };
    assert_eq!(payload.request_kind, "infrastructure.get");
    send_client_frame(&mut ws, outbound).await;
    client.apply(receive_frame(&mut ws).await).unwrap();

    let QueryResult::Infrastructure(response) = client.take(request).unwrap() else {
        panic!("expected infrastructure response");
    };
    assert_eq!(response.context, "dev-local");
    assert_eq!(response.totals.nodes, 2);
    assert_eq!(response.totals.pods, 22);
    assert_eq!(response.totals.workloads, 6);
    assert_eq!(response.nodes.len(), 2);
    assert!(response.nodes.iter().any(|node| node.status == "Not Ready"));
    assert_eq!(response.storage.persistent_volume_claims.len(), 1);
    assert_eq!(response.storage.persistent_volumes.len(), 1);
    assert_eq!(response.storage.storage_classes.len(), 1);
    assert_eq!(
        response.metrics.availability,
        MetricsAvailability::Available
    );
    assert!(response.cluster_cpu.used.is_some());
    assert!(response.cluster_memory.used.is_some());
    assert!(response.pod_capacity.used.is_some());
    assert!(!response.generated_at.is_empty());
    assert!(response.metrics.source_updated_at.is_some());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn fake_full_partial_forbidden_and_stale_cases_preserve_missing_values() {
    for (scenario, availability, condition) in [
        (
            FakeMetricsScenario::Full,
            MetricsAvailability::Available,
            MetricsCondition::Fresh,
        ),
        (
            FakeMetricsScenario::Partial,
            MetricsAvailability::Partial,
            MetricsCondition::Partial,
        ),
        (
            FakeMetricsScenario::Forbidden,
            MetricsAvailability::Unavailable,
            MetricsCondition::Forbidden,
        ),
        (
            FakeMetricsScenario::Stale,
            MetricsAvailability::Unavailable,
            MetricsCondition::Stale,
        ),
    ] {
        let fake = FakeKubernetes::with_metrics_scenario(scenario);
        let server = spawn_loopback(
            ServerConfig {
                access_token: "secret".into(),
                ..ServerConfig::default()
            },
            BackendKernel::new(fake),
        )
        .await
        .unwrap();
        let (mut ws, _client) = connected_client(&server).await;
        ws.send(Message::Text(
            serde_json::json!({
                "kind": "request",
                "requestId": "infra-case",
                "payload": {
                    "kind": "infrastructure.get",
                    "payload": {"context": "dev-local"}
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        let frame = receive_frame(&mut ws).await;
        let response: InfrastructureResponse = frame.decode_response_payload().unwrap();
        assert_eq!(response.metrics.availability, availability, "{scenario:?}");
        assert_eq!(response.metrics.condition, condition, "{scenario:?}");
        if scenario != FakeMetricsScenario::Full {
            assert!(
                response.cluster_memory.used.is_none()
                    || response.cluster_cpu.used.is_none()
                    || response.pod_capacity.used.is_none(),
                "non-full fake states must preserve at least one missing value"
            );
            let json = serde_json::to_value(&response).unwrap();
            assert_ne!(
                json.pointer("/clusterMemory/used"),
                Some(&serde_json::json!(0))
            );
            assert_ne!(
                json.pointer("/clusterCpu/used"),
                Some(&serde_json::json!(0))
            );
        }
        assert_eq!(response.metrics.source, "metrics.k8s.io");
        server.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn telemetry_updates_use_a_sequenced_subscription_and_reach_client_state() {
    let fake = FakeKubernetes::standard();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new(fake.clone()),
    )
    .await
    .unwrap();
    let (mut ws, mut client) = connected_client(&server).await;

    let subscription = client.subscribe_infrastructure("dev-local").unwrap();
    let subscribe = client.take_outbound().unwrap();
    assert_eq!(subscribe.kind, ClientKind::Subscribe);
    send_client_frame(&mut ws, subscribe).await;
    let subscribed = receive_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);
    assert!(subscribed.sequence.is_some());
    client.apply(subscribed).unwrap();
    send_client_frame(&mut ws, client.take_outbound().unwrap()).await;

    fake.set_metrics_scenario(FakeMetricsScenario::Partial);
    let updated = receive_frame(&mut ws).await;
    assert_eq!(updated.kind, ServerKind::Event);
    assert_eq!(updated.subscription_id.as_ref(), Some(subscription.id()));
    assert!(updated.sequence.is_some(), "P2 telemetry is sequenced");
    client.apply(updated).unwrap();
    let latest = client
        .infrastructure("dev-local")
        .expect("telemetry event updates shared client state");
    assert_eq!(latest.metrics.availability, MetricsAvailability::Partial);
    assert!(latest.cluster_memory.used.is_none());

    server.shutdown().await.unwrap();
}
