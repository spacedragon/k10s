use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{ServerFrame, ServerKind, YamlOutcome, YamlValidateRequest, buffer_hash};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const PATH: &str = "/apis/apps/v1/namespaces/default/deployments/web";
const OBJECT: &str = r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"42"},"spec":{"replicas":2}}"#;
const YAML: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: default\n  uid: uid-web\n  resourceVersion: '42'\nspec:\n  replicas: 3\n";

#[tokio::test]
async fn real_adapter_validation_round_trips_over_the_authenticated_control_socket() {
    let recorded = RecordedApiServer::standard();
    recorded.set_method_response("GET", PATH, 200, OBJECT);
    recorded.set_method_response("PATCH", PATH, 200, OBJECT);
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "recorded".into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [("recorded", recorded.clone().into_client("default"))],
    )
    .unwrap();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "yaml-loopback"),
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
    ws.send(Message::Text(json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}}).to_string().into())).await.unwrap();
    assert_eq!(receive(&mut ws).await.kind, ServerKind::Welcome);
    ws.send(Message::Text(json!({
        "kind":"request","requestId":"validate-real",
        "payload":{"kind":"yaml.validate","payload":serde_json::to_value(YamlValidateRequest { context:"recorded".into(), yaml:YAML.into() }).unwrap()}
    }).to_string().into())).await.unwrap();
    let frame = receive(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    let outcome: YamlOutcome = frame.decode_response_payload().unwrap();
    let YamlOutcome::Valid { ticket } = outcome else {
        panic!("expected valid ticket")
    };
    assert_eq!(ticket.buffer_hash, buffer_hash(YAML));
    assert_eq!(recorded.hit_count(PATH), 2);
    server.shutdown().await.unwrap();
}

async fn receive<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> ServerFrame
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}
