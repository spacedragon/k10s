use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{LOGS_PATH, StreamServerMessage};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
const POD: &str = "/api/v1/namespaces/default/pods/web";
const LOG: &str = "/api/v1/namespaces/default/pods/web/log";

#[tokio::test]
async fn real_kubernetes_logs_flow_only_over_the_authenticated_dedicated_socket() {
    let recorded = RecordedApiServer::standard();
    recorded.set_method_response("GET", POD, 200, r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"7"},"spec":{"containers":[{"name":"app","image":"busybox"}]}}"#);
    recorded.set_method_response("GET", LOG, 200, "2026-08-25T00:00:00Z hello\nnext\n");
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
        BackendKernel::new_with_instance_id(adapter, "log-loopback"),
    )
    .await
    .unwrap();

    let mut control = connect(&server, k10s_protocol::CONTROL_PATH).await;
    control.send(Message::Text(json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}}).to_string().into())).await.unwrap();
    assert_eq!(receive_text(&mut control).await["kind"], "welcome");
    control.send(Message::Text(json!({
        "kind":"request", "requestId":"logs",
        "payload":{"kind":k10s_protocol::REQUEST_STREAM_TICKET,"payload":{
            "target":{"context":"recorded","namespace":"default","pod":"web","uid":"uid-web","container":"app"},
            "streamType":"logs","tty":false,"tailLines":10,"sinceSeconds":30,"timestamps":true,"follow":true
        }}
    }).to_string().into())).await.unwrap();
    let response = receive_text(&mut control).await;
    assert_eq!(response["kind"], "response", "{response:?}");
    let ticket = response["payload"]["ticketId"].as_str().unwrap();

    let mut logs = connect(&server, LOGS_PATH).await;
    logs.send(Message::Text(
        json!({"kind":"hello","protocolMajor":1,"accessToken":"secret","streamTicket":ticket})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let ready: StreamServerMessage = serde_json::from_value(receive_text(&mut logs).await).unwrap();
    assert!(matches!(ready, StreamServerMessage::Ready { container, .. } if container == "app"));
    let message = receive(&mut logs).await;
    let Message::Binary(payload) = message else {
        panic!("binary log payload")
    };
    let decoded = k10s_protocol::decode_stream_payload(&payload).unwrap();
    assert_eq!(decoded.kind, k10s_protocol::payload_kind::STDOUT);
    assert_eq!(
        String::from_utf8(decoded.data.to_vec()).unwrap(),
        "2026-08-25T00:00:00Z hello\n"
    );
    assert!(
        recorded
            .request_uris(LOG)
            .iter()
            .any(|uri| uri.contains("tailLines=10")
                && uri.contains("sinceSeconds=30")
                && uri.contains("timestamps=true")
                && uri.contains("follow=true"))
    );
    server.shutdown().await.unwrap();
}

async fn connect(server: &k10s_server::ServerHandle, path: &str) -> Ws {
    connect_async(format!("ws://{}{}", server.addr(), path))
        .await
        .unwrap()
        .0
}

async fn receive(ws: &mut Ws) -> Message {
    tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn receive_text(ws: &mut Ws) -> Value {
    let Message::Text(text) = receive(ws).await else {
        panic!("text")
    };
    serde_json::from_str(&text).unwrap()
}
