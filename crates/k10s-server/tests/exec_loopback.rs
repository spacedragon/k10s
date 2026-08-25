use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, KubeAdapter};
use k10s_protocol::{EXEC_PATH, ErrorCode, StreamServerMessage};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
const POD: &str = "/api/v1/namespaces/default/pods/web";
const EXEC: &str = "/api/v1/namespaces/default/pods/web/exec";

#[tokio::test]
async fn real_exec_is_opened_only_after_authenticated_dedicated_socket_redeem() {
    let recorded = RecordedApiServer::standard();
    recorded.set_method_response("GET", POD, 200, r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"containers":[{"name":"app","image":"busybox"}]}}"#);
    recorded.set_method_response(
        "GET",
        EXEC,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#,
    );
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "recorded".into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("recorded", recorded.clone().into_client("default"))],
    )
    .unwrap();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "exec-loopback"),
    )
    .await
    .unwrap();

    let mut control = connect(&server, k10s_protocol::CONTROL_PATH).await;
    control.send(Message::Text(json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}}).to_string().into())).await.unwrap();
    assert_eq!(receive_text(&mut control).await["kind"], "welcome");
    control.send(Message::Text(json!({
        "kind":"request", "requestId":"exec",
        "payload":{"kind":k10s_protocol::REQUEST_STREAM_TICKET,"payload":{
            "target":{"context":"recorded","namespace":"default","pod":"web","uid":"uid-web","container":"app"},
            "streamType":"exec","tty":true,"command":["/bin/sh","-c","printf exact"]
        }}
    }).to_string().into())).await.unwrap();
    let response = receive_text(&mut control).await;
    assert_eq!(response["kind"], "response", "{response:?}");
    let ticket = response["payload"]["ticketId"].as_str().unwrap();
    assert_eq!(
        recorded.hit_count(EXEC),
        0,
        "ticket issuance must not execute anything"
    );

    let mut exec = connect(&server, EXEC_PATH).await;
    exec.send(Message::Text(
        json!({"kind":"hello","protocolMajor":1,"accessToken":"secret","streamTicket":ticket})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let error: StreamServerMessage = serde_json::from_value(receive_text(&mut exec).await).unwrap();
    assert!(matches!(
        error,
        StreamServerMessage::Error {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));
    let uris = recorded.request_uris(EXEC);
    assert!(
        uris.iter().any(|uri| uri.contains("command=%2Fbin%2Fsh")
            && uri.contains("command=-c")
            && uri.contains("command=printf+exact")
            && uri.contains("container=app")
            && uri.contains("stdin=true")
            && uri.contains("stdout=true")
            && !uri.contains("stderr=true")
            && uri.contains("tty=true")),
        "{uris:?}"
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
