use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{ClientFrame, ClientKind, RequestId, ServerFrame, ServerKind};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

fn hello(token: &str) -> ClientFrame {
    ClientFrame {
        kind: ClientKind::Hello,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: json!({
            "protocolMajor": 1,
            "protocolMinor": 9,
            "capabilities": ["logs.tail", "not-supported"],
            "accessToken": token
        }),
    }
}

async fn server() -> k10s_server::ServerHandle {
    spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            hello_timeout: Duration::from_millis(100),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "test-server"),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn mounts_only_control_as_websocket() {
    let server = server().await;
    for path in [k10s_protocol::LOGS_PATH, k10s_protocol::EXEC_PATH] {
        let error = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("501"), "{error}");
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn hello_negotiates_then_bootstrap_preserves_request_id() {
    let server = server().await;
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        serde_json::to_string(&hello("secret")).unwrap().into(),
    ))
    .await
    .unwrap();
    let welcome: ServerFrame =
        serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(welcome.kind, ServerKind::Welcome);
    assert_eq!(welcome.payload["protocol"], json!({"major": 1, "minor": 1}));
    assert_eq!(welcome.payload["capabilities"], json!(["logs.tail"]));

    let request_id = RequestId::from("req-7");
    let request = ClientFrame {
        kind: ClientKind::Request,
        request_id: Some(request_id.clone()),
        subscription_id: None,
        sequence: None,
        payload: json!({"kind": "bootstrap", "deadline": 1000}),
    };
    ws.send(Message::Text(
        serde_json::to_string(&request).unwrap().into(),
    ))
    .await
    .unwrap();
    let response: ServerFrame =
        serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(response.kind, ServerKind::Response);
    assert_eq!(response.request_id, Some(request_id));
    assert_eq!(response.payload["contexts"][0]["name"], "dev-local");
    assert!(response.payload.to_string().find("secret").is_none());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_first_frame_is_explicitly_closed() {
    let server = server().await;
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({"kind":"ping","payload":{}}).to_string().into(),
    ))
    .await
    .unwrap();
    let close = ws.next().await.unwrap().unwrap();
    assert!(matches!(close, Message::Close(Some(frame)) if frame.reason.contains("hello")));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_status_subscription_is_acknowledged() {
    let server = server().await;
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        serde_json::to_string(&hello("secret")).unwrap().into(),
    ))
    .await
    .unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"sub-1",
            "payload":{"kind":"bootstrapStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let response: ServerFrame =
        serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(response.kind, ServerKind::Subscribed);
    assert_eq!(response.subscription_id.unwrap().as_str(), "sub-1");
    server.shutdown().await.unwrap();
}
