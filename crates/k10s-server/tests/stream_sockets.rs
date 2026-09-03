//! Dedicated log socket and major-1 exec compatibility tombstone coverage.
use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{ErrorCode, StreamTarget, StreamTicketRequest, StreamType};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = k10s_server::spawn_loopback(
        k10s_server::ServerConfig {
            access_token: "secret".into(),
            ..Default::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "stream-tombstone"),
    )
    .await
    .unwrap();
    (handle, fake)
}

async fn recv_json(ws: &mut Ws) -> Value {
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(text) = frame else {
        panic!("expected text frame: {frame:?}")
    };
    serde_json::from_str(&text).unwrap()
}

async fn hello(ws: &mut Ws, token: &str, ticket: &str) {
    ws.send(Message::Text(
        json!({"kind":"hello","protocolMajor":1,"accessToken":token,"streamTicket":ticket})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}

async fn control(handle: &k10s_server::ServerHandle) -> Ws {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        handle.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":5,"capabilities":[],"accessToken":"secret"}}).to_string().into())).await.unwrap();
    assert_eq!(recv_json(&mut ws).await["kind"], "welcome");
    ws
}

#[tokio::test]
async fn legacy_exec_ticket_request_is_typed_unsupported_before_backend_dispatch() {
    let (handle, fake) = server().await;
    let mut ws = control(&handle).await;
    let request = StreamTicketRequest {
        target: StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: "web-frontend-7d9f8-00001".into(),
            uid: String::new(),
            container: "app".into(),
        },
        stream_type: StreamType::Exec,
        tty: true,
        command: vec!["/bin/sh".into()],
        tail_lines: None,
        since_seconds: None,
        previous: false,
        timestamps: false,
        follow: false,
    };
    ws.send(Message::Text(json!({"kind":"request","requestId":"legacy-exec","payload":{"kind":k10s_protocol::REQUEST_STREAM_TICKET,"payload":request}}).to_string().into())).await.unwrap();
    let error = recv_json(&mut ws).await;
    assert_eq!(
        error["payload"]["code"],
        serde_json::to_value(ErrorCode::UnsupportedMessage).unwrap()
    );
    assert_eq!(fake.live_stream_sessions(), 0);
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn exec_tombstone_authenticates_but_never_redeems_arbitrary_ticket() {
    let (handle, fake) = server().await;
    let url = format!("ws://{}{}", handle.addr(), k10s_protocol::EXEC_PATH);
    let (mut denied, _) = connect_async(&url).await.unwrap();
    hello(&mut denied, "wrong", "anything").await;
    assert_eq!(
        recv_json(&mut denied).await["code"],
        serde_json::to_value(ErrorCode::Unauthorized).unwrap()
    );

    let (mut tombstone, _) = connect_async(&url).await.unwrap();
    hello(&mut tombstone, "secret", "not-a-real-ticket").await;
    assert_eq!(
        recv_json(&mut tombstone).await["code"],
        serde_json::to_value(ErrorCode::UnsupportedMessage).unwrap()
    );
    assert!(matches!(
        tombstone.next().await,
        Some(Ok(Message::Close(_))) | None
    ));
    assert_eq!(fake.live_stream_sessions(), 0);
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn logs_remain_redeemable() {
    let (handle, fake) = server().await;
    let mut ws = control(&handle).await;
    let request = StreamTicketRequest {
        target: StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: "web-frontend-7d9f8-00001".into(),
            uid: String::new(),
            container: "app".into(),
        },
        stream_type: StreamType::Logs,
        tty: false,
        command: Vec::new(),
        tail_lines: None,
        since_seconds: None,
        previous: false,
        timestamps: false,
        follow: true,
    };
    ws.send(Message::Text(json!({"kind":"request","requestId":"logs","payload":{"kind":k10s_protocol::REQUEST_STREAM_TICKET,"payload":request}}).to_string().into())).await.unwrap();
    let response = recv_json(&mut ws).await;
    let ticket = response["payload"]["ticketId"].as_str().unwrap();
    let (mut logs, _) = connect_async(format!(
        "ws://{}{}",
        handle.addr(),
        k10s_protocol::LOGS_PATH
    ))
    .await
    .unwrap();
    hello(&mut logs, "secret", ticket).await;
    assert_eq!(recv_json(&mut logs).await["kind"], "ready");
    fake.tick_stream(ticket);
    assert!(matches!(logs.next().await, Some(Ok(Message::Binary(_)))));
    handle.shutdown().await.unwrap();
}
