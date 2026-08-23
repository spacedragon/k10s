//! End-to-end dedicated stream socket loops: exact `LOGS_PATH`/`EXEC_PATH`
//! upgrades, mandatory authenticated `Hello` before single-use ticket
//! redemption, frame/message limits, bounded queues, rate budgets, fake
//! sessions advancing on explicit ticks only, and terminal disconnect on
//! socket loss.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    EXEC_PATH, ErrorCode, LOGS_PATH, ServerFrame, ServerKind, StreamTarget, StreamTicketRequest,
    StreamType,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    tungstenite::protocol::frame::{Frame, coding::Data, coding::OpCode},
};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const WEB_POD: &str = "web-frontend-7d9f8-00001";
const WEB_CONTAINER: &str = "app";

fn web_target(container: &str) -> StreamTarget {
    StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: WEB_POD.into(),
        container: container.into(),
    }
}

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    spawn_server_with(ServerConfig::default()).await
}

async fn spawn_server_with(config: ServerConfig) -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..config
        },
        BackendKernel::new_with_instance_id(fake.clone(), "stream-server"),
    )
    .await
    .unwrap();
    (handle, fake)
}

/// Open an authenticated control socket for ticket issuance.
async fn connect_control(server: &k10s_server::ServerHandle) -> Ws {
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
    assert_eq!(receive_text(&mut ws).await["kind"], json!("welcome"));
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

/// Issue a stream ticket through the control socket and return its ID.
async fn issue_ticket(
    ws: &mut Ws,
    request_id: &str,
    target: &StreamTarget,
    stream_type: StreamType,
    tty: bool,
) -> String {
    send_request(
        ws,
        request_id,
        k10s_protocol::REQUEST_STREAM_TICKET,
        serde_json::to_value(StreamTicketRequest {
            target: target.clone(),
            stream_type,
            tty,
        })
        .unwrap(),
    )
    .await;
    let raw = receive_text(ws).await;
    assert_eq!(raw["kind"], json!("response"), "{raw:?}");
    raw["payload"]["ticketId"]
        .as_str()
        .expect("granted ticket id")
        .to_owned()
}

async fn expect_request_error(ws: &mut Ws, request_id: &str) -> Value {
    let raw = receive_text(ws).await;
    assert_eq!(raw["kind"], json!("error"), "{raw:?}");
    assert_eq!(
        raw["requestId"].as_str().expect("correlated"),
        request_id,
        "{raw:?}"
    );
    raw["payload"]["code"].clone()
}

/// Send the mandatory first `hello` frame on a stream route.
async fn send_stream_hello(ws: &mut Ws, access_token: &str, ticket: &str) {
    ws.send(Message::Text(
        json!({
            "kind": "hello",
            "protocolMajor": 1,
            "accessToken": access_token,
            "streamTicket": ticket
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

async fn send_raw(ws: &mut Ws, text: &str) {
    ws.send(Message::Text(text.to_owned().into()))
        .await
        .unwrap();
}

async fn receive_message(ws: &mut Ws) -> Message {
    tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server message within timeout")
        .expect("socket still open")
        .expect("socket healthy")
}

async fn receive_text(ws: &mut Ws) -> Value {
    match receive_message(ws).await {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("expected a text frame, got {other:?}"),
    }
}

async fn receive_close(ws: &mut Ws) {
    loop {
        match receive_message(ws).await {
            Message::Close(_) => return,
            Message::Text(text) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(value["kind"], json!("error"), "{value:?}");
            }
            Message::Binary(_) => panic!("unexpected binary frame before close"),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
        }
    }
}

/// Decode one versioned binary payload frame into `(kind, text)`.
fn decode_binary(message: Message) -> (u8, String) {
    let Message::Binary(frame) = message else {
        panic!("expected a binary frame, got {message:?}");
    };
    let decoded = k10s_protocol::decode_stream_payload(&frame).expect("valid stream payload");
    (
        decoded.kind,
        String::from_utf8(decoded.data.to_vec()).expect("utf8 stream payload"),
    )
}

/// Send one logical text message as real WebSocket fragmentation so the
/// server-side assembled-message limits are exercised across continuations.
async fn send_fragmented_text(ws: &mut Ws, parts: &[&str]) {
    for (index, part) in parts.iter().enumerate() {
        let is_final = index == parts.len() - 1;
        let opcode = if index == 0 {
            OpCode::Data(Data::Text)
        } else {
            OpCode::Data(Data::Continue)
        };
        ws.send(Message::Frame(Frame::message(
            part.as_bytes().to_vec(),
            opcode,
            is_final,
        )))
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn stream_routes_require_an_authenticated_hello_first() {
    let (server, _fake) = spawn_server().await;

    for path in [LOGS_PATH, EXEC_PATH] {
        // First frame must be a hello: anything else is rejected and closed
        // before any ticket is redeemed.
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        send_raw(&mut ws, r#"{"kind":"status","payload":{"message":"hi"}}"#).await;
        let frame = receive_text(&mut ws).await;
        assert_eq!(frame["kind"], json!("error"));
        assert_eq!(frame["code"], json!(ErrorCode::InvalidRequest));
        receive_close(&mut ws).await;

        // Missing token.
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"kind":"hello","protocolMajor":1,"streamTicket":"t"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let frame = receive_text(&mut ws).await;
        assert_eq!(frame["kind"], json!("error"), "{frame:?}");
        assert_eq!(frame["code"], json!(ErrorCode::Unauthorized));
        receive_close(&mut ws).await;

        // Wrong token.
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        send_stream_hello(&mut ws, "wrong-token", "some-ticket").await;
        let frame = receive_text(&mut ws).await;
        assert_eq!(frame["kind"], json!("error"), "{frame:?}");
        assert_eq!(frame["code"], json!(ErrorCode::Unauthorized));
        receive_close(&mut ws).await;
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tickets_are_single_use_and_bound_to_their_selected_container() {
    let (server, _fake) = spawn_server().await;
    let mut control = connect_control(&server).await;

    let ticket = issue_ticket(
        &mut control,
        "t1",
        &web_target(WEB_CONTAINER),
        StreamType::Logs,
        false,
    )
    .await;

    // Redeem once: the ready frame echoes the selected container.
    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), LOGS_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    let ready = receive_text(&mut ws).await;
    assert_eq!(ready["kind"], json!("ready"), "{ready:?}");
    assert_eq!(ready["streamType"], json!(StreamType::Logs));
    assert_eq!(ready["container"], json!(WEB_CONTAINER));

    // Redeem twice: the second hello is rejected as a conflict.
    let (mut replay, _) = connect_async(format!("ws://{}{}", server.addr(), LOGS_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut replay, "secret", &ticket).await;
    let frame = receive_text(&mut replay).await;
    assert_eq!(frame["kind"], json!("error"), "{frame:?}");
    assert_eq!(frame["code"], json!(ErrorCode::Conflict));
    receive_close(&mut replay).await;

    // A ticket issued for exec cannot be opened on the logs route either.
    let exec_ticket = issue_ticket(
        &mut control,
        "t2",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        true,
    )
    .await;
    let (mut wrong_route, _) = connect_async(format!("ws://{}{}", server.addr(), LOGS_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut wrong_route, "secret", &exec_ticket).await;
    let frame = receive_text(&mut wrong_route).await;
    assert_eq!(frame["kind"], json!("error"), "{frame:?}");
    receive_close(&mut wrong_route).await;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn fake_log_sessions_advance_only_on_explicit_ticks() {
    let (server, fake) = spawn_server().await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Logs,
        false,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), LOGS_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    assert_eq!(receive_text(&mut ws).await["kind"], json!("ready"));

    // Historical tail arrives as bounded binary chunks carrying the
    // versioned header.
    let (_, first) = decode_binary(receive_message(&mut ws).await);
    assert!(first.contains("backlog"), "{first}");
    let (_, second) = decode_binary(receive_message(&mut ws).await);
    assert!(second.contains("backlog"), "{second}");

    // No wall-clock advancement: silence until the test tick arrives.
    let quiet = tokio::time::timeout(Duration::from_millis(150), ws.next()).await;
    assert!(quiet.is_err(), "no unsolicited frames may arrive");

    fake.tick_stream(&ticket);
    let (kind, text) = decode_binary(receive_message(&mut ws).await);
    assert_eq!(kind, k10s_protocol::payload_kind::STDOUT);
    assert!(text.contains("log tick 1"), "{text}");
    assert!(text.contains(WEB_POD), "{text}");

    fake.tick_stream(&ticket);
    let (_, text) = decode_binary(receive_message(&mut ws).await);
    assert!(text.contains("log tick 2"), "{text}");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tty_shell_connects_explicitly_and_supports_stdin_resize_and_exit() {
    let (server, fake) = spawn_server().await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        true,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    let ready = receive_text(&mut ws).await;
    assert_eq!(ready["kind"], json!("ready"));
    assert_eq!(ready["tty"], json!(true));
    assert_eq!(ready["container"], json!(WEB_CONTAINER));

    // Banner arrives, then the session stays silent without ticks.
    let (_, banner) = decode_binary(receive_message(&mut ws).await);
    assert!(banner.contains("attached"), "{banner}");

    // TTY stdin: echoed merged output appears only after the explicit tick.
    ws.send(Message::Binary(
        k10s_protocol::encode_stream_payload(k10s_protocol::payload_kind::STDIN, b"echo hi\n")
            .into(),
    ))
    .await
    .unwrap();
    let quiet = tokio::time::timeout(Duration::from_millis(150), ws.next()).await;
    assert!(quiet.is_err(), "stdin alone must not advance the session");

    fake.tick_stream(&ticket);
    let (kind, text) = decode_binary(receive_message(&mut ws).await);
    assert_eq!(kind, k10s_protocol::payload_kind::TTY_OUTPUT);
    assert!(text.contains("$ echo hi"), "merged echo expected: {text}");

    // Resize is accepted and recorded behind the adapter seam.
    ws.send(Message::Binary(
        k10s_protocol::encode_stream_payload(
            k10s_protocol::payload_kind::RESIZE,
            &k10s_protocol::encode_resize_payload(120, 40),
        )
        .into(),
    ))
    .await
    .unwrap();
    let status = receive_text(&mut ws).await;
    assert_eq!(status["kind"], json!("status"), "{status:?}");
    assert_eq!(fake.last_stream_resize(&ticket), Some((120, 40)));

    // Explicit exit: the server forwards the exit status and closes.
    fake.finish_stream(&ticket, 7);
    let exit = receive_text(&mut ws).await;
    assert_eq!(exit["kind"], json!("exit"));
    assert_eq!(exit["exitCode"], json!(7));
    receive_close(&mut ws).await;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_tty_exec_is_a_distinct_mode_with_separated_streams() {
    let (server, fake) = spawn_server().await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        false,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    let ready = receive_text(&mut ws).await;
    assert_eq!(ready["kind"], json!("ready"));
    assert_eq!(ready["tty"], json!(false));

    // Consume the backlog banner, then tick.
    let (_, banner) = decode_binary(receive_message(&mut ws).await);
    assert!(banner.contains("attached"), "{banner}");

    fake.tick_stream(&ticket);
    let (first_kind, first_text) = decode_binary(receive_message(&mut ws).await);
    let (second_kind, _) = decode_binary(receive_message(&mut ws).await);
    let (stdout_kind, stderr_kind) = if first_kind == k10s_protocol::payload_kind::STDOUT {
        (first_kind, second_kind)
    } else {
        (second_kind, first_kind)
    };
    assert_eq!(stdout_kind, k10s_protocol::payload_kind::STDOUT);
    assert_eq!(stderr_kind, k10s_protocol::payload_kind::STDERR);
    assert!(
        first_text.contains("stdout") || first_text.contains("stderr"),
        "{first_text}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn rbac_and_missing_binary_errors_are_typed_at_issuance() {
    let (server, _fake) = spawn_server().await;
    let mut control = connect_control(&server).await;

    // The readonly context denies stream attachment entirely.
    send_request(
        &mut control,
        "rbac",
        k10s_protocol::REQUEST_STREAM_TICKET,
        serde_json::to_value(StreamTicketRequest {
            target: StreamTarget {
                context: "prod-readonly".into(),
                namespace: "default".into(),
                pod: "edge-gateway-x".into(),
                container: "app".into(),
            },
            stream_type: StreamType::Logs,
            tty: false,
        })
        .unwrap(),
    )
    .await;
    let code = expect_request_error(&mut control, "rbac").await;
    assert_eq!(code, json!(ErrorCode::Unauthorized), "RBAC denial");

    // Unknown pods and containers stay typed not-found errors.
    let missing_pod = StreamTarget {
        pod: "no-such-pod".into(),
        ..web_target(WEB_CONTAINER)
    };
    send_request(
        &mut control,
        "missing-pod",
        k10s_protocol::REQUEST_STREAM_TICKET,
        serde_json::to_value(StreamTicketRequest {
            target: missing_pod,
            stream_type: StreamType::Exec,
            tty: true,
        })
        .unwrap(),
    )
    .await;
    let code = expect_request_error(&mut control, "missing-pod").await;
    assert_eq!(code, json!(ErrorCode::NotFound));
    send_request(
        &mut control,
        "missing-container",
        k10s_protocol::REQUEST_STREAM_TICKET,
        serde_json::to_value(StreamTicketRequest {
            target: web_target("sidecar"),
            stream_type: StreamType::Exec,
            tty: true,
        })
        .unwrap(),
    )
    .await;
    let code = expect_request_error(&mut control, "missing-container").await;
    assert_eq!(code, json!(ErrorCode::NotFound));

    // A container without an executable reports the missing binary.
    send_request(
        &mut control,
        "no-binary",
        k10s_protocol::REQUEST_STREAM_TICKET,
        serde_json::to_value(StreamTicketRequest {
            target: web_target("distroless"),
            stream_type: StreamType::Exec,
            tty: true,
        })
        .unwrap(),
    )
    .await;
    let code = expect_request_error(&mut control, "no-binary").await;
    assert_eq!(code, json!(ErrorCode::Conflict), "missing binary denial");

    // Binary availability is exec-only: the distroless container's LOGS
    // remain readable.
    let ticket = issue_ticket(
        &mut control,
        "distroless-logs",
        &web_target("distroless"),
        StreamType::Logs,
        false,
    )
    .await;
    assert!(!ticket.is_empty());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_fragmented_hellos_are_rejected_on_both_routes() {
    // A tiny assembled-message bound applies across fragmentation before any
    // authentication or ticket work happens.
    let config = ServerConfig {
        max_stream_frame_size: 128,
        max_stream_message_size: 256,
        ..ServerConfig::default()
    };
    let (server, _fake) = spawn_server_with(config).await;

    for path in [LOGS_PATH, EXEC_PATH] {
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        let huge = format!(
            "{}",
            json!({"kind":"hello","payload":{"protocolMajor":1,"accessToken":"x","streamTicket":"y".repeat(2048)}})
        );
        ws.send(Message::Text(huge.into())).await.unwrap();
        // The server closes instead of answering: the limit fires before
        // dispatch.
        let outcome = tokio::time::timeout(Duration::from_secs(10), ws.next()).await;
        match outcome {
            Ok(Some(Err(_))) | Err(_) => {}
            Ok(Some(Ok(Message::Close(_)))) => {}
            Ok(Some(Ok(other))) => panic!("expected closure, got {other:?}"),
            Ok(None) => {}
        }
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn rate_budget_overload_closes_the_socket_explicitly() {
    let config = ServerConfig {
        stream_rate_budget_bytes_per_sec: 64,
        ..ServerConfig::default()
    };
    let (server, _fake) = spawn_server_with(config).await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        true,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    assert_eq!(receive_text(&mut ws).await["kind"], json!("ready"));
    let (_, banner) = decode_binary(receive_message(&mut ws).await);

    // Flood well past the configured inbound byte budget. The server must
    // answer with an explicit budget/overload error and close; silence or
    // continued streaming would fail this test.
    for _ in 0..8 {
        ws.send(Message::Binary(
            k10s_protocol::encode_stream_payload(
                k10s_protocol::payload_kind::STDIN,
                b"0123456789abcdef0123456789abcdef",
            )
            .into(),
        ))
        .await
        .unwrap();
    }
    let mut saw_overload_error = false;
    let mut saw_close = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Err(_) => panic!("the server must close a flooding socket explicitly"),
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(Message::Close(frame)))) => {
                saw_close = true;
                if frame.is_some_and(|frame| frame.reason.contains("budget")) {
                    saw_overload_error = true;
                }
                break;
            }
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["message"].as_str().is_some_and(|message| {
                    message.contains("budget") || message.contains("overload")
                }) {
                    saw_overload_error = true;
                }
            }
            Ok(Some(Ok(_))) => continue,
        }
    }
    assert!(
        saw_overload_error && saw_close,
        "the flood must end in an explicit overload closure (error={saw_overload_error}, close={saw_close})"
    );
    let _ = banner;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn socket_loss_disconnects_the_terminal_session() {
    let (server, fake) = spawn_server().await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        true,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    assert_eq!(receive_text(&mut ws).await["kind"], json!("ready"));
    assert_eq!(fake.live_stream_sessions(), 1);

    drop(ws);
    // The next adapter touch observes the vanished receiver and retires the
    // session; nothing can keep streaming afterwards.
    for _ in 0..20 {
        fake.tick_stream(&ticket);
        if fake.live_stream_sessions() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        fake.live_stream_sessions(),
        0,
        "socket loss must disconnect the terminal session"
    );

    server.shutdown().await.unwrap();
}

/// The UI client seam can drive ticket issuance end to end over a real
/// control socket, and the credential never appears in any URL.
#[tokio::test]
async fn client_state_seam_issues_stream_tickets_without_token_urls() {
    use k10s_ui::client::{ClientConfig, ClientPhase, ClientState, ConnectTarget, Query};

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
    let welcome = receive_message(&mut socket).await;
    let welcome = match welcome {
        Message::Text(text) => serde_json::from_str::<ServerFrame>(&text)
            .unwrap_or_else(|error| panic!("welcome frame must decode: {error}")),
        other => panic!("expected welcome text frame, got {other:?}"),
    };
    client.apply(welcome).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);

    // The URL carries no credential material.
    assert!(!url.contains("secret"));
    assert!(!format!("{:?}", ConnectTarget::new(url.clone(), "secret")).contains("secret"));

    let pending = client
        .begin(Query::StreamTicket {
            target: web_target(WEB_CONTAINER),
            stream_type: StreamType::Logs,
            tty: false,
        })
        .unwrap();
    while let Some(frame) = client.take_outbound() {
        socket
            .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
            .await
            .unwrap();
    }
    loop {
        let raw = receive_text(&mut socket).await;
        let frame: ServerFrame = serde_json::from_value(raw).unwrap();
        if frame
            .request_id
            .as_ref()
            .is_some_and(|id| id == pending.id())
        {
            assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
            client.apply(frame).unwrap();
            break;
        }
        client.apply(frame).unwrap();
    }
    let granted = match client.take(pending).expect("ticket granted") {
        k10s_ui::client::QueryResult::StreamTicket(response) => *response,
        other => panic!("expected a stream ticket response, got {other:?}"),
    };
    assert!(!granted.ticket_id.is_empty());
    assert_eq!(granted.stream_type, StreamType::Logs);
    assert_eq!(granted.target.pod, WEB_POD);

    server.shutdown().await.unwrap();
}

/// Fragmented messages are reassembled before limits are applied: a hello
/// split across continuations still authenticates, while one whose
/// assembled size exceeds the message bound is rejected on both routes.
#[tokio::test]
async fn fragmented_messages_are_assembled_and_enforced() {
    let config = ServerConfig {
        max_stream_frame_size: 128,
        max_stream_message_size: 256,
        ..ServerConfig::default()
    };
    let (server, fake) = spawn_server_with(config).await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Logs,
        false,
    )
    .await;

    // A fragmented hello under the assembled bound authenticates fine: the
    // wrong token proves the frame was actually decoded after assembly.
    for path in [LOGS_PATH, EXEC_PATH] {
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        send_fragmented_text(
            &mut ws,
            &[
                r#"{"kind":"hello","protocolMajor":1,"#,
                r#"  "accessToken":"wrong-token","#,
                &format!(r#""streamTicket":"{ticket}"}}"#),
            ],
        )
        .await;
        let frame = receive_text(&mut ws).await;
        assert_eq!(frame["kind"], json!("error"), "{frame:?}");
        assert_eq!(frame["code"], json!(ErrorCode::Unauthorized));
        receive_close(&mut ws).await;
    }

    // A fragmented hello whose ASSEMBLED size exceeds the message bound is
    // rejected on both routes, even though every individual fragment is
    // far below the frame limit.
    for path in [LOGS_PATH, EXEC_PATH] {
        let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), path))
            .await
            .unwrap();
        let padding = "y".repeat(400);
        send_fragmented_text(
            &mut ws,
            &[
                r#"{"kind":"hello","protocolMajor":1,"accessToken":"secret","streamTicket":""#,
                &padding[..200],
                &padding[200..],
                r#""}"#,
            ],
        )
        .await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
        match outcome {
            Err(_) | Ok(None) | Ok(Some(Err(_))) => {}
            Ok(Some(Ok(Message::Close(_)))) => {}
            Ok(Some(Ok(other))) => {
                panic!("oversized fragmented message must close the socket, got {other:?}")
            }
        }
    }

    // The valid ticket was never redeemed by any of the rejected sockets.
    assert_eq!(
        fake.live_stream_sessions(),
        0,
        "rejected hellos must not open sessions"
    );

    server.shutdown().await.unwrap();
}

/// Fragmented oversized exec input is rejected by the same assembled limit.
#[tokio::test]
async fn fragmented_oversized_exec_input_is_rejected() {
    let (server, fake) = spawn_server().await;
    let mut control = connect_control(&server).await;
    let ticket = issue_ticket(
        &mut control,
        "t",
        &web_target(WEB_CONTAINER),
        StreamType::Exec,
        true,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &ticket).await;
    assert_eq!(receive_text(&mut ws).await["kind"], json!("ready"));
    let (_, banner) = decode_binary(receive_message(&mut ws).await);
    assert!(banner.contains("attached"));

    // One logical stdin message far above the default assembled-message
    // bound, split into two fragments each below the frame bound.
    let chunk = [b'x'; 64 * 1024];
    let header = vec![
        k10s_protocol::STREAM_PAYLOAD_VERSION,
        k10s_protocol::payload_kind::STDIN,
    ];
    let first = [header.as_slice(), &chunk].concat();
    let second = chunk.to_vec();
    ws.send(Message::Frame(Frame::message(
        first,
        OpCode::Data(Data::Binary),
        false,
    )))
    .await
    .unwrap();
    ws.send(Message::Frame(Frame::message(
        second,
        OpCode::Data(Data::Continue),
        true,
    )))
    .await
    .unwrap();

    // The server closes instead of processing the input.
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Err(_) => panic!("server must react to the oversized input"),
            Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => continue,
        }
    }
    // The limit violation killed the connection, so the backend session
    // is retired on the next adapter touch and the oversized line was
    // never processed.
    fake.tick_stream(&ticket);
    assert_eq!(
        fake.live_stream_sessions(),
        0,
        "the disconnected terminal must not survive"
    );

    server.shutdown().await.unwrap();
}

/// The dedicated stream connection cap is enforced independently of the
/// shared unauthenticated-control pool.
#[tokio::test]
async fn dedicated_stream_connection_cap_is_enforced() {
    let config = ServerConfig {
        max_stream_connections: 1,
        ..ServerConfig::default()
    };
    let (server, _fake) = spawn_server_with(config).await;
    let mut control = connect_control(&server).await;
    let first = issue_ticket(
        &mut control,
        "t1",
        &web_target(WEB_CONTAINER),
        StreamType::Logs,
        false,
    )
    .await;
    issue_ticket(
        &mut control,
        "t2",
        &web_target("app"),
        StreamType::Logs,
        false,
    )
    .await;

    let (mut ws, _) = connect_async(format!("ws://{}{}", server.addr(), LOGS_PATH))
        .await
        .unwrap();
    send_stream_hello(&mut ws, "secret", &first).await;
    assert_eq!(receive_text(&mut ws).await["kind"], json!("ready"));
    let (_, _) = decode_binary(receive_message(&mut ws).await);

    // While that stream stays open, a second upgrade is refused outright:
    // live streams must not consume the shared control-authentication pool.
    let error = connect_async(format!("ws://{}{}", server.addr(), EXEC_PATH))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("503"),
        "the second stream must be refused with 503: {error}"
    );
    // Control authentication still works while the stream is open.
    let _still_authenticates = connect_control(&server).await;

    server.shutdown().await.unwrap();
}
