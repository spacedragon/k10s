use std::{future::Future, pin::Pin, time::Duration};

use futures_util::{SinkExt, StreamExt};
use k10s_backend::port::{BootstrapInfo, ContextInfo};
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, Command, FakeKubernetes, KubernetesAccess,
    OperationId, Query, QueryResult, Subscribe, SubscriptionHandle,
};
use k10s_protocol::{
    Ack, ClientFrame, ClientKind, ErrorCode, ErrorScope, RequestId, Retryability, ServerFrame,
    ServerKind, ServerPayload,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::tungstenite::protocol::frame::{
    Frame,
    coding::{Data, OpCode},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone)]
struct SlowKubernetes;

#[derive(Debug, Clone)]
struct SensitiveKubernetes;

#[derive(Debug, Clone)]
struct HugeKubernetes;

#[derive(Debug, Clone)]
struct StatusKubernetes {
    events: tokio::sync::broadcast::Sender<BackendEvent>,
}

impl StatusKubernetes {
    fn new() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(4);
        Self { events }
    }

    fn unavailable(&self, context: &str, reason: &str) {
        self.events
            .send(BackendEvent::ContextUnavailable {
                context: context.into(),
                reason: reason.into(),
            })
            .expect("control subscription receives the transition");
    }
}

impl KubernetesAccess for StatusKubernetes {
    fn query<'a>(
        &'a self,
        _: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("query")) })
    }

    fn execute<'a>(
        &'a self,
        _: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("execute")) })
    }

    fn subscribe<'a>(
        &'a self,
        request: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let id = match request {
                Subscribe::BootstrapStatus => "bootstrap-status",
                _ => "resource-watch",
            };
            Ok(SubscriptionHandle::with_events(id, self.events.subscribe()))
        })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: k10s_backend::StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}

impl KubernetesAccess for HugeKubernetes {
    fn query<'a>(
        &'a self,
        _: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async {
            Ok(QueryResult::Bootstrap(BootstrapInfo {
                contexts: vec![ContextInfo {
                    name: "x".repeat(16 * 1024 * 1024),
                    cluster: "large".into(),
                    namespace: None,
                    is_current: true,
                    availability: k10s_protocol::ContextAvailability::Available,
                    unavailable_reason: None,
                }],
            }))
        })
    }
    fn execute<'a>(
        &'a self,
        _: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("execute")) })
    }
    fn subscribe<'a>(
        &'a self,
        _: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("subscribe")) })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: k10s_backend::StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}

impl KubernetesAccess for SensitiveKubernetes {
    fn query<'a>(
        &'a self,
        _: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async {
            Err(BackendError::Internal(
                "credential=super-secret-query".into(),
            ))
        })
    }
    fn execute<'a>(
        &'a self,
        _: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async {
            Err(BackendError::Internal(
                "credential=super-secret-command".into(),
            ))
        })
    }
    fn subscribe<'a>(
        &'a self,
        _: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async {
            Err(BackendError::Internal(
                "credential=super-secret-subscription".into(),
            ))
        })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: k10s_backend::StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}

impl KubernetesAccess for SlowKubernetes {
    fn query<'a>(
        &'a self,
        _: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(QueryResult::Bootstrap(BootstrapInfo {
                contexts: vec![ContextInfo {
                    name: "slow".into(),
                    cluster: "slow".into(),
                    namespace: None,
                    is_current: true,
                    availability: k10s_protocol::ContextAvailability::Available,
                    unavailable_reason: None,
                }],
            }))
        })
    }
    fn execute<'a>(
        &'a self,
        _: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("execute")) })
    }
    fn subscribe<'a>(
        &'a self,
        _: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("subscribe")) })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: k10s_backend::StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}

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

async fn connect(server: &k10s_server::ServerHandle) -> Ws {
    connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap()
    .0
}

async fn authenticate(ws: &mut Ws) {
    ws.send(Message::Text(
        serde_json::to_string(&hello("secret")).unwrap().into(),
    ))
    .await
    .unwrap();
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Welcome);
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap()
}

fn assert_close_reason(message: Message, reason: &str) {
    assert!(
        matches!(message, Message::Close(Some(ref frame)) if frame.reason.contains(reason)),
        "{message:?}"
    );
}

#[tokio::test]
async fn dedicated_stream_routes_are_websocket_upgrades() {
    let server = server().await;
    for path in [k10s_protocol::LOGS_PATH, k10s_protocol::EXEC_PATH] {
        // The dedicated routes are live WebSocket upgrades now: a connection
        // succeeds but is closed because the mandatory hello never arrives
        // (or arrives invalid), never a plain HTTP error like 501.
        let outcome = connect_async(format!("ws://{}{}", server.addr(), path)).await;
        match outcome {
            Ok((mut ws, _)) => {
                ws.send(Message::Text(r#"{"kind":"status"}"#.to_owned().into()))
                    .await
                    .unwrap();
                let reply = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
                    .await
                    .expect("stream route answers within timeout");
                assert!(
                    reply.is_some(),
                    "the stream route must answer before closing: {path}"
                );
            }
            Err(error) => panic!("dedicated stream route {path} must upgrade: {error}"),
        }
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
    assert_eq!(welcome.payload["protocol"], json!({"major": 1, "minor": 5}));
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
        json!({"kind":"ping","payload":null}).to_string().into(),
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

#[tokio::test]
async fn bootstrap_status_is_singleton_without_consuming_resource_watch_capacity() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            max_resource_subscriptions_per_session: 1,
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(StatusKubernetes::new(), "status-limit-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;

    for id in ["status-1", "status-2"] {
        ws.send(Message::Text(
            json!({
                "kind":"subscribe", "subscriptionId":id,
                "payload":{"kind":"bootstrapStatus"}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        let response = receive_frame(&mut ws).await;
        assert_eq!(
            response.kind,
            if id == "status-1" {
                ServerKind::Subscribed
            } else {
                ServerKind::Error
            },
            "{response:?}"
        );
    }

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"resource-1",
            "payload":{
                "kind":"resource", "context":"dev-local",
                "gvk":{"group":"","version":"v1","kind":"Pod"},
                "namespace":"default"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        receive_frame(&mut ws).await.kind,
        ServerKind::Subscribed,
        "the status receiver must not consume resource-watch capacity"
    );

    ws.send(Message::Text(
        json!({
            "kind":"unsubscribe", "subscriptionId":"status-1",
            "payload":null
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"status-2",
            "payload":{"kind":"bootstrapStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Subscribed);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_status_forwards_background_context_failures() {
    let backend = StatusKubernetes::new();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(backend.clone(), "status-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"status-1",
            "payload":{"kind":"bootstrapStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let subscribed = receive_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);

    backend.unavailable("broken", "credential plugin denied");
    let transition = receive_frame(&mut ws).await;
    assert_eq!(transition.kind, ServerKind::Error);
    assert_eq!(
        transition.subscription_id.as_ref().map(|id| id.as_str()),
        Some("status-1")
    );
    let ServerPayload::Error(error) = transition.decode_payload().unwrap() else {
        panic!("transition is a structured error");
    };
    assert_eq!(error.scope, ErrorScope::Subscription);
    assert_eq!(error.retryability, Retryability::AfterRefresh);
    assert_eq!(
        error.details,
        Some(json!({
            "kind": "contextUnavailable",
            "context": "broken",
            "reason": "credential plugin denied",
        }))
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn ack_cursor_must_match_envelope_and_be_monotonic_not_future() {
    let server = server().await;
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;

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
    let first = receive_frame(&mut ws).await;
    assert_eq!(first.sequence, Some(1));

    send_ack(&mut ws, 2, 1).await;
    assert_error_code(receive_frame(&mut ws).await, ErrorCode::InvalidRequest);
    send_ack(&mut ws, 2, 2).await;
    assert_error_code(receive_frame(&mut ws).await, ErrorCode::InvalidRequest);

    send_ack(&mut ws, 1, 1).await;
    assert_no_error_via_ping(&mut ws).await;
    send_ack(&mut ws, 1, 1).await;
    assert_no_error_via_ping(&mut ws).await;
    send_ack_without_envelope_sequence(&mut ws, 1).await;
    assert_no_error_via_ping(&mut ws).await;

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"sub-2",
            "payload":{"kind":"bootstrapStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let second = receive_frame(&mut ws).await;
    assert_eq!(second.sequence, Some(2));
    send_ack(&mut ws, 2, 2).await;
    assert_no_error_via_ping(&mut ws).await;

    send_ack(&mut ws, 1, 1).await;
    assert_error_code(receive_frame(&mut ws).await, ErrorCode::InvalidRequest);
    server.shutdown().await.unwrap();
}

async fn send_ack(ws: &mut Ws, envelope_sequence: u64, cursor: u64) {
    ws.send(Message::Text(
        serde_json::to_string(&ClientFrame {
            kind: ClientKind::Ack,
            request_id: None,
            subscription_id: None,
            sequence: Some(envelope_sequence),
            payload: serde_json::to_value(Ack {
                last_acked_sequence: cursor,
            })
            .unwrap(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
}

async fn send_ack_without_envelope_sequence(ws: &mut Ws, cursor: u64) {
    ws.send(Message::Text(
        serde_json::to_string(&ClientFrame {
            kind: ClientKind::Ack,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(Ack {
                last_acked_sequence: cursor,
            })
            .unwrap(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
}

fn assert_error_code(frame: ServerFrame, expected: ErrorCode) {
    let ServerPayload::Error(error) = frame.decode_payload().unwrap() else {
        panic!("expected structured error");
    };
    assert_eq!(error.code, expected);
}

async fn assert_no_error_via_ping(ws: &mut Ws) {
    ws.send(Message::Text(
        json!({"kind":"ping","payload":null}).to_string().into(),
    ))
    .await
    .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(200), ws.next())
        .await
        .expect("pong timeout")
        .unwrap()
        .unwrap();
    let frame: ServerFrame = serde_json::from_str(&response.into_text().unwrap()).unwrap();
    assert_eq!(frame.kind, ServerKind::Pong, "unexpected frame: {frame:?}");
}

#[tokio::test]
async fn wrong_token_and_incompatible_major_close_explicitly() {
    let server = server().await;
    let mut wrong = connect(&server).await;
    wrong
        .send(Message::Text(
            serde_json::to_string(&hello("wrong")).unwrap().into(),
        ))
        .await
        .unwrap();
    let rejected = receive_frame(&mut wrong).await;
    let ServerPayload::Error(rejected) = rejected.decode_payload().unwrap() else {
        panic!("expected terminal authentication error");
    };
    assert_eq!(rejected.code, ErrorCode::Unauthorized);
    assert_eq!(rejected.retryability, Retryability::Never);
    assert!(!rejected.safe_message.contains("wrong"));
    assert!(!rejected.safe_message.contains("secret"));
    assert_close_reason(wrong.next().await.unwrap().unwrap(), "authentication");

    let mut incompatible = connect(&server).await;
    let mut value = serde_json::to_value(hello("secret")).unwrap();
    value["payload"]["protocolMajor"] = json!(2);
    incompatible
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
    let upgrade = receive_frame(&mut incompatible).await;
    let ServerPayload::Error(upgrade) = upgrade.decode_payload().unwrap() else {
        panic!("expected terminal protocol error");
    };
    assert_eq!(upgrade.code, ErrorCode::IncompatibleProtocol);
    assert_eq!(upgrade.retryability, Retryability::Never);
    assert_close_reason(incompatible.next().await.unwrap().unwrap(), "incompatible");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn hello_timeout_closes_explicitly() {
    let server = server().await;
    let mut ws = connect(&server).await;
    assert_close_reason(ws.next().await.unwrap().unwrap(), "timeout");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_and_unsupported_frames_return_structured_errors() {
    let server = server().await;
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text("{".into())).await.unwrap();
    let malformed = receive_frame(&mut ws).await;
    assert_eq!(malformed.kind, ServerKind::Error);
    assert_eq!(malformed.payload["code"], json!("invalidRequest"));
    ws.send(Message::Text(
        json!({"kind":"futureKind","payload":{}}).to_string().into(),
    ))
    .await
    .unwrap();
    let unsupported = receive_frame(&mut ws).await;
    assert_eq!(unsupported.kind, ServerKind::Error);
    assert_eq!(unsupported.payload["code"], json!("unsupportedMessage"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn deadline_and_idempotent_cancel_produce_structured_terminal_errors() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(SlowKubernetes, "slow-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(json!({"kind":"request","requestId":"deadline","payload":{"kind":"bootstrap","deadline":1}}).to_string().into())).await.unwrap();
    let timeout = receive_frame(&mut ws).await;
    assert_eq!(timeout.request_id.as_ref().unwrap().as_str(), "deadline");
    assert_eq!(timeout.payload["code"], json!("timeout"));

    ws.send(Message::Text(
        json!({"kind":"request","requestId":"cancel","payload":{"kind":"bootstrap"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let cancel = Message::Text(
        json!({"kind":"cancelRequest","requestId":"cancel","payload":null})
            .to_string()
            .into(),
    );
    ws.send(cancel.clone()).await.unwrap();
    ws.send(cancel).await.unwrap();
    let cancelled = receive_frame(&mut ws).await;
    assert_eq!(cancelled.request_id.as_ref().unwrap().as_str(), "cancel");
    assert_eq!(cancelled.payload["code"], json!("cancelled"));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), ws.next())
            .await
            .is_err()
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn frame_and_fragmented_message_limits_are_enforced() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            max_frame_size: 256,
            max_message_size: 300,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let mut frame_ws = connect(&server).await;
    authenticate(&mut frame_ws).await;
    frame_ws
        .send(Message::Text("x".repeat(257).into()))
        .await
        .unwrap();
    let rejected = frame_ws.next().await.unwrap();
    assert!(rejected.is_err() || matches!(rejected, Ok(Message::Close(_))));

    let mut fragmented = connect(&server).await;
    authenticate(&mut fragmented).await;
    fragmented
        .send(Message::Frame(Frame::message(
            "x".repeat(160),
            OpCode::Data(Data::Text),
            false,
        )))
        .await
        .unwrap();
    fragmented
        .send(Message::Frame(Frame::message(
            "x".repeat(160),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
    let rejected = fragmented.next().await.unwrap();
    assert!(rejected.is_err() || matches!(rejected, Ok(Message::Close(_))));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unauthenticated_and_authenticated_limits_reject_excess_connections() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            max_unauthenticated_connections: 1,
            max_authenticated_connections: 1,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let _waiting = connect(&server).await;
    let error = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap_err();
    assert!(error.to_string().contains("503"), "{error}");
    drop(_waiting);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut first = connect(&server).await;
    authenticate(&mut first).await;
    let mut second = connect(&server).await;
    second
        .send(Message::Text(
            serde_json::to_string(&hello("secret")).unwrap().into(),
        ))
        .await
        .unwrap();
    assert_close_reason(
        second.next().await.unwrap().unwrap(),
        "authenticated connection limit",
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbound_overload_closes_explicitly_at_configured_bound() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            outbound_queue_capacity: 1,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    for _ in 0..64 {
        ws.feed(Message::Text("{".into())).await.unwrap();
    }
    ws.flush().await.unwrap();
    let mut saw_overload = false;
    for _ in 0..64 {
        match ws.next().await {
            Some(Ok(Message::Close(Some(frame)))) if frame.reason.contains("overload") => {
                saw_overload = true;
                break;
            }
            Some(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saw_overload);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_notifies_active_socket_and_joins() {
    let server = server().await;
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    let shutdown = tokio::spawn(server.shutdown());
    let notice = receive_frame(&mut ws).await;
    assert_eq!(notice.kind, ServerKind::ShutdownNotice);
    drop(ws);
    tokio::time::timeout(Duration::from_secs(1), shutdown)
        .await
        .expect("server shutdown must join")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_returns_only_after_socket_tasks_finish_their_graceful_flush() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            graceful_flush_timeout: Duration::from_secs(1),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(HugeKubernetes, "huge-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"request", "requestId":"huge",
            "payload":{"kind":"bootstrap"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    // Give the huge response time to wedge the writer on TCP backpressure.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let shutdown = tokio::spawn(server.shutdown());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for upgraded socket tasks, not just the listener"
    );
    drop(ws);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown must finish once the flush window closes")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn concurrent_fresh_clients_receive_distinct_session_ids() {
    let server = server().await;
    let mut third = connect(&server).await;
    let mut fourth = connect(&server).await;
    third
        .send(Message::Text(
            serde_json::to_string(&hello("secret")).unwrap().into(),
        ))
        .await
        .unwrap();
    fourth
        .send(Message::Text(
            serde_json::to_string(&hello("secret")).unwrap().into(),
        ))
        .await
        .unwrap();
    let third_welcome = receive_frame(&mut third).await;
    let fourth_welcome = receive_frame(&mut fourth).await;
    assert_ne!(
        third_welcome.payload["sessionId"],
        fourth_welcome.payload["sessionId"]
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_uses_the_connections_negotiated_protocol_and_capabilities() {
    let server = server().await;
    let mut ws = connect(&server).await;
    let mut custom_hello = serde_json::to_value(hello("secret")).unwrap();
    custom_hello["payload"]["protocolMinor"] = json!(0);
    custom_hello["payload"]["capabilities"] = json!(["exec.attach"]);
    ws.send(Message::Text(custom_hello.to_string().into()))
        .await
        .unwrap();
    let welcome = receive_frame(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"request", "requestId":"negotiated-bootstrap",
            "payload":{"kind":"bootstrap"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let bootstrap = receive_frame(&mut ws).await;
    assert_eq!(bootstrap.payload["protocol"], welcome.payload["protocol"]);
    assert_eq!(
        bootstrap.payload["capabilities"],
        welcome.payload["capabilities"]
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn internal_backend_diagnostics_never_reach_query_or_subscription_errors() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(SensitiveKubernetes, "sensitive-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"request", "requestId":"internal-query",
            "payload":{"kind":"bootstrap"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let query_error = receive_frame(&mut ws).await;
    assert_eq!(query_error.payload["code"], json!("internal"));
    assert_eq!(
        query_error.payload["safeMessage"],
        json!("internal server error")
    );
    assert!(!query_error.payload.to_string().contains("super-secret"));

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"internal-subscription",
            "payload":{"kind":"bootstrapStatus"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let subscription_error = receive_frame(&mut ws).await;
    assert_eq!(subscription_error.payload["code"], json!("internal"));
    assert_eq!(
        subscription_error.payload["safeMessage"],
        json!("internal server error")
    );
    assert!(
        !subscription_error
            .payload
            .to_string()
            .contains("super-secret")
    );
    assert_eq!(
        subscription_error
            .subscription_id
            .as_ref()
            .unwrap()
            .as_str(),
        "internal-subscription"
    );
    assert_eq!(subscription_error.payload["scope"], json!("subscription"));
    assert_eq!(
        subscription_error.payload["correlationId"],
        json!("internal-subscription")
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_and_invalid_subscription_errors_keep_subscription_context() {
    let server = server().await;
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"unsupported-sub",
            "payload":{"kind":"futureSubscription"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let unsupported = receive_frame(&mut ws).await;
    assert_eq!(unsupported.payload["code"], json!("unsupportedMessage"));
    assert_eq!(
        unsupported.subscription_id.as_ref().unwrap().as_str(),
        "unsupported-sub"
    );
    assert_eq!(unsupported.payload["scope"], json!("subscription"));
    assert_eq!(
        unsupported.payload["correlationId"],
        json!("unsupported-sub")
    );

    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"invalid-sub", "payload":null
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let invalid = receive_frame(&mut ws).await;
    assert_eq!(invalid.payload["code"], json!("invalidRequest"));
    assert_eq!(
        invalid.subscription_id.as_ref().unwrap().as_str(),
        "invalid-sub"
    );
    assert_eq!(invalid.payload["scope"], json!("subscription"));
    assert_eq!(invalid.payload["correlationId"], json!("invalid-sub"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_is_bounded_when_peer_does_not_read_large_response() {
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            graceful_flush_timeout: Duration::from_millis(20),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(HugeKubernetes, "huge-server"),
    )
    .await
    .unwrap();
    let mut ws = connect(&server).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"request", "requestId":"huge",
            "payload":{"kind":"bootstrap"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_millis(500), server.shutdown())
        .await
        .expect("shutdown must not wait forever on a blocked websocket sink")
        .unwrap();
    drop(ws);
}
