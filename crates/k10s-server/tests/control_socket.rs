use std::{future::Future, pin::Pin, time::Duration};

use futures_util::{SinkExt, StreamExt};
use k10s_backend::port::{BootstrapInfo, ContextInfo};
use k10s_backend::{
    BackendError, BackendKernel, Command, FakeKubernetes, KubernetesAccess, OperationId, Query,
    QueryResult, Subscribe, SubscriptionHandle,
};
use k10s_protocol::{
    ClientFrame, ClientKind, ErrorCode, RequestId, Retryability, ServerFrame, ServerKind,
    ServerPayload,
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
