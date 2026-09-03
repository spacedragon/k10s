//! Dedicated log socket and major-1 exec compatibility tombstone coverage.
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendKernel, Command, FakeKubernetes, KubernetesAccess, OperationId, Query,
    QueryResult, Subscribe, SubscriptionHandle,
};
use k10s_protocol::{ErrorCode, StreamTarget, StreamTicketRequest, StreamType};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone)]
struct DispatchSpy {
    inner: FakeKubernetes,
    queries: Arc<AtomicUsize>,
    subscriptions: Arc<AtomicUsize>,
}

impl KubernetesAccess for DispatchSpy {
    fn query<'a>(
        &'a self,
        request: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        self.inner.query(request)
    }

    fn execute<'a>(
        &'a self,
        command: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        self.inner.execute(command)
    }

    fn subscribe<'a>(
        &'a self,
        request: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        self.inner.subscribe(request)
    }
}

#[derive(Debug, Clone)]
struct DispatchCounts {
    queries: Arc<AtomicUsize>,
    subscriptions: Arc<AtomicUsize>,
}

async fn spy_server() -> (k10s_server::ServerHandle, DispatchCounts) {
    let counts = DispatchCounts {
        queries: Arc::new(AtomicUsize::new(0)),
        subscriptions: Arc::new(AtomicUsize::new(0)),
    };
    let adapter = DispatchSpy {
        inner: FakeKubernetes::standard(),
        queries: Arc::clone(&counts.queries),
        subscriptions: Arc::clone(&counts.subscriptions),
    };
    let handle = k10s_server::spawn_loopback(
        k10s_server::ServerConfig {
            access_token: "secret".into(),
            ..Default::default()
        },
        BackendKernel::new_with_instance_id(adapter, "stream-tombstone-spy"),
    )
    .await
    .unwrap();
    (handle, counts)
}

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
    let (handle, counts) = spy_server().await;
    let mut ws = control(&handle).await;
    counts.queries.store(0, Ordering::SeqCst);
    counts.subscriptions.store(0, Ordering::SeqCst);
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
    assert_eq!(counts.queries.load(Ordering::SeqCst), 0);
    assert_eq!(counts.subscriptions.load(Ordering::SeqCst), 0);
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn exec_tombstone_authenticates_but_never_redeems_arbitrary_ticket() {
    let (handle, counts) = spy_server().await;
    counts.queries.store(0, Ordering::SeqCst);
    counts.subscriptions.store(0, Ordering::SeqCst);
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
    assert_eq!(counts.queries.load(Ordering::SeqCst), 0);
    assert_eq!(counts.subscriptions.load(Ordering::SeqCst), 0);
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
