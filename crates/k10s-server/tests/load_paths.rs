//! Opt-in production-path capacity gates run by `tests/load/run.rs`.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, FakeKubernetes, KubeAdapter};
use k10s_protocol::{
    LOGS_PATH, ResourceSnapshotPage, ServerFrame, ServerKind, StreamServerMessage,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(server: &k10s_server::ServerHandle, path: &str) -> Ws {
    connect_async(format!("ws://{}{}", server.addr(), path))
        .await
        .unwrap()
        .0
}

async fn receive_text(ws: &mut Ws) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("expected text, got {other:?}"),
        }
    }
}

async fn authenticate(ws: &mut Ws) {
    ws.send(Message::Text(
        json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_text(ws).await["kind"], "welcome");
}

#[tokio::test]
#[ignore = "run through tests/load/run.rs"]
async fn control_socket_delivers_the_complete_chunked_50k_snapshot() {
    const OBJECTS: usize = 50_000;
    const EXPECTED_ROWS: usize = OBJECTS * 3 / 8;
    const PAGE_ROWS: usize = 7;
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            snapshot_rows_per_chunk: PAGE_ROWS,
            // The load profile intentionally selects seven-row pages, so its
            // bounded scheduler must admit the complete 2,679-frame initial
            // snapshot while the socket writer drains concurrently.
            outbound_queue_capacity: EXPECTED_ROWS.div_ceil(PAGE_ROWS) + 16,
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(
            FakeKubernetes::with_capacity(OBJECTS, 1_000),
            "load-snapshot",
        ),
    )
    .await
    .unwrap();
    let mut ws = connect(&server, k10s_protocol::CONTROL_PATH).await;
    authenticate(&mut ws).await;
    ws.send(Message::Text(
        json!({
            "kind":"subscribe", "subscriptionId":"load-pods",
            "payload":{"kind":"resource","context":"dev-local","gvk":{"group":"","version":"v1","kind":"Pod"}}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let expected_chunks = EXPECTED_ROWS.div_ceil(PAGE_ROWS);
    let mut chunks = 0_usize;
    let mut rows = 0_usize;
    let mut saw_begin = false;
    let mut checksum = None;
    tokio::time::timeout(Duration::from_secs(30), async {
        while checksum.is_none() {
            let value = receive_text(&mut ws).await;
            let frame: ServerFrame = serde_json::from_value(value).unwrap();
            match frame.kind {
                ServerKind::Subscribed => {}
                ServerKind::SnapshotBegin => {
                    assert!(!saw_begin);
                    saw_begin = true;
                    assert_eq!(frame.payload["totalChunks"], json!(expected_chunks));
                }
                ServerKind::SnapshotChunk => {
                    assert!(saw_begin);
                    assert_eq!(frame.payload["chunkIndex"], json!(chunks));
                    let page: ResourceSnapshotPage =
                        serde_json::from_value(frame.payload["data"].clone()).unwrap();
                    assert!(page.rows.len() <= PAGE_ROWS);
                    rows += page.rows.len();
                    chunks += 1;
                }
                ServerKind::SnapshotEnd => {
                    checksum = frame.payload["checksum"].as_str().map(str::to_owned);
                }
                other => panic!("unexpected snapshot frame {other:?}"),
            }
        }
    })
    .await
    .expect("production snapshot path must meet the 30 second budget");
    assert_eq!(chunks, expected_chunks);
    assert_eq!(rows, EXPECTED_ROWS);
    assert!(checksum.is_some_and(|checksum| !checksum.is_empty()));
    server.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "run through tests/load/run.rs"]
async fn dedicated_log_socket_bounds_a_slow_consumer_under_10_mib_source() {
    const POD: &str = "/api/v1/namespaces/default/pods/web";
    const LOG: &str = "/api/v1/namespaces/default/pods/web/log";
    const SOURCE_BYTES: usize = 10 * 1024 * 1024;
    const RATE_BUDGET: usize = 512 * 1024;
    let recorded = RecordedApiServer::standard();
    recorded.set_method_response(
        "GET",
        POD,
        200,
        r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"7"},"spec":{"containers":[{"name":"app","image":"busybox"}]}}"#,
    );
    let line = format!("{}\n", "x".repeat(1023));
    let body = line.repeat(SOURCE_BYTES / line.len());
    assert_eq!(body.len(), SOURCE_BYTES);
    recorded.set_method_response("GET", LOG, 200, &body);
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "recorded".into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("recorded", recorded.into_client("default"))],
    )
    .unwrap();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            stream_rate_budget_bytes_per_sec: RATE_BUDGET,
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(adapter, "load-logs"),
    )
    .await
    .unwrap();

    let mut control = connect(&server, k10s_protocol::CONTROL_PATH).await;
    authenticate(&mut control).await;
    control
        .send(Message::Text(
            json!({
                "kind":"request", "requestId":"load-logs",
                "payload":{"kind":k10s_protocol::REQUEST_STREAM_TICKET,"payload":{
                    "target":{"context":"recorded","namespace":"default","pod":"web","uid":"uid-web","container":"app"},
                    "streamType":"logs","tty":false,"tailLines":null,"sinceSeconds":null,"timestamps":false,"follow":true
                }}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let ticket_response = receive_text(&mut control).await;
    let ticket = ticket_response["payload"]["ticketId"].as_str().unwrap();
    let mut logs = connect(&server, LOGS_PATH).await;
    logs.send(Message::Text(
        json!({"kind":"hello","protocolMajor":1,"accessToken":"secret","streamTicket":ticket})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let ready: StreamServerMessage = serde_json::from_value(receive_text(&mut logs).await).unwrap();
    assert!(matches!(ready, StreamServerMessage::Ready { .. }));

    // Deliberately stop reading while the real Kubernetes log producer,
    // bounded backend queue, binary framing, and socket rate budget interact.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut delivered = 0_usize;
    let mut explicit_overload = false;
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(message) = logs.next().await {
            match message.unwrap() {
                Message::Binary(payload) => {
                    let decoded = k10s_protocol::decode_stream_payload(&payload).unwrap();
                    delivered += decoded.data.len();
                }
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    explicit_overload |= value["message"].as_str().is_some_and(|message| {
                        message.contains("budget") || message.contains("overload")
                    });
                }
                Message::Close(frame) => {
                    explicit_overload |= frame.is_some_and(|frame| {
                        frame.reason.contains("budget") || frame.reason.contains("overload")
                    });
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("slow log consumer must terminate explicitly within the load budget");
    assert!(
        explicit_overload,
        "slow log path must report bounded overload"
    );
    assert!(
        delivered <= RATE_BUDGET,
        "rate budget retained too much log data"
    );
    server.shutdown().await.unwrap();
}
