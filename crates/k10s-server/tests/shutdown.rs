//! Black-box verification of the approved graceful-shutdown order and log redaction.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{ClientFrame, ClientKind, RequestId, ServerFrame, ServerKind, ServerPayload};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ACCESS_TOKEN: &str = "super-secret-access-token";

/// Shared tracing capture used by every test in this binary.
///
/// Callsite interest is resolved against whichever dispatcher exists when a
/// callsite first evaluates, process-wide. Installing a subscriber in *every*
/// test keeps that resolution deterministic under the default parallel harness
/// instead of poisoning sibling tests with `Interest::never`.
#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn install_capture() -> (tracing::subscriber::DefaultGuard, Arc<Mutex<Vec<u8>>>) {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(Capture(Arc::clone(&buffer)))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, buffer)
}

fn shutdown_config() -> ServerConfig {
    ServerConfig {
        access_token: ACCESS_TOKEN.into(),
        drain_grace_timeout: Duration::from_secs(2),
        ..ServerConfig::default()
    }
}

fn hello_frame() -> String {
    serde_json::to_string(&ClientFrame {
        kind: ClientKind::Hello,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: json!({
            "protocolMajor": 1,
            "protocolMinor": 9,
            "capabilities": ["logs.tail"],
            "accessToken": ACCESS_TOKEN,
        }),
    })
    .unwrap()
}

async fn connect_authenticated(server_addr: SocketAddr) -> Ws {
    let (mut ws, _) = connect_async(format!("ws://{server_addr}{}", k10s_protocol::CONTROL_PATH))
        .await
        .unwrap();
    ws.send(Message::Text(hello_frame().into())).await.unwrap();
    let welcome: ServerFrame =
        serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(welcome.kind, ServerKind::Welcome);
    ws
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("frame deadline")
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
    )
    .unwrap()
}

/// Minimal HTTP/1.1 probe straight over TCP so the listener lifecycle stays black-box.
async fn http_probe(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(addr).await.ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut raw))
        .await
        .ok()?
        .ok()?;
    let raw = String::from_utf8(raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body.to_owned()))
}

async fn tcp_connectable(addr: SocketAddr) -> bool {
    TcpStream::connect(addr).await.is_ok()
}

#[tokio::test]
async fn shutdown_follows_the_approved_order_and_closes_the_listener() {
    let (_capture, _) = install_capture();
    let server = spawn_loopback(
        shutdown_config(),
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();
    let mut ws = connect_authenticated(addr).await;

    let shutdown = tokio::spawn(server.shutdown());

    // Stage 1: readiness flips to 503/draining before anything else observable.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((status, body)) = http_probe(addr, "/readyz").await
                && status == 503
                && body.contains("draining")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("readyz must report draining");

    // Stage 2: the listener still answers, but application upgrades are refused.
    assert!(tcp_connectable(addr).await, "listener closes last");
    let rejection = connect_async(format!("ws://{addr}{}", k10s_protocol::CONTROL_PATH))
        .await
        .unwrap_err();
    assert!(
        rejection.to_string().contains("503"),
        "application connections must be rejected during drain: {rejection}"
    );

    // Stage 3: the surviving session receives an explicit shutdown notice.
    let notice = receive_frame(&mut ws).await;
    assert_eq!(notice.kind, ServerKind::ShutdownNotice);
    let ServerPayload::ShutdownNotice(payload) = notice.decode_payload().unwrap() else {
        panic!("expected a typed shutdown notice");
    };
    assert!(!payload.reason.is_empty());

    // The notice is only fired after not-ready and acceptance-closed stages, so
    // a draining probe must already be observable the instant it arrives.
    let readyz = http_probe(addr, "/readyz").await.expect("listener open");
    assert_eq!(readyz.0, 503);
    assert!(readyz.1.contains("draining"));

    // Stage 4: mutations are rejected while status reads keep working.
    ws.send(Message::Text(
        json!({"kind":"request","requestId":"mutation-1","payload":{"kind":"exec"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let mutation = receive_frame(&mut ws).await;
    assert_eq!(mutation.kind, ServerKind::Error);
    assert_eq!(
        mutation.request_id,
        Some(RequestId::from("mutation-1")),
        "the rejection must correlate with the rejected request"
    );
    assert_eq!(mutation.payload["code"], json!("cancelled"));

    ws.send(Message::Text(
        json!({"kind":"request","requestId":"read-1","payload":{"kind":"bootstrap","deadline":2000}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let status_read = receive_frame(&mut ws).await;
    assert_eq!(status_read.kind, ServerKind::Response);
    assert_eq!(
        status_read.request_id,
        Some(RequestId::from("read-1")),
        "status reads must survive the drain window"
    );

    // /healthz stays live for the whole drain because the listener closes last.
    let health = http_probe(addr, "/healthz")
        .await
        .expect("healthz reachable");
    assert_eq!(health, (200, "ok\n".to_owned()));

    drop(ws);
    tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("shutdown must finish once the drain completes")
        .unwrap()
        .unwrap();

    // Final stage: the listener itself is gone.
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tcp_connectable(addr).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    deadline -= Duration::from_secs(1);
    assert!(!tcp_connectable(addr).await, "listener must be closed");
    assert!(http_probe(addr, "/healthz").await.is_none());
}

#[tokio::test]
async fn captured_tracing_output_never_contains_the_access_token() {
    let (_capture, buffer) = install_capture();

    let server = spawn_loopback(
        shutdown_config(),
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();
    let mut ws = connect_authenticated(addr).await;
    let shutdown = tokio::spawn(server.shutdown());
    let notice = receive_frame(&mut ws).await;
    assert_eq!(notice.kind, ServerKind::ShutdownNotice);

    // Drain progress is published through the shared readiness probe; the open
    // socket keeps the tracker busy, so draining is observable deterministically.
    let mut flipped = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Some((status, _)) = http_probe(addr, "/readyz").await
            && status == 503
        {
            flipped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(flipped, "readyz must expose the drain phase");

    drop(ws);
    tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("shutdown joins")
        .unwrap()
        .unwrap();

    let captured = String::from_utf8(
        buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap();
    assert!(
        captured.contains("control session authenticated"),
        "expected captured tracing output, got: {captured:?}"
    );
    assert!(
        captured.contains("shutdown"),
        "expected lifecycle shutdown telemetry, got: {captured:?}"
    );
    assert!(!captured.contains(ACCESS_TOKEN));
    assert!(!captured.contains("kubeconfig"));
}

#[tokio::test]
async fn embedded_desktop_launch_shuts_down_through_the_same_lifecycle() {
    let (_capture, _) = install_capture();
    let server = spawn_loopback(
        ServerConfig {
            access_token: ACCESS_TOKEN.into(),
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();
    assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    server.shutdown().await.unwrap();
    assert!(!tcp_connectable(addr).await);
}

#[tokio::test]
async fn drain_deadline_bounds_shutdown_even_when_a_socket_holds_its_grace_window() {
    let (_capture, _) = install_capture();
    let drain_timeout = Duration::from_millis(400);
    let graceful_flush = ServerConfig::default().graceful_flush_timeout;
    let server = spawn_loopback(
        ServerConfig {
            access_token: ACCESS_TOKEN.into(),
            drain_grace_timeout: Duration::from_secs(30),
            drain_timeout,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();
    let mut ws = connect_authenticated(addr).await;

    let started = std::time::Instant::now();
    // The connected socket wants 30s of grace, but the drain deadline is one
    // absolute bound measured from shutdown start.
    let result = tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("shutdown must stay bounded by the drain deadline");
    let elapsed = started.elapsed();

    assert!(
        matches!(&result, Err(error) if error.kind() == std::io::ErrorKind::TimedOut),
        "an abandoned socket must surface as a timed-out shutdown: {result:?}"
    );
    assert!(
        elapsed >= drain_timeout,
        "shutdown returned before the configured deadline: {elapsed:?}"
    );
    // One deadline plus one bounded forced-unwind window, with scheduler slack.
    let bound = drain_timeout + graceful_flush * 2 + Duration::from_millis(500);
    assert!(
        elapsed < bound,
        "shutdown exceeded the deadline plus one unwind window: {elapsed:?} (bound {bound:?})"
    );

    // No socket task may outlive shutdown(): the surviving session must observe
    // its connection being torn down instead of hanging in the grace window.
    // Frames enqueued before the deadline (the notice itself) may still arrive;
    // a hang or live data stream past this point would mean surviving tasks.
    let tail = tokio::time::timeout(graceful_flush * 4, async {
        while let Some(message) = ws.next().await {
            if matches!(message, Err(_) | Ok(Message::Close(_))) {
                return;
            }
        }
    })
    .await;
    assert!(
        tail.is_ok(),
        "socket task survived shutdown(): no teardown observed"
    );
    drop(ws);
}

#[tokio::test]
async fn healthz_stays_reachable_through_forced_teardown() {
    let (_capture, _) = install_capture();
    let drain_timeout = Duration::from_millis(400);
    let graceful_flush = ServerConfig::default().graceful_flush_timeout;
    let server = spawn_loopback(
        ServerConfig {
            access_token: ACCESS_TOKEN.into(),
            drain_grace_timeout: Duration::from_secs(30),
            drain_timeout,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();
    let mut ws = connect_authenticated(addr).await;

    let started = std::time::Instant::now();
    let mut shutdown = tokio::spawn(server.shutdown());
    let notice = receive_frame(&mut ws).await;
    assert_eq!(notice.kind, ServerKind::ShutdownNotice);

    // Probe /healthz until shutdown resolves: forced teardown must run while
    // the listener is still serving, so at least one late probe succeeds.
    let mut healthy_after_notice = false;
    let outcome = loop {
        if let Some((status, _)) = http_probe(addr, "/healthz").await {
            assert_eq!(status, 200, "healthz must answer through forced teardown");
            healthy_after_notice = true;
        }
        match tokio::time::timeout(Duration::from_millis(20), &mut shutdown).await {
            Ok(joined) => break joined.unwrap(),
            Err(_) => continue,
        }
    };
    let elapsed = started.elapsed();

    assert!(
        matches!(&outcome, Err(error) if error.kind() == std::io::ErrorKind::TimedOut),
        "an abandoned socket must surface as a timed-out shutdown: {outcome:?}"
    );
    assert!(elapsed >= drain_timeout);
    let bound = drain_timeout + graceful_flush * 2 + Duration::from_millis(500);
    assert!(
        elapsed < bound,
        "shutdown exceeded the deadline plus one unwind window: {elapsed:?} (bound {bound:?})"
    );
    assert!(
        healthy_after_notice,
        "/healthz stopped answering before the listener closed"
    );
    drop(ws);

    // Listener closes last: only after forced teardown has fully completed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tcp_connectable(addr).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!tcp_connectable(addr).await);
    assert!(http_probe(addr, "/healthz").await.is_none());
}

#[tokio::test]
async fn accepted_upgrade_racing_shutdown_cannot_outlive_it() {
    let (_capture, _) = install_capture();
    let drain_timeout = Duration::from_millis(300);
    let graceful_flush = ServerConfig::default().graceful_flush_timeout;
    let server = spawn_loopback(
        ServerConfig {
            access_token: ACCESS_TOKEN.into(),
            drain_grace_timeout: Duration::from_secs(30),
            drain_timeout,
            ..ServerConfig::default()
        },
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let addr = server.addr();

    // A raw handshake request racing shutdown itself: the write, the server's
    // accept/register path, and the drain all interleave nondeterministically.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        k10s_protocol::CONTROL_PATH
    );
    let started = std::time::Instant::now();
    let shutdown = tokio::spawn(async move {
        // Give the request a head start at registering, then shut down while
        // the upgrade is still in flight.
        tokio::time::sleep(Duration::from_millis(5)).await;
        server.shutdown().await
    });
    stream.write_all(request.as_bytes()).await.unwrap();
    let response = read_response_head(&mut stream).await;
    assert!(
        response.is_empty()
            || response.starts_with("HTTP/1.1 101")
            || response.starts_with("HTTP/1.1 503"),
        "unexpected probe response during shutdown race: {response:?}"
    );
    let outcome = tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("shutdown must stay bounded despite the racing upgrade")
        .unwrap();
    let elapsed = started.elapsed();

    // The racing upgrade is either swept into the drain (Ok) or, if it lands
    // late enough, swept up by the hard deadline (TimedOut). Both outcomes are
    // bounded; surviving past shutdown() is not acceptable.
    assert!(
        outcome.is_ok()
            || matches!(&outcome, Err(error) if error.kind() == std::io::ErrorKind::TimedOut),
        "unexpected shutdown outcome: {outcome:?}"
    );
    let bound = drain_timeout + graceful_flush * 3 + Duration::from_millis(500);
    assert!(
        elapsed < bound,
        "shutdown exceeded the deadline plus one unwind window: {elapsed:?} (bound {bound:?})"
    );

    // The half-open racing session must observe teardown instead of lingering.
    let tail = tokio::time::timeout(graceful_flush * 4, async {
        let mut rest = Vec::new();
        let _ = stream.read_to_end(&mut rest).await;
    })
    .await;
    assert!(tail.is_ok(), "racing session outlived shutdown()");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tcp_connectable(addr).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!tcp_connectable(addr).await, "listener must be closed");
}

/// Read an HTTP response head byte-wise so a stalled handshake cannot hang the test.
async fn read_response_head(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 64];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .expect("response head deadline")
            .expect("response head read");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(buffer).unwrap_or_default()
}
