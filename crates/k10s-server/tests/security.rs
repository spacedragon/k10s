//! Security hardening suite for token configuration and localhost/web exposure.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::{SinkExt, StreamExt};
use k10s_protocol::CONTROL_PATH;
use k10s_server::{ServerConfig, spawn_loopback};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

static FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Temporary access-token file with guaranteed unique name and cleanup.
struct TempTokenFile(PathBuf);

impl TempTokenFile {
    fn create(content: &str) -> Self {
        let id = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "k10s-security-test-{}-{id}.txt",
            std::process::id()
        ));
        std::fs::write(&path, content).expect("test fixture writes token file");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempTokenFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Send a raw WebSocket upgrade request and return the HTTP status code.
async fn raw_upgrade_status(addr: std::net::SocketAddr, extra_headers: &[&str]) -> u16 {
    let lines: Vec<Vec<u8>> = extra_headers
        .iter()
        .map(|line| line.as_bytes().to_vec())
        .collect();
    raw_upgrade_status_with_lines(addr, &lines).await
}

/// Like `raw_upgrade_status`, but takes raw header-line bytes so tests can
/// embed non-UTF-8 sequences that stringly typed headers would not allow.
async fn raw_upgrade_status_with_lines(
    addr: std::net::SocketAddr,
    extra_headers: &[Vec<u8>],
) -> u16 {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut body = format!(
        "GET {CONTROL_PATH} HTTP/1.1\r\nHost: {addr}\r\n\
         Upgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBkYXRh\r\nSec-WebSocket-Version: 13\r\n"
    )
    .into_bytes();
    for line in extra_headers {
        body.extend_from_slice(line);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"\r\n");
    stream.write_all(&body).await.unwrap();
    let mut response_line = String::new();
    let mut buf: [u8; 1] = [0; 1];
    loop {
        stream.read_exact(&mut buf).await.unwrap();
        if buf[0] == b'\n' || response_line.len() > 256 {
            break;
        }
        response_line.push(buf[0] as char);
    }
    // "HTTP/1.1 403 Forbidden" style; the reason phrase may contain spaces.
    let parts: Vec<&str> = response_line.split_whitespace().collect();
    assert!(
        parts.len() >= 2 && parts[0].starts_with("HTTP/"),
        "status line well formed: {response_line:?}"
    );
    parts[1].parse().expect("numeric status code")
}

async fn origin_server() -> k10s_server::ServerHandle {
    spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            hello_timeout: std::time::Duration::from_millis(400),
            ..ServerConfig::default()
        },
        k10s_backend::BackendKernel::new(k10s_backend::FakeKubernetes::standard()),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn same_origin_upgrade_is_accepted() {
    let server = origin_server().await;
    let addr = server.addr();
    let origin_header = format!("Origin: http://{addr}");
    let status = raw_upgrade_status(addr, &[origin_header.as_str()]).await;
    assert_eq!(status, 101, "same-origin upgrade must switch protocols");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_origin_upgrade_is_rejected() {
    let server = origin_server().await;
    let addr = server.addr();
    let status = raw_upgrade_status(
        addr,
        &[
            "Origin: http://evil.example.com",
            "Referer: https://evil.example.com/x",
        ],
    )
    .await;
    assert_eq!(status, 403, "cross-origin upgrade must be forbidden");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn path_bearing_same_authority_origin_is_rejected() {
    let server = origin_server().await;
    let addr = server.addr();
    // A same authority plus a path is not what browsers ever send in Origin;
    // it must fail closed even though the embedded host matches exactly.
    let status = raw_upgrade_status(addr, &[format!("Origin: http://{addr}/admin").as_str()]).await;
    assert_eq!(status, 403, "path-bearing Origin must be forbidden");

    // A clean same-origin origin still upgrades.
    let clean = format!("Origin: http://{addr}");
    let accepted = raw_upgrade_status(addr, &[clean.as_str()]).await;
    assert_eq!(
        accepted, 101,
        "clean same-origin upgrade must switch protocols"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_authority_origin_is_rejected() {
    let server = origin_server().await;
    let addr = server.addr();
    // A trailing-colon authority like http://127.0.0.1: is not a form browsers
    // ever emit and must fail closed with 403 on CONTROL_PATH.
    let malformed_status =
        raw_upgrade_status(addr, &[format!("Origin: http://{addr}:").as_str()]).await;
    assert_eq!(malformed_status, 403, "empty-port Origin must be forbidden");

    // A clean same-origin origin still upgrades.
    let clean = format!("Origin: http://{addr}");
    let accepted = raw_upgrade_status(addr, &[clean.as_str()]).await;
    assert_eq!(
        accepted, 101,
        "clean same-origin upgrade must switch protocols"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_client_without_origin_still_connects() {
    let server = origin_server().await;
    let status = raw_upgrade_status(server.addr(), &[]).await;
    assert_eq!(
        status, 101,
        "native clients send no Origin and must connect"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn undecodable_origin_header_is_rejected() {
    let server = origin_server().await;
    // Invalid UTF-8 bytes in a present Origin header must be rejected, not
    // laundered into the absent-Origin allowance for native clients.
    let status =
        raw_upgrade_status_with_lines(server.addr(), &[b"Origin: \xff\xfe\x80".to_vec()]).await;
    assert_eq!(status, 403, "undecodable Origin must be forbidden");

    // The no-Origin contract is unchanged by the fail-closed rule above.
    let native_status = raw_upgrade_status_with_lines(server.addr(), &[]).await;
    assert_eq!(native_status, 101, "no-Origin upgrades still admitted");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn blank_or_whitespace_only_origins_are_rejected() {
    let server = origin_server().await;
    // A present Origin with an empty value must be 403 on CONTROL_PATH, not
    // laundered into the absent-Origin allowance for native clients.
    let blank_status = raw_upgrade_status(server.addr(), &["Origin:"]).await;
    assert_eq!(blank_status, 403, "empty-valued Origin must be forbidden");

    // Whitespace-only values (\r\n-style padding) fail closed the same way.
    let padded_status =
        raw_upgrade_status_with_lines(server.addr(), &[b"Origin: \t ".to_vec()]).await;
    assert_eq!(
        padded_status, 403,
        "whitespace-only Origin must be forbidden"
    );

    // The no-Origin contract is unchanged by the fail-closed rule.
    let native_status = raw_upgrade_status(server.addr(), &[]).await;
    assert_eq!(native_status, 101, "no-Origin upgrades still admitted");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn spoofed_proxy_headers_do_not_change_outcomes() {
    let server = origin_server().await;
    // With matching same-origin header the upgrade still succeeds even when
    // proxy headers claim a different client.
    let addr = server.addr();
    let origin_header = format!("Origin: http://{addr}");
    let spoofed = raw_upgrade_status(
        addr,
        &[
            origin_header.as_str(),
            "X-Forwarded-For: 8.8.8.8",
            "X-Real-Ip: 9.9.9.9",
        ],
    )
    .await;
    assert_eq!(spoofed, 101, "proxy headers are ignored by default policy");

    // And a cross-origin request stays rejected with or without them.
    let rejected = raw_upgrade_status(
        addr,
        &[
            "Origin: http://evil.example.com",
            "X-Forwarded-For: 127.0.0.1",
        ],
    )
    .await;
    assert_eq!(rejected, 403);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn hello_deadline_closes_slow_client() {
    let server = origin_server().await; // hello_timeout = 400ms
    let (mut ws, _) = connect_async(format!("ws://{}{CONTROL_PATH}", server.addr()))
        .await
        .unwrap();
    // Send nothing: the first-frame deadline must close us.
    let started = std::time::Instant::now();
    let close_frame = loop {
        match ws.next().await {
            None => panic!("stream ended without a close frame"),
            Some(Ok(Message::Close(Some(frame)))) => break frame,
            Some(Ok(_)) => {} // no Hello means nothing but the close may arrive
            Some(Err(error)) => panic!("unexpected protocol error: {error}"),
        }
    };
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "hello deadline must fire well before 3s"
    );
    assert!(close_frame.reason.contains("hello"), "{close_frame:?}");
    server.shutdown().await.unwrap();
}

#[test]
fn server_config_debug_redacts_access_token() {
    let config = ServerConfig {
        access_token: "probe-secret".into(),
        ..ServerConfig::default()
    };
    let rendered = format!("{config:?}");
    assert!(!rendered.contains("probe-secret"), "{rendered}");
}

#[tokio::test]
async fn unauthenticated_connection_cap_enforced_at_runtime() {
    // cap=2: the third concurrent upgrade must be refused while two sessions
    // still await their Hello frames (hello_timeout is long so permits stay held).
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            hello_timeout: std::time::Duration::from_secs(10),
            max_unauthenticated_connections: 2,
            ..ServerConfig::default()
        },
        k10s_backend::BackendKernel::new(k10s_backend::FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let url = format!("ws://{}{CONTROL_PATH}", server.addr());
    let (a, b, c) = tokio::join!(
        connect_async(url.clone()),
        connect_async(url.clone()),
        connect_async(url)
    );
    // Collect only the refused attempts; successful streams drop at scope end.
    let mut errors: Vec<String> = Vec::new();
    for outcome in [&a, &b, &c] {
        if let Err(error) = outcome {
            errors.push(error.to_string());
        }
    }
    assert_eq!(
        errors.len(),
        1,
        "exactly one upgrade must be refused: {errors:?}"
    );
    server.shutdown().await.unwrap();
}

#[test]
fn token_file_precedence_over_env_token() {
    let file = TempTokenFile::create("file-token\n");
    let resolved = k10s_server::resolve_access_token(Some("env-token"), Some(file.path()))
        .expect("both sources valid must resolve");
    assert_eq!(resolved.as_deref(), Some("file-token"));
}

#[test]
fn env_token_used_when_no_file_configured() {
    let resolved =
        k10s_server::resolve_access_token(Some("env-token"), None).expect("valid env token");
    assert_eq!(resolved.as_deref(), Some("env-token"));
}

#[test]
fn absent_or_empty_sources_yield_none_for_loopback_dev() {
    assert_eq!(
        k10s_server::resolve_access_token(None, None)
            .expect("no sources is a valid loopback configuration"),
        None
    );
    assert_eq!(
        k10s_server::resolve_access_token(Some(""), None)
            .expect("empty env value counts as absent"),
        None
    );
}

#[test]
fn token_file_content_is_trimmed_of_surrounding_whitespace() {
    let file = TempTokenFile::create("  tok-123\n");
    assert_eq!(
        k10s_server::resolve_access_token(None, Some(file.path()))
            .expect("token file resolves")
            .as_deref(),
        Some("tok-123"),
    );
}

#[test]
fn empty_token_file_is_rejected_with_safe_error() {
    let file = TempTokenFile::create("  \n\t\n");
    let error = k10s_server::resolve_access_token(None, Some(file.path()))
        .expect_err("whitespace-only token is not a secret");
    assert!(
        !error.to_string().contains('\t'),
        "no raw file content in diagnostics"
    );
}

#[test]
fn missing_token_file_is_rejected_with_safe_error() {
    let phantom = std::env::temp_dir().join(format!(
        "k10s-security-test-{}-does-not-exist.txt",
        std::process::id()
    ));
    let error = k10s_server::resolve_access_token(None, Some(&phantom))
        .expect_err("unreadable token file must refuse to start");
    assert!(
        error.to_string().contains("token"),
        "diagnostic names the failing source: {error}"
    );
}

#[tokio::test]
async fn access_token_never_emitted_in_server_output() {
    let probe = "leak-probe-token-xyz";
    let server = spawn_loopback(
        ServerConfig {
            access_token: probe.into(),
            hello_timeout: std::time::Duration::from_secs(5),
            ..ServerConfig::default()
        },
        k10s_backend::BackendKernel::new(k10s_backend::FakeKubernetes::standard()),
    )
    .await
    .unwrap();

    // Happy path: every frame the server sends after a correct Hello must be
    // credential-free.
    let mut ws = connect_async(format!("ws://{}{CONTROL_PATH}", server.addr()))
        .await
        .expect("control socket connects")
        .0;
    ws.send(Message::Text(
        serde_json::json!({
            "kind": "hello",
            "payload": {
                "protocolMajor": 1,
                "protocolMinor": 9,
                "capabilities": ["logs.tail"],
                "accessToken": probe
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let welcome = loop {
        let Some(Ok(message)) = ws.next().await else {
            panic!("connection closed before welcome");
        };
        if let Some(welcome_text) = message
            .into_text()
            .ok()
            .filter(|text| text.as_str().contains("\"welcome\""))
        {
            break welcome_text.as_str().to_owned();
        }
    };
    assert!(
        !welcome.contains(probe),
        "welcome echoes the token: {welcome}"
    );

    // Error path: a rejected Hello must not leak either credential.
    let mut impostor = connect_async(format!("ws://{}{CONTROL_PATH}", server.addr()))
        .await
        .expect("control socket connects")
        .0;
    impostor
        .send(Message::Text(
            serde_json::json!({
                "kind": "hello",
                "payload": {
                    "protocolMajor": 1,
                    "protocolMinor": 9,
                    "capabilities": ["logs.tail"],
                    "accessToken": "wrong-token"
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let mut saw_error = false;
    for _ in 0..16 {
        let Some(message) = ws_or_close(&mut impostor).await else {
            break;
        };
        if !saw_error && message.contains("\"error\"") {
            saw_error = true;
            assert!(
                !message.contains(probe),
                "terminal error leaks the real token: {message}"
            );
            assert!(
                !message.contains("wrong-token"),
                "terminal error echoes the submitted token: {message}"
            );
        }
    }
    assert!(saw_error, "impostor must receive a terminal error frame");
    server.shutdown().await.unwrap();
}

async fn ws_or_close(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Option<String> {
    match ws.next().await {
        Some(Ok(message)) => message
            .into_text()
            .ok()
            .map(|text| text.as_str().to_owned()),
        _ => None,
    }
}
