//! Opt-in, credential-free live-cluster stability smoke.
//!
//! The test reads only `K10S_LIVE_CONTEXT`; Kubernetes credentials continue
//! to be discovered by the same default kubeconfig path as the desktop app.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, KubeAdapter};
use k10s_protocol::{
    Ack, BootstrapResponse, ClientFrame, ClientKind, ContextSwitchRequest, ContextSwitchResponse,
    ErrorFrame, GroupVersionKind, PROTOCOL_MAJOR, PROTOCOL_MINOR, ResourceSnapshotPage,
    ResourceTypesRequest, ResourceTypesResponse, ResourceWatchSpec, ServerFrame, ServerKind,
    SnapshotBegin, SnapshotChunk, SubscriptionSelector,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const STABILITY_WINDOW: Duration = Duration::from_secs(60);

#[tokio::test]
#[ignore = "live cluster: set K10S_LIVE_CONTEXT=<kube-context> and pass --ignored"]
async fn selected_live_context_completes_a_pod_snapshot_and_stays_stable() {
    let context = std::env::var("K10S_LIVE_CONTEXT").unwrap_or_else(|_| {
        panic!(
            "K10S_LIVE_CONTEXT is required; run: \
             K10S_LIVE_CONTEXT=<kube-context> cargo test -p k10s-server \
             --test live_context -- --ignored --nocapture"
        )
    });
    assert!(
        !context.trim().is_empty(),
        "K10S_LIVE_CONTEXT must not be empty"
    );

    let token = Uuid::new_v4().to_string();
    let adapter = KubeAdapter::from_kubeconfig(None)
        .expect("the desktop-default kubeconfig must load for the live smoke");
    let server = spawn_loopback(
        ServerConfig {
            access_token: token.clone(),
            ..ServerConfig::default()
        },
        BackendKernel::new(adapter),
    )
    .await
    .expect("the loopback server must start");

    let outcome = run_smoke(&server, &token, &context).await;
    server
        .shutdown()
        .await
        .expect("the live-smoke loopback server must shut down cleanly");
    if let Err(reason) = outcome {
        panic!("live context stability smoke failed: {reason}");
    }
}

async fn run_smoke(
    server: &k10s_server::ServerHandle,
    token: &str,
    wanted_context: &str,
) -> Result<(), String> {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .map_err(|_| "control WebSocket connection failed".to_owned())?;

    send_json(
        &mut ws,
        &ClientFrame {
            kind: ClientKind::Hello,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: json!({
                "protocolMajor": PROTOCOL_MAJOR,
                "protocolMinor": PROTOCOL_MINOR,
                "capabilities": [],
                "accessToken": token,
            }),
        },
    )
    .await?;
    expect_kind(&mut ws, ServerKind::Welcome, None, None)
        .await
        .map_err(|reason| format!("hello: {reason}"))?;

    send_request(&mut ws, "live-bootstrap", "bootstrap", json!({})).await?;
    let bootstrap_frame = expect_kind(&mut ws, ServerKind::Response, Some("live-bootstrap"), None)
        .await
        .map_err(|reason| format!("bootstrap: {reason}"))?;
    let bootstrap: BootstrapResponse = bootstrap_frame
        .decode_response_payload()
        .map_err(|_| "bootstrap response was invalid".to_owned())?;
    let selected = bootstrap
        .contexts
        .iter()
        .find(|entry| entry.name == wanted_context)
        .ok_or_else(|| {
            "K10S_LIVE_CONTEXT was not present in desktop-default kubeconfig".to_owned()
        })?;
    let namespace = selected
        .namespace
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "default".to_owned());

    if !selected.is_current {
        send_request(
            &mut ws,
            "live-switch",
            k10s_protocol::REQUEST_CONTEXT_SWITCH,
            serde_json::to_value(ContextSwitchRequest {
                to: wanted_context.to_owned(),
            })
            .map_err(|_| "context switch request could not serialize".to_owned())?,
        )
        .await?;
        let frame = expect_kind(&mut ws, ServerKind::Response, Some("live-switch"), None)
            .await
            .map_err(|reason| format!("context switch: {reason}"))?;
        let switched: ContextSwitchResponse = frame
            .decode_response_payload()
            .map_err(|_| "context switch response was invalid".to_owned())?;
        if switched.current != wanted_context {
            return Err("context switch committed an unexpected context".to_owned());
        }
    }

    send_request(
        &mut ws,
        "live-types",
        "resource.types",
        serde_json::to_value(ResourceTypesRequest {
            context: wanted_context.to_owned(),
        })
        .map_err(|_| "resource.types request could not serialize".to_owned())?,
    )
    .await?;
    let types_frame = expect_kind(&mut ws, ServerKind::Response, Some("live-types"), None)
        .await
        .map_err(|reason| format!("resource.types: {reason}"))?;
    let types: ResourceTypesResponse = types_frame
        .decode_response_payload()
        .map_err(|_| "resource.types response was invalid".to_owned())?;
    if !types
        .types
        .iter()
        .any(|entry| entry.gvk == GroupVersionKind::core("v1", "Pod") && entry.namespaced)
    {
        return Err("resource.types did not advertise namespaced core/v1 Pods".to_owned());
    }

    send_json(
        &mut ws,
        &ClientFrame {
            kind: ClientKind::Subscribe,
            request_id: None,
            subscription_id: Some("live-pods".into()),
            sequence: None,
            payload: serde_json::to_value(SubscriptionSelector::Resource(ResourceWatchSpec {
                context: wanted_context.to_owned(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                namespace: Some(namespace),
            }))
            .map_err(|_| "Pod selector could not serialize".to_owned())?,
        },
    )
    .await?;

    let mut last_sequence = None;
    let subscribed = receive_checked(&mut ws, &mut last_sequence, FRAME_TIMEOUT)
        .await
        .map_err(|reason| format!("Pod subscription: {reason}"))?;
    require_frame(&subscribed, ServerKind::Subscribed, None, Some("live-pods"))?;
    ack_latest(&mut ws, last_sequence).await?;

    let begin = receive_checked(&mut ws, &mut last_sequence, FRAME_TIMEOUT).await?;
    require_frame(&begin, ServerKind::SnapshotBegin, None, Some("live-pods"))?;
    let begin: SnapshotBegin = serde_json::from_value(begin.payload)
        .map_err(|_| "snapshotBegin payload was invalid".to_owned())?;
    ack_latest(&mut ws, last_sequence).await?;

    let mut pages = Vec::with_capacity(begin.total_chunks as usize);
    for expected_index in 0..begin.total_chunks {
        let frame = receive_checked(&mut ws, &mut last_sequence, FRAME_TIMEOUT).await?;
        require_frame(&frame, ServerKind::SnapshotChunk, None, Some("live-pods"))?;
        let chunk: SnapshotChunk = serde_json::from_value(frame.payload)
            .map_err(|_| "snapshotChunk payload was invalid".to_owned())?;
        if chunk.chunk_index != expected_index {
            return Err("snapshot chunks were not contiguous".to_owned());
        }
        let page: ResourceSnapshotPage = serde_json::from_value(chunk.data)
            .map_err(|_| "snapshot page was invalid".to_owned())?;
        pages.push(page);
        ack_latest(&mut ws, last_sequence).await?;
    }
    if pages.len() != begin.total_chunks as usize {
        return Err("snapshot reassembly was incomplete".to_owned());
    }
    let end = receive_checked(&mut ws, &mut last_sequence, FRAME_TIMEOUT).await?;
    require_frame(&end, ServerKind::SnapshotEnd, None, Some("live-pods"))?;
    ack_latest(&mut ws, last_sequence).await?;

    let deadline = Instant::now() + STABILITY_WINDOW;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_secs(5));
        match tokio::time::timeout(wait, receive_message(&mut ws)).await {
            Ok(Ok(frame)) => {
                inspect_runtime_frame(&frame)?;
                check_sequence(&frame, &mut last_sequence)?;
                ack_latest(&mut ws, last_sequence).await?;
            }
            Ok(Err(reason)) => return Err(reason),
            Err(_) => send_ping(&mut ws).await?,
        }
    }
    Ok(())
}

async fn send_request(ws: &mut Ws, id: &str, kind: &str, payload: Value) -> Result<(), String> {
    send_json(
        ws,
        &ClientFrame {
            kind: ClientKind::Request,
            request_id: Some(id.into()),
            subscription_id: None,
            sequence: None,
            payload: json!({"kind": kind, "payload": payload}),
        },
    )
    .await
}

async fn send_ping(ws: &mut Ws) -> Result<(), String> {
    send_json(
        ws,
        &ClientFrame {
            kind: ClientKind::Ping,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: Value::Null,
        },
    )
    .await
}

async fn ack_latest(ws: &mut Ws, sequence: Option<u64>) -> Result<(), String> {
    if let Some(last_acked_sequence) = sequence {
        send_json(
            ws,
            &ClientFrame {
                kind: ClientKind::Ack,
                request_id: None,
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(Ack {
                    last_acked_sequence,
                })
                .map_err(|_| "ACK could not serialize".to_owned())?,
            },
        )
        .await?;
    }
    Ok(())
}

async fn send_json(ws: &mut Ws, value: &impl Serialize) -> Result<(), String> {
    let text = serde_json::to_string(value).map_err(|_| "client frame could not serialize")?;
    ws.send(Message::Text(text.into()))
        .await
        .map_err(|_| "control WebSocket send failed".to_owned())
}

async fn expect_kind(
    ws: &mut Ws,
    kind: ServerKind,
    request_id: Option<&str>,
    subscription_id: Option<&str>,
) -> Result<ServerFrame, String> {
    let frame = receive_message_with_timeout(ws, FRAME_TIMEOUT).await?;
    inspect_runtime_frame(&frame)?;
    require_frame(&frame, kind, request_id, subscription_id)?;
    Ok(frame)
}

async fn receive_checked(
    ws: &mut Ws,
    last_sequence: &mut Option<u64>,
    timeout: Duration,
) -> Result<ServerFrame, String> {
    let frame = receive_message_with_timeout(ws, timeout).await?;
    inspect_runtime_frame(&frame)?;
    check_sequence(&frame, last_sequence)?;
    Ok(frame)
}

async fn receive_message_with_timeout(
    ws: &mut Ws,
    timeout: Duration,
) -> Result<ServerFrame, String> {
    tokio::time::timeout(timeout, receive_message(ws))
        .await
        .map_err(|_| "timed out waiting for a server frame".to_owned())?
}

async fn receive_message(ws: &mut Ws) -> Result<ServerFrame, String> {
    let message = ws
        .next()
        .await
        .ok_or_else(|| "control WebSocket closed unexpectedly".to_owned())?
        .map_err(|_| "control WebSocket read failed".to_owned())?;
    match message {
        Message::Text(text) => serde_json::from_str(&text)
            .map_err(|_| "server sent an invalid control frame".to_owned()),
        Message::Close(_) => Err("server closed the control WebSocket".to_owned()),
        Message::Ping(_) | Message::Pong(_) => {
            Err("unexpected WebSocket transport frame".to_owned())
        }
        Message::Binary(_) | Message::Frame(_) => {
            Err("server sent a non-text control frame".to_owned())
        }
    }
}

fn inspect_runtime_frame(frame: &ServerFrame) -> Result<(), String> {
    match frame.kind {
        ServerKind::Error => {
            let error: ErrorFrame = serde_json::from_value(frame.payload.clone())
                .map_err(|_| "server returned an invalid error frame".to_owned())?;
            Err(format!(
                "server returned protocol error {:?} ({:?}/{:?}): {}",
                error.code, error.scope, error.retryability, error.safe_message
            ))
        }
        ServerKind::ResyncRequired => Err("server demanded an unexpected resync".to_owned()),
        ServerKind::ShutdownNotice => Err("server announced an unexpected shutdown".to_owned()),
        _ => Ok(()),
    }
}

fn check_sequence(frame: &ServerFrame, last_sequence: &mut Option<u64>) -> Result<(), String> {
    let Some(sequence) = frame.sequence else {
        return Ok(());
    };
    if let Some(previous) = *last_sequence
        && sequence != previous + 1
    {
        return Err("server sequence contained a gap".to_owned());
    }
    *last_sequence = Some(sequence);
    Ok(())
}

fn require_frame(
    frame: &ServerFrame,
    kind: ServerKind,
    request_id: Option<&str>,
    subscription_id: Option<&str>,
) -> Result<(), String> {
    if frame.kind != kind
        || frame.request_id.as_ref().map(|id| id.as_str()) != request_id
        || frame.subscription_id.as_ref().map(|id| id.as_str()) != subscription_id
    {
        return Err("server returned an unexpected frame envelope".to_owned());
    }
    Ok(())
}
