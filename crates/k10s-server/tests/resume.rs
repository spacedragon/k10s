//! Resume-journal integration tests for control sessions.
//!
//! The journal is an optimization over the existing full-resync path: any
//! gap that cannot be filled must fall back to a fresh session and today's
//! reconnect behavior, never to partial or reordered replay.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes, Gvk, KubernetesAccess, Query as BackendQuery};
use k10s_protocol::{ResumeStatus, ServerFrame, ServerKind, Welcome};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const INSTANCE: &str = "resume-server";

fn pod_gvk() -> Gvk {
    Gvk {
        group: String::new(),
        version: "v1".to_owned(),
        kind: "Pod".to_owned(),
    }
}

async fn server_with(config: ServerConfig) -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let kernel = BackendKernel::new_with_instance_id(fake.clone(), INSTANCE);
    let handle = spawn_loopback(config, kernel).await.unwrap();
    (handle, fake)
}

async fn default_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    server_with(ServerConfig {
        access_token: "secret".into(),
        ..ServerConfig::default()
    })
    .await
}

fn hello(extra: serde_json::Value) -> String {
    let mut payload = json!({
        "protocolMajor": 1,
        "protocolMinor": 1,
        "capabilities": ["logs.tail"],
        "accessToken": "secret",
    });
    for (key, value) in extra.as_object().expect("resume fields must be an object") {
        payload[key] = value.clone();
    }
    serde_json::to_string(&json!({ "kind": "hello", "payload": payload })).unwrap()
}

fn resume_fields(session: &str, instance: &str, cursor: u64) -> serde_json::Value {
    json!({
        "serverInstanceId": instance,
        "sessionId": session,
        "lastAckedSequence": cursor,
    })
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

async fn handshake(ws: &mut Ws, extra: serde_json::Value) -> Welcome {
    ws.send(Message::Text(hello(extra).into())).await.unwrap();
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Welcome);
    serde_json::from_value(frame.payload).expect("welcome decodes")
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("frame arrives within deadline")
        .expect("socket stays open for this frame")
        .expect("socket frame decodes");
    let message = match message {
        Message::Text(text) => text,
        other => panic!("expected text frame, got {other:?}"),
    };
    serde_json::from_str(&message).unwrap()
}

/// Send `subscribe` for the dev-local default Pods, drain the sequenced
/// snapshot, and return its final sequence.
async fn subscribe_pods(ws: &mut Ws) -> u64 {
    ws.send(Message::Text(
        json!({
            "kind": "subscribe", "subscriptionId": "res-1",
            "payload": {
                "kind": "resource",
                "context": "dev-local",
                "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                "namespace": "default"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(ws).await.kind, ServerKind::Subscribed);
    let begin = receive_frame(ws).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    let total_chunks: usize = begin.payload["totalChunks"].as_u64().expect("chunk count") as usize;
    for _ in 0..total_chunks {
        assert_eq!(receive_frame(ws).await.kind, ServerKind::SnapshotChunk);
    }
    let end = receive_frame(ws).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);
    end.sequence.expect("snapshot end is sequenced")
}

/// Pod names in the fake world's dev-local/default namespace.
async fn pod_names(fake: &FakeKubernetes) -> Vec<String> {
    let data = match fake
        .query(BackendQuery::ResourceList {
            context: "dev-local".into(),
            gvk: pod_gvk(),
            namespace: Some("default".into()),
        })
        .await
    {
        Ok(k10s_backend::QueryResult::ResourceList(data)) => data,
        other => panic!("expected a resource list from the fake world: {other:?}"),
    };
    assert!(!data.rows.is_empty(), "fake world has default pods");
    data.rows
        .iter()
        .map(|row| row.reference.name.clone())
        .collect()
}

/// Touch one pod through the shared fake world and read its event frame.
async fn touch_and_read_event(ws: &mut Ws, fake: &FakeKubernetes, name: &str) -> ServerFrame {
    let gvk = pod_gvk();
    assert!(
        fake.touch_resource("dev-local", &gvk, Some("default"), name)
            .is_some()
    );
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Event);
    frame
}

/// Send one Ack whose envelope sequence matches its contiguous cursor.
async fn send_ack(ws: &mut Ws, cursor: u64) {
    ws.send(Message::Text(
        json!({
            "kind": "ack",
            "sequence": cursor,
            "payload": {"lastAckedSequence": cursor},
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

/// Read the next frame or None if nothing arrives within 400ms.
async fn peek_frame(ws: &mut Ws) -> Option<ServerFrame> {
    let message = tokio::time::timeout(Duration::from_millis(400), ws.next())
        .await
        .ok()?;
    let text = message?.ok()?.into_text().ok()?;
    serde_json::from_str::<ServerFrame>(&text).ok()
}

#[tokio::test]
async fn resume_replays_only_unacked_frames_with_contiguous_acks() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;

    // First transport: sequenced traffic 1..=N+2 around the ack cursor.
    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    let session_id = welcome.session_id.as_str().to_owned();

    let snapshot_end = subscribe_pods(&mut first).await;
    touch_and_read_event(&mut first, &fake, &pods[0]).await; // seq N+1 (unacked)
    send_ack(&mut first, snapshot_end).await; // contiguous ack at N
    touch_and_read_event(&mut first, &fake, &pods[1]).await; // seq N+2 (unacked)

    drop(first);

    // Resume exactly at the contiguous ack cursor.
    let mut resumed = connect(&server).await;
    let welcome = handshake(
        &mut resumed,
        resume_fields(&session_id, INSTANCE, snapshot_end),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Resumed);
    assert_eq!(
        welcome.session_id.as_str(),
        session_id,
        "a fillable cursor resumes the same session"
    );

    let mut replayed = Vec::new();
    for _ in 0..2 {
        replayed.push(receive_frame(&mut resumed).await.sequence.unwrap());
    }
    assert_eq!(
        replayed,
        vec![snapshot_end + 1, snapshot_end + 2],
        "replay is contiguous from the ack cursor with no gaps"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn count_budget_bounds_the_replayable_window() {
    let config = ServerConfig {
        access_token: "secret".into(),
        resume_max_journal_entries: 4,
        ..ServerConfig::default()
    };
    let (server, fake) = server_with(config).await;
    let pods = pod_names(&fake).await;

    // Snapshot frames plus touches push the earliest entries out of the
    // four-slot journal.
    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();

    let snapshot_end = subscribe_pods(&mut first).await; // >= 5 sequenced frames
    touch_and_read_event(&mut first, &fake, &pods[0]).await;
    touch_and_read_event(&mut first, &fake, &pods[1]).await;
    touch_and_read_event(&mut first, &fake, &pods[2]).await; // last_sent >= N+3

    drop(first);

    // A cursor inside the evicted region cannot be filled: fresh session.
    let mut stale = connect(&server).await;
    let welcome = handshake(&mut stale, resume_fields(&session_id, INSTANCE, 0)).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(
        welcome.session_id.as_str(),
        session_id,
        "an unfillable cursor starts a fresh session"
    );

    // A cursor at the journal's edge still resumes with only the retained tail.
    let mut resumed = connect(&server).await;
    let welcome = handshake(
        &mut resumed,
        resume_fields(&session_id, INSTANCE, snapshot_end + 2),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Resumed);
    assert_eq!(welcome.session_id.as_str(), session_id);
    let replayed = receive_frame(&mut resumed).await.sequence.unwrap();
    assert_eq!(replayed, snapshot_end + 3, "only the retained tail replays");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_larger_than_outbound_queue_falls_back_before_welcome() {
    let config = ServerConfig {
        access_token: "secret".into(),
        resume_max_journal_entries: 128,
        outbound_queue_capacity: 64,
        ..ServerConfig::default()
    };
    let (server, fake) = server_with(config).await;
    let pods = pod_names(&fake).await;
    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    let snapshot_end = subscribe_pods(&mut first).await;

    for index in 0..70 {
        touch_and_read_event(&mut first, &fake, &pods[index % pods.len()]).await;
    }
    drop(first);

    let mut resumed = connect(&server).await;
    let welcome = handshake(
        &mut resumed,
        resume_fields(&session_id, INSTANCE, snapshot_end),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(welcome.session_id.as_str(), session_id);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn age_budget_expires_replayable_entries() {
    let config = ServerConfig {
        access_token: "secret".into(),
        resume_entry_max_age: Duration::from_millis(80),
        ..ServerConfig::default()
    };
    let (server, fake) = server_with(config).await;
    let pods = pod_names(&fake).await;

    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    subscribe_pods(&mut first).await;
    touch_and_read_event(&mut first, &fake, &pods[0]).await;

    drop(first);
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Every journaled frame is now older than the budget: full resync only.
    let mut expired = connect(&server).await;
    let welcome = handshake(&mut expired, resume_fields(&session_id, INSTANCE, 1)).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(
        welcome.session_id.as_str(),
        session_id,
        "aged entries cannot replay"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn aged_live_session_cannot_resume_with_an_empty_incomplete_tail() {
    let config = ServerConfig {
        access_token: "secret".into(),
        resume_entry_max_age: Duration::from_millis(80),
        ..ServerConfig::default()
    };
    let (server, _fake) = server_with(config).await;
    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    subscribe_pods(&mut first).await;

    tokio::time::sleep(Duration::from_millis(250)).await;

    let mut takeover = connect(&server).await;
    let welcome = handshake(&mut takeover, resume_fields(&session_id, INSTANCE, 0)).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(welcome.session_id.as_str(), session_id);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn expired_cursor_falls_back_to_full_resync() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;

    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    subscribe_pods(&mut first).await;
    touch_and_read_event(&mut first, &fake, &pods[0]).await;

    drop(first);

    // A cursor beyond anything this server sent is invalid: fresh session.
    let mut resumed = connect(&server).await;
    let welcome = handshake(&mut resumed, resume_fields(&session_id, INSTANCE, 9_999)).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(
        welcome.session_id.as_str(),
        session_id,
        "invalid cursor cannot resume"
    );

    // The full-resync path still serves bootstrap on the new session.
    let request_id = k10s_protocol::RequestId::from("req-resume");
    resumed
        .send(Message::Text(
            json!({
                "kind": "request", "requestId": request_id.as_str(),
                "payload": {"kind": "bootstrap"}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let response = receive_frame(&mut resumed).await;
    assert_eq!(response.kind, ServerKind::Response);
    assert!(
        !response.payload["contexts"]
            .as_array()
            .is_none_or(|items| items.is_empty()),
        "bootstrap still answers after a resume fallback"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_server_instance_or_unknown_session_is_rejected() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;

    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    subscribe_pods(&mut first).await;
    touch_and_read_event(&mut first, &fake, &pods[0]).await;
    drop(first);

    // A fillable cursor on the wrong server instance must not resume.
    let mut foreign = connect(&server).await;
    let welcome = handshake(
        &mut foreign,
        resume_fields(&session_id, "another-instance", 1),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(
        welcome.session_id.as_str(),
        session_id,
        "a wrong instance ID rejects the resume"
    );

    // An unknown session ID falls back to a fresh session as well.
    let mut ghost = connect(&server).await;
    let welcome = handshake(&mut ghost, resume_fields("ghost-session", INSTANCE, 1)).await;
    assert_eq!(welcome.resume_status, ResumeStatus::Fresh);
    assert_ne!(welcome.session_id.as_str(), "ghost-session");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_takeover_replaces_the_live_transport() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;
    let gvk = pod_gvk();

    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    let snapshot_end = subscribe_pods(&mut first).await; // 1..=N
    touch_and_read_event(&mut first, &fake, &pods[0]).await; // N+1
    send_ack(&mut first, snapshot_end.saturating_sub(1)).await;

    // A second transport claims the same session while the first is live.
    let mut taken = connect(&server).await;
    let welcome = handshake(
        &mut taken,
        resume_fields(&session_id, INSTANCE, snapshot_end.saturating_sub(1)),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Resumed);
    assert_eq!(welcome.session_id.as_str(), session_id);

    // Replay covers exactly the frames after the ack cursor.
    let mut replayed = Vec::new();
    for _ in 0..2 {
        replayed.push(receive_frame(&mut taken).await.sequence.unwrap());
    }
    assert_eq!(replayed, vec![snapshot_end, snapshot_end + 1]);

    // The old transport is terminated by the server.
    let next = tokio::time::timeout(Duration::from_secs(5), first.next())
        .await
        .expect("old transport reacts to takeover");
    match next {
        Some(Ok(Message::Close(Some(frame)))) => {
            assert!(
                frame.reason.contains("resumed"),
                "takeover close must explain itself, got: {}",
                frame.reason
            );
        }
        other => panic!("expected a server close on the old transport, got {other:?}"),
    }

    // The client rebuild path reissues desired subscriptions after reconnect.
    let rebuilt_end = subscribe_pods(&mut taken).await;

    // The session continues under the new lease with fresh sequences.
    assert!(
        fake.touch_resource("dev-local", &gvk, Some("default"), &pods[1])
            .is_some()
    );
    let continued = receive_frame(&mut taken).await;
    assert_eq!(continued.sequence, Some(rebuilt_end + 1));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_resume_never_replays_acked_frames() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;

    let mut first = connect(&server).await;
    let welcome = handshake(&mut first, json!({})).await;
    let session_id = welcome.session_id.as_str().to_owned();
    let snapshot_end = subscribe_pods(&mut first).await; // 1..=N
    touch_and_read_event(&mut first, &fake, &pods[0]).await; // N+1
    send_ack(&mut first, snapshot_end + 1).await; // ack everything sent

    drop(first);

    // Resume at a fully-acked cursor: nothing may be replayed.
    let mut resumed = connect(&server).await;
    let welcome = handshake(
        &mut resumed,
        resume_fields(&session_id, INSTANCE, snapshot_end + 1),
    )
    .await;
    assert_eq!(welcome.resume_status, ResumeStatus::Resumed);
    assert_eq!(welcome.session_id.as_str(), session_id);

    let silent = peek_frame(&mut resumed).await;
    assert!(
        silent.is_none(),
        "no replayed frame at a fully-acked cursor"
    );

    // Reconnect recovery reissues desired subscriptions before new traffic.
    let rebuilt_end = subscribe_pods(&mut resumed).await;

    // New traffic continues contiguously after the resume.
    let continued = touch_and_read_event(&mut resumed, &fake, &pods[1]).await;
    assert_eq!(continued.sequence, Some(rebuilt_end + 1));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_sessions_stay_independent_when_no_resume_fields() {
    let (server, _fake) = default_server().await;

    let mut a = connect(&server).await;
    let welcome_a = handshake(&mut a, json!({})).await;
    assert_eq!(welcome_a.resume_status, ResumeStatus::Fresh);

    let mut b = connect(&server).await;
    let welcome_b = handshake(&mut b, json!({})).await;
    assert_eq!(welcome_b.resume_status, ResumeStatus::Fresh);
    assert_ne!(
        welcome_a.session_id.as_str(),
        welcome_b.session_id.as_str(),
        "each fresh connection gets its own session"
    );

    // Bootstrap still round-trips on a plain fresh session.
    let request_id = k10s_protocol::RequestId::from("req-fresh");
    b.send(Message::Text(
        json!({
            "kind": "request", "requestId": request_id.as_str(),
            "payload": {"kind": "bootstrap"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let response = receive_frame(&mut b).await;
    assert_eq!(response.kind, ServerKind::Response);
    assert!(
        !response.payload["contexts"]
            .as_array()
            .is_none_or(|items| items.is_empty()),
        "bootstrap still answers on fresh sessions"
    );

    server.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Deterministic resume-sequence property check (Task 1 step 4 stand-in):
// across a fixed pattern of touches, disconnects and resumes the wire-level
// sequence contract must hold on every transport, and the final projection
// must equal a fresh snapshot's.
// ---------------------------------------------------------------------------

use std::collections::BTreeMap;

/// Mirrors k10s-ui's client rules at protocol level: tracks the contiguous
/// ack cursor, applies snapshots/deltas with a baseline requirement, resets
/// on a Fresh welcome exactly like the UI does, and offers resume fields.
#[derive(Debug)]
struct ModelClient {
    session_id: Option<String>,
    server_instance_id: Option<String>,
    last_acked_sequence: Option<u64>,
    rows: BTreeMap<String, u64>, // resource name -> applied revision
}

impl ModelClient {
    fn new() -> Self {
        Self {
            session_id: None,
            server_instance_id: None,
            last_acked_sequence: None,
            rows: BTreeMap::new(),
        }
    }

    /// Build the hello payload with resume fields exactly when retained.
    fn hello_payload(&self) -> serde_json::Value {
        let mut extra = serde_json::Map::new();
        if let Some(instance) = &self.server_instance_id {
            extra.insert(
                "serverInstanceId".to_owned(),
                serde_json::Value::String(instance.clone()),
            );
        }
        if let Some(session) = &self.session_id {
            extra.insert("sessionId".to_owned(), session.as_str().into());
        }
        if let Some(cursor) = self.last_acked_sequence {
            extra.insert(
                "lastAckedSequence".to_owned(),
                serde_json::Value::Number(cursor.into()),
            );
        }
        let mut hello = json!({
            "protocolMajor": 1,
            "protocolMinor": 1,
            "capabilities": ["logs.tail"],
            "accessToken": "secret",
        });
        for (key, value) in extra {
            hello[key] = value;
        }
        json!({ "kind": "hello", "payload": hello })
    }

    /// Apply one frame with the UI's continuity and baseline rules. A broken
    /// resume (gap, duplicate, out-of-order replay) fails the test here.
    fn apply(&mut self, frame: ServerFrame) {
        if let Some(sequence) = frame.sequence {
            let expected = self
                .last_acked_sequence
                .map_or(1, |ack| ack.saturating_add(1));
            assert_eq!(
                sequence, expected,
                "sequenced continuity broken: expected {expected}, got {sequence}"
            );
        }
        let payload = frame.decode_payload().expect("frame decodes");
        match &payload {
            k10s_protocol::ServerPayload::Welcome(welcome) => {
                if welcome.resume_status == ResumeStatus::Fresh {
                    self.last_acked_sequence = None;
                    self.rows.clear();
                }
                self.session_id = Some(welcome.session_id.as_str().to_owned());
                self.server_instance_id = Some(welcome.server_instance_id.clone());
            }
            k10s_protocol::ServerPayload::SnapshotChunk(chunk) => {
                let page: k10s_protocol::ResourceSnapshotPage =
                    serde_json::from_value(chunk.data.clone()).unwrap();
                for row in &page.rows {
                    self.rows.insert(
                        row.identity.name.clone(),
                        page.revision.get().max(row.revision.get()),
                    );
                }
            }
            k10s_protocol::ServerPayload::Event(event) => match event.event_kind.as_str() {
                "resource.changed" => {
                    let changed: k10s_protocol::ResourceChanged =
                        serde_json::from_value(event.payload.clone()).unwrap();
                    // Same baseline rule as the UI: deltas need a snapshot.
                    if self.rows.contains_key(&changed.identity.name) {
                        self.rows
                            .insert(changed.identity.name.clone(), changed.row.revision.get());
                    }
                }
                "resource.gone" => {
                    let gone: k10s_protocol::ResourceGone =
                        serde_json::from_value(event.payload.clone()).unwrap();
                    self.rows.remove(&gone.identity.name);
                }
                _ => {}
            },
            _ => {}
        }
        if let Some(sequence) = frame.sequence {
            self.last_acked_sequence = Some(sequence);
        }
    }

    /// Authenticate over an open transport (resuming when possible) and drain
    /// one full Pod-watch snapshot, applying every frame in arrival order.
    async fn hello_and_rebuild(&mut self, ws: &mut Ws, server: &k10s_server::ServerHandle) {
        let _ = server; // kept for symmetry with the connect helper
        ws.send(Message::Text(
            serde_json::to_string(&self.hello_payload()).unwrap().into(),
        ))
        .await
        .expect("hello sends");
        self.apply(receive_frame(ws).await);

        send_subscribe(ws, "res-model").await;
        loop {
            let frame = receive_frame(ws).await;
            let kind = frame.kind;
            self.apply(frame);
            if kind == ServerKind::SnapshotEnd {
                return;
            }
        }
    }
}

async fn send_subscribe(ws: &mut Ws, subscription_id: &str) {
    ws.send(Message::Text(
        json!({
            "kind": "subscribe", "subscriptionId": subscription_id,
            "payload": {
                "kind": "resource",
                "context": "dev-local",
                "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                "namespace": "default"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

/// Read frames until the changed-event for `name` is applied, or deadline.
async fn await_event_for(client: &mut ModelClient, ws: &mut Ws, name: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline.into(), peek_frame(ws)).await {
            Ok(Some(frame)) => {
                let is_target = frame.kind == ServerKind::Event && event_targets(&frame, name);
                client.apply(frame);
                if is_target {
                    return true;
                }
            }
            _ => return false,
        }
    }
}

fn event_targets(frame: &ServerFrame, name: &str) -> bool {
    frame
        .payload
        .get("payload")
        .and_then(|inner| inner.get("identity"))
        .and_then(|identity| identity.get("name"))
        .and_then(serde_json::Value::as_str)
        == Some(name)
}

#[tokio::test]
async fn resume_sequence_contract_holds_and_projection_matches_fresh_snapshot() {
    let (server, fake) = default_server().await;
    let pods = pod_names(&fake).await;
    let gvk = pod_gvk();
    assert!(pods.len() > 8, "scenario needs at least nine pods");

    // Transport A: baseline projection plus acknowledged traffic.
    let mut client = ModelClient::new();
    let mut ws_a = connect(&server).await;
    client.hello_and_rebuild(&mut ws_a, &server).await;
    for name in [&pods[0], &pods[1]] {
        assert!(
            fake.touch_resource("dev-local", &gvk, Some("default"), name)
                .is_some()
        );
        assert!(await_event_for(&mut client, &mut ws_a, name).await);
    }

    // Two more touches whose events A never reads: resume must replay them.
    for name in [&pods[2], &pods[3]] {
        assert!(
            fake.touch_resource("dev-local", &gvk, Some("default"), name)
                .is_some()
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await; // let frames flush
    drop(ws_a);

    // Transport B resumes the same session and rebuilds its projection.
    let mut ws_b = connect(&server).await;
    client.hello_and_rebuild(&mut ws_b, &server).await;
    for name in [&pods[4], &pods[5]] {
        assert!(
            fake.touch_resource("dev-local", &gvk, Some("default"), name)
                .is_some()
        );
        assert!(await_event_for(&mut client, &mut ws_b, name).await);
    }

    // Second abrupt disconnect with one unread event.
    assert!(
        fake.touch_resource("dev-local", &gvk, Some("default"), &pods[6])
            .is_some()
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(ws_b);

    // Transport C resumes again after the second disconnect.
    let mut ws_c = connect(&server).await;
    client.hello_and_rebuild(&mut ws_c, &server).await;

    // Reference: a brand-new session reading the same final world.
    let mut reference = ModelClient::new();
    let mut ws_ref = connect(&server).await;
    reference.hello_and_rebuild(&mut ws_ref, &server).await;

    assert_eq!(
        client.rows, reference.rows,
        "resumed projection must equal a fresh snapshot"
    );
    assert!(!client.rows.is_empty());

    server.shutdown().await.unwrap();
}
