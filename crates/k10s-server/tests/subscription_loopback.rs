//! Connected subscription loopback: chunked snapshots and P2 deltas through
//! the real control socket into the shared client state, plus a forced
//! socket drop proving the Plan 1 baseline reconnect performs a full
//! bootstrap/resubscribe/resync while preserving windows, filters, and the
//! retained applied list.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    ClientFrame, ClientKind, ClientPayload, GroupVersionKind, ServerFrame, ServerKind,
    SnapshotBegin, SnapshotChunk,
};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::{ClientConfig, ClientPhase, ClientState, ConnectTarget};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn deployments() -> GroupVersionKind {
    GroupVersionKind {
        group: "apps".into(),
        version: "v1".into(),
        kind: "Deployment".into(),
    }
}

fn backend_gvk(gvk: &GroupVersionKind) -> k10s_backend::Gvk {
    k10s_backend::Gvk {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
    }
}

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "subscription-server"),
    )
    .await
    .unwrap();
    (handle, fake)
}

async fn send_client_frame(ws: &mut Ws, frame: &ClientFrame) {
    ws.send(Message::Text(serde_json::to_string(frame).unwrap().into()))
        .await
        .unwrap();
}

/// Flush every queued client frame to the socket.
async fn flush_outbound(ws: &mut Ws, client: &mut ClientState) {
    while let Some(frame) = client.take_outbound() {
        send_client_frame(ws, &frame).await;
    }
}

async fn recv_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

/// Apply inbound frames to the client (echoing its outbound traffic back)
/// until the predicate observes a frame; returns that final frame.
async fn pump_until(
    ws: &mut Ws,
    client: &mut ClientState,
    done: impl Fn(&ServerFrame) -> bool,
) -> ServerFrame {
    loop {
        let frame = recv_frame(ws).await;
        let finished = done(&frame);
        client
            .apply_at(frame.clone(), 1_000, 7)
            .unwrap_or_else(|error| panic!("client rejected {frame:?}: {error}"));
        flush_outbound(ws, client).await;
        if finished {
            return frame;
        }
    }
}

async fn ready_client(server: &k10s_server::ServerHandle) -> (Ws, ClientState) {
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let mut client = ClientState::new(ClientConfig::default());
    client.connect(ConnectTarget::new(url, "secret")).unwrap();
    let mut ws =
        connect_async(format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH).as_str())
            .await
            .unwrap()
            .0;
    flush_outbound(&mut ws, &mut client).await;
    pump_until(&mut ws, &mut client, |frame| {
        frame.kind == ServerKind::Welcome
    })
    .await;
    assert_eq!(client.phase(), ClientPhase::Ready);
    (ws, client)
}

#[tokio::test]
async fn snapshots_and_deltas_apply_into_retained_client_lists() {
    let (server, fake) = spawn_server().await;
    let (mut ws, mut client) = ready_client(&server).await;

    let subscription = client
        .subscribe_resource(
            "dev-local",
            "apps",
            "v1",
            "Deployment",
            Some("default".into()),
        )
        .unwrap();
    flush_outbound(&mut ws, &mut client).await;

    // The P1 subscription lifecycle stays lossless ahead of the P2 stream:
    // Subscribed is sequenced before the bounded snapshot pages.
    let subscribed = recv_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);
    let subscribed_sequence = subscribed.sequence.expect("lifecycle frames are sequenced");
    client.apply(subscribed).unwrap();

    let end = pump_until(&mut ws, &mut client, |frame| {
        matches!(
            frame.decode_payload(),
            Ok(k10s_protocol::ServerPayload::SnapshotEnd(_))
        )
    })
    .await;
    let end_sequence = end.sequence.expect("snapshot end is sequenced");
    assert!(end_sequence > subscribed_sequence);

    let list = client
        .resource_list(subscription.id())
        .expect("the snapshot completes into the retained list");
    let names: Vec<_> = list.rows().map(|row| row.identity.name.as_str()).collect();
    assert_eq!(names, vec!["api-server", "web-frontend"]);
    assert!(list.revision().unwrap().get() > 0);

    // A changed delta updates the retained row in place.
    let touched = fake
        .touch_resource(
            "dev-local",
            &backend_gvk(&deployments()),
            Some("default"),
            "api-server",
        )
        .expect("api-server exists");
    let changed = pump_until(&mut ws, &mut client, |frame| {
        frame.kind == ServerKind::Event
    })
    .await;
    assert_eq!(
        changed.sequence,
        Some(end_sequence + 1),
        "P2 deltas keep contiguous connection sequences"
    );
    let list = client.resource_list(subscription.id()).unwrap();
    assert_eq!(list.revision().unwrap().get(), touched);
    let api = list
        .rows()
        .find(|row| row.identity.name == "api-server")
        .unwrap();
    assert_eq!(api.revision.get(), touched);

    // Re-applying the same delta is stale and must be ignored.
    client.apply(changed.clone()).unwrap();
    assert_eq!(
        client
            .resource_list(subscription.id())
            .unwrap()
            .revision()
            .unwrap()
            .get(),
        touched,
        "a replayed delta must never regress the applied revision"
    );

    // A gone delta removes the row from the retained list.
    assert!(fake.delete_resource(
        "dev-local",
        &backend_gvk(&deployments()),
        Some("default"),
        "web-frontend",
    ));
    pump_until(&mut ws, &mut client, |frame| {
        frame.kind == ServerKind::Event
    })
    .await;
    let list = client.resource_list(subscription.id()).unwrap();
    let names: Vec<_> = list.rows().map(|row| row.identity.name.as_str()).collect();
    assert_eq!(names, vec!["api-server"]);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn forced_socket_drop_recovers_through_bootstrap_resubscribe_and_resync() {
    let (server, _fake) = spawn_server().await;
    let (_ws, mut client) = ready_client(&server).await;

    // Local UI state survives the loss; windows and filters are exactly this
    // retained state plus the desired subscription set.
    client.local_ui_mut().selected_context = Some("dev-local".into());
    let subscription = client
        .subscribe_resource(
            "dev-local",
            "apps",
            "v1",
            "Deployment",
            Some("default".into()),
        )
        .unwrap();

    // Force the socket drop before the snapshot could arrive.
    client.transport_lost(1_000, 0);
    assert_eq!(client.phase(), ClientPhase::Disconnected);
    assert!(
        client.retry_if_due(20_000).unwrap(),
        "the Plan 1 baseline reconnect becomes due"
    );
    let hello = client
        .take_outbound()
        .expect("reconnect queues a fresh Hello");

    // The retained desired set drives a full bootstrap and a resubscribe of
    // the very same subscription ID on the new connection generation.
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let mut ws = connect_async(url.as_str()).await.unwrap().0;
    send_client_frame(&mut ws, &hello).await;
    let welcome = recv_frame(&mut ws).await;
    assert_eq!(welcome.kind, ServerKind::Welcome);
    client.apply(welcome).unwrap();

    let mut saw_bootstrap_request = false;
    let mut resubscribed_id = None;
    while let Some(outbound) = client.take_outbound() {
        match outbound.kind {
            ClientKind::Request => {
                if let Ok(ClientPayload::Request(request)) = outbound.decode_payload()
                    && request.request_kind == "bootstrap"
                    && outbound.request_id.is_some()
                {
                    saw_bootstrap_request = true;
                }
            }
            ClientKind::Subscribe => resubscribed_id = outbound.subscription_id.clone(),
            _ => {}
        }
        send_client_frame(&mut ws, &outbound).await;
    }
    assert!(
        saw_bootstrap_request,
        "recovery must issue a full bootstrap request"
    );
    assert_eq!(
        resubscribed_id.as_ref(),
        Some(subscription.id()),
        "the preserved window resubscribes under its original subscription ID"
    );

    // The resubscribed watch performs a full snapshot resync on the new
    // generation without any client-visible gap.
    pump_until(&mut ws, &mut client, |frame| {
        matches!(
            frame.decode_payload(),
            Ok(k10s_protocol::ServerPayload::SnapshotEnd(_))
        )
    })
    .await;
    let list = client
        .resource_list(subscription.id())
        .expect("resync repopulates the retained list");
    assert_eq!(list.rows().count(), 2, "both deployments resync completely");
    assert_eq!(
        client.local_ui().selected_context.as_deref(),
        Some("dev-local"),
        "local UI state survives the forced disconnect"
    );
    assert_eq!(client.phase(), ClientPhase::Ready);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_snapshots_still_complete_into_an_empty_client_list() {
    let (server, _fake) = spawn_server().await;
    let (mut ws, mut client) = ready_client(&server).await;

    let subscription = client
        .subscribe_resource("prod-readonly", "", "v1", "Pod", Some("default".into()))
        .unwrap();
    flush_outbound(&mut ws, &mut client).await;
    client.apply(recv_frame(&mut ws).await).unwrap(); // Subscribed

    let begin = recv_frame(&mut ws).await;
    let total_chunks = serde_json::from_value::<SnapshotBegin>(begin.payload.clone())
        .unwrap()
        .total_chunks;
    client.apply(begin).unwrap();
    for expected in 0..total_chunks {
        let chunk = recv_frame(&mut ws).await;
        let payload: SnapshotChunk = serde_json::from_value(chunk.payload.clone()).unwrap();
        assert_eq!(payload.chunk_index, expected);
        client.apply(chunk).unwrap();
    }
    let end = recv_frame(&mut ws).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);
    client.apply(end).unwrap();

    let list = client.resource_list(subscription.id()).unwrap();
    assert!(list.rows().next().is_none());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_initial_snapshots_are_serialized_per_session() {
    let fake = FakeKubernetes::with_capacity(12_000, 256);
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new(fake),
    )
    .await
    .unwrap();
    let (mut ws, mut client) = ready_client(&server).await;
    client
        .subscribe_resource("dev-local", "", "v1", "Pod", None)
        .unwrap();
    client
        .subscribe_resource("dev-local", "", "v1", "Node", None)
        .unwrap();
    flush_outbound(&mut ws, &mut client).await;

    let mut active = None;
    let mut completed = 0;
    let mut last_sequence = None;
    while completed < 2 {
        let frame = recv_frame(&mut ws).await;
        if let Some(sequence) = frame.sequence {
            if let Some(previous) = last_sequence {
                assert_eq!(sequence, previous + 1);
            }
            last_sequence = Some(sequence);
        }
        match frame.kind {
            ServerKind::SnapshotBegin => {
                assert!(active.is_none(), "snapshot lifecycles interleaved");
                active = frame.subscription_id.clone();
            }
            ServerKind::SnapshotChunk => assert_eq!(frame.subscription_id, active),
            ServerKind::SnapshotEnd => {
                assert_eq!(frame.subscription_id, active);
                active = None;
                completed += 1;
            }
            _ => {}
        }
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_snapshot_releases_permit_for_the_next_subscription() {
    let fake = FakeKubernetes::with_capacity(12_000, 64);
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new(fake),
    )
    .await
    .unwrap();
    let (mut ws, mut client) = ready_client(&server).await;
    let pods = client
        .subscribe_resource("dev-local", "", "v1", "Pod", None)
        .unwrap();
    flush_outbound(&mut ws, &mut client).await;
    loop {
        let frame = recv_frame(&mut ws).await;
        let first_chunk = frame.subscription_id.as_ref() == Some(pods.id())
            && frame.kind == ServerKind::SnapshotChunk;
        client.apply(frame).unwrap();
        flush_outbound(&mut ws, &mut client).await;
        if first_chunk {
            break;
        }
    }
    client.unsubscribe(&pods).unwrap();
    let nodes = client
        .subscribe_resource("dev-local", "", "v1", "Node", None)
        .unwrap();
    flush_outbound(&mut ws, &mut client).await;

    let mut pod_end = false;
    let mut node_end = false;
    while !node_end {
        let frame = recv_frame(&mut ws).await;
        pod_end |= frame.subscription_id.as_ref() == Some(pods.id())
            && frame.kind == ServerKind::SnapshotEnd;
        node_end = frame.subscription_id.as_ref() == Some(nodes.id())
            && frame.kind == ServerKind::SnapshotEnd;
    }
    assert!(!pod_end, "cancelled partial snapshot emitted snapshotEnd");
    server.shutdown().await.unwrap();
}
