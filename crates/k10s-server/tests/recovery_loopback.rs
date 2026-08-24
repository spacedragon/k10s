//! Cross-layer recovery: one forced control drop must rebuild resource state
//! and refresh every nonterminal operation before retry decisions resume.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{ClientFrame, ClientKind, ClientPayload, ServerFrame, ServerKind};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::{
    ClientConfig, ClientPhase, ClientState, Command, ConnectTarget, QueryResult,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn send(ws: &mut Ws, frame: &ClientFrame) {
    ws.send(Message::Text(serde_json::to_string(frame).unwrap().into()))
        .await
        .unwrap();
}

async fn flush(ws: &mut Ws, client: &mut ClientState) {
    while let Some(frame) = client.take_outbound() {
        send(ws, &frame).await;
    }
}

async fn receive(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn pump_until(
    ws: &mut Ws,
    client: &mut ClientState,
    done: impl Fn(&ServerFrame) -> bool,
) -> ServerFrame {
    loop {
        let frame = receive(ws).await;
        let done = done(&frame);
        client.apply(frame.clone()).unwrap();
        flush(ws, client).await;
        if done {
            return frame;
        }
    }
}

#[tokio::test]
async fn forced_drop_full_resyncs_and_queries_the_nonterminal_operation() {
    let fake = FakeKubernetes::standard();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake, "recovery-loopback"),
    )
    .await
    .unwrap();
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(url.clone(), "secret"))
        .unwrap();
    let mut first = connect_async(&url).await.unwrap().0;
    flush(&mut first, &mut client).await;
    pump_until(&mut first, &mut client, |frame| {
        frame.kind == ServerKind::Welcome
    })
    .await;
    assert_eq!(client.phase(), ClientPhase::Ready);

    let watch = client
        .subscribe_resource(
            "dev-local",
            "apps",
            "v1",
            "Deployment",
            Some("default".into()),
        )
        .unwrap();
    flush(&mut first, &mut client).await;
    pump_until(&mut first, &mut client, |frame| {
        frame.kind == ServerKind::SnapshotEnd
    })
    .await;
    let target = client
        .resource_list(watch.id())
        .unwrap()
        .rows()
        .find(|row| row.identity.name == "web-frontend")
        .unwrap()
        .identity
        .clone();

    let pending = client
        .begin_command(Command::Scale {
            target,
            replicas: 3,
            idempotency_key: "recovery-scale".into(),
        })
        .unwrap();
    flush(&mut first, &mut client).await;
    pump_until(&mut first, &mut client, |frame| {
        frame.kind == ServerKind::Response && frame.request_id.as_ref() == Some(pending.id())
    })
    .await;
    let QueryResult::Applied(accepted) = client.take(pending).unwrap() else {
        panic!("operation accepted")
    };
    let operation_id = accepted.operation_id;
    assert!(client.nonterminal_operation_ids().contains(&operation_id));

    first.close(None).await.unwrap();
    client.transport_lost(1_000, 7);
    assert!(client.retry_if_due(u64::MAX).unwrap());
    let hello = client.take_outbound().unwrap();
    let mut second = connect_async(&url).await.unwrap().0;
    send(&mut second, &hello).await;
    let welcome = receive(&mut second).await;
    client.apply(welcome).unwrap();

    let mut saw_bootstrap = false;
    let mut saw_resubscribe = false;
    let mut saw_operation_refresh = false;
    while let Some(frame) = client.take_outbound() {
        match frame.kind {
            ClientKind::Request => {
                if let Ok(ClientPayload::Request(request)) = frame.decode_payload() {
                    saw_bootstrap |= request.request_kind == "bootstrap";
                    saw_operation_refresh |= request.request_kind == "operation.status"
                        && request.payload["operationIds"]
                            .as_array()
                            .is_some_and(|ids| ids.iter().any(|id| id == operation_id.as_str()));
                }
            }
            ClientKind::Subscribe => {
                saw_resubscribe |= frame.subscription_id.as_ref() == Some(watch.id());
            }
            _ => {}
        }
        send(&mut second, &frame).await;
    }
    assert!(saw_bootstrap);
    assert!(saw_resubscribe);
    assert!(saw_operation_refresh);

    let mut snapshot_done = false;
    let mut refresh_done = false;
    while !(snapshot_done && refresh_done) {
        let frame = receive(&mut second).await;
        snapshot_done |= frame.kind == ServerKind::SnapshotEnd;
        refresh_done |= frame.kind == ServerKind::Response
            && frame
                .decode_response_payload::<k10s_protocol::OperationStatusResponse>()
                .is_ok_and(|status| {
                    status
                        .operations
                        .iter()
                        .any(|entry| entry.operation_id == operation_id)
                });
        client.apply(frame).unwrap();
        flush(&mut second, &mut client).await;
    }
    assert_eq!(client.resource_list(watch.id()).unwrap().rows().count(), 2);
    assert!(client.operation(&operation_id).is_some());
    assert_eq!(client.phase(), ClientPhase::Ready);
    server.shutdown().await.unwrap();
}
