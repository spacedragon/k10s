//! Crash-adjacent system recovery and repeated shutdown leak gates.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    OperationAccepted, OperationId, OperationStatus, OperationUpdate, ProtocolVersion,
    ResourceIdentity, ResumeStatus, ServerFrame, ServerKind, SessionId, Welcome,
};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::{
    ClientConfig, ClientPhase, ClientState, Command, ConnectTarget, RetryEligibility,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

async fn authenticated_welcome(
    server: &k10s_server::ServerHandle,
) -> (
    Welcome,
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
) {
    let (mut socket, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    socket
        .send(Message::Text(
            serde_json::json!({"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":""}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let frame: ServerFrame =
        serde_json::from_str(&socket.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(frame.kind, ServerKind::Welcome);
    (serde_json::from_value(frame.payload).unwrap(), socket)
}

fn welcome(instance: &str, status: ResumeStatus) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(Welcome {
            protocol: ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec![],
            session_id: SessionId::new(format!("session-{instance}")),
            server_instance_id: instance.into(),
            resume_status: status,
        })
        .unwrap(),
    }
}

fn deployment() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: k10s_protocol::GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: "web".into(),
        uid: "uid-web".into(),
    }
}

#[test]
fn backend_restart_during_mutation_requires_status_refresh_before_retry() {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "ws://127.0.0.1/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _old_hello = client.take_outbound().unwrap();
    client
        .apply(welcome("server-before-crash", ResumeStatus::Fresh))
        .unwrap();

    client
        .begin_command(Command::Scale {
            target: deployment(),
            replicas: 3,
            idempotency_key: "restart-idempotency-key".into(),
        })
        .unwrap();
    let request = client.take_outbound().unwrap();
    let request_id = request.request_id.unwrap();
    client
        .apply(ServerFrame::response(
            request_id,
            OperationAccepted {
                operation_id: OperationId::new("operation-before-crash"),
            },
        ))
        .unwrap();
    client
        .apply(ServerFrame {
            kind: ServerKind::OperationUpdate,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(OperationUpdate {
                operation_id: OperationId::new("operation-before-crash"),
                status: OperationStatus::Running,
                progress: None,
            })
            .unwrap(),
        })
        .unwrap();

    client.transport_lost(1_000, 17);
    assert!(client.retry_if_due(u64::MAX).unwrap());
    let _restart_hello = client.take_outbound().unwrap();
    client
        .apply(welcome("server-after-crash", ResumeStatus::Fresh))
        .unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);
    assert!(matches!(
        client.retry_eligibility("restart-idempotency-key"),
        RetryEligibility::RefreshPending
    ));

    let refresh_id = loop {
        let frame = client.take_outbound().expect("recovery request is queued");
        let Some(id) = frame.request_id.clone() else {
            continue;
        };
        let payload = frame.decode_payload().unwrap();
        let k10s_protocol::ClientPayload::Request(request) = payload else {
            continue;
        };
        if request.request_kind == "operation.status" {
            assert_eq!(
                request.payload["operationIds"],
                serde_json::json!(["operation-before-crash"])
            );
            break id;
        }
    };
    // A restarted backend cannot authoritatively call the old write failed or
    // succeeded. Absence after the mandatory refresh becomes Unknown.
    client
        .apply(ServerFrame::response(
            refresh_id,
            k10s_protocol::OperationStatusResponse { operations: vec![] },
        ))
        .unwrap();
    assert_eq!(
        client
            .operation(&OperationId::new("operation-before-crash"))
            .unwrap()
            .status(),
        OperationStatus::Unknown
    );
    assert!(matches!(
        client.retry_eligibility("restart-idempotency-key"),
        RetryEligibility::Eligible
    ));
}

#[tokio::test]
async fn repeated_full_runtime_shutdown_leaves_no_listener_or_session_task() {
    for iteration in 0..20 {
        let server = spawn_loopback(
            ServerConfig {
                drain_timeout: Duration::from_secs(2),
                ..ServerConfig::default()
            },
            BackendKernel::new_with_instance_id(
                FakeKubernetes::standard(),
                format!("shutdown-cycle-{iteration}"),
            ),
        )
        .await
        .unwrap();
        let address = server.addr();
        let (_, mut socket) = authenticated_welcome(&server).await;
        socket.close(None).await.unwrap();
        drop(socket);
        server.shutdown().await.unwrap();
        assert!(
            TcpStream::connect(address).await.is_err(),
            "cycle {iteration} leaked its listener"
        );
    }
}

#[tokio::test]
async fn restarted_runtime_exposes_a_new_server_instance_identity() {
    let first = spawn_loopback(
        ServerConfig::default(),
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let (first_welcome, first_socket) = authenticated_welcome(&first).await;
    drop(first_socket);
    first.shutdown().await.unwrap();

    let second = spawn_loopback(
        ServerConfig::default(),
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();
    let (second_welcome, second_socket) = authenticated_welcome(&second).await;
    assert_ne!(
        first_welcome.server_instance_id,
        second_welcome.server_instance_id
    );
    drop(second_socket);
    second.shutdown().await.unwrap();
}
