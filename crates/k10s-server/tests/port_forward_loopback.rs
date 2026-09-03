//! Control-socket port-forward loopback: start/list/stop dispatch,
//! capability gating, session snapshot subscription, and reconnect
//! reconstruction over a real control socket against the fake adapter.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_protocol::{
    ClientKind, GroupVersionKind, PortForwardListResponse, PortForwardSessionState,
    PortForwardStartRequest, PortForwardStartResponse, PortForwardStopRequest,
    PortForwardStopResponse, PortForwardTarget, REQUEST_PORT_FORWARD_LIST,
    REQUEST_PORT_FORWARD_START, REQUEST_PORT_FORWARD_STOP, ResourceIdentity, ServerFrame,
    ServerKind, ServerPayload, SubscriptionSelector,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn service() -> ResourceIdentity {
    service_named("web-frontend")
}

fn service_named(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: name.into(),
        uid: format!("uid-dev-local-service-default-{name}"),
    }
}

fn pod() -> ResourceIdentity {
    pod_named("web-frontend-7d9f8-00001")
}

fn pod_named(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: name.into(),
        uid: format!("uid-dev-local-pod-default-{name}"),
    }
}

async fn spawn(capability: bool) -> k10s_server::ServerHandle {
    spawn_with_capabilities(capability, capability).await
}

async fn spawn_with_capabilities(
    service_capability: bool,
    pod_capability: bool,
) -> k10s_server::ServerHandle {
    let mut capabilities = vec!["logs.tail".to_owned()];
    if service_capability {
        capabilities.push(k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned());
    }
    if pod_capability {
        capabilities.push(k10s_protocol::CAPABILITY_POD_PORT_FORWARD.to_owned());
    }
    spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            capabilities,
            ..ServerConfig::default()
        },
        k10s_backend::BackendKernel::new_with_instance_id(
            k10s_backend::FakeKubernetes::standard(),
            "pf-loopback",
        ),
    )
    .await
    .unwrap()
}

async fn connect_authenticated(server: &k10s_server::ServerHandle) -> Ws {
    connect_with_minor(server, k10s_protocol::PROTOCOL_MINOR).await
}

async fn connect_with_minor(server: &k10s_server::ServerHandle, minor: u16) -> Ws {
    connect_with_minor_and_capabilities(server, minor, &["service.portForward", "pod.portForward"])
        .await
        .0
}

async fn connect_with_minor_and_welcome(
    server: &k10s_server::ServerHandle,
    minor: u16,
) -> (Ws, ServerFrame) {
    connect_with_minor_and_capabilities(server, minor, &["service.portForward", "pod.portForward"])
        .await
}

async fn connect_with_minor_and_capabilities(
    server: &k10s_server::ServerHandle,
    minor: u16,
    capabilities: &[&str],
) -> (Ws, ServerFrame) {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"hello",
            "payload":{
                "protocolMajor":1,
                "protocolMinor":minor,
                "capabilities": capabilities,
                "accessToken":"secret"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let welcome: ServerFrame =
        serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(welcome.kind, ServerKind::Welcome);
    (ws, welcome)
}

async fn request(ws: &mut Ws, id: &str, kind: &str, payload: serde_json::Value) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": id,
            "payload": {"kind": kind, "payload": payload}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("frame within timeout")
        .expect("socket open")
        .expect("healthy socket");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

#[tokio::test]
async fn start_list_and_stop_round_trip_over_the_control_socket() {
    let server = spawn(true).await;
    let (mut ws, welcome) =
        connect_with_minor_and_welcome(&server, k10s_protocol::PROTOCOL_MINOR).await;
    assert_eq!(
        welcome.payload["capabilities"],
        json!(["service.portForward", "pod.portForward"])
    );

    request(
        &mut ws,
        "start-1",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Name {
                    name: "http".into(),
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let frame = receive_frame(&mut ws).await;
    if frame.kind == ServerKind::Error {
        panic!("start failed: {}", frame.payload);
    }
    assert_eq!(frame.kind, ServerKind::Response);
    let started: PortForwardStartResponse = frame.decode_response_payload().unwrap();
    assert_eq!(started.session.state, PortForwardSessionState::Active);
    assert!(started.session.local_addr.starts_with("127.0.0.1:"));
    assert_eq!(started.session.pod_port, 80);

    // Duplicate start focuses the existing session.
    request(
        &mut ws,
        "start-2",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Name {
                    name: "http".into(),
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let focused: PortForwardStartResponse = receive_frame(&mut ws)
        .await
        .decode_response_payload()
        .unwrap();
    assert_eq!(focused.session.id, started.session.id);

    request(
        &mut ws,
        "start-pod",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod(),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let pod_started: PortForwardStartResponse = receive_frame(&mut ws)
        .await
        .decode_response_payload()
        .unwrap();
    assert!(matches!(
        pod_started.session.target,
        PortForwardTarget::Pod { .. }
    ));
    assert_eq!(pod_started.session.requested_local_port, 0);

    // List reconstructs state after a fresh connection ("reconnect").
    drop(ws);
    let mut reconnected = connect_authenticated(&server).await;
    request(
        &mut reconnected,
        "list-1",
        REQUEST_PORT_FORWARD_LIST,
        json!({}),
    )
    .await;
    let listed: PortForwardListResponse = receive_frame(&mut reconnected)
        .await
        .decode_response_payload()
        .unwrap();
    assert_eq!(listed.sessions.len(), 2);
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.id == started.session.id)
    );
    assert!(
        listed
            .sessions
            .iter()
            .any(|session| session.id == pod_started.session.id)
    );

    // Stop is idempotent by session ID.
    request(
        &mut reconnected,
        "stop-1",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: started.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    let stopped: PortForwardStopResponse = receive_frame(&mut reconnected)
        .await
        .decode_response_payload()
        .unwrap();
    assert_eq!(
        stopped.session.expect("final snapshot").state,
        PortForwardSessionState::Stopped
    );

    request(
        &mut reconnected,
        "stop-2",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: started.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    let repeat: PortForwardStopResponse = receive_frame(&mut reconnected)
        .await
        .decode_response_payload()
        .unwrap();
    assert!(repeat.session.is_none(), "idempotent stop has no snapshot");

    // Unknown ids also stop idempotently.
    request(
        &mut reconnected,
        "stop-3",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: k10s_protocol::PortForwardSessionId::try_new("pf-unknown").unwrap(),
        })
        .unwrap(),
    )
    .await;
    let unknown: PortForwardStopResponse = receive_frame(&mut reconnected)
        .await
        .decode_response_payload()
        .unwrap();
    assert!(unknown.session.is_none());

    request(
        &mut reconnected,
        "stop-pod",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: pod_started.session.id,
        })
        .unwrap(),
    )
    .await;
    let pod_stopped: PortForwardStopResponse = receive_frame(&mut reconnected)
        .await
        .decode_response_payload()
        .unwrap();
    assert_eq!(
        pod_stopped.session.unwrap().state,
        PortForwardSessionState::Stopped
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn sessions_subscription_streams_snapshots_and_disabled_servers_reject() {
    let server = spawn(true).await;
    let mut ws = connect_authenticated(&server).await;

    ws.send(Message::Text(
        json!({
            "kind": "subscribe",
            "subscriptionId": "pf-sessions",
            "payload": SubscriptionSelector::PortForwardSessions,
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let subscribed = receive_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);

    request(
        &mut ws,
        "start-ev",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Number { number: 80 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let started_frame = receive_frame(&mut ws).await;
    let _started: PortForwardStartResponse = started_frame.decode_response_payload().unwrap();

    // The Active snapshot arrives on the bounded session stream.
    let event = receive_frame(&mut ws).await;
    assert_eq!(event.kind, ServerKind::Event);
    match event.decode_payload().unwrap() {
        ServerPayload::Event(event) => {
            assert_eq!(event.event_kind, "portForward.session");
        }
        other => panic!("expected an event, got {other:?}"),
    }
    server.shutdown().await.unwrap();

    // A server without the capability rejects both requests and the
    // subscription even when a client sends them manually.
    let disabled = spawn(false).await;
    let (mut ws, welcome) =
        connect_with_minor_and_welcome(&disabled, k10s_protocol::PROTOCOL_MINOR).await;
    assert_eq!(welcome.payload["capabilities"], json!([]));
    request(
        &mut ws,
        "denied-start",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Number { number: 80 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.kind, ServerKind::Error);
    assert_eq!(error.payload["code"], json!("unsupportedMessage"));

    ws.send(Message::Text(
        json!({
            "kind": "subscribe",
            "subscriptionId": "pf-denied",
            "payload": SubscriptionSelector::PortForwardSessions,
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let denied = receive_frame(&mut ws).await;
    assert_eq!(denied.kind, ServerKind::Error);
    disabled.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_and_unvalidated_start_payloads_fail_without_binding() {
    let server = spawn(true).await;
    let mut ws = connect_authenticated(&server).await;

    // Wrong GVK fails typed validation.
    request(
        &mut ws,
        "bad-gvk",
        REQUEST_PORT_FORWARD_START,
        json!({
            "service": {"context": "dev-local",
                        "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                        "namespace": "default", "name": "x", "uid": "uid-x"},
            "port": {"kind": "number", "number": 80},
            "localPort": 0
        }),
    )
    .await;
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.kind, ServerKind::Error);
    assert_eq!(error.payload["code"], json!("invalidRequest"));

    // Malformed payloads are invalid, never internal.
    request(
        &mut ws,
        "garbage",
        REQUEST_PORT_FORWARD_START,
        json!({"localPort": "zero"}),
    )
    .await;
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.payload["code"], json!("invalidRequest"));

    // A vanished Service UID resolves to a typed vanished failure.
    request(
        &mut ws,
        "stale-uid",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                ResourceIdentity {
                    uid: "uid-from-a-past-life".into(),
                    ..service()
                },
                k10s_protocol::PortForwardPortSelector::Number { number: 80 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let error = receive_frame(&mut ws).await;
    assert_eq!(error.kind, ServerKind::Error);
    assert_eq!(error.payload["code"], json!("notFound"));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn prior_minor_clients_use_legacy_service_shapes_and_never_see_pods() {
    let server = spawn(true).await;
    let mut current = connect_authenticated(&server).await;
    request(
        &mut current,
        "current-service",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Name {
                    name: "http".into(),
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let current_service = receive_frame(&mut current).await;
    assert!(current_service.payload["session"].get("target").is_some());
    request(
        &mut current,
        "current-pod",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod(),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let current_pod = receive_frame(&mut current).await;
    assert_eq!(
        current_pod.payload["session"]["target"]["kind"],
        json!("pod")
    );

    let mut legacy = connect_with_minor(&server, k10s_protocol::PROTOCOL_MINOR - 1).await;
    request(
        &mut legacy,
        "legacy-list",
        REQUEST_PORT_FORWARD_LIST,
        json!({}),
    )
    .await;
    let list = receive_frame(&mut legacy).await;
    assert_eq!(list.payload["sessions"].as_array().unwrap().len(), 1);
    let legacy_service = &list.payload["sessions"][0];
    assert!(legacy_service.get("service").is_some());
    assert_eq!(legacy_service["servicePort"], json!(80));
    assert!(legacy_service.get("target").is_none());

    legacy
        .send(Message::Text(
            json!({
                "kind": "subscribe",
                "subscriptionId": "legacy-pf",
                "payload": SubscriptionSelector::PortForwardSessions,
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        receive_frame(&mut legacy).await.kind,
        ServerKind::Subscribed
    );

    request(
        &mut current,
        "current-pod-2",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod_named("web-frontend-7d9f8-00002"),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let _ = receive_frame(&mut current).await;
    request(
        &mut current,
        "current-service-2",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service_named("api-server"),
                k10s_protocol::PortForwardPortSelector::Number { number: 443 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let _ = receive_frame(&mut current).await;

    let event = receive_frame(&mut legacy).await;
    assert_eq!(event.kind, ServerKind::Event);
    let session = &event.payload["payload"]["session"];
    assert_eq!(session["service"]["name"], json!("api-server"));
    assert!(session.get("target").is_none());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_minor_can_start_legacy_service_but_cannot_start_pod() {
    let server = spawn(true).await;
    let mut legacy = connect_with_minor(&server, k10s_protocol::PROTOCOL_MINOR - 1).await;
    request(
        &mut legacy,
        "legacy-service",
        REQUEST_PORT_FORWARD_START,
        json!({
            "service": service(),
            "port": {"kind": "name", "name": "http"},
            "localPort": 0
        }),
    )
    .await;
    let started = receive_frame(&mut legacy).await;
    assert_eq!(started.kind, ServerKind::Response);
    assert!(started.payload["session"].get("service").is_some());
    assert!(started.payload["session"].get("target").is_none());

    request(
        &mut legacy,
        "legacy-pod",
        REQUEST_PORT_FORWARD_START,
        json!({
            "target": {
                "kind": "pod",
                "identity": pod(),
                "containerName": "app",
                "remotePort": 8080
            },
            "localPort": 0
        }),
    )
    .await;
    assert_eq!(receive_frame(&mut legacy).await.kind, ServerKind::Error);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn pod_start_requires_the_pod_capability_even_when_service_is_enabled() {
    let server = spawn_with_capabilities(true, false).await;
    let mut ws = connect_authenticated(&server).await;
    request(
        &mut ws,
        "denied-pod",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod(),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let denied = receive_frame(&mut ws).await;
    assert_eq!(denied.kind, ServerKind::Error);
    assert_eq!(denied.payload["code"], json!("unsupportedMessage"));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn list_and_stop_are_filtered_and_authorized_by_target_capability() {
    let server = spawn(true).await;
    let mut admin = connect_authenticated(&server).await;
    request(
        &mut admin,
        "seed-service",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Number { number: 80 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let service_session: PortForwardStartResponse = receive_frame(&mut admin)
        .await
        .decode_response_payload()
        .unwrap();
    request(
        &mut admin,
        "seed-pod",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod(),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let pod_session: PortForwardStartResponse = receive_frame(&mut admin)
        .await
        .decode_response_payload()
        .unwrap();

    let (mut service_only, _) = connect_with_minor_and_capabilities(
        &server,
        k10s_protocol::PROTOCOL_MINOR,
        &["service.portForward"],
    )
    .await;
    request(
        &mut service_only,
        "service-list",
        REQUEST_PORT_FORWARD_LIST,
        json!({}),
    )
    .await;
    let service_list = receive_frame(&mut service_only).await;
    assert_eq!(
        service_list.payload["sessions"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        service_list.payload["sessions"][0]["target"]["kind"],
        json!("service")
    );
    request(
        &mut service_only,
        "service-stop-pod",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: pod_session.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    assert_eq!(
        receive_frame(&mut service_only).await.kind,
        ServerKind::Error
    );

    let (mut pod_only, _) = connect_with_minor_and_capabilities(
        &server,
        k10s_protocol::PROTOCOL_MINOR,
        &["pod.portForward"],
    )
    .await;
    request(
        &mut pod_only,
        "pod-list",
        REQUEST_PORT_FORWARD_LIST,
        json!({}),
    )
    .await;
    let pod_list = receive_frame(&mut pod_only).await;
    assert_eq!(pod_list.payload["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(
        pod_list.payload["sessions"][0]["target"]["kind"],
        json!("pod")
    );
    request(
        &mut pod_only,
        "pod-stop-service",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: service_session.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    assert_eq!(receive_frame(&mut pod_only).await.kind, ServerKind::Error);

    request(
        &mut service_only,
        "service-stop-service",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: service_session.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    assert_eq!(
        receive_frame(&mut service_only).await.kind,
        ServerKind::Response
    );
    request(
        &mut pod_only,
        "pod-stop-pod",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: pod_session.session.id.clone(),
        })
        .unwrap(),
    )
    .await;
    assert_eq!(
        receive_frame(&mut pod_only).await.kind,
        ServerKind::Response
    );

    let (mut neither, _) =
        connect_with_minor_and_capabilities(&server, k10s_protocol::PROTOCOL_MINOR, &[]).await;
    request(
        &mut neither,
        "no-list",
        REQUEST_PORT_FORWARD_LIST,
        json!({}),
    )
    .await;
    assert_eq!(receive_frame(&mut neither).await.kind, ServerKind::Error);
    request(
        &mut neither,
        "no-stop",
        REQUEST_PORT_FORWARD_STOP,
        serde_json::to_value(PortForwardStopRequest {
            session_id: k10s_protocol::PortForwardSessionId::try_new("pf-unknown").unwrap(),
        })
        .unwrap(),
    )
    .await;
    assert_eq!(receive_frame(&mut neither).await.kind, ServerKind::Error);
    neither
        .send(Message::Text(
            json!({
                "kind": "subscribe",
                "subscriptionId": "no-subscription",
                "payload": SubscriptionSelector::PortForwardSessions,
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    assert_eq!(receive_frame(&mut neither).await.kind, ServerKind::Error);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_minor_session_events_follow_the_negotiated_capability_matrix() {
    let server = spawn(true).await;
    let (mut service_only, _) = connect_with_minor_and_capabilities(
        &server,
        k10s_protocol::PROTOCOL_MINOR,
        &["service.portForward"],
    )
    .await;
    let (mut pod_only, _) = connect_with_minor_and_capabilities(
        &server,
        k10s_protocol::PROTOCOL_MINOR,
        &["pod.portForward"],
    )
    .await;
    let mut both = connect_authenticated(&server).await;
    for (socket, id) in [
        (&mut service_only, "service-events"),
        (&mut pod_only, "pod-events"),
        (&mut both, "both-events"),
    ] {
        socket
            .send(Message::Text(
                json!({
                    "kind": "subscribe",
                    "subscriptionId": id,
                    "payload": SubscriptionSelector::PortForwardSessions,
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        assert_eq!(receive_frame(socket).await.kind, ServerKind::Subscribed);
    }

    let mut admin = connect_authenticated(&server).await;
    request(
        &mut admin,
        "event-service",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_service(
                service(),
                k10s_protocol::PortForwardPortSelector::Number { number: 80 },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let _ = receive_frame(&mut admin).await;
    let service_event = receive_frame(&mut service_only).await;
    assert_eq!(
        service_event.payload["payload"]["session"]["target"]["kind"],
        json!("service")
    );
    let both_service = receive_frame(&mut both).await;
    assert_eq!(
        both_service.payload["payload"]["session"]["target"]["kind"],
        json!("service")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), receive_frame(&mut pod_only))
            .await
            .is_err()
    );

    request(
        &mut admin,
        "event-pod",
        REQUEST_PORT_FORWARD_START,
        serde_json::to_value(
            PortForwardStartRequest::try_target(
                PortForwardTarget::Pod {
                    identity: pod(),
                    container_name: "app".into(),
                    remote_port: 8_080,
                },
                0,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await;
    let _ = receive_frame(&mut admin).await;
    let pod_event = receive_frame(&mut pod_only).await;
    assert_eq!(
        pod_event.payload["payload"]["session"]["target"]["kind"],
        json!("pod")
    );
    let both_pod = receive_frame(&mut both).await;
    assert_eq!(
        both_pod.payload["payload"]["session"]["target"]["kind"],
        json!("pod")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), receive_frame(&mut service_only))
            .await
            .is_err()
    );
    server.shutdown().await.unwrap();
}

// Keep the unused-kind import honest for future envelope extensions.
#[allow(dead_code)]
fn _kind_marker(kind: ClientKind) -> ClientKind {
    kind
}
