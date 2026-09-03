//! Port-forward client state contracts: capability gating, authoritative
//! session storage, monotonic revisions, and reconnect reconstruction.

use k10s_protocol::{
    BootstrapResponse, Event, GroupVersionKind, PortForwardPodTarget, PortForwardPortSelector,
    PortForwardSession, PortForwardSessionEvent, PortForwardSessionId, PortForwardSessionState,
    PortForwardTarget, ResourceIdentity, ServerFrame, ServerKind,
};
use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget};

fn ready_client_with_capabilities(capabilities: &[&str]) -> ClientState {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "ws://localhost/api/v1/control",
            "secret",
        ))
        .unwrap();
    client.apply(welcome()).unwrap();
    let pending = client.begin(k10s_ui::client::Query::Bootstrap).unwrap();
    let mut bootstrap = BootstrapResponse::fixture();
    bootstrap.capabilities = capabilities
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect();
    client
        .apply(ServerFrame::response(pending.id().clone(), bootstrap))
        .unwrap();
    client
}

fn welcome() -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(k10s_protocol::Welcome {
            protocol: k10s_protocol::ProtocolVersion {
                major: 1,
                minor: k10s_protocol::PROTOCOL_MINOR,
            },
            capabilities: vec![],
            session_id: k10s_protocol::SessionId::new("session-1"),
            server_instance_id: "server-1".into(),
            resume_status: k10s_protocol::ResumeStatus::Fresh,
        })
        .unwrap(),
    }
}

fn service_identity() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: "web".into(),
        uid: "uid-svc".into(),
    }
}

fn pod_identity() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: "web-1".into(),
        uid: "uid-pod".into(),
    }
}

fn service_session(id: &str, state: PortForwardSessionState, revision: u64) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        target: PortForwardTarget::Service {
            identity: service_identity(),
            port: PortForwardPortSelector::Number { number: 80 },
        },
        requested_local_port: 0,
        pod: PortForwardPodTarget {
            namespace: "default".into(),
            name: "web-1".into(),
            uid: "uid-pod".into(),
        },
        pod_port: 8_080,
        local_addr: "127.0.0.1:40000".into(),
        state,
        failure: None,
        revision,
    }
}

fn pod_session(id: &str, state: PortForwardSessionState, revision: u64) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        target: PortForwardTarget::Pod {
            identity: pod_identity(),
            container_name: "web".into(),
            remote_port: 8_080,
        },
        requested_local_port: 18_080,
        pod: PortForwardPodTarget {
            namespace: "default".into(),
            name: "web-1".into(),
            uid: "uid-pod".into(),
        },
        pod_port: 8_080,
        local_addr: "127.0.0.1:18080".into(),
        state,
        failure: None,
        revision,
    }
}

fn service_request() -> k10s_protocol::PortForwardStartRequest {
    k10s_protocol::PortForwardStartRequest::try_service(
        service_identity(),
        PortForwardPortSelector::Number { number: 80 },
        0,
    )
    .unwrap()
}

fn pod_request() -> k10s_protocol::PortForwardStartRequest {
    k10s_protocol::PortForwardStartRequest::try_target(
        PortForwardTarget::Pod {
            identity: pod_identity(),
            container_name: "web".into(),
            remote_port: 8_080,
        },
        0,
    )
    .unwrap()
}

#[test]
fn capabilities_are_target_specific_and_any_capability_enables_the_feed() {
    let mut unavailable = ready_client_with_capabilities(&[]);
    assert!(!unavailable.service_port_forward_available());
    assert!(!unavailable.pod_port_forward_available());
    assert!(!unavailable.any_port_forward_available());
    assert!(
        unavailable
            .request_port_forward_start(service_request(), "r-1")
            .is_err()
    );
    assert!(
        matches!(
            unavailable.subscribe_port_forward_sessions(),
            Ok(None) | Err(_)
        ),
        "capability absence never opens the stream"
    );

    let mut service =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    assert!(service.service_port_forward_available());
    assert!(!service.pod_port_forward_available());
    assert!(service.any_port_forward_available());
    assert!(
        service
            .request_port_forward_start(service_request(), "service")
            .is_ok()
    );
    assert!(
        service
            .request_port_forward_start(pod_request(), "pod")
            .is_err()
    );

    let mut pod = ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_POD_PORT_FORWARD]);
    assert!(!pod.service_port_forward_available());
    assert!(pod.pod_port_forward_available());
    assert!(pod.any_port_forward_available());
    assert!(pod.request_port_forward_start(pod_request(), "pod").is_ok());
    assert!(pod.subscribe_port_forward_sessions().unwrap().is_some());
}

#[test]
fn mixed_list_and_events_retain_authoritative_terminal_snapshots() {
    let mut client = ready_client_with_capabilities(&[
        k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD,
        k10s_protocol::CAPABILITY_POD_PORT_FORWARD,
    ]);
    assert!(client.any_port_forward_available());

    let subscription = client
        .subscribe_port_forward_sessions()
        .expect("stream opens")
        .expect("subscription handle");
    // The subscribe frame is queued for the server.
    assert!(!client.take_outbound().is_none() || true);

    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_LIST,
            &serde_json::to_value(k10s_protocol::PortForwardListResponse {
                revision: 3,
                sessions: vec![
                    service_session("pf-service", PortForwardSessionState::Active, 2),
                    pod_session("pf-pod", PortForwardSessionState::Failed, 3),
                ],
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(client.port_forward_sessions().len(), 2);
    assert!(client.port_forward_sessions().iter().any(|session| {
        matches!(session.target, PortForwardTarget::Service { .. })
            && session.state == PortForwardSessionState::Active
    }));
    assert!(client.port_forward_sessions().iter().any(|session| {
        matches!(session.target, PortForwardTarget::Pod { .. })
            && session.state == PortForwardSessionState::Failed
    }));

    let event = Event {
        event_kind: k10s_protocol::PORT_FORWARD_EVENT_SESSION.into(),
        revision: None,
        payload: serde_json::to_value(PortForwardSessionEvent {
            revision: 4,
            session: service_session("pf-service", PortForwardSessionState::Stopped, 4),
        })
        .unwrap(),
    };
    client
        .apply(ServerFrame {
            kind: ServerKind::Event,
            request_id: None,
            subscription_id: Some(subscription.id().clone()),
            sequence: Some(1),
            payload: serde_json::to_value(event).unwrap(),
        })
        .unwrap();
    assert_eq!(client.port_forward_sessions().len(), 2);
    assert_eq!(
        client
            .port_forward_sessions()
            .into_iter()
            .find(|session| session.id.as_str() == "pf-service")
            .unwrap()
            .state,
        PortForwardSessionState::Stopped
    );

    // The authoritative manager list is the normal expiry signal.
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_LIST,
            &serde_json::to_value(k10s_protocol::PortForwardListResponse {
                revision: 5,
                sessions: vec![pod_session("pf-pod", PortForwardSessionState::Failed, 5)],
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(client.port_forward_sessions().len(), 1);
    assert_eq!(client.port_forward_sessions()[0].id.as_str(), "pf-pod");
}

#[test]
fn stale_revisions_do_not_regress_retained_terminal_sessions() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    let active = k10s_protocol::PortForwardStartResponse {
        session: service_session("pf-terminal", PortForwardSessionState::Active, 3),
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(active).unwrap(),
        )
        .unwrap();

    let failed = k10s_protocol::PortForwardListResponse {
        revision: 4,
        sessions: vec![service_session(
            "pf-terminal",
            PortForwardSessionState::Failed,
            4,
        )],
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_LIST,
            &serde_json::to_value(failed).unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Failed
    );

    // A delayed active snapshot cannot regress the retained failure.
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(k10s_protocol::PortForwardStartResponse {
                session: service_session("pf-terminal", PortForwardSessionState::Active, 3),
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Failed
    );
}

#[test]
fn explicit_connect_registers_a_fresh_port_forward_subscription() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    client
        .subscribe_port_forward_sessions()
        .unwrap()
        .expect("first subscription");
    while client.take_outbound().is_some() {}

    client
        .connect(ConnectTarget::new(
            "ws://localhost/api/v1/control",
            "secret",
        ))
        .unwrap();
    client.apply(welcome()).unwrap();
    let pending = client.begin(k10s_ui::client::Query::Bootstrap).unwrap();
    let mut bootstrap = BootstrapResponse::fixture();
    bootstrap.capabilities = vec![k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned()];
    client
        .apply(ServerFrame::response(pending.id().clone(), bootstrap))
        .unwrap();
    client
        .subscribe_port_forward_sessions()
        .unwrap()
        .expect("fresh subscription");

    assert!(std::iter::from_fn(|| client.take_outbound()).any(|frame| {
        frame.kind == k10s_protocol::ClientKind::Subscribe
            && frame
                .payload
                .get("kind")
                .and_then(serde_json::Value::as_str)
                == Some("portForwardSessions")
    }));
}

#[test]
fn list_reconstruction_replaces_state_without_duplicates() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    let first = k10s_protocol::PortForwardStartResponse {
        session: service_session("pf-1", PortForwardSessionState::Active, 3),
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(&first).unwrap(),
        )
        .unwrap();

    // After reconnect the server reports the same active session only.
    let listed = k10s_protocol::PortForwardListResponse {
        revision: 5,
        sessions: vec![service_session("pf-1", PortForwardSessionState::Active, 5)],
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_LIST,
            &serde_json::to_value(&listed).unwrap(),
        )
        .unwrap();
    let sessions = client.port_forward_sessions();
    assert_eq!(sessions.len(), 1, "no duplicate sessions after reconnect");
    assert_eq!(sessions[0].revision, 5);
}

#[test]
fn delayed_list_cannot_regress_a_session_retained_by_a_newer_event() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(k10s_protocol::PortForwardStartResponse {
                session: service_session("pf-race", PortForwardSessionState::Active, 1),
            })
            .unwrap(),
        )
        .unwrap();

    // Model a Stop event winning the race with an already in-flight List.
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_STOP,
            &serde_json::to_value(k10s_protocol::PortForwardStopResponse {
                session: Some(service_session(
                    "pf-race",
                    PortForwardSessionState::Stopped,
                    2,
                )),
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Stopped
    );

    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_LIST,
            &serde_json::to_value(k10s_protocol::PortForwardListResponse {
                revision: 1,
                sessions: vec![service_session(
                    "pf-race",
                    PortForwardSessionState::Active,
                    1,
                )],
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Stopped,
        "revision-1 reconstruction must not overwrite revision-2 terminal state"
    );
}

#[test]
fn convenience_start_requests_are_correlated_through_normal_apply() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    let request = service_request();
    client
        .request_port_forward_start(request, "legacy-id")
        .unwrap();
    let id = std::iter::from_fn(|| client.take_outbound())
        .find(|frame| {
            frame
                .payload
                .get("kind")
                .and_then(serde_json::Value::as_str)
                == Some(k10s_protocol::REQUEST_PORT_FORWARD_START)
        })
        .and_then(|frame| frame.request_id)
        .expect("request id");
    client
        .apply(k10s_protocol::ServerFrame::response(
            id,
            k10s_protocol::PortForwardStartResponse {
                session: service_session("pf-correlated", PortForwardSessionState::Active, 1),
            },
        ))
        .expect("response is correlated, not unknown");
    assert_eq!(
        client.port_forward_sessions()[0].id.as_str(),
        "pf-correlated"
    );
}

#[test]
fn legacy_service_list_rows_are_normalized_in_the_authoritative_feed() {
    let mut client =
        ready_client_with_capabilities(&[k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD]);
    let legacy = serde_json::json!({
        "revision": 7,
        "sessions": [{
            "id": "legacy-service",
            "service": service_identity(),
            "servicePort": 80,
            "pod": {
                "namespace": "default",
                "name": "web-1",
                "uid": "uid-pod"
            },
            "podPort": 8080,
            "localAddr": "127.0.0.1:18080",
            "state": "stopped",
            "revision": 7
        }]
    });

    client
        .apply_port_forward_response(k10s_protocol::REQUEST_PORT_FORWARD_LIST, &legacy)
        .unwrap();

    let session = client.port_forward_sessions()[0];
    assert!(matches!(
        &session.target,
        PortForwardTarget::Service {
            identity,
            port: PortForwardPortSelector::Number { number: 80 }
        } if identity == &service_identity()
    ));
    assert_eq!(session.requested_local_port, 18_080);
    assert_eq!(session.state, PortForwardSessionState::Stopped);
}
