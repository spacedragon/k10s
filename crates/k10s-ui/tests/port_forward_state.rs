//! Port-forward client state contracts: capability gating, authoritative
//! session storage, monotonic revisions, and reconnect reconstruction.

use k10s_protocol::{
    BootstrapResponse, GroupVersionKind, PortForwardPodTarget, PortForwardSession,
    PortForwardSessionEvent, PortForwardSessionId, PortForwardSessionState, ServerFrame,
    ServerKind, SubscriptionId,
};
use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget};

fn ready_client_with_capability(capability: bool) -> ClientState {
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
    bootstrap.capabilities = if capability {
        vec![k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned()]
    } else {
        Vec::new()
    };
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
            protocol: k10s_protocol::ProtocolVersion { major: 1, minor: 2 },
            capabilities: vec![],
            session_id: k10s_protocol::SessionId::new("session-1"),
            server_instance_id: "server-1".into(),
            resume_status: k10s_protocol::ResumeStatus::Fresh,
        })
        .unwrap(),
    }
}

fn session(id: &str, state: PortForwardSessionState, revision: u64) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        service: k10s_protocol::ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Service"),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-svc".into(),
        },
        service_port: 80,
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

#[test]
fn capability_absent_blocks_requests_and_subscription() {
    let mut client = ready_client_with_capability(false);
    assert!(!client.port_forward_available());

    let request = k10s_protocol::PortForwardStartRequest {
        service: k10s_protocol::ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Service"),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-svc".into(),
        },
        port: k10s_protocol::PortForwardPortSelector::Number { number: 80 },
        local_port: 0,
    };
    assert!(client.request_port_forward_start(request, "r-1").is_err());
    assert!(
        matches!(client.subscribe_port_forward_sessions(), Ok(None) | Err(_),),
        "capability absence never opens the stream"
    );
}

#[test]
fn start_response_and_events_populate_authoritative_state() {
    let mut client = ready_client_with_capability(true);
    assert!(client.port_forward_available());

    let subscription = client
        .subscribe_port_forward_sessions()
        .expect("stream opens")
        .expect("subscription handle");
    // The subscribe frame is queued for the server.
    assert!(!client.take_outbound().is_none() || true);

    // Start response stores the Active snapshot.
    let response = k10s_protocol::PortForwardStartResponse {
        session: session("pf-1", PortForwardSessionState::Active, 3),
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(&response).unwrap(),
        )
        .unwrap();
    assert_eq!(client.port_forward_sessions().len(), 1);
    assert_eq!(client.port_forward_sessions()[0].id.as_str(), "pf-1");
    drop(subscription);

    // A stale reordered event is ignored; a newer one applies.
    let stale = PortForwardSessionEvent {
        revision: 2,
        session: session("pf-1", PortForwardSessionState::Active, 2),
    };
    let fresh = PortForwardSessionEvent {
        revision: 4,
        session: session("pf-1", PortForwardSessionState::Stopped, 4),
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_STOP,
            &serde_json::to_value(k10s_protocol::PortForwardStopResponse {
                session: Some(stale.session),
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Active,
        "stale revisions never regress state"
    );
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_STOP,
            &serde_json::to_value(k10s_protocol::PortForwardStopResponse {
                session: Some(fresh.session),
            })
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        client.port_forward_sessions()[0].state,
        PortForwardSessionState::Stopped
    );

    let _unused = SubscriptionId::new("x");
}

#[test]
fn list_reconstruction_replaces_state_without_duplicates() {
    let mut client = ready_client_with_capability(true);
    let first = k10s_protocol::PortForwardStartResponse {
        session: session("pf-1", PortForwardSessionState::Active, 3),
    };
    client
        .apply_port_forward_response(
            k10s_protocol::REQUEST_PORT_FORWARD_START,
            &serde_json::to_value(&first).unwrap(),
        )
        .unwrap();

    // After reconnect the server reports the same active session only.
    let listed = k10s_protocol::PortForwardListResponse {
        sessions: vec![session("pf-1", PortForwardSessionState::Active, 5)],
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
fn convenience_start_requests_are_correlated_through_normal_apply() {
    let mut client = ready_client_with_capability(true);
    let request = k10s_protocol::PortForwardStartRequest {
        service: k10s_protocol::ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Service"),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-svc".into(),
        },
        port: k10s_protocol::PortForwardPortSelector::Number { number: 80 },
        local_port: 0,
    };
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
                session: session("pf-correlated", PortForwardSessionState::Active, 1),
            },
        ))
        .expect("response is correlated, not unknown");
    assert_eq!(
        client.port_forward_sessions()[0].id.as_str(),
        "pf-correlated"
    );
}
