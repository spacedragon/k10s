use k10s_protocol::{
    BootstrapResponse, ClientKind, ErrorCode, ErrorFrame, ErrorScope, Event, ProtocolVersion,
    RequestId, ResumeStatus, Retryability, ServerFrame, ServerKind, SessionId, Subscribed,
    SubscriptionId, Welcome,
};
use k10s_ui::client::{
    ClientConfig, ClientError, ClientPhase, ClientState, ConnectTarget, Query, QueryResult,
    RetrySchedule,
};

#[test]
fn disconnected_authenticating_ready() {
    let mut client = ClientState::new(ClientConfig::default());
    assert_eq!(client.phase(), ClientPhase::Disconnected);

    client
        .connect(ConnectTarget::new(
            "ws://127.0.0.1/api/v1/control",
            "secret",
        ))
        .unwrap();
    assert_eq!(client.phase(), ClientPhase::Authenticating);
    let hello = client.take_outbound().unwrap();
    assert_eq!(hello.kind, k10s_protocol::ClientKind::Hello);
    let hello_json = serde_json::to_string(&hello).unwrap();
    assert!(hello_json.contains("secret"));
    assert!(!hello_json.contains("127.0.0.1"));
    let hello = hello.decode_payload().unwrap();
    assert!(matches!(hello, k10s_protocol::ClientPayload::Hello(_)));

    client.apply(welcome()).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);
}

#[test]
fn bootstrap_response_completes_only_matching_request() {
    let mut client = ready_client();
    let first = client.begin(Query::Bootstrap).unwrap();
    let second = client.begin(Query::Bootstrap).unwrap();

    client
        .apply(ServerFrame::response(
            second.id().clone(),
            BootstrapResponse::fixture(),
        ))
        .unwrap();

    assert!(client.take(first.clone()).is_none());
    assert_eq!(
        client.take(second),
        Some(QueryResult::Bootstrap(BootstrapResponse::fixture()))
    );
    assert!(client.is_pending(&first));
}

#[test]
fn response_with_unknown_request_id_is_rejected() {
    let mut client = ready_client();
    let error = client
        .apply(ServerFrame::response(
            RequestId::from_u128(999),
            BootstrapResponse::fixture(),
        ))
        .unwrap_err();
    assert_eq!(
        error,
        ClientError::UnknownResponse(RequestId::from_u128(999))
    );
}

#[test]
fn cancellation_is_idempotent_and_deadlines_cancel_pending_requests() {
    let mut client = ready_client();
    let cancelled = client.begin(Query::Bootstrap).unwrap();
    let request = client.take_outbound().unwrap();
    assert_eq!(request.kind, ClientKind::Request);
    assert!(client.cancel(&cancelled));
    assert!(!client.cancel(&cancelled));
    let cancel = client.take_outbound().unwrap();
    assert_eq!(cancel.kind, ClientKind::CancelRequest);
    assert_eq!(cancel.request_id(), Some(cancelled.id()));

    let expiring = client
        .begin_with_deadline(Query::Bootstrap, 1_000, 250)
        .unwrap();
    let request = client.take_outbound().unwrap();
    assert_eq!(request.kind, ClientKind::Request);
    let request_payload = request.decode_payload().unwrap();
    let k10s_protocol::ClientPayload::Request(request_payload) = request_payload else {
        panic!("expected request payload");
    };
    assert_eq!(request_payload.deadline, Some(250));
    assert!(client.expire_deadlines(1_249).is_empty());
    assert_eq!(client.expire_deadlines(1_250), vec![expiring.clone()]);
    assert!(!client.is_pending(&expiring));
    let cancel = client.take_outbound().unwrap();
    assert_eq!(cancel.kind, ClientKind::CancelRequest);
    assert_eq!(cancel.request_id(), Some(expiring.id()));
}

#[test]
fn contiguous_sequences_advance_ack_and_a_gap_requests_resync() {
    let mut client = ready_client();
    let subscription = client.subscribe_bootstrap_status().unwrap();
    let _subscribe = client.take_outbound().unwrap();
    client.apply(subscribed(subscription.id(), 1)).unwrap();
    let ack = client.take_outbound().unwrap();
    assert_eq!(ack.kind, ClientKind::Ack);
    assert_eq!(client.last_acked_sequence(), Some(1));

    client.apply(event(subscription.id(), 2)).unwrap();
    assert_eq!(client.last_acked_sequence(), Some(2));
    let _ack = client.take_outbound().unwrap();

    client.apply(event(subscription.id(), 2)).unwrap();
    let duplicate_ack = client.take_outbound().unwrap();
    assert_eq!(duplicate_ack.kind, ClientKind::Ack);
    let k10s_protocol::ClientPayload::Ack(duplicate_ack) = duplicate_ack.decode_payload().unwrap()
    else {
        panic!("expected ack payload");
    };
    assert_eq!(duplicate_ack.last_acked_sequence, 2);

    let error = client.apply(event(subscription.id(), 4)).unwrap_err();
    assert_eq!(
        error,
        ClientError::SequenceGap {
            expected: 3,
            got: 4
        }
    );
    assert!(client.server_state_invalid());
    let kinds: Vec<_> = std::iter::from_fn(|| client.take_outbound())
        .map(|frame| frame.kind)
        .collect();
    assert_eq!(kinds, [ClientKind::Request, ClientKind::Subscribe]);
}

#[test]
fn full_jitter_retry_is_bounded_and_increases_exponentially() {
    let mut client = ClientState::new(ClientConfig {
        retry_base_ms: 100,
        retry_cap_ms: 1_000,
        ..ClientConfig::default()
    });
    client
        .connect(ConnectTarget::new(
            "ws://localhost/api/v1/control",
            "secret",
        ))
        .unwrap();

    client.transport_lost(10_000, u64::MAX);
    assert_eq!(
        client.retry_schedule(),
        Some(RetrySchedule {
            attempt: 0,
            max_delay_ms: 100,
            retry_at_ms: 10_000 + (u64::MAX % 101),
        })
    );
    assert!(!client.retry_if_due(10_050));
    assert!(client.retry_if_due(10_100));

    client.transport_lost(20_000, 199);
    assert_eq!(
        client.retry_schedule(),
        Some(RetrySchedule {
            attempt: 1,
            max_delay_ms: 200,
            retry_at_ms: 20_199,
        })
    );
}

#[test]
fn reconnect_preserves_local_state_and_rebuilds_server_state() {
    let mut client = ready_client();
    client.local_ui_mut().selected_context = Some("dev-local".into());
    let bootstrap = client.begin(Query::Bootstrap).unwrap();
    let _request = client.take_outbound();
    client
        .apply(ServerFrame::response(
            bootstrap.id().clone(),
            BootstrapResponse::fixture(),
        ))
        .unwrap();
    let subscription = client.subscribe_bootstrap_status().unwrap();
    let _subscribe = client.take_outbound();
    client.apply(subscribed(subscription.id(), 1)).unwrap();
    let _ack = client.take_outbound();
    assert!(!client.server_state_invalid());

    client.transport_lost(1_000, 0);
    assert_eq!(
        client.local_ui().selected_context.as_deref(),
        Some("dev-local")
    );
    assert!(client.server_state_invalid());
    assert!(client.server_bootstrap().is_none());
    assert!(client.retry_if_due(1_000));
    let hello = client.take_outbound().unwrap();
    assert_eq!(hello.kind, ClientKind::Hello);
    let k10s_protocol::ClientPayload::Hello(hello) = hello.decode_payload().unwrap() else {
        panic!("expected hello payload");
    };
    assert_eq!(hello.session_id, Some(SessionId::new("session-1")));
    assert_eq!(hello.server_instance_id.as_deref(), Some("server-1"));
    assert_eq!(hello.last_acked_sequence, Some(1));

    client.apply(welcome_resumed()).unwrap();
    let kinds: Vec<_> = std::iter::from_fn(|| client.take_outbound())
        .map(|frame| frame.kind)
        .collect();
    assert_eq!(kinds, [ClientKind::Request, ClientKind::Subscribe]);
}

#[test]
fn resync_required_reissues_bootstrap_and_live_subscriptions() {
    let mut client = ready_client();
    let subscription = client.subscribe_bootstrap_status().unwrap();
    let _subscribe = client.take_outbound();
    client.apply(subscribed(subscription.id(), 1)).unwrap();
    let _ack = client.take_outbound();

    client.apply(resync_required(2)).unwrap();
    let kinds: Vec<_> = std::iter::from_fn(|| client.take_outbound())
        .map(|frame| frame.kind)
        .collect();
    assert_eq!(kinds, [ClientKind::Request, ClientKind::Subscribe]);
}

#[test]
fn authentication_rejection_returns_to_web_gate_without_retry() {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "wss://example.test/api/v1/control",
            "wrong",
        ))
        .unwrap();
    let _hello = client.take_outbound();

    let error = client.apply(error_frame(ErrorCode::Unauthorized, Retryability::Never));
    assert_eq!(error.unwrap_err(), ClientError::AuthenticationRejected);
    assert_eq!(client.phase(), ClientPhase::WebGate);
    assert_eq!(client.retry_schedule(), None);
    client.transport_lost(1_000, 7);
    assert_eq!(client.retry_schedule(), None);
}

#[test]
fn incompatible_protocol_major_requires_upgrade_without_retry() {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "wss://example.test/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _hello = client.take_outbound();
    let mut incompatible = welcome();
    incompatible.payload = serde_json::to_value(Welcome {
        protocol: ProtocolVersion { major: 2, minor: 0 },
        capabilities: vec![],
        session_id: SessionId::new("future-session"),
        server_instance_id: "future-server".into(),
        resume_status: ResumeStatus::Fresh,
    })
    .unwrap();

    assert_eq!(
        client.apply(incompatible).unwrap_err(),
        ClientError::IncompatibleProtocol {
            client_major: 1,
            server_major: 2,
        }
    );
    assert_eq!(client.phase(), ClientPhase::UpgradeRequired);
    assert_eq!(client.retry_schedule(), None);
    client.transport_lost(1_000, 7);
    assert_eq!(client.retry_schedule(), None);
}

#[test]
fn explicit_user_or_application_close_stays_closed_until_new_connect() {
    for close in [ClientState::user_close, ClientState::application_close] {
        let mut client = ready_client();
        client.transport_lost(1_000, 0);
        assert!(client.retry_schedule().is_some());
        close(&mut client);
        assert_eq!(client.phase(), ClientPhase::Closed);
        assert_eq!(client.retry_schedule(), None);
        client.transport_lost(2_000, 0);
        assert!(!client.retry_if_due(u64::MAX));
        assert!(client.take_outbound().is_none());

        client
            .connect(ConnectTarget::new("ws://localhost/api/v1/control", "new"))
            .unwrap();
        assert_eq!(client.phase(), ClientPhase::Authenticating);
        assert_eq!(client.take_outbound().unwrap().kind, ClientKind::Hello);
    }
}

#[test]
fn only_transient_loss_and_after_reconnect_server_errors_schedule_retry() {
    let mut client = ready_client();
    let error = client
        .apply_at(
            error_frame(ErrorCode::Internal, Retryability::AfterReconnect),
            5_000,
            0,
        )
        .unwrap_err();
    assert!(matches!(error, ClientError::Server(_)));
    assert!(client.retry_schedule().is_some());

    let mut client = ready_client();
    let _error = client
        .apply_at(
            error_frame(ErrorCode::Conflict, Retryability::AfterRefresh),
            5_000,
            0,
        )
        .unwrap_err();
    assert_eq!(client.retry_schedule(), None);
}

#[test]
fn reconnect_discards_stale_outbound_and_sends_only_hello_before_welcome() {
    let mut client = ready_client();
    let pending = client.begin(Query::Bootstrap).unwrap();
    assert!(client.cancel(&pending));

    client.transport_lost(1_000, 0);
    assert!(client.take_outbound().is_none());
    assert!(client.retry_if_due(1_000));
    assert_eq!(client.take_outbound().unwrap().kind, ClientKind::Hello);
    assert!(client.take_outbound().is_none());
}

#[test]
fn resync_discards_stale_outbound_before_recovery_frames() {
    let mut client = ready_client();
    let live = client.subscribe_bootstrap_status().unwrap();
    let _subscribe = client.take_outbound();
    client.apply(subscribed(live.id(), 1)).unwrap();
    let _ack = client.take_outbound();
    let _stale_request = client.begin(Query::Bootstrap).unwrap();

    client.apply(resync_required(2)).unwrap();
    let kinds: Vec<_> = std::iter::from_fn(|| client.take_outbound())
        .map(|frame| frame.kind)
        .collect();
    assert_eq!(kinds, [ClientKind::Request, ClientKind::Subscribe]);
}

#[test]
fn request_scoped_error_terminates_only_the_matching_pending_request() {
    let mut client = ready_client();
    let first = client.begin(Query::Bootstrap).unwrap();
    let second = client.begin(Query::Bootstrap).unwrap();
    let _first_frame = client.take_outbound();
    let _second_frame = client.take_outbound();

    let error = client
        .apply(request_error_frame(
            first.id(),
            ErrorCode::Timeout,
            Retryability::Never,
        ))
        .unwrap_err();
    assert!(matches!(error, ClientError::Server(_)));
    assert!(!client.is_pending(&first));
    assert!(client.is_pending(&second));
}

#[test]
fn request_scoped_error_with_unknown_id_is_rejected() {
    let mut client = ready_client();
    let unknown = RequestId::from_u128(404);
    let error = client
        .apply(request_error_frame(
            &unknown,
            ErrorCode::Timeout,
            Retryability::Never,
        ))
        .unwrap_err();
    assert_eq!(error, ClientError::UnknownResponse(unknown));
}

#[test]
fn real_server_terminal_error_codes_reach_terminal_client_states() {
    let mut rejected = ClientState::new(ClientConfig::default());
    rejected
        .connect(ConnectTarget::new(
            "wss://example.test/api/v1/control",
            "bad",
        ))
        .unwrap();
    let _hello = rejected.take_outbound();
    assert_eq!(
        rejected
            .apply(error_frame(ErrorCode::Unauthorized, Retryability::Never))
            .unwrap_err(),
        ClientError::AuthenticationRejected
    );
    assert_eq!(rejected.phase(), ClientPhase::WebGate);

    let mut incompatible = ClientState::new(ClientConfig::default());
    incompatible
        .connect(ConnectTarget::new(
            "wss://example.test/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _hello = incompatible.take_outbound();
    assert_eq!(
        incompatible
            .apply(error_frame(
                ErrorCode::IncompatibleProtocol,
                Retryability::Never,
            ))
            .unwrap_err(),
        ClientError::IncompatibleProtocol {
            client_major: 1,
            server_major: 2,
        }
    );
    assert_eq!(incompatible.phase(), ClientPhase::UpgradeRequired);
    assert_eq!(incompatible.retry_schedule(), None);
}

#[test]
fn connect_target_debug_redacts_access_token() {
    let debug = format!(
        "{:?}",
        ConnectTarget::new("wss://example.test/api/v1/control", "top-secret")
    );
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("top-secret"));
}

fn ready_client() -> ClientState {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "ws://localhost/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _hello = client.take_outbound();
    client.apply(welcome()).unwrap();
    client
}

fn welcome() -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(Welcome {
            protocol: ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec![],
            session_id: SessionId::new("session-1"),
            server_instance_id: "server-1".into(),
            resume_status: ResumeStatus::Fresh,
        })
        .unwrap(),
    }
}

fn welcome_resumed() -> ServerFrame {
    let mut frame = welcome();
    frame.payload = serde_json::to_value(Welcome {
        protocol: ProtocolVersion { major: 1, minor: 1 },
        capabilities: vec![],
        session_id: SessionId::new("session-2"),
        server_instance_id: "server-1".into(),
        resume_status: ResumeStatus::Resumed,
    })
    .unwrap();
    frame
}

fn subscribed(id: &SubscriptionId, sequence: u64) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Subscribed,
        request_id: None,
        subscription_id: Some(id.clone()),
        sequence: Some(sequence),
        payload: serde_json::to_value(Subscribed).unwrap(),
    }
}

fn event(id: &SubscriptionId, sequence: u64) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Event,
        request_id: None,
        subscription_id: Some(id.clone()),
        sequence: Some(sequence),
        payload: serde_json::to_value(Event {
            event_kind: "bootstrapStatus".into(),
            revision: None,
            payload: serde_json::json!({"ready": true}),
        })
        .unwrap(),
    }
}

fn resync_required(sequence: u64) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::ResyncRequired,
        request_id: None,
        subscription_id: None,
        sequence: Some(sequence),
        payload: serde_json::json!({"reason":"journal unavailable"}),
    }
}

fn error_frame(code: ErrorCode, retryability: Retryability) -> ServerFrame {
    let mut error = ErrorFrame::new(
        code,
        "safe error",
        retryability,
        ErrorScope::Session,
        "correlation-1",
    );
    if code == ErrorCode::IncompatibleProtocol {
        error = error.with_details(serde_json::json!({"serverProtocolMajor": 2}));
    }
    ServerFrame {
        kind: ServerKind::Error,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(error).unwrap(),
    }
}

fn request_error_frame(
    request_id: &RequestId,
    code: ErrorCode,
    retryability: Retryability,
) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Error,
        request_id: Some(request_id.clone()),
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(ErrorFrame::new(
            code,
            "request failed",
            retryability,
            ErrorScope::Request,
            request_id.as_str(),
        ))
        .unwrap(),
    }
}
