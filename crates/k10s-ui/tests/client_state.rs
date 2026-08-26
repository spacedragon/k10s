use k10s_protocol::{
    BackendRevision, BootstrapResponse, CapacityUsage, ClientKind, ClusterTotals, ErrorCode,
    ErrorFrame, ErrorScope, Event, InfrastructureRequest, InfrastructureResponse,
    MetricsAvailability, MetricsCondition, MetricsStatus, ProtocolVersion, RequestId, ResumeStatus,
    Retryability, ServerFrame, ServerKind, SessionId, StorageInventory, Subscribed, SubscriptionId,
    Welcome,
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
    assert!(client.cancel(&cancelled).unwrap());
    assert!(!client.cancel(&cancelled).unwrap());
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
    assert!(client.expire_deadlines(1_249).unwrap().is_empty());
    assert_eq!(
        client.expire_deadlines(1_250).unwrap(),
        vec![expiring.clone()]
    );
    assert!(!client.is_pending(&expiring));
    let cancel = client.take_outbound().unwrap();
    assert_eq!(cancel.kind, ClientKind::CancelRequest);
    assert_eq!(cancel.request_id(), Some(expiring.id()));
}

#[test]
fn cancelled_request_terminal_frames_are_consumed_without_becoming_unknown() {
    let mut client = ready_client();
    let response_request = client.begin(Query::Bootstrap).unwrap();
    let _request = client.take_outbound().unwrap();
    assert!(client.cancel(&response_request).unwrap());
    let _cancel = client.take_outbound().unwrap();
    client
        .apply(ServerFrame::response(
            response_request.id().clone(),
            BootstrapResponse::fixture(),
        ))
        .expect("a response already queued before cancellation is consumed");
    assert!(client.take(response_request).is_none());

    let error_request = client.begin(Query::Bootstrap).unwrap();
    let _request = client.take_outbound().unwrap();
    assert!(client.cancel(&error_request).unwrap());
    let _cancel = client.take_outbound().unwrap();
    client
        .apply(request_error_frame(
            error_request.id(),
            ErrorCode::Cancelled,
            Retryability::Never,
        ))
        .expect("the cancellation error closes the tombstone without failing the session");
    assert!(client.take(error_request).is_none());
}

#[test]
fn late_infrastructure_response_cannot_regress_newer_telemetry() {
    let mut client = ready_client();
    let request = client
        .begin(Query::Infrastructure(InfrastructureRequest {
            context: "dev-local".into(),
        }))
        .unwrap();
    let _request = client.take_outbound().unwrap();
    let subscription = client.subscribe_infrastructure("dev-local").unwrap();
    let _subscribe = client.take_outbound().unwrap();
    client.apply(subscribed(subscription.id(), 1)).unwrap();
    let _ack = client.take_outbound().unwrap();

    let newer = infrastructure_response(2, MetricsAvailability::Partial);
    client
        .apply(ServerFrame {
            kind: ServerKind::Event,
            request_id: None,
            subscription_id: Some(subscription.id().clone()),
            sequence: Some(2),
            payload: serde_json::to_value(Event {
                event_kind: k10s_protocol::INFRASTRUCTURE_EVENT_UPDATED.into(),
                revision: Some("2".into()),
                payload: serde_json::to_value(newer).unwrap(),
            })
            .unwrap(),
        })
        .unwrap();
    let _ack = client.take_outbound().unwrap();

    let older = infrastructure_response(1, MetricsAvailability::Available);
    client
        .apply(ServerFrame::response(request.id().clone(), older))
        .unwrap();

    assert_eq!(
        client.infrastructure("dev-local").unwrap().revision,
        BackendRevision::new(2)
    );
    let QueryResult::Infrastructure(completed) = client.take(request).unwrap() else {
        panic!("query still completes with its own response");
    };
    assert_eq!(completed.revision, BackendRevision::new(1));
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
    assert!(!client.retry_if_due(10_050).unwrap());
    assert!(client.retry_if_due(10_100).unwrap());

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
    assert!(client.retry_if_due(1_000).unwrap());
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
    assert_eq!(
        kinds,
        [ClientKind::Request, ClientKind::Subscribe, ClientKind::Ack]
    );
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
        assert!(!client.retry_if_due(u64::MAX).unwrap());
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
    assert!(client.cancel(&pending).unwrap());

    client.transport_lost(1_000, 0);
    assert!(client.take_outbound().is_none());
    assert!(client.retry_if_due(1_000).unwrap());
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
    assert_eq!(
        kinds,
        [ClientKind::Request, ClientKind::Subscribe, ClientKind::Ack]
    );
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

#[test]
fn client_state_debug_never_renders_queued_hello_credentials() {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "wss://example.test/api/v1/control",
            "queued-exact-secret",
        ))
        .unwrap();

    let debug = format!("{client:?}");
    assert!(!debug.contains("queued-exact-secret"));
    assert!(!debug.contains("accessToken"));
}

#[test]
fn reliable_outbound_capacity_is_an_exact_hard_bound() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 2,
        ..ClientConfig::default()
    });
    let first = client.begin(Query::Bootstrap).unwrap();
    let second = client.begin(Query::Bootstrap).unwrap();
    assert_eq!(client.outbound_len(), 2);

    assert_eq!(
        client.begin(Query::Bootstrap).unwrap_err(),
        ClientError::OutboundOverload { capacity: 2 }
    );
    assert_eq!(client.outbound_len(), 2);
    assert!(client.is_pending(&first));
    assert!(client.is_pending(&second));
    assert_eq!(client.phase(), ClientPhase::Closed);
    assert!(client.take_outbound().is_none());
}

#[test]
fn subscribe_rolls_back_when_reliable_outbound_is_full() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 3,
        ..ClientConfig::default()
    });
    let _first = client.begin(Query::Bootstrap).unwrap();
    let _second = client.begin(Query::Bootstrap).unwrap();
    let _third = client.begin(Query::Bootstrap).unwrap();
    assert_eq!(client.outbound_len(), 3);
    assert_eq!(client.live_subscription_count(), 0);

    assert_eq!(
        client.subscribe_bootstrap_status().unwrap_err(),
        ClientError::OutboundOverload { capacity: 3 }
    );
    assert_eq!(client.outbound_len(), 3);
    assert_eq!(client.live_subscription_count(), 0);
}

#[test]
fn subscription_change_preflight_is_non_mutating_when_outbound_is_full() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 4,
        ..ClientConfig::default()
    });
    client
        .subscribe_resource("dev", "", "v1", "Pod", Some("default".to_owned()))
        .unwrap();
    client.begin(Query::Bootstrap).unwrap();
    client.begin(Query::Bootstrap).unwrap();
    assert_eq!(client.outbound_len(), 3);

    assert_eq!(
        client.preflight_subscription_changes(1, 1).unwrap_err(),
        ClientError::OutboundOverload { capacity: 4 }
    );
    assert_eq!(client.phase(), ClientPhase::Ready);
    assert_eq!(client.outbound_len(), 3);
    assert_eq!(client.live_subscription_count(), 1);
}

#[test]
fn subscription_change_preflight_rejects_final_live_set_before_mutation() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 4,
        ..ClientConfig::default()
    });
    client
        .subscribe_resource("dev", "", "v1", "Pod", Some("a".to_owned()))
        .unwrap();
    client
        .subscribe_resource("dev", "", "v1", "Pod", Some("b".to_owned()))
        .unwrap();

    assert_eq!(
        client.preflight_subscription_changes(0, 1).unwrap_err(),
        ClientError::LiveSubscriptionLimit { limit: 2 }
    );
    assert_eq!(client.outbound_len(), 2);
    assert_eq!(client.live_subscription_count(), 2);
}

#[test]
fn cancel_keeps_request_pending_when_cancel_frame_cannot_be_enqueued() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 1,
        ..ClientConfig::default()
    });
    let pending = client.begin(Query::Bootstrap).unwrap();

    assert_eq!(
        client.cancel(&pending).unwrap_err(),
        ClientError::OutboundOverload { capacity: 1 }
    );
    assert!(client.is_pending(&pending));
    assert_eq!(client.outbound_len(), 1);
}

#[test]
fn zero_capacity_rejects_hello_without_queuing_or_authenticating() {
    let mut client = ClientState::new(ClientConfig {
        outbound_capacity: 0,
        ..ClientConfig::default()
    });
    assert_eq!(
        client
            .connect(ConnectTarget::new(
                "wss://example.test/api/v1/control",
                "secret",
            ))
            .unwrap_err(),
        ClientError::OutboundOverload { capacity: 0 }
    );
    assert_eq!(client.outbound_len(), 0);
    assert_eq!(client.phase(), ClientPhase::Closed);
}

#[test]
fn recovery_preflights_the_whole_reliable_batch_and_rolls_back() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 1,
        ..ClientConfig::default()
    });

    assert_eq!(
        client.apply(resync_required(1)).unwrap_err(),
        ClientError::OutboundOverload { capacity: 1 }
    );
    assert_eq!(client.live_subscription_count(), 0);
    assert_eq!(client.outbound_len(), 0);
    assert_eq!(client.last_acked_sequence(), None);
    assert_eq!(client.phase(), ClientPhase::Closed);
}

#[test]
fn malformed_sequenced_event_does_not_advance_or_ack_cursor() {
    let mut client = ready_client();
    let malformed = ServerFrame {
        kind: ServerKind::Event,
        request_id: None,
        subscription_id: Some(SubscriptionId::new("sub-malformed")),
        sequence: Some(1),
        payload: serde_json::json!({}),
    };

    assert!(matches!(
        client.apply(malformed).unwrap_err(),
        ClientError::Protocol(_)
    ));
    assert_eq!(client.last_acked_sequence(), None);
    assert_eq!(client.outbound_len(), 0);
}

#[test]
fn ack_overload_fails_closed_without_claiming_cursor_progress() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 1,
        ..ClientConfig::default()
    });
    let _pending = client.begin(Query::Bootstrap).unwrap();
    assert_eq!(client.outbound_len(), 1);

    assert_eq!(
        client
            .apply(event(&SubscriptionId::new("sub-1"), 1))
            .unwrap_err(),
        ClientError::OutboundOverload { capacity: 1 }
    );
    assert_eq!(client.last_acked_sequence(), None);
    assert_eq!(client.outbound_len(), 1);
    assert_eq!(client.phase(), ClientPhase::Closed);
    assert!(client.server_state_invalid());
}

#[test]
fn undrained_acks_coalesce_to_the_highest_contiguous_cursor_at_exact_bound() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 1,
        ..ClientConfig::default()
    });
    let subscription = SubscriptionId::new("sub-1");

    client.apply(event(&subscription, 1)).unwrap();
    assert_eq!(client.outbound_len(), 1);
    client.apply(event(&subscription, 2)).unwrap();
    assert_eq!(client.outbound_len(), 1);
    assert_eq!(client.last_acked_sequence(), Some(2));

    let ack = client.take_outbound().unwrap();
    let k10s_protocol::ClientPayload::Ack(ack) = ack.decode_payload().unwrap() else {
        panic!("expected coalesced ack");
    };
    assert_eq!(ack.last_acked_sequence, 2);
}

#[test]
fn maximum_live_subscription_set_always_fits_sequenced_resync_recovery() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 4,
        ..ClientConfig::default()
    });
    assert_eq!(client.live_subscription_limit(), 2);

    for _ in 0..2 {
        let _subscription = client.subscribe_bootstrap_status().unwrap();
        assert_eq!(client.take_outbound().unwrap().kind, ClientKind::Subscribe);
    }
    assert_eq!(client.live_subscription_count(), 2);

    assert_eq!(
        client.subscribe_bootstrap_status().unwrap_err(),
        ClientError::LiveSubscriptionLimit { limit: 2 }
    );
    assert_eq!(client.live_subscription_count(), 2);
    assert_eq!(client.phase(), ClientPhase::Ready);

    client.apply(resync_required(1)).unwrap();
    assert_eq!(client.outbound_len(), 4);
    let kinds: Vec<_> = std::iter::from_fn(|| client.take_outbound())
        .map(|frame| frame.kind)
        .collect();
    assert_eq!(
        kinds,
        [
            ClientKind::Request,
            ClientKind::Subscribe,
            ClientKind::Subscribe,
            ClientKind::Ack,
        ]
    );
}

#[test]
fn small_outbound_config_rejects_unrecoverable_live_subscription_set() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 1,
        ..ClientConfig::default()
    });
    assert_eq!(client.live_subscription_limit(), 0);
    assert_eq!(
        client.subscribe_bootstrap_status().unwrap_err(),
        ClientError::LiveSubscriptionLimit { limit: 0 }
    );
    assert_eq!(client.live_subscription_count(), 0);
    assert_eq!(client.outbound_len(), 0);
    assert_eq!(client.phase(), ClientPhase::Ready);
}

#[test]
fn pending_and_completed_requests_share_an_exact_retention_budget() {
    let mut client = ready_client_with_config(ClientConfig {
        request_capacity: 2,
        ..ClientConfig::default()
    });
    let first = client.begin(Query::Bootstrap).unwrap();
    let _first_frame = client.take_outbound();
    let second = client.begin(Query::Bootstrap).unwrap();
    let _second_frame = client.take_outbound();

    assert_eq!(
        client.begin(Query::Bootstrap).unwrap_err(),
        ClientError::RequestRetentionLimit { limit: 2 }
    );
    assert_eq!(client.outbound_len(), 0);
    assert!(client.is_pending(&first));
    assert!(client.is_pending(&second));

    client
        .apply(ServerFrame::response(
            first.id().clone(),
            BootstrapResponse::fixture(),
        ))
        .unwrap();
    assert!(!client.is_pending(&first));
    assert_eq!(
        client.begin(Query::Bootstrap).unwrap_err(),
        ClientError::RequestRetentionLimit { limit: 2 }
    );

    assert_eq!(
        client.take(first),
        Some(QueryResult::Bootstrap(BootstrapResponse::fixture()))
    );
    let third = client.begin(Query::Bootstrap).unwrap();
    assert!(client.is_pending(&third));
    assert!(client.is_pending(&second));
}

#[test]
fn resync_clears_retained_results_before_allocating_recovery_bootstrap() {
    let mut client = ready_client_with_config(ClientConfig {
        outbound_capacity: 2,
        request_capacity: 1,
        ..ClientConfig::default()
    });
    let completed = client.begin(Query::Bootstrap).unwrap();
    let _request = client.take_outbound();
    client
        .apply(ServerFrame::response(
            completed.id().clone(),
            BootstrapResponse::fixture(),
        ))
        .unwrap();

    client.apply(resync_required(1)).unwrap();
    assert!(client.take(completed).is_none());
    assert_eq!(client.outbound_len(), 2);
    assert_eq!(
        client.begin(Query::Bootstrap).unwrap_err(),
        ClientError::RequestRetentionLimit { limit: 1 }
    );
}

fn ready_client() -> ClientState {
    ready_client_with_config(ClientConfig::default())
}

fn ready_client_with_config(config: ClientConfig) -> ClientState {
    let mut client = ClientState::new(config);
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

fn infrastructure_response(
    revision: u64,
    availability: MetricsAvailability,
) -> InfrastructureResponse {
    InfrastructureResponse {
        context: "dev-local".into(),
        revision: BackendRevision::new(revision),
        generated_at: format!("2026-08-21T01:0{revision}:00Z"),
        totals: ClusterTotals::default(),
        cluster_cpu: CapacityUsage::default(),
        cluster_memory: CapacityUsage::default(),
        pod_capacity: CapacityUsage::default(),
        metrics: MetricsStatus {
            availability,
            condition: if availability == MetricsAvailability::Partial {
                MetricsCondition::Partial
            } else {
                MetricsCondition::Fresh
            },
            source: "metrics.k8s.io".into(),
            source_updated_at: None,
            detail: "test metrics".into(),
        },
        workload_health: Vec::new(),
        attention: Vec::new(),
        nodes: Vec::new(),
        storage: StorageInventory::default(),
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
