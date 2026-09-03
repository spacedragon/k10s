use k10s_protocol::{
    Ack, BootstrapResponse, CONTROL_PATH, ClientPayload, Complete, EXEC_PATH, ErrorCode, Hello,
    LOGS_PATH, ProtocolVersion, Request, RequestId, ResumeStatus, ServerFrame, ServerPayload,
    SessionId, Welcome, decode_client_frame, decode_server_frame, validate_bootstrap_response,
};
use serde_json::json;

fn fixture(name: &str) -> serde_json::Value {
    let path = format!("tests/fixtures/protocol/{name}");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("missing fixture: {path}"));
    serde_json::from_str(&raw).unwrap()
}

#[test]
fn bootstrap_response_matches_v1_fixture() {
    let frame = ServerFrame::response(RequestId::from_u128(1), BootstrapResponse::fixture());
    assert_eq!(
        serde_json::to_value(&frame).unwrap(),
        fixture("bootstrap-v1.2.json")
    );
}

#[test]
fn unknown_kind_is_reported_without_panicking() {
    let raw = r#"{"kind":"future.notice","payload":{"x":1}}"#;
    let err = decode_client_frame(raw).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedMessage);
}

#[test]
fn kind_specific_envelope_metadata_is_required() {
    let request =
        decode_client_frame(r#"{"kind":"request","payload":{"kind":"bootstrap"}}"#).unwrap_err();
    assert_eq!(request.code, ErrorCode::InvalidRequest);

    for frame in [
        json!({
            "kind": "event",
            "subscriptionId": "subscription-1",
            "payload": { "kind": "changed" }
        }),
        json!({
            "kind": "event",
            "sequence": 1,
            "payload": { "kind": "changed" }
        }),
        json!({ "kind": "complete", "payload": null }),
        json!({ "kind": "response", "payload": {} }),
    ] {
        let error = decode_server_frame(frame).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidRequest);
    }
}

#[test]
fn current_client_decodes_previous_minor_and_ignores_optional_fields() {
    let frame = decode_server_frame(fixture("bootstrap-v1.0.json")).unwrap();
    let bootstrap = validate_bootstrap_response(&frame.payload).unwrap();

    assert_eq!(bootstrap.protocol, ProtocolVersion { major: 1, minor: 0 });
    assert_eq!(bootstrap.capabilities, ["logs.tail"]);
    assert_eq!(bootstrap.server, None);
}

#[test]
fn current_client_decodes_the_previous_v1_1_minor() {
    let frame = decode_server_frame(fixture("bootstrap-v1.1.json")).unwrap();
    let bootstrap = validate_bootstrap_response(&frame.payload).unwrap();

    assert_eq!(bootstrap.protocol, ProtocolVersion { major: 1, minor: 1 });
}

#[test]
fn typed_client_payloads_preserve_negotiation_deadlines_and_ack_cursor() {
    let hello = decode_client_frame(
        r#"{"kind":"hello","payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":["logs.tail"],"accessToken":"secret"}}"#,
    )
    .unwrap();
    assert!(matches!(
        hello.decode_payload().unwrap(),
        ClientPayload::Hello(Hello {
            protocol_major: 1,
            protocol_minor: 1,
            ..
        })
    ));

    let request = decode_client_frame(
        r#"{"kind":"request","requestId":"7","payload":{"kind":"bootstrap","deadline":1500}}"#,
    )
    .unwrap();
    assert_eq!(request.request_id(), Some(&RequestId::from_u128(7)));
    assert!(matches!(
        request.decode_payload().unwrap(),
        ClientPayload::Request(Request {
            request_kind,
            deadline: Some(1500),
            ..
        }) if request_kind == "bootstrap"
    ));

    let ack = decode_client_frame(r#"{"kind":"ack","payload":{"lastAckedSequence":42}}"#).unwrap();
    assert!(matches!(
        ack.decode_payload().unwrap(),
        ClientPayload::Ack(Ack {
            last_acked_sequence: 42
        })
    ));
}

#[test]
fn typed_server_payloads_preserve_welcome_and_completion_sequence() {
    let welcome = decode_server_frame(json!({
        "kind": "welcome",
        "payload": {
            "protocol": { "major": 1, "minor": 1 },
            "capabilities": ["logs.tail"],
            "sessionId": "session-1",
            "serverInstanceId": "instance-1",
            "resumeStatus": "fresh"
        }
    }))
    .unwrap();
    assert!(matches!(
        welcome.decode_payload().unwrap(),
        ServerPayload::Welcome(Welcome {
            protocol: ProtocolVersion { major: 1, minor: 1 },
            session_id,
            resume_status: ResumeStatus::Fresh,
            ..
        }) if session_id == SessionId::from("session-1")
    ));

    let complete = decode_server_frame(json!({
        "kind": "complete",
        "subscriptionId": "subscription-1",
        "sequence": 9,
        "payload": null
    }))
    .unwrap();
    assert_eq!(complete.sequence(), Some(9));
    assert!(matches!(
        complete.decode_payload().unwrap(),
        ServerPayload::Complete(Complete)
    ));
}

#[test]
fn application_routes_are_stable() {
    assert_eq!(CONTROL_PATH, "/api/v1/control");
    assert_eq!(LOGS_PATH, "/api/v1/logs");
    assert_eq!(EXEC_PATH, "/api/v1/exec");
}

#[test]
fn retired_exec_discriminants_remain_reserved_for_the_major_one_tombstone() {
    assert_eq!(EXEC_PATH, "/api/v1/exec");
    let legacy: k10s_protocol::StreamTicketRequest = serde_json::from_value(json!({
        "target": {"context":"dev","namespace":"default","pod":"web","uid":"uid-web","container":"app"},
        "streamType":"exec","tty":true,"command":["/bin/sh"]
    })).unwrap();
    assert_eq!(legacy.stream_type, k10s_protocol::StreamType::Exec);
    for (kind, value) in [
        (k10s_protocol::payload_kind::TTY_OUTPUT, 3),
        (k10s_protocol::payload_kind::STDIN, 4),
        (k10s_protocol::payload_kind::RESIZE, 5),
    ] {
        assert_eq!(kind, value);
        assert_eq!(
            k10s_protocol::decode_stream_payload(&[1, kind])
                .unwrap()
                .kind,
            kind
        );
    }
}

#[test]
fn active_exec_symbols_are_absent_from_production_layers() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .unwrap();
    let forbidden = [
        "StreamKind::Exec",
        "StreamRouteKind::Exec",
        "StreamRoute::Exec",
        "ExecSessions",
        "send_stdin",
        "send_resize",
        "exec.attach",
    ];
    for relative in [
        "crates/k10s-backend/src",
        "crates/k10s-server/src",
        "crates/k10s-ui/src/client",
    ] {
        assert_source_tree_omits(&workspace.join(relative), &forbidden);
    }
}

fn assert_source_tree_omits(directory: &std::path::Path, forbidden: &[&str]) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_source_tree_omits(&path, forbidden);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            for symbol in forbidden {
                assert!(
                    !source.contains(symbol),
                    "{} contains {symbol}",
                    path.display()
                );
            }
        }
    }
}
