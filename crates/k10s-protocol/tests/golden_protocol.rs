use k10s_protocol::{
    BootstrapResponse, CONTROL_PATH, EXEC_PATH, ErrorCode, LOGS_PATH, RequestId, ServerFrame,
    decode_client_frame, decode_server_frame,
};

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
        fixture("bootstrap-v1.1.json")
    );
}

#[test]
fn unknown_kind_is_reported_without_panicking() {
    let raw = r#"{"kind":"future.notice","payload":{"x":1}}"#;
    let err = decode_client_frame(raw).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedMessage);
}

#[test]
fn current_client_decodes_previous_minor_and_ignores_optional_fields() {
    let frame = fixture("bootstrap-v1.0.json");
    assert!(decode_server_frame(frame).is_ok());
}

#[test]
fn application_routes_are_stable() {
    assert_eq!(CONTROL_PATH, "/api/v1/control");
    assert_eq!(LOGS_PATH, "/api/v1/logs");
    assert_eq!(EXEC_PATH, "/api/v1/exec");
}
