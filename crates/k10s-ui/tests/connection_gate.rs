use k10s_protocol::CONTROL_PATH;
use k10s_ui::{ConnectionGate, GateError, derive_control_url};

#[test]
fn fresh_web_app_shows_an_empty_token_gate() {
    let gate = ConnectionGate::new("ws://example.test/api/v1/control");

    assert!(gate.is_visible());
    assert!(gate.token_input().is_empty());
    assert_eq!(gate.error(), None);
}

#[test]
fn wrong_token_returns_to_a_clean_gate() {
    let mut gate = ConnectionGate::new("ws://example.test/api/v1/control");
    gate.set_token_input("wrong-secret");
    let _target = gate.begin_connection().unwrap();

    gate.authentication_rejected();

    assert!(gate.is_visible());
    assert!(gate.token_input().is_empty());
    assert_eq!(gate.error(), Some("Authentication failed. Try again."));
}

#[test]
fn successful_authentication_clears_the_input_buffer() {
    let mut gate = ConnectionGate::new("ws://example.test/api/v1/control");
    gate.set_token_input("secret");
    let target = gate.begin_connection().unwrap();

    gate.authentication_succeeded();

    assert_eq!(target.url(), "ws://example.test/api/v1/control");
    assert!(gate.token_input().is_empty());
    assert!(!gate.is_visible());
}

#[test]
fn empty_token_stays_at_the_gate() {
    let mut gate = ConnectionGate::new("ws://example.test/api/v1/control");

    assert_eq!(gate.begin_connection(), Err(GateError::EmptyToken));
    assert!(gate.is_visible());
}

#[test]
fn persisted_settings_never_include_the_token() {
    let mut gate = ConnectionGate::new("ws://example.test/api/v1/control");
    gate.set_token_input("do-not-persist");

    let serialized = serde_json::to_string(gate.persisted_settings()).unwrap();

    assert!(!serialized.contains("do-not-persist"));
    assert_eq!(
        serialized,
        r#"{"control_url":"ws://example.test/api/v1/control"}"#
    );
    assert!(!format!("{gate:?}").contains("do-not-persist"));
}

#[test]
fn location_derivation_preserves_authority_and_replaces_the_path() {
    assert_eq!(
        derive_control_url("http:", "127.0.0.1:8080").unwrap(),
        format!("ws://127.0.0.1:8080{CONTROL_PATH}")
    );
    assert_eq!(
        derive_control_url("https:", "host.example").unwrap(),
        format!("wss://host.example{CONTROL_PATH}")
    );
}

#[test]
fn location_derivation_rejects_non_http_schemes_and_empty_authorities() {
    assert!(derive_control_url("file:", "").is_err());
    assert!(derive_control_url("https:", "").is_err());
}
