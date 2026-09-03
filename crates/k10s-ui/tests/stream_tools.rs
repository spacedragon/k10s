//! Pure state-machine tests for the connected log viewer.

use k10s_protocol::{ClientKind, StreamTarget, StreamTicketResponse, StreamType};
use k10s_ui::client::{ConnectTarget, Query};
use k10s_ui::ui::tools::{LogsPhase, LogsTool, LogsViews, MAX_LINE_CHARS, TRUNCATION_MARKER};
use k10s_ui::workspace::WindowId;

fn logs_tool() -> LogsTool {
    LogsTool::new(
        StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: "web-frontend-7d9f8-00001".into(),
            uid: "uid-web".into(),
            container: "app".into(),
        },
        3,
    )
}

#[test]
fn fresh_logs_claim_exactly_one_automatic_connection_attempt() {
    let mut tool = logs_tool();

    tool.set_follow(false);
    assert!(tool.begin_auto_connect());
    assert_eq!(tool.phase(), LogsPhase::Connecting);
    assert!(tool.follows());
    assert!(
        !tool.begin_auto_connect(),
        "the automatic claim is one-shot"
    );

    tool.attach();
    assert_eq!(tool.phase(), LogsPhase::Streaming);
    assert!(
        !tool.begin_auto_connect(),
        "a live stream cannot claim again"
    );
}

#[test]
fn failed_automatic_connection_requires_one_explicit_retry() {
    let mut tool = logs_tool();
    assert!(tool.begin_auto_connect());
    tool.fail("ticket rejected");

    assert_eq!(tool.phase(), LogsPhase::Disconnected);
    assert_eq!(tool.last_error(), Some("ticket rejected"));
    assert!(!tool.begin_auto_connect());
    assert!(tool.can_retry());

    tool.set_follow(false);
    assert!(tool.retry());
    assert_eq!(tool.phase(), LogsPhase::Connecting);
    assert!(tool.follows());
    assert_eq!(tool.last_error(), None);
    assert!(!tool.retry(), "explicit retry also starts only once");
    assert!(
        tool.take_scroll_reset(),
        "retry must request one renderer-side bottom reset"
    );
    assert!(!tool.take_scroll_reset(), "the reset is consumed once");
}

#[test]
fn connection_loss_preserves_history_and_exposes_retry() {
    let mut tool = logs_tool();
    assert!(tool.begin_auto_connect());
    tool.attach();
    tool.append("before disconnect");
    tool.connection_lost();

    assert_eq!(tool.phase(), LogsPhase::Disconnected);
    assert_eq!(tool.last_error(), Some("log stream disconnected"));
    assert!(!tool.begin_auto_connect());
    assert!(tool.can_retry());
    assert_eq!(tool.export_text(), "before disconnect");
}

#[test]
fn changing_each_log_source_restores_auto_connect_and_follow() {
    let mut container = logs_tool();
    assert!(container.begin_auto_connect());
    container.fail("no stream");
    container.set_follow(false);
    container.select_container("metrics");
    assert!(container.follows());
    assert!(container.take_scroll_reset());
    assert!(!container.take_scroll_reset());
    assert!(container.begin_auto_connect());

    let mut previous = logs_tool();
    previous.connect();
    previous.attach();
    previous.append("retained across previous change");
    previous.connection_lost();
    previous.set_follow(false);
    previous.set_previous(true);
    assert!(previous.follows());
    assert!(previous.take_scroll_reset());
    assert!(previous.begin_auto_connect());
    assert_eq!(previous.export_text(), "");

    let mut since = logs_tool();
    since.connect();
    since.attach();
    since.append("retained across since change");
    since.connection_lost();
    since.set_follow(false);
    since.set_since_seconds(Some(900));
    assert!(since.follows());
    assert!(since.take_scroll_reset());
    assert!(since.begin_auto_connect());
    assert_eq!(since.export_text(), "");
}

#[test]
fn logs_tail_truncation_keeps_the_newest_lines_and_counts_dropped() {
    let mut tool = logs_tool();
    assert_eq!(tool.phase(), LogsPhase::Disconnected);

    tool.connect();
    tool.attach();
    tool.append("line-1");
    tool.append("line-2");
    tool.append("line-3");
    tool.append("line-4");

    let visible: Vec<_> = tool.visible_lines().map(String::as_str).collect();
    assert_eq!(visible, ["line-2", "line-3", "line-4"], "tail bound is 3");
    assert_eq!(tool.truncated_lines(), 1, "the oldest line was dropped");
}

#[test]
fn log_source_toolbar_state_controls_container_history_wrap_and_export() {
    let mut tool = logs_tool();
    tool.set_previous(true);
    tool.set_since_seconds(Some(900));
    tool.set_wrap(true);
    tool.select_container("metrics");

    assert!(tool.previous());
    assert_eq!(tool.since_seconds(), Some(900));
    assert!(tool.wraps());
    assert_eq!(tool.target().container, "metrics");

    tool.connect();
    tool.attach();
    tool.append("first");
    tool.append("second");
    assert_eq!(tool.export_text(), "first\nsecond");
}

#[test]
fn changing_log_source_mode_clears_history_before_the_replacement_stream() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    tool.append("current-container output");

    tool.set_previous(true);
    assert_eq!(tool.phase(), LogsPhase::Disconnected);
    assert_eq!(tool.export_text(), "");

    tool.connect();
    tool.attach();
    tool.append("previous-container output");
    tool.set_since_seconds(Some(900));
    assert_eq!(tool.phase(), LogsPhase::Disconnected);
    assert_eq!(tool.export_text(), "");
}

#[test]
fn selected_container_survives_default_target_reconciliation() {
    let window = WindowId(42);
    let mut views = LogsViews::default();
    let default_target = logs_tool().target().clone();
    views
        .ensure(window, default_target.clone())
        .select_container("metrics");

    let reconciled = views.ensure(window, default_target);
    assert_eq!(reconciled.target().container, "metrics");
}

#[test]
fn oversize_lines_are_truncated_with_a_marker() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    let long = "x".repeat(MAX_LINE_CHARS + 500);
    tool.append(&long);
    let line = tool.visible_lines().next().unwrap();
    assert!(line.chars().count() <= MAX_LINE_CHARS + TRUNCATION_MARKER.len());
    assert!(line.ends_with(TRUNCATION_MARKER));
}

#[test]
fn pause_stops_buffering_and_resume_continues() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    tool.pause();
    assert!(tool.is_paused());
    tool.append("dropped-1");
    tool.append("dropped-2");
    assert!(
        tool.visible_lines().count() == 0,
        "a paused stream buffers nothing"
    );
    assert_eq!(tool.paused_dropped_lines(), 2);

    tool.resume();
    assert!(!tool.is_paused());
    tool.append("kept");
    let kept: Vec<_> = tool.visible_lines().map(String::as_str).collect();
    assert_eq!(kept, ["kept"]);
}

#[test]
fn find_filters_retained_lines_case_insensitively() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    for line in [
        "GET /healthz 200",
        "ERROR upstream timeout",
        "get /metrics 200",
    ] {
        tool.append(line);
    }
    tool.set_find(Some("error"));
    let matches: Vec<_> = tool.find_matches().iter().map(|l| l.as_str()).collect();
    assert_eq!(matches, ["ERROR upstream timeout"]);
    tool.set_find(None);
    assert_eq!(tool.find_matches().len(), 3);

    // Find never destroys the retained buffer.
    assert_eq!(tool.visible_lines().count(), 3);
}

#[test]
fn connection_loss_marks_the_log_view_disconnected_without_losing_history() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    tool.append("before");
    tool.connection_lost();
    assert_eq!(tool.phase(), LogsPhase::Disconnected);
    let before: Vec<_> = tool.visible_lines().map(String::as_str).collect();
    assert_eq!(before, ["before"]);
}

#[test]
fn client_state_encodes_stream_ticket_queries_safely() {
    use k10s_protocol::{REQUEST_STREAM_TICKET, ServerFrame, ServerKind};

    let target = StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "web-frontend-7d9f8-00001".into(),
        uid: "uid-web".into(),
        container: "app".into(),
    };
    let mut client = k10s_ui::client::ClientState::new(k10s_ui::client::ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "ws://127.0.0.1:1/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _hello = client.take_outbound().unwrap();
    client.apply_at(welcome_frame(), 0, 0).unwrap();

    let pending = client
        .begin(Query::StreamTicket {
            target: target.clone(),
            since_seconds: None,
            previous: false,
        })
        .unwrap();
    let frame = client.take_outbound().unwrap();
    assert_eq!(frame.kind, ClientKind::Request);
    let raw = serde_json::to_value(&frame).unwrap();
    assert_eq!(raw["payload"]["kind"], json_str(REQUEST_STREAM_TICKET));
    assert_eq!(
        raw["payload"]["payload"]["target"]["pod"],
        "web-frontend-7d9f8-00001"
    );
    assert_eq!(raw["payload"]["payload"]["streamType"], "logs");
    assert_eq!(raw["payload"]["payload"]["tty"], false);

    let response = ServerFrame {
        kind: ServerKind::Response,
        request_id: Some(pending.id().clone()),
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(StreamTicketResponse {
            ticket_id: "stream-ticket-0001".into(),
            target: target.clone(),
            stream_type: StreamType::Logs,
            tty: false,
        })
        .unwrap(),
    };
    client.apply(response).unwrap();
    let result = client.take(pending).expect("decoded result");
    match result {
        k10s_ui::client::QueryResult::StreamTicket(granted) => {
            assert_eq!(granted.ticket_id, "stream-ticket-0001");
            assert_eq!(granted.stream_type, StreamType::Logs);
            assert!(!granted.tty);
        }
        other => panic!("expected a stream ticket, got {other:?}"),
    }

    let target_debug = format!("{target:?}");
    assert!(!target_debug.contains("secret"));
}

fn welcome_frame() -> k10s_protocol::ServerFrame {
    k10s_protocol::ServerFrame {
        kind: k10s_protocol::ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(k10s_protocol::Welcome {
            protocol: k10s_protocol::ProtocolVersion {
                major: k10s_protocol::PROTOCOL_MAJOR,
                minor: k10s_protocol::PROTOCOL_MINOR,
            },
            capabilities: vec![],
            session_id: k10s_protocol::SessionId::new(String::from("session-1")),
            server_instance_id: "server-1".into(),
            resume_status: k10s_protocol::ResumeStatus::Fresh,
        })
        .unwrap(),
    }
}

fn json_str(value: &str) -> serde_json::Value {
    serde_json::Value::String(value.to_owned())
}

/// The dedicated-stream session glue: derive_stream_url keeps credentials
/// out of URLs, and a session projects server frames into signals.
#[test]
fn stream_sessions_derive_credential_free_urls_and_project_signals() {
    use ewebsock::{WsEvent, WsMessage};
    use k10s_ui::client::{StreamRoute, StreamSession, StreamSignal, derive_stream_url};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    let url = derive_stream_url("ws://127.0.0.1:1/api/v1/control", StreamRoute::Logs).unwrap();
    assert_eq!(url, "ws://127.0.0.1:1/api/v1/logs");
    assert!(!url.contains("secret"));
    assert!(derive_stream_url("ws://127.0.0.1:1/other", StreamRoute::Logs).is_err());

    // A scripted socket proves log signal projection
    // without any network.
    #[derive(Debug)]
    struct ScriptedSocket {
        sent_text: Arc<Mutex<Vec<String>>>,
        events: mpsc::Receiver<WsEvent>,
    }
    impl k10s_ui::client::StreamIo for ScriptedSocket {
        fn try_recv(&mut self) -> Option<WsEvent> {
            self.events.try_recv().ok()
        }
        fn send_text(&mut self, text: String) {
            self.sent_text.lock().unwrap().push(text);
        }
    }

    let (tx, rx) = mpsc::channel();
    let mut session = StreamSession::new(
        StreamRoute::Logs,
        StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: "db-postgres-0".into(),
            uid: "uid-db".into(),
            container: "app".into(),
        },
    );

    // Before open_with_ticket the session cannot be driven; inject the
    // scripted transport directly through the test seam.
    let sent_text = Arc::new(Mutex::new(Vec::new()));
    session.inject_for_test(ScriptedSocket {
        sent_text: Arc::clone(&sent_text),
        events: rx,
    });

    tx.send(WsEvent::Message(WsMessage::Text(
        r#"{"kind":"ready","streamType":"logs","tty":false,"container":"app"}"#.to_owned(),
    )))
    .unwrap();
    tx.send(WsEvent::Message(WsMessage::Binary(vec![
        k10s_protocol::STREAM_PAYLOAD_VERSION,
        k10s_protocol::payload_kind::STDOUT,
        b'$',
        b' ',
        b'o',
        b'k',
    ])))
    .unwrap();

    let signals = session.poll();
    assert_eq!(
        signals,
        vec![
            StreamSignal::Ready {
                stream_type: StreamType::Logs,
                container: "app".into(),
            },
            StreamSignal::Output("$ ok".to_owned()),
        ]
    );
}

#[test]
fn since_filters_older_lines_until_cleared() {
    let mut tool = logs_tool();
    tool.connect();
    tool.attach();
    tool.append("old-1");
    tool.append("old-2");

    tool.set_since_now();
    assert!(tool.since_active());
    tool.append("new-1");
    let visible: Vec<_> = tool.visible_lines().map(String::as_str).collect();
    assert_eq!(visible, ["new-1"], "only post-since lines are visible");

    tool.clear_since();
    assert!(!tool.since_active());
    let visible: Vec<_> = tool.visible_lines().map(String::as_str).collect();
    assert_eq!(visible, ["old-1", "old-2", "new-1"]);
}
