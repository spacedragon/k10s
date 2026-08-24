//! Pure state-machine tests for the connected log viewer and terminal
//! tools: tail truncation, follow/pause/find, explicit shell connect, TTY
//! input/resize/exit queueing, and disconnect handling. No egui runtime.

use k10s_protocol::{ClientKind, StreamTarget, StreamTicketResponse, StreamType};
use k10s_ui::client::{ConnectTarget, Query};
use k10s_ui::ui::tools::{
    LogsPhase, LogsTool, MAX_LINE_CHARS, ShellAction, ShellPhase, ShellTool, TRUNCATION_MARKER,
};

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
fn shell_requires_an_explicit_connect_before_attach() {
    let mut shell = ShellTool::new(StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "db-postgres-0".into(),
        uid: "uid-db".into(),
        container: "app".into(),
    });
    assert_eq!(*shell.phase(), ShellPhase::Disconnected);
    assert!(
        !shell.can_attach(),
        "attaching before an explicit connect must be impossible"
    );

    shell.connect();
    assert_eq!(*shell.phase(), ShellPhase::Connecting);
    assert!(shell.can_attach());
    shell.attach();
    assert_eq!(*shell.phase(), ShellPhase::Attached);
}

#[test]
fn tty_output_merges_into_one_terminal_buffer() {
    let mut shell = ShellTool::new(StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "db-postgres-0".into(),
        uid: "uid-db".into(),
        container: "app".into(),
    });
    shell.connect();
    shell.attach();

    // TTY mode merges every origin into a single stream; the tool cannot
    // tell (and does not care) which descriptor produced a line.
    shell.apply_output("$ ls\r\n");
    shell.apply_output("src\r\n");
    shell.apply_output("\r\n");
    let merged: Vec<_> = shell.buffer().map(String::as_str).collect();
    assert_eq!(merged.len(), 3);
    assert_eq!(merged[0], "$ ls");
    assert_eq!(merged[2], "");
    assert!(!shell.buffer_is_empty());
}

#[test]
fn stdin_and_resize_are_queued_as_drainable_actions() {
    let mut shell = ShellTool::new(StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "db-postgres-0".into(),
        uid: "uid-db".into(),
        container: "app".into(),
    });
    shell.connect();
    shell.attach();

    shell.send_input("echo hi");
    shell.send_input("exit");
    shell.resize(120, 40);

    let actions = shell.drain_actions();
    assert_eq!(
        actions,
        vec![
            ShellAction::Input("echo hi\n".into()),
            ShellAction::Input("exit\n".into()),
            ShellAction::Resize {
                cols: 120,
                rows: 40
            },
        ]
    );
    assert!(shell.drain_actions().is_empty(), "draining is one-shot");
}

#[test]
fn exit_and_disconnect_are_distinct_terminal_states() {
    let mut shell = ShellTool::new(StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "db-postgres-0".into(),
        uid: "uid-db".into(),
        container: "app".into(),
    });
    shell.connect();
    shell.attach();
    shell.apply_output("work\n");

    // Socket loss: the terminal is disconnected and the scrollback survives.
    shell.connection_lost();
    assert_eq!(
        *shell.phase(),
        ShellPhase::Failed("terminal disconnected".to_owned())
    );
    let scrollback: Vec<_> = shell.buffer().map(String::as_str).collect();
    assert_eq!(scrollback, ["work"]);

    // A clean exit reports the code and keeps the scrollback readable.
    let mut exited = ShellTool::new(StreamTarget {
        context: "dev-local".into(),
        namespace: "default".into(),
        pod: "db-postgres-0".into(),
        uid: "uid-db".into(),
        container: "app".into(),
    });
    exited.connect();
    exited.attach();
    exited.exit(0);
    assert_eq!(*exited.phase(), ShellPhase::Exited(0));
    assert!(
        !exited.can_attach(),
        "an exited session cannot be re-attached without a new connect"
    );
}

/// The shared client state encodes the stream.ticket request kind, decodes
/// its response, and never leaks credentials into URLs or debug output.
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
            stream_type: StreamType::Exec,
            tty: true,
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
    assert_eq!(raw["payload"]["payload"]["tty"], true);
    assert_eq!(
        raw["payload"]["payload"]["command"],
        serde_json::json!(["/bin/sh"])
    );

    let response = ServerFrame {
        kind: ServerKind::Response,
        request_id: Some(pending.id().clone()),
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(StreamTicketResponse {
            ticket_id: "stream-ticket-0001".into(),
            target: target.clone(),
            stream_type: StreamType::Exec,
            tty: true,
        })
        .unwrap(),
    };
    client.apply(response).unwrap();
    let result = client.take(pending).expect("decoded result");
    match result {
        k10s_ui::client::QueryResult::StreamTicket(granted) => {
            assert_eq!(granted.ticket_id, "stream-ticket-0001");
            assert_eq!(granted.stream_type, StreamType::Exec);
            assert!(granted.tty);
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
    assert!(derive_stream_url("ws://127.0.0.1:1/other", StreamRoute::Exec).is_err());

    // A scripted socket proves hello/stdin framing and signal projection
    // without any network.
    #[derive(Debug)]
    struct ScriptedSocket {
        sent_text: Arc<Mutex<Vec<String>>>,
        sent_binary: Arc<Mutex<Vec<Vec<u8>>>>,
        events: mpsc::Receiver<WsEvent>,
    }
    impl k10s_ui::client::StreamIo for ScriptedSocket {
        fn try_recv(&mut self) -> Option<WsEvent> {
            self.events.try_recv().ok()
        }
        fn send_text(&mut self, text: String) {
            self.sent_text.lock().unwrap().push(text);
        }
        fn send_binary(&mut self, bytes: Vec<u8>) {
            self.sent_binary.lock().unwrap().push(bytes);
        }
    }

    let (tx, rx) = mpsc::channel();
    let mut session = StreamSession::new(
        StreamRoute::Exec,
        StreamTarget {
            context: "dev-local".into(),
            namespace: "default".into(),
            pod: "db-postgres-0".into(),
            uid: "uid-db".into(),
            container: "app".into(),
        },
        true,
    );

    // Before open_with_ticket the session cannot be driven; inject the
    // scripted transport directly through the test seam.
    let sent_text = Arc::new(Mutex::new(Vec::new()));
    let sent_binary = Arc::new(Mutex::new(Vec::new()));
    session.inject_for_test(ScriptedSocket {
        sent_text: Arc::clone(&sent_text),
        sent_binary: Arc::clone(&sent_binary),
        events: rx,
    });

    tx.send(WsEvent::Message(WsMessage::Text(
        r#"{"kind":"ready","streamType":"exec","tty":true,"container":"app"}"#.to_owned(),
    )))
    .unwrap();
    tx.send(WsEvent::Message(WsMessage::Binary(vec![
        k10s_protocol::STREAM_PAYLOAD_VERSION,
        k10s_protocol::payload_kind::TTY_OUTPUT,
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
                stream_type: StreamType::Exec,
                tty: true,
                container: "app".into(),
            },
            StreamSignal::Output("$ ok".to_owned()),
        ]
    );

    // The newline comes from the tool's queued action; send_stdin sends
    // exactly what it is given (no double termination).
    session.send_stdin("ls\n");
    let binary = sent_binary.lock().unwrap();
    assert_eq!(binary.len(), 1);
    let decoded = k10s_protocol::decode_stream_payload(&binary[0]).unwrap();
    assert_eq!(decoded.kind, k10s_protocol::payload_kind::STDIN);
    assert_eq!(decoded.data, b"ls\n");
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
