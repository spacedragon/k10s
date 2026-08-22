use std::ops::ControlFlow;

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_ui::client::{WebSocketTransport, bounded_event_callback};

#[test]
fn undrained_burst_breaks_callback_exactly_at_inbox_capacity() {
    const CAPACITY: usize = 4;
    let (inbox, callback) = bounded_event_callback(CAPACITY);

    let mut accepted = 0;
    for index in 0..(CAPACITY + 3) {
        let event = WsEvent::Message(WsMessage::Text(index.to_string()));
        match callback(event) {
            ControlFlow::Continue(()) => accepted += 1,
            ControlFlow::Break(()) => break,
        }
    }

    assert_eq!(accepted, CAPACITY);
    assert!(inbox.overflowed());
    let drained: Vec<_> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert_eq!(drained.len(), CAPACITY);
    let messages: Vec<_> = drained
        .into_iter()
        .map(|event| match event {
            WsEvent::Message(WsMessage::Text(text)) => text,
            unexpected => panic!("unexpected event: {unexpected:?}"),
        })
        .collect();
    assert_eq!(messages, ["0", "1", "2", "3"]);
}

#[test]
fn transport_rejects_credentials_or_tokens_in_url_metadata() {
    for url in [
        "wss://user:secret@example.test/api/v1/control",
        "wss://example.test/api/v1/control?token=secret",
        "wss://example.test/api/v1/control#secret",
    ] {
        let error = WebSocketTransport::connect(url, Options::default(), 4).unwrap_err();
        assert!(error.0.contains("must not contain credentials"));
    }
}
