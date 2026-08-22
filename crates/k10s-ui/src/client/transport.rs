//! Private target-selected ewebsock transport adapter.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TrySendError, sync_channel};

use ewebsock::{Options, WsEvent, WsMessage, WsSender};
use k10s_protocol::ClientFrame;

/// The exact callback shape passed to low-level `ewebsock::ws_connect`.
pub type BoundedEventCallback = Box<dyn Send + Fn(WsEvent) -> ControlFlow<()>>;

/// The UI-owned receiving end of the one and only inbound event queue.
pub struct BoundedInbox {
    receiver: Receiver<WsEvent>,
    overflowed: Arc<AtomicBool>,
}

impl std::fmt::Debug for BoundedInbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedInbox")
            .finish_non_exhaustive()
    }
}

impl BoundedInbox {
    /// Try to remove one transport event without blocking the UI.
    pub fn try_recv(&self) -> Option<WsEvent> {
        self.receiver.try_recv().ok()
    }

    /// Whether the callback reached capacity and closed the transport.
    #[must_use]
    pub fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }
}

/// Build the bounded callback contract used identically on native and WASM.
///
/// The callback writes directly to a `sync_channel`; there is no intermediate
/// receiver queue. A full inbox immediately returns `Break`,
/// which instructs ewebsock to close the connection.
#[must_use]
pub fn bounded_event_callback(capacity: usize) -> (BoundedInbox, BoundedEventCallback) {
    let (sender, receiver) = sync_channel(capacity);
    let overflowed = Arc::new(AtomicBool::new(false));
    let callback_overflowed = Arc::clone(&overflowed);
    let callback = Box::new(move |event| match sender.try_send(event) {
        Ok(()) => ControlFlow::Continue(()),
        Err(TrySendError::Full(_)) => {
            callback_overflowed.store(true, Ordering::Release);
            ControlFlow::Break(())
        }
        Err(TrySendError::Disconnected(_)) => ControlFlow::Break(()),
    });
    (
        BoundedInbox {
            receiver,
            overflowed,
        },
        callback,
    )
}

/// A low-level ewebsock connection using the bounded callback contract.
pub struct WebSocketTransport {
    sender: WsSender,
}

impl std::fmt::Debug for WebSocketTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSocketTransport")
            .finish_non_exhaustive()
    }
}

/// A safe transport setup or encoding failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TransportError {}

impl WebSocketTransport {
    /// Connect to a credential-free control endpoint.
    ///
    /// Authentication belongs only in the serialized `Hello` passed to
    /// [`Self::send_frame`]. Query strings, fragments, and URL userinfo are
    /// rejected so credentials cannot accidentally become request metadata.
    pub fn connect(
        url: &str,
        options: Options,
        inbox_capacity: usize,
    ) -> Result<(Self, BoundedInbox), TransportError> {
        validate_credential_free_url(url)?;
        let (inbox, callback) = bounded_event_callback(inbox_capacity);
        let sender =
            ewebsock::ws_connect(url.to_owned(), options, callback).map_err(TransportError)?;
        Ok((Self { sender }, inbox))
    }

    /// Send one protocol frame as JSON on either native or WASM.
    pub fn send_frame(&mut self, frame: &ClientFrame) -> Result<(), TransportError> {
        let json = serde_json::to_string(frame)
            .map_err(|error| TransportError(format!("could not encode client frame: {error}")))?;
        self.sender.send(WsMessage::Text(json));
        Ok(())
    }

    /// Explicitly close the underlying WebSocket.
    pub fn close(&mut self) {
        self.sender.close();
    }
}

fn validate_credential_free_url(url: &str) -> Result<(), TransportError> {
    let authority_and_path = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .ok_or_else(|| TransportError("WebSocket URL must use ws:// or wss://".to_owned()))?;
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    if authority.is_empty()
        || authority.contains('@')
        || authority_and_path.contains('?')
        || authority_and_path.contains('#')
    {
        return Err(TransportError(
            "WebSocket URL must not contain credentials, query, or fragment".to_owned(),
        ));
    }
    Ok(())
}
