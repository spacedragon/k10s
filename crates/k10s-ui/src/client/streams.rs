//! Dedicated stream-socket sessions for the connected log viewer and
//! terminal tools.
//!
//! A [`StreamSession`] owns one dedicated WebSocket on `LOGS_PATH` or
//! `EXEC_PATH`. The access token and single-use ticket travel only inside
//! the first `hello` frame — never in any URL. Inbound frames are projected
//! into [`StreamSignal`]s that the application layer feeds into its
//! per-window tools; outbound stdin/resize leave as versioned binary
//! payloads.

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_protocol::{
    EXEC_PATH, LOGS_PATH, PROTOCOL_MAJOR, StreamClientMessage, StreamServerMessage, StreamTarget,
    decode_stream_payload,
};

use crate::client::transport::{BoundedInbox, TransportError, WebSocketTransport};

/// Which dedicated route a session attaches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamRoute {
    /// `/api/v1/logs`.
    Logs,
    /// `/api/v1/exec`.
    Exec,
}

impl StreamRoute {
    /// Dedicated path served by this route.
    #[must_use]
    pub fn path(self) -> &'static str {
        match self {
            Self::Logs => LOGS_PATH,
            Self::Exec => EXEC_PATH,
        }
    }
}

/// Derive a credential-free stream endpoint from a credential-free control
/// URL by replacing the trailing control path.
pub fn derive_stream_url(control_url: &str, route: StreamRoute) -> Result<String, TransportError> {
    let base = control_url
        .strip_suffix(k10s_protocol::CONTROL_PATH)
        .ok_or_else(|| {
            TransportError("control URL must end in the protocol control path".to_owned())
        })?;
    Ok(format!("{}{}", base, route.path()))
}

/// One live dedicated-stream connection.
pub struct StreamSocket {
    transport: WebSocketTransport,
    inbox: BoundedInbox,
}

impl std::fmt::Debug for StreamSocket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamSocket")
            .finish_non_exhaustive()
    }
}

/// The transport seam behind one stream session, so the glue can be driven
/// by tests without a network.
pub trait StreamIo {
    /// Try to remove one transport event without blocking.
    fn try_recv(&mut self) -> Option<WsEvent>;
    /// Send one raw text message.
    fn send_text(&mut self, text: String);
    /// Send one raw binary message (already framed by the caller).
    fn send_binary(&mut self, bytes: Vec<u8>);
}

impl StreamIo for StreamSocket {
    fn try_recv(&mut self) -> Option<WsEvent> {
        self.inbox.try_recv()
    }

    fn send_text(&mut self, text: String) {
        self.transport.send_text(text);
    }

    fn send_binary(&mut self, bytes: Vec<u8>) {
        self.transport.send_binary(bytes);
    }
}

impl StreamSocket {
    /// Connect to `url` (credential-free; authentication is in-frame).
    pub fn connect(url: &str) -> Result<Self, TransportError> {
        let (transport, inbox) = WebSocketTransport::connect(url, Options::default(), 64)?;
        Ok(Self { transport, inbox })
    }

    /// Send the mandatory first `hello` carrying token and ticket.
    pub fn send_hello(
        &mut self,
        access_token: &str,
        stream_ticket: &str,
    ) -> Result<(), TransportError> {
        let hello = StreamClientMessage::Hello {
            protocol_major: PROTOCOL_MAJOR,
            access_token: access_token.to_owned(),
            stream_ticket: stream_ticket.to_owned(),
        };
        let json = serde_json::to_string(&hello)
            .map_err(|error| TransportError(format!("could not encode hello: {error}")))?;
        self.transport.send_text(json);
        Ok(())
    }

    /// Send one versioned binary payload frame.
    pub fn send_payload(&mut self, kind: u8, data: &[u8]) {
        self.transport
            .send_binary(k10s_protocol::encode_stream_payload(kind, data));
    }

    /// Try to remove one transport event without blocking.
    pub fn try_recv(&mut self) -> Option<WsEvent> {
        self.inbox.try_recv()
    }

    /// Whether the bounded inbound queue overflowed and closed transport.
    #[must_use]
    pub fn overflowed(&self) -> bool {
        self.inbox.overflowed()
    }

    /// Close the underlying socket.
    pub fn close(&mut self) {
        self.transport.close();
    }
}

/// One projected event from a dedicated stream session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamSignal {
    /// Ticket redeemed; the bound identity was echoed back.
    Ready {
        /// Bound stream type.
        stream_type: k10s_protocol::StreamType,
        /// Bound exec mode.
        tty: bool,
        /// Selected container.
        container: String,
    },
    /// One decoded output chunk (logs data, TTY merged output, stdout).
    Output(String),
    /// One informational status message (e.g. resize acknowledgement).
    Status(String),
    /// The exec session ended with this exit code.
    Exited(i32),
    /// The server rejected the hello/ticket; the session is over.
    Rejected(String),
}

/// Lifecycle of one dedicated stream session owned by the application.
pub struct StreamSession {
    route: StreamRoute,
    target: StreamTarget,
    tty: bool,
    socket: Option<Box<dyn StreamIo>>,
}

impl std::fmt::Debug for StreamSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamSession")
            .field("route", &self.route)
            .field("target", &self.target)
            .field("tty", &self.tty)
            .field("socket_live", &self.socket.is_some())
            .finish()
    }
}

impl StreamSession {
    /// Create a session whose socket still has to be opened with the
    /// granted ticket.
    pub fn new(route: StreamRoute, target: StreamTarget, tty: bool) -> Self {
        Self {
            route,
            target,
            tty,
            socket: None,
        }
    }

    /// Bound route of this session.
    #[must_use]
    pub fn route(&self) -> StreamRoute {
        self.route
    }

    /// Open the dedicated socket against `control_url`'s origin and send
    /// the authenticated `hello` with the granted single-use ticket. The
    /// token stays inside the frame — it never touches any URL.
    pub fn open_with_ticket(
        &mut self,
        control_url: &str,
        access_token: &str,
        ticket_id: &str,
    ) -> Result<(), TransportError> {
        if self.socket.is_some() {
            return Ok(());
        }
        let url = derive_stream_url(control_url, self.route)?;
        let mut socket = StreamSocket::connect(&url)?;
        socket.send_hello(access_token, ticket_id)?;
        self.socket = Some(Box::new(socket));
        Ok(())
    }

    /// Test seam: replace the transport with a scripted implementation.
    pub fn inject_for_test(&mut self, socket: impl StreamIo + 'static) {
        self.socket = Some(Box::new(socket));
    }

    /// Whether stdin/resize can be sent right now.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.socket.is_some()
    }

    /// Queue one line of TTY standard input.
    pub fn send_stdin(&mut self, line: &str) {
        if let Some(socket) = self.socket.as_mut() {
            socket.send_binary(k10s_protocol::encode_stream_payload(
                k10s_protocol::payload_kind::STDIN,
                format!("{line}\n").as_bytes(),
            ));
        }
    }

    /// Queue a terminal resize.
    pub fn send_resize(&mut self, cols: u32, rows: u32) {
        if let Some(socket) = self.socket.as_mut() {
            socket.send_binary(k10s_protocol::encode_stream_payload(
                k10s_protocol::payload_kind::RESIZE,
                &k10s_protocol::encode_resize_payload(cols, rows),
            ));
        }
    }

    /// Drain every available transport event into signals.
    pub fn poll(&mut self) -> Vec<StreamSignal> {
        let Some(socket) = self.socket.as_mut() else {
            return Vec::new();
        };
        let mut signals = Vec::new();
        while let Some(event) = StreamIo::try_recv(socket.as_mut()) {
            match event {
                WsEvent::Opened => {}
                WsEvent::Message(WsMessage::Text(text)) => {
                    match serde_json::from_str::<StreamServerMessage>(&text) {
                        Ok(StreamServerMessage::Ready {
                            stream_type,
                            tty,
                            container,
                        }) => signals.push(StreamSignal::Ready {
                            stream_type,
                            tty,
                            container,
                        }),
                        Ok(StreamServerMessage::Status { message }) => {
                            signals.push(StreamSignal::Status(message));
                        }
                        Ok(StreamServerMessage::Error { message, .. }) => {
                            signals.push(StreamSignal::Rejected(message));
                        }
                        Ok(StreamServerMessage::Exit { exit_code }) => {
                            signals.push(StreamSignal::Exited(exit_code));
                        }
                        Err(_) => signals.push(StreamSignal::Rejected(
                            "undecodable stream status frame".to_owned(),
                        )),
                    }
                }
                WsEvent::Message(WsMessage::Binary(frame)) => {
                    if let Ok(payload) = decode_stream_payload(&frame)
                        && let Ok(text) = std::str::from_utf8(payload.data)
                    {
                        // TTY mode merges every origin; non-TTY keeps
                        // stdout/stderr apart for the tool layer.
                        signals.push(StreamSignal::Output(text.to_owned()));
                    }
                }
                WsEvent::Error(error) => {
                    signals.push(StreamSignal::Rejected(format!("transport error: {error}")));
                }
                WsEvent::Closed => signals.push(StreamSignal::Rejected(
                    "the stream connection closed".to_owned(),
                )),
                _ => {}
            }
        }
        // Overflow observability stays on the concrete socket; scripted
        // transports simply never report it.
        signals.retain(|signal| !matches!(signal, StreamSignal::Output(chunk) if chunk.is_empty()));
        signals
    }

    /// Close the socket; the backend retires the session on next touch.
    pub fn disconnect(&mut self) {
        self.socket = None;
    }

    /// Echoed target of this session.
    #[must_use]
    pub fn target(&self) -> &StreamTarget {
        &self.target
    }

    /// Bound exec mode.
    #[must_use]
    pub fn tty(&self) -> bool {
        self.tty
    }
}
