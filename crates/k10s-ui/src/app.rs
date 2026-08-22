//! Minimal application state driven exclusively through the shared protocol client.

use ewebsock::{Options, WsEvent, WsMessage};
use k10s_protocol::ServerFrame;

use crate::client::{
    BoundedInbox, ClientConfig, ClientPhase, ClientState, ConnectTarget, PendingRequest, Query,
    QueryResult, TransportError, WebSocketTransport,
};

/// User-visible foundation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppView {
    /// The control connection is opening, authenticating, or bootstrapping.
    Connecting,
    /// Bootstrap data received over the authenticated control WebSocket.
    Ready {
        /// Identity of the embedded server instance.
        server_instance_id: String,
        /// Safe Kubernetes context names.
        context_names: Vec<String>,
    },
    /// A safe connection or protocol failure.
    Failed {
        /// Credential-free error suitable for display.
        message: String,
    },
}

/// Minimal shared k10s application.
pub struct K10sApp {
    connection_url: String,
    client: ClientState,
    transport: WebSocketTransport,
    inbox: BoundedInbox,
    bootstrap: Option<PendingRequest>,
    view: AppView,
}

impl std::fmt::Debug for K10sApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("K10sApp")
            .field("connection_url", &self.connection_url)
            .field("client", &self.client)
            .field("transport", &self.transport)
            .field("inbox", &self.inbox)
            .field("bootstrap", &self.bootstrap)
            .field("view", &self.view)
            .finish()
    }
}

impl K10sApp {
    /// Connect through the Task 5 transport and queue the protocol `Hello`.
    pub fn connect(target: ConnectTarget) -> Result<Self, TransportError> {
        let connection_url = target.url().to_owned();
        let mut client = ClientState::new(ClientConfig::default());
        client
            .connect(target)
            .map_err(|error| TransportError(error.to_string()))?;
        let (transport, inbox) =
            WebSocketTransport::connect(&connection_url, Options::default(), 64)?;
        Ok(Self {
            connection_url,
            client,
            transport,
            inbox,
            bootstrap: None,
            view: AppView::Connecting,
        })
    }

    /// Process all currently available transport events without blocking the UI thread.
    pub fn poll(&mut self) {
        while let Some(event) = self.inbox.try_recv() {
            if let Err(message) = self.handle_event(event) {
                self.view = AppView::Failed { message };
                self.transport.close();
                break;
            }
        }
    }

    /// Current user-visible state.
    #[must_use]
    pub fn view(&self) -> &AppView {
        &self.view
    }

    /// Credential-free endpoint used by the shared transport.
    #[must_use]
    pub fn connection_url(&self) -> &str {
        &self.connection_url
    }

    /// Render the minimal foundation view as text.
    #[must_use]
    pub fn render_text(&self) -> String {
        match &self.view {
            AppView::Connecting => "Connecting".to_owned(),
            AppView::Ready {
                server_instance_id,
                context_names,
            } => format!(
                "Server {server_instance_id}\nContexts: {}",
                context_names.join(", ")
            ),
            AppView::Failed { message } => format!("Connection failed: {message}"),
        }
    }

    fn handle_event(&mut self, event: WsEvent) -> Result<(), String> {
        match event {
            WsEvent::Opened => self.flush_outbound(),
            WsEvent::Message(WsMessage::Text(text)) => {
                let frame: ServerFrame = serde_json::from_str(&text)
                    .map_err(|error| format!("could not decode server frame: {error}"))?;
                self.client
                    .apply(frame)
                    .map_err(|error| error.to_string())?;
                if self.client.phase() == ClientPhase::Ready && self.bootstrap.is_none() {
                    self.bootstrap = Some(
                        self.client
                            .begin(Query::Bootstrap)
                            .map_err(|error| error.to_string())?,
                    );
                }
                if let Some(request) = self.bootstrap.clone()
                    && let Some(QueryResult::Bootstrap(response)) = self.client.take(request)
                {
                    let server_instance_id = response
                        .server
                        .ok_or_else(|| "bootstrap omitted server identity".to_owned())?
                        .instance_id;
                    let context_names = response
                        .contexts
                        .into_iter()
                        .map(|context| context.name)
                        .collect();
                    self.view = AppView::Ready {
                        server_instance_id,
                        context_names,
                    };
                }
                self.flush_outbound()
            }
            WsEvent::Message(_) => Ok(()),
            WsEvent::Error(message) => Err(message),
            WsEvent::Closed => Err("control connection closed".to_owned()),
        }
    }

    fn flush_outbound(&mut self) -> Result<(), String> {
        while let Some(frame) = self.client.take_outbound() {
            self.transport
                .send_frame(&frame)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

impl Drop for K10sApp {
    fn drop(&mut self) {
        self.client.application_close();
        self.transport.close();
    }
}
