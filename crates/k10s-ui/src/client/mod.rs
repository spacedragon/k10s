//! Shared protocol-client state and bounded WebSocket transport.

mod state;
mod streams;
mod transport;

pub use state::{
    ClientConfig, ClientError, ClientPhase, ClientState, Command, ConnectTarget, LiveSubscription,
    LocalUiState, PendingRequest, Query, QueryResult, ResourceListQuery, ResourceListState,
    ResourceSnapshot, RetrySchedule,
};
pub use streams::{
    StreamIo, StreamRoute, StreamSession, StreamSignal, StreamSocket, derive_stream_url,
};
pub use transport::{
    BoundedEventCallback, BoundedInbox, TransportError, WebSocketTransport, bounded_event_callback,
};
