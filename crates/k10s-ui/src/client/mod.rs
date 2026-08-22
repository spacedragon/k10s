//! Shared protocol-client state and bounded WebSocket transport.

mod state;
mod transport;

pub use state::{
    ClientConfig, ClientError, ClientPhase, ClientState, ConnectTarget, LiveSubscription,
    LocalUiState, PendingRequest, Query, QueryResult, ResourceListQuery, ResourceSnapshot,
    RetrySchedule,
};
pub use transport::{
    BoundedEventCallback, BoundedInbox, TransportError, WebSocketTransport, bounded_event_callback,
};
