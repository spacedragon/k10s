//! Dedicated `/api/v1/logs` log-tail stream socket route.

use axum::extract::{State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::Response;

use crate::streams::StreamRoute;

/// Upgrade handler for the dedicated logs socket.
pub(crate) async fn upgrade(
    state: State<crate::lifecycle::AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, axum::http::StatusCode> {
    crate::lifecycle::stream_upgrade(state, headers, ws, StreamRoute::Logs).await
}
