use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    http::StatusCode,
    response::Response,
    routing::get,
};
use k10s_backend::BackendKernel;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{config::ServerConfig, control::serve_socket};

#[derive(Debug, Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: Arc<Semaphore>,
    authenticated: Arc<Semaphore>,
}

/// Handle for an embeddable loopback server.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl ServerHandle {
    /// Bound loopback address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Gracefully stop and join the server.
    pub async fn shutdown(self) -> io::Result<()> {
        self.cancel.cancel();
        self.task.await.map_err(io::Error::other)?
    }
}

/// Spawn a test-friendly server on an ephemeral loopback port.
pub async fn spawn_loopback(
    config: ServerConfig,
    kernel: BackendKernel,
) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(run(listener, config, kernel, task_cancel));
    Ok(ServerHandle { addr, cancel, task })
}

/// Serve an existing listener until cancellation.
pub async fn run(
    listener: TcpListener,
    config: ServerConfig,
    kernel: BackendKernel,
    cancel: CancellationToken,
) -> io::Result<()> {
    let state = AppState {
        unauthenticated: Arc::new(Semaphore::new(config.max_unauthenticated_connections)),
        authenticated: Arc::new(Semaphore::new(config.max_authenticated_connections)),
        config: Arc::new(config),
        kernel: Arc::new(kernel),
    };
    let app = Router::new()
        .route(k10s_protocol::CONTROL_PATH, get(control_upgrade))
        .route(k10s_protocol::LOGS_PATH, get(not_implemented))
        .route(k10s_protocol::EXEC_PATH, get(not_implemented))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
}

async fn not_implemented() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

async fn control_upgrade(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let permit = state
        .unauthenticated
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let config = state.config.clone();
    let kernel = state.kernel.clone();
    let auth = state.authenticated.clone();
    Ok(ws
        .max_frame_size(config.max_frame_size)
        .max_message_size(config.max_message_size)
        .on_upgrade(move |socket| serve_socket(socket, config, kernel, permit, auth)))
}
