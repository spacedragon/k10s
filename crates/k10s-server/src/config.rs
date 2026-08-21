use std::time::Duration;

/// Runtime limits for the control server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Shared bearer secret accepted in the first `Hello` frame.
    pub access_token: String,
    /// Maximum time allowed for the first frame.
    pub hello_timeout: Duration,
    /// Maximum best-effort writer flush period before cancellation.
    pub graceful_flush_timeout: Duration,
    /// Maximum WebSocket frame size.
    pub max_frame_size: usize,
    /// Maximum assembled WebSocket message size.
    pub max_message_size: usize,
    /// Maximum sockets awaiting authentication.
    pub max_unauthenticated_connections: usize,
    /// Maximum authenticated sockets.
    pub max_authenticated_connections: usize,
    /// Bounded outbound queue capacity per socket.
    pub outbound_queue_capacity: usize,
    /// Capabilities implemented by this server.
    pub capabilities: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            access_token: String::new(),
            hello_timeout: Duration::from_secs(5),
            graceful_flush_timeout: Duration::from_millis(250),
            max_frame_size: 1 << 20,
            max_message_size: 4 << 20,
            max_unauthenticated_connections: 32,
            max_authenticated_connections: 128,
            outbound_queue_capacity: 64,
            capabilities: vec!["logs.tail".into(), "exec.attach".into()],
        }
    }
}
