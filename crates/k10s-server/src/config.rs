use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Validated standalone listener, secret, and exact Trunk distribution source.
#[derive(Clone, PartialEq, Eq)]
pub struct StandaloneConfig {
    bind_addr: SocketAddr,
    access_token: String,
    dist_dir: PathBuf,
}

impl std::fmt::Debug for StandaloneConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StandaloneConfig")
            .field("bind_addr", &self.bind_addr)
            .field("access_token", &"[REDACTED]")
            .field("dist_dir", &self.dist_dir)
            .finish()
    }
}

/// Safe standalone configuration rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandaloneConfigError {
    /// Public listeners require an explicitly supplied token.
    TokenRequired,
    /// Asset directories are filesystem paths, not credential-bearing URLs.
    InvalidDistDirectory,
}

impl std::fmt::Display for StandaloneConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TokenRequired => {
                formatter.write_str("an explicit access token is required for non-loopback bind")
            }
            Self::InvalidDistDirectory => formatter.write_str("invalid Trunk distribution path"),
        }
    }
}

impl std::error::Error for StandaloneConfigError {}

impl StandaloneConfig {
    /// Validate security-sensitive configuration before the listener is bound.
    pub fn new(
        bind_addr: SocketAddr,
        access_token: Option<String>,
        dist_dir: PathBuf,
    ) -> Result<Self, StandaloneConfigError> {
        let access_token = access_token.unwrap_or_default();
        if !bind_addr.ip().is_loopback() && access_token.is_empty() {
            return Err(StandaloneConfigError::TokenRequired);
        }
        let path = dist_dir.to_string_lossy();
        if path.is_empty() || path.contains(['?', '#']) {
            return Err(StandaloneConfigError::InvalidDistDirectory);
        }
        Ok(Self {
            bind_addr,
            access_token,
            dist_dir,
        })
    }

    /// Validated listener address.
    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Explicit first-frame secret, possibly empty for loopback-only development.
    #[must_use]
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Exact Trunk output tree served by the standalone runtime.
    #[must_use]
    pub fn dist_dir(&self) -> &Path {
        &self.dist_dir
    }
}

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
