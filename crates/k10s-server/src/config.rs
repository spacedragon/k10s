use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use k10s_backend::BackendMode;

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
    /// Tokens above the comparison bound would defeat constant-time checks.
    OversizedToken,
}

impl std::fmt::Display for StandaloneConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TokenRequired => {
                formatter.write_str("an explicit access token is required for non-loopback bind")
            }
            Self::InvalidDistDirectory => formatter.write_str("invalid Trunk distribution path"),
            Self::OversizedToken => formatter.write_str(&format!(
                "the configured access token exceeds the {max}-byte security bound",
                max = crate::auth::MAX_ACCESS_TOKEN_BYTES
            )),
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
        if access_token.len() > crate::auth::MAX_ACCESS_TOKEN_BYTES {
            return Err(StandaloneConfigError::OversizedToken);
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

/// Resolve the runtime backend mode for a standalone launch from explicit
/// operator inputs, before any listener is bound.
///
/// Normal launches default to the real `Kube` adapter (with an explicit
/// kubeconfig path when given). `Fake` is never implicit: it requires the
/// explicit development flag. File validity itself is enforced by the backend
/// factory at kernel construction, which entry points must run before bind.
pub fn resolve_backend_mode(fake_requested: bool, kubeconfig_path: Option<&Path>) -> BackendMode {
    if fake_requested {
        return BackendMode::Fake;
    }
    BackendMode::Kube {
        kubeconfig: kubeconfig_path.map(Path::to_path_buf),
    }
}

/// Runtime limits for the control server.
#[derive(Clone)]
pub struct ServerConfig {
    /// Shared bearer secret accepted in the first `Hello` frame.
    pub access_token: String,
    /// Time to remain live but not ready while standalone bootstrap settles.
    pub startup_readiness_delay: Duration,
    /// Minimum externally observable draining interval for process probes.
    pub probe_drain_grace: Duration,
    /// Maximum time allowed for the first frame.
    pub hello_timeout: Duration,
    /// Maximum period for best-effort writer flushes and overload close handshakes.
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
    /// Maximum live resource-watch subscriptions per authenticated socket.
    pub max_resource_subscriptions_per_session: usize,
    /// Maximum normalized resource rows carried by one snapshot chunk.
    pub snapshot_rows_per_chunk: usize,
    /// Per-socket window after the shutdown notice during which status reads stay served.
    pub drain_grace_timeout: Duration,
    /// Hard deadline for draining tracked connection tasks.
    pub drain_timeout: Duration,
    /// Capabilities implemented by this server.
    pub capabilities: Vec<String>,
    /// Maximum individual WebSocket frame size on the dedicated logs/exec
    /// stream sockets. Deliberately separate from (and smaller than) the
    /// control-socket limit.
    pub max_stream_frame_size: usize,
    /// Maximum assembled WebSocket message size across fragmentation on the
    /// stream sockets, enforced before authentication or payload dispatch.
    pub max_stream_message_size: usize,
    /// Maximum time allowed for the mandatory first `hello` frame.
    pub stream_hello_timeout: Duration,
    /// Byte budget in each direction per stream socket per second; exceeding
    /// it closes the socket with an explicit overload error.
    pub stream_rate_budget_bytes_per_sec: usize,
    /// Maximum concurrent dedicated stream sockets.
    pub max_stream_connections: usize,
    /// Maximum journaled frames retained per control session for resume replay.
    pub resume_max_journal_entries: usize,
    /// Maximum retained control sessions, including disconnected resumable sessions.
    pub resume_max_sessions: usize,
    /// Maximum age of a journaled frame before it is no longer replayable.
    pub resume_entry_max_age: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            access_token: String::new(),
            startup_readiness_delay: Duration::ZERO,
            probe_drain_grace: Duration::ZERO,
            hello_timeout: Duration::from_secs(5),
            graceful_flush_timeout: Duration::from_millis(250),
            max_frame_size: 1 << 20,
            max_message_size: 4 << 20,
            max_unauthenticated_connections: 32,
            max_authenticated_connections: 128,
            outbound_queue_capacity: 64,
            max_resource_subscriptions_per_session: 64,
            snapshot_rows_per_chunk: 128,
            drain_grace_timeout: Duration::from_millis(250),
            drain_timeout: Duration::from_secs(10),
            capabilities: vec!["logs.tail".into(), "exec.attach".into()],
            max_stream_frame_size: 64 << 10,
            max_stream_message_size: 256 << 10,
            stream_hello_timeout: Duration::from_secs(5),
            stream_rate_budget_bytes_per_sec: 512 << 10,
            max_stream_connections: 64,
            resume_max_journal_entries: 1_024,
            resume_max_sessions: 256,
            resume_entry_max_age: Duration::from_secs(30),
        }
    }
}

/// Redacted debug view of [`ServerConfig`] so the shared secret never appears
/// in logs, panics, or test output.
impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("access_token", &"[REDACTED]")
            .field("startup_readiness_delay", &self.startup_readiness_delay)
            .field("probe_drain_grace", &self.probe_drain_grace)
            .field("hello_timeout", &self.hello_timeout)
            .field("graceful_flush_timeout", &self.graceful_flush_timeout)
            .field("max_frame_size", &self.max_frame_size)
            .field("max_message_size", &self.max_message_size)
            .field(
                "max_unauthenticated_connections",
                &self.max_unauthenticated_connections,
            )
            .field(
                "max_authenticated_connections",
                &self.max_authenticated_connections,
            )
            .field("outbound_queue_capacity", &self.outbound_queue_capacity)
            .field("snapshot_rows_per_chunk", &self.snapshot_rows_per_chunk)
            .field("drain_grace_timeout", &self.drain_grace_timeout)
            .field("drain_timeout", &self.drain_timeout)
            .field("capabilities", &self.capabilities)
            .field("max_stream_frame_size", &self.max_stream_frame_size)
            .field("max_stream_message_size", &self.max_stream_message_size)
            .field("stream_hello_timeout", &self.stream_hello_timeout)
            .field(
                "stream_rate_budget_bytes_per_sec",
                &self.stream_rate_budget_bytes_per_sec,
            )
            .field("max_stream_connections", &self.max_stream_connections)
            .field(
                "resume_max_journal_entries",
                &self.resume_max_journal_entries,
            )
            .field("resume_max_sessions", &self.resume_max_sessions)
            .field("resume_entry_max_age", &self.resume_entry_max_age)
            .finish()
    }
}

/// One invalid runtime resource budget rejected before a listener is bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetConfigError {
    field: &'static str,
    reason: &'static str,
}

impl BudgetConfigError {
    /// Name of the rejected configuration field.
    #[must_use]
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl std::fmt::Display for BudgetConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid {}: {}", self.field, self.reason)
    }
}

impl std::error::Error for BudgetConfigError {}

impl ServerConfig {
    /// Validate every independently configurable hard bound.
    ///
    /// Startup rejects invalid values instead of silently widening zero or
    /// internally contradictory budgets with `.max(1)` fallbacks.
    pub fn validate(&self) -> Result<(), BudgetConfigError> {
        macro_rules! nonzero {
            ($($field:ident),+ $(,)?) => {$({
                if self.$field == 0 {
                    return Err(BudgetConfigError { field: stringify!($field), reason: "must be greater than zero" });
                }
            })+};
        }
        nonzero!(
            max_frame_size,
            max_message_size,
            max_unauthenticated_connections,
            max_authenticated_connections,
            outbound_queue_capacity,
            max_resource_subscriptions_per_session,
            snapshot_rows_per_chunk,
            max_stream_frame_size,
            max_stream_message_size,
            stream_rate_budget_bytes_per_sec,
            max_stream_connections,
            resume_max_journal_entries,
            resume_max_sessions,
        );
        for (field, value) in [
            ("hello_timeout", self.hello_timeout),
            ("graceful_flush_timeout", self.graceful_flush_timeout),
            ("drain_grace_timeout", self.drain_grace_timeout),
            ("drain_timeout", self.drain_timeout),
            ("stream_hello_timeout", self.stream_hello_timeout),
            ("resume_entry_max_age", self.resume_entry_max_age),
        ] {
            if value.is_zero() {
                return Err(BudgetConfigError {
                    field,
                    reason: "must be greater than zero",
                });
            }
        }
        let Some(graceful_shutdown_budget) = self
            .drain_grace_timeout
            .checked_add(self.graceful_flush_timeout)
        else {
            return Err(BudgetConfigError {
                field: "drain_grace_timeout",
                reason: "plus graceful_flush_timeout must not overflow",
            });
        };
        if graceful_shutdown_budget > self.drain_timeout {
            return Err(BudgetConfigError {
                field: "drain_grace_timeout",
                reason: "plus graceful_flush_timeout must not exceed drain_timeout",
            });
        }
        if self.probe_drain_grace > self.drain_timeout {
            return Err(BudgetConfigError {
                field: "probe_drain_grace",
                reason: "must not exceed drain_timeout",
            });
        }
        Ok(())
    }
}

/// Safe rejection of an invalid or unreadable access-token source.
#[derive(Debug, Clone)]
pub enum AccessTokenSourceError {
    /// The configured token file cannot be read.
    UnreadableFile(PathBuf),
    /// The configured token is empty after trimming surrounding whitespace.
    EmptyToken,
    /// The resolved token exceeds the comparison bound enforced at startup.
    OversizedToken,
}

impl std::fmt::Display for AccessTokenSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnreadableFile(path) => write!(
                formatter,
                "cannot read the access token file at {}",
                path.display()
            ),
            Self::EmptyToken => formatter.write_str("the configured access token is empty"),
            Self::OversizedToken => {
                // Naming the bound lets operators fix the secret without any
                // of its content appearing in diagnostics.
                write!(
                    formatter,
                    "the configured access token exceeds the {}-byte security bound",
                    crate::auth::MAX_ACCESS_TOKEN_BYTES
                )
            }
        }
    }
}

impl std::error::Error for AccessTokenSourceError {}

/// Resolve the access token from validated sources.
///
/// Documented precedence: an explicitly configured token file always wins over
/// the environment value — a file is the recommended secret mechanism, and an
/// operator configuring both has most likely rotated to the file while leaving
/// a stale env value behind. With no source configured the result is `None`,
/// which only loopback-only configurations may run (see
/// [`StandaloneConfig::new`]). File contents are trimmed of surrounding
/// whitespace so trailing newlines in generated secret files never leak into
/// the compared credential.
pub fn resolve_access_token(
    env_token: Option<&str>,
    token_file_path: Option<&Path>,
) -> Result<Option<String>, AccessTokenSourceError> {
    let resolved = if let Some(path) = token_file_path {
        let content = std::fs::read_to_string(path)
            .map_err(|_| AccessTokenSourceError::UnreadableFile(path.to_path_buf()))?;
        let trimmed = content.trim();
        if trimmed.is_empty() {
            return Err(AccessTokenSourceError::EmptyToken);
        }
        Some(trimmed.to_owned())
    } else {
        env_token
            .filter(|token| !token.is_empty())
            .map(str::to_owned)
    };
    let Some(token) = resolved else {
        return Ok(None);
    };
    if token.len() > crate::auth::MAX_ACCESS_TOKEN_BYTES {
        // Refuse startup instead of weakening the fixed-iteration comparison.
        return Err(AccessTokenSourceError::OversizedToken);
    }
    Ok(Some(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::MAX_ACCESS_TOKEN_BYTES;

    #[test]
    fn server_config_debug_is_redacted() {
        let config = ServerConfig {
            access_token: "unit-probe-secret".into(),
            ..ServerConfig::default()
        };
        assert!(!format!("{config:?}").contains("unit-probe-secret"));
    }

    #[test]
    fn empty_env_value_counts_as_absent() {
        assert_eq!(resolve_access_token(Some(""), None).unwrap(), None);
        assert_eq!(resolve_access_token(None, None).unwrap(), None);
    }

    #[test]
    fn oversized_tokens_are_refused_before_startup() {
        let too_long = "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1);
        // Oversized env token refuses startup.
        assert!(matches!(
            resolve_access_token(Some(&too_long), None),
            Err(AccessTokenSourceError::OversizedToken)
        ));
        // Oversized file token is refused the same way.
        let dir = std::env::temp_dir().join(format!("k10s-config-test-{}", std::process::id()));
        let path = dir.join("oversized-token.txt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, &too_long).unwrap();
        assert!(matches!(
            resolve_access_token(None, Some(&path)),
            Err(AccessTokenSourceError::OversizedToken)
        ));
    }

    #[test]
    fn token_at_the_bound_is_accepted() {
        let exact = "x".repeat(MAX_ACCESS_TOKEN_BYTES);
        assert_eq!(
            resolve_access_token(Some(&exact), None).unwrap().as_deref(),
            Some(exact.as_str())
        );
    }

    #[test]
    fn standalone_config_refuses_oversized_tokens() {
        let too_long = "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1);
        assert!(matches!(
            StandaloneConfig::new(
                "127.0.0.1:8080".parse().unwrap(),
                Some(too_long),
                PathBuf::from("dist")
            ),
            Err(StandaloneConfigError::OversizedToken)
        ));
    }
}
