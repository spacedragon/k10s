//! Web connection-gate state and same-origin endpoint derivation.

use k10s_protocol::CONTROL_PATH;
use serde::Serialize;

use crate::client::ConnectTarget;

/// Safe, serializable web preferences. Authentication material is deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistedSettings {
    control_url: String,
}

/// Ephemeral web authentication form state.
pub struct ConnectionGate {
    settings: PersistedSettings,
    token_input: String,
    error: Option<String>,
    visible: bool,
}

impl std::fmt::Debug for ConnectionGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionGate")
            .field("settings", &self.settings)
            .field("token_input", &"[REDACTED]")
            .field("error", &self.error)
            .field("visible", &self.visible)
            .finish()
    }
}

impl ConnectionGate {
    /// Create the initially visible gate for one credential-free endpoint.
    #[must_use]
    pub fn new(control_url: impl Into<String>) -> Self {
        Self {
            settings: PersistedSettings {
                control_url: control_url.into(),
            },
            token_input: String::new(),
            error: None,
            visible: true,
        }
    }

    /// Whether the authentication form should be shown.
    #[must_use]
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Current ephemeral input buffer.
    #[must_use]
    pub fn token_input(&self) -> &str {
        &self.token_input
    }

    /// Replace the ephemeral input buffer.
    pub fn set_token_input(&mut self, token: impl Into<String>) {
        self.token_input = token.into();
        self.error = None;
    }

    /// Current credential-free validation or authentication message.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Move the token directly into a protocol connection target and clear the form buffer.
    ///
    /// An empty buffer is submitted as-is: standalone servers started without a token
    /// accept unauthenticated control connections for loopback development, so the
    /// default gate must stay usable there.
    pub fn begin_connection(&mut self) -> ConnectTarget {
        let token = std::mem::take(&mut self.token_input);
        self.visible = false;
        self.error = None;
        ConnectTarget::new(self.settings.control_url.clone(), token)
    }

    /// Return to a blank gate after the server rejects authentication.
    pub fn authentication_rejected(&mut self) {
        self.token_input.clear();
        self.visible = true;
        self.error = Some("Authentication failed. Try again.".to_owned());
    }

    /// Hide the gate and discard any remaining editable credential bytes.
    pub fn authentication_succeeded(&mut self) {
        self.token_input.clear();
        self.visible = false;
        self.error = None;
    }

    /// Credential-free settings safe for persistence.
    #[must_use]
    pub fn persisted_settings(&self) -> &PersistedSettings {
        &self.settings
    }
}

/// Build the root-level control WebSocket URL from `window.location` fields only.
pub fn derive_control_url(scheme: &str, authority: &str) -> Result<String, &'static str> {
    if authority.is_empty() || authority.contains(['/', '?', '#', '@']) {
        return Err("window location has no valid authority");
    }
    let socket_scheme = match scheme {
        "http:" => "ws",
        "https:" => "wss",
        _ => return Err("window location must use HTTP or HTTPS"),
    };
    Ok(format!("{socket_scheme}://{authority}{CONTROL_PATH}"))
}
