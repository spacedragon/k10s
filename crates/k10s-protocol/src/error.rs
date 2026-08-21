//! Error contract for the k10s control protocol.

use serde::{Deserialize, Serialize};

/// Stable error codes shared across the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorCode {
    /// The server does not support this message kind.
    UnsupportedMessage,
    /// The request payload is malformed or invalid.
    InvalidRequest,
    /// Authentication or authorization failed.
    Unauthorized,
    /// The requested resource was not found.
    NotFound,
    /// The operation is already in progress.
    Conflict,
    /// The server encountered an internal error.
    Internal,
    /// The request timed out.
    Timeout,
    /// The operation was cancelled.
    Cancelled,
}

/// Retry strategy for a failed operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Retryability {
    /// Never retry; the operation cannot be retried.
    Never,
    /// Retry after reconnecting to the server.
    AfterReconnect,
    /// Retry after refreshing the resource.
    AfterRefresh,
    /// Requires user action to proceed.
    UserAction,
}

/// Scope of the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorScope {
    /// The error is specific to a request.
    Request,
    /// The error is specific to a session.
    Session,
    /// The error is specific to a subscription.
    Subscription,
}

/// A protocol error frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorFrame {
    /// The stable error code.
    pub code: ErrorCode,
    /// A safe, user-facing error message.
    pub safe_message: String,
    /// The retry strategy for this error.
    pub retryability: Retryability,
    /// The scope of the error.
    pub scope: ErrorScope,
    /// The correlation ID linking this error to a request.
    pub correlation_id: String,
    /// Optional additional details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ErrorFrame {
    /// Create a new error frame.
    #[must_use]
    pub fn new(
        code: ErrorCode,
        safe_message: impl Into<String>,
        retryability: Retryability,
        scope: ErrorScope,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            code,
            safe_message: safe_message.into(),
            retryability,
            scope,
            correlation_id: correlation_id.into(),
            details: None,
        }
    }

    /// Set additional details on the error frame.
    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}
