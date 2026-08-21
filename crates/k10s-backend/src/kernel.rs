//! Backend kernel: the sole protocol-facing interface.
//!
//! Owns all Kubernetes-facing product behavior. Maps to normalized protocol
//! payloads and enforces deadlines/cancellation. Fake data never escapes as
//! fixture types.

use std::sync::Arc;
use std::time::Duration;

use k10s_protocol::{BootstrapResponse, ProtocolVersion, ServerInfo};
use serde::Serialize;

use crate::port::{
    BackendError, BootstrapInfo, Command, ContextInfo, KubernetesAccess, OperationId, Query,
    QueryResult, Subscribe, SubscriptionHandle,
};

/// The backend kernel.
///
/// The sole protocol-facing interface. Owns mapping to normalized protocol
/// payloads and enforces deadlines/cancellation.
#[derive(Debug)]
pub struct BackendKernel {
    adapter: Arc<dyn KubernetesAccess>,
    server_instance_id: String,
}

impl BackendKernel {
    /// Create a new backend kernel with the given adapter.
    #[must_use]
    pub fn new(adapter: impl KubernetesAccess + 'static) -> Self {
        Self {
            adapter: Arc::new(adapter),
            server_instance_id: "instance-1".into(),
        }
    }

    /// Return the server instance ID.
    #[must_use]
    pub fn server_instance_id(&self) -> &str {
        &self.server_instance_id
    }

    /// Execute a behavior-level query.
    ///
    /// Returns a protocol-facing result with normalized payloads.
    pub async fn query(&self, req: Query) -> Result<KernelQueryResult, BackendError> {
        self.query_with_deadline(req, None).await
    }

    /// Execute a behavior-level query with an optional deadline.
    ///
    /// If the deadline elapses before the adapter responds, the query is
    /// cancelled and a [`BackendError::Timeout`] is returned.
    pub async fn query_with_deadline(
        &self,
        req: Query,
        deadline: Option<Duration>,
    ) -> Result<KernelQueryResult, BackendError> {
        let fut = self.adapter.query(req);
        let result = match deadline {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| BackendError::Timeout)?,
            None => fut.await,
        };
        Ok(match result? {
            QueryResult::Bootstrap(info) => KernelQueryResult::Bootstrap(BootstrapResult::new(
                info,
                self.server_instance_id.clone(),
            )),
        })
    }

    /// Execute a behavior-level command (mutation).
    ///
    /// Always returns an `OperationId` when supported.
    pub async fn execute(&self, cmd: Command) -> Result<OperationId, BackendError> {
        self.execute_with_deadline(cmd, None).await
    }

    /// Execute a behavior-level command with an optional deadline.
    ///
    /// If the deadline elapses before the adapter responds, the command is
    /// cancelled and a [`BackendError::Timeout`] is returned.
    pub async fn execute_with_deadline(
        &self,
        cmd: Command,
        deadline: Option<Duration>,
    ) -> Result<OperationId, BackendError> {
        let fut = self.adapter.execute(cmd);
        match deadline {
            Some(d) => tokio::time::timeout(d, fut)
                .await
                .map_err(|_| BackendError::Timeout)?,
            None => fut.await,
        }
    }

    /// Open a behavior-level subscription.
    ///
    /// Subscriptions are long-lived; deadlines do not apply.
    pub async fn subscribe(&self, req: Subscribe) -> Result<SubscriptionHandle, BackendError> {
        self.adapter.subscribe(req).await
    }
}

/// Result of a kernel query.
#[derive(Debug, Clone)]
pub enum KernelQueryResult {
    /// Bootstrap result with contexts and server metadata.
    Bootstrap(BootstrapResult),
}

impl KernelQueryResult {
    /// Return the context names for bootstrap results.
    #[must_use]
    pub fn context_names(&self) -> Vec<&str> {
        match self {
            Self::Bootstrap(b) => b.context_names(),
        }
    }

    /// Serialize the result to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        match self {
            Self::Bootstrap(b) => b.serialized(),
        }
    }
}

/// Bootstrap result with protocol metadata and context information.
#[derive(Debug, Clone, Serialize)]
pub struct BootstrapResult {
    /// The negotiated protocol version and capabilities.
    protocol: BootstrapResponse,
    /// Safe context metadata exposed to the UI.
    contexts: Vec<ContextInfo>,
}

impl BootstrapResult {
    /// Create a new bootstrap result.
    #[must_use]
    pub fn new(info: BootstrapInfo, server_instance_id: String) -> Self {
        Self {
            protocol: BootstrapResponse {
                protocol: ProtocolVersion { major: 1, minor: 1 },
                capabilities: vec!["logs.tail".into(), "exec.attach".into()],
                server: Some(ServerInfo {
                    instance_id: server_instance_id,
                    version: "0.1.0".into(),
                }),
            },
            contexts: info.contexts,
        }
    }

    /// Return the context names.
    #[must_use]
    pub fn context_names(&self) -> Vec<&str> {
        self.contexts.iter().map(|c| c.name.as_str()).collect()
    }

    /// Serialize the result to a JSON string.
    ///
    /// Never includes credentials or tokens.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(self).expect("BootstrapResult must serialize")
    }
}
