//! Deterministic fake Kubernetes adapter.
//!
//! Returns fixed contexts and never exposes credentials or tokens. Used by
//! tests and the desktop app for offline development.

use std::pin::Pin;

use crate::port::{
    BackendError, BootstrapInfo, Command, ContextInfo, KubernetesAccess, OperationId, Query,
    QueryResult, Subscribe, SubscriptionHandle,
};

/// A deterministic fake Kubernetes adapter.
///
/// Returns fixed contexts and never exposes credentials or tokens.
#[derive(Debug, Clone)]
pub struct FakeKubernetes {
    contexts: Vec<ContextInfo>,
}

impl FakeKubernetes {
    /// Create a standard fake Kubernetes adapter with two contexts.
    #[must_use]
    pub fn standard() -> Self {
        Self {
            contexts: vec![
                ContextInfo {
                    name: "dev-local".into(),
                    cluster: "dev-cluster".into(),
                    namespace: Some("default".into()),
                    is_current: true,
                },
                ContextInfo {
                    name: "prod-readonly".into(),
                    cluster: "prod-cluster".into(),
                    namespace: Some("default".into()),
                    is_current: false,
                },
            ],
        }
    }

    /// Create a fake Kubernetes adapter with custom contexts.
    #[must_use]
    pub fn with_contexts(contexts: Vec<ContextInfo>) -> Self {
        Self { contexts }
    }
}

impl Default for FakeKubernetes {
    fn default() -> Self {
        Self::standard()
    }
}

impl KubernetesAccess for FakeKubernetes {
    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match req {
                Query::Bootstrap => Ok(QueryResult::Bootstrap(BootstrapInfo {
                    contexts: self.contexts.clone(),
                })),
                Query::ValidateApply { .. } => Err(BackendError::unsupported("validate.apply")),
                Query::StreamTicket { .. } => Err(BackendError::unsupported("stream.ticket")),
            }
        })
    }

    fn execute<'a>(
        &'a self,
        cmd: Command,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<OperationId, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = cmd;
            Err(BackendError::unsupported("execute"))
        })
    }

    fn subscribe<'a>(
        &'a self,
        req: Subscribe,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match req {
                Subscribe::BootstrapStatus => Ok(SubscriptionHandle::new("bootstrap-status")),
                Subscribe::ResourceList { .. } => Err(BackendError::unsupported("resource.list")),
            }
        })
    }
}
