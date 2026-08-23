//! Real Kubernetes adapter backed by kube-rs.
//!
//! Kube-rs types are confined to this module tree: the rest of k10s only ever
//! sees normalized port types and [`AdapterError`]s. Bootstrap is served from
//! a committed, credential-free context registry; capabilities that have not
//! been implemented yet return typed unsupported errors like every other
//! adapter does.

mod config;

use std::path::Path;
use std::pin::Pin;

use crate::port::{
    AdapterError, BackendError, BootstrapInfo, Command, KubernetesAccess, OperationId, Query,
    QueryResult, StreamInput, Subscribe, SubscriptionHandle,
};
use crate::runtime::ContextRegistry;

/// Real Kubernetes adapter that loads contexts from a kubeconfig file.
///
/// The committed [`ContextRegistry`] is the only state: it holds safe context
/// summaries and nothing that resembles credential material.
#[derive(Debug)]
pub struct KubeAdapter {
    registry: ContextRegistry,
}

impl KubeAdapter {
    /// Build an adapter from an explicit kubeconfig path or standard
    /// discovery (`KUBECONFIG`, then `~/.kube/config`).
    ///
    /// Follows the prepare-then-commit protocol: loading and validation run
    /// first (prepare), and only a complete, valid registry is installed as
    /// bootstrap state (commit). Any failure returns a normalized
    /// [`AdapterError`] without leaving partial state.
    pub fn from_kubeconfig(path: Option<&Path>) -> Result<Self, AdapterError> {
        // Prepare: load and validate credential-free summaries off-line.
        let prepared = config::load_context_summaries(path)?;
        // Commit: install the complete registry as immutable adapter state.
        Ok(Self {
            registry: ContextRegistry::prepare(prepared)?,
        })
    }
}

impl KubernetesAccess for KubeAdapter {
    fn query<'a>(
        &'a self,
        req: Query,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<QueryResult, BackendError>> + Send + 'a>>
    {
        Box::pin(async move {
            match req {
                // Bootstrap is fully supported in Task 1: safe summaries only.
                Query::Bootstrap => Ok(QueryResult::Bootstrap(BootstrapInfo {
                    contexts: self.registry.contexts().to_vec(),
                })),
                // Cluster-facing capabilities arrive with later Plan 3 tasks;
                // until then they are typed, not guessed.
                Query::ValidateApply { .. } => Err(BackendError::unsupported("validate.apply")),
                Query::StreamTicket { .. } => Err(BackendError::unsupported("stream.ticket")),
                Query::ResourceList { .. } => Err(BackendError::unsupported("resource.list")),
                Query::ResourceDetail { .. } => Err(BackendError::unsupported("resource.detail")),
                Query::ResourceMetrics { .. } => Err(BackendError::unsupported("resource.metrics")),
                Query::ResourceRelations { .. } => {
                    Err(BackendError::unsupported("resource.relations"))
                }
                Query::ResourceTypes { .. } => Err(BackendError::unsupported("resource.types")),
                Query::Infrastructure { .. } => Err(BackendError::unsupported("infrastructure")),
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
                // Same protocol shape as the fake adapter's bootstrap status.
                Subscribe::BootstrapStatus => Ok(SubscriptionHandle::new("bootstrap-status")),
                Subscribe::ResourceWatch { .. } => Err(BackendError::unsupported("resource.watch")),
                Subscribe::Infrastructure { .. } => {
                    Err(BackendError::unsupported("infrastructure.watch"))
                }
                Subscribe::StreamRedeem { .. } => Err(BackendError::unsupported("stream.redeem")),
            }
        })
    }

    fn stream_input<'a>(
        &'a self,
        _ticket_id: &'a str,
        _input: StreamInput,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("stream.input")) })
    }
}
