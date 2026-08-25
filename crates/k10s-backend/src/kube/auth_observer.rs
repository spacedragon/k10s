//! Outer kube client layer that turns runtime exec-token refresh failures into
//! context availability transitions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use kube::client::Body;
use tower::{BoxError, Layer, Service};

use crate::port::ContextAvailability;
use crate::runtime::ContextRegistry;
use crate::runtime::cluster::{ClusterMetrics, ClusterWatches};

use super::auth;

#[derive(Clone)]
pub(super) struct AuthObserverLayer {
    context: Arc<str>,
    registry: Arc<StdMutex<ContextRegistry>>,
    clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
    watches: ClusterWatches,
    metrics: ClusterMetrics,
    uses_exec_plugin: bool,
}

impl AuthObserverLayer {
    pub(super) fn new(
        context: &str,
        registry: Arc<StdMutex<ContextRegistry>>,
        clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
        watches: ClusterWatches,
        metrics: ClusterMetrics,
        uses_exec_plugin: bool,
    ) -> Self {
        Self {
            context: Arc::from(context),
            registry,
            clients,
            watches,
            metrics,
            uses_exec_plugin,
        }
    }
}

impl<S> Layer<S> for AuthObserverLayer {
    type Service = AuthObserver<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthObserver {
            inner,
            context: Arc::clone(&self.context),
            registry: Arc::clone(&self.registry),
            clients: Arc::clone(&self.clients),
            watches: self.watches.clone(),
            metrics: self.metrics.clone(),
            uses_exec_plugin: self.uses_exec_plugin,
        }
    }
}

#[derive(Clone)]
pub(super) struct AuthObserver<S> {
    inner: S,
    context: Arc<str>,
    registry: Arc<StdMutex<ContextRegistry>>,
    clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
    watches: ClusterWatches,
    metrics: ClusterMetrics,
    uses_exec_plugin: bool,
}

impl<S> Service<http::Request<Body>> for AuthObserver<S>
where
    S: Service<http::Request<Body>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    S::Response: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let future = self.inner.call(request);
        let context = Arc::clone(&self.context);
        let registry = Arc::clone(&self.registry);
        let clients = Arc::clone(&self.clients);
        let watches = self.watches.clone();
        let metrics = self.metrics.clone();
        let uses_exec_plugin = self.uses_exec_plugin;
        Box::pin(async move {
            match future.await.map_err(Into::into) {
                Ok(response) => Ok(response),
                Err(error) => {
                    let Some(reason) = error
                        .downcast_ref::<kube::client::AuthError>()
                        .and_then(|error| auth::classify_exec_auth_error(error, uses_exec_plugin))
                    else {
                        return Err(error);
                    };

                    let transitioned = {
                        let mut registry = registry
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let already_unavailable = registry.find(&context).is_some_and(|entry| {
                            entry.availability == ContextAvailability::Unavailable
                        });
                        if !already_unavailable {
                            let (generation, _) = registry.snapshot();
                            let changed =
                                registry.mark_unavailable(generation, &context, reason.clone());
                            if changed {
                                registry.choose_available_fallback();
                            }
                            changed
                        } else {
                            false
                        }
                    };
                    if transitioned {
                        watches.retire_context(&context);
                        metrics.retire_context(&context);
                        tracing::warn!(
                            context = %context,
                            reason = %reason,
                            "Kubernetes context credential plugin failed during token refresh"
                        );
                    }
                    clients.lock().await.remove(context.as_ref());
                    Err(Box::new(ContextUnavailableMarker {
                        context: context.to_string(),
                        reason,
                    }) as BoxError)
                }
            }
        })
    }
}

#[derive(Debug)]
pub(super) struct ContextUnavailableMarker {
    pub(super) context: String,
    pub(super) reason: String,
}

impl fmt::Display for ContextUnavailableMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Kubernetes context credential plugin is unavailable")
    }
}

impl std::error::Error for ContextUnavailableMarker {}

#[cfg(test)]
mod tests {
    use std::process::{ExitStatus, Output};
    use std::sync::Arc;
    use std::time::Duration;

    use tower::{Layer as _, ServiceExt as _, service_fn};

    use crate::port::{ContextInfo, Gvk};
    use crate::runtime::cluster::{
        ClusterMetrics, ClusterWatches, MetricsApiState, MetricsPollSource, MetricsSnapshot,
    };
    use crate::runtime::supervisor::{ListedState, WatchSource, WatchUpdate};
    use crate::watch::WatchSelector;

    use super::*;

    #[cfg(unix)]
    fn failed_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(17 << 8)
    }

    #[derive(Debug)]
    struct PendingWatch;

    impl WatchSource for PendingWatch {
        fn list<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<ListedState, String>> + Send + 'a>> {
            Box::pin(std::future::pending())
        }

        fn attach_watch<'a>(
            &'a self,
            _resource_version: String,
            _out: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Debug)]
    struct OneMetricsCut;

    impl MetricsPollSource for OneMetricsCut {
        fn poll(&self) -> Pin<Box<dyn Future<Output = MetricsSnapshot> + Send + '_>> {
            Box::pin(async {
                MetricsSnapshot {
                    context: "active".into(),
                    collected_at: "2026-08-25T00:00:00Z".into(),
                    source_updated_at: None,
                    window_seconds: None,
                    state: MetricsApiState::Ready,
                    node_usage: Default::default(),
                    pod_usage: Default::default(),
                    node_names: Vec::new(),
                    pod_capacity_total: None,
                }
            })
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn runtime_exec_failure_disables_context_and_selects_fallback() {
        let registry = Arc::new(StdMutex::new(
            ContextRegistry::prepare(vec![
                ContextInfo::available("active", "cluster-a", None, true),
                ContextInfo::available("fallback", "cluster-b", None, false),
            ])
            .expect("registry prepares"),
        ));
        let clients = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let watches = ClusterWatches::new(Duration::from_secs(60));
        let _watch = watches.subscribe(
            WatchSelector {
                context: "active".into(),
                gvk: Gvk::core("v1", "Pod"),
                namespace: Some("default".into()),
            },
            Arc::new(PendingWatch),
        );
        let metrics = ClusterMetrics::new(Duration::from_secs(60), Duration::from_secs(60));
        metrics
            .collect_for_consumer("active", || Arc::new(OneMetricsCut))
            .await
            .expect("first metrics cut arrives");
        assert_eq!(watches.live_selections(), 1);
        assert_eq!(metrics.live_collectors(), 1);
        let service = service_fn(|_: http::Request<Body>| async move {
            Err::<http::Response<Body>, BoxError>(Box::new(kube::client::AuthError::AuthExecRun {
                cmd: "command-secret".into(),
                status: failed_status(),
                out: Output {
                    status: failed_status(),
                    stdout: b"stdout-secret".to_vec(),
                    stderr: b"access denied TOKEN=stderr-secret".to_vec(),
                },
            }))
        });
        let observed = AuthObserverLayer::new(
            "active",
            Arc::clone(&registry),
            clients,
            watches.clone(),
            metrics.clone(),
            true,
        )
        .layer(service);

        let error = observed
            .oneshot(
                http::Request::builder()
                    .uri("https://cluster.invalid/api")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect_err("runtime auth failure is observed");
        let marker = error
            .downcast_ref::<ContextUnavailableMarker>()
            .expect("typed unavailable marker survives");
        assert!(marker.reason.contains("access denied"));
        for secret in ["stdout-secret", "stderr-secret", "command-secret"] {
            assert!(!marker.reason.contains(secret));
        }

        let registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let active = registry.find("active").expect("active remains visible");
        assert_eq!(active.availability, ContextAvailability::Unavailable);
        assert!(!active.is_current);
        assert!(
            registry
                .find("fallback")
                .expect("fallback exists")
                .is_current
        );
        assert_eq!(
            watches.live_selections(),
            0,
            "runtime failure retires retained watch clients"
        );
        assert_eq!(
            metrics.live_collectors(),
            0,
            "runtime failure retires retained metrics clients"
        );
    }
}
