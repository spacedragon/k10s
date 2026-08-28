//! Outer kube client layer that turns runtime exec-token refresh failures into
//! context availability transitions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use kube::client::Body;
use tower::{BoxError, Layer, Service};

use crate::port::{BackendEvent, ContextAvailability};
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
    gate: Arc<tokio::sync::Mutex<()>>,
    availability_events: tokio::sync::broadcast::Sender<BackendEvent>,
}

impl AuthObserverLayer {
    pub(super) fn new(
        context: &str,
        registry: Arc<StdMutex<ContextRegistry>>,
        clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
        watches: ClusterWatches,
        metrics: ClusterMetrics,
        uses_exec_plugin: bool,
        availability_events: tokio::sync::broadcast::Sender<BackendEvent>,
    ) -> Self {
        Self {
            context: Arc::from(context),
            registry,
            clients,
            watches,
            metrics,
            uses_exec_plugin,
            gate: Arc::new(tokio::sync::Mutex::new(())),
            availability_events,
        }
    }
}

impl<S> Layer<S> for AuthObserverLayer {
    type Service = AuthObserver<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthObserver {
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
            context: Arc::clone(&self.context),
            registry: Arc::clone(&self.registry),
            clients: Arc::clone(&self.clients),
            watches: self.watches.clone(),
            metrics: self.metrics.clone(),
            uses_exec_plugin: self.uses_exec_plugin,
            gate: Arc::clone(&self.gate),
            availability_events: self.availability_events.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct AuthObserver<S> {
    inner: Arc<tokio::sync::Mutex<S>>,
    context: Arc<str>,
    registry: Arc<StdMutex<ContextRegistry>>,
    clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
    watches: ClusterWatches,
    metrics: ClusterMetrics,
    uses_exec_plugin: bool,
    gate: Arc<tokio::sync::Mutex<()>>,
    availability_events: tokio::sync::broadcast::Sender<BackendEvent>,
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

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is polled again after acquiring the async per-service
        // guard in `call`; this keeps a non-Clone boxed kube service safe to
        // share across the cloned outer clients.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        let context = Arc::clone(&self.context);
        let registry = Arc::clone(&self.registry);
        let clients = Arc::clone(&self.clients);
        let watches = self.watches.clone();
        let metrics = self.metrics.clone();
        let uses_exec_plugin = self.uses_exec_plugin;
        let gate = Arc::clone(&self.gate);
        let availability_events = self.availability_events.clone();
        Box::pin(async move {
            // kube-rs refreshes exec credentials inside the request future.
            // Serialize that boundary for this client so the first failure can
            // poison shared availability before another retained clone starts
            // the same credential helper.
            let _gate = if uses_exec_plugin {
                Some(gate.lock().await)
            } else {
                None
            };
            if uses_exec_plugin {
                let unavailable = registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .find(&context)
                    .filter(|entry| entry.availability == ContextAvailability::Unavailable)
                    .map(|entry| {
                        entry
                            .unavailable_reason
                            .clone()
                            .unwrap_or_else(|| "credential plugin is unavailable".into())
                    });
                if let Some(reason) = unavailable {
                    return Err(Box::new(ContextUnavailableMarker {
                        context: context.to_string(),
                        reason,
                    }) as BoxError);
                }
            }
            let future = {
                let mut inner = inner.lock().await;
                std::future::poll_fn(|cx| inner.poll_ready(cx))
                    .await
                    .map_err(Into::into)?;
                inner.call(request)
            };
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
                        let _ = availability_events.send(BackendEvent::ContextUnavailable {
                            context: context.to_string(),
                            reason: reason.clone(),
                        });
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

#[cfg(all(test, unix))]
mod tests {
    #[cfg(unix)]
    use std::process::{ExitStatus, Output};
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    use tower::{Layer as _, ServiceExt as _, service_fn};

    #[cfg(unix)]
    use crate::port::{ContextInfo, Gvk};
    #[cfg(unix)]
    use crate::runtime::cluster::{
        ClusterMetrics, ClusterWatches, MetricsApiState, MetricsPollSource, MetricsSnapshot,
    };
    #[cfg(unix)]
    use crate::runtime::supervisor::{ListedState, WatchSource, WatchUpdate};
    #[cfg(unix)]
    use crate::watch::WatchSelector;

    #[cfg(unix)]
    use super::*;

    #[cfg(unix)]
    fn failed_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(17 << 8)
    }

    #[cfg(unix)]
    #[derive(Debug)]
    struct PendingWatch;

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[derive(Debug)]
    struct OneMetricsCut;

    #[cfg(unix)]
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
        let (availability_events, mut availability_receiver) = tokio::sync::broadcast::channel(4);
        let observed = AuthObserverLayer::new(
            "active",
            Arc::clone(&registry),
            clients,
            watches.clone(),
            metrics.clone(),
            true,
            availability_events,
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
        let BackendEvent::ContextUnavailable { context, reason } = availability_receiver
            .try_recv()
            .expect("transition publishes")
        else {
            panic!("bootstrap-status receives a context transition");
        };
        assert_eq!(context, "active");
        assert!(reason.contains("access denied"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn concurrent_refresh_failure_executes_inner_service_once() {
        let registry = Arc::new(StdMutex::new(
            ContextRegistry::prepare(vec![ContextInfo::available(
                "active",
                "cluster-a",
                None,
                true,
            )])
            .expect("registry prepares"),
        ));
        let clients = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let service = service_fn({
            let calls = Arc::clone(&calls);
            move |_: http::Request<Body>| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Err::<http::Response<Body>, BoxError>(Box::new(
                        kube::client::AuthError::AuthExecRun {
                            cmd: "fixture".into(),
                            status: failed_status(),
                            out: Output {
                                status: failed_status(),
                                stdout: Vec::new(),
                                stderr: b"burst denied".to_vec(),
                            },
                        },
                    ))
                }
            }
        });
        let (availability_events, mut availability_receiver) = tokio::sync::broadcast::channel(4);
        let observed = AuthObserverLayer::new(
            "active",
            Arc::clone(&registry),
            clients,
            ClusterWatches::default(),
            ClusterMetrics::default(),
            true,
            availability_events,
        )
        .layer(service);

        let requests = (0..16).map(|_| {
            let service = observed.clone();
            async move {
                service
                    .oneshot(
                        http::Request::builder()
                            .uri("https://cluster.invalid/api")
                            .body(Body::empty())
                            .expect("request builds"),
                    )
                    .await
                    .expect_err("poisoned context rejects every caller")
            }
        });
        for error in futures_util::future::join_all(requests).await {
            assert!(error.downcast_ref::<ContextUnavailableMarker>().is_some());
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            availability_receiver.try_recv(),
            Ok(BackendEvent::ContextUnavailable { .. })
        ));
        assert!(availability_receiver.try_recv().is_err());
    }
}
