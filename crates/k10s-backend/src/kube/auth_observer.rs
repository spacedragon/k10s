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

use super::auth;

#[derive(Clone)]
pub(super) struct AuthObserverLayer {
    context: Arc<str>,
    registry: Arc<StdMutex<ContextRegistry>>,
    clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
}

impl AuthObserverLayer {
    pub(super) fn new(
        context: &str,
        registry: Arc<StdMutex<ContextRegistry>>,
        clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
    ) -> Self {
        Self {
            context: Arc::from(context),
            registry,
            clients,
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
        }
    }
}

#[derive(Clone)]
pub(super) struct AuthObserver<S> {
    inner: S,
    context: Arc<str>,
    registry: Arc<StdMutex<ContextRegistry>>,
    clients: Arc<tokio::sync::Mutex<std::collections::HashMap<String, kube::Client>>>,
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
        Box::pin(async move {
            match future.await.map_err(Into::into) {
                Ok(response) => Ok(response),
                Err(error) => {
                    let Some(reason) = error
                        .downcast_ref::<kube::client::AuthError>()
                        .and_then(auth::classify_exec_auth_error)
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

    use tower::{Layer as _, ServiceExt as _, service_fn};

    use crate::port::ContextInfo;

    use super::*;

    #[cfg(unix)]
    fn failed_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(17 << 8)
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
        let observed =
            AuthObserverLayer::new("active", Arc::clone(&registry), clients).layer(service);

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
    }
}
