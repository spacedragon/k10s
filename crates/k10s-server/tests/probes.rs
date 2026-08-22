use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_server::{Readiness, ReadinessState, ServerConfig, router};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

async fn probe(readiness: &Arc<Readiness>, path: &str) -> (StatusCode, String) {
    let app = router(
        ServerConfig::default(),
        BackendKernel::new(FakeKubernetes::standard()),
        CancellationToken::new(),
        Arc::clone(readiness),
        None,
    );
    let response = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn health_is_live_for_every_process_lifecycle_state() {
    let readiness = Readiness::new();
    for state in [
        ReadinessState::Starting,
        ReadinessState::Ready,
        ReadinessState::InitializationFailed,
        ReadinessState::Draining,
    ] {
        readiness.set(state);
        let (status, body) = probe(&readiness, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok\n");
    }
}

#[tokio::test]
async fn ready_only_after_initialization_and_request_acceptance() {
    let readiness = Readiness::new();
    assert_eq!(
        probe(&readiness, "/readyz").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    readiness.set(ReadinessState::Ready);
    assert_eq!(
        probe(&readiness, "/readyz").await,
        (StatusCode::OK, "ready\n".to_owned())
    );
}

#[tokio::test]
async fn readiness_failure_and_draining_bodies_are_safe() {
    let readiness = Readiness::new();
    readiness.set(ReadinessState::InitializationFailed);
    let failed = probe(&readiness, "/readyz").await;
    assert_eq!(
        failed,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "initialization failed\n".to_owned()
        )
    );

    readiness.set(ReadinessState::Draining);
    let draining = probe(&readiness, "/readyz").await;
    assert_eq!(
        draining,
        (StatusCode::SERVICE_UNAVAILABLE, "draining\n".to_owned())
    );

    for (_, body) in [failed, draining] {
        assert!(!body.contains("token"));
        assert!(!body.contains("kubeconfig"));
        assert!(!body.contains('/'));
    }
}
