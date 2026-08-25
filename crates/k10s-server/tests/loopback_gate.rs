//! End-to-end seam: the web connection gate against a default standalone server.

use std::time::{Duration, Instant};

use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::CONTROL_PATH;
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::{AppView, ConnectionGate, K10sApp};

#[tokio::test]
async fn gate_submission_authenticates_the_default_standalone_server() {
    let server = spawn_loopback(
        ServerConfig::default(),
        BackendKernel::new(FakeKubernetes::standard()),
    )
    .await
    .unwrap();

    // Default standalone startup carries no token; the gate submits an empty
    // buffer and the full protocol stack must still reach the bootstrap view.
    let mut gate = ConnectionGate::new(format!("ws://{}{CONTROL_PATH}", server.addr()));
    assert!(gate.is_visible());
    let target = gate.begin_connection();
    assert!(!gate.is_visible());

    let mut app = K10sApp::connect(target).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !matches!(app.view(), AppView::Ready { .. }) {
        assert!(
            Instant::now() < deadline,
            "gate connection never reached ready"
        );
        app.poll();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let AppView::Ready {
        server_instance_id: _,
        context_names,
        ..
    } = app.view()
    else {
        unreachable!("loop exited on Ready");
    };
    assert_eq!(context_names.len(), 2);

    drop(app);
    server.shutdown().await.unwrap();
}
