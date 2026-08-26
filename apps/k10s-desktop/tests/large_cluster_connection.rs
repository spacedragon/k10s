use std::thread;
use std::time::{Duration, Instant};

use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_server::{ServerConfig, spawn_loopback};
use k10s_ui::client::ConnectTarget;
use k10s_ui::workspace::{NamespaceScope, WorkloadKind};
use k10s_ui::{AppView, K10sApp};

const OBJECTS: usize = 12_000;
const NODES: usize = 16;
const EXPECTED_PODS: usize = OBJECTS * 3 / 8;
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn production_desktop_connection_drains_a_4300_plus_pod_snapshot() {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_thread = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let server = spawn_loopback(
                ServerConfig {
                    access_token: "secret".into(),
                    ..ServerConfig::default()
                },
                BackendKernel::new_with_instance_id(
                    FakeKubernetes::with_capacity(OBJECTS, NODES),
                    "desktop-large-snapshot",
                ),
            )
            .await
            .unwrap();
            ready_tx.send(server.addr()).unwrap();
            let _ = shutdown_rx.await;
            server.shutdown().await.unwrap();
        });
    });

    let addr = ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut app = K10sApp::connect(ConnectTarget::new(
        format!("ws://{addr}{}", k10s_protocol::CONTROL_PATH),
        "secret",
    ))
    .unwrap();
    let deadline = Instant::now() + DEADLINE;
    while matches!(app.view(), AppView::Connecting) && Instant::now() < deadline {
        app.poll();
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        matches!(app.view(), AppView::Ready { .. }),
        "{:?}",
        app.view()
    );

    let window = app.web_activate_workload(WorkloadKind::Pods).unwrap();
    app.web_set_namespace_scope(window, NamespaceScope::AllNamespaces);
    while app.web_resource_rows(WorkloadKind::Pods).len() < EXPECTED_PODS
        && Instant::now() < deadline
    {
        app.poll();
        assert!(
            matches!(app.view(), AppView::Ready { .. }),
            "large snapshot connection regressed: {:?}",
            app.view()
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        app.web_resource_rows(WorkloadKind::Pods).len(),
        EXPECTED_PODS
    );

    drop(app);
    shutdown_tx.send(()).ok();
    server_thread.join().unwrap();
}
