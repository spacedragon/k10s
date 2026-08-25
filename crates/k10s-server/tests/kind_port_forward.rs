//! Real kind Service -> EndpointSlice -> Pod port-forward lifecycle.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use k10s_backend::{BackendMode, PortForwardPortSelection, build_kernel};
use k10s_protocol::{GroupVersionKind, ResourceIdentity};
use k10s_server::port_forward::{PortForwardManager, StopOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

fn kubeconfig() -> PathBuf {
    std::env::var_os("K10S_KIND_KUBECONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/.kubeconfig")
        })
}

fn kubectl(args: &[&str]) -> String {
    let output = Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .args(args)
        .output()
        .expect("kubectl is installed");
    assert!(
        output.status.success(),
        "kubectl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn real_service_forwards_http_and_releases_automatic_and_explicit_ports() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/port-forward.yaml");
    kubectl(&["apply", "-f", fixture.to_str().unwrap()]);
    kubectl(&[
        "rollout",
        "status",
        "deployment/port-forward-echo",
        "--timeout=120s",
    ]);
    let uid = kubectl(&[
        "get",
        "service",
        "port-forward-echo",
        "-o",
        "jsonpath={.metadata.uid}",
    ]);
    let context = kubectl(&["config", "current-context"]).trim().to_owned();
    let kernel = build_kernel(&BackendMode::Kube {
        kubeconfig: Some(kubeconfig()),
    })
    .unwrap();
    let connector = kernel.port_forward_connector().expect("kube connector");
    let cancel = CancellationToken::new();
    let (events, _) = tokio::sync::broadcast::channel(64);
    let manager = PortForwardManager::new(connector, cancel, events);
    let identity = ResourceIdentity {
        context: context.clone(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: "port-forward-echo".into(),
        uid,
    };

    for requested in [
        0_u16,
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port(),
    ] {
        let session = manager
            .start(
                identity.clone(),
                PortForwardPortSelection::Name("http".into()),
                requested,
                context.clone(),
            )
            .await
            .unwrap();
        let mut socket = tokio::net::TcpStream::connect(&session.local_addr)
            .await
            .unwrap();
        socket
            .write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(15), socket.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&response).contains("k10s-port-forward-ok"));
        let local = session.local_addr.clone();
        assert!(matches!(
            manager.stop(session.id.as_str()).await,
            StopOutcome::Stopped(_)
        ));
        assert!(
            std::net::TcpListener::bind(local).is_ok(),
            "Stop releases the local port"
        );
    }
    manager.shutdown().await;
}
