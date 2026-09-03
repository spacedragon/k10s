//! Real kind Service and direct Pod port-forward lifecycle.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use k10s_backend::{BackendMode, Query, build_kernel};
use k10s_protocol::{
    GroupVersionKind, PortForwardPortSelector, PortForwardSessionState, PortForwardTarget,
    ResourceIdentity,
};
use k10s_server::port_forward::{PortForwardManager, StopOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

const ADMIN_CONTEXT: &str = "kind-k10s-read-path";

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
        .arg("--context")
        .arg(ADMIN_CONTEXT)
        .args(args)
        .output()
        .expect("kubectl is installed");
    assert!(
        output.status.success(),
        "kubectl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn loopback_addr(local_addr: &str) -> std::net::SocketAddr {
    let address: std::net::SocketAddr = local_addr.parse().unwrap();
    assert_eq!(
        address.ip(),
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        "port forwards bind only to IPv4 localhost"
    );
    address
}

async fn exchange_http(local_addr: std::net::SocketAddr) {
    let mut socket = tokio::net::TcpStream::connect(local_addr).await.unwrap();
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
}

#[tokio::test]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn real_service_and_pod_forwards_share_management_and_release_ports() {
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/port-forward.yaml");
    kubectl(&["apply", "-f", fixture.to_str().unwrap()]);
    kubectl(&[
        "rollout",
        "status",
        "deployment/port-forward-echo",
        "--timeout=120s",
    ]);
    let service_uid = kubectl(&[
        "get",
        "service",
        "port-forward-echo",
        "-o",
        "jsonpath={.metadata.uid}",
    ]);
    let pod_name = kubectl(&[
        "get",
        "pod",
        "-l",
        "app=port-forward-echo",
        "-o",
        "jsonpath={.items[0].metadata.name}",
    ]);
    let pod_uid = kubectl(&[
        "get",
        "pod",
        pod_name.as_str(),
        "-o",
        "jsonpath={.metadata.uid}",
    ]);
    let context = ADMIN_CONTEXT.to_owned();
    let kernel = build_kernel(&BackendMode::Kube {
        kubeconfig: Some(kubeconfig()),
    })
    .unwrap();
    kernel
        .query(Query::Bootstrap)
        .await
        .expect("desktop bootstrap succeeds");
    kernel
        .query(Query::ResourceTypes {
            context: ADMIN_CONTEXT.into(),
        })
        .await
        .expect("context-scoped discovery initializes the selected client");
    let connector = kernel.port_forward_connector().expect("kube connector");
    let cancel = CancellationToken::new();
    let (events, _) = tokio::sync::broadcast::channel(64);
    let manager = PortForwardManager::new(connector, cancel, events);
    let service_identity = ResourceIdentity {
        context: context.clone(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: "port-forward-echo".into(),
        uid: service_uid,
    };
    let pod_identity = ResourceIdentity {
        context: context.clone(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: pod_name,
        uid: pod_uid,
    };

    let explicit_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let automatic_service = manager
        .start(
            PortForwardTarget::Service {
                identity: service_identity.clone(),
                port: PortForwardPortSelector::Name {
                    name: "http".into(),
                },
            },
            0,
            context.clone(),
        )
        .await
        .unwrap();
    let automatic_service_addr = loopback_addr(&automatic_service.local_addr);
    exchange_http(automatic_service_addr).await;
    assert!(matches!(
        manager.stop(automatic_service.id.as_str()).await,
        StopOutcome::Stopped(_)
    ));
    assert!(
        std::net::TcpListener::bind(automatic_service_addr).is_ok(),
        "Stop releases the automatically assigned Service port"
    );

    let service = manager
        .start(
            PortForwardTarget::Service {
                identity: service_identity,
                port: PortForwardPortSelector::Name {
                    name: "http".into(),
                },
            },
            explicit_port,
            context.clone(),
        )
        .await
        .unwrap();
    let service_addr = loopback_addr(&service.local_addr);
    exchange_http(service_addr).await;

    let pod = manager
        .start(
            PortForwardTarget::Pod {
                identity: pod_identity,
                container_name: "echo".into(),
                remote_port: 8_080,
            },
            0,
            context,
        )
        .await
        .unwrap();
    let pod_addr = loopback_addr(&pod.local_addr);
    exchange_http(pod_addr).await;
    exchange_http(pod_addr).await;
    assert_eq!(
        manager.session(pod.id.as_str()).await.unwrap().state,
        PortForwardSessionState::Active,
        "separate completed connections leave the Pod session active"
    );

    let sessions = manager.list().await;
    assert!(
        sessions
            .iter()
            .any(|session| matches!(session.target, PortForwardTarget::Service { .. }))
    );
    assert!(
        sessions
            .iter()
            .any(|session| matches!(session.target, PortForwardTarget::Pod { .. }))
    );

    assert!(matches!(
        manager.stop(pod.id.as_str()).await,
        StopOutcome::Stopped(_)
    ));
    assert!(
        std::net::TcpListener::bind(pod_addr).is_ok(),
        "Stop releases the direct Pod listener"
    );

    assert_eq!(
        manager.session(service.id.as_str()).await.unwrap().state,
        PortForwardSessionState::Active,
        "the Service session remains live for shutdown cleanup"
    );
    manager.shutdown().await;
    assert!(manager.list().await.is_empty());
    assert!(
        std::net::TcpListener::bind(service_addr).is_ok(),
        "shutdown releases the exact live Service listener"
    );
}
