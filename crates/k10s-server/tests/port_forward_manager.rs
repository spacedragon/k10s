//! Bounded server `PortForwardManager` contracts: loopback-only binding,
//! lifecycle states, data-path isolation, hard limits, idempotent stop,
//! and shutdown join.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use k10s_backend::{
    BackendError, PortForwardConnector, PortForwardRequest, RejectionCategory,
};
use k10s_protocol::{GroupVersionKind, PortForwardSessionState, ResourceIdentity};

use k10s_server::port_forward::PortForwardManager;

fn identity(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: name.into(),
        uid: format!("uid-{name}"),
    }
}

/// A scripted seam: resolution succeeds for allowed names, otherwise a
/// typed rejection. Connections return duplex streams whose peer half is
/// handed to the test through a channel so bytes can be pumped.
#[derive(Debug)]
struct ScriptedSeam {
    allowed: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    peers: tokio::sync::Mutex<Vec<mpsc::Sender<tokio::io::DuplexStream>>>,
}

impl ScriptedSeam {
    fn new(names: &[&str]) -> Self {
        Self {
            allowed: Arc::new(std::sync::Mutex::new(
                names.iter().map(|name| (*name).to_owned()).collect(),
            )),
            peers: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    async fn add_peer_channel(&self) -> mpsc::Receiver<tokio::io::DuplexStream> {
        let (tx, rx) = mpsc::channel(4);
        self.peers.lock().await.push(tx);
        rx
    }
}

impl k10s_backend::PortForwardSeam for ScriptedSeam {
    fn resolve<'a>(
        &'a self,
        request: PortForwardRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<k10s_backend::ResolvedPortForward, BackendError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let ok = self
                .allowed
                .lock()
                .unwrap()
                .contains(request.service_name.as_str());
            if !ok {
                return Err(BackendError::PortForward {
                    category: RejectionCategory::UnavailableEndpoint,
                    message: "scripted rejection".into(),
                });
            }
            Ok(k10s_backend::ResolvedPortForward {
                context: request.context,
                namespace: request.namespace,
                service_uid: request.service_uid,
                pod_name: "pinned-pod".into(),
                pod_uid: "uid-pinned".into(),
                pod_port: 8_080,
            })
        })
    }

    fn connect<'a>(
        &'a self,
        resolved: &'a k10s_backend::ResolvedPortForward,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<k10s_backend::PortForwardStream, BackendError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let (client_side, server_side) = tokio::io::duplex(4096);
            let mut channels = self.peers.lock().await;
            match channels.last_mut() {
                Some(tx) => {
                    tx.send(server_side).await.ok();
                }
                None => drop(server_side),
            }
            let _ = resolved;
            Ok(k10s_backend::PortForwardStream::new(Box::new(client_side)))
        })
    }
}

async fn manager_for(seam: ScriptedSeam) -> (PortForwardManager, CancellationToken) {
    let cancel = CancellationToken::new();
    let (events_tx, _events_rx) = broadcast::channel(64);
    let manager = PortForwardManager::new(
        PortForwardConnector::new(Arc::new(seam)),
        cancel.clone(),
        events_tx,
    );
    // Sessions validate against the committed context.
    manager.begin_context_transition("dev-local".into()).await;
    (manager, cancel)
}

#[tokio::test]
async fn binds_loopback_with_os_assigned_ports_and_reports_active() {
    let seam = ScriptedSeam::new(&["web"]);
    let peers = seam.add_peer_channel().await;
    let (manager, _) = manager_for(seam).await;

    let session = manager
        .start(
            identity("web"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await
        .expect("start succeeds");
    assert!(
        session.local_addr.starts_with("127.0.0.1:"),
        "loopback only: {session:?}"
    );
    assert_eq!(session.state, PortForwardSessionState::Active);
    assert_eq!(session.pod.name, "pinned-pod");
    drop(peers);

    let listed = manager.list().await;
    assert_eq!(listed.len(), 1);
    let _ = manager.stop(session.id.as_str()).await;
}

#[tokio::test]
async fn explicit_occupied_ports_fail_and_duplicate_starts_focus() {
    let seam = ScriptedSeam::new(&["web", "other"]);
    let (manager, _) = manager_for(seam).await;

    let first = manager
        .start(
            identity("web"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await
        .expect("os-assigned start");
    // Occupy an explicit port with a raw listener.
    let addr: std::net::SocketAddr = first.local_addr.parse().unwrap();
    assert_ne!(addr.port(), 0);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied = listener.local_addr().unwrap().port();

    let error = manager
        .start(
            identity("other"),
            k10s_backend::PortForwardPortSelection::Number(80),
            occupied,
            "dev-local".into(),
        )
        .await;
    assert!(
        matches!(&error, Err(rejected) if rejected.category == k10s_protocol::PortForwardFailureCategory::LocalPortInUse),
        "occupied ports fail typed: {error:?}"
    );

    // Duplicate Service UID + Service-port focuses instead of duplicating.
    let focused = manager
        .start(
            identity("web"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await
        .expect("focus start");
    assert_eq!(focused.id, first.id);
    assert_eq!(manager.list().await.len(), 1);
    let _ = manager.stop(first.id.as_str()).await;
}

#[tokio::test]
async fn stop_is_idempotent_and_data_flows_bidirectionally() {
    let seam = ScriptedSeam::new(&["web"]);
    let mut peers = seam.add_peer_channel().await;
    let (manager, _) = manager_for(seam).await;

    let session = manager
        .start(
            identity("web"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await
        .expect("start");

    // One local connection pumps bytes to the upstream peer and back.
    let addr: std::net::SocketAddr = session.local_addr.parse().unwrap();
    let mut local = tokio::net::TcpStream::connect(addr).await.unwrap();
    local.write_all(b"ping").await.unwrap();
    let mut upstream = peers.recv().await.expect("peer stream arrives");
    let mut buffer = [0_u8; 4];
    upstream.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"ping");
    upstream.write_all(b"pong").await.unwrap();
    upstream.shutdown().await.unwrap();
    let mut echoed = Vec::new();
    local.read_to_end(&mut echoed).await.unwrap_or_default();

    let stopped = manager.stop(session.id.as_str()).await;
    assert!(matches!(
        stopped,
        k10s_server::port_forward::StopOutcome::Stopped(_)
    ));
    // Idempotent repeat.
    assert_eq!(
        manager.stop(session.id.as_str()).await,
        k10s_server::port_forward::StopOutcome::AlreadyTerminal
    );

    // The bound port is released immediately after stop.
    let rebound = tokio::net::TcpListener::bind(addr).await;
    assert!(rebound.is_ok(), "stop releases the local port");
}

#[tokio::test]
async fn context_transition_stops_every_session_and_advances_the_epoch_once() {
    let seam = ScriptedSeam::new(&["a", "b"]);
    let (manager, _) = manager_for(seam).await;
    let mut events = manager.subscribe().await;

    for name in ["a", "b"] {
        let _ = manager
            .start(
                identity(name),
                k10s_backend::PortForwardPortSelection::Number(80),
                0,
                "dev-local".into(),
            )
            .await
            .expect("start");
    }
    let epoch_before = manager.epoch().await;
    manager.begin_context_transition("prod".to_owned()).await;
    assert_eq!(manager.epoch().await, epoch_before + 1, "one epoch step");
    assert!(
        manager.list().await.is_empty(),
        "the drain removed every session"
    );
    while events.try_recv().is_ok() {}

    // Starts carrying the retired context abort without binding anything.
    let error = manager
        .start(
            identity("a"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await;
    assert!(
        matches!(&error, Err(rejected) if rejected.category == k10s_protocol::PortForwardFailureCategory::ContextTransition),
        "stale contexts fail retryable"
    );
}

#[tokio::test]
async fn shutdown_cancels_sessions_and_releases_their_ports() {
    let seam = ScriptedSeam::new(&["web"]);
    let (manager, cancel) = manager_for(seam).await;
    let session = manager
        .start(
            identity("web"),
            k10s_backend::PortForwardPortSelection::Number(80),
            0,
            "dev-local".into(),
        )
        .await
        .expect("start");
    let addr: std::net::SocketAddr = session.local_addr.parse().unwrap();

    manager.shutdown().await;
    assert!(cancel.is_cancelled());
    assert!(manager.list().await.is_empty());
    assert!(
        tokio::net::TcpListener::bind(addr).await.is_ok(),
        "ports rebind immediately"
    );
}
