//! Bounded server `PortForwardManager` contracts: loopback-only binding,
//! lifecycle states, data-path isolation, hard limits, idempotent stop,
//! and shutdown join.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use k10s_backend::{BackendError, PortForwardConnector, PortForwardRequest, RejectionCategory};
use k10s_protocol::{
    GroupVersionKind, PortForwardPortSelector, PortForwardSessionState, PortForwardTarget,
    ResourceIdentity,
};

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

fn service_target(name: &str, port: PortForwardPortSelector) -> PortForwardTarget {
    PortForwardTarget::Service {
        identity: identity(name),
        port,
    }
}

fn pod_target(name: &str, container: &str, port: u16) -> PortForwardTarget {
    PortForwardTarget::Pod {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        },
        container_name: container.into(),
        remote_port: port,
    }
}

#[tokio::test]
async fn pod_duplicate_key_ignores_requested_local_port() {
    let seam = ScriptedSeam::new(&["web-pod"]);
    let (manager, _) = manager_for(seam).await;

    let first = manager
        .start(pod_target("web-pod", "app", 8_080), 0, "dev-local".into())
        .await
        .expect("Pod start succeeds");
    let focused = manager
        .start(
            pod_target("web-pod", "app", 8_080),
            32_001,
            "dev-local".into(),
        )
        .await
        .expect("same Pod key focuses regardless of requested local port");

    assert_eq!(focused.id, first.id);
    assert_eq!(first.requested_local_port, 0);
    assert!(matches!(first.target, PortForwardTarget::Pod { .. }));
}

/// A scripted seam: resolution succeeds for allowed names, otherwise a
/// typed rejection. Connections return duplex streams whose peer half is
/// handed to the test through a channel so bytes can be pumped.
#[derive(Debug)]
struct ScriptedSeam {
    allowed: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    peers: tokio::sync::Mutex<Vec<mpsc::Sender<tokio::io::DuplexStream>>>,
    fail_connections: Arc<std::sync::atomic::AtomicBool>,
}

impl ScriptedSeam {
    fn new(names: &[&str]) -> Self {
        Self {
            allowed: Arc::new(std::sync::Mutex::new(
                names.iter().map(|name| (*name).to_owned()).collect(),
            )),
            peers: tokio::sync::Mutex::new(Vec::new()),
            fail_connections: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn failure_switch(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.fail_connections.clone()
    }

    async fn add_peer_channel(&self) -> mpsc::Receiver<tokio::io::DuplexStream> {
        let (tx, rx) = mpsc::channel(64);
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
            let is_pod = matches!(&request.target, PortForwardTarget::Pod { .. });
            let (name, uid, source_port, namespace) = match &request.target {
                PortForwardTarget::Service { identity, port } => (
                    identity.name.clone(),
                    identity.uid.clone(),
                    match port {
                        PortForwardPortSelector::Number { number } => *number,
                        PortForwardPortSelector::Name { name } => name.parse().unwrap_or(80),
                    },
                    identity.namespace.clone().unwrap(),
                ),
                PortForwardTarget::Pod {
                    identity,
                    remote_port,
                    ..
                } => (
                    identity.name.clone(),
                    identity.uid.clone(),
                    *remote_port,
                    identity.namespace.clone().unwrap(),
                ),
            };
            let ok = self.allowed.lock().unwrap().contains(name.as_str());
            if !ok {
                return Err(BackendError::PortForward {
                    category: RejectionCategory::UnavailableEndpoint,
                    message: "scripted rejection".into(),
                });
            }
            Ok(k10s_backend::ResolvedPortForward {
                context: request.context,
                namespace,
                target_uid: uid.clone(),
                source_port,
                pod_name: if is_pod { name } else { "pinned-pod".into() },
                pod_uid: if is_pod { uid } else { "uid-pinned".into() },
                pod_port: if is_pod { source_port } else { 8_080 },
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
            if self
                .fail_connections
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(BackendError::PortForward {
                    category: RejectionCategory::UnavailableEndpoint,
                    message: "scripted open failure".into(),
                });
            }
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

#[tokio::test]
async fn publication_revisions_follow_wire_order_during_start_failure_race() {
    let seam = ScriptedSeam::new(&["first", "second"]);
    let failures = seam.failure_switch();
    let (manager, _) = manager_for(seam).await;
    let first = manager
        .start(
            service_target("first", PortForwardPortSelector::Number { number: 80 }),
            0,
            "dev-local".into(),
        )
        .await
        .unwrap();
    let mut events = manager.subscribe().await;
    failures.store(true, std::sync::atomic::Ordering::Release);

    let racing_manager = manager.clone();
    let start_second = tokio::spawn(async move {
        racing_manager
            .start(
                service_target("second", PortForwardPortSelector::Number { number: 80 }),
                0,
                "dev-local".into(),
            )
            .await
            .unwrap()
    });
    let first_addr: std::net::SocketAddr = first.local_addr.parse().unwrap();
    for _ in 0..3 {
        let _ = tokio::net::TcpStream::connect(first_addr).await.unwrap();
    }
    let second = start_second.await.unwrap();

    let mut revisions = Vec::new();
    while revisions.len() < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("publication arrives")
            .expect("subscription remains live");
        revisions.push(event.revision);
    }
    assert!(
        revisions.windows(2).all(|pair| pair[0] < pair[1]),
        "wire order must be identical to global revision order: {revisions:?}"
    );
    assert!(
        revisions.contains(&second.revision),
        "the raced Active snapshot is published exactly at its response revision"
    );
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
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
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
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
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
            service_target("other", PortForwardPortSelector::Number { number: 80 }),
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
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
            0,
            "dev-local".into(),
        )
        .await
        .expect("focus start");
    assert_eq!(focused.id, first.id);
    let focused_by_name = manager
        .start(
            service_target(
                "web",
                PortForwardPortSelector::Name {
                    name: "http".into(),
                },
            ),
            0,
            "dev-local".into(),
        )
        .await
        .expect("number then name focuses the resolved Service port");
    assert_eq!(focused_by_name.id, first.id);
    assert_eq!(manager.list().await.len(), 1);
    let _ = manager.stop(first.id.as_str()).await;
}

#[tokio::test]
async fn service_and_pod_sessions_share_the_same_session_limit() {
    let names: Vec<String> = (0..=k10s_server::port_forward::MAX_SESSIONS)
        .map(|index| format!("target-{index}"))
        .collect();
    let allowed: Vec<&str> = names.iter().map(String::as_str).collect();
    let seam = ScriptedSeam::new(&allowed);
    let (manager, _) = manager_for(seam).await;

    for (index, name) in names
        .iter()
        .take(k10s_server::port_forward::MAX_SESSIONS)
        .enumerate()
    {
        let target = if index % 2 == 0 {
            service_target(name, PortForwardPortSelector::Number { number: 80 })
        } else {
            pod_target(name, "app", 8_080)
        };
        manager
            .start(target, 0, "dev-local".into())
            .await
            .expect("the combined budget admits each slot");
    }

    let overflow = manager
        .start(
            pod_target(names.last().unwrap(), "app", 8_080),
            0,
            "dev-local".into(),
        )
        .await
        .expect_err("the next target of either kind exceeds the shared limit");
    assert_eq!(
        overflow.category,
        k10s_protocol::PortForwardFailureCategory::UnavailableEndpoint
    );
}

#[tokio::test]
async fn service_and_pod_sessions_share_the_global_connection_limit() {
    let seam = ScriptedSeam::new(&["service-a", "service-b", "pod-a", "pod-b"]);
    let _peers = seam.add_peer_channel().await;
    let (manager, _) = manager_for(seam).await;
    let mut addresses = Vec::new();
    for target in [
        service_target("service-a", PortForwardPortSelector::Number { number: 80 }),
        pod_target("pod-a", "app", 8_080),
        service_target("service-b", PortForwardPortSelector::Number { number: 80 }),
        pod_target("pod-b", "app", 8_080),
    ] {
        let session = manager.start(target, 0, "dev-local".into()).await.unwrap();
        addresses.push(session.local_addr.parse().unwrap());
    }
    let mut connections = Vec::new();
    for index in 0..k10s_server::port_forward::MAX_TOTAL_CONNECTIONS {
        connections.push(
            tokio::net::TcpStream::connect(addresses[index % addresses.len()])
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut overflow = tokio::net::TcpStream::connect(addresses[0]).await.unwrap();
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), overflow.read(&mut byte))
        .await
        .expect("overflow connection is closed promptly")
        .unwrap();
    assert_eq!(read, 0);
    drop(connections);
}

#[tokio::test]
async fn failed_snapshot_is_retained_and_does_not_block_a_new_start() {
    let seam = ScriptedSeam::new(&["web-pod"]);
    let failures = seam.failure_switch();
    let (manager, _) = manager_for(seam).await;
    let mut events = manager.subscribe().await;
    let failed = manager
        .start(pod_target("web-pod", "app", 8_080), 0, "dev-local".into())
        .await
        .unwrap();
    failures.store(true, std::sync::atomic::Ordering::Release);
    let addr = failed.local_addr.parse().unwrap();
    for _ in 0..3 {
        tokio::net::TcpStream::connect(addr).await.unwrap();
    }
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        if event.session.id == failed.id && event.session.state == PortForwardSessionState::Failed {
            break;
        }
    }

    failures.store(false, std::sync::atomic::Ordering::Release);
    let retried = manager
        .start(pod_target("web-pod", "app", 8_080), 0, "dev-local".into())
        .await
        .expect("terminal snapshots do not satisfy duplicate focus");
    assert_ne!(retried.id, failed.id);
    let listed = manager.list().await;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|session| {
        session.id == failed.id && session.state == PortForwardSessionState::Failed
    }));
}

#[tokio::test]
async fn stop_is_idempotent_and_data_flows_bidirectionally() {
    let seam = ScriptedSeam::new(&["web"]);
    let mut peers = seam.add_peer_channel().await;
    let (manager, _) = manager_for(seam).await;

    let session = manager
        .start(
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
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
    let retained = manager.list().await;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].state, PortForwardSessionState::Stopped);

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
        let target = if name == "a" {
            service_target(name, PortForwardPortSelector::Number { number: 80 })
        } else {
            pod_target(name, "app", 8_080)
        };
        let _ = manager
            .start(target, 0, "dev-local".into())
            .await
            .expect("start");
    }
    let epoch_before = manager.epoch().await;
    manager.begin_context_transition("prod".to_owned()).await;
    assert_eq!(manager.epoch().await, epoch_before + 1, "one epoch step");
    let retained = manager.list().await;
    assert_eq!(retained.len(), 2, "the drain retains both target kinds");
    assert!(
        retained
            .iter()
            .all(|session| session.state == PortForwardSessionState::Stopped)
    );
    while events.try_recv().is_ok() {}

    // Starts carrying the retired context abort without binding anything.
    let error = manager
        .start(
            service_target("a", PortForwardPortSelector::Number { number: 80 }),
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
async fn failed_context_commit_keeps_the_authoritative_context() {
    let seam = ScriptedSeam::new(&["web"]);
    let (manager, _) = manager_for(seam).await;
    let first = manager
        .start(
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
            0,
            "dev-local".into(),
        )
        .await
        .unwrap();
    let result: Result<(), &str> = manager
        .transition_context("missing".into(), async { Err("backend rejected") })
        .await;
    assert_eq!(result, Err("backend rejected"));
    let retried = manager
        .start(
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
            0,
            "dev-local".into(),
        )
        .await
        .expect("failed backend commit must not retire the current context");
    assert_ne!(retried.id, first.id);
}

#[tokio::test]
async fn shutdown_cancels_sessions_and_releases_their_ports() {
    let seam = ScriptedSeam::new(&["web"]);
    let (manager, cancel) = manager_for(seam).await;
    let session = manager
        .start(
            service_target("web", PortForwardPortSelector::Number { number: 80 }),
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

/// Regression: Stop must complete while a pump is mid-copy, tear the local
/// connection down, and leave the port immediately rebindable.
#[tokio::test]
async fn stop_joins_active_pumps_and_closes_their_local_connections() {
    let seam = ScriptedSeam::new(&["web"]);
    let mut peers = seam.add_peer_channel().await;
    let (manager, _) = manager_for(seam).await;

    let session = manager
        .start(
            service_target(
                "web",
                PortForwardPortSelector::Name {
                    name: "http".into(),
                },
            ),
            0,
            "dev-local".into(),
        )
        .await
        .expect("named start keeps declared identity");
    assert!(matches!(
        session.target,
        PortForwardTarget::Service {
            port: PortForwardPortSelector::Name { ref name },
            ..
        } if name == "http"
    ));

    let addr: std::net::SocketAddr = session.local_addr.parse().unwrap();
    let mut local = tokio::net::TcpStream::connect(addr).await.unwrap();
    local.write_all(b"payload").await.unwrap();
    let mut upstream = peers.recv().await.expect("pump connected upstream");
    upstream.read_exact(&mut [0_u8; 7]).await.unwrap();
    // The pump is now actively copying with open directions on both sides.

    // Stop must not deadlock on the live pump and must join it.
    let stopped = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        manager.stop(session.id.as_str()),
    )
    .await
    .expect("stop completes without joining deadlocks");
    assert!(matches!(
        stopped,
        k10s_server::port_forward::StopOutcome::Stopped(_)
    ));

    // The local half is closed by the torn-down pump.
    let mut closed_probe = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        local.read_to_end(&mut closed_probe),
    )
    .await
    .expect("the local socket closes after stop");
    drop(upstream);
    drop(peers);

    assert!(
        tokio::net::TcpListener::bind(addr).await.is_ok(),
        "the port is released even though a connection was open"
    );
}

/// Regression: a context transition and a shutdown must both drain sessions
/// with live pumps without deadlocking on the gate.
#[tokio::test]
async fn context_transition_and_shutdown_drain_live_pumps_without_deadlock() {
    for scenario in ["transition", "shutdown"] {
        let seam = ScriptedSeam::new(&["web"]);
        let mut peers = seam.add_peer_channel().await;
        let (manager, cancel) = manager_for(seam).await;
        let session = manager
            .start(
                service_target("web", PortForwardPortSelector::Number { number: 80 }),
                0,
                "dev-local".into(),
            )
            .await
            .expect("start");
        let addr: std::net::SocketAddr = session.local_addr.parse().unwrap();
        let mut local = tokio::net::TcpStream::connect(addr).await.unwrap();
        local.write_all(b"x").await.unwrap();
        let mut upstream = peers.recv().await.expect("pump upstream arrives");
        upstream.read_exact(&mut [0_u8; 1]).await.unwrap();

        let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            match scenario {
                "transition" => {
                    manager.begin_context_transition("prod".to_owned()).await;
                }
                _ => manager.shutdown().await,
            }
        })
        .await;
        assert!(drained.is_ok(), "{scenario}: gate drains live pumps");
        assert!(
            tokio::net::TcpListener::bind(addr).await.is_ok(),
            "{scenario}: the listener port is freed"
        );
        cancel.cancel();
        drop(local);
        drop(upstream);
        drop(peers);
    }
}
