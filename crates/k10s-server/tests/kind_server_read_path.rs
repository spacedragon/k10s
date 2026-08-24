//! Ignored real-control-socket smoke over the ephemeral kind cluster.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_protocol::{
    BootstrapResponse, ContextPermissionsRequest, ContextPermissionsResponse, GroupVersionKind,
    MetricsAvailability, PermissionOutcome, PermissionProbe, ResourceDetailResponse,
    ResourceListRequest, ResourceListResponse, ResourceMetricsResponse, ResourceRefRequest,
    ResourceSnapshotPage, ResourceTypesRequest, ResourceTypesResponse, ServerFrame, ServerKind,
    ServerPayload, SnapshotBegin, SnapshotChunk,
};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct ServerProcess {
    child: Child,
    dist_dir: PathBuf,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dist_dir);
    }
}

fn server_binary() -> PathBuf {
    std::env::var_os("K10S_SERVER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join(format!("k10s-server{}", std::env::consts::EXE_SUFFIX))
        })
}

fn spawn_server_app() -> (ServerProcess, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let dist_dir = std::env::temp_dir().join(format!("k10s-server-kind-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dist_dir);
    std::fs::create_dir_all(&dist_dir).unwrap();
    std::fs::write(
        dist_dir.join("index.html"),
        "<!doctype html><title>k10s</title>",
    )
    .unwrap();
    let child = Command::new(server_binary())
        .args(["--kubeconfig"])
        .arg(kubeconfig())
        .env("K10S_BIND_ADDR", address.to_string())
        .env("K10S_ACCESS_TOKEN", "kind-secret")
        .env("K10S_DIST_DIR", &dist_dir)
        .spawn()
        .expect("build k10s-server-app before running this E2E");
    (
        ServerProcess { child, dist_dir },
        format!("ws://{address}{}", k10s_protocol::CONTROL_PATH),
    )
}

fn kubeconfig() -> PathBuf {
    std::env::var_os("K10S_KIND_KUBECONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/.kubeconfig")
        })
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(90), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket remains open")
        .expect("socket remains healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

fn admin_context() -> String {
    let cluster =
        std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".to_owned());
    format!("kind-{cluster}")
}

fn kubectl_status(args: &[&str]) -> bool {
    Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args(args)
        .status()
        .unwrap()
        .success()
}

fn kubectl(args: &[&str]) {
    assert!(kubectl_status(args), "kubectl failed: {args:?}");
}

async fn consume_snapshot(ws: &mut Ws) -> Vec<k10s_protocol::ResourceIdentity> {
    let begin = receive_frame(ws).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin, "{begin:?}");
    consume_snapshot_from_begin(ws, begin).await
}

async fn consume_snapshot_from_begin(
    ws: &mut Ws,
    begin: ServerFrame,
) -> Vec<k10s_protocol::ResourceIdentity> {
    let begin: SnapshotBegin = serde_json::from_value(begin.payload).unwrap();
    let mut identities = Vec::new();
    for _ in 0..begin.total_chunks {
        let chunk = receive_frame(ws).await;
        assert_eq!(chunk.kind, ServerKind::SnapshotChunk, "{chunk:?}");
        let chunk: SnapshotChunk = serde_json::from_value(chunk.payload).unwrap();
        let page: ResourceSnapshotPage = serde_json::from_value(chunk.data).unwrap();
        identities.extend(page.rows.into_iter().map(|row| row.identity));
    }
    assert_eq!(receive_frame(ws).await.kind, ServerKind::SnapshotEnd);
    identities
}

async fn receive_resource_event(ws: &mut Ws, expected: &str) -> Value {
    loop {
        let frame = receive_frame(ws).await;
        if frame.kind != ServerKind::Event {
            continue;
        }
        let ServerPayload::Event(event) = frame.decode_payload().unwrap() else {
            continue;
        };
        if event.event_kind == expected {
            return event.payload;
        }
    }
}

async fn send_request(ws: &mut Ws, request_id: &str, kind: &str, payload: Value) -> ServerFrame {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {"kind": kind, "payload": payload}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame
}

#[tokio::test]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn standalone_control_socket_serves_live_kind_data_not_fake_fixtures() {
    let (_server, url) = spawn_server_app();
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match connect_async(&url).await {
                Ok(socket) => break socket,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("the standalone server binds its configured socket");
    ws.send(Message::Text(
        json!({
            "kind":"hello",
            "payload":{
                "protocolMajor":1,
                "protocolMinor":1,
                "capabilities":[],
                "accessToken":"kind-secret"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Welcome);

    let bootstrap = send_request(&mut ws, "bootstrap", "bootstrap", json!({})).await;
    let bootstrap: BootstrapResponse = bootstrap.decode_response_payload().unwrap();
    assert!(
        bootstrap
            .contexts
            .iter()
            .any(|context| context.name == "k10s-limited")
    );
    assert!(
        bootstrap
            .contexts
            .iter()
            .all(|context| context.name != "dev-local")
    );

    let types = send_request(
        &mut ws,
        "types",
        "resource.types",
        serde_json::to_value(ResourceTypesRequest {
            context: "k10s-limited".into(),
        })
        .unwrap(),
    )
    .await;
    let types: ResourceTypesResponse = types.decode_response_payload().unwrap();
    assert!(types.types.iter().any(|entry| entry.gvk.kind == "Widget"));

    let list = send_request(
        &mut ws,
        "pods",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "k10s-limited".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("k10s-read".into()),
        })
        .unwrap(),
    )
    .await;
    let list: ResourceListResponse = list.decode_response_payload().unwrap();
    assert_eq!(list.rows.len(), 2);
    assert!(
        list.rows
            .iter()
            .all(|row| row.identity.context == "k10s-limited")
    );

    let metrics = send_request(
        &mut ws,
        "metrics",
        "resource.metrics",
        serde_json::to_value(ResourceRefRequest {
            identity: list.rows[0].identity.clone(),
        })
        .unwrap(),
    )
    .await;
    let metrics: ResourceMetricsResponse = metrics.decode_response_payload().unwrap();
    assert_eq!(
        metrics.metrics.availability,
        MetricsAvailability::Unavailable
    );
    assert!(metrics.metrics.cpu_millicores.is_none());
    assert!(metrics.metrics.memory_bytes.is_none());

    let deployments = send_request(
        &mut ws,
        "deployments",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "k10s-limited".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("k10s-read".into()),
        })
        .unwrap(),
    )
    .await;
    let deployments: ResourceListResponse = deployments.decode_response_payload().unwrap();
    let deployment = deployments
        .rows
        .iter()
        .find(|row| row.identity.name == "read-path-web")
        .unwrap()
        .identity
        .clone();
    let detail = send_request(
        &mut ws,
        "detail",
        "resource.detail",
        serde_json::to_value(ResourceRefRequest {
            identity: deployment,
        })
        .unwrap(),
    )
    .await;
    let detail: ResourceDetailResponse = detail.decode_response_payload().unwrap();
    assert!(detail.manifest.contains("read-path-web"));
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.reason == "FixtureReady")
    );
    assert!(detail.related.iter().any(|group| group.gvk.kind == "Pod"));

    let permissions = send_request(
        &mut ws,
        "permissions",
        "context.permissions",
        serde_json::to_value(ContextPermissionsRequest {
            context: "k10s-limited".into(),
            probes: vec![
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-read".into()),
                },
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-forbidden".into()),
                },
            ],
        })
        .unwrap(),
    )
    .await;
    let permissions: ContextPermissionsResponse = permissions.decode_response_payload().unwrap();
    assert_eq!(permissions.checks[0].outcome, PermissionOutcome::Allowed);
    assert_eq!(permissions.checks[1].outcome, PermissionOutcome::Denied);

    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": "forbidden",
            "payload": {
                "kind": "resource.list",
                "payload": {
                    "context": "k10s-limited",
                    "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                    "namespace": "k10s-forbidden"
                }
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Error);

    ws.send(Message::Text(
        json!({
            "kind": "subscribe",
            "subscriptionId": "live-pods",
            "payload": {
                "kind": "resource",
                "context": "k10s-limited",
                "gvk": {"group": "", "version": "v1", "kind": "Pod"},
                "namespace": "k10s-read"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Subscribed);
    let snapshot = consume_snapshot(&mut ws).await;
    assert_eq!(snapshot.len(), 2);
    let old_pod = snapshot[0].name.clone();

    kubectl(&[
        "-n",
        "k10s-read",
        "scale",
        "deployment/read-path-web",
        "--replicas=3",
    ]);
    let changed = receive_resource_event(&mut ws, "resource.changed").await;
    assert_eq!(changed["identity"]["context"], "k10s-limited");

    kubectl(&["-n", "k10s-read", "delete", "pod", &old_pod, "--wait=false"]);
    let gone = receive_resource_event(&mut ws, "resource.gone").await;
    assert_eq!(gone["identity"]["name"], old_pod);

    let cluster =
        std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".to_owned());
    assert!(
        Command::new("docker")
            .args(["restart", &format!("{cluster}-control-plane")])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if kubectl_status(&["get", "--raw=/readyz"]) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("the kind control plane recovers");
    let recovery = loop {
        let frame = receive_frame(&mut ws).await;
        if frame.kind == ServerKind::SnapshotBegin {
            break consume_snapshot_from_begin(&mut ws, frame).await;
        }
    };
    assert!(
        recovery.len() >= 2,
        "the same WebSocket subscription receives a recovery snapshot"
    );

    kubectl(&[
        "-n",
        "k10s-read",
        "scale",
        "deployment/read-path-web",
        "--replicas=2",
    ]);
}
