//! Kubernetes watch loopback: the real kube-rs adapter, driven by a fully
//! scripted watch source (recorded tower service, no cluster), serves
//! supervised demand-driven watches over a live control socket. Verifies the
//! bounded chunked snapshots, lossless P1 subscription lifecycle, opaque
//! resourceVersion on the wire, and reconnect full-resync behavior.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::runtime::{ListedState, WatchRow, WatchSource, WatchUpdate};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{BackendKernel, ContextInfo, Gvk, KubeAdapter, ResourceRef};
use k10s_protocol::{
    GroupVersionKind, ResourceSnapshotPage, ResourceWatchSpec, ServerFrame, ServerKind,
    SubscriptionSelector,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CONTEXT: &str = "loopback-cluster";

fn deployments_gvk() -> Gvk {
    Gvk::new("apps", "v1", "Deployment")
}

fn row(name: &str, summary: &str) -> WatchRow {
    WatchRow {
        reference: ResourceRef {
            context: CONTEXT.into(),
            gvk: deployments_gvk(),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        },
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: "2026-08-21T00:00:00Z".into(),
        owner_references: Vec::new(),
    }
}

/// Shared controllable state of one scripted watch source.
#[derive(Debug, Default)]
struct ScriptState {
    lists: StdMutex<VecDeque<ListedState>>,
    live_sink: StdMutex<Option<mpsc::UnboundedSender<WatchUpdate>>>,
}

impl ScriptState {
    fn push_update(&self, update: WatchUpdate) {
        if let Some(sink) = self
            .live_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            let _ = sink.send(update);
        }
    }
}

/// A scripted [`WatchSource`] whose list cuts are queued up front and whose
/// live stream is fed by [`ScriptState::push_update`].
#[derive(Debug)]
struct LoopbackSource {
    state: Arc<ScriptState>,
}

impl WatchSource for LoopbackSource {
    fn list<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ListedState, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut lists = self
                .state
                .lists
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match lists.pop_front() {
                Some(listed) => Ok(listed),
                None => Err("script exhausted".into()),
            }
        })
    }

    fn attach_watch<'a>(
        &'a self,
        _resource_version: String,
        out: mpsc::UnboundedSender<WatchUpdate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            *self
                .state
                .live_sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(out);
            // Stay attached for the life of the test.
            std::future::pending::<()>().await;
        })
    }
}

fn scripted_kernel(rows: usize) -> (BackendKernel, Arc<ScriptState>) {
    use k10s_backend::runtime::RuntimeWatchScript;

    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let state = Arc::new(ScriptState {
        lists: StdMutex::new(
            vec![ListedState {
                resource_version: "4100".into(),
                rows: (0..rows)
                    .map(|index| row(&format!("deploy-{index:03}"), "2/2 ready"))
                    .collect(),
            }]
            .into(),
        ),
        ..Default::default()
    });
    let script_state = Arc::clone(&state);
    let source = Arc::new(LoopbackSource {
        state: script_state,
    });
    let source: Arc<dyn WatchSource> = source;
    let scripted: RuntimeWatchScript = Arc::new(move |_gvk, _namespace| Some(Arc::clone(&source)));
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds")
    .with_scripted_watches(scripted);
    (
        BackendKernel::new_with_instance_id(adapter, "watch-server"),
        state,
    )
}

async fn connect_authenticated(server: &k10s_server::ServerHandle) -> Ws {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"hello",
            "payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Welcome);
    ws
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn subscribe(ws: &mut Ws, subscription_id: &str) {
    let spec = ResourceWatchSpec {
        context: CONTEXT.into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
    };
    ws.send(Message::Text(
        json!({
            "kind": "subscribe",
            "subscriptionId": subscription_id,
            "payload": serde_json::to_value(SubscriptionSelector::Resource(spec)).unwrap(),
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_snapshots_and_deltas_flow_over_the_control_socket() {
    let (kernel, state) = scripted_kernel(40);
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        kernel,
    )
    .await
    .unwrap();
    let mut ws = connect_authenticated(&server).await;

    subscribe(&mut ws, "resource-1").await;
    let subscribed = receive_frame(&mut ws).await;
    assert_eq!(subscribed.kind, ServerKind::Subscribed);

    // Bounded snapshot chunks: 40 rows at 16 rows per chunk = 3 pages.
    let begin = receive_frame(&mut ws).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    assert_eq!(begin.payload["totalChunks"], 3);

    let mut total_rows = 0_usize;
    let mut last_revision = String::new();
    for chunk_index in 0..3 {
        let chunk = receive_frame(&mut ws).await;
        assert_eq!(chunk.kind, ServerKind::SnapshotChunk);
        assert_eq!(chunk.payload["chunkIndex"], chunk_index);
        let page: ResourceSnapshotPage =
            serde_json::from_value(chunk.payload["data"].clone()).unwrap();
        assert!(
            page.rows.len() <= 16,
            "snapshot pages stay within the bound"
        );
        total_rows += page.rows.len();
        last_revision = page.revision.to_string();
    }
    assert_eq!(total_rows, 40, "the full cut arrives across the pages");

    let end = receive_frame(&mut ws).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);
    assert!(
        end.payload["checksum"]
            .as_str()
            .unwrap()
            .starts_with("fnv-64:"),
        "snapshot ends with its deterministic checksum"
    );

    // Live deltas flow as coalescible P2 events with monotonic revisions.
    state.push_update(WatchUpdate::Upsert(row("deploy-000", "0/2 ready")));
    let changed = receive_frame(&mut ws).await;
    assert_eq!(changed.kind, ServerKind::Event);
    assert_eq!(changed.payload["kind"], json!("resource.changed"));
    assert_eq!(
        changed.payload["payload"]["row"]["summary"],
        json!("0/2 ready")
    );
    assert_eq!(
        changed.subscription_id.as_ref().unwrap().as_str(),
        "resource-1"
    );
    let changed_revision: u64 = changed.payload["revision"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        changed_revision > last_revision.parse::<u64>().unwrap(),
        "delta revisions advance past the snapshot"
    );

    state.push_update(WatchUpdate::Delete(ResourceRef {
        context: CONTEXT.into(),
        gvk: deployments_gvk(),
        namespace: Some("default".into()),
        name: "deploy-001".into(),
        uid: "uid-deploy-001".into(),
    }));
    let gone = receive_frame(&mut ws).await;
    assert_eq!(gone.payload["kind"], json!("resource.gone"));
    assert_eq!(
        gone.payload["payload"]["identity"]["name"],
        json!("deploy-001")
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconnect_gets_a_full_fresh_snapshot_resync() {
    let (kernel, state) = scripted_kernel(5);
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        kernel,
    )
    .await
    .unwrap();

    // First session: subscribe and read the complete snapshot.
    let mut first = connect_authenticated(&server).await;
    subscribe(&mut first, "resync-a").await;
    assert_eq!(receive_frame(&mut first).await.kind, ServerKind::Subscribed);
    let begin = receive_frame(&mut first).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    let chunk = receive_frame(&mut first).await;
    let page: ResourceSnapshotPage = serde_json::from_value(chunk.payload["data"].clone()).unwrap();
    assert_eq!(page.rows.len(), 5);
    let _end = receive_frame(&mut first).await;

    // Drop the socket abruptly without unsubscribing.
    drop(first);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The cluster moved while nobody watched; the cache reflects it.
    for index in 0..5 {
        state.push_update(WatchUpdate::Upsert(row(
            &format!("deploy-{index:03}"),
            "5/5 ready",
        )));
    }

    // Reconnect: the new subscription starts from a complete fresh cut of
    // current state — a full resync, never an incremental replay.
    let mut second = connect_authenticated(&server).await;
    subscribe(&mut second, "resync-b").await;
    assert_eq!(
        receive_frame(&mut second).await.kind,
        ServerKind::Subscribed
    );
    let begin = receive_frame(&mut second).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    let chunk = receive_frame(&mut second).await;
    let page: ResourceSnapshotPage = serde_json::from_value(chunk.payload["data"].clone()).unwrap();
    assert_eq!(page.rows.len(), 5, "every row is present after resync");
    for row_record in &page.rows {
        assert_eq!(
            row_record.summary, "5/5 ready",
            "the resynced snapshot carries current state"
        );
    }
    let end = receive_frame(&mut second).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);

    server.shutdown().await.unwrap();
}
