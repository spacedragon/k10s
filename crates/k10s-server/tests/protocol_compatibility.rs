//! Negotiated-minor compatibility coverage for structured resource projections.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, Command, FakeKubernetes, KubernetesAccess,
    OperationId, Query, QueryResult, StreamInput, Subscribe, SubscriptionHandle,
};
use k10s_protocol::{
    GroupVersionKind, ProtocolVersion, ResourceDetailResponse, ResourceIdentity,
    ResourceListResponse, ResourceMetricsResponse, ResourceProjection, ResourceRelationsResponse,
    ServerFrame, ServerKind, SnapshotChunk, SnapshotEnd, Welcome,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone)]
struct ProjectionFake {
    inner: FakeKubernetes,
}

impl ProjectionFake {
    fn standard() -> Self {
        Self {
            inner: FakeKubernetes::standard(),
        }
    }

    fn project(record: &mut k10s_backend::ResourceRecord) {
        use k10s_backend::port::{
            DeploymentProjection, PodProjection, ReplicaSetProjection, ResourceProjection,
        };

        record.projection = match record.reference.gvk.kind.as_str() {
            "Pod" => Some(ResourceProjection::Pod(PodProjection {
                phase: Some("Running".into()),
                ready_containers: Some(1),
                total_containers: Some(1),
                restart_count: Some(0),
                containers: Vec::new(),
                conditions: Vec::new(),
                node_name: Some("dev-node-1".into()),
                pod_ip: None,
                host_ip: None,
                qos_class: None,
                priority: None,
                service_account: None,
                restart_policy: Some("Always".into()),
                ports: Vec::new(),
                labels: record.labels.clone(),
                annotations: Default::default(),
                created_at: Some(record.created_at.clone()),
            })),
            "Deployment" => Some(ResourceProjection::Deployment(DeploymentProjection {
                desired_replicas: Some(2),
                ready_replicas: Some(2),
                updated_replicas: Some(2),
                available_replicas: Some(2),
                strategy: Some("RollingUpdate".into()),
                selector: record.labels.clone(),
                max_surge: Some("25%".into()),
                max_unavailable: Some("25%".into()),
                conditions: Vec::new(),
                template_containers: Vec::new(),
                template_labels: record.labels.clone(),
                template_annotations: Default::default(),
                labels: record.labels.clone(),
                annotations: Default::default(),
                created_at: Some(record.created_at.clone()),
            })),
            "ReplicaSet" => Some(ResourceProjection::ReplicaSet(ReplicaSetProjection {
                revision: 1,
                replicas: Some(2),
                ready_replicas: Some(2),
                created_at: Some(record.created_at.clone()),
                images: Vec::new(),
            })),
            _ => record.projection.clone(),
        };
    }

    fn project_result(result: &mut QueryResult) {
        match result {
            QueryResult::ResourceList(data) => data.rows.iter_mut().for_each(Self::project),
            QueryResult::ResourceDetail(record) => Self::project(record),
            QueryResult::ResourceRelations(data) => data
                .groups
                .iter_mut()
                .flat_map(|group| &mut group.records)
                .for_each(Self::project),
            QueryResult::ResourceMetrics(metrics) => {
                metrics
                    .containers
                    .push(k10s_backend::port::ContainerMetricsSample {
                        name: "postgres".into(),
                        cpu_millicores: Some(25),
                        memory_bytes: Some(64 * 1024 * 1024),
                    });
            }
            _ => {}
        }
    }

    fn project_event(event: &mut BackendEvent) {
        match event {
            BackendEvent::Snapshot(data) => data.rows.iter_mut().for_each(Self::project),
            BackendEvent::Changed(record) => Self::project(record),
            _ => {}
        }
    }
}

impl KubernetesAccess for ProjectionFake {
    fn query<'a>(
        &'a self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let mut result = self.inner.query(query).await?;
            Self::project_result(&mut result);
            Ok(result)
        })
    }

    fn execute<'a>(
        &'a self,
        command: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        self.inner.execute(command)
    }

    fn subscribe<'a>(
        &'a self,
        subscribe: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let mut handle = self.inner.subscribe(subscribe).await?;
            let Some(mut source) = handle.take_events() else {
                return Ok(handle);
            };
            let id = handle.id.clone();
            let (sender, receiver) = tokio::sync::broadcast::channel(64);
            tokio::spawn(async move {
                while let Ok(mut event) = source.recv().await {
                    Self::project_event(&mut event);
                    let _ = sender.send(event);
                }
            });
            Ok(SubscriptionHandle::with_events(id, receiver))
        })
    }

    fn stream_input<'a>(
        &'a self,
        ticket_id: &'a str,
        input: StreamInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        self.inner.stream_input(ticket_id, input)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum V12ResourceProjection {
    Service,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceRow {
    #[serde(default)]
    projection: Option<V12ResourceProjection>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceList {
    rows: Vec<V12ResourceRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceDetail {
    #[serde(default)]
    projection: Option<V12ResourceProjection>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12RelatedGroup {
    rows: Vec<V12ResourceRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceRelations {
    groups: Vec<V12RelatedGroup>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12SnapshotPage {
    rows: Vec<V12ResourceRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceChanged {
    row: V12ResourceRow,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V12ResourceMetrics {
    identity: ResourceIdentity,
    metrics: k10s_protocol::PodMetrics,
}

fn gvk(group: &str, kind: &str) -> GroupVersionKind {
    GroupVersionKind {
        group: group.into(),
        version: "v1".into(),
        kind: kind.into(),
    }
}

fn uid(kind: &str, name: &str) -> String {
    format!("uid-dev-local-{}-default-{name}", kind.to_lowercase())
}

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = ProjectionFake::standard();
    let control = fake.inner.clone();
    let server = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake, "protocol-compatibility-server"),
    )
    .await
    .unwrap();
    (server, control)
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn connect_with_minor(server: &k10s_server::ServerHandle, minor: u16) -> (Ws, Welcome) {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind": "hello",
            "payload": {
                "protocolMajor": 1,
                "protocolMinor": minor,
                "capabilities": [],
                "accessToken": "secret"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let welcome = receive_frame(&mut ws).await;
    assert_eq!(welcome.kind, ServerKind::Welcome);
    (ws, serde_json::from_value(welcome.payload).unwrap())
}

async fn request(ws: &mut Ws, request_id: &str, kind: &str, payload: Value) -> ServerFrame {
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

fn reference(kind: &str, name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: gvk(if kind == "Pod" { "" } else { "apps" }, kind),
        namespace: Some("default".into()),
        name: name.into(),
        uid: uid(kind, name),
    }
}

async fn request_list(ws: &mut Ws, request_id: &str, group: &str, kind: &str) -> ServerFrame {
    request(
        ws,
        request_id,
        "resource.list",
        json!({
            "context": "dev-local",
            "gvk": gvk(group, kind),
            "namespace": "default"
        }),
    )
    .await
}

async fn request_detail(ws: &mut Ws, request_id: &str, identity: ResourceIdentity) -> ServerFrame {
    request(
        ws,
        request_id,
        "resource.detail",
        json!({"identity": identity}),
    )
    .await
}

async fn request_relations(
    ws: &mut Ws,
    request_id: &str,
    identity: ResourceIdentity,
) -> ServerFrame {
    request(
        ws,
        request_id,
        k10s_protocol::REQUEST_RESOURCE_RELATIONS,
        json!({"identity": identity}),
    )
    .await
}

async fn subscribe_deployments(ws: &mut Ws, subscription_id: &str) {
    ws.send(Message::Text(
        json!({
            "kind": "subscribe",
            "subscriptionId": subscription_id,
            "payload": {
                "kind": "resource",
                "context": "dev-local",
                "gvk": gvk("apps", "Deployment"),
                "namespace": "default"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(ws).await.kind, ServerKind::Subscribed);
}

async fn receive_snapshot(ws: &mut Ws) -> (Vec<Value>, String) {
    let begin = receive_frame(ws).await;
    assert_eq!(begin.kind, ServerKind::SnapshotBegin);
    let total_chunks = begin.payload["totalChunks"].as_u64().unwrap();
    let mut pages = Vec::new();
    for _ in 0..total_chunks {
        let chunk = receive_frame(ws).await;
        assert_eq!(chunk.kind, ServerKind::SnapshotChunk);
        let chunk: SnapshotChunk = serde_json::from_value(chunk.payload).unwrap();
        pages.push(chunk.data);
    }
    let end = receive_frame(ws).await;
    assert_eq!(end.kind, ServerKind::SnapshotEnd);
    let end: SnapshotEnd = serde_json::from_value(end.payload).unwrap();
    (pages, end.checksum)
}

fn checksum(values: &[Value]) -> String {
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for value in values {
        for byte in serde_json::to_vec(value).unwrap() {
            checksum = (checksum ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("fnv-64:{checksum:016x}")
}

#[tokio::test]
async fn current_protocol_reports_minor_four_and_still_negotiates_older_minors() {
    assert_eq!(k10s_protocol::PROTOCOL_MINOR, 4);
    let (server, _fake) = spawn_server().await;

    let (_v12, welcome12) = connect_with_minor(&server, 2).await;
    assert_eq!(welcome12.protocol, ProtocolVersion { major: 1, minor: 2 });
    let (_v13, welcome13) = connect_with_minor(&server, 3).await;
    assert_eq!(welcome13.protocol, ProtocolVersion { major: 1, minor: 3 });
    let (_v14, welcome14) = connect_with_minor(&server, 4).await;
    assert_eq!(welcome14.protocol, ProtocolVersion { major: 1, minor: 4 });

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn v12_query_frames_strip_only_projection_variants_added_in_v13() {
    let (server, _fake) = spawn_server().await;
    let (mut ws, welcome) = connect_with_minor(&server, 2).await;
    assert_eq!(welcome.protocol.minor, 2);

    let list = request_list(&mut ws, "list", "apps", "Deployment").await;
    let list: V12ResourceList = serde_json::from_value(list.payload).unwrap();
    assert!(list.rows.iter().all(|row| row.projection.is_none()));

    let detail = request_detail(&mut ws, "detail", reference("Pod", "db-postgres-0")).await;
    let detail: V12ResourceDetail = serde_json::from_value(detail.payload).unwrap();
    assert!(detail.projection.is_none());

    let relations = request_relations(
        &mut ws,
        "relations",
        reference("Deployment", "web-frontend"),
    )
    .await;
    let relations: V12ResourceRelations = serde_json::from_value(relations.payload).unwrap();
    assert!(
        relations
            .groups
            .iter()
            .flat_map(|group| &group.rows)
            .all(|row| row.projection.is_none())
    );

    let services = request_list(&mut ws, "services", "", "Service").await;
    let services: V12ResourceList = serde_json::from_value(services.payload).unwrap();
    assert!(
        services
            .rows
            .iter()
            .all(|row| matches!(row.projection, Some(V12ResourceProjection::Service))),
        "the v1.2 Service projection remains available"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn v12_watch_snapshot_and_delta_decode_with_the_legacy_projection_enum() {
    let (server, fake) = spawn_server().await;
    let (mut ws, _) = connect_with_minor(&server, 2).await;
    subscribe_deployments(&mut ws, "legacy-watch").await;

    let (pages, advertised_checksum) = receive_snapshot(&mut ws).await;
    for page in &pages {
        let legacy_page: V12SnapshotPage = serde_json::from_value(page.clone()).unwrap();
        assert!(legacy_page.rows.iter().all(|row| row.projection.is_none()));
    }
    assert_eq!(advertised_checksum, checksum(&pages));

    fake.touch_resource(
        "dev-local",
        &k10s_backend::Gvk {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        Some("default"),
        "api-server",
    )
    .unwrap();
    let event = receive_frame(&mut ws).await;
    assert_eq!(event.kind, ServerKind::Event);
    let changed: V12ResourceChanged =
        serde_json::from_value(event.payload["payload"].clone()).unwrap();
    assert!(changed.row.projection.is_none());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn v13_queries_and_watches_receive_full_typed_projections() {
    let (server, fake) = spawn_server().await;
    let (mut ws, welcome) = connect_with_minor(&server, 3).await;
    assert_eq!(welcome.protocol.minor, 3);

    let list: ResourceListResponse = request_list(&mut ws, "list", "apps", "Deployment")
        .await
        .decode_response_payload()
        .unwrap();
    assert!(
        list.rows
            .iter()
            .all(|row| matches!(row.projection, Some(ResourceProjection::Deployment(_))))
    );

    let detail: ResourceDetailResponse =
        request_detail(&mut ws, "detail", reference("Pod", "db-postgres-0"))
            .await
            .decode_response_payload()
            .unwrap();
    assert!(matches!(
        detail.projection,
        Some(ResourceProjection::Pod(_))
    ));

    let deployment_detail: ResourceDetailResponse = request_detail(
        &mut ws,
        "deployment-detail",
        reference("Deployment", "web-frontend"),
    )
    .await
    .decode_response_payload()
    .unwrap();
    assert!(matches!(
        deployment_detail.projection,
        Some(ResourceProjection::Deployment(_))
    ));
    assert!(deployment_detail.capabilities.can_restart);

    let metrics: ResourceMetricsResponse = request(
        &mut ws,
        "metrics",
        "resource.metrics",
        json!({"identity": reference("Pod", "db-postgres-0")}),
    )
    .await
    .decode_response_payload()
    .unwrap();
    assert_eq!(
        metrics
            .containers
            .iter()
            .map(|container| container.name.as_str())
            .collect::<Vec<_>>(),
        vec!["postgres"]
    );

    let relations: ResourceRelationsResponse = request_relations(
        &mut ws,
        "relations",
        reference("Deployment", "web-frontend"),
    )
    .await
    .decode_response_payload()
    .unwrap();
    assert!(
        relations
            .groups
            .iter()
            .flat_map(|group| &group.rows)
            .any(|row| matches!(row.projection, Some(ResourceProjection::ReplicaSet(_))))
    );
    assert!(
        relations
            .groups
            .iter()
            .flat_map(|group| &group.rows)
            .any(|row| matches!(row.projection, Some(ResourceProjection::Pod(_))))
    );

    subscribe_deployments(&mut ws, "current-watch").await;
    let (pages, advertised_checksum) = receive_snapshot(&mut ws).await;
    for page in &pages {
        let page: k10s_protocol::ResourceSnapshotPage =
            serde_json::from_value(page.clone()).unwrap();
        assert!(
            page.rows
                .iter()
                .all(|row| matches!(row.projection, Some(ResourceProjection::Deployment(_))))
        );
    }
    assert_eq!(advertised_checksum, checksum(&pages));

    fake.touch_resource(
        "dev-local",
        &k10s_backend::Gvk {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        Some("default"),
        "api-server",
    )
    .unwrap();
    let event = receive_frame(&mut ws).await;
    let changed: k10s_protocol::ResourceChanged =
        serde_json::from_value(event.payload["payload"].clone()).unwrap();
    assert!(matches!(
        changed.row.projection,
        Some(ResourceProjection::Deployment(_))
    ));

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn v12_metrics_model_ignores_v13_container_samples() {
    let (server, _fake) = spawn_server().await;
    let (mut ws, _) = connect_with_minor(&server, 2).await;
    let identity = reference("Pod", "db-postgres-0");
    let frame = request(
        &mut ws,
        "metrics",
        "resource.metrics",
        json!({"identity": identity}),
    )
    .await;

    let current: ResourceMetricsResponse = serde_json::from_value(frame.payload.clone()).unwrap();
    assert!(frame.payload.get("containers").is_some());
    assert_eq!(current.containers[0].name, "postgres");
    let legacy: V12ResourceMetrics = serde_json::from_value(frame.payload).unwrap();
    assert_eq!(legacy.identity.name, "db-postgres-0");
    assert_eq!(legacy.metrics, current.metrics);

    server.shutdown().await.unwrap();
}
