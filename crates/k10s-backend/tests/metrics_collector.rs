//! Resource Metrics API collector tests for the real adapter.
//!
//! Drives the `metrics.k8s.io/v1beta1` poller through a real kube-rs client
//! against a recorded tower-level API server (no live cluster) and asserts
//! the documented behavior: discovered/absent/forbidden Metrics API states,
//! full and partial NodeMetrics coverage, stale timestamps, PodMetrics
//! container sums, consumer-driven poll start/linger/stop, and pod capacity
//! derived from core Node allocatable — never from metrics data, and never
//! mapped onto false zeroes.

use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k10s_backend::runtime::{
    ClusterMetrics, MetricsApiState, MetricsCoverage, MetricsPollSource, MetricsSnapshot,
};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, BackendKernel, ContextInfo, Gvk, KernelQueryResult, KubeAdapter, Query,
    ResourceRef,
};
use serde_json::{Value, json};
use tokio::sync::oneshot;

const CONTEXT: &str = "metrics-mock";
const NS: &str = "default";

// Recorded request paths.
const CORE_NODES: &str = "/api/v1/nodes";
const NODE_METRICS: &str = "/apis/metrics.k8s.io/v1beta1/nodes";
const POD_METRICS: &str = "/apis/metrics.k8s.io/v1beta1/pods";

fn pods_gvk() -> Gvk {
    Gvk::core("v1", "Pod")
}

fn pod_ref(name: &str, uid: &str) -> ResourceRef {
    ResourceRef {
        context: CONTEXT.into(),
        gvk: pods_gvk(),
        namespace: Some(NS.into()),
        name: name.into(),
        uid: uid.into(),
    }
}

/// One adapter around a fresh recorded server sharing the standard discovery
/// surface, tuned with short metrics timings so lifecycle tests stay fast.
fn adapter_with_timing(
    server: &RecordedApiServer,
    linger: Duration,
    poll: Duration,
) -> KubeAdapter {
    let client = server.clone().into_client(NS);
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some(NS.into()),
            is_current: true,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds around the recorded server")
    .with_metrics_timing(linger, poll)
}

/// The kernel under test plus a shared handle to its metrics registry.
struct World {
    kernel: BackendKernel,
    metrics: ClusterMetrics,
}

/// The standard test world: short linger, one background cycle per minute.
fn world(server: &RecordedApiServer) -> World {
    timed_world(server, Duration::from_millis(300), Duration::from_secs(60))
}

fn timed_world(server: &RecordedApiServer, linger: Duration, poll: Duration) -> World {
    let adapter = adapter_with_timing(server, linger, poll);
    let metrics = adapter.metrics_registry();
    World {
        kernel: BackendKernel::new(adapter),
        metrics,
    }
}

/// RFC 3339 UTC timestamp `age_secs` seconds before the test ran, so samples
/// can be crafted fresh relative to real wall-clock time.
fn recent_rfc3339(age_secs: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock advances")
        .as_secs();
    rfc3339_unix(now.saturating_sub(age_secs))
}

/// Format unix seconds as an RFC 3339 UTC timestamp without external crates.
fn rfc3339_unix(unix_secs: u64) -> String {
    let days = unix_secs / 86_400;
    let secs_of_day = unix_secs % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60
    )
}

/// Howard Hinnant's days-to-civil conversion for UTC dates after 1970.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year =
        day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100 + year_of_era / 400);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (year, month as u32, day as u32)
}

/// Two core nodes whose allocatable pod capacities sum to 330.
fn record_core_nodes(server: &RecordedApiServer) {
    server.set_response(
        CORE_NODES,
        200,
        &json!({
            "kind": "NodeList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "100"},
            "items": [
                {
                    "metadata": {"name": "node-a", "uid": "uid-node-a", "creationTimestamp": "2026-08-01T00:00:00Z"},
                    "status": {"allocatable": {"pods": "110", "cpu": "4", "memory": "16Gi"}}
                },
                {
                    "metadata": {"name": "node-b", "uid": "uid-node-b", "creationTimestamp": "2026-08-01T00:00:00Z"},
                    "status": {"allocatable": {"pods": "220", "cpu": "4", "memory": "16Gi"}}
                }
            ]
        })
        .to_string(),
    );
}

/// NodeMetrics for exactly the given node names, sampled at `timestamp`.
fn record_node_metrics(server: &RecordedApiServer, names: &[&str], timestamp: &str) {
    let items: Vec<Value> = names
        .iter()
        .map(|name| {
            json!({
                "metadata": {"name": name, "creationTimestamp": timestamp},
                "timestamp": timestamp,
                "window": "30s",
                "usage": {"cpu": "1250m", "memory": "123456Ki"}
            })
        })
        .collect();
    server.set_response(
        NODE_METRICS,
        200,
        &json!({
            "kind": "NodeMetricsList",
            "apiVersion": "metrics.k8s.io/v1beta1",
            "metadata": {},
            "items": items
        })
        .to_string(),
    );
}

/// One PodMetrics item with the given container usage list.
fn pod_metric_item(name: &str, timestamp: &str, containers: Value) -> Value {
    json!({
        "metadata": {"name": name, "namespace": NS, "creationTimestamp": timestamp},
        "timestamp": timestamp,
        "window": "30s",
        "containers": containers
    })
}

/// Record PodMetrics for the given items under one list cut.
fn record_pod_metrics(server: &RecordedApiServer, items: Vec<Value>) {
    server.set_response(
        POD_METRICS,
        200,
        &json!({
            "kind": "PodMetricsList",
            "apiVersion": "metrics.k8s.io/v1beta1",
            "metadata": {},
            "items": items
        })
        .to_string(),
    );
}

/// Record an existing pod object so exact-identity verification resolves.
fn record_pod(server: &RecordedApiServer, name: &str) {
    server.set_response(
        &format!("/api/v1/namespaces/{NS}/pods/{name}"),
        200,
        &json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": name,
                "namespace": NS,
                "uid": format!("uid-{name}"),
                "resourceVersion": "41",
                "creationTimestamp": "2026-08-21T00:00:00Z",
            },
            "status": {"phase": "Running"},
        })
        .to_string(),
    );
}

async fn metrics_wire(kernel: &BackendKernel, reference: ResourceRef) -> Value {
    match kernel.query(Query::ResourceMetrics { reference }).await {
        Ok(KernelQueryResult::ResourceMetrics(result)) => {
            serde_json::to_value(result.wire_payload()).expect("payload serializes")
        }
        Ok(other) => panic!("kernel must map metrics into its wire payload, got {other:?}"),
        Err(error) => panic!("metrics query failed: {error}"),
    }
}

fn probe_hits(server: &RecordedApiServer) -> usize {
    server.hit_count(NODE_METRICS) + server.hit_count(POD_METRICS) + server.hit_count(CORE_NODES)
}

#[tokio::test]
async fn absent_metrics_api_reports_unavailability_without_zeroes() {
    let server = RecordedApiServer::standard();
    record_core_nodes(&server);
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    // Absence is typed, never zero-filled: no value or timestamp keys exist.
    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(
        metrics.keys().cloned().collect::<Vec<_>>(),
        vec!["availability"],
        "an absent sample carries no fabricated values: {metrics:?}"
    );
    assert_eq!(payload["metrics"]["availability"], "unavailable");
    assert!(
        server.hit_count(NODE_METRICS) >= 1,
        "the absent API was probed"
    );

    let snapshot = world.metrics.snapshot_of(CONTEXT);
    assert_eq!(
        snapshot.as_deref().map(|snapshot| snapshot.state),
        Some(MetricsApiState::Absent),
        "a 404 from the metrics API means the API is absent, not empty"
    );
}

#[tokio::test]
async fn forbidden_metrics_api_reports_unavailability_without_zeroes() {
    let server = RecordedApiServer::standard();
    server.set_response(
        NODE_METRICS,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden","reason":"Forbidden","code":403}"#,
    );
    record_core_nodes(&server);
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(
        metrics.keys().cloned().collect::<Vec<_>>(),
        vec!["availability"],
        "RBAC denial must never fabricate values: {metrics:?}"
    );
    assert_eq!(payload["metrics"]["availability"], "unavailable");

    let snapshot = world.metrics.snapshot_of(CONTEXT);
    assert_eq!(
        snapshot.as_deref().map(|snapshot| snapshot.state),
        Some(MetricsApiState::Forbidden),
    );
}

#[tokio::test]
async fn full_coverage_collects_usage_and_allocatable_pod_capacity() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], &timestamp);
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "web",
            &timestamp,
            json!([
                {"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}},
                {"name": "sidecar", "usage": {"cpu": "150m", "memory": "2Mi"}}
            ]),
        )],
    );
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    // Container usages sum; the source timestamp travels unchanged.
    assert_eq!(payload["metrics"]["availability"], "available");
    assert_eq!(payload["metrics"]["cpuMillicores"], 250);
    assert_eq!(payload["metrics"]["memoryBytes"], 3_145_728);
    assert_eq!(
        payload["metrics"]["collectedAt"],
        json!(timestamp),
        "the source-reported timestamp is carried through verbatim"
    );

    let snapshot = world
        .metrics
        .snapshot_of(CONTEXT)
        .expect("the collector cached its cut");
    assert_eq!(snapshot.state, MetricsApiState::Ready);
    assert_eq!(snapshot.window_seconds, Some(30));
    assert_eq!(snapshot.node_usage.len(), 2);
    assert_eq!(snapshot.node_coverage(), MetricsCoverage::Full);
    // Pod capacity comes from core Node allocatable, never from metrics data.
    assert_eq!(snapshot.pod_capacity_total, Some(330));
    let node_a = &snapshot.node_usage["node-a"];
    assert_eq!(node_a.cpu_millicores, Some(1250));
    assert_eq!(node_a.memory_bytes, Some(123456 * 1024));
}

#[tokio::test]
async fn partial_node_coverage_is_reported_honestly() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    // Only node-a reported; node-b exists but has no metrics cut.
    record_node_metrics(&server, &["node-a"], &timestamp);
    record_pod_metrics(&server, Vec::new());
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let _ = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    let snapshot = world
        .metrics
        .snapshot_of(CONTEXT)
        .expect("the collector cached its cut");
    assert_eq!(snapshot.node_names.len(), 2);
    assert_eq!(snapshot.node_usage.len(), 1);
    assert_eq!(snapshot.node_coverage(), MetricsCoverage::Partial);
    assert!(
        !snapshot.node_usage.contains_key("node-b"),
        "a node without metrics stays absent — never zero-filled"
    );
    assert_eq!(snapshot.pod_capacity_total, Some(330));
}

#[tokio::test]
async fn coverage_requires_every_core_node_not_equal_counts() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    // node-c was just removed from the cluster yet still reports metrics;
    // equal counts ({a,b} core vs {a,c} cut) must not read as full coverage.
    record_node_metrics(&server, &["node-a", "node-c"], &timestamp);
    record_pod_metrics(&server, Vec::new());
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let _ = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    let snapshot = world
        .metrics
        .snapshot_of(CONTEXT)
        .expect("the collector cached its cut");
    assert_eq!(snapshot.node_usage.len(), 2);
    assert!(snapshot.node_names.contains(&"node-b".to_owned()));
    assert!(snapshot.node_usage.contains_key("node-c"));
    assert_eq!(
        snapshot.node_coverage(),
        MetricsCoverage::Partial,
        "node-b never reported, so coverage stays partial despite equal counts"
    );
}

#[tokio::test]
async fn stale_source_timestamps_are_never_served_as_values() {
    let server = RecordedApiServer::standard();
    const ANCIENT: &str = "2020-01-01T00:00:00Z";
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], ANCIENT);
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "web",
            ANCIENT,
            json!([{"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}}]),
        )],
    );
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("web", "uid-web")).await;

    // Stale values are withheld, but the last-known collection time stays
    // visible so the UI can show age instead of pretending freshness.
    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(
        metrics
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["availability", "collectedAt"]
            .map(str::to_owned)
            .into_iter()
            .collect(),
        "stale samples expose their age, never stale numbers: {metrics:?}"
    );
    assert_eq!(payload["metrics"]["availability"], "unavailable");
    assert_eq!(payload["metrics"]["collectedAt"], ANCIENT);

    let snapshot = world
        .metrics
        .snapshot_of(CONTEXT)
        .expect("the collector cached its cut");
    assert_eq!(snapshot.source_updated_at.as_deref(), Some(ANCIENT));
}

#[tokio::test]
async fn per_pod_freshness_gates_each_sample_by_its_own_timestamp() {
    let server = RecordedApiServer::standard();
    const ANCIENT: &str = "2020-01-01T00:00:00Z";
    let fresh = recent_rfc3339(10);
    record_core_nodes(&server);
    // The node cut and pod "live" are fresh; "web" predates the freshness
    // window and "garbled" carries no parseable timestamp at all.
    record_node_metrics(&server, &["node-a", "node-b"], &fresh);
    record_pod_metrics(
        &server,
        vec![
            pod_metric_item(
                "web",
                ANCIENT,
                json!([{"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}}]),
            ),
            pod_metric_item(
                "live",
                &fresh,
                json!([{"name": "app", "usage": {"cpu": "200m", "memory": "2Mi"}}]),
            ),
            // A valid object timestamp whose sample `timestamp` is garbage.
            {
                let mut item = pod_metric_item(
                    "garbled",
                    &fresh,
                    json!([{"name": "app", "usage": {"cpu": "300m", "memory": "3Mi"}}]),
                );
                item["timestamp"] = json!("not-a-time");
                item
            },
        ],
    );
    record_pod(&server, "web");
    record_pod(&server, "live");
    record_pod(&server, "garbled");
    let world = world(&server);
    let kernel = &world.kernel;

    let web = metrics_wire(kernel, pod_ref("web", "uid-web")).await;
    let live = metrics_wire(kernel, pod_ref("live", "uid-live")).await;
    let garbled = metrics_wire(kernel, pod_ref("garbled", "uid-garbled")).await;

    // A fresh sibling item must never vouch for this pod's ancient sample.
    let web_metrics = web["metrics"].as_object().expect("metrics is an object");
    assert_eq!(
        web_metrics
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["availability", "collectedAt"]
            .map(str::to_owned)
            .into_iter()
            .collect(),
        "the stale pod keeps only its own age, never fresh-vouched numbers: {web_metrics:?}"
    );
    assert_eq!(web["metrics"]["availability"], "unavailable");
    assert_eq!(web["metrics"]["collectedAt"], ANCIENT);

    // An unparseable per-pod timestamp fails closed to plain absence.
    assert_eq!(
        garbled["metrics"]
            .as_object()
            .expect("metrics is an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["availability"],
        "a pod without a parseable timestamp has nothing honest to show: {garbled:?}"
    );
    assert_eq!(garbled["metrics"]["availability"], "unavailable");

    // The genuinely fresh pod next door is still served normally.
    assert_eq!(live["metrics"]["availability"], "available");
    assert_eq!(live["metrics"]["cpuMillicores"], 200);
    assert_eq!(live["metrics"]["collectedAt"], json!(fresh));

    // Shared cut metadata stayed fresh — per-sample gating did the work.
    let snapshot = world
        .metrics
        .snapshot_of(CONTEXT)
        .expect("the collector cached its cut");
    assert_eq!(snapshot.source_updated_at.as_deref(), Some(fresh.as_str()));
}

#[tokio::test]
async fn partially_reported_pods_map_to_partial_wire_availability() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], &timestamp);
    // This container reported CPU but no memory.
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "half",
            &timestamp,
            json!([{"name": "app", "usage": {"cpu": "90m"}}]),
        )],
    );
    record_pod(&server, "half");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("half", "uid-half")).await;

    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(metrics["availability"], "partial");
    assert_eq!(metrics["cpuMillicores"], 90);
    assert!(
        !metrics.contains_key("memoryBytes"),
        "missing memory stays absent instead of collapsing to zero: {metrics:?}"
    );
}

#[tokio::test]
async fn pod_aggregates_fail_closed_when_a_container_omits_a_field() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], &timestamp);
    // "sidecar" reports memory but omits CPU entirely; skipping its missing
    // CPU contribution would fabricate a 100m total.
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "mixed",
            &timestamp,
            json!([
                {"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}},
                {"name": "sidecar", "usage": {"memory": "2Mi"}}
            ]),
        )],
    );
    record_pod(&server, "mixed");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("mixed", "uid-mixed")).await;

    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(
        metrics
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["availability", "collectedAt", "memoryBytes"]
            .map(str::to_owned)
            .into_iter()
            .collect(),
        "an incomplete CPU total stays absent even though memory completed: {metrics:?}"
    );
    assert_eq!(
        metrics["availability"], "partial",
        "an incomplete field must never read as fully available"
    );
    assert_eq!(metrics["memoryBytes"], 3_145_728);
}

#[tokio::test]
async fn unmetered_existing_pods_stay_absent_without_zeroes() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(10);
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], &timestamp);
    // Only "web" appears in the PodMetrics cut; "idle" exists but is unmetered.
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "web",
            &timestamp,
            json!([{"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}}]),
        )],
    );
    record_pod(&server, "web");
    record_pod(&server, "idle");
    let world = world(&server);
    let kernel = &world.kernel;

    let payload = metrics_wire(kernel, pod_ref("idle", "uid-idle")).await;

    let metrics = payload["metrics"]
        .as_object()
        .expect("metrics is an object");
    assert_eq!(
        metrics.keys().cloned().collect::<Vec<_>>(),
        vec!["availability"],
        "an existing pod without metrics stays absent — never zero-filled: {metrics:?}"
    );
    assert_eq!(payload["metrics"]["availability"], "unavailable");
}

#[tokio::test]
async fn unknown_or_nonpod_references_are_typed_not_founds() {
    let server = RecordedApiServer::standard();
    record_pod(&server, "web");
    let world = world(&server);
    let kernel = &world.kernel;

    // A vanished pod.
    let vanished = kernel
        .query(Query::ResourceMetrics {
            reference: pod_ref("ghost", "uid-ghost"),
        })
        .await
        .expect_err("unknown pods are not found");
    assert_eq!(vanished, BackendError::NotFound);

    // A reused name carrying a foreign UID never resolves.
    let stale_uid = kernel
        .query(Query::ResourceMetrics {
            reference: pod_ref("web", "uid-from-a-past-life"),
        })
        .await
        .expect_err("stale identities are not found");
    assert_eq!(stale_uid, BackendError::NotFound);

    // Metrics exist only for pods.
    let nonpod = kernel
        .query(Query::ResourceMetrics {
            reference: ResourceRef {
                context: CONTEXT.into(),
                gvk: Gvk::new("apps", "v1", "Deployment"),
                namespace: Some(NS.into()),
                name: "web".into(),
                uid: "uid-web-deploy".into(),
            },
        })
        .await
        .expect_err("non-pod references have no metrics identity");
    assert_eq!(nonpod, BackendError::NotFound);
}

/// Consumers drive the poll lifecycle: nothing polls without a consumer, the
/// first query starts exactly one collector, warm consumers share it without
/// extra cycles, and after the last consumer lingers away the collector exits.
#[tokio::test]
async fn polling_starts_shares_and_lingers_with_consumers() {
    let server = RecordedApiServer::standard();
    let timestamp = recent_rfc3339(0);
    record_core_nodes(&server);
    record_node_metrics(&server, &["node-a", "node-b"], &timestamp);
    record_pod_metrics(
        &server,
        vec![pod_metric_item(
            "web",
            &timestamp,
            json!([{"name": "app", "usage": {"cpu": "100m", "memory": "1Mi"}}]),
        )],
    );
    record_pod(&server, "web");
    // Linger well above the assertion spacing, poll cadence far below.
    let linger = Duration::from_millis(400);
    let world = timed_world(&server, linger, Duration::from_secs(60));
    let kernel = &world.kernel;

    // No consumers yet: no polling at all.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        probe_hits(&server),
        0,
        "nothing polls without an active consumer"
    );
    assert_eq!(world.metrics.live_collectors(), 0);

    // First consumer: one collector starts and completes one full cycle.
    let _ = metrics_wire(kernel, pod_ref("web", "uid-web")).await;
    assert_eq!(probe_hits(&server), 3, "exactly one poll cycle ran");
    assert_eq!(world.metrics.live_collectors(), 1);

    // A second immediate consumer joins the warm collector: no new cycle.
    let _ = metrics_wire(kernel, pod_ref("web", "uid-web")).await;
    assert_eq!(
        probe_hits(&server),
        3,
        "warm consumers are served from the shared cache"
    );
    assert_eq!(world.metrics.live_collectors(), 1);

    // After the last consumer lingers away the collector exits quietly.
    tokio::time::sleep(linger + Duration::from_millis(300)).await;
    let plateau_a = probe_hits(&server);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let plateau_b = probe_hits(&server);
    assert_eq!(plateau_a, plateau_b, "no polling continues after linger");
    assert_eq!(world.metrics.live_collectors(), 0);

    // A returning consumer starts a fresh collector with a new cycle.
    let _ = metrics_wire(kernel, pod_ref("web", "uid-web")).await;
    assert_eq!(world.metrics.live_collectors(), 1);
    assert!(
        probe_hits(&server) >= plateau_b + 3,
        "a returning consumer restarts collection"
    );
}

// --- Runtime lifecycle: the demand-driven collector registry itself. ---

#[derive(Debug)]
struct ScriptedSource {
    polls: AtomicUsize,
    gate: StdMutex<Option<oneshot::Receiver<()>>>,
}

impl ScriptedSource {
    fn gated(gate: oneshot::Receiver<()>) -> Self {
        Self {
            polls: AtomicUsize::new(0),
            gate: StdMutex::new(Some(gate)),
        }
    }

    fn free() -> Self {
        Self {
            polls: AtomicUsize::new(0),
            gate: StdMutex::new(None),
        }
    }
}

impl MetricsPollSource for ScriptedSource {
    fn poll(
        &'_ self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = MetricsSnapshot> + std::marker::Send + '_>,
    > {
        Box::pin(async move {
            self.polls.fetch_add(1, Ordering::SeqCst);
            // Take the gate out of the lock before awaiting it: the guard
            // must never be held across an await.
            let gate = self
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            MetricsSnapshot {
                context: CONTEXT.into(),
                collected_at: "2026-08-21T00:00:00Z".into(),
                source_updated_at: None,
                window_seconds: None,
                state: MetricsApiState::Ready,
                node_usage: Default::default(),
                pod_usage: Default::default(),
                node_names: Vec::new(),
                pod_capacity_total: None,
            }
        })
    }
}

#[tokio::test]
async fn registry_starts_on_first_consumer_and_shares_the_cycle() {
    let registry = ClusterMetrics::new(Duration::from_millis(500), Duration::from_secs(60));
    let (gate_tx, gate_rx) = oneshot::channel();
    let source = Arc::new(ScriptedSource::gated(gate_rx));

    let first = {
        let source = Arc::clone(&source);
        let registry = registry.clone();
        tokio::spawn(async move { registry.collect_for_consumer(CONTEXT, move || source).await })
    };
    // Let the first consumer register and enter its blocked poll.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // A second consumer joins the warm collector while the first cycle is
    // still in flight; it must not trigger another poll.
    let second = {
        let source = Arc::clone(&source);
        let registry = registry.clone();
        tokio::spawn(async move { registry.collect_for_consumer(CONTEXT, move || source).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = gate_tx.send(());

    let first = first.await.expect("task joins").expect("snapshot ready");
    let second = second.await.expect("task joins").expect("snapshot ready");
    assert!(Arc::ptr_eq(&first, &second), "warm consumers share one cut");
    assert_eq!(source.polls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.live_collectors(), 1);
}

#[tokio::test]
async fn registry_exits_after_idle_linger_and_restarts_on_return() {
    let registry = ClusterMetrics::new(Duration::from_millis(150), Duration::from_secs(60));
    let source = Arc::new(ScriptedSource::free());

    {
        let source = Arc::clone(&source);
        let snapshot = registry
            .collect_for_consumer(CONTEXT, move || source)
            .await
            .expect("first cycle completes");
        assert_eq!(snapshot.context, CONTEXT);
    }
    assert_eq!(registry.live_collectors(), 1);

    // No further consumers: the collector exits after one idle linger.
    for _ in 0..200 {
        if registry.live_collectors() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(registry.live_collectors(), 0, "idle collectors exit");
    assert_eq!(source.polls.load(Ordering::SeqCst), 1);

    // A returning consumer spawns a new generation.
    let replacement = Arc::new(ScriptedSource::free());
    let factory_source = Arc::clone(&replacement);
    let snapshot = registry
        .collect_for_consumer(CONTEXT, move || factory_source)
        .await
        .expect("restart completes");
    assert_eq!(snapshot.context, CONTEXT);
    assert_eq!(replacement.polls.load(Ordering::SeqCst), 1);
}
