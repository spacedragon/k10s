//! Prepare-then-commit context switching and advisory permission projection
//! for the real kube-rs adapter, driven by recorded tower-level API servers
//! (no live cluster).
//!
//! Covered behavior: SelfSubjectAccessReview projection as advisory metadata,
//! forbidden/absent review fallback to explicit Unknown outcomes, failed
//! destination prepares preserving the current context, cleared warm
//! selection state across a successful switch, unavailable GVKs surfaced
//! honestly against the destination catalog, retirement of the previous
//! context's watchers and metrics collectors, and later operations still
//! reaching the API server under its own authorization decisions.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use k10s_backend::runtime::{
    ClusterMetrics, ClusterWatches, ListedState, RuntimeWatchScript, WatchRow, WatchSource,
};
use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::watch::WatchSelector;
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, ContextInfo, Gvk, KernelQueryResult, KubeAdapter,
    PermissionProbe, Query, ResourceRef, Subscribe,
};

const CONTEXT_A: &str = "switch-a";
const CONTEXT_B: &str = "switch-b";
const CONTEXT_C: &str = "switch-c";
const NS: &str = "default";

/// Request path of the SelfSubjectAccessReview submission endpoint.
const SSAR_PATH: &str = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews";
/// Discovery group list, hit once per full discovery pass.
const APIS_PATH: &str = "/apis";
const PODS_LIST_PATH: &str = "/api/v1/namespaces/default/pods";
const POD_GET_PATH: &str = "/api/v1/namespaces/default/pods/web";
const CORE_NODES_PATH: &str = "/api/v1/nodes";
const NODE_METRICS_PATH: &str = "/apis/metrics.k8s.io/v1beta1/nodes";
const POD_METRICS_PATH: &str = "/apis/metrics.k8s.io/v1beta1/pods";

fn pods_gvk() -> Gvk {
    Gvk::core("v1", "Pod")
}

fn probe(verb: &str, resource: &str, namespace: Option<&str>) -> PermissionProbe {
    PermissionProbe {
        verb: verb.into(),
        resource: resource.into(),
        group: None,
        namespace: namespace.map(str::to_owned),
    }
}

fn probe_with_group(
    verb: &str,
    resource: &str,
    group: Option<&str>,
    namespace: Option<&str>,
) -> PermissionProbe {
    PermissionProbe {
        group: group.map(str::to_owned),
        ..probe(verb, resource, namespace)
    }
}

fn contexts() -> Vec<ContextInfo> {
    vec![
        ContextInfo {
            name: CONTEXT_A.into(),
            cluster: "recorded-a".into(),
            namespace: Some(NS.into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
        ContextInfo {
            name: CONTEXT_B.into(),
            cluster: "recorded-b".into(),
            namespace: Some(NS.into()),
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
    ]
}

fn three_contexts() -> Vec<ContextInfo> {
    let mut contexts = contexts();
    contexts.push(ContextInfo {
        name: CONTEXT_C.into(),
        cluster: "recorded-c".into(),
        namespace: Some(NS.into()),
        is_current: false,
        availability: k10s_protocol::ContextAvailability::Available,
        unavailable_reason: None,
    });
    contexts
}

/// One adapter whose two contexts are backed by two independent recorded API
/// servers, so either context's read path can fail independently.
fn two_context_adapter(server_a: RecordedApiServer, server_b: RecordedApiServer) -> KubeAdapter {
    let client_a = server_a.clone().into_client(NS);
    let client_b = server_b.clone().into_client(NS);
    KubeAdapter::with_cluster_clients(contexts(), [(CONTEXT_A, client_a), (CONTEXT_B, client_b)])
        .expect("adapter builds around recorded clients")
}

/// The kernel under test plus a shared handle to its metrics registry.
struct World {
    kernel: BackendKernel,
    metrics: ClusterMetrics,
}

impl World {
    fn new(adapter: KubeAdapter) -> Self {
        let metrics = adapter.metrics_registry();
        Self {
            kernel: BackendKernel::new(adapter),
            metrics,
        }
    }
}

fn world(server_a: RecordedApiServer, server_b: RecordedApiServer) -> World {
    World::new(two_context_adapter(server_a, server_b))
}

/// Record one SelfSubjectAccessReview answer served for every probe.
fn record_ssar(server: &RecordedApiServer, body: &str) {
    server.set_response(SSAR_PATH, 200, body);
}

fn ssar_allowed(reason: &str) -> String {
    format!(
        r#"{{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",
            "metadata":{{"creationTimestamp":null}},
            "spec":{{"resourceAttributes":{{"namespace":"{NS}","verb":"list","resource":"pods"}}}},
            "status":{{"allowed":true,"reason":"{reason}"}}}}"#
    )
}

fn ssar_denied(reason: &str) -> String {
    format!(
        r#"{{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",
            "metadata":{{"creationTimestamp":null}},
            "spec":{{"resourceAttributes":{{"namespace":"{NS}","verb":"list","resource":"pods"}}}},
            "status":{{"allowed":false,"reason":"{reason}"}}}}"#
    )
}

fn ssar_evaluation_error(detail: &str) -> String {
    format!(
        r#"{{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",
            "metadata":{{"creationTimestamp":null}},
            "spec":{{"resourceAttributes":{{"namespace":"{NS}","verb":"list","resource":"pods"}}}},
            "status":{{"allowed":false,"evaluationError":"{detail}"}}}}"#
    )
}

fn ssar_forbidden_status() -> String {
    r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
        "message":"selfsubjectaccessreviews is forbidden","reason":"Forbidden","code":403}"#
        .to_owned()
}

fn status_error(code: u16, message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":"{message}","reason":"Forbidden","code":{code}}}"#
    )
}

/// The recorded /apis group list with the CRD-backed group removed, so live
/// discovery observes a surface the warm cache predates.
const APIS_GROUP_LIST_WITHOUT_CRD_GROUP: &str = r#"{"kind":"APIGroupList","apiVersion":"v1","groups":[
  {"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}},
  {"name":"batch","versions":[{"groupVersion":"batch/v1","version":"v1"}],"preferredVersion":{"groupVersion":"batch/v1","version":"v1"}},
  {"name":"apiextensions.k8s.io","versions":[{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}},
  {"name":"storage.k8s.io","versions":[{"groupVersion":"storage.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"storage.k8s.io/v1","version":"v1"}}
]}"#;

const API_VERSIONS_V1: &str = r#"{"kind":"APIVersions","apiVersion":"v1","versions":["v1"],"resources":["namespacedNames","nonNamespacedNames"]}"#;

fn empty_pod_list() -> String {
    r#"{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"41"},"items":[]}"#.into()
}

fn one_pod_list() -> String {
    r#"{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"41"},"items":[
      {"metadata":{"name":"web","uid":"uid-web","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
       "status":{"phase":"Running"}}
    ]}"#
        .into()
}

/// Unwrap a context-permissions result into its wire payload.
async fn permissions_of(
    kernel: &BackendKernel,
    context: &str,
    probes: Vec<PermissionProbe>,
) -> serde_json::Value {
    match kernel
        .query(Query::ContextPermissions {
            context: context.into(),
            probes,
        })
        .await
        .expect("permission projection succeeds")
    {
        KernelQueryResult::ContextPermissions(data) => {
            serde_json::to_value(data.wire_payload()).expect("payload serializes")
        }
        other => panic!("permissions must map to their result, got {other:?}"),
    }
}

/// Poll until `predicate` holds, with a hard timeout.
async fn wait_until(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition never held: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The context currently carrying the `is_current` marker at bootstrap.
async fn current_context(kernel: &BackendKernel) -> String {
    match kernel
        .query(Query::Bootstrap)
        .await
        .expect("bootstrap works")
    {
        KernelQueryResult::Bootstrap(bootstrap) => {
            let payload = bootstrap.wire_payload();
            let currents: Vec<_> = payload
                .contexts
                .iter()
                .filter(|context| context.is_current)
                .collect();
            assert_eq!(
                currents.len(),
                1,
                "exactly one context stays current, got {}",
                currents.len()
            );
            currents[0].name.clone()
        }
        other => panic!("bootstrap must map to its wire payload, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Advisory capability projection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn self_subject_access_reviews_project_advisory_capabilities() {
    let server = RecordedApiServer::standard();
    record_ssar(&server, &ssar_allowed("RBAC: allowed by role binding"));

    let world = world(server.clone(), RecordedApiServer::default());
    let probes = vec![
        probe("list", "pods", Some(NS)),
        probe("delete", "deployments", Some("production")),
        probe("get", "nodes", None),
    ];
    let payload = permissions_of(&world.kernel, CONTEXT_A, probes.clone()).await;

    // Every check echoes its probe verbatim and reports the reviewed verdict.
    assert_eq!(payload["context"], CONTEXT_A);
    let checks = payload["checks"].as_array().expect("checks are an array");
    assert_eq!(checks.len(), probes.len());
    for (sent, answered) in probes.iter().zip(checks) {
        assert_eq!(answered["verb"], sent.verb);
        assert_eq!(answered["resource"], sent.resource);
        assert_eq!(answered["namespace"].as_str(), sent.namespace.as_deref());
        assert_eq!(answered["outcome"], "allowed");
    }

    // Each projected check is backed by exactly one real review call: the
    // projection reads the cluster's authorization, never guesses it.
    assert_eq!(
        server.hit_count(SSAR_PATH),
        probes.len(),
        "one SelfSubjectAccessReview per distinct probe"
    );

    // Rewriting the recorded verdict flips the projection honestly: a denial
    // is reported as Denied, distinctly from Unknown.
    record_ssar(&server, &ssar_denied("not allowed by policy"));
    let denied = permissions_of(
        &world.kernel,
        CONTEXT_A,
        vec![probe("list", "configmaps", Some(NS))],
    )
    .await;
    assert_eq!(denied["checks"][0]["outcome"], "denied");
}

#[tokio::test]
async fn a_forbidden_review_degrades_to_unknown_without_failing_the_query() {
    let server = RecordedApiServer::standard();
    // The user may not even create SelfSubjectAccessReviews: the review call
    // itself is rejected by authorization.
    server.set_response(SSAR_PATH, 403, &ssar_forbidden_status());

    let world = world(server.clone(), RecordedApiServer::default());
    let probes = vec![
        probe("list", "pods", Some(NS)),
        probe("delete", "deployments", Some(NS)),
    ];
    let forbidden = permissions_of(&world.kernel, CONTEXT_A, probes).await;

    // Forbidden reviews degrade to explicit Unknown outcomes instead of
    // erroring the whole flow or fabricating allow/deny verdicts.
    for check in forbidden["checks"].as_array().unwrap() {
        assert_eq!(check["outcome"], "unknown");
    }

    // An answered review carrying an evaluation error is equally unknowable.
    record_ssar(&server, &ssar_evaluation_error("authorizer misconfigured"));
    let errored = permissions_of(
        &world.kernel,
        CONTEXT_A,
        vec![probe("get", "pods", Some(NS))],
    )
    .await;
    assert_eq!(errored["checks"][0]["outcome"], "unknown");

    // A review response without any status section says nothing at all.
    record_ssar(
        &server,
        r#"{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1"}"#,
    );
    let statusless = permissions_of(
        &world.kernel,
        CONTEXT_A,
        vec![probe("watch", "pods", Some(NS))],
    )
    .await;
    assert_eq!(statusless["checks"][0]["outcome"], "unknown");
}

#[tokio::test]
async fn duplicate_probes_normalize_into_one_review_call() {
    let server = RecordedApiServer::standard();
    record_ssar(&server, &ssar_allowed("RBAC: allowed"));
    let world = world(server.clone(), RecordedApiServer::default());

    let data = permissions_of(
        &world.kernel,
        CONTEXT_A,
        vec![
            probe("list", "pods", Some(NS)),
            probe("list", "pods", Some(NS)),
        ],
    )
    .await;

    assert_eq!(
        server.hit_count(SSAR_PATH),
        1,
        "identical probes share one review"
    );
    assert_eq!(data["checks"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Prepare-then-commit switching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_validated_prepare_commits_the_new_current_context() {
    let server_a = RecordedApiServer::standard();
    let server_b = RecordedApiServer::standard();
    server_a.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    server_b.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    let world = world(server_a.clone(), server_b.clone());

    let b_apis_before = server_b.hit_count(APIS_PATH);
    match world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits")
    {
        KernelQueryResult::ContextSwitch(switched) => {
            let payload = switched.wire_payload();
            assert_eq!(payload.current, CONTEXT_B);
            assert_eq!(payload.previous.as_deref(), Some(CONTEXT_A));
        }
        other => panic!("switch must map to its result, got {other:?}"),
    }
    assert!(
        server_b.hit_count(APIS_PATH) > b_apis_before,
        "preparation discovered through the destination client"
    );

    // The commit swapped the bootstrap marker atomically.
    assert_eq!(current_context(&world.kernel).await, CONTEXT_B);

    // Switching to the already-current context stays a validated success
    // without disturbing the single-current invariant.
    match world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("redundant switch succeeds")
    {
        KernelQueryResult::ContextSwitch(switched) => {
            assert_eq!(switched.wire_payload().current, CONTEXT_B);
        }
        other => panic!("switch must map to its result, got {other:?}"),
    }
    assert_eq!(current_context(&world.kernel).await, CONTEXT_B);
}

#[tokio::test]
async fn a_failed_destination_prepare_preserves_the_current_context() {
    let server_a = RecordedApiServer::standard();
    // The destination's API server answers nothing usable: discovery fails.
    let server_b = RecordedApiServer::default();
    server_a.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    let world = world(server_a.clone(), server_b);

    let failure = world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect_err("a broken destination refuses to commit");
    assert!(
        matches!(failure, BackendError::Internal(ref detail) if detail.contains(CONTEXT_B)),
        "prepare failures stay sanitized and name the context: {failure:?}"
    );

    // Nothing observable moved: the current marker and the working read path
    // of the previous context survive the aborted switch.
    assert_eq!(current_context(&world.kernel).await, CONTEXT_A);
    let listed_before_failure = server_a.hit_count(PODS_LIST_PATH);
    world
        .kernel
        .query(Query::ResourceList {
            context: CONTEXT_A.into(),
            gvk: pods_gvk(),
            namespace: Some(NS.into()),
        })
        .await
        .expect("the preserved context still serves lists");
    assert!(server_a.hit_count(PODS_LIST_PATH) > listed_before_failure);

    // An entirely unknown destination is a typed not-found before any
    // cluster traffic happens.
    let ghost = world
        .kernel
        .query(Query::ContextSwitch {
            to: "ghost-context".into(),
        })
        .await
        .expect_err("unknown destinations are not-found");
    assert_eq!(ghost, BackendError::NotFound);
    assert_eq!(current_context(&world.kernel).await, CONTEXT_A);
}

// ---------------------------------------------------------------------------
// Selection clearing and previous-runtime retirement
// ---------------------------------------------------------------------------

/// A scripted watch source serving one static pod row and counting its LIST
/// calls, so fresh generations stay distinguishable from revived ones.
#[derive(Debug)]
struct CountingSource {
    context: &'static str,
    list_calls: AtomicUsize,
}

impl CountingSource {
    fn new(context: &'static str) -> Arc<Self> {
        Arc::new(Self {
            context,
            list_calls: AtomicUsize::new(0),
        })
    }
}

impl WatchSource for CountingSource {
    fn list<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ListedState, String>> + Send + 'a>>
    {
        Box::pin(async move {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ListedState {
                resource_version: "41".into(),
                rows: vec![WatchRow {
                    reference: ResourceRef {
                        context: self.context.into(),
                        gvk: pods_gvk(),
                        namespace: Some(NS.into()),
                        name: "web".into(),
                        uid: "uid-web".into(),
                    },
                    labels: Default::default(),
                    summary: "Running".into(),
                    created_at: "2026-08-21T00:00:00Z".into(),
                    owner_references: Vec::new(),
                    projection: None,
                }],
            })
        })
    }

    fn attach_watch<'a>(
        &'a self,
        _resource_version: String,
        _out: tokio::sync::mpsc::UnboundedSender<k10s_backend::runtime::WatchUpdate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { std::future::pending::<()>().await })
    }
}

/// An adapter wired for scripted-watch tests plus its registries and kernel.
struct ScriptedWorld {
    kernel: BackendKernel,
    watches: ClusterWatches,
    source: Arc<CountingSource>,
}

fn script_for(source: &Arc<CountingSource>) -> RuntimeWatchScript {
    let source = Arc::clone(source);
    Arc::new(move |_gvk, _namespace| Some(Arc::clone(&source) as Arc<dyn WatchSource>))
}

impl ScriptedWorld {
    fn new(context: &'static str) -> Self {
        let source = CountingSource::new(context);
        let adapter =
            two_context_adapter(RecordedApiServer::standard(), RecordedApiServer::standard())
                .with_scripted_watches(script_for(&source));
        let watches = adapter.watches_registry();
        Self {
            kernel: BackendKernel::new(adapter),
            watches,
            source,
        }
    }

    /// The same world over three contexts backed by three recorded servers,
    /// so overlapping switches have distinct valid destinations.
    fn with_three_contexts() -> Self {
        let source = CountingSource::new(CONTEXT_A);
        let adapter = KubeAdapter::with_cluster_clients(
            three_contexts(),
            [
                (CONTEXT_A, RecordedApiServer::standard().into_client(NS)),
                (CONTEXT_B, RecordedApiServer::standard().into_client(NS)),
                (CONTEXT_C, RecordedApiServer::standard().into_client(NS)),
            ],
        )
        .expect("adapter builds around recorded clients")
        .with_scripted_watches(script_for(&source));
        let watches = adapter.watches_registry();
        Self {
            kernel: BackendKernel::new(adapter),
            watches,
            source,
        }
    }
}

/// Subscribe a pods watch on `context` through the kernel, await its warm
/// snapshot, and return the live event receiver.
async fn subscribe_warm_watch(
    world: &ScriptedWorld,
    context: &'static str,
) -> tokio::sync::broadcast::Receiver<BackendEvent> {
    let mut handle = world
        .kernel
        .subscribe(Subscribe::ResourceWatch {
            context: context.to_owned(),
            gvk: pods_gvk(),
            namespace: Some(NS.into()),
        })
        .await
        .expect("watch subscribes");
    let mut events = handle.take_events().expect("watches carry events");
    let snapshot = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("snapshot arrives in time")
        .expect("channel open");
    assert!(
        matches!(snapshot, BackendEvent::Snapshot(_)),
        "first event must be the snapshot"
    );
    events
}

#[tokio::test]
async fn warm_selection_state_clears_across_a_successful_switch() {
    let world = ScriptedWorld::new(CONTEXT_A);
    // Keep the subscriber attached across the switch: even a live consumer
    // cannot keep the previous context's warm state alive past the commit.
    let _events = subscribe_warm_watch(&world, CONTEXT_A).await;
    let selector = WatchSelector {
        context: CONTEXT_A.into(),
        gvk: pods_gvk(),
        namespace: Some(NS.into()),
    };
    assert_eq!(world.watches.live_selections(), 1);
    assert_eq!(
        world
            .watches
            .cached_rows(&selector)
            .expect("warm rows exist")
            .len(),
        1
    );

    match world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits")
    {
        KernelQueryResult::ContextSwitch(_) => {}
        other => panic!("switch must map to its result, got {other:?}"),
    }

    // The committed switch cleared every warm selection of the previous
    // context: no cached rows survive it, even with a subscribed consumer.
    assert!(world.watches.cached_rows(&selector).is_none());
    assert_eq!(world.watches.live_selections(), 0);
    assert!(world.watches.phases(&selector).is_none());
}

#[tokio::test]
async fn the_previous_contexts_runtime_is_retired_after_commit() {
    let scripted = ScriptedWorld::new(CONTEXT_A);
    let mut events = subscribe_warm_watch(&scripted, CONTEXT_A).await;
    let lists_before_switch = scripted.source.list_calls.load(Ordering::SeqCst);
    assert!(lists_before_switch >= 1, "the warm selection listed once");

    scripted
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits");

    // Retirement ends the old generation outright: its event channel closes
    // instead of lingering open behind a dead selection.
    wait_until("old watch channel closes after retirement", || {
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Closed)
        )
    })
    .await;
    drop(events);

    // A later subscriber of the retired selection starts a fresh generation:
    // the source relists instead of reviving dead cache state.
    let mut fresh = subscribe_warm_watch(&scripted, CONTEXT_A).await;
    let next_event = tokio::time::timeout(Duration::from_secs(5), fresh.recv())
        .await
        .expect("fresh generation snapshots in time")
        .expect("channel open");
    assert!(matches!(next_event, BackendEvent::Snapshot(_)));
    assert!(
        scripted.source.list_calls.load(Ordering::SeqCst) > lists_before_switch,
        "retirement forced a fresh relist on resubscribe"
    );

    // Metrics collectors of the previous context retire the same way: no
    // leaked poll tasks outlive their context's relevance.
    let server_a = RecordedApiServer::standard();
    server_a.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    server_a.set_response(
        POD_GET_PATH,
        200,
        r#"{"kind":"Pod","apiVersion":"v1","metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"41","creationTimestamp":"2026-08-21T00:00:00Z"},"status":{"phase":"Running"}}"#,
    );
    server_a.set_response(
        CORE_NODES_PATH,
        200,
        r#"{"kind":"NodeList","apiVersion":"v1","metadata":{"resourceVersion":"100"},"items":[]}"#,
    );
    server_a.set_response(NODE_METRICS_PATH, 200, r#"{"kind":"NodeMetricsList","apiVersion":"metrics.k8s.io/v1beta1","metadata":{},"items":[]}"#);
    server_a.set_response(POD_METRICS_PATH, 200, r#"{"kind":"PodMetricsList","apiVersion":"metrics.k8s.io/v1beta1","metadata":{},"items":[]}"#);
    let collector_world = world(server_a, RecordedApiServer::standard());
    collector_world
        .kernel
        .query(Query::ResourceMetrics {
            reference: ResourceRef {
                context: CONTEXT_A.into(),
                gvk: pods_gvk(),
                namespace: Some(NS.into()),
                name: "web".into(),
                uid: "uid-web".into(),
            },
        })
        .await
        .expect("metrics query starts the collector");
    assert_eq!(
        collector_world.metrics.live_collectors(),
        1,
        "the queried context owns one live collector"
    );
    collector_world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits");
    wait_until("previous context's metrics collector retires", || {
        collector_world.metrics.live_collectors() == 0
    })
    .await;
}

// ---------------------------------------------------------------------------
// Honest post-switch read behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_gvk_missing_from_destination_discovery_is_surfaced_honestly() {
    let server_a = RecordedApiServer::standard();
    let server_b = RecordedApiServer::standard();
    server_b.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    let world = world(server_a, server_b);

    world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits");

    // The destination's own discovery catalog decides what exists there; the
    // recorded world carries no Secrets at all.
    match world
        .kernel
        .query(Query::ResourceTypes {
            context: CONTEXT_B.into(),
        })
        .await
        .expect("destination discovery works")
    {
        KernelQueryResult::ResourceTypes(types) => {
            let payload =
                serde_json::to_value(types.wire_payload()).expect("types payload serializes");
            let secret = payload["types"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["gvk"]["kind"] == "Secret");
            assert!(!secret, "Secret is absent from the destination catalog");
        }
        other => panic!("types must map to their result, got {other:?}"),
    }

    // Listing the absent type is an honest typed not-found — never an empty
    // successful list and never a fabricated capability statement.
    let missing = world
        .kernel
        .query(Query::ResourceList {
            context: CONTEXT_B.into(),
            gvk: Gvk::core("v1", "Secret"),
            namespace: Some(NS.into()),
        })
        .await
        .expect_err("an unavailable GVK refuses to list");
    assert_eq!(missing, BackendError::NotFound);
}

#[tokio::test]
async fn later_reads_still_reach_the_api_server_and_respect_authorization() {
    let server_a = RecordedApiServer::default();
    let server_b = RecordedApiServer::standard();
    server_b.set_response(PODS_LIST_PATH, 200, &one_pod_list());
    // Advisory projection ahead of the switch: the user may list pods there.
    record_ssar(&server_b, &ssar_allowed("RBAC: allowed by role binding"));
    let world = world(server_a, server_b.clone());

    let advisory = permissions_of(
        &world.kernel,
        CONTEXT_B,
        vec![probe("list", "pods", Some(NS))],
    )
    .await;
    assert_eq!(advisory["checks"][0]["outcome"], "allowed");

    world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits");

    // Reads keep hitting the API server after the switch — the projection
    // never became a local shortcut around Kubernetes.
    let before = server_b.hit_count(PODS_LIST_PATH);
    world
        .kernel
        .query(Query::ResourceList {
            context: CONTEXT_B.into(),
            gvk: pods_gvk(),
            namespace: Some(NS.into()),
        })
        .await
        .expect("the allowed list succeeds");
    assert!(
        server_b.hit_count(PODS_LIST_PATH) > before,
        "post-switch reads still reach the api server"
    );

    // The API server revokes access underneath the advisory metadata: the
    // read respects the server's decision instead of trusting Allowed.
    server_b.set_response(PODS_LIST_PATH, 403, &status_error(403, "pods is forbidden"));
    let revoked = world
        .kernel
        .query(Query::ResourceList {
            context: CONTEXT_B.into(),
            gvk: pods_gvk(),
            namespace: Some(NS.into()),
        })
        .await
        .expect_err("authorization still applies after the switch");
    assert_eq!(revoked, BackendError::Forbidden);
}

// ---------------------------------------------------------------------------
// Review hardening: live validation, serialized switches, and grouped probes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn switch_validation_always_contacts_the_destination_even_with_a_fresh_cache() {
    let server_a = RecordedApiServer::standard();
    let server_b = RecordedApiServer::standard();
    server_a.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    let world = world(server_a.clone(), server_b.clone());

    // Warm B's discovery cache while it works.
    world
        .kernel
        .query(Query::ResourceTypes {
            context: CONTEXT_B.into(),
        })
        .await
        .expect("destination discovery warms the cache");
    let b_apis_before = server_b.hit_count(APIS_PATH);
    assert!(b_apis_before > 0, "the warm-up discovered through B");

    // B breaks without its cached catalog expiring.
    server_b.set_accept_response(
        "GET",
        APIS_PATH,
        "apidiscovery.k8s.io",
        500,
        &status_error(500, "api group list down"),
    );

    let failure = world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect_err("a fresh cached catalog must not stand in for a live check");
    assert!(
        matches!(failure, BackendError::Internal(ref detail) if detail.contains(CONTEXT_B)),
        "prepare failures stay sanitized and name the context: {failure:?}"
    );
    assert!(
        server_b.hit_count(APIS_PATH) > b_apis_before,
        "validation sent live traffic to the destination"
    );
    // The healthy current context survives the aborted switch untouched.
    assert_eq!(current_context(&world.kernel).await, CONTEXT_A);
}

#[tokio::test]
async fn a_successful_switch_publishes_the_freshly_observed_destination_catalog() {
    async fn resource_types_wire(kernel: &BackendKernel, context: &str) -> serde_json::Value {
        match kernel
            .query(Query::ResourceTypes {
                context: context.to_owned(),
            })
            .await
            .expect("destination discovery works")
        {
            KernelQueryResult::ResourceTypes(types) => {
                serde_json::to_value(types.wire_payload()).expect("types payload serializes")
            }
            other => panic!("types must map to their result, got {other:?}"),
        }
    }

    fn carries_crd_group(payload: &serde_json::Value) -> bool {
        payload["types"]
            .as_array()
            .expect("types are an array")
            .iter()
            .any(|entry| entry["gvk"]["group"] == "k10s.example.com")
    }

    let server_a = RecordedApiServer::standard();
    let server_b = RecordedApiServer::standard();
    server_a.set_response(PODS_LIST_PATH, 200, &empty_pod_list());
    let world = world(server_a.clone(), server_b.clone());

    // Warm B's catalog while its CRD group still exists.
    let warm = resource_types_wire(&world.kernel, CONTEXT_B).await;
    assert!(
        carries_crd_group(&warm),
        "the warm cache carries the CRD group's Gadget type"
    );

    // B loses its CRD group without the warm catalog expiring.
    server_b.set_response(APIS_PATH, 200, APIS_GROUP_LIST_WITHOUT_CRD_GROUP);
    server_b.set_accept_response(
        "GET",
        APIS_PATH,
        "apidiscovery.k8s.io",
        200,
        APIS_GROUP_LIST_WITHOUT_CRD_GROUP,
    );
    server_b.set_accept_response("GET", "/api", "apidiscovery.k8s.io", 200, API_VERSIONS_V1);

    world
        .kernel
        .query(Query::ContextSwitch {
            to: CONTEXT_B.into(),
        })
        .await
        .expect("validated switch commits");
    assert_eq!(current_context(&world.kernel).await, CONTEXT_B);

    // The picker reflects what live discovery just observed during switch
    // validation — never the stale warm cache.
    let fresh = resource_types_wire(&world.kernel, CONTEXT_B).await;
    assert!(
        !carries_crd_group(&fresh),
        "the post-switch catalog must come from the live discovery, not the cache"
    );
}

#[tokio::test]
async fn overlapping_switches_serialize_into_complete_transactions() {
    let scripted = ScriptedWorld::with_three_contexts();
    let mut events = subscribe_warm_watch(&scripted, CONTEXT_A).await;

    // Two switches race toward different destinations; each must capture,
    // validate, commit, and retire as one atomic transaction.
    let first = scripted.kernel.query(Query::ContextSwitch {
        to: CONTEXT_B.into(),
    });
    let second = scripted.kernel.query(Query::ContextSwitch {
        to: CONTEXT_C.into(),
    });
    let (first, second) = tokio::join!(first, second,);

    fn committed(result: Result<KernelQueryResult, BackendError>) -> (String, Option<String>) {
        match result.expect("both validated switches commit") {
            KernelQueryResult::ContextSwitch(switched) => {
                let payload = switched.wire_payload();
                (payload.current, payload.previous)
            }
            other => panic!("switch must map to its result, got {other:?}"),
        }
    }
    let (to_a, previous_a) = committed(first);
    let (to_b, previous_b) = committed(second);
    assert_ne!(to_a, to_b);

    // Exactly one switch replaced A: whoever committed second reports that
    // destination as its previous — never a stale snapshot of A.
    let (_first_to, second_to) = if previous_a.as_deref() == Some(CONTEXT_A) {
        (to_a.as_str(), to_b.as_str())
    } else {
        assert_eq!(
            previous_b.as_deref(),
            Some(CONTEXT_A),
            "exactly one racing switch replaces A"
        );
        (to_b.as_str(), to_a.as_str())
    };
    assert_ne!(
        previous_a, previous_b,
        "each commit saw its own predecessor"
    );

    // The final marker belongs to the last committer, and retirement chased
    // exactly the contexts each commit replaced: nothing leaks.
    assert_eq!(current_context(&scripted.kernel).await, second_to);
    wait_until("the replaced context's watch channel closes", || {
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Closed)
        )
    })
    .await;
    drop(events);
    assert_eq!(
        scripted.watches.live_selections(),
        0,
        "no watch selection outlives its context's retirement"
    );
}

#[tokio::test]
async fn permission_reviews_carry_the_api_group_of_the_reviewed_resource() {
    let server = RecordedApiServer::standard();
    record_ssar(&server, &ssar_allowed("RBAC: allowed"));
    let world = world(server.clone(), RecordedApiServer::default());

    let payload = permissions_of(
        &world.kernel,
        CONTEXT_A,
        vec![
            probe_with_group("list", "deployments", Some("apps"), Some(NS)),
            probe_with_group("get", "nodes", None, None),
            probe_with_group("list", "deployments", None, Some(NS)),
        ],
    )
    .await;

    // The submitted reviews ask about the exact API group: a core-group
    // default would review `deployments` as if it were `apps/deployments`
    // (or vice versa), turning advisory outcomes into false denials.
    let bodies = server.request_bodies(SSAR_PATH);
    assert_eq!(bodies.len(), 3, "one review per distinct probe");
    let submitted: Vec<serde_json::Value> = bodies
        .iter()
        .map(|body| serde_json::from_str(body).expect("review bodies are json"))
        .collect();
    assert_eq!(
        submitted[0]["spec"]["resourceAttributes"]["group"], "apps",
        "the grouped resource reviews its own group"
    );
    assert!(
        submitted[1]["spec"]["resourceAttributes"]
            .get("group")
            .is_none(),
        "core probes leave the authorizer default untouched"
    );
    assert!(
        submitted[2]["spec"]["resourceAttributes"]
            .get("group")
            .is_none(),
        "an unset group reviews the core group explicitly by absence"
    );
    // The grouped and core deployments probes are distinct reviews.
    assert_ne!(submitted[0], submitted[2]);

    // Group is part of the probe identity and echoes back on every check.
    let checks = payload["checks"].as_array().expect("checks are an array");
    assert_eq!(checks.len(), 3);
    assert_eq!(checks[0]["group"], "apps");
    assert!(checks[1].get("group").is_none());
    assert!(checks[2].get("group").is_none());
}
