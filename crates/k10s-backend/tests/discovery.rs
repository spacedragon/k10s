//! Discovery and dynamic resource catalog behavior for the real kube-rs
//! adapter, driven by a recorded tower Service (no live cluster).

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, ContextInfo, KubeAdapter, KubernetesAccess, Query, QueryResult, ResourceTypesData,
};

/// Build an adapter whose single context is backed by a recorded API server.
fn adapter_with_recorded_context() -> (KubeAdapter, RecordedApiServer) {
    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let contexts = vec![ContextInfo {
        name: "mock-cluster".into(),
        cluster: "recorded-apiserver".into(),
        namespace: Some("default".into()),
        is_current: true,
    }];
    let adapter = KubeAdapter::with_cluster_clients(contexts, [("mock-cluster", client)])
        .expect("adapter builds around recorded clients");
    (adapter, server)
}

async fn types_for(adapter: &KubeAdapter, context: &str) -> QueryResult {
    adapter
        .query(Query::ResourceTypes {
            context: context.into(),
        })
        .await
        .expect("discovery succeeds against the recorded server")
}

fn resource_types(result: QueryResult) -> ResourceTypesData {
    match result {
        QueryResult::ResourceTypes(data) => data,
        other => panic!("kernel seam must normalize discovery into types, got {other:?}"),
    }
}

#[tokio::test]
async fn builtins_are_normalized_with_scope_and_plural() {
    let (adapter, _server) = adapter_with_recorded_context();
    let data = resource_types(types_for(&adapter, "mock-cluster").await);

    // Built-in core types carry their GVK, scope, and plural names.
    assert_eq!(
        data.find_kind("Pod")
            .map(|entry| (entry.gvk.group.as_str(), entry.gvk.version.as_str())),
        Some(("", "v1")),
        "core Pod resolves from discovery"
    );
    let pods = data.find_plural("pods").expect("pods plural resolves");
    assert_eq!(pods.gvk.kind, "Pod");
    assert!(pods.namespaced);

    // Cluster-scoped built-ins must not be marked namespaced.
    for kind in ["Node", "Namespace"] {
        let entry = data.find_kind(kind).expect("cluster builtin resolves");
        assert!(!entry.namespaced, "{kind} is cluster-scoped");
    }

    // The core group/version slice of the catalog stays complete.
    let core_v1 = data.of_group_version("", "v1");
    assert!(core_v1.iter().any(|entry| entry.gvk.kind == "Pod"));

    // apps/v1 built-ins arrive through the same normalized path.
    let deployments = data.find_kind("Deployment").expect("deployment resolves");
    assert_eq!(&deployments.gvk.group, "apps");
    assert!(deployments.namespaced);

    // Create-only submission types that discovery advertises (TokenReview in
    // the recorded fixture) must not reach the selectable catalog.
    assert!(
        data.find_kind("TokenReview").is_none(),
        "create-only types have no list verb and cannot be picker entries"
    );
}

#[tokio::test]
async fn crd_types_search_by_kind_plural_group_version() {
    let (adapter, _server) = adapter_with_recorded_context();
    let data = resource_types(types_for(&adapter, "mock-cluster").await);

    // The recorded CRD resolves through every catalog search dimension.
    let by_kind = data.find_kind("Gadget").expect("CRD kind searches");
    assert_eq!(by_kind.gvk.group, "k10s.example.com");
    assert_eq!(by_kind.gvk.version, "v1alpha1");

    let by_plural = data.find_plural("gadgets").expect("CRD plural searches");
    assert_eq!(by_plural.gvk.kind, "Gadget");

    let in_group_version = data.of_group_version("k10s.example.com", "v1alpha1");
    assert_eq!(in_group_version.len(), 1);
    assert_eq!(in_group_version[0].gvk.kind, "Gadget");

    // Group/version search also covers built-in groups.
    let apps_v1 = data.of_group_version("apps", "v1");
    assert!(apps_v1.iter().any(|entry| entry.gvk.kind == "Deployment"));
}

#[tokio::test]
async fn scale_subresource_capability_is_detected() {
    let (adapter, _server) = adapter_with_recorded_context();
    let data = resource_types(types_for(&adapter, "mock-cluster").await);

    // Real clusters expose /scale for these workloads.
    for kind in ["Deployment", "ReplicaSet", "StatefulSet"] {
        let entry = data.find_kind(kind).expect("workload resolves");
        assert!(entry.supports_scale, "{kind} exposes a scale subresource");
    }

    // Everything else in the recorded world does not.
    for kind in ["DaemonSet", "Pod", "Node", "Gadget"] {
        let entry = data.find_kind(kind).expect("type resolves");
        assert!(!entry.supports_scale, "{kind} has no scale subresource");
    }
}

#[tokio::test]
async fn unavailable_context_and_gvk_are_typed_failures() {
    let (adapter, _server) = adapter_with_recorded_context();

    // Unknown context: typed not-found, never an empty catalog.
    let error = adapter
        .query(Query::ResourceTypes {
            context: "missing-context".into(),
        })
        .await
        .expect_err("unknown contexts must fail");
    assert!(matches!(error, BackendError::NotFound), "{error:?}");

    // Unknown kinds search to None instead of panicking or guessing.
    let data = resource_types(types_for(&adapter, "mock-cluster").await);
    assert!(data.find_kind("DoesNotExist").is_none());
    assert!(data.find_plural("ghosts").is_none());
}

#[tokio::test]
async fn api_server_rejections_stay_typed_and_never_empty_catalog() {
    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let contexts = vec![ContextInfo {
        name: "denied-cluster".into(),
        cluster: "denied-apiserver".into(),
        namespace: Some("default".into()),
        is_current: true,
    }];
    let adapter = KubeAdapter::with_cluster_clients(contexts, [("denied-cluster", client)])
        .expect("adapter builds");

    // The recorded API server denies discovery with a 403 Status body.
    server.set_response("/apis", 403, r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"discovery denied by policy","reason":"Forbidden","code":403}"#);

    let error = adapter
        .query(Query::ResourceTypes {
            context: "denied-cluster".into(),
        })
        .await
        .expect_err("denied discovery must fail closed");
    match &error {
        BackendError::Internal(detail) => {
            // Operator-facing detail stays sanitized: the server's raw
            // Status message must not be echoed back.
            assert!(!detail.contains("discovery denied by policy"), "{detail}");
            assert!(detail.len() < 200, "details stay bounded: {detail}");
        }
        other => panic!("expected typed internal error, got {other:?}"),
    }
}

#[tokio::test]
async fn discovery_is_cached_until_invalidated() {
    let (adapter, server) = adapter_with_recorded_context();

    // First query discovers; the second serves from cache.
    resource_types(types_for(&adapter, "mock-cluster").await);
    let after_first = server.hit_count("/apis");
    assert_eq!(after_first, 1, "first query runs discovery");
    resource_types(types_for(&adapter, "mock-cluster").await);
    assert_eq!(
        server.hit_count("/apis"),
        after_first,
        "cache serves repeats"
    );

    // Invalidation forces a re-discovery that sees updated responses.
    server.set_response(
        "/apis/k10s.example.com/v1alpha1",
        200,
        r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"k10s.example.com/v1alpha1","resources":[
          {"name":"gadgets","singularName":"gadget","namespaced":true,"kind":"Gadget","verbs":["get","list","watch","create","update","patch","delete"]},
          {"name":"widgets","singularName":"widget","namespaced":false,"kind":"Widget","verbs":["get","list"]}
        ]}"#,
    );
    assert!(adapter.invalidate_discovery("mock-cluster"));

    let data = resource_types(types_for(&adapter, "mock-cluster").await);
    assert_eq!(
        server.hit_count("/apis"),
        after_first + 1,
        "invalidated cache re-discovers"
    );
    assert!(data.find_kind("Widget").is_some(), "refresh sees new kinds");

    // Invalidating twice or for unknown contexts reports honestly.
    assert!(!adapter.invalidate_discovery("never-seen-context"));
}

#[tokio::test]
async fn discovery_refreshes_after_ttl_expiry() {
    tokio::time::pause();
    let (adapter, server) = adapter_with_recorded_context();

    resource_types(types_for(&adapter, "mock-cluster").await);
    let after_first = server.hit_count("/apis");
    assert_eq!(after_first, 1);

    // Move past the documented catalog TTL.
    tokio::time::advance(k10s_backend::DISCOVERY_TTL + std::time::Duration::from_secs(1)).await;

    let data = resource_types(types_for(&adapter, "mock-cluster").await);
    assert!(
        server.hit_count("/apis") > after_first,
        "expired catalog re-discovers"
    );
    // The refreshed catalog stays normalized and complete.
    assert!(data.find_kind("Deployment").is_some());
}

#[tokio::test]
async fn cache_evicts_oldest_contexts_when_full() {
    let mut contexts = Vec::new();
    let mut servers: Vec<RecordedApiServer> = Vec::new();
    let mut clients: Vec<(String, kube::Client)> = Vec::new();
    for index in 0..k10s_backend::MAX_CACHED_CONTEXTS + 3 {
        let name = format!("ctx-{index}");
        contexts.push(ContextInfo {
            name: name.clone(),
            cluster: format!("cluster-{index}"),
            namespace: None,
            is_current: index == 0,
        });
        let server = RecordedApiServer::standard();
        clients.push((name.clone(), server.clone().into_client("default")));
        servers.push(server);
    }

    let adapter = KubeAdapter::with_cluster_clients(contexts, clients)
        .expect("adapter builds for many contexts");

    // Fill the bounded cache by discovering every context.
    for index in 0..servers.len() {
        resource_types(types_for(&adapter, &format!("ctx-{index}")).await);
    }

    // The context bound evicts the oldest catalog: ctx-0 holds no entry.
    assert!(
        !adapter.invalidate_discovery("ctx-0"),
        "oldest catalog must be evicted by the context bound"
    );
    resource_types(types_for(&adapter, "ctx-0").await);
    assert_eq!(
        servers[0].hit_count("/apis"),
        2,
        "evicted context re-discovers instead of serving stale data"
    );
}
