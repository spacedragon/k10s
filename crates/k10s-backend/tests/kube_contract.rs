//! Shared bootstrap/context wire contract for the fake and kube-rs adapters.
//!
//! Both adapters feed the same kernel, so they must produce structurally
//! identical `BootstrapResponse` payloads: same keys, same context shape, one
//! current context — with no credentials in either payload.

use std::io::Write;
use std::path::PathBuf;

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendKernel, ContextInfo, FakeKubernetes, KernelQueryResult, KubeAdapter, Query,
};
use serde_json::Value;

/// Real-kubeconfig fixture carrying credential material that must never reach
/// the wire.
const KUBECONFIG_YAML: &str = r#"apiVersion: v1
kind: Config
current-context: fixture-beta
clusters:
- name: alpha-cluster
  cluster:
    server: https://alpha.example.internal:6443
- name: beta-cluster
  cluster:
    server: https://beta.example.internal:6443
contexts:
- name: fixture-alpha
  context:
    cluster: alpha-cluster
    user: alpha-user
- name: fixture-beta
  context:
    cluster: beta-cluster
    namespace: production
    user: beta-user
users:
- name: alpha-user
  user:
    token: CONTRACT-TOKEN-MARKER-k10s-5d8e2b
- name: beta-user
  user:
    client-certificate-data: Q09OVFJBQ1QtQ0VSVC1NQVJLRVIta2EwczUtZDhlMmI=
"#;

fn bootstrap_wire_payload(adapter: impl k10s_backend::KubernetesAccess + 'static) -> Value {
    let kernel = BackendKernel::new_with_instance_id(adapter, "contract-instance");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime builds");
    match runtime
        .block_on(kernel.query(Query::Bootstrap))
        .expect("bootstrap succeeds")
    {
        KernelQueryResult::Bootstrap(bootstrap) => {
            serde_json::to_value(bootstrap.wire_payload()).expect("payload serializes")
        }
        other => panic!("kernel must map bootstrap into its wire payload, got {other:?}"),
    }
}

/// Pin the shared wire shape one adapter must produce; ignores values.
fn assert_bootstrap_shape(payload: &Value, label: &str) {
    let keys = payload
        .as_object()
        .unwrap_or_else(|| panic!("{label}: payload is an object"))
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        ["capabilities", "contexts", "protocol", "server"]
            .map(str::to_owned)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "{label}: top-level keys drifted"
    );
    let context_keys = payload["contexts"]
        .as_array()
        .unwrap_or_else(|| panic!("{label}: contexts must be an array"))
        .iter()
        .flat_map(|context| {
            context
                .as_object()
                .expect("contexts are objects")
                .keys()
                .cloned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    // Every context shares the required keys (camelCase on the wire); namespace
    // is optional, and no credential-bearing key may ever appear.
    assert!(
        ["availability", "cluster", "isCurrent", "name"]
            .iter()
            .all(|key| context_keys.contains(*key)),
        "{label}: contexts lost a required key: {context_keys:?}"
    );
    let allowed: std::collections::BTreeSet<String> = [
        "availability",
        "cluster",
        "isCurrent",
        "name",
        "namespace",
        "unavailableReason",
    ]
    .map(str::to_owned)
    .into_iter()
    .collect();
    assert!(
        context_keys.is_subset(&allowed),
        "{label}: contexts expose unexpected keys: {context_keys:?}"
    );
}

#[test]
fn fake_and_kube_adapters_agree_on_bootstrap_shape() {
    let fake_payload = bootstrap_wire_payload(FakeKubernetes::standard());

    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "k10s-kube-contract-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    let path: PathBuf = dir.join("kubeconfig");
    let mut file = std::fs::File::create(&path).expect("fixture file creates");
    Write::write_all(&mut file, KUBECONFIG_YAML.as_bytes()).expect("fixture yaml writes");

    let kube_adapter = KubeAdapter::from_kubeconfig(Some(&path))
        .expect("fixture kubeconfig loads through the real adapter seam");
    let kube_payload = bootstrap_wire_payload(kube_adapter);
    std::fs::remove_file(&path).ok();

    // Both adapters produce the same wire shape with safe keys only.
    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        assert_bootstrap_shape(payload, label);
    }

    // The shared protocol validator accepts both payloads.
    k10s_protocol::validate_bootstrap_response(&fake_payload)
        .expect("fake payload passes the shared validator");
    k10s_protocol::validate_bootstrap_response(&kube_payload)
        .expect("kube payload passes the shared validator");

    // Exactly one current context on either adapter.
    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        let contexts = payload["contexts"]
            .as_array()
            .unwrap_or_else(|| panic!("{label} payload must carry contexts"));
        assert!(!contexts.is_empty(), "{label} payload must not be empty");
        let currents = contexts
            .iter()
            .filter(|context| context["isCurrent"].as_bool().unwrap_or(false))
            .count();
        assert_eq!(
            currents, 1,
            "{label} adapter must mark exactly one current context"
        );
    }

    // The kube payload never exposes the fixture's credential material.
    let serialized = kube_payload.to_string();
    for marker in [
        "CONTRACT-TOKEN-MARKER-k10s-5d8e2b",
        "Q09OVFJBQ1QtQ0VSVC1NQVJLRVIta2EwczUtZDhlMmI=",
    ] {
        assert!(
            !serialized.contains(marker),
            "credential material leaked: {marker}"
        );
    }
}

/// Shared resource-types wire payload one adapter must produce; ignores values.
async fn resource_types_wire_payload(kernel: BackendKernel, context: &str) -> Value {
    match kernel
        .query(Query::ResourceTypes {
            context: context.into(),
        })
        .await
        .expect("resource types succeed")
    {
        KernelQueryResult::ResourceTypes(types) => {
            serde_json::to_value(types.wire_payload()).expect("payload serializes")
        }
        other => panic!("kernel must map discovery into its wire payload, got {other:?}"),
    }
}

/// Pin the shared resource-types wire shape; ignores values.
fn assert_resource_types_shape(payload: &Value, label: &str) {
    let keys = payload
        .as_object()
        .unwrap_or_else(|| panic!("{label}: payload is an object"))
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        ["context", "types"]
            .map(str::to_owned)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "{label}: top-level keys drifted"
    );
    let types = payload["types"]
        .as_array()
        .unwrap_or_else(|| panic!("{label}: types must be an array"));
    assert!(!types.is_empty(), "{label}: catalog must not be empty");

    // Every entry shares the normalized camelCase shape; nothing else may appear.
    let allowed: std::collections::BTreeSet<String> = ["gvk", "namespaced"]
        .map(str::to_owned)
        .into_iter()
        .collect();
    for entry in types {
        assert!(
            entry
                .as_object()
                .expect("entries are objects")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                == allowed,
            "{label}: entry keys drifted: {entry:?}"
        );
    }

    // Raw kube-rs discovery vocabulary never crosses the seam.
    let wire = payload.to_string();
    for marker in ["APIResourceList", "singularName", "verbs"] {
        assert!(
            !wire.contains(marker),
            "{label}: raw discovery leaked: {marker}"
        );
    }
}

#[tokio::test]
async fn fake_and_kube_adapters_agree_on_resource_types_shape() {
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");

    // The real adapter answers from a recorded tower-level Kubernetes service.
    // Client construction and the query both run inside this test's runtime,
    // as kube-rs requires for raising its client stack.
    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds around the recorded server");

    let fake_payload = resource_types_wire_payload(fake_kernel, "dev-local").await;
    let kube_kernel = BackendKernel::new_with_instance_id(kube_adapter, "contract-instance");
    let kube_payload = resource_types_wire_payload(kube_kernel, "contract-mock").await;

    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        assert_resource_types_shape(payload, label);
    }
}

/// Shared resource-list golden contract: the fake adapter's stored rows and
/// the real kube adapter (driven by a recorded pod list cut) must map onto
/// the same wire shape through the kernel — same keys, same row projection,
/// and no opaque Kubernetes resourceVersion anywhere.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_resource_list_shape() {
    use k10s_backend::{Gvk, Query};

    let pods_gvk = Gvk::core("v1", "Pod");
    let recorded_pod_list = r#"{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"41"},"items":[
      {"metadata":{"name":"web","uid":"uid-web","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"app":"web"}},
       "status":{"phase":"Running"}}
    ]}"#;

    async fn list_wire_payload(kernel: &BackendKernel, context: &str, gvk: &Gvk) -> Value {
        let namespace = Some("default".to_owned());
        match kernel
            .query(Query::ResourceList {
                context: context.into(),
                gvk: gvk.clone(),
                namespace,
            })
            .await
            .expect("resource list succeeds")
        {
            KernelQueryResult::ResourceList(result) => {
                serde_json::to_value(result.wire_payload()).expect("payload serializes")
            }
            other => panic!("kernel must map the list into its wire payload, got {other:?}"),
        }
    }

    // Fake side: the standard dataset lists its pods.
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");
    let fake_payload = list_wire_payload(&fake_kernel, "dev-local", &pods_gvk).await;

    // Real side: one recorded pod list cut served by the tower-level fixture.
    let server = RecordedApiServer::standard();
    server.set_response("/api/v1/namespaces/default/pods", 200, recorded_pod_list);
    let client = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);
    let kube_payload = list_wire_payload(&kube_kernel, "contract-mock", &pods_gvk).await;

    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        let keys = payload
            .as_object()
            .unwrap_or_else(|| panic!("{label}: payload is an object"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            [
                "capabilities",
                "context",
                "generatedAt",
                "gvk",
                "namespace",
                "revision",
                "rows"
            ]
            .map(str::to_owned)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
            "{label}: resource-list top-level keys drifted"
        );
        let text = payload.to_string();
        assert!(!text.contains("resourceVersion"), "{label}: rv leaked");
        assert!(!text.contains("resource_version"), "{label}: rv leaked");
    }

    // Row-level shape parity (row counts differ by dataset design).
    let fake_rows = fake_payload["rows"].as_array().unwrap();
    let kube_rows = kube_payload["rows"].as_array().unwrap();
    assert!(!fake_rows.is_empty() && !kube_rows.is_empty());
    let row_keys = |row: &Value| {
        row.as_object()
            .unwrap_or_else(|| panic!("row is an object: {row:?}"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };
    for kube_row in kube_rows {
        assert_eq!(
            row_keys(&fake_rows[0]),
            row_keys(kube_row),
            "row keys drifted between adapters"
        );
        let identity_keys = |row: &Value| {
            row["identity"]
                .as_object()
                .expect("identity is an object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(identity_keys(&fake_rows[0]), identity_keys(kube_row));
    }

    // Both adapters produce honest summaries on the same projection: the
    // kube pod carries the phase from its recorded status.
    assert_eq!(kube_rows[0]["summary"], "Running");
}

/// Shared resource-detail wire contract: the fake adapter's stored records
/// and the real kube adapter (driven by a recorded Deployment → ReplicaSet →
/// Pod cut) must map onto the same detail wire shape through the kernel —
/// identical top-level keys, section shapes, related-group shapes, event-row
/// shapes, and no opaque Kubernetes resourceVersion anywhere.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_resource_detail_shape() {
    use k10s_backend::{Gvk, ResourceRef};

    let deployments_gvk = Gvk::new("apps", "v1", "Deployment");

    // Real side: one recorded cut serving the deployment, its replicaset,
    // its pod, and both event API variants.
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/apps/v1/namespaces/default/deployments/web",
        200,
        r#"{"kind":"Deployment","apiVersion":"apps/v1","metadata":{"name":"web","namespace":"default","uid":"uid-kube-web","resourceVersion":"41","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"app":"web"}},"spec":{"replicas":2},"status":{"readyReplicas":2}}"#,
    );
    server.set_response(
        "/apis/apps/v1/namespaces/default/replicasets",
        200,
        r#"{"kind":"ReplicaSetList","apiVersion":"apps/v1","metadata":{"resourceVersion":"42"},"items":[
          {"metadata":{"name":"web-rs","namespace":"default","uid":"uid-kube-rs","creationTimestamp":"2026-08-21T00:01:00Z",
           "ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"web","uid":"uid-kube-web","controller":true}]}}
        ]}"#,
    );
    server.set_response(
        "/api/v1/namespaces/default/pods",
        200,
        r#"{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"43"},"items":[
          {"metadata":{"name":"web-pod","namespace":"default","uid":"uid-kube-pod","creationTimestamp":"2026-08-21T00:02:00Z",
           "ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"web-rs","uid":"uid-kube-rs","controller":true}]},
           "status":{"phase":"Running"}}
        ]}"#,
    );
    server.set_response(
        "/api/v1/namespaces/default/events",
        200,
        r#"{"kind":"EventList","apiVersion":"v1","metadata":{"resourceVersion":"44"},"items":[
          {"metadata":{"name":"ev.1","namespace":"default","uid":"uid-ev"},"involvedObject":{"kind":"Deployment","name":"web","namespace":"default","uid":"uid-kube-web"},"reason":"ScalingReplicaSet","message":"Scaled up replica set","count":1,"lastTimestamp":"2026-08-21T00:01:00Z"}
        ]}"#,
    );

    async fn detail_wire_payload(kernel: &BackendKernel, reference: ResourceRef) -> Value {
        match kernel.query(Query::ResourceDetail { reference }).await {
            Ok(KernelQueryResult::ResourceDetail(result)) => {
                serde_json::to_value(result.wire_payload()).expect("payload serializes")
            }
            other => panic!("kernel must map the detail into its wire payload, got {other:?}"),
        }
    }

    // Fake side: standard dataset's deployment detail, resolved through the
    // same kernel composition (related rows + events + manifest).
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");
    let fake_payload = detail_wire_payload(
        &fake_kernel,
        ResourceRef {
            context: "dev-local".into(),
            gvk: deployments_gvk.clone(),
            namespace: Some("default".into()),
            name: "web-frontend".into(),
            uid: "uid-dev-local-deployment-default-web-frontend".into(),
        },
    )
    .await;

    let client = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);
    let kube_payload = detail_wire_payload(
        &kube_kernel,
        ResourceRef {
            context: "contract-mock".into(),
            gvk: deployments_gvk.clone(),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-kube-web".into(),
        },
    )
    .await;

    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        let keys = payload
            .as_object()
            .unwrap_or_else(|| panic!("{label}: payload is an object"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            [
                "capabilities",
                "createdAt",
                "events",
                "identity",
                "manifest",
                "ownerReferences",
                "related",
                "revision",
                "sections"
            ]
            .map(str::to_owned)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
            "{label}: resource-detail top-level keys drifted"
        );
        // Section and row shapes agree across adapters.
        let sections = payload["sections"].as_array().unwrap();
        assert!(!sections.is_empty(), "{label}: details carry sections");
        for section in sections {
            let section_keys = section
                .as_object()
                .expect("section is an object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                section_keys,
                ["rows", "title"].map(str::to_owned).into_iter().collect(),
                "{label}: section keys drifted"
            );
        }
        for group in payload["related"].as_array().unwrap() {
            let group_keys = group
                .as_object()
                .expect("related group is an object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                group_keys,
                ["gvk", "rows", "title"]
                    .map(str::to_owned)
                    .into_iter()
                    .collect(),
                "{label}: related-group keys drifted"
            );
        }
        for event in payload["events"].as_array().unwrap() {
            let event_keys = event
                .as_object()
                .expect("event is an object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                event_keys,
                ["count", "lastSeen", "message", "reason"]
                    .map(str::to_owned)
                    .into_iter()
                    .collect(),
                "{label}: event keys drifted"
            );
        }
        // The opaque Kubernetes resourceVersion never crosses either wire —
        // only inside the backend-rendered manifest is it bound to the UID.
        let text = payload.to_string();
        assert!(!text.contains("\"resourceVersion\""), "{label}: rv leaked");
        assert!(text.contains("manifest"), "{label}: manifest missing");
    }

    // The kube side resolves the traversal with honest recorded data.
    let related = kube_payload["related"].as_array().unwrap();
    assert!(
        related.iter().any(|group| group["gvk"]["kind"] == "Pod"),
        "the kube traversal reaches pods transitively"
    );
}

/// Shared resource-metrics wire contract: the fake adapter's stored samples
/// and the real kube adapter (driven by a recorded metrics.k8s.io cut) must
/// map onto the same availability-gated wire shape through the kernel —
/// identical top-level keys, identical per-case availability, and absent or
/// withheld values that never degrade into zeroes on either wire.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_resource_metrics_shape() {
    use k10s_backend::{Gvk, ResourceRef};

    /// RFC 3339 UTC timestamp `age_secs` seconds before the test ran, so the
    /// recorded metrics cut stays inside the freshness window.
    fn recent_rfc3339(age_secs: u64) -> String {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock advances")
            .as_secs()
            - age_secs;
        let days = secs / 86_400;
        let secs_of_day = secs % 86_400;
        let z = days as i64 + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100 + yoe / 400);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        format!(
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
            secs_of_day / 3_600,
            (secs_of_day % 3_600) / 60,
            secs_of_day % 60
        )
    }

    async fn metrics_wire(kernel: &BackendKernel, reference: ResourceRef) -> Value {
        match kernel.query(Query::ResourceMetrics { reference }).await {
            Ok(KernelQueryResult::ResourceMetrics(result)) => {
                serde_json::to_value(result.wire_payload()).expect("payload serializes")
            }
            other => panic!("kernel must map metrics into its wire payload, got {other:?}"),
        }
    }

    // Fake side: one fully sampled pod, one partially sampled pod, and one
    // existing pod with no stored sample at all.
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");
    fn fake_pod(name: &str) -> ResourceRef {
        ResourceRef {
            context: "dev-local".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-dev-local-pod-default-{name}"),
        }
    }
    let fake_full = metrics_wire(&fake_kernel, fake_pod("web-frontend-7d9f8-00001")).await;
    let fake_partial = metrics_wire(&fake_kernel, fake_pod("api-server-5cc4d-qw8rt")).await;
    let fake_absent = metrics_wire(&fake_kernel, fake_pod("db-postgres-0")).await;

    // Real side: the same three roles served from one recorded cut, sampled
    // moments ago so the freshness window keeps the values servable.
    let sampled_at = recent_rfc3339(5);
    let server = RecordedApiServer::standard();
    server.set_response(
        "/api/v1/nodes",
        200,
        r#"{"kind":"NodeList","apiVersion":"v1","metadata":{"resourceVersion":"100"},"items":[
      {"metadata":{"name":"node-a","uid":"uid-node-a"},"status":{"allocatable":{"pods":"110"}}}
    ]}"#,
    );
    server.set_response(
        "/apis/metrics.k8s.io/v1beta1/nodes",
        200,
        &format!(
            r#"{{"kind":"NodeMetricsList","apiVersion":"metrics.k8s.io/v1beta1","metadata":{{}},"items":[
      {{"metadata":{{"name":"node-a"}},"timestamp":"{sampled_at}","window":"30s","usage":{{"cpu":"1250m","memory":"123456Ki"}}}}
    ]}}"#
        ),
    );
    server.set_response(
        "/apis/metrics.k8s.io/v1beta1/pods",
        200,
        &format!(
            r#"{{"kind":"PodMetricsList","apiVersion":"metrics.k8s.io/v1beta1","metadata":{{}},"items":[
      {{"metadata":{{"name":"web","namespace":"default"}},"timestamp":"{sampled_at}","window":"30s",
       "containers":[{{"name":"app","usage":{{"cpu":"220m","memory":"134217728Ki"}}}}]}},
      {{"metadata":{{"name":"half","namespace":"default"}},"timestamp":"{sampled_at}","window":"30s",
       "containers":[{{"name":"app","usage":{{"cpu":"90m"}}}}]}}
    ]}}"#
        ),
    );
    for (name, uid) in [
        ("web", "uid-kube-web"),
        ("half", "uid-kube-half"),
        ("idle", "uid-kube-idle"),
    ] {
        server.set_response(
            &format!("/api/v1/namespaces/default/pods/{name}"),
            200,
            &format!(
                r#"{{"kind":"Pod","apiVersion":"v1","metadata":{{"name":"{name}","namespace":"default","uid":"{uid}","resourceVersion":"41","creationTimestamp":"2026-08-21T00:00:00Z"}},"status":{{"phase":"Running"}}}}"#
            ),
        );
    }
    let client = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);
    fn kube_pod(name: &str, uid: &str) -> ResourceRef {
        ResourceRef {
            context: "contract-mock".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: uid.into(),
        }
    }
    let kube_full = metrics_wire(&kube_kernel, kube_pod("web", "uid-kube-web")).await;
    let kube_partial = metrics_wire(&kube_kernel, kube_pod("half", "uid-kube-half")).await;
    let kube_absent = metrics_wire(&kube_kernel, kube_pod("idle", "uid-kube-idle")).await;

    let metric_keys = |payload: &Value| {
        payload["metrics"]
            .as_object()
            .unwrap_or_else(|| panic!("metrics is an object: {payload:?}"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };

    for (label, fake, kube) in [
        ("full", &fake_full, &kube_full),
        ("partial", &fake_partial, &kube_partial),
        ("absent", &fake_absent, &kube_absent),
    ] {
        // Top-level wire shape agrees across adapters.
        for payload in [fake, kube] {
            let keys = payload
                .as_object()
                .expect("payload is an object")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                keys,
                ["identity", "metrics"]
                    .map(str::to_owned)
                    .into_iter()
                    .collect(),
                "{label}: resource-metrics top-level keys drifted"
            );
        }
        // Per-case availability agrees, and the present-key sets match.
        assert_eq!(
            metric_keys(fake),
            metric_keys(kube),
            "{label}: key sets drifted"
        );
        assert_eq!(
            fake["metrics"]["availability"], kube["metrics"]["availability"],
            "{label}: availability drifted"
        );
        // The opaque Kubernetes vocabulary never crosses either wire.
        let text = payload_text(&[fake, kube]);
        assert!(!text.contains("resourceVersion"), "{label}: rv leaked");
    }

    assert_eq!(fake_full["metrics"]["availability"], "available");
    assert_eq!(fake_partial["metrics"]["availability"], "partial");
    assert_eq!(fake_absent["metrics"]["availability"], "unavailable");

    // Absent samples carry no fabricated value keys on either adapter.
    assert_eq!(
        metric_keys(&fake_absent),
        ["availability"].map(str::to_owned).into_iter().collect()
    );
    assert_eq!(
        metric_keys(&kube_absent),
        ["availability"].map(str::to_owned).into_iter().collect()
    );
}

fn payload_text(payloads: &[&Value]) -> String {
    payloads
        .iter()
        .map(|payload| payload.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Shared resource-watch wire contract: the fake adapter and the real kube
/// adapter (driven by a fully scripted watch source, no cluster) must
/// normalize identical backend types, so the kernel maps both onto the same
/// snapshot-page and delta payloads with no raw Kubernetes vocabulary.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_resource_watch_shape() {
    use std::sync::{Arc, Mutex as StdMutex};

    use k10s_backend::runtime::{ListedState, WatchRow, WatchSource, WatchUpdate};
    use k10s_backend::{BackendEvent, Gvk, ResourceRef, Subscribe};

    fn pods_gvk() -> Gvk {
        Gvk::core("v1", "Pod")
    }

    #[derive(Debug)]
    struct ContractSource {
        /// Updates flushed into the stream as soon as it attaches.
        updates: StdMutex<Vec<WatchUpdate>>,
    }

    impl WatchSource for ContractSource {
        fn list<'a>(
            &'a self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ListedState, String>> + Send + 'a>,
        > {
            Box::pin(async move {
                Ok(ListedState {
                    resource_version: "41".into(),
                    rows: vec![WatchRow {
                        reference: ResourceRef {
                            context: "contract-mock".into(),
                            gvk: pods_gvk(),
                            namespace: Some("default".into()),
                            name: "web".into(),
                            uid: "uid-web".into(),
                        },
                        labels: [("app".into(), "web".into())].into_iter().collect(),
                        summary: String::new(),
                        created_at: "2026-08-21T00:00:00Z".into(),
                        owner_references: Vec::new(),
                    }],
                })
            })
        }

        fn attach_watch<'a>(
            &'a self,
            _resource_version: String,
            out: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                for update in self
                    .updates
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .drain(..)
                {
                    let _ = out.send(update);
                }
                std::future::pending::<()>().await;
            })
        }
    }

    /// Collect one snapshot plus two deltas and map them through the kernel
    /// into their wire payloads.
    async fn wire_events<A: k10s_backend::KubernetesAccess + 'static>(
        adapter: A,
        context: &str,
        drive_change: impl FnOnce(),
    ) -> Vec<Value> {
        let kernel = BackendKernel::new(adapter);
        let mut handle = kernel
            .subscribe(Subscribe::ResourceWatch {
                context: context.into(),
                gvk: pods_gvk(),
                namespace: Some("default".into()),
            })
            .await
            .expect("resource watch subscribes");
        let mut events = handle.take_events().expect("watches carry events");

        let mut payloads = Vec::new();
        // Snapshot first.
        match tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
            .await
            .expect("snapshot arrives")
            .expect("channel open")
        {
            BackendEvent::Snapshot(data) => {
                let page = kernel.snapshot_page(data.revision, &data.rows);
                payloads.push(serde_json::to_value(&page).expect("snapshot page serializes"));
            }
            other => panic!("first event must be the snapshot, got {other:?}"),
        }
        // Drive one changed and one gone delta.
        drive_change();
        for _ in 0..2 {
            match tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .expect("delta arrives")
                .expect("channel open")
            {
                BackendEvent::Changed(record) => {
                    let delta = kernel.changed_delta(&record);
                    payloads.push(serde_json::to_value(&delta).expect("delta serializes"));
                }
                BackendEvent::Gone {
                    reference,
                    revision,
                } => {
                    let delta = kernel.gone_delta(&reference, revision);
                    payloads.push(serde_json::to_value(&delta).expect("gone serializes"));
                }
                other => panic!("unexpected event {other:?}"),
            }
        }
        payloads
    }

    // Fake side: standard dataset, then touch and delete through its seams.
    let fake = FakeKubernetes::standard();
    let fake_payloads = wire_events(fake.clone(), "dev-local", || {
        fake.touch_resource(
            "dev-local",
            &pods_gvk(),
            Some("default"),
            "web-frontend-7d9f8-00001",
        )
        .expect("touched pod exists");
        fake.delete_resource(
            "dev-local",
            &pods_gvk(),
            Some("default"),
            "api-server-5cc4d-qw8rt",
        );
    })
    .await;

    // Kube side: fully scripted source emitting the same observable shape.
    let server = RecordedApiServer::standard();
    let client = server.clone().into_client("default");
    let source: Arc<dyn WatchSource> = Arc::new(ContractSource {
        updates: StdMutex::new(vec![
            WatchUpdate::Upsert(WatchRow {
                reference: ResourceRef {
                    context: "contract-mock".into(),
                    gvk: pods_gvk(),
                    namespace: Some("default".into()),
                    name: "web".into(),
                    uid: "uid-web".into(),
                },
                labels: [("app".into(), "web".into())].into_iter().collect(),
                summary: "CrashLoopBackOff".into(),
                created_at: "2026-08-21T00:00:00Z".into(),
                owner_references: Vec::new(),
            }),
            WatchUpdate::Delete(ResourceRef {
                context: "contract-mock".into(),
                gvk: pods_gvk(),
                namespace: Some("default".into()),
                name: "web".into(),
                uid: "uid-web".into(),
            }),
        ]),
    });
    let scripted: k10s_backend::runtime::RuntimeWatchScript =
        Arc::new(move |_gvk, _namespace| Some(Arc::clone(&source)));
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds")
    .with_scripted_watches(scripted);
    let kube_payloads = wire_events(kube_adapter, "contract-mock", || {}).await;

    assert_eq!(fake_payloads.len(), 3);
    assert_eq!(kube_payloads.len(), 3);

    // Identical normalized key shapes on every payload pair.
    fn keys(value: &Value) -> std::collections::BTreeSet<String> {
        value
            .as_object()
            .unwrap_or_else(|| panic!("payload is an object: {value:?}"))
            .keys()
            .cloned()
            .collect()
    }
    for (fake_event, kube_event) in fake_payloads.iter().zip(kube_payloads.iter()) {
        assert_eq!(
            keys(fake_event),
            keys(kube_event),
            "event payload keys drifted"
        );
    }
    // Row-level shape parity on the snapshot pages (row counts differ by
    // dataset design; the wire shape must not).
    let fake_rows = fake_payloads[0]["rows"].as_array().unwrap();
    let kube_rows = kube_payloads[0]["rows"].as_array().unwrap();
    assert!(fake_rows.len() > 1 && kube_rows.len() == 1);
    let row_keys = |row: &Value| {
        row.as_object()
            .unwrap_or_else(|| panic!("row is an object: {row:?}"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(row_keys(&fake_rows[0]), row_keys(&kube_rows[0]));
    let identity_keys = |row: &Value| {
        row["identity"]
            .as_object()
            .unwrap_or_else(|| panic!("identity is an object"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(identity_keys(&fake_rows[0]), identity_keys(&kube_rows[0]));

    // The opaque Kubernetes resourceVersion never reaches either wire.
    for payload in fake_payloads.iter().chain(kube_payloads.iter()) {
        let text = payload.to_string();
        assert!(!text.contains("resourceVersion"));
        assert!(!text.contains("resource_version"));
    }
}

/// Shared context-switch wire contract: both adapters commit a validated
/// switch through the same kernel mapping onto an identical wire shape, and
/// both keep exactly one current context afterwards.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_context_switch_shape() {
    async fn switch_wire(kernel: &BackendKernel, to: &str) -> Value {
        match kernel
            .query(Query::ContextSwitch { to: to.to_owned() })
            .await
            .expect("validated switch commits")
        {
            KernelQueryResult::ContextSwitch(result) => {
                serde_json::to_value(result.wire_payload()).expect("payload serializes")
            }
            other => panic!("kernel must map the switch into its wire payload, got {other:?}"),
        }
    }

    async fn current_of(kernel: &BackendKernel) -> Vec<String> {
        let payload = match kernel
            .query(Query::Bootstrap)
            .await
            .expect("bootstrap works")
        {
            KernelQueryResult::Bootstrap(bootstrap) => bootstrap.wire_payload(),
            other => panic!("kernel must map bootstrap, got {other:?}"),
        };
        payload
            .contexts
            .iter()
            .filter(|context| context.is_current)
            .map(|context| context.name.clone())
            .collect()
    }

    // Fake side: switching from the standard world's current context.
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");
    let fake_payload = switch_wire(&fake_kernel, "prod-readonly").await;
    assert_eq!(current_of(&fake_kernel).await, vec!["prod-readonly"]);

    // Real side: two contexts backed by one recorded standard API server,
    // so the destination's prepare discovers successfully.
    let server = RecordedApiServer::standard();
    let client_a = server.clone().into_client("default");
    let client_b = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![
            ContextInfo {
                name: "contract-a".into(),
                cluster: "recorded-apiserver".into(),
                namespace: Some("default".into()),
                is_current: true,
                availability: k10s_protocol::ContextAvailability::Available,
                unavailable_reason: None,
            },
            ContextInfo {
                name: "contract-b".into(),
                cluster: "recorded-apiserver".into(),
                namespace: Some("default".into()),
                is_current: false,
                availability: k10s_protocol::ContextAvailability::Available,
                unavailable_reason: None,
            },
        ],
        [("contract-a", client_a), ("contract-b", client_b)],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);
    let kube_payload = switch_wire(&kube_kernel, "contract-b").await;

    for (label, origin, destination, payload) in [
        ("fake", "dev-local", "prod-readonly", &fake_payload),
        ("kube", "contract-a", "contract-b", &kube_payload),
    ] {
        let keys = payload
            .as_object()
            .unwrap_or_else(|| panic!("{label}: payload is an object"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            ["current", "previous"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
            "{label}: context-switch top-level keys drifted"
        );
        assert_eq!(payload["current"], destination);
        assert_eq!(payload["previous"], origin);
    }

    // Exactly one current context survives the commit on both adapters.
    assert_eq!(current_of(&kube_kernel).await, vec!["contract-b"]);
}

/// Shared advisory-permissions wire contract: both adapters normalize the
/// same probes onto identical check-entry shapes. Verdict values differ by
/// design — the fake serves no authorization truth and stays explicitly
/// Unknown, while the real adapter reports what its review answered.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_context_permissions_shape() {
    use k10s_backend::PermissionProbe;

    fn probe(verb: &str, resource: &str, namespace: Option<&str>) -> PermissionProbe {
        PermissionProbe {
            verb: verb.into(),
            resource: resource.into(),
            group: None,
            namespace: namespace.map(str::to_owned),
        }
    }

    async fn permissions_wire(kernel: &BackendKernel, context: &str) -> Value {
        let mut probes = vec![
            probe("list", "pods", Some("default")),
            probe("delete", "deployments", Some("production")),
        ];
        // The grouped resource reviews its own API group, never the core
        // group default.
        probes[1].group = Some("apps".into());
        match kernel
            .query(Query::ContextPermissions {
                context: context.into(),
                probes,
            })
            .await
            .expect("permission projection succeeds")
        {
            KernelQueryResult::ContextPermissions(result) => {
                serde_json::to_value(result.wire_payload()).expect("payload serializes")
            }
            other => panic!("kernel must map permissions into their wire payload, got {other:?}"),
        }
    }

    // Fake side: no fabricated verdicts.
    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");
    let fake_payload = permissions_wire(&fake_kernel, "dev-local").await;

    // Real side: one recorded SelfSubjectAccessReview answer for all probes.
    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        200,
        r#"{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",
            "metadata":{"creationTimestamp":null},
            "spec":{},"status":{"allowed":true,"reason":"RBAC: allowed by role binding"}}"#,
    );
    let client = server.clone().into_client("default");
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", client)],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);
    let kube_payload = permissions_wire(&kube_kernel, "contract-mock").await;

    for (label, payload) in [("fake", &fake_payload), ("kube", &kube_payload)] {
        let keys = payload
            .as_object()
            .unwrap_or_else(|| panic!("{label}: payload is an object"))
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            ["checks", "context"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
            "{label}: context-permissions top-level keys drifted"
        );
        let checks = payload["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 2, "{label}: every probe yields one check");
        // The core probe omits its group; the grouped probe echoes it.
        let entry_keys = |check: &Value| {
            check
                .as_object()
                .expect("check entries are objects")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(
            entry_keys(&checks[0]),
            ["namespace", "outcome", "resource", "verb"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
            "{label}: core check-entry keys drifted"
        );
        assert_eq!(
            entry_keys(&checks[1]),
            ["group", "namespace", "outcome", "resource", "verb"]
                .map(str::to_owned)
                .into_iter()
                .collect(),
            "{label}: grouped check-entry keys drifted"
        );
        for check in checks {
            let outcome = check["outcome"].as_str().expect("outcome is a string");
            assert!(
                matches!(outcome, "allowed" | "denied" | "unknown"),
                "{label}: outcome vocabulary drifted: {outcome}"
            );
        }
        // Probes echo verbatim, in order; the grouped probe carries its group.
        assert_eq!(checks[0]["verb"], "list");
        assert_eq!(checks[0]["resource"], "pods");
        assert_eq!(checks[0]["namespace"], "default");
        assert!(
            checks[0].get("group").is_none(),
            "core probes carry no group"
        );
        assert_eq!(checks[1]["verb"], "delete");
        assert_eq!(checks[1]["resource"], "deployments");
        assert_eq!(checks[1]["group"], "apps");
        assert_eq!(checks[1]["namespace"], "production");
    }

    // Each side is honest about what it knows.
    assert_eq!(fake_payload["checks"][0]["outcome"], "unknown");
    assert_eq!(fake_payload["checks"][1]["outcome"], "unknown");
    assert_eq!(kube_payload["checks"][0]["outcome"], "allowed");
    assert_eq!(kube_payload["checks"][1]["outcome"], "allowed");

    // Raw Kubernetes review vocabulary never crosses either wire.
    let wire = format!("{}\n{}", fake_payload, kube_payload);
    for marker in ["evaluationError", "selfsubjectaccessreviews"] {
        assert!(
            !wire.contains(marker),
            "{marker}: raw review vocabulary leaked onto the wire"
        );
    }
}

/// Shared permission-probe hardening contract: both adapters enforce the
/// identical probe bound with the same typed conflict, and duplicate probes
/// collapse onto their first occurrence in identical order.
#[tokio::test]
async fn fake_and_kube_adapters_agree_on_permission_probe_bound_and_duplicate_handling() {
    use k10s_backend::{BackendError, PermissionProbe};

    const SSAR_PATH: &str = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews";

    fn probe(verb: &str, resource: &str, namespace: Option<&str>) -> PermissionProbe {
        PermissionProbe {
            verb: verb.into(),
            resource: resource.into(),
            group: None,
            namespace: namespace.map(str::to_owned),
        }
    }

    async fn check_sequence(
        kernel: &BackendKernel,
        context: &str,
        probes: Vec<PermissionProbe>,
    ) -> Vec<(String, String)> {
        match kernel
            .query(Query::ContextPermissions {
                context: context.to_owned(),
                probes,
            })
            .await
            .expect("permission projection succeeds")
        {
            KernelQueryResult::ContextPermissions(data) => data
                .wire_payload()
                .checks
                .into_iter()
                .map(|check| (check.verb, check.resource))
                .collect(),
            other => panic!("kernel must map permissions into their wire payload, got {other:?}"),
        }
    }

    let fake_kernel =
        BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "contract-instance");

    let server = RecordedApiServer::standard();
    server.set_response(
        SSAR_PATH,
        200,
        r#"{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1",
            "metadata":{"creationTimestamp":null},
            "spec":{},"status":{"allowed":true,"reason":"RBAC: allowed by role binding"}}"#,
    );
    let kube_adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: "contract-mock".into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [("contract-mock", server.clone().into_client("default"))],
    )
    .expect("adapter builds around the recorded server");
    let kube_kernel = BackendKernel::new(kube_adapter);

    // The probe bound is identical on both sides: one past the limit is a
    // typed conflict carrying the same reason, never unbounded fan-out.
    let oversized: Vec<PermissionProbe> = (0..33)
        .map(|index| probe("list", &format!("kind{index}"), None))
        .collect();
    let expected =
        BackendError::Conflict("permission review requests carry at most 32 probes".into());
    let fake_error = fake_kernel
        .query(Query::ContextPermissions {
            context: "dev-local".into(),
            probes: oversized.clone(),
        })
        .await
        .expect_err("the fake adapter enforces the probe bound");
    let kube_error = kube_kernel
        .query(Query::ContextPermissions {
            context: "contract-mock".into(),
            probes: oversized,
        })
        .await
        .expect_err("the kube adapter enforces the probe bound");
    assert_eq!(fake_error, expected);
    assert_eq!(kube_error, expected);

    // Duplicate probes collapse onto their first occurrence in the same
    // first-seen order on both adapters; the real adapter additionally burns
    // exactly one review per distinct probe.
    let duplicated = vec![
        probe("list", "pods", Some("default")),
        probe("get", "nodes", None),
        probe("list", "pods", Some("default")),
        probe("delete", "pods", Some("default")),
        probe("get", "nodes", None),
    ];
    let expected_sequence = vec![
        ("list".to_owned(), "pods".to_owned()),
        ("get".to_owned(), "nodes".to_owned()),
        ("delete".to_owned(), "pods".to_owned()),
    ];
    let fake_checks = check_sequence(&fake_kernel, "dev-local", duplicated.clone()).await;
    assert_eq!(fake_checks, expected_sequence, "fake dedup drifted");
    let kube_checks = check_sequence(&kube_kernel, "contract-mock", duplicated).await;
    assert_eq!(kube_checks, expected_sequence, "kube dedup drifted");
    assert_eq!(kube_checks, fake_checks);
    assert_eq!(
        server.hit_count(SSAR_PATH),
        3,
        "identical probes share one review on the real adapter too"
    );
}
