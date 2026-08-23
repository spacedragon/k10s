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
        ["cluster", "isCurrent", "name"]
            .iter()
            .all(|key| context_keys.contains(*key)),
        "{label}: contexts lost a required key: {context_keys:?}"
    );
    let allowed: std::collections::BTreeSet<String> = ["cluster", "isCurrent", "name", "namespace"]
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
