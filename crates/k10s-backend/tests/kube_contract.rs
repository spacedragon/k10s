//! Shared bootstrap/context wire contract for the fake and kube-rs adapters.
//!
//! Both adapters feed the same kernel, so they must produce structurally
//! identical `BootstrapResponse` payloads: same keys, same context shape, one
//! current context — with no credentials in either payload.

use std::io::Write;
use std::path::PathBuf;

use k10s_backend::{BackendKernel, FakeKubernetes, KernelQueryResult, KubeAdapter, Query};
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
