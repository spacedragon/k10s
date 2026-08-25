//! Context registry loading and prepare-then-commit guarantees.
//!
//! The real kube-rs adapter must expose contexts as credential-free summaries:
//! no tokens, client certificates, or keys may survive the mapping from a raw
//! kubeconfig into the committed [`ContextRegistry`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use k10s_backend::{
    AdapterError, ContextInfo, ContextRegistry, KubeAdapter, KubernetesAccess, Query, QueryResult,
};

/// Distinctive markers embedded in fixture kubeconfigs so redaction failures
/// are exact substring checks instead of guesses.
const TOKEN_MARKER: &str = "TOKEN-MARKER-k10s-redact-7f3a9c";
const CA_MARKER_B64: &str = "Q0EtTUFSS0VSLWsxMHMtcmVkYWN0LWM5ZDI0ZQ==";
const CERT_MARKER_B64: &str = "Q0VSVC1NQVJLRVItazEwcy1yZWRhY3QtYjRlMjFk";
const KEY_MARKER_B64: &str = "S0VZLU1BUktFUi1rMTBzLXJlZGFjdC1hOGM1N2Y=";

/// A valid kubeconfig with two contexts, credential material on one user, and
/// `staging-web` selected as the current context.
const KUBECONFIG_YAML: &str = r#"apiVersion: v1
kind: Config
current-context: staging-web
clusters:
- name: dev-cluster
  cluster:
    server: https://dev.example.internal:6443
    certificate-authority-data: Q0EtTUFSS0VSLWsxMHMtcmVkYWN0LWM5ZDI0ZQ==
- name: staging-cluster
  cluster:
    server: https://staging.example.internal:6443
contexts:
- name: dev-local
  context:
    cluster: dev-cluster
    user: dev-user
    namespace: default
- name: staging-web
  context:
    cluster: staging-cluster
    user: staging-user
users:
- name: dev-user
  user:
    token: TOKEN-MARKER-k10s-redact-7f3a9c
    client-certificate-data: Q0VSVC1NQVJLRVItazEwcy1yZWRhY3QtYjRlMjFk
    client-key-data: S0VZLU1BUktFUi1rMTBzLXJlZGFjdC1hOGM1N2Y=
- name: staging-user
  user:
    client-certificate-data: Q0VSVC1NQVJLRVItazEwcy1yZWRhY3QtYjRlMjFk
"#;

fn write_fixture(name: &str, yaml: &str) -> PathBuf {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "k10s-context-registry-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    let path = dir.join(name);
    let mut file = std::fs::File::create(&path).expect("fixture file writes");
    file.write_all(yaml.as_bytes())
        .expect("fixture yaml writes");
    path
}

async fn bootstrap_contexts(adapter: &KubeAdapter) -> Vec<ContextInfo> {
    match adapter
        .query(Query::Bootstrap)
        .await
        .expect("bootstrap query succeeds")
    {
        QueryResult::Bootstrap(info) => info.contexts,
        other => panic!("bootstrap must return context summaries, got {other:?}"),
    }
}

#[tokio::test]
async fn enumerates_every_context_from_an_explicit_kubeconfig_path() {
    let path = write_fixture("kubeconfig", KUBECONFIG_YAML);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("explicit kubeconfig loads");

    let contexts = bootstrap_contexts(&adapter).await;
    assert_eq!(
        contexts
            .iter()
            .map(|context| context.name.as_str())
            .collect::<Vec<_>>(),
        ["dev-local", "staging-web"]
    );
    // Cluster references resolve to the named clusters, never to credentials.
    let dev = &contexts[0];
    assert_eq!(dev.cluster, "dev-cluster");
    assert_eq!(dev.namespace.as_deref(), Some("default"));
    let staging = &contexts[1];
    assert_eq!(staging.cluster, "staging-cluster");
    assert_eq!(staging.namespace, None);

    // The summary payload carries no credential material in any form.
    let serialized = serde_json::to_string(&contexts).expect("summaries serialize");
    for marker in [TOKEN_MARKER, CA_MARKER_B64, CERT_MARKER_B64, KEY_MARKER_B64] {
        assert!(
            !serialized.contains(marker),
            "credential material leaked: {marker}"
        );
    }
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn selects_the_configured_current_context() {
    let path = write_fixture("kubeconfig", KUBECONFIG_YAML);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("explicit kubeconfig loads");

    let contexts = bootstrap_contexts(&adapter).await;
    assert_eq!(
        contexts.iter().filter(|context| context.is_current).count(),
        1,
        "exactly one context is current"
    );
    assert!(contexts[1].is_current);
    assert!(!contexts[0].is_current);
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn missing_kubeconfig_files_report_a_clear_typed_error() {
    let missing = Path::new("/definitely/not/a/real/kubeconfig/path");
    let error = KubeAdapter::from_kubeconfig(Some(missing))
        .expect_err("a missing kubeconfig must fail cleanly, not panic or fall back");

    assert_eq!(
        error,
        AdapterError::KubeconfigMissing(missing.to_path_buf())
    );
    assert!(
        error.to_string().contains("not/a/real/kubeconfig"),
        "operators need the path in the message: {error}"
    );
}

#[tokio::test]
async fn exec_plugin_credentials_are_rejected_before_commit() {
    let yaml = r#"apiVersion: v1
kind: Config
current-context: aks-cluster
clusters:
- name: aks
  cluster:
    server: https://aks.example.com:443
contexts:
- name: aks-cluster
  context:
    cluster: aks
    user: aks-admin
users:
- name: aks-admin
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: aks-cli
      args: ["get-access-token"]
"#;
    let path = write_fixture("kubeconfig", yaml);
    let error = KubeAdapter::from_kubeconfig(Some(&path)).expect_err(
        "k10s must refuse to execute external credential helpers instead of committing them",
    );

    assert_eq!(
        error,
        AdapterError::ExecPluginRejected {
            context: "aks-cluster".into(),
            user: "aks-admin".into()
        }
    );
    std::fs::remove_file(&path).ok();
}

/// A parseable kubeconfig whose current context references an undefined
/// cluster must still fail startup instead of committing a dangling summary.
#[tokio::test]
async fn dangling_cluster_references_are_rejected_before_commit() {
    let yaml = r#"apiVersion: v1
kind: Config
current-context: broken-ctx
clusters:
- name: real-cluster
  cluster:
    server: https://real.example.internal:6443
contexts:
- name: broken-ctx
  context:
    cluster: does-not-exist"#;
    let path = write_fixture("kubeconfig", yaml);
    let error = KubeAdapter::from_kubeconfig(Some(&path))
        .expect_err("a context pointing at an undefined cluster must be rejected before commit");

    assert!(matches!(error, AdapterError::KubeconfigInvalid { .. }));
    assert!(
        error.to_string().contains("does-not-exist"),
        "operators need the dangling reference named: {error}"
    );
    std::fs::remove_file(&path).ok();
}

/// A selected cluster with no server URL is structurally unusable and must
/// fail cleanly rather than reach bootstrap.
#[tokio::test]
async fn clusters_without_a_server_url_are_rejected() {
    let yaml = r#"apiVersion: v1
kind: Config
current-context: no-url-ctx
clusters:
- name: url-less
  cluster:
    certificate-authority-data: Q0EtTUFSS0VSLWsxMHMtcmVkYWN0LWM5ZDI0ZQ==
contexts:
- name: no-url-ctx
  context:
    cluster: url-less"#;
    let path = write_fixture("kubeconfig", yaml);
    let error = KubeAdapter::from_kubeconfig(Some(&path))
        .expect_err("a cluster without a server URL must be rejected before commit");

    assert!(matches!(error, AdapterError::KubeconfigInvalid { .. }));
    assert!(
        error.to_string().to_lowercase().contains("server url"),
        "the missing URL must be named in the error: {error}"
    );
    std::fs::remove_file(&path).ok();
}

/// An unparseable server URL must fail cleanly, and its raw value (which may
/// carry userinfo credentials) must never leak into operator-facing errors.
#[tokio::test]
async fn unparseable_cluster_urls_are_rejected_without_leaking_credentials() {
    let yaml = r#"apiVersion: v1
kind: Config
current-context: bad-url-ctx
clusters:
- name: broken-cluster
  cluster:
    server: https://user:LEAKED-URL-PASSWORD-5f2e9a@not a url at all
contexts:
- name: bad-url-ctx
  context:
    cluster: broken-cluster"#;
    let path = write_fixture("kubeconfig", yaml);
    let error = KubeAdapter::from_kubeconfig(Some(&path))
        .expect_err("an unparseable server URL must be rejected before commit");

    assert!(matches!(error, AdapterError::KubeconfigInvalid { .. }));
    let rendered = error.to_string();
    assert!(
        !rendered.contains("LEAKED-URL-PASSWORD-5f2e9a"),
        "the raw URL embeds credentials and must not be echoed: {rendered}"
    );
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn prepare_commits_only_complete_registries() {
    let summaries = vec![
        ContextInfo {
            name: "first".into(),
            cluster: "cluster-a".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
        ContextInfo {
            name: "second".into(),
            cluster: "cluster-b".into(),
            namespace: None,
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
    ];

    let registry = ContextRegistry::prepare(summaries).expect("valid summaries commit");
    assert_eq!(registry.contexts().len(), 2);
    assert_eq!(registry.context_names(), ["first", "second"]);
    assert_eq!(registry.find("second").unwrap().cluster, "cluster-b");
    assert!(registry.find("absent").is_none());
}

#[test]
fn prepare_refuses_ambiguous_or_corrupt_registries() {
    let duplicate = vec![
        ContextInfo {
            name: "same".into(),
            cluster: "a".into(),
            namespace: None,
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
        ContextInfo {
            name: "same".into(),
            cluster: "b".into(),
            namespace: None,
            is_current: false,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
    ];
    assert!(matches!(
        ContextRegistry::prepare(duplicate),
        Err(AdapterError::InvalidContextSummaries { .. })
    ));

    let two_current = vec![
        ContextInfo {
            name: "a".into(),
            cluster: "x".into(),
            namespace: None,
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
        ContextInfo {
            name: "b".into(),
            cluster: "y".into(),
            namespace: None,
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        },
    ];
    assert!(matches!(
        ContextRegistry::prepare(two_current),
        Err(AdapterError::InvalidContextSummaries { .. })
    ));
}
