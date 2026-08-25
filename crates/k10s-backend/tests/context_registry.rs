//! Context registry loading and prepare-then-commit guarantees.
//!
//! The real kube-rs adapter must expose contexts as credential-free summaries:
//! no tokens, client certificates, or keys may survive the mapping from a raw
//! kubeconfig into the committed [`ContextRegistry`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use k10s_backend::{
    AdapterError, BackendError, ContextAvailability, ContextInfo, ContextRegistry, KubeAdapter,
    KubernetesAccess, Query, QueryResult,
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

#[cfg(unix)]
fn exec_plugin_fixture(
    current_succeeds: bool,
    fallback_succeeds: bool,
) -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let seed = write_fixture("seed", "");
    let dir = seed.parent().expect("fixture has parent");
    let current_counter = dir.join("current.count");
    let fallback_counter = dir.join("fallback.count");

    let write_plugin = |name: &str, counter: &Path, succeeds: bool| {
        let path = dir.join(name);
        let outcome = if succeeds {
            r#"printf '%s\n' '{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"token":"fixture-token"}}'"#
                .to_owned()
        } else {
            "echo 'fixture plugin denied' >&2\nexit 17".to_owned()
        };
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\ncount=0\n[ ! -f '{counter}' ] || count=$(cat '{counter}')\ncount=$((count + 1))\nprintf '%s' \"$count\" > '{counter}'\n{outcome}\n",
                counter = counter.display()
            ),
        )
        .expect("plugin fixture writes");
        let mut permissions = std::fs::metadata(&path)
            .expect("plugin metadata reads")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).expect("plugin becomes executable");
        path
    };

    let current_plugin = write_plugin("current-plugin.sh", &current_counter, current_succeeds);
    let fallback_plugin = write_plugin("fallback-plugin.sh", &fallback_counter, fallback_succeeds);
    let kubeconfig = dir.join("config");
    std::fs::write(
        &kubeconfig,
        format!(
            r#"apiVersion: v1
kind: Config
current-context: current
clusters:
- name: cluster
  cluster:
    server: https://127.0.0.1:9
    insecure-skip-tls-verify: true
contexts:
- name: current
  context:
    cluster: cluster
    user: current-user
- name: fallback
  context:
    cluster: cluster
    user: fallback-user
users:
- name: current-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: /bin/sh
      args: [{current_plugin}]
      interactiveMode: Never
- name: fallback-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: /bin/sh
      args: [{fallback_plugin}]
      interactiveMode: Never
"#,
            current_plugin = current_plugin.display(),
            fallback_plugin = fallback_plugin.display(),
        ),
    )
    .expect("exec kubeconfig writes");
    (kubeconfig, current_counter, fallback_counter)
}

#[cfg(unix)]
fn expiring_exec_plugin_fixture() -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let seed = write_fixture("expiring-seed", "");
    let dir = seed.parent().expect("fixture has parent");
    let counter = dir.join("expiring.count");
    let return_nonrefreshable = dir.join("return-nonrefreshable");
    let plugin = dir.join("expiring-plugin.sh");
    std::fs::write(
        &plugin,
        format!(
            "#!/bin/sh\ncount=0\n[ ! -f '{counter}' ] || count=$(cat '{counter}')\ncount=$((count + 1))\nprintf '%s' \"$count\" > '{counter}'\nmode=''\n[ ! -f '{mode}' ] || mode=$(cat '{mode}')\nif [ \"$mode\" = 'nonrefreshable' ]; then\n  printf '%s\\n' '{{\"apiVersion\":\"client.authentication.k8s.io/v1\",\"kind\":\"ExecCredential\",\"status\":{{\"token\":\"replacement-token\"}}}}'\nelif [ \"$mode\" = 'invalid-expiration' ]; then\n  printf '%s\\n' '{{\"apiVersion\":\"client.authentication.k8s.io/v1\",\"kind\":\"ExecCredential\",\"status\":{{\"token\":\"invalid-token\",\"expirationTimestamp\":\"not-a-timestamp\"}}}}'\nelse\n  printf '%s\\n' '{{\"apiVersion\":\"client.authentication.k8s.io/v1\",\"kind\":\"ExecCredential\",\"status\":{{\"token\":\"expired-token\",\"expirationTimestamp\":\"1970-01-01T00:00:00Z\"}}}}'\nfi\n",
            counter = counter.display(),
            mode = return_nonrefreshable.display(),
        ),
    )
    .expect("expiring plugin writes");
    let mut permissions = std::fs::metadata(&plugin)
        .expect("plugin metadata reads")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&plugin, permissions).expect("plugin becomes executable");
    let kubeconfig = dir.join("expiring-config");
    std::fs::write(
        &kubeconfig,
        format!(
            r#"apiVersion: v1
kind: Config
current-context: expiring
clusters:
- name: cluster
  cluster:
    server: https://127.0.0.1:9
    insecure-skip-tls-verify: true
contexts:
- name: expiring
  context:
    cluster: cluster
    user: expiring-user
users:
- name: expiring-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: /bin/sh
      args: [{plugin}]
      interactiveMode: Never
"#,
            plugin = plugin.display(),
        ),
    )
    .expect("expiring kubeconfig writes");
    (kubeconfig, counter, return_nonrefreshable)
}

#[cfg(unix)]
fn invocation_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
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
async fn exec_plugin_context_is_accepted_for_lazy_validation() {
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
    let adapter = KubeAdapter::from_kubeconfig(Some(&path))
        .expect("exec credential helpers are accepted and validated lazily");
    drop(adapter);
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
#[cfg(unix)]
async fn bootstrap_runs_only_current_exec_plugin() {
    let (path, current_counter, fallback_counter) = exec_plugin_fixture(true, false);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");
    assert_eq!(invocation_count(&current_counter), 0);
    assert_eq!(invocation_count(&fallback_counter), 0);

    let contexts = bootstrap_contexts(&adapter).await;

    let current_invocations = invocation_count(&current_counter);
    assert!(current_invocations > 0);
    assert_eq!(invocation_count(&fallback_counter), 0);
    assert_eq!(contexts[0].availability, ContextAvailability::Available);
    assert!(contexts[0].is_current);
    assert_eq!(contexts[1].availability, ContextAvailability::Unknown);

    let refreshed = bootstrap_contexts(&adapter).await;
    assert_eq!(invocation_count(&current_counter), current_invocations);
    assert_eq!(invocation_count(&fallback_counter), 0);
    assert_eq!(refreshed, contexts);
}

#[tokio::test]
#[cfg(unix)]
async fn failed_current_exec_falls_back_without_failing_bootstrap() {
    let (path, current_counter, fallback_counter) = exec_plugin_fixture(false, true);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");
    assert_eq!(invocation_count(&current_counter), 0);
    assert_eq!(invocation_count(&fallback_counter), 0);

    let contexts = bootstrap_contexts(&adapter).await;

    assert_eq!(invocation_count(&current_counter), 1);
    assert!(invocation_count(&fallback_counter) > 0);
    assert_eq!(contexts[0].availability, ContextAvailability::Unavailable);
    assert!(
        contexts[0]
            .unavailable_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("fixture plugin denied"))
    );
    assert!(!contexts[0].is_current);
    assert_eq!(contexts[1].availability, ContextAvailability::Available);
    assert!(contexts[1].is_current);
}

#[tokio::test]
#[cfg(unix)]
async fn all_exec_failures_remain_visible_without_current_context() {
    let (path, current_counter, fallback_counter) = exec_plugin_fixture(false, false);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");

    let contexts = bootstrap_contexts(&adapter).await;

    assert_eq!(invocation_count(&current_counter), 1);
    assert_eq!(invocation_count(&fallback_counter), 1);
    assert_eq!(contexts.len(), 2);
    assert!(
        contexts
            .iter()
            .all(|context| context.availability == ContextAvailability::Unavailable)
    );
    assert!(contexts.iter().all(|context| !context.is_current));
}

#[tokio::test]
#[cfg(unix)]
async fn non_current_exec_is_lazy_and_failed_selection_stays_disabled() {
    let (path, current_counter, fallback_counter) = exec_plugin_fixture(true, false);
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");
    let initial = bootstrap_contexts(&adapter).await;
    assert!(initial[0].is_current);
    assert_eq!(invocation_count(&fallback_counter), 0);
    let current_invocations = invocation_count(&current_counter);

    let error = adapter
        .query(Query::ContextSwitch {
            to: "fallback".into(),
        })
        .await
        .expect_err("failed plugin prevents the switch without exiting");
    assert!(matches!(
        error,
        BackendError::ContextUnavailable { context, reason }
            if context == "fallback" && reason.contains("fixture plugin denied")
    ));
    assert_eq!(invocation_count(&fallback_counter), 1);

    // Bootstrap is also Refresh: the disabled context is retried, remains
    // visible, and cannot displace the still-available current context.
    let refreshed = bootstrap_contexts(&adapter).await;
    assert_eq!(invocation_count(&fallback_counter), 2);
    assert_eq!(invocation_count(&current_counter), current_invocations);
    assert!(refreshed[0].is_current);
    assert_eq!(refreshed[1].availability, ContextAvailability::Unavailable);
    assert!(!refreshed[1].is_current);
}

#[tokio::test]
#[cfg(unix)]
async fn expired_exec_credential_failure_disables_once() {
    let (path, counter, return_nonrefreshable) = expiring_exec_plugin_fixture();
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");
    let contexts = bootstrap_contexts(&adapter).await;
    assert_eq!(contexts[0].availability, ContextAvailability::Available);
    let initial_invocations = invocation_count(&counter);
    assert!(initial_invocations > 0);
    std::fs::write(&return_nonrefreshable, "nonrefreshable").expect("fixture mode changes");

    let first = adapter
        .query(Query::ResourceTypes {
            context: "expiring".into(),
        })
        .await
        .expect_err("unrefreshable exec response disables the context");
    assert!(matches!(
        first,
        BackendError::ContextUnavailable { ref context, .. } if context == "expiring"
    ));
    let failed_invocations = invocation_count(&counter);
    assert_eq!(failed_invocations, initial_invocations + 1);

    let second = adapter
        .query(Query::ResourceTypes {
            context: "expiring".into(),
        })
        .await
        .expect_err("later requests are blocked without rerunning the plugin");
    assert!(matches!(
        second,
        BackendError::ContextUnavailable { ref context, .. } if context == "expiring"
    ));
    assert_eq!(invocation_count(&counter), failed_invocations);
}

#[tokio::test]
#[cfg(unix)]
async fn malformed_exec_expiration_is_context_unavailable() {
    let (path, _counter, mode) = expiring_exec_plugin_fixture();
    std::fs::write(mode, "invalid-expiration").expect("fixture mode changes");
    let adapter = KubeAdapter::from_kubeconfig(Some(&path)).expect("adapter constructs");

    let contexts = bootstrap_contexts(&adapter).await;

    assert_eq!(contexts[0].availability, ContextAvailability::Unavailable);
    assert!(
        contexts[0]
            .unavailable_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("invalid expiration timestamp"))
    );
    assert!(!contexts[0].is_current);
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

fn availability_registry() -> ContextRegistry {
    ContextRegistry::prepare(vec![
        ContextInfo::available("first", "cluster-a", None, true),
        ContextInfo {
            availability: ContextAvailability::Unknown,
            ..ContextInfo::available("second", "cluster-b", None, false)
        },
        ContextInfo::available("third", "cluster-c", None, false),
    ])
    .expect("availability registry prepares")
}

#[test]
fn availability_transitions_are_generation_checked_and_normalized() {
    let mut registry = availability_registry();
    let (generation, snapshot) = registry.snapshot();
    assert_eq!(generation, 0);
    assert_eq!(snapshot.len(), 3);

    assert!(registry.mark_unavailable(generation, "first", "plugin failed".into()));
    let (next_generation, snapshot) = registry.snapshot();
    assert_eq!(next_generation, generation + 1);
    assert_eq!(snapshot[0].availability, ContextAvailability::Unavailable);
    assert_eq!(
        snapshot[0].unavailable_reason.as_deref(),
        Some("plugin failed")
    );

    assert!(!registry.mark_available(generation, "first"));
    assert!(registry.mark_available(next_generation, "first"));
    let (_, snapshot) = registry.snapshot();
    assert_eq!(snapshot[0].availability, ContextAvailability::Available);
    assert_eq!(snapshot[0].unavailable_reason, None);
}

#[test]
fn unavailable_switch_is_typed_and_available_fallback_is_stable() {
    let mut registry = availability_registry();
    let (generation, _) = registry.snapshot();
    assert!(registry.mark_unavailable(generation, "first", "auth denied".into()));

    assert!(matches!(
        registry.prepare_switch("first"),
        Err(BackendError::ContextUnavailable { context, reason })
            if context == "first" && reason == "auth denied"
    ));

    assert_eq!(
        registry.choose_available_fallback().as_deref(),
        Some("third")
    );
    assert_eq!(
        registry
            .contexts()
            .iter()
            .find(|context| context.is_current)
            .map(|context| context.name.as_str()),
        Some("third")
    );
    assert_eq!(registry.context_names(), ["first", "second", "third"]);
}

#[test]
fn fallback_clears_current_when_no_context_is_available() {
    let mut registry = availability_registry();
    let (generation, _) = registry.snapshot();
    assert!(registry.mark_unavailable(generation, "first", "failed".into()));
    let (generation, _) = registry.snapshot();
    assert!(registry.mark_unavailable(generation, "third", "failed".into()));

    assert_eq!(registry.choose_available_fallback(), None);
    assert!(
        registry
            .contexts()
            .iter()
            .all(|context| !context.is_current)
    );
}
