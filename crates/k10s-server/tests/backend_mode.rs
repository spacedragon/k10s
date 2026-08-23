//! Runtime backend mode selection for standalone launches.
//!
//! Normal launches default to the real `Kube` adapter; fake is only ever
//! selected by an explicit operator request, and a broken kubeconfig never
//! silently degrades into demo data.

use std::path::{Path, PathBuf};

use k10s_backend::{AdapterError, BackendMode, KernelQueryResult, Query, build_kernel};

#[test]
fn normal_launches_default_to_kube_discovery() {
    assert_eq!(resolve(false, None), BackendMode::Kube { kubeconfig: None });
}

#[test]
fn fake_is_selected_only_when_explicitly_requested() {
    let explicit = PathBuf::from("/tmp/should-be-ignored");
    // The explicit flag is the development opt-in; a stray path never
    // overrides it into kube mode.
    assert_eq!(resolve(true, Some(&explicit)), BackendMode::Fake);
    assert_eq!(resolve(true, None), BackendMode::Fake);
}

#[test]
fn an_explicit_kubeconfig_path_flows_into_the_mode() {
    let path = PathBuf::from("/Users/dev/.kube/config");
    assert_eq!(
        resolve(false, Some(&path)),
        BackendMode::Kube {
            kubeconfig: Some(path)
        }
    );
}

/// The same mapping the standalone entry point uses at startup.
fn resolve(fake_requested: bool, kubeconfig_path: Option<&Path>) -> BackendMode {
    k10s_server::resolve_backend_mode(fake_requested, kubeconfig_path)
}

#[test]
fn a_missing_kubeconfig_file_fails_startup_instead_of_falling_back_to_fake() {
    let missing = PathBuf::from("/definitely/not/a/real/kubeconfig/path");
    let error = build_kernel(&BackendMode::Kube {
        kubeconfig: Some(missing.clone()),
    })
    .expect_err("a missing kubeconfig must be a clean startup failure, never fake data");

    assert_eq!(error, AdapterError::KubeconfigMissing(missing));
}

#[tokio::test]
async fn fake_mode_builds_a_working_fake_kernel() {
    let kernel = build_kernel(&BackendMode::Fake).expect("fake mode builds immediately");
    match kernel.query(Query::Bootstrap).await.unwrap() {
        KernelQueryResult::Bootstrap(bootstrap) => {
            assert_eq!(
                bootstrap.context_names(),
                ["dev-local", "prod-readonly"],
                "the fake demo dataset stays reachable through the factory"
            );
        }
        other => panic!("bootstrap expected, got {other:?}"),
    }
}
