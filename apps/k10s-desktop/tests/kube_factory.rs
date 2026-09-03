//! Desktop launcher backend factory selection.
//!
//! The embedded server must be built through the shared runtime factory — a
//! real kubeconfig flows into `Kube` mode end-to-end, and no direct kernel
//! shortcut may hand fake data to a production launch path.

use std::io::Write;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use k10s_backend::{AdapterError, BackendMode};
use k10s_desktop::{EmbeddedServerError, launch_embedded_server_with_mode};
use k10s_ui::client::ConnectTarget;
use k10s_ui::{AppView, K10sApp};

const KUBECONFIG_YAML: &str = r#"apiVersion: v1
kind: Config
current-context: desktop-beta
clusters:
- name: alpha-cluster
  cluster:
    server: https://alpha.example.internal:6443
- name: beta-cluster
  cluster:
    server: https://beta.example.internal:6443
contexts:
- name: desktop-alpha
  context:
    cluster: alpha-cluster
    user: alpha-user
- name: desktop-beta
  context:
    cluster: beta-cluster
    namespace: production
    user: beta-user
users:
- name: alpha-user
  user:
    token: DESKTOP-TOKEN-MARKER-k10s-e2f4a7
- name: beta-user
  user: {}
"#;

fn write_fixture(name: &str, yaml: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "k10s-desktop-factory-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("fixture dir creates");
    let path = dir.join(name);
    let mut file = std::fs::File::create(&path).expect("fixture file creates");
    Write::write_all(&mut file, yaml.as_bytes()).expect("fixture yaml writes");
    path
}

fn bootstrap_context_names(server: &k10s_desktop::EmbeddedServerHandle) -> Vec<String> {
    let target = ConnectTarget::new(server.control_url(), server.access_token());
    let mut app = K10sApp::connect(target).expect("shared websocket transport must start");
    let deadline = Instant::now() + Duration::from_secs(5);
    while matches!(app.view(), AppView::Connecting) && Instant::now() < deadline {
        app.poll();
        thread::sleep(Duration::from_millis(5));
    }
    match app.view() {
        AppView::Ready { context_names, .. } => {
            let names = context_names.clone();
            drop(app);
            names
        }
        other => panic!("app did not bootstrap: {other:?}"),
    }
}

#[test]
fn the_launcher_serves_real_kubeconfig_contexts_through_the_factory() {
    let path = write_fixture("kubeconfig", KUBECONFIG_YAML);
    let mut server = launch_embedded_server_with_mode(&BackendMode::Kube {
        kubeconfig: Some(path.clone()),
    })
    .expect("a valid explicit kubeconfig launches the embedded server");

    // These names come only from the fixture file — fake mode would report
    // dev-local/prod-readonly instead, so a passing check proves the real
    // adapter seam is wired through the launcher.
    let contexts = bootstrap_context_names(&server);
    assert_eq!(contexts, ["desktop-alpha", "desktop-beta"]);

    server.shutdown().expect("server stops cleanly");
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_missing_kubeconfig_fails_the_launch_without_an_implicit_fake_fallback() {
    let missing = PathBuf::from("/definitely/not/a/real/kubeconfig/path");
    let error = launch_embedded_server_with_mode(&BackendMode::Kube {
        kubeconfig: Some(missing.clone()),
    })
    .expect_err("a missing kubeconfig must fail the launch cleanly, never fall back to fake");

    assert!(
        matches!(&error, EmbeddedServerError::Backend(backend) if *backend == AdapterError::KubeconfigMissing(missing)),
        "expected a typed backend error for the missing file: {error:?}"
    );
}

#[test]
fn an_explicit_fake_mode_still_serves_the_offline_demo() {
    let mut server = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("explicit fake mode launches without any kubeconfig");

    assert_eq!(
        bootstrap_context_names(&server),
        ["dev-local", "prod-readonly"]
    );
    assert!(server.kubectl_launch_descriptor().is_none());

    server.shutdown().expect("server stops cleanly");
}
