//! Ignored live-cluster verification for the complete Plan 3 read path.
//!
//! Run `tests/kind/cluster.sh up` first, then:
//! `cargo test --locked -p k10s-backend --test kind_read_path -- --ignored --nocapture`.

use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use k10s_backend::{
    BackendError, BackendEvent, Gvk, KubeAdapter, KubernetesAccess, PermissionProbe, Query,
    QueryResult, ResourceRef, Subscribe,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn kubeconfig() -> PathBuf {
    std::env::var_os("K10S_KIND_KUBECONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/.kubeconfig")
        })
}

fn gvk(group: &str, version: &str, kind: &str) -> Gvk {
    Gvk {
        group: group.into(),
        version: version.into(),
        kind: kind.into(),
    }
}

fn admin_context() -> String {
    let cluster_name =
        std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".to_owned());
    format!("kind-{cluster_name}")
}

fn kubectl_status<S: AsRef<OsStr>>(args: &[S]) -> bool {
    Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args(args)
        .status()
        .expect("kubectl must be installed by the kind harness")
        .success()
}

fn kubectl<S: AsRef<OsStr> + Debug>(args: &[S]) {
    let status = kubectl_status(args);
    assert!(status, "kubectl failed: {args:?}");
}

fn command_output(program: &str, args: &[&str]) -> String {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(output.status.success(), "{program} failed: {args:?}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

struct MetricsTlsFixture {
    directory: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MetricsTlsFixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

async fn start_metrics_tls_fixture(pod_name: &str) -> MetricsTlsFixture {
    let directory = std::env::temp_dir().join(format!("k10s-metrics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let cert = directory.join("tls.crt");
    let key = directory.join("tls.key");
    let status = Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .args(["-subj", "/CN=k10s-metrics-api", "-days", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("openssl is required for the optional metrics fixture");
    assert!(
        status.success(),
        "openssl could not create the fixture certificate"
    );

    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(&cert).unwrap()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let private_key = rustls_pemfile::private_key(&mut BufReader::new(File::open(&key).unwrap()))
        .unwrap()
        .unwrap();
    let tls = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(tls));
    let listener = TcpListener::bind("0.0.0.0:9443").await.unwrap();
    let pod_name = pod_name.to_owned();
    let timestamp = command_output("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]);
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let pod_name = pod_name.clone();
            let timestamp = timestamp.clone();
            tokio::spawn(async move {
                let Ok(mut stream) = acceptor.accept(stream).await else {
                    return;
                };
                let mut request = vec![0; 8192];
                let Ok(read) = stream.read(&mut request).await else {
                    return;
                };
                let request = String::from_utf8_lossy(&request[..read]);
                let path = request.split_whitespace().nth(1).unwrap_or_default();
                let body = if path.ends_with("/nodes") {
                    r#"{"apiVersion":"metrics.k8s.io/v1beta1","kind":"NodeMetricsList","items":[]}"#
                        .to_owned()
                } else if path.ends_with("/pods") {
                    format!(
                        r#"{{"apiVersion":"metrics.k8s.io/v1beta1","kind":"PodMetricsList","items":[{{"metadata":{{"name":"{pod_name}","namespace":"k10s-read"}},"timestamp":"{timestamp}","window":"30s","containers":[{{"name":"web","usage":{{"cpu":"25m"}}}}]}}]}}"#
                    )
                } else {
                    r#"{"apiVersion":"v1","groupVersion":"metrics.k8s.io/v1beta1","kind":"APIResourceList","resources":[{"name":"nodes","namespaced":false,"kind":"NodeMetrics","verbs":["get","list"]},{"name":"pods","namespaced":true,"kind":"PodMetrics","verbs":["get","list"]}]}"#.to_owned()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    MetricsTlsFixture { directory, task }
}

fn metrics_fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/metrics-fixture.yaml")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn live_kind_cluster_serves_the_complete_normalized_read_path() {
    let adapter = KubeAdapter::from_kubeconfig(Some(&kubeconfig()))
        .expect("the generated kind kubeconfig is valid")
        .with_metrics_timing(Duration::from_secs(2), Duration::from_millis(200));

    let QueryResult::Bootstrap(bootstrap) = adapter.query(Query::Bootstrap).await.unwrap() else {
        panic!("bootstrap query returned the wrong result");
    };
    assert!(
        bootstrap
            .contexts
            .iter()
            .any(|context| context.name == "k10s-limited" && context.is_current),
        "the generated least-privilege context must be current: {:?}",
        bootstrap.contexts
    );
    assert!(
        bootstrap
            .contexts
            .iter()
            .all(|context| !context.name.contains("dev-local")),
        "live Kube mode must never leak fake fixture contexts"
    );

    let QueryResult::ResourceTypes(types) = adapter
        .query(Query::ResourceTypes {
            context: "k10s-limited".into(),
        })
        .await
        .unwrap()
    else {
        panic!("resource-types query returned the wrong result");
    };
    assert!(types.find_kind("Pod").is_some());
    assert!(types.find_kind("Deployment").is_some());
    assert!(
        types
            .types
            .iter()
            .any(|entry| entry.gvk == gvk("example.k10s.io", "v1", "Widget")),
        "the fixture CRD must be discovered"
    );

    let QueryResult::ResourceList(pods) = adapter
        .query(Query::ResourceList {
            context: "k10s-limited".into(),
            gvk: gvk("", "v1", "Pod"),
            namespace: Some("k10s-read".into()),
        })
        .await
        .unwrap()
    else {
        panic!("pod list returned the wrong result");
    };
    assert_eq!(pods.rows.len(), 2, "the deployment owns two fixture pods");
    assert!(
        pods.rows
            .iter()
            .all(|row| row.reference.context == "k10s-limited")
    );

    let forbidden = adapter
        .query(Query::ResourceList {
            context: "k10s-limited".into(),
            gvk: gvk("", "v1", "Pod"),
            namespace: Some("k10s-forbidden".into()),
        })
        .await;
    assert_eq!(forbidden.unwrap_err(), BackendError::Forbidden);

    let QueryResult::ResourceList(deployments) = adapter
        .query(Query::ResourceList {
            context: "k10s-limited".into(),
            gvk: gvk("apps", "v1", "Deployment"),
            namespace: Some("k10s-read".into()),
        })
        .await
        .unwrap()
    else {
        panic!("deployment list returned the wrong result");
    };
    let deployment = deployments
        .rows
        .iter()
        .find(|row| row.reference.name == "read-path-web")
        .expect("fixture deployment is listed")
        .reference
        .clone();
    let QueryResult::ResourceDetail(detail) = adapter
        .query(Query::ResourceDetail {
            reference: deployment.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("detail query returned the wrong result");
    };
    assert!(detail.manifest.contains("read-path-web"));
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.reason == "FixtureReady"),
        "normalized detail must include the deterministic Event"
    );

    let QueryResult::ResourceRelations(relations) = adapter
        .query(Query::ResourceRelations {
            reference: deployment,
        })
        .await
        .unwrap()
    else {
        panic!("relations query returned the wrong result");
    };
    assert!(
        relations
            .groups
            .iter()
            .any(|group| group.gvk.kind == "ReplicaSet")
    );
    assert!(relations.groups.iter().any(|group| group.gvk.kind == "Pod"));

    let QueryResult::ContextPermissions(permissions) = adapter
        .query(Query::ContextPermissions {
            context: "k10s-limited".into(),
            probes: vec![
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-read".into()),
                },
                PermissionProbe {
                    verb: "list".into(),
                    resource: "pods".into(),
                    group: None,
                    namespace: Some("k10s-forbidden".into()),
                },
            ],
        })
        .await
        .unwrap()
    else {
        panic!("permission query returned the wrong result");
    };
    assert_eq!(permissions.checks.len(), 2);
    assert_ne!(permissions.checks[0].outcome, permissions.checks[1].outcome);

    // The Metrics API is optional. Without metrics-server, the adapter must
    // return honest missing values rather than manufacturing zero usage.
    let QueryResult::ResourceMetrics(metrics) = adapter
        .query(Query::ResourceMetrics {
            reference: ResourceRef {
                context: "k10s-limited".into(),
                gvk: gvk("", "v1", "Pod"),
                namespace: Some("k10s-read".into()),
                name: pods.rows[0].reference.name.clone(),
                uid: pods.rows[0].reference.uid.clone(),
            },
        })
        .await
        .unwrap()
    else {
        panic!("metrics query returned the wrong result");
    };
    assert!(metrics.cpu_millicores.is_none());
    assert!(metrics.memory_bytes.is_none());

    // Install the optional aggregated API only after proving absence. Its TLS
    // responder publishes a fresh CPU-only sample for this exact live pod.
    let metrics_server = start_metrics_tls_fixture(&pods.rows[0].reference.name).await;
    let gateways = command_output(
        "docker",
        &[
            "network",
            "inspect",
            "kind",
            "--format",
            "{{range .IPAM.Config}}{{println .Gateway}}{{end}}",
        ],
    );
    let gateway = gateways
        .lines()
        .find(|address| address.contains('.'))
        .expect("the kind bridge exposes an IPv4 gateway");
    kubectl(&[
        "apply".to_owned(),
        "-f".to_owned(),
        metrics_fixture_path().display().to_string(),
    ]);
    kubectl(&[
        "-n".to_owned(),
        "k10s-read".to_owned(),
        "patch".to_owned(),
        "endpoints/k10s-metrics-api".to_owned(),
        "--type=json".to_owned(),
        "-p".to_owned(),
        format!(r#"[{{"op":"replace","path":"/subsets/0/addresses/0/ip","value":"{gateway}"}}]"#),
    ]);
    kubectl(&[
        "wait",
        "--for=condition=Available",
        "apiservice/v1beta1.metrics.k8s.io",
        "--timeout=60s",
    ]);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let QueryResult::ResourceMetrics(partial) = adapter
                .query(Query::ResourceMetrics {
                    reference: pods.rows[0].reference.clone(),
                })
                .await
                .unwrap()
            else {
                panic!("partial metrics query returned the wrong result");
            };
            if partial.cpu_millicores == Some(25) {
                assert!(partial.memory_bytes.is_none());
                assert!(partial.collected_at.is_some());
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the optional API produces a fresh partial sample");
    kubectl(&[
        "delete".to_owned(),
        "-f".to_owned(),
        metrics_fixture_path().display().to_string(),
        "--ignore-not-found".to_owned(),
    ]);
    drop(metrics_server);

    let mut watch = adapter
        .subscribe(Subscribe::ResourceWatch {
            context: "k10s-limited".into(),
            gvk: gvk("", "v1", "Pod"),
            namespace: Some("k10s-read".into()),
            identity: None,
        })
        .await
        .unwrap();
    let mut events = watch.take_events().expect("resource watches carry events");
    let first = tokio::time::timeout(Duration::from_secs(30), events.recv())
        .await
        .expect("initial snapshot arrives")
        .unwrap();
    assert!(matches!(first, BackendEvent::Snapshot(_)));

    kubectl(&[
        "-n",
        "k10s-read",
        "scale",
        "deployment/read-path-web",
        "--replicas=3",
    ]);
    let changed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if matches!(events.recv().await.unwrap(), BackendEvent::Changed(_)) {
                break;
            }
        }
    })
    .await;
    assert!(changed.is_ok(), "the live watch observes an applied pod");

    let old_pod = &pods.rows[0].reference.name;
    kubectl(&["-n", "k10s-read", "delete", "pod", old_pod, "--wait=false"]);
    let gone = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let BackendEvent::Gone { reference, .. } = events.recv().await.unwrap()
                && reference.name == *old_pod
            {
                break;
            }
        }
    })
    .await;
    assert!(
        gone.is_ok(),
        "the live watch observes deletion and recovery"
    );

    // Restart the control-plane container to sever the underlying HTTP watch.
    // The supervisor must reconnect, relist, and publish a recovery snapshot
    // through this same subscription rather than requiring a resubscribe.
    let cluster_name =
        std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".to_owned());
    command_output(
        "docker",
        &["restart", &format!("{cluster_name}-control-plane")],
    );
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if kubectl_status(&["get", "--raw=/readyz"]) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("the kind control plane recovers");
    let recovery = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if matches!(events.recv().await.unwrap(), BackendEvent::Snapshot(_)) {
                break;
            }
        }
    })
    .await;
    assert!(
        recovery.is_ok(),
        "the same subscription relists after its watch connection is severed"
    );

    // Leave the shared fixture deterministic for the server and desktop
    // gates that run after this mutation-heavy watch test.
    kubectl(&[
        "-n",
        "k10s-read",
        "scale",
        "deployment/read-path-web",
        "--replicas=2",
    ]);
    kubectl(&[
        "-n",
        "k10s-read",
        "rollout",
        "status",
        "deployment/read-path-web",
        "--timeout=120s",
    ]);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let QueryResult::ResourceList(restored) = adapter
                .query(Query::ResourceList {
                    context: "k10s-limited".into(),
                    gvk: gvk("", "v1", "Pod"),
                    namespace: Some("k10s-read".into()),
                })
                .await
                .unwrap()
            else {
                panic!("restoration pod list returned the wrong result");
            };
            if restored.rows.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("fixture returns to exactly two pods");
}
