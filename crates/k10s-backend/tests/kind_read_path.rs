//! Ignored live-cluster verification for the complete Plan 3 read path.
//!
//! Run `tests/kind/cluster.sh up` first, then:
//! `cargo test --locked -p k10s-backend --test kind_read_path -- --ignored --nocapture`.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use k10s_backend::{
    BackendError, BackendEvent, Gvk, KubeAdapter, KubernetesAccess, PermissionProbe, Query,
    QueryResult, ResourceRef, Subscribe,
};

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

fn kubectl(args: &[&str]) {
    let cluster_name =
        std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".to_owned());
    let admin_context = format!("kind-{cluster_name}");
    let status = Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context)
        .args(args)
        .status()
        .expect("kubectl must be installed by the kind harness");
    assert!(status.success(), "kubectl failed: {args:?}");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn live_kind_cluster_serves_the_complete_normalized_read_path() {
    let adapter = KubeAdapter::from_kubeconfig(Some(&kubeconfig()))
        .expect("the generated kind kubeconfig is valid");

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
    assert!(
        metrics.cpu_millicores.is_none() || metrics.memory_bytes.is_some(),
        "partial metrics must never invent a CPU zero"
    );

    let mut watch = adapter
        .subscribe(Subscribe::ResourceWatch {
            context: "k10s-limited".into(),
            gvk: gvk("", "v1", "Pod"),
            namespace: Some("k10s-read".into()),
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
