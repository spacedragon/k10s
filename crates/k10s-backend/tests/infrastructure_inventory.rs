use k10s_backend::{
    ContextInfo, KubeAdapter, KubernetesAccess, Query, QueryResult, Subscribe,
    testkit::RecordedApiServer,
};

const CONTEXT: &str = "kind-bunyip";

fn adapter(server: &RecordedApiServer) -> KubeAdapter {
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo::available(CONTEXT, "bunyip", None, true)],
        [(CONTEXT, server.clone().into_client("default"))],
    )
    .unwrap()
}

fn set_empty_overview_lists(server: &RecordedApiServer) {
    for (path, body) in [
        (
            "/api/v1/nodes",
            r#"{"apiVersion":"v1","kind":"NodeList","items":[]}"#,
        ),
        (
            "/api/v1/pods",
            r#"{"apiVersion":"v1","kind":"PodList","items":[]}"#,
        ),
        (
            "/apis/apps/v1/deployments",
            r#"{"apiVersion":"apps/v1","kind":"DeploymentList","items":[]}"#,
        ),
        (
            "/apis/apps/v1/statefulsets",
            r#"{"apiVersion":"apps/v1","kind":"StatefulSetList","items":[]}"#,
        ),
        (
            "/apis/apps/v1/daemonsets",
            r#"{"apiVersion":"apps/v1","kind":"DaemonSetList","items":[]}"#,
        ),
        (
            "/apis/batch/v1/jobs",
            r#"{"apiVersion":"batch/v1","kind":"JobList","items":[]}"#,
        ),
        (
            "/apis/batch/v1/cronjobs",
            r#"{"apiVersion":"batch/v1","kind":"CronJobList","items":[]}"#,
        ),
    ] {
        server.set_response(path, 200, body);
    }
}

#[tokio::test]
async fn infrastructure_query_lists_bound_pvc_capacity_and_storage_class() {
    let server = RecordedApiServer::standard();
    set_empty_overview_lists(&server);
    server.set_response(
        "/api/v1/persistentvolumeclaims",
        200,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeClaimList","items":[{"metadata":{"name":"scratch","namespace":"demo","uid":"pvc-2","creationTimestamp":"2026-08-27T01:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"500M"}},"storageClassName":"standard"},"status":{"phase":"Pending"}},{"metadata":{"name":"tiny","namespace":"demo","uid":"pvc-3","creationTimestamp":"2026-08-27T02:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1e3"}},"storageClassName":"standard"},"status":{"phase":"Pending"}},{"metadata":{"name":"data","namespace":"demo","uid":"pvc-1","creationTimestamp":"2026-08-27T00:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}},"storageClassName":"standard","volumeName":"pv-data"},"status":{"phase":"Bound","capacity":{"storage":"1Gi"}}}]}"#,
    );
    server.set_response(
        "/api/v1/persistentvolumes",
        200,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeList","items":[{"metadata":{"name":"pv-free","uid":"pv-2","creationTimestamp":"2026-08-26T01:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"capacity":{"storage":"1e3"},"persistentVolumeReclaimPolicy":"Retain","storageClassName":"standard"},"status":{"phase":"Available"}},{"metadata":{"name":"pv-data","uid":"pv-1","creationTimestamp":"2026-08-26T00:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"capacity":{"storage":"1Gi"},"claimRef":{"name":"data","namespace":"demo"},"persistentVolumeReclaimPolicy":"Delete","storageClassName":"standard"},"status":{"phase":"Bound"}}]}"#,
    );
    server.set_response(
        "/apis/storage.k8s.io/v1/storageclasses",
        200,
        r#"{"apiVersion":"storage.k8s.io/v1","kind":"StorageClassList","items":[{"metadata":{"name":"standard","uid":"sc-1","creationTimestamp":"2026-08-25T00:00:00Z"},"provisioner":"example.com/csi","reclaimPolicy":"Delete","volumeBindingMode":"WaitForFirstConsumer"}]}"#,
    );

    let QueryResult::Infrastructure(snapshot) = adapter(&server)
        .query(Query::Infrastructure {
            context: CONTEXT.into(),
        })
        .await
        .unwrap()
    else {
        panic!("expected infrastructure snapshot");
    };
    let response = snapshot.into_protocol();
    let claim = &response.storage.persistent_volume_claims[0];
    assert_eq!(
        (&claim.namespace, &claim.name),
        (&"demo".to_owned(), &"data".to_owned())
    );
    assert_eq!(claim.status, "Bound");
    assert_eq!(claim.capacity, "1Gi");
    assert_eq!(claim.storage_class, "standard");
    assert_eq!(claim.bound_volume, "pv-data");
    assert!(!claim.age.contains('T'));

    let unbound_claim = &response.storage.persistent_volume_claims[1];
    assert_eq!(unbound_claim.name, "scratch");
    assert_eq!(unbound_claim.capacity, "500M");
    assert_eq!(unbound_claim.bound_volume, "—");

    let volume = &response.storage.persistent_volumes[0];
    assert_eq!(volume.name, "pv-data");
    assert_eq!(volume.status, "Bound");
    assert_eq!(volume.bound_claim, "demo/data");
    assert_eq!(volume.reclaim_policy, "Delete");
    assert!(!volume.age.contains('T'));

    let unbound_volume = &response.storage.persistent_volumes[1];
    assert_eq!(unbound_volume.name, "pv-free");
    assert_eq!(unbound_volume.bound_claim, "—");

    let class = &response.storage.storage_classes[0];
    assert_eq!(class.name, "standard");
    assert_eq!(class.provisioner, "example.com/csi");
    assert_eq!(class.volume_binding_mode, "WaitForFirstConsumer");
    assert!(!class.age.contains('T'));
    assert_eq!(
        response.totals.persistent_storage_bytes,
        (1 << 30) + 500_000_000 + 1_000
    );
    assert_eq!(response.launcher.network, 0);
    assert_eq!(response.launcher.config, 0);
    assert_eq!(response.launcher.storage, 6);
}

#[tokio::test]
async fn infrastructure_subscription_is_advertised_for_known_context() {
    let server = RecordedApiServer::standard();
    let handle = adapter(&server)
        .subscribe(Subscribe::Infrastructure {
            context: CONTEXT.into(),
        })
        .await
        .unwrap();
    assert_eq!(handle.id, "infrastructure:kind-bunyip");
}

#[tokio::test]
async fn infrastructure_query_and_subscription_reject_unknown_context() {
    let server = RecordedApiServer::standard();
    assert!(
        adapter(&server)
            .query(Query::Infrastructure {
                context: "missing".into(),
            })
            .await
            .is_err()
    );
    assert!(
        adapter(&server)
            .subscribe(Subscribe::Infrastructure {
                context: "missing".into(),
            })
            .await
            .is_err()
    );
}
