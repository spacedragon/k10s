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

#[tokio::test]
async fn infrastructure_query_lists_bound_pvc_capacity_and_storage_class() {
    let server = RecordedApiServer::default();
    server.set_response(
        "/api/v1/persistentvolumeclaims",
        200,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeClaimList","items":[{"metadata":{"name":"data","namespace":"demo","uid":"pvc-1","creationTimestamp":"2026-08-27T00:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}},"storageClassName":"standard","volumeName":"pv-data"},"status":{"phase":"Bound","capacity":{"storage":"1Gi"}}}]}"#,
    );
    server.set_response(
        "/api/v1/persistentvolumes",
        200,
        r#"{"apiVersion":"v1","kind":"PersistentVolumeList","items":[{"metadata":{"name":"pv-data","uid":"pv-1","creationTimestamp":"2026-08-26T00:00:00Z"},"spec":{"accessModes":["ReadWriteOnce"],"capacity":{"storage":"1Gi"},"claimRef":{"name":"data","namespace":"demo"},"persistentVolumeReclaimPolicy":"Delete","storageClassName":"standard"},"status":{"phase":"Bound"}}]}"#,
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

    let volume = &response.storage.persistent_volumes[0];
    assert_eq!(volume.name, "pv-data");
    assert_eq!(volume.status, "Bound");
    assert_eq!(volume.bound_claim, "demo/data");
    assert_eq!(volume.reclaim_policy, "Delete");

    let class = &response.storage.storage_classes[0];
    assert_eq!(class.name, "standard");
    assert_eq!(class.provisioner, "example.com/csi");
    assert_eq!(class.volume_binding_mode, "WaitForFirstConsumer");
    assert_eq!(response.totals.persistent_storage_bytes, 1 << 30);
}

#[tokio::test]
async fn infrastructure_subscription_is_advertised_for_known_context() {
    let server = RecordedApiServer::default();
    let handle = adapter(&server)
        .subscribe(Subscribe::Infrastructure {
            context: CONTEXT.into(),
        })
        .await
        .unwrap();
    assert_eq!(handle.id, "infrastructure-kind-bunyip");
}

#[tokio::test]
async fn infrastructure_query_and_subscription_reject_unknown_context() {
    let server = RecordedApiServer::default();
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
