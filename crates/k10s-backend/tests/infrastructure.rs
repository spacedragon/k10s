use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendKernel, ContextInfo, KernelQueryResult, KubeAdapter, KubernetesAccess, Query, Subscribe,
};

const CONTEXT: &str = "infrastructure-mock";

fn adapter(server: &RecordedApiServer) -> KubeAdapter {
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo::available(
            CONTEXT,
            "recorded-apiserver",
            Some("default".into()),
            true,
        )],
        [(CONTEXT, server.clone().into_client("default"))],
    )
    .unwrap()
}

#[tokio::test]
async fn real_adapter_projects_core_nodes_instead_of_rejecting_infrastructure() {
    let server = RecordedApiServer::standard();
    for (path, kind, api_version) in [
        ("/api/v1/pods", "PodList", "v1"),
        ("/apis/apps/v1/deployments", "DeploymentList", "apps/v1"),
        ("/apis/apps/v1/statefulsets", "StatefulSetList", "apps/v1"),
        ("/apis/apps/v1/daemonsets", "DaemonSetList", "apps/v1"),
        ("/apis/batch/v1/jobs", "JobList", "batch/v1"),
        ("/apis/batch/v1/cronjobs", "CronJobList", "batch/v1"),
    ] {
        server.set_response(
            path,
            200,
            &serde_json::json!({
                "kind": kind,
                "apiVersion": api_version,
                "metadata": {"resourceVersion": "42"},
                "items": []
            })
            .to_string(),
        );
    }
    server.set_response(
        "/api/v1/nodes",
        200,
        &serde_json::json!({
            "kind": "NodeList",
            "apiVersion": "v1",
            "metadata": {"resourceVersion": "42"},
            "items": [{
                "metadata": {
                    "name": "bunyip-control-plane",
                    "labels": {"node-role.kubernetes.io/control-plane": ""}
                },
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "nodeInfo": {"kubeletVersion": "v1.36.1"},
                    "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}
                }
            }]
        })
        .to_string(),
    );
    let kernel = BackendKernel::new(adapter(&server));

    let KernelQueryResult::Infrastructure(result) = kernel
        .query(Query::Infrastructure {
            context: CONTEXT.into(),
        })
        .await
        .expect("real infrastructure query succeeds")
    else {
        panic!("expected infrastructure result");
    };
    let response = result.wire_payload();
    assert_eq!(response.totals.nodes, 1);
    assert_eq!(response.nodes[0].name, "bunyip-control-plane");
    assert_eq!(response.nodes[0].status, "Ready");
    assert_eq!(response.nodes[0].roles, ["control-plane"]);
    assert_eq!(response.nodes[0].kubernetes_version, "v1.36.1");
    assert_eq!(response.nodes[0].cpu.capacity, Some(4_000));
    assert_eq!(response.nodes[0].memory.capacity, Some(8 * 1_073_741_824));
    assert_eq!(response.nodes[0].pods.capacity, Some(110));
}

#[tokio::test]
async fn real_adapter_accepts_the_infrastructure_subscription_capability() {
    let server = RecordedApiServer::standard();
    let adapter = adapter(&server);

    let handle = adapter
        .subscribe(Subscribe::Infrastructure {
            context: CONTEXT.into(),
        })
        .await
        .expect("supported subscription must not force the UI unavailable state");
    assert_eq!(handle.id, format!("infrastructure:{CONTEXT}"));
}

#[tokio::test]
async fn real_adapter_rejects_unknown_infrastructure_context_without_cluster_traffic() {
    let server = RecordedApiServer::standard();
    let kernel = BackendKernel::new(adapter(&server));

    let result = kernel
        .query(Query::Infrastructure {
            context: "missing".into(),
        })
        .await;
    assert!(matches!(result, Err(k10s_backend::BackendError::NotFound)));
}
