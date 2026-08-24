use std::time::Duration;

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, Command, ContextInfo, Gvk, KubeAdapter, KubernetesAccess, OperationState,
    Propagation, ResourceRef,
};

const CTX: &str = "recorded";
const JOB: &str = "/apis/batch/v1/namespaces/default/jobs/backup";
const JOBS: &str = "/apis/batch/v1/namespaces/default/jobs";
const CRON: &str = "/apis/batch/v1/namespaces/default/cronjobs/nightly";

fn adapter(server: &RecordedApiServer) -> KubeAdapter {
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CTX.into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [(CTX, server.clone().into_client("default"))],
    )
    .unwrap()
}

fn target(kind: &str, name: &str, uid: &str) -> ResourceRef {
    ResourceRef {
        context: CTX.into(),
        gvk: Gvk::new("batch", "v1", kind),
        namespace: Some("default".into()),
        name: name.into(),
        uid: uid.into(),
    }
}

async fn success(adapter: &KubeAdapter, id: &str) {
    for _ in 0..100 {
        if adapter
            .operation_engine()
            .status(&[id.into()])
            .operations
            .first()
            .is_some_and(|v| v.state == OperationState::Succeeded)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("operation did not succeed")
}

#[tokio::test]
async fn job_and_cronjob_sources_create_native_generated_jobs_without_private_annotations() {
    for (path, source, body) in [
        (
            JOB,
            target("Job", "backup", "uid-job"),
            r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"backup","namespace":"default","uid":"uid-job","resourceVersion":"7"},"spec":{"selector":{"matchLabels":{"controller-uid":"old"}},"template":{"metadata":{"labels":{"controller-uid":"old","job-name":"backup","keep":"yes"}},"spec":{"restartPolicy":"Never","containers":[{"name":"job","image":"busybox"}]}}}}"#,
        ),
        (
            CRON,
            target("CronJob", "nightly", "uid-cron"),
            r#"{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"nightly","namespace":"default","uid":"uid-cron","resourceVersion":"9"},"spec":{"jobTemplate":{"spec":{"template":{"spec":{"restartPolicy":"Never","containers":[{"name":"job","image":"busybox"}]}}}}}}"#,
        ),
    ] {
        let server = RecordedApiServer::standard();
        server.set_method_response("GET", path, 200, body);
        server.set_method_response("POST", JOBS, 201, body);
        let adapter = adapter(&server);
        let id = adapter
            .execute(Command::CreateJob {
                source,
                idempotency_key: format!("create-{path}"),
            })
            .await
            .unwrap();
        success(&adapter, id.as_str()).await;
        let request = server.request_bodies(JOBS).pop().unwrap();
        assert!(request.contains("generateName"));
        assert!(!request.contains("controller-uid"));
        assert!(!request.contains("job-name"));
        assert!(!request.contains("k10s"));
        if path == JOB {
            assert!(request.contains("\"keep\":\"yes\""));
        }
    }
}

#[tokio::test]
async fn cronjob_suspend_and_resume_patch_exact_uid_and_resource_version() {
    let server = RecordedApiServer::standard();
    let body = r#"{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"nightly","namespace":"default","uid":"uid-cron","resourceVersion":"9"},"spec":{"suspend":false}}"#;
    server.set_method_response("GET", CRON, 200, body);
    server.set_method_response("PATCH", CRON, 200, body);
    let adapter = adapter(&server);
    for suspended in [true, false] {
        let id = adapter
            .execute(Command::SetCronJobSuspended {
                target: target("CronJob", "nightly", "uid-cron"),
                suspended,
                idempotency_key: format!("suspend-{suspended}"),
            })
            .await
            .unwrap();
        success(&adapter, id.as_str()).await;
    }
    let bodies = server.request_bodies(CRON);
    assert!(
        bodies
            .iter()
            .any(|v| v.contains("\"suspend\":true") && v.contains("\"resourceVersion\":\"9\""))
    );
    assert!(bodies.iter().any(|v| v.contains("\"suspend\":false")));
}

#[tokio::test]
async fn custom_resources_obey_discovered_scale_and_delete_capabilities() {
    let enabled = RecordedApiServer::standard();
    enabled.set_response(
        "/apis/k10s.example.com/v1alpha1",
        200,
        r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"k10s.example.com/v1alpha1","resources":[{"name":"gadgets","singularName":"gadget","namespaced":true,"kind":"Gadget","verbs":["get","list","watch","patch","delete"]},{"name":"gadgets/scale","singularName":"","namespaced":true,"kind":"Scale","verbs":["get","patch"]}]}"#,
    );
    let path = "/apis/k10s.example.com/v1alpha1/namespaces/default/gadgets/widget";
    let scale_path = "/apis/k10s.example.com/v1alpha1/namespaces/default/gadgets/widget/scale";
    let object = r#"{"apiVersion":"k10s.example.com/v1alpha1","kind":"Gadget","metadata":{"name":"widget","namespace":"default","uid":"uid-widget","resourceVersion":"3"}}"#;
    enabled.set_method_response("GET", path, 200, object);
    enabled.set_method_response("PATCH", scale_path, 200, object);
    let enabled_adapter = adapter(&enabled);
    let id = enabled_adapter
        .execute(Command::Scale {
            context: CTX.into(),
            gvk: Gvk::new("k10s.example.com", "v1alpha1", "Gadget"),
            namespace: Some("default".into()),
            name: "widget".into(),
            uid: "uid-widget".into(),
            replicas: 2,
            idempotency_key: "enabled-custom-scale".into(),
        })
        .await
        .unwrap();
    success(&enabled_adapter, id.as_str()).await;
    assert!(enabled.request_bodies(scale_path)[0].contains("\"replicas\":2"));

    let server = RecordedApiServer::standard();
    server.set_response(
        "/apis/k10s.example.com/v1alpha1",
        200,
        r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"k10s.example.com/v1alpha1","resources":[{"name":"gadgets","singularName":"gadget","namespaced":true,"kind":"Gadget","verbs":["get","list","watch"]}]}"#,
    );
    server.set_method_response("GET", path, 200, object);
    let adapter = adapter(&server);
    let scale = adapter
        .execute(Command::Scale {
            context: CTX.into(),
            gvk: Gvk::new("k10s.example.com", "v1alpha1", "Gadget"),
            namespace: Some("default".into()),
            name: "widget".into(),
            uid: "uid-widget".into(),
            replicas: 2,
            idempotency_key: "custom-scale".into(),
        })
        .await;
    assert!(matches!(
        scale,
        Err(BackendError::Unsupported { capability: _ })
    ));
    let delete = adapter
        .execute(Command::Delete {
            target: ResourceRef {
                context: CTX.into(),
                gvk: Gvk::new("k10s.example.com", "v1alpha1", "Gadget"),
                namespace: Some("default".into()),
                name: "widget".into(),
                uid: "uid-widget".into(),
            },
            propagation: Propagation::Background,
            idempotency_key: "custom-delete".into(),
        })
        .await;
    assert!(matches!(
        delete,
        Err(BackendError::Unsupported { capability: _ })
    ));
}
