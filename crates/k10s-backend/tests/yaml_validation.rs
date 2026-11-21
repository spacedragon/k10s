use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, BackendKernel, ContextInfo, KernelQueryResult, KubeAdapter, Query,
};
use k10s_protocol::{YamlOutcome, buffer_hash};

const CONTEXT: &str = "recorded";
const PATH: &str = "/apis/apps/v1/namespaces/default/deployments/web";
const OBJECT: &str = r#"{
  "apiVersion":"apps/v1","kind":"Deployment",
  "metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"42"},
  "spec":{"replicas":2,"selector":{"matchLabels":{"app":"web"}},"template":{"metadata":{"labels":{"app":"web"}},"spec":{"containers":[{"name":"web","image":"nginx"}]}}}
}"#;
const YAML: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: default\n  uid: uid-web\n  resourceVersion: '42'\nspec:\n  replicas: 3\n  selector:\n    matchLabels:\n      app: web\n  template:\n    metadata:\n      labels:\n        app: web\n    spec:\n      containers:\n        - name: web\n          image: nginx\n";

fn kernel(server: &RecordedApiServer) -> BackendKernel {
    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [(CONTEXT, server.clone().into_client("default"))],
    )
    .unwrap();
    BackendKernel::new_with_instance_id(adapter, "validation-loopback")
}

async fn validate(kernel: &BackendKernel, yaml: &str) -> Result<YamlOutcome, BackendError> {
    match kernel
        .query(Query::ValidateApply {
            context: CONTEXT.into(),
            yaml: yaml.into(),
        })
        .await?
    {
        KernelQueryResult::YamlValidate(result) => Ok(result.wire_payload()),
        other => panic!("unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn exact_object_is_dry_run_and_issued_an_opaque_process_local_ticket() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", PATH, 200, OBJECT);
    server.set_method_response("PATCH", PATH, 200, OBJECT);
    let outcome = validate(&kernel(&server), YAML).await.unwrap();
    let YamlOutcome::Valid { ticket } = outcome else {
        panic!("expected valid outcome")
    };
    assert_eq!(ticket.target.uid, "uid-web");
    assert_eq!(ticket.buffer_hash, buffer_hash(YAML));
    assert!(ticket.disruptive);
    assert!(!ticket.id.contains("web"));
    assert_eq!(
        server.hit_count(PATH),
        2,
        "GET plus server-side dry-run PATCH"
    );
    let submitted = server.request_bodies(PATH);
    assert_eq!(submitted.len(), 2);
    assert!(submitted[1].contains("\"replicas\":3"));
    let uris = server.request_uris(PATH);
    assert!(uris[1].contains("dryRun=All"), "{uris:?}");
    assert!(uris[1].contains("fieldValidation=Strict"), "{uris:?}");
}

#[tokio::test]
async fn parse_schema_identity_and_dry_run_fail_closed_without_secret_echoes() {
    let server = RecordedApiServer::standard();
    let kernel = kernel(&server);
    let malformed = validate(&kernel, "apiVersion: v1\ndata: TOP-SECRET\n  broken")
        .await
        .unwrap();
    let YamlOutcome::Invalid { diagnostics } = malformed else {
        panic!("invalid")
    };
    assert!(
        diagnostics
            .iter()
            .all(|d| !d.message.contains("TOP-SECRET"))
    );

    let unavailable = YAML.replace("apps/v1", "unknown.example/v1");
    assert!(matches!(
        validate(&kernel, &unavailable).await.unwrap(),
        YamlOutcome::Invalid { .. }
    ));

    server.set_method_response("GET", PATH, 200, OBJECT);
    let wrong_uid = YAML.replace("uid-web", "uid-other");
    assert!(matches!(
        validate(&kernel, &wrong_uid).await,
        Err(BackendError::Conflict(_))
    ));
    let wrong_rv = YAML.replace("'42'", "'41'");
    assert!(matches!(
        validate(&kernel, &wrong_rv).await,
        Err(BackendError::Conflict(_))
    ));

    server.set_method_response(
        "PATCH",
        PATH,
        422,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"TOP-SECRET invalid","reason":"Invalid","code":422}"#,
    );
    let unknown_field = YAML.replace("replicas: 3", "replicas: 3\n  replics: 99");
    let rejected = validate(&kernel, &unknown_field).await.unwrap();
    let YamlOutcome::Invalid { diagnostics } = rejected else {
        panic!("invalid")
    };
    assert!(diagnostics.iter().any(|d| d.message.contains("dry-run")));
    assert!(
        diagnostics
            .iter()
            .all(|d| !d.message.contains("TOP-SECRET"))
    );
}

#[tokio::test]
async fn rendered_deployment_and_pod_manifests_strip_server_managed_fields_and_force_ssa() {
    use k10s_backend::{Command, Gvk, KubernetesAccess, QueryResult, ResourceRef};

    const DEPLOYMENT_WITH_LIVE_FIELDS: &str = r#"{
      "apiVersion": "apps/v1",
      "kind": "Deployment",
      "metadata": {
        "name": "web",
        "namespace": "default",
        "uid": "uid-web",
        "resourceVersion": "42",
        "generation": 2,
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "managedFields": [
          {
            "manager": "kubectl-client-side-apply",
            "operation": "Update",
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": { "f:spec": { "f:replicas": {} } }
          }
        ]
      },
      "spec": {
        "replicas": 2,
        "selector": { "matchLabels": { "app": "web" } },
        "template": {
          "metadata": { "creationTimestamp": null, "labels": { "app": "web" } },
          "spec": { "containers": [{ "name": "web", "image": "nginx:1.25" }] }
        }
      },
      "status": { "replicas": 2, "readyReplicas": 2 }
    }"#;

    const POD_PATH: &str = "/api/v1/namespaces/default/pods/web-0";
    const POD_WITH_LIVE_FIELDS: &str = r#"{
      "apiVersion": "v1",
      "kind": "Pod",
      "metadata": {
        "name": "web-0",
        "namespace": "default",
        "uid": "uid-pod-0",
        "resourceVersion": "99",
        "creationTimestamp": "2026-01-01T00:00:00Z",
        "managedFields": [
          {
            "manager": "kube-controller-manager",
            "operation": "Update",
            "apiVersion": "v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": { "f:spec": {} }
          }
        ]
      },
      "spec": {
        "nodeName": "worker-1",
        "containers": [{ "name": "web", "image": "nginx:1.25" }]
      },
      "status": { "phase": "Running" }
    }"#;

    let server = RecordedApiServer::standard();
    server.set_method_response("GET", PATH, 200, DEPLOYMENT_WITH_LIVE_FIELDS);
    server.set_method_response("PATCH", PATH, 200, DEPLOYMENT_WITH_LIVE_FIELDS);
    server.set_method_response("GET", POD_PATH, 200, POD_WITH_LIVE_FIELDS);
    server.set_method_response("PATCH", POD_PATH, 200, POD_WITH_LIVE_FIELDS);
    server.set_method_response(
        "GET",
        "/api/v1/namespaces/default/events",
        200,
        r#"{"apiVersion":"v1","kind":"EventList","items":[]}"#,
    );

    let adapter = KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [(CONTEXT, server.clone().into_client("default"))],
    )
    .unwrap();

    // Deployment detail -> edit spec -> validate -> apply.
    let deploy_ref = ResourceRef {
        context: CONTEXT.into(),
        gvk: Gvk::new("apps", "v1", "Deployment"),
        namespace: Some("default".into()),
        name: "web".into(),
        uid: "uid-web".into(),
    };
    let QueryResult::ResourceDetail(deploy_detail) = adapter
        .query(Query::ResourceDetail {
            reference: deploy_ref.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("expected deployment detail");
    };
    assert!(
        !deploy_detail.manifest.contains("managedFields"),
        "rendered manifest should omit managedFields: {}",
        deploy_detail.manifest
    );
    let edited_deploy = deploy_detail
        .manifest
        .replace("replicas: 2", "replicas: 4")
        .replace("nginx:1.25", "nginx:1.26");
    let QueryResult::YamlValidation(deploy_val) = adapter
        .query(Query::ValidateApply {
            context: CONTEXT.into(),
            yaml: edited_deploy.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("expected yaml validation result");
    };
    let k10s_backend::operation::OutcomeData::Valid { ticket } = deploy_val.outcome else {
        panic!("expected valid deployment validation ticket");
    };
    let _op_id = adapter
        .execute(Command::Apply {
            context: CONTEXT.into(),
            yaml: edited_deploy,
            idempotency_key: "deploy-apply-1".into(),
            ticket_id: ticket.id,
            buffer_hash: ticket.buffer_hash,
            target: deploy_ref,
        })
        .await
        .unwrap();

    // Wait briefly for the spawned apply PATCH to hit the recorded server.
    for _ in 0..50 {
        if server.request_uris(PATH).len() >= 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let deploy_uris = server.request_uris(PATH);
    let deploy_bodies = server.request_bodies(PATH);
    assert!(
        deploy_uris.iter().any(|uri| uri.contains("dryRun=All")
            && uri.contains("force=true")
            && uri.contains("fieldValidation=Strict")),
        "dry-run PATCH should use force=true and strict validation: {deploy_uris:?}"
    );
    assert!(
        deploy_uris.iter().any(|uri| !uri.contains("dryRun=All")
            && uri.contains("force=true")
            && uri.contains("fieldValidation=Strict")),
        "live apply PATCH should use force=true and strict validation: {deploy_uris:?}"
    );
    for body in deploy_bodies.iter().filter(|body| !body.is_empty()) {
        assert!(
            !body.contains("managedFields"),
            "PATCH leaked managedFields: {body}"
        );
        assert!(!body.contains("\"status\""), "PATCH leaked status: {body}");
        assert!(
            body.contains("\"replicas\":4"),
            "PATCH missing spec edit: {body}"
        );
        assert!(
            body.contains("nginx:1.26"),
            "PATCH missing container image edit: {body}"
        );
    }

    // Pod detail -> edit spec.containers[0].image -> validate -> apply.
    let pod_ref = ResourceRef {
        context: CONTEXT.into(),
        gvk: Gvk::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: "web-0".into(),
        uid: "uid-pod-0".into(),
    };
    let QueryResult::ResourceDetail(pod_detail) = adapter
        .query(Query::ResourceDetail {
            reference: pod_ref.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("expected pod detail");
    };
    assert!(!pod_detail.manifest.contains("managedFields"));
    let edited_pod = pod_detail.manifest.replace("nginx:1.25", "nginx:1.27");
    let QueryResult::YamlValidation(pod_val) = adapter
        .query(Query::ValidateApply {
            context: CONTEXT.into(),
            yaml: edited_pod.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("expected pod validation result");
    };
    let k10s_backend::operation::OutcomeData::Valid { ticket: pod_ticket } = pod_val.outcome else {
        panic!("expected valid pod validation ticket");
    };
    let _pod_op = adapter
        .execute(Command::Apply {
            context: CONTEXT.into(),
            yaml: edited_pod,
            idempotency_key: "pod-apply-1".into(),
            ticket_id: pod_ticket.id,
            buffer_hash: pod_ticket.buffer_hash,
            target: pod_ref,
        })
        .await
        .unwrap();

    for _ in 0..50 {
        if server.request_uris(POD_PATH).len() >= 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let pod_uris = server.request_uris(POD_PATH);
    let pod_bodies = server.request_bodies(POD_PATH);
    assert!(
        pod_uris
            .iter()
            .any(|uri| uri.contains("dryRun=All") && uri.contains("force=true")),
        "pod dry-run PATCH should use force=true: {pod_uris:?}"
    );
    assert!(
        pod_uris
            .iter()
            .any(|uri| !uri.contains("dryRun=All") && uri.contains("force=true")),
        "pod live apply PATCH should use force=true: {pod_uris:?}"
    );
    for body in pod_bodies.iter().filter(|body| !body.is_empty()) {
        assert!(
            !body.contains("managedFields"),
            "pod PATCH leaked managedFields: {body}"
        );
        assert!(
            !body.contains("\"status\""),
            "pod PATCH leaked status: {body}"
        );
        assert!(
            body.contains("nginx:1.27"),
            "pod PATCH missing image edit: {body}"
        );
    }
}

#[tokio::test]
async fn pasted_yaml_with_managed_fields_and_status_is_sanitized_before_dry_run() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", PATH, 200, OBJECT);
    server.set_method_response("PATCH", PATH, 200, OBJECT);
    let yaml_with_noise = format!(
        "{YAML}status:\n  replicas: 2\n"
    )
    .replace(
        "  resourceVersion: '42'\n",
        "  resourceVersion: '42'\n  managedFields:\n    - manager: kubectl\n      operation: Update\n",
    );
    let outcome = validate(&kernel(&server), &yaml_with_noise).await.unwrap();
    assert!(matches!(outcome, YamlOutcome::Valid { .. }));
    let bodies = server.request_bodies(PATH);
    let patch_body = &bodies[1];
    assert!(!patch_body.contains("managedFields"), "{patch_body}");
    assert!(!patch_body.contains("\"status\""), "{patch_body}");
}
