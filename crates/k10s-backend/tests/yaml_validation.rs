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
    let rejected = validate(&kernel, YAML).await.unwrap();
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
