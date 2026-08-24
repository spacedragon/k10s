use std::time::Duration;

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, Command, ContextInfo, Gvk, KubeAdapter, KubernetesAccess, OperationState,
    Propagation, Query, QueryResult, ResourceRef,
};

const CONTEXT: &str = "recorded";
const DEPLOYMENT: &str = "/apis/apps/v1/namespaces/default/deployments/web";
const SCALE: &str = "/apis/apps/v1/namespaces/default/deployments/web/scale";
const OBJECT: &str = r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"default","uid":"uid-web","resourceVersion":"42"},"spec":{"replicas":2}}"#;
const RECREATED_OBJECT: &str = r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"default","uid":"uid-recreated","resourceVersion":"84"},"spec":{"replicas":2}}"#;
const YAML: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: default\n  uid: uid-web\n  resourceVersion: '42'\nspec:\n  replicas: 4\n";

fn adapter(server: &RecordedApiServer) -> KubeAdapter {
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "fixture".into(),
            namespace: Some("default".into()),
            is_current: true,
        }],
        [(CONTEXT, server.clone().into_client("default"))],
    )
    .unwrap()
}

fn target() -> ResourceRef {
    ResourceRef {
        context: CONTEXT.into(),
        gvk: Gvk::new("apps", "v1", "Deployment"),
        namespace: Some("default".into()),
        name: "web".into(),
        uid: "uid-web".into(),
    }
}

async fn terminal(adapter: &KubeAdapter, id: &str) -> OperationState {
    for _ in 0..100 {
        let data = adapter.operation_engine().status(&[id.to_owned()]);
        if let Some(record) = data.operations.first()
            && matches!(
                record.state,
                OperationState::Succeeded | OperationState::Failed | OperationState::OutcomeUnknown
            )
        {
            return record.state;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("operation did not settle")
}

#[tokio::test]
async fn scale_and_restart_use_exact_uid_rv_capability_and_native_payloads() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    server.set_method_response("PATCH", SCALE, 200, OBJECT);
    server.set_method_response("PATCH", DEPLOYMENT, 200, OBJECT);
    let adapter = adapter(&server);

    let scale = adapter
        .execute(Command::Scale {
            context: CONTEXT.into(),
            gvk: target().gvk,
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
            replicas: 3,
            idempotency_key: "scale-3".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, scale.as_str()).await,
        OperationState::Succeeded
    );
    let scale_body = server.request_bodies(SCALE).pop().unwrap();
    assert!(scale_body.contains("\"resourceVersion\":\"42\""));
    assert!(scale_body.contains("\"replicas\":3"));

    let restart = adapter
        .execute(Command::Restart {
            target: target(),
            idempotency_key: "restart-1".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, restart.as_str()).await,
        OperationState::Succeeded
    );
    let restart_body = server.request_bodies(DEPLOYMENT).pop().unwrap();
    assert!(restart_body.contains("kubectl.kubernetes.io/restartedAt"));
    assert!(restart_body.contains("\"resourceVersion\":\"42\""));

    let mut stale = target();
    stale.uid = "uid-recreated".into();
    assert!(matches!(
        adapter
            .execute(Command::Restart {
                target: stale,
                idempotency_key: "stale".into()
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
}

#[tokio::test]
async fn delete_carries_uid_rv_preconditions_and_all_propagation_policies() {
    for propagation in [
        Propagation::Background,
        Propagation::Foreground,
        Propagation::Orphan,
    ] {
        let server = RecordedApiServer::standard();
        server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
        server.set_method_response("DELETE", DEPLOYMENT, 200, OBJECT);
        let adapter = adapter(&server);
        let id = adapter
            .execute(Command::Delete {
                target: target(),
                propagation,
                idempotency_key: format!("delete-{propagation:?}"),
            })
            .await
            .unwrap();
        assert_eq!(
            terminal(&adapter, id.as_str()).await,
            OperationState::Succeeded
        );
        let body = server.request_bodies(DEPLOYMENT).pop().unwrap();
        assert!(body.contains("uid-web"));
        assert!(body.contains("\"resourceVersion\":\"42\""));
        assert!(
            body.to_ascii_lowercase()
                .contains(&format!("{propagation:?}").to_ascii_lowercase())
        );
    }
}

#[tokio::test]
async fn completed_delete_replays_before_a_live_target_preflight() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    server.set_method_response("DELETE", DEPLOYMENT, 200, OBJECT);
    let adapter = adapter(&server);
    let command = Command::Delete {
        target: target(),
        propagation: Propagation::Foreground,
        idempotency_key: "delete-replay".into(),
    };

    let id = adapter.execute(command.clone()).await.unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await,
        OperationState::Succeeded
    );
    let hits = server.hit_count(DEPLOYMENT);
    server.set_method_response(
        "GET",
        DEPLOYMENT,
        404,
        r#"{"kind":"Status","status":"Failure","reason":"NotFound","code":404}"#,
    );

    assert_eq!(adapter.execute(command).await.unwrap(), id);
    assert_eq!(
        server.hit_count(DEPLOYMENT),
        hits,
        "a retained replay must not require the deleted object to exist"
    );
}

#[tokio::test]
async fn retained_keys_distinguish_recreated_uids_and_api_versions() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    server.set_method_response("PATCH", SCALE, 200, OBJECT);
    let adapter = adapter(&server);
    let id = adapter
        .execute(Command::Scale {
            context: CONTEXT.into(),
            gvk: target().gvk,
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
            replicas: 3,
            idempotency_key: "exact-key".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await,
        OperationState::Succeeded
    );

    server.set_method_response("GET", DEPLOYMENT, 200, RECREATED_OBJECT);
    assert!(matches!(
        adapter
            .execute(Command::Scale {
                context: CONTEXT.into(),
                gvk: target().gvk,
                namespace: Some("default".into()),
                name: "web".into(),
                uid: "uid-recreated".into(),
                replicas: 3,
                idempotency_key: "exact-key".into(),
            })
            .await,
        Err(BackendError::Conflict(_))
    ));

    let mut other_version = target();
    other_version.gvk.version = "v2".into();
    assert!(matches!(
        adapter.operation_engine().replay(
            "exact-key",
            &format!("scale/{}/3", other_version.exact_identity_key())
        ),
        Err(BackendError::Conflict(_))
    ));
}

#[tokio::test]
async fn authoritative_absence_releases_an_unknown_delete_scope() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    server.set_transport_error("DELETE", DEPLOYMENT);
    let adapter = adapter(&server);
    let id = adapter
        .execute(Command::Delete {
            target: target(),
            propagation: Propagation::Background,
            idempotency_key: "unknown-delete".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await,
        OperationState::OutcomeUnknown
    );

    server.set_method_response(
        "GET",
        DEPLOYMENT,
        404,
        r#"{"kind":"Status","status":"Failure","reason":"NotFound","code":404}"#,
    );
    assert_eq!(
        adapter
            .query(Query::ResourceDetail {
                reference: target(),
            })
            .await
            .unwrap_err(),
        BackendError::NotFound
    );

    server.set_method_response("GET", DEPLOYMENT, 200, RECREATED_OBJECT);
    server.set_method_response("PATCH", SCALE, 200, RECREATED_OBJECT);
    let retry = adapter
        .execute(Command::Scale {
            context: CONTEXT.into(),
            gvk: target().gvk,
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-recreated".into(),
            replicas: 4,
            idempotency_key: "after-delete-refresh".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, retry.as_str()).await,
        OperationState::Succeeded
    );
}

#[tokio::test]
async fn apply_consumes_only_the_exact_ticket_and_uses_strict_server_side_apply() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    server.set_method_response("PATCH", DEPLOYMENT, 200, OBJECT);
    let adapter = adapter(&server);
    let validation = adapter
        .query(Query::ValidateApply {
            context: CONTEXT.into(),
            yaml: YAML.into(),
        })
        .await
        .unwrap();
    let QueryResult::YamlValidation(data) = validation else {
        panic!("validation")
    };
    let k10s_backend::operation::OutcomeData::Valid { ticket } = data.outcome else {
        panic!("ticket")
    };
    let id = adapter
        .execute(Command::Apply {
            context: CONTEXT.into(),
            yaml: YAML.into(),
            idempotency_key: "apply-1".into(),
            ticket_id: ticket.id.clone(),
            buffer_hash: ticket.buffer_hash.clone(),
            target: target(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await,
        OperationState::Succeeded
    );
    let replay = adapter
        .execute(Command::Apply {
            context: CONTEXT.into(),
            yaml: YAML.into(),
            idempotency_key: "apply-1".into(),
            ticket_id: ticket.id,
            buffer_hash: ticket.buffer_hash,
            target: target(),
        })
        .await
        .unwrap();
    assert_eq!(
        replay, id,
        "idempotency replay does not redeem the ticket twice"
    );
    let uris = server.request_uris(DEPLOYMENT);
    assert!(uris.iter().any(|uri| uri.contains("dryRun=All")));
    assert!(
        uris.iter()
            .any(|uri| uri.contains("fieldValidation=Strict") && !uri.contains("dryRun=All"))
    );
}

#[tokio::test]
async fn authorization_and_unknown_transport_outcomes_remain_distinct() {
    let forbidden = RecordedApiServer::standard();
    forbidden.set_method_response(
        "GET",
        DEPLOYMENT,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#,
    );
    assert_eq!(
        adapter(&forbidden)
            .execute(Command::Restart {
                target: target(),
                idempotency_key: "forbidden".into()
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );

    let broken = RecordedApiServer::standard();
    broken.set_method_response("GET", DEPLOYMENT, 200, OBJECT);
    broken.set_transport_error("PATCH", SCALE);
    let adapter = adapter(&broken);
    let id = adapter
        .execute(Command::Scale {
            context: CONTEXT.into(),
            gvk: target().gvk,
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-web".into(),
            replicas: 5,
            idempotency_key: "unknown".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await,
        OperationState::OutcomeUnknown
    );
    assert!(matches!(
        adapter
            .execute(Command::Scale {
                context: CONTEXT.into(),
                gvk: target().gvk,
                namespace: Some("default".into()),
                name: "web".into(),
                uid: "uid-web".into(),
                replicas: 6,
                idempotency_key: "retry-too-soon".into(),
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
}
