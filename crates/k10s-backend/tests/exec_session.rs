use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, ContextInfo, KubeAdapter, KubernetesAccess, Query, QueryResult, StreamKind,
    StreamRouteKind, Subscribe,
};

const CONTEXT: &str = "recorded";
const POD: &str = "/api/v1/namespaces/default/pods/web";
const EXEC: &str = "/api/v1/namespaces/default/pods/web/exec";

fn pod(uid: &str) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"Pod","metadata":{{"name":"web","namespace":"default","uid":"{uid}"}},"spec":{{"containers":[{{"name":"app","image":"busybox"}}],"initContainers":[{{"name":"setup","image":"busybox"}}]}}}}"#
    )
}

fn adapter(server: &RecordedApiServer) -> KubeAdapter {
    KubeAdapter::with_cluster_clients(
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
    .unwrap()
}

fn request(uid: &str, container: &str, command: &[&str], tty: bool) -> StreamKind {
    StreamKind::Exec {
        context: CONTEXT.into(),
        namespace: "default".into(),
        pod: "web".into(),
        uid: uid.into(),
        container: container.into(),
        command: command.iter().map(|item| (*item).to_owned()).collect(),
        tty,
    }
}

async fn grant(adapter: &KubeAdapter, stream: StreamKind) -> k10s_backend::StreamGrant {
    let QueryResult::StreamTicket(grant) =
        adapter.query(Query::StreamTicket { stream }).await.unwrap()
    else {
        panic!("ticket")
    };
    grant
}

#[tokio::test]
async fn issuance_binds_exact_uid_container_command_and_mode() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    let adapter = adapter(&server);
    let grant = grant(
        &adapter,
        request("", "setup", &["/bin/sh", "-c", "printf exact"], false),
    )
    .await;
    assert_eq!(
        grant.stream,
        request(
            "uid-web",
            "setup",
            &["/bin/sh", "-c", "printf exact"],
            false
        )
    );

    assert!(matches!(
        adapter
            .query(Query::StreamTicket {
                stream: request("stale", "app", &["/bin/sh"], true)
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(
        adapter
            .query(Query::StreamTicket {
                stream: request("", "missing", &["/bin/sh"], true)
            })
            .await
            .unwrap_err(),
        BackendError::NotFound
    );
    assert!(matches!(
        adapter
            .query(Query::StreamTicket {
                stream: request("", "app", &[], true)
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
}

#[tokio::test]
async fn redeem_rechecks_identity_and_surfaces_exec_subresource_rbac() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    server.set_method_response(
        "GET",
        EXEC,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#,
    );
    let adapter = adapter(&server);
    let ticket = grant(&adapter, request("", "app", &["/bin/sh"], true)).await;
    assert_eq!(
        adapter
            .subscribe(Subscribe::StreamRedeem {
                ticket_id: ticket.ticket_id,
                route: StreamRouteKind::Exec,
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );
    let uris = server.request_uris(EXEC);
    assert!(
        uris.iter().any(|uri| uri.contains("command=%2Fbin%2Fsh")
            && uri.contains("container=app")
            && uri.contains("stdin=true")
            && uri.contains("stdout=true")
            && !uri.contains("stderr=true")
            && uri.contains("tty=true")),
        "{uris:?}"
    );

    let replacement = grant(&adapter, request("", "app", &["/bin/sh"], true)).await;
    server.set_method_response("GET", POD, 200, &pod("uid-replaced"));
    assert!(matches!(
        adapter
            .subscribe(Subscribe::StreamRedeem {
                ticket_id: replacement.ticket_id,
                route: StreamRouteKind::Exec,
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
}

#[tokio::test]
async fn wrong_route_does_not_consume_the_single_use_exec_ticket() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    server.set_method_response(
        "GET",
        EXEC,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#,
    );
    let adapter = adapter(&server);
    let ticket = grant(&adapter, request("", "app", &["/bin/sh"], true)).await;
    assert!(matches!(
        adapter
            .subscribe(Subscribe::StreamRedeem {
                ticket_id: ticket.ticket_id.clone(),
                route: StreamRouteKind::Logs,
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(
        adapter
            .subscribe(Subscribe::StreamRedeem {
                ticket_id: ticket.ticket_id,
                route: StreamRouteKind::Exec,
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );
}
