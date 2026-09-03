use std::time::Duration;

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendError, BackendEvent, ContextInfo, KubeAdapter, KubernetesAccess, Query, QueryResult,
    StreamKind, StreamRouteKind, Subscribe,
};

const CONTEXT: &str = "recorded";
const POD: &str = "/api/v1/namespaces/default/pods/web";
const LOG: &str = "/api/v1/namespaces/default/pods/web/log";

fn pod(uid: &str) -> String {
    format!(
        r#"{{"apiVersion":"v1","kind":"Pod","metadata":{{"name":"web","namespace":"default","uid":"{uid}","resourceVersion":"7"}},"spec":{{"containers":[{{"name":"app","image":"busybox"}}],"initContainers":[{{"name":"setup","image":"busybox"}}]}}}}"#
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

fn request(container: &str) -> StreamKind {
    StreamKind::Logs {
        context: CONTEXT.into(),
        namespace: "default".into(),
        pod: "web".into(),
        uid: String::new(),
        container: container.into(),
        tail_lines: Some(25),
        since_seconds: Some(60),
        previous: false,
        timestamps: true,
        follow: true,
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
async fn exact_container_and_log_options_reach_the_dedicated_kubernetes_stream() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    server.set_method_response("GET", LOG, 200, "first\nsecond\n");
    let adapter = adapter(&server);
    let grant = grant(&adapter, request("app")).await;
    let StreamKind::Logs { uid, .. } = &grant.stream;
    assert_eq!(
        uid, "uid-web",
        "the ticket binds the observed immutable UID"
    );

    let mut handle = adapter
        .subscribe(Subscribe::StreamRedeem {
            ticket_id: grant.ticket_id.clone(),
            route: StreamRouteKind::Logs,
        })
        .await
        .unwrap();
    let mut events = handle.take_events().unwrap();
    let BackendEvent::Stream(first) = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("stream")
    };
    assert_eq!(first.text, "first\n");
    let uris = server.request_uris(LOG);
    assert!(
        uris.iter().any(|uri| uri.contains("tailLines=25")
            && uri.contains("sinceSeconds=60")
            && uri.contains("timestamps=true")
            && uri.contains("follow=true")
            && uri.contains("container=app")),
        "{uris:?}"
    );

    assert!(
        matches!(
            adapter
                .subscribe(Subscribe::StreamRedeem {
                    ticket_id: grant.ticket_id,
                    route: StreamRouteKind::Logs
                })
                .await,
            Err(BackendError::Conflict(_))
        ),
        "tickets are single use"
    );
}

#[tokio::test]
async fn issuance_validates_container_and_log_subresource_authorization() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    server.set_method_response(
        "GET",
        LOG,
        403,
        r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#,
    );
    let adapter = adapter(&server);
    let mut stale = request("app");
    let StreamKind::Logs { uid, .. } = &mut stale;
    *uid = "uid-stale".into();
    assert!(matches!(
        adapter.query(Query::StreamTicket { stream: stale }).await,
        Err(BackendError::Conflict(_))
    ));
    assert_eq!(
        adapter
            .query(Query::StreamTicket {
                stream: request("app")
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );
    assert_eq!(
        adapter
            .query(Query::StreamTicket {
                stream: request("missing")
            })
            .await
            .unwrap_err(),
        BackendError::NotFound
    );
}

#[tokio::test]
async fn pod_replacement_between_issue_and_redeem_fails_closed() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-first"));
    server.set_method_response("GET", LOG, 200, "probe\n");
    let adapter = adapter(&server);
    let grant = grant(&adapter, request("setup")).await;
    server.set_method_response("GET", POD, 200, &pod("uid-replacement"));
    assert!(matches!(
        adapter
            .subscribe(Subscribe::StreamRedeem {
                ticket_id: grant.ticket_id,
                route: StreamRouteKind::Logs
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
}

#[tokio::test]
async fn invalid_history_bounds_and_wrong_routes_do_not_consume_valid_authority() {
    let server = RecordedApiServer::standard();
    server.set_method_response("GET", POD, 200, &pod("uid-web"));
    server.set_method_response("GET", LOG, 200, "line\n");
    let adapter = adapter(&server);
    let mut invalid = request("app");
    let StreamKind::Logs { tail_lines, .. } = &mut invalid;
    *tail_lines = Some(-1);
    assert!(matches!(
        adapter.query(Query::StreamTicket { stream: invalid }).await,
        Err(BackendError::Conflict(_))
    ));

    let grant = grant(&adapter, request("app")).await;
    adapter
        .subscribe(Subscribe::StreamRedeem {
            ticket_id: grant.ticket_id,
            route: StreamRouteKind::Logs,
        })
        .await
        .unwrap();
}
