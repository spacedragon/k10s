//! End-to-end guarded YAML validation loop: deterministic fake schema and
//! dry-run results, ticket binding to target identity and backend revision,
//! single-use apply returning an `OperationId`, conflict rejection for stale
//! tickets, and reconnect semantics over a real control socket.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes, Gvk};
use k10s_protocol::{
    ErrorCode, GroupVersionKind, ResourceIdentity, ResourceListRequest, ResourceListResponse,
    ServerFrame, ServerKind, YamlApplyRequest, YamlOutcome, YamlValidateRequest, buffer_hash,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const WEB_FRONTEND_MANIFEST: &str = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web-frontend\n  namespace: default\nspec:\n  replicas: 20\n";

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "validation-server"),
    )
    .await
    .unwrap();
    (handle, fake)
}

async fn connect_authenticated(server: &k10s_server::ServerHandle) -> Ws {
    let (mut ws, _) = connect_async(format!(
        "ws://{}{}",
        server.addr(),
        k10s_protocol::CONTROL_PATH
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({
            "kind":"hello",
            "payload":{"protocolMajor":1,"protocolMinor":1,"capabilities":[],"accessToken":"secret"}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert_eq!(receive_frame(&mut ws).await.kind, ServerKind::Welcome);
    ws
}

async fn send_request(ws: &mut Ws, request_id: &str, kind: &str, payload: Value) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {"kind": kind, "payload": payload}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

async fn receive_frame(ws: &mut Ws) -> ServerFrame {
    let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("server frame within timeout")
        .expect("socket open")
        .expect("socket healthy");
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn expect_error(ws: &mut Ws, request_id: &str, code: ErrorCode) -> String {
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Error, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    assert_eq!(frame.payload["code"], json!(code));
    frame.payload["safeMessage"]
        .as_str()
        .expect("error carries a safe message")
        .to_owned()
}

/// Validate `yaml` and return the decoded outcome.
async fn validate(ws: &mut Ws, request_id: &str, context: &str, yaml: &str) -> YamlOutcome {
    send_request(
        ws,
        request_id,
        "yaml.validate",
        serde_json::to_value(YamlValidateRequest {
            context: context.to_owned(),
            yaml: yaml.to_owned(),
        })
        .unwrap(),
    )
    .await;
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    frame.decode_response_payload().unwrap()
}

/// Send one `yaml.apply` request carrying its envelope-level idempotency key.
async fn send_apply(ws: &mut Ws, request_id: &str, apply: &YamlApplyRequest) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {
                "kind": "yaml.apply",
                "idempotencyKey": format!("idem-{request_id}"),
                "payload": serde_json::to_value(apply).unwrap(),
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
}

/// Apply through the validated ticket and return the operation ID.
async fn apply_ticket(ws: &mut Ws, request_id: &str, outcome: &YamlOutcome) -> String {
    let YamlOutcome::Valid { ticket } = outcome else {
        panic!("apply requires a valid ticket");
    };
    let apply = YamlApplyRequest {
        context: ticket.target.context.clone(),
        ticket_id: ticket.id.clone(),
        target: ticket.target.clone(),
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: WEB_FRONTEND_MANIFEST.to_owned(),
    };
    send_apply(ws, request_id, &apply).await;
    let frame = receive_frame(ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    let accepted: k10s_protocol::OperationAccepted = frame.decode_response_payload().unwrap();
    accepted.operation_id.as_str().to_owned()
}

async fn fetch_web_frontend_identity(ws: &mut Ws) -> (ResourceIdentity, u64) {
    send_request(
        ws,
        "list",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
        })
        .unwrap(),
    )
    .await;
    let frame = receive_frame(ws).await;
    let list: ResourceListResponse = frame.decode_response_payload().unwrap();
    let row = list
        .rows
        .iter()
        .find(|row| row.identity.name == "web-frontend")
        .expect("web-frontend deployment exists")
        .clone();
    (row.identity, list.revision.get())
}

#[tokio::test]
async fn validate_issues_a_deterministic_ticket_bound_to_identity_revision_and_buffer() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let (identity, revision) = fetch_web_frontend_identity(&mut ws).await;
    let outcome = validate(&mut ws, "validate-1", "dev-local", WEB_FRONTEND_MANIFEST).await;

    let YamlOutcome::Valid { ticket } = outcome else {
        panic!("expected a valid outcome, got {outcome:?}");
    };
    assert_eq!(ticket.id, "ticket-0001", "ticket IDs are deterministic");
    assert_eq!(ticket.buffer_hash, buffer_hash(WEB_FRONTEND_MANIFEST));
    assert_eq!(
        ticket.resource_revision.get(),
        revision,
        "the ticket binds to the backend revision it was validated against"
    );

    // Revalidating the same buffer yields a fresh ticket with identical
    // bindings; nothing is fabricated client-side.
    let again = validate(&mut ws, "validate-2", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let YamlOutcome::Valid { ticket: second } = again else {
        panic!("expected a valid outcome, got {again:?}");
    };
    assert_ne!(second.id, ticket.id);
    assert_eq!(second.buffer_hash, ticket.buffer_hash);
    assert_eq!(second.resource_revision, ticket.resource_revision);
    assert_eq!(second.target, identity);

    // Unknown contexts stay a typed not-found.
    send_request(
        &mut ws,
        "validate-missing",
        "yaml.validate",
        serde_json::to_value(YamlValidateRequest {
            context: "missing".into(),
            yaml: WEB_FRONTEND_MANIFEST.to_owned(),
        })
        .unwrap(),
    )
    .await;
    expect_error(&mut ws, "validate-missing", ErrorCode::NotFound).await;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn schema_errors_are_reported_deterministically() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let missing_name = validate(
        &mut ws,
        "schema-1",
        "dev-local",
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  namespace: default\n",
    )
    .await;
    let YamlOutcome::Invalid { diagnostics } = missing_name else {
        panic!("expected schema diagnostics, got {missing_name:?}");
    };
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("name")),
        "the missing metadata.name is reported: {diagnostics:?}"
    );
    assert!(
        diagnostics.iter().all(|diagnostic| diagnostic.line >= 1),
        "diagnostics carry deterministic line numbers"
    );

    let unknown_kind = validate(
        &mut ws,
        "schema-2",
        "dev-local",
        "apiVersion: widgets.example.com/v9\nkind: Widget\nmetadata:\n  name: gear\n",
    )
    .await;
    let YamlOutcome::Invalid { diagnostics } = unknown_kind else {
        panic!("expected schema diagnostics, got {unknown_kind:?}");
    };
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("unknown kind")),
        "unknown apiVersion/kind pairs are rejected: {diagnostics:?}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn apply_consumes_the_ticket_once_and_returns_an_operation_id() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let outcome = validate(&mut ws, "validate", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let operation_id = apply_ticket(&mut ws, "apply-1", &outcome).await;
    assert!(!operation_id.is_empty(), "apply returns an OperationId");

    // Tickets are single-use: replaying the same ticket is rejected.
    let YamlOutcome::Valid { ticket } = &outcome else {
        unreachable!()
    };
    let apply = YamlApplyRequest {
        context: ticket.target.context.clone(),
        ticket_id: ticket.id.clone(),
        target: ticket.target.clone(),
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: WEB_FRONTEND_MANIFEST.to_owned(),
    };
    send_apply(&mut ws, "replay", &apply).await;
    let message = expect_error(&mut ws, "replay", ErrorCode::Conflict).await;
    assert!(
        message.to_lowercase().contains("ticket"),
        "the rejection explains the ticket state: {message}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_tickets_are_rejected_as_conflicts_without_destroying_the_buffer_path() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let outcome = validate(&mut ws, "validate", "dev-local", WEB_FRONTEND_MANIFEST).await;

    // The cluster moves on after validation: the bound revision is stale now.
    assert!(
        fake.touch_resource(
            "dev-local",
            &Gvk::new("apps", "v1", "Deployment"),
            Some("default"),
            "web-frontend"
        )
        .is_some()
    );

    let YamlOutcome::Valid { ticket } = &outcome else {
        unreachable!()
    };
    let apply = YamlApplyRequest {
        context: ticket.target.context.clone(),
        ticket_id: ticket.id.clone(),
        target: ticket.target.clone(),
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: WEB_FRONTEND_MANIFEST.to_owned(),
    };
    send_apply(&mut ws, "stale-apply", &apply).await;
    let message = expect_error(&mut ws, "stale-apply", ErrorCode::Conflict).await;
    assert!(
        message.to_lowercase().contains("changed") || message.to_lowercase().contains("conflict"),
        "the rejection explains the staleness: {message}"
    );

    // Nothing was destroyed: re-validating the same buffer issues a fresh
    // ticket against the new revision, and that path applies cleanly.
    let fresh = validate(&mut ws, "revalidate", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let YamlOutcome::Valid {
        ticket: fresh_ticket,
    } = &fresh
    else {
        panic!("re-validation must succeed after a conflict");
    };
    assert_ne!(fresh_ticket.id, ticket.id);
    let operation_id = apply_ticket(&mut ws, "fresh-apply", &fresh).await;
    assert!(!operation_id.is_empty());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn applied_changes_reach_watchers_and_subsequent_lists() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // A create of a brand-new object validates against the cluster revision.
    let created_manifest = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: brand-new\n  namespace: default\nspec:\n  replicas: 2\n";
    let outcome = validate(&mut ws, "create-validate", "dev-local", created_manifest).await;
    let YamlOutcome::Valid { ticket } = &outcome else {
        panic!("creating a new object validates as a dry-run, got {outcome:?}");
    };
    assert!(
        !ticket.disruptive,
        "creates never restart existing workloads"
    );

    let apply = YamlApplyRequest {
        context: ticket.target.context.clone(),
        ticket_id: ticket.id.clone(),
        target: ticket.target.clone(),
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: created_manifest.to_owned(),
    };
    send_apply(&mut ws, "create-apply", &apply).await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    let accepted: k10s_protocol::OperationAccepted = frame.decode_response_payload().unwrap();
    assert!(!accepted.operation_id.as_str().is_empty());

    // The mutation is real backend state: the object shows up in lists.
    send_request(
        &mut ws,
        "list-after",
        "resource.list",
        serde_json::to_value(ResourceListRequest {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
        })
        .unwrap(),
    )
    .await;
    let frame = receive_frame(&mut ws).await;
    let list: ResourceListResponse = frame.decode_response_payload().unwrap();
    assert!(
        list.rows.iter().any(|row| row.identity.name == "brand-new"),
        "the applied create became observable backend state"
    );

    server.shutdown().await.unwrap();
}

/// Prove the shared UI client state can drive validation and apply end to
/// end, and that a forced reconnect invalidates server-issued tickets while
/// the editor keeps its dirty buffer.
#[tokio::test]
async fn client_state_seam_drives_validation_and_survives_a_forced_reconnect() {
    use k10s_ui::client::{ClientConfig, ClientPhase, ClientState, Command, ConnectTarget};
    use k10s_ui::ui::tools::{DiffKind, YamlEditor};

    let edited = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web-frontend\n  namespace: default\nspec:\n  replicas: 5\n";
    let original = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web-frontend\n  namespace: default\n";

    let (server, fake) = spawn_server().await;
    let url = format!("ws://{}{}", server.addr(), k10s_protocol::CONTROL_PATH);
    let (mut socket, _) = connect_async(&url).await.unwrap();
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(url.clone(), "secret"))
        .unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&client.take_outbound().unwrap())
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    client.apply(receive_frame(&mut socket).await).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);

    // The editor starts read-only and keeps its dirty buffer across what
    // follows; only its ticket is connection-scoped.
    let identity = k10s_protocol::ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: "web-frontend".into(),
        uid: "uid-dev-local-deployment-default-web-frontend".into(),
    };
    let mut editor = YamlEditor::for_target(identity.clone(), original);
    editor.begin_edit();
    editor.set_buffer(edited.to_owned());
    editor.review();

    let validate_request = client
        .begin(k10s_ui::client::Query::YamlValidate {
            context: "dev-local".into(),
            yaml: edited.to_owned(),
        })
        .unwrap();
    socket
        .send(Message::Text(
            serde_json::to_string(&client.take_outbound().unwrap())
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    let frame = receive_frame(&mut socket).await;
    client.apply(frame).unwrap();
    let outcome = match client.take(validate_request).expect("validation completes") {
        k10s_ui::client::QueryResult::YamlValidate(outcome) => *outcome,
        other => panic!("expected a validation result, got {other:?}"),
    };
    let YamlOutcome::Valid { ticket } = &outcome else {
        panic!("expected a valid outcome, got {outcome:?}");
    };
    assert_eq!(ticket.target, identity);
    assert_eq!(ticket.buffer_hash, buffer_hash(edited));

    editor.apply_outcome(&outcome);
    // Updating a Deployment restarts pods, so the fake marks the ticket
    // disruptive and the editor gates Apply on an explicit acknowledgement.
    if editor.has_disruption_warning() {
        editor.acknowledge_disruption();
    }
    assert!(editor.can_apply());

    // Force the connection down and reconnect: completed results — including
    // the issued ticket — are dropped, while the dirty buffer survives.
    drop(socket);
    client.transport_lost(100, 0);
    assert!(
        client.retry_if_due(u64::MAX).unwrap(),
        "the scheduled reconnect fires"
    );
    // The application layer notifies its editors on transport loss: tickets
    // die, dirty buffers survive.
    editor.connection_lost();

    // Reconnect over a fresh socket.
    let (mut socket, _) = connect_async(&url).await.unwrap();
    let hello = client.take_outbound().expect("reconnect queues a hello");
    socket
        .send(Message::Text(serde_json::to_string(&hello).unwrap().into()))
        .await
        .unwrap();
    client.apply(receive_frame(&mut socket).await).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);

    assert!(
        editor.ticket().is_none(),
        "connection loss invalidated the ticket"
    );

    // The old ticket cannot apply anymore: the cluster moved on since
    // validation, so the server rejects it as a conflict even though the
    // client session is fresh.
    assert!(
        fake.touch_resource(
            "dev-local",
            &Gvk::new("apps", "v1", "Deployment"),
            Some("default"),
            "web-frontend"
        )
        .is_some()
    );
    let apply = YamlApplyRequest {
        context: ticket.target.context.clone(),
        ticket_id: ticket.id.clone(),
        target: ticket.target.clone(),
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: edited.to_owned(),
    };
    let pending = client
        .begin_command(Command::YamlApply {
            request: apply,
            idempotency_key: "idem-after-reconnect".into(),
        })
        .unwrap();
    // Flush everything queued for the rebuilt connection (the automatic
    // bootstrap request plus the apply), then read until the apply verdict
    // arrives.
    while let Some(frame) = client.take_outbound() {
        socket
            .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
            .await
            .unwrap();
    }
    loop {
        let frame = receive_frame(&mut socket).await;
        if frame
            .request_id
            .as_ref()
            .is_some_and(|id| id == pending.id())
        {
            assert_eq!(frame.kind, ServerKind::Error, "{frame:?}");
            assert_eq!(frame.payload["code"], json!("conflict"));
            break;
        }
        client.apply(frame).unwrap();
    }
    assert!(
        editor
            .diff()
            .iter()
            .any(|line| line.kind == DiffKind::Added),
        "the dirty buffer survived the reconnect"
    );
    assert!(!editor.can_apply(), "a stale ticket can never apply");

    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn apply_envelopes_are_rejected_when_the_declared_target_differs_from_the_ticket() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // Validate web-frontend, then declare api-server as the apply target
    // while reusing web-frontend's ticket and hash verbatim.
    let outcome = validate(&mut ws, "validate", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let YamlOutcome::Valid { ticket } = &outcome else {
        unreachable!()
    };
    let mut forged = YamlApplyRequest {
        context: "dev-local".into(),
        ticket_id: ticket.id.clone(),
        target: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "api-server".into(),
            uid: "uid-dev-local-deployment-default-api-server".into(),
        },
        buffer_hash: ticket.buffer_hash.clone(),
        yaml: WEB_FRONTEND_MANIFEST.to_owned(),
    };
    send_apply(&mut ws, "forged-name", &forged).await;
    let message = expect_error(&mut ws, "forged-name", ErrorCode::Conflict).await;
    assert!(
        message.contains("target"),
        "a mismatched declared target is rejected: {message}"
    );

    // A different context with the same ticket is equally rejected.
    forged.target.name = ticket.target.name.clone();
    forged.context = "prod-readonly".into();
    send_apply(&mut ws, "forged-context", &forged).await;
    expect_error(&mut ws, "forged-context", ErrorCode::Conflict).await;

    // The honest envelope still applies: nothing was mutated by the forgeries.
    send_apply(
        &mut ws,
        "honest",
        &YamlApplyRequest {
            context: "dev-local".into(),
            ticket_id: ticket.id.clone(),
            target: ticket.target.clone(),
            buffer_hash: ticket.buffer_hash.clone(),
            yaml: WEB_FRONTEND_MANIFEST.to_owned(),
        },
    )
    .await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");

    server.shutdown().await.unwrap();
}

/// The buffer identity is an authorization boundary between validation and
/// apply, so it must be a collision-resistant digest preserved end to end.
#[tokio::test]
async fn buffer_digest_is_collision_resistant_and_stable_across_the_stack() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let outcome = validate(&mut ws, "validate", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let YamlOutcome::Valid { ticket } = &outcome else {
        panic!("expected a valid outcome");
    };
    // SHA-256 tagged encoding, stable across client and backend computation.
    assert_eq!(
        ticket.buffer_hash,
        k10s_protocol::buffer_hash(WEB_FRONTEND_MANIFEST)
    );
    assert!(
        ticket.buffer_hash.starts_with("sha-256:")
            && ticket.buffer_hash.len() == "sha-256:".len() + 64,
        "the digest carries its algorithm tag and full 256-bit width"
    );
    // The exact validated bytes redeem the ticket.
    let operation_id = apply_ticket(&mut ws, "apply", &outcome).await;
    assert!(!operation_id.is_empty());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unredeemed_tickets_expire_and_cannot_accumulate_without_bound() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // Issue one ticket and let it age past the expiry window through real
    // backend mutations; it must no longer be redeemable afterwards.
    let outcome = validate(&mut ws, "validate-1", "dev-local", WEB_FRONTEND_MANIFEST).await;
    let YamlOutcome::Valid { ticket } = &outcome else {
        unreachable!()
    };
    for _ in 0..200 {
        fake.touch_resource(
            "dev-local",
            &Gvk::new("apps", "v1", "Deployment"),
            Some("default"),
            "web-frontend",
        )
        .expect("target exists");
    }
    send_apply(
        &mut ws,
        "expired",
        &YamlApplyRequest {
            context: "dev-local".into(),
            ticket_id: ticket.id.clone(),
            target: ticket.target.clone(),
            buffer_hash: ticket.buffer_hash.clone(),
            yaml: WEB_FRONTEND_MANIFEST.to_owned(),
        },
    )
    .await;
    let message = expect_error(&mut ws, "expired", ErrorCode::Conflict).await;
    assert!(
        message.to_lowercase().contains("expire") || message.to_lowercase().contains("unknown"),
        "an aged-out ticket is rejected as expired or gone: {message}"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn ticket_store_evicts_oldest_unredeemed_tickets_under_capacity_pressure() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // Fill the bounded store well past capacity with fresh validations of
    // the same buffer (no mutations happen, so only eviction can retire).
    let first = validate(
        &mut ws,
        "validate-first",
        "dev-local",
        WEB_FRONTEND_MANIFEST,
    )
    .await;
    let YamlOutcome::Valid {
        ticket: first_ticket,
    } = &first
    else {
        unreachable!()
    };
    let mut last_outcome = None;
    for index in 0..(64 + 8) {
        last_outcome = Some(
            validate(
                &mut ws,
                &format!("validate-{index}"),
                "dev-local",
                WEB_FRONTEND_MANIFEST,
            )
            .await,
        );
    }
    let Some(YamlOutcome::Valid {
        ticket: last_ticket,
    }) = last_outcome.as_ref()
    else {
        panic!("the newest validation still issues a ticket under pressure");
    };
    assert_ne!(last_ticket.id, first_ticket.id);

    // The oldest ticket was evicted and cannot be redeemed anymore.
    send_apply(
        &mut ws,
        "evicted",
        &YamlApplyRequest {
            context: "dev-local".into(),
            ticket_id: first_ticket.id.clone(),
            target: first_ticket.target.clone(),
            buffer_hash: first_ticket.buffer_hash.clone(),
            yaml: WEB_FRONTEND_MANIFEST.to_owned(),
        },
    )
    .await;
    expect_error(&mut ws, "evicted", ErrorCode::Conflict).await;

    // The newest ticket remains redeemable.
    let apply = YamlApplyRequest {
        context: last_ticket.target.context.clone(),
        ticket_id: last_ticket.id.clone(),
        target: last_ticket.target.clone(),
        buffer_hash: last_ticket.buffer_hash.clone(),
        yaml: WEB_FRONTEND_MANIFEST.to_owned(),
    };
    send_apply(&mut ws, "newest", &apply).await;
    let frame = receive_frame(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");

    server.shutdown().await.unwrap();
}
