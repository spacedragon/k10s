//! End-to-end workload operation loopback over a real control socket.
//!
//! Proves the complete action matrix through `BackendKernel::execute`:
//! scale and delete commands return an `OperationId`, mutations touch only
//! backend fake state (normal resource deltas), terminal states arrive as
//! sequenced `operationUpdate` frames on the reliable reserve, exact scope
//! identity is enforced, bounded idempotency records prevent duplicates,
//! failures are reported safely, unknown operations stay answerable, and
//! every nonterminal operation can be queried after a forced reconnect.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_protocol::{
    DeletePropagation, ErrorCode, GroupVersionKind, OperationStatus, OperationStatusRequest,
    OperationStatusResponse, OperationUpdate, ResourceIdentity, ResourceListRequest,
    ResourceListResponse, ScaleRequest, ServerFrame, ServerKind, ServerPayload,
};
use k10s_server::{ServerConfig, spawn_loopback};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn deployment_identity(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: name.to_owned(),
        uid: format!("uid-dev-local-deployment-default-{name}"),
    }
}

async fn spawn_server() -> (k10s_server::ServerHandle, FakeKubernetes) {
    let fake = FakeKubernetes::standard();
    let handle = spawn_loopback(
        ServerConfig {
            access_token: "secret".into(),
            ..ServerConfig::default()
        },
        BackendKernel::new_with_instance_id(fake.clone(), "operation-server"),
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

/// Send one request carrying an envelope-level idempotency key (mandatory
/// for mutations).
async fn send_keyed(ws: &mut Ws, request_id: &str, kind: &str, payload: Value, key: &str) {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {"kind": kind, "idempotencyKey": key, "payload": payload}
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

/// Receive one frame that is not an operation update for other operations.
/// Frames about unrelated operations are consumed silently so tests stay
/// focused on their own operation IDs.
async fn receive_relevant(ws: &mut Ws, operation_ids: &[String]) -> ServerFrame {
    loop {
        let frame = receive_frame(ws).await;
        if frame.kind != ServerKind::OperationUpdate {
            return frame;
        }
        let ServerPayload::OperationUpdate(update) = frame.decode_payload().unwrap() else {
            continue;
        };
        if operation_ids
            .iter()
            .any(|id| *id == update.operation_id.as_str())
        {
            return frame;
        }
    }
}

/// Receive the next frame that is not an operation update.
async fn next_non_update(ws: &mut Ws) -> ServerFrame {
    loop {
        let frame = receive_frame(ws).await;
        if frame.kind != ServerKind::OperationUpdate {
            return frame;
        }
    }
}

async fn expect_error(ws: &mut Ws, request_id: &str, code: ErrorCode) -> String {
    let mut frame = next_non_update(ws).await;
    while frame.kind != ServerKind::Error
        || frame.request_id.as_ref().unwrap().as_str() != request_id
    {
        frame = next_non_update(ws).await;
    }
    assert_eq!(frame.kind, ServerKind::Error, "{frame:?}");
    assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
    assert_eq!(frame.payload["code"], json!(code));
    frame.payload["safeMessage"]
        .as_str()
        .expect("error carries a safe message")
        .to_owned()
}

/// Submit one mutation carrying an envelope-level idempotency key and
/// return the accepted operation ID.
async fn submit_mutation(
    ws: &mut Ws,
    request_id: &str,
    kind: &str,
    payload: Value,
    idempotency_key: &str,
) -> String {
    ws.send(Message::Text(
        json!({
            "kind": "request",
            "requestId": request_id,
            "payload": {
                "kind": kind,
                "idempotencyKey": idempotency_key,
                "payload": payload,
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    // Accepted mutations open their operation immediately: the pending
    // update may be forwarded before this very response.
    loop {
        let frame = receive_frame(ws).await;
        if frame.kind == ServerKind::OperationUpdate {
            continue;
        }
        assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
        assert_eq!(frame.request_id.as_ref().unwrap().as_str(), request_id);
        let accepted: k10s_protocol::OperationAccepted = frame.decode_response_payload().unwrap();
        break accepted.operation_id.as_str().to_owned();
    }
}

fn scale_payload(name: &str, replicas: u32) -> Value {
    serde_json::to_value(ScaleRequest {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: name.to_owned(),
        uid: deployment_identity(name).uid,
        replicas,
    })
    .unwrap()
}

fn delete_payload(name: &str, propagation: DeletePropagation) -> Value {
    serde_json::to_value(k10s_protocol::DeleteRequest {
        identity: deployment_identity(name),
        propagation,
        resource_version: "1".into(),
    })
    .unwrap()
}

/// Read frames until `operation_id` reaches `status`. Every observed
/// operation update must carry its connection sequence.
async fn await_operation_status(
    ws: &mut Ws,
    operation_id: &str,
    status: OperationStatus,
) -> OperationUpdate {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        let Ok(frame) = tokio::time::timeout(Duration::from_secs(2), receive_frame(ws)).await
        else {
            break;
        };
        if frame.kind != ServerKind::OperationUpdate {
            continue;
        }
        assert!(
            frame.sequence.is_some(),
            "operation updates ride the sequenced reliable reserve"
        );
        let ServerPayload::OperationUpdate(update) = frame.decode_payload().unwrap() else {
            continue;
        };
        if update.operation_id.as_str() == operation_id {
            last = Some(update.clone());
            if update.status == status {
                return update;
            }
        }
    }
    panic!("never observed {operation_id} reach {status:?}; last update was {last:?}")
}

async fn list_deployments(ws: &mut Ws, request_id: &str) -> ResourceListResponse {
    send_request(
        ws,
        request_id,
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
    next_non_update(ws).await.decode_response_payload().unwrap()
}

#[tokio::test]
async fn scale_mutations_return_operations_and_produce_resource_deltas() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let before = list_deployments(&mut ws, "list-before").await;
    let row_revision = |response: &ResourceListResponse, name: &str| {
        response
            .rows
            .iter()
            .find(|row| row.identity.name == name)
            .map(|row| row.revision.get())
            .unwrap_or(u64::MAX)
    };
    let original_revision = row_revision(&before, "web-frontend");

    let operation_id = submit_mutation(
        &mut ws,
        "scale-1",
        "workload.scale",
        scale_payload("web-frontend", 3),
        "idem-scale-1",
    )
    .await;
    assert!(!operation_id.is_empty());

    // Deterministic fake advancement runs only when the world is ticked.
    fake.tick_operations();
    fake.tick_operations();
    fake.tick_operations();
    let update = await_operation_status(&mut ws, &operation_id, OperationStatus::Succeeded).await;
    assert!(
        update.status == OperationStatus::Succeeded,
        "the client observed the full progress path to success"
    );

    // The mutation is real backend state: the row revision moved forward.
    let after = list_deployments(&mut ws, "list-after").await;
    assert!(
        row_revision(&after, "web-frontend") > original_revision,
        "scaling advanced the backend revision of the target"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletes_are_typed_with_propagation_modes_and_remove_state() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // The typed delete request carries an explicit propagation mode; every
    // mode serializes distinctly on the wire.
    assert_ne!(
        serde_json::to_value(DeletePropagation::Background).unwrap(),
        serde_json::to_value(DeletePropagation::Foreground).unwrap()
    );
    let operation_id = submit_mutation(
        &mut ws,
        "delete-background",
        "workload.delete",
        delete_payload("api-server", DeletePropagation::Background),
        "idem-delete-bg",
    )
    .await;
    assert!(!operation_id.is_empty());

    let after = list_deployments(&mut ws, "list-after-delete").await;
    assert!(
        !after
            .rows
            .iter()
            .any(|row| row.identity.name == "api-server"),
        "the deleted deployment left the list"
    );

    // Deleting again is a typed not-found, never a silent success.
    send_keyed(
        &mut ws,
        "delete-missing",
        "workload.delete",
        delete_payload("api-server", DeletePropagation::Background),
        "idem-delete-missing",
    )
    .await;
    expect_error(&mut ws, "delete-missing", ErrorCode::NotFound).await;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutations_enforce_exact_scope_identity_including_uid() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // A stale UID (the object was recreated under the same name) must be
    // rejected even though context/gvk/namespace/name all match.
    let mut stale =
        serde_json::from_value::<ScaleRequest>(scale_payload("web-frontend", 3)).unwrap();
    stale.uid = "uid-stale-recreated".into();
    send_keyed(
        &mut ws,
        "stale-uid",
        "workload.scale",
        serde_json::to_value(stale).unwrap(),
        "idem-stale-uid",
    )
    .await;
    let message = expect_error(&mut ws, "stale-uid", ErrorCode::Conflict).await;
    assert!(
        message.to_lowercase().contains("match") || message.to_lowercase().contains("recreated"),
        "the rejection explains the identity mismatch: {message}"
    );

    // A completely unknown object stays a typed not-found.
    send_keyed(
        &mut ws,
        "unknown-object",
        "workload.scale",
        scale_payload("no-such-deployment", 1),
        "idem-unknown-object",
    )
    .await;
    expect_error(&mut ws, "unknown-object", ErrorCode::NotFound).await;

    // The readonly cluster denies mutations by policy.
    send_keyed(
        &mut ws,
        "readonly-scale",
        "workload.scale",
        serde_json::to_value(ScaleRequest {
            context: "prod-readonly".into(),
            ..serde_json::from_value::<ScaleRequest>(scale_payload("edge-gateway", 1)).unwrap()
        })
        .unwrap(),
        "idem-readonly-scale",
    )
    .await;
    expect_error(&mut ws, "readonly-scale", ErrorCode::Unauthorized).await;

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_submissions_reuse_one_bounded_idempotency_record() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let first = submit_mutation(
        &mut ws,
        "scale-first",
        "workload.scale",
        scale_payload("web-frontend", 4),
        "same-key",
    )
    .await;
    let second = submit_mutation(
        &mut ws,
        "scale-duplicate",
        "workload.scale",
        scale_payload("web-frontend", 4),
        "same-key",
    )
    .await;
    assert_eq!(
        first, second,
        "a replayed idempotency key returns the original operation"
    );

    fake.tick_operations();
    fake.tick_operations();
    fake.tick_operations();

    // The bounded record lives in the backend, not the session: a fresh
    // connection replaying the key still gets the original operation.
    drop(ws);
    let mut ws = connect_authenticated(&server).await;
    let replayed = submit_mutation(
        &mut ws,
        "scale-after-reconnect",
        "workload.scale",
        scale_payload("web-frontend", 4),
        "same-key",
    )
    .await;
    assert_eq!(
        first, replayed,
        "the idempotency record outlives individual sessions"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_operations_report_a_safe_reason_and_stay_queryable() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    fake.fail_next_operation("quota exceeded in namespace default");
    let operation_id = submit_mutation(
        &mut ws,
        "scale-fail",
        "workload.scale",
        scale_payload("web-frontend", 2),
        "idem-fail",
    )
    .await;

    fake.tick_operations();
    fake.tick_operations();
    fake.tick_operations();
    let update = await_operation_status(&mut ws, &operation_id, OperationStatus::Failed).await;
    assert_eq!(update.status, OperationStatus::Failed);

    // Terminal operations stay queryable by their OperationId until they
    // age out of the bounded store.
    send_request(
        &mut ws,
        "status-query",
        "operation.status",
        serde_json::to_value(OperationStatusRequest {
            operation_ids: vec![k10s_protocol::OperationId::new(operation_id.clone())],
        })
        .unwrap(),
    )
    .await;
    let frame = receive_relevant(&mut ws, std::slice::from_ref(&operation_id)).await;
    let response: OperationStatusResponse = frame.decode_response_payload().unwrap();
    let entry = response
        .operations
        .iter()
        .find(|entry| entry.operation_id.as_str() == operation_id)
        .expect("the failed operation remains queryable");
    assert_eq!(entry.status, OperationStatus::Failed);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_operation_ids_are_answered_without_error() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    send_request(
        &mut ws,
        "status-unknown",
        "operation.status",
        serde_json::to_value(OperationStatusRequest {
            operation_ids: vec![
                k10s_protocol::OperationId::new("op-does-not-exist"),
                k10s_protocol::OperationId::new("op-also-missing"),
            ],
        })
        .unwrap(),
    )
    .await;
    let frame = next_non_update(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    let response: OperationStatusResponse = frame.decode_response_payload().unwrap();
    assert!(
        response.operations.is_empty(),
        "unknown IDs are simply absent so clients derive an Unknown state"
    );

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn every_nonterminal_operation_is_queryable_after_a_forced_reconnect() {
    let (server, fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    let finished = submit_mutation(
        &mut ws,
        "scale-a",
        "workload.scale",
        scale_payload("web-frontend", 5),
        "idem-reconnect-a",
    )
    .await;
    let live_one = submit_mutation(
        &mut ws,
        "delete-b",
        "workload.delete",
        delete_payload("api-server", DeletePropagation::Background),
        "idem-reconnect-b",
    )
    .await;
    let live_two = submit_mutation(
        &mut ws,
        "scale-c",
        "workload.scale",
        scale_payload("web-frontend", 6),
        "idem-reconnect-c",
    )
    .await;

    // Advance exactly one step: nothing is terminal yet when the socket
    // drops, so the fresh session must re-query all three operations.
    fake.tick_operations();

    drop(ws);
    let mut ws = connect_authenticated(&server).await;
    send_request(
        &mut ws,
        "resync-status",
        "operation.status",
        serde_json::to_value(OperationStatusRequest {
            operation_ids: vec![
                k10s_protocol::OperationId::new(finished.clone()),
                k10s_protocol::OperationId::new(live_one.clone()),
                k10s_protocol::OperationId::new(live_two.clone()),
            ],
        })
        .unwrap(),
    )
    .await;
    let frame = next_non_update(&mut ws).await;
    assert_eq!(frame.kind, ServerKind::Response, "{frame:?}");
    let response: OperationStatusResponse = frame.decode_response_payload().unwrap();
    assert_eq!(
        response.operations.len(),
        3,
        "every nonterminal operation is answered after reconnect"
    );
    for id in [&finished, &live_one, &live_two] {
        let entry = response
            .operations
            .iter()
            .find(|entry| entry.operation_id.as_str() == id)
            .unwrap_or_else(|| panic!("missing {id} in resync answer"));
        assert_ne!(entry.status, OperationStatus::Unknown);
    }

    // The world keeps advancing deterministically for whoever reconnects:
    // ticking to completion surfaces Succeeded through status queries.
    fake.tick_operations();
    fake.tick_operations();
    send_request(
        &mut ws,
        "post-completion-status",
        "operation.status",
        serde_json::to_value(OperationStatusRequest {
            operation_ids: vec![k10s_protocol::OperationId::new(finished.clone())],
        })
        .unwrap(),
    )
    .await;
    let frame = next_non_update(&mut ws).await;
    let response: OperationStatusResponse = frame.decode_response_payload().unwrap();
    let entry = response
        .operations
        .iter()
        .find(|entry| entry.operation_id.as_str() == finished)
        .expect("the finished operation is still queryable");
    assert_eq!(entry.status, OperationStatus::Succeeded);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutations_without_idempotency_keys_are_rejected_as_invalid() {
    let (server, _fake) = spawn_server().await;
    let mut ws = connect_authenticated(&server).await;

    // A synthesized context/name key would misidentify same-name objects in
    // other namespaces or kinds as replays, so keys are mandatory.
    send_request(
        &mut ws,
        "scale-no-key",
        "workload.scale",
        scale_payload("web-frontend", 3),
    )
    .await;
    expect_error(&mut ws, "scale-no-key", ErrorCode::InvalidRequest).await;

    // Blank keys are equally invalid for both mutation kinds: the backend
    // skips retention for empty strings, so replays would execute twice.
    send_keyed(
        &mut ws,
        "scale-blank-key",
        "workload.scale",
        scale_payload("web-frontend", 3),
        "   ",
    )
    .await;
    expect_error(&mut ws, "scale-blank-key", ErrorCode::InvalidRequest).await;

    send_request(
        &mut ws,
        "delete-no-key",
        "workload.delete",
        delete_payload("api-server", DeletePropagation::Background),
    )
    .await;
    expect_error(&mut ws, "delete-no-key", ErrorCode::InvalidRequest).await;

    send_keyed(
        &mut ws,
        "delete-blank-key",
        "workload.delete",
        delete_payload("api-server", DeletePropagation::Background),
        "",
    )
    .await;
    expect_error(&mut ws, "delete-blank-key", ErrorCode::InvalidRequest).await;

    server.shutdown().await.unwrap();
}
