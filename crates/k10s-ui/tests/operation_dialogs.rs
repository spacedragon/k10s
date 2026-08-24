//! Workload operation workflows: dialogs, client-state operation tracking,
//! and recovery semantics.
//!
//! Pure-state tests over the operation dialogs and the shared client state:
//! the full action matrix (scale, delete, yaml apply), exact scope identity,
//! typed delete propagation modes, disabled reasons, idempotency keys,
//! progress/success/failure/unknown operation states, retry eligibility,
//! refresh-before-retry, and querying every nonterminal `OperationId` after
//! a forced control reconnect. Authoritative backend behavior is proven
//! separately by `operation_loopback`.

use std::collections::VecDeque;

use k10s_protocol::{
    DeletePropagation, GroupVersionKind, OperationId, OperationProgress, OperationStatus,
    OperationUpdate, ResourceIdentity, ResumeStatus, ServerFrame, ServerKind, SessionId, Welcome,
};
use k10s_ui::client::{
    ClientConfig, ClientError, ClientPhase, ClientState, Command, ConnectTarget, Query, QueryResult,
};
use k10s_ui::ui::dialogs::{
    DeleteDialog, DialogAction, DialogPhase, OperationDialogs, ScaleDialog,
};

const CONTEXT: &str = "dev-local";

fn deployment(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: CONTEXT.to_owned(),
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

// ---------------------------------------------------------------------------
// Scale dialogs: disabled reasons, validation, single-shot submission
// ---------------------------------------------------------------------------

#[test]
fn scale_dialogs_disable_submission_until_the_replica_count_is_valid() {
    let mut dialog = ScaleDialog::for_target(deployment("web-frontend"), Some(20));

    assert!(
        dialog.disabled_reason().is_none(),
        "a suggested replica count starts valid"
    );
    assert!(dialog.can_submit());

    dialog.set_input("not-a-number");
    assert_eq!(
        dialog.disabled_reason(),
        Some("replicas must be a whole number between 0 and 999")
    );
    assert!(!dialog.can_submit());
    assert!(dialog.take_action().is_none(), "disabled dialogs never act");

    dialog.set_input("-3");
    assert!(dialog.disabled_reason().is_some());
    dialog.set_input("1000");
    assert!(dialog.disabled_reason().is_some());

    dialog.set_input("3");
    assert!(dialog.disabled_reason().is_none());
    let action = dialog.take_action().expect("valid input submits");
    match action {
        DialogAction::SubmitScale {
            target,
            replicas,
            idempotency_key,
        } => {
            assert_eq!(target, deployment("web-frontend"));
            assert_eq!(replicas, 3);
            assert!(
                !idempotency_key.is_empty(),
                "every submission carries an idempotency key"
            );
        }
        other => panic!("expected a scale action, got {other:?}"),
    }
    assert_eq!(
        dialog.phase(),
        DialogPhase::Submitted,
        "a consumed dialog cannot submit twice"
    );
    assert!(dialog.take_action().is_none(), "submission is single-shot");
}

#[test]
fn scale_dialogs_report_disconnection_as_a_disabled_reason() {
    let mut dialog = ScaleDialog::for_target(deployment("web-frontend"), Some(3));
    assert!(dialog.can_submit());

    dialog.connection_lost();
    assert_eq!(dialog.disabled_reason(), Some("not connected"));
    assert!(!dialog.can_submit());
    assert!(dialog.take_action().is_none());
}

#[test]
fn scale_dialogs_show_their_submitted_operation_and_failures() {
    let mut dialog = ScaleDialog::for_target(deployment("web-frontend"), Some(3));
    let _action = dialog.take_action().unwrap();
    assert_eq!(dialog.phase(), DialogPhase::Submitted);

    dialog.operation_accepted(OperationId::new("op-000001"));
    assert_eq!(
        dialog.submitted_operation().as_ref().map(|id| id.as_str()),
        Some("op-000001")
    );

    dialog.operation_failed("scale rejected");
    assert_eq!(dialog.failure_message(), Some("scale rejected"));
    assert!(dialog.can_resubmit(), "failures allow a corrected retry");
}

// ---------------------------------------------------------------------------
// Delete dialogs: typed propagation modes and typed confirmation
// ---------------------------------------------------------------------------

#[test]
fn delete_dialogs_require_typed_confirmation_and_carry_a_propagation_mode() {
    let mut dialog = DeleteDialog::for_target(deployment("api-server"));

    assert_eq!(
        dialog.disabled_reason(),
        Some("type the resource name to confirm deletion")
    );
    assert!(!dialog.can_submit());

    dialog.set_confirmation("wrong-name");
    assert!(
        dialog.disabled_reason().is_some(),
        "the exact resource name is required"
    );

    dialog.set_confirmation("api-server");
    assert!(dialog.disabled_reason().is_none());
    assert_eq!(dialog.propagation(), DeletePropagation::Background);

    dialog.set_propagation(DeletePropagation::Foreground);
    let action = dialog.take_action().expect("confirmed deletes submit");
    match action {
        DialogAction::SubmitDelete {
            target,
            propagation,
            idempotency_key,
        } => {
            assert_eq!(target, deployment("api-server"));
            assert_eq!(propagation, DeletePropagation::Foreground);
            assert!(!idempotency_key.is_empty());
        }
        other => panic!("expected a delete action, got {other:?}"),
    }
    assert!(dialog.take_action().is_none(), "deletion is single-shot");
}

#[test]
fn delete_dialogs_support_every_propagation_mode_and_disconnect_guards() {
    let mut dialog = DeleteDialog::for_target(deployment("api-server"));
    for mode in [
        DeletePropagation::Foreground,
        DeletePropagation::Background,
        DeletePropagation::Orphan,
    ] {
        dialog.set_propagation(mode);
        assert_eq!(dialog.propagation(), mode);
    }

    dialog.set_confirmation("api-server");
    assert!(dialog.can_submit());
    dialog.connection_lost();
    assert_eq!(dialog.disabled_reason(), Some("not connected"));
    assert!(dialog.take_action().is_none());
}

// ---------------------------------------------------------------------------
// The dialog store: per-window lifetime and drained submissions
// ---------------------------------------------------------------------------

#[test]
fn the_dialog_store_queues_one_action_per_window_for_the_application_layer() {
    let mut dialogs = OperationDialogs::default();
    let window = k10s_ui::workspace::WindowId(7);

    dialogs.open_scale(window, deployment("web-frontend"), Some(20));
    dialogs.open_delete(window, deployment("web-frontend"));
    assert_eq!(
        dialogs.active(window),
        Some(k10s_ui::ui::dialogs::ActiveDialogKind::Delete),
        "opening another dialog on one window replaces the first"
    );

    if let Some(k10s_ui::ui::dialogs::DialogHandle::Delete(delete)) = dialogs.active_mut(window) {
        delete.set_confirmation("web-frontend");
    }
    dialogs.submit_active(window);
    let actions = dialogs.drain_actions();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].0, window);
    assert!(matches!(actions[0].1, DialogAction::SubmitDelete { .. }));
    assert!(dialogs.drain_actions().is_empty());

    dialogs.retain(|_| false);
    assert!(dialogs.active(window).is_none());
}

// ---------------------------------------------------------------------------
// Client state: the complete action matrix through begin_command
// ---------------------------------------------------------------------------

fn welcome(resume: ResumeStatus) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(Welcome {
            protocol: k10s_protocol::ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec![],
            session_id: SessionId::new("op-session"),
            server_instance_id: "op-server".to_owned(),
            resume_status: resume,
        })
        .unwrap(),
    }
}

fn ready_client() -> ClientState {
    let mut client = ClientState::new(ClientConfig::default());
    client
        .connect(ConnectTarget::new(
            "ws://127.0.0.1/api/v1/control",
            "secret",
        ))
        .unwrap();
    let _hello = client.take_outbound().unwrap();
    client.apply(welcome(ResumeStatus::Fresh)).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);
    client
}

fn encoded_request(
    client: &mut ClientState,
) -> (
    k10s_protocol::RequestId,
    String,
    serde_json::Value,
    Option<String>,
) {
    let frame = client.take_outbound().unwrap();
    assert_eq!(frame.kind, k10s_protocol::ClientKind::Request);
    let decoded = frame.decode_payload().unwrap();
    let k10s_protocol::ClientPayload::Request(request) = decoded else {
        panic!("expected a request frame");
    };
    (
        frame.request_id.unwrap(),
        request.request_kind,
        request.payload,
        request.idempotency_key,
    )
}

#[test]
fn every_mutation_command_travels_with_an_exact_scope_identity_and_idempotency_key() {
    let mut client = ready_client();

    let scale = client
        .begin_command(Command::Scale {
            target: deployment("web-frontend"),
            replicas: 3,
            idempotency_key: "idem-scale-1".into(),
        })
        .unwrap();
    let (id, kind, payload, idem) = encoded_request(&mut client);
    assert_eq!(kind, "workload.scale");
    assert_eq!(idem.as_deref(), Some("idem-scale-1"));
    assert_eq!(payload["context"], CONTEXT);
    assert_eq!(payload["gvk"]["kind"], "Deployment");
    assert_eq!(payload["namespace"], "default");
    assert_eq!(payload["name"], "web-frontend");
    assert_eq!(
        payload["uid"],
        deployment("web-frontend").uid,
        "the exact scope identity includes the immutable UID"
    );
    assert_eq!(payload["replicas"], 3);

    client
        .apply(ServerFrame::response(
            id,
            k10s_protocol::OperationAccepted {
                operation_id: OperationId::new("op-000001"),
            },
        ))
        .unwrap();
    match client.take(scale).unwrap() {
        QueryResult::Applied(accepted) => {
            assert_eq!(accepted.operation_id.as_str(), "op-000001");
        }
        other => panic!("expected an accepted operation, got {other:?}"),
    }

    let delete = client
        .begin_command(Command::Delete {
            target: deployment("api-server"),
            propagation: DeletePropagation::Foreground,
            idempotency_key: "idem-delete-1".into(),
        })
        .unwrap();
    let (id, kind, payload, idem) = encoded_request(&mut client);
    assert_eq!(kind, "workload.delete");
    assert_eq!(idem.as_deref(), Some("idem-delete-1"));
    assert_eq!(payload["identity"]["name"], "api-server");
    assert_eq!(payload["propagation"], "foreground");

    client
        .apply(ServerFrame::response(
            id,
            k10s_protocol::OperationAccepted {
                operation_id: OperationId::new("op-000002"),
            },
        ))
        .unwrap();
    assert!(matches!(
        client.take(delete).unwrap(),
        QueryResult::Applied(_)
    ));

    let mut cron = deployment("nightly");
    cron.gvk = k10s_protocol::GroupVersionKind {
        group: "batch".into(),
        version: "v1".into(),
        kind: "CronJob".into(),
    };
    let _create = client
        .begin_command(Command::CreateJob {
            source: cron.clone(),
            idempotency_key: "idem-run-now".into(),
        })
        .unwrap();
    let (_, kind, payload, idem) = encoded_request(&mut client);
    assert_eq!(kind, "job.create");
    assert_eq!(idem.as_deref(), Some("idem-run-now"));
    assert_eq!(payload["source"]["uid"], cron.uid);

    let _suspend = client
        .begin_command(Command::SetCronJobSuspended {
            target: cron.clone(),
            suspended: true,
            idempotency_key: "idem-suspend".into(),
        })
        .unwrap();
    let (_, kind, payload, idem) = encoded_request(&mut client);
    assert_eq!(kind, "cronjob.suspend");
    assert_eq!(idem.as_deref(), Some("idem-suspend"));
    assert_eq!(payload["identity"]["uid"], cron.uid);
    assert_eq!(payload["suspended"], true);

    assert_eq!(
        client
            .submitted_operation("idem-scale-1")
            .map(|id| id.as_str()),
        Some("op-000001"),
        "accepted mutations remember their idempotency record"
    );
}

// ---------------------------------------------------------------------------
// Client state: progress, success/failure/unknown operation states
// ---------------------------------------------------------------------------

/// Build one sequenced `operationUpdate` frame; `sequence` must be
/// contiguous per client, mirroring the server's connection sequences.
fn operation_update(
    id: &str,
    status: OperationStatus,
    progress: Option<OperationProgress>,
) -> ServerFrame {
    operation_update_at(next_sequence(), id, status, progress)
}

fn next_sequence() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static SEQUENCE: Cell<u64> = const { Cell::new(0) };
    }
    SEQUENCE.with(|sequence| {
        let next = sequence.get() + 1;
        sequence.set(next);
        next
    })
}

fn operation_update_at(
    sequence: u64,
    id: &str,
    status: OperationStatus,
    progress: Option<OperationProgress>,
) -> ServerFrame {
    ServerFrame {
        kind: ServerKind::OperationUpdate,
        request_id: None,
        subscription_id: None,
        sequence: Some(sequence),
        payload: serde_json::to_value(OperationUpdate {
            operation_id: OperationId::new(id),
            status,
            progress: progress.map(|p| serde_json::to_value(p).unwrap()),
        })
        .unwrap(),
    }
}

#[test]
fn operation_updates_track_progress_and_terminal_states() {
    let mut client = ready_client();
    client
        .begin_command(Command::Scale {
            target: deployment("web-frontend"),
            replicas: 3,
            idempotency_key: "idem-progress".into(),
        })
        .unwrap();
    let (_, _, _, _) = encoded_request(&mut client);
    client
        .apply(ServerFrame::response(
            k10s_protocol::RequestId::from_u128(1),
            k10s_protocol::OperationAccepted {
                operation_id: OperationId::new("op-000001"),
            },
        ))
        .unwrap();

    client
        .apply(operation_update(
            "op-000001",
            OperationStatus::Running,
            Some(OperationProgress {
                completed: 1,
                total: 3,
            }),
        ))
        .unwrap();
    let view = client.operation(&OperationId::new("op-000001")).unwrap();
    assert_eq!(view.status(), OperationStatus::Running);
    assert_eq!(
        view.progress(),
        Some(OperationProgress {
            completed: 1,
            total: 3
        })
    );
    assert!(!view.is_terminal());

    client
        .apply(operation_update(
            "op-000001",
            OperationStatus::Succeeded,
            None,
        ))
        .unwrap();
    let view = client.operation(&OperationId::new("op-000001")).unwrap();
    assert_eq!(view.status(), OperationStatus::Succeeded);
    assert!(view.is_terminal());
    assert!(client.nonterminal_operation_ids().is_empty());

    // Failures carry their safe reason through to the retained view.
    client
        .apply(operation_update("op-000001", OperationStatus::Failed, None))
        .unwrap();
    let view = client.operation(&OperationId::new("op-000001")).unwrap();
    assert_eq!(view.status(), OperationStatus::Failed);
    assert!(view.is_terminal());
}

#[test]
fn outcome_unknown_stays_nonterminal_and_blocks_blind_retry() {
    let mut client = ready_client();
    client
        .begin_command(Command::Scale {
            target: deployment("web-frontend"),
            replicas: 3,
            idempotency_key: "idem-unknown".into(),
        })
        .unwrap();
    let (request_id, _, _, _) = encoded_request(&mut client);
    client
        .apply(ServerFrame::response(
            request_id,
            k10s_protocol::OperationAccepted {
                operation_id: OperationId::new("op-unknown"),
            },
        ))
        .unwrap();
    client
        .apply(operation_update(
            "op-unknown",
            OperationStatus::OutcomeUnknown,
            None,
        ))
        .unwrap();

    assert_eq!(
        client.nonterminal_operation_ids(),
        vec![OperationId::new("op-unknown")]
    );
    assert!(matches!(
        client.retry_eligibility("idem-unknown"),
        k10s_ui::client::RetryEligibility::Blocked
    ));
}

// ---------------------------------------------------------------------------
// Client state: forced reconnect, resync queries, retry eligibility
// ---------------------------------------------------------------------------

#[test]
fn forced_reconnect_queries_every_nonterminal_operation_and_retries_only_after_refresh() {
    let mut client = ready_client();

    // Two accepted mutations: one finishes before the drop, one does not.
    for (key, op) in [("idem-done", "op-000001"), ("idem-live", "op-000002")] {
        client
            .begin_command(Command::Scale {
                target: deployment("web-frontend"),
                replicas: 3,
                idempotency_key: key.into(),
            })
            .unwrap();
        let (id, _, _, _) = encoded_request(&mut client);
        client
            .apply(ServerFrame::response(
                id,
                k10s_protocol::OperationAccepted {
                    operation_id: OperationId::new(op),
                },
            ))
            .unwrap();
    }
    client
        .apply(operation_update(
            "op-000001",
            OperationStatus::Succeeded,
            None,
        ))
        .unwrap();
    client
        .apply(operation_update(
            "op-000002",
            OperationStatus::Running,
            Some(OperationProgress {
                completed: 1,
                total: 3,
            }),
        ))
        .unwrap();

    // A retried submission is blocked while its predecessor is nonterminal…
    assert!(
        matches!(
            client.retry_eligibility("idem-live"),
            k10s_ui::client::RetryEligibility::Blocked
        ),
        "an unfinished operation blocks reuse of its idempotency key"
    );
    assert!(matches!(
        client.retry_eligibility("idem-done"),
        k10s_ui::client::RetryEligibility::Eligible
    ));

    // …and the transport drops before op-000002 reaches a terminal state.
    client.transport_lost(1_000, 42);
    assert!(client.retry_if_due(u64::MAX).unwrap());
    let _hello = client.take_outbound().unwrap();
    client.apply(welcome(ResumeStatus::ResyncRequired)).unwrap();

    // Recovery must query EVERY nonterminal operation by ID before anything
    // may retry. Bootstrap is rebuilt first; keep reading until the
    // operation refresh request is queued behind it.
    let (refresh_id, _kind, payload, _) = loop {
        let next = encoded_request(&mut client);
        if next.1 == "operation.status" {
            break next;
        }
    };
    let ids: Vec<&str> = payload["operationIds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["op-000002"],
        "only the nonterminal operation is queried"
    );
    assert!(
        matches!(
            client.retry_eligibility("idem-live"),
            k10s_ui::client::RetryEligibility::RefreshPending
        ),
        "retry stays blocked until the refresh answers"
    );

    // The server no longer knows the operation: it becomes unknown, which
    // finally unlocks a guarded retry of the idempotency key.
    client
        .apply(ServerFrame::response(
            refresh_id,
            k10s_protocol::OperationStatusResponse {
                operations: Vec::new(),
            },
        ))
        .unwrap();
    let view = client.operation(&OperationId::new("op-000002")).unwrap();
    assert_eq!(view.status(), OperationStatus::Unknown);
    assert!(matches!(
        client.retry_eligibility("idem-live"),
        k10s_ui::client::RetryEligibility::Eligible
    ));

    // A status answer reporting a terminal state unlocks retries too.
    let (refresh_id, kind, _, _) = {
        client
            .begin(Query::OperationStatus(vec![OperationId::new("op-000003")]))
            .unwrap();
        let tuple = encoded_request(&mut client);
        assert_eq!(tuple.1, "operation.status");
        tuple
    };
    let _ = kind;
    client
        .apply(ServerFrame::response(
            refresh_id,
            k10s_protocol::OperationStatusResponse {
                operations: vec![k10s_protocol::OperationSnapshotEntry {
                    operation_id: OperationId::new("op-000003"),
                    status: OperationStatus::Failed,
                    progress: None,
                }],
            },
        ))
        .unwrap();
    assert_eq!(
        client
            .operation(&OperationId::new("op-000003"))
            .unwrap()
            .status(),
        OperationStatus::Failed
    );
}

#[test]
fn unknown_operations_never_block_retries_and_stores_stay_bounded() {
    let mut client = ready_client();

    // Unknown keys are always eligible: nothing has been submitted yet.
    assert!(matches!(
        client.retry_eligibility("never-submitted"),
        k10s_ui::client::RetryEligibility::Eligible
    ));

    // The retained operation store stays bounded: the oldest terminal
    // operations are evicted first.
    for index in 0..200_u32 {
        let id = OperationId::new(format!("op-{index:06}"));
        client
            .apply(operation_update(
                id.as_str(),
                OperationStatus::Succeeded,
                None,
            ))
            .unwrap();
    }
    assert!(
        client.tracked_operations().count() < 256,
        "retention remains bounded under pressure"
    );
    assert!(
        client.operation(&OperationId::new("op-000000")).is_none()
            && client.operation(&OperationId::new("op-000005")).is_none(),
        "the oldest terminal operations were evicted first"
    );
    let newest = OperationId::new("op-000199");
    assert!(client.operation(&newest).is_some());
}

#[test]
fn explicit_close_drops_retained_operations() {
    let mut client = ready_client();
    client
        .apply(operation_update(
            "op-000001",
            OperationStatus::Running,
            None,
        ))
        .unwrap();
    assert!(client.operation(&OperationId::new("op-000001")).is_some());
    client.user_close();
    assert_eq!(client.phase(), ClientPhase::Closed);
    assert!(client.operation(&OperationId::new("op-000001")).is_none());
    assert!(matches!(
        client.retry_eligibility("idem-x"),
        k10s_ui::client::RetryEligibility::Eligible
    ));
}

#[allow(dead_code)]
fn assert_client_error_is_displayable(error: ClientError) {
    let _ = error.to_string();
}

#[allow(dead_code)]
fn unused(_: &mut VecDeque<u8>) {}
