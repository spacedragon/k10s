//! Guarded YAML workflow state machines.
//!
//! Pure-state tests over the tools YAML editor and the shared client state:
//! read-only default, single writer, edit/review/diff flow, buffer-hash and
//! target binding, disruption acknowledgement, conflict preservation,
//! invalidation, Apply gating, and reconnect semantics. The authoritative
//! backend behavior is proven separately by `validation_loopback`.

use k10s_protocol::{
    BackendRevision, GroupVersionKind, ResumeStatus, ServerFrame, ServerKind, SessionId,
    ValidationTicket, Welcome, YamlDiagnostic, YamlOutcome, YamlValidateRequest, buffer_hash,
};
use k10s_ui::client::{
    ClientConfig, ClientPhase, ClientState, Command, ConnectTarget, Query, QueryResult,
};
use k10s_ui::ui::tools::{DiffKind, YamlEditor};
use k10s_ui::workspace::{
    LauncherItem, WindowContent, WorkloadKind, WorkspaceCommand, WorkspaceEvent, WorkspaceState,
};

const CONTEXT: &str = "dev-local";

fn identity(name: &str) -> ResourceIdentityStub {
    ResourceIdentityStub {
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

use k10s_protocol::ResourceIdentity as ResourceIdentityStub;

fn original_manifest() -> String {
    "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web-frontend\n".to_owned()
}

fn edited_manifest() -> String {
    "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web-frontend\nspec:\n  replicas: 3\n"
        .to_owned()
}

fn ticket(buffer: &str) -> ValidationTicket {
    ValidationTicket {
        id: "ticket-0001".into(),
        target: identity("web-frontend"),
        resource_revision: BackendRevision::new(1_000),
        buffer_hash: buffer_hash(buffer),
        disruptive: false,
    }
}

// ---------------------------------------------------------------------------
// Read-only default, single writer, edit/review/diff
// ---------------------------------------------------------------------------

#[test]
fn editors_start_read_only_until_edit_begins() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());

    assert_eq!(editor.phase(), k10s_ui::ui::tools::YamlPhase::ReadOnly);
    assert!(!editor.is_dirty());
    assert!(editor.ticket().is_none());
    assert!(!editor.can_apply());
    assert!(
        editor.take_apply_request().is_none(),
        "read-only editors never apply"
    );

    editor.begin_edit();
    assert_eq!(editor.phase(), k10s_ui::ui::tools::YamlPhase::Editing);
    assert!(editor.is_dirty());
}

#[test]
fn only_one_window_may_hold_the_writable_yaml_buffer_per_identity() {
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestIdentity {
        name: &'static str,
    }
    let pod = TestIdentity { name: "shared" };

    let mut state = WorkspaceState::<TestIdentity>::new();
    let out = state.apply(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(WorkloadKind::Pods),
    ));
    let window = out
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(id) => Some(*id),
            _ => None,
        })
        .unwrap();
    let out = state.apply(WorkspaceCommand::SelectRow(window, pod.clone()));
    assert!(out.is_empty());
    let out = state.apply(WorkspaceCommand::OpenDedicatedDetail(pod.clone()));
    let dedicated = out
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(id) => Some(*id),
            _ => None,
        })
        .unwrap();

    let out = state.apply(WorkspaceCommand::BeginYamlEdit(window));
    assert!(
        !out.iter()
            .any(|event| matches!(event, WorkspaceEvent::YamlOwnerInUse { .. }))
    );

    let out = state.apply(WorkspaceCommand::BeginYamlEdit(dedicated));
    match out.as_slice() {
        [WorkspaceEvent::YamlOwnerInUse { owner }] => assert_eq!(*owner, window),
        other => panic!("expected a single-writer rejection, got {other:?}"),
    }
    // The dedicated view stays read-only while the owner keeps the buffer.
    let WindowContent::Detail(detail) = &state.window(dedicated).unwrap().content else {
        panic!("dedicated window expected");
    };
    assert!(!detail.yaml.dirty);
}

#[test]
fn review_computes_a_line_diff_between_original_and_buffer() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();

    assert_eq!(editor.phase(), k10s_ui::ui::tools::YamlPhase::Reviewing);
    let diff = editor.diff();
    assert!(
        diff.iter()
            .any(|line| line.kind == DiffKind::Added && line.text.contains("replicas: 3"))
    );
    // The name line survives unchanged and is never marked as removed.
    assert!(
        !diff
            .iter()
            .any(|line| line.kind == DiffKind::Removed && line.text.contains("name: web-frontend"))
    );
    assert!(
        diff.iter()
            .any(|line| line.kind == DiffKind::Unchanged && line.text.contains("apiVersion"))
    );
}

// ---------------------------------------------------------------------------
// Buffer hash, target identity/revision binding, disruption warning
// ---------------------------------------------------------------------------

#[test]
fn valid_tickets_bind_to_the_exact_buffer_hash_and_target() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();

    let issued = ticket(&edited_manifest());
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: issued.clone(),
    });

    assert_eq!(editor.ticket().map(|t| t.id.as_str()), Some("ticket-0001"));
    assert!(editor.can_apply());
    let request = editor.take_apply_request().expect("gated apply passes");
    assert_eq!(request.buffer_hash, buffer_hash(&edited_manifest()));
    assert_eq!(request.ticket_id, "ticket-0001");
    assert_eq!(request.target, issued.target);
    assert_eq!(request.context, CONTEXT);
    assert_eq!(request.yaml, edited_manifest());
}

#[test]
fn editing_after_validation_drops_the_stale_ticket() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    assert!(editor.can_apply());

    editor.edit_again();
    editor.set_buffer(format!("{}\n# tweaked\n", edited_manifest()));
    editor.review();

    assert!(
        editor.ticket().is_none(),
        "a changed buffer invalidates the old ticket"
    );
    assert!(!editor.can_apply());
    assert!(editor.take_apply_request().is_none());
}

#[test]
fn tickets_for_other_targets_never_apply() {
    let mut editor = YamlEditor::for_target(identity("api-server"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();

    // A ticket issued for a different identity arrives (defensive wiring).
    let mut foreign = ticket(&edited_manifest());
    foreign.target = identity("web-frontend");
    editor.apply_outcome(&YamlOutcome::Valid { ticket: foreign });

    assert!(!editor.can_apply());
    assert!(
        editor.take_apply_request().is_none(),
        "cross-target tickets are rejected"
    );
}

#[test]
fn target_revision_bumps_invalidate_the_ticket() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    assert!(editor.can_apply());

    editor.on_target_revision(BackendRevision::new(1_001));

    assert!(
        editor.ticket().is_none(),
        "a newer target revision invalidates the ticket"
    );
    assert!(!editor.can_apply());
    // The dirty buffer itself survives the invalidation.
    assert!(editor.is_dirty());
    assert_eq!(editor.buffer(), edited_manifest());

    // Equal or older revisions never invalidate.
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    editor.on_target_revision(BackendRevision::new(1_000));
    assert!(editor.can_apply());
}

#[test]
fn disruptive_changes_require_an_explicit_acknowledgement() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();

    let mut disruptive = ticket(&edited_manifest());
    disruptive.disruptive = true;
    editor.apply_outcome(&YamlOutcome::Valid { ticket: disruptive });

    assert!(editor.has_disruption_warning());
    assert!(
        !editor.can_apply(),
        "disruptive applies are gated on acknowledgement"
    );

    editor.acknowledge_disruption();
    assert!(editor.can_apply());
}

// ---------------------------------------------------------------------------
// Schema errors, conflicts, invalidation, Apply gating
// ---------------------------------------------------------------------------

#[test]
fn schema_errors_preserve_the_buffer_and_clear_the_ticket() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Invalid {
        diagnostics: vec![YamlDiagnostic {
            line: 3,
            message: "missing required field metadata.name".into(),
        }],
    });

    assert!(editor.ticket().is_none());
    assert!(!editor.can_apply());
    assert_eq!(editor.diagnostics().len(), 1);
    assert_eq!(
        editor.diagnostics()[0].message,
        "missing required field metadata.name"
    );
    // The user's work is never destroyed by a failed validation.
    assert_eq!(editor.buffer(), edited_manifest());
    assert_eq!(editor.phase(), k10s_ui::ui::tools::YamlPhase::Reviewing);
}

#[test]
fn conflicts_preserve_the_dirty_buffer_while_dropping_the_ticket() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    assert!(editor.can_apply());

    editor.apply_outcome(&YamlOutcome::Conflict {
        message: "target changed since validation".into(),
    });

    assert_eq!(
        editor.conflict_message(),
        Some("target changed since validation")
    );
    assert!(editor.ticket().is_none());
    assert!(!editor.can_apply());
    assert_eq!(
        editor.buffer(),
        edited_manifest(),
        "conflicts keep the user's edits"
    );
    assert!(editor.take_apply_request().is_none());
}

#[test]
fn connection_loss_invalidates_tickets_but_keeps_the_dirty_buffer() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    assert!(editor.can_apply());

    editor.connection_lost();

    assert!(
        editor.ticket().is_none(),
        "server-issued tickets do not survive reconnect"
    );
    assert!(!editor.can_apply());
    assert!(editor.is_dirty());
    assert_eq!(editor.buffer(), edited_manifest());

    // Re-validation after reconnect restores a gated apply path.
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    assert!(editor.can_apply());
}

#[test]
fn discard_returns_to_the_read_only_original() {
    let mut editor = YamlEditor::for_target(identity("web-frontend"), &original_manifest());
    editor.begin_edit();
    editor.set_buffer(edited_manifest());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    });
    editor.discard();

    assert_eq!(editor.phase(), k10s_ui::ui::tools::YamlPhase::ReadOnly);
    assert!(!editor.is_dirty());
    assert!(editor.ticket().is_none());
    assert_eq!(editor.buffer(), original_manifest());
    assert!(editor.take_apply_request().is_none());
}

// ---------------------------------------------------------------------------
// Client-state transport: validation queries and apply commands
// ---------------------------------------------------------------------------

fn welcome() -> ServerFrame {
    ServerFrame {
        kind: ServerKind::Welcome,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::to_value(Welcome {
            protocol: k10s_protocol::ProtocolVersion { major: 1, minor: 1 },
            capabilities: vec![],
            session_id: SessionId::new("yaml-session"),
            server_instance_id: "yaml-server".to_owned(),
            resume_status: ResumeStatus::Fresh,
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
    client.apply(welcome()).unwrap();
    assert_eq!(client.phase(), ClientPhase::Ready);
    client
}

#[test]
fn yaml_validate_round_trips_through_the_shared_client_state() {
    let mut client = ready_client();
    let request = client
        .begin(Query::YamlValidate {
            context: CONTEXT.to_owned(),
            yaml: edited_manifest(),
        })
        .unwrap();
    let frame = client.take_outbound().unwrap();
    assert_eq!(frame.kind, k10s_protocol::ClientKind::Request);
    let decoded = frame.decode_payload().unwrap();
    let k10s_protocol::ClientPayload::Request(request_payload) = decoded else {
        panic!("expected a request frame");
    };
    assert_eq!(request_payload.request_kind, "yaml.validate");
    let parsed: YamlValidateRequest = serde_json::from_value(request_payload.payload).unwrap();
    assert_eq!(parsed.context, CONTEXT);
    assert_eq!(parsed.yaml, edited_manifest());

    let outcome = YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    };
    client
        .apply(ServerFrame::response(request.id().clone(), &outcome))
        .unwrap();

    match client.take(request).unwrap() {
        QueryResult::YamlValidate(received) => assert_eq!(*received, outcome),
        other => panic!("expected a validation result, got {other:?}"),
    }
}

#[test]
fn yaml_apply_is_a_separate_command_returning_an_operation_id() {
    let mut client = ready_client();
    let apply_request = k10s_protocol::YamlApplyRequest {
        context: CONTEXT.to_owned(),
        ticket_id: "ticket-0001".into(),
        target: identity("web-frontend"),
        buffer_hash: buffer_hash(&edited_manifest()),
        yaml: edited_manifest(),
    };
    let pending = client
        .begin_command(Command::YamlApply {
            request: apply_request,
            idempotency_key: "idem-yaml-1".into(),
        })
        .unwrap();

    let frame = client.take_outbound().unwrap();
    let decoded = frame.decode_payload().unwrap();
    let k10s_protocol::ClientPayload::Request(request_payload) = decoded else {
        panic!("expected a request frame");
    };
    assert_eq!(request_payload.request_kind, "yaml.apply");
    assert_eq!(
        request_payload.idempotency_key.as_deref(),
        Some("idem-yaml-1")
    );

    client
        .apply(ServerFrame::response(
            pending.id().clone(),
            k10s_protocol::OperationAccepted {
                operation_id: k10s_protocol::OperationId::new("op-000001"),
            },
        ))
        .unwrap();

    match client.take(pending).unwrap() {
        QueryResult::Applied(accepted) => {
            assert_eq!(accepted.operation_id.as_str(), "op-000001")
        }
        other => panic!("expected an applied result, got {other:?}"),
    }
}

#[test]
fn reconnect_clears_completed_validation_results_from_the_client() {
    let mut client = ready_client();
    let request = client
        .begin(Query::YamlValidate {
            context: CONTEXT.to_owned(),
            yaml: edited_manifest(),
        })
        .unwrap();
    let outcome = YamlOutcome::Valid {
        ticket: ticket(&edited_manifest()),
    };
    client
        .apply(ServerFrame::response(request.id().clone(), &outcome))
        .unwrap();
    assert!(client.take(request.clone()).is_some());

    // Transport loss tears down every server-issued artifact: the buffered
    // request results (including validation tickets) are dropped while the
    // client stays alive for the scheduled reconnect.
    client.transport_lost(1_000, 42);
    assert_eq!(client.phase(), ClientPhase::Disconnected);
    assert!(client.retry_schedule().is_some());
    assert!(
        client.take(request).is_none(),
        "stale tickets must not survive a reconnect"
    );
}
