//! Cross-feature recovery projections shared by fake and real Kubernetes modes.

use k10s_protocol::{
    BackendRevision, GroupVersionKind, ResourceIdentity, StreamTarget, ValidationTicket,
    YamlOutcome, buffer_hash,
};
use k10s_ui::ui::tools::{LogsPhase, LogsTool, ShellPhase, ShellTool, YamlEditor};
use k10s_ui::workspace::{
    BlockResolution, LauncherItem, WorkloadKind, WorkspaceCommand, WorkspaceEvent, WorkspaceState,
};

fn identity() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev".into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: "web".into(),
        uid: "uid-web".into(),
    }
}

fn target() -> StreamTarget {
    StreamTarget {
        context: "dev".into(),
        namespace: "default".into(),
        pod: "web-0".into(),
        uid: "uid-web-0".into(),
        container: "app".into(),
    }
}

#[test]
fn conflicts_watch_drift_and_reconnect_revoke_only_server_authority() {
    let original = "apiVersion: apps/v1\nkind: Deployment\n";
    let edited = format!("{original}spec:\n  replicas: 3\n");
    let mut editor = YamlEditor::for_target(identity(), original);
    editor.begin_edit();
    editor.set_buffer(edited.clone());
    editor.review();
    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ValidationTicket {
            id: "validation".into(),
            target: identity(),
            resource_revision: BackendRevision::new(10),
            buffer_hash: buffer_hash(&edited),
            disruptive: false,
        },
    });
    assert!(editor.can_apply());

    editor.apply_outcome(&YamlOutcome::Conflict {
        message: "target changed since validation".into(),
    });
    assert!(editor.is_dirty());
    assert_eq!(editor.buffer(), edited);
    assert!(editor.ticket().is_none());

    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ValidationTicket {
            id: "validation-2".into(),
            target: identity(),
            resource_revision: BackendRevision::new(10),
            buffer_hash: buffer_hash(editor.buffer()),
            disruptive: false,
        },
    });
    editor.on_target_revision(BackendRevision::new(11));
    assert!(editor.ticket().is_none());
    assert!(editor.is_dirty());

    editor.apply_outcome(&YamlOutcome::Valid {
        ticket: ValidationTicket {
            id: "validation-3".into(),
            target: identity(),
            resource_revision: BackendRevision::new(11),
            buffer_hash: buffer_hash(editor.buffer()),
            disruptive: false,
        },
    });
    editor.connection_lost();
    assert!(editor.ticket().is_none());
    assert_eq!(editor.buffer(), edited);
}

#[test]
fn stream_loss_preserves_logs_but_exec_never_resumes_implicitly() {
    let mut logs = LogsTool::new(target(), 16);
    logs.connect();
    logs.attach();
    logs.append("before drop");
    logs.connection_lost();
    assert_eq!(logs.phase(), LogsPhase::Disconnected);
    assert_eq!(
        logs.visible_lines().map(String::as_str).collect::<Vec<_>>(),
        ["before drop"]
    );
    logs.connect();
    assert_eq!(
        logs.phase(),
        LogsPhase::Connecting,
        "logs reconnect only explicitly"
    );

    let mut shell = ShellTool::new(target());
    shell.connect();
    shell.attach();
    shell.apply_output("remote output\n");
    shell.connection_lost();
    assert!(matches!(shell.phase(), ShellPhase::Failed(_)));
    shell.connect();
    assert!(
        matches!(shell.phase(), ShellPhase::Failed(_)),
        "a failed exec cannot silently resume"
    );
    shell.dismiss_failure();
    shell.connect();
    assert_eq!(*shell.phase(), ShellPhase::Connecting);
}

#[test]
fn active_exec_guard_blocks_context_switch_until_disconnect_resolution() {
    let mut workspace = WorkspaceState::<ResourceIdentity>::new();
    workspace.apply(WorkspaceCommand::CommitContextSwitch { to: "dev".into() });
    let opened = workspace.apply(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(WorkloadKind::Pods),
    ));
    let window = opened
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(window) => Some(*window),
            _ => None,
        })
        .unwrap();
    workspace.apply(WorkspaceCommand::SelectRow(window, identity()));
    workspace.apply(WorkspaceCommand::ConnectShell(window));
    let blocked = workspace.apply(WorkspaceCommand::ContextSwitch { to: "prod".into() });
    assert!(
        blocked
            .iter()
            .any(|event| matches!(event, WorkspaceEvent::Blocked(_)))
    );
    let resolved = workspace.apply(WorkspaceCommand::ResolveBlock(
        BlockResolution::DisconnectShell { window },
    ));
    assert!(resolved.iter().any(|event| matches!(
        event,
        WorkspaceEvent::ContextSwitchRequested { to } if to == "prod"
    )));
}
