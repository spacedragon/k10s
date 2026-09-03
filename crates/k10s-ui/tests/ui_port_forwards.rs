//! Shared port-forward start modal and application-owned presentation behavior.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, GroupVersionKind, PortForwardFailure, PortForwardFailureCategory,
    PortForwardPodTarget, PortForwardPortSelector, PortForwardSession, PortForwardSessionId,
    PortForwardSessionState, PortForwardTarget, ResourceCapabilities, ResourceDetailResponse,
    ResourceIdentity,
};
use k10s_ui::{
    ui::{
        ConnectionState, DetailAuthority, DetailLifecycle, PortForwardAction,
        PortForwardRetryErrors, PortForwardStartModal, ResourceFeed, UiShell, WindowFreshness,
        retry_start_request,
    },
    workspace::{
        BlockResolution, LauncherItem, WindowKind, WorkloadKind, WorkspaceCommand, WorkspaceEvent,
    },
};

fn pod_identity() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: "web-0".into(),
        uid: "uid-pod".into(),
    }
}

fn pod_target() -> PortForwardTarget {
    PortForwardTarget::Pod {
        identity: pod_identity(),
        container_name: "web".into(),
        remote_port: 8_080,
    }
}

fn service_target() -> PortForwardTarget {
    PortForwardTarget::Service {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Service"),
            namespace: Some("default".into()),
            name: "web".into(),
            uid: "uid-service".into(),
        },
        port: PortForwardPortSelector::Number { number: 80 },
    }
}

fn target_identity(target: &PortForwardTarget) -> &ResourceIdentity {
    match target {
        PortForwardTarget::Service { identity, .. } | PortForwardTarget::Pod { identity, .. } => {
            identity
        }
    }
}

fn authorized_feed(target: &PortForwardTarget) -> ResourceFeed {
    let identity = target_identity(target).clone();
    let mut feed = ResourceFeed::default();
    match target {
        PortForwardTarget::Service { .. } => feed.port_forward_available = true,
        PortForwardTarget::Pod { .. } => feed.pod_port_forward_available = true,
    }
    feed.details.insert(
        identity.clone(),
        ResourceDetailResponse {
            identity: identity.clone(),
            revision: BackendRevision::new(1),
            created_at: String::new(),
            owner_references: Vec::new(),
            sections: Vec::new(),
            events_condition: k10s_protocol::EventsCondition::Available,
            events: Vec::new(),
            related: Vec::new(),
            capabilities: ResourceCapabilities::default(),
            manifest: String::new(),
            projection: None,
        },
    );
    feed.detail_authority.insert(
        identity,
        DetailAuthority {
            freshness: WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    feed
}

fn failed_session(id: &str, revision: u64) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        target: pod_target(),
        requested_local_port: 18_080,
        pod: PortForwardPodTarget {
            namespace: "default".into(),
            name: "web-0".into(),
            uid: "uid-pod".into(),
        },
        pod_port: 8_080,
        local_addr: String::new(),
        state: PortForwardSessionState::Failed,
        failure: Some(PortForwardFailure {
            category: PortForwardFailureCategory::LocalPortInUse,
            message: "local port 18080 is already in use".into(),
        }),
        revision,
    }
}

#[test]
fn modal_prefills_the_remote_port() {
    let modal = PortForwardStartModal::new(pod_target(), "web · 8080/TCP", 8_080);

    assert_eq!(modal.local_port_draft, "8080");
    assert_eq!(modal.remote_label, "web · 8080/TCP");
    assert_eq!(modal.target, pod_target());
}

#[test]
fn requested_port_normalizes_blank_and_zero_and_validates_the_u16_domain() {
    let mut modal = PortForwardStartModal::new(pod_target(), "web · 8080/TCP", 8_080);

    for draft in ["", "   ", "0", " 0 "] {
        modal.local_port_draft = draft.into();
        assert_eq!(modal.requested_port(), Ok(0), "draft {draft:?}");
    }
    for port in [1_u16, 8_080, 65_535] {
        modal.local_port_draft = port.to_string();
        assert_eq!(modal.requested_port(), Ok(port));
    }
    for draft in ["nope", "-1", "65536", "1.5"] {
        modal.local_port_draft = draft.into();
        assert!(modal.requested_port().is_err(), "draft {draft:?}");
    }
}

#[test]
fn modal_start_is_disabled_while_invalid_or_pending() {
    let mut modal = PortForwardStartModal::new(pod_target(), "web · 8080/TCP", 8_080);
    assert!(modal.can_start());

    modal.local_port_draft = "invalid".into();
    assert!(!modal.can_start());

    modal.local_port_draft = "8080".into();
    modal.pending = true;
    assert!(!modal.can_start());
}

fn render_modal(ui: &mut egui::Ui, shell: &mut UiShell<ResourceIdentity>) {
    let mut selected = Some("dev-local".to_owned());
    let feed = shell
        .port_forward_start_modal()
        .map(|modal| authorized_feed(&modal.target))
        .unwrap_or_default();
    shell.show_with_resources(
        ui,
        ConnectionState::Connected,
        &[],
        &mut selected,
        None,
        &feed,
    );
}

fn modal_harness() -> Harness<'static, UiShell<ResourceIdentity>> {
    let mut shell = UiShell::new();
    shell.open_port_forward_start(pod_target(), "web · 8080/TCP", 8_080);
    Harness::builder()
        .with_size(egui::vec2(900.0, 640.0))
        .build_ui_state(render_modal, shell)
}

struct AuthorizationFixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
}

fn render_authorization_fixture(ui: &mut egui::Ui, fixture: &mut AuthorizationFixture) {
    let mut selected = Some("dev-local".to_owned());
    fixture.shell.show_with_resources(
        ui,
        ConnectionState::Connected,
        &[],
        &mut selected,
        None,
        &fixture.feed,
    );
}

fn authorization_harness(target: PortForwardTarget) -> Harness<'static, AuthorizationFixture> {
    let mut shell = UiShell::new();
    shell.open_port_forward_start(target.clone(), "remote port", 8_080);
    Harness::builder()
        .with_size(egui::vec2(900.0, 640.0))
        .build_ui_state(
            render_authorization_fixture,
            AuthorizationFixture {
                shell,
                feed: authorized_feed(&target),
            },
        )
}

#[test]
fn pod_capability_loss_after_open_disables_modal_submission() {
    let mut harness = authorization_harness(pod_target());
    harness.run_steps(2);
    harness.state_mut().feed.pod_port_forward_available = false;
    harness.run_steps(2);

    let start = harness.get_by_role_and_label(Role::Button, "Start port forward");
    assert!(start.accesskit_node().is_disabled());
    harness.get_by_label("Pod port forwarding is unavailable on this connection");
    start.click();
    harness.run_steps(2);
    assert!(
        harness
            .state_mut()
            .shell
            .drain_port_forward_actions()
            .is_empty()
    );
}

#[test]
fn service_authority_loss_after_open_disables_modal_submission() {
    let target = service_target();
    let identity = target_identity(&target).clone();
    let mut harness = authorization_harness(target);
    harness.run_steps(2);
    harness.state_mut().feed.detail_authority.insert(
        identity,
        DetailAuthority {
            freshness: WindowFreshness::StaleRetrying {
                last_sync_age: "30s".into(),
                retry_in: "2s".into(),
                attempt: 1,
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    harness.run_steps(2);

    let start = harness.get_by_role_and_label(Role::Button, "Start port forward");
    assert!(start.accesskit_node().is_disabled());
    harness.get_by_label("Port forwarding requires live, matching resource details");
    start.click();
    harness.run_steps(2);
    assert!(
        harness
            .state_mut()
            .shell
            .drain_port_forward_actions()
            .is_empty()
    );
}

#[test]
fn service_modal_submits_blank_and_zero_as_automatic_local_port() {
    for draft in ["", "0"] {
        let target = service_target();
        let mut harness = authorization_harness(target.clone());
        harness
            .state_mut()
            .shell
            .port_forward_start_modal_mut()
            .unwrap()
            .local_port_draft = draft.into();
        harness.run_steps(2);
        harness
            .get_by_role_and_label(Role::Button, "Start port forward")
            .click();
        harness.run_steps(2);

        assert!(matches!(
            harness
                .state_mut()
                .shell
                .drain_port_forward_actions()
                .as_slice(),
            [PortForwardAction::Start { request, .. }]
                if request.target() == &target && request.local_port() == 0
        ));
    }
}

#[test]
fn cancel_closes_the_shared_modal() {
    let mut harness = modal_harness();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "Cancel port forward")
        .click();
    harness.run_steps(2);

    assert!(harness.state().port_forward_start_modal().is_none());
}

#[test]
fn cancel_closes_the_shared_modal_while_start_is_pending() {
    let mut harness = modal_harness();
    harness
        .state_mut()
        .port_forward_start_modal_mut()
        .unwrap()
        .pending = true;
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "Cancel port forward")
        .click();
    harness.run_steps(2);

    assert!(harness.state().port_forward_start_modal().is_none());
}

#[test]
fn recoverable_error_preserves_the_draft_and_reenables_start() {
    let mut shell: UiShell<ResourceIdentity> = UiShell::new();
    shell.open_port_forward_start(pod_target(), "web · 8080/TCP", 8_080);
    shell
        .port_forward_start_modal_mut()
        .unwrap()
        .local_port_draft = "18080".into();
    shell.port_forward_start_modal_mut().unwrap().pending = true;

    shell.port_forward_start_failed("local port 18080 is already in use");

    let modal = shell.port_forward_start_modal().unwrap();
    assert_eq!(modal.local_port_draft, "18080");
    assert_eq!(
        modal.error.as_deref(),
        Some("local port 18080 is already in use")
    );
    assert!(!modal.pending);
    assert!(modal.can_start());
}

#[test]
fn modal_start_emits_a_validated_target_request_and_becomes_pending() {
    let mut harness = modal_harness();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "Start port forward")
        .click();
    harness.run_steps(2);

    let actions = harness.state_mut().drain_port_forward_actions();
    assert!(matches!(
        actions.as_slice(),
        [PortForwardAction::Start { request, .. }]
            if request.target() == &pod_target() && request.local_port() == 8_080
    ));
    assert!(harness.state().port_forward_start_modal().unwrap().pending);
}

#[test]
fn success_and_duplicate_success_close_the_modal_and_focus_the_returned_session() {
    for session_id in ["new-session", "existing-duplicate"] {
        let mut shell: UiShell<ResourceIdentity> = UiShell::new();
        shell.open_port_forward_start(pod_target(), "web · 8080/TCP", 8_080);

        shell.port_forward_start_succeeded(session_id);

        assert!(shell.port_forward_start_modal().is_none());
        let windows: Vec<_> = shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == WindowKind::PortForwards)
            .collect();
        assert_eq!(windows.len(), 1);
        assert_eq!(
            shell
                .workspace()
                .port_forward_state(windows[0].id)
                .unwrap()
                .focused_session
                .as_deref(),
            Some(session_id)
        );
    }
}

#[test]
fn successful_start_focus_replays_after_dirty_yaml_guard_resolution() {
    let mut shell: UiShell<ResourceIdentity> = UiShell::new();
    let opened = shell.apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(WorkloadKind::Pods),
    ));
    let pods = opened
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(window) => Some(*window),
            _ => None,
        })
        .unwrap();
    shell.apply_workspace_command(WorkspaceCommand::SelectRow(pods, pod_identity()));
    shell.apply_workspace_command(WorkspaceCommand::BeginYamlEdit(pods));
    assert!(matches!(
        shell
            .apply_workspace_command(WorkspaceCommand::CloseWindow(pods))
            .as_slice(),
        [WorkspaceEvent::Blocked(_)]
    ));

    shell.focus_port_forward_session("started-during-guard");
    assert!(
        shell
            .workspace()
            .windows()
            .iter()
            .all(|window| window.kind != WindowKind::PortForwards)
    );

    shell.apply_workspace_command(WorkspaceCommand::ResolveBlock(
        BlockResolution::DiscardYaml { window: pods },
    ));

    let manager = shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::PortForwards)
        .expect("the completed start focus is replayed after the guard resolves");
    assert_eq!(
        shell
            .workspace()
            .port_forward_state(manager.id)
            .unwrap()
            .focused_session
            .as_deref(),
        Some("started-during-guard")
    );
}

#[test]
fn cancelled_start_error_cannot_overwrite_reopened_same_target_modal() {
    let mut shell: UiShell<ResourceIdentity> = UiShell::new();
    let target = pod_target();
    let first = shell.open_port_forward_start(target.clone(), "first", 8_080);
    shell.port_forward_start_modal_mut().unwrap().pending = true;
    shell.dismiss_port_forward_start();
    shell.open_port_forward_start(target.clone(), "second", 9_090);

    shell.port_forward_start_failed_for(first, "stale error");

    let current = shell.port_forward_start_modal().unwrap();
    assert_eq!(current.remote_label, "second");
    assert_eq!(current.local_port_draft, "9090");
    assert_eq!(current.error, None);
}

#[test]
fn cancelled_start_success_cannot_close_reopened_same_target_modal() {
    let mut shell: UiShell<ResourceIdentity> = UiShell::new();
    let target = pod_target();
    let first = shell.open_port_forward_start(target.clone(), "first", 8_080);
    shell.port_forward_start_modal_mut().unwrap().pending = true;
    shell.dismiss_port_forward_start();
    shell.open_port_forward_start(target.clone(), "second", 9_090);

    shell.port_forward_start_succeeded_for(first, "stale-session");

    let current = shell.port_forward_start_modal().unwrap();
    assert_eq!(current.remote_label, "second");
    assert_eq!(current.local_port_draft, "9090");
}

#[test]
fn retry_uses_the_retained_failed_snapshot_target_and_requested_port() {
    let session = failed_session("failed", 7);

    let request = retry_start_request(&session).unwrap();

    assert_eq!(request.target(), &session.target);
    assert_eq!(request.local_port(), session.requested_local_port);
}

#[test]
fn retry_conflict_overlay_is_reconciled_without_mutating_authoritative_sessions() {
    const GUIDANCE: &str =
        "Local port is in use; start a new forward from the Pod or Service with another port.";
    let session = failed_session("failed", 7);
    let authoritative = vec![session.clone()];
    let mut errors = PortForwardRetryErrors::default();

    errors.local_port_conflict(&session);
    assert_eq!(errors.get(&session.id), Some(GUIDANCE));
    assert_eq!(authoritative, vec![session.clone()]);

    errors.reconcile(&authoritative);
    assert_eq!(errors.get(&session.id), Some(GUIDANCE));

    let mut revised = session.clone();
    revised.revision = 8;
    errors.reconcile(std::slice::from_ref(&revised));
    assert_eq!(errors.get(&session.id), None);

    errors.local_port_conflict(&revised);
    errors.retry_succeeded(&revised.id);
    assert_eq!(errors.get(&session.id), None);

    errors.local_port_conflict(&revised);
    errors.reconcile(&[]);
    assert_eq!(errors.get(&session.id), None);
}

#[test]
fn service_targets_can_use_the_same_modal() {
    let identity = ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: "api".into(),
        uid: "uid-service".into(),
    };
    let target = PortForwardTarget::Service {
        identity,
        port: PortForwardPortSelector::Name {
            name: "https".into(),
        },
    };
    let modal = PortForwardStartModal::new(target.clone(), "https · 443/TCP", 443);

    assert_eq!(modal.target, target);
    assert_eq!(modal.requested_port(), Ok(443));
}
