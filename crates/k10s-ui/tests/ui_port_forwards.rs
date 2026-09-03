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
        ConnectionState, DetailAuthority, DetailLifecycle, PortForwardAction, PortForwardListState,
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

fn session(
    id: &str,
    target: PortForwardTarget,
    state: PortForwardSessionState,
    local_addr: &str,
) -> PortForwardSession {
    let (pod, pod_port) = match &target {
        PortForwardTarget::Service { .. } => (
            PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-backing-0".into(),
                uid: "uid-backing-pod".into(),
            },
            8_080,
        ),
        PortForwardTarget::Pod {
            identity,
            remote_port,
            ..
        } => (
            PortForwardPodTarget {
                namespace: identity.namespace.clone().unwrap_or_default(),
                name: identity.name.clone(),
                uid: identity.uid.clone(),
            },
            *remote_port,
        ),
    };
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        target,
        requested_local_port: 18_080,
        pod,
        pod_port,
        local_addr: local_addr.into(),
        state,
        failure: (state == PortForwardSessionState::Failed).then(|| PortForwardFailure {
            category: PortForwardFailureCategory::VanishedResource,
            message: "safe failure: upstream pod disappeared".into(),
        }),
        revision: 1,
    }
}

struct ManagementFixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
    connection: ConnectionState,
}

fn render_management(ui: &mut egui::Ui, fixture: &mut ManagementFixture) {
    let mut selected = Some("dev-local".to_owned());
    fixture.shell.show_with_resources(
        ui,
        fixture.connection,
        &[],
        &mut selected,
        None,
        &fixture.feed,
    );
}

fn management_harness(sessions: Vec<PortForwardSession>) -> Harness<'static, ManagementFixture> {
    let mut shell = UiShell::new();
    let opened = shell.apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::PortForwards,
    ));
    let window = opened
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(window) => Some(*window),
            _ => None,
        })
        .unwrap();
    shell.apply_workspace_command(WorkspaceCommand::SetGeometry(
        window,
        k10s_ui::workspace::WindowGeom {
            position: [0.0, 0.0],
            size: [1_000.0, 600.0],
            collapsed: false,
        },
    ));
    Harness::builder()
        .with_size(egui::vec2(1_280.0, 800.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(
            render_management,
            ManagementFixture {
                shell,
                feed: ResourceFeed {
                    port_forward_available: true,
                    pod_port_forward_available: true,
                    port_forward_sessions: sessions,
                    ..ResourceFeed::default()
                },
                connection: ConnectionState::Connected,
            },
        )
}

#[test]
fn manager_projects_mixed_targets_with_complete_columns_and_stable_row_ids() {
    let service = session(
        "pf-service",
        service_target(),
        PortForwardSessionState::Active,
        "127.0.0.1:18080",
    );
    let pod = session(
        "pf-pod",
        pod_target(),
        PortForwardSessionState::Starting,
        "",
    );
    let harness = management_harness(vec![service, pod]);

    for column in [
        "Target",
        "Namespace",
        "Remote",
        "Local address",
        "Status",
        "Actions",
    ] {
        harness.get_by_label(column);
    }
    harness.get_by_label("Service web");
    harness.get_by_label("port 80 · backing Pod web-backing-0:8080");
    harness.get_by_label("Pod web-0");
    harness.get_by_label("container web · port 8080");
    harness.get_by_label("Port forward session pf-service");
    harness.get_by_label("Port forward session pf-pod");
}

#[test]
fn manager_maps_actions_strictly_by_state_and_preserves_safe_failures_verbatim() {
    let mut stopping = session(
        "pf-stopping",
        pod_target(),
        PortForwardSessionState::Stopping,
        "127.0.0.1:18081",
    );
    stopping.revision = 2;
    let failed = session(
        "pf-failed",
        pod_target(),
        PortForwardSessionState::Failed,
        "",
    );
    let stopped = session(
        "pf-stopped",
        service_target(),
        PortForwardSessionState::Stopped,
        "127.0.0.1:18082",
    );
    let mut harness = management_harness(vec![stopping, failed, stopped]);
    harness.run_steps(3);

    harness.get_by_label("Port forward session pf-stopping");
    assert!(
        harness
            .get_by_role_and_label(Role::Button, "Stop port forward pf-stopping")
            .accesskit_node()
            .is_disabled()
    );
    harness.get_by_label("Port forward session pf-failed");
    harness.get_by_label("safe failure: upstream pod disappeared");
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Stop port forward pf-failed")
            .is_none()
    );
    let retry = harness.get_by_role_and_label(Role::Button, "Retry port forward pf-failed");
    assert!(!retry.accesskit_node().is_disabled());
    retry.scroll_to_me();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "Retry port forward pf-failed")
        .click();
    harness.step();
    assert_eq!(
        harness.state_mut().shell.drain_port_forward_actions(),
        vec![PortForwardAction::Retry(
            PortForwardSessionId::try_new("pf-failed").unwrap()
        )]
    );

    harness.get_by_label("Port forward session pf-stopped");
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Copy address for pf-stopped")
            .is_none()
    );
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Stop port forward pf-stopped")
            .is_none()
    );
}

#[test]
fn local_address_enables_copy_and_starting_and_active_enable_stop() {
    for state in [
        PortForwardSessionState::Starting,
        PortForwardSessionState::Active,
    ] {
        let id = if state == PortForwardSessionState::Starting {
            "pf-starting"
        } else {
            "pf-active"
        };
        let mut harness =
            management_harness(vec![session(id, pod_target(), state, "127.0.0.1:18080")]);
        harness.run_steps(3);
        let row_label = format!("Port forward session {id}");
        harness.get_by_label(&row_label);
        harness
            .get_by_role_and_label(Role::Button, &format!("Copy address for {id}"))
            .click();
        harness.step();
        assert_eq!(
            harness.state_mut().shell.drain_port_forward_actions(),
            vec![PortForwardAction::CopyAddress("127.0.0.1:18080".into())]
        );
        harness
            .get_by_role_and_label(Role::Button, &format!("Stop port forward {id}"))
            .click();
        harness.step();
        assert_eq!(
            harness.state_mut().shell.drain_port_forward_actions(),
            vec![PortForwardAction::Stop(id.into())]
        );
    }

    let harness = management_harness(vec![session(
        "pf-no-address",
        pod_target(),
        PortForwardSessionState::Starting,
        "",
    )]);
    harness.get_by_label("Port forward session pf-no-address");
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Copy address for pf-no-address")
            .is_none()
    );
}

#[test]
fn manager_empty_and_connection_states_are_honest() {
    let mut harness = management_harness(Vec::new());
    harness.get_by_label("No port forwards yet. Start one from Pod Ports or Service Ports.");

    harness.state_mut().feed.port_forward_available = false;
    harness.state_mut().feed.pod_port_forward_available = false;
    harness.run_steps(2);
    harness.get_by_label("Port forwarding is unavailable on this connection.");

    harness.state_mut().connection = ConnectionState::Failed;
    harness.run_steps(2);
    harness.get_by_label("Disconnected. Existing port-forward sessions are unavailable.");

    harness.state_mut().connection = ConnectionState::Connecting;
    harness.run_steps(2);
    harness.get_by_label("Reconnecting to port-forward sessions…");
}

#[test]
fn connected_pending_lists_render_loading_or_reconstructing_before_empty() {
    for (state, expected) in [
        (
            PortForwardListState::Loading,
            "Loading port-forward sessions…",
        ),
        (
            PortForwardListState::Reconstructing,
            "Reconstructing port-forward sessions…",
        ),
    ] {
        let mut harness = management_harness(Vec::new());
        harness.state_mut().feed.port_forward_list_state = state;
        harness.run_steps(3);

        harness.get_by_label(expected);
        assert!(
            harness
                .query_by_label("No port forwards yet. Start one from Pod Ports or Service Ports.")
                .is_none()
        );
    }
}

#[test]
fn long_failure_and_retry_messages_are_visible_verbatim_in_row_details() {
    const FAILURE: &str =
        "safe failure: the selected backing Pod disappeared while the tunnel was being established";
    const OVERLAY: &str =
        "retry rejected: local address 127.0.0.1:18080 remains occupied; choose another local port";
    let mut failed = session(
        "pf-long-errors",
        pod_target(),
        PortForwardSessionState::Failed,
        "",
    );
    failed.failure.as_mut().unwrap().message = FAILURE.into();
    let mut harness = management_harness(vec![failed.clone()]);
    harness
        .state_mut()
        .feed
        .port_forward_retry_errors
        .insert(failed.id, OVERLAY.into());
    harness.run_steps(3);

    for text in [FAILURE, OVERLAY] {
        let visible = harness.get_by(|node| {
            node.role() == Role::Label && node.value().as_deref() == Some(text) && !node.is_hidden()
        });
        assert!(visible.rect().height() > 0.0);
    }
}

#[test]
fn focused_rows_are_consumed_and_stale_focus_is_cleared_without_panicking() {
    let mut harness = management_harness(vec![session(
        "pf-focus",
        pod_target(),
        PortForwardSessionState::Active,
        "127.0.0.1:18080",
    )]);
    harness
        .state_mut()
        .shell
        .focus_port_forward_session("pf-focus");
    harness.run_steps(4);
    let manager = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::PortForwards)
        .unwrap();
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .port_forward_state(manager.id)
            .unwrap()
            .focused_session,
        None
    );

    harness
        .state_mut()
        .shell
        .focus_port_forward_session("pf-stale");
    harness.run_steps(4);
    let manager = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::PortForwards)
        .unwrap();
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .port_forward_state(manager.id)
            .unwrap()
            .focused_session,
        None
    );
}

#[test]
fn authoritative_omission_removes_a_terminal_row() {
    let mut harness = management_harness(vec![session(
        "pf-terminal",
        service_target(),
        PortForwardSessionState::Stopped,
        "127.0.0.1:18080",
    )]);
    harness.get_by_label("Port forward session pf-terminal");

    harness.state_mut().feed.port_forward_sessions.clear();
    harness.run_steps(3);

    assert!(
        harness
            .query_by_label("Port forward session pf-terminal")
            .is_none()
    );
    harness.get_by_label("No port forwards yet. Start one from Pod Ports or Service Ports.");
}

#[test]
fn manager_sort_has_a_deterministic_session_id_tiebreaker() {
    let mut harness = management_harness(vec![
        session(
            "pf-z",
            pod_target(),
            PortForwardSessionState::Active,
            "127.0.0.1:18081",
        ),
        session(
            "pf-a",
            pod_target(),
            PortForwardSessionState::Active,
            "127.0.0.1:18080",
        ),
    ]);
    let manager = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::PortForwards)
        .unwrap()
        .id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetPortForwardSort(
            manager,
            Some(k10s_ui::workspace::SortSpec {
                column: "namespace".into(),
                ascending: true,
            }),
        ));
    harness.run_steps(3);

    assert!(
        harness
            .get_by_label("Port forward session pf-a")
            .rect()
            .top()
            < harness
                .get_by_label("Port forward session pf-z")
                .rect()
                .top()
    );
}

#[test]
fn retry_error_overlay_is_attached_to_its_authoritative_failed_row() {
    let failed = session(
        "pf-overlay",
        pod_target(),
        PortForwardSessionState::Failed,
        "",
    );
    let mut harness = management_harness(vec![failed.clone()]);
    harness
        .state_mut()
        .feed
        .port_forward_retry_errors
        .insert(failed.id, "retry safely rejected".into());
    harness.run_steps(3);

    harness.get_by_label("Port forward session pf-overlay");
    harness.get_by_label("retry safely rejected");
}

#[test]
fn launcher_gates_the_singleton_and_counts_only_live_sessions() {
    let sessions = vec![
        session(
            "starting",
            pod_target(),
            PortForwardSessionState::Starting,
            "",
        ),
        session(
            "active",
            pod_target(),
            PortForwardSessionState::Active,
            "127.0.0.1:18080",
        ),
        session(
            "stopping",
            service_target(),
            PortForwardSessionState::Stopping,
            "127.0.0.1:18081",
        ),
        session("failed", pod_target(), PortForwardSessionState::Failed, ""),
        session(
            "stopped",
            service_target(),
            PortForwardSessionState::Stopped,
            "127.0.0.1:18082",
        ),
    ];
    let mut harness = management_harness(sessions);
    harness.get_by_label("3 live Port Forwards");

    harness.state_mut().feed.port_forward_available = false;
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Button, "Port Forwards");
    harness.state_mut().feed.port_forward_available = true;
    harness.state_mut().feed.pod_port_forward_available = false;
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Button, "Port Forwards");

    let filter = harness.get_by_role_and_label(Role::TextInput, "Filter resources…");
    filter.click();
    filter.type_text("forward");
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Button, "Port Forwards");

    harness
        .get_by_role_and_label(Role::Button, "Port Forwards")
        .click();
    harness.run_steps(3);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == WindowKind::PortForwards)
            .count(),
        1
    );

    harness.state_mut().feed.port_forward_available = false;
    harness.run_steps(3);
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Port Forwards")
            .is_none()
    );
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
