//! Kind-specific detail views: exact tabs and actions per kind, the
//! identity header, backend-resolved related rows, single-click integrated
//! detail, double-click/context-menu popouts pinned to stable identity,
//! and independent tab state across views.

use egui::accesskit::Role;
use egui_kittest::{Harness, kittest::Queryable as _};
use k10s_protocol::{
    BackendRevision, DetailRow, DetailSection, EventRow, GroupVersionKind, RelatedGroup,
    ResourceCapabilities, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceRelationsResponse,
};
use k10s_ui::{
    ui::{
        ConnectionState, PrimaryDetailState, RelationState, ResourceAction, ResourceFeed,
        SafeUiError, UiShell,
    },
    workspace::{
        DetailTab as WorkspaceDetailTab, LauncherItem, WindowContent, WindowKind, WorkloadKind,
        WorkspaceCommand, WorkspaceState,
    },
};

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
}

#[test]
fn primary_failure_and_relation_states_render_independently() {
    let mut harness = harness();
    let deployment = identity("Deployment", "web-frontend");
    harness.state_mut().feed.primary_details.insert(
        deployment.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("details are temporarily unavailable")),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("Details unavailable: details are temporarily unavailable");
    window
        .get_by_role_and_label(Role::Button, "Retry details")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::RetryPrimary(deployment.clone())]
    );
    harness.run_steps(2);
    assert!(
        harness
            .state_mut()
            .shell
            .drain_resource_actions()
            .is_empty()
    );

    harness.state_mut().feed.primary_details.insert(
        deployment.clone(),
        PrimaryDetailState::Loaded(deployment_detail("web-frontend")),
    );
    harness.state_mut().feed.relations.insert(
        deployment.clone(),
        RelationState::Failed(SafeUiError::new("relations are temporarily unavailable")),
    );
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(3);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("Related resources unavailable: relations are temporarily unavailable");
    window
        .get_by_role_and_label(Role::Button, "Retry related resources")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::RetryRelations(deployment)]
    );
}

#[test]
fn unavailable_events_are_explicitly_safe() {
    let mut harness = harness();
    let mut detail = pod_detail("db-postgres-0");
    detail.events.clear();
    detail.events_condition = k10s_protocol::EventsCondition::Unavailable;
    harness
        .state_mut()
        .feed
        .details
        .insert(detail.identity.clone(), detail);
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_label("Events unavailable");
}

#[test]
fn shared_frame_keeps_pinned_identity_actions_while_details_load() {
    let mut harness = harness();
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(3);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Pod · default / db-postgres-0");
    window.get_by_role_and_label(Role::Button, "Copy name");
    window.get_by_role_and_label(Role::Button, "Copy namespace");
    window.get_by_role_and_label(Role::Button, "Copy UID");
    window.get_by_role_and_label(Role::Button, "Pop out ↗");
    window.get_by_role_and_label(Role::Button, "Maximize");
    window.get_by_label("Loading details");
}

#[test]
fn crashloop_logs_default_to_previous_with_complete_toolbar() {
    let mut harness = harness();
    let pod = identity("Pod", "web-frontend-7d9f8-00001");
    let mut detail = pod_detail("web-frontend-7d9f8-00001");
    detail.sections[0]
        .rows
        .iter_mut()
        .find(|row| row.label == "Status")
        .unwrap()
        .value = "CrashLoopBackOff".into();
    harness.state_mut().feed.details.insert(pod, detail);
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "web-frontend-7d9f8-00001")
        .click();
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    for label in ["Previous", "Wrap"] {
        window.get_by_role_and_label(Role::CheckBox, label);
    }
    for label in ["Connect logs", "Export"] {
        window.get_by_role_and_label(Role::Button, label);
    }
    window.get_by_role_and_label(Role::TextInput, "Find in logs");
    window.get_by_label(
        "CrashLoopBackOff: showing logs from the previous terminated container by default",
    );
}

impl Default for Fixture {
    fn default() -> Self {
        let mut fixture = Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
        };
        fixture.feed.lists.insert(
            k10s_ui::workspace::WorkloadKind::Deployments,
            vec![
                list_row("apps", "v1", "Deployment", "web-frontend", "20/20 ready"),
                list_row("apps", "v1", "Deployment", "api-server", "2/2 ready"),
            ],
        );
        fixture.feed.lists.insert(
            k10s_ui::workspace::WorkloadKind::Pods,
            vec![
                list_row("", "v1", "Pod", "web-frontend-7d9f8-00001", "Running"),
                list_row("", "v1", "Pod", "db-postgres-0", "Running"),
            ],
        );
        fixture
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let mut selected_context = Some(CONTEXT.to_owned());
    let contexts = [CONTEXT.to_owned()];
    fixture.shell.show_with_resources(
        ui,
        ConnectionState::Connected,
        &contexts,
        &mut selected_context,
        None,
        &fixture.feed,
    );
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
}

fn identity(kind: &str, name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: CONTEXT.to_owned(),
        gvk: if kind == "Pod" {
            GroupVersionKind::core("v1", "Pod")
        } else {
            GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: kind.into(),
            }
        },
        namespace: Some("default".into()),
        name: name.to_owned(),
        uid: format!("uid-{CONTEXT}-{}-default-{name}", kind.to_lowercase()),
    }
}

fn list_row(group: &str, version: &str, kind: &str, name: &str, summary: &str) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: group.to_owned(),
                version: version.to_owned(),
                kind: kind.to_owned(),
            },
            namespace: Some("default".into()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-{}-default-{name}", kind.to_lowercase()),
        },
        revision: BackendRevision::new(1_000),
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: "2026-08-21T00:00:00Z".to_owned(),
        projection: None,
    }
}

fn overview_section(rows: &[(&str, &str)]) -> DetailSection {
    DetailSection {
        title: "Overview".into(),
        rows: rows
            .iter()
            .map(|(label, value)| DetailRow {
                label: (*label).to_owned(),
                value: (*value).to_owned(),
            })
            .collect(),
    }
}

/// A deployment-shaped response with traversal-resolved related rows.
fn deployment_detail(name: &str) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: identity("Deployment", name),
        revision: BackendRevision::new(1_010),
        created_at: "2026-08-21T00:05:00Z".to_owned(),
        owner_references: Vec::new(),
        sections: vec![overview_section(&[
            ("Kind", "Deployment"),
            ("Name", name),
            ("Status", "20/20 ready"),
        ])],
        events_condition: k10s_protocol::EventsCondition::Available,
        events: vec![EventRow {
            reason: "Started".into(),
            message: format!("{name} reached 20/20 ready"),
            count: 1,
            last_seen: "2026-08-21T00:06:45Z".to_owned(),
        }],
        related: vec![
            RelatedGroup {
                title: "ReplicaSets".into(),
                gvk: GroupVersionKind {
                    group: "apps".into(),
                    version: "v1".into(),
                    kind: "ReplicaSet".into(),
                },
                rows: vec![list_row(
                    "apps",
                    "v1",
                    "ReplicaSet",
                    format!("{name}-7d9f8").as_str(),
                    "20 desired",
                )],
            },
            RelatedGroup {
                title: "Pods".into(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                rows: vec![list_row(
                    "",
                    "v1",
                    "Pod",
                    &format!("{name}-7d9f8-00001"),
                    "Running",
                )],
            },
        ],
        capabilities: ResourceCapabilities {
            can_scale: true,
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {name}\n"),
        projection: None,
    }
}

fn pod_detail(name: &str) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: identity("Pod", name),
        revision: BackendRevision::new(1_011),
        created_at: "2026-08-21T00:50:10Z".to_owned(),
        owner_references: Vec::new(),
        sections: vec![overview_section(&[("Status", "Running")])],
        events_condition: k10s_protocol::EventsCondition::Available,
        events: vec![EventRow {
            reason: "Started".into(),
            message: "container started".into(),
            count: 1,
            last_seen: "2026-08-21T00:50:55Z".to_owned(),
        }],
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_view_logs: true,
            can_exec: true,
            can_edit_yaml: true,
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n"),
        projection: None,
    }
}

fn workload_window_id(
    workspace: &WorkspaceState<ResourceIdentity>,
    kind: WorkloadKind,
) -> k10s_ui::workspace::WindowId {
    workspace
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Workload(kind))
        .expect("workload window is open")
        .id
}

fn detail_window(
    workspace: &WorkspaceState<ResourceIdentity>,
) -> &k10s_ui::workspace::Window<ResourceIdentity> {
    match workspace
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
    {
        Some(window) => window,
        None => panic!("a dedicated detail window is open"),
    }
}

fn pinned_identity(workspace: &WorkspaceState<ResourceIdentity>) -> &ResourceIdentity {
    match &detail_window(workspace).content {
        WindowContent::Detail(detail) => &detail.identity,
        WindowContent::Resource(_) | WindowContent::Services(_) => {
            panic!("detail windows pin a single identity")
        }
    }
}

fn integrated_tab(workspace: &WorkspaceState<ResourceIdentity>) -> WorkspaceDetailTab {
    let id = workload_window_id(workspace, WorkloadKind::Deployments);
    workspace
        .resource_state(id)
        .and_then(|resource| resource.detail.as_ref())
        .map(|detail| detail.active_tab)
        .expect("integrated detail exists")
}

fn open(harness: &mut Harness<'static, Fixture>, item: LauncherItem) {
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(item));
    harness.run_steps(4);
}

#[test]
fn tabs_and_actions_are_exact_per_kind() {
    let mut harness = harness();
    // Deployment: controller tabs plus Scale.
    harness.state_mut().feed.details.insert(
        identity("Deployment", "web-frontend"),
        deployment_detail("web-frontend"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    for tab in ["Tab Overview", "Tab Pods", "Tab Events", "Tab YAML"] {
        window.get_by_role_and_label(Role::Button, tab);
    }
    for absent in ["Tab Logs", "Tab Shell"] {
        assert!(
            window
                .query_by_role_and_label(Role::Button, absent)
                .is_none(),
            "{absent} must not be offered on a deployment"
        );
    }
    window.get_by_role_and_label(Role::Button, "Scale workload");
    assert!(window.query_by_label("Exec shell").is_none());

    // Pod: runtime tabs plus logs/exec actions, never Scale.
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    for tab in [
        "Tab Overview",
        "Tab Events",
        "Tab YAML",
        "Tab Logs",
        "Tab Shell",
    ] {
        window.get_by_role_and_label(Role::Button, tab);
    }
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Tab Pods")
            .is_none(),
        "pods own nothing, so no related-workloads tab"
    );
    assert!(window.query_by_label("Scale workload").is_none());
}

#[test]
fn workload_detail_is_a_selection_driven_bottom_panel() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(window.query_by_label("Details").is_none());
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Show details")
            .is_none()
    );
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Hide details")
            .is_none()
    );

    window
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Details");
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);

    assert!(
        harness
            .get_by_role_and_label(Role::Window, "Pods")
            .query_by_label("Details")
            .is_none()
    );
}

#[test]
fn pod_edit_yaml_action_opens_the_read_only_manifest_before_editing() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(4);

    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Edit YAML")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Read-only");
    window.get_by_label("apiVersion: v1\nkind: Pod\nmetadata:\n  name: db-postgres-0\n");
    let pods_id = workload_window_id(
        harness.state().shell.workspace(),
        k10s_ui::workspace::WorkloadKind::Pods,
    );
    let detail = harness
        .state()
        .shell
        .workspace()
        .resource_state(pods_id)
        .and_then(|resource| resource.detail.as_ref())
        .expect("pod detail is selected");
    assert_eq!(detail.active_tab, WorkspaceDetailTab::Yaml);
    assert!(!detail.yaml.dirty, "opening YAML must remain read-only");
}

#[test]
fn identity_header_renders_from_the_pinned_identity() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );

    // Before the backend response arrives, the header still renders from
    // the selected identity with an explicit loading state.
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Details");
    window.get_by_label("Kind Pod");
    window.get_by_label("Namespace default");
    window.get_by_label("Loading details");

    // Once resolved, the header shows the backend-asserted fields and the
    // Overview section renders its rows.
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Created 2026-08-21T00:50:10Z");
    window.get_by_label("UID uid-dev-local-pod-default-db-postgres-0");
    window.get_by_label("Status Running");
    assert!(window.query_by_label("Loading details").is_none());

    // Backend-resolved events render on their own tab.
    window
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_label("Started container started");
}

#[test]
fn deployment_related_tab_renders_resolved_traversal_rows() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Deployment", "web-frontend"),
        deployment_detail("web-frontend"),
    );
    let detail = deployment_detail("web-frontend");
    harness.state_mut().feed.relations.insert(
        identity("Deployment", "web-frontend"),
        RelationState::Loaded {
            response: std::sync::Arc::new(ResourceRelationsResponse {
                identity: identity("Deployment", "web-frontend"),
                revision: BackendRevision::new(1_010),
                groups: detail.related,
            }),
            loaded_at_ms: 0,
            refreshing: false,
            refresh_error: None,
        },
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    let deployments_id = workload_window_id(
        harness.state().shell.workspace(),
        k10s_ui::workspace::WorkloadKind::Deployments,
    );
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(4);

    // Give the detail pane nearly the whole window so both resolved groups
    // are on screen at once.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(deployments_id, 0.05));
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("ReplicaSets");
    window.get_by_label("web-frontend-7d9f8 · 20 desired");
    window.get_by_label("web-frontend-7d9f8-00001 · Running");

    // Clicking a related row pops a dedicated window out for that row.
    window
        .get_by_role_and_label(Role::Button, "web-frontend-7d9f8-00001 · Running")
        .click();
    harness.run_steps(4);
    let workspace = harness.state().shell.workspace();
    let pinned = workspace
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Detail)
        .map(|window| match &window.content {
            WindowContent::Detail(detail) => detail.identity.clone(),
            WindowContent::Resource(_) | WindowContent::Services(_) => {
                panic!("detail windows pin a single identity")
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pinned,
        vec![identity("Pod", "web-frontend-7d9f8-00001")],
        "a related row click opens exactly one dedicated window for it"
    );
}

#[test]
fn failed_relation_refresh_keeps_stale_rows_and_retries_once() {
    let mut harness = harness();
    let deployment = identity("Deployment", "web-frontend");
    let detail = deployment_detail("web-frontend");
    harness
        .state_mut()
        .feed
        .details
        .insert(deployment.clone(), detail.clone());
    harness.state_mut().feed.relations.insert(
        deployment.clone(),
        RelationState::Loaded {
            response: std::sync::Arc::new(ResourceRelationsResponse {
                identity: deployment.clone(),
                revision: BackendRevision::new(1_010),
                groups: detail.related,
            }),
            loaded_at_ms: 0,
            refreshing: false,
            refresh_error: Some(SafeUiError::new("refresh denied")),
        },
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(3);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("web-frontend-7d9f8-00001 · Running");
    window.get_by_label("Refresh failed: refresh denied");
    window
        .get_by_role_and_label(Role::Button, "Retry related resources")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::RetryRelations(deployment)]
    );
    harness.run_steps(2);
    assert!(
        harness
            .state_mut()
            .shell
            .drain_resource_actions()
            .is_empty()
    );
}

#[test]
fn popout_is_pinned_and_never_follows_later_selection() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Deployment", "web-frontend"),
        deployment_detail("web-frontend"),
    );
    harness.state_mut().feed.details.insert(
        identity("Deployment", "api-server"),
        deployment_detail("api-server"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );

    // The integrated pane first shows api-server.
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "api-server")
        .click();
    harness.run_steps(4);

    // A popout pins web-frontend into a dedicated window. The popout clones
    // the stable identity at open time (row double-click and the row context
    // menu queue this same command).
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity(
            "Deployment",
            "web-frontend",
        )));
    harness.run_steps(4);
    let workspace = harness.state().shell.workspace();
    assert_eq!(
        workspace
            .windows()
            .iter()
            .filter(|window| window.kind == WindowKind::Detail)
            .count(),
        1
    );
    assert_eq!(
        pinned_identity(workspace),
        &identity("Deployment", "web-frontend"),
        "the dedicated window pins the identity it was opened with"
    );

    // The pinned window renders its own identity, not the integrated one.
    let dedicated = harness.get_by_role_and_label(Role::Window, "Detail");
    dedicated.get_by_label("UID uid-dev-local-deployment-default-web-frontend");
    assert!(
        dedicated
            .query_by_role_and_label(Role::Button, "Pop out ↗")
            .is_none(),
        "a dedicated detail must not offer another pop-out"
    );
    assert!(
        dedicated
            .query_by_role_and_label(Role::Button, "Maximize")
            .is_none(),
        "pane-only maximize is hidden in a dedicated window"
    );
    assert!(
        dedicated
            .query_by_label("UID uid-dev-local-deployment-default-api-server")
            .is_none()
    );

    // Moving the integrated selection later never touches the pin.
    let deployments_id =
        workload_window_id(harness.state().shell.workspace(), WorkloadKind::Deployments);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(
            deployments_id,
            identity("Deployment", "api-server"),
        ));
    harness.run_steps(4);
    assert_eq!(
        pinned_identity(harness.state().shell.workspace()),
        &identity("Deployment", "web-frontend"),
        "the dedicated window keeps the identity it was opened with"
    );
}

#[test]
fn tabs_stay_independent_between_integrated_and_pinned_views() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Deployment", "web-frontend"),
        deployment_detail("web-frontend"),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );

    // Integrated pane switches to Events.
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(4);

    // A dedicated popout starts on Overview regardless of the integrated
    // pane's active tab.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity(
            "Deployment",
            "web-frontend",
        )));
    harness.run_steps(4);

    let workspace = harness.state().shell.workspace();
    match &detail_window(workspace).content {
        WindowContent::Detail(detail) => {
            assert_eq!(
                detail.active_tab,
                WorkspaceDetailTab::Overview,
                "the pinned view must not inherit the integrated tab"
            );
        }
        WindowContent::Resource(_) | WindowContent::Services(_) => {
            panic!("detail windows pin a single identity")
        }
    }

    assert_eq!(
        integrated_tab(workspace),
        WorkspaceDetailTab::Events,
        "the integrated tab stays where the user left it"
    );
}
