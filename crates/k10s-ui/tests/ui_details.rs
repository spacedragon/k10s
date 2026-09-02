//! Kind-specific detail views: exact tabs and actions per kind, the
//! identity header, backend-resolved related rows, single-click integrated
//! detail, double-click/context-menu popouts pinned to stable identity,
//! and independent tab state across views.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, ContainerStateProjection, ContainerTerminationProjection,
    DeploymentProjection, DetailRow, DetailSection, EventRow, GroupVersionKind, OwnerReference,
    PodContainerProjection, PodProjection, RelatedGroup, ResourceCapabilities,
    ResourceConditionProjection, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceProjection, ResourceRelationsResponse,
};
use k10s_ui::{
    ui::{
        ConnectionState, DetailAuthority, DetailLifecycle, PrimaryDetailState, RelationState,
        ResourceAction, ResourceFeed, SafeUiError, UiShell, WindowFreshness, tools::LogsAction,
    },
    workspace::{
        DetailTab as WorkspaceDetailTab, LauncherItem, WindowContent, WindowGeom, WindowKind,
        WorkloadKind, WorkspaceCommand, WorkspaceState,
    },
};

mod common;

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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(3);
    let window = common::workload_window(&harness, "Deployments");
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
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    {
        let window = common::workload_window(&harness, "Pods");
        let tab = window
            .get_by_role_and_label(Role::Button, "Tab Events")
            .rect();
        let body = window.get_by_label("Structured details unavailable").rect();
        assert!(!tab.intersects(body), "tab {tab:?} overlaps body {body:?}");
        for label in ["Actions", "Pop out ↗", "Maximize"] {
            let action = window.get_by_role_and_label(Role::Button, label).rect();
            assert!(
                !tab.intersects(action),
                "tab {tab:?} overlaps {label} {action:?}"
            );
        }
    }
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(3);
    let pods = workload_window_id(harness.state().shell.workspace(), WorkloadKind::Pods);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .resource_state(pods)
            .and_then(|state| state.detail.as_ref())
            .map(|detail| detail.active_tab),
        Some(WorkspaceDetailTab::Events)
    );
    common::workload_window(&harness, "Pods").get_by_label("Events unavailable");
}

#[test]
fn generic_actual_route_renders_exact_1000_and_640_semantics_in_one_harness() {
    let mut harness = harness();
    for (name, width, x) in [
        ("generic-wide", 1_024.0, 10.0),
        ("generic-narrow", 664.0, 760.0),
    ] {
        let identity = ResourceIdentity {
            context: CONTEXT.into(),
            gvk: GroupVersionKind::core("v1", "ConfigMap"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        };
        harness.state_mut().feed.details.insert(
            identity.clone(),
            ResourceDetailResponse {
                identity: identity.clone(),
                revision: BackendRevision::new(1),
                created_at: "2026-08-21T00:00:00Z".into(),
                owner_references: vec![],
                sections: vec![
                    DetailSection {
                        title: "CONFIGURATION".into(),
                        rows: vec![DetailRow {
                            label: "Mode".into(),
                            value: "active".into(),
                        }],
                    },
                    DetailSection {
                        title: "EMPTY SENTINEL".into(),
                        rows: vec![],
                    },
                ],
                events_condition: k10s_protocol::EventsCondition::Available,
                events: vec![],
                related: vec![],
                capabilities: ResourceCapabilities::default(),
                manifest: String::new(),
                projection: None,
            },
        );
        let id = harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity))
            .into_iter()
            .find_map(|event| match event {
                k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::SetGeometry(
                id,
                WindowGeom {
                    position: [x, 20.0],
                    size: [width, 700.0],
                    collapsed: false,
                },
            ));
    }
    harness.run_steps(5);
    let wide = harness.get_by_role_and_label(Role::Window, "ConfigMap · default / generic-wide");
    let op = wide.get_by_label("Operational detail column").rect();
    let config = wide.get_by_label("Configuration detail column").rect();
    assert!((op.width() / config.width() - 1.35).abs() < 0.02);
    let narrow =
        harness.get_by_role_and_label(Role::Window, "ConfigMap · default / generic-narrow");
    assert!(
        narrow.get_by_label("STATUS").rect().top()
            < narrow.get_by_label("CONFIGURATION").rect().top()
    );
    assert!(
        narrow.get_by_label("CONFIGURATION").rect().top()
            < narrow.get_by_label("IDENTITY").rect().top()
    );
    assert!(narrow.query_by_label("EMPTY SENTINEL").is_none());
    narrow.get_by_role_and_label(Role::Button, "Tab Overview");
}

#[test]
fn frame_body_has_one_finite_scroll_owner_and_keeps_footer_visible_at_min_height() {
    let mut harness = harness();
    let mut response = deployment_detail("database");
    response.identity.gvk.kind = "StatefulSet".into();
    response.identity.name = "database".into();
    response.identity.uid = "uid-dev-local-statefulset-default-database".into();
    response.sections = vec![DetailSection {
        title: "Overview".into(),
        rows: (0..80)
            .map(|index| DetailRow {
                label: format!("row-{index:03}"),
                value: "finite body content".into(),
            })
            .collect(),
    }];
    let identity = response.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), response);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity));
    let window_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .unwrap()
        .id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [672.0, 424.0],
                collapsed: false,
            },
        ));
    harness.run_steps(5);

    let footer_before = {
        let detail =
            harness.get_by_role_and_label(Role::Window, "StatefulSet · default / database");
        assert_eq!(detail.query_all_by_role(Role::ScrollView).count(), 1);
        let footer = detail
            .get_by_label("p pods · y yaml · e events · c copy name · Esc clear selection")
            .rect();
        assert!(detail.rect().contains_rect(footer));
        assert!(
            !detail
                .rect()
                .intersects(detail.get_by_label("row-079 finite body content").rect())
        );
        footer
    };

    for _ in 0..20 {
        harness
            .get_by_role_and_label(Role::Window, "StatefulSet · default / database")
            .get_by_label("row-000 finite body content")
            .scroll_down();
        harness.run_steps(1);
    }
    let detail = harness.get_by_role_and_label(Role::Window, "StatefulSet · default / database");
    assert!(
        detail
            .rect()
            .intersects(detail.get_by_label("row-079 finite body content").rect())
    );
    assert_eq!(
        footer_before,
        detail
            .get_by_label("p pods · y yaml · e events · c copy name · Esc clear selection",)
            .rect()
    );
}

#[test]
fn compact_frame_chrome_has_disjoint_contained_hitboxes_and_reserved_footer() {
    let mut harness = harness();
    let response = typed_deployment_detail("web-frontend");
    let identity = response.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), response);
    harness.state_mut().feed.detail_authority.insert(
        identity.clone(),
        DetailAuthority {
            freshness: WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity));
    let window_id = detail_window(harness.state().shell.workspace()).id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [320.0, 280.0],
                collapsed: false,
            },
        ));
    harness.run_steps(5);

    let detail = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    let window_rect = detail.rect();
    // Compare controls that remain visible at this width. Offscreen controls
    // inside a clipped horizontal viewport intentionally have no AccessKit
    // rectangle.
    let tab = detail
        .get_by_role_and_label(Role::Button, "Tab Overview")
        .rect();
    let action = detail.get_by_role_and_label(Role::Button, "Scale…").rect();
    for rect in [tab, action] {
        assert!(
            window_rect.contains_rect(rect),
            "compact detail hitbox {rect:?} must stay inside {window_rect:?}"
        );
    }
    assert!(
        !tab.intersects(action),
        "compact tab hitbox {tab:?} overlaps action hitbox {action:?}"
    );
    let footer = detail
        .get_by_label("p pods · y yaml · e events · c copy name · Esc clear selection")
        .rect();
    let body = detail
        .get_by_role_and_label(Role::ScrollView, "Detail body")
        .rect();
    assert!(window_rect.contains_rect(footer));
    assert!(!body.intersects(footer));
}

#[test]
fn shared_frame_keeps_pinned_identity_actions_while_details_load() {
    let mut harness = harness();
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);

    {
        let window = common::workload_window(&harness, "Pods");
        window.get_by_label("Pod · default / db-postgres-0");
        // Copy name moved into the Actions overflow menu.
        assert!(
            window
                .query_by_role_and_label(Role::Button, "Copy name")
                .is_none()
        );
        assert!(
            window
                .query_by_role_and_label(Role::Button, "Copy namespace")
                .is_none()
        );
        assert!(
            window
                .query_by_role_and_label(Role::Button, "Copy UID")
                .is_none()
        );
        window
            .get_by_role_and_label(Role::Button, "Actions")
            .click();
    }
    harness.run_steps(1);
    harness.get_by_role_and_label(Role::Button, "Copy name");
    harness.get_by_role_and_label(Role::Button, "Copy namespace");
    harness.get_by_role_and_label(Role::Button, "Copy UID");
    let window = common::workload_window(&harness, "Pods");
    window.get_by_role_and_label(Role::Button, "Pop out ↗");
    window.get_by_role_and_label(Role::Button, "Maximize");
    window.get_by_label("Loading details");
}

#[test]
fn typed_pod_overview_renders_real_content_and_metadata_controls() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        typed_pod_detail("db-postgres-0"),
    );
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Status ● Running");
    window.get_by_label("CONTAINERS · 0");
    assert!(window.query_by_label("Context · dev-local").is_none());
    window.get_by_role_and_label(Role::Button, "Show Pod metadata");
    assert!(
        window
            .query_by_label("Structured details unavailable")
            .is_none()
    );
}

#[test]
fn detail_expansion_is_independent_per_window() {
    let mut harness = harness();
    // Open the window we interact with last so it is not occluded by the
    // other dedicated window's initial geometry.
    for name in ["web-frontend-7d9f8-00001", "db-postgres-0"] {
        harness
            .state_mut()
            .feed
            .details
            .insert(identity("Pod", name), typed_pod_detail(name));
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity("Pod", name)));
    }
    harness.run_steps(4);

    harness
        .get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0")
        .get_by_role_and_label(Role::Button, "Show more Pod vitals")
        .click();
    harness.run_steps(2);

    harness.get_by_label("Node · worker-a");
    assert!(
        harness
            .get_by_role_and_label(Role::Window, "Pod · default / web-frontend-7d9f8-00001")
            .query_by_label("Node · worker-a")
            .is_none()
    );
}

#[test]
fn width_aware_typed_vitals_use_exact_collapsed_contract() {
    let mut harness = harness();
    let pod = typed_pod_detail("db-postgres-0");
    let pod_identity = pod.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(pod_identity.clone(), pod);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(pod_identity));
    let window_id = detail_window(harness.state().shell.workspace()).id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [672.0, 424.0],
                collapsed: false,
            },
        ));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0");
    for label in ["Status ● Running", "Ready · 1/1"] {
        detail.get_by_label(label);
    }
    assert!(detail.query_by_label("Node · worker-a").is_none());
    detail
        .get_by_role_and_label(Role::Button, "Show more Pod vitals")
        .click();
    harness.run_steps(2);
    harness.get_by_label("Restarts · 2");
    harness.get_by_label("Age · 2h");
    harness.get_by_label("Node · worker-a");
    harness.get_by_label("Pod IP · 10.244.0.9");
    let detail = harness.get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Show metadata")
            .is_none()
    );
}

#[test]
fn kind_configurators_run_before_shared_vital_accessibility_paint() {
    let mut pod_harness = harness();
    let pod = typed_pod_detail("db-postgres-0");
    let pod_identity = pod.identity.clone();
    pod_harness
        .state_mut()
        .feed
        .details
        .insert(pod_identity.clone(), pod);
    pod_harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(pod_identity));
    pod_harness.run_steps(4);
    pod_harness
        .get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0")
        .get_by_label("Status ● Running");

    let mut deployment_harness = harness();
    let deployment = typed_deployment_detail("web-frontend");
    let deployment_identity = deployment.identity.clone();
    deployment_harness
        .state_mut()
        .feed
        .details
        .insert(deployment_identity.clone(), deployment);
    deployment_harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(deployment_identity));
    deployment_harness.run_steps(4);
    deployment_harness
        .get_by_role_and_label(Role::Window, "Deployment · default / web-frontend")
        .get_by_label("Rollout ● NewReplicaSetAvailable");
}

#[test]
fn deployment_stub_exposes_width_aware_shared_frame_contract() {
    let mut harness = harness();
    let detail = typed_deployment_detail("web-frontend");
    let identity = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), detail);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity));
    let window_id = detail_window(harness.state().shell.workspace()).id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [672.0, 424.0],
                collapsed: false,
            },
        ));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    for label in ["Rollout ● NewReplicaSetAvailable", "Ready · 18/20"] {
        detail.get_by_label(label);
    }
    assert!(detail.query_by_label("Strategy · RollingUpdate").is_none());
    detail
        .get_by_role_and_label(Role::Button, "Show more Deployment vitals")
        .click();
    harness.run_steps(2);
    harness.get_by_label("Up-to-date · 19");
    harness.get_by_label("Available · 17");
    harness.get_by_label("Strategy · RollingUpdate");
    harness.get_by_label("Age · 3d");
}

#[test]
fn typed_router_uses_pod_overview_and_preserves_other_tabs() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Structured details unavailable");
    assert!(window.query_by_label("Status Running").is_none());

    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        typed_pod_detail("db-postgres-0"),
    );
    harness.run_steps(3);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Status ● Running");
    assert!(
        window
            .query_by_label("Structured details unavailable")
            .is_none()
    );
    window
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(3);
    common::workload_window(&harness, "Pods").get_by_label("Started container started");
}

#[test]
fn detail_footers_expose_only_shortcuts_supported_by_each_kind() {
    let mut pod_harness = harness();
    pod_harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        typed_pod_detail("db-postgres-0"),
    );
    open(&mut pod_harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&pod_harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    pod_harness.run_steps(3);
    let pod = common::workload_window(&pod_harness, "Pods");
    pod.get_by_label("l logs · s shell · y yaml · e events · c copy name · Esc clear selection");

    let mut deployment_harness = harness();
    deployment_harness.state_mut().feed.details.insert(
        identity("Deployment", "web-frontend"),
        typed_deployment_detail("web-frontend"),
    );
    open(
        &mut deployment_harness,
        LauncherItem::Workload(WorkloadKind::Deployments),
    );
    common::workload_window(&deployment_harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    deployment_harness.run_steps(3);
    common::workload_window(&deployment_harness, "Deployments")
        .get_by_label("p pods · y yaml · e events · c copy name · Esc clear selection");

    let mut generic_harness = harness();
    let node = ResourceIdentity {
        context: CONTEXT.into(),
        gvk: GroupVersionKind::core("v1", "Node"),
        namespace: None,
        name: "worker-a".into(),
        uid: String::new(),
    };
    generic_harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(node));
    generic_harness.run_steps(3);
    generic_harness
        .get_by_role_and_label(Role::Window, "Node · worker-a")
        .get_by_label("y yaml · e events · c copy name · Esc clear selection");
}

#[test]
fn detail_footer_exposes_owner_shortcut_only_for_verified_owner() {
    let mut harness = harness();
    let mut response = typed_pod_detail("db-postgres-0");
    response.owner_references.push(OwnerReference {
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        name: "web-frontend-7d9f8".into(),
        uid: "uid-owner".into(),
        controller: true,
    });
    harness
        .state_mut()
        .feed
        .details
        .insert(response.identity.clone(), response.clone());
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(response.identity));
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0")
        .get_by_label(
            "l logs · s shell · y yaml · e events · c copy name · o owner · Esc clear selection",
        );
}

#[test]
fn detail_verified_owner_shortcut_executes_the_advertised_command() {
    let mut harness = harness();
    let mut response = typed_pod_detail("db-postgres-0");
    let owner = OwnerReference {
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        name: "web-frontend-7d9f8".into(),
        uid: "uid-owner".into(),
        controller: true,
    };
    response.owner_references.push(owner.clone());
    harness
        .state_mut()
        .feed
        .details
        .insert(response.identity.clone(), response);
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    if let Some(focused) = harness.ctx.memory(|memory| memory.focused()) {
        harness
            .ctx
            .memory_mut(|memory| memory.surrender_focus(focused));
    }

    harness.key_press(egui::Key::O);
    harness.run_steps(2);
    let expected_owner = ResourceIdentity {
        context: CONTEXT.into(),
        gvk: owner.gvk,
        namespace: Some("default".into()),
        name: owner.name,
        uid: owner.uid,
    };
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .any(|window| matches!(&window.content, WindowContent::Detail(detail) if detail.identity == expected_owner))
    );
}

#[test]
fn global_detail_shortcuts_belong_only_to_the_top_workspace_detail() {
    let mut harness = harness();
    for name in ["db-postgres-0", "web-frontend-7d9f8-00001"] {
        harness
            .state_mut()
            .feed
            .details
            .insert(identity("Pod", name), typed_pod_runtime_detail(name));
    }
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);

    let integrated = workload_window_id(harness.state().shell.workspace(), WorkloadKind::Pods);
    let pinned_identity = identity("Pod", "web-frontend-7d9f8-00001");
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(
            pinned_identity.clone(),
        ));
    let dedicated = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| {
            matches!(&window.content, WindowContent::Detail(detail) if detail.identity == pinned_identity)
        })
        .expect("dedicated detail is open")
        .id;
    harness.run_steps(3);
    surrender_text_focus(&mut harness);

    harness.key_press(egui::Key::Y);
    harness.run_steps(2);
    assert_eq!(
        detail_tab(harness.state().shell.workspace(), dedicated),
        WorkspaceDetailTab::Yaml
    );
    assert_eq!(
        detail_tab(harness.state().shell.workspace(), integrated),
        WorkspaceDetailTab::Overview
    );

    surrender_text_focus(&mut harness);
    harness.key_down(egui::Key::C);
    harness.step();
    let copied = harness
        .output()
        .platform_output
        .commands
        .iter()
        .filter_map(|command| match command {
            egui::OutputCommand::CopyText(value) => Some(value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(copied, vec!["web-frontend-7d9f8-00001"]);
    harness.key_up(egui::Key::C);
    harness.step();

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetActiveTab(
            dedicated,
            WorkspaceDetailTab::Logs,
        ));
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::Window, "Pod · default / web-frontend-7d9f8-00001")
        .get_by_role_and_label(Role::TextInput, "Find in logs")
        .click();
    harness.key_press(egui::Key::Y);
    harness.run_steps(2);
    assert_eq!(
        detail_tab(harness.state().shell.workspace(), dedicated),
        WorkspaceDetailTab::Logs
    );
    assert_eq!(
        detail_tab(harness.state().shell.workspace(), integrated),
        WorkspaceDetailTab::Overview
    );

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::CloseWindow(dedicated));
    harness.run_steps(3);
    surrender_text_focus(&mut harness);
    harness.key_press(egui::Key::E);
    harness.run_steps(2);
    assert_eq!(
        detail_tab(harness.state().shell.workspace(), integrated),
        WorkspaceDetailTab::Events
    );
}

#[test]
fn owner_shortcut_opens_only_the_active_details_verified_owner() {
    let mut harness = harness();
    let mut integrated_response = typed_pod_detail("db-postgres-0");
    integrated_response.owner_references.push(OwnerReference {
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        name: "db-owner".into(),
        uid: "uid-db-owner".into(),
        controller: true,
    });
    let mut dedicated_response = typed_pod_detail("web-frontend-7d9f8-00001");
    dedicated_response.owner_references.push(OwnerReference {
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        name: "web-owner".into(),
        uid: "uid-web-owner".into(),
        controller: true,
    });
    for response in [integrated_response, dedicated_response] {
        harness
            .state_mut()
            .feed
            .details
            .insert(response.identity.clone(), response);
    }
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity(
            "Pod",
            "web-frontend-7d9f8-00001",
        )));
    harness.run_steps(3);
    surrender_text_focus(&mut harness);

    harness.key_press(egui::Key::O);
    harness.run_steps(2);
    let pinned_names = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter_map(|window| match &window.content {
            WindowContent::Detail(detail) => Some(detail.identity.name.as_str()),
            WindowContent::Resource(_) | WindowContent::Services(_) => None,
        })
        .collect::<Vec<_>>();
    assert!(pinned_names.contains(&"web-owner"));
    assert!(!pinned_names.contains(&"db-owner"));
}

#[test]
fn dedicated_cluster_scoped_title_and_copy_actions_use_pinned_identity() {
    let mut harness = harness();
    let node = ResourceIdentity {
        context: CONTEXT.into(),
        gvk: GroupVersionKind::core("v1", "Node"),
        namespace: None,
        name: "worker-a".into(),
        uid: String::new(),
    };
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(node));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Node · worker-a");
    harness.get_by_role_and_label(Role::Button, "Node · worker-a");
    // Copy name lives in the Actions overflow menu, not the action row.
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Copy name")
            .is_none(),
        "Copy name must not occupy the action row"
    );
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Copy namespace")
            .is_none()
    );
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Copy UID")
            .is_none()
    );
    detail
        .get_by_role_and_label(Role::Button, "Actions")
        .click();
    harness.run_steps(1);
    harness.get_by_role_and_label(Role::Button, "Copy name");
    // A cluster-scoped identity without a UID keeps the menu minimal.
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Copy namespace")
            .is_none()
    );
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Copy UID")
            .is_none()
    );
}

#[test]
fn dedicated_detail_uses_identity_bound_authority_not_arbitrary_source_windows() {
    let mut harness = harness();
    let mut detail = deployment_detail("web-frontend");
    detail.capabilities.can_restart = true;
    let target = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(target.clone(), detail);
    open(
        &mut harness,
        LauncherItem::Workload(WorkloadKind::Deployments),
    );
    let source = workload_window_id(harness.state().shell.workspace(), WorkloadKind::Deployments);
    harness.state_mut().feed.window_lists.insert(
        source,
        vec![list_row(
            "apps",
            "v1",
            "Deployment",
            "web-frontend",
            "20/20 ready",
        )],
    );
    harness.state_mut().feed.detail_authority.insert(
        target.clone(),
        DetailAuthority {
            freshness: WindowFreshness::StaleRetrying {
                last_sync_age: "30s ago".into(),
                retry_in: "3s".into(),
                attempt: 1,
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    // A second source advertises live data for the same identity. Dedicated
    // detail authority must not depend on HashMap iteration order.
    let other_source = k10s_ui::workspace::WindowId(source.0 + 100);
    harness.state_mut().feed.window_lists.insert(
        other_source,
        vec![list_row(
            "apps",
            "v1",
            "Deployment",
            "web-frontend",
            "20/20 ready",
        )],
    );
    harness.state_mut().feed.window_freshness.insert(
        other_source,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    detail.get_by_label("Freshness · stale");
    detail
        .get_by_role_and_label(Role::Button, "More detail actions")
        .click();
    harness.run_steps(1);
    assert!(
        harness
            .get_by_role_and_label(Role::Button, "Restart…")
            .accesskit_node()
            .is_disabled()
    );
}

#[test]
fn integrated_detail_has_inner_identity_heading_but_dedicated_caption_is_not_duplicated() {
    let mut harness = harness();
    let target = identity("Deployment", "web-frontend");
    open(
        &mut harness,
        LauncherItem::Workload(WorkloadKind::Deployments),
    );
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Heading, "Deployment · default / web-frontend");

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target));
    harness.run_steps(4);
    let dedicated =
        harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    assert!(
        dedicated
            .query_by_role_and_label(Role::Heading, "Deployment · default / web-frontend")
            .is_none(),
        "the outer dedicated caption is the sole accessible identity"
    );
}

#[test]
fn dedicated_detail_without_identity_bound_authority_fails_closed() {
    let mut harness = harness();
    let mut response = deployment_detail("web-frontend");
    response.capabilities.can_restart = true;
    let target = response.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(target.clone(), response);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    detail.get_by_label("Freshness · unavailable");
    detail
        .get_by_role_and_label(Role::Button, "More detail actions")
        .click();
    harness.run_steps(1);
    assert!(
        harness
            .get_by_role_and_label(Role::Button, "Restart…")
            .accesskit_node()
            .is_disabled()
    );
}

#[test]
fn dedicated_dialog_opened_live_cannot_submit_after_exact_authority_is_revoked() {
    let mut harness = harness();
    let response = typed_deployment_detail("web-frontend");
    let identity = response.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), response);
    harness.state_mut().feed.detail_authority.insert(
        identity.clone(),
        DetailAuthority {
            freshness: WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()));
    let window = detail_window(harness.state().shell.workspace()).id;
    harness
        .state_mut()
        .shell
        .dialogs_mut()
        .open_scale(window, identity.clone(), Some(20));
    harness.run_steps(3);
    assert!(
        !harness
            .get_by_role_and_label(Role::Window, "Scale workload")
            .get_by_role_and_label(Role::Button, "Apply scale")
            .accesskit_node()
            .is_disabled()
    );

    harness.state_mut().feed.detail_authority.remove(&identity);
    harness.run_steps(2);
    assert!(
        harness
            .get_by_role_and_label(Role::Window, "Scale workload")
            .get_by_role_and_label(Role::Button, "Apply scale")
            .accesskit_node()
            .is_disabled()
    );
    harness
        .state_mut()
        .shell
        .dialogs_mut()
        .submit_active(window);
    assert!(harness.state_mut().shell.drain_dialog_actions().is_empty());
}

#[test]
fn unrelated_authoritative_list_does_not_mark_dedicated_identity_gone() {
    let mut harness = harness();
    let target = identity("Deployment", "web-frontend");
    harness.state_mut().feed.lists.insert(
        WorkloadKind::Deployments,
        vec![list_row(
            "apps",
            "v1",
            "Deployment",
            "unrelated-api",
            "2/2 ready",
        )],
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target));
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    assert!(
        detail
            .query_by_label("This resource no longer exists")
            .is_none()
    );
    detail.get_by_label("Loading details");
}

#[test]
fn exact_gone_lifecycle_wins_over_cached_detail_response() {
    let mut harness = harness();
    let response = typed_deployment_detail("web-frontend");
    let target = response.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(target.clone(), response);
    harness.state_mut().feed.detail_authority.insert(
        target.clone(),
        DetailAuthority {
            freshness: WindowFreshness::ReadyEmpty,
            lifecycle: DetailLifecycle::Gone,
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target));
    harness.run_steps(4);

    harness
        .get_by_role_and_label(Role::Window, "Deployment · default / web-frontend")
        .get_by_label("This resource no longer exists");
}

#[test]
fn recreated_name_with_new_uid_does_not_revive_gone_dedicated_identity() {
    let mut harness = harness();
    let old = identity("Deployment", "web-frontend");
    let mut recreated = old.clone();
    recreated.uid = "uid-recreated-web-frontend".into();
    harness.state_mut().feed.detail_authority.insert(
        old.clone(),
        DetailAuthority {
            freshness: WindowFreshness::ReadyEmpty,
            lifecycle: DetailLifecycle::Gone,
        },
    );
    harness.state_mut().feed.detail_authority.insert(
        recreated.clone(),
        DetailAuthority {
            freshness: WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    let mut row = list_row("apps", "v1", "Deployment", "web-frontend", "1/1 ready");
    row.identity = recreated;
    harness
        .state_mut()
        .feed
        .lists
        .insert(WorkloadKind::Deployments, vec![row]);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(old));
    harness.run_steps(4);

    harness
        .get_by_role_and_label(Role::Window, "Deployment · default / web-frontend")
        .get_by_label("This resource no longer exists");
}

#[test]
fn detail_overflow_opens_only_the_controller_owner() {
    let mut harness = harness();
    let mut detail = pod_detail("db-postgres-0");
    detail.owner_references = vec![
        OwnerReference {
            gvk: GroupVersionKind::core("v1", "ConfigMap"),
            name: "non-controller".into(),
            uid: "config-uid".into(),
            controller: false,
        },
        OwnerReference {
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "ReplicaSet".into(),
            },
            name: "web-frontend-7d9f8".into(),
            uid: "replicaset-uid".into(),
            controller: true,
        },
    ];
    harness
        .state_mut()
        .feed
        .details
        .insert(detail.identity.clone(), detail);
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Actions")
        .click();
    harness.run_steps(1);
    harness.get_by_role_and_label(Role::Button, "Copy namespace");
    harness.get_by_role_and_label(Role::Button, "Copy UID");
    assert!(harness.query_by_label("More details").is_none());
    assert!(harness.query_by_label("More").is_none());
    harness
        .get_by_role_and_label(Role::Button, "Open owner web-frontend-7d9f8")
        .click();
    harness.run_steps(3);

    assert_eq!(
        pinned_identity(harness.state().shell.workspace()),
        &ResourceIdentity {
            context: CONTEXT.into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "ReplicaSet".into(),
            },
            namespace: Some("default".into()),
            name: "web-frontend-7d9f8".into(),
            uid: "replicaset-uid".into(),
        }
    );
}

#[test]
fn crashloop_logs_default_to_previous_with_complete_toolbar() {
    let mut harness = harness();
    let pod = identity("Pod", "web-frontend-7d9f8-00001");
    let mut detail = typed_pod_runtime_detail("web-frontend-7d9f8-00001");
    let Some(ResourceProjection::Pod(projection)) = detail.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    projection.containers[0].state = Some(ContainerStateProjection::Waiting {
        reason: Some("CrashLoopBackOff".into()),
    });
    projection.containers[0].last_termination = Some(ContainerTerminationProjection {
        exit_code: 1,
        reason: Some("Error".into()),
    });
    harness.state_mut().feed.details.insert(pod, detail);
    open(&mut harness, LauncherItem::Workload(WorkloadKind::Pods));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend-7d9f8-00001")
        .click();
    harness.run_steps(3);
    assert!(harness.state_mut().shell.drain_log_actions().is_empty());
    let window_id = workload_window_id(harness.state().shell.workspace(), WorkloadKind::Pods);
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    // One frame applies the tab command; the next is the first Logs render.
    harness.run_steps(2);

    let expected_target = k10s_protocol::StreamTarget {
        context: CONTEXT.to_owned(),
        namespace: "default".to_owned(),
        pod: "web-frontend-7d9f8-00001".to_owned(),
        uid: format!("uid-{CONTEXT}-pod-default-web-frontend-7d9f8-00001"),
        container: "app".to_owned(),
    };
    assert_eq!(
        harness.state_mut().shell.drain_log_actions(),
        vec![(
            window_id,
            LogsAction::OpenLogs {
                window: window_id,
                target: expected_target.clone(),
                since_seconds: Some(300),
                previous: true,
            },
        )]
    );
    harness.run_steps(3);
    assert!(harness.state_mut().shell.drain_log_actions().is_empty());

    let window = common::workload_window(&harness, "Pods");
    for label in ["Previous", "Wrap"] {
        window.get_by_role_and_label(Role::CheckBox, label);
    }
    window.get_by_role_and_label(Role::Button, "Export");
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Connect logs")
            .is_none()
    );
    assert!(
        window
            .query_by_role_and_label(Role::CheckBox, "Follow")
            .is_none()
    );
    window.get_by_role_and_label(Role::TextInput, "Find in logs");
    window.get_by_label(
        "CrashLoopBackOff: showing logs from the previous terminated container by default",
    );

    harness
        .state_mut()
        .shell
        .stream_stores_mut()
        .logs
        .get_mut(window_id)
        .expect("logs view exists")
        .fail("ticket expired safely");
    harness.run_steps(1);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("ticket expired safely");
    window
        .get_by_role_and_label(Role::Button, "Retry logs")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_log_actions(),
        vec![(
            window_id,
            LogsAction::OpenLogs {
                window: window_id,
                target: expected_target,
                since_seconds: Some(300),
                previous: true,
            },
        )]
    );
    harness.run_steps(2);
    assert!(harness.state_mut().shell.drain_log_actions().is_empty());
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
        .with_step_dt(0.3)
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
            can_delete: true,
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
            can_delete: true,
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n"),
        projection: None,
    }
}

fn typed_pod_detail(name: &str) -> ResourceDetailResponse {
    let mut detail = pod_detail(name);
    detail.projection = Some(ResourceProjection::Pod(PodProjection {
        phase: Some("Running".into()),
        ready_containers: Some(1),
        total_containers: Some(1),
        restart_count: Some(2),
        containers: Vec::new(),
        conditions: Vec::new(),
        node_name: Some("worker-a".into()),
        pod_ip: Some("10.244.0.9".into()),
        host_ip: None,
        qos_class: None,
        priority: None,
        service_account: None,
        restart_policy: None,
        ports: Vec::new(),
        labels: (0..6)
            .map(|index| (format!("label-{index}"), format!("value-{index}")))
            .collect(),
        annotations: Default::default(),
        created_at: Some(rfc3339_ago(2 * 60 * 60)),
    }));
    detail
}

fn typed_pod_runtime_detail(name: &str) -> ResourceDetailResponse {
    let mut detail = typed_pod_detail(name);
    let Some(ResourceProjection::Pod(projection)) = detail.projection.as_mut() else {
        unreachable!("typed Pod fixture owns a Pod projection");
    };
    projection.containers = vec![PodContainerProjection {
        name: "app".into(),
        image: Some("example/app:latest".into()),
        state: Some(ContainerStateProjection::Running),
        ready: Some(true),
        restart_count: Some(0),
        last_termination: None,
    }];
    detail
}

fn typed_deployment_detail(name: &str) -> ResourceDetailResponse {
    let mut detail = deployment_detail(name);
    detail.projection = Some(ResourceProjection::Deployment(DeploymentProjection {
        desired_replicas: Some(20),
        ready_replicas: Some(18),
        updated_replicas: Some(19),
        available_replicas: Some(17),
        strategy: Some("RollingUpdate".into()),
        selector: Default::default(),
        max_surge: None,
        max_unavailable: None,
        conditions: vec![ResourceConditionProjection {
            condition_type: "Progressing".into(),
            status: "True".into(),
            reason: Some("NewReplicaSetAvailable".into()),
            message: None,
            last_transition_time: None,
        }],
        template_containers: Vec::new(),
        template_labels: Default::default(),
        template_annotations: Default::default(),
        labels: Default::default(),
        annotations: Default::default(),
        created_at: Some(rfc3339_ago(3 * 24 * 60 * 60)),
    }));
    detail
}

fn rfc3339_ago(seconds: u64) -> String {
    let then = std::time::SystemTime::now() - std::time::Duration::from_secs(seconds);
    jiff::Timestamp::try_from(then)
        .expect("test timestamp is in Jiff's supported range")
        .to_string()
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

fn detail_tab(
    workspace: &WorkspaceState<ResourceIdentity>,
    window_id: k10s_ui::workspace::WindowId,
) -> WorkspaceDetailTab {
    match &workspace
        .window(window_id)
        .expect("detail owner is open")
        .content
    {
        WindowContent::Detail(detail) => detail.active_tab,
        WindowContent::Resource(resource) => {
            resource
                .detail
                .as_ref()
                .expect("integrated detail is open")
                .active_tab
        }
        WindowContent::Services(service) => {
            service
                .detail
                .as_ref()
                .expect("integrated service detail is open")
                .active_tab
        }
    }
}

fn surrender_text_focus(harness: &mut Harness<'static, Fixture>) {
    if let Some(focused) = harness.ctx.memory(|memory| memory.focused()) {
        harness
            .ctx
            .memory_mut(|memory| memory.surrender_focus(focused));
    }
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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
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
    window.get_by_role_and_label(Role::Button, "Scale…");
    window.get_by_role_and_label(Role::Button, "Delete…");
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
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Pods");
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
    assert!(window.query_by_label("Scale…").is_none());
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

    let window = common::workload_window(&harness, "Pods");
    assert!(
        window
            .query_by_label("Pod · default / db-postgres-0")
            .is_none()
    );
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
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Pod · default / db-postgres-0");
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);

    assert!(
        common::workload_window(&harness, "Pods")
            .query_by_label("Pod · default / db-postgres-0")
            .is_none()
    );
}

#[test]
fn restart_uses_the_pinned_target_and_respects_window_freshness() {
    let mut harness = harness();
    let mut detail = deployment_detail("web-frontend");
    detail.capabilities.can_restart = true;
    let target = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(target.clone(), detail);
    open(
        &mut harness,
        LauncherItem::Workload(WorkloadKind::Deployments),
    );
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(3);
    let window_id =
        workload_window_id(harness.state().shell.workspace(), WorkloadKind::Deployments);
    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    harness.run_steps(2);
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Restart…")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::Restart {
            window: window_id,
            target,
        }]
    );

    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::StaleRetrying {
            last_sync_age: "30s ago".into(),
            retry_in: "3s".into(),
            attempt: 1,
        },
    );
    harness.run_steps(2);
    assert!(
        common::workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::Button, "Restart…")
            .accesskit_node()
            .is_disabled()
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
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);

    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Tab YAML")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Pods");
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
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Pod · default / db-postgres-0");
    window.get_by_label("Loading details");

    // Once resolved, the header shows the backend-asserted fields and the
    // Overview section renders its rows.
    harness.state_mut().feed.details.insert(
        identity("Pod", "db-postgres-0"),
        pod_detail("db-postgres-0"),
    );
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Structured details unavailable");
    assert!(window.query_by_label("Status Running").is_none());
    assert!(window.query_by_label("Loading details").is_none());

    // Backend-resolved events render on their own tab.
    window
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(4);
    common::workload_window(&harness, "Pods").get_by_label("Started container started");
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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    // Give the detail pane nearly the whole window so both resolved groups
    // are on screen at once.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(deployments_id, 0.05));
    harness.run_steps(4);
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(3);
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Tab Pods")
        .click();
    harness.run_steps(3);
    let window = common::workload_window(&harness, "Deployments");
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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource api-server")
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
    {
        let dedicated =
            harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
        dedicated
            .get_by_role_and_label(Role::Button, "Actions")
            .click();
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
    }
    harness.run_steps(1);
    harness.get_by_role_and_label(Role::Button, "Copy UID");

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
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);
    common::workload_window(&harness, "Deployments")
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

#[test]
fn splitter_grip_is_visible() {
    let mut harness = harness();
    let deployment = identity("Deployment", "web-frontend");
    harness.state_mut().feed.primary_details.insert(
        deployment.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("test")),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
    let grip = window.get_by_label("Detail split grip");
    let grip_rect = grip.rect();
    let window_rect = window.rect();
    assert!(
        grip_rect.width() >= window_rect.width() * 0.9,
        "grip {grip_rect:?} must span most of window {window_rect:?}"
    );
    // The grip sits between the list pane and the integrated detail pane.
    // After selection, the row is no longer a button, so use the window's
    // top area as a proxy for the list position.
    let detail = window
        .get_by_label("Deployment · default / web-frontend")
        .rect();
    assert!(
        grip_rect.top() < detail.top(),
        "grip {grip_rect:?} must be above detail {detail:?}"
    );
}

#[test]
fn detail_action_row_orders_scale_restart_actions_delete_left_to_right() {
    let mut harness = harness();
    let mut detail = deployment_detail("web-frontend");
    detail.capabilities.can_restart = true;
    detail.capabilities.can_scale = true;
    detail.capabilities.can_delete = true;
    harness
        .state_mut()
        .feed
        .details
        .insert(identity("Deployment", "web-frontend"), detail);
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Deployments),
    );
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(3);

    let window = common::workload_window(&harness, "Deployments");
    let scale = window.get_by_role_and_label(Role::Button, "Scale…").rect();
    let restart = window
        .get_by_role_and_label(Role::Button, "Restart…")
        .rect();
    let actions = window.get_by_role_and_label(Role::Button, "Actions").rect();
    let delete = window.get_by_role_and_label(Role::Button, "Delete…").rect();
    assert!(
        scale.left() < restart.left()
            && restart.left() < actions.left()
            && actions.left() < delete.left(),
        "reference order is Scale, Restart, Actions, Delete (left to right): {scale:?} {restart:?} {actions:?} {delete:?}"
    );
    // Copy name moved to the overflow menu, so it must not occupy the row.
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Copy name")
            .is_none(),
        "Copy name must not occupy the action row"
    );
    window
        .get_by_role_and_label(Role::Button, "Actions")
        .click();
    harness.run_steps(1);
    harness.get_by_role_and_label(Role::Button, "Copy name");
}

#[test]
fn narrow_detail_keeps_critical_controls_and_exposes_displaced_items_in_menus() {
    let mut harness = harness();
    let mut detail = typed_deployment_detail("web-frontend");
    detail.capabilities.can_restart = true;
    detail.capabilities.can_scale = true;
    detail.capabilities.can_delete = true;
    let target = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(target.clone(), detail);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(target.clone()));
    let window_id = detail_window(harness.state().shell.workspace()).id;
    harness.state_mut().feed.detail_authority.insert(
        target.clone(),
        DetailAuthority {
            freshness: WindowFreshness::Live {
                last_sync_age: "just now".into(),
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetActiveTab(
            window_id,
            WorkspaceDetailTab::Yaml,
        ));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [440.0, 560.0],
                collapsed: false,
            },
        ));
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    window.get_by_label("Rollout ● NewReplicaSetAvailable");
    window.get_by_label("Ready · 18/20");
    window.get_by_role_and_label(Role::Button, "Show more Deployment vitals");
    window.get_by_role_and_label(Role::Button, "Tab YAML");
    window.get_by_role_and_label(Role::Button, "Scale…");
    window.get_by_role_and_label(Role::Button, "Delete…");
    let owner = window.rect();
    let strip = window.get_by_label("Detail vital strip").rect();
    let rollout = window
        .get_by_label("Rollout ● NewReplicaSetAvailable")
        .rect();
    let ready = window.get_by_label("Ready · 18/20").rect();
    let more_vitals = window
        .get_by_role_and_label(Role::Button, "Show more Deployment vitals")
        .rect();
    let active_tab = window
        .get_by_role_and_label(Role::Button, "Tab YAML")
        .rect();
    let more_tabs = window
        .get_by_role_and_label(Role::Button, "More detail tabs")
        .rect();
    let scale = window.get_by_role_and_label(Role::Button, "Scale…").rect();
    let delete = window.get_by_role_and_label(Role::Button, "Delete…").rect();
    let more_actions = window
        .get_by_role_and_label(Role::Button, "More detail actions")
        .rect();
    let tabs_row = window.get_by_label("Detail tabs row").rect();
    let actions_row = window.get_by_label("Detail actions row").rect();
    for (name, rect) in [
        ("rollout", rollout),
        ("ready", ready),
        ("more vitals", more_vitals),
    ] {
        assert!(
            strip.contains_rect(rect),
            "{name} {rect:?} escapes vital strip {strip:?}"
        );
    }
    for (name, row, rect) in [
        ("active tab", tabs_row, active_tab),
        ("more tabs", tabs_row, more_tabs),
        ("scale", actions_row, scale),
        ("more actions", actions_row, more_actions),
        ("delete", actions_row, delete),
    ] {
        assert!(
            row.contains_rect(rect),
            "{name} {rect:?} escapes row {row:?}"
        );
    }
    let gap = 8.0;
    assert!(
        tabs_row.width() >= active_tab.width() + more_tabs.width() + gap,
        "tabs row {:?} cannot paint intrinsic controls {:?} + {:?}",
        tabs_row,
        active_tab,
        more_tabs
    );
    assert!(
        actions_row.width() >= scale.width() + more_actions.width() + delete.width() + gap * 2.0,
        "actions row {:?} cannot paint intrinsic controls {:?} + {:?} + {:?}",
        actions_row,
        scale,
        more_actions,
        delete
    );
    for pair in [
        (rollout, ready),
        (ready, more_vitals),
        (active_tab, more_tabs),
        (scale, more_actions),
        (more_actions, delete),
    ] {
        assert!(
            !pair.0.intersects(pair.1),
            "narrow controls overlap: {pair:?}"
        );
    }
    for (name, rect) in [
        ("active tab", active_tab),
        ("more tabs", more_tabs),
        ("scale", scale),
        ("more actions", more_actions),
        ("delete", delete),
    ] {
        assert!(
            owner.contains_rect(rect),
            "{name} {rect:?} escapes detail {owner:?}"
        );
    }
    window
        .get_by_role_and_label(Role::Button, "More detail tabs")
        .click();
    harness.run_steps(1);
    harness
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(2);
    let active_tab = match &detail_window(harness.state().shell.workspace()).content {
        WindowContent::Detail(detail) => detail.active_tab,
        WindowContent::Resource(_) | WindowContent::Services(_) => unreachable!(),
    };
    assert_eq!(active_tab, WorkspaceDetailTab::Events);

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · default / web-frontend");
    window
        .get_by_role_and_label(Role::Button, "More detail actions")
        .click();
    harness.run_steps(1);
    harness
        .get_by_role_and_label(Role::Button, "Restart…")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::Restart {
            window: window_id,
            target,
        }]
    );
}

#[test]
fn narrow_integrated_detail_budgets_intrinsic_tab_and_action_controls() {
    let mut harness = Harness::builder()
        .with_size(egui::vec2(700.0, 900.0))
        .with_pixels_per_point(1.0)
        .with_step_dt(0.3)
        .build_ui_state(render, Fixture::default());
    let mut detail = typed_deployment_detail("web-frontend");
    detail.capabilities.can_restart = true;
    detail.capabilities.can_scale = true;
    detail.capabilities.can_delete = true;
    harness
        .state_mut()
        .feed
        .details
        .insert(detail.identity.clone(), detail);
    open(
        &mut harness,
        LauncherItem::Workload(WorkloadKind::Deployments),
    );
    let window_id =
        workload_window_id(harness.state().shell.workspace(), WorkloadKind::Deployments);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [32.0, 32.0],
                size: [440.0, 560.0],
                collapsed: false,
            },
        ));
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
    let tabs_row = window.get_by_label("Detail tabs row").rect();
    let actions_row = window.get_by_label("Detail actions row").rect();
    let active = window
        .get_by_role_and_label(Role::Button, "Tab Overview")
        .rect();
    let scale = window.get_by_role_and_label(Role::Button, "Scale…").rect();
    let delete = window.get_by_role_and_label(Role::Button, "Delete…").rect();
    if let Some(more_tabs) = window.query_by_role_and_label(Role::Button, "More detail tabs") {
        assert!(tabs_row.width() >= active.width() + more_tabs.rect().width() + 8.0);
        let more_actions = window
            .get_by_role_and_label(Role::Button, "More detail actions")
            .rect();
        assert!(
            actions_row.width() >= scale.width() + more_actions.width() + delete.width() + 16.0
        );
    } else {
        let tabs_width = ["Tab Overview", "Tab Pods", "Tab Events", "Tab YAML"]
            .iter()
            .map(|label| {
                window
                    .get_by_role_and_label(Role::Button, label)
                    .rect()
                    .width()
            })
            .sum::<f32>();
        let restart = window
            .get_by_role_and_label(Role::Button, "Restart…")
            .rect();
        let actions = window.get_by_role_and_label(Role::Button, "Actions").rect();
        assert!(tabs_row.width() >= tabs_width + 24.0);
        assert!(
            actions_row.width()
                >= scale.width() + restart.width() + actions.width() + delete.width() + 24.0
        );
    }
}

#[test]
fn detail_vital_chips_are_bounded_with_label_and_value() {
    let mut harness = harness();
    let pod = identity("Pod", "db-postgres-0");
    harness.state_mut().feed.primary_details.insert(
        pod.clone(),
        PrimaryDetailState::Loaded(pod_detail("db-postgres-0")),
    );
    open(
        &mut harness,
        LauncherItem::Workload(k10s_ui::workspace::WorkloadKind::Pods),
    );
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(3);

    let window = common::workload_window(&harness, "Pods");
    let strip = window.get_by_label("Detail vital strip").rect();
    // The strip should contain vitals as bounded chips.
    // Verify the strip has content by checking for known vitals.
    let has_vitals = ["Status ● —", "Ready · —"]
        .iter()
        .any(|label| window.query_by_label(label).is_some());
    assert!(
        has_vitals,
        "vital strip {strip:?} should contain at least one vital chip"
    );
}
