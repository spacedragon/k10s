//! Deployment-only characterization, projection, relation, responsive, and
//! accessibility coverage for the redesigned typed Overview.

use std::collections::BTreeMap;
use std::sync::Arc;

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, ContainerImageProjection, ContainerStateProjection,
    ContainerTerminationProjection, DeploymentProjection, DetailRow, DetailSection, EventRow,
    GroupVersionKind, PodContainerProjection, PodProjection, RelatedGroup, ReplicaSetProjection,
    ResourceCapabilities, ResourceConditionProjection, ResourceDetailResponse, ResourceIdentity,
    ResourceListRow, ResourceProjection, ResourceRelationsResponse,
};
use k10s_ui::{
    ui::{
        ConnectionState, DetailAuthority, DetailLifecycle, PrimaryDetailState, RelationState,
        ResourceFeed, SafeUiError, UiShell, WindowFreshness,
    },
    workspace::{LauncherItem, WindowGeom, WindowKind, WorkloadKind, WorkspaceCommand},
};

const CONTEXT: &str = "deployment-test";
const NAME: &str = "checkout";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let contexts = [CONTEXT.to_owned()];
    let mut selected_context = Some(CONTEXT.to_owned());
    fixture.shell.show_with_resources(
        ui,
        ConnectionState::Connected,
        &contexts,
        &mut selected_context,
        None,
        &fixture.feed,
    );
}

fn harness(size: egui::Vec2) -> Harness<'static, Fixture> {
    let mut fixture = Fixture::default();
    // Pin the frame clock to 2026-09-01T08:00:00Z: the checkout fixture was
    // created 2026-08-01T08:00:00Z, so its vital-strip age stays a stable
    // `31d` instead of drifting with wall-clock time.
    fixture.feed.render_time =
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_788_249_600));
    Harness::builder()
        .with_size(size)
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture)
}

fn snapshot_deployment_window(harness: &Harness<Fixture>, name: &str) {
    fn walk(node: egui_kittest::Node<'_>, depth: usize, output: &mut String) {
        let accessible = node.accesskit_node();
        output.push_str(&"  ".repeat(depth));
        output.push_str(&format!("{:?}", accessible.role()));
        if let Some(label) = accessible.label() {
            output.push_str(&format!(" label={label:?}"));
        }
        if let Some(value) = accessible.value() {
            output.push_str(&format!(" value={value:?}"));
        }
        output.push('\n');
        for child in node.children() {
            walk(child, depth + 1, output);
        }
    }

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    let mut actual = String::new();
    walk(window, 0, &mut actual);
    let path = std::path::Path::new("tests")
        .join("snapshots")
        .join(format!("deployment_detail_{name}.txt"));
    if std::env::var_os("K10S_UPDATE_DEPLOYMENT_SNAPSHOTS").is_some() {
        std::fs::create_dir_all(path.parent().expect("snapshot parent"))
            .expect("snapshot directory");
        std::fs::write(&path, actual).expect("snapshot write");
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("missing Deployment snapshot {}: {error}", path.display()));
    assert_eq!(expected.replace("\r\n", "\n"), actual);
}

fn deployment_identity(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: CONTEXT.into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("payments".into()),
        name: name.into(),
        uid: format!("uid-deployment-{name}"),
    }
}

fn deployment_row(name: &str) -> ResourceListRow {
    ResourceListRow {
        identity: deployment_identity(name),
        revision: BackendRevision::new(44),
        labels: BTreeMap::new(),
        summary: "3/3 ready".into(),
        created_at: "2026-08-01T08:00:00Z".into(),
        projection: Some(ResourceProjection::Deployment(projection(Vec::new()))),
    }
}

fn open_integrated_deployment(
    harness: &mut Harness<'static, Fixture>,
    detail: ResourceDetailResponse,
    verify_list_overflow: bool,
) {
    let identity = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .lists
        .insert(WorkloadKind::Deployments, vec![deployment_row(NAME)]);
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), detail);
    harness
        .state_mut()
        .feed
        .relations
        .insert(identity.clone(), exact_relations(&identity));
    harness.state_mut().feed.detail_authority.insert(
        identity,
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
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkloadKind::Deployments),
        ));
    harness.run_steps(4);
    let window = integrated_deployment_window(harness);
    window.get_by_role_and_label(Role::TextInput, "Search deployments");
    assert_eq!(
        window
            .query_all_by_role(Role::ComboBox)
            .filter(|node| {
                node.accesskit_node()
                    .label()
                    .or_else(|| node.value())
                    .is_some_and(|text| {
                        text.starts_with("Namespace: ") || text.starts_with("Status: ")
                    })
            })
            .count(),
        2,
        "the integrated list toolbar exposes Namespace and Status controls"
    );
    window.get_by_label("1 deployments");
    if verify_list_overflow {
        window
            .get_by_role_and_label(Role::Button, "More list controls")
            .click();
        harness.step();
        let owner = integrated_deployment_window(harness).rect();
        for item in ["switch to absolute", "Refresh list"] {
            let rect = harness
                .root()
                .children_recursive()
                .find(|node| {
                    node.accesskit_node().role() == Role::Button
                        && node
                            .accesskit_node()
                            .label()
                            .is_some_and(|label| label.contains(item))
                })
                .unwrap_or_else(|| panic!("{item} in list overflow"))
                .rect();
            assert_rect_within(owner, rect, 1.0, item);
        }
        let columns = harness
            .root()
            .children_recursive()
            .find(|node| {
                node.accesskit_node().role() == Role::Button
                    && node
                        .accesskit_node()
                        .label()
                        .is_some_and(|label| label.contains("Columns ▾"))
            })
            .expect("Columns control in list overflow");
        assert_rect_within(owner, columns.rect(), 1.0, "Columns menu");
        harness.get_by_label_contains("Age shown as relative");
        harness
            .get_by_role_and_label(Role::Button, "More list controls")
            .click();
        harness.run_steps(1);
    }
    integrated_deployment_window(harness)
        .get_by_role_and_label(Role::Button, "Select resource checkout")
        .click();
    harness.run_steps(5);
}

fn assert_rect_within(owner: egui::Rect, child: egui::Rect, tolerance: f32, name: &str) {
    let owner = owner.expand(tolerance);
    assert!(
        owner.contains_rect(child),
        "{name} {child:?} escapes owner {owner:?} (including {tolerance}pt tolerance)"
    );
}

fn integrated_deployment_window<'a>(
    harness: &'a Harness<'static, Fixture>,
) -> egui_kittest::Node<'a> {
    harness
        .query_all_by_label_contains("Deployments ·")
        .find(|node| node.accesskit_node().role() == Role::Window)
        .expect("integrated Deployment workload window")
}

fn condition(
    condition_type: &str,
    status: &str,
    reason: Option<&str>,
) -> ResourceConditionProjection {
    ResourceConditionProjection {
        condition_type: condition_type.into(),
        status: status.into(),
        reason: reason.map(str::to_owned),
        message: None,
        last_transition_time: Some("2026-08-30T10:00:00Z".into()),
    }
}

fn projection(conditions: Vec<ResourceConditionProjection>) -> DeploymentProjection {
    DeploymentProjection {
        desired_replicas: Some(3),
        ready_replicas: Some(3),
        updated_replicas: Some(3),
        available_replicas: Some(3),
        strategy: Some("RollingUpdate".into()),
        selector: BTreeMap::from([("app".into(), "checkout".into())]),
        max_surge: Some("25%".into()),
        max_unavailable: Some("1".into()),
        conditions,
        template_containers: vec![ContainerImageProjection {
            name: "api".into(),
            image: Some("ghcr.io/acme/checkout:v4".into()),
        }],
        template_labels: BTreeMap::from([("app".into(), "checkout".into())]),
        template_annotations: BTreeMap::from([("checksum/config".into(), "abc123".into())]),
        labels: BTreeMap::from([
            ("app.kubernetes.io/instance".into(), "checkout-prod".into()),
            ("app.kubernetes.io/managed-by".into(), "Helm".into()),
            ("app.kubernetes.io/name".into(), "checkout".into()),
            ("app.kubernetes.io/part-of".into(), "shop".into()),
            ("team".into(), "payments".into()),
            ("tier".into(), "backend".into()),
        ]),
        annotations: BTreeMap::from([
            ("meta.helm.sh/release-name".into(), "checkout-prod".into()),
            ("meta.helm.sh/release-namespace".into(), "payments".into()),
        ]),
        created_at: Some("2026-08-01T08:00:00Z".into()),
    }
}

fn detail_with(projection: Option<DeploymentProjection>) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: deployment_identity(NAME),
        revision: BackendRevision::new(44),
        created_at: "generic-created-at-must-not-render".into(),
        owner_references: Vec::new(),
        sections: Vec::new(),
        events_condition: k10s_protocol::EventsCondition::Available,
        events: vec![EventRow {
            reason: "ScalingReplicaSet".into(),
            message: "Scaled up replica set checkout-4 to 3".into(),
            count: 2,
            last_seen: "2026-08-30T10:01:00Z".into(),
        }],
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_scale: true,
            can_restart: true,
            can_delete: true,
            ..ResourceCapabilities::default()
        },
        manifest: "typed-renderer-must-not-parse-this".into(),
        projection: projection.map(ResourceProjection::Deployment),
    }
}

fn pod_row(name: &str) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("payments".into()),
            name: name.into(),
            uid: format!("uid-pod-{name}"),
        },
        revision: BackendRevision::new(45),
        labels: BTreeMap::new(),
        summary: "summary-must-not-render".into(),
        created_at: "row-created-at-must-not-render".into(),
        projection: Some(ResourceProjection::Pod(PodProjection {
            phase: Some("Running".into()),
            ready_containers: Some(1),
            total_containers: Some(1),
            restart_count: Some(2),
            containers: Vec::new(),
            conditions: Vec::new(),
            node_name: Some("worker-a".into()),
            pod_ip: None,
            host_ip: None,
            qos_class: None,
            priority: None,
            service_account: None,
            restart_policy: None,
            ports: Vec::new(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            created_at: Some("4m".into()),
        })),
    }
}

fn replica_set_row(name: &str, revision: Option<u64>) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "ReplicaSet".into(),
            },
            namespace: Some("payments".into()),
            name: name.into(),
            uid: format!("uid-rs-{name}"),
        },
        revision: BackendRevision::new(43),
        labels: BTreeMap::new(),
        summary: "history-summary-must-not-render".into(),
        created_at: "row-created-at-must-not-render".into(),
        projection: revision.map(|revision| {
            ResourceProjection::ReplicaSet(ReplicaSetProjection {
                revision,
                replicas: Some(3),
                ready_replicas: Some(2),
                created_at: Some("2026-08-30T09:55:00Z".into()),
                images: vec![ContainerImageProjection {
                    name: "api".into(),
                    image: Some(format!("ghcr.io/acme/checkout:v{revision}")),
                }],
            })
        }),
    }
}

fn exact_relations(identity: &ResourceIdentity) -> RelationState {
    RelationState::Loaded {
        response: Arc::new(ResourceRelationsResponse {
            identity: identity.clone(),
            revision: BackendRevision::new(46),
            groups: vec![
                RelatedGroup {
                    title: "Pods".into(),
                    gvk: GroupVersionKind::core("v1", "Pod"),
                    rows: vec![pod_row("checkout-4-x7k9p")],
                },
                RelatedGroup {
                    title: "ReplicaSets".into(),
                    gvk: GroupVersionKind {
                        group: "apps".into(),
                        version: "v1".into(),
                        kind: "ReplicaSet".into(),
                    },
                    rows: vec![
                        replica_set_row("checkout-4", Some(4)),
                        replica_set_row("checkout-without-revision", None),
                    ],
                },
            ],
        }),
        loaded_at_ms: 1_000,
        refreshing: false,
        refresh_error: None,
    }
}

fn open_detail(
    harness: &mut Harness<'static, Fixture>,
    detail: ResourceDetailResponse,
    relation: Option<RelationState>,
    size: [f32; 2],
) {
    let identity = detail.identity.clone();
    open_detail_for(harness, identity, detail, relation, size);
}

fn open_detail_for(
    harness: &mut Harness<'static, Fixture>,
    identity: ResourceIdentity,
    detail: ResourceDetailResponse,
    relation: Option<RelationState>,
    size: [f32; 2],
) {
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), detail);
    if let Some(relation) = relation {
        harness
            .state_mut()
            .feed
            .relations
            .insert(identity.clone(), relation);
    }
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
    let window_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .expect("detail window is open")
        .id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [24.0, 24.0],
                size,
                collapsed: false,
            },
        ));
    harness.run_steps(5);
}

#[test]
fn deployment_primary_same_name_different_uid_fails_closed_without_body_or_vitals() {
    let identity = deployment_identity(NAME);
    let mut mismatched = detail_with(Some(projection(vec![condition(
        "Progressing",
        "True",
        Some("MISMATCHED_PRIMARY_MUST_NOT_RENDER"),
    )])));
    mismatched.identity.uid = "uid-recreated-checkout".into();

    let mut harness = harness(egui::vec2(1_120.0, 760.0));
    open_detail_for(&mut harness, identity, mismatched, None, [980.0, 620.0]);

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("Structured details unavailable");
    for unavailable in [
        "Rollout · —",
        "Ready · —",
        "Up-to-date · —",
        "Available · —",
        "Strategy · —",
        "Age · —",
    ] {
        window.get_by_label(unavailable);
    }
    for leaked in [
        "Rollout ● MISMATCHED_PRIMARY_MUST_NOT_RENDER",
        "Ready · 3/3",
        "Up-to-date · 3",
        "Available · 3",
        "Strategy · RollingUpdate",
        "TEMPLATE",
    ] {
        assert!(window.query_by_label(leaked).is_none(), "leaked {leaked}");
    }
}

#[test]
fn deployment_projection_complete_uses_typed_fields_only() {
    let mut harness = harness(egui::vec2(1_240.0, 820.0));
    let detail = detail_with(Some(projection(vec![condition(
        "Progressing",
        "True",
        Some("NewReplicaSetAvailable"),
    )])));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [1_050.0, 700.0],
    );

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    for label in [
        "Rollout ● NewReplicaSetAvailable",
        "PODS · 1",
        "ROLLOUT HISTORY",
        "4 current",
        "v4",
        "TEMPLATE",
        "Image (api): ghcr.io/acme/checkout:v4",
        "Selector: app=checkout",
        "MANAGED BY",
        "Manager · Helm",
        "Helm release · checkout-prod",
        "LABELS · 6",
        "IDENTITY",
        "Context · deployment-test",
    ] {
        window.get_by_label(label);
    }
    let painted = window
        .query_all_by_role(Role::TextRun)
        .filter_map(|node| node.value())
        .collect::<Vec<_>>();
    assert!(
        painted
            .iter()
            .any(|value| value == "ghcr.io/acme/checkout:v4")
    );
    assert!(painted.iter().any(|value| value == "app=checkout"));
    assert!(window.query_by_label("checkout-without-revision").is_none());
    assert!(
        window
            .query_by_label("typed-renderer-must-not-parse-this")
            .is_none()
    );
    assert!(window.query_by_label("summary-must-not-render").is_none());
    assert!(window.query_by_label("Roll back…").is_none());
}

#[test]
fn deployment_projection_progressing_failed_and_missing_are_semantic() {
    let cases = [
        (
            Some(projection(vec![condition(
                "Progressing",
                "True",
                Some("ReplicaSetUpdated"),
            )])),
            "Rollout ▲ ReplicaSetUpdated",
            false,
        ),
        (
            Some(projection(vec![condition(
                "Progressing",
                "False",
                Some("ProgressDeadlineExceeded"),
            )])),
            "Rollout ✕ ProgressDeadlineExceeded",
            false,
        ),
        (None, "Rollout · —", true),
    ];

    for (projection, rollout, unavailable) in cases {
        let mut harness = harness(egui::vec2(1_050.0, 700.0));
        open_detail(&mut harness, detail_with(projection), None, [900.0, 560.0]);
        let window =
            harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
        window.get_by_label(rollout);
        assert_eq!(
            window
                .query_by_label("Structured details unavailable")
                .is_some(),
            unavailable
        );
    }
}

#[test]
fn deployment_projection_incomplete_typed_fields_render_dashes() {
    let incomplete = DeploymentProjection {
        desired_replicas: None,
        ready_replicas: None,
        updated_replicas: None,
        available_replicas: None,
        strategy: None,
        selector: BTreeMap::new(),
        max_surge: None,
        max_unavailable: None,
        conditions: Vec::new(),
        template_containers: vec![ContainerImageProjection {
            name: "api".into(),
            image: None,
        }],
        template_labels: BTreeMap::new(),
        template_annotations: BTreeMap::new(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        created_at: None,
    };
    let mut harness = harness(egui::vec2(1_120.0, 760.0));
    open_detail(
        &mut harness,
        detail_with(Some(incomplete)),
        None,
        [980.0, 620.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    for label in [
        "Rollout · —",
        "Ready · —",
        "Up-to-date · —",
        "Available · —",
        "Strategy · —",
        "Age · —",
        "Image (api): —",
        "Selector: —",
        "Created · —",
    ] {
        window.get_by_label(label);
    }
    assert!(window.query_by_label("MANAGED BY").is_none());
    assert!(window.query_by_label("LABELS · 0").is_none());
    assert!(window.query_by_label("ANNOTATIONS · 0").is_none());
}

#[test]
fn related_pod_status_prefers_typed_container_failure_over_running_phase() {
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    let mut waiting_row = pod_row("checkout-crashloop");
    let Some(ResourceProjection::Pod(pod)) = waiting_row.projection.as_mut() else {
        panic!("fixture carries typed Pod projection");
    };
    pod.containers = vec![PodContainerProjection {
        name: "api".into(),
        image: Some("ghcr.io/acme/checkout:broken".into()),
        state: Some(ContainerStateProjection::Waiting {
            reason: Some("CrashLoopBackOff".into()),
        }),
        ready: Some(false),
        restart_count: Some(7),
        last_termination: None,
    }];
    let mut terminated_row = pod_row("checkout-exited");
    let Some(ResourceProjection::Pod(pod)) = terminated_row.projection.as_mut() else {
        panic!("fixture carries typed Pod projection");
    };
    pod.containers = vec![PodContainerProjection {
        name: "worker".into(),
        image: Some("ghcr.io/acme/checkout:broken".into()),
        state: Some(ContainerStateProjection::Terminated(
            ContainerTerminationProjection {
                exit_code: 137,
                reason: None,
            },
        )),
        ready: Some(false),
        restart_count: Some(1),
        last_termination: None,
    }];
    let relations = RelationState::Loaded {
        response: Arc::new(ResourceRelationsResponse {
            identity,
            revision: BackendRevision::new(46),
            groups: vec![RelatedGroup {
                title: "Pods".into(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                rows: vec![waiting_row, terminated_row],
            }],
        }),
        loaded_at_ms: 1_000,
        refreshing: false,
        refresh_error: None,
    };
    let mut harness = harness(egui::vec2(1_050.0, 700.0));
    open_detail(&mut harness, detail, Some(relations), [900.0, 560.0]);

    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("▲ CrashLoopBackOff");
    window.get_by_label("✕ Exit 137");
    assert!(window.query_by_label("● Running").is_none());
}

#[test]
fn deployment_projection_unavailable_rollout_events_are_explicit() {
    let mut detail = detail_with(Some(projection(Vec::new())));
    detail.events.clear();
    detail.events_condition = k10s_protocol::EventsCondition::Unavailable;
    let mut harness = harness(egui::vec2(1_120.0, 760.0));
    open_detail(&mut harness, detail, None, [980.0, 620.0]);
    harness
        .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
        .get_by_label("Rollout events unavailable");
}

#[test]
fn deployment_relations_loading_failed_stale_and_exact_are_explicit() {
    let identity = deployment_identity(NAME);
    let cases = [
        (RelationState::Loading, "Loading related resources"),
        (
            RelationState::Failed(SafeUiError::new("RBAC denied")),
            "Related resources unavailable: RBAC denied",
        ),
        (
            RelationState::Loaded {
                response: Arc::new(ResourceRelationsResponse {
                    identity: identity.clone(),
                    revision: BackendRevision::new(46),
                    groups: vec![RelatedGroup {
                        title: "Pods".into(),
                        gvk: GroupVersionKind::core("v1", "Pod"),
                        rows: vec![pod_row("checkout-stale")],
                    }],
                }),
                loaded_at_ms: 1_000,
                refreshing: true,
                refresh_error: Some(SafeUiError::new("refresh denied")),
            },
            "Refresh failed: refresh denied",
        ),
    ];

    for (relations, expected) in cases {
        let mut harness = harness(egui::vec2(1_050.0, 700.0));
        open_detail(
            &mut harness,
            detail_with(Some(projection(Vec::new()))),
            Some(relations),
            [900.0, 560.0],
        );
        let window =
            harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
        window.get_by_label(expected);
    }

    let mut harness = harness(egui::vec2(1_050.0, 700.0));
    let mut wrong = identity.clone();
    wrong.uid = "recreated-deployment".into();
    let mismatched = RelationState::Loaded {
        response: Arc::new(ResourceRelationsResponse {
            identity: wrong,
            revision: BackendRevision::new(46),
            groups: vec![RelatedGroup {
                title: "Pods".into(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                rows: vec![pod_row("must-not-leak")],
            }],
        }),
        loaded_at_ms: 1_000,
        refreshing: false,
        refresh_error: None,
    };
    open_detail(
        &mut harness,
        detail_with(Some(projection(Vec::new()))),
        Some(mismatched),
        [900.0, 560.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("Related resources unavailable for this deployment");
    assert!(window.query_by_label("must-not-leak").is_none());
}

#[test]
fn deployment_layout_narrow_prioritizes_operations_and_collapses_metadata() {
    let mut harness = harness(egui::vec2(820.0, 620.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [759.0, 520.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("PODS · 1");
    assert!(window.query_by_label("TEMPLATE").is_none());
    let expand = window.get_by_role_and_label(Role::Button, "Show Deployment metadata");
    expand.click();
    harness.run_steps(2);
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("TEMPLATE");
    window.get_by_role_and_label(Role::Button, "Hide Deployment metadata");
}

/// Regression guard for the Overview geometry: at the 1000x700 wide viewport
/// the operational/configuration columns keep the 1.35:1 ratio and label chips
/// never overlap the expand buttons; at the 640x700 narrow viewport the
/// metadata column collapses behind its disclosure button.
#[test]
fn deployment_overview_verification_geometries_and_no_overlap() {
    // Wide 1000x700: select from the real Deployment list and verify the
    // integrated detail pane, rather than manufacturing a dedicated window.
    let mut wide = harness(egui::vec2(1_000.0, 700.0));
    open_integrated_deployment(&mut wide, detail_with(Some(projection(Vec::new()))), true);
    let window = integrated_deployment_window(&wide);
    let operational = window.get_by_label("Operational detail column").rect();
    let configuration = window.get_by_label("Configuration detail column").rect();
    assert!(
        (operational.width() / configuration.width() - 1.35).abs() < 0.02,
        "wide 1.35:1 ratio drifted: {operational:?} {configuration:?}"
    );
    let show_more = window
        .get_by_role_and_label(Role::Button, "Show 2 more labels")
        .rect();
    let annot_button = window
        .get_by_role_and_label(Role::Button, "Show 2 annotations")
        .rect();
    for control in [
        "Tab Overview",
        "Tab YAML",
        "Tab Events",
        "Scale…",
        "Restart…",
        "Delete…",
        "Actions",
    ] {
        let rect = window.get_by_role_and_label(Role::Button, control).rect();
        assert_rect_within(window.rect(), rect, 1.0, control);
    }
    for node in window.query_all_by_role(Role::Label) {
        let Some(value) = node.value() else { continue };
        if !value.contains(':') || !value.contains('.') {
            continue;
        }
        // A rendered label chip is `key: value`; it must sit above the
        // buttons rather than overlap them.
        let chip = node.rect();
        assert!(
            chip.bottom() <= show_more.top() + 0.5,
            "chip '{value}' overlaps the show-more button: {chip:?} vs {show_more:?}"
        );
    }
    // Annotations button must be present and reachable (not pushed below the
    // fold), which is guaranteed when the chips wrap within the column width.
    window.get_by_role_and_label(Role::Button, "Show 2 annotations");
    let _ = annot_button;
    let actions = window.get_by_role_and_label(Role::Button, "Actions");
    assert!(!actions.accesskit_node().is_disabled());
    actions.click();
    let owner = window.rect();
    wide.run_steps(1);
    let copy_name = wide.get_by_role_and_label(Role::Button, "Copy name").rect();
    assert_rect_within(owner, copy_name, 1.0, "Copy name menu item");

    // Narrow 640x700: the same integrated selection collapses metadata.
    let mut narrow = harness(egui::vec2(640.0, 700.0));
    open_integrated_deployment(
        &mut narrow,
        detail_with(Some(projection(Vec::new()))),
        false,
    );
    let window = integrated_deployment_window(&narrow);
    window.get_by_label("PODS · 1");
    assert!(window.query_by_label("TEMPLATE").is_none());
    for control in [
        "Tab Overview",
        "More detail tabs",
        "Scale…",
        "Delete…",
        "More detail actions",
        "Show Deployment metadata",
    ] {
        let rect = window.get_by_role_and_label(Role::Button, control).rect();
        assert_rect_within(window.rect(), rect, 1.0, control);
    }
    window
        .get_by_role_and_label(Role::Button, "Show Deployment metadata")
        .click();
    narrow.run_steps(2);
    let window = integrated_deployment_window(&narrow);
    window.get_by_label("TEMPLATE");
    window.get_by_role_and_label(Role::Button, "Hide Deployment metadata");
}

#[test]
fn deployment_actual_1000_640_resize_preserves_identity_and_expansion() {
    let mut harness = harness(egui::vec2(1_300.0, 760.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [1_024.0, 620.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    let operational = window.get_by_label("Operational detail column").rect();
    let configuration = window.get_by_label("Configuration detail column").rect();
    assert!(
        (operational.width() / configuration.width() - 1.35).abs() < 0.02,
        "{operational:?} {configuration:?}"
    );

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    let rect = harness
        .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
        .rect();
    let target = rect.min + egui::vec2(664.0, 620.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let narrow = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    narrow
        .get_by_role_and_label(Role::Button, "Show Deployment metadata")
        .click();
    harness.run_steps(3);
    let narrow = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    assert!(
        narrow.get_by_label("PODS · 1").rect().top() < narrow.get_by_label("TEMPLATE").rect().top()
    );
    assert!(
        narrow.get_by_label("TEMPLATE").rect().top() < narrow.get_by_label("IDENTITY").rect().top()
    );
    narrow.get_by_label("UID · uid-deployment-checkout");

    let rect = narrow.rect();
    let target = rect.min + egui::vec2(1_024.0, 620.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let restored = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    restored.get_by_label("Operational detail column");
    restored.get_by_label("UID · uid-deployment-checkout");
    let rect = restored.rect();
    let target = rect.min + egui::vec2(664.0, 620.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let final_narrow =
        harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    final_narrow.get_by_role_and_label(Role::Button, "Hide Deployment metadata");
    final_narrow.get_by_label("TEMPLATE");
    final_narrow.get_by_label("UID · uid-deployment-checkout");
}

#[test]
fn deployment_tables_keep_last_columns_reachable_with_one_vertical_scroll_owner() {
    // Window chrome consumes 24 points, so these exercise a 760-point wide
    // body and a deliberately narrow 420-point body.
    for window_width in [784.0, 444.0] {
        let mut harness = harness(egui::vec2(1_120.0, 760.0));
        let detail = detail_with(Some(projection(Vec::new())));
        let identity = detail.identity.clone();
        open_detail(
            &mut harness,
            detail,
            Some(exact_relations(&identity)),
            [window_width, 620.0],
        );

        assert_eq!(
            harness
                .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
                .query_all_by_role(Role::ScrollView)
                .count(),
            1
        );

        for (table_label, last_column) in [
            ("Deployment rollout history table", "When"),
            ("Deployment Pods table", "Age"),
        ] {
            harness
                .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
                .get_by_role_and_label(Role::Table, table_label)
                .scroll_to_me();
            harness.run_steps(2);
            let table_center = harness
                .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
                .get_by_role_and_label(Role::Table, table_label)
                .rect()
                .center();
            harness.hover_at(table_center);
            harness.event(egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: egui::vec2(-400.0, 0.0),
                phase: egui::TouchPhase::Move,
                modifiers: egui::Modifiers::NONE,
            });
            harness.run_steps(2);

            let window =
                harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
            let table = window.get_by_role_and_label(Role::Table, table_label);
            let last_column = window.get_by_label(last_column);
            assert!(
                table.rect().intersects(last_column.rect()),
                "{table_label} must make its last column reachable at window width {window_width}: table={:?}, column={:?}",
                table.rect(),
                last_column.rect(),
            );
        }
    }
}

#[test]
fn deployment_pods_use_dense_body_rows_and_aligned_semantic_columns() {
    let mut harness = harness(egui::vec2(1_120.0, 760.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [1_024.0, 620.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    let text_run = |value: &str| {
        window
            .query_all_by_role(Role::TextRun)
            .find(|node| node.value().as_deref() == Some(value))
            .unwrap_or_else(|| {
                let available = window
                    .query_all_by_role(Role::TextRun)
                    .filter_map(|node| node.value())
                    .collect::<Vec<_>>();
                panic!("TextRun {value:?}; available: {available:?}")
            })
            .rect()
    };

    let ready_header = text_run("Ready");
    let ready = text_run("1/1");
    let restarts_header = text_run("Restarts");
    let restarts = text_run("2");
    let age_header = text_run("Age");
    let age = window
        .query_all_by_role(Role::TextRun)
        .filter(|node| node.value().as_deref() == Some("—"))
        .map(|node| node.rect())
        .find(|rect| {
            rect.top() > age_header.bottom() && (rect.right() - age_header.right()).abs() <= 12.0
        })
        .unwrap_or_else(|| {
            let dashes = window
                .query_all_by_role(Role::TextRun)
                .filter(|node| node.value().as_deref() == Some("—"))
                .map(|node| node.rect())
                .collect::<Vec<_>>();
            panic!("Pod Age value TextRun beneath {age_header:?}; dashes: {dashes:?}")
        });
    let status = text_run("● Running");
    let node = text_run("worker-a");

    for (header, value, column) in [
        (ready_header, ready, "Ready"),
        (restarts_header, restarts, "Restarts"),
        (age_header, age, "Age"),
    ] {
        assert!(
            (header.right() - value.right()).abs() <= 1.0,
            "{column} glyphs must share the semantic column's right edge: {header:?} {value:?}"
        );
    }
    for (left, right) in [
        (ready, status),
        (status, restarts),
        (restarts, node),
        (node, age),
    ] {
        assert!(
            left.right() <= right.left(),
            "Pod glyphs overlap: {left:?} {right:?}"
        );
    }

    let style = harness.ctx.style_of(egui::Theme::Dark);
    let expected_row_height = style
        .spacing
        .interact_size
        .y
        .max(style.text_styles[&egui::TextStyle::Body].size);
    assert!(
        (ready.top() - ready_header.top()) >= expected_row_height,
        "header/body baselines must preserve the dense Body row height"
    );
}

#[test]
fn deployment_layout_boundary_and_minimum_height_keep_shared_contract() {
    // The egui window contributes 24 points of chrome, so these exercise
    // Deployment body widths 759 and 760 exactly.
    for (outer_width, wide) in [(783.0, false), (784.0, true)] {
        let mut harness = harness(egui::vec2(1_120.0, 620.0));
        let detail = detail_with(Some(projection(Vec::new())));
        let identity = detail.identity.clone();
        open_detail(
            &mut harness,
            detail,
            Some(exact_relations(&identity)),
            [outer_width, 500.0],
        );
        let window =
            harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
        assert_eq!(window.query_by_label("TEMPLATE").is_some(), wide);
        assert_eq!(
            window
                .query_by_role_and_label(Role::Button, "Show Deployment metadata")
                .is_some(),
            !wide
        );
    }

    let mut harness = harness(egui::vec2(760.0, 520.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [672.0, 424.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    assert_eq!(window.query_all_by_role(Role::ScrollView).count(), 1);
    let footer = window
        .get_by_label("p pods · l logs · y yaml · e events · c copy name · Esc clear selection")
        .rect();
    assert!(window.rect().contains_rect(footer));
}

#[test]
fn deployment_accessibility_expands_labels_and_annotations_without_rollback() {
    let mut harness = harness(egui::vec2(1_240.0, 820.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [1_050.0, 700.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    assert!(window.query_by_label("team: payments").is_none());
    window
        .get_by_role_and_label(Role::Button, "Show 2 more labels")
        .click();
    harness.run_steps(2);
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window
        .get_by_role_and_label(Role::Button, "Show 2 annotations")
        .click();
    harness.run_steps(2);
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    // Labels render as chips; the chip exposes `key: value` as its accessible
    // name even though the visible text drops the colon.
    window.get_by_label("team: payments");
    window.get_by_label("meta.helm.sh/release-name: checkout-prod");
    window.get_by_role_and_label(Role::Button, "Hide 2 labels");
    window.get_by_role_and_label(Role::Button, "Hide annotations");
    assert!(window.query_by_label("Roll back…").is_none());
}

#[test]
fn deployment_commands_remain_shared_capability_and_authority_driven() {
    let mut harness = harness(egui::vec2(1_050.0, 700.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [900.0, 560.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    for action in ["Scale…", "Restart…", "Delete…", "Actions"] {
        window.get_by_role_and_label(Role::Button, action);
    }
    // Copy name no longer occupies the action row; it lives in the
    // Actions overflow menu.
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
    harness
        .get_by_role_and_label(Role::Button, "Actions")
        .click();
    harness.run_steps(1);
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    window.get_by_label("p pods · l logs · y yaml · e events · c copy name · Esc clear selection");
    assert!(window.query_by_label("Roll back…").is_none());

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
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    for action in ["Scale…", "Restart…", "Delete…"] {
        assert!(
            window
                .get_by_role_and_label(Role::Button, action)
                .accesskit_node()
                .is_disabled()
        );
    }
}

#[test]
fn deployment_scale_prefill_uses_typed_desired_replicas_over_summary() {
    let mut harness = harness(egui::vec2(1_050.0, 700.0));
    let mut detail = detail_with(Some(projection(Vec::new())));
    detail.sections = vec![DetailSection {
        title: "Overview".into(),
        rows: vec![DetailRow {
            label: "Status".into(),
            value: "99/99 ready".into(),
        }],
    }];
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [900.0, 560.0],
    );
    harness
        .get_by_role_and_label(Role::Button, "Scale…")
        .click();
    harness.run_steps(2);
    assert_eq!(
        harness
            .get_by_role_and_label(Role::TextInput, "Desired replicas")
            .value(),
        Some("3".into())
    );
}

#[test]
fn deployment_identity_is_stable_for_loading_failed_stale_and_gone() {
    let identity = deployment_identity(NAME);
    let mut harness = harness(egui::vec2(1_050.0, 700.0));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity.clone()));
    harness.run_steps(3);
    harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");

    harness.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("temporary failure")),
    );
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");

    harness.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Loaded(detail_with(None)),
    );
    harness.state_mut().feed.detail_authority.insert(
        identity.clone(),
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
    harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");

    harness.state_mut().feed.detail_authority.insert(
        identity,
        DetailAuthority {
            freshness: WindowFreshness::ReadyEmpty,
            lifecycle: DetailLifecycle::Gone,
        },
    );
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Window, "Deployment · payments / checkout")
        .get_by_label("This resource no longer exists");
}

#[test]
fn deployment_accessibility_snapshots_cover_wide_and_narrow_overview() {
    let mut wide = harness(egui::vec2(1_240.0, 820.0));
    let detail = detail_with(Some(projection(vec![condition(
        "Progressing",
        "True",
        Some("NewReplicaSetAvailable"),
    )])));
    let identity = detail.identity.clone();
    open_detail(
        &mut wide,
        detail,
        Some(exact_relations(&identity)),
        [1_050.0, 700.0],
    );
    snapshot_deployment_window(&wide, "wide_overview");

    let mut narrow = harness(egui::vec2(820.0, 620.0));
    let detail = detail_with(Some(projection(vec![condition(
        "Progressing",
        "True",
        Some("ReplicaSetUpdated"),
    )])));
    let identity = detail.identity.clone();
    open_detail(
        &mut narrow,
        detail,
        Some(exact_relations(&identity)),
        [759.0, 520.0],
    );
    snapshot_deployment_window(&narrow, "narrow_overview");
}

#[test]
fn deployment_overview_body_stays_clipped_above_the_fixed_footer() {
    // The Detail body must scroll/clip within its own rect so no overview
    // content paints underneath the shortcut footer, which is fixed at the
    // bottom of the detail pane.
    let mut harness = harness(egui::vec2(1_240.0, 820.0));
    let detail = detail_with(Some(projection(Vec::new())));
    let identity = detail.identity.clone();
    open_detail(
        &mut harness,
        detail,
        Some(exact_relations(&identity)),
        [1_050.0, 700.0],
    );
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");

    // The footer is fixed at the bottom of the detail window.
    let footer_before = window
        .get_by_label("p pods · l logs · y yaml · e events · c copy name · Esc clear selection")
        .rect();
    assert!(
        footer_before.top() > window.rect().bottom() - footer_before.height() - 12.0,
        "footer must sit at the bottom of the detail window: footer={footer_before:?} window={:?}",
        window.rect()
    );

    // The body is a scroll area, so tall overview content scrolls inside it
    // instead of painting under the footer.
    let scroll = window
        .query_by_role_and_label(Role::ScrollView, "Detail body")
        .expect("the Detail body must be a scroll view");
    let body_rect = scroll.rect();
    assert!(
        body_rect.bottom() <= footer_before.top() + 1.0,
        "the body scroll area must end at or above the footer: body={body_rect:?} footer={footer_before:?}"
    );

    // Scroll the body and confirm the footer stays fixed while content moves.
    scroll.scroll_to_me();
    harness.hover_at(body_rect.center());
    harness.event(egui::Event::MouseWheel {
        unit: egui::MouseWheelUnit::Point,
        delta: egui::vec2(0.0, 240.0),
        phase: egui::TouchPhase::Move,
        modifiers: egui::Modifiers::NONE,
    });
    harness.run_steps(2);
    let window = harness.get_by_role_and_label(Role::Window, "Deployment · payments / checkout");
    let footer_after = window
        .get_by_label("p pods · l logs · y yaml · e events · c copy name · Esc clear selection")
        .rect();
    assert!(
        (footer_after.center() - footer_before.center()).length() <= 1.0,
        "the footer must stay fixed while the body scrolls: before={footer_before:?} after={footer_after:?}"
    );

    // The operational and configuration columns must not overlap across the
    // 1.35:1 split.
    let operational = window.get_by_label("Operational detail column").rect();
    let configuration = window.get_by_label("Configuration detail column").rect();
    assert!(
        operational.right() <= configuration.left() + 1.0,
        "columns must not overlap across the split: operational={operational:?} configuration={configuration:?}"
    );
}
