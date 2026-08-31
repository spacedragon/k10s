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
    BackendRevision, ContainerImageProjection, DeploymentProjection, EventRow, GroupVersionKind,
    PodProjection, RelatedGroup, ReplicaSetProjection, ResourceCapabilities,
    ResourceConditionProjection, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceProjection, ResourceRelationsResponse,
};
use k10s_ui::{
    ui::{
        ConnectionState, DetailAuthority, DetailLifecycle, PrimaryDetailState, RelationState,
        ResourceFeed, SafeUiError, UiShell, WindowFreshness,
    },
    workspace::{WindowGeom, WindowKind, WorkspaceCommand},
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
    Harness::builder()
        .with_size(size)
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
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
        "Revision 4 · checkout-4",
        "Images · api=ghcr.io/acme/checkout:v4",
        "Replicas · 2/3 ready",
        "TEMPLATE",
        "Image (api) · ghcr.io/acme/checkout:v4",
        "Selector · app=checkout",
        "MANAGED BY",
        "Manager · Helm",
        "Helm release · checkout-prod",
        "LABELS · 6",
        "IDENTITY",
        "Context · deployment-test",
    ] {
        window.get_by_label(label);
    }
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
        "Image (api) · —",
        "Selector · —",
        "Manager · —",
        "Created · —",
    ] {
        window.get_by_label(label);
    }
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
        .get_by_label("Recent rollout events unavailable");
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
        .get_by_label("Shortcuts: p pods · y yaml · e events · c copy name · Esc restore/close")
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
    assert!(window.query_by_label("team · payments").is_none());
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
    window.get_by_label("team · payments");
    window.get_by_label("meta.helm.sh/release-name · checkout-prod");
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
    for action in ["Scale…", "Restart…", "Delete…", "Copy name"] {
        window.get_by_role_and_label(Role::Button, action);
    }
    window.get_by_label("Shortcuts: p pods · y yaml · e events · c copy name · Esc restore/close");
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
