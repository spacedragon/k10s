use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    AttentionRow, BackendRevision, CapacityUsage, ClusterTotals, HealthLevel,
    InfrastructureResponse, MetricsAvailability, MetricsCondition, MetricsStatus, NodeRow,
    PersistentVolumeClaimRow, PersistentVolumeRow, StorageClassRow, StorageInventory,
    WorkloadHealth,
};
use k10s_ui::{
    ui::{ConnectionState, InfrastructureLoad, UiShell},
    workspace::{LauncherItem, WorkspaceCommand},
};

const CONTEXT: &str = "dev-local";
const GIB: u64 = 1_073_741_824;

struct Fixture {
    shell: UiShell<()>,
    response: InfrastructureResponse,
    show_response: bool,
    connection: ConnectionState,
    selected_context: Option<String>,
    load: InfrastructureLoad,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            response: full_response(),
            show_response: true,
            connection: ConnectionState::Connected,
            selected_context: Some(CONTEXT.to_owned()),
            load: InfrastructureLoad::Available,
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let response = fixture.show_response.then_some(&fixture.response);
    fixture.shell.show_with_infrastructure_load(
        ui,
        fixture.connection,
        &[CONTEXT.to_owned()],
        &mut fixture.selected_context,
        response,
        fixture.load,
    );
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
}

fn large_attention_response() -> InfrastructureResponse {
    let mut response = full_response();
    response.attention = (0..80)
        .map(|index| AttentionRow {
            namespace: Some("default".into()),
            kind: "Deployment".into(),
            name: format!("attention-row-{index:03}"),
            status: "Degraded".into(),
            reason: format!("attention reason {index:03}"),
        })
        .collect();
    response
}

#[test]
fn launcher_inventory_badges_share_loading_zero_warning_and_unavailable_contract() {
    let mut harness = harness();

    harness.get_by_label("6 Workloads resources");
    harness.get_by_label("4 warning Events resources");

    harness.state_mut().response.launcher = Default::default();
    harness.run_steps(4);
    for label in [
        "0 Events resources",
        "0 Workloads resources",
        "0 Network resources",
        "0 Config resources",
        "0 Storage resources",
        "0 Access resources",
    ] {
        harness.get_by_label(label);
    }

    harness.state_mut().show_response = false;
    harness.state_mut().load = InfrastructureLoad::Loading;
    harness.run_steps(4);
    for label in [
        "Events",
        "Workloads",
        "Network",
        "Config",
        "Storage",
        "Access",
    ] {
        harness.get_by_label(&format!("Loading {label} inventory"));
    }

    harness.state_mut().load = InfrastructureLoad::Unavailable;
    harness.run_steps(4);
    for label in [
        "Events",
        "Workloads",
        "Network",
        "Config",
        "Storage",
        "Access",
    ] {
        harness.get_by_label(&format!("{label} inventory unavailable"));
    }
}

fn full_response() -> InfrastructureResponse {
    InfrastructureResponse {
        context: CONTEXT.into(),
        revision: BackendRevision::new(1_042),
        generated_at: "2026-08-21T01:05:00Z".into(),
        totals: ClusterTotals {
            nodes: 2,
            pods: 22,
            workloads: 6,
            persistent_storage_bytes: 60 * GIB,
        },
        launcher: k10s_protocol::LauncherCounts {
            events_warning: 4,
            workloads: 6,
            network: 4,
            config: 2,
            storage: 3,
            access: 4,
        },
        cluster_cpu: CapacityUsage::new(Some(3_200), Some(8_000)),
        cluster_memory: CapacityUsage::new(Some(12 * GIB), Some(32 * GIB)),
        pod_capacity: CapacityUsage::new(Some(22), Some(220)),
        metrics: MetricsStatus {
            availability: MetricsAvailability::Available,
            condition: MetricsCondition::Fresh,
            source: "metrics.k8s.io".into(),
            source_updated_at: Some("2026-08-21T01:04:30Z".into()),
            detail: "All node metrics are current".into(),
        },
        workload_health: vec![
            WorkloadHealth {
                level: HealthLevel::Healthy,
                label: "Healthy".into(),
                count: 4,
            },
            WorkloadHealth {
                level: HealthLevel::Warning,
                label: "Pending".into(),
                count: 1,
            },
            WorkloadHealth {
                level: HealthLevel::Failure,
                label: "Unhealthy".into(),
                count: 1,
            },
        ],
        attention: vec![AttentionRow {
            namespace: Some("default".into()),
            kind: "Deployment".into(),
            name: "checkout".into(),
            status: "Degraded".into(),
            reason: "1 replica unavailable".into(),
        }],
        nodes: vec![
            NodeRow {
                name: "dev-node-1".into(),
                status: "Ready".into(),
                roles: vec!["control-plane".into()],
                kubernetes_version: "v1.34.0".into(),
                cpu: CapacityUsage::new(Some(2_200), Some(4_000)),
                memory: CapacityUsage::new(Some(8 * GIB), Some(16 * GIB)),
                pods: CapacityUsage::new(Some(12), Some(110)),
                age: "14d".into(),
            },
            NodeRow {
                name: "dev-node-2".into(),
                status: "Not Ready".into(),
                roles: vec!["worker".into()],
                kubernetes_version: "v1.34.0".into(),
                cpu: CapacityUsage::new(Some(1_000), Some(4_000)),
                memory: CapacityUsage::new(Some(4 * GIB), Some(16 * GIB)),
                pods: CapacityUsage::new(Some(10), Some(110)),
                age: "8d".into(),
            },
        ],
        storage: StorageInventory {
            persistent_volume_claims: vec![PersistentVolumeClaimRow {
                namespace: "default".into(),
                name: "postgres-data".into(),
                status: "Bound".into(),
                capacity: "20 GiB".into(),
                access_modes: vec!["ReadWriteOnce".into()],
                storage_class: "fast-ssd".into(),
                bound_volume: "pv-postgres-data".into(),
                age: "12d".into(),
            }],
            persistent_volumes: vec![PersistentVolumeRow {
                name: "pv-postgres-data".into(),
                status: "Bound".into(),
                capacity: "20 GiB".into(),
                access_modes: vec!["ReadWriteOnce".into()],
                storage_class: "fast-ssd".into(),
                bound_claim: "default/postgres-data".into(),
                reclaim_policy: "Retain".into(),
                age: "12d".into(),
            }],
            storage_classes: vec![StorageClassRow {
                name: "fast-ssd".into(),
                provisioner: "csi.example.com".into(),
                reclaim_policy: "Delete".into(),
                volume_binding_mode: "WaitForFirstConsumer".into(),
                age: "90d".into(),
            }],
        },
    }
}

#[test]
fn overview_renders_totals_capacity_health_attention_and_refresh_timestamp() {
    let harness = harness();
    let overview = harness.get_by_role_and_label(Role::Window, "Overview");

    for label in [
        "2 nodes",
        "22 pods",
        "6 workloads",
        "60 GiB persistent storage",
        "● Healthy 4",
        "● Pending 1",
        "● Unhealthy 1",
        "Needs attention",
        "default",
        "Deployment",
        "checkout",
        "Degraded",
        "1 replica unavailable",
        "Last updated: 2026-08-21T01:05:00Z",
        "Metrics: Available",
        "Source: metrics.k8s.io",
        "Source updated: 2026-08-21T01:04:30Z",
    ] {
        overview.get_by_label(label);
    }
    overview.get_by_role_and_label(Role::Button, "Refresh overview");

    for progress_text in [
        "CPU 3.2 / 8.0 cores",
        "Memory 12.0 / 32.0 GiB",
        "Pod capacity 22 / 220 pods",
    ] {
        overview.get_by_role_and_label(Role::ProgressIndicator, progress_text);
    }
}

#[test]
fn overview_attention_rows_scroll_inside_the_window() {
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_280.0, 800.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default());

    let mut snapshot = harness.state().shell.workspace().snapshot();
    let overview = snapshot
        .windows
        .iter_mut()
        .find(|window| window.title == "Overview")
        .expect("the default workspace contains Overview");
    overview.geometry.position = [32.0, 24.0];
    overview.geometry.size = [920.0, 620.0];
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::RestoreSnapshot(snapshot));
    harness.state_mut().response = large_attention_response();
    harness.state_mut().response.metrics.detail =
        "Wrapping metrics detail remains inside Overview. ".repeat(6);
    harness.run_steps(4);

    let (overview_before, summary_before, health_before) = {
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        let overview_before = overview.rect();
        assert!(overview_before.left() >= 0.0 && overview_before.top() >= 0.0);
        assert!(overview_before.right() <= 1_280.0 && overview_before.bottom() <= 800.0);
        assert!(
            overview_before.width() <= 923.0 && overview_before.height() <= 621.0,
            "restored 920x620 Overview grew to {overview_before:?}"
        );

        let late = overview.get_by_label("attention-row-079");
        assert!(
            !overview_before.intersects(late.rect()),
            "the late attention row should initially be clipped"
        );
        let metrics_detail = overview.get_by_label(&harness.state().response.metrics.detail);
        assert!(
            overview_before.contains_rect(metrics_detail.rect()),
            "the wrapping metrics footer should remain inside Overview"
        );
        (
            overview_before,
            overview.get_by_label("2 nodes").rect(),
            overview.get_by_label("Workload health").rect(),
        )
    };

    let mut late_visible = false;
    for _ in 0..24 {
        {
            let overview = harness.get_by_role_and_label(Role::Window, "Overview");
            overview.get_by_label("attention-row-000").scroll_down();
        }
        harness.run_steps(2);
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        let late = overview.get_by_label("attention-row-079");
        if overview.rect().intersects(late.rect()) {
            late_visible = true;
            break;
        }
    }
    assert!(
        late_visible,
        "vertical scrolling should reveal the late attention row"
    );

    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    let overview_after = overview.rect();
    let summary_after = overview.get_by_label("2 nodes").rect();
    let health_after = overview.get_by_label("Workload health").rect();
    for (before, after, label) in [
        (overview_before, overview_after, "Overview"),
        (summary_before, summary_after, "summary"),
        (health_before, health_after, "Workload health"),
    ] {
        assert!(
            (before.min - after.min).length() <= 1.0 && (before.max - after.max).length() <= 1.0,
            "{label} rectangle moved while the attention list scrolled"
        );
    }
    overview.get_by_role_and_label(Role::Button, "Refresh overview");
    overview.get_by_label("Metrics: Available");
}

#[test]
fn compact_overview_keeps_large_attention_content_reachable() {
    let mut harness = Harness::builder()
        .with_size(egui::vec2(720.0, 420.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default());

    let mut snapshot = harness.state().shell.workspace().snapshot();
    let overview = snapshot
        .windows
        .iter_mut()
        .find(|window| window.title == "Overview")
        .expect("the default workspace contains Overview");
    overview.geometry.position = [0.0, 0.0];
    overview.geometry.size = [480.0, 320.0];
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::RestoreSnapshot(snapshot));
    harness.state_mut().response = large_attention_response();
    harness.run_steps(4);

    let overview_before = harness
        .get_by_role_and_label(Role::Window, "Overview")
        .rect();
    assert!(overview_before.left() >= 0.0 && overview_before.top() >= 0.0);
    assert!(overview_before.right() <= 720.0 && overview_before.bottom() <= 420.0);
    assert!(
        overview_before.width() <= 483.0 && overview_before.height() <= 325.0,
        "restored 480x320 Overview grew to {overview_before:?}"
    );
    let summary_before = harness.get_by_label("2 nodes").rect();
    assert!(overview_before.intersects(summary_before));
    for label in ["Needs attention", "Metrics: Available"] {
        let rect = harness.get_by_label(label).rect();
        assert!(
            !overview_before.intersects(rect),
            "{label} should initially be clipped, but was at {rect:?}"
        );
    }

    for wanted in ["Needs attention", "Metrics: Available"] {
        let mut visible = false;
        for _ in 0..64 {
            {
                let overview = harness.get_by_role_and_label(Role::Window, "Overview");
                let target = [
                    "2 nodes",
                    "Cluster capacity",
                    "Workload health",
                    "Needs attention",
                    "Metrics: Available",
                ]
                .into_iter()
                .map(|label| overview.get_by_label(label))
                .find(|node| overview.rect().intersects(node.rect()))
                .expect("outer fallback keeps fixed content scrollable");
                let target_center = target.rect().center();
                harness
                    .input_mut()
                    .events
                    .push(egui::Event::PointerMoved(target_center));
                harness.input_mut().events.push(egui::Event::MouseWheel {
                    unit: egui::MouseWheelUnit::Point,
                    delta: egui::vec2(0.0, -100.0),
                    phase: egui::TouchPhase::Move,
                    modifiers: egui::Modifiers::NONE,
                });
            }
            harness.run_steps(2);
            let overview = harness.get_by_role_and_label(Role::Window, "Overview");
            if overview
                .rect()
                .intersects(overview.get_by_label(wanted).rect())
            {
                visible = true;
                break;
            }
        }
        assert!(visible, "outer scrolling should reveal {wanted}");
        assert_eq!(
            overview_before,
            harness
                .get_by_role_and_label(Role::Window, "Overview")
                .rect(),
            "outer scrolling changed the Overview rectangle"
        );
    }

    let mut first_row_visible = false;
    for _ in 0..64 {
        {
            let overview = harness.get_by_role_and_label(Role::Window, "Overview");
            let footer = overview.get_by_label("Metrics: Available");
            let footer_center = footer.rect().center();
            harness
                .input_mut()
                .events
                .push(egui::Event::PointerMoved(footer_center));
            harness.input_mut().events.push(egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: egui::vec2(0.0, 100.0),
                phase: egui::TouchPhase::Move,
                modifiers: egui::Modifiers::NONE,
            });
        }
        harness.run_steps(2);
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        let heading = overview.get_by_label("Needs attention").rect();
        let first = overview.get_by_label("attention-row-000").rect();
        if overview.rect().contains(first.center())
            && first.center().y >= heading.bottom() + 4.0
            && first.center().y <= heading.bottom() + 72.0
        {
            first_row_visible = true;
            break;
        }
    }
    assert!(
        first_row_visible,
        "outer scrolling should reveal the attention viewport"
    );
    {
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        let heading_center = overview.get_by_label("Workload health").rect().center();
        harness
            .input_mut()
            .events
            .push(egui::Event::PointerMoved(heading_center));
        harness.input_mut().events.push(egui::Event::MouseWheel {
            unit: egui::MouseWheelUnit::Point,
            delta: egui::vec2(0.0, -100.0),
            phase: egui::TouchPhase::Move,
            modifiers: egui::Modifiers::NONE,
        });
    }
    harness.run_steps(2);

    let mut late_row_visible = false;
    for _ in 0..64 {
        {
            let overview = harness.get_by_role_and_label(Role::Window, "Overview");
            let heading = overview.get_by_label("Needs attention").rect();
            let row_labels = (0..80)
                .map(|index| format!("attention-row-{index:03}"))
                .collect::<Vec<_>>();
            let visible_row = row_labels
                .iter()
                .map(|label| overview.get_by_label(label))
                .find(|row| {
                    let rect = row.rect();
                    overview.rect().contains(rect.center())
                        && rect.center().y >= heading.bottom() + 4.0
                        && rect.center().y <= heading.bottom() + 72.0
                })
                .expect("inner scroll events must target a visible attention row");
            visible_row.hover();
            visible_row.scroll_down();
        }
        harness.run_steps(2);
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        let heading = overview.get_by_label("Needs attention").rect();
        let late = overview.get_by_label("attention-row-079").rect();
        if overview.rect().contains(late.center())
            && late.center().y >= heading.bottom() + 4.0
            && late.center().y <= heading.bottom() + 72.0
        {
            late_row_visible = true;
            break;
        }
    }
    assert!(
        late_row_visible,
        "inner scrolling should reveal the late attention row"
    );
    assert_eq!(
        overview_before,
        harness
            .get_by_role_and_label(Role::Window, "Overview")
            .rect(),
        "nested scrolling changed the Overview rectangle"
    );
}

#[test]
fn nodes_table_is_searchable_sortable_and_progress_always_has_numeric_text() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    harness.run();
    let nodes = harness.get_by_role_and_label(Role::Window, "Nodes");

    nodes.get_by_label("Ready 1");
    nodes.get_by_label("Not Ready 1");
    nodes.get_by_role_and_label(Role::TextInput, "Search nodes");
    for heading in [
        "Name",
        "Status",
        "Roles",
        "Kubernetes version",
        "CPU",
        "Memory",
        "Pods",
        "Age",
    ] {
        nodes.get_by_label(heading);
    }
    for column in [
        "name",
        "status",
        "roles",
        "Kubernetes version",
        "CPU",
        "memory",
        "pods",
        "age",
    ] {
        let label = format!("Sort nodes by {column}");
        nodes.get_by_role_and_label(Role::Button, &label);
    }
    for progress_text in ["2.2 / 4.0 cores", "8.0 / 16.0 GiB", "12 / 110 pods"] {
        nodes.get_by_role_and_label(Role::ProgressIndicator, progress_text);
    }

    nodes
        .get_by_role_and_label(Role::Button, "Sort nodes by age")
        .click();
    harness.run();
    let nodes = harness.get_by_role_and_label(Role::Window, "Nodes");
    assert!(
        nodes.get_by_label("dev-node-2").rect().top()
            < nodes.get_by_label("dev-node-1").rect().top(),
        "8d must sort before 14d when age is ascending"
    );

    nodes
        .get_by_role_and_label(Role::TextInput, "Search nodes")
        .focus();
    harness.run();
    harness
        .get_by_role_and_label(Role::Window, "Nodes")
        .get_by_role_and_label(Role::TextInput, "Search nodes")
        .type_text("node-2");
    harness.run();
    let nodes = harness.get_by_role_and_label(Role::Window, "Nodes");
    nodes.get_by_label("dev-node-2");
    assert!(nodes.query_by_label("dev-node-1").is_none());

    nodes
        .get_by_role_and_label(Role::Button, "Sort nodes by status")
        .click();
}

#[test]
fn node_filter_empty_state_can_be_cleared() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    harness.run();
    harness
        .get_by_role_and_label(Role::Window, "Nodes")
        .get_by_role_and_label(Role::TextInput, "Search nodes")
        .focus();
    harness.run();
    harness
        .get_by_role_and_label(Role::Window, "Nodes")
        .get_by_role_and_label(Role::TextInput, "Search nodes")
        .type_text("does-not-exist");
    harness.run();

    let nodes = harness.get_by_role_and_label(Role::Window, "Nodes");
    nodes.get_by_label("No resources match these filters");
    nodes
        .get_by_role_and_label(Role::Button, "Clear filters")
        .click();
    harness.run();
    let nodes = harness.get_by_role_and_label(Role::Window, "Nodes");
    nodes.get_by_label("dev-node-1");
    nodes.get_by_label("dev-node-2");
}

#[test]
fn storage_tabs_render_only_protocol_columns_applicable_to_each_kind() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Storage,
        ));
    harness.run();

    let storage = harness.get_by_role_and_label(Role::Window, "Storage");
    for tab in [
        "PersistentVolumeClaims",
        "PersistentVolumes",
        "StorageClasses",
    ] {
        storage.get_by_role_and_label(Role::Button, tab);
    }
    for label in [
        "Namespace",
        "Name",
        "Status",
        "Capacity",
        "Access modes",
        "Class",
        "Bound volume",
        "Age",
        "default",
        "postgres-data",
        "20 GiB",
        "ReadWriteOnce",
        "fast-ssd",
        "pv-postgres-data",
        "12d",
    ] {
        storage.get_by_label(label);
    }

    storage
        .get_by_role_and_label(Role::Button, "PersistentVolumes")
        .click();
    harness.run();
    let storage = harness.get_by_role_and_label(Role::Window, "Storage");
    for label in [
        "Bound claim",
        "Reclaim policy",
        "default/postgres-data",
        "Retain",
    ] {
        storage.get_by_label(label);
    }

    storage
        .get_by_role_and_label(Role::Button, "StorageClasses")
        .click();
    harness.run();
    let storage = harness.get_by_role_and_label(Role::Window, "Storage");
    for label in [
        "Provisioner",
        "Reclaim policy",
        "Binding mode",
        "csi.example.com",
        "Delete",
        "WaitForFirstConsumer",
        "90d",
    ] {
        storage.get_by_label(label);
    }
}

#[test]
fn metrics_states_and_connection_staleness_are_textual_and_missing_is_never_zero() {
    let mut harness = harness();
    for (availability, condition, detail, expected) in [
        (
            MetricsAvailability::Available,
            MetricsCondition::Fresh,
            "All node metrics are current",
            "Metrics: Available",
        ),
        (
            MetricsAvailability::Partial,
            MetricsCondition::Partial,
            "Memory is missing for dev-node-2",
            "Metrics: Partial",
        ),
        (
            MetricsAvailability::Unavailable,
            MetricsCondition::Forbidden,
            "Forbidden: cannot list nodes.metrics.k8s.io",
            "Metrics: Unavailable · RBAC forbidden",
        ),
        (
            MetricsAvailability::Unavailable,
            MetricsCondition::Stale,
            "Last sample is outside the freshness window",
            "Metrics: Unavailable · stale",
        ),
    ] {
        harness.state_mut().response.metrics.availability = availability;
        harness.state_mut().response.metrics.condition = condition;
        harness.state_mut().response.metrics.detail = detail.into();
        harness.run();
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        overview.get_by_label(expected);
        overview.get_by_label(detail);
    }

    harness.state_mut().response.metrics.availability = MetricsAvailability::Partial;
    harness.state_mut().response.metrics.condition = MetricsCondition::Partial;
    harness.state_mut().response.cluster_memory = CapacityUsage::new(None, Some(32 * GIB));
    harness.run();
    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    let missing = overview.get_by_label("Memory —");
    assert!(overview.query_by_label("Memory 0").is_none());
    assert_eq!(
        overview
            .query_all_by_role(Role::ProgressIndicator)
            .filter(|node| node
                .accesskit_node()
                .label()
                .is_some_and(|label| label.contains("Memory")))
            .count(),
        0,
        "missing memory must not be represented as a zero-filled progress bar"
    );
    missing.hover();
    harness.run();
    harness.get_by_label("Metric was not reported; — does not mean zero.");

    harness.state_mut().connection = ConnectionState::Connecting;
    harness.run();
    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    overview.get_by_label(
        "Connection stale · showing last successful update from 2026-08-21T01:05:00Z",
    );
    overview.get_by_role_and_label(Role::Button, "Retry connection");

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Storage,
        ));
    harness.run();
    for window in ["Nodes", "Storage"] {
        harness
            .get_by_role_and_label(Role::Window, window)
            .get_by_label(
                "Connection stale · showing last successful update from 2026-08-21T01:05:00Z",
            );
    }
}

#[test]
fn loading_and_empty_storage_states_keep_window_chrome_explanatory() {
    let mut harness = harness();
    harness.state_mut().show_response = false;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Storage,
        ));
    harness.step();
    for (window, label) in [
        ("Overview", "Loading cluster overview"),
        ("Nodes", "Loading node inventory"),
        ("Storage", "Loading storage inventory"),
    ] {
        let window = harness.get_by_role_and_label(Role::Window, window);
        window.get_by_role(Role::ProgressIndicator);
        window.get_by_label(label);
    }

    harness.state_mut().show_response = true;
    harness.state_mut().response.storage = StorageInventory::default();
    harness.run();
    let storage = harness.get_by_role_and_label(Role::Window, "Storage");
    storage.get_by_label("No PersistentVolumeClaims in this namespace");
    storage
        .get_by_role_and_label(Role::Button, "PersistentVolumes")
        .click();
    harness.run();
    harness
        .get_by_role_and_label(Role::Window, "Storage")
        .get_by_label("No PersistentVolumes");
    harness
        .get_by_role_and_label(Role::Window, "Storage")
        .get_by_role_and_label(Role::Button, "StorageClasses")
        .click();
    harness.run();
    harness
        .get_by_role_and_label(Role::Window, "Storage")
        .get_by_label("No StorageClasses");
}

#[test]
fn unavailable_infrastructure_has_safe_copy_refresh_and_no_spinner() {
    let mut harness = harness();
    harness.state_mut().show_response = false;
    harness.state_mut().load = InfrastructureLoad::Unavailable;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Storage,
        ));
    harness.step();

    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    overview.get_by_label("Cluster overview is not available in this build");
    overview.get_by_role_and_label(Role::Button, "Refresh overview");
    assert!(overview.query_by_role(Role::ProgressIndicator).is_none());
    assert!(
        overview
            .query_by_label("Loading cluster overview")
            .is_none()
    );
    for name in ["Nodes", "Storage"] {
        let window = harness.get_by_role_and_label(Role::Window, name);
        window.get_by_label("Cluster infrastructure is not available in this build");
        assert!(window.query_by_role(Role::ProgressIndicator).is_none());
    }
}
