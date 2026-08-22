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
    ui::{ConnectionState, UiShell},
    workspace::{LauncherItem, WorkspaceCommand},
};

const CONTEXT: &str = "dev-local";
const GIB: u64 = 1_073_741_824;

struct Fixture {
    shell: UiShell<()>,
    response: InfrastructureResponse,
    connection: ConnectionState,
    selected_context: Option<String>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            response: full_response(),
            connection: ConnectionState::Connected,
            selected_context: Some(CONTEXT.to_owned()),
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    fixture.shell.show_with_infrastructure(
        ui,
        fixture.connection,
        &[CONTEXT.to_owned()],
        &mut fixture.selected_context,
        Some(&fixture.response),
    );
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
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
    harness
        .get_by_role_and_label(Role::Window, "Overview")
        .get_by_label(
            "Connection stale · showing last successful update from 2026-08-21T01:05:00Z",
        );
}
