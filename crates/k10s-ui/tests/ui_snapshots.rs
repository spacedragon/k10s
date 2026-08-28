//! Stable accessibility-tree snapshots of the approved screen set.
//!
//! Each snapshot is a deterministic text dump of the AccessKit tree
//! (roles, labels, and values in widget order) rendered at a fixed
//! viewport and density. Compared against the checked-in files under
//! `tests/snapshots/`; regenerate with `K10S_UPDATE_SNAPSHOTS=1 cargo
//! test -p k10s-ui --test ui_snapshots`.
//!
//! Textual tree snapshots are deliberately preferred over pixel PNGs:
//! they are byte-stable across renderers and CI runners, they fail loudly
//! when any accessible name regresses, and they double as the AccessKit
//! coverage the plan requires. Pixel snapshots can revisit this once the
//! project pins one software renderer for all environments.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, CapacityUsage, ClusterTotals, DetailRow, DetailSection, EventRow,
    GroupVersionKind, InfrastructureResponse, MetricsAvailability, MetricsCondition, MetricsStatus,
    NodeRow, ResourceCapabilities, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceTypeEntry, StreamTarget,
};
use k10s_ui::{
    ui::{ConnectionState, ResourceFeed, UiShell, WindowFreshness},
    workspace::{LauncherItem, WindowGeom, WindowId, WorkloadKind as W, WorkspaceCommand},
};

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
    response: Option<InfrastructureResponse>,
    selected_context: String,
    connection: ConnectionState,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
            response: None,
            selected_context: CONTEXT.to_owned(),
            connection: ConnectionState::Connected,
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let contexts = [CONTEXT.to_owned()];
    let mut selected = Some(fixture.selected_context.clone());
    fixture.shell.show_with_resources(
        ui,
        fixture.connection,
        &contexts,
        &mut selected,
        fixture.response.as_ref(),
        &fixture.feed,
    );
    if let Some(selected) = selected {
        fixture.selected_context = selected;
    }
}

fn harness() -> Harness<'static, Fixture> {
    harness_with_size(egui::vec2(1_280.0, 800.0))
}

fn harness_with_size(size: egui::Vec2) -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(size)
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
}

/// Dump the accessibility tree as stable text: role plus accessible
/// label/value per node, indented by depth. Widget order is fully
/// determined by the frame's code path, so the dump is reproducible.
fn snapshot_tree(harness: &Harness<Fixture>, name: &str) {
    fn walk(node: egui_kittest::Node<'_>, depth: usize, out: &mut String) {
        let accessible = node.accesskit_node();
        for _ in 0..depth {
            out.push_str("  ");
        }
        out.push_str(&format!("{:?}", accessible.role()));
        if let Some(label) = accessible.label() {
            out.push_str(&format!(" label={label:?}"));
        }
        if let Some(value) = accessible.value() {
            out.push_str(&format!(" value={value:?}"));
        }
        out.push('\n');
        for child in node.children() {
            walk(child, depth + 1, out);
        }
    }

    let mut actual = String::new();
    walk(harness.root(), 0, &mut actual);

    let dir = std::path::Path::new("tests").join("snapshots");
    let path = dir.join(format!("{name}.txt"));
    if std::env::var_os("K10S_UPDATE_SNAPSHOTS").is_some() {
        std::fs::create_dir_all(&dir).expect("snapshot directory");
        std::fs::write(&path, actual).expect("snapshot write");
        return;
    }
    // Normal mode never writes: a missing or misnamed baseline must fail
    // loudly instead of being silently recreated in CI.
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "snapshot baseline {} is missing ({error}); regenerate intentionally \
             with K10S_UPDATE_SNAPSHOTS=1 cargo test -p k10s-ui --test ui_snapshots",
            path.display()
        )
    });
    let expected = expected.replace("\r\n", "\n");
    assert!(
        expected == actual,
        "accessibility snapshot {name} drifted.\n\
         If the change is intentional, regenerate with \
         K10S_UPDATE_SNAPSHOTS=1 cargo test -p k10s-ui --test ui_snapshots\n\
         --- diff ---\n{}",
        diff_summary(&expected, &actual)
    );
}

fn diff_summary(expected: &str, actual: &str) -> String {
    let mut differences = String::new();
    for (index, (expected_line, actual_line)) in expected
        .lines()
        .zip(actual.lines())
        .enumerate()
        .filter(|(_, (expected_line, actual_line))| expected_line != actual_line)
        .take(10)
    {
        differences.push_str(&format!(
            "{index}: -{expected_line:?}\n{index}: +{actual_line:?}\n"
        ));
    }
    if expected.lines().count() != actual.lines().count() {
        differences.push_str(&format!(
            "line count {} -> {}\n",
            expected.lines().count(),
            actual.lines().count()
        ));
    }
    differences
}

// ---------------------------------------------------------------------------
// Fixtures shared with the approved screens
// ---------------------------------------------------------------------------

fn list_row(group: &str, version: &str, kind: &str, name: &str, summary: &str) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: group.to_owned(),
                version: version.to_owned(),
                kind: kind.to_owned(),
            },
            namespace: Some("default".to_owned()),
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

fn pod_detail(name: &str) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: String::new(),
                version: "v1".to_owned(),
                kind: "Pod".to_owned(),
            },
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-pod-default-{name}"),
        },
        revision: BackendRevision::new(1_011),
        created_at: "2026-08-21T00:50:10Z".to_owned(),
        owner_references: Vec::new(),
        sections: vec![DetailSection {
            title: "Overview".to_owned(),
            rows: vec![DetailRow {
                label: "Status".to_owned(),
                value: "Running".to_owned(),
            }],
        }],
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
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n"),
        projection: None,
    }
}

fn infrastructure_response(condition: MetricsCondition, detail: &str) -> InfrastructureResponse {
    const GIB: u64 = 1_073_741_824;
    InfrastructureResponse {
        context: CONTEXT.into(),
        revision: BackendRevision::new(1_042),
        generated_at: "2026-08-21T01:05:00Z".into(),
        totals: ClusterTotals {
            nodes: 2,
            pods: 3,
            workloads: 2,
            persistent_storage_bytes: 20 * GIB,
        },
        launcher: Default::default(),
        cluster_cpu: CapacityUsage::new(Some(500), Some(4_000)),
        cluster_memory: CapacityUsage::new(Some(2 * GIB), Some(16 * GIB)),
        pod_capacity: CapacityUsage::new(Some(3), Some(110)),
        metrics: MetricsStatus {
            availability: MetricsAvailability::Unavailable,
            condition,
            source: "metrics.k8s.io".into(),
            source_updated_at: None,
            detail: detail.into(),
        },
        workload_health: Vec::new(),
        attention: Vec::new(),
        nodes: vec![
            NodeRow {
                name: "dev-node-1".into(),
                status: "Ready".into(),
                roles: vec!["control-plane".into()],
                kubernetes_version: "v1.34.0".into(),
                cpu: CapacityUsage::new(Some(500), Some(4_000)),
                memory: CapacityUsage::new(Some(2 * GIB), Some(16 * GIB)),
                pods: CapacityUsage::new(Some(2), Some(110)),
                age: "14d".into(),
            },
            NodeRow {
                name: "dev-node-2".into(),
                status: "Not Ready".into(),
                roles: vec!["worker".into()],
                kubernetes_version: "v1.34.0".into(),
                cpu: CapacityUsage::new(None, Some(4_000)),
                memory: CapacityUsage::new(None, Some(16 * GIB)),
                pods: CapacityUsage::new(Some(1), Some(110)),
                age: "8d".into(),
            },
        ],
        storage: Default::default(),
    }
}

fn workload_id(fixture: &Fixture, kind: W) -> WindowId {
    fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == k10s_ui::workspace::WindowKind::Workload(kind))
        .expect("workload window is open")
        .id
}

fn run_steps(harness: &mut Harness<'static, Fixture>) {
    harness.run_steps(4);
}

// ---------------------------------------------------------------------------
// The approved screen set
// ---------------------------------------------------------------------------

#[test]
fn overview_while_loading() {
    let mut harness = harness();
    run_steps(&mut harness);
    snapshot_tree(&harness, "overview_loading");
}

#[test]
fn overview_with_forbidden_metrics_and_missing_values() {
    let mut harness = harness();
    harness.state_mut().response = Some(infrastructure_response(
        MetricsCondition::Forbidden,
        "Forbidden: cannot list nodes.metrics.k8s.io",
    ));
    run_steps(&mut harness);
    snapshot_tree(&harness, "overview_forbidden_metrics");
}

#[test]
fn nodes_inventory() {
    let mut harness = harness();
    harness.state_mut().response = Some(infrastructure_response(
        MetricsCondition::Stale,
        "Last sample is outside the freshness window",
    ));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    run_steps(&mut harness);
    snapshot_tree(&harness, "nodes_inventory");
}

#[test]
fn deployments_list_window() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        W::Deployments,
        vec![
            list_row("apps", "v1", "Deployment", "api-server", "2/2 ready"),
            list_row("apps", "v1", "Deployment", "web-frontend", "20/20 ready"),
        ],
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(W::Deployments),
        ));
    run_steps(&mut harness);
    snapshot_tree(&harness, "deployments_list");
}

#[test]
fn custom_resources_picker() {
    let mut harness = harness();
    harness.state_mut().feed.types = vec![ResourceTypeEntry {
        gvk: GroupVersionKind {
            group: "monitoring.example.com".to_owned(),
            version: "v1".to_owned(),
            kind: "Dashboard".to_owned(),
        },
        namespaced: true,
    }];
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(W::CustomResources),
        ));
    run_steps(&mut harness);
    snapshot_tree(&harness, "custom_resources_picker");
}

#[test]
fn pod_detail_overview_with_resolved_response() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        W::Pods,
        vec![list_row("", "v1", "Pod", "db-postgres-0", "Running")],
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(W::Pods),
        ));
    run_steps(&mut harness);
    let id = workload_id(harness.state(), W::Pods);
    // Give the integrated detail pane nearly the whole window so nothing
    // is clipped out of the snapshot.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 1.0));
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    run_steps(&mut harness);
    let identity = harness
        .state()
        .shell
        .workspace()
        .resource_state(id)
        .and_then(|state| state.selection.clone())
        .expect("a row was selected");
    harness
        .state_mut()
        .feed
        .details
        .insert(identity, pod_detail("db-postgres-0"));
    run_steps(&mut harness);
    snapshot_tree(&harness, "pod_detail_overview");
}

#[test]
fn pod_detail_disconnected_logs() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        W::Pods,
        vec![list_row("", "v1", "Pod", "db-postgres-0", "Running")],
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(W::Pods),
        ));
    run_steps(&mut harness);
    let id = workload_id(harness.state(), W::Pods);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 1.0));
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    run_steps(&mut harness);
    let identity = harness
        .state()
        .shell
        .workspace()
        .resource_state(id)
        .and_then(|state| state.selection.clone())
        .expect("a row was selected");
    harness
        .state_mut()
        .feed
        .details
        .insert(identity, pod_detail("db-postgres-0"));
    run_steps(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    run_steps(&mut harness);

    // Retained history from an earlier session, now disconnected.
    let target = StreamTarget {
        context: CONTEXT.to_owned(),
        namespace: "default".to_owned(),
        pod: "db-postgres-0".to_owned(),
        uid: format!("uid-{CONTEXT}-pod-default-db-postgres-0"),
        container: "app".to_owned(),
    };
    {
        let stores = harness.state_mut().shell.stream_stores_mut();
        let view = stores.logs.ensure(id, target);
        view.connect();
        view.attach();
        view.append("kubelet started pod");
        view.connection_lost();
    }
    run_steps(&mut harness);
    snapshot_tree(&harness, "pod_detail_disconnected_logs");
}

#[test]
fn scale_dialog_with_conflict_reason() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        W::Deployments,
        vec![list_row(
            "apps",
            "v1",
            "Deployment",
            "api-server",
            "2/2 ready",
        )],
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(W::Deployments),
        ));
    run_steps(&mut harness);
    let id = workload_id(harness.state(), W::Deployments);

    harness.state_mut().shell.dialogs_mut().open_scale(
        id,
        list_row("apps", "v1", "Deployment", "api-server", "2/2 ready").identity,
        Some(2),
    );
    if let Some(mut dialog) = harness.state_mut().shell.dialogs_mut().active_mut(id) {
        dialog.operation_failed("the target changed since validation");
    }
    run_steps(&mut harness);
    snapshot_tree(&harness, "scale_dialog_conflict");
}

#[test]
fn window_freshness_states() {
    let mut harness = Harness::builder()
        .with_size(egui::vec2(2_200.0, 1_500.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default());
    for kind in [
        W::Deployments,
        W::Pods,
        W::StatefulSets,
        W::DaemonSets,
        W::Jobs,
        W::CronJobs,
        W::ConfigMaps,
    ] {
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Workload(kind),
            ));
    }
    let deployments = workload_id(harness.state(), W::Deployments);
    let pods = workload_id(harness.state(), W::Pods);
    let stateful_sets = workload_id(harness.state(), W::StatefulSets);
    let daemon_sets = workload_id(harness.state(), W::DaemonSets);
    let jobs = workload_id(harness.state(), W::Jobs);
    let cron_jobs = workload_id(harness.state(), W::CronJobs);
    let config_maps = workload_id(harness.state(), W::ConfigMaps);
    for (window, position) in [
        (deployments, [10.0, 20.0]),
        (pods, [1_010.0, 20.0]),
        (stateful_sets, [10.0, 380.0]),
        (daemon_sets, [1_010.0, 380.0]),
        (jobs, [10.0, 740.0]),
        (cron_jobs, [1_010.0, 740.0]),
        (config_maps, [510.0, 1_100.0]),
    ] {
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::SetGeometry(
                window,
                WindowGeom {
                    position,
                    size: [950.0, 330.0],
                    collapsed: false,
                },
            ));
    }
    let feed = &mut harness.state_mut().feed;
    feed.window_lists.insert(
        deployments,
        vec![list_row(
            "apps",
            "v1",
            "Deployment",
            "healthy-api",
            "2/2 ready",
        )],
    );
    feed.window_lists.insert(
        pods,
        vec![list_row("", "v1", "Pod", "cached-pod", "Running")],
    );
    feed.window_lists.insert(stateful_sets, Vec::new());
    feed.window_lists.insert(daemon_sets, Vec::new());
    feed.window_lists.insert(jobs, Vec::new());
    feed.window_lists.insert(
        cron_jobs,
        vec![list_row("batch", "v1", "CronJob", "nightly", "Active 0")],
    );
    feed.window_lists.insert(
        config_maps,
        vec![list_row("", "v1", "ConfigMap", "app-settings", "3 keys")],
    );
    feed.window_freshness.insert(
        deployments,
        WindowFreshness::Live {
            last_sync_age: "4s ago".into(),
        },
    );
    feed.window_freshness.insert(
        pods,
        WindowFreshness::StaleRetrying {
            last_sync_age: "37s ago".into(),
            retry_in: "3s".into(),
            attempt: 2,
        },
    );
    feed.window_freshness.insert(
        stateful_sets,
        WindowFreshness::Forbidden {
            user: "alice@example.com".into(),
            verb: "list".into(),
            resource: "statefulsets.apps".into(),
            scope: "--namespace=payments".into(),
        },
    );
    feed.window_freshness.insert(
        daemon_sets,
        WindowFreshness::Failed {
            message: "watch ended unexpectedly".into(),
        },
    );
    feed.window_freshness
        .insert(jobs, WindowFreshness::ReadyEmpty);
    feed.window_freshness.insert(
        cron_jobs,
        WindowFreshness::Reconnecting {
            last_sync_age: "42s ago".into(),
            retry_in: "2s".into(),
            attempt: 3,
        },
    );
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSearch(
            config_maps,
            "does-not-exist".into(),
        ));
    run_steps(&mut harness);

    snapshot_tree(&harness, "window_freshness_states");
    if std::env::var_os("K10S_CAPTURE_ISSUE_171").is_some() {
        harness
            .render()
            .expect("render window-state gallery")
            .save("../../docs/screenshots/issue-171/after-window-states-standard-2200x1500.png")
            .expect("save window-state screenshot");
    }
}

#[test]
fn compact_window_freshness_states() {
    let cases = [
        (
            "live",
            Some(WindowFreshness::Live {
                last_sync_age: "4s ago".into(),
            }),
            false,
        ),
        (
            "stale",
            Some(WindowFreshness::StaleRetrying {
                last_sync_age: "37s ago".into(),
                retry_in: "3s".into(),
                attempt: 2,
            }),
            false,
        ),
        (
            "reconnecting",
            Some(WindowFreshness::Reconnecting {
                last_sync_age: "42s ago".into(),
                retry_in: "2s".into(),
                attempt: 3,
            }),
            false,
        ),
        (
            "forbidden",
            Some(WindowFreshness::Forbidden {
                user: "alice@example.com".into(),
                verb: "list".into(),
                resource: "deployments.apps".into(),
                scope: "--namespace=payments".into(),
            }),
            false,
        ),
        (
            "failed",
            Some(WindowFreshness::Failed {
                message: "watch ended unexpectedly".into(),
            }),
            false,
        ),
        ("empty", Some(WindowFreshness::ReadyEmpty), false),
        ("filtered-empty", None, true),
    ];

    for (name, freshness, filtered_empty) in cases {
        let mut harness = harness_with_size(egui::vec2(640.0, 480.0));
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Workload(W::Deployments),
            ));
        run_steps(&mut harness);
        let window = workload_id(harness.state(), W::Deployments);
        harness.state_mut().feed.window_lists.insert(
            window,
            if matches!(freshness, Some(WindowFreshness::ReadyEmpty)) {
                Vec::new()
            } else {
                vec![list_row(
                    "apps",
                    "v1",
                    "Deployment",
                    "api-server",
                    "2/2 ready",
                )]
            },
        );
        if let Some(freshness) = freshness {
            harness
                .state_mut()
                .feed
                .window_freshness
                .insert(window, freshness);
        }
        if filtered_empty {
            harness
                .state_mut()
                .shell
                .apply_workspace_command(WorkspaceCommand::SetSearch(
                    window,
                    "does-not-exist".into(),
                ));
        }
        run_steps(&mut harness);
        snapshot_tree(&harness, &format!("issue_171_compact_{name}"));
        if std::env::var_os("K10S_CAPTURE_ISSUE_171").is_some() {
            harness
                .render()
                .expect("render compact state")
                .save(format!(
                    "../../docs/screenshots/issue-171/after-window-{name}-compact-640x480.png"
                ))
                .expect("save compact state screenshot");
        }
    }
}

#[test]
fn compact_taskbar_exposes_instance_status_and_keyboard_reachable_overflow() {
    let mut harness = harness_with_size(egui::vec2(640.0, 420.0));
    let pod = list_row("", "v1", "Pod", "api-0", "Running").identity;
    {
        let fixture = harness.state_mut();
        fixture.connection = ConnectionState::Connecting;
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(W::Pods));
        let first_pods = workload_id(fixture, W::Pods);
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
                first_pods,
                k10s_ui::workspace::NamespaceScope::Namespace("payments".into()),
            ));
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(W::Pods));
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(pod));
        let detail = fixture
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| window.kind == k10s_ui::workspace::WindowKind::Detail)
            .expect("dedicated detail")
            .id;
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::BeginYamlEdit(detail));
    }
    run_steps(&mut harness);
    snapshot_tree(&harness, "taskbar_layouts");

    // The compact overflow control participates in ordinary keyboard focus
    // traversal, while numbered accelerators still address registry entries.
    let overflow = harness.get_by(|node| {
        node.role() == Role::ComboBox && node.value().as_deref() == Some("More tasks (3)")
    });
    overflow.focus();
    run_steps(&mut harness);
    let overflow = harness.get_by(|node| {
        node.role() == Role::ComboBox && node.value().as_deref() == Some("More tasks (3)")
    });
    assert!(overflow.is_focused());
    harness.key_press_modifiers(egui::Modifiers::ALT, egui::Key::Num1);
    run_steps(&mut harness);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .max_by_key(|window| window.z)
            .unwrap()
            .id,
        harness.state().shell.workspace().windows()[0].id
    );
}
