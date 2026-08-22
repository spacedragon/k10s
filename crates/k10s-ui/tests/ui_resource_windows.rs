//! Connected workload windows: seven kinds, independent per-window state,
//! the searchable GVK picker for cluster-scoped custom resources, the
//! 640×420 window minimum, split-pane minima, hide/restore, selection,
//! and snapshot resync preserving filters and selections.

use egui::accesskit::Role;
use egui_kittest::{Harness, kittest::Queryable as _};
use k10s_protocol::{
    BackendRevision, GroupVersionKind, ResourceIdentity, ResourceListRow, ResourceTypeEntry,
};
use k10s_ui::{
    ui::{ConnectionState, ResourceFeed, UiShell},
    workspace::{
        LauncherItem, WindowGeom, WindowId, WindowKind, WorkloadKind as WorkspaceWorkload,
        WorkspaceCommand,
    },
};

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: default_feed(),
        }
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

fn default_feed() -> ResourceFeed {
    let mut feed = ResourceFeed::default();
    feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            list_row(
                "apps",
                "v1",
                "Deployment",
                Some("default"),
                "api-server",
                "2/2 ready",
                "2026-08-21T00:00:00Z",
            ),
            list_row(
                "apps",
                "v1",
                "Deployment",
                Some("default"),
                "web-frontend",
                "20/20 ready",
                "2026-08-21T00:05:00Z",
            ),
        ],
    );
    feed.lists.insert(
        WorkspaceWorkload::Pods,
        vec![
            list_row(
                "",
                "v1",
                "Pod",
                Some("default"),
                "web-frontend-7d9f8-00001",
                "Running",
                "2026-08-21T00:50:10Z",
            ),
            list_row(
                "",
                "v1",
                "Pod",
                Some("default"),
                "web-frontend-7d9f8-00002",
                "Running",
                "2026-08-21T00:50:20Z",
            ),
            list_row(
                "",
                "v1",
                "Pod",
                Some("default"),
                "db-postgres-0",
                "Running",
                "2026-08-21T01:26:40Z",
            ),
        ],
    );
    feed.types = vec![
        type_entry("apps", "v1", "Deployment", true),
        type_entry("apps", "v1", "StatefulSet", true),
        type_entry("apps", "v1", "DaemonSet", true),
        type_entry("batch", "v1", "Job", true),
        type_entry("batch", "v1", "CronJob", true),
        type_entry("", "v1", "Pod", true),
        type_entry(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            false,
        ),
        type_entry("monitoring.example.com", "v1", "Dashboard", true),
    ];
    feed
}

fn list_row(
    group: &str,
    version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    summary: &str,
    created_at: &str,
) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: group.to_owned(),
                version: version.to_owned(),
                kind: kind.to_owned(),
            },
            namespace: namespace.map(str::to_owned),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-{}-{name}", kind.to_lowercase()),
        },
        revision: BackendRevision::new(1_000),
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: created_at.to_owned(),
    }
}

fn type_entry(group: &str, version: &str, kind: &str, namespaced: bool) -> ResourceTypeEntry {
    ResourceTypeEntry {
        gvk: GroupVersionKind {
            group: group.to_owned(),
            version: version.to_owned(),
            kind: kind.to_owned(),
        },
        namespaced,
    }
}

fn workload_id(fixture: &Fixture, kind: WorkspaceWorkload) -> WindowId {
    fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Workload(kind))
        .expect("workload window is open")
        .id
}

fn open(harness: &mut Harness<'static, Fixture>, item: LauncherItem) {
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(item));
    harness.run_steps(4);
}

#[test]
fn all_seven_workload_kinds_render_rows_and_columns() {
    let mut harness = harness();
    harness.state_mut().feed.lists.clear();
    for (kind, (group, version, gvk_kind)) in [
        (WorkspaceWorkload::Deployments, ("apps", "v1", "Deployment")),
        (
            WorkspaceWorkload::StatefulSets,
            ("apps", "v1", "StatefulSet"),
        ),
        (WorkspaceWorkload::DaemonSets, ("apps", "v1", "DaemonSet")),
        (WorkspaceWorkload::Jobs, ("batch", "v1", "Job")),
        (WorkspaceWorkload::CronJobs, ("batch", "v1", "CronJob")),
        (WorkspaceWorkload::Pods, ("", "v1", "Pod")),
    ] {
        harness.state_mut().feed.lists.insert(
            kind,
            vec![list_row(
                group,
                version,
                gvk_kind,
                Some("default"),
                "sample-one",
                "1/1 ready",
                "2026-08-21T00:00:00Z",
            )],
        );
    }
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::CustomResources,
        vec![list_row(
            "monitoring.example.com",
            "v1",
            "Dashboard",
            Some("default"),
            "traffic-overview",
            "1 panel",
            "2026-08-21T00:45:00Z",
        )],
    );

    for title in [
        WorkspaceWorkload::Deployments,
        WorkspaceWorkload::StatefulSets,
        WorkspaceWorkload::DaemonSets,
        WorkspaceWorkload::Jobs,
        WorkspaceWorkload::CronJobs,
        WorkspaceWorkload::Pods,
    ] {
        open(&mut harness, LauncherItem::Workload(title));
    }

    for title in [
        "Deployments",
        "StatefulSets",
        "DaemonSets",
        "Jobs",
        "CronJobs",
        "Pods",
    ] {
        let window = harness.get_by_role_and_label(Role::Window, title);
        for header in ["Name", "Status", "Created"] {
            window.get_by_label(header);
        }
        window.get_by_label("sample-one");
        window.get_by_label("1/1 ready");
    }

    // Custom Resources renders only after an explicit type is picked.
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::CustomResources),
    );
    let picker = harness.get_by_role_and_label(Role::Window, "Custom Resources");
    picker.get_by_role_and_label(Role::Button, "monitoring.example.com/v1 Dashboard");
    assert!(
        picker.query_by_label("traffic-overview").is_none(),
        "custom resource rows must not render before a type is picked"
    );

    let custom_id = workload_id(harness.state(), WorkspaceWorkload::CustomResources);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetCustomKind(
            custom_id,
            Some("monitoring.example.com/v1/Dashboard".to_owned()),
        ));
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Custom Resources");
    for header in ["Namespace", "Name", "Status", "Created"] {
        window.get_by_label(header);
    }
    window.get_by_label("traffic-overview");
}

#[test]
fn searchable_gvk_picker_selects_cluster_scoped_custom_resources() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::CustomResources,
        vec![list_row(
            "apiextensions.k8s.io",
            "v1",
            "CustomResourceDefinition",
            None,
            "dashboards.monitoring.example.com",
            "Established",
            "2026-08-21T00:45:00Z",
        )],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::CustomResources),
    );

    let picker = harness.get_by_role_and_label(Role::Window, "Custom Resources");
    picker.get_by_role_and_label(Role::Button, "monitoring.example.com/v1 Dashboard");
    picker
        .get_by_role_and_label(Role::TextInput, "Search resource types")
        .focus();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Custom Resources")
        .get_by_role_and_label(Role::TextInput, "Search resource types")
        .type_text("custom");
    harness.run_steps(4);

    let picker = harness.get_by_role_and_label(Role::Window, "Custom Resources");
    let crd_button = picker.get_by_role_and_label(
        Role::Button,
        "apiextensions.k8s.io/v1 CustomResourceDefinition",
    );
    assert!(
        picker
            .query_by_label("monitoring.example.com/v1 Dashboard")
            .is_none(),
        "the picker search must narrow the selectable types"
    );
    crd_button.click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Custom Resources");
    // Cluster-scoped: no namespace column and no namespace filter control.
    assert!(window.query_by_label("Namespace").is_none());
    assert!(
        window
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    window.get_by_label("dashboards.monitoring.example.com");
    window.get_by_label("Established");

    window
        .get_by_role_and_label(Role::Button, "Change resource type")
        .click();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Custom Resources")
        .get_by_role_and_label(Role::TextInput, "Search resource types");
}

#[test]
fn workload_windows_enforce_the_640x420_minimum_size() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ));
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    // Applied before the first frame so the window never renders larger.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [10.0, 30.0],
                size: [240.0, 160.0],
                collapsed: false,
            },
        ));
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    let rect = window.rect();
    assert!(
        rect.width() >= 639.0 && rect.height() >= 419.0,
        "window rect {rect:?} must respect the 640x420 minimum"
    );
}

#[test]
fn windows_keep_search_sort_and_namespace_independent() {
    let mut harness = harness();
    // Open Pods first so the Deployments window renders on top of the
    // staggered overlap region and receives pointer clicks.
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    // Sorting flips order in the sorted window only.
    let deployments = harness.get_by_role_and_label(Role::Window, "Deployments");
    deployments
        .get_by_role_and_label(Role::Button, "Sort deployments by created")
        .click();
    harness.run_steps(4);
    let deployments = harness.get_by_role_and_label(Role::Window, "Deployments");
    deployments
        .get_by_role_and_label(Role::Button, "Sort deployments by created")
        .click();
    harness.run_steps(4);
    let deployments = harness.get_by_role_and_label(Role::Window, "Deployments");
    assert!(
        deployments.get_by_label("web-frontend").rect().top()
            < deployments.get_by_label("api-server").rect().top(),
        "descending creation order must put web-frontend first"
    );
    let pods = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(
        pods.get_by_label("web-frontend-7d9f8-00001").rect().top()
            < pods.get_by_label("db-postgres-0").rect().top(),
        "the unsorted window must keep its original order"
    );

    // A search typed into one window never leaks into another window.
    let deployments = harness.get_by_role_and_label(Role::Window, "Deployments");
    deployments
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .type_text("api");
    harness.run_steps(4);

    let deployments = harness.get_by_role_and_label(Role::Window, "Deployments");
    deployments.get_by_label("api-server");
    assert!(deployments.query_by_label("web-frontend").is_none());
    let pods = harness.get_by_role_and_label(Role::Window, "Pods");
    pods.get_by_label("web-frontend-7d9f8-00001");
    pods.get_by_label("db-postgres-0");

    // Two instances of the same kind keep fully independent namespace state.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
            WorkspaceWorkload::Deployments,
        ));
    harness.run_steps(4);
    let instances: Vec<WindowId> = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkspaceWorkload::Deployments))
        .map(|window| window.id)
        .collect();
    assert_eq!(instances.len(), 2);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespace(
            instances[0],
            Some("kube-system".to_owned()),
        ));
    let workspace = harness.state().shell.workspace();
    assert_eq!(
        workspace_resource_namespace(workspace, instances[0]).as_deref(),
        Some("kube-system")
    );
    assert_eq!(
        workspace_resource_namespace(workspace, instances[1]),
        None,
        "the second instance must keep its own namespace filter"
    );
}

fn workspace_resource_namespace(
    workspace: &k10s_ui::workspace::WorkspaceState<ResourceIdentity>,
    id: WindowId,
) -> Option<String> {
    workspace
        .resource_state(id)
        .and_then(|resource| resource.namespace.clone())
}

#[test]
fn detail_pane_selection_hide_restore_respects_split_minima() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Pods);

    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Details");
    window.get_by_label("Name db-postgres-0");
    window.get_by_label("Kind Pod");
    window.get_by_label("Status Running");

    // Extreme split ratios clamp to the pane minima instead of collapsing.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 0.0));
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("web-frontend-7d9f8-00001");
    window.get_by_label("Details");

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 1.0));
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("web-frontend-7d9f8-00001");
    window.get_by_label("Details");

    // Hiding keeps the selection; restoring brings the detail back.
    window
        .get_by_role_and_label(Role::Button, "Hide details")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(window.query_by_label("Details").is_none());
    window.get_by_label("web-frontend-7d9f8-00001");
    window
        .get_by_role_and_label(Role::Button, "Show details")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Details");
    window.get_by_label("Name db-postgres-0");

    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(window.query_by_label("Details").is_none());
}

#[test]
fn snapshot_resync_replaces_rows_while_preserving_filters_and_selection() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::Button, "web-frontend")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .type_text("web");
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("web-frontend");
    assert!(window.query_by_label("api-server").is_none());

    // A fresh snapshot replaces every row; the local filter and selection
    // survive the resync and follow the updated row content.
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            list_row(
                "apps",
                "v1",
                "Deployment",
                Some("default"),
                "api-server",
                "4/4 ready",
                "2026-08-21T00:00:00Z",
            ),
            list_row(
                "apps",
                "v1",
                "Deployment",
                Some("default"),
                "web-frontend",
                "18/18 ready",
                "2026-08-21T00:05:00Z",
            ),
            list_row(
                "apps",
                "v1",
                "Deployment",
                Some("default"),
                "checkout",
                "0/1 ready",
                "2026-08-21T02:00:00Z",
            ),
        ],
    );
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("web-frontend");
    assert!(
        window.query_by_label("api-server").is_none(),
        "the resynced snapshot must honor the surviving filter"
    );
    assert!(window.query_by_label("checkout").is_none());
    window.get_by_label("Status 18/18 ready");

    // Clearing the filter reveals the rest of the resynced snapshot.
    window
        .get_by_role_and_label(Role::Button, "Clear filters")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("api-server");
    window.get_by_label("checkout");
}
