//! Connected workload windows: seven kinds, independent per-window state,
//! the searchable GVK picker for cluster-scoped custom resources, the
//! conditional window size policy, split-pane minima, hide/restore, selection,
//! and snapshot resync preserving filters and selections.

use egui::accesskit::Role;
use egui_kittest::{Harness, kittest::Queryable as _};
use k10s_protocol::{
    BackendRevision, GroupVersionKind, ResourceIdentity, ResourceListRow, ResourceTypeEntry,
};
use k10s_ui::{
    ui::{
        ConnectionState, NamespaceCatalogState, ResourceAction, ResourceFeed, SafeUiError, UiShell,
    },
    workspace::{
        LauncherItem, WindowGeom, WindowId, WindowKind, WorkloadKind as WorkspaceWorkload,
        WorkspaceCommand,
    },
};

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
    context_namespace: Option<String>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: default_feed(),
            context_namespace: None,
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let mut selected_context = Some(CONTEXT.to_owned());
    let contexts = [k10s_protocol::Context {
        name: CONTEXT.to_owned(),
        cluster: "cluster".into(),
        namespace: fixture.context_namespace.clone(),
        is_current: true,
        availability: k10s_protocol::ContextAvailability::Available,
        unavailable_reason: None,
    }];
    fixture.shell.show_with_contexts_and_resources(
        ui,
        ConnectionState::Connected,
        &contexts,
        &mut selected_context,
        None,
        &fixture.feed,
    );
}

#[test]
fn workload_namespace_combobox_searches_authoritative_options_and_selects() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog =
        NamespaceCatalogState::Ready(vec!["default".into(), "Sea-Team".into(), "other".into()]);
    let window = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            window,
            k10s_ui::workspace::NamespaceScope::AllNamespaces,
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    let window_node = harness.get_by_role_and_label(Role::Window, "Deployments");
    assert!(
        window_node
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    window_node
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(3);
    let search = harness.get_by_role_and_label(Role::TextInput, "Search namespaces");
    search.type_text("SEA");
    harness.run_steps(2);
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "other")
            .is_none()
    );
    harness
        .get_by_role_and_label(Role::Button, "Sea-Team")
        .click();
    harness.run_steps(2);
    assert_eq!(
        workspace_resource_namespace(harness.state().shell.workspace(), window).as_deref(),
        Some("Sea-Team")
    );
}

#[test]
fn missing_workload_namespace_stays_narrow_until_explicitly_cleared() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog = NamespaceCatalogState::Ready(vec!["default".into()]);
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            id,
            k10s_ui::workspace::NamespaceScope::Namespace("deleted-team".into()),
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    let selector = harness.get_by_role_and_label(Role::ComboBox, "Namespace");
    assert_eq!(
        selector.value().as_deref(),
        Some("deleted-team · namespace no longer exists")
    );
    assert_eq!(
        workspace_resource_namespace(harness.state().shell.workspace(), id).as_deref(),
        Some("deleted-team")
    );
    selector.click();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "All namespaces")
        .click();
    harness.run_steps(2);
    assert_eq!(
        workspace_resource_namespace(harness.state().shell.workspace(), id),
        None
    );
}

#[test]
fn namespace_catalog_unavailable_renders_only_safe_message_and_retries() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog =
        NamespaceCatalogState::Unavailable(SafeUiError::new("namespace access denied"));
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Pods),
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Namespaces unavailable: namespace access denied");
    assert!(window.query_by_label("backend raw details").is_none());
    window
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    assert!(
        harness
            .query_by_role_and_label(Role::TextInput, "Search namespaces")
            .is_none(),
        "an unavailable catalog must keep the namespace selector closed"
    );
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "default")
            .is_none()
    );
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Retry namespaces")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::RetryNamespaceCatalog]
    );
}

#[test]
fn namespace_catalog_lifecycle_distinguishes_not_requested_loading_and_ready_empty() {
    let mut fixture = Fixture::default();
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Pods),
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    let selector = harness.get_by_role_and_label(Role::ComboBox, "Namespace");
    assert_eq!(
        selector.value().as_deref(),
        Some("Namespace catalog not requested")
    );
    selector.click();
    harness.run_steps(2);
    assert!(
        harness
            .query_by_role_and_label(Role::TextInput, "Search namespaces")
            .is_none()
    );

    harness.state_mut().feed.namespace_catalog = NamespaceCatalogState::Loading;
    harness.run_steps(2);
    harness.get_by_label("Loading namespaces");
    harness
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    assert!(
        harness
            .query_by_role_and_label(Role::TextInput, "Search namespaces")
            .is_none()
    );

    harness.state_mut().feed.namespace_catalog = NamespaceCatalogState::Ready(Vec::new());
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::TextInput, "Search namespaces");
    harness.get_by_label("No namespaces found");
    harness.get_by_role_and_label(Role::Button, "All namespaces");
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .with_step_dt(0.05)
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
        projection: None,
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
fn same_kind_windows_render_their_own_window_keyed_rows() {
    let mut fixture = Fixture::default();
    fixture.feed.lists.remove(&WorkspaceWorkload::Pods);
    let first = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
            WorkspaceWorkload::Pods,
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    let second = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(
            WorkspaceWorkload::Pods,
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    fixture.feed.window_lists.insert(
        first,
        vec![list_row(
            "",
            "v1",
            "Pod",
            Some("default"),
            "pod-first",
            "Running",
            "2026-08-21T00:00:00Z",
        )],
    );
    fixture.feed.window_lists.insert(
        second,
        vec![list_row(
            "",
            "v1",
            "Pod",
            Some("default"),
            "pod-second",
            "Running",
            "2026-08-21T00:00:00Z",
        )],
    );

    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    harness.get_by_label("Select resource pod-first");
    harness.get_by_label("Select resource pod-second");
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
        window.get_by_label("Select resource sample-one");
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
    for header in ["Name", "Status", "Created"] {
        window.get_by_label(header);
    }
    window.get_by_role_and_label(Role::ComboBox, "Namespace");
    window.get_by_label("Select resource traffic-overview");
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
    let custom_id = workload_id(harness.state(), WorkspaceWorkload::CustomResources);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            custom_id,
            k10s_ui::workspace::NamespaceScope::Namespace("team-a".into()),
        ));

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
    assert!(
        window
            .query_by_role_and_label(Role::ComboBox, "Namespace")
            .is_none()
    );
    // Cluster-scoped: no namespace column and no namespace filter control.
    assert!(window.query_by_label("Namespace").is_none());
    assert!(
        window
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    assert!(window.query_by_label("Clear filters").is_none());
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .resource_state(custom_id)
            .unwrap()
            .namespace_scope,
        k10s_ui::workspace::NamespaceScope::Namespace("team-a".into()),
        "ignored cluster scope intent is preserved for a later namespaced GVK"
    );
    window.get_by_label("Select resource dashboards.monitoring.example.com");
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
fn namespace_search_scratch_is_per_window() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog =
        NamespaceCatalogState::Ready(vec!["default".into(), "sea-team".into(), "other".into()]);
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Pods),
        ));
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ));
    let ids: Vec<_> = fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| matches!(window.kind, WindowKind::Workload(_)))
        .map(|window| window.id)
        .collect();
    for (id, x) in ids.into_iter().zip([0.0, 680.0]) {
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::SetGeometry(
                id,
                WindowGeom {
                    position: [x, 30.0],
                    size: [640.0, 520.0],
                    collapsed: false,
                },
            ));
    }
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_440.0, 700.0))
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    let search = harness.get_by_role_and_label(Role::TextInput, "Search namespaces");
    search.type_text("sea");
    harness.run_steps(2);
    assert_eq!(
        harness
            .get_by_role_and_label(Role::TextInput, "Search namespaces")
            .value()
            .as_deref(),
        Some("sea")
    );
    harness
        .get_by_role_and_label(Role::Window, "Deployments")
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    assert_eq!(
        harness
            .get_by_role_and_label(Role::TextInput, "Search namespaces")
            .value()
            .as_deref(),
        Some("")
    );
    harness.get_by_role_and_label(Role::Button, "other");
}

#[test]
fn namespace_combobox_remains_reachable_in_compact_viewport() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog = NamespaceCatalogState::Ready(vec!["default".into()]);
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Pods),
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    let mut harness = Harness::builder()
        .with_size(egui::vec2(680.0, 700.0))
        .build_ui_state(render, fixture);
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "default")
        .click();
    harness.run_steps(2);
    assert_eq!(
        workspace_resource_namespace(harness.state().shell.workspace(), id).as_deref(),
        Some("default")
    );
}

fn set_geometry(fixture: &mut Fixture, id: WindowId, size: [f32; 2]) {
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [10.0, 30.0],
                size,
                collapsed: false,
            },
        ));
}

fn window_id(fixture: &Fixture, kind: WindowKind) -> WindowId {
    fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == kind)
        .expect("window is open")
        .id
}

fn assert_normal_size(rect: egui::Rect, minimum: egui::Vec2) {
    assert!(
        rect.width() >= minimum.x - 1.0 && rect.height() >= minimum.y - 1.0,
        "window rect {rect:?} must respect the {minimum:?} minimum"
    );
}

fn assert_compact_size(rect: egui::Rect) {
    assert!(
        rect.width() < 300.0 && rect.height() < 220.0,
        "window rect {rect:?} must preserve the requested compact size"
    );
}

fn assert_compact_detail_size(rect: egui::Rect) {
    assert!(
        rect.width() < 360.0 && rect.height() < 280.0,
        "detail window rect {rect:?} must remain compact while preserving its fixed frame chrome"
    );
}

fn resize_window_toward_compact(
    harness: &mut Harness<'static, Fixture>,
    title: &str,
) -> egui::Rect {
    let rect = harness.get_by_role_and_label(Role::Window, title).rect();
    let grab = rect.max;
    let target = rect.min + egui::vec2(240.0, 160.0);
    harness.hover_at(grab);
    harness.run_steps(1);
    harness.drag_at(grab);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Window, title).rect()
}

#[test]
fn normal_mode_enforces_workload_minimum() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ));
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    // Applied before the first frame so the window never renders larger.
    set_geometry(harness.state_mut(), id, [240.0, 160.0]);
    harness.run_steps(4);

    assert_normal_size(
        harness
            .get_by_role_and_label(Role::Window, "Deployments")
            .rect(),
        egui::vec2(640.0, 420.0),
    );
}

#[test]
fn free_mode_preserves_compact_workload_geometry() {
    let mut harness = harness();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Deployments),
        ));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    set_geometry(harness.state_mut(), id, [240.0, 160.0]);
    harness.run_steps(4);

    assert_compact_size(
        harness
            .get_by_role_and_label(Role::Window, "Deployments")
            .rect(),
    );
}

#[test]
fn normal_and_free_modes_apply_overview_size_policy() {
    let mut normal = harness();
    let id = window_id(normal.state(), WindowKind::Overview);
    set_geometry(normal.state_mut(), id, [240.0, 160.0]);
    normal.run_steps(4);
    assert_normal_size(
        resize_window_toward_compact(&mut normal, "Overview"),
        egui::vec2(480.0, 320.0),
    );

    let mut free = harness();
    free.state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    let id = window_id(free.state(), WindowKind::Overview);
    set_geometry(free.state_mut(), id, [240.0, 160.0]);
    free.run_steps(4);
    assert_compact_size(resize_window_toward_compact(&mut free, "Overview"));
}

#[test]
fn normal_and_free_modes_apply_detail_size_policy() {
    fn fixture(free: bool) -> Fixture {
        let mut fixture = Fixture::default();
        if free {
            fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
        }
        let identity = fixture.feed.lists[&WorkspaceWorkload::Pods][0]
            .identity
            .clone();
        fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity));
        let id = window_id(&fixture, WindowKind::Detail);
        set_geometry(&mut fixture, id, [240.0, 160.0]);
        fixture
    }

    let mut normal = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture(false));
    normal.run_steps(4);
    assert_normal_size(
        normal
            .get_by_role_and_label(Role::Window, "Pod · default / web-frontend-7d9f8-00001")
            .rect(),
        egui::vec2(640.0, 420.0),
    );

    let mut free = Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture(true));
    free.run_steps(4);
    assert_compact_detail_size(
        free.get_by_role_and_label(Role::Window, "Pod · default / web-frontend-7d9f8-00001")
            .rect(),
    );
}

#[test]
fn window_size_policy_handles_an_undersized_canvas() {
    fn compact_harness(free: bool) -> Harness<'static, Fixture> {
        let mut fixture = Fixture::default();
        if free {
            fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
        }
        let id = window_id(&fixture, WindowKind::Overview);
        set_geometry(&mut fixture, id, [240.0, 160.0]);
        Harness::builder()
            .with_size(egui::vec2(430.0, 280.0))
            .with_pixels_per_point(1.0)
            .build_ui_state(render, fixture)
    }

    let mut normal = compact_harness(false);
    normal.run_steps(4);
    let rect = normal
        .get_by_role_and_label(Role::Window, "Overview")
        .rect();
    assert!(
        rect.width() >= 460.0 && rect.height() >= 310.0,
        "undersized canvas must keep the Overview class minimum practical, got {rect:?}"
    );

    let mut free = compact_harness(true);
    free.run_steps(4);
    assert_compact_size(free.get_by_role_and_label(Role::Window, "Overview").rect());
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
        deployments
            .get_by_label("Select resource web-frontend")
            .rect()
            .top()
            < deployments
                .get_by_label("Select resource api-server")
                .rect()
                .top(),
        "descending creation order must put web-frontend first"
    );
    let pods = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(
        pods.get_by_label("Select resource web-frontend-7d9f8-00001")
            .rect()
            .top()
            < pods
                .get_by_label("Select resource db-postgres-0")
                .rect()
                .top(),
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
    deployments.get_by_label("Select resource api-server");
    assert!(deployments.query_by_label("web-frontend").is_none());
    let pods = harness.get_by_role_and_label(Role::Window, "Pods");
    pods.get_by_label("Select resource web-frontend-7d9f8-00001");
    pods.get_by_label("Select resource db-postgres-0");

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
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            instances[0],
            k10s_ui::workspace::NamespaceScope::Namespace("kube-system".to_owned()),
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
        .and_then(|resource| match &resource.namespace_scope {
            k10s_ui::workspace::NamespaceScope::Namespace(value) => Some(value.clone()),
            _ => None,
        })
}

#[test]
fn selection_driven_detail_panel_respects_split_minima() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Pods);

    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Pod · default / db-postgres-0");
    // Without a resolved backend response the pane keeps its pinned
    // identity header and shows a loading state.
    window.get_by_label("Loading details");

    // Extreme split ratios clamp to the pane minima instead of collapsing.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 0.0));
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Select resource web-frontend-7d9f8-00001");
    window.get_by_label("Pod · default / db-postgres-0");

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 1.0));
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    window.get_by_label("Select resource web-frontend-7d9f8-00001");
    window.get_by_label("Pod · default / db-postgres-0");

    // Clearing selection removes the contextual bottom panel.
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    assert!(
        window
            .query_by_label("Pod · default / db-postgres-0")
            .is_none()
    );
}

#[test]
fn selected_row_second_click_clears_selection_and_detail() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );

    let row_label = "Select resource db-postgres-0";
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, row_label)
        .click();
    harness.run_steps(4);
    assert!(
        harness
            .get_by_role_and_label(Role::Window, "Pods")
            .query_by_label("Pod · default / db-postgres-0")
            .is_some()
    );

    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .expect("Pods window has resource state");
    assert!(resource.selection.is_none());
    assert!(resource.detail.is_none());
}

#[test]
fn resource_double_click_opens_dedicated_without_selecting_or_guarding() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let row = harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0");
    row.click();
    row.click();
    harness.run_steps(4);

    assert!(
        harness
            .state()
            .shell
            .workspace()
            .resource_state(window)
            .unwrap()
            .selection
            .is_none()
    );
    assert!(harness.state().shell.workspace().pending().is_none());
    harness.get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0");
}

#[test]
fn selected_dirty_resource_double_click_preserves_selection_and_skips_guard() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::BeginYamlEdit(window));
    let row = harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0");
    row.click();
    row.click();
    harness.run_steps(4);

    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .unwrap();
    assert!(resource.selection.is_some());
    assert!(harness.state().shell.workspace().pending().is_none());
    harness.get_by_role_and_label(Role::Window, "Pod · default / db-postgres-0");
}

#[test]
fn detail_close_is_in_identity_row() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    harness
        .get_by_role_and_label(Role::Window, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = harness.get_by_role_and_label(Role::Window, "Pods");
    let identity_row = window.get_by_label("Detail identity row");
    let close = window.get_by_role_and_label(Role::Button, "Clear selection");
    assert!(
        identity_row.rect().contains_rect(close.rect()),
        "Clear selection must be a compact control inside the Detail identity-row layout area"
    );
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
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
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
    window.get_by_role_and_label(Role::Button, "Clear selection for resource web-frontend");
    assert!(
        window
            .query_by_label("Select resource api-server")
            .is_none()
    );

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
    window.get_by_role_and_label(Role::Button, "Clear selection for resource web-frontend");
    assert!(
        window
            .query_by_label("Select resource api-server")
            .is_none(),
        "the resynced snapshot must honor the surviving filter"
    );
    assert!(window.query_by_label("Select resource checkout").is_none());
    window.get_by_label("18/18 ready");

    // Clearing the filter reveals the rest of the resynced snapshot.
    window
        .get_by_role_and_label(Role::Button, "Clear filters")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Deployments");
    window.get_by_label("Select resource api-server");
    window.get_by_label("Select resource checkout");
}
