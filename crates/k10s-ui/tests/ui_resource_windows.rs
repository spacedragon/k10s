//! Connected workload windows: seven kinds, independent per-window state,
//! the searchable GVK picker for cluster-scoped custom resources, the
//! conditional window size policy, split-pane minima, hide/restore, selection,
//! and snapshot resync preserving filters and selections.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, ContainerImageProjection, DeploymentProjection, GroupVersionKind,
    ResourceIdentity, ResourceListRow, ResourceProjection, ResourceTypeEntry,
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

mod common;

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

use common::{namespace_combobox, workload_window};

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
    let window_node = workload_window(&harness, "Deployments");
    assert!(
        window_node
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    namespace_combobox(window_node).click();
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
    let selector = namespace_combobox(harness.root());
    assert_eq!(
        selector.accesskit_node().label().as_deref(),
        Some("Namespace: deleted-team · no longer exists")
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
    let window = workload_window(&harness, "Pods");
    window.get_by_label("Namespaces unavailable: namespace access denied");
    assert!(window.query_by_label("backend raw details").is_none());
    namespace_combobox(window).click();
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
    workload_window(&harness, "Pods")
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
    let selector = namespace_combobox(harness.root());
    assert_eq!(
        selector.accesskit_node().label().as_deref(),
        Some("Namespace: not requested")
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
    namespace_combobox(harness.root()).click();
    harness.run_steps(2);
    assert!(
        harness
            .query_by_role_and_label(Role::TextInput, "Search namespaces")
            .is_none()
    );

    harness.state_mut().feed.namespace_catalog = NamespaceCatalogState::Ready(Vec::new());
    harness.run_steps(2);
    namespace_combobox(harness.root()).click();
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
    let mut feed = ResourceFeed {
        render_time: Some(web_time::UNIX_EPOCH + web_time::Duration::from_secs(1_788_220_800)),
        ..ResourceFeed::default()
    };
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
fn responsive_deployment_headers_elision_alignment_and_sort_contract() {
    let mut fixture = Fixture::default();
    fixture
        .feed
        .lists
        .get_mut(&WorkspaceWorkload::Deployments)
        .unwrap()[0]
        .projection = Some(ResourceProjection::Deployment(DeploymentProjection {
        desired_replicas: Some(2),
        ready_replicas: Some(2),
        updated_replicas: Some(2),
        available_replicas: Some(2),
        strategy: Some("RollingUpdate".into()),
        selector: Default::default(),
        max_surge: None,
        max_unavailable: None,
        conditions: vec![],
        template_containers: vec![ContainerImageProjection {
            name: "api".into(),
            image: Some("ghcr.io/containers/kubernetes-mcp:v0.3.1".into()),
        }],
        template_labels: Default::default(),
        template_annotations: Default::default(),
        labels: Default::default(),
        annotations: Default::default(),
        created_at: None,
    }));
    fixture.feed.lists.get_mut(&WorkspaceWorkload::Deployments).unwrap()[1].projection = Some(
        serde_json::from_value(serde_json::json!({"kind":"deployment","desiredReplicas":1,"readyReplicas":1,"templateContainers":[{"name":"web","image":"nginx:v1"}]})).unwrap(),
    );
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
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [20.0, 30.0],
                size: [1_000.0, 520.0],
                collapsed: false,
            },
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_100.0, 650.0))
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    let wide = workload_window(&harness, "Deployments");
    // The Namespace label lives in the table header; the toolbar selector
    // carries its own label inside the control text.
    wide.get_by_label("Namespace");
    for header in ["Name", "Ready", "Status", "Image", "Age"] {
        wide.get_by_label(header);
    }
    for key in ["namespace", "name", "status", "created"] {
        wide.get_by_role_and_label(Role::Button, format!("Sort deployments by {key}").as_str());
    }
    for key in ["ready", "image"] {
        assert!(
            wide.query_by_role_and_label(
                Role::Button,
                format!("Sort deployments by {key}").as_str()
            )
            .is_none()
        );
    }
    let ready_value = wide.get_by_label("2/2").rect();
    let status_value = wide.get_by_label("2/2 ready").rect();
    assert!(ready_value.right() > wide.get_by_label("Ready").rect().center().x);
    let age_right = wide
        .get_all_by_label("Resource age")
        .map(|node| node.rect().right())
        .fold(f32::NEG_INFINITY, f32::max);
    assert!(
        wide.rect().right() - age_right <= 40.0,
        "the final table column must reach the window edge so its scrollbar stays at the edge: window={:?}, age_right={age_right}",
        wide.rect()
    );
    assert!(
        ready_value.right() <= status_value.left(),
        "wide Ready and Status values must not overlap: ready={ready_value:?}, status={status_value:?}"
    );
    wide.get_by_label("ghcr.io/containers/kubernetes-mcp:v0.3.1")
        .hover();
    harness.run_steps(15);
    assert!(
        harness
            .get_all_by_label("ghcr.io/containers/kubernetes-mcp:v0.3.1")
            .count()
            >= 2
    );
    harness.get_by_label("nginx:v1").hover();
    harness.run_steps(15);
    assert_eq!(
        harness.get_all_by_label("nginx:v1").count(),
        1,
        "short images have no redundant tooltip"
    );

    let rect = workload_window(&harness, "Deployments").rect();
    let target = rect.min + egui::vec2(640.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let compact = workload_window(&harness, "Deployments");
    assert!(compact.query_by_label("Image").is_none());
    compact.get_by_label("Status");
    let first_age_value_left = compact
        .get_all_by_label("Resource age")
        .map(|node| node.rect().left())
        .fold(f32::INFINITY, f32::min);
    assert!(
        compact
            .get_all_by_label("Resource age")
            .all(|node| node.rect().width() <= 56.0),
        "compact Age values must fit the resolved 56-point column"
    );
    assert!(
        compact.get_by_label("2/2").rect().right() <= first_age_value_left,
        "compact Ready values must not overlap the adjacent Age column"
    );
    let rect = compact.rect();
    let target = rect.min + egui::vec2(1_000.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let restored = workload_window(&harness, "Deployments");
    restored.get_by_label("Image");
    restored.get_by_label("Status");
}

#[test]
fn responsive_cluster_scoped_list_omits_namespace_and_reclaims_width() {
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
    let id = workload_id(harness.state(), WorkspaceWorkload::CustomResources);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetCustomKind(
            id,
            Some("apiextensions.k8s.io/v1/CustomResourceDefinition".into()),
        ));
    harness.run_steps(4);
    let window = workload_window(&harness, "Custom Resources");
    assert!(window.query_by_label("Namespace").is_none());
    window.get_by_label("Name");
    window.get_by_label("Status");
    window.get_by_label("Age");
    let rect = window.rect();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    harness.run_steps(2);
    let target = rect.min + egui::vec2(350.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let medium = workload_window(&harness, "Custom Resources");
    assert!(medium.query_by_label("Status").is_none());
    medium.get_by_label("Age");
    let rect = medium.rect();
    let target = rect.min + egui::vec2(180.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    assert!(
        workload_window(&harness, "Custom Resources")
            .query_by_label("Age")
            .is_none()
    );
}

#[test]
fn responsive_pod_schema_uses_kind_and_hides_node_before_restarts() {
    let mut fixture = Fixture::default();
    fixture
        .feed
        .lists
        .get_mut(&WorkspaceWorkload::Pods)
        .unwrap()[0]
        .summary = "Zulu summary".into();
    fixture.feed.lists.get_mut(&WorkspaceWorkload::Pods).unwrap()[0].projection = Some(
        serde_json::from_value(serde_json::json!({"kind":"pod","phase":"AlphaPhase","readyContainers":1,"totalContainers":1,"restartCount":7,"nodeName":"worker-with-a-long-name"})).unwrap(),
    );
    fixture
        .feed
        .lists
        .get_mut(&WorkspaceWorkload::Pods)
        .unwrap()[1]
        .summary = "Alpha summary".into();
    fixture.feed.lists.get_mut(&WorkspaceWorkload::Pods).unwrap()[1].projection = Some(
        serde_json::from_value(serde_json::json!({"kind":"pod","phase":"ZuluPhase","readyContainers":1,"totalContainers":1,"restartCount":1,"nodeName":"worker-two"})).unwrap(),
    );
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
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [20.0, 30.0],
                size: [1_000.0, 520.0],
                collapsed: false,
            },
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_100.0, 650.0))
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    let wide = workload_window(&harness, "Pods");
    for header in [
        "Namespace",
        "Name",
        "Ready",
        "Status",
        "Restarts",
        "Node",
        "Age",
    ] {
        assert!(wide.get_all_by_label(header).count() >= 1);
    }
    for key in ["ready", "restarts", "node"] {
        assert!(
            wide.query_by_role_and_label(Role::Button, format!("Sort pods by {key}").as_str())
                .is_none()
        );
    }
    assert!(
        wide.get_by_label("7").rect().right() > wide.get_by_label("Restarts").rect().center().x
    );
    let rect = wide.rect();
    wide.get_by_role_and_label(Role::Button, "Sort pods by status")
        .click();
    harness.run_steps(4);
    let sorted = workload_window(&harness, "Pods");
    assert!(
        sorted
            .get_by_label("Select resource web-frontend-7d9f8-00001")
            .rect()
            .top()
            < sorted
                .get_by_label("Select resource web-frontend-7d9f8-00002")
                .rect()
                .top(),
        "visible Pod order follows displayed phase, not conflicting summary"
    );
    let target = rect.min + egui::vec2(680.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let medium = workload_window(&harness, "Pods");
    assert!(medium.query_by_label("Node").is_none());
    medium.get_by_label("Restarts");
    let rect = medium.rect();
    let target = rect.min + egui::vec2(520.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    assert!(
        workload_window(&harness, "Pods")
            .query_by_label("Restarts")
            .is_none()
    );
}

#[test]
fn responsive_generic_namespaced_hides_status_then_age() {
    let mut fixture = Fixture::default();
    fixture.feed.lists.insert(
        WorkspaceWorkload::StatefulSets,
        vec![list_row(
            "apps",
            "v1",
            "StatefulSet",
            Some("default"),
            "database",
            "Ready",
            "2026-08-21T00:00:00Z",
        )],
    );
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::StatefulSets),
        ))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [20.0, 30.0],
                size: [640.0, 520.0],
                collapsed: false,
            },
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(800.0, 650.0))
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    let wide = workload_window(&harness, "StatefulSets");
    for header in ["Namespace", "Name", "Status", "Age"] {
        assert!(wide.get_all_by_label(header).count() >= 1);
    }
    let rect = wide.rect();
    let target = rect.min + egui::vec2(450.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(8);
    let medium = workload_window(&harness, "StatefulSets");
    assert!(medium.query_by_label("Status").is_none());
    medium.get_by_label("Age");
    let rect = medium.rect();
    let target = rect.min + egui::vec2(180.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    assert!(
        workload_window(&harness, "StatefulSets")
            .query_by_label("Age")
            .is_none()
    );
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
        let window = workload_window(&harness, title);
        for header in ["Name", "Status", "Age"] {
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
    let picker = workload_window(&harness, "Custom Resources");
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
    let window = workload_window(&harness, "Custom Resources");
    for header in ["Name", "Status", "Age"] {
        window.get_by_label(header);
    }
    namespace_combobox(window);
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

    let picker = workload_window(&harness, "Custom Resources");
    picker.get_by_role_and_label(Role::Button, "monitoring.example.com/v1 Dashboard");
    picker
        .get_by_role_and_label(Role::TextInput, "Search resource types")
        .focus();
    harness.run_steps(4);
    workload_window(&harness, "Custom Resources")
        .get_by_role_and_label(Role::TextInput, "Search resource types")
        .type_text("custom");
    harness.run_steps(4);

    let picker = workload_window(&harness, "Custom Resources");
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

    let window = workload_window(&harness, "Custom Resources");
    // Cluster-scoped: no namespace selector, column, or filter control.
    assert!(window.query_all_by_role(Role::ComboBox).all(|node| {
        !node
            .value()
            .is_some_and(|value| value.starts_with("Namespace: "))
    }));
    assert!(window.query_by_label("Namespace").is_none());
    assert!(
        window
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    assert!(window.query_by_label("Reset").is_none());
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
        .get_by_role_and_label(Role::Button, "More list controls")
        .click();
    harness.step();
    harness
        .get_by_role_and_label(Role::Button, "Change resource type")
        .click();
    harness.run_steps(4);
    workload_window(&harness, "Custom Resources")
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
    namespace_combobox(workload_window(&harness, "Deployments")).click();
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
    namespace_combobox(workload_window(&harness, "Deployments")).click();
    harness.run_steps(2);
    namespace_combobox(workload_window(&harness, "Pods")).click();
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
    namespace_combobox(harness.root()).click();
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
        workload_window(&harness, "Deployments").rect(),
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

    assert_compact_size(workload_window(&harness, "Deployments").rect());
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
    let deployments = workload_window(&harness, "Deployments");
    deployments
        .get_by_role_and_label(Role::Button, "Sort deployments by created")
        .click();
    harness.run_steps(4);
    let deployments = workload_window(&harness, "Deployments");
    deployments
        .get_by_role_and_label(Role::Button, "Sort deployments by created")
        .click();
    harness.run_steps(4);
    let deployments = workload_window(&harness, "Deployments");
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
    let pods = workload_window(&harness, "Pods");
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
    let deployments = workload_window(&harness, "Deployments");
    deployments
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .type_text("api");
    harness.run_steps(4);

    let deployments = workload_window(&harness, "Deployments");
    deployments.get_by_label("Select resource api-server");
    assert!(deployments.query_by_label("web-frontend").is_none());
    let pods = workload_window(&harness, "Pods");
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

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);

    let window = workload_window(&harness, "Pods");
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
    let window = workload_window(&harness, "Pods");
    window.get_by_label("Select resource web-frontend-7d9f8-00001");
    window.get_by_label("Pod · default / db-postgres-0");

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 1.0));
    harness.run_steps(4);
    let window = workload_window(&harness, "Pods");
    window.get_by_label("Select resource web-frontend-7d9f8-00001");
    window.get_by_label("Pod · default / db-postgres-0");

    // Clearing selection removes the contextual bottom panel.
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);
    let window = workload_window(&harness, "Pods");
    assert!(
        window
            .query_by_label("Pod · default / db-postgres-0")
            .is_none()
    );
}

#[test]
fn selected_row_single_click_eventually_clears_selection_once() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );

    let row_label = "Select resource db-postgres-0";
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, row_label)
        .click();
    harness.run_steps(10);
    assert!(
        workload_window(&harness, "Pods")
            .query_by_label("Pod · default / db-postgres-0")
            .is_some()
    );

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.run_steps(10);

    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .expect("Pods window has resource state");
    assert!(resource.selection.is_none());
    assert!(resource.detail.is_none());
    assert!(harness.state().shell.workspace().pending().is_none());
}

#[test]
fn clicking_another_resource_replaces_the_pending_row_action() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetActiveTab(
            window,
            k10s_ui::workspace::DetailTab::Yaml,
        ));
    harness.run_steps(2);

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.step();
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend-7d9f8-00001")
        .click();
    harness.run_steps(10);

    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .unwrap();
    assert_eq!(
        resource
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("web-frontend-7d9f8-00001")
    );
    assert_eq!(
        resource.detail.as_ref().unwrap().identity.name,
        "web-frontend-7d9f8-00001"
    );
}

#[test]
fn hidden_resource_row_action_expires_once_at_table_scope() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.step();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSearch(window, "web-frontend".into()));
    harness.run_steps(10);
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .resource_state(window)
            .unwrap()
            .selection
            .is_none(),
        "the pending clear must execute while its row is filtered out"
    );

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend-7d9f8-00001")
        .click();
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSearch(window, String::new()));
    harness.run_steps(10);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .resource_state(window)
            .unwrap()
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("web-frontend-7d9f8-00001"),
        "restoring the old row must not replay its consumed clear"
    );
}

#[test]
fn virtualized_large_list_recycles_rows_and_keeps_interaction_correct() {
    let mut fixture = Fixture::default();
    fixture.feed.lists.insert(
        WorkspaceWorkload::Pods,
        (0..500)
            .map(|index| {
                list_row(
                    "",
                    "v1",
                    "Pod",
                    Some("default"),
                    &format!("pod-{index:03}"),
                    "Running",
                    "2026-08-21T00:00:00Z",
                )
            })
            .collect(),
    );
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkspaceWorkload::Pods),
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.0, 600.0))
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    let first = workload_window(&harness, "Pods");
    first
        .get_by_role_and_label(Role::Button, "Select resource pod-000")
        .click();
    harness.step();
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource pod-000")
        .scroll_down();
    harness.step();
    harness.run_steps(6);
    let recycled = workload_window(&harness, "Pods");
    assert!(
        recycled
            .query_by_role_and_label(Role::Button, "Select resource pod-000")
            .is_none()
    );
    assert!(
        (1..500).any(|index| recycled
            .query_by_role_and_label(Role::Button, &format!("Select resource pod-{index:03}"))
            .is_some()),
        "a recycled later row is rendered"
    );
    harness.run_steps(10);
    let selection = &harness
        .state()
        .shell
        .workspace()
        .resource_state(workload_id(harness.state(), WorkspaceWorkload::Pods))
        .unwrap()
        .selection;
    assert_eq!(
        selection
            .as_ref()
            .map(|identity| format!("Select resource {}", identity.name))
            .as_deref(),
        Some("Select resource pod-000")
    );
    let recycled = workload_window(&harness, "Pods");
    let later_index = (1..500)
        .find(|index| {
            recycled
                .query_by_role_and_label(Role::Button, &format!("Select resource pod-{index:03}"))
                .is_some()
        })
        .expect("a later recycled row is visible");
    let later_label = format!("Select resource pod-{later_index:03}");
    recycled
        .get_by_role_and_label(Role::Button, &later_label)
        .click();
    harness.run_steps(10);
    let expected_name = format!("pod-{later_index:03}");
    let selection = &harness
        .state()
        .shell
        .workspace()
        .resource_state(workload_id(harness.state(), WorkspaceWorkload::Pods))
        .unwrap()
        .selection;
    assert_eq!(
        selection.as_ref().map(|identity| identity.name.as_str()),
        Some(expected_name.as_str())
    );
}

#[test]
fn cross_row_resource_double_click_does_not_change_integrated_selection() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetActiveTab(
            window,
            k10s_ui::workspace::DetailTab::Yaml,
        ));
    harness.run_steps(2);

    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend-7d9f8-00001")
        .click();
    harness.step();
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend-7d9f8-00001")
        .click();
    harness.step();
    harness.run_steps(10);

    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .unwrap();
    assert_eq!(
        resource
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("db-postgres-0")
    );
    assert_eq!(
        resource.detail.as_ref().unwrap().active_tab,
        k10s_ui::workspace::DetailTab::Yaml
    );
    harness.get_by_role_and_label(Role::Window, "Pod · default / web-frontend-7d9f8-00001");
}

#[test]
fn resource_double_click_opens_dedicated_without_selecting_or_guarding() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let row = workload_window(&harness, "Pods")
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
fn selected_clean_resource_double_click_across_frames_preserves_detail() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetActiveTab(
            window,
            k10s_ui::workspace::DetailTab::Yaml,
        ));
    harness.run_steps(2);
    let row = workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0");
    row.click();
    harness.step();
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.step();
    harness.run_steps(4);

    let resource = harness
        .state()
        .shell
        .workspace()
        .resource_state(window)
        .unwrap();
    assert!(resource.selection.is_some());
    assert_eq!(
        resource.detail.as_ref().unwrap().active_tab,
        k10s_ui::workspace::DetailTab::Yaml
    );
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
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::BeginYamlEdit(window));
    let row = workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0");
    row.click();
    harness.step();
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Clear selection for resource db-postgres-0")
        .click();
    harness.step();
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
    workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(10);

    let window = workload_window(&harness, "Pods");
    let identity_row = window.get_by_label("Detail identity row");
    let close = window.get_by_role_and_label(Role::Button, "Clear selection");
    assert!(
        identity_row.rect().contains_rect(close.rect()),
        "Clear selection must be a compact control inside the Detail identity-row layout area"
    );
}

#[test]
fn first_deployment_window_uses_the_wide_canvas_for_its_integrated_detail() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let saved_geometry = fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.id == id)
        .expect("Deployments window is persisted")
        .geometry;
    let row = fixture.feed.lists[&WorkspaceWorkload::Deployments][0]
        .identity
        .clone();
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(id, row));
    harness.run_steps(4);

    assert!(
        workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::ScrollView, "Detail body")
            .rect()
            .width()
            >= 760.0,
        "a first Deployment window should use the wide canvas for its integrated Detail body"
    );
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| window.id == id)
            .expect("Deployments window remains persisted")
            .geometry,
        saved_geometry,
        "first-render sizing must not overwrite persisted geometry"
    );
}

#[test]
fn manually_supplied_deployment_geometry_remains_untouched_on_a_wide_canvas() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let manual_geometry = WindowGeom {
        position: [10.0, 30.0],
        size: [700.0, 480.0],
        collapsed: false,
    };
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(id, manual_geometry));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| window.id == id)
            .expect("Deployments window remains persisted")
            .geometry,
        manual_geometry,
        "an explicitly supplied geometry must not be replaced by first-render sizing"
    );
}

#[test]
fn first_deployment_resize_persists_after_the_wide_canvas_render() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    let rect = workload_window(&harness, "Deployments").rect();
    let target = rect.min + egui::vec2(680.0, 450.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let resized_size = workload_window(&harness, "Deployments").rect().size();
    let persisted_size = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.id == id)
        .expect("Deployments window remains persisted")
        .geometry
        .size;

    assert_eq!(
        persisted_size,
        [resized_size.x, resized_size.y],
        "a first-window resize must replace the temporary wide render geometry in the workspace"
    );
    assert_ne!(
        persisted_size,
        [700.0, 480.0],
        "a first-window resize must not leave the normal default geometry persisted"
    );
}

#[test]
fn sub_1000_viewport_keeps_the_first_deployment_detail_compact() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let row = fixture.feed.lists[&WorkspaceWorkload::Deployments][0]
        .identity
        .clone();
    let mut harness = Harness::builder()
        .with_size(egui::vec2(999.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(id, row));
    harness.run_steps(4);

    assert!(
        workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::ScrollView, "Detail body")
            .rect()
            .width()
            < 760.0,
        "the 1000-point first-render treatment must not apply below that viewport"
    );
}

#[test]
fn above_1000_viewport_uses_the_wide_first_deployment_detail() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let row = fixture.feed.lists[&WorkspaceWorkload::Deployments][0]
        .identity
        .clone();
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.5, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(id, row));
    harness.run_steps(4);

    assert!(
        workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::ScrollView, "Detail body")
            .rect()
            .width()
            >= 760.0,
        "a canvas that fits the wide first Deployment layout must use it"
    );
}

#[test]
fn sub_1000_first_deployment_does_not_expand_after_a_viewport_resize() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let row = fixture.feed.lists[&WorkspaceWorkload::Deployments][0]
        .identity
        .clone();
    let mut harness = Harness::builder()
        .with_size(egui::vec2(999.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness.set_size(egui::vec2(1_000.0, 700.0));
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(id, row));
    harness.run_steps(4);

    assert!(
        workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::ScrollView, "Detail body")
            .rect()
            .width()
            < 760.0,
        "the first-open decision must not be retroactively widened by a viewport resize"
    );
}

#[test]
fn viewport_shrink_does_not_persist_the_temporary_wide_deployment_geometry() {
    let mut fixture = Fixture::default();
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
        .expect("Deployments window opens");
    let saved_geometry = fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.id == id)
        .expect("Deployments window is persisted")
        .geometry;
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_000.0, 700.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness.set_size(egui::vec2(640.0, 700.0));
    harness.run_steps(4);

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .find(|window| window.id == id)
            .expect("Deployments window remains persisted")
            .geometry,
        saved_geometry,
        "canvas constraints after a viewport resize must not overwrite saved geometry"
    );
}

#[test]
fn integrated_detail_transitions_preserve_shared_workload_window_geometry() {
    for kind in [WorkspaceWorkload::Deployments, WorkspaceWorkload::Pods] {
        for size in [[700.0, 500.0], [640.0, 420.0]] {
            let mut fixture = Fixture::default();
            let id = fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                    LauncherItem::Workload(kind),
                ))
                .into_iter()
                .find_map(|event| match event {
                    k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
                    _ => None,
                })
                .expect("workload window opens");
            set_geometry(&mut fixture, id, size);
            fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::SetSplitRatio(id, 0.37));
            let rows = fixture.feed.lists[&kind].clone();
            let title = match kind {
                WorkspaceWorkload::Deployments => "Deployments",
                WorkspaceWorkload::Pods => "Pods",
                _ => unreachable!("the regression covers representative shared workload layouts"),
            };
            let mut harness = Harness::builder()
                .with_size(egui::vec2(1_440.0, 900.0))
                .with_pixels_per_point(1.0)
                .build_ui_state(render, fixture);
            harness.run_steps(4);
            let expected = harness
                .state()
                .shell
                .workspace()
                .windows()
                .iter()
                .find(|window| window.id == id)
                .expect("workload window remains open")
                .geometry;
            let expected_size = workload_window(&harness, title).rect().size();

            for command in [
                WorkspaceCommand::SelectRow(id, rows[0].identity.clone()),
                WorkspaceCommand::ClearSelection(id),
                WorkspaceCommand::SelectRow(id, rows[1].identity.clone()),
                WorkspaceCommand::MaximizeDetailPane(id),
                WorkspaceCommand::RestoreDetailPane(id),
            ] {
                harness.state_mut().shell.apply_workspace_command(command);
                harness.run_steps(4);
                let geometry = harness
                    .state()
                    .shell
                    .workspace()
                    .windows()
                    .iter()
                    .find(|window| window.id == id)
                    .expect("workload window remains open")
                    .geometry;
                assert_eq!(geometry, expected, "{title} geometry changed");
                assert!(
                    (workload_window(&harness, title).rect().size() - expected_size).length()
                        <= 1.0,
                    "{title} outer rectangle changed"
                );
            }

            let split_window = workload_window(&harness, title);
            let first_row_label = format!("Select resource {}", rows[0].identity.name);
            let list_anchor_before = split_window
                .get_by_role_and_label(Role::Button, &first_row_label)
                .rect();
            let detail_body_before = split_window
                .get_by_role_and_label(Role::ScrollView, "Detail body")
                .rect();
            assert!(
                detail_body_before.height() > 0.0
                    && detail_body_before.bottom() <= split_window.rect().bottom() + 1.0,
                "{title} detail overflow must remain in its finite scroll region"
            );

            harness
                .state_mut()
                .shell
                .apply_workspace_command(WorkspaceCommand::MaximizeDetailPane(id));
            harness.run_steps(4);
            let maximized = workload_window(&harness, title);
            assert!(
                maximized
                    .query_by_role_and_label(Role::Button, &first_row_label)
                    .is_none(),
                "{title} maximize must reallocate the interior away from the list"
            );
            maximized.get_by_role_and_label(Role::ScrollView, "Detail body");

            harness
                .state_mut()
                .shell
                .apply_workspace_command(WorkspaceCommand::RestoreDetailPane(id));
            harness.run_steps(4);
            let restored = workload_window(&harness, title);
            let list_anchor_after = restored
                .get_by_role_and_label(Role::Button, &first_row_label)
                .rect();
            let detail_body_after = restored
                .get_by_role_and_label(Role::ScrollView, "Detail body")
                .rect();
            assert!(
                (list_anchor_after.min - list_anchor_before.min).length() <= 1.0
                    && (detail_body_after.min - detail_body_before.min).length() <= 1.0
                    && (detail_body_after.size() - detail_body_before.size()).length() <= 1.0,
                "{title} restore must recover the prior list/detail allocation"
            );
        }
    }
}

#[test]
fn snapshot_resync_replaces_rows_while_preserving_filters_and_selection() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);
    let window = workload_window(&harness, "Deployments");
    window
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .type_text("web");
    harness.run_steps(4);
    let window = workload_window(&harness, "Deployments");
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

    let window = workload_window(&harness, "Deployments");
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
        .get_by_role_and_label(Role::Button, "More list controls")
        .click();
    harness.step();
    harness.get_by_role_and_label(Role::Button, "Reset").click();
    harness.run_steps(4);
    let window = workload_window(&harness, "Deployments");
    window.get_by_label("Select resource api-server");
    window.get_by_label("Select resource checkout");
}
