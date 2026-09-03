//! The singleton Services window: the Network launcher group, list columns
//! rendered strictly from normalized `ResourceListRow` projections (never
//! from `summary`), loading/empty/filtered/stale/gone states, structured
//! desktop capability-gated port-forward controls, and accessibility names.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, GroupVersionKind, PortForwardPodTarget, PortForwardPortSelector,
    PortForwardSession, PortForwardSessionId, PortForwardSessionState, PortForwardTarget,
    ResourceCapabilities, ResourceDetailResponse, ResourceIdentity, ResourceListRow,
    ResourceProjection, ServicePort, ServiceProjection, TargetPort, TransportProtocol,
};
use k10s_ui::{
    ui::{
        ConnectionState, PrimaryDetailState, ResourceAction, ResourceFeed, SafeUiError, UiShell,
        WindowFreshness,
    },
    workspace::{WindowGeom, WindowId, WorkspaceCommand},
};
use std::collections::BTreeMap;

mod common;

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
    connection: ConnectionState,
    context_namespace: Option<String>,
}

impl Default for Fixture {
    fn default() -> Self {
        let mut fixture = Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
            connection: ConnectionState::Connected,
            context_namespace: None,
        };
        fixture.feed.render_time =
            Some(web_time::UNIX_EPOCH + web_time::Duration::from_secs(1_788_220_800));
        fixture.feed.services = Some(vec![
            service_row(
                "web-frontend",
                Some(ServiceProjection {
                    service_type: "ClusterIP".into(),
                    cluster_ips: vec!["10.96.0.10".into()],
                    selector: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
                    external_name: None,
                    session_affinity: None,
                    external_traffic_policy: None,
                    internal_traffic_policy: None,
                    ports: vec![ServicePort {
                        name: Some("http".into()),
                        service_port: 80,
                        target_port: TargetPort::Number { number: 8080 },
                        node_port: None,
                        protocol: TransportProtocol::Tcp,
                        app_protocol: None,
                    }],
                }),
                "2026-08-21T00:00:00Z",
            ),
            service_row(
                "api-server",
                Some(ServiceProjection {
                    service_type: "ClusterIP".into(),
                    cluster_ips: vec!["10.96.0.20".into()],
                    selector: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    external_name: None,
                    session_affinity: Some("ClientIP".into()),
                    external_traffic_policy: None,
                    internal_traffic_policy: None,
                    ports: vec![
                        ServicePort {
                            name: Some("https".into()),
                            service_port: 443,
                            target_port: TargetPort::Name {
                                name: "https".into(),
                            },
                            node_port: None,
                            protocol: TransportProtocol::Tcp,
                            app_protocol: Some("https".into()),
                        },
                        ServicePort {
                            name: Some("metrics".into()),
                            service_port: 9100,
                            target_port: TargetPort::Number { number: 9100 },
                            node_port: None,
                            protocol: TransportProtocol::Udp,
                            app_protocol: None,
                        },
                    ],
                }),
                "2026-08-21T00:05:00Z",
            ),
        ]);
        fixture
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
        fixture.connection,
        &contexts,
        &mut selected_context,
        None,
        &fixture.feed,
    );
}

use common::namespace_combobox;

#[test]
fn service_namespace_combobox_only_selects_ready_catalog_values() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog =
        k10s_ui::ui::NamespaceCatalogState::Ready(vec!["default".into(), "team-b".into()]);
    let window = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            k10s_ui::workspace::LauncherItem::Services,
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
    let window_node = harness.get_by_role_and_label(Role::Window, "Services");
    assert!(
        window_node
            .query_by_role_and_label(Role::TextInput, "Namespace filter")
            .is_none()
    );
    namespace_combobox(window_node).click();
    harness.run_steps(3);
    let search = harness.get_by_role_and_label(Role::TextInput, "Search namespaces");
    search.type_text("TEAM");
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "team-b")
        .click();
    harness.run_steps(2);
    assert!(matches!(
        &harness.state().shell.workspace().window(window).unwrap().content,
        k10s_ui::workspace::WindowContent::Services(state)
            if state.namespace_scope == k10s_ui::workspace::NamespaceScope::Namespace("team-b".into())
    ));
}

#[test]
fn missing_service_namespace_is_reported_without_broadening() {
    let mut fixture = Fixture::default();
    fixture.feed.namespace_catalog =
        k10s_ui::ui::NamespaceCatalogState::Ready(vec!["default".into()]);
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            k10s_ui::workspace::LauncherItem::Services,
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
    assert_eq!(
        namespace_combobox(harness.root()).value().as_deref(),
        Some("Namespace: deleted-team · no longer exists")
    );
    assert!(matches!(
        &harness.state().shell.workspace().window(id).unwrap().content,
        k10s_ui::workspace::WindowContent::Services(state)
            if state.namespace_scope == k10s_ui::workspace::NamespaceScope::Namespace("deleted-team".into())
    ));
    namespace_combobox(harness.root()).click();
    harness.run_steps(2);
    harness
        .get_by_role_and_label(Role::Button, "All namespaces")
        .click();
    harness.run_steps(2);
    assert!(matches!(
        &harness.state().shell.workspace().window(id).unwrap().content,
        k10s_ui::workspace::WindowContent::Services(state)
            if state.namespace_scope == k10s_ui::workspace::NamespaceScope::AllNamespaces
    ));
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .with_step_dt(0.05)
        .build_ui_state(render, Fixture::default())
}

fn service_row(
    name: &str,
    projection: Option<ServiceProjection>,
    created_at: &str,
) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind::core("v1", "Service"),
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-service-default-{name}"),
        },
        revision: BackendRevision::new(1_000),
        labels: Default::default(),
        // Deliberately misleading: the table must never fall back to it.
        summary: "NEVER-SUMMARY".to_owned(),
        created_at: created_at.to_owned(),
        projection: projection.map(ResourceProjection::Service),
    }
}

fn services_window_id(fixture: &Fixture) -> WindowId {
    fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == k10s_ui::workspace::WindowKind::Services)
        .expect("the Services window is open")
        .id
}

fn open_via_launcher(harness: &mut Harness<'static, Fixture>) {
    harness
        .get_by_role_and_label(Role::Button, "Services")
        .click();
    harness.run_steps(8);
}

#[test]
fn responsive_service_headers_hide_order_tooltip_and_sort_affordances() {
    let mut fixture = Fixture::default();
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            k10s_ui::workspace::LauncherItem::Services,
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
    let wide = harness.get_by_role_and_label(Role::Window, "Services");
    // The Namespace label lives in the table header; the toolbar selector
    // carries its own label inside the control text.
    wide.get_by_label("Namespace");
    for header in ["Name", "Type", "Cluster IP", "Ports", "Age"] {
        wide.get_by_label(header);
    }
    for key in ["namespace", "name", "type", "cluster_ip", "ports", "age"] {
        wide.get_by_role_and_label(Role::Button, format!("Sort services by {key}").as_str());
    }
    let compact_port = wide.get_by_label("https 443→https/TCP, metrics 9100→9100/UDP");
    compact_port.hover();
    harness.run_steps(15);
    assert!(
        harness
            .get_all_by_label("https 443→https/TCP, metrics 9100→9100/UDP")
            .count()
            >= 2
    );
    harness.get_by_label("http 80→8080/TCP").hover();
    harness.run_steps(15);
    assert_eq!(
        harness.get_all_by_label("http 80→8080/TCP").count(),
        1,
        "short Ports values have no redundant tooltip"
    );

    let rect = harness
        .get_by_role_and_label(Role::Window, "Services")
        .rect();
    let target = rect.min + egui::vec2(640.0, 520.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(3);
    let compact = harness.get_by_role_and_label(Role::Window, "Services");
    assert!(compact.query_by_label("Cluster IP").is_none());
    assert!(compact.query_by_label("Type").is_none());
    for key in ["namespace", "name", "ports", "age"] {
        compact.get_by_role_and_label(Role::Button, format!("Sort services by {key}").as_str());
    }
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
    let restored = harness.get_by_role_and_label(Role::Window, "Services");
    restored.get_by_label("Cluster IP");
    restored.get_by_label("Type");
}

#[test]
fn network_launcher_group_offers_the_services_singleton() {
    let mut harness = harness();
    harness.run_steps(2);

    // The Network group header and its singleton entry exist.
    harness.get_by_role_and_label(Role::Button, "Network");
    open_via_launcher(&mut harness);

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == k10s_ui::workspace::WindowKind::Services)
            .count(),
        1,
        "the launcher opens exactly one Services singleton"
    );

    // Activating the launcher entry again keeps the single window focused.
    open_via_launcher(&mut harness);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == k10s_ui::workspace::WindowKind::Services)
            .count(),
        1
    );
}

#[test]
fn service_details_share_integrated_chrome_but_dedicated_windows_hide_pane_actions() {
    let mut harness = harness();
    let service = harness.state().feed.services.as_ref().unwrap()[0]
        .identity
        .clone();
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(10);

    let integrated = harness.get_by_role_and_label(Role::Window, "Services");
    integrated.get_by_role_and_label(Role::Button, "Pop out ↗");
    integrated.get_by_role_and_label(Role::Button, "Maximize");
    integrated.get_by_label("y yaml · e events · c copy name · Esc clear selection");
    assert_eq!(integrated.query_all_by_role(Role::ScrollView).count(), 1);
    assert!(
        integrated.rect().contains_rect(
            integrated
                .get_by_label("y yaml · e events · c copy name · Esc clear selection",)
                .rect()
        )
    );

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(service));
    harness.run_steps(4);
    let dedicated = harness.get_by_role_and_label(Role::Window, "Service · default / web-frontend");
    assert!(
        dedicated
            .query_by_role_and_label(Role::Button, "Pop out ↗")
            .is_none()
    );
    assert!(
        dedicated
            .query_by_role_and_label(Role::Button, "Maximize")
            .is_none()
    );
}

#[test]
fn selected_service_single_click_eventually_clears_selection_once() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    let row_label = "Select service web-frontend";

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, row_label)
        .click();
    harness.run_steps(10);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend")
        .click();
    harness.run_steps(10);

    let service = harness
        .state()
        .shell
        .workspace()
        .service_state(window)
        .expect("Services window has service state");
    assert!(service.selection.is_none());
    assert!(service.detail.is_none());
    assert!(harness.state().shell.workspace().pending().is_none());
}

#[test]
fn clicking_another_service_replaces_the_pending_row_action() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
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

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend")
        .click();
    harness.step();
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service api-server")
        .click();
    harness.run_steps(10);

    let service = harness
        .state()
        .shell
        .workspace()
        .service_state(window)
        .unwrap();
    assert_eq!(
        service
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("api-server")
    );
    assert_eq!(service.detail.as_ref().unwrap().identity.name, "api-server");
}

#[test]
fn hidden_service_row_action_expires_once_at_table_scope() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(10);

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend")
        .click();
    harness.step();
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetSearch(window, "api-server".into()));
    harness.run_steps(10);
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .service_state(window)
            .unwrap()
            .selection
            .is_none(),
        "the pending clear must execute while its row is filtered out"
    );

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service api-server")
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
            .service_state(window)
            .unwrap()
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("api-server"),
        "restoring the old row must not replay its consumed clear"
    );
}

#[test]
fn cross_row_service_double_click_does_not_change_integrated_selection() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
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

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service api-server")
        .click();
    harness.step();
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service api-server")
        .click();
    harness.step();
    harness.run_steps(10);

    let service = harness
        .state()
        .shell
        .workspace()
        .service_state(window)
        .unwrap();
    assert_eq!(
        service
            .selection
            .as_ref()
            .map(|identity| identity.name.as_str()),
        Some("web-frontend")
    );
    assert_eq!(
        service.detail.as_ref().unwrap().active_tab,
        k10s_ui::workspace::DetailTab::Yaml
    );
    harness.get_by_role_and_label(Role::Window, "Service · default / api-server");
}

#[test]
fn service_double_click_opens_dedicated_without_selecting_or_guarding() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    let row = harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend");
    row.click();
    row.click();
    harness.run_steps(4);

    assert!(
        harness
            .state()
            .shell
            .workspace()
            .service_state(window)
            .unwrap()
            .selection
            .is_none()
    );
    assert!(harness.state().shell.workspace().pending().is_none());
    harness.get_by_role_and_label(Role::Window, "Service · default / web-frontend");
}

#[test]
fn selected_clean_service_double_click_across_frames_preserves_detail() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
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
    let row = harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend");
    row.click();
    harness.step();
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend")
        .click();
    harness.step();
    harness.run_steps(4);

    let service = harness
        .state()
        .shell
        .workspace()
        .service_state(window)
        .unwrap();
    assert!(service.selection.is_some());
    assert_eq!(
        service.detail.as_ref().unwrap().active_tab,
        k10s_ui::workspace::DetailTab::Yaml
    );
    harness.get_by_role_and_label(Role::Window, "Service · default / web-frontend");
}

#[test]
fn selected_dirty_service_double_click_preserves_selection_and_skips_guard() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    harness.run_steps(10);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::BeginYamlEdit(window));
    let row = harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend");
    row.click();
    harness.step();
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Clear selection for service web-frontend")
        .click();
    harness.step();
    harness.run_steps(4);

    assert!(
        harness
            .state()
            .shell
            .workspace()
            .service_state(window)
            .unwrap()
            .selection
            .is_some()
    );
    assert!(harness.state().shell.workspace().pending().is_none());
    harness.get_by_role_and_label(Role::Window, "Service · default / web-frontend");
}

#[test]
fn service_selection_derives_detail_visibility() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleDetailPane(window));
    harness.run_steps(2);

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(10);

    let services = harness.get_by_role_and_label(Role::Window, "Services");
    services.get_by_label("Service · default / web-frontend");
    assert!(
        services
            .query_by_role_and_label(Role::Button, "Hide details")
            .is_none(),
        "selection-derived Detail visibility removes the legacy toggle"
    );
}

#[test]
fn list_columns_render_strictly_from_projections() {
    let mut harness = harness();
    open_via_launcher(&mut harness);

    let window = harness.get_by_role_and_label(Role::Window, "Services");
    for header in ["Name", "Type", "Cluster IP", "Ports", "Age"] {
        window.get_by_label(header);
    }
    namespace_combobox(window);
    for key in ["name", "namespace", "type", "cluster_ip", "ports", "age"] {
        window.get_by_role_and_label(Role::Button, format!("Sort services by {key}").as_str());
    }

    // Structured cells come only from the projection.
    assert_eq!(
        window.get_all_by_label("ClusterIP").count(),
        2,
        "both rows render their Service type strictly from the projection"
    );
    window.get_by_label("10.96.0.10");
    window.get_by_label("http 80→8080/TCP");
    // Multi-port rows join their compact labels.
    window.get_by_label("https 443→https/TCP, metrics 9100→9100/UDP");
    assert_eq!(window.get_all_by_label("Service age").count(), 2);
    assert!(
        window
            .get_all_by_label("Service age")
            .all(|node| node.rect().width() <= 56.0),
        "Service Age values fit their compact column"
    );

    // The summary text is never rendered anywhere in this window.
    assert!(
        window.query_by_label("NEVER-SUMMARY").is_none(),
        "the Services table must never parse or render summary"
    );
}

#[test]
fn loading_empty_and_filtered_states_are_distinct() {
    let mut harness = harness();
    harness.state_mut().feed.services = None;
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_label("Loading services");

    // Zero authoritative rows: a plain empty state.
    harness.state_mut().feed.services = Some(Vec::new());
    let id = services_window_id(harness.state());
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            id,
            k10s_ui::workspace::NamespaceScope::AllNamespaces,
        ));
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_label("No services");

    // Rows that exist but are filtered away by the namespace filter.
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetNamespaceScope(
            id,
            k10s_ui::workspace::NamespaceScope::Namespace("kube-system".to_owned()),
        ));
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_label("No services match the current filters");
}

#[test]
fn stale_connection_shows_the_banner() {
    let mut harness = harness();
    harness.state_mut().connection = ConnectionState::Failed;
    open_via_launcher(&mut harness);

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_label("[~] Reconnecting · last sync unknown · retry in pending · attempt 1");
}

#[test]
fn service_uses_yaml_tab_without_a_duplicate_edit_yaml_action() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    let window = services_window_id(harness.state());
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    harness.state_mut().feed.window_freshness.insert(
        window,
        WindowFreshness::Failed {
            message: "watch ended".into(),
        },
    );
    harness.run_steps(4);

    let detail = harness.get_by_role_and_label(Role::Window, "Services");
    detail.get_by_role_and_label(Role::Button, "Tab YAML");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Edit YAML")
            .is_none()
    );
}

#[test]
fn udp_and_sctp_ports_render_readonly_without_start_controls() {
    let mut harness = harness();
    let mut sctp_projection = ServiceProjection {
        service_type: "ClusterIP".into(),
        cluster_ips: vec!["10.96.0.30".into()],
        selector: BTreeMap::new(),
        external_name: None,
        session_affinity: None,
        external_traffic_policy: None,
        internal_traffic_policy: None,
        ports: vec![ServicePort {
            name: Some("sync".into()),
            service_port: 9101,
            target_port: TargetPort::Name {
                name: "data".into(),
            },
            node_port: None,
            protocol: TransportProtocol::Sctp,
            app_protocol: None,
        }],
    };
    sctp_projection
        .selector
        .insert("app".to_owned(), "sync".to_owned());
    harness.state_mut().feed.services = Some(vec![service_row(
        "mesh-sync",
        Some(sctp_projection),
        "2026-08-21T00:10:00Z",
    )]);
    open_via_launcher(&mut harness);

    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("sync 9101→data/SCTP");

    // No port-forward controls exist anywhere on this surface yet.
    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Start")
            .is_none(),
        "port-forward Start must not render before its capability task"
    );
    assert!(harness.query_by_label("Stop").is_none());
}

#[test]
fn selecting_a_service_shows_integrated_detail_with_service_tabs() {
    let mut harness = harness();
    open_via_launcher(&mut harness);

    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(8);

    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("Service · default / web-frontend");
    for tab in ["Tab Overview", "Tab Ports", "Tab Events", "Tab YAML"] {
        window.get_by_role_and_label(Role::Button, tab);
    }
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Tab Pods")
            .is_none(),
        "services never offer workload tabs"
    );
    // Before the backend response resolves, the pane shows its loading state.
    window.get_by_label("Loading details");

    // The Ports tab renders structured labels from the detail projection.
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Tab Ports")
        .click();
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Services");
    // Accessible names exist for every port row.
    window.get_by_role_and_label(Role::Label, "Port http · 80 → 8080 · TCP");
}

#[test]
fn failed_service_detail_is_safe_and_retries_the_exact_identity_once() {
    let mut harness = harness();
    let identity = service_identity("web-frontend");
    harness.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("service detail denied")),
    );
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(10);

    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("Details unavailable: service detail denied");
    assert!(window.query_by_label("Loading details").is_none());
    window
        .get_by_role_and_label(Role::Button, "Retry details")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_resource_actions(),
        vec![ResourceAction::RetryPrimary(identity)]
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
fn desktop_capability_renders_start_and_opens_the_shared_service_target() {
    let mut harness = harness();
    harness.state_mut().feed.port_forward_available = true;
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    let window_id = services_window_id(harness.state());
    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Button, "Tab Ports")
        .click();
    harness.run_steps(4);
    harness.get_by_role_and_label(Role::Button, "Start").click();
    harness.run_steps(2);
    let actions = harness.state_mut().shell.drain_port_forward_actions();
    assert!(matches!(
        actions.as_slice(),
        [k10s_ui::ui::PortForwardAction::OpenStart {
            target: k10s_protocol::PortForwardTarget::Service {
                identity,
                port: k10s_protocol::PortForwardPortSelector::Name { name },
            },
            initial_local_port: 8080,
            ..
        }] if identity == &service_identity("web-frontend") && name == "http"
    ));
}

#[test]
fn named_service_target_prefills_the_declared_service_port_in_the_shared_modal() {
    let mut harness = harness();
    harness.state_mut().feed.port_forward_available = true;
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    let mut detail = service_detail("web-frontend", false);
    let Some(ResourceProjection::Service(service)) = detail.projection.as_mut() else {
        panic!("fixture has a typed Service projection");
    };
    service.ports[0].target_port = TargetPort::Name {
        name: "http-backend".into(),
    };
    harness
        .state_mut()
        .feed
        .details
        .insert(service_identity("web-frontend"), detail);
    let window_id = services_window_id(harness.state());
    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Button, "Tab Ports")
        .click();
    harness.run_steps(4);
    assert!(
        harness
            .query_by_role_and_label(Role::TextInput, "Local port (blank = automatic)")
            .is_none(),
        "the Ports tab delegates blank/zero validation to the shared modal"
    );
    harness.get_by_role_and_label(Role::Button, "Start").click();
    harness.run_steps(2);
    assert!(matches!(
        harness
            .state_mut()
            .shell
            .drain_port_forward_actions()
            .as_slice(),
        [k10s_ui::ui::PortForwardAction::OpenStart {
            target: k10s_protocol::PortForwardTarget::Service {
                identity,
                port: k10s_protocol::PortForwardPortSelector::Name { name },
            },
            initial_local_port: 80,
            ..
        }] if identity == &service_identity("web-frontend") && name == "http"
    ));
}

#[test]
fn terminal_only_port_forward_exposes_start_instead_of_stop() {
    let mut harness = harness();
    harness.state_mut().feed.port_forward_available = true;
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    let window_id = services_window_id(harness.state());
    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    harness.state_mut().feed.port_forward_sessions = vec![service_port_forward_session(
        "stopped",
        PortForwardSessionState::Stopped,
        1,
    )];
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Button, "Tab Ports")
        .click();
    harness.run_steps(4);

    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Stop")
            .is_none()
    );
    harness.get_by_role_and_label(Role::Button, "Start");
}

#[test]
fn active_port_forward_is_not_shadowed_by_an_older_terminal_session() {
    let mut harness = harness();
    harness.state_mut().feed.port_forward_available = true;
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(4);
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    let window_id = services_window_id(harness.state());
    harness.state_mut().feed.window_freshness.insert(
        window_id,
        WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
    );
    let mut active = service_port_forward_session("active", PortForwardSessionState::Active, 2);
    active.local_addr = "127.0.0.1:18081".into();
    harness.state_mut().feed.port_forward_sessions = vec![
        service_port_forward_session("stopped", PortForwardSessionState::Stopped, 1),
        active,
    ];
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Button, "Tab Ports")
        .click();
    harness.run_steps(4);

    assert!(
        harness
            .query_by_role_and_label(Role::Button, "Start")
            .is_none()
    );
    harness.get_by_label("127.0.0.1:18081 · web-0:8080 · Active");
    harness.get_by_role_and_label(Role::Button, "Stop").click();
    harness.run_steps(2);
    assert_eq!(
        harness.state_mut().shell.drain_port_forward_actions(),
        vec![k10s_ui::ui::PortForwardAction::Stop("active".into())]
    );

    harness.state_mut().feed.port_forward_sessions = vec![service_port_forward_session(
        "stopping",
        PortForwardSessionState::Stopping,
        3,
    )];
    harness.run_steps(3);
    assert!(
        harness
            .get_by_role_and_label(Role::Button, "Stop")
            .accesskit_node()
            .is_disabled(),
        "a draining session remains visible but cannot be stopped twice"
    );
}

#[test]
fn port_forward_start_requires_live_loaded_service_authority() {
    let states = [
        WindowFreshness::StaleRetrying {
            last_sync_age: "30s".into(),
            retry_in: "2s".into(),
            attempt: 1,
        },
        WindowFreshness::Reconnecting {
            last_sync_age: "30s".into(),
            retry_in: "2s".into(),
            attempt: 1,
        },
        WindowFreshness::Failed {
            message: "watch failed".into(),
        },
        WindowFreshness::Forbidden {
            user: "alice".into(),
            verb: "list".into(),
            resource: "services".into(),
            scope: "default".into(),
        },
    ];
    for freshness in states {
        let mut harness = harness();
        harness.state_mut().feed.port_forward_available = true;
        open_via_launcher(&mut harness);
        let window_id = services_window_id(harness.state());
        harness
            .get_by_role_and_label(Role::Window, "Services")
            .get_by_role_and_label(Role::Button, "Select service web-frontend")
            .click();
        harness.run_steps(4);
        harness.state_mut().feed.details.insert(
            service_identity("web-frontend"),
            service_detail("web-frontend", false),
        );
        harness.state_mut().feed.port_forward_sessions = vec![PortForwardSession {
            id: PortForwardSessionId::try_new("existing").unwrap(),
            target: PortForwardTarget::Service {
                identity: service_identity("web-frontend"),
                port: PortForwardPortSelector::Number { number: 80 },
            },
            requested_local_port: 18_080,
            pod: PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-0".into(),
                uid: "pod-uid".into(),
            },
            pod_port: 8080,
            local_addr: "127.0.0.1:18080".into(),
            state: PortForwardSessionState::Active,
            failure: None,
            revision: 1,
        }];
        harness
            .state_mut()
            .feed
            .window_freshness
            .insert(window_id, freshness);
        harness.run_steps(4);
        harness
            .get_by_role_and_label(Role::Button, "Tab Ports")
            .click();
        harness.run_steps(3);
        assert!(
            harness
                .query_by_role_and_label(Role::Button, "Start")
                .is_none()
        );
        harness.get_by_role_and_label(Role::Button, "Stop");
    }

    let mut loading = harness();
    loading.state_mut().feed.port_forward_available = true;
    open_via_launcher(&mut loading);
    loading
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    loading.run_steps(4);
    loading.state_mut().feed.primary_details.insert(
        service_identity("web-frontend"),
        PrimaryDetailState::Loading,
    );
    loading.run_steps(3);
    assert!(
        loading
            .query_by_role_and_label(Role::Button, "Start")
            .is_none()
    );
}

#[test]
fn overview_traffic_policy_fields_appear_only_when_present() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(8);

    // Without policies nothing renders.
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("Type ClusterIP");
    window.get_by_label("Cluster IPs 10.96.0.10");
    window.get_by_label("Selector: app=web");
    assert!(window.query_by_label("TRAFFIC & SESSION").is_none());
    assert!(
        window
            .query_by_label("External traffic policy Local")
            .is_none(),
        "absent traffic policies must not render"
    );

    // With policies present they render verbatim.
    harness
        .state_mut()
        .feed
        .details
        .remove(&service_identity("web-frontend"));
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", true),
    );
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("Session affinity ClientIP");
    window.get_by_label("External policy Local");
    window.get_by_label("Internal policy Cluster");
}

#[test]
fn empty_service_configuration_sections_collapse_completely() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(8);
    let mut detail = service_detail("web-frontend", false);
    let Some(ResourceProjection::Service(service)) = detail.projection.as_mut() else {
        panic!("typed service fixture");
    };
    service.selector.clear();
    harness
        .state_mut()
        .feed
        .details
        .insert(service_identity("web-frontend"), detail);
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Services");
    assert!(window.query_by_label("SELECTORS").is_none());
    assert!(window.query_by_label("TRAFFIC & SESSION").is_none());
    window.get_by_label("IDENTITY");
}

#[test]
fn service_actual_route_renders_exact_1000_and_640_semantics_in_one_harness() {
    let mut fixture = Fixture::default();
    for (name, width, x) in [("wide", 1_024.0, 10.0), ("narrow", 664.0, 1_050.0)] {
        let identity = service_identity(name);
        fixture
            .feed
            .details
            .insert(identity.clone(), service_detail(name, true));
        let id = fixture
            .shell
            .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity))
            .into_iter()
            .find_map(|event| match event {
                k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
                _ => None,
            })
            .unwrap();
        fixture
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
    let identity = service_identity("untyped");
    let mut untyped = service_detail("untyped", false);
    untyped.projection = None;
    fixture.feed.details.insert(identity.clone(), untyped);
    let id = fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity))
        .into_iter()
        .find_map(|event| match event {
            k10s_ui::workspace::WorkspaceEvent::Opened(id) => Some(id),
            _ => None,
        })
        .unwrap();
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [1_050.0, 400.0],
                size: [664.0, 360.0],
                collapsed: false,
            },
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(1_800.0, 800.0))
        .build_ui_state(render, fixture);
    harness.run_steps(5);
    let wide = harness.get_by_role_and_label(Role::Window, "Service · default / wide");
    let op = wide.get_by_label("Operational detail column").rect();
    let config = wide.get_by_label("Configuration detail column").rect();
    assert!((op.width() / config.width() - 1.35).abs() < 0.02);
    let narrow = harness.get_by_role_and_label(Role::Window, "Service · default / narrow");
    assert!(
        narrow.get_by_label("PORTS").rect().top() < narrow.get_by_label("SELECTORS").rect().top()
    );
    assert!(
        narrow.get_by_label("SELECTORS").rect().top()
            < narrow.get_by_label("IDENTITY").rect().top()
    );
    narrow.get_by_role_and_label(Role::Button, "Tab Overview");
    let untyped = harness.get_by_role_and_label(Role::Window, "Service · default / untyped");
    assert_eq!(
        untyped
            .get_all_by_label("Structured details unavailable")
            .count(),
        2
    );
    untyped.get_by_label("UID uid-dev-local-service-default-untyped");
}

#[test]
fn gone_selection_renders_no_longer_exists() {
    let mut harness = harness();
    harness.state_mut().feed.details.insert(
        service_identity("web-frontend"),
        service_detail("web-frontend", false),
    );
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service web-frontend")
        .click();
    harness.run_steps(8);

    // The authoritative watch drops the pinned row.
    harness.state_mut().feed.services = Some(Vec::new());
    harness.run_steps(4);
    let window = harness.get_by_role_and_label(Role::Window, "Services");
    window.get_by_label("This resource no longer exists");
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Pop out ↗")
            .is_none(),
        "a cached gone detail cannot be popped into a stale dedicated window"
    );
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Maximize")
            .is_none()
    );
    harness.key_press_modifiers(egui::Modifiers::CTRL, egui::Key::Enter);
    harness.run_steps(3);
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .all(|window| window.kind != k10s_ui::workspace::WindowKind::Detail),
        "modified Enter cannot pop a cached gone service"
    );
}

#[test]
fn row_context_menu_pops_out_a_dedicated_service_window() {
    let mut harness = harness();
    open_via_launcher(&mut harness);
    harness
        .get_by_role_and_label(Role::Window, "Services")
        .get_by_role_and_label(Role::Button, "Select service api-server")
        .click_secondary();
    harness.run_steps(4);
    harness.get_by_label("Open dedicated window").click();
    harness.run_steps(8);

    let workspace = harness.state().shell.workspace();
    let dedicated = workspace
        .windows()
        .iter()
        .filter(|window| window.kind == k10s_ui::workspace::WindowKind::Detail)
        .count();
    assert_eq!(dedicated, 1, "a pop-out opens one dedicated detail");
}

fn service_identity(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: CONTEXT.to_owned(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".to_owned()),
        name: name.to_owned(),
        uid: format!("uid-{CONTEXT}-service-default-{name}"),
    }
}

fn service_port_forward_session(
    id: &str,
    state: PortForwardSessionState,
    revision: u64,
) -> PortForwardSession {
    PortForwardSession {
        id: PortForwardSessionId::try_new(id).unwrap(),
        target: PortForwardTarget::Service {
            identity: service_identity("web-frontend"),
            port: PortForwardPortSelector::Number { number: 80 },
        },
        requested_local_port: 18_080,
        pod: PortForwardPodTarget {
            namespace: "default".into(),
            name: "web-0".into(),
            uid: "pod-uid".into(),
        },
        pod_port: 8080,
        local_addr: "127.0.0.1:18080".into(),
        state,
        failure: None,
        revision,
    }
}

/// A core/v1 Service detail response carrying its normalized projection;
/// `policies` toggles the optional traffic-policy fields.
fn service_detail(name: &str, policies: bool) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: service_identity(name),
        revision: BackendRevision::new(1_010),
        created_at: "2026-08-21T00:00:00Z".to_owned(),
        owner_references: Vec::new(),
        sections: Vec::new(),
        events_condition: k10s_protocol::EventsCondition::Available,
        events: Vec::new(),
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_edit_yaml: true,
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: v1\nkind: Service\nmetadata:\n  name: {name}\n"),
        projection: Some(ResourceProjection::Service(ServiceProjection {
            service_type: "ClusterIP".into(),
            cluster_ips: vec!["10.96.0.10".into()],
            selector: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
            external_name: None,
            session_affinity: if policies {
                Some("ClientIP".into())
            } else {
                None
            },
            external_traffic_policy: if policies { Some("Local".into()) } else { None },
            internal_traffic_policy: if policies {
                Some("Cluster".into())
            } else {
                None
            },
            ports: vec![ServicePort {
                name: Some("http".into()),
                service_port: 80,
                target_port: TargetPort::Number { number: 8080 },
                node_port: None,
                protocol: TransportProtocol::Tcp,
                app_protocol: None,
            }],
        })),
    }
}
