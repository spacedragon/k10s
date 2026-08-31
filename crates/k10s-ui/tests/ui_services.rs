//! The singleton Services window: the Network launcher group, list columns
//! rendered strictly from normalized `ResourceListRow` projections (never
//! from `summary`), loading/empty/filtered/stale/gone states, structured
//! desktop capability-gated port-forward controls, and accessibility names.

use egui::accesskit::Role;
use egui_kittest::{Harness, kittest::Queryable as _};
use k10s_protocol::{
    BackendRevision, GroupVersionKind, ResourceCapabilities, ResourceDetailResponse,
    ResourceIdentity, ResourceListRow, ResourceProjection, ServicePort, ServiceProjection,
    TargetPort, TransportProtocol,
};
use k10s_ui::{
    ui::{
        ConnectionState, PrimaryDetailState, ResourceAction, ResourceFeed, SafeUiError, UiShell,
        WindowFreshness,
    },
    workspace::{WindowId, WorkspaceCommand},
};
use std::collections::BTreeMap;

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
    window_node
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
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
        harness
            .get_by_role_and_label(Role::ComboBox, "Namespace")
            .value()
            .as_deref(),
        Some("deleted-team · namespace no longer exists")
    );
    assert!(matches!(
        &harness.state().shell.workspace().window(id).unwrap().content,
        k10s_ui::workspace::WindowContent::Services(state)
            if state.namespace_scope == k10s_ui::workspace::NamespaceScope::Namespace("deleted-team".into())
    ));
    harness
        .get_by_role_and_label(Role::ComboBox, "Namespace")
        .click();
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
    harness.run_steps(4);

    let integrated = harness.get_by_role_and_label(Role::Window, "Services");
    integrated.get_by_role_and_label(Role::Button, "Pop out ↗");
    integrated.get_by_role_and_label(Role::Button, "Maximize");
    integrated.get_by_label(
        "Shortcuts: l Logs · p Pods · s Shell · y YAML · e Events · Esc restore/close",
    );
    assert_eq!(integrated.query_all_by_role(Role::ScrollView).count(), 1);
    assert!(
        integrated.rect().contains_rect(
            integrated
                .get_by_label(
                    "Shortcuts: l Logs · p Pods · s Shell · y YAML · e Events · Esc restore/close",
                )
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
fn list_columns_render_strictly_from_projections() {
    let mut harness = harness();
    open_via_launcher(&mut harness);

    let window = harness.get_by_role_and_label(Role::Window, "Services");
    for header in ["Name", "Type", "Cluster IP", "Ports", "Age"] {
        window.get_by_label(header);
    }
    window.get_by_role_and_label(Role::ComboBox, "Namespace");
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
    assert_eq!(
        window.get_all_by_label("2026-08-21").count(),
        2,
        "the Age column renders the creation-date portion monospaced"
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
    harness.run_steps(4);

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
fn desktop_capability_renders_start_and_queues_an_authoritative_request() {
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
        [k10s_ui::ui::PortForwardAction::Start {
            local_port: 0,
            port: k10s_protocol::PortForwardPortSelector::Name { name },
            ..
        }] if name == "http"
    ));
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
    window.get_by_label("Selector app=web");
    assert!(window.query_by_label("Session affinity ClientIP").is_none());
    assert!(
        window
            .query_by_label("External traffic policy Local")
            .is_none(),
        "absent traffic policies must not render"
    );
    assert!(window.query_by_label("Internal traffic policy").is_none());

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
    window.get_by_label("External traffic policy Local");
    window.get_by_label("Internal traffic policy Cluster");
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
