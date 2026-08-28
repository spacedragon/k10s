use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, Context, ContextAvailability, GroupVersionKind, ResourceIdentity,
    ResourceListRow,
};
use k10s_ui::{
    ui::{ConnectionState, NamespaceCatalogState, ResourceFeed, UiShell},
    workspace::{LauncherItem, WindowKind, WorkloadKind, WorkspaceCommand},
};

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    contexts: Vec<Context>,
    selected: Option<String>,
    feed: ResourceFeed,
}

fn crashloop_pod() -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("payments".into()),
            name: "worker-7f498f8b6c-x2psq".into(),
            uid: "uid-worker".into(),
        },
        revision: BackendRevision::new(7),
        labels: Default::default(),
        summary: "1/2 ready · CrashLoopBackOff · 7 restarts".into(),
        created_at: "2026-08-28T00:00:00Z".into(),
        projection: None,
    }
}

impl Default for Fixture {
    fn default() -> Self {
        let mut feed = ResourceFeed::default();
        feed.lists.insert(WorkloadKind::Pods, vec![crashloop_pod()]);
        feed.namespace_catalog = NamespaceCatalogState::Ready(vec!["payments".into()]);
        Self {
            shell: UiShell::new(),
            contexts: vec![Context {
                name: "dev-local".into(),
                cluster: "development".into(),
                namespace: Some("default".into()),
                is_current: true,
                availability: ContextAvailability::Available,
                unavailable_reason: None,
            }],
            selected: Some("dev-local".into()),
            feed,
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    fixture.shell.show_with_contexts_and_resources(
        ui,
        ConnectionState::Connected,
        &fixture.contexts,
        &mut fixture.selected,
        None,
        &fixture.feed,
    );
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_280.0, 800.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
}

fn open_palette(harness: &mut Harness<'_, Fixture>) {
    harness.key_press_modifiers(egui::Modifiers::CTRL, egui::Key::K);
    harness.run_steps(3);
    harness.get_by_role_and_label(Role::Window, "Command palette");
}

#[test]
fn shortcut_opens_grouped_accessible_results_and_escape_dismisses() {
    let mut harness = harness();
    open_palette(&mut harness);

    for heading in ["RESOURCE JUMPS", "LIST WINDOWS", "COMMANDS"] {
        harness.get_by_label(heading);
    }
    harness.get_by_role_and_label(Role::TextInput, "Command palette search");
    harness.get_by_label("Keyboard help: Up and Down or J and K navigate; Enter opens or focuses; Shift Enter opens a new window; Escape closes");
    assert!(
        harness
            .query_all_by_role(Role::Button)
            .filter_map(|node| node.accesskit_node().label())
            .any(|label| label.contains("worker-7f498f8b6c-x2psq")
                && label.contains("CrashLoopBackOff")
                && label.contains("7 restarts"))
    );

    harness.key_press(egui::Key::Escape);
    harness.run_steps(3);
    assert!(harness.query_by_label("Command palette").is_none());
}

#[test]
fn plain_enter_reuses_list_and_modified_enter_opens_dedicated_detail() {
    let mut harness = harness();
    open_palette(&mut harness);
    harness.key_press(egui::Key::Enter);
    harness.run_steps(4);
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        1
    );
    let pod_windows = harness.state().shell.workspace().windows().len();

    open_palette(&mut harness);
    harness.key_press_modifiers(egui::Modifiers::SHIFT, egui::Key::Enter);
    harness.run_steps(4);
    assert_eq!(
        harness.state().shell.workspace().windows().len(),
        pod_windows + 1
    );
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .any(|window| window.kind == WindowKind::Detail)
    );
}

#[test]
fn arrow_navigation_changes_activation_and_text_editing_owns_colon() {
    let mut harness = harness();
    open_palette(&mut harness);
    harness.key_press(egui::Key::ArrowDown);
    harness.key_press(egui::Key::Enter);
    harness.run_steps(4);
    let detail = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find_map(|window| match &window.content {
            k10s_ui::workspace::WindowContent::Resource(state) => state.detail.as_ref(),
            _ => None,
        })
        .expect("the second resource result opens Logs in an integrated detail");
    assert_eq!(detail.active_tab, k10s_ui::workspace::DetailTab::Logs);

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkloadKind::Pods),
        ));
    harness.run_steps(3);
    harness
        .get_by_role_and_label(Role::TextInput, "Search pods")
        .click();
    harness.run_steps(2);
    harness.event(egui::Event::Text(":".into()));
    harness.run_steps(3);
    assert!(
        harness.query_by_label("Command palette").is_none(),
        "focused text editing must own ':'"
    );
}

#[test]
fn j_navigation_activates_the_next_result() {
    let mut harness = harness();
    open_palette(&mut harness);
    harness.key_press(egui::Key::J);
    harness.key_press(egui::Key::Enter);
    harness.run_steps(4);

    let detail = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find_map(|window| match &window.content {
            k10s_ui::workspace::WindowContent::Resource(state) => state.detail.as_ref(),
            _ => None,
        })
        .expect("J selects the next resource jump");
    assert_eq!(detail.active_tab, k10s_ui::workspace::DetailTab::Logs);
}

#[test]
fn modified_enter_opens_a_second_services_list() {
    let mut harness = harness();
    open_palette(&mut harness);
    harness
        .get_by_role_and_label(Role::TextInput, "Command palette search")
        .type_text("svc");
    harness.run_steps(2);
    harness.key_press(egui::Key::Enter);
    harness.run_steps(4);

    open_palette(&mut harness);
    harness
        .get_by_role_and_label(Role::TextInput, "Command palette search")
        .type_text("svc");
    harness.run_steps(2);
    harness.key_press_modifiers(egui::Modifiers::SHIFT, egui::Key::Enter);
    harness.run_steps(4);

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .windows()
            .iter()
            .filter(|window| window.kind == WindowKind::Services)
            .count(),
        2
    );
}

#[test]
fn namespace_command_applies_scope_when_it_opens_the_first_list() {
    let mut harness = harness();
    open_palette(&mut harness);
    harness
        .get_by_role_and_label(Role::TextInput, "Command palette search")
        .type_text("ns pay");
    harness.run_steps(2);
    harness.key_press(egui::Key::Enter);
    harness.run_steps(4);

    let resource = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find_map(|window| match &window.content {
            k10s_ui::workspace::WindowContent::Resource(state) => Some(state),
            _ => None,
        })
        .expect("namespace activation opens a Pods list");
    assert_eq!(
        resource.namespace_scope,
        k10s_ui::workspace::NamespaceScope::Namespace("payments".into())
    );
}
