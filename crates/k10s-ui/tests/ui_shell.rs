use egui::accesskit::{Role, Toggled};
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_ui::{
    ui::{ConnectionState, UiShell},
    workspace::{WindowKind, WorkloadKind},
};

const PRIMARY_CONTEXT: &str = "dev-admin@singapore-development";
const SECONDARY_CONTEXT: &str = "prod-admin@singapore-production";

struct ShellFixture {
    shell: UiShell<()>,
    connection: ConnectionState,
    contexts: Vec<String>,
    selected_context: Option<String>,
}

impl Default for ShellFixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            connection: ConnectionState::Connected,
            contexts: vec![PRIMARY_CONTEXT.to_owned(), SECONDARY_CONTEXT.to_owned()],
            selected_context: Some(PRIMARY_CONTEXT.to_owned()),
        }
    }
}

fn render_shell(ui: &mut egui::Ui, fixture: &mut ShellFixture) {
    fixture.shell.show(
        ui,
        fixture.connection,
        &fixture.contexts,
        &mut fixture.selected_context,
    );
}

fn shell_harness() -> Harness<'static, ShellFixture> {
    Harness::builder()
        .with_size(egui::vec2(1_280.0, 800.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render_shell, ShellFixture::default())
}

#[test]
fn initial_shell_has_compact_top_bar_fixed_launcher_and_only_overview() {
    let harness = shell_harness();

    for menu in ["File", "View", "Help"] {
        harness.get_by_role_and_label(Role::Button, menu);
    }
    harness.get_by_label("Connected");
    harness.get_by_role_and_label(Role::Button, "Refresh");
    let context = harness.get_by_role_and_label(Role::ComboBox, "Kubernetes context");
    assert_eq!(context.value().as_deref(), Some(PRIMARY_CONTEXT));
    assert_eq!(harness.state().shell.workspace().context(), PRIMARY_CONTEXT);

    let overview_launcher = harness.get_by_role_and_label(Role::Button, "Overview");
    let overview_window = harness.get_by_role_and_label(Role::Window, "Overview");
    assert!(
        overview_launcher.rect().right() < overview_window.rect().left(),
        "the fixed launcher must remain to the left of the free window canvas"
    );
    assert_eq!(
        harness
            .query_all_by_role(Role::Window)
            .filter(|node| node.accesskit_node().label().is_some())
            .count(),
        1,
        "Overview is the only initial workspace window"
    );
    assert_eq!(harness.ctx.theme(), egui::Theme::Dark);
}

#[test]
fn workloads_group_expands_and_launcher_never_uses_checkbox_roles() {
    let mut harness = shell_harness();

    for label in [
        "Deployments",
        "Pods",
        "StatefulSets",
        "DaemonSets",
        "Jobs",
        "CronJobs",
        "Custom Resources…",
    ] {
        harness.get_by_role_and_label(Role::Button, label);
    }
    assert_eq!(harness.query_all_by_role(Role::CheckBox).count(), 0);

    harness
        .get_by_role_and_label(Role::Button, "Workloads")
        .click();
    harness.run();
    assert!(harness.query_by_label("Pods").is_none());

    harness
        .get_by_role_and_label(Role::Button, "Workloads")
        .click();
    harness.run();
    harness.get_by_role_and_label(Role::Button, "Pods");
    assert_eq!(harness.query_all_by_role(Role::CheckBox).count(), 0);
}

#[test]
fn workload_highlight_count_plus_and_close_track_workspace_instances() {
    let mut harness = shell_harness();

    harness.get_by_role_and_label(Role::Button, "Pods").click();
    harness.run();

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        1
    );
    assert_eq!(
        harness
            .get_by_role_and_label(Role::Button, "Pods")
            .accesskit_node()
            .toggled(),
        Some(Toggled::True)
    );
    harness.get_by_label("1 open Pods window");

    harness
        .get_by_role_and_label(Role::Button, "Open another Pods window")
        .click();
    harness.run();
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        2
    );
    harness.get_by_label("2 open Pods windows");
    assert_eq!(
        harness
            .get_all_by_role_and_label(Role::Window, "Pods")
            .count(),
        2
    );

    for _ in 0..2 {
        let pods_window = harness
            .get_all_by_role_and_label(Role::Window, "Pods")
            .next()
            .expect("a Pods window remains open");
        pods_window
            .get_by_role_and_label(Role::Button, "Close window")
            .click();
        harness.run();
    }
    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        0
    );
    assert_eq!(
        harness
            .get_by_role_and_label(Role::Button, "Pods")
            .accesskit_node()
            .toggled(),
        Some(Toggled::False)
    );
    assert!(harness.query_by_label("1 open Pods window").is_none());
}

#[test]
fn singleton_launcher_item_opens_once_then_focuses_existing_window() {
    let mut harness = shell_harness();

    harness.get_by_role_and_label(Role::Button, "Nodes").click();
    harness.run();
    let first_z = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Nodes)
        .expect("Nodes opens")
        .z;

    harness.get_by_role_and_label(Role::Button, "Nodes").click();
    harness.run();
    let nodes: Vec<_> = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Nodes)
        .collect();

    assert_eq!(nodes.len(), 1);
    assert!(
        nodes[0].z > first_z,
        "second click focuses and raises Nodes"
    );
    assert!(
        harness
            .query_by_label("Open another Nodes window")
            .is_none()
    );
    assert!(harness.query_by_label("1 open Nodes window").is_none());
}

#[test]
fn plus_always_opens_independent_staggered_workload_windows() {
    let mut harness = shell_harness();

    for _ in 0..2 {
        harness
            .get_by_role_and_label(Role::Button, "Open another Deployments window")
            .click();
        harness.run();
    }

    let deployments: Vec<_> = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkloadKind::Deployments))
        .collect();
    assert_eq!(deployments.len(), 2);
    assert_ne!(deployments[0].id, deployments[1].id);
    assert_ne!(
        deployments[0].geometry.position,
        deployments[1].geometry.position
    );
}

#[test]
fn highlighted_workload_item_focuses_the_most_recent_instance() {
    let mut harness = shell_harness();

    for _ in 0..2 {
        harness
            .get_by_role_and_label(Role::Button, "Open another Pods window")
            .click();
        harness.run();
    }
    let (mru_id, mru_z) = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkloadKind::Pods))
        .max_by_key(|window| window.z)
        .map(|window| (window.id, window.z))
        .expect("two Pods windows are open");

    harness.get_by_role_and_label(Role::Button, "Pods").click();
    harness.run();

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        2
    );
    assert!(
        harness
            .state()
            .shell
            .workspace()
            .window(mru_id)
            .expect("MRU window remains open")
            .z
            > mru_z
    );
}

#[test]
fn context_selector_commits_global_context_after_rendering() {
    let mut harness = shell_harness();

    harness
        .get_by_role_and_label(Role::ComboBox, "Kubernetes context")
        .click();
    harness.run();
    harness
        .get_by_role_and_label(Role::Button, SECONDARY_CONTEXT)
        .click();
    harness.run();

    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(SECONDARY_CONTEXT)
    );
    assert_eq!(
        harness.state().shell.workspace().context(),
        SECONDARY_CONTEXT
    );
    assert_eq!(
        harness
            .get_by_role_and_label(Role::ComboBox, "Kubernetes context")
            .value()
            .as_deref(),
        Some(SECONDARY_CONTEXT)
    );
}

#[test]
fn controls_and_window_chrome_keep_stable_accessibility_identity() {
    let mut harness = shell_harness();

    let refresh_id = harness
        .get_by_role_and_label(Role::Button, "Refresh")
        .accesskit_node()
        .id();
    let overview_id = harness
        .get_by_role_and_label(Role::Window, "Overview")
        .accesskit_node()
        .id();
    harness
        .get_by_role_and_label(Role::Window, "Overview")
        .get_by_role_and_label(Role::Button, "Close window");
    assert!(harness.query_all_by_role(Role::Splitter).count() > 0);

    harness.run();

    assert_eq!(
        harness
            .get_by_role_and_label(Role::Button, "Refresh")
            .accesskit_node()
            .id(),
        refresh_id
    );
    assert_eq!(
        harness
            .get_by_role_and_label(Role::Window, "Overview")
            .accesskit_node()
            .id(),
        overview_id
    );

    harness
        .get_by_role_and_label(Role::Window, "Overview")
        .get_by_role_and_label(Role::Button, "Hide")
        .click();
    harness.run();
    assert!(
        harness.state().shell.workspace().windows()[0]
            .geometry
            .collapsed
    );

    harness
        .get_by_role_and_label(Role::Window, "Overview")
        .get_by_role_and_label(Role::Button, "Show")
        .click();
    harness.run();
    assert!(
        !harness.state().shell.workspace().windows()[0]
            .geometry
            .collapsed
    );
}
