use egui::accesskit::{Role, Toggled};
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_ui::{
    ui::{ConnectionState, UiShell},
    workspace::{
        BlockResolution, LauncherItem, WindowId, WindowKind, WorkloadKind, WorkspaceCommand,
    },
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

fn window_layer(id: WindowId) -> egui::LayerId {
    egui::LayerId::new(egui::Order::Middle, egui::Id::new(("k10s.window", id.0)))
}

fn choose_secondary_context(harness: &mut Harness<'_, ShellFixture>) {
    harness
        .get_by_role_and_label(Role::ComboBox, "Kubernetes context")
        .click();
    harness.run_steps(4);
    harness
        .get_by_role_and_label(Role::Button, SECONDARY_CONTEXT)
        .click();
    harness.run_steps(4);
}

fn add_guarded_pods_detail(harness: &mut Harness<'_, ShellFixture>, dirty_yaml: bool) {
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkloadKind::Pods),
        ));
    let window = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Workload(WorkloadKind::Pods))
        .expect("Pods window opens")
        .id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(window, ()));
    let guard = if dirty_yaml {
        WorkspaceCommand::BeginYamlEdit(window)
    } else {
        WorkspaceCommand::ConnectShell(window)
    };
    harness.state_mut().shell.apply_workspace_command(guard);
    harness.run_steps(4);
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
    harness.run_steps(4);
    assert!(harness.query_by_label("Pods").is_none());

    harness
        .get_by_role_and_label(Role::Button, "Workloads")
        .click();
    harness.run_steps(4);
    harness.get_by_role_and_label(Role::Button, "Pods");
    assert_eq!(harness.query_all_by_role(Role::CheckBox).count(), 0);
}

#[test]
fn workload_highlight_count_plus_and_close_track_workspace_instances() {
    let mut harness = shell_harness();

    harness.get_by_role_and_label(Role::Button, "Pods").click();
    harness.run_steps(4);

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
    harness.run_steps(4);
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
        harness.run_steps(4);
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
    harness.run_steps(4);
    let first_z = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Nodes)
        .expect("Nodes opens")
        .z;

    harness
        .get_by_role_and_label(Role::Button, "Storage")
        .click();
    harness.run_steps(4);
    let storage_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Storage)
        .expect("Storage opens")
        .id;
    assert_eq!(harness.ctx.top_layer_id(), Some(window_layer(storage_id)));

    harness.get_by_role_and_label(Role::Button, "Nodes").click();
    harness.run_steps(4);
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
    assert_eq!(
        harness.ctx.top_layer_id(),
        Some(window_layer(nodes[0].id)),
        "launcher focus must also raise the existing egui window layer"
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
        harness.run_steps(4);
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
        harness.run_steps(4);
    }
    let mru_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkloadKind::Pods))
        .max_by_key(|window| window.z)
        .map(|window| window.id)
        .expect("two Pods windows are open");
    assert_eq!(harness.ctx.top_layer_id(), Some(window_layer(mru_id)));

    let older_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkloadKind::Pods))
        .min_by_key(|window| window.z)
        .map(|window| window.id)
        .expect("an older Pods window is open");
    let older_title = harness
        .get_all_by_role_and_label(Role::Window, "Pods")
        .min_by(|left, right| left.rect().top().total_cmp(&right.rect().top()))
        .expect("the older staggered Pods window is visible")
        .rect()
        .left_top()
        + egui::vec2(96.0, 10.0);
    harness.drag_at(older_title);
    harness.run_steps(4);
    harness.drop_at(older_title);
    harness.run_steps(4);

    assert_eq!(
        harness.ctx.top_layer_id(),
        Some(window_layer(older_id)),
        "direct window interaction raises its egui layer"
    );
    let workspace_mru = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter(|window| window.kind == WindowKind::Workload(WorkloadKind::Pods))
        .max_by_key(|window| window.z)
        .map(|window| window.id);
    assert_eq!(
        workspace_mru,
        Some(older_id),
        "direct egui focus must update workspace MRU"
    );

    harness.get_by_role_and_label(Role::Button, "Pods").click();
    harness.run_steps(4);

    assert_eq!(
        harness
            .state()
            .shell
            .workspace()
            .instance_count(WorkloadKind::Pods),
        2
    );
    assert_eq!(
        harness.ctx.top_layer_id(),
        Some(window_layer(older_id)),
        "workload launcher focuses the actual MRU window"
    );
}

#[test]
fn context_selector_commits_global_context_after_rendering() {
    let mut harness = shell_harness();

    choose_secondary_context(&mut harness);

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
fn dirty_yaml_cancel_keeps_context_selection_and_does_not_requeue() {
    let mut harness = shell_harness();
    add_guarded_pods_detail(&mut harness, true);

    choose_secondary_context(&mut harness);
    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(PRIMARY_CONTEXT)
    );
    assert_eq!(harness.state().shell.workspace().context(), PRIMARY_CONTEXT);
    assert!(harness.state().shell.workspace().pending().is_some());

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ResolveBlock(BlockResolution::Cancel));
    harness.run_steps(4);

    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(PRIMARY_CONTEXT)
    );
    assert_eq!(harness.state().shell.workspace().context(), PRIMARY_CONTEXT);
    assert!(harness.state().shell.workspace().pending().is_none());
}

#[test]
fn connected_shell_cancel_keeps_context_selection_and_does_not_requeue() {
    let mut harness = shell_harness();
    add_guarded_pods_detail(&mut harness, false);

    choose_secondary_context(&mut harness);
    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(PRIMARY_CONTEXT)
    );
    assert_eq!(harness.state().shell.workspace().context(), PRIMARY_CONTEXT);
    assert!(harness.state().shell.workspace().pending().is_some());

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ResolveBlock(BlockResolution::Cancel));
    harness.run_steps(4);

    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(PRIMARY_CONTEXT)
    );
    assert_eq!(harness.state().shell.workspace().context(), PRIMARY_CONTEXT);
    assert!(harness.state().shell.workspace().pending().is_none());
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

    harness.run_steps(4);

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
    harness.run_steps(4);
    assert!(
        harness.state().shell.workspace().windows()[0]
            .geometry
            .collapsed
    );

    harness
        .get_by_role_and_label(Role::Window, "Overview")
        .get_by_role_and_label(Role::Button, "Show")
        .click();
    harness.run_steps(4);
    assert!(
        !harness.state().shell.workspace().windows()[0]
            .geometry
            .collapsed
    );
}
