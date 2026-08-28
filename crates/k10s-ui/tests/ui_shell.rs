use egui::accesskit::{Role, Toggled};
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{Context, ContextAvailability};
use k10s_ui::{
    ui::{ConnectionState, ResourceFeed, UiShell},
    workspace::{
        BlockResolution, LauncherItem, WindowId, WindowKind, WorkloadKind, WorkspaceCommand,
        WorkspaceEvent,
    },
};

const PRIMARY_CONTEXT: &str = "dev-admin@singapore-development";
const SECONDARY_CONTEXT: &str = "prod-admin@singapore-production";

struct ShellFixture {
    shell: UiShell<()>,
    connection: ConnectionState,
    contexts: Vec<Context>,
    selected_context: Option<String>,
}

impl Default for ShellFixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            connection: ConnectionState::Connected,
            contexts: vec![
                context(PRIMARY_CONTEXT, true),
                context(SECONDARY_CONTEXT, false),
            ],
            selected_context: Some(PRIMARY_CONTEXT.to_owned()),
        }
    }
}

fn render_shell(ui: &mut egui::Ui, fixture: &mut ShellFixture) {
    fixture.shell.show_with_contexts_and_resources(
        ui,
        fixture.connection,
        &fixture.contexts,
        &mut fixture.selected_context,
        None,
        &ResourceFeed::default(),
    );
    // Simulate the application layer's side of the contract: a staged
    // switch is validated against the backend and committed locally only
    // after success. These shell-level fixtures treat every guard-clear
    // destination as confirmed, and route guarded ones through the normal
    // blocking path.
    if let Some((to, _origin)) = fixture.shell.take_requested_context() {
        if fixture
            .shell
            .workspace()
            .context_switch_blockers()
            .is_empty()
        {
            for event in fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::CommitContextSwitch { to })
            {
                // The application layer records the committed selection.
                if let WorkspaceEvent::ContextSwitched { to } = event {
                    fixture.selected_context = Some(to);
                }
            }
        } else {
            fixture
                .shell
                .apply_workspace_command(WorkspaceCommand::ContextSwitch { to });
        }
    }
}

fn context(name: &str, is_current: bool) -> Context {
    Context {
        name: name.into(),
        cluster: format!("{name}-cluster"),
        namespace: Some("default".into()),
        is_current,
        availability: ContextAvailability::Available,
        unavailable_reason: None,
    }
}

fn shell_harness() -> Harness<'static, ShellFixture> {
    shell_harness_at(egui::vec2(1_280.0, 800.0))
}

fn shell_harness_at(size: egui::Vec2) -> Harness<'static, ShellFixture> {
    Harness::builder()
        .with_size(size)
        .with_pixels_per_point(1.0)
        .build_ui_state(render_shell, ShellFixture::default())
}

fn window_layer(id: WindowId) -> egui::LayerId {
    egui::LayerId::new(egui::Order::Middle, egui::Id::new(("k10s.window", id.0)))
}

fn rendered_window(harness: &Harness<'_, ShellFixture>, title: &str) -> egui::Rect {
    harness.get_by_role_and_label(Role::Window, title).rect()
}

fn assert_rect_matches_geometry(
    rect: egui::Rect,
    canvas_origin: egui::Pos2,
    geometry: k10s_ui::workspace::WindowGeom,
) {
    let expected = egui::Rect::from_min_size(
        canvas_origin + egui::vec2(geometry.position[0], geometry.position[1]),
        egui::vec2(geometry.size[0], geometry.size[1]),
    );
    assert!(
        (rect.min - expected.min).length() <= 1.0
            && (rect.size() - expected.size()).length() <= 1.0,
        "rendered {rect:?} did not apply workspace geometry {expected:?}"
    );
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
    harness.get_by_role_and_label(Role::Button, "Overview · ● Active");
}

#[test]
fn shell_bands_and_top_bar_remain_non_overlapping_at_supported_viewports() {
    for size in [
        egui::vec2(640.0, 420.0),
        egui::vec2(1_280.0, 800.0),
        egui::vec2(1_440.0, 900.0),
    ] {
        let harness = shell_harness_at(size);
        let controls = [
            harness.get_by_role_and_label(Role::Button, "File").rect(),
            harness.get_by_role_and_label(Role::Button, "View").rect(),
            harness.get_by_role_and_label(Role::Button, "Help").rect(),
            harness
                .get_by_role_and_label(Role::Button, "Refresh")
                .rect(),
            harness
                .get_by_role_and_label(Role::ComboBox, "Kubernetes context")
                .rect(),
        ];
        let top = controls
            .iter()
            .map(egui::Rect::top)
            .fold(f32::INFINITY, f32::min);
        for (index, left) in controls.iter().enumerate() {
            assert!(
                left.top() >= top && left.bottom() <= top + 29.0,
                "{size:?}: {left:?}"
            );
            for right in controls.iter().skip(index + 1) {
                assert!(
                    !left.intersects(*right),
                    "top-bar controls overlap at {size:?}: {left:?} and {right:?}"
                );
            }
        }

        let launcher = harness
            .get_by_role_and_label(Role::Button, "Overview")
            .rect();
        let window = harness
            .get_by_role_and_label(Role::Window, "Overview")
            .rect();
        let launcher_panel_left = launcher.left() - 9.0;
        assert!(
            window.left() - launcher_panel_left >= 196.0,
            "the window canvas must begin beyond the 196 px launcher at {size:?}: launcher={launcher:?}, window={window:?}"
        );
        let task = harness
            .get_by_role_and_label(Role::Button, "Overview · ● Active")
            .rect();
        assert!(
            task.top() >= size.y - 37.0 && task.bottom() <= size.y,
            "the taskbar must occupy the bottom 29 px at {size:?}: {task:?}"
        );
    }
}

#[test]
fn layout_commands_apply_to_already_rendered_window_rectangles() {
    let mut harness = shell_harness_at(egui::vec2(1_280.0, 800.0));
    for kind in [WorkloadKind::Pods, WorkloadKind::Jobs] {
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::AddWorkloadInstance(kind));
    }
    harness.run_steps(4);

    let canvas = [1_080.0, 700.0];
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::Tile(canvas));
    harness.run_steps(4);

    let tiled: Vec<_> = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .map(|window| (window.title.clone(), window.geometry))
        .collect();
    let first_rect = rendered_window(&harness, &tiled[0].0);
    let canvas_origin = first_rect.min - egui::vec2(tiled[0].1.position[0], tiled[0].1.position[1]);
    let tiled_rects: Vec<_> = tiled
        .iter()
        .map(|(title, geometry)| {
            let rect = rendered_window(&harness, title);
            assert_rect_matches_geometry(rect, canvas_origin, *geometry);
            rect
        })
        .collect();
    for (index, left) in tiled_rects.iter().enumerate() {
        for right in tiled_rects.iter().skip(index + 1) {
            let separated = left.right() <= right.left()
                || right.right() <= left.left()
                || left.bottom() <= right.top()
                || right.bottom() <= left.top();
            assert!(
                separated,
                "tiled rendered windows overlap: {left:?} {right:?}"
            );
        }
    }
    assert!(
        tiled_rects.iter().any(|rect| rect.right() > 1_280.0),
        "compact tiling keeps usable minima on an intentional overflow surface"
    );

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFocus(canvas));
    harness.run_steps(4);
    let focused = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .max_by_key(|window| window.z)
        .expect("active window");
    assert_rect_matches_geometry(
        rendered_window(&harness, &focused.title),
        canvas_origin,
        focused.geometry,
    );

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFocus(canvas));
    harness.run_steps(4);
    for (title, geometry) in &tiled {
        assert_rect_matches_geometry(rendered_window(&harness, title), canvas_origin, *geometry);
    }

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::Cascade(canvas));
    harness.run_steps(4);
    for window in harness.state().shell.workspace().windows() {
        assert_rect_matches_geometry(
            rendered_window(&harness, &window.title),
            canvas_origin,
            window.geometry,
        );
    }

    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::Cascade(canvas));
    harness.run_steps(4);
    for (title, geometry) in &tiled {
        assert_rect_matches_geometry(rendered_window(&harness, title), canvas_origin, *geometry);
    }
}

#[test]
fn compact_taskbar_overflow_remains_keyboard_reachable() {
    let mut harness = shell_harness_at(egui::vec2(640.0, 420.0));
    for kind in WorkloadKind::ALL {
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
                LauncherItem::Workload(kind),
            ));
    }
    harness.run_steps(4);

    let overflow = harness.get_by(|node| {
        node.role() == Role::ComboBox && node.value().as_deref() == Some("More tasks (7)")
    });
    overflow.focus();
    harness.run_steps(4);

    let overflow = harness.get_by(|node| {
        node.role() == Role::ComboBox && node.value().as_deref() == Some("More tasks (7)")
    });
    assert!(overflow.is_focused());
    assert!(overflow.rect().right() <= 640.0);
}

#[test]
fn top_bar_menus_expose_window_view_and_help_actions() {
    let mut harness = shell_harness();

    harness.get_by_role_and_label(Role::Button, "File").click();
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Button, "Exit");

    harness.get_by_role_and_label(Role::Button, "View").click();
    harness.run_steps(2);
    harness.get_by_role_and_label(Role::Button, "Minimize");
    harness.get_by_role_and_label(Role::Button, "Enter full screen");

    harness.get_by_role_and_label(Role::Button, "Help").click();
    harness.run_steps(2);
    harness.get_by_label("Documentation");
    let help_buttons = harness
        .query_all_by_role(Role::Button)
        .filter_map(|node| node.accesskit_node().label())
        .collect::<Vec<_>>();
    assert!(
        help_buttons
            .iter()
            .any(|label| label.starts_with("Keyboard shortcuts"))
    );
    assert!(
        help_buttons
            .iter()
            .any(|label| label.starts_with("About k10s"))
    );
}

#[test]
fn unavailable_context_stays_visible_but_cannot_dispatch_and_shows_reason() {
    let mut harness = shell_harness();
    harness.state_mut().contexts[1].availability = ContextAvailability::Unavailable;
    harness.state_mut().contexts[1].unavailable_reason = Some("fixture plugin denied".into());
    harness.run_steps(2);

    harness
        .get_by_role_and_label(Role::ComboBox, "Kubernetes context")
        .click();
    harness.run_steps(4);
    let disabled = harness.get_by_label(SECONDARY_CONTEXT);
    assert!(
        harness
            .query_by_role_and_label(Role::Button, SECONDARY_CONTEXT)
            .is_none(),
        "an unavailable context must not expose a clickable selector action"
    );
    disabled.hover();
    harness.run_steps(2);
    assert_eq!(
        harness.state().selected_context.as_deref(),
        Some(PRIMARY_CONTEXT)
    );
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

    harness
        .get_by(|node| {
            node.role() == Role::Button
                && node.label().as_deref() == Some("Nodes")
                && node.toggled() == Some(Toggled::True)
        })
        .click();
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
