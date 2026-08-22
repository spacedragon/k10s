//! Pure state tests for the workspace module.
//!
//! These tests never initialize egui; the workspace state is plain Rust data
//! driven exclusively by `WorkspaceCommand`s.

use std::collections::HashSet;

use k10s_ui::workspace::{
    BlockReason, BlockResolution, DetailTab, LauncherItem, WindowContent, WindowKind, WorkloadKind,
    WorkspaceCommand, WorkspaceEvent, WorkspaceState,
};

/// Stand-in for the protocol `ResourceIdentity`; the workspace state is
/// generic over the identity type so this module has no protocol dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestIdentity {
    context: &'static str,
    kind: &'static str,
    namespace: &'static str,
    name: &'static str,
    uid: &'static str,
}

impl TestIdentity {
    fn pod(name: &'static str) -> Self {
        Self {
            context: "dev",
            kind: "Pod",
            namespace: "default",
            name,
            uid: name,
        }
    }
}

fn events(
    state: &mut WorkspaceState<TestIdentity>,
    command: WorkspaceCommand<TestIdentity>,
) -> Vec<WorkspaceEvent<TestIdentity>> {
    state.apply(command)
}

fn opened(events: &[WorkspaceEvent<TestIdentity>]) -> k10s_ui::workspace::WindowId {
    events
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Opened(id) => Some(*id),
            _ => None,
        })
        .expect("expected an Opened event")
}

fn blocked(
    events: &[WorkspaceEvent<TestIdentity>],
) -> &k10s_ui::workspace::PendingNavigation<TestIdentity> {
    events
        .iter()
        .find_map(|event| match event {
            WorkspaceEvent::Blocked(pending) => Some(pending),
            _ => None,
        })
        .expect("expected a Blocked event")
}

/// Open a Pods list window: first click activates the launcher item,
/// subsequent opens use the `+` button, which always opens a new instance.
fn open_pods(state: &mut WorkspaceState<TestIdentity>) -> k10s_ui::workspace::WindowId {
    let command = if state.instance_count(WorkloadKind::Pods) == 0 {
        WorkspaceCommand::ActivateLauncherItem(LauncherItem::Workload(WorkloadKind::Pods))
    } else {
        WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods)
    };
    let out = events(state, command);
    opened(&out)
}

fn select(
    state: &mut WorkspaceState<TestIdentity>,
    window: k10s_ui::workspace::WindowId,
    identity: TestIdentity,
) -> Vec<WorkspaceEvent<TestIdentity>> {
    events(state, WorkspaceCommand::SelectRow(window, identity))
}

// ---------------------------------------------------------------------------
// Startup and launcher behavior
// ---------------------------------------------------------------------------

#[test]
fn overview_is_the_only_window_on_startup() {
    let state = WorkspaceState::<TestIdentity>::new();
    let windows = state.windows();
    assert_eq!(windows.len(), 1);
    assert!(matches!(windows[0].kind, WindowKind::Overview));
    assert_eq!(state.pending(), None);
}

#[test]
fn singleton_items_open_or_focus_a_single_window() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let overview = state.windows()[0].id;

    let out = events(
        &mut state,
        WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes),
    );
    let nodes = opened(&out);
    assert_eq!(state.windows().len(), 2);

    // Activating a singleton again focuses the existing window; no new one.
    let out = events(
        &mut state,
        WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes),
    );
    assert_eq!(out, vec![WorkspaceEvent::Focused(nodes)]);
    assert_eq!(state.windows().len(), 2);

    let out = events(
        &mut state,
        WorkspaceCommand::ActivateLauncherItem(LauncherItem::Overview),
    );
    assert_eq!(out, vec![WorkspaceEvent::Focused(overview)]);
    assert_eq!(state.windows().len(), 2);
}

#[test]
fn workload_first_click_opens_its_first_instance() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    assert!(matches!(
        state.window(window).unwrap().kind,
        WindowKind::Workload(WorkloadKind::Pods)
    ));
    assert_eq!(state.instance_count(WorkloadKind::Pods), 1);
    assert!(state.launcher_highlight(LauncherItem::Workload(WorkloadKind::Pods)));
}

#[test]
fn plus_always_opens_another_instance_and_badge_counts_them() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let first = open_pods(&mut state);

    let out = events(
        &mut state,
        WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Pods),
    );
    let second = opened(&out);
    assert_ne!(first, second);
    assert_eq!(state.instance_count(WorkloadKind::Pods), 2);

    let out = events(
        &mut state,
        WorkspaceCommand::AddWorkloadInstance(WorkloadKind::Deployments),
    );
    let _deployment = opened(&out);
    assert_eq!(state.instance_count(WorkloadKind::Pods), 2);
    assert_eq!(state.instance_count(WorkloadKind::Deployments), 1);
}

#[test]
fn clicking_a_highlighted_item_focuses_its_most_recently_used_instance() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let first = open_pods(&mut state);
    let second = open_pods(&mut state);

    // Focus `first`, making it the most recently used instance.
    let out = events(&mut state, WorkspaceCommand::FocusWindow(first));
    assert_eq!(out, vec![WorkspaceEvent::Focused(first)]);

    // Clicking the highlighted launcher item raises the MRU instance.
    let out = events(
        &mut state,
        WorkspaceCommand::ActivateLauncherItem(LauncherItem::Workload(WorkloadKind::Pods)),
    );
    assert_eq!(out, vec![WorkspaceEvent::Focused(first)]);
    assert_eq!(state.windows().len(), 3); // overview + two pods windows

    // The focused window is now above the previously opened one.
    let z_first = state.window(first).unwrap().z;
    let z_second = state.window(second).unwrap().z;
    assert!(z_first > z_second);
}

#[test]
fn closing_the_last_instance_removes_highlight_and_badge() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let first = open_pods(&mut state);
    let second = open_pods(&mut state);

    events(&mut state, WorkspaceCommand::CloseWindow(first));
    assert_eq!(state.instance_count(WorkloadKind::Pods), 1);
    assert!(state.launcher_highlight(LauncherItem::Workload(WorkloadKind::Pods)));

    events(&mut state, WorkspaceCommand::CloseWindow(second));
    assert_eq!(state.instance_count(WorkloadKind::Pods), 0);
    assert!(!state.launcher_highlight(LauncherItem::Workload(WorkloadKind::Pods)));

    // Clicking again starts a fresh first instance.
    let window = open_pods(&mut state);
    assert_ne!(window, first);
    assert_ne!(window, second);
}

#[test]
fn window_ids_are_stable_and_never_reused() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let mut seen = HashSet::new();
    for _ in 0..5 {
        let window = open_pods(&mut state);
        assert!(seen.insert(window));
        events(&mut state, WorkspaceCommand::CloseWindow(window));
    }
}

#[test]
fn commands_for_unknown_windows_are_ignored() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let bogus = k10s_ui::workspace::WindowId(999);
    let before = state.windows().len();

    let out = events(
        &mut state,
        WorkspaceCommand::SetNamespace(bogus, Some("x".into())),
    );
    assert!(out.is_empty());
    let out = events(&mut state, WorkspaceCommand::CloseWindow(bogus));
    assert!(out.is_empty());
    assert_eq!(state.windows().len(), before);
}

// ---------------------------------------------------------------------------
// Resource window state: selection, filters, splits
// ---------------------------------------------------------------------------

#[test]
fn selecting_a_row_sets_the_integrated_detail() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    let identity = TestIdentity::pod("payment-api");

    let out = select(&mut state, window, identity.clone());
    assert!(out.is_empty());

    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection.as_ref(), Some(&identity));
    let detail = resource.detail.as_ref().unwrap();
    assert_eq!(detail.identity, identity);
    assert_eq!(detail.active_tab, DetailTab::Overview);

    // Re-selecting the same row does not destroy state and is not blocked.
    let out = select(&mut state, window, identity.clone());
    assert!(out.is_empty());
}

#[test]
fn list_windows_have_independent_namespace_search_filters_and_sort() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let first = open_pods(&mut state);
    let second = open_pods(&mut state);

    events(
        &mut state,
        WorkspaceCommand::SetNamespace(first, Some("payments".into())),
    );
    events(
        &mut state,
        WorkspaceCommand::SetSearch(second, "fluentd".into()),
    );
    events(
        &mut state,
        WorkspaceCommand::SetFilter(second, "phase".into(), "Running".into()),
    );
    events(
        &mut state,
        WorkspaceCommand::SetSort(
            first,
            Some(k10s_ui::workspace::SortSpec {
                column: "age".into(),
                ascending: false,
            }),
        ),
    );

    let first_state = match &state.window(first).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };
    let second_state = match &state.window(second).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };

    assert_eq!(first_state.namespace.as_deref(), Some("payments"));
    assert_eq!(second_state.namespace, None);
    assert_eq!(first_state.search, "");
    assert_eq!(second_state.search, "fluentd");
    assert!(first_state.filters.is_empty());
    assert_eq!(
        second_state.filters.get("phase").map(String::as_str),
        Some("Running")
    );
    assert!(first_state.sort.is_some());
    assert!(second_state.sort.is_none());
}

#[test]
fn list_windows_have_independent_split_and_detail_visibility() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let first = open_pods(&mut state);
    let second = open_pods(&mut state);

    events(&mut state, WorkspaceCommand::SetSplitRatio(first, 0.3));
    events(&mut state, WorkspaceCommand::ToggleDetailPane(second));

    let first_state = match &state.window(first).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };
    let second_state = match &state.window(second).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };

    assert!((first_state.split_ratio - 0.3).abs() < f32::EPSILON);
    assert_eq!(second_state.split_ratio, 0.5);
    assert!(first_state.detail_visible);
    assert!(!second_state.detail_visible);

    // Ratios clamp to the unit interval and survive pane toggling.
    events(&mut state, WorkspaceCommand::SetSplitRatio(first, 1.7));
    let first_state = match &state.window(first).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };
    assert_eq!(first_state.split_ratio, 1.0);
    events(&mut state, WorkspaceCommand::ToggleDetailPane(first));
    let first_state = match &state.window(first).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected resource window, got {other:?}"),
    };
    assert_eq!(first_state.split_ratio, 1.0);
}

// ---------------------------------------------------------------------------
// Dedicated detail windows
// ---------------------------------------------------------------------------

#[test]
fn dedicated_detail_windows_are_pinned_to_their_identity() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let list = open_pods(&mut state);
    select(&mut state, list, TestIdentity::pod("list-selection"));

    let pinned = TestIdentity::pod("pinned-pod");
    let out = events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(pinned.clone()),
    );
    let dedicated = opened(&out);

    // Selecting another row in the list never changes the pinned window.
    select(&mut state, list, TestIdentity::pod("other-row"));
    let detail = match &state.window(dedicated).unwrap().content {
        WindowContent::Detail(detail) => detail,
        other => panic!("expected a detail window, got {other:?}"),
    };
    assert_eq!(detail.identity, pinned);
}

#[test]
fn multiple_dedicated_detail_windows_may_show_the_same_identity() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let identity = TestIdentity::pod("compared-pod");

    let first = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(identity.clone()),
    ));
    let second = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(identity.clone()),
    ));
    assert_ne!(first, second);
    for id in [first, second] {
        let detail = match &state.window(id).unwrap().content {
            WindowContent::Detail(detail) => detail,
            other => panic!("expected a detail window, got {other:?}"),
        };
        assert_eq!(detail.identity, identity);
    }
}

#[test]
fn dedicated_detail_tabs_are_independent_of_integrated_selection() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let list = open_pods(&mut state);
    select(&mut state, list, TestIdentity::pod("integrated"));

    let dedicated = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(TestIdentity::pod("dedicated")),
    ));
    events(
        &mut state,
        WorkspaceCommand::SetActiveTab(dedicated, DetailTab::Yaml),
    );

    let integrated = match &state.window(list).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    let dedicated = match &state.window(dedicated).unwrap().content {
        WindowContent::Detail(detail) => detail,
        other => panic!("expected a detail window, got {other:?}"),
    };
    assert_eq!(
        integrated.detail.as_ref().unwrap().active_tab,
        DetailTab::Overview
    );
    assert_eq!(dedicated.active_tab, DetailTab::Yaml);
}

// ---------------------------------------------------------------------------
// Guards: dirty YAML and connected shells
// ---------------------------------------------------------------------------

#[test]
fn dirty_yaml_blocks_row_selection_until_resolved() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    select(&mut state, window, TestIdentity::pod("original"));

    events(&mut state, WorkspaceCommand::BeginYamlEdit(window));

    let out = select(&mut state, window, TestIdentity::pod("replacement"));
    let pending = blocked(&out);
    assert_eq!(pending.blockers.len(), 1);
    assert_eq!(pending.blockers[0].window, window);
    assert_eq!(pending.blockers[0].reason, BlockReason::DirtyYaml);
    // Selection is preserved while blocked.
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection.as_ref().unwrap().name, "original");

    // Discarding the buffer resolves the blocker and commits the navigation.
    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::DiscardYaml { window }),
    );
    assert!(out.is_empty());
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection.as_ref().unwrap().name, "replacement");
    assert_eq!(state.pending(), None);
}

#[test]
fn dirty_yaml_blocks_window_close_until_resolved() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    select(&mut state, window, TestIdentity::pod("doomed"));
    events(&mut state, WorkspaceCommand::BeginYamlEdit(window));

    let out = events(&mut state, WorkspaceCommand::CloseWindow(window));
    assert!(!blocked(&out).blockers.is_empty());
    assert!(state.window(window).is_some());

    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::DiscardYaml { window }),
    );
    assert!(
        out.iter()
            .any(|event| matches!(event, WorkspaceEvent::Closed(_)))
    );
    assert!(state.window(window).is_none());
    // Closing the owner releases the writable-YAML claim.
    assert!(state.yaml_owner(&TestIdentity::pod("doomed")).is_none());
}

#[test]
fn cancel_preserves_selection_window_and_context() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    select(&mut state, window, TestIdentity::pod("original"));
    events(&mut state, WorkspaceCommand::BeginYamlEdit(window));

    let out = select(&mut state, window, TestIdentity::pod("replacement"));
    assert!(!blocked(&out).blockers.is_empty());

    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::Cancel),
    );
    assert!(out.is_empty());
    assert_eq!(state.pending(), None);
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection.as_ref().unwrap().name, "original");
    assert!(resource.detail.as_ref().unwrap().yaml.dirty);
    assert!(state.window(window).is_some());
}

#[test]
fn connected_shell_blocks_navigation_until_disconnected() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    select(&mut state, window, TestIdentity::pod("original"));

    events(&mut state, WorkspaceCommand::ConnectShell(window));

    let out = select(&mut state, window, TestIdentity::pod("replacement"));
    let pending = blocked(&out);
    assert_eq!(pending.blockers[0].reason, BlockReason::ConnectedShell);

    // While the guard dialog is pending, later commands are held back.
    let out = events(&mut state, WorkspaceCommand::ClearSelection(window));
    assert!(out.is_empty());
    assert!(state.pending().is_some());

    // Cancel preserves the selection and the connected session.
    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::Cancel),
    );
    assert!(out.is_empty());
    assert_eq!(state.pending(), None);
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection.as_ref().unwrap().name, "original");
    assert!(resource.detail.as_ref().unwrap().shell.connected);

    // Clearing the selection is also blocked, then disconnect resolves it.
    let out = events(&mut state, WorkspaceCommand::ClearSelection(window));
    assert_eq!(
        blocked(&out).blockers[0].reason,
        BlockReason::ConnectedShell
    );
    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::DisconnectShell { window }),
    );
    assert!(out.is_empty());
    assert_eq!(state.pending(), None);
    // The pending ClearSelection executed after the blocker resolved.
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection, None);
    assert!(resource.detail.is_none());
}

#[test]
fn context_switch_lists_every_affected_detail_state() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let dirty = open_pods(&mut state);
    select(&mut state, dirty, TestIdentity::pod("dirty-pod"));
    events(&mut state, WorkspaceCommand::BeginYamlEdit(dirty));

    let shelled = open_pods(&mut state);
    select(&mut state, shelled, TestIdentity::pod("shelled-pod"));
    events(&mut state, WorkspaceCommand::ConnectShell(shelled));

    let dedicated = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(TestIdentity::pod("dedicated-pod")),
    ));
    events(&mut state, WorkspaceCommand::ConnectShell(dedicated));

    let out = events(
        &mut state,
        WorkspaceCommand::ContextSwitch { to: "prod".into() },
    );
    let pending = blocked(&out);
    let blockers: Vec<(_, _)> = pending
        .blockers
        .iter()
        .map(|blocker| (blocker.window, blocker.reason))
        .collect();
    assert_eq!(
        blockers,
        vec![
            (dirty, BlockReason::DirtyYaml),
            (shelled, BlockReason::ConnectedShell),
            (dedicated, BlockReason::ConnectedShell),
        ]
    );
    assert_eq!(state.pending(), Some(pending));
}

#[test]
fn context_switch_commits_after_all_blockers_resolve() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let dirty = open_pods(&mut state);
    select(&mut state, dirty, TestIdentity::pod("dirty-pod"));
    events(&mut state, WorkspaceCommand::BeginYamlEdit(dirty));

    let clean = open_pods(&mut state);
    select(&mut state, clean, TestIdentity::pod("clean-pod"));
    events(
        &mut state,
        WorkspaceCommand::SetNamespace(clean, Some("payments".into())),
    );
    events(&mut state, WorkspaceCommand::SetSplitRatio(clean, 0.35));

    let dedicated = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(TestIdentity::pod("pinned-pod")),
    ));

    let out = events(
        &mut state,
        WorkspaceCommand::ContextSwitch { to: "prod".into() },
    );
    assert!(!blocked(&out).blockers.is_empty());

    let out = events(
        &mut state,
        WorkspaceCommand::ResolveBlock(BlockResolution::DiscardYaml { window: dirty }),
    );
    // The dedicated window closes and selections are cleared.
    assert!(state.window(dedicated).is_none());
    assert_eq!(state.pending(), None);
    assert!(
        out.iter()
            .any(|event| matches!(event, WorkspaceEvent::Closed(id) if *id == dedicated))
    );

    // List windows survive with kind, namespace, filters, and split intact.
    for id in [dirty, clean] {
        let resource = match &state.window(id).unwrap().content {
            WindowContent::Resource(resource) => resource,
            other => panic!("expected a resource window, got {other:?}"),
        };
        assert_eq!(resource.selection, None);
        assert!(resource.detail.is_none());
    }
    let clean_state = match &state.window(clean).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(clean_state.namespace.as_deref(), Some("payments"));
    assert!((clean_state.split_ratio - 0.35).abs() < f32::EPSILON);
}

#[test]
fn context_switch_without_blockers_proceeds_directly() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let list = open_pods(&mut state);
    select(&mut state, list, TestIdentity::pod("clean-pod"));
    let dedicated = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(TestIdentity::pod("pinned-pod")),
    ));

    let out = events(
        &mut state,
        WorkspaceCommand::ContextSwitch { to: "prod".into() },
    );
    assert!(
        !out.iter()
            .any(|event| matches!(event, WorkspaceEvent::Blocked(_)))
    );
    assert!(state.window(dedicated).is_none());
    let resource = match &state.window(list).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert_eq!(resource.selection, None);
}

#[test]
fn only_one_writable_yaml_buffer_per_resource_identity() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let identity = TestIdentity::pod("shared-pod");

    let first = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(identity.clone()),
    ));
    let second = opened(&events(
        &mut state,
        WorkspaceCommand::OpenDedicatedDetail(identity.clone()),
    ));

    events(&mut state, WorkspaceCommand::BeginYamlEdit(first));
    let detail = match &state.window(first).unwrap().content {
        WindowContent::Detail(detail) => detail,
        other => panic!("expected a detail window, got {other:?}"),
    };
    assert!(detail.yaml.dirty);
    assert_eq!(state.yaml_owner(&identity), Some(first));

    // A second view of the same identity opens YAML read-only and is pointed
    // at the existing editor.
    let out = events(&mut state, WorkspaceCommand::BeginYamlEdit(second));
    assert_eq!(out, vec![WorkspaceEvent::YamlOwnerInUse { owner: first }]);
    let detail = match &state.window(second).unwrap().content {
        WindowContent::Detail(detail) => detail,
        other => panic!("expected a detail window, got {other:?}"),
    };
    assert!(!detail.yaml.dirty);

    // Discarding transfers the writable claim to the next editor.
    events(&mut state, WorkspaceCommand::DiscardYaml(first));
    let out = events(&mut state, WorkspaceCommand::BeginYamlEdit(second));
    assert!(out.is_empty());
    let detail = match &state.window(second).unwrap().content {
        WindowContent::Detail(detail) => detail,
        other => panic!("expected a detail window, got {other:?}"),
    };
    assert!(detail.yaml.dirty);
    assert_eq!(state.yaml_owner(&identity), Some(second));
}

#[test]
fn non_destructive_updates_are_allowed_while_yaml_is_dirty() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    let window = open_pods(&mut state);
    select(&mut state, window, TestIdentity::pod("dirty-pod"));
    events(&mut state, WorkspaceCommand::BeginYamlEdit(window));

    let commands = [
        WorkspaceCommand::SetNamespace(window, Some("other".into())),
        WorkspaceCommand::SetSearch(window, "needle".into()),
        WorkspaceCommand::SetFilter(window, "phase".into(), "Running".into()),
        WorkspaceCommand::SetSplitRatio(window, 0.4),
        WorkspaceCommand::ToggleDetailPane(window),
        WorkspaceCommand::SetActiveTab(window, DetailTab::Yaml),
    ];
    for command in commands {
        let out = events(&mut state, command);
        assert!(
            !out.iter()
                .any(|event| matches!(event, WorkspaceEvent::Blocked(_))),
            "non-destructive command must not be blocked: {out:?}"
        );
    }
    let resource = match &state.window(window).unwrap().content {
        WindowContent::Resource(resource) => resource,
        other => panic!("expected a resource window, got {other:?}"),
    };
    assert!(resource.detail.as_ref().unwrap().yaml.dirty);
}
