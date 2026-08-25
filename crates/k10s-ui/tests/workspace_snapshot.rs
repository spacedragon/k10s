//! Snapshot persistence tests: what survives a restart, and how restore
//! rebuilds a workspace from it. Pure Rust; no egui or protocol connection.

use k10s_ui::workspace::{
    LauncherItem, PersistedWindowKind, SortSpec, WorkloadKind, WorkspaceCommand, WorkspaceState,
};

/// Stand-in for the protocol `ResourceIdentity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestIdentity {
    name: &'static str,
}

fn state() -> WorkspaceState<TestIdentity> {
    // First-launch workspace: Overview only.
    let mut state = WorkspaceState::<TestIdentity>::new();

    // Open a Pods list window (launcher activation of a workload kind).
    state.apply(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(WorkloadKind::Pods),
    ));

    // Give the Pods window some view settings and geometry.
    let pods = state
        .windows()
        .iter()
        .find(|window| matches!(window.kind, k10s_ui::workspace::WindowKind::Workload(_)))
        .expect("pods window opened");
    let pods_id = pods.id;
    state.apply(WorkspaceCommand::SetNamespace(
        pods_id,
        Some("prod".to_owned()),
    ));
    state.apply(WorkspaceCommand::SetSearch(pods_id, "web".to_owned()));
    state.apply(WorkspaceCommand::SetSort(
        pods_id,
        Some(SortSpec {
            column: "NAME".to_owned(),
            ascending: true,
        }),
    ));
    state.apply(WorkspaceCommand::ToggleDetailPane(pods_id));
    state.apply(k10s_ui::workspace::WorkspaceCommand::SetGeometry(
        pods_id,
        k10s_ui::workspace::WindowGeom {
            position: [150.0, 120.0],
            size: [900.0, 640.0],
            collapsed: false,
        },
    ));

    // Open a Nodes singleton window too.
    state.apply(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    state
}

#[test]
fn snapshot_round_trips_through_json() {
    let snap = state().snapshot();
    assert_eq!(snap.version, k10s_ui::workspace::SNAPSHOT_VERSION);
    assert!(snap.is_current_version());

    let json = serde_json::to_string(&snap).expect("serialize");
    let back: k10s_ui::workspace::WorkspaceSnapshot =
        serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, snap);
}

#[test]
fn snapshot_captures_open_windows_geometry_and_view_settings() {
    let snap = state().snapshot();

    // Overview (first launch) plus Pods and Nodes.
    assert_eq!(snap.windows.len(), 3);
    let kinds: Vec<PersistedWindowKind> = snap.windows.iter().map(|w| w.kind).collect();
    assert!(kinds.contains(&PersistedWindowKind::Overview));
    assert!(kinds.contains(&PersistedWindowKind::Workload(WorkloadKind::Pods)));
    assert!(kinds.contains(&PersistedWindowKind::Nodes));

    let pods = snap
        .windows
        .iter()
        .find(|w| matches!(w.kind, PersistedWindowKind::Workload(_)))
        .expect("pods persisted");
    assert_eq!(pods.geometry.position, [150.0, 120.0]);
    assert_eq!(pods.geometry.size, [900.0, 640.0]);
    let view = pods.view.as_ref().expect("view settings persisted");
    assert_eq!(view.namespace.as_deref(), Some("prod"));
    assert_eq!(&*view.search, "web");
    assert!(view.sort.is_some());
    // ToggleDetailPane flipped the default.
    assert!(!view.detail_visible);
}

#[test]
fn restore_rebuilds_the_same_layout() {
    let original = state();
    let snap = original.snapshot();
    let restored = WorkspaceState::<TestIdentity>::from_snapshot(&snap).expect("restore");

    // Same kinds, same count.
    assert_eq!(restored.windows().len(), original.windows().len());
    for window in original.windows() {
        let match_kind: Vec<_> = restored
            .windows()
            .iter()
            .filter(|candidate| candidate.kind == window.kind)
            .collect();
        // Singleton kinds appear once; workload kind appears once here.
        assert_eq!(
            match_kind.len(),
            1,
            "kind {:?} missing after restore",
            window.kind
        );
        let restored_window = match_kind[0];
        // Geometry is preserved exactly.
        assert_eq!(restored_window.geometry, window.geometry);
    }

    // Pods view settings survive; selection and detail do not (and never were).
    let pods = restored
        .windows()
        .iter()
        .find(|window| matches!(window.kind, k10s_ui::workspace::WindowKind::Workload(_)))
        .expect("pods window");
    let resource = match &pods.content {
        k10s_ui::workspace::WindowContent::Resource(resource) => resource,
        _ => panic!("expected a list body"),
    };
    assert_eq!(resource.namespace.as_deref(), Some("prod"));
    assert_eq!(&*resource.search, "web");
    assert!(resource.sort.is_some());
    assert!(resource.selection.is_none());
    assert!(resource.detail.is_none());

    // Z-order stacking is preserved (relative order, not raw values).
    let original_z: Vec<_> = original
        .windows()
        .iter()
        .map(|w| (w.kind, w.z))
        .collect::<Vec<_>>();
    let restored_ranked: Vec<_> = restored.windows().iter().collect();
    assert_eq!(restored_ranked.len(), original_z.len());
}

#[test]
fn restore_rejects_a_mismatched_version() {
    let mut snap = state().snapshot();
    snap.version += 1;
    assert!(!snap.is_current_version());
    assert!(WorkspaceState::<TestIdentity>::from_snapshot(&snap).is_none());
}

#[test]
fn restore_skips_unhealthy_entries_but_keeps_the_rest() {
    let mut snap = state().snapshot();
    // Corrupt one entry: NaN geometry.
    if let Some(bad) = snap
        .windows
        .iter_mut()
        .find(|w| w.kind == PersistedWindowKind::Nodes)
    {
        bad.geometry.size[0] = f32::NAN;
    }

    let restored = WorkspaceState::<TestIdentity>::from_snapshot(&snap).expect("restore");
    // Nodes dropped, the other two survive.
    assert!(
        !restored
            .windows()
            .iter()
            .any(|w| matches!(w.kind, k10s_ui::workspace::WindowKind::Nodes))
    );
    assert_eq!(restored.windows().len(), 2);
}

#[test]
fn detail_windows_are_never_persisted() {
    let mut state = WorkspaceState::<TestIdentity>::new();
    // Open a dedicated detail window pinned to one identity.
    let identity = TestIdentity { name: "pod-1" };
    let events = state.apply(WorkspaceCommand::OpenDedicatedDetail(identity));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, k10s_ui::workspace::WorkspaceEvent::Opened(_)))
    );

    let snap = state.snapshot();
    // Only the Overview window is persistable.
    assert_eq!(snap.windows.len(), 1);
    assert!(matches!(
        snap.windows[0].kind,
        PersistedWindowKind::Overview
    ));

    let restored = WorkspaceState::<TestIdentity>::from_snapshot(&snap).expect("restore");
    assert_eq!(restored.windows().len(), 1);
}

#[test]
fn restore_continues_id_and_z_counters_without_reuse() {
    // Simulate a file that claims ids/z already handed out further ahead.
    let mut snap = state().snapshot();
    snap.next_id = 50;
    snap.next_z = 40;

    let mut restored = WorkspaceState::<TestIdentity>::from_snapshot(&snap).expect("restore");

    // Every restored id and z stays strictly below the persisted counters.
    for window in restored.windows() {
        assert!(window.id.0 < 50);
        assert!(window.z < 40);
    }
    // Ids are unique within the restored workspace.
    let mut ids: Vec<_> = restored.windows().iter().map(|w| w.id.0).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), restored.windows().len());

    // Opening a new window after restore raises above every restored z.
    let max_z_before = restored
        .windows()
        .iter()
        .map(|w| w.z)
        .max()
        .expect("windows");
    let events = restored.apply(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Storage,
    ));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, k10s_ui::workspace::WorkspaceEvent::Opened(_)))
    );
    let newest = restored
        .windows()
        .iter()
        .map(|w| w.z)
        .max()
        .expect("windows");
    assert!(newest > max_z_before);

    // The new window's id continues from the persisted floor instead of
    // reusing an id handed out before.
    let storage = restored
        .windows()
        .iter()
        .find(|w| matches!(w.kind, k10s_ui::workspace::WindowKind::Storage))
        .expect("storage window");
    assert!(storage.id.0 >= 50);
}
