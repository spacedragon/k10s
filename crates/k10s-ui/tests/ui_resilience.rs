//! Resilient states of the connected UI prototype: loading, empty,
//! filtered-empty, stale, forbidden metrics, conflicts, gone resources,
//! unavailable GVKs after a context switch, disconnected logs, active-shell
//! guards, textual status, focus order, and minimum-size non-overlap.
//!
//! Every state is fed through [`ResourceFeed`] / [`InfrastructureResponse`]
//! projections exactly like the application layer builds them; no backend
//! state leaks into this crate.

use egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};
use k10s_protocol::{
    BackendRevision, CapacityUsage, ClusterTotals, ContainerStateProjection, DetailRow,
    DetailSection, GroupVersionKind, InfrastructureResponse, MetricsAvailability, MetricsCondition,
    MetricsStatus, NodeRow, PodContainerProjection, PodProjection, ResourceCapabilities,
    ResourceDetailResponse, ResourceIdentity, ResourceListRow, ResourceProjection, StreamTarget,
};
use k10s_ui::{
    ui::{ConnectionState, ResourceFeed, UiShell, WindowFreshness, tools::LogsAction},
    workspace::{
        BlockReason, BlockResolution, LauncherItem, WindowGeom, WindowId, WindowKind,
        WorkloadKind as WorkspaceWorkload, WorkspaceCommand, WorkspaceEvent,
    },
};

mod common;

const CONTEXT: &str = "dev-local";
const OTHER_CONTEXT: &str = "prod-readonly";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
    response: Option<InfrastructureResponse>,
    connection: ConnectionState,
    selected_context: String,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
            response: None,
            connection: ConnectionState::Connected,
            selected_context: CONTEXT.to_owned(),
        }
    }
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let contexts = [CONTEXT.to_owned(), OTHER_CONTEXT.to_owned()];
    let mut selected = Some(fixture.selected_context.clone());
    fixture.shell.show_with_resources(
        ui,
        fixture.connection,
        &contexts,
        &mut selected,
        fixture.response.as_ref(),
        &fixture.feed,
    );
    if let Some(selected) = selected {
        fixture.selected_context = selected;
    }
}

fn harness() -> Harness<'static, Fixture> {
    Harness::builder()
        .with_size(egui::vec2(1_440.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, Fixture::default())
}

fn pod_row(name: &str, summary: &str) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: String::new(),
                version: "v1".to_owned(),
                kind: "Pod".to_owned(),
            },
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-pod-default-{name}"),
        },
        revision: BackendRevision::new(1_000),
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: "2026-08-21T00:50:10Z".to_owned(),
        projection: None,
    }
}

fn deployment_row(name: &str, summary: &str) -> ResourceListRow {
    ResourceListRow {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: "apps".to_owned(),
                version: "v1".to_owned(),
                kind: "Deployment".to_owned(),
            },
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-deployment-default-{name}"),
        },
        revision: BackendRevision::new(1_000),
        labels: Default::default(),
        summary: summary.to_owned(),
        created_at: "2026-08-21T00:00:00Z".to_owned(),
        projection: None,
    }
}

/// A backend-resolved deployment response carrying the full mutation
/// surface, so the cached-view path of the gone projection is exercised.
fn deployment_detail(name: &str) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: "apps".to_owned(),
                version: "v1".to_owned(),
                kind: "Deployment".to_owned(),
            },
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-deployment-default-{name}"),
        },
        revision: BackendRevision::new(1_010),
        created_at: "2026-08-21T00:00:00Z".to_owned(),
        owner_references: Vec::new(),
        sections: vec![DetailSection {
            title: "Overview".to_owned(),
            rows: vec![DetailRow {
                label: "Status".to_owned(),
                value: "2/2 ready".to_owned(),
            }],
        }],
        events_condition: k10s_protocol::EventsCondition::Available,
        events: Vec::new(),
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_scale: true,
            can_delete: true,
            can_edit_yaml: true,
            ..ResourceCapabilities::default()
        },
        manifest: format!("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {name}\n"),
        projection: None,
    }
}

/// A minimal backend-resolved pod response so the runtime tool tabs render.
fn pod_detail(name: &str) -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: ResourceIdentity {
            context: CONTEXT.to_owned(),
            gvk: GroupVersionKind {
                group: String::new(),
                version: "v1".to_owned(),
                kind: "Pod".to_owned(),
            },
            namespace: Some("default".to_owned()),
            name: name.to_owned(),
            uid: format!("uid-{CONTEXT}-pod-default-{name}"),
        },
        revision: BackendRevision::new(1_011),
        created_at: "2026-08-21T00:50:10Z".to_owned(),
        owner_references: Vec::new(),
        sections: vec![DetailSection {
            title: "Overview".to_owned(),
            rows: vec![DetailRow {
                label: "Status".to_owned(),
                value: "Running".to_owned(),
            }],
        }],
        events_condition: k10s_protocol::EventsCondition::Available,
        events: Vec::new(),
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_view_logs: true,
            can_exec: true,
            ..ResourceCapabilities::default()
        },
        manifest: "SENTINEL MANIFEST: runtime tools must not parse this".into(),
        projection: Some(ResourceProjection::Pod(PodProjection {
            phase: Some("Running".into()),
            ready_containers: Some(1),
            total_containers: Some(1),
            restart_count: Some(0),
            containers: vec![PodContainerProjection {
                name: "app".into(),
                image: None,
                state: Some(ContainerStateProjection::Running),
                ready: Some(true),
                restart_count: Some(0),
                last_termination: None,
            }],
            conditions: Vec::new(),
            node_name: None,
            pod_ip: None,
            host_ip: None,
            qos_class: None,
            priority: None,
            service_account: None,
            restart_policy: None,
            ports: Vec::new(),
            labels: Default::default(),
            annotations: Default::default(),
            created_at: None,
        })),
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

/// Give the integrated detail pane almost the whole window height so its
/// tab body stays visible instead of clipped by the window edge.
fn tall_detail_pane(id: WindowId) -> WorkspaceCommand<ResourceIdentity> {
    WorkspaceCommand::SetSplitRatio(id, 1.0)
}

// ---------------------------------------------------------------------------
// Loading / empty / filtered-empty
// ---------------------------------------------------------------------------

#[test]
fn missing_snapshots_render_a_loading_state_not_an_empty_table() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("Loading deployments");
    // A loading list must not be mistaken for an authoritative empty one.
    assert!(
        window
            .query_by_label("No Deployments in this view")
            .is_none(),
        "loading must never render as the empty state"
    );
}

#[test]
fn empty_and_filtered_empty_states_are_distinct_and_recoverable() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![deployment_row("api-server", "2/2 ready")],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    // An authoritative empty snapshot says so plainly.
    harness
        .state_mut()
        .feed
        .lists
        .insert(WorkspaceWorkload::Deployments, Vec::new());
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("No Deployments in this view");

    // Rows return, but a filter with no matches gets its own state.
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            deployment_row("api-server", "2/2 ready"),
            deployment_row("web-frontend", "20/20 ready"),
        ],
    );
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    window
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .type_text("no-such-workload");
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("No resources match these filters");
    assert!(
        window
            .query_by_label("No Deployments in this view")
            .is_none(),
        "a filter miss must not claim the namespace is empty"
    );

    window
        .get_by_role_and_label(Role::Button, "More list controls")
        .click();
    harness.step();
    harness.get_by_role_and_label(Role::Button, "Reset").click();
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("Select resource api-server");
    assert!(
        window
            .query_by_label("No resources match these filters")
            .is_none(),
        "Reset clears the search and restores the list"
    );
}

// ---------------------------------------------------------------------------
// Stale connection and status without color
// ---------------------------------------------------------------------------

#[test]
fn stale_connections_banner_every_data_window_as_text() {
    let mut harness = harness();
    harness.state_mut().connection = ConnectionState::Failed;
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Pods,
        vec![pod_row("db-postgres-0", "Running")],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );

    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("[~] Reconnecting · last sync unknown · retry in pending · attempt 1");
    window.get_by_label("Mutations are disabled; recovery controls unlock after reconnecting.");
    assert!(
        window
            .get_by_role_and_label(Role::Button, "Retry now")
            .accesskit_node()
            .is_disabled()
    );
    assert!(
        window
            .get_by_role_and_label(Role::Button, "Full resync")
            .accesskit_node()
            .is_disabled()
    );
    window.get_by_label("Select resource db-postgres-0");

    // Status must survive without color: the dot carries its state in its
    // accessible value and the refresh control relabels as Retry.
    harness
        .root()
        .children_recursive()
        .find(|node| {
            let accessible = node.accesskit_node();
            accessible
                .label()
                .is_some_and(|label| label == "Connection status: Connection failed")
                || accessible
                    .value()
                    .is_some_and(|value| value == "Connection status: Connection failed")
        })
        .expect("the status dot exposes its state as accessible text");
    harness.get_by_role_and_label(Role::Button, "Retry").click();
}

#[test]
fn every_window_freshness_state_is_independent_and_recoverable() {
    let mut harness = harness();
    for kind in [
        WorkspaceWorkload::Deployments,
        WorkspaceWorkload::Pods,
        WorkspaceWorkload::StatefulSets,
        WorkspaceWorkload::DaemonSets,
        WorkspaceWorkload::Jobs,
    ] {
        open(&mut harness, LauncherItem::Workload(kind));
    }

    let deployments = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    let pods = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let stateful_sets = workload_id(harness.state(), WorkspaceWorkload::StatefulSets);
    let daemon_sets = workload_id(harness.state(), WorkspaceWorkload::DaemonSets);
    let jobs = workload_id(harness.state(), WorkspaceWorkload::Jobs);

    let feed = &mut harness.state_mut().feed;
    feed.window_lists.insert(
        deployments,
        vec![deployment_row("healthy-api", "2/2 ready")],
    );
    feed.window_lists
        .insert(pods, vec![pod_row("cached-pod", "Running")]);
    feed.window_lists.insert(stateful_sets, Vec::new());
    feed.window_lists.insert(daemon_sets, Vec::new());
    feed.window_lists.insert(jobs, Vec::new());
    feed.window_freshness.insert(
        deployments,
        WindowFreshness::StaleRetrying {
            last_sync_age: "37s ago".into(),
            retry_in: "3s".into(),
            attempt: 2,
        },
    );
    feed.window_freshness.insert(
        pods,
        WindowFreshness::Live {
            last_sync_age: "4s ago".into(),
        },
    );
    feed.window_freshness.insert(
        stateful_sets,
        WindowFreshness::Forbidden {
            user: "alice@example.com".into(),
            verb: "list".into(),
            resource: "statefulsets.apps".into(),
            scope: "--namespace=payments".into(),
        },
    );
    feed.window_freshness.insert(
        daemon_sets,
        WindowFreshness::Failed {
            message: "watch ended unexpectedly".into(),
        },
    );
    feed.window_freshness
        .insert(jobs, WindowFreshness::ReadyEmpty);
    harness.run_steps(4);

    let stale = common::workload_window(&harness, "Deployments");
    stale.get_by_label("▲ Stale · last sync 37s ago · retry in 3s · attempt 2");
    stale
        .get_by_role_and_label(Role::Button, "Retry now")
        .click();
    let live = common::workload_window(&harness, "Pods");
    live.get_by_label("Live; synced 4s ago");
    live.get_by_label("Select resource cached-pod");

    let forbidden = common::workload_window(&harness, "StatefulSets");
    forbidden.get_by_label(
        "■ Forbidden · user alice@example.com cannot list statefulsets.apps in --namespace=payments",
    );
    forbidden.get_by_label(
        "kubectl auth can-i list statefulsets.apps --as=alice@example.com --namespace=payments",
    );
    forbidden.get_by_role_and_label(Role::Button, "Copy auth can-i command");
    common::workload_window(&harness, "DaemonSets")
        .get_by_label("✕ Failed · watch ended unexpectedly");
    let ready_empty = common::workload_window(&harness, "Jobs");
    ready_empty.get_by_label("◇ Ready · no resources");
    ready_empty.get_by_role_and_label(Role::Button, "More list controls");
}

#[test]
fn stale_window_disables_only_its_mutation_controls_with_a_reason() {
    let mut harness = harness();
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );
    let window = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(tall_detail_pane(window));
    harness
        .state_mut()
        .feed
        .window_lists
        .insert(window, vec![deployment_row("stale-api", "2/2 ready")]);
    harness.state_mut().feed.window_freshness.insert(
        window,
        WindowFreshness::StaleRetrying {
            last_sync_age: "37s ago".into(),
            retry_in: "3s".into(),
            attempt: 2,
        },
    );
    harness.run_steps(2);
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource stale-api")
        .click();
    harness.run_steps(2);
    let detail = deployment_detail("stale-api");
    let identity = detail.identity.clone();
    harness
        .state_mut()
        .feed
        .details
        .insert(identity.clone(), detail);
    // These surfaces were opened while live in the regression that prompted
    // this test. Injecting them directly verifies that a later window-local
    // failure gates already-open state, not only its launch buttons.
    harness
        .state_mut()
        .shell
        .dialogs_mut()
        .open_scale(window, identity.clone(), Some(2));
    harness.run_steps(2);

    let deployment_window = common::workload_window(&harness, "Deployments");
    assert!(
        deployment_window
            .get_by_role_and_label(Role::Button, "Scale…")
            .accesskit_node()
            .is_disabled()
    );
    deployment_window.get_by_label(
        "Scale, restart, delete, and YAML edits are disabled until this window is live",
    );
    assert!(
        harness
            .get_by_role_and_label(Role::Window, "Scale workload")
            .get_by_role_and_label(Role::Button, "Apply scale")
            .accesskit_node()
            .is_disabled(),
        "an already-open dialog follows the owning window freshness"
    );

    // The connection-derived fallback is the effective freshness when the
    // application has not supplied a window-local projection yet.
    harness.state_mut().feed.window_freshness.remove(&window);
    harness.state_mut().connection = ConnectionState::Failed;
    harness.run_steps(2);
    assert!(
        common::workload_window(&harness, "Deployments")
            .get_by_role_and_label(Role::Button, "Scale…")
            .accesskit_node()
            .is_disabled(),
        "the disconnected fallback gates cached detail controls"
    );
}

#[test]
fn forbidden_and_stale_metrics_report_their_condition_in_text() {
    let mut harness = harness();
    harness.state_mut().response = Some(infrastructure_response(
        MetricsCondition::Forbidden,
        "Forbidden: cannot list nodes.metrics.k8s.io",
    ));
    harness.run_steps(4);
    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    overview.get_by_label("Metrics: Unavailable · RBAC forbidden");
    overview.get_by_label("Forbidden: cannot list nodes.metrics.k8s.io");

    harness.state_mut().response = Some(infrastructure_response(
        MetricsCondition::Stale,
        "Last sample is outside the freshness window",
    ));
    harness.run_steps(4);
    let overview = harness.get_by_role_and_label(Role::Window, "Overview");
    overview.get_by_label("Metrics: Unavailable · stale");
    overview.get_by_label("Last sample is outside the freshness window");
}

fn infrastructure_response(condition: MetricsCondition, detail: &str) -> InfrastructureResponse {
    const GIB: u64 = 1_073_741_824;
    InfrastructureResponse {
        context: CONTEXT.into(),
        revision: BackendRevision::new(1_042),
        generated_at: "2026-08-21T01:05:00Z".into(),
        totals: ClusterTotals {
            nodes: 1,
            pods: 3,
            workloads: 1,
            persistent_storage_bytes: 20 * GIB,
        },
        launcher: Default::default(),
        cluster_cpu: CapacityUsage::new(Some(500), Some(4_000)),
        cluster_memory: CapacityUsage::new(Some(2 * GIB), Some(16 * GIB)),
        pod_capacity: CapacityUsage::new(Some(3), Some(110)),
        metrics: MetricsStatus {
            availability: MetricsAvailability::Unavailable,
            condition,
            source: "metrics.k8s.io".into(),
            source_updated_at: None,
            detail: detail.into(),
        },
        workload_health: Vec::new(),
        attention: Vec::new(),
        nodes: vec![NodeRow {
            name: "dev-node-1".into(),
            status: "Ready".into(),
            roles: vec!["control-plane".into()],
            kubernetes_version: "v1.34.0".into(),
            cpu: CapacityUsage::new(Some(500), Some(4_000)),
            memory: CapacityUsage::new(Some(2 * GIB), Some(16 * GIB)),
            pods: CapacityUsage::new(Some(3), Some(110)),
            age: "14d".into(),
        }],
        storage: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// Conflicts surface in dialogs as safe retryable reasons
// ---------------------------------------------------------------------------

#[test]
fn mutation_conflicts_render_the_safe_reason_inside_the_dialog() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![deployment_row("api-server", "2/2 ready")],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);

    harness.state_mut().shell.dialogs_mut().open_scale(
        id,
        deployment_row("api-server", "2/2 ready").identity,
        Some(2),
    );
    if let Some(mut dialog) = harness.state_mut().shell.dialogs_mut().active_mut(id) {
        dialog.operation_failed("the target changed since validation");
    }
    harness.run_steps(4);

    let dialog = harness.get_by_role_and_label(Role::Window, "Scale workload");
    dialog.get_by_label("Set replicas for api-server");
    dialog.get_by_label("Failed: the target changed since validation");
    // A failed submission stays open for a corrected retry.
    dialog.get_by_role_and_label(Role::Button, "Apply scale");
}

// ---------------------------------------------------------------------------
// Gone resources
// ---------------------------------------------------------------------------

#[test]
fn a_gone_selection_shows_a_gone_state_instead_of_loading_forever() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            deployment_row("api-server", "2/2 ready"),
            deployment_row("web-frontend", "20/20 ready"),
        ],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);

    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(tall_detail_pane(id));
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("Deployment · default / web-frontend");

    // The object is deleted behind the watch: the authoritative rows drop
    // it while this window still pins its identity.
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![deployment_row("api-server", "2/2 ready")],
    );
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Select resource web-frontend")
            .is_none(),
        "the gone row must leave the table"
    );
    // Gone renders only the pinned identity header plus the banner.
    window.get_by_label("Deployment · default / web-frontend");
    window.get_by_label("This resource no longer exists");
    assert!(
        window.query_by_label("Loading details").is_none(),
        "gone must never be presented as still loading"
    );
    // The user can dismiss the pinned selection themselves.
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    assert!(
        window
            .query_by_label("Deployment · default / web-frontend")
            .is_none()
    );
}

#[test]
fn a_gone_selection_beats_any_cached_detail_response() {
    let mut harness = harness();
    let selected_row = deployment_row("web-frontend", "20/20 ready");
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            deployment_row("api-server", "2/2 ready"),
            selected_row.clone(),
        ],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Deployments);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(tall_detail_pane(id));
    harness.run_steps(4);

    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::Button, "Select resource web-frontend")
        .click();
    harness.run_steps(4);

    // The detail response resolved BEFORE the deletion: production caches
    // it, and the gone state must still take precedence.
    harness.state_mut().feed.details.insert(
        selected_row.identity.clone(),
        deployment_detail("web-frontend"),
    );
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    window.get_by_label("Structured details unavailable");
    window.get_by_role_and_label(Role::Button, "Scale…");

    // The object is deleted behind the watch while the cache is hot.
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![deployment_row("api-server", "2/2 ready")],
    );
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Deployments");
    assert!(
        window
            .query_by_role_and_label(Role::Button, "Select resource web-frontend")
            .is_none(),
        "the gone row must leave the table"
    );
    window.get_by_label("This resource no longer exists");
    for stale_control in ["Scale…", "Delete…", "Loading details"] {
        assert!(
            window.query_by_label(stale_control).is_none()
                && window
                    .query_by_role_and_label(Role::Button, stale_control)
                    .is_none(),
            "a gone resource must not keep its {stale_control} control"
        );
    }
    window
        .get_by_role_and_label(Role::Button, "Clear selection")
        .click();
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Deployments");
    assert!(
        window
            .query_by_label("Deployment · default / web-frontend")
            .is_none()
    );
}

// ---------------------------------------------------------------------------
// Unavailable GVK after a context switch
// ---------------------------------------------------------------------------

#[test]
fn a_custom_kind_missing_after_a_context_switch_falls_back_to_the_picker() {
    let mut harness = harness();
    use k10s_protocol::ResourceTypeEntry;
    harness.state_mut().feed.types = vec![ResourceTypeEntry {
        gvk: GroupVersionKind {
            group: "monitoring.example.com".to_owned(),
            version: "v1".to_owned(),
            kind: "Dashboard".to_owned(),
        },
        namespaced: true,
    }];
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::CustomResources,
        vec![ResourceListRow {
            identity: ResourceIdentity {
                context: CONTEXT.to_owned(),
                gvk: GroupVersionKind {
                    group: "monitoring.example.com".to_owned(),
                    version: "v1".to_owned(),
                    kind: "Dashboard".to_owned(),
                },
                namespace: Some("default".to_owned()),
                name: "traffic-overview".to_owned(),
                uid: "uid-dashboard".to_owned(),
            },
            revision: BackendRevision::new(1_000),
            labels: Default::default(),
            summary: "1 panel".to_owned(),
            created_at: "2026-08-21T00:45:00Z".to_owned(),
            projection: None,
        }],
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
            Some("monitoring.example.com/v1/Dashboard".to_owned()),
        ));
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Custom Resources");
    window.get_by_label("Select resource traffic-overview");

    // Switching contexts drops every authoritative row and the new
    // context's types do not include the previously picked GVK.
    harness.state_mut().feed.lists.clear();
    harness.state_mut().feed.types.clear();
    for event in
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::ContextSwitch {
                to: OTHER_CONTEXT.to_owned(),
            })
    {
        assert!(
            matches!(event, WorkspaceEvent::ContextSwitchRequested { .. }),
            "an unguarded switch only requests; it never commits locally"
        );
    }
    // The backend confirmed the destination: commit the local transition.
    for event in
        harness
            .state_mut()
            .shell
            .apply_workspace_command(WorkspaceCommand::CommitContextSwitch {
                to: OTHER_CONTEXT.to_owned(),
            })
    {
        assert!(
            matches!(event, WorkspaceEvent::ContextSwitched { .. }),
            "the committed switch reports its new context"
        );
    }
    harness.state_mut().selected_context = OTHER_CONTEXT.to_owned();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Custom Resources");
    window.get_by_label("Pick a resource type");
    assert!(
        window.query_by_label("traffic-overview").is_none(),
        "rows of the previous context's GVK must never leak across a switch"
    );
}

// ---------------------------------------------------------------------------
// Disconnected logs retain history and offer reconnect
// ---------------------------------------------------------------------------

#[test]
fn disconnected_logs_keep_their_history_and_reconnect_explicitly() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Pods,
        vec![pod_row("db-postgres-0", "Running")],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let logs_window = workload_id(harness.state(), WorkspaceWorkload::Pods);
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(tall_detail_pane(logs_window));
    harness.run_steps(4);
    // The backend resolves the pinned identity, unlocking the runtime
    // tools of the detail view.
    let detail_identity = pod_row("db-postgres-0", "Running").identity;
    harness
        .state_mut()
        .feed
        .details
        .insert(detail_identity.clone(), pod_detail("db-postgres-0"));
    harness.run_steps(4);

    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    harness.run_steps(4);

    let id = logs_window;
    let target = StreamTarget {
        context: CONTEXT.to_owned(),
        namespace: "default".to_owned(),
        pod: "db-postgres-0".to_owned(),
        uid: format!("uid-{CONTEXT}-pod-default-db-postgres-0"),
        container: "app".to_owned(),
    };
    // Opening Logs automatically claims exactly one connection attempt.
    assert_eq!(
        harness.state_mut().shell.drain_log_actions(),
        vec![(
            id,
            LogsAction::OpenLogs {
                window: id,
                target: target.clone(),
                since_seconds: Some(300),
                previous: false,
            },
        )]
    );
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("Connecting");

    // Simulate a live session streaming two lines, then losing the socket.
    {
        let stores = harness.state_mut().shell.stream_stores_mut();
        let view = stores.logs.ensure(id, target.clone());
        view.connect();
        view.attach();
        view.append("kubelet started pod");
        view.append("container ready");
    }
    harness.run_steps(4);
    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("kubelet started pod");
    window.get_by_label("container ready");

    harness
        .state_mut()
        .shell
        .stream_stores_mut()
        .logs
        .connection_lost();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Pods");
    window.get_by_label("kubelet started pod");
    window.get_by_label("container ready");
    window.get_by_label("Disconnected");
    window
        .get_by_role_and_label(Role::Button, "Retry logs")
        .click();
    harness.run_steps(1);
    assert_eq!(
        harness.state_mut().shell.drain_log_actions(),
        vec![(
            id,
            LogsAction::OpenLogs {
                window: id,
                target,
                since_seconds: Some(300),
                previous: false,
            },
        )]
    );
}

// ---------------------------------------------------------------------------
// Active-shell navigation guards
// ---------------------------------------------------------------------------

#[test]
fn an_active_shell_blocks_closing_and_context_switches_until_resolved() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Pods,
        vec![pod_row("db-postgres-0", "Running")],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Pods);
    let identity = pod_row("db-postgres-0", "Running").identity;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SelectRow(id, identity));
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ConnectShell(id));

    // Closing a window with a live terminal parks the navigation.
    let blocked: Vec<_> = harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::CloseWindow(id))
        .into_iter()
        .map(|event| match event {
            WorkspaceEvent::Blocked(pending) => pending,
            other => panic!("expected a block, got {other:?}"),
        })
        .collect();
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].blockers.len(), 1);
    assert_eq!(blocked[0].blockers[0].reason, BlockReason::ConnectedShell);
    assert!(
        harness.state().shell.workspace().window(id).is_some(),
        "a guarded window stays open while the navigation waits"
    );

    // Any further command is held back while a navigation pends.
    let held = harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(LauncherItem::Nodes));
    assert!(held.is_empty(), "commands queue behind the pending guard");

    // Disconnecting resolves the blocker and commits the close.
    let events = harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ResolveBlock(
            BlockResolution::DisconnectShell { window: id },
        ));
    assert!(events.iter().any(|event| matches!(
        event,
        WorkspaceEvent::Closed(closed) if *closed == id
    )));
    assert!(harness.state().shell.workspace().window(id).is_none());
}

// ---------------------------------------------------------------------------
// Focus order
// ---------------------------------------------------------------------------

#[test]
fn interactive_controls_follow_a_stable_focus_order_within_a_window() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Deployments,
        vec![
            deployment_row("api-server", "2/2 ready"),
            deployment_row("web-frontend", "20/20 ready"),
        ],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Deployments),
    );

    // The tree order defines keyboard focus order: the search field comes
    // before the sort headers and rows. Workload details are selection-driven,
    // so there is no separate detail-visibility control in the toolbar.
    let labels: Vec<String> = common::workload_window(&harness, "Deployments")
        .children_recursive()
        .filter(|node| matches!(node.accesskit_node().role(), Role::TextInput | Role::Button))
        .filter_map(|node| node.accesskit_node().label())
        .collect();
    let search = labels
        .iter()
        .position(|label| label == "Search deployments")
        .expect("search field is labelled");
    let sort = labels
        .iter()
        .position(|label| label == "Sort deployments by created")
        .expect("sort header present");
    let row = labels
        .iter()
        .position(|label| label == "Select resource api-server")
        .expect("row button present");
    assert!(search < sort);
    assert!(sort < row, "toolbar precedes table controls");

    // Keyboard focus starts on nothing and Tab walks forward.
    common::workload_window(&harness, "Deployments")
        .get_by_role_and_label(Role::TextInput, "Search deployments")
        .focus();
    harness.run_steps(4);
    harness.key_press(egui::Key::Tab);
    harness.run_steps(4);
    let focused: Vec<String> = harness
        .root()
        .children_recursive()
        .filter(|node| node.accesskit_node().is_focused())
        .filter_map(|node| node.accesskit_node().label())
        .collect();
    assert!(
        focused.iter().all(|label| label != "Search deployments"),
        "Tab must move focus off the search field, focused={focused:?}"
    );
}

// ---------------------------------------------------------------------------
// Minimum size without content overlap
// ---------------------------------------------------------------------------

#[test]
fn minimum_size_windows_keep_list_and_details_non_overlapping() {
    let mut harness = harness();
    harness.state_mut().feed.lists.insert(
        WorkspaceWorkload::Pods,
        vec![
            pod_row("web-frontend-7d9f8-00001", "Running"),
            pod_row("db-postgres-0", "Running"),
        ],
    );
    open(
        &mut harness,
        LauncherItem::Workload(WorkspaceWorkload::Pods),
    );
    let id = workload_id(harness.state(), WorkspaceWorkload::Pods);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            id,
            WindowGeom {
                position: [12.0, 40.0],
                size: [640.0, 420.0],
                collapsed: false,
            },
        ));
    common::workload_window(&harness, "Pods")
        .get_by_role_and_label(Role::Button, "Select resource db-postgres-0")
        .click();
    harness.run_steps(4);

    let window = common::workload_window(&harness, "Pods");
    let window_rect = window.rect();
    let row = window.get_by_label("Select resource web-frontend-7d9f8-00001");
    let row_rect = row.rect();
    let details = window.get_by_label("Pod · default / db-postgres-0");
    let details_rect = details.rect();

    assert!(
        row_rect.width() > 0.0 && details_rect.width() > 0.0,
        "both panes stay visible at the minimum window size"
    );
    assert!(
        !row_rect.intersects(details_rect),
        "list rows {row_rect:?} and details {details_rect:?} must not overlap"
    );
    assert!(
        window_rect.contains_rect(row_rect) && window_rect.contains_rect(details_rect),
        "both panes stay inside the window rect {window_rect:?}"
    );
}
