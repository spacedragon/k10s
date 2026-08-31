//! Typed Pod Detail projection, responsive layout, and interaction coverage.

use egui::accesskit::Role;
use egui_kittest::{Harness, Node, kittest::Queryable as _};
use k10s_protocol::{
    BackendRevision, ContainerMetrics, ContainerStateProjection, ContainerTerminationProjection,
    EventRow, GroupVersionKind, MetricsAvailability, OwnerReference, PodContainerPort,
    PodContainerProjection, PodMetrics, PodProjection, ResourceCapabilities,
    ResourceConditionProjection, ResourceDetailResponse, ResourceIdentity, ResourceMetricsResponse,
    ResourceProjection, TransportProtocol,
};
use k10s_ui::{
    ui::{
        ConnectionState, DetailAuthority, DetailLifecycle, PrimaryDetailState, ResourceFeed,
        SafeUiError, UiShell, WindowFreshness,
    },
    workspace::{WindowContent, WindowGeom, WindowKind, WorkspaceCommand},
};

const CONTEXT: &str = "dev-local";

struct Fixture {
    shell: UiShell<ResourceIdentity>,
    feed: ResourceFeed,
}

#[test]
fn projection_healthy_uses_only_typed_fields_and_exact_container_metrics() {
    let mut harness = harness(1_100.0, healthy_detail());
    let identity = pod_identity("web-0");
    harness.state_mut().feed.metrics.insert(
        identity.clone(),
        ResourceMetricsResponse {
            identity,
            metrics: PodMetrics {
                availability: MetricsAvailability::Available,
                cpu_millicores: Some(143),
                memory_bytes: Some(80 * 1_048_576),
                collected_at: Some("2026-08-31T01:02:03Z".into()),
            },
            containers: vec![
                container_metrics("sidecar", 18, 16),
                container_metrics("web", 125, 64),
            ],
        },
    );
    harness.run_steps(4);

    let detail = pod_window(&harness);
    for label in [
        "Status ● Running",
        "Ready · 2/2",
        "Restarts · 1",
        "Age · 4d 2h",
        "Node · worker-a",
        "Pod IP · 10.244.0.9",
        "CONTAINERS · 2",
        "CONDITIONS",
        "RECENT EVENTS",
        "PLACEMENT",
        "NETWORK",
        "LABELS · 6",
        "ANNOTATIONS · 1",
        "IDENTITY",
        "ghcr.io/example/web:1.2.3",
        "125m / 64Mi",
        "18m / 16Mi",
        "Started · container started · ×2 · 1m",
    ] {
        detail.get_by_label(label);
    }
    assert!(detail.query_by_label("WHY IT'S FAILING").is_none());
    assert!(detail.query_by_label("SENTINEL SUMMARY").is_none());
    assert!(detail.query_by_label("SENTINEL MANIFEST").is_none());
}

#[test]
fn projection_crashloop_surfaces_authoritative_reason_and_last_exit() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.phase = Some("Running".into());
    pod.ready_containers = Some(1);
    pod.restart_count = Some(7);
    pod.containers[0] = PodContainerProjection {
        name: "web".into(),
        image: Some("ghcr.io/example/web:1.2.3".into()),
        state: Some(ContainerStateProjection::Waiting {
            reason: Some("CrashLoopBackOff".into()),
        }),
        ready: Some(false),
        restart_count: Some(7),
        last_termination: Some(ContainerTerminationProjection {
            exit_code: 1,
            reason: Some("Error".into()),
        }),
    };
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);

    for label in [
        "Status ▲ CrashLoopBackOff",
        "WHY IT'S FAILING",
        "web · CrashLoopBackOff · exit 1 · Error",
        "Waiting · CrashLoopBackOff",
        "1 · Error",
    ] {
        detail.get_by_label(label);
    }
}

#[test]
fn projection_succeeded_completion_is_not_a_failure() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.phase = Some("Succeeded".into());
    pod.ready_containers = Some(0);
    pod.restart_count = Some(0);
    pod.containers = vec![PodContainerProjection {
        name: "job".into(),
        image: Some("ghcr.io/example/job:1.0.0".into()),
        state: Some(ContainerStateProjection::Terminated(
            ContainerTerminationProjection {
                exit_code: 0,
                reason: Some("Completed".into()),
            },
        )),
        ready: Some(false),
        restart_count: Some(0),
        last_termination: None,
    }];
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);

    detail.get_by_label("Status ● Succeeded");
    assert!(detail.query_by_label("WHY IT'S FAILING").is_none());
}

#[test]
fn projection_missing_typed_data_never_parses_sections_or_manifest() {
    let mut response = healthy_detail();
    response.projection = None;
    response.sections[0].rows[0].value = "SENTINEL SUMMARY".into();
    response.manifest = "SENTINEL MANIFEST".into();
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);

    detail.get_by_label("Structured details unavailable");
    for label in [
        "Status ● —",
        "Ready · —",
        "Restarts · —",
        "Age · —",
        "Node · —",
        "Pod IP · —",
    ] {
        detail.get_by_label(label);
    }
    assert!(detail.query_by_label("SENTINEL SUMMARY").is_none());
    assert!(detail.query_by_label("SENTINEL MANIFEST").is_none());
}

#[test]
fn projection_metrics_reject_mismatched_identity_name_and_partial_samples() {
    let mut harness = harness(1_100.0, healthy_detail());
    let identity = pod_identity("web-0");
    harness.state_mut().feed.metrics.insert(
        identity.clone(),
        ResourceMetricsResponse {
            identity: ResourceIdentity {
                uid: "different-uid".into(),
                ..identity.clone()
            },
            metrics: PodMetrics::unavailable(),
            containers: vec![container_metrics("web", 999, 999)],
        },
    );
    harness.run_steps(3);
    assert_eq!(pod_window(&harness).query_all_by_label("— / —").count(), 2);

    harness.state_mut().feed.metrics.insert(
        identity.clone(),
        ResourceMetricsResponse {
            identity,
            metrics: PodMetrics::unavailable(),
            containers: vec![
                ContainerMetrics {
                    name: "web".into(),
                    metrics: PodMetrics {
                        availability: MetricsAvailability::Partial,
                        cpu_millicores: Some(999),
                        memory_bytes: Some(999 * 1_048_576),
                        collected_at: None,
                    },
                },
                container_metrics("not-sidecar", 999, 999),
            ],
        },
    );
    harness.run_steps(3);
    assert_eq!(pod_window(&harness).query_all_by_label("— / —").count(), 2);
    assert!(
        pod_window(&harness)
            .query_by_label("999m / 999Mi")
            .is_none()
    );
}

#[test]
fn projection_owner_labels_and_events_remain_exact_and_deterministic() {
    let mut response = healthy_detail();
    response.owner_references = vec![
        OwnerReference {
            gvk: GroupVersionKind::core("v1", "Secret"),
            name: "not-controller".into(),
            uid: "secret-uid".into(),
            controller: false,
        },
        OwnerReference {
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "ReplicaSet".into(),
            },
            name: "web-abc123".into(),
            uid: "rs-uid".into(),
            controller: true,
        },
    ];
    response.events = vec![EventRow {
        reason: "Unhealthy".into(),
        message: "readiness probe failed".into(),
        count: 3,
        last_seen: "2m".into(),
    }];
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);

    detail.get_by_label("OWNER CHAIN");
    detail.get_by_role_and_label(Role::Button, "Open owner ReplicaSet/default/web-abc123");
    detail.get_by_label("this Pod · default/web-0");
    detail.get_by_label("Unhealthy · readiness probe failed · ×3 · 2m");
    for label in [
        "alpha=first",
        "bravo=second",
        "charlie=third",
        "delta=fourth",
    ] {
        detail.get_by_label(label);
    }
    detail.get_by_role_and_label(Role::Button, "Show 2 more labels");
    assert!(detail.query_by_label("echo=fifth").is_none());
    assert!(detail.query_by_label("foxtrot=sixth").is_none());
    assert!(detail.query_by_label("not-controller").is_none());
    assert!(detail.query_by_label("Deployment/web").is_none());
    assert!(detail.query_by_label("Warning").is_none());
    assert!(detail.query_by_label("kubelet").is_none());
}

#[test]
fn pod_layout_759_is_operational_first_with_collapsed_metadata_and_vitals() {
    let mut harness = harness(783.0, healthy_detail());
    let detail = pod_window(&harness);
    let body = detail.get_by_role_and_label(Role::ScrollView, "Detail body");
    assert!(
        (body.rect().width() - 759.0).abs() < 0.1,
        "fixture must exercise the 759-point body boundary: {:?}",
        body.rect()
    );
    let containers = detail.get_by_label("CONTAINERS · 2").rect();
    let events = detail.get_by_label("RECENT EVENTS").rect();
    assert!(containers.top() < events.top());
    detail.get_by_role_and_label(Role::Button, "Show Pod metadata");
    detail.get_by_role_and_label(Role::Button, "Show more Pod vitals");
    assert!(detail.query_by_label("PLACEMENT").is_none());
    assert!(detail.query_by_label("Node · worker-a").is_none());
    assert!(detail.query_by_label("Pod IP · 10.244.0.9").is_none());

    detail
        .get_by_role_and_label(Role::Button, "Show more Pod vitals")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("Node · worker-a");
    detail.get_by_label("Pod IP · 10.244.0.9");
    detail.get_by_role_and_label(Role::Button, "Hide more Pod vitals");

    detail
        .get_by_role_and_label(Role::Button, "Show Pod metadata")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_role_and_label(Role::Button, "Hide Pod metadata");
    detail.get_by_label("PLACEMENT");
    detail.get_by_label("IDENTITY");
}

#[test]
fn pod_layout_760_is_two_columns_with_all_vitals_and_collapsed_annotations() {
    let harness = harness(784.0, healthy_detail());
    let detail = pod_window(&harness);
    let body = detail.get_by_role_and_label(Role::ScrollView, "Detail body");
    assert!(
        (body.rect().width() - 760.0).abs() < 0.1,
        "fixture must exercise the 760-point body boundary: {:?}",
        body.rect()
    );
    let containers = detail.get_by_label("CONTAINERS · 2").rect();
    let placement = detail.get_by_label("PLACEMENT").rect();
    assert!(
        placement.left() > containers.left(),
        "wide metadata must occupy the right column"
    );
    detail.get_by_label("Node · worker-a");
    detail.get_by_label("Pod IP · 10.244.0.9");
    detail.get_by_role_and_label(Role::Button, "ANNOTATIONS · 1");
    assert!(detail.query_by_label("checksum/config").is_none());
    assert!(detail.query_by_label("Show Pod metadata").is_none());
    assert!(detail.query_by_label("Show more Pod vitals").is_none());
}

#[test]
fn pod_tables_at_760_keep_wide_columns_reachable_via_horizontal_regions() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.conditions[0].message = Some("Containers are ready and accepting traffic".into());

    let mut harness = harness(784.0, response);
    let identity = pod_identity("web-0");
    harness.state_mut().feed.metrics.insert(
        identity.clone(),
        ResourceMetricsResponse {
            identity,
            metrics: PodMetrics::unavailable(),
            containers: vec![container_metrics("web", 125, 64)],
        },
    );
    harness.run_steps(3);

    let detail = pod_window(&harness);
    let body = detail.get_by_role_and_label(Role::ScrollView, "Detail body");
    assert_eq!(detail.query_all_by_role(Role::ScrollView).count(), 1);
    for label in [
        "LAST EXIT",
        "CPU / MEM",
        "0 · Completed",
        "125m / 64Mi",
        "Containers are ready and accepting traffic",
    ] {
        detail.get_by_label(label);
    }
    for label in ["Pod containers table", "Pod conditions table"] {
        let table = detail.get_by_role_and_label(Role::Table, label);
        assert!(
            body.rect().contains_rect(table.rect()),
            "{label} viewport must stay inside the one vertical detail body"
        );
    }
}

#[test]
fn pod_interaction_expands_labels_and_annotations_accessibly() {
    let mut harness = harness(1_100.0, healthy_detail());
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Show 2 more labels")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("echo=fifth");
    detail.get_by_label("foxtrot=sixth");
    detail.get_by_role_and_label(Role::Button, "Show fewer labels");

    detail
        .get_by_role_and_label(Role::Button, "ANNOTATIONS · 1")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("checksum/config");
    detail.get_by_label("abcdef");
}

#[test]
fn pod_interaction_owner_navigation_uses_only_the_verified_exact_identity() {
    let mut response = healthy_detail();
    response.owner_references = vec![OwnerReference {
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        name: "web-abc123".into(),
        uid: "rs-uid".into(),
        controller: true,
    }];
    let mut harness = harness(1_100.0, response);
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Open owner ReplicaSet/default/web-abc123")
        .click();
    harness.run_steps(3);

    let identities = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .filter_map(|window| match &window.content {
            WindowContent::Detail(detail) => Some(detail.identity.clone()),
            WindowContent::Resource(_) | WindowContent::Services(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(identities.len(), 2);
    assert!(identities.contains(&ResourceIdentity {
        context: CONTEXT.into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "ReplicaSet".into(),
        },
        namespace: Some("default".into()),
        name: "web-abc123".into(),
        uid: "rs-uid".into(),
    }));
}

#[test]
fn pod_interaction_lifecycle_states_keep_shared_frame_semantics() {
    let identity = pod_identity("web-0");

    let mut loading = harness(1_100.0, healthy_detail());
    loading.state_mut().feed.details.remove(&identity);
    loading.run_steps(3);
    let detail = pod_window(&loading);
    detail.get_by_label("Loading details");
    detail.get_by_label("Status ● —");
    detail.get_by_label(
        "Shortcuts: l logs · s shell · y yaml · e events · c copy name · Esc restore/close",
    );

    loading.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("pod detail denied")),
    );
    loading.run_steps(3);
    pod_window(&loading).get_by_label("Details unavailable: pod detail denied");

    let mut stale = harness(1_100.0, healthy_detail());
    stale.state_mut().feed.detail_authority.insert(
        identity.clone(),
        DetailAuthority {
            freshness: WindowFreshness::StaleRetrying {
                last_sync_age: "30s ago".into(),
                retry_in: "3s".into(),
                attempt: 1,
            },
            lifecycle: DetailLifecycle::Present,
        },
    );
    stale.run_steps(3);
    let detail = pod_window(&stale);
    detail.get_by_label("Freshness · stale");
    detail.get_by_label("CONTAINERS · 2");

    stale.state_mut().feed.detail_authority.insert(
        identity,
        DetailAuthority {
            freshness: WindowFreshness::ReadyEmpty,
            lifecycle: DetailLifecycle::Gone,
        },
    );
    stale.run_steps(3);
    let detail = pod_window(&stale);
    detail.get_by_label("This resource no longer exists");
    assert!(detail.query_by_label("CONTAINERS · 2").is_none());
}

#[test]
fn pod_interaction_non_overview_tools_stay_on_existing_router_flows() {
    let mut harness = harness(1_100.0, healthy_detail());
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab Events")
        .click();
    harness.run_steps(3);
    pod_window(&harness).get_by_label("Started container started");

    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab YAML")
        .click();
    harness.run_steps(3);
    pod_window(&harness).get_by_label("SENTINEL MANIFEST");

    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_role_and_label(Role::CheckBox, "Previous");
    detail.get_by_role_and_label(Role::Button, "Connect logs");

    detail
        .get_by_role_and_label(Role::Button, "Tab Shell")
        .click();
    harness.run_steps(3);
    pod_window(&harness).get_by_role_and_label(Role::Button, "Connect shell");
}

fn harness(width: f32, detail: ResourceDetailResponse) -> Harness<'static, Fixture> {
    let identity = detail.identity.clone();
    let mut fixture = Fixture::default();
    fixture.feed.details.insert(identity.clone(), detail);
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::OpenDedicatedDetail(identity));
    let window_id = fixture
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .expect("dedicated detail is open")
        .id;
    fixture
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [20.0, 20.0],
                size: [width, 800.0],
                collapsed: false,
            },
        ));
    let mut harness = Harness::builder()
        .with_size(egui::vec2(width + 400.0, 900.0))
        .with_pixels_per_point(1.0)
        .build_ui_state(render, fixture);
    harness.run_steps(4);
    harness
}

fn render(ui: &mut egui::Ui, fixture: &mut Fixture) {
    let mut selected_context = Some(CONTEXT.to_owned());
    fixture.shell.show_with_resources(
        ui,
        ConnectionState::Connected,
        &[CONTEXT.to_owned()],
        &mut selected_context,
        None,
        &fixture.feed,
    );
}

fn pod_window<'a>(harness: &'a Harness<'static, Fixture>) -> Node<'a> {
    harness.get_by_role_and_label(Role::Window, "Pod · default / web-0")
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            shell: UiShell::new(),
            feed: ResourceFeed::default(),
        }
    }
}

fn pod_identity(name: &str) -> ResourceIdentity {
    ResourceIdentity {
        context: CONTEXT.into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        name: name.into(),
        uid: format!("pod-{name}-uid"),
    }
}

fn healthy_detail() -> ResourceDetailResponse {
    ResourceDetailResponse {
        identity: pod_identity("web-0"),
        revision: BackendRevision::new(9),
        created_at: "SENTINEL CREATED".into(),
        owner_references: Vec::new(),
        sections: vec![k10s_protocol::DetailSection {
            title: "Overview".into(),
            rows: vec![k10s_protocol::DetailRow {
                label: "Status".into(),
                value: "SENTINEL SUMMARY".into(),
            }],
        }],
        events_condition: k10s_protocol::EventsCondition::Available,
        events: vec![EventRow {
            reason: "Started".into(),
            message: "container started".into(),
            count: 2,
            last_seen: "1m".into(),
        }],
        related: Vec::new(),
        capabilities: ResourceCapabilities {
            can_view_logs: true,
            can_exec: true,
            ..ResourceCapabilities::default()
        },
        manifest: "SENTINEL MANIFEST".into(),
        projection: Some(ResourceProjection::Pod(PodProjection {
            phase: Some("Running".into()),
            ready_containers: Some(2),
            total_containers: Some(2),
            restart_count: Some(1),
            containers: vec![
                PodContainerProjection {
                    name: "web".into(),
                    image: Some("ghcr.io/example/web:1.2.3".into()),
                    state: Some(ContainerStateProjection::Running),
                    ready: Some(true),
                    restart_count: Some(1),
                    last_termination: Some(ContainerTerminationProjection {
                        exit_code: 0,
                        reason: Some("Completed".into()),
                    }),
                },
                PodContainerProjection {
                    name: "sidecar".into(),
                    image: None,
                    state: Some(ContainerStateProjection::Running),
                    ready: Some(true),
                    restart_count: Some(0),
                    last_termination: None,
                },
            ],
            conditions: vec![ResourceConditionProjection {
                condition_type: "Ready".into(),
                status: "True".into(),
                reason: None,
                message: None,
                last_transition_time: Some("3m".into()),
            }],
            node_name: Some("worker-a".into()),
            pod_ip: Some("10.244.0.9".into()),
            host_ip: Some("10.0.0.2".into()),
            qos_class: Some("Burstable".into()),
            priority: Some(0),
            service_account: Some("web".into()),
            restart_policy: Some("Always".into()),
            ports: vec![PodContainerPort {
                container_name: "web".into(),
                name: Some("http".into()),
                container_port: 8080,
                host_port: None,
                protocol: TransportProtocol::Tcp,
            }],
            labels: [
                ("foxtrot", "sixth"),
                ("alpha", "first"),
                ("echo", "fifth"),
                ("charlie", "third"),
                ("delta", "fourth"),
                ("bravo", "second"),
            ]
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
            annotations: [("checksum/config".into(), "abcdef".into())]
                .into_iter()
                .collect(),
            created_at: Some(rfc3339_ago(4 * 24 * 60 * 60 + 2 * 60 * 60)),
        })),
    }
}

fn container_metrics(name: &str, cpu: u64, memory_mib: u64) -> ContainerMetrics {
    ContainerMetrics {
        name: name.into(),
        metrics: PodMetrics {
            availability: MetricsAvailability::Available,
            cpu_millicores: Some(cpu),
            memory_bytes: Some(memory_mib * 1_048_576),
            collected_at: Some("2026-08-31T01:02:03Z".into()),
        },
    }
}

fn rfc3339_ago(seconds: u64) -> String {
    let then = std::time::SystemTime::now() - std::time::Duration::from_secs(seconds);
    jiff::Timestamp::try_from(then)
        .expect("test timestamp is in Jiff's supported range")
        .to_string()
}
