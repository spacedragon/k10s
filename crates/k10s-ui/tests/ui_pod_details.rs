//! Typed Pod Detail projection, responsive layout, and interaction coverage.

use egui::accesskit::Role;
use egui_kittest::{
    Harness, Node,
    kittest::{NodeT as _, Queryable as _},
};
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
        SafeUiError, UiShell, WindowFreshness, tools::LogsPhase,
    },
    workspace::{DetailTab, WindowContent, WindowGeom, WindowKind, WorkspaceCommand},
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
        "Image: ghcr.io/example/web:1.2.3",
        "125m / 64Mi",
        "18m / 16Mi",
        "Started · container started · ×2 · 1m",
    ] {
        detail.get_by_label(label);
    }
    assert!(detail.query_by_label("WHY IT'S FAILING").is_none());
    let chips = [
        "alpha=first",
        "bravo=second",
        "charlie=third",
        "delta=fourth",
        "echo=fifth",
        "foxtrot=sixth",
    ]
    .map(|label| detail.get_by_label(label).rect());
    let metadata_right = detail
        .get_by_label("Configuration detail column")
        .rect()
        .right();
    assert!(
        chips
            .iter()
            .all(|rect| rect.right() <= metadata_right + 1.0)
    );
    let mut tops = chips.map(|rect| rect.top()).to_vec();
    tops.sort_by(f32::total_cmp);
    tops.dedup_by(|left, right| (*left - *right).abs() < 1.0);
    assert!(tops.len() >= 2, "constrained metadata labels must wrap");
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
    pod.containers[1] = PodContainerProjection {
        name: "sidecar".into(),
        image: Some("ghcr.io/example/sidecar:1.2.3".into()),
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
    let mut harness = harness(1_100.0, response);
    let window_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .expect("dedicated Pod detail remains open")
        .id;
    let other_window = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.id != window_id)
        .expect("another workspace window is open")
        .id;
    let target = k10s_protocol::StreamTarget {
        context: CONTEXT.into(),
        namespace: "default".into(),
        pod: "web-0".into(),
        uid: "pod-web-0-uid".into(),
        container: "web".into(),
    };
    harness
        .state_mut()
        .shell
        .stream_stores_mut()
        .logs
        .ensure(window_id, target.clone())
        .apply_source_defaults(false);
    harness
        .state_mut()
        .shell
        .stream_stores_mut()
        .logs
        .ensure(other_window, target)
        .apply_source_defaults(false);
    let detail = pod_window(&harness);

    for label in [
        "Status ▲ CrashLoopBackOff",
        "WHY IT'S FAILING",
        "sidecar · CrashLoopBackOff · exit 1 · Error",
        "Waiting · CrashLoopBackOff",
    ] {
        detail.get_by_label(label);
    }
    detail
        .get_by_role_and_label(Role::Button, "Previous logs")
        .click();
    harness.run_steps(3);
    let active_tab = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find_map(|window| match &window.content {
            WindowContent::Detail(detail) => Some(detail.active_tab),
            WindowContent::Resource(_) | WindowContent::Services(_) => None,
        })
        .expect("dedicated Pod detail remains open");
    assert_eq!(active_tab, DetailTab::Logs);
    assert!(
        harness
            .state()
            .shell
            .stream_stores()
            .logs
            .get(window_id)
            .expect("active log viewer remains bound")
            .previous()
    );
    assert_eq!(
        harness
            .state()
            .shell
            .stream_stores()
            .logs
            .get(window_id)
            .expect("active log viewer remains bound")
            .target()
            .container,
        "sidecar",
        "Previous logs selects the exact failing container"
    );
    assert!(
        !harness
            .state()
            .shell
            .stream_stores()
            .logs
            .get(other_window)
            .expect("other log viewer remains bound")
            .previous()
    );
    pod_window(&harness).get_by_role_and_label(Role::CheckBox, "Previous");
}

#[test]
fn projection_terminated_failure_requires_a_nonempty_authoritative_reason() {
    for reason in [None, Some(String::new())] {
        let mut response = healthy_detail();
        let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
            panic!("fixture has a typed Pod projection");
        };
        pod.containers[0] = PodContainerProjection {
            name: "web".into(),
            image: Some("ghcr.io/example/web:1.2.3".into()),
            state: Some(ContainerStateProjection::Terminated(
                ContainerTerminationProjection {
                    exit_code: 137,
                    reason: reason.clone(),
                },
            )),
            ready: Some(false),
            restart_count: Some(1),
            last_termination: Some(ContainerTerminationProjection {
                exit_code: 137,
                reason,
            }),
        };

        let harness = harness(1_100.0, response);
        let detail = pod_window(&harness);
        detail.get_by_label("Terminated · —");
        assert!(detail.query_by_label("WHY IT'S FAILING").is_none());
        assert!(
            detail
                .query_by_role_and_label(Role::Button, "Previous logs")
                .is_none()
        );
        assert!(detail.query_by_label("Exit 137").is_none());
    }
}

#[test]
fn projection_terminated_failure_with_reason_exposes_previous_logs() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.containers[0] = PodContainerProjection {
        name: "web".into(),
        image: Some("ghcr.io/example/web:1.2.3".into()),
        state: Some(ContainerStateProjection::Terminated(
            ContainerTerminationProjection {
                exit_code: 137,
                reason: Some("OOMKilled".into()),
            },
        )),
        ready: Some(false),
        restart_count: Some(1),
        last_termination: Some(ContainerTerminationProjection {
            exit_code: 137,
            reason: Some("OOMKilled".into()),
        }),
    };

    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);
    detail.get_by_label("Status ⨯ OOMKilled");
    detail.get_by_label("WHY IT'S FAILING");
    detail.get_by_label("web · OOMKilled · exit 137 · OOMKilled");
    detail.get_by_role_and_label(Role::Button, "Previous logs");
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
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Previous logs")
            .is_none()
    );
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
    detail.get_by_label("echo=fifth");
    detail.get_by_label("foxtrot=sixth");
    assert!(detail.query_by_label("Show 2 more labels").is_none());
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
    harness.get_by_label("Node · worker-a");
    harness.get_by_label("Pod IP · 10.244.0.9");
    let detail = pod_window(&harness);

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
fn freely_resized_narrow_vital_strip_keeps_controls_and_freshness_reachable() {
    let mut harness = harness(700.0, healthy_detail());
    let window_id = harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .expect("dedicated Pod detail remains open")
        .id;
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::SetGeometry(
            window_id,
            WindowGeom {
                position: [20.0, 20.0],
                size: [240.0, 500.0],
                collapsed: false,
            },
        ));
    harness.run_steps(5);

    let detail = pod_window(&harness);
    assert_eq!(detail.query_all_by_role(Role::ScrollView).count(), 1);
    let show_more = detail.get_by_role_and_label(Role::Button, "Show more Pod vitals");
    show_more.scroll_to_me();
    harness.run_steps(2);
    let detail = pod_window(&harness);
    let show_more = detail.get_by_role_and_label(Role::Button, "Show more Pod vitals");
    assert!(detail.rect().intersects(show_more.rect()));
    show_more.click();
    harness.run_steps(3);

    harness.get_by_label("Node · worker-a");
    harness.get_by_label("Pod IP · 10.244.0.9");
    pod_window(&harness)
        .get_by_label("Freshness · unavailable")
        .scroll_to_me();
    harness.run_steps(2);
    let detail = pod_window(&harness);
    let freshness = detail.get_by_label("Freshness · unavailable");
    assert!(detail.rect().intersects(freshness.rect()));
    assert_eq!(detail.query_all_by_role(Role::ScrollView).count(), 1);
}

#[test]
fn pod_layout_760_is_two_columns_with_all_vitals_and_annotations_section() {
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
    detail.get_by_label("ANNOTATIONS · 1");
    detail.get_by_label("checksum/config: abcdef");
    assert!(detail.query_by_label("Show Pod metadata").is_none());
    assert!(detail.query_by_label("Show more Pod vitals").is_none());
}

#[test]
fn pod_actual_1000_640_resize_preserves_identity_and_metadata_expansion() {
    let mut harness = harness(1_024.0, healthy_detail());
    let wide = pod_window(&harness);
    let operational = wide.get_by_label("Operational detail column").rect();
    let configuration = wide.get_by_label("Configuration detail column").rect();
    assert!(operational.width() > configuration.width());
    assert!(wide.query_all_by_role(Role::Splitter).next().is_some());
    harness
        .state_mut()
        .shell
        .apply_workspace_command(WorkspaceCommand::ToggleFreeWindowResizing);
    let rect = pod_window(&harness).rect();
    let target = rect.min + egui::vec2(664.0, 800.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let narrow = pod_window(&harness);
    narrow
        .get_by_role_and_label(Role::Button, "Show Pod metadata")
        .click();
    harness.run_steps(3);
    let narrow = pod_window(&harness);
    assert!(
        narrow.get_by_label("CONTAINERS · 2").rect().top()
            < narrow.get_by_label("PLACEMENT").rect().top()
    );
    assert!(
        narrow.get_by_label("PLACEMENT").rect().top()
            < narrow.get_by_label("IDENTITY").rect().top()
    );
    narrow.get_by_label("pod-web-0-uid");
    let rect = narrow.rect();
    let target = rect.min + egui::vec2(1_024.0, 800.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let restored = pod_window(&harness);
    restored.get_by_label("Operational detail column");
    restored.get_by_label("pod-web-0-uid");
    let rect = restored.rect();
    let target = rect.min + egui::vec2(664.0, 800.0);
    harness.hover_at(rect.max);
    harness.run_steps(1);
    harness.drag_at(rect.max);
    harness.run_steps(1);
    harness.hover_at(target);
    harness.run_steps(1);
    harness.drop_at(target);
    harness.run_steps(4);
    let final_narrow = pod_window(&harness);
    final_narrow.get_by_role_and_label(Role::Button, "Hide Pod metadata");
    final_narrow.get_by_label("PLACEMENT");
    final_narrow.get_by_label("pod-web-0-uid");
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
    for label in ["CPU / MEM", "125m / 64Mi"] {
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
fn pod_metadata_renders_annotations_as_a_sibling_section() {
    let harness = harness(1_100.0, healthy_detail());
    let detail = pod_window(&harness);
    detail.get_by_label("echo=fifth");
    detail.get_by_label("foxtrot=sixth");
    assert!(detail.query_by_label("Show 2 more labels").is_none());
    let labels = detail.get_by_label("LABELS · 6").rect();
    let annotations = detail.get_by_label("ANNOTATIONS · 1").rect();
    assert!(annotations.top() > labels.top());
    detail.get_by_label("checksum/config: abcdef");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Annotations 1 ▾")
            .is_none()
    );
}

#[test]
fn pod_metadata_omits_empty_combined_region() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!()
    };
    pod.labels.clear();
    pod.annotations.clear();
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);
    assert!(detail.query_by_label("LABELS · 0").is_none());
    assert!(detail.query_by_label("ANNOTATIONS · 0").is_none());
}

#[test]
fn pod_metadata_annotations_only_omits_empty_labels_heading() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!()
    };
    pod.labels.clear();
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);
    assert!(detail.query_by_label("LABELS · 0").is_none());
    detail.get_by_label("ANNOTATIONS · 1");
}

#[test]
fn pod_narrow_metadata_bounds_long_annotations_compactly() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!()
    };
    pod.annotations = [
        (
            "example.io/very-long-unbroken-annotation-key-alpha".into(),
            "a".repeat(240),
        ),
        (
            "example.io/very-long-unbroken-annotation-key-bravo".into(),
            "b".repeat(240),
        ),
    ]
    .into_iter()
    .collect();
    let mut harness = harness(700.0, response);
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Show Pod metadata")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    let rows = [
        detail
            .get_by_label(&format!(
                "{}: {}",
                "example.io/very-long-unbroken-annotation-key-alpha",
                "a".repeat(240)
            ))
            .rect(),
        detail
            .get_by_label(&format!(
                "{}: {}",
                "example.io/very-long-unbroken-annotation-key-bravo",
                "b".repeat(240)
            ))
            .rect(),
    ];
    assert!(
        rows.iter()
            .all(|row| row.right() <= detail.rect().right() + 1.0)
    );
    assert!(
        rows[1].top() - rows[0].top() <= 28.0,
        "annotation rows must remain Body-dense: {rows:?}"
    );
}

#[test]
fn pod_multi_container_images_stay_in_one_aligned_grid_cell() {
    let harness = harness(1_100.0, healthy_detail());
    let detail = pod_window(&harness);
    let image_header = detail.get_by_label("IMAGE").rect();
    let state_header = detail.get_by_label("STATE").rect();
    let web = detail
        .get_by_label("Image: ghcr.io/example/web:1.2.3")
        .rect();
    let sidecar = detail.get_by_label("Image: —").rect();
    assert!((web.left() - sidecar.left()).abs() < 0.1);
    assert!(web.left() >= image_header.left());
    assert!(web.right() <= state_header.left());
    assert!(sidecar.right() <= state_header.left());
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
    detail.get_by_label("l logs · s shell · y yaml · e events · c copy name · Esc clear selection");

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
fn detail_freshness_combines_primary_state_with_exact_source_authority() {
    let identity = pod_identity("web-0");
    let live = DetailAuthority {
        freshness: WindowFreshness::Live {
            last_sync_age: "just now".into(),
        },
        lifecycle: DetailLifecycle::Present,
    };

    let mut harness = harness(1_100.0, healthy_detail());
    harness
        .state_mut()
        .feed
        .detail_authority
        .insert(identity.clone(), live.clone());
    harness
        .state_mut()
        .feed
        .primary_details
        .insert(identity.clone(), PrimaryDetailState::Loading);
    harness.run_steps(3);
    pod_window(&harness).get_by_label("Freshness · loading");

    harness.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Failed(SafeUiError::new("detail denied")),
    );
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("Freshness · unavailable");
    assert!(
        detail
            .query_by_label("Freshness · live (just now)")
            .is_none()
    );

    harness.state_mut().feed.primary_details.insert(
        identity.clone(),
        PrimaryDetailState::Loaded(healthy_detail()),
    );
    harness.run_steps(3);
    pod_window(&harness).get_by_label("Freshness · live (just now)");

    harness.state_mut().feed.detail_authority.insert(
        identity,
        DetailAuthority {
            freshness: WindowFreshness::ReadyEmpty,
            lifecycle: DetailLifecycle::Gone,
        },
    );
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("Freshness · gone");
    assert!(
        detail
            .query_by_label("Freshness · live (just now)")
            .is_none()
    );
}

#[test]
fn pod_interaction_non_overview_tools_stay_on_existing_router_flows() {
    let mut harness = harness(1_100.0, healthy_detail());
    let window_id = detail_window_id(&harness);
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
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Connect logs")
            .is_none()
    );
    assert_eq!(
        harness
            .state()
            .shell
            .stream_stores()
            .logs
            .get(window_id)
            .expect("Logs tab creates its viewer")
            .phase(),
        LogsPhase::Connecting
    );

    detail
        .get_by_role_and_label(Role::Button, "Tab Shell")
        .click();
    harness.run_steps(3);
    pod_window(&harness).get_by_role_and_label(Role::Button, "Connect shell");
}

#[test]
fn pod_runtime_tabs_use_only_typed_container_and_failure_data() {
    let mut response = healthy_detail();
    response.manifest = "spec:\n  containers:\n    - name: legacy-manifest\n".into();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.containers[0].state = Some(ContainerStateProjection::Waiting {
        reason: Some("CrashLoopBackOff".into()),
    });

    let mut harness = harness(1_100.0, response);
    let window_id = detail_window_id(&harness);
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    harness.run_steps(3);

    let logs = harness
        .state()
        .shell
        .stream_stores()
        .logs
        .get(window_id)
        .expect("typed Pod runtime creates the log target");
    assert_eq!(logs.target().container, "web");
    assert!(
        logs.previous(),
        "the typed failing container has a real previous instance"
    );
    let detail = pod_window(&harness);
    let container_picker = detail
        .query_all_by_role(Role::ComboBox)
        .find(|node| node.accesskit_node().value().as_deref() == Some("Container: web"))
        .expect("typed default container is exposed by the picker");
    container_picker.click();
    harness.run_steps(1);
    harness.get_by_label("sidecar");
    assert!(harness.query_by_label("legacy-manifest").is_none());

    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab Shell")
        .click();
    harness.run_steps(3);
    assert_eq!(
        harness
            .state()
            .shell
            .stream_stores()
            .shells
            .get(window_id)
            .expect("typed Pod runtime creates the shell target")
            .target()
            .container,
        "web"
    );
}

#[test]
fn pod_runtime_tabs_fail_closed_without_typed_projection() {
    let mut response = healthy_detail();
    response.projection = None;
    response.manifest = "spec:\n  containers:\n    - name: legacy-manifest\n".into();
    response.sections[0].rows[0].value = "CrashLoopBackOff".into();

    let mut harness = harness(1_100.0, response);
    let window_id = detail_window_id(&harness);
    pod_window(&harness)
        .get_by_role_and_label(Role::Button, "Tab Logs")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("Pod runtime details unavailable");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Connect logs")
            .is_none()
    );
    assert!(
        harness
            .state()
            .shell
            .stream_stores()
            .logs
            .get(window_id)
            .is_none(),
        "legacy manifest data must not create a log target"
    );

    detail
        .get_by_role_and_label(Role::Button, "Tab Shell")
        .click();
    harness.run_steps(3);
    let detail = pod_window(&harness);
    detail.get_by_label("Pod runtime details unavailable");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Connect shell")
            .is_none()
    );
    assert!(
        harness
            .state()
            .shell
            .stream_stores()
            .shells
            .get(window_id)
            .is_none(),
        "legacy manifest data must not create a shell target"
    );
}

#[test]
fn waiting_failure_without_a_previous_instance_hides_previous_logs() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.containers[0].state = Some(ContainerStateProjection::Waiting {
        reason: Some("CrashLoopBackOff".into()),
    });
    pod.containers[0].last_termination = None;

    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);
    detail.get_by_label("WHY IT'S FAILING");
    assert!(
        detail
            .query_by_role_and_label(Role::Button, "Previous logs")
            .is_none(),
        "waiting alone does not prove a previous container instance exists"
    );
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

fn detail_window_id(harness: &Harness<'static, Fixture>) -> k10s_ui::workspace::WindowId {
    harness
        .state()
        .shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == WindowKind::Detail)
        .expect("dedicated Pod detail remains open")
        .id
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

#[test]
fn pod_conditions_and_placement_metadata_are_left_aligned() {
    let mut response = healthy_detail();
    let Some(ResourceProjection::Pod(pod)) = response.projection.as_mut() else {
        panic!("fixture has a typed Pod projection");
    };
    pod.conditions = vec![
        ResourceConditionProjection {
            condition_type: "ContainersReady".into(),
            status: "True".into(),
            reason: Some("PodCompleted".into()),
            message: None,
            last_transition_time: Some("6d 20h".into()),
        },
        ResourceConditionProjection {
            condition_type: "Initialized".into(),
            status: "True".into(),
            reason: Some("PodCompleted".into()),
            message: None,
            last_transition_time: Some("6d 20h".into()),
        },
        ResourceConditionProjection {
            condition_type: "PodReadyToStartContainers".into(),
            status: "False".into(),
            reason: None,
            message: None,
            last_transition_time: Some("6d 20h".into()),
        },
        ResourceConditionProjection {
            condition_type: "Ready".into(),
            status: "False".into(),
            reason: Some("PodCompleted".into()),
            message: None,
            last_transition_time: Some("6d 20h".into()),
        },
    ];
    pod.priority = Some(100);
    pod.service_account = Some("unique-sa".into());
    let harness = harness(1_100.0, response);
    let detail = pod_window(&harness);

    // Conditions must all start at the same left edge.
    let c1 = detail.get_by_label("ContainersReady").rect().left();
    let c2 = detail.get_by_label("Initialized").rect().left();
    let c3 = detail
        .get_by_label("PodReadyToStartContainers")
        .rect()
        .left();
    let c4 = detail.get_by_label("Ready").rect().left();
    assert!((c1 - c2).abs() < 1.0, "c1 {c1} vs c2 {c2}");
    assert!((c1 - c3).abs() < 1.0, "c1 {c1} vs c3 {c3}");
    assert!((c1 - c4).abs() < 1.0, "c1 {c1} vs c4 {c4}");

    // Placement labels must all start at the same left edge.
    let node_lbl = detail.get_by_label("Node").rect().left();
    let qos_lbl = detail.get_by_label("QoS class").rect().left();
    let priority_lbl = detail.get_by_label("Priority").rect().left();
    let sa_lbl = detail.get_by_label("Service account").rect().left();
    let rp_lbl = detail.get_by_label("Restart policy").rect().left();
    assert!((node_lbl - qos_lbl).abs() < 1.0);
    assert!((node_lbl - priority_lbl).abs() < 1.0);
    assert!((node_lbl - sa_lbl).abs() < 1.0);
    assert!((node_lbl - rp_lbl).abs() < 1.0);

    // Placement values must also all start at the same left edge.
    let node_val = detail.get_by_label("worker-a").rect().left();
    let qos_val = detail.get_by_label("Burstable").rect().left();
    let priority_val = detail.get_by_label("100").rect().left();
    let sa_val = detail.get_by_label("unique-sa").rect().left();
    let rp_val = detail.get_by_label("Always").rect().left();
    assert!(
        (node_val - qos_val).abs() < 1.0,
        "node_val {node_val} vs qos_val {qos_val}"
    );
    assert!(
        (node_val - priority_val).abs() < 1.0,
        "node_val {node_val} vs priority_val {priority_val}"
    );
    assert!(
        (node_val - sa_val).abs() < 1.0,
        "node_val {node_val} vs sa_val {sa_val}"
    );
    assert!(
        (node_val - rp_val).abs() < 1.0,
        "node_val {node_val} vs rp_val {rp_val}"
    );
}
