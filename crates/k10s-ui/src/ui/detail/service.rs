//! The core/v1 Service detail body: Overview rows from the normalized
//! projection, structured read-only Ports, events, and the guarded YAML
//! workflow.
//!
//! This panel is deliberately action-free beyond YAML editing and bounded
//! port-forward lifecycle controls: no Scale, Delete, logs, or exec.

use egui::{Grid, RichText};
use k10s_protocol::{ResourceDetailResponse, ResourceIdentity, ServiceProjection};

use crate::ui::tools;
use crate::workspace::{DetailState, DetailTab, WindowId, WorkspaceCommand};

use crate::ui::resource_window::RowIdentity;

#[allow(clippy::too_many_arguments)]
pub(super) fn show<I>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    detail: &DetailState<I>,
    view: &ResourceDetailResponse,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    yaml: &mut tools::YamlEditors,
    _port_drafts: Option<&std::collections::BTreeMap<String, String>>,
    _resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) where
    I: RowIdentity,
{
    let projection = projection_of(view, presentation.identity);
    match detail.active_tab {
        DetailTab::Overview => overview_tab(ui, window_id, projection, presentation),
        DetailTab::Endpoints => endpoints_tab(ui, window_id, projection, presentation, queued),
        DetailTab::Pods => pods_tab(ui, presentation, _resource_actions, queued),
        DetailTab::Events => super::events::show(ui, view.events_condition, &view.events),
        DetailTab::Yaml => {
            let mutations_allowed =
                presentation.mutations_allowed && view.capabilities.can_edit_yaml;
            tools::yaml::show(
                ui,
                window_id,
                yaml,
                detail.identity.as_row_identity(),
                Some(view.manifest.as_str()),
                mutations_allowed,
                queued,
            );
        }
        // Services never offer workload tabs; a stale command targeting one
        // of them renders nothing rather than generic content.
        _ => {}
    }
}

/// The Service projection of a detail response, if populated.
fn projection_of<'a>(
    view: &'a ResourceDetailResponse,
    identity: &ResourceIdentity,
) -> Option<&'a ServiceProjection> {
    if view.identity != *identity {
        return None;
    }
    match &view.projection {
        Some(k10s_protocol::ResourceProjection::Service(projection)) => Some(projection),
        _ => None,
    }
}

/// Overview tab: every field comes from the projection only when present.
fn overview_tab(
    ui: &mut egui::Ui,
    window_id: WindowId,
    projection: Option<&ServiceProjection>,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    let Some(projection) = projection else {
        show_unavailable(ui, window_id, presentation);
        return;
    };
    if super::overview::two_column(
        ui,
        |column| {
            service_ports(column, window_id, projection);
            service_dns(column, projection, presentation);
        },
        |column| {
            service_routing(column, window_id, projection);
            service_selectors(column, projection);
            service_identity(column, window_id, presentation);
        },
    ) {
        return;
    }
    service_ports(ui, window_id, projection);
    service_dns(ui, projection, presentation);
    service_routing(ui, window_id, projection);
    service_selectors(ui, projection);
    service_identity(ui, window_id, presentation);
}

pub(super) fn show_unavailable(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    if super::overview::two_column(
        ui,
        |column| {
            column.heading("OPERATIONAL");
            column.label("Structured details unavailable");
        },
        |column| {
            column.heading("CONFIGURATION");
            column.label("Structured details unavailable");
            service_identity(column, window_id, presentation);
        },
    ) {
        return;
    }
    ui.heading("OPERATIONAL");
    ui.label("Structured details unavailable");
    ui.heading("CONFIGURATION");
    ui.label("Structured details unavailable");
    service_identity(ui, window_id, presentation);
}

fn service_ports(ui: &mut egui::Ui, window_id: WindowId, projection: &ServiceProjection) {
    ui.heading("PORTS");
    if projection.ports.is_empty() {
        ui.label("No declared ports");
        return;
    }
    Grid::new(("k10s.detail.service.ports", window_id.0))
        .num_columns(5)
        .striped(true)
        .show(ui, |ui| {
            for header in ["NAME", "PORT", "TARGET PORT", "NODE PORT", "PROTOCOL"] {
                ui.label(
                    RichText::new(header)
                        .small()
                        .color(crate::ui::theme::MUTED_TEXT),
                );
            }
            ui.end_row();
            for port in &projection.ports {
                ui.label(port.name.as_deref().unwrap_or("—"));
                ui.label(port.service_port.to_string());
                let target = match &port.target_port {
                    k10s_protocol::TargetPort::Number { number } => number.to_string(),
                    k10s_protocol::TargetPort::Name { name } => format!("{name} · named port"),
                };
                ui.label(target);
                ui.label(
                    port.node_port
                        .map_or_else(|| "—".into(), |port| port.to_string()),
                );
                ui.label(format!("{:?}", port.protocol).to_uppercase());
                ui.end_row();
            }
        });
}

fn service_dns(
    ui: &mut egui::Ui,
    projection: &ServiceProjection,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    ui.heading("DNS");
    let primary_port = projection.ports.first().map(|p| p.service_port);
    let port_suffix = primary_port.map_or_else(String::new, |port| format!(":{port}"));
    let name = &presentation.identity.name;
    let (cluster_dns, same_ns_dns, cross_ns_dns) = match presentation.identity.namespace.as_deref()
    {
        Some(ns) => (
            format!("{name}.{ns}.svc.cluster.local{port_suffix}"),
            format!("{name}{port_suffix}"),
            format!("{name}.{ns}{port_suffix}"),
        ),
        None => (
            format!("{name}.svc.cluster.local{port_suffix}"),
            format!("{name}{port_suffix}"),
            name.clone(),
        ),
    };
    super::overview::kv_value_row(
        ui,
        "Cluster DNS",
        super::overview::KvValue::new(&cluster_dns).copyable(),
    );
    super::overview::kv_value_row(
        ui,
        "Same namespace",
        super::overview::KvValue::new(&same_ns_dns).copyable(),
    );
    super::overview::kv_value_row(
        ui,
        "Cross namespace",
        super::overview::KvValue::new(&cross_ns_dns).copyable(),
    );
}

fn service_routing(ui: &mut egui::Ui, window_id: WindowId, projection: &ServiceProjection) {
    ui.heading("ROUTING");
    Grid::new(("k10s.detail.service.traffic", window_id.0)).show(ui, |ui| {
        overview_row(ui, "Type", nonempty(&projection.service_type));
        overview_row(ui, "Cluster IP", &cluster_ip_display(projection));
        overview_row(
            ui,
            "Session affinity",
            projection
                .session_affinity
                .as_deref()
                .map(nonempty)
                .unwrap_or("None"),
        );
        overview_row(
            ui,
            "Internal policy",
            projection
                .internal_traffic_policy
                .as_deref()
                .map(nonempty)
                .unwrap_or("Cluster"),
        );
        if let Some(value) = projection.external_traffic_policy.as_deref() {
            overview_row(ui, "External policy", nonempty(value));
        }
        if let Some(value) = &projection.external_name {
            overview_row(ui, "External name", value);
        }
        overview_row(ui, "IP family", &ip_family_display(projection));
    });
}

fn cluster_ip_display(projection: &ServiceProjection) -> String {
    if projection.cluster_ips.is_empty() {
        "—".into()
    } else {
        projection.cluster_ips.join(", ")
    }
}

fn ip_family_display(projection: &ServiceProjection) -> String {
    let ipv4 = projection
        .cluster_ips
        .iter()
        .any(|ip| !ip.contains(':') && ip != "None");
    let ipv6 = projection.cluster_ips.iter().any(|ip| ip.contains(':'));
    match (ipv4, ipv6) {
        (true, true) => "IPv4 / IPv6 · DualStack".into(),
        (true, false) => "IPv4 · SingleStack".into(),
        (false, true) => "IPv6 · SingleStack".into(),
        (false, false) => "—".into(),
    }
}

fn service_selectors(ui: &mut egui::Ui, projection: &ServiceProjection) {
    ui.heading("SELECTOR");
    if projection.selector.is_empty() {
        ui.label("No selector");
        return;
    }
    let selectors = projection
        .selector
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    super::overview::metadata_chips(ui, &selectors, " ");
}

fn nonempty(value: &str) -> &str {
    if value.is_empty() { "—" } else { value }
}

fn service_identity(
    ui: &mut egui::Ui,
    window_id: WindowId,
    presentation: &super::presentation::DetailPresentationInput<'_>,
) {
    ui.heading("IDENTITY");
    Grid::new(("k10s.detail.service.identity", window_id.0)).show(ui, |ui| {
        overview_row(ui, "Name", &presentation.identity.name);
        overview_row(
            ui,
            "Namespace",
            presentation.identity.namespace.as_deref().unwrap_or("—"),
        );
        overview_row(
            ui,
            "UID",
            if presentation.identity.uid.is_empty() {
                "—"
            } else {
                &presentation.identity.uid
            },
        );
        overview_row(ui, "Context", &presentation.identity.context);
    });
}

fn overview_row(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.label(format!("{label} {value}"));
    ui.end_row();
}

#[derive(Clone, Default)]
struct EndpointsTabState {
    filter: String,
    unroutable_only: bool,
    group_by_slice: bool,
}

pub(super) fn configure_frame(
    presentation: &super::presentation::DetailPresentationInput<'_>,
    frame: &mut super::presentation::DetailFrameProjection<'_>,
    active_tab: DetailTab,
) {
    if active_tab == DetailTab::Endpoints {
        frame.vitals_in_footer = true;
    }
    let projection = match presentation.primary {
        super::presentation::DetailPrimary::Loaded(view) => {
            frame.actions.can_delete = view.capabilities.can_delete;
            projection_of(view, presentation.identity)
        }
        _ => None,
    };
    if let Some(projection) = projection {
        let total = projection.endpoints.len();
        let routable = projection
            .endpoints
            .iter()
            .filter(|ep| ep.ready && ep.address.is_some())
            .count();
        let draining = projection
            .endpoints
            .iter()
            .filter(|ep| ep.terminating)
            .count();
        let slices_count = if projection.slices.is_empty() {
            if total > 0 { 1 } else { 0 }
        } else {
            projection.slices.len()
        };

        if total > 0 {
            frame.endpoint_count = Some(total);
        }

        let mut pods_set = std::collections::BTreeSet::new();
        for ep in &projection.endpoints {
            if let Some(pod) = &ep.target_pod {
                pods_set.insert(pod.clone());
            }
        }
        if !pods_set.is_empty() {
            frame.pod_count = Some(pods_set.len());
        }

        let (status_val, status_tone) = if total == 0 {
            (
                "○ No endpoints".to_owned(),
                super::presentation::DetailVitalTone::Neutral,
            )
        } else if routable < total {
            (
                format!("▲ {routable} of {total} routable"),
                super::presentation::DetailVitalTone::Warning,
            )
        } else {
            (
                format!("● {total} endpoints ready"),
                super::presentation::DetailVitalTone::Healthy,
            )
        };

        frame.visible_vitals = vec![
            super::presentation::DetailVital {
                label: "Status",
                value: status_val,
                tone: status_tone,
                shape: None,
                hint: None,
            },
            super::presentation::DetailVital::new("Slices", slices_count.to_string()),
            super::presentation::DetailVital::new(
                "Address type",
                projection
                    .slices
                    .first()
                    .map_or("IPv4", |s| s.address_type.as_str()),
            ),
            super::presentation::DetailVital::new(
                "Port",
                projection.ports.first().map_or_else(
                    || "—".into(),
                    |p| {
                        format!(
                            "{}/{}",
                            p.service_port,
                            format!("{:?}", p.protocol).to_uppercase()
                        )
                    },
                ),
            ),
        ];

        if draining > 0 {
            frame.visible_vitals.push(super::presentation::DetailVital {
                label: "Draining",
                value: draining.to_string(),
                tone: super::presentation::DetailVitalTone::Warning,
                shape: None,
                hint: None,
            });
        }
    }
}

/// Endpoints tab: interactive addresses, slices, and topology breakdown.
fn endpoints_tab<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    projection: Option<&ServiceProjection>,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    let Some(projection) = projection else {
        ui.label("No structured projection available");
        return;
    };

    let state_id = egui::Id::new(("k10s.detail.service.endpoints_state", window_id.0));
    let mut state = ui
        .ctx()
        .data_mut(|d| d.get_temp::<EndpointsTabState>(state_id))
        .unwrap_or_default();

    // 1. Sub-toolbar
    ui.horizontal(|ui| {
        ui.add(
            egui::TextEdit::singleline(&mut state.filter)
                .hint_text("⌕ Filter address, pod, node...")
                .desired_width(220.0),
        );
        let unroutable_text = if state.unroutable_only {
            RichText::new("▲ 仅看不可路由").color(crate::ui::theme::WARNING)
        } else {
            RichText::new("▲ 仅看不可路由")
        };
        if ui
            .selectable_label(state.unroutable_only, unroutable_text)
            .clicked()
        {
            state.unroutable_only = !state.unroutable_only;
        }
        if ui
            .selectable_label(state.group_by_slice, "Group by slice")
            .clicked()
        {
            state.group_by_slice = !state.group_by_slice;
        }
        if ui.button("⧉ 复制全部地址").clicked() {
            let addrs: Vec<String> = projection
                .endpoints
                .iter()
                .filter_map(|ep| {
                    if ep.ready && ep.address.is_some() {
                        let addr = ep.address.as_deref().unwrap_or_default();
                        let port = ep.port.map_or(String::new(), |p| format!(":{p}"));
                        Some(format!("{addr}{port}"))
                    } else {
                        None
                    }
                })
                .collect();
            ui.ctx().copy_text(addrs.join("\n"));
        }
        let _ = ui.button("↻");
    });

    render_port_forward_controls(ui, window_id, projection, presentation, queued);

    ui.add_space(8.0);

    let filter_lower = state.filter.to_lowercase();
    let filtered_endpoints: Vec<&k10s_protocol::ServiceEndpointProjection> = projection
        .endpoints
        .iter()
        .filter(|ep| {
            if state.unroutable_only && ep.ready && ep.address.is_some() && !ep.terminating {
                return false;
            }
            if !filter_lower.is_empty() {
                let matches_addr = ep
                    .address
                    .as_deref()
                    .is_some_and(|a| a.to_lowercase().contains(&filter_lower));
                let matches_pod = ep
                    .target_pod
                    .as_deref()
                    .is_some_and(|p| p.to_lowercase().contains(&filter_lower));
                let matches_node = ep
                    .node
                    .as_deref()
                    .is_some_and(|n| n.to_lowercase().contains(&filter_lower));
                let matches_zone = ep
                    .zone
                    .as_deref()
                    .is_some_and(|z| z.to_lowercase().contains(&filter_lower));
                let matches_port = ep
                    .port
                    .is_some_and(|p| p.to_string().contains(&filter_lower));
                if !matches_addr && !matches_pod && !matches_node && !matches_zone && !matches_port
                {
                    return false;
                }
            }
            true
        })
        .collect();

    let total = projection.endpoints.len();
    let routable = projection
        .endpoints
        .iter()
        .filter(|ep| ep.ready && ep.address.is_some())
        .count();

    // 2. ADDRESSES section
    ui.horizontal(|ui| {
        ui.heading("ADDRESSES");
        ui.label(
            RichText::new(format!("{total} 个 · {routable} 可路由"))
                .color(crate::ui::theme::MUTED_TEXT),
        );
    });

    if filtered_endpoints.is_empty() {
        if projection.endpoints.is_empty() {
            ui.label("No endpoint addresses found");
        } else {
            ui.label("No addresses match filter");
        }
    } else {
        Grid::new(("k10s.detail.service.endpoints_grid", window_id.0))
            .num_columns(6)
            .striped(true)
            .show(ui, |ui| {
                for header in [
                    "ADDRESS",
                    "PORT",
                    "TARGET POD",
                    "NODE",
                    "ZONE",
                    "CONDITIONS",
                ] {
                    ui.label(
                        RichText::new(header)
                            .small()
                            .color(crate::ui::theme::MUTED_TEXT),
                    );
                }
                ui.end_row();

                let render_row =
                    |ui: &mut egui::Ui, ep: &k10s_protocol::ServiceEndpointProjection| {
                        ui.label(ep.address.as_deref().unwrap_or("—"));
                        ui.label(ep.port.map_or_else(|| "—".into(), |p| p.to_string()));
                        ui.label(ep.target_pod.as_deref().unwrap_or("—"));
                        ui.label(ep.node.as_deref().unwrap_or("—"));
                        ui.label(ep.zone.as_deref().unwrap_or("—"));
                        if ep.ready && ep.serving && !ep.terminating {
                            ui.label(
                                RichText::new("● ready · serving").color(crate::ui::theme::HEALTHY),
                            );
                        } else if ep.terminating {
                            ui.label(
                                RichText::new("◐ terminating · 仍在 serving")
                                    .color(crate::ui::theme::WARNING),
                            );
                        } else {
                            ui.label(
                                RichText::new("✕ not ready · 无地址")
                                    .color(crate::ui::theme::DANGER),
                            );
                        }
                        ui.end_row();
                    };

                if state.group_by_slice {
                    let mut grouped: std::collections::BTreeMap<
                        &str,
                        Vec<&k10s_protocol::ServiceEndpointProjection>,
                    > = std::collections::BTreeMap::new();
                    for ep in &filtered_endpoints {
                        grouped
                            .entry(ep.slice_name.as_deref().unwrap_or("default"))
                            .or_default()
                            .push(ep);
                    }
                    for (slice_name, eps) in grouped {
                        ui.label(RichText::new(format!("Slice: {slice_name}")).strong());
                        ui.end_row();
                        for ep in eps {
                            render_row(ui, ep);
                        }
                    }
                } else {
                    for ep in &filtered_endpoints {
                        render_row(ui, ep);
                    }
                }
            });

        ui.add_space(4.0);
        ui.label(
            RichText::new("◐ 的地址仍会收到已建立连接的流量，但不再接受新连接。kube-proxy 在 Pod 完全消失后才移除。")
                .small()
                .color(crate::ui::theme::MUTED_TEXT),
        );
    }

    ui.add_space(14.0);

    // 3. SLICES section
    ui.horizontal(|ui| {
        ui.heading("SLICES");
        ui.label(
            RichText::new(format!("{}", projection.slices.len()))
                .color(crate::ui::theme::MUTED_TEXT),
        );
    });
    if projection.slices.is_empty() {
        ui.label("No EndpointSlices");
    } else {
        Grid::new(("k10s.detail.service.slices_grid", window_id.0))
            .num_columns(5)
            .striped(true)
            .show(ui, |ui| {
                for header in ["NAME", "ADDRESS TYPE", "PORTS", "ENDPOINTS", "AGE"] {
                    ui.label(
                        RichText::new(header)
                            .small()
                            .color(crate::ui::theme::MUTED_TEXT),
                    );
                }
                ui.end_row();
                for slice in &projection.slices {
                    let name_text = match &slice.managed_by {
                        Some(managed) => format!("{} · 由 {managed} 生成", slice.name),
                        None => slice.name.clone(),
                    };
                    ui.label(name_text);
                    ui.label(&slice.address_type);
                    ui.label(if slice.ports.is_empty() {
                        "—".into()
                    } else {
                        slice.ports.join(", ")
                    });
                    ui.label(format!(
                        "{} / {}",
                        slice.endpoint_count, slice.max_endpoints
                    ));
                    ui.label(slice.age.as_deref().unwrap_or("—"));
                    ui.end_row();
                }
            });
    }

    ui.add_space(14.0);

    // 4. TOPOLOGY section
    ui.heading("TOPOLOGY");
    Grid::new(("k10s.detail.service.topology_grid", window_id.0)).show(ui, |ui| {
        overview_row(
            ui,
            "Topology hints",
            projection
                .topology_hints
                .as_deref()
                .unwrap_or("未启用 · 流量不按 zone 优先"),
        );
        let policy = match projection.internal_traffic_policy.as_deref() {
            Some("Local") => "Local · 仅转发至本节点端点",
            _ => "Cluster · 任意节点均可转发",
        };
        overview_row(ui, "Internal policy", policy);
    });

    ui.ctx().data_mut(|d| d.insert_temp(state_id, state));
}

fn pods_tab<I: RowIdentity>(
    ui: &mut egui::Ui,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    resource_actions: &mut Vec<crate::ui::ResourceAction>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    super::related::show(
        ui,
        presentation.identity,
        presentation.relations,
        resource_actions,
        queued,
    );
}

fn render_port_forward_controls<I: RowIdentity>(
    ui: &mut egui::Ui,
    window_id: WindowId,
    projection: &ServiceProjection,
    presentation: &super::presentation::DetailPresentationInput<'_>,
    queued: &mut Vec<WorkspaceCommand<I>>,
) {
    if projection.ports.is_empty() {
        ui.label("No declared ports");
        return;
    }
    if let Some(error) = presentation.port_forward_error {
        ui.label(RichText::new(error).color(crate::ui::theme::WARNING));
    }
    match presentation.port_forward_list_state {
        crate::ui::PortForwardListState::Loading => {
            ui.label("Loading port-forward sessions…");
        }
        crate::ui::PortForwardListState::Reconstructing => {
            ui.label("Reconstructing port-forward sessions…");
        }
        crate::ui::PortForwardListState::Ready => {}
    }
    for port in &projection.ports {
        let line = crate::ui::port_detail_label(port);
        let label = ui.label(RichText::new(line.clone()).strong());
        label.widget_info(|| {
            egui::WidgetInfo::labeled(egui::WidgetType::Label, true, format!("Port {line}"))
        });
        if port.protocol != k10s_protocol::TransportProtocol::Tcp {
            continue;
        }
        if presentation.port_forward_list_state != crate::ui::PortForwardListState::Ready {
            continue;
        }
        let service = presentation.identity;
        let session = presentation
            .port_forward_sessions
            .iter()
            .filter(|session| {
                matches!(
                    session.state,
                    k10s_protocol::PortForwardSessionState::Starting
                        | k10s_protocol::PortForwardSessionState::Active
                        | k10s_protocol::PortForwardSessionState::Stopping
                ) && matches!(
                    &session.target,
                    k10s_protocol::PortForwardTarget::Service { identity, port: selector }
                        if identity.uid == service.uid
                            && match selector {
                                k10s_protocol::PortForwardPortSelector::Name { name } => {
                                    port.name.as_ref() == Some(name)
                                }
                                k10s_protocol::PortForwardPortSelector::Number { number } => {
                                    *number == port.service_port
                                }
                            }
                )
            })
            .min_by_key(|session| match session.state {
                k10s_protocol::PortForwardSessionState::Active => 0,
                k10s_protocol::PortForwardSessionState::Starting => 1,
                k10s_protocol::PortForwardSessionState::Stopping => 2,
                k10s_protocol::PortForwardSessionState::Stopped
                | k10s_protocol::PortForwardSessionState::Failed => 3,
            });
        if let Some(session) = session {
            ui.label(format!(
                "{} · {}:{} · {:?}",
                session.local_addr, session.pod.name, session.pod_port, session.state
            ));
            ui.horizontal(|ui| {
                if ui.button("Copy address").clicked() {
                    ui.ctx().copy_text(session.local_addr.clone());
                }
                let scheme = if port.service_port == 443
                    || port.name.as_deref() == Some("https")
                    || port.app_protocol.as_deref() == Some("https")
                {
                    "https"
                } else {
                    "http"
                };
                let url = format!("{scheme}://{}", session.local_addr);
                if ui.button("Copy URL").clicked() {
                    ui.ctx().copy_text(url);
                }
                let stoppable = matches!(
                    session.state,
                    k10s_protocol::PortForwardSessionState::Starting
                        | k10s_protocol::PortForwardSessionState::Active
                );
                if ui
                    .add_enabled(stoppable, egui::Button::new("Stop"))
                    .clicked()
                {
                    queued.push(WorkspaceCommand::StopPortForward(
                        session.id.as_str().to_owned(),
                    ));
                }
            });
        } else if presentation.port_forward_capability {
            let start = ui.push_id(
                (
                    "k10s.detail.service.port.start",
                    window_id.0,
                    port.service_port,
                ),
                |ui| ui.add_enabled(presentation.mutations_allowed, egui::Button::new("Start")),
            );
            if !presentation.mutations_allowed {
                ui.label(
                    RichText::new(crate::ui::port_forward::PORT_FORWARD_AUTHORITY_UNAVAILABLE)
                        .color(crate::ui::theme::WARNING),
                );
            }
            if start.inner.clicked() {
                let selector = port.name.clone().map_or(
                    k10s_protocol::PortForwardPortSelector::Number {
                        number: port.service_port,
                    },
                    |name| k10s_protocol::PortForwardPortSelector::Name { name },
                );
                let initial_local_port = match port.target_port {
                    k10s_protocol::TargetPort::Number { number } => number,
                    k10s_protocol::TargetPort::Name { .. } => port.service_port,
                };
                queued.push(WorkspaceCommand::StartPortForward {
                    target: k10s_protocol::PortForwardTarget::Service {
                        identity: service.clone(),
                        port: selector,
                    },
                    remote_label: line,
                    initial_local_port,
                });
            }
        } else {
            ui.label("Port forwarding is available in the desktop application");
        }
    }
}
