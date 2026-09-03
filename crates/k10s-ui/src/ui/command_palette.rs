//! Keyboard-first command palette search, rendering, and activation intents.

use std::collections::HashSet;

use egui::{
    Color32, Frame, Key, KeyboardShortcut, Modifiers, RichText, Stroke, TextEdit, WidgetInfo,
    WidgetType,
};
use k10s_protocol::{Context, ResourceIdentity, ResourceListRow};

use crate::workspace::{DetailTab, LauncherItem, WorkloadKind};

use super::{NamespaceCatalogState, ResourceFeed};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceJump {
    Detail,
    Logs,
    PreviousLogs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaletteAction {
    Resource(ResourceIdentity, ResourceJump),
    List(LauncherItem),
    Context(String),
    Namespace(String),
    Refresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultGroup {
    ResourceJumps,
    ListWindows,
    Commands,
}

impl ResultGroup {
    fn label(self) -> &'static str {
        match self {
            Self::ResourceJumps => "RESOURCE JUMPS",
            Self::ListWindows => "LIST WINDOWS",
            Self::Commands => "COMMANDS",
        }
    }

    fn result_limit(self) -> usize {
        match self {
            Self::ResourceJumps => 30,
            Self::ListWindows => 4,
            Self::Commands => 7,
        }
    }
}

#[derive(Debug, Clone)]
struct PaletteResult {
    group: ResultGroup,
    icon: &'static str,
    label: String,
    metadata: String,
    action: PaletteAction,
    score: i32,
}

#[derive(Debug, Default)]
pub(crate) struct CommandPalette {
    open: bool,
    query: String,
    cursor: usize,
    focus_query: bool,
}

impl CommandPalette {
    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    pub(crate) fn handle_global_shortcut(&mut self, ctx: &egui::Context) {
        if self.open || ctx.egui_wants_keyboard_input() {
            return;
        }
        let open = ctx.input_mut(|input| {
            let ctrl_k = input.consume_shortcut(&KeyboardShortcut::new(Modifiers::CTRL, Key::K));
            let colon = input
                .events
                .iter()
                .any(|event| matches!(event, egui::Event::Text(text) if text == ":"));
            if colon {
                input
                    .events
                    .retain(|event| !matches!(event, egui::Event::Text(text) if text == ":"));
            }
            ctrl_k || colon
        });
        if open {
            self.open = true;
            self.query.clear();
            self.cursor = 0;
            self.focus_query = true;
        }
    }

    pub(crate) fn show(
        &mut self,
        ctx: &egui::Context,
        contexts: &[Context],
        feed: &ResourceFeed,
    ) -> Option<(PaletteAction, bool)> {
        if !self.open {
            return None;
        }

        let mut results = search(&self.query, contexts, feed);
        self.cursor = self.cursor.min(results.len().saturating_sub(1));
        let mut chosen = None;
        let mut dismiss = false;

        egui::Window::new("Command palette")
            .id(egui::Id::new("k10s.command_palette"))
            .collapsible(false)
            .resizable(false)
            .fixed_size([620.0, 500.0])
            .anchor(egui::Align2::CENTER_TOP, [0.0, 72.0])
            .order(egui::Order::Foreground)
            .show(ctx, |ui| {
                // With an empty query, plain J/K are navigation keys as
                // advertised. Once a query exists they remain ordinary text
                // so resource names containing either letter stay searchable.
                let empty_query = self.query.is_empty();
                let (plain_j, plain_k) = ui.input_mut(|input| {
                    let plain_j = empty_query && input.consume_key(Modifiers::NONE, Key::J);
                    let plain_k = empty_query && input.consume_key(Modifiers::NONE, Key::K);
                    if plain_j || plain_k {
                        input.events.retain(|event| {
                            !matches!(event, egui::Event::Text(text) if (plain_j && text.eq_ignore_ascii_case("j")) || (plain_k && text.eq_ignore_ascii_case("k")))
                        });
                    }
                    (plain_j, plain_k)
                });
                let query = ui.add(
                    TextEdit::singleline(&mut self.query)
                        .hint_text("Search resources or type po, deploy, svc, ctx, ns…")
                        .desired_width(f32::INFINITY),
                );
                query.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::TextEdit, true, "Command palette search")
                });
                if self.focus_query {
                    query.request_focus();
                    self.focus_query = false;
                }
                if query.changed() {
                    self.cursor = 0;
                    results = search(&self.query, contexts, feed);
                }

                let (up, down, escape, enter, modified) = ui.input_mut(|input| {
                    let up = input.consume_key(Modifiers::NONE, Key::ArrowUp)
                        || input.consume_key(Modifiers::CTRL, Key::K)
                        || plain_k;
                    let down = input.consume_key(Modifiers::NONE, Key::ArrowDown)
                        || input.consume_key(Modifiers::CTRL, Key::J)
                        || plain_j;
                    let escape = input.consume_key(Modifiers::NONE, Key::Escape);
                    let modified = input.modifiers.shift || input.modifiers.ctrl || input.modifiers.command;
                    let enter = input.consume_key(input.modifiers, Key::Enter);
                    (up, down, escape, enter, modified)
                });
                if up && !results.is_empty() {
                    self.cursor = self.cursor.checked_sub(1).unwrap_or(results.len() - 1);
                }
                if down && !results.is_empty() {
                    self.cursor = (self.cursor + 1) % results.len();
                }
                dismiss = escape;
                if enter {
                    chosen = results.get(self.cursor).map(|result| (result.action.clone(), modified));
                }

                ui.separator();
                egui::ScrollArea::vertical().max_height(408.0).show(ui, |ui| {
                    let mut group = None;
                    for (index, result) in results.iter().enumerate() {
                        if group != Some(result.group) {
                            group = Some(result.group);
                            ui.add_space(8.0);
                            ui.label(RichText::new(result.group.label()).small().strong().color(super::theme::ACCENT));
                            ui.separator();
                        }
                        let selected = index == self.cursor;
                        let label = format!("{}  {}", result.icon, result.label);
                        let response = Frame::new()
                            .fill(if selected { super::theme::SELECTED_ROW } else { Color32::TRANSPARENT })
                            .stroke(if selected { Stroke::new(1.5, super::theme::ACCENT) } else { Stroke::NONE })
                            .inner_margin(egui::Margin::symmetric(8, 5))
                            .show(ui, |ui| {
                                ui.set_min_width(ui.available_width());
                                ui.horizontal(|ui| {
                                    ui.label(if selected { "▶" } else { " " });
                                    ui.vertical(|ui| {
                                        ui.label(RichText::new(label).strong());
                                        ui.label(RichText::new(&result.metadata).color(super::theme::MUTED_TEXT));
                                    }).response
                                }).inner
                            }).inner;
                        response.widget_info(|| {
                            WidgetInfo::selected(
                                WidgetType::Button,
                                true,
                                selected,
                                format!("{}; {}", result.label, result.metadata),
                            )
                        });
                        if response.clicked() {
                            chosen = Some((result.action.clone(), false));
                        }
                    }
                });
                ui.separator();
                let help = ui.horizontal_wrapped(|ui| {
                    for (key, action) in [("↑↓ / J K", "Navigate"), ("Enter", "Open / focus"), ("Shift+Enter", "New window"), ("Esc", "Close")] {
                        ui.label(RichText::new(key).monospace().strong());
                        ui.label(RichText::new(action).color(super::theme::MUTED_TEXT));
                    }
                }).response;
                help.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, "Keyboard help: Up and Down or J and K navigate; Enter opens or focuses; Shift Enter opens a new window; Escape closes")
                });
            });

        if dismiss || chosen.is_some() {
            self.open = false;
        }
        chosen
    }
}

fn search(query: &str, contexts: &[Context], feed: &ResourceFeed) -> Vec<PaletteResult> {
    let (prefix, needle) = split_prefix(query);
    let mut results = Vec::new();
    let mut identities = HashSet::new();
    let rows = feed
        .window_lists
        .values()
        .flat_map(|rows| rows.iter())
        .chain(feed.lists.values().flat_map(|rows| rows.iter()))
        .chain(feed.window_services.values().flat_map(|rows| rows.iter()))
        .chain(feed.services.iter().flatten());
    for row in rows {
        if !identities.insert(row.identity.clone()) || !prefix_allows_row(prefix, row) {
            continue;
        }
        if let Some(score) = resource_score(needle, row) {
            let namespace = row
                .identity
                .namespace
                .as_deref()
                .unwrap_or("cluster-scoped");
            let metadata = format!(
                "{} · {} · {} · {}",
                row.identity.context, namespace, row.identity.gvk.kind, row.summary
            );
            for (suffix, jump, adjustment) in [
                ("", ResourceJump::Detail, 0),
                (" — Logs", ResourceJump::Logs, -2),
                (" — Previous logs", ResourceJump::PreviousLogs, -3),
            ] {
                if jump != ResourceJump::Detail && row.identity.gvk.kind != "Pod" {
                    continue;
                }
                results.push(PaletteResult {
                    group: ResultGroup::ResourceJumps,
                    icon: resource_icon(&row.identity.gvk.kind),
                    label: format!("{}{}", row.identity.name, suffix),
                    metadata: metadata.clone(),
                    action: PaletteAction::Resource(row.identity.clone(), jump),
                    score: score + adjustment,
                });
            }
        }
    }

    if prefix.is_none()
        || matches!(
            prefix,
            Some(
                "po" | "pod"
                    | "pods"
                    | "deploy"
                    | "deployment"
                    | "deployments"
                    | "svc"
                    | "service"
                    | "services"
            )
        )
    {
        let mut candidates = list_candidates().to_vec();
        if feed.port_forward_available || feed.pod_port_forward_available {
            candidates.push((
                "Port Forwards",
                LauncherItem::PortForwards,
                &["pf", "port-forward", "port-forwards"],
            ));
        }
        for (label, item, aliases) in candidates {
            if prefix.is_some_and(|p| !aliases.contains(&p)) {
                continue;
            }
            if let Some(score) = text_score(needle, label) {
                results.push(PaletteResult {
                    group: ResultGroup::ListWindows,
                    icon: "[L]",
                    label: label.into(),
                    metadata: "Open or focus list window".into(),
                    action: PaletteAction::List(item),
                    score,
                });
            }
        }
    }

    if prefix.is_none() || prefix == Some("ctx") {
        for context in contexts {
            if let Some(score) = text_score(needle, &context.name) {
                results.push(PaletteResult {
                    group: ResultGroup::Commands,
                    icon: "[C]",
                    label: format!("Switch context to {}", context.name),
                    metadata: context.cluster.clone(),
                    action: PaletteAction::Context(context.name.clone()),
                    score,
                });
            }
        }
    }
    if (prefix.is_none() || prefix == Some("ns"))
        && let NamespaceCatalogState::Ready(names) = &feed.namespace_catalog
    {
        for name in names {
            if let Some(score) = text_score(needle, name) {
                results.push(PaletteResult {
                    group: ResultGroup::Commands,
                    icon: "[N]",
                    label: format!("Jump to namespace {name}"),
                    metadata: "Set namespace on the active list".into(),
                    action: PaletteAction::Namespace(name.clone()),
                    score,
                });
            }
        }
    }
    if prefix.is_none() && text_score(needle, "refresh resources").is_some() {
        results.push(PaletteResult {
            group: ResultGroup::Commands,
            icon: "[R]",
            label: "Refresh resources".into(),
            metadata: "Reload connected projections".into(),
            action: PaletteAction::Refresh,
            score: text_score(needle, "refresh resources").unwrap(),
        });
    }

    results.sort_by(|a, b| {
        a.group
            .cmp(&b.group)
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| a.label.cmp(&b.label))
    });
    let mut group_counts = [0_usize; 3];
    results.retain(|result| {
        let count = &mut group_counts[result.group as usize];
        if *count >= result.group.result_limit() {
            return false;
        }
        *count += 1;
        true
    });
    results
}

impl Ord for ResultGroup {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}
impl PartialOrd for ResultGroup {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn split_prefix(query: &str) -> (Option<&str>, &str) {
    let trimmed = query.trim();
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let first = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default().trim();
    let prefix = if first.eq_ignore_ascii_case("po") {
        Some("po")
    } else if first.eq_ignore_ascii_case("pod") {
        Some("pod")
    } else if first.eq_ignore_ascii_case("pods") {
        Some("pods")
    } else if first.eq_ignore_ascii_case("deploy") {
        Some("deploy")
    } else if first.eq_ignore_ascii_case("deployment") {
        Some("deployment")
    } else if first.eq_ignore_ascii_case("deployments") {
        Some("deployments")
    } else if first.eq_ignore_ascii_case("svc") {
        Some("svc")
    } else if first.eq_ignore_ascii_case("service") {
        Some("service")
    } else if first.eq_ignore_ascii_case("services") {
        Some("services")
    } else if first.eq_ignore_ascii_case("ctx") {
        Some("ctx")
    } else if first.eq_ignore_ascii_case("ns") {
        Some("ns")
    } else {
        None
    };
    if prefix.is_some() {
        (prefix, rest)
    } else {
        (None, trimmed)
    }
}

fn prefix_allows_row(prefix: Option<&str>, row: &ResourceListRow) -> bool {
    match prefix {
        None => true,
        Some("po" | "pod" | "pods") => row.identity.gvk.kind == "Pod",
        Some("deploy" | "deployment" | "deployments") => row.identity.gvk.kind == "Deployment",
        Some("svc" | "service" | "services") => row.identity.gvk.kind == "Service",
        Some("ctx" | "ns") => false,
        Some(_) => true,
    }
}

fn resource_score(needle: &str, row: &ResourceListRow) -> Option<i32> {
    let haystacks = [
        row.identity.name.as_str(),
        row.identity.context.as_str(),
        row.identity.namespace.as_deref().unwrap_or(""),
        row.identity.gvk.kind.as_str(),
        row.summary.as_str(),
    ];
    let terms = needle.split_whitespace().collect::<Vec<_>>();
    if terms.is_empty() {
        return Some(1);
    }
    terms.into_iter().try_fold(0, |total, term| {
        let term = term.to_lowercase();
        haystacks
            .iter()
            .filter_map(|haystack| text_score(&term, haystack))
            .max()
            .map(|score| total + score)
    })
}

fn text_score(needle: &str, haystack: &str) -> Option<i32> {
    if needle.is_empty() {
        return Some(1);
    }
    let haystack = haystack.to_lowercase();
    if haystack == needle {
        Some(1000)
    } else if haystack.starts_with(needle) {
        Some(800 - haystack.len() as i32)
    } else if let Some(index) = haystack.find(needle) {
        Some(600 - index as i32)
    } else {
        let mut chars = needle.chars();
        let mut wanted = chars.next()?;
        let mut gaps = 0;
        for character in haystack.chars() {
            if character == wanted {
                if let Some(next) = chars.next() {
                    wanted = next;
                } else {
                    return Some(300 - gaps);
                }
            } else {
                gaps += 1;
            }
        }
        None
    }
}

fn resource_icon(kind: &str) -> &'static str {
    match kind {
        "Pod" => "[P]",
        "Deployment" => "[D]",
        "Service" => "[S]",
        _ => "[*]",
    }
}

fn list_candidates() -> [(&'static str, LauncherItem, &'static [&'static str]); 3] {
    [
        (
            "Pods",
            LauncherItem::Workload(WorkloadKind::Pods),
            &["po", "pod", "pods"],
        ),
        (
            "Deployments",
            LauncherItem::Workload(WorkloadKind::Deployments),
            &["deploy", "deployment", "deployments"],
        ),
        (
            "Services",
            LauncherItem::Services,
            &["svc", "service", "services"],
        ),
    ]
}

pub(crate) fn tab_for_jump(jump: ResourceJump) -> DetailTab {
    match jump {
        ResourceJump::Detail => DetailTab::Overview,
        ResourceJump::Logs | ResourceJump::PreviousLogs => DetailTab::Logs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k10s_protocol::{BackendRevision, GroupVersionKind};

    fn row(kind: &str, name: &str, summary: &str) -> ResourceListRow {
        ResourceListRow {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: GroupVersionKind::core("v1", kind),
                namespace: Some("payments".into()),
                name: name.into(),
                uid: format!("uid-{name}"),
            },
            revision: BackendRevision::new(1),
            labels: Default::default(),
            summary: summary.into(),
            created_at: String::new(),
            projection: None,
        }
    }

    #[test]
    fn ranks_exact_name_above_status_and_exposes_pod_actions() {
        let mut feed = ResourceFeed::default();
        feed.lists.insert(
            WorkloadKind::Pods,
            vec![
                row("Pod", "worker-7f498", "CrashLoopBackOff · 7 restarts"),
                row("Pod", "crashloopbackoff", "Running · 0 restarts"),
            ],
        );
        let results = search("crashloopbackoff", &[], &feed);
        assert_eq!(results[0].label, "crashloopbackoff");
        assert!(
            results
                .iter()
                .any(|result| result.label == "worker-7f498 — Logs")
        );
        assert!(
            results
                .iter()
                .any(|result| result.label == "worker-7f498 — Previous logs")
        );
    }

    #[test]
    fn prefixes_restrict_resources_lists_contexts_and_namespaces() {
        let mut feed = ResourceFeed::default();
        feed.lists.insert(
            WorkloadKind::Pods,
            vec![
                row("Pod", "worker", "CrashLoopBackOff · 7 restarts"),
                row("Deployment", "worker", "Degraded"),
            ],
        );
        feed.namespace_catalog = NamespaceCatalogState::Ready(vec!["payments".into()]);
        let contexts = vec![Context {
            name: "dev-local".into(),
            cluster: "dev".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }];
        assert!(search("po worker", &contexts, &feed).iter().all(|result| !matches!(&result.action, PaletteAction::Resource(identity, _) if identity.gvk.kind != "Pod")));
        assert!(
            search("deploy", &contexts, &feed)
                .iter()
                .any(|result| result.label == "Deployments")
        );
        assert!(
            search("svc", &contexts, &feed)
                .iter()
                .any(|result| result.label == "Services")
        );
        assert!(
            search("ctx dev", &contexts, &feed)
                .iter()
                .any(|result| matches!(result.action, PaletteAction::Context(_)))
        );
        assert!(
            search("ns pay", &contexts, &feed)
                .iter()
                .any(|result| matches!(result.action, PaletteAction::Namespace(_)))
        );
    }

    #[test]
    fn compound_terms_match_across_namespace_and_status() {
        let mut feed = ResourceFeed::default();
        feed.lists.insert(
            WorkloadKind::Pods,
            vec![row("Pod", "worker", "CrashLoopBackOff · 7 restarts")],
        );

        let results = search("po payments crash", &[], &feed);
        assert!(results.iter().any(|result| {
            matches!(
                &result.action,
                PaletteAction::Resource(identity, ResourceJump::Detail)
                    if identity.name == "worker"
            )
        }));
    }

    #[test]
    fn result_cap_reserves_space_for_every_required_group() {
        let mut feed = ResourceFeed::default();
        feed.lists.insert(
            WorkloadKind::Pods,
            (0..25)
                .map(|index| row("Pod", &format!("worker-{index}"), "Running"))
                .collect(),
        );
        feed.namespace_catalog = NamespaceCatalogState::Ready(vec!["payments".into()]);
        let contexts = vec![Context {
            name: "dev-local".into(),
            cluster: "dev".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }];

        let results = search("", &contexts, &feed);
        assert!(results.len() <= 40);
        for group in [
            ResultGroup::ResourceJumps,
            ResultGroup::ListWindows,
            ResultGroup::Commands,
        ] {
            assert!(results.iter().any(|result| result.group == group));
        }
    }
}
