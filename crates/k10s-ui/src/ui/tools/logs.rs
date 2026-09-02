//! Connected log viewer state.
//!
//! A pure state machine fed by stream chunks from a dedicated logs socket.
//! The retained view is a bounded tail: appending beyond the bound truncates
//! the oldest lines deterministically and counts them. Pause stops buffering
//! (dropped lines are counted, never silently mixed into history), follow is
//! reserved for autoscroll behavior in the renderer, and find filters the
//! retained buffer without destroying it.

use std::collections::{HashMap, VecDeque};

use egui::{RichText, ScrollArea};
use k10s_protocol::StreamTarget;

use crate::workspace::WindowId;

/// Hard character cap applied to each retained line; longer source lines are
/// truncated with [`LogsTool::TRUNCATION_MARKER`].
pub const MAX_LINE_CHARS: usize = 2_000;
/// Marker appended to truncated lines.
pub const TRUNCATION_MARKER: &str = "…";

/// Lifecycle of one connected logs view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogsPhase {
    /// Not attached to any stream socket.
    Disconnected,
    /// Ticket redeemed; awaiting the first chunk.
    Connecting,
    /// Streaming with the tail bound applied to every append.
    Streaming,
}

/// Retained bounded log view for one pod/container target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsTool {
    target: StreamTarget,
    tail_capacity: usize,
    lines: VecDeque<String>,
    phase: LogsPhase,
    /// Whether this source still owns its single automatic connection claim.
    auto_connect_available: bool,
    paused: bool,
    follow: bool,
    /// One-shot request for the renderer to discard persisted scroll state.
    scroll_reset: bool,
    previous: bool,
    source_defaults_applied: bool,
    since_seconds: Option<i64>,
    wrap: bool,
    find: Option<String>,
    truncated_lines: u64,
    dropped_while_paused: u64,
    /// Absolute number of chunks ever buffered; drives `since`.
    total_received: u64,
    /// When set, only chunks buffered at or after this absolute index are
    /// shown (a deterministic stand-in for a server-side since cursor).
    since_received: Option<u64>,
    /// Last rejection reason surfaced by the application layer.
    last_error: Option<String>,
}

impl LogsTool {
    /// Create a disconnected viewer retaining at most `tail_capacity`
    /// newest lines.
    #[must_use]
    pub fn new(target: StreamTarget, tail_capacity: usize) -> Self {
        Self {
            target,
            tail_capacity: tail_capacity.max(1),
            lines: VecDeque::new(),
            phase: LogsPhase::Disconnected,
            auto_connect_available: true,
            paused: false,
            follow: true,
            scroll_reset: true,
            previous: false,
            source_defaults_applied: false,
            since_seconds: Some(300),
            wrap: false,
            find: None,
            truncated_lines: 0,
            dropped_while_paused: 0,
            total_received: 0,
            since_received: None,
            last_error: None,
        }
    }

    /// Target this viewer streams from.
    #[must_use]
    pub fn target(&self) -> &StreamTarget {
        &self.target
    }

    /// Current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> LogsPhase {
        self.phase
    }

    /// Atomically claim this source's one automatic connection attempt.
    pub fn begin_auto_connect(&mut self) -> bool {
        if self.phase != LogsPhase::Disconnected || !self.auto_connect_available {
            return false;
        }
        self.auto_connect_available = false;
        self.phase = LogsPhase::Connecting;
        self.last_error = None;
        self.follow = true;
        true
    }

    /// Whether a consumed or failed attempt can be retried explicitly.
    #[must_use]
    pub fn can_retry(&self) -> bool {
        self.phase == LogsPhase::Disconnected && !self.auto_connect_available
    }

    /// Start one user-requested retry for the current source.
    pub fn retry(&mut self) -> bool {
        if !self.can_retry() {
            return false;
        }
        self.phase = LogsPhase::Connecting;
        self.last_error = None;
        self.follow = true;
        self.scroll_reset = true;
        true
    }

    /// Whether incoming chunks are currently being dropped.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Whether the view should autoscroll to the newest line.
    #[must_use]
    pub fn follows(&self) -> bool {
        self.follow
    }

    #[must_use]
    pub fn previous(&self) -> bool {
        self.previous
    }

    pub fn set_previous(&mut self, previous: bool) {
        if self.previous != previous {
            self.previous = previous;
            self.reset_source_history();
        }
    }

    pub fn apply_source_defaults(&mut self, previous: bool) {
        if !self.source_defaults_applied {
            self.previous = previous;
            self.source_defaults_applied = true;
        }
    }

    #[must_use]
    pub fn since_seconds(&self) -> Option<i64> {
        self.since_seconds
    }

    pub fn set_since_seconds(&mut self, since_seconds: Option<i64>) {
        if self.since_seconds != since_seconds {
            self.since_seconds = since_seconds;
            self.reset_source_history();
        }
    }

    #[must_use]
    pub fn wraps(&self) -> bool {
        self.wrap
    }

    pub fn set_wrap(&mut self, wrap: bool) {
        self.wrap = wrap;
    }

    pub fn select_container(&mut self, container: &str) {
        if self.target.container != container {
            self.target.container = container.to_owned();
            self.reset_source_history();
        }
    }

    fn reset_source_history(&mut self) {
        self.lines.clear();
        self.truncated_lines = 0;
        self.dropped_while_paused = 0;
        self.total_received = 0;
        self.since_received = None;
        self.reset_source_attempt();
    }

    #[must_use]
    pub fn export_text(&self) -> String {
        self.visible_lines()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Lines retained after the tail bound, pause drops, and the `since`
    /// filter were applied.
    pub fn visible_lines(&self) -> impl Iterator<Item = &String> {
        let first_visible = self.since_received.unwrap_or(0);
        let first_absolute = self.total_received - self.lines.len() as u64;
        self.lines
            .iter()
            .enumerate()
            .filter_map(move |(index, line)| {
                (first_absolute + index as u64 >= first_visible).then_some(line)
            })
    }

    /// Total number of oldest lines dropped by the tail bound.
    #[must_use]
    pub fn truncated_lines(&self) -> u64 {
        self.truncated_lines
    }

    /// Total number of chunks dropped while paused.
    #[must_use]
    pub fn paused_dropped_lines(&self) -> u64 {
        self.dropped_while_paused
    }

    /// Whether a `since` filter is active.
    #[must_use]
    pub fn since_active(&self) -> bool {
        self.since_received.is_some()
    }

    /// Show only what arrives after now (the deterministic since cursor).
    pub fn set_since_now(&mut self) {
        self.since_received = Some(self.total_received);
    }

    /// Show the whole retained buffer again.
    pub fn clear_since(&mut self) {
        self.since_received = None;
    }

    /// Project a ticket/socket rejection into the view.
    pub fn fail(&mut self, reason: &str) {
        if self.phase != LogsPhase::Disconnected {
            self.phase = LogsPhase::Disconnected;
        }
        self.auto_connect_available = false;
        self.last_error = Some(reason.to_owned());
    }

    /// The last rejection reason, if any.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Begin attaching: the application opens the dedicated socket next.
    pub fn connect(&mut self) {
        if self.phase == LogsPhase::Disconnected {
            self.auto_connect_available = false;
            self.phase = LogsPhase::Connecting;
            self.last_error = None;
            self.follow = true;
        }
    }

    /// The first chunk arrived; streaming is live.
    pub fn attach(&mut self) {
        if self.phase == LogsPhase::Connecting {
            self.phase = LogsPhase::Streaming;
        }
    }

    /// Apply one decoded chunk. While paused nothing is buffered; while
    /// streaming the tail bound and per-line char cap apply.
    pub fn append(&mut self, text: &str) {
        if self.phase != LogsPhase::Streaming {
            return;
        }
        if self.paused {
            self.dropped_while_paused += 1;
            return;
        }
        let mut line = String::from(text);
        if line.chars().count() > MAX_LINE_CHARS {
            line = line.chars().take(MAX_LINE_CHARS).collect::<String>() + TRUNCATION_MARKER;
        }
        self.lines.push_back(line);
        self.total_received += 1;
        while self.lines.len() > self.tail_capacity {
            self.lines.pop_front();
            self.truncated_lines += 1;
        }
    }

    /// Stop buffering incoming chunks.
    pub fn pause(&mut self) {
        if self.phase == LogsPhase::Streaming {
            self.paused = true;
        }
    }

    /// Resume buffering.
    pub fn resume(&mut self) {
        self.paused = false;
    }

    /// Toggle autoscroll intent used by the renderer.
    pub fn set_follow(&mut self, follow: bool) {
        self.follow = follow;
    }

    /// Consume a fresh-connection/source-change request to align at bottom.
    pub fn take_scroll_reset(&mut self) -> bool {
        std::mem::take(&mut self.scroll_reset)
    }

    /// Set or clear the case-insensitive find filter.
    pub fn set_find(&mut self, query: Option<&str>) {
        self.find = match query {
            Some(query) if !query.is_empty() => Some(query.to_lowercase()),
            _ => None,
        };
    }

    /// The active find filter, if any.
    #[must_use]
    pub fn find(&self) -> Option<&str> {
        self.find.as_deref()
    }

    /// Retained lines matching the active find filter.
    pub fn find_matches(&self) -> Vec<&String> {
        match &self.find {
            Some(query) => self
                .lines
                .iter()
                .filter(|line| line.to_lowercase().contains(query.as_str()))
                .collect(),
            None => self.lines.iter().collect(),
        }
    }

    /// Socket loss disconnects the view; retained history survives so the
    /// user can still read what streamed before the drop.
    pub fn connection_lost(&mut self) {
        self.phase = LogsPhase::Disconnected;
        self.auto_connect_available = false;
        self.paused = false;
        self.last_error = Some("log stream disconnected".to_owned());
    }

    fn reset_source_attempt(&mut self) {
        self.phase = LogsPhase::Disconnected;
        self.auto_connect_available = true;
        self.follow = true;
        self.scroll_reset = true;
        self.paused = false;
        self.last_error = None;
    }
}

/// Protocol action queued by one log view during rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogsAction {
    /// Request a stream ticket and open the dedicated logs socket.
    OpenLogs {
        /// Window that owns the view.
        window: WindowId,
        /// Target resolved from the window's pinned identity.
        target: StreamTarget,
        since_seconds: Option<i64>,
        previous: bool,
    },
    /// Request one logs stream for every related pod/container and merge
    /// their source-prefixed output into the owning workload view.
    OpenAggregateLogs {
        window: WindowId,
        targets: Vec<StreamTarget>,
        since_seconds: Option<i64>,
    },
}

/// Per-window connected log views plus the actions queued during rendering.
/// Owned by the UI shell; the application drains actions each frame and
/// feeds [`StreamSignal`]s back into the views.
#[derive(Debug, Default)]
pub struct LogsViews {
    views: HashMap<WindowId, LogsTool>,
    aggregate_targets: HashMap<WindowId, Vec<StreamTarget>>,
    actions: Vec<(WindowId, LogsAction)>,
}

impl LogsViews {
    /// Lazily ensure the view for `window`, bound to `target`.
    ///
    /// The view is rebound whenever its window's pinned identity resolves
    /// to a different pod. Container choice belongs to this viewer and must
    /// survive the manifest-default target supplied by subsequent renders.
    pub fn ensure(&mut self, window: WindowId, target: StreamTarget) -> &mut LogsTool {
        self.aggregate_targets.remove(&window);
        if self
            .target_of(window)
            .as_ref()
            .is_none_or(|current| !same_workload(current, &target))
        {
            self.views
                .insert(window, LogsTool::new(target.clone(), DEFAULT_TAIL_CAPACITY));
        }
        self.views.get_mut(&window).expect("view just ensured")
    }

    /// Lazily bind a merged view to an exact, sorted set of pod/container
    /// targets. A rollout that changes the set resets the retained history
    /// and grants one fresh automatic fan-out attempt.
    pub fn ensure_aggregate(
        &mut self,
        window: WindowId,
        targets: &[StreamTarget],
    ) -> Option<&mut LogsTool> {
        let first = targets.first()?.clone();
        if self
            .aggregate_targets
            .get(&window)
            .is_none_or(|current| current != targets)
        {
            self.aggregate_targets.insert(window, targets.to_vec());
            self.views
                .insert(window, LogsTool::new(first, DEFAULT_TAIL_CAPACITY));
        }
        self.views.get_mut(&window)
    }

    #[must_use]
    pub fn aggregate_targets(&self, window: WindowId) -> Option<&[StreamTarget]> {
        self.aggregate_targets.get(&window).map(Vec::as_slice)
    }

    /// Bound target of one view, if it exists.
    #[must_use]
    pub fn target_of(&self, window: WindowId) -> Option<StreamTarget> {
        self.views.get(&window).map(|view| view.target().clone())
    }

    /// View access for signal projection.
    #[must_use]
    pub fn get_mut(&mut self, window: WindowId) -> Option<&mut LogsTool> {
        self.views.get_mut(&window)
    }

    /// Read one window's log viewer without mutating its stream state.
    #[must_use]
    pub fn get(&self, window: WindowId) -> Option<&LogsTool> {
        self.views.get(&window)
    }

    /// Queue one protocol action produced during rendering.
    pub fn queue(&mut self, window: WindowId, action: LogsAction) {
        self.actions.push((window, action));
    }

    /// Drain every queued protocol action with its owning window.
    pub fn drain_actions(&mut self) -> Vec<(WindowId, LogsAction)> {
        std::mem::take(&mut self.actions)
    }

    /// Mark every view disconnected (control transport loss).
    pub fn connection_lost(&mut self) {
        for view in self.views.values_mut() {
            if view.phase() == LogsPhase::Streaming || view.phase() == LogsPhase::Connecting {
                view.connection_lost();
            }
        }
    }

    /// Drop entries for closed windows.
    pub fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.views.retain(|id, _| live(*id));
        self.aggregate_targets.retain(|id, _| live(*id));
    }
}

/// Container choice is viewer state; the remaining fields identify the pod
/// whose history must be discarded when the workspace selection changes.
pub(crate) fn same_workload(left: &StreamTarget, right: &StreamTarget) -> bool {
    left.context == right.context
        && left.namespace == right.namespace
        && left.pod == right.pod
        && left.uid == right.uid
}

/// Tail capacity used by detail-view log panes.
pub const DEFAULT_TAIL_CAPACITY: usize = 512;

const BOTTOM_TOLERANCE: f32 = 2.0;

fn is_at_bottom(actual_offset: f32, max_offset: f32) -> bool {
    actual_offset >= max_offset.max(0.0) - BOTTOM_TOLERANCE
}

fn normalize_bottom_state(
    ctx: &egui::Context,
    id: egui::Id,
    state: egui::scroll_area::State,
    max_offset: f32,
) -> bool {
    if !is_at_bottom(state.offset.y, max_offset) {
        return false;
    }
    let max_offset = max_offset.max(0.0);
    if state.offset.y == max_offset {
        return true;
    }

    let mut normalized = egui::scroll_area::State::default();
    normalized.offset = egui::vec2(state.offset.x, max_offset);
    normalized.store(ctx, id);
    true
}

/// Render the connected Logs tab content for one detail view.
pub(crate) fn show(
    ui: &mut egui::Ui,
    window_id: WindowId,
    views: &mut LogsViews,
    target: Option<StreamTarget>,
    containers: &[String],
    default_previous: bool,
) {
    let Some(target) = target else {
        ui.label("Select a pod to stream logs");
        return;
    };
    let mut open_requested = false;
    {
        let view = views.ensure(window_id, target.clone());
        if !containers.is_empty()
            && !containers
                .iter()
                .any(|container| container == &view.target().container)
        {
            view.select_container(&containers[0]);
        }
        view.apply_source_defaults(default_previous);
        if let Some(error) = view.last_error() {
            ui.label(
                RichText::new(error.to_owned()).color(egui::Color32::from_rgb(0xc0, 0x39, 0x2b)),
            );
        }
        ui.label(
            RichText::new("LOG SOURCE")
                .small()
                .strong()
                .color(crate::ui::theme::MUTED_TEXT),
        );
        ui.horizontal_wrapped(|ui| {
            egui::ComboBox::from_id_salt(("logs.container", window_id.0))
                .selected_text(format!("Container: {}", view.target().container))
                .show_ui(ui, |ui| {
                    for container in containers {
                        if ui
                            .selectable_label(view.target().container == *container, container)
                            .clicked()
                        {
                            view.select_container(container);
                        }
                    }
                });
            let mut previous = view.previous();
            if ui.checkbox(&mut previous, "Previous").changed() {
                view.set_previous(previous);
            }
            egui::ComboBox::from_id_salt(("logs.since", window_id.0))
                .selected_text(match view.since_seconds() {
                    Some(300) => "Since: 5m",
                    Some(900) => "Since: 15m",
                    Some(3600) => "Since: 1h",
                    _ => "Since: all",
                })
                .show_ui(ui, |ui| {
                    for (label, seconds) in [
                        ("All retained", None),
                        ("Last 5 minutes", Some(300)),
                        ("Last 15 minutes", Some(900)),
                        ("Last hour", Some(3600)),
                    ] {
                        if ui
                            .selectable_label(view.since_seconds() == seconds, label)
                            .clicked()
                        {
                            view.set_since_seconds(seconds);
                        }
                    }
                });
            open_requested |= view.begin_auto_connect();
            match view.phase() {
                LogsPhase::Disconnected => {
                    if view.can_retry() && ui.button("Retry logs").clicked() {
                        open_requested = view.retry();
                    }
                    ui.label(RichText::new("Disconnected").weak());
                }
                LogsPhase::Connecting => {
                    ui.label("Connecting");
                }
                LogsPhase::Streaming => {
                    let pause_label = if view.is_paused() { "Resume" } else { "Pause" };
                    if ui.button(pause_label).clicked() {
                        if view.is_paused() {
                            view.resume();
                        } else {
                            view.pause();
                        }
                    }
                    if view.is_paused() {
                        ui.label(RichText::new("Paused").weak());
                    }
                    let since_label = if view.since_active() {
                        "Show all"
                    } else {
                        "Since now"
                    };
                    if ui.button(since_label).clicked() {
                        if view.since_active() {
                            view.clear_since();
                        } else {
                            view.set_since_now();
                        }
                    }
                }
            }
        });
        ui.add_space(4.0);
        ui.label(
            RichText::new("VIEW")
                .small()
                .strong()
                .color(crate::ui::theme::MUTED_TEXT),
        );
        ui.horizontal_wrapped(|ui| {
            let mut find = view.find().unwrap_or_default().to_owned();
            let find_edit = ui.add(egui::TextEdit::singleline(&mut find).hint_text("Find"));
            find_edit.widget_info(|| {
                egui::WidgetInfo::labeled(
                    egui::WidgetType::TextEdit,
                    true,
                    "Find in logs".to_owned(),
                )
            });
            if find_edit.changed() {
                view.set_find(Some(&find));
            }
            let mut wrap = view.wraps();
            if ui.checkbox(&mut wrap, "Wrap").changed() {
                view.set_wrap(wrap);
            }
            if ui.button("Export").clicked() {
                ui.ctx().copy_text(view.export_text());
            }
        });
        if default_previous && view.previous() {
            ui.label(
                RichText::new(
                    "CrashLoopBackOff: showing logs from the previous terminated container by default",
                )
                .color(crate::ui::theme::WARNING),
            );
        }
        let was_following = view.follows();
        let scroll_id = ui.make_persistent_id(("logs.stream", window_id.0));
        if view.take_scroll_reset() {
            egui::scroll_area::State::default().store(ui.ctx(), scroll_id);
        }
        let log_scroll = if view.wraps() {
            ScrollArea::vertical()
        } else {
            ScrollArea::both()
        };
        let scroll_output = log_scroll
            .id_salt(("logs.stream", window_id.0))
            .stick_to_bottom(was_following)
            .show(ui, |ui| {
                // An active Find filters the retained buffer; otherwise the
                // since/tail-filtered view is shown.
                if view.find().is_some() {
                    for line in view.find_matches() {
                        ui.add(
                            egui::Label::new(RichText::new(line.as_str()).monospace()).wrap_mode(
                                if view.wraps() {
                                    egui::TextWrapMode::Wrap
                                } else {
                                    egui::TextWrapMode::Extend
                                },
                            ),
                        );
                    }
                } else {
                    for line in view.visible_lines() {
                        ui.add(
                            egui::Label::new(RichText::new(line.as_str()).monospace()).wrap_mode(
                                if view.wraps() {
                                    egui::TextWrapMode::Wrap
                                } else {
                                    egui::TextWrapMode::Extend
                                },
                            ),
                        );
                    }
                }
                if view.truncated_lines() > 0 {
                    ui.label(
                        RichText::new(format!("{} older lines truncated", view.truncated_lines()))
                            .weak(),
                    );
                }
            });
        let max_offset =
            (scroll_output.content_size.y - scroll_output.inner_rect.height()).max(0.0);
        view.set_follow(normalize_bottom_state(
            ui.ctx(),
            scroll_output.id,
            scroll_output.state,
            max_offset,
        ));
    }
    if open_requested {
        let selected_target = views
            .target_of(window_id)
            .expect("a rendered logs view has a target");
        views.queue(
            window_id,
            LogsAction::OpenLogs {
                window: window_id,
                target: selected_target,
                since_seconds: views.get(window_id).and_then(LogsTool::since_seconds),
                previous: views.get(window_id).is_some_and(LogsTool::previous),
            },
        );
    }
}

/// Render a workload log viewer backed by all exact related pod/container
/// targets. Incoming chunks are prefixed by the application layer before
/// being appended to this shared bounded view.
pub(crate) fn show_aggregate(
    ui: &mut egui::Ui,
    window_id: WindowId,
    views: &mut LogsViews,
    targets: &[StreamTarget],
) {
    if targets.is_empty() {
        ui.label("No related pods with loggable containers");
        return;
    }
    let mut open_requested = false;
    {
        let Some(view) = views.ensure_aggregate(window_id, targets) else {
            return;
        };
        if let Some(error) = view.last_error() {
            ui.label(RichText::new(error).color(crate::ui::theme::DANGER));
        }
        ui.label(
            RichText::new("AGGREGATE SOURCE")
                .small()
                .strong()
                .color(crate::ui::theme::MUTED_TEXT),
        );
        ui.horizontal_wrapped(|ui| {
            ui.label(format!(
                "{} pods · {} container streams",
                targets
                    .iter()
                    .map(|target| target.pod.as_str())
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                targets.len()
            ));
            egui::ComboBox::from_id_salt(("logs.aggregate.since", window_id.0))
                .selected_text(match view.since_seconds() {
                    Some(300) => "Since: 5m",
                    Some(900) => "Since: 15m",
                    Some(3600) => "Since: 1h",
                    _ => "Since: all",
                })
                .show_ui(ui, |ui| {
                    for (label, seconds) in [
                        ("All retained", None),
                        ("Last 5 minutes", Some(300)),
                        ("Last 15 minutes", Some(900)),
                        ("Last hour", Some(3600)),
                    ] {
                        if ui
                            .selectable_label(view.since_seconds() == seconds, label)
                            .clicked()
                        {
                            view.set_since_seconds(seconds);
                        }
                    }
                });
            open_requested |= view.begin_auto_connect();
            match view.phase() {
                LogsPhase::Disconnected => {
                    if view.can_retry() && ui.button("Retry logs").clicked() {
                        open_requested = view.retry();
                    }
                    ui.label(RichText::new("Disconnected").weak());
                }
                LogsPhase::Connecting => {
                    ui.label("Connecting");
                }
                LogsPhase::Streaming => {
                    let label = if view.is_paused() { "Resume" } else { "Pause" };
                    if ui.button(label).clicked() {
                        if view.is_paused() {
                            view.resume();
                        } else {
                            view.pause();
                        }
                    }
                }
            }
        });
        ui.horizontal_wrapped(|ui| {
            let mut find = view.find().unwrap_or_default().to_owned();
            if ui
                .add(egui::TextEdit::singleline(&mut find).hint_text("Find across pods"))
                .changed()
            {
                view.set_find(Some(&find));
            }
            let mut wrap = view.wraps();
            if ui.checkbox(&mut wrap, "Wrap").changed() {
                view.set_wrap(wrap);
            }
            if ui.button("Export").clicked() {
                ui.ctx().copy_text(view.export_text());
            }
        });
        let log_scroll = if view.wraps() {
            ScrollArea::vertical()
        } else {
            ScrollArea::both()
        };
        log_scroll
            .id_salt(("logs.aggregate.stream", window_id.0))
            .stick_to_bottom(view.follows())
            .show(ui, |ui| {
                let lines: Vec<&String> = if view.find().is_some() {
                    view.find_matches()
                } else {
                    view.visible_lines().collect()
                };
                for line in lines {
                    ui.add(egui::Label::new(RichText::new(line).monospace()).wrap_mode(
                        if view.wraps() {
                            egui::TextWrapMode::Wrap
                        } else {
                            egui::TextWrapMode::Extend
                        },
                    ));
                }
            });
    }
    if open_requested {
        views.queue(
            window_id,
            LogsAction::OpenAggregateLogs {
                window: window_id,
                targets: targets.to_vec(),
                since_seconds: views.get(window_id).and_then(LogsTool::since_seconds),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{LogsTool, is_at_bottom, normalize_bottom_state};
    use egui::{Context, Id, RawInput, Rect, ScrollArea, Vec2, pos2, vec2};
    use k10s_protocol::StreamTarget;

    fn target() -> StreamTarget {
        StreamTarget {
            context: "test".to_owned(),
            namespace: "default".to_owned(),
            pod: "pod".to_owned(),
            container: "container".to_owned(),
            uid: "uid".to_owned(),
        }
    }

    #[test]
    fn bottom_detection_accepts_exact_bottom() {
        assert!(is_at_bottom(80.0, 80.0));
    }

    #[test]
    fn bottom_detection_accepts_offset_within_two_logical_pixels() {
        assert!(is_at_bottom(78.0, 80.0));
    }

    #[test]
    fn bottom_detection_rejects_offset_beyond_two_logical_pixels() {
        assert!(!is_at_bottom(77.9, 80.0));
    }

    #[test]
    fn bottom_detection_clamps_negative_max_offset_for_short_content() {
        assert!(is_at_bottom(0.0, -20.0));
    }

    #[test]
    fn scroll_position_disengages_and_restores_follow() {
        let mut logs = LogsTool::new(target(), 10);

        logs.set_follow(is_at_bottom(40.0, 100.0));
        assert!(!logs.follows());

        logs.set_follow(is_at_bottom(100.0, 100.0));
        assert!(logs.follows());
    }

    fn render_scroll(
        ctx: &Context,
        id: Id,
        rows: usize,
        stick_to_bottom: bool,
    ) -> (egui::scroll_area::State, f32, Id) {
        let mut rendered = None;
        let input = RawInput {
            screen_rect: Some(Rect::from_min_size(pos2(0.0, 0.0), vec2(240.0, 160.0))),
            ..RawInput::default()
        };
        let mut frame_output = ctx.run_ui(input, |ui| {
            let output = ScrollArea::vertical()
                .id_salt(id)
                .max_height(100.0)
                .stick_to_bottom(stick_to_bottom)
                .show(ui, |ui| {
                    for row in 0..rows {
                        ui.label(format!("log line {row}"));
                    }
                });
            let max_offset = (output.content_size.y - output.inner_rect.height()).max(0.0);
            rendered = Some((output.state, max_offset, output.id));
        });
        frame_output.textures_delta.clear();
        rendered.expect("scroll area rendered")
    }

    #[test]
    fn near_bottom_state_sticks_to_new_content_after_normalization() {
        let ctx = Context::default();
        let id = Id::new("logs-scroll-regression");
        let (initial, initial_max, scroll_id) = render_scroll(&ctx, id, 20, false);

        let mut near_bottom = initial;
        near_bottom.offset = Vec2::new(0.0, initial_max - 1.0);
        near_bottom.store(&ctx, scroll_id);

        assert!(normalize_bottom_state(
            &ctx,
            scroll_id,
            near_bottom,
            initial_max
        ));
        let (appended, appended_max, _) = render_scroll(&ctx, id, 24, true);

        assert!(
            appended.offset.y > initial_max,
            "appended offset {} did not advance beyond initial max {initial_max}; appended max {appended_max}",
            appended.offset.y
        );
        assert_eq!(appended.offset.y, appended_max);
    }

    #[test]
    fn retry_resets_scrolled_up_frame_to_exact_bottom_and_keeps_following_append() {
        let ctx = Context::default();
        let id = Id::new("logs-retry-scroll-reset");
        let (initial, initial_max, scroll_id) = render_scroll(&ctx, id, 20, false);
        let mut scrolled_up = initial;
        scrolled_up.offset.y = initial_max - 12.0;
        scrolled_up.store(&ctx, scroll_id);

        let mut logs = LogsTool::new(target(), 10);
        assert!(logs.begin_auto_connect());
        assert!(logs.take_scroll_reset());
        logs.fail("disconnected");
        assert!(logs.retry());
        assert!(logs.take_scroll_reset());
        egui::scroll_area::State::default().store(&ctx, scroll_id);

        let (reset, reset_max, _) = render_scroll(&ctx, id, 20, logs.follows());
        assert_eq!(reset.offset.y, reset_max);
        logs.set_follow(normalize_bottom_state(&ctx, scroll_id, reset, reset_max));
        let (appended, appended_max, _) = render_scroll(&ctx, id, 24, logs.follows());
        assert_eq!(appended.offset.y, appended_max);
        assert!(logs.follows());
    }
}
