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
    paused: bool,
    follow: bool,
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
            paused: false,
            follow: true,
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
            self.phase = LogsPhase::Connecting;
            self.last_error = None;
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
        self.paused = false;
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
    },
}

/// Per-window connected log views plus the actions queued during rendering.
/// Owned by the UI shell; the application drains actions each frame and
/// feeds [`StreamSignal`]s back into the views.
#[derive(Debug, Default)]
pub struct LogsViews {
    views: HashMap<WindowId, LogsTool>,
    actions: Vec<(WindowId, LogsAction)>,
}

impl LogsViews {
    /// Lazily ensure the view for `window`, bound to `target`.
    ///
    /// The view is rebound whenever its window's pinned identity resolves
    /// to a different pod/container: history from one pod must never be
    /// displayed under another pod's identity.
    pub fn ensure(&mut self, window: WindowId, target: StreamTarget) -> &mut LogsTool {
        if self.target_of(window).as_ref() != Some(&target) {
            self.views
                .insert(window, LogsTool::new(target.clone(), DEFAULT_TAIL_CAPACITY));
        }
        self.views.get_mut(&window).expect("view just ensured")
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
    }
}

/// Tail capacity used by detail-view log panes.
pub const DEFAULT_TAIL_CAPACITY: usize = 512;

/// Render the connected Logs tab content for one detail view.
pub(crate) fn show(
    ui: &mut egui::Ui,
    window_id: WindowId,
    views: &mut LogsViews,
    target: Option<StreamTarget>,
) {
    let Some(target) = target else {
        ui.label("Select a pod to stream logs");
        return;
    };
    let mut connect_requested = false;
    {
        let view = views.ensure(window_id, target.clone());
        if let Some(error) = view.last_error() {
            ui.label(
                RichText::new(error.to_owned()).color(egui::Color32::from_rgb(0xc0, 0x39, 0x2b)),
            );
        }
        ui.horizontal(|ui| {
            match view.phase() {
                LogsPhase::Disconnected => {
                    let button = ui.button("Connect logs");
                    button.widget_info(|| {
                        egui::WidgetInfo::labeled(
                            egui::WidgetType::Button,
                            true,
                            "Connect logs".to_owned(),
                        )
                    });
                    connect_requested = button.clicked();
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
                    let follow = view.follows();
                    if ui.checkbox(&mut { follow }, "Follow").changed() {
                        view.set_follow(!follow);
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
        });
        ScrollArea::vertical()
            .id_salt(("logs.stream", window_id.0))
            .show(ui, |ui| {
                // An active Find filters the retained buffer; otherwise the
                // since/tail-filtered view is shown.
                if view.find().is_some() {
                    for line in view.find_matches() {
                        ui.label(RichText::new(line.as_str()).monospace());
                    }
                } else {
                    for line in view.visible_lines() {
                        ui.label(RichText::new(line.as_str()).monospace());
                    }
                }
                if view.truncated_lines() > 0 {
                    ui.label(
                        RichText::new(format!("{} older lines truncated", view.truncated_lines()))
                            .weak(),
                    );
                }
                // Follow autoscrolls to the newest line; a disengaged
                // follow leaves the scroll position to the user.
                if view.follows() && !view.is_paused() {
                    ui.scroll_to_cursor(Some(egui::Align::BOTTOM));
                }
            });
    }
    if connect_requested {
        views.queue(
            window_id,
            LogsAction::OpenLogs {
                window: window_id,
                target,
            },
        );
    }
}
