//! Connected log viewer state.
//!
//! A pure state machine fed by stream chunks from a dedicated logs socket.
//! The retained view is a bounded tail: appending beyond the bound truncates
//! the oldest lines deterministically and counts them. Pause stops buffering
//! (dropped lines are counted, never silently mixed into history), follow is
//! reserved for autoscroll behavior in the renderer, and find filters the
//! retained buffer without destroying it.

use std::collections::VecDeque;

use k10s_protocol::StreamTarget;

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

    /// Lines retained after the tail bound and pause drops were applied.
    pub fn visible_lines(&self) -> impl Iterator<Item = &String> {
        self.lines.iter()
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

    /// Begin attaching: the application opens the dedicated socket next.
    pub fn connect(&mut self) {
        if self.phase == LogsPhase::Disconnected {
            self.phase = LogsPhase::Connecting;
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
