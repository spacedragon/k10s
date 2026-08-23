//! Connected terminal (exec) state.
//!
//! A pure state machine for the dedicated exec socket. Attaching requires an
//! explicit user connect; TTY output merges every origin into one scrollback
//! buffer; stdin and resize are queued as drainable protocol actions for the
//! application's transport layer. Exit and socket loss are distinct terminal
//! states, and neither destroys the scrollback.

use std::collections::VecDeque;

use k10s_protocol::StreamTarget;

/// Lifecycle of one connected terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellPhase {
    /// No session requested yet.
    Disconnected,
    /// Ticket issued; connecting to the dedicated socket.
    Connecting,
    /// Attached to a live TTY session.
    Attached,
    /// The session ended with the reported exit code.
    Exited(i32),
    /// The session failed: transport loss or a server-side rejection.
    Failed(String),
}

/// One queued protocol action produced by the terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellAction {
    /// One line of TTY standard input, newline terminated.
    Input(String),
    /// Terminal resize.
    Resize { cols: u32, rows: u32 },
}

/// Retained terminal state for one pod/container target.
#[derive(Debug, Clone)]
pub struct ShellTool {
    target: StreamTarget,
    phase: ShellPhase,
    buffer: VecDeque<String>,
    actions: Vec<ShellAction>,
    scrollback_capacity: usize,
}

impl ShellTool {
    /// Create a disconnected terminal bound to `target`.
    #[must_use]
    pub fn new(target: StreamTarget) -> Self {
        Self {
            target,
            phase: ShellPhase::Disconnected,
            buffer: VecDeque::new(),
            actions: Vec::new(),
            scrollback_capacity: 4_096,
        }
    }

    /// Target this terminal attaches to.
    #[must_use]
    pub fn target(&self) -> &StreamTarget {
        &self.target
    }

    /// Current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> &ShellPhase {
        &self.phase
    }

    /// Whether an explicit connect has happened and attach is allowed.
    #[must_use]
    pub fn can_attach(&self) -> bool {
        matches!(self.phase, ShellPhase::Connecting)
    }

    /// Merged TTY scrollback, oldest first.
    pub fn buffer(&self) -> impl Iterator<Item = &String> {
        self.buffer.iter()
    }

    /// Whether any output has been retained.
    #[must_use]
    pub fn buffer_is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Request an explicit shell session. Nothing is sent until the
    /// application drains actions and opens the dedicated socket.
    pub fn connect(&mut self) {
        if self.phase == ShellPhase::Disconnected {
            self.phase = ShellPhase::Connecting;
        }
    }

    /// The dedicated socket is live; the session is attached.
    pub fn attach(&mut self) {
        if self.can_attach() {
            self.phase = ShellPhase::Attached;
        }
    }

    /// Apply one merged-output chunk to the scrollback.
    pub fn apply_output(&mut self, text: &str) {
        if self.phase != ShellPhase::Attached {
            return;
        }
        let normalized = text.replace("\r\n", "\n");
        let mut lines: Vec<&str> = normalized.split('\n').collect();
        // A trailing newline terminates the last line; it does not open a
        // new blank one.
        if normalized.ends_with('\n') {
            lines.pop();
        }
        for line in lines {
            self.buffer.push_back(line.to_owned());
        }
        while self.buffer.len() > self.scrollback_capacity {
            self.buffer.pop_front();
        }
    }

    /// Queue one line of standard input (a newline is appended).
    pub fn send_input(&mut self, line: &str) {
        if self.phase == ShellPhase::Attached {
            self.actions.push(ShellAction::Input(format!("{line}\n")));
        }
    }

    /// Queue a terminal resize.
    pub fn resize(&mut self, cols: u32, rows: u32) {
        if self.phase == ShellPhase::Attached {
            self.actions.push(ShellAction::Resize { cols, rows });
        }
    }

    /// Drain every queued protocol action; draining is one-shot.
    pub fn drain_actions(&mut self) -> Vec<ShellAction> {
        std::mem::take(&mut self.actions)
    }

    /// The session ended cleanly with `exit_code`.
    pub fn exit(&mut self, exit_code: i32) {
        if self.phase == ShellPhase::Attached {
            self.phase = ShellPhase::Exited(exit_code);
        }
    }

    /// Socket loss fails the session without touching the scrollback.
    pub fn connection_lost(&mut self) {
        if self.phase == ShellPhase::Attached || self.phase == ShellPhase::Connecting {
            self.phase = ShellPhase::Failed("terminal disconnected".to_owned());
        }
    }
}
