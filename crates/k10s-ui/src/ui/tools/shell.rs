//! Connected terminal (exec) state.
//!
//! A pure state machine for the dedicated exec socket. Attaching requires an
//! explicit user connect; TTY output merges every origin into one scrollback
//! buffer; stdin and resize are queued as drainable protocol actions for the
//! application's transport layer. Exit and socket loss are distinct terminal
//! states, and neither destroys the scrollback.

use std::collections::VecDeque;

use k10s_protocol::StreamTarget;

const SCROLLBACK_LINE_CAPACITY: usize = 64 * 1024;

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
    continuation: bool,
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
            continuation: false,
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
        for segment in normalized.split_inclusive('\n') {
            let complete = segment.ends_with('\n');
            let text = segment.strip_suffix('\n').unwrap_or(segment);
            if self.continuation {
                if let Some(line) = self.buffer.back_mut() {
                    line.push_str(text);
                    truncate_line_start(line);
                }
            } else {
                self.buffer.push_back(text.to_owned());
                if let Some(line) = self.buffer.back_mut() {
                    truncate_line_start(line);
                }
            }
            self.continuation = !complete;
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

    /// Fail the session with an explicit typed reason (ticket denial,
    /// RBAC, missing binary); the scrollback survives. Recovery goes
    /// through [`Self::dismiss_failure`].
    pub fn fail(&mut self, reason: &str) {
        if self.phase == ShellPhase::Attached || self.phase == ShellPhase::Connecting {
            self.phase = ShellPhase::Failed(reason.to_owned());
        }
    }

    /// Intentional teardown by the application (selection rebind or guard
    /// resolution): returns to the reconnectable Disconnected state,
    /// dropping queued input/actions while preserving the scrollback.
    pub fn disconnect_intentional(&mut self) {
        if !matches!(self.phase, ShellPhase::Disconnected) {
            self.phase = ShellPhase::Disconnected;
        }
        self.actions.clear();
    }

    /// Dismiss a failure report and allow a fresh explicit connect.
    pub fn dismiss_failure(&mut self) {
        if matches!(self.phase, ShellPhase::Failed(_)) {
            self.phase = ShellPhase::Disconnected;
            self.actions.clear();
        }
    }
}

fn truncate_line_start(line: &mut String) {
    if line.len() <= SCROLLBACK_LINE_CAPACITY {
        return;
    }
    let mut start = line.len() - SCROLLBACK_LINE_CAPACITY;
    while !line.is_char_boundary(start) {
        start += 1;
    }
    line.drain(..start);
}

use std::collections::HashMap;

use egui::{RichText, ScrollArea};

use crate::workspace::WindowId;

/// Per-window terminal sessions plus rendering-time queues. Owned by the UI
/// shell: the application drains connect requests and stdin/resize actions,
/// forwards them into live [`crate::client::StreamSession`]s, and projects
/// [`crate::client::StreamSignal`]s back into these sessions.
#[derive(Debug, Default)]
pub struct ShellSessions {
    sessions: HashMap<WindowId, ShellTool>,
    input_buffers: HashMap<WindowId, String>,
    connects: Vec<(WindowId, StreamTarget)>,
}

impl ShellSessions {
    /// Lazily ensure the terminal for `window`, bound to `target`.
    ///
    /// The terminal is rebound when its window's identity changes so one
    /// pod's session can never be presented under another pod's detail.
    pub fn ensure(&mut self, window: WindowId, target: StreamTarget) -> &mut ShellTool {
        if self.target_of(window).as_ref() != Some(&target) {
            self.sessions.insert(window, ShellTool::new(target.clone()));
        }
        self.sessions
            .get_mut(&window)
            .expect("session just ensured")
    }

    /// Bound target of one terminal, if it exists.
    #[must_use]
    pub fn target_of(&self, window: WindowId) -> Option<StreamTarget> {
        self.sessions.get(&window).map(|s| s.target().clone())
    }

    /// Session access for signal projection.
    #[must_use]
    pub fn get_mut(&mut self, window: WindowId) -> Option<&mut ShellTool> {
        self.sessions.get_mut(&window)
    }

    /// Read one window's terminal without mutating its stream state.
    #[must_use]
    pub fn get(&self, window: WindowId) -> Option<&ShellTool> {
        self.sessions.get(&window)
    }

    /// Mark every session failed (control transport loss).
    pub fn connection_lost(&mut self) {
        for session in self.sessions.values_mut() {
            session.connection_lost();
        }
    }

    /// Drop entries for closed windows.
    pub fn retain(&mut self, live: impl Fn(WindowId) -> bool) {
        self.sessions.retain(|id, _| live(*id));
        self.input_buffers.retain(|id, _| live(*id));
    }

    /// Drain queued stdin/resize actions from every window's terminal.
    pub fn drain_actions(&mut self) -> Vec<(WindowId, ShellAction)> {
        let mut drained = Vec::new();
        for (window, session) in self.sessions.iter_mut() {
            for action in session.drain_actions() {
                drained.push((*window, action));
            }
        }
        drained
    }

    /// Queue one explicit shell-connect request produced during rendering.
    pub fn queue_connect(&mut self, window: WindowId, target: StreamTarget) {
        self.connects.push((window, target));
    }

    /// Drain every queued explicit connect request.
    pub fn drain_connects(&mut self) -> Vec<(WindowId, StreamTarget)> {
        std::mem::take(&mut self.connects)
    }

    /// The pending single-line input buffer of one window.
    #[must_use]
    pub fn input_buffer(&self, window: WindowId) -> &str {
        self.input_buffers
            .get(&window)
            .map(String::as_str)
            .unwrap_or_default()
    }

    /// Replace the pending single-line input buffer of one window.
    pub fn set_input_buffer(&mut self, window: WindowId, text: String) {
        self.input_buffers.insert(window, text);
    }

    /// Take (clear) the pending input buffer of one window.
    pub fn take_input_buffer(&mut self, window: WindowId) -> String {
        self.input_buffers.remove(&window).unwrap_or_default()
    }
}

/// Render the connected Shell tab content for one detail view.
pub(crate) fn show(
    ui: &mut egui::Ui,
    window_id: WindowId,
    sessions: &mut ShellSessions,
    target: Option<StreamTarget>,
) {
    let Some(target) = target else {
        ui.label("Select a pod to open a shell");
        return;
    };
    let mut connect_requested = false;
    let mut pending_input = sessions.take_input_buffer(window_id);
    {
        let session = sessions.ensure(window_id, target.clone());
        match session.phase().clone() {
            ShellPhase::Disconnected => {
                let button = ui.button("Connect shell");
                button.widget_info(|| {
                    egui::WidgetInfo::labeled(
                        egui::WidgetType::Button,
                        true,
                        "Connect shell".to_owned(),
                    )
                });
                connect_requested = button.clicked();
                ui.label(RichText::new("Disconnected").weak());
            }
            ShellPhase::Connecting => {
                ui.label("Connecting");
            }
            ShellPhase::Attached => {
                ui.horizontal(|ui| {
                    let edit = ui.add(
                        egui::TextEdit::singleline(&mut pending_input).hint_text("Type a command"),
                    );
                    if edit.lost_focus()
                        && ui.input(|input| input.key_pressed(egui::Key::Enter))
                        && !pending_input.trim().is_empty()
                    {
                        session.send_input(pending_input.trim());
                        pending_input.clear();
                    }
                });
            }
            ShellPhase::Exited(code) => {
                ui.label(format!("Session exited with code {code}"));
            }
            ShellPhase::Failed(reason) => {
                ui.label(RichText::new(format!("Session failed: {reason}")));
                let dismiss = ui.button("New session");
                dismiss.widget_info(|| {
                    egui::WidgetInfo::labeled(
                        egui::WidgetType::Button,
                        true,
                        "New session".to_owned(),
                    )
                });
                if dismiss.clicked() {
                    session.dismiss_failure();
                }
            }
        }
        ScrollArea::vertical()
            .id_salt(("shell.terminal", window_id.0))
            .show(ui, |ui| {
                for line in session.buffer() {
                    ui.label(RichText::new(line.as_str()).monospace());
                }
                if session.buffer_is_empty() {
                    ui.label(RichText::new("No output yet").weak());
                }
            });
    }
    sessions.set_input_buffer(window_id, pending_input);
    if connect_requested {
        sessions.queue_connect(window_id, target);
    }
}
