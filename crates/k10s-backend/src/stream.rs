//! Kernel-owned Stream Hub: bounded single-use stream tickets and fake
//! stream sessions.
//!
//! Tickets are issued by adapters through `Query::StreamTicket` and redeemed
//! exactly once through `Subscribe::StreamRedeem`, which returns a bounded
//! broadcast receiver of stream chunks. Sessions advance only when the test
//! explicitly ticks them — no command or process ever executes. Receivers
//! that vanish (socket loss) are pruned on the next tick, disconnecting the
//! terminal session.

use std::collections::{HashMap, VecDeque};

use tokio::sync::broadcast;

use crate::port::{BackendError, BackendEvent, StreamKind};

/// Broadcast capacity per live stream; bounded like every other queue.
pub const STREAM_QUEUE_CAPACITY: usize = 128;
/// Hard bound on unredeemed stream tickets kept per process.
const TICKET_CAPACITY: usize = 32;
/// A ticket older than this many backend revisions has expired.
const TICKET_MAX_AGE_REVISIONS: u64 = 256;

/// Origin descriptor of one stream chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOrigin {
    /// Non-TTY standard output or log data.
    Stdout,
    /// Non-TTY standard error (the distinct non-TTY mode).
    Stderr,
    /// TTY merged output.
    TtyOutput,
}

impl StreamOrigin {
    /// Map the origin onto its protocol payload-kind byte.
    #[must_use]
    pub fn payload_kind(self) -> u8 {
        match self {
            Self::Stdout => k10s_protocol::payload_kind::STDOUT,
            Self::Stderr => k10s_protocol::payload_kind::STDERR,
            Self::TtyOutput => k10s_protocol::payload_kind::TTY_OUTPUT,
        }
    }
}

/// One chunk of a fake stream session. `exit_code` terminates the session:
/// the server forwards the exit status and closes after this chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamChunk {
    /// Which output the chunk belongs to.
    pub origin: StreamOrigin,
    /// Chunk text.
    pub text: String,
    /// Set on the final chunk of an exec session with the process exit code.
    pub exit_code: Option<i32>,
}

#[derive(Debug)]
struct PendingTicket {
    stream: StreamKind,
    issued_revision: u64,
}

#[derive(Debug)]
struct StreamSession {
    stream: StreamKind,
    sender: broadcast::Sender<BackendEvent>,
    ticks: u64,
    pending_stdin: VecDeque<String>,
    last_resize: Option<(u32, u32)>,
}

/// Bounded registry of stream tickets and live sessions.
#[derive(Debug)]
pub struct StreamHub {
    tickets: HashMap<String, PendingTicket>,
    order: VecDeque<String>,
    sessions: HashMap<String, StreamSession>,
    next_ticket: u64,
}

impl Default for StreamHub {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamHub {
    /// Create an empty hub.
    pub fn new() -> Self {
        Self {
            tickets: HashMap::new(),
            order: VecDeque::new(),
            sessions: HashMap::new(),
            next_ticket: 1,
        }
    }

    /// Issue a single-use ticket bound to `stream`. Bounded retention evicts
    /// the oldest unredeemed tickets first.
    pub fn issue_ticket(
        &mut self,
        stream: StreamKind,
        issued_revision: u64,
    ) -> Result<String, BackendError> {
        let id = format!("stream-ticket-{:04}", self.next_ticket);
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.tickets.insert(
            id.clone(),
            PendingTicket {
                stream,
                issued_revision,
            },
        );
        self.order.push_back(id.clone());
        while self.tickets.len() > TICKET_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.tickets.remove(&oldest);
            }
        }
        Ok(id)
    }

    /// Redeem a ticket exactly once, opening a bounded session channel and
    /// emitting the deterministic historical backlog into it before
    /// returning. Unknown, expired, or already-redeemed tickets are typed
    /// conflicts; the caller maps route mismatches onto errors beforehand.
    pub fn redeem(
        &mut self,
        ticket_id: &str,
        expected_route: crate::port::StreamRouteKind,
        current_revision: u64,
    ) -> Result<(broadcast::Receiver<BackendEvent>, StreamKind), BackendError> {
        // Every binding is verified before the single-use consume: unknown,
        // expired, wrong-route, or already-redeemed tickets are rejected.
        let Some(pending) = self.tickets.get(ticket_id) else {
            return Err(BackendError::Conflict(
                "the stream ticket is unknown or already used".into(),
            ));
        };
        if current_revision - pending.issued_revision > TICKET_MAX_AGE_REVISIONS {
            return Err(BackendError::Conflict(
                "the stream ticket has expired".into(),
            ));
        }
        if ticket_route(&pending.stream) != expected_route {
            return Err(BackendError::Conflict(
                "the stream ticket was issued for a different route".into(),
            ));
        }
        let PendingTicket { stream, .. } = self.tickets.remove(ticket_id).expect("checked above");
        self.order.retain(|id| id != ticket_id);
        let (sender, receiver) = broadcast::channel(STREAM_QUEUE_CAPACITY);
        for chunk in backlog(&stream) {
            publish(&sender, chunk);
        }
        self.sessions.insert(
            ticket_id.to_owned(),
            StreamSession {
                sender,
                ticks: 0,
                pending_stdin: VecDeque::new(),
                last_resize: None,
                stream: stream.clone(),
            },
        );
        Ok((receiver, stream))
    }

    /// Queue TTY stdin for the next explicit tick. Unknown sessions are
    /// rejected so redeemed-but-gone streams cannot be fed.
    pub fn queue_stdin(&mut self, ticket_id: &str, line: String) -> Result<(), BackendError> {
        let Some(session) = self.sessions.get_mut(ticket_id) else {
            return Err(BackendError::Conflict(
                "the stream session is not active".into(),
            ));
        };
        session.pending_stdin.push_back(line);
        Ok(())
    }

    /// Record a terminal resize behind the adapter seam.
    pub fn record_resize(
        &mut self,
        ticket_id: &str,
        cols: u32,
        rows: u32,
    ) -> Result<(), BackendError> {
        let Some(session) = self.sessions.get_mut(ticket_id) else {
            return Err(BackendError::Conflict(
                "the stream session is not active".into(),
            ));
        };
        session.last_resize = Some((cols, rows));
        Ok(())
    }

    /// Last recorded resize of a live session; observability for tests.
    #[must_use]
    pub fn last_resize(&self, ticket_id: &str) -> Option<(u32, u32)> {
        self.sessions.get(ticket_id).and_then(|s| s.last_resize)
    }

    /// Advance one explicit test tick: prune sessions whose receivers all
    /// vanished (terminal disconnect on socket loss), then emit the next
    /// deterministic chunk(s). No wall clock, no process, no command.
    pub fn tick(&mut self, ticket_id: &str) {
        self.prune();
        let Some(session) = self.sessions.get_mut(ticket_id) else {
            return;
        };
        session.ticks += 1;
        let ticks = session.ticks;
        let chunks: Vec<StreamChunk> = match &session.stream {
            StreamKind::Logs { pod, container, .. } => vec![StreamChunk {
                origin: StreamOrigin::Stdout,
                text: format!("{pod}/{container} log tick {ticks}"),
                exit_code: None,
            }],
            StreamKind::Exec {
                pod,
                container,
                tty,
                ..
            } => {
                let mut chunks = Vec::new();
                while let Some(line) = session.pending_stdin.pop_front() {
                    if *tty {
                        chunks.push(StreamChunk {
                            origin: StreamOrigin::TtyOutput,
                            text: format!("$ {line}\r\nok\r\n"),
                            exit_code: None,
                        });
                    } else {
                        chunks.push(StreamChunk {
                            origin: StreamOrigin::Stdout,
                            text: format!("[stdout] echoed: {line}"),
                            exit_code: None,
                        });
                        chunks.push(StreamChunk {
                            origin: StreamOrigin::Stderr,
                            text: format!("[stderr] noted: {line}"),
                            exit_code: None,
                        });
                    }
                }
                if chunks.is_empty() && *tty {
                    chunks.push(StreamChunk {
                        origin: StreamOrigin::TtyOutput,
                        text: format!("shell idle tick {ticks}"),
                        exit_code: None,
                    });
                }
                if chunks.is_empty() && !*tty {
                    chunks.push(StreamChunk {
                        origin: StreamOrigin::Stdout,
                        text: format!("{pod}/{container} stdout tick {ticks}"),
                        exit_code: None,
                    });
                    chunks.push(StreamChunk {
                        origin: StreamOrigin::Stderr,
                        text: format!("{pod}/{container} stderr tick {ticks}"),
                        exit_code: None,
                    });
                }
                chunks
            }
        };
        let sender = session.sender.clone();
        for chunk in chunks {
            publish(&sender, chunk);
        }
    }

    /// Terminate a session deterministically: forward the final chunk with
    /// the exit code and retire the session.
    pub fn finish(&mut self, ticket_id: &str, exit_code: i32) {
        self.prune();
        if let Some(session) = self.sessions.remove(ticket_id) {
            let origin = match &session.stream {
                StreamKind::Logs { .. } => StreamOrigin::Stdout,
                StreamKind::Exec { tty, .. } => {
                    if *tty {
                        StreamOrigin::TtyOutput
                    } else {
                        StreamOrigin::Stdout
                    }
                }
            };
            publish(
                &session.sender,
                StreamChunk {
                    origin,
                    text: String::new(),
                    exit_code: Some(exit_code),
                },
            );
        }
    }

    /// Drop sessions whose receivers were all dropped.
    pub fn prune(&mut self) {
        self.sessions
            .retain(|_, session| session.sender.receiver_count() > 0);
    }

    /// Number of live sessions; observability for disconnect tests.
    #[must_use]
    pub fn live_session_count(&self) -> usize {
        self.sessions.len()
    }
}

fn ticket_route(stream: &StreamKind) -> crate::port::StreamRouteKind {
    match stream {
        StreamKind::Logs { .. } => crate::port::StreamRouteKind::Logs,
        StreamKind::Exec { .. } => crate::port::StreamRouteKind::Exec,
    }
}

/// Kernel-mapped stream-ticket grant carrying the exact wire payload.
#[derive(Debug, Clone)]
pub struct StreamTicketResult {
    payload: k10s_protocol::stream::StreamTicketResponse,
}

impl StreamTicketResult {
    /// Map a backend-owned grant into the protocol-facing response.
    #[must_use]
    pub fn new(grant: crate::port::StreamGrant) -> Self {
        let (context, namespace, pod, uid, container, stream_type, tty) = match &grant.stream {
            StreamKind::Logs {
                context,
                namespace,
                pod,
                uid,
                container,
                ..
            } => (
                context.clone(),
                namespace.clone(),
                pod.clone(),
                uid.clone(),
                container.clone(),
                k10s_protocol::StreamType::Logs,
                false,
            ),
            StreamKind::Exec {
                context,
                namespace,
                pod,
                uid,
                container,
                tty,
                ..
            } => (
                context.clone(),
                namespace.clone(),
                pod.clone(),
                uid.clone(),
                container.clone(),
                k10s_protocol::StreamType::Exec,
                *tty,
            ),
        };
        Self {
            payload: k10s_protocol::stream::StreamTicketResponse {
                ticket_id: grant.ticket_id,
                target: k10s_protocol::stream::StreamTarget {
                    context,
                    namespace,
                    pod,
                    uid,
                    container,
                },
                stream_type,
                tty,
            },
        }
    }

    /// Return the exact response payload for a `response` frame.
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::stream::StreamTicketResponse {
        self.payload.clone()
    }

    /// Serialize the wire payload to a JSON string.
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("StreamTicketResponse must serialize")
    }
}

/// Deterministic historical tail emitted at redemption time.
fn backlog(stream: &StreamKind) -> Vec<StreamChunk> {
    match stream {
        StreamKind::Logs { pod, container, .. } => (1..=2)
            .map(|index| StreamChunk {
                origin: StreamOrigin::Stdout,
                text: format!("{pod}/{container} backlog line {index}"),
                exit_code: None,
            })
            .collect(),
        StreamKind::Exec {
            pod,
            container,
            tty,
            ..
        } => vec![StreamChunk {
            origin: if *tty {
                StreamOrigin::TtyOutput
            } else {
                StreamOrigin::Stdout
            },
            text: format!("attached to {pod}/{container}"),
            exit_code: None,
        }],
    }
}

fn publish(sender: &broadcast::Sender<BackendEvent>, chunk: StreamChunk) {
    let _ = sender.send(BackendEvent::Stream(chunk));
}
