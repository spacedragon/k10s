//! Kernel-owned bounded, single-use log stream tickets and fake sessions.
use crate::port::{BackendError, BackendEvent, StreamKind};
use std::collections::{HashMap, VecDeque};
use tokio::sync::broadcast;

pub const STREAM_QUEUE_CAPACITY: usize = 128;
const TICKET_CAPACITY: usize = 32;
const TICKET_MAX_AGE_REVISIONS: u64 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamOrigin {
    Stdout,
}
impl StreamOrigin {
    #[must_use]
    pub fn payload_kind(self) -> u8 {
        k10s_protocol::payload_kind::STDOUT
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamChunk {
    pub origin: StreamOrigin,
    pub text: String,
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
}

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
    pub fn new() -> Self {
        Self {
            tickets: HashMap::new(),
            order: VecDeque::new(),
            sessions: HashMap::new(),
            next_ticket: 1,
        }
    }
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
    pub fn redeem(
        &mut self,
        ticket_id: &str,
        expected_route: crate::port::StreamRouteKind,
        current_revision: u64,
    ) -> Result<(broadcast::Receiver<BackendEvent>, StreamKind), BackendError> {
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
        if expected_route != crate::port::StreamRouteKind::Logs {
            return Err(BackendError::Conflict(
                "the stream ticket was issued for a different route".into(),
            ));
        }
        let PendingTicket { stream, .. } = self.tickets.remove(ticket_id).expect("checked above");
        self.order.retain(|id| id != ticket_id);
        let (sender, receiver) = broadcast::channel(STREAM_QUEUE_CAPACITY);
        let StreamKind::Logs { pod, container, .. } = &stream;
        for index in 1..=2 {
            publish(
                &sender,
                StreamChunk {
                    origin: StreamOrigin::Stdout,
                    text: format!("{pod}/{container} backlog line {index}"),
                },
            );
        }
        self.sessions.insert(
            ticket_id.to_owned(),
            StreamSession {
                sender,
                ticks: 0,
                stream: stream.clone(),
            },
        );
        Ok((receiver, stream))
    }
    pub fn tick(&mut self, ticket_id: &str) {
        self.prune();
        let Some(session) = self.sessions.get_mut(ticket_id) else {
            return;
        };
        session.ticks += 1;
        let StreamKind::Logs { pod, container, .. } = &session.stream;
        publish(
            &session.sender,
            StreamChunk {
                origin: StreamOrigin::Stdout,
                text: format!("{pod}/{container} log tick {}", session.ticks),
            },
        );
    }
    pub fn prune(&mut self) {
        self.sessions
            .retain(|_, session| session.sender.receiver_count() > 0);
    }
    #[must_use]
    pub fn live_session_count(&self) -> usize {
        self.sessions.len()
    }
}

#[derive(Debug, Clone)]
pub struct StreamTicketResult {
    payload: k10s_protocol::StreamTicketResponse,
}
impl StreamTicketResult {
    #[must_use]
    pub fn new(grant: crate::port::StreamGrant) -> Self {
        let StreamKind::Logs {
            context,
            namespace,
            pod,
            uid,
            container,
            ..
        } = &grant.stream;
        Self {
            payload: k10s_protocol::StreamTicketResponse {
                ticket_id: grant.ticket_id,
                target: k10s_protocol::StreamTarget {
                    context: context.clone(),
                    namespace: namespace.clone(),
                    pod: pod.clone(),
                    uid: uid.clone(),
                    container: container.clone(),
                },
                stream_type: k10s_protocol::StreamType::Logs,
                tty: false,
            },
        }
    }
    #[must_use]
    pub fn wire_payload(&self) -> k10s_protocol::StreamTicketResponse {
        self.payload.clone()
    }
    #[must_use]
    pub fn serialized(&self) -> String {
        serde_json::to_string(&self.payload).expect("StreamTicketResponse must serialize")
    }
}
fn publish(sender: &broadcast::Sender<BackendEvent>, chunk: StreamChunk) {
    let _ = sender.send(BackendEvent::Stream(chunk));
}
