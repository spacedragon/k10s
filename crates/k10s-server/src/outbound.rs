use axum::extract::ws::Message;
use tokio::sync::mpsc;

/// Priority class for bounded outbound scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    P0,
    P1,
    P2,
}

#[derive(Debug, Clone)]
pub(crate) struct Outbound {
    tx: mpsc::Sender<Message>,
}

impl Outbound {
    pub(crate) fn new(tx: mpsc::Sender<Message>) -> Self {
        Self { tx }
    }

    pub(crate) fn send(&self, message: Message, priority: Priority) -> Result<(), &'static str> {
        self.tx.try_send(message).map_err(|_| match priority {
            Priority::P0 | Priority::P1 => "outbound overload",
            Priority::P2 => "resource delta coalesced",
        })
    }
}
