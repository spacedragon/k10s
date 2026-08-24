//! Bounded, short-lived validation tickets owned by one backend process.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::operation::Ticket;
use crate::port::BackendError;

pub(crate) const TICKET_CAPACITY: usize = 1_024;
pub(crate) const TICKET_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub(crate) struct TicketStore {
    tickets: HashMap<String, (Ticket, Instant)>,
    order: VecDeque<String>,
    instance_id: String,
}

impl TicketStore {
    pub(crate) fn new() -> Self {
        Self {
            tickets: HashMap::new(),
            order: VecDeque::new(),
            instance_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub(crate) fn issue(&mut self, mut ticket: Ticket) -> Ticket {
        self.expire();
        ticket.id = format!("{}:{}", self.instance_id, uuid::Uuid::new_v4());
        self.order.push_back(ticket.id.clone());
        self.tickets
            .insert(ticket.id.clone(), (ticket.clone(), Instant::now()));
        while self.tickets.len() > TICKET_CAPACITY {
            if let Some(id) = self.order.pop_front() {
                self.tickets.remove(&id);
            }
        }
        ticket
    }

    /// Consume a ticket exactly once. IDs issued by another server instance,
    /// expired IDs, and already consumed IDs are deliberately indistinguishable.
    // Wired into the apply command in Plan 4 Task 3; retained here because
    // issuance and redemption must share the exact same authority boundary.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn take(&mut self, id: &str) -> Result<Ticket, BackendError> {
        self.expire();
        if !id.starts_with(&self.instance_id) {
            return Err(BackendError::Conflict(
                "the validation ticket is unknown or expired".into(),
            ));
        }
        let Some((ticket, _)) = self.tickets.remove(id) else {
            return Err(BackendError::Conflict(
                "the validation ticket is unknown or expired".into(),
            ));
        };
        self.order.retain(|queued| queued != id);
        Ok(ticket)
    }

    /// Inspect a current ticket without consuming it. Mutation admission uses
    /// this before idempotency acceptance; only a fresh accepted operation
    /// consumes the ticket, while an exact replay returns its original ID.
    pub(crate) fn inspect(&mut self, id: &str) -> Result<Ticket, BackendError> {
        self.expire();
        if !id.starts_with(&self.instance_id) {
            return Err(BackendError::Conflict(
                "the validation ticket is unknown or expired".into(),
            ));
        }
        self.tickets
            .get(id)
            .map(|(ticket, _)| ticket.clone())
            .ok_or_else(|| {
                BackendError::Conflict("the validation ticket is unknown or expired".into())
            })
    }

    fn expire(&mut self) {
        let now = Instant::now();
        self.tickets
            .retain(|_, (_, issued)| now.duration_since(*issued) < TICKET_TTL);
        self.order.retain(|id| self.tickets.contains_key(id));
    }
}

impl Default for TicketStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{Gvk, ResourceRef};

    fn ticket() -> Ticket {
        Ticket {
            id: String::new(),
            target: ResourceRef {
                context: "ctx".into(),
                gvk: Gvk::core("v1", "ConfigMap"),
                namespace: Some("default".into()),
                name: "settings".into(),
                uid: "uid-1".into(),
            },
            resource_revision: 1,
            opaque_resource_version: Some("42".into()),
            issued_revision: 1,
            buffer_hash: "hash".into(),
            disruptive: false,
        }
    }

    #[test]
    fn ids_are_opaque_and_bound_to_each_store_instance() {
        let mut first = TicketStore::new();
        let mut restarted = TicketStore::new();
        let first_id = first.issue(ticket()).id;
        let restarted_id = restarted.issue(ticket()).id;
        assert_ne!(first_id.split(':').next(), restarted_id.split(':').next());
        assert!(!first_id.contains("settings"));
        assert!(
            restarted.take(&first_id).is_err(),
            "restart invalidates authority"
        );
        assert!(first.take(&first_id).is_ok());
        assert!(first.take(&first_id).is_err(), "tickets are single-use");
    }

    #[test]
    fn expired_tickets_are_rejected_and_removed() {
        let mut store = TicketStore::new();
        let issued = store.issue(ticket());
        store.tickets.get_mut(&issued.id).unwrap().1 = Instant::now() - TICKET_TTL;
        assert!(store.take(&issued.id).is_err());
        assert!(store.tickets.is_empty());
    }
}
