use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::extract::ws::Message;
use tokio::sync::Notify;

/// Priority class for bounded outbound scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    P0,
    P1,
    P2,
}

/// Reason an outbound item was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueError {
    Overloaded,
    Coalesced,
}

/// Detectable discontinuity between resource revisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevisionGap {
    pub expected: u64,
    pub actual: u64,
}

#[derive(Debug)]
pub struct ScheduledItem {
    pub message: Message,
    pub gap: Option<RevisionGap>,
}

#[derive(Debug)]
struct Entry {
    priority: Priority,
    message: Message,
    resource: Option<String>,
    revision: Option<u64>,
    gap: Option<RevisionGap>,
}

#[derive(Debug)]
struct State {
    queue: VecDeque<Entry>,
    closed: bool,
}

/// Fixed-capacity priority scheduler with reliable reserve and P2 coalescing.
#[derive(Debug, Clone)]
pub struct Scheduler {
    capacity: usize,
    reliable_reserve: usize,
    state: Arc<Mutex<State>>,
    ready: Arc<Notify>,
}

impl Scheduler {
    pub fn new(capacity: usize, reliable_reserve: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            reliable_reserve: reliable_reserve.min(capacity),
            state: Arc::new(Mutex::new(State {
                queue: VecDeque::with_capacity(capacity),
                closed: false,
            })),
            ready: Arc::new(Notify::new()),
        }
    }

    pub fn enqueue(&self, priority: Priority, message: Message) -> Result<(), EnqueueError> {
        debug_assert_ne!(priority, Priority::P2, "P2 requires identity and revision");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.queue.len() == self.capacity {
            return Err(EnqueueError::Overloaded);
        }
        state.queue.push_back(Entry {
            priority,
            message,
            resource: None,
            revision: None,
            gap: None,
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    pub fn enqueue_p2(
        &self,
        resource: impl Into<String>,
        revision: u64,
        message: Message,
    ) -> Result<(), EnqueueError> {
        let resource = resource.into();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = state.queue.iter_mut().find(|entry| {
            entry.priority == Priority::P2 && entry.resource.as_deref() == Some(resource.as_str())
        }) {
            let previous = entry.revision.unwrap_or(revision);
            let expected = previous.saturating_add(1);
            entry.message = message;
            entry.revision = Some(revision);
            if revision != previous {
                entry.gap = Some(RevisionGap {
                    expected,
                    actual: revision,
                });
            }
            return Ok(());
        }
        let p2_limit = self.capacity.saturating_sub(self.reliable_reserve);
        let p2_count = state
            .queue
            .iter()
            .filter(|entry| entry.priority == Priority::P2)
            .count();
        if state.closed || state.queue.len() == self.capacity || p2_count == p2_limit {
            return Err(EnqueueError::Coalesced);
        }
        state.queue.push_back(Entry {
            priority: Priority::P2,
            message,
            resource: Some(resource),
            revision: Some(revision),
            gap: None,
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    pub async fn recv(&self) -> Option<ScheduledItem> {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let index = state
                    .queue
                    .iter()
                    .position(|entry| entry.priority == Priority::P0)
                    .or_else(|| {
                        state
                            .queue
                            .iter()
                            .position(|entry| entry.priority == Priority::P1)
                    })
                    .or_else(|| (!state.queue.is_empty()).then_some(0));
                if let Some(index) = index {
                    let entry = state.queue.remove(index).expect("selected entry exists");
                    return Some(ScheduledItem {
                        message: entry.message,
                        gap: entry.gap,
                    });
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.ready.notify_waiters();
    }

    /// Current number of scheduled items, for pressure telemetry.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .queue
            .len()
    }

    /// Whether the scheduler currently contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn overload_close(&self, message: Message) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.queue.clear();
        state.queue.push_back(Entry {
            priority: Priority::P0,
            message,
            resource: None,
            revision: None,
            gap: None,
        });
        drop(state);
        self.ready.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(value: &str) -> Message {
        Message::Text(value.to_owned().into())
    }

    #[tokio::test]
    async fn priority_ordering_and_fixed_reserve() {
        let scheduler = Scheduler::new(4, 2);
        scheduler.enqueue_p2("pod/a", 1, text("delta")).unwrap();
        scheduler.enqueue(Priority::P1, text("response")).unwrap();
        scheduler.enqueue(Priority::P0, text("terminal")).unwrap();
        assert_eq!(scheduler.recv().await.unwrap().message, text("terminal"));
        assert_eq!(scheduler.recv().await.unwrap().message, text("response"));
        assert_eq!(scheduler.recv().await.unwrap().message, text("delta"));
    }

    #[tokio::test]
    async fn same_resource_coalesces_and_marks_revision_gap() {
        let scheduler = Scheduler::new(3, 1);
        scheduler.enqueue_p2("pod/a", 7, text("old")).unwrap();
        scheduler.enqueue_p2("pod/a", 9, text("new")).unwrap();
        let item = scheduler.recv().await.unwrap();
        assert_eq!(item.message, text("new"));
        assert_eq!(
            item.gap,
            Some(RevisionGap {
                expected: 8,
                actual: 9
            })
        );
    }

    #[test]
    fn overload_is_exact_and_never_silent_for_reliable_priorities() {
        let scheduler = Scheduler::new(2, 1);
        scheduler.enqueue(Priority::P1, text("one")).unwrap();
        scheduler.enqueue(Priority::P0, text("two")).unwrap();
        assert_eq!(
            scheduler.enqueue(Priority::P1, text("three")),
            Err(EnqueueError::Overloaded)
        );
    }

    #[test]
    fn p2_cannot_consume_reliable_reserve() {
        let scheduler = Scheduler::new(3, 1);
        scheduler.enqueue_p2("pod/a", 1, text("a")).unwrap();
        scheduler.enqueue_p2("pod/b", 1, text("b")).unwrap();
        assert_eq!(
            scheduler.enqueue_p2("pod/c", 1, text("c")),
            Err(EnqueueError::Coalesced)
        );
        scheduler.enqueue(Priority::P0, text("close")).unwrap();
    }
}
