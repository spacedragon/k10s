use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::extract::ws::Message;
use tokio::sync::Notify;

use crate::compatibility::SessionProtocol;

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

#[derive(Debug)]
pub struct ScheduledItem {
    pub message: Message,
}

#[derive(Debug)]
struct Entry {
    priority: Priority,
    message: Message,
    p2_identity: Option<P2Identity>,
    sequence: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
enum P2Identity {
    Resource {
        subscription: Option<String>,
        resource: String,
    },
    RecoveryBarrier,
}

#[derive(Debug)]
struct State {
    queue: VecDeque<Entry>,
    closed: bool,
}

/// Fixed-capacity priority scheduler with reliable reserve and
/// subscription-scoped P2 coalescing.
#[derive(Debug, Clone)]
pub struct Scheduler {
    capacity: usize,
    reliable_reserve: usize,
    state: Arc<Mutex<State>>,
    ready: Arc<Notify>,
    protocol: SessionProtocol,
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
            protocol: SessionProtocol::current(),
        }
    }

    /// Bind all subsequent outbound serialization to the version negotiated
    /// for this control session.
    pub(crate) fn set_negotiated_protocol(&self, protocol: k10s_protocol::ProtocolVersion) {
        self.protocol.set_negotiated(protocol);
    }

    /// Serialize one payload through the same per-session compatibility
    /// policy used by the queue boundary. Snapshot checksums use this form so
    /// they cover the exact compatible bytes sent to the client.
    pub(crate) fn compatible_value<T: serde::Serialize>(&self, value: T) -> serde_json::Value {
        self.protocol.compatible_value(value)
    }

    pub fn enqueue(&self, priority: Priority, message: Message) -> Result<(), EnqueueError> {
        debug_assert_ne!(priority, Priority::P2, "P2 requires a coalescing identity");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.queue.len() == self.capacity {
            return Err(EnqueueError::Overloaded);
        }
        state.queue.push_back(Entry {
            priority,
            message: self.protocol.prepare_message(message),
            p2_identity: None,
            sequence: None,
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Enqueue a reliable sequenced item while holding the scheduler lock so
    /// allocation order and queue order cannot diverge across forwarders.
    pub fn enqueue_sequenced(
        &self,
        build: impl FnOnce() -> Result<(u64, Message), EnqueueError>,
    ) -> Result<(), EnqueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.queue.len() == self.capacity {
            return Err(EnqueueError::Overloaded);
        }
        let (sequence, message) = build()?;
        state.queue.push_back(Entry {
            priority: Priority::P1,
            message: self.protocol.prepare_message(message),
            p2_identity: None,
            sequence: Some(sequence),
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Enqueue a lossless sequenced item at `P0` priority while holding the
    /// scheduler lock so connection sequences stay contiguous with every
    /// other sequenced frame. Operation updates ride this path: they must
    /// reach the client even when coalescible `P2` traffic fills its
    /// partition, but never create a wire-level sequence hole.
    pub fn enqueue_p0_sequenced(
        &self,
        build: impl FnOnce() -> Result<(u64, Message), EnqueueError>,
    ) -> Result<(), EnqueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.queue.len() == self.capacity {
            return Err(EnqueueError::Overloaded);
        }
        let (sequence, message) = build()?;
        state.queue.push_back(Entry {
            priority: Priority::P0,
            message: self.protocol.prepare_message(message),
            p2_identity: None,
            sequence: Some(sequence),
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    pub fn enqueue_p2(
        &self,
        resource: impl Into<String>,
        message: Message,
    ) -> Result<(), EnqueueError> {
        let p2_identity = P2Identity::Resource {
            subscription: None,
            resource: resource.into(),
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(EnqueueError::Coalesced);
        }
        if let Some(entry) = state
            .queue
            .iter_mut()
            .find(|entry| entry.p2_identity.as_ref() == Some(&p2_identity))
        {
            entry.message = self.protocol.prepare_message(message);
            return Ok(());
        }
        let p2_limit = self.capacity.saturating_sub(self.reliable_reserve);
        let p2_count = state
            .queue
            .iter()
            .filter(|entry| {
                matches!(
                    entry.p2_identity.as_ref(),
                    Some(P2Identity::Resource { .. })
                )
            })
            .count();
        if state.queue.len() == self.capacity || p2_count >= p2_limit {
            return Err(EnqueueError::Coalesced);
        }
        state.queue.push_back(Entry {
            priority: Priority::P2,
            message: self.protocol.prepare_message(message),
            p2_identity: Some(p2_identity),
            sequence: None,
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Enqueue a subscription-scoped sequenced P2 item, allocating a
    /// connection sequence only for a new queue slot. Replacements rebuild
    /// their payload with the original slot sequence so coalescing cannot
    /// create a wire-level sequence hole.
    pub fn enqueue_p2_sequenced(
        &self,
        subscription: impl Into<String>,
        resource: impl Into<String>,
        build: impl FnOnce(Option<u64>) -> Result<(u64, Message), EnqueueError>,
    ) -> Result<(), EnqueueError> {
        let p2_identity = P2Identity::Resource {
            subscription: Some(subscription.into()),
            resource: resource.into(),
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(EnqueueError::Coalesced);
        }
        if let Some(entry) = state
            .queue
            .iter_mut()
            .find(|entry| entry.p2_identity.as_ref() == Some(&p2_identity))
        {
            let queued_sequence = entry
                .sequence
                .expect("sequenced P2 entries retain their connection sequence");
            let (sequence, message) = build(Some(queued_sequence))?;
            debug_assert_eq!(sequence, queued_sequence);
            entry.message = self.protocol.prepare_message(message);
            return Ok(());
        }
        let p2_limit = self.capacity.saturating_sub(self.reliable_reserve);
        let p2_count = state
            .queue
            .iter()
            .filter(|entry| {
                matches!(
                    entry.p2_identity.as_ref(),
                    Some(P2Identity::Resource { .. })
                )
            })
            .count();
        if state.queue.len() == self.capacity || p2_count >= p2_limit {
            return Err(EnqueueError::Coalesced);
        }
        let (sequence, message) = build(None)?;
        state.queue.push_back(Entry {
            priority: Priority::P2,
            message: self.protocol.prepare_message(message),
            p2_identity: Some(p2_identity),
            sequence: Some(sequence),
        });
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    /// Append a sequenced recovery barrier behind the current P2 tail, or
    /// reuse the queued barrier when another watch has already demanded
    /// recovery. The sequence is allocated while holding the queue lock so
    /// later sequenced frames cannot be admitted ahead of it.
    pub fn enqueue_p2_barrier(
        &self,
        build: impl FnOnce() -> Result<(u64, Message), EnqueueError>,
    ) -> Result<(), EnqueueError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(EnqueueError::Overloaded);
        }
        if state
            .queue
            .iter()
            .any(|entry| entry.p2_identity.as_ref() == Some(&P2Identity::RecoveryBarrier))
        {
            return Ok(());
        }
        if state.queue.len() == self.capacity {
            return Err(EnqueueError::Overloaded);
        }
        let (sequence, message) = build()?;
        state.queue.push_back(Entry {
            priority: Priority::P2,
            message: self.protocol.prepare_message(message),
            p2_identity: Some(P2Identity::RecoveryBarrier),
            sequence: Some(sequence),
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
                    .position(|entry| entry.priority == Priority::P0 && entry.sequence.is_none())
                    .or_else(|| {
                        state.queue.iter().position(|entry| {
                            entry.priority == Priority::P1 && entry.sequence.is_none()
                        })
                    })
                    // Sequenced frames must always drain in sequence order
                    // regardless of their class, or clients would observe a
                    // sequence gap.
                    .or_else(|| {
                        state
                            .queue
                            .iter()
                            .enumerate()
                            .filter_map(|(index, entry)| {
                                entry.sequence.map(|sequence| (sequence, index))
                            })
                            .min_by_key(|(sequence, _)| *sequence)
                            .map(|(_, index)| index)
                    })
                    .or_else(|| (!state.queue.is_empty()).then_some(0));
                if let Some(index) = index {
                    let entry = state.queue.remove(index).expect("selected entry exists");
                    return Some(ScheduledItem {
                        message: entry.message,
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
            p2_identity: None,
            sequence: None,
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
        scheduler.enqueue_p2("pod/a", text("delta")).unwrap();
        scheduler.enqueue(Priority::P1, text("response")).unwrap();
        scheduler.enqueue(Priority::P0, text("terminal")).unwrap();
        assert_eq!(scheduler.recv().await.unwrap().message, text("terminal"));
        assert_eq!(scheduler.recv().await.unwrap().message, text("response"));
        assert_eq!(scheduler.recv().await.unwrap().message, text("delta"));
    }

    #[tokio::test]
    async fn same_resource_coalesces_to_the_latest_payload() {
        let scheduler = Scheduler::new(3, 1);
        scheduler.enqueue_p2("pod/a", text("old")).unwrap();
        scheduler.enqueue_p2("pod/a", text("new")).unwrap();
        let item = scheduler.recv().await.unwrap();
        assert_eq!(item.message, text("new"));
    }

    #[tokio::test]
    async fn sequenced_replacement_reuses_the_queued_sequence() {
        let scheduler = Scheduler::new(3, 1);
        let allocated = std::sync::atomic::AtomicU64::new(0);
        for payload in ["old", "new"] {
            scheduler
                .enqueue_p2_sequenced("sub-a", "pod/a", |queued| {
                    let sequence = queued.unwrap_or_else(|| {
                        allocated.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1
                    });
                    Ok((sequence, text(&format!("{payload}-{sequence}"))))
                })
                .unwrap();
        }
        assert_eq!(allocated.load(std::sync::atomic::Ordering::Acquire), 1);
        assert_eq!(scheduler.recv().await.unwrap().message, text("new-1"));
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
        scheduler.enqueue_p2("pod/a", text("a")).unwrap();
        scheduler.enqueue_p2("pod/b", text("b")).unwrap();
        assert_eq!(
            scheduler.enqueue_p2("pod/c", text("c")),
            Err(EnqueueError::Coalesced)
        );
        scheduler.enqueue(Priority::P0, text("close")).unwrap();
    }

    #[test]
    fn recovery_barrier_does_not_expand_the_resource_partition() {
        let scheduler = Scheduler::new(4, 2);
        scheduler.enqueue_p2("pod/a", text("a")).unwrap();
        scheduler.enqueue_p2("pod/b", text("b")).unwrap();
        scheduler
            .enqueue_p2_barrier(|| Ok((3, text("resync"))))
            .unwrap();

        assert_eq!(
            scheduler.enqueue_p2("pod/c", text("c")),
            Err(EnqueueError::Coalesced),
            "the queued barrier must not let resource traffic enter the reserve"
        );
        assert_eq!(scheduler.len(), 3);
    }

    #[test]
    fn concurrent_recovery_barriers_share_one_reliable_slot() {
        let scheduler = Scheduler::new(4, 2);
        scheduler.enqueue_p2("pod/a", text("a")).unwrap();
        scheduler.enqueue_p2("pod/b", text("b")).unwrap();
        let allocated = std::sync::atomic::AtomicU64::new(2);
        for _ in 0..2 {
            scheduler
                .enqueue_p2_barrier(|| {
                    let sequence = allocated.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
                    Ok((sequence, text(&format!("resync-{sequence}"))))
                })
                .unwrap();
        }

        assert_eq!(allocated.load(std::sync::atomic::Ordering::Acquire), 3);
        assert_eq!(scheduler.len(), 3, "only one recovery barrier is queued");
    }
}
