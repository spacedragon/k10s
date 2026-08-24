//! Bounded idempotency-key retention for accepted mutations.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(super) struct Entry {
    pub(super) operation_id: String,
    pub(super) fingerprint: String,
    accepted_at: Instant,
}

#[derive(Debug)]
pub(super) struct IdempotencyStore {
    entries: HashMap<String, Entry>,
    order: VecDeque<String>,
    capacity: usize,
    ttl: Duration,
}

impl IdempotencyStore {
    pub(super) fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            ttl,
        }
    }

    pub(super) fn get(
        &mut self,
        key: &str,
        now: Instant,
        live_operations: &HashSet<String>,
    ) -> Option<Entry> {
        self.expire(now, live_operations);
        self.entries.get(key).cloned()
    }

    pub(super) fn insert(&mut self, key: String, entry: Entry) {
        self.entries.insert(key.clone(), entry);
        self.order.push_back(key);
    }

    pub(super) fn make_room(&mut self, now: Instant, live_operations: &HashSet<String>) -> bool {
        self.expire(now, live_operations);
        while self.entries.len() >= self.capacity {
            let Some(victim) = self
                .order
                .iter()
                .find(|key| {
                    self.entries
                        .get(*key)
                        .is_some_and(|entry| !live_operations.contains(&entry.operation_id))
                })
                .cloned()
            else {
                return false;
            };
            self.entries.remove(&victim);
            self.order.retain(|key| key != &victim);
        }
        true
    }

    pub(super) fn remove_operation(&mut self, operation_id: &str) {
        self.entries
            .retain(|_, entry| entry.operation_id != operation_id);
        self.order.retain(|key| self.entries.contains_key(key));
    }

    fn expire(&mut self, now: Instant, live_operations: &HashSet<String>) {
        self.entries.retain(|_, entry| {
            live_operations.contains(&entry.operation_id)
                || now.duration_since(entry.accepted_at) < self.ttl
        });
        self.order.retain(|key| self.entries.contains_key(key));
    }

    pub(super) fn entry(operation_id: String, fingerprint: String, now: Instant) -> Entry {
        Entry {
            operation_id,
            fingerprint,
            accepted_at: now,
        }
    }
}
