//! Shared bounded operation lifecycle engine for fake and real submissions.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use super::idempotency::IdempotencyStore;
use super::{OperationEvent, OperationRecord, OperationState, OperationStatusData};
use crate::port::{BackendError, BackendEvent, OperationId};

pub const DEFAULT_OPERATION_CAPACITY: usize = 1_024;
pub const DEFAULT_IDEMPOTENCY_CAPACITY: usize = 1_024;
pub const DEFAULT_RETENTION_TTL: Duration = Duration::from_secs(15 * 60);
const EVENT_CAPACITY: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptOutcome {
    Accepted(OperationId),
    Replayed(OperationId),
}

impl AcceptOutcome {
    #[must_use]
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::Accepted(id) | Self::Replayed(id) => id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperationEngine {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Retained {
    record: OperationRecord,
    fingerprint: String,
    updated_at: Instant,
}

#[derive(Debug)]
struct Inner {
    instance_id: String,
    next_id: u64,
    operations: HashMap<String, Retained>,
    order: VecDeque<String>,
    operation_capacity: usize,
    ttl: Duration,
    idempotency: IdempotencyStore,
    watchers: Vec<broadcast::Sender<BackendEvent>>,
}

impl Default for OperationEngine {
    fn default() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string())
    }
}

impl OperationEngine {
    #[must_use]
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self::with_limits(
            instance_id,
            DEFAULT_OPERATION_CAPACITY,
            DEFAULT_IDEMPOTENCY_CAPACITY,
            DEFAULT_RETENTION_TTL,
        )
    }

    #[must_use]
    pub fn with_limits(
        instance_id: impl Into<String>,
        operations: usize,
        keys: usize,
        ttl: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                instance_id: instance_id.into(),
                next_id: 1,
                operations: HashMap::new(),
                order: VecDeque::new(),
                operation_capacity: operations,
                ttl,
                idempotency: IdempotencyStore::new(keys, ttl),
                watchers: Vec::new(),
            })),
        }
    }

    pub fn accept(
        &self,
        idempotency_key: &str,
        fingerprint: &str,
    ) -> Result<AcceptOutcome, BackendError> {
        if idempotency_key.is_empty() {
            return Err(BackendError::Conflict(
                "an idempotency key is required".into(),
            ));
        }
        let mut inner = self.lock();
        let now = Instant::now();
        inner.expire(now);
        let live = inner.live_operation_ids();
        if let Some(entry) = inner.idempotency.get(idempotency_key, now, &live) {
            if entry.fingerprint != fingerprint {
                return Err(BackendError::Conflict(
                    "the idempotency key was already used for a different submission".into(),
                ));
            }
            return Ok(AcceptOutcome::Replayed(OperationId::new(
                entry.operation_id,
            )));
        }
        if inner
            .operations
            .values()
            .any(|entry| !entry.record.state.is_terminal() && entry.fingerprint == fingerprint)
        {
            return Err(BackendError::Conflict(
                "an equivalent operation is already in flight".into(),
            ));
        }
        inner.make_room()?;
        if !inner.idempotency.make_room(now, &live) {
            return Err(BackendError::Conflict(
                "the idempotency capacity is full".into(),
            ));
        }
        let id = format!("{}:op-{:06}", inner.instance_id, inner.next_id);
        inner.next_id = inner.next_id.wrapping_add(1);
        let record = OperationRecord {
            id: id.clone(),
            state: OperationState::Pending,
            progress: None,
            detail: None,
        };
        inner.operations.insert(
            id.clone(),
            Retained {
                record: record.clone(),
                fingerprint: fingerprint.into(),
                updated_at: now,
            },
        );
        inner.order.push_back(id.clone());
        inner.idempotency.insert(
            idempotency_key.into(),
            IdempotencyStore::entry(id.clone(), fingerprint.into(), now),
        );
        inner.emit(event_from(&record, None));
        tracing::info!(operation_id = %id, state = ?OperationState::Pending, "operation accepted");
        Ok(AcceptOutcome::Accepted(OperationId::new(id)))
    }

    pub fn running(&self, id: &str, progress: Option<(u32, u32)>) -> Result<(), BackendError> {
        self.transition(id, OperationState::Running, progress, None)
    }
    pub fn succeeded(&self, id: &str) -> Result<(), BackendError> {
        self.transition(id, OperationState::Succeeded, None, None)
    }
    pub fn failed(&self, id: &str, safe_detail: impl Into<String>) -> Result<(), BackendError> {
        self.transition(id, OperationState::Failed, None, Some(safe_detail.into()))
    }
    pub fn outcome_unknown(&self, id: &str) -> Result<(), BackendError> {
        self.transition(
            id,
            OperationState::OutcomeUnknown,
            None,
            Some("the submission outcome is unknown; refresh the target before retrying".into()),
        )
    }
    pub fn cancel_before_submit(&self, id: &str) -> Result<(), BackendError> {
        self.transition(id, OperationState::Cancelled, None, None)
    }

    pub fn status(&self, ids: &[String]) -> OperationStatusData {
        let mut inner = self.lock();
        inner.expire(Instant::now());
        OperationStatusData {
            operations: ids
                .iter()
                .filter_map(|id| inner.operations.get(id).map(|v| public_record(&v.record)))
                .collect(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BackendEvent> {
        let mut inner = self.lock();
        inner.expire(Instant::now());
        let (sender, receiver) = broadcast::channel(EVENT_CAPACITY);
        let mut live: Vec<OperationRecord> = inner
            .operations
            .values()
            .filter(|v| !v.record.state.is_terminal())
            .map(|v| v.record.clone())
            .collect();
        live.sort_by(|left, right| left.id.cmp(&right.id));
        for record in live {
            let _ = sender.send(BackendEvent::Operation(event_from(&record, None)));
        }
        inner.watchers.push(sender);
        receiver
    }

    fn transition(
        &self,
        id: &str,
        next: OperationState,
        progress: Option<(u32, u32)>,
        detail: Option<String>,
    ) -> Result<(), BackendError> {
        let mut inner = self.lock();
        let now = Instant::now();
        let event = {
            let retained = inner.operations.get_mut(id).ok_or(BackendError::NotFound)?;
            if retained.record.state.is_terminal() {
                return Err(BackendError::Conflict(
                    "the operation is already terminal".into(),
                ));
            }
            let allowed = matches!(
                (retained.record.state, next),
                (
                    OperationState::Pending,
                    OperationState::Running | OperationState::Cancelled | OperationState::Failed
                ) | (
                    OperationState::Running,
                    OperationState::Running
                        | OperationState::Succeeded
                        | OperationState::Failed
                        | OperationState::OutcomeUnknown
                ) | (
                    OperationState::OutcomeUnknown,
                    OperationState::Succeeded | OperationState::Failed
                )
            );
            if !allowed {
                return Err(BackendError::Conflict(
                    "the operation transition is not valid".into(),
                ));
            }
            if progress.is_some_and(|(completed, total)| total == 0 || completed > total) {
                return Err(BackendError::Conflict(
                    "operation progress is invalid".into(),
                ));
            }
            retained.record.state = next;
            retained.record.progress = progress;
            retained.updated_at = now;
            retained.record.detail = detail;
            event_from(&retained.record, retained.record.detail.clone())
        };
        if matches!(next, OperationState::Failed | OperationState::Cancelled) {
            inner.idempotency.remove_operation(id);
        }
        inner.emit(event);
        tracing::info!(operation_id = %id, state = ?next, "operation state changed");
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Inner {
    fn live_operation_ids(&self) -> std::collections::HashSet<String> {
        self.operations
            .iter()
            .filter(|(_, retained)| !retained.record.state.is_terminal())
            .map(|(id, _)| id.clone())
            .collect()
    }
    fn expire(&mut self, now: Instant) {
        self.operations.retain(|_, v| {
            !v.record.state.is_terminal() || now.duration_since(v.updated_at) < self.ttl
        });
        self.order.retain(|id| self.operations.contains_key(id));
    }
    fn make_room(&mut self) -> Result<(), BackendError> {
        while self.operations.len() >= self.operation_capacity {
            let Some(victim) = self
                .order
                .iter()
                .find(|id| {
                    self.operations
                        .get(*id)
                        .is_some_and(|v| v.record.state.is_terminal())
                })
                .cloned()
            else {
                return Err(BackendError::Conflict(
                    "the operation capacity is full".into(),
                ));
            };
            self.operations.remove(&victim);
            self.order.retain(|id| id != &victim);
        }
        Ok(())
    }
    fn emit(&mut self, event: OperationEvent) {
        self.watchers.retain(|sender| sender.receiver_count() > 0);
        for sender in &self.watchers {
            let _ = sender.send(BackendEvent::Operation(event.clone()));
        }
    }
}

fn public_record(record: &OperationRecord) -> OperationRecord {
    record.clone()
}
fn event_from(record: &OperationRecord, detail: Option<String>) -> OperationEvent {
    OperationEvent {
        id: record.id.clone(),
        state: record.state,
        progress: record.progress,
        detail,
    }
}
