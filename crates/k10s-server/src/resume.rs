//! Bounded per-session resume journals for the control WebSocket.
//!
//! The journal is a pure optimization over the Plan 1 full-jitter reconnect /
//! full-resync baseline: whenever a gap cannot be filled within budget, callers
//! fall back to a fresh session and the standard resync behavior. Nothing in
//! this module may ever become a correctness dependency for clients.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

/// One journaled frame: the exact wire text as sent, with its sequence.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// The session's contiguous sequence for this frame.
    pub sequence: u64,
    /// When this frame passed the writer.
    pub sent_at: Instant,
    /// Serialized `ServerFrame` text ready to resend byte-for-byte.
    pub message: String,
}

/// Bounded per-session journal of frames ordered by sequence.
#[derive(Debug, Default)]
pub struct SessionJournal {
    entries: VecDeque<JournalEntry>,
    last_sequence: u64,
}

impl SessionJournal {
    /// Highest sequence issued on this session so far (0 when unused).
    #[must_use]
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Record one frame, evicting entries that exceed the budgets.
    ///
    /// Frames at or below `last_sequence` (replays of already-journaled frames)
    /// are ignored so a resume never double-records.
    pub fn record(&mut self, sequence: u64, message: &str, max_entries: usize, max_age: Duration) {
        if sequence <= self.last_sequence {
            return;
        }
        let now = Instant::now();
        self.entries.push_back(JournalEntry {
            sequence,
            sent_at: now,
            message: message.to_owned(),
        });
        self.last_sequence = sequence;
        self.prune(max_entries, max_age);
    }

    fn prune(&mut self, max_entries: usize, max_age: Duration) {
        while let Some(front) = self.entries.front() {
            if self.entries.len() > max_entries || front.sent_at.elapsed() >= max_age {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Return the contiguous run of frames after `cursor`, or `None` when it
    /// cannot be filled.
    ///
    /// `None` is returned when:
    /// - `cursor` exceeds the highest issued sequence (an invalid claim),
    /// - the first required frame was evicted, leaving a gap,
    /// - any required frame has aged past `max_age`.
    #[must_use]
    pub fn replay_from(&self, cursor: u64, max_age: Duration) -> Option<Vec<JournalEntry>> {
        if cursor > self.last_sequence {
            return None;
        }
        let mut replayed = Vec::new();
        for entry in &self.entries {
            if entry.sequence <= cursor {
                continue;
            }
            if entry.sent_at.elapsed() >= max_age {
                return None;
            }
            replayed.push(entry.clone());
        }
        // The first required frame must sit exactly at `cursor + 1`.
        if let Some(first) = replayed.first().map(|entry| entry.sequence)
            && first != cursor + 1
        {
            return None;
        }
        Some(replayed)
    }
}

/// Per-session runtime: journal plus the current transport lease.
#[derive(Debug)]
pub struct SessionState {
    pub(crate) journal: SessionJournal,
    /// Bumped on every (re-)lease; stale transports must not clobber successors.
    pub(crate) lease_generation: u64,
    /// Wake channel for the transport currently holding this session.
    pub(crate) takeover_tx: Option<tokio::sync::oneshot::Sender<()>>,
    last_activity: Instant,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            journal: SessionJournal::default(),
            lease_generation: 0,
            takeover_tx: None,
            last_activity: Instant::now(),
        }
    }
}

/// Server-wide registry of resumable sessions and their live leases.
#[derive(Debug)]
pub struct ResumeState {
    pub(crate) sessions: BTreeMap<String, SessionState>,
    pub(crate) max_entries: usize,
    pub(crate) max_sessions: usize,
    pub(crate) max_age: Duration,
}

/// Atomic result of selecting a replay and transferring its transport lease.
pub struct SessionClaim {
    pub session_id: String,
    pub resumed: bool,
    pub replay: Vec<JournalEntry>,
    pub previous_transport: Option<tokio::sync::oneshot::Sender<()>>,
    pub lease_generation: u64,
    pub last_sequence: u64,
}

/// Shared handle to the resume state (one per control server).
pub type ResumeStore = std::sync::Arc<std::sync::Mutex<ResumeState>>;

impl ResumeState {
    /// Create a store with the configured replay budgets.
    #[must_use]
    pub fn new(max_entries: usize, max_sessions: usize, max_age: Duration) -> Self {
        Self {
            sessions: BTreeMap::new(),
            max_entries,
            max_sessions: max_sessions.max(1),
            max_age,
        }
    }

    /// Atomically select a complete replay (when possible), capture its
    /// watermark, and transfer the transport lease. If resume is not safe, a
    /// bounded fresh session is created instead.
    #[allow(clippy::too_many_arguments)]
    pub fn claim(
        &mut self,
        hello_server_instance: Option<&str>,
        server_instance_id: &str,
        claimed_session: Option<&str>,
        last_acked_sequence: Option<u64>,
        fresh_session_id: String,
        takeover_tx: tokio::sync::oneshot::Sender<()>,
        replay_capacity: usize,
    ) -> Result<SessionClaim, ()> {
        self.prune();
        if let (Some(instance), Some(session_id), Some(cursor)) =
            (hello_server_instance, claimed_session, last_acked_sequence)
            && instance == server_instance_id
            && let Some(replay) = self
                .sessions
                .get(session_id)
                .and_then(|session| session.journal.replay_from(cursor, self.max_age))
            && replay.len() <= replay_capacity
        {
            let state = self
                .sessions
                .get_mut(session_id)
                .expect("session still exists");
            state.lease_generation = state.lease_generation.wrapping_add(1);
            state.last_activity = Instant::now();
            let previous_transport = state.takeover_tx.replace(takeover_tx);
            return Ok(SessionClaim {
                session_id: session_id.to_owned(),
                resumed: true,
                replay,
                previous_transport,
                lease_generation: state.lease_generation,
                last_sequence: state.journal.last_sequence(),
            });
        }

        self.make_room()?;
        let state = SessionState {
            lease_generation: 1,
            takeover_tx: Some(takeover_tx),
            ..SessionState::default()
        };
        self.sessions.insert(fresh_session_id.clone(), state);
        Ok(SessionClaim {
            session_id: fresh_session_id,
            resumed: false,
            replay: Vec::new(),
            previous_transport: None,
            lease_generation: 1,
            last_sequence: 0,
        })
    }

    fn prune(&mut self) {
        for state in self.sessions.values_mut() {
            state.journal.prune(self.max_entries, self.max_age);
        }
        self.sessions.retain(|_, state| {
            state.takeover_tx.is_some() || state.last_activity.elapsed() < self.max_age
        });
    }

    fn make_room(&mut self) -> Result<(), ()> {
        while self.sessions.len() >= self.max_sessions {
            let Some(oldest) = self
                .sessions
                .iter()
                .filter(|(_, state)| state.takeover_tx.is_none())
                .min_by_key(|(_, state)| state.last_activity)
                .map(|(id, _)| id.clone())
            else {
                return Err(());
            };
            self.sessions.remove(&oldest);
        }
        Ok(())
    }

    /// Record a wire frame for the session as it leaves the writer.
    pub fn record_frame(
        &mut self,
        session_id: &str,
        generation: u64,
        sequence: u64,
        message: &str,
    ) {
        if generation == 0 {
            self.sessions.entry(session_id.to_owned()).or_default();
        }
        if let Some(state) = self.sessions.get_mut(session_id)
            && state.lease_generation == generation
        {
            state.last_activity = Instant::now();
            state
                .journal
                .record(sequence, message, self.max_entries, self.max_age);
        }
    }

    /// Whether a writer still owns the live lease for this session.
    pub fn is_current_lease(&self, session_id: &str, generation: u64) -> bool {
        self.sessions
            .get(session_id)
            .is_some_and(|state| state.lease_generation == generation)
    }

    /// Release a transport lease without clearing a newer takeover owner.
    pub fn release_lease(&mut self, session_id: &str, generation: u64) {
        if let Some(state) = self.sessions.get_mut(session_id)
            && state.lease_generation == generation
        {
            state.takeover_tx = None;
            state.last_activity = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(max_entries: usize, max_age: Duration) -> ResumeState {
        ResumeState::new(max_entries, 64, max_age)
    }

    #[test]
    fn replay_requires_exact_cursor_plus_one_contiguity() {
        let mut state = store(16, Duration::from_secs(30));
        for sequence in 1..=5 {
            state.record_frame("s", 0, sequence, &format!("frame-{sequence}"));
        }

        // Cursor at the edge: nothing to replay.
        assert_eq!(
            state.sessions["s"]
                .journal
                .replay_from(5, Duration::from_secs(30))
                .unwrap()
                .len(),
            0
        );
        // Invalid cursor beyond anything sent.
        assert!(
            state.sessions["s"]
                .journal
                .replay_from(6, Duration::from_secs(30))
                .is_none()
        );
        // Mid-stream cursor: the tail only.
        let run = state.sessions["s"]
            .journal
            .replay_from(2, Duration::from_secs(30))
            .unwrap();
        assert_eq!(
            run.iter().map(|entry| entry.sequence).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[test]
    fn evicted_head_breaks_contiguity() {
        let mut state = store(2, Duration::from_secs(30));
        for sequence in 1..=6 {
            state.record_frame("s", 0, sequence, &format!("frame-{sequence}"));
        }
        // Only sequences 5 and 6 remain; cursor 0 cannot be filled.
        assert!(
            state.sessions["s"]
                .journal
                .replay_from(0, Duration::from_secs(30))
                .is_none()
        );
        // Cursor at the retained edge still fills.
        let run = state.sessions["s"]
            .journal
            .replay_from(4, Duration::from_secs(30))
            .unwrap();
        assert_eq!(
            run.iter().map(|entry| entry.sequence).collect::<Vec<_>>(),
            vec![5, 6]
        );
    }

    #[test]
    fn aged_entries_block_replay() {
        let mut state = store(16, Duration::from_millis(20));
        state.record_frame("s", 0, 1, "frame-1");
        std::thread::sleep(Duration::from_millis(40));
        assert!(
            state.sessions["s"]
                .journal
                .replay_from(0, Duration::from_millis(20))
                .is_none()
        );
    }

    #[test]
    fn claim_validates_instance_session_cursor_and_capacity() {
        let mut state = store(16, Duration::from_secs(30));
        for sequence in 1..=4 {
            state.record_frame("s", 0, sequence, &format!("frame-{sequence}"));
        }

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let resumed = state
            .claim(Some("i"), "i", Some("s"), Some(2), "fresh".into(), tx, 16)
            .unwrap();
        assert!(resumed.resumed);
        assert_eq!(resumed.session_id, "s");
        assert_eq!(resumed.replay.len(), 2);

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let fallback = state
            .claim(Some("i"), "i", Some("s"), Some(0), "fresh".into(), tx, 2)
            .unwrap();
        assert!(
            !fallback.resumed,
            "an oversized replay must fall back before welcome"
        );
        assert_eq!(fallback.session_id, "fresh");
    }

    #[test]
    fn lease_takeover_returns_the_previous_wake_channel() {
        let mut state = store(16, Duration::from_secs(30));
        let (tx_a, _rx_a) = tokio::sync::oneshot::channel::<()>();
        let first = state
            .claim(None, "i", None, None, "s".into(), tx_a, 16)
            .unwrap();
        assert!(first.previous_transport.is_none());
        assert_eq!(first.last_sequence, 0);

        let (tx_b, _rx_b) = tokio::sync::oneshot::channel::<()>();
        let second = state
            .claim(Some("i"), "i", Some("s"), Some(0), "fresh".into(), tx_b, 16)
            .unwrap();
        assert_eq!(second.lease_generation, first.lease_generation + 1);
        // The previous holder's wake channel is still live and addressable.
        second
            .previous_transport
            .expect("previous lease is returned")
            .send(())
            .expect("old transport still holds its end");
    }

    #[test]
    fn stale_writer_cannot_advance_successor_journal() {
        let mut state = store(16, Duration::from_secs(30));
        let (tx_a, _rx_a) = tokio::sync::oneshot::channel();
        let first = state
            .claim(None, "i", None, None, "s".into(), tx_a, 16)
            .unwrap();
        state.record_frame("s", first.lease_generation, 1, "frame-1");

        let (tx_b, _rx_b) = tokio::sync::oneshot::channel();
        let second = state
            .claim(Some("i"), "i", Some("s"), Some(0), "fresh".into(), tx_b, 16)
            .unwrap();
        state.record_frame("s", first.lease_generation, 2, "stale-frame-2");

        assert_eq!(second.last_sequence, 1);
        assert_eq!(state.sessions["s"].journal.last_sequence(), 1);
    }

    #[test]
    fn session_cap_evicts_only_inactive_leases() {
        let mut state = ResumeState::new(4, 2, Duration::from_secs(30));
        let (tx_a, _rx_a) = tokio::sync::oneshot::channel();
        let a = state
            .claim(None, "i", None, None, "a".into(), tx_a, 4)
            .unwrap();
        state.release_lease("a", a.lease_generation);

        let (tx_b, _rx_b) = tokio::sync::oneshot::channel();
        state
            .claim(None, "i", None, None, "b".into(), tx_b, 4)
            .unwrap();
        let (tx_c, _rx_c) = tokio::sync::oneshot::channel();
        state
            .claim(None, "i", None, None, "c".into(), tx_c, 4)
            .unwrap();

        assert!(!state.sessions.contains_key("a"));
        assert!(state.sessions.contains_key("b"));
        assert!(state.sessions.contains_key("c"));
    }
}
