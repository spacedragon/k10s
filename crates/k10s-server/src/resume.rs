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
#[derive(Debug, Default)]
pub struct SessionState {
    pub(crate) journal: SessionJournal,
    /// Bumped on every (re-)lease; stale transports must not clobber successors.
    pub(crate) lease_generation: u64,
    /// Wake channel for the transport currently holding this session.
    pub(crate) takeover_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Server-wide registry of resumable sessions and their live leases.
#[derive(Debug)]
pub struct ResumeState {
    pub(crate) sessions: BTreeMap<String, SessionState>,
    pub(crate) max_entries: usize,
    pub(crate) max_age: Duration,
}

/// Shared handle to the resume state (one per control server).
pub type ResumeStore = std::sync::Arc<std::sync::Mutex<ResumeState>>;

impl ResumeState {
    /// Create a store with the configured replay budgets.
    #[must_use]
    pub fn new(max_entries: usize, max_age: Duration) -> Self {
        Self {
            sessions: BTreeMap::new(),
            max_entries,
            max_age,
        }
    }

    /// Attempt a resume claim from an authenticated `Hello`.
    ///
    /// Returns the claimed session ID and its contiguous replay run when every
    /// field is present, the server instance matches, the session is known,
    /// and the cursor can be filled within budget. Otherwise `Err(())`: the
    /// caller must start a fresh session instead.
    pub fn attempt_resume(
        &mut self,
        hello_server_instance: Option<&str>,
        server_instance_id: &str,
        claimed_session: Option<&str>,
        last_acked_sequence: Option<u64>,
    ) -> Result<(String, Vec<JournalEntry>), ()> {
        let (Some(instance), Some(session_id), Some(cursor)) =
            (hello_server_instance, claimed_session, last_acked_sequence)
        else {
            return Err(());
        };
        if instance != server_instance_id {
            return Err(());
        }
        match self
            .sessions
            .get_mut(session_id)
            .and_then(|session| session.journal.replay_from(cursor, self.max_age))
        {
            Some(run) => Ok((session_id.to_owned(), run)),
            None => Err(()),
        }
    }

    /// Atomically ensure `session_id` exists and hand its lease to this
    /// transport. Returns `(previous_transport_wake_channel, our_generation,
    /// current_last_sequence)`.
    pub fn acquire_lease(
        &mut self,
        session_id: &str,
        takeover_tx: tokio::sync::oneshot::Sender<()>,
    ) -> (Option<tokio::sync::oneshot::Sender<()>>, u64, u64) {
        let state = self.sessions.entry(session_id.to_owned()).or_default();
        state.lease_generation += 1;
        let previous = state.takeover_tx.replace(takeover_tx);
        (
            previous,
            state.lease_generation,
            state.journal.last_sequence(),
        )
    }

    /// Record a wire frame for the session as it leaves the writer.
    pub fn record_frame(&mut self, session_id: &str, sequence: u64, message: &str) {
        self.sessions
            .entry(session_id.to_owned())
            .or_default()
            .journal
            .record(sequence, message, self.max_entries, self.max_age);
    }

    /// Release a transport lease without clearing a newer takeover owner.
    pub fn release_lease(&mut self, session_id: &str, generation: u64) {
        if let Some(state) = self.sessions.get_mut(session_id)
            && state.lease_generation == generation
        {
            state.takeover_tx = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(max_entries: usize, max_age: Duration) -> ResumeState {
        ResumeState::new(max_entries, max_age)
    }

    #[test]
    fn replay_requires_exact_cursor_plus_one_contiguity() {
        let mut state = store(16, Duration::from_secs(30));
        for sequence in 1..=5 {
            state.record_frame("s", sequence, &format!("frame-{sequence}"));
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
            state.record_frame("s", sequence, &format!("frame-{sequence}"));
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
        state.record_frame("s", 1, "frame-1");
        std::thread::sleep(Duration::from_millis(40));
        assert!(
            state.sessions["s"]
                .journal
                .replay_from(0, Duration::from_millis(20))
                .is_none()
        );
    }

    #[test]
    fn attempt_resume_validates_instance_session_and_cursor() {
        let mut state = store(16, Duration::from_secs(30));
        for sequence in 1..=4 {
            state.record_frame("s", sequence, &format!("frame-{sequence}"));
        }

        // Missing fields: fresh.
        assert!(state.attempt_resume(None, "i", Some("s"), None).is_err());
        // Wrong instance: fresh even with a fillable cursor.
        assert!(
            state
                .attempt_resume(Some("other"), "i", Some("s"), Some(0))
                .is_err()
        );
        // Unknown session: fresh.
        assert!(
            state
                .attempt_resume(Some("i"), "i", Some("ghost"), Some(0))
                .is_err()
        );
        // Fillable claim succeeds with the replay run.
        let (session, run) = state
            .attempt_resume(Some("i"), "i", Some("s"), Some(2))
            .unwrap();
        assert_eq!(session, "s");
        assert_eq!(run.len(), 2);

        let mut wrong_instance = ResumeState::new(16, Duration::from_secs(30));
        for sequence in 1..=4 {
            wrong_instance.record_frame("s", sequence, &format!("frame-{sequence}"));
        }
        // Cursor beyond what was sent: fresh.
        assert!(
            wrong_instance
                .attempt_resume(Some("i"), "i", Some("s"), Some(99))
                .is_err()
        );
    }

    #[test]
    fn lease_takeover_returns_the_previous_wake_channel() {
        let mut state = store(16, Duration::from_secs(30));
        let (tx_a, _rx_a) = tokio::sync::oneshot::channel::<()>();
        let (prev, gen_a, last) = state.acquire_lease("s", tx_a);
        assert!(prev.is_none());
        assert_eq!(last, 0);

        let (tx_b, _rx_b) = tokio::sync::oneshot::channel::<()>();
        let (prev, gen_b, _) = state.acquire_lease("s", tx_b);
        assert_eq!(gen_b, gen_a + 1);
        // The previous holder's wake channel is still live and addressable.
        prev.expect("previous lease is returned")
            .send(())
            .expect("old transport still holds its end");
    }
}
