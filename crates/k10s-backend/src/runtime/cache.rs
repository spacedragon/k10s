//! Normalized summary cache behind every supervised watch selection.
//!
//! The cache holds the last known normalized rows for one `(context, GVK,
//! scope)` selection. Replacement is atomic: a relist swaps the entire row
//! set inside one critical section, so concurrent readers only ever observe
//! the complete previous state or the complete new state — never a partially
//! applied list cut. While a relist is in flight the previous rows stay
//! readable and are flagged stale.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::port::{ResourceListData, ResourceRecord, ResourceRef};
use crate::runtime::supervisor::WatchRow;
use crate::watch::WatchSelector;

/// First monotonic backend revision handed out by the watch runtime.
pub const INITIAL_WATCH_REVISION: u64 = 1_000;

/// Monotonic backend revision allocator shared by every supervised watch.
///
/// Every published snapshot or delta takes exactly one revision from here,
/// so revisions never move backwards anywhere in the system regardless of
/// how many selections run concurrently. Clones share one counter.
#[derive(Debug, Default)]
pub struct RevisionCounter(Arc<AtomicU64>);

impl Clone for RevisionCounter {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl RevisionCounter {
    /// Create a counter starting at [`INITIAL_WATCH_REVISION`].
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(INITIAL_WATCH_REVISION)))
    }

    /// Allocate the next strictly increasing revision; the first allocation
    /// yields [`INITIAL_WATCH_REVISION`].
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// Stamp one normalized row into its published record form at `revision`.
///
/// Shared by cache replacement, live deltas, and on-demand list reads so
/// every published row carries exactly the same shape.
pub(crate) fn record_from_row(row: &WatchRow, revision: u64) -> ResourceRecord {
    ResourceRecord {
        reference: row.reference.clone(),
        revision,
        labels: row.labels.clone(),
        summary: row.summary.clone(),
        created_at: row.created_at.clone(),
        owner_references: row.owner_references.clone(),
        events: Vec::new(),
    }
}

/// The last known normalized rows of one supervised selection.
///
/// All mutation funnels through one interior mutex whose critical sections
/// never call back into observers, so every read sees whole states only.
#[derive(Debug)]
pub struct SummaryCache {
    selector: WatchSelector,
    rows: std::sync::Mutex<BTreeMap<ResourceRef, ResourceRecord>>,
    stale: AtomicBool,
}

impl SummaryCache {
    /// Create an empty cache for one selection.
    #[must_use]
    pub fn new_for(selector: WatchSelector) -> Self {
        Self {
            selector,
            rows: std::sync::Mutex::new(BTreeMap::new()),
            stale: AtomicBool::new(false),
        }
    }

    /// Whether the cached rows predate an in-flight relist.
    pub fn stale(&self) -> bool {
        self.stale.load(Ordering::SeqCst)
    }

    /// Flag the cache as predating an in-flight relist.
    pub fn mark_stale(&self) {
        self.stale.store(true, Ordering::SeqCst);
    }

    /// Sorted snapshot of the cached rows.
    ///
    /// Rows come back in stable reference order; readers can never observe
    /// an intermediate state because replacement swaps the whole map inside
    /// one lock acquisition.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ResourceRecord> {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// Number of cached rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether the cache holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Atomically replace the whole row set with one list cut.
    ///
    /// Returns the backend revision covering the snapshot plus its records
    /// as published (revisions stamped, sorted by stable identity).
    pub fn replace(
        &self,
        rows: Vec<WatchRow>,
        revisions: &RevisionCounter,
    ) -> (u64, Vec<ResourceRecord>) {
        let revision = revisions.next();
        let stamped: BTreeMap<ResourceRef, ResourceRecord> = rows
            .into_iter()
            .map(|row| {
                let record = record_from_row(&row, revision);
                (record.reference.clone(), record)
            })
            .collect();
        let published = stamped.values().cloned().collect();
        // One lock section swaps the entire state: readers observe either
        // the complete previous cut or the complete new one.
        *self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = stamped;
        self.stale.store(false, Ordering::SeqCst);
        (revision, published)
    }

    /// Apply one live delta to the cache and return its broadcast event.
    pub fn apply_update(
        &self,
        update: crate::runtime::supervisor::WatchUpdate,
        revisions: &RevisionCounter,
    ) -> crate::port::BackendEvent {
        match update {
            crate::runtime::supervisor::WatchUpdate::Upsert(row) => {
                let revision = revisions.next();
                let record = record_from_row(&row, revision);
                self.rows
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(row.reference, record.clone());
                crate::port::BackendEvent::Changed(record)
            }
            crate::runtime::supervisor::WatchUpdate::Delete(reference) => {
                let revision = revisions.next();
                self.rows
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&reference);
                crate::port::BackendEvent::Gone {
                    reference,
                    revision,
                }
            }
        }
    }

    /// Rebuild the full snapshot payload from current cache contents; used
    /// when a late subscriber joins a warm selection so it also starts from
    /// a complete cut instead of mid-stream deltas.
    #[must_use]
    pub fn publish_current(&self, revisions: &RevisionCounter) -> ResourceListData {
        let revision = revisions.next();
        ResourceListData {
            context: self.selector.context.clone(),
            gvk: self.selector.gvk.clone(),
            namespace: self.selector.namespace.clone(),
            revision,
            rows: self.snapshot(),
            generated_at: now_rfc3339(),
        }
    }
}

/// Current UTC time formatted as RFC 3339 without external crates.
pub(crate) fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    rfc3339(secs)
}

/// Format unix seconds as an RFC 3339 UTC timestamp without external crates.
pub(crate) fn rfc3339(unix_secs: u64) -> String {
    let days = unix_secs / 86_400;
    let secs_of_day = unix_secs % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60
    )
}

/// Howard Hinnant's days-to-civil conversion for UTC dates after 1970.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::Gvk;
    use crate::runtime::supervisor::WatchUpdate;

    fn selector() -> WatchSelector {
        WatchSelector {
            context: "dev".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: Some("default".into()),
        }
    }

    fn row(name: &str, summary: &str) -> WatchRow {
        WatchRow {
            reference: ResourceRef {
                context: "dev".into(),
                gvk: Gvk::core("v1", "Pod"),
                namespace: Some("default".into()),
                name: name.into(),
                uid: format!("uid-{name}"),
            },
            labels: Default::default(),
            summary: summary.to_owned(),
            created_at: "2026-08-21T00:00:00Z".into(),
            owner_references: Vec::new(),
        }
    }

    #[test]
    fn replacement_is_atomic_and_sorted() {
        let cache = SummaryCache::new_for(selector());
        let revisions = RevisionCounter::new();

        let (first_revision, first_rows) = cache.replace(
            vec![row("web", "Running"), row("api", "Pending")],
            &revisions,
        );
        assert_eq!(first_revision, INITIAL_WATCH_REVISION);
        let names: Vec<_> = first_rows
            .iter()
            .map(|record| record.reference.name.clone())
            .collect();
        assert_eq!(names, ["api", "web"], "published rows arrive sorted");
        assert!(first_rows.iter().all(|r| r.revision == first_revision));

        let (second_revision, second_rows) = cache.replace(vec![row("web", "Running")], &revisions);
        assert!(second_revision > first_revision);
        assert_eq!(second_rows.len(), 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn staleness_tracks_relist_lifecycle() {
        let cache = SummaryCache::new_for(selector());
        let revisions = RevisionCounter::new();
        assert!(!cache.stale());
        cache.mark_stale();
        assert!(cache.stale(), "the relist flags the old rows stale");
        cache.replace(vec![row("web", "Running")], &revisions);
        assert!(!cache.stale(), "a completed relist clears staleness");
    }

    #[test]
    fn deltas_apply_upserts_and_deletes_with_increasing_revisions() {
        let cache = SummaryCache::new_for(selector());
        let revisions = RevisionCounter::new();
        let (_, initial) = cache.replace(vec![row("web", "Running")], &revisions);

        let changed = cache.apply_update(
            WatchUpdate::Upsert(row("web", "CrashLoopBackOff")),
            &revisions,
        );
        match changed {
            crate::port::BackendEvent::Changed(record) => {
                assert!(record.revision > initial[0].revision);
                assert_eq!(cache.snapshot()[0].summary, "CrashLoopBackOff");
            }
            other => panic!("upsert must broadcast Changed, got {other:?}"),
        }

        let gone = cache.apply_update(
            WatchUpdate::Delete(cache.snapshot()[0].reference.clone()),
            &revisions,
        );
        match gone {
            crate::port::BackendEvent::Gone { revision, .. } => {
                assert!(revision > INITIAL_WATCH_REVISION + 1);
                assert!(cache.is_empty(), "delete empties the cache");
            }
            other => panic!("delete must broadcast Gone, got {other:?}"),
        }
    }

    #[test]
    fn late_joiner_snapshot_carries_the_full_current_state() {
        let cache = SummaryCache::new_for(selector());
        let revisions = RevisionCounter::new();
        cache.replace(
            vec![row("web", "Running"), row("api", "Pending")],
            &revisions,
        );
        let data = cache.publish_current(&revisions);
        assert_eq!(data.context, "dev");
        assert_eq!(data.rows.len(), 2);
        assert!(data.revision > INITIAL_WATCH_REVISION);
    }
}
