//! On-demand event reads for the real adapter.
//!
//! Kubernetes serves events through two parallel APIs — core/v1 `Event`
//! (involvedObject/message/lastTimestamp) and events.k8s.io/v1 `Event`
//! (regarding/note/series.lastObservedTime). Both normalize into the same
//! backend [`RecordEvent`] projection, merge newest-first, and collapse to
//! one row per persisted Event: the endpoints mirror one store, so rows are
//! deduplicated by the Event object's own metadata UID. Events are
//! matched by exact object UID, never by reused names or labels; unreadable
//! or unavailable event APIs contribute nothing rather than failing the
//! detail read they decorate.

use std::collections::HashSet;

use kube::api::{Api, ListParams};
use kube::core::DynamicObject;

use crate::port::{BackendError, Gvk, RecordEvent, ResourceRef};

use super::watch::dynamic_api;

/// Group/version of the core Event API.
const CORE_EVENTS: (&str, &str) = ("", "v1");
/// Group/version of the dedicated events.k8s.io API.
const GROUPED_EVENTS: (&str, &str) = ("events.k8s.io", "v1");

/// Collect the normalized, newest-first events observed for one exact
/// object identity.
pub(crate) async fn events_for(
    client: &kube::Client,
    reference: &ResourceRef,
    namespaced: bool,
) -> Result<Vec<RecordEvent>, BackendError> {
    let mut events = Vec::new();
    for (group, version) in [CORE_EVENTS, GROUPED_EVENTS] {
        let gvk = Gvk::new(group, version, "Event");
        // Cluster-scoped targets read cluster-wide lists; namespaced ones
        // stay inside their namespace.
        let api = if namespaced {
            dynamic_api(
                client.clone(),
                gvk,
                "events".to_owned(),
                true,
                reference.namespace.clone(),
            )
        } else {
            dynamic_api(client.clone(), gvk, "events".to_owned(), false, None)
        };
        // An unavailable variant (missing RBAC, older server) contributes
        // nothing instead of failing the detail it decorates.
        if let Ok(items) = list_events(&api).await {
            events.extend(items);
        }
    }
    // The two endpoints mirror one persisted Event store: the same Event
    // object arrives through both, so its own metadata UID keeps it to
    // exactly one row.
    let mut seen = HashSet::new();
    events.retain(|event| event.object_uid.is_empty() || seen.insert(event.object_uid.clone()));
    // Only events regarding exactly this object (UID equality).
    events.retain(|event| event_uid_matches(&event.message_meta_uid, reference));
    // Newest first; ties break deterministically by reason then message.
    events.sort_by(|left, right| {
        right
            .record
            .last_seen
            .cmp(&left.record.last_seen)
            .then_with(|| left.record.reason.cmp(&right.record.reason))
            .then_with(|| left.record.message.cmp(&right.record.message))
    });
    Ok(events.into_iter().map(|event| event.record).collect())
}

async fn list_events(api: &Api<DynamicObject>) -> Result<Vec<RawEvent>, BackendError> {
    let listed = api
        .list(&ListParams::default())
        .await
        .map_err(sanitize_list_error)?;
    Ok(listed.items.iter().filter_map(normalize_event).collect())
}

/// Sanitized event-list failure detail; raw Kubernetes Status text never
/// crosses the seam.
fn sanitize_list_error(error: kube::Error) -> BackendError {
    if let Some(unavailable) = super::auth::context_unavailable(&error) {
        return unavailable;
    }
    match error {
        kube::Error::Api(status) => BackendError::Internal(format!(
            "event list rejected by the api server with HTTP {}",
            status.code
        )),
        _ => BackendError::Internal("kubernetes api unreachable for event list".to_owned()),
    }
}

/// One normalized event before UID filtering and ordering.
struct RawEvent {
    record: RecordEvent,
    /// UID of the object the event regards.
    message_meta_uid: String,
    /// UID of the persisted Event itself, shared by both API views.
    object_uid: String,
}

fn event_uid_matches(uid: &str, reference: &ResourceRef) -> bool {
    !uid.is_empty() && uid == reference.uid
}

/// Normalize either Event API variant into the shared projection.
///
/// Returns `None` for objects that are not recognizable events of either
/// shape; nothing is guessed from partial data.
fn normalize_event(object: &DynamicObject) -> Option<RawEvent> {
    let value = serde_json::to_value(object).ok()?;
    let metadata = value.get("metadata")?;
    let involved = value
        .get("involvedObject")
        .or_else(|| value.get("regarding"))?;

    let reason = value
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let message = value
        .get("message")
        .or_else(|| value.get("note"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();

    // The dedicated API counts repeats inside `series`; core carries a flat
    // `count`. Absent counts mean the event happened once.
    let count = value
        .get("series")
        .and_then(|series| series.get("count"))
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            value
                .get("deprecatedCount")
                .and_then(serde_json::Value::as_i64)
        })
        .or_else(|| value.get("count").and_then(serde_json::Value::as_i64))
        .unwrap_or(1)
        .clamp(1, u32::MAX as i64) as u32;

    // Newest observable moment: series observation beats lastTimestamp
    // beats eventTime beats firstTimestamp/deprecated timestamp.
    let last_seen = [
        value
            .get("series")
            .and_then(|series| series.get("lastObservedTime"))
            .and_then(as_time_text),
        value.get("lastTimestamp").and_then(as_time_text),
        value.get("eventTime").and_then(as_time_text),
        value.get("firstTimestamp").and_then(as_time_text),
        value.get("deprecatedLastTimestamp").and_then(as_time_text),
    ]
    .into_iter()
    .flatten()
    .next()
    .unwrap_or_default();

    if reason.is_empty() && message.is_empty() {
        return None;
    }

    Some(RawEvent {
        record: RecordEvent {
            reason,
            message,
            count,
            last_seen,
        },
        message_meta_uid: involved
            .get("uid")
            .and_then(serde_json::Value::as_str)
            .or_else(|| metadata.get("uid").and_then(serde_json::Value::as_str))
            .unwrap_or_default()
            .to_owned(),
        object_uid: metadata
            .get("uid")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

/// Extract an RFC 3339 text out of a Kubernetes Time/MicroTime JSON node.
fn as_time_text(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}
