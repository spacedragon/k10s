//! Controller-UID owner traversal for the real adapter.
//!
//! Resolves every transitive controller-owned descendant of one object by
//! matching child `ownerReferences` against the traversal frontier's UIDs —
//! the Deployment → ReplicaSet → Pod chain. Ownership is never resolved by
//! labels or reused names: a controller flag plus an exact UID match is the
//! only evidence accepted.
//!
//! Candidate descendants come from one list sweep over the context's
//! namespaced discovery catalog inside the object's namespace; types whose
//! lists are unavailable (missing RBAC, older servers) contribute nothing.

use kube::Client;

use crate::port::{ApiResourceDescriptor, Gvk, RelatedData, RelatedRecordGroup, ResourceRef};
use crate::runtime::supervisor::WatchRow;

/// Sweep one namespace's catalog types into normalized candidate rows.
///
/// Per-type failures are swallowed on purpose: a single forbidden or absent
/// type must not erase the relations that did resolve.
pub(crate) async fn sweep_candidates(
    client: &Client,
    context: &str,
    catalog_types: &[ApiResourceDescriptor],
    namespace: Option<&str>,
) -> Vec<WatchRow> {
    let mut rows = Vec::new();
    for descriptor in catalog_types {
        if !descriptor.namespaced || is_event_kind(&descriptor.gvk) {
            continue;
        }
        if let Ok(read) = super::read::list_resource(
            client,
            context,
            &descriptor.gvk,
            &descriptor.plural,
            true,
            namespace,
        )
        .await
        {
            rows.extend(read.rows);
        }
    }
    rows
}

/// Event kinds never appear in related tabs; they surface on the Events tab.
fn is_event_kind(gvk: &Gvk) -> bool {
    gvk.kind == "Event" && (gvk.group.is_empty() || gvk.group == "events.k8s.io")
}

/// Assemble the related-data answer for one reference from candidates,
/// stamping every resolved row with the caller's monotonic revision.
///
/// The traversal mirrors the fake adapter exactly so both adapters emit
/// identical protocol shapes.
pub(crate) fn related_data(
    reference: ResourceRef,
    candidates: &[WatchRow],
    revision: u64,
) -> RelatedData {
    let mut groups: Vec<RelatedRecordGroup> = Vec::new();
    let mut visited: Vec<String> = vec![reference.uid.clone()];
    loop {
        let frontier: Vec<String> = visited.clone();
        let mut discovered = false;
        for row in candidates {
            let candidate = &row.reference;
            if candidate.context != reference.context || visited.contains(&candidate.uid) {
                continue;
            }
            // Only a controller owner reference whose UID is already in the
            // frontier proves ownership.
            let owned = row
                .owner_references
                .iter()
                .any(|owner| owner.controller && frontier.contains(&owner.uid));
            if !owned {
                continue;
            }
            visited.push(candidate.uid.clone());
            discovered = true;
            match groups.iter_mut().find(|group| group.gvk == candidate.gvk) {
                Some(group) => group
                    .records
                    .push(crate::runtime::record_from_row(row, revision)),
                None => groups.push(RelatedRecordGroup {
                    gvk: candidate.gvk.clone(),
                    records: vec![crate::runtime::record_from_row(row, revision)],
                }),
            }
        }
        if !discovered {
            break;
        }
    }
    for group in &mut groups {
        group
            .records
            .sort_by(|left, right| left.reference.cmp(&right.reference));
    }
    groups.sort_by(|left, right| left.gvk.cmp(&right.gvk));
    RelatedData { reference, groups }
}
