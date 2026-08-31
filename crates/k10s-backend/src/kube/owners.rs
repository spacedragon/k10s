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

/// Select the catalog types that can participate in an ownership traversal.
///
/// Deployments in the built-in apps group have a fixed descendant chain, so
/// their sweep is bounded to the same-version ReplicaSet and core/v1 Pod
/// descriptors already present in discovery. All other targets retain the
/// full catalog sweep.
pub(super) fn candidate_descriptors<'a>(
    reference: &ResourceRef,
    catalog: &'a [ApiResourceDescriptor],
) -> Vec<&'a ApiResourceDescriptor> {
    if reference.gvk.group != "apps" || reference.gvk.kind != "Deployment" {
        return catalog.iter().collect();
    }

    catalog
        .iter()
        .filter(|descriptor| {
            (descriptor.gvk.group.is_empty()
                && descriptor.gvk.version == "v1"
                && descriptor.gvk.kind == "Pod")
                || (descriptor.gvk.group == "apps"
                    && descriptor.gvk.version == reference.gvk.version
                    && descriptor.gvk.kind == "ReplicaSet")
        })
        .collect()
}

/// Sweep one namespace's catalog types into normalized candidate rows.
///
/// Per-type failures are swallowed on purpose: a single forbidden or absent
/// type must not erase the relations that did resolve.
pub(crate) async fn sweep_candidates(
    client: &Client,
    context: &str,
    catalog_types: &[&ApiResourceDescriptor],
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
    RelatedData {
        reference,
        revision,
        groups,
    }
}

#[cfg(test)]
mod tests {
    use super::candidate_descriptors;
    use crate::port::{ApiResourceDescriptor, Gvk, ResourceRef};

    fn descriptor(
        group: &str,
        version: &str,
        kind: &str,
        namespaced: bool,
    ) -> ApiResourceDescriptor {
        ApiResourceDescriptor {
            gvk: Gvk::new(group, version, kind),
            plural: format!("{}s", kind.to_ascii_lowercase()),
            namespaced,
            supports_scale: false,
            supports_watch: true,
            supports_patch: false,
            supports_create: false,
            supports_delete: false,
        }
    }

    fn catalog() -> Vec<ApiResourceDescriptor> {
        vec![
            descriptor("apps", "v1beta1", "ReplicaSet", true),
            descriptor("", "v1", "Service", true),
            descriptor("apps", "v1", "ReplicaSet", true),
            descriptor("custom.example", "v1", "Deployment", true),
            descriptor("", "v1", "Pod", true),
            descriptor("", "v1", "Event", true),
            descriptor("custom.example", "v2", "Deployment", true),
            descriptor("", "v1", "Node", false),
        ]
    }

    fn reference(group: &str, version: &str, kind: &str) -> ResourceRef {
        ResourceRef {
            context: "test".into(),
            gvk: Gvk::new(group, version, kind),
            namespace: Some("default".into()),
            name: "target".into(),
            uid: "target-uid".into(),
        }
    }

    #[test]
    fn apps_deployment_selects_matching_replica_set_and_core_pod_in_catalog_order() {
        let catalog = catalog();

        let selected = candidate_descriptors(&reference("apps", "v1", "Deployment"), &catalog);

        assert_eq!(
            selected
                .into_iter()
                .map(|entry| &entry.gvk)
                .collect::<Vec<_>>(),
            vec![&catalog[2].gvk, &catalog[4].gvk]
        );
    }

    #[test]
    fn other_targets_retain_the_full_catalog_candidate_set() {
        let catalog = catalog();
        let expected = catalog.iter().collect::<Vec<_>>();

        assert_eq!(
            candidate_descriptors(&reference("custom.example", "v1", "Deployment"), &catalog),
            expected
        );
        assert_eq!(
            candidate_descriptors(&reference("apps", "v1", "StatefulSet"), &catalog),
            expected
        );
    }

    #[test]
    fn apps_deployment_does_not_fall_back_to_another_replica_set_version() {
        let catalog = catalog()
            .into_iter()
            .filter(|entry| entry.gvk != Gvk::new("apps", "v1", "ReplicaSet"))
            .collect::<Vec<_>>();

        let selected = candidate_descriptors(&reference("apps", "v1", "Deployment"), &catalog);

        assert_eq!(
            selected
                .into_iter()
                .map(|entry| &entry.gvk)
                .collect::<Vec<_>>(),
            vec![&Gvk::core("v1", "Pod")]
        );
    }
}
