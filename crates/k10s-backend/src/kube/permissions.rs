//! Advisory RBAC capability projection for real clusters.
//!
//! Every probe runs as one SelfSubjectAccessReview create through the same
//! tower service seam as all other cluster traffic, then normalizes into a
//! backend-owned outcome. The projection is advisory metadata only: it hints
//! at what later operations are expected to be allowed and is never enforced
//! client-side — later operations keep hitting the API server and respecting
//! its authorization decisions.
//!
//! Fallback honesty: when the review itself cannot be evaluated (the user may
//! not create reviews, the endpoint is absent or unreachable, or the server
//! reports an evaluation error), outcomes degrade to explicit
//! [`PermissionOutcome::Unknown`] values instead of failing the whole flow or
//! fabricating allow/deny verdicts. Raw Kubernetes Status text never crosses
//! the seam.

use std::collections::HashSet;

use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
    SubjectAccessReviewStatus,
};
use kube::api::{Api, PostParams};

use crate::port::{BackendError, PermissionCheck, PermissionOutcome, PermissionProbe};

/// Hard bound on probes carried by one permission query, so one request can
/// never fan out into unbounded cluster traffic.
pub(crate) const MAX_PROBES: usize = 32;

/// Reject probe sets past the documented bound before any cluster traffic.
pub(crate) fn validate_probe_count(probes: &[PermissionProbe]) -> Result<(), BackendError> {
    if probes.len() > MAX_PROBES {
        return Err(BackendError::Conflict(format!(
            "permission review requests carry at most {MAX_PROBES} probes"
        )));
    }
    Ok(())
}

/// Project each distinct probe through one SelfSubjectAccessReview call.
///
/// Identical probes collapse into a single review (preserving first-seen
/// order): repeating them would burn identical calls without changing the
/// answer. The projection never fails the query — every probe yields an
/// answered check.
pub(crate) async fn project_capabilities(
    client: &kube::Client,
    probes: Vec<PermissionProbe>,
) -> Vec<PermissionCheck> {
    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let mut seen = HashSet::new();
    let mut checks = Vec::new();
    for probe in probes {
        if !seen.insert(probe.clone()) {
            continue;
        }
        let outcome = review_once(&api, &probe).await;
        checks.push(PermissionCheck {
            verb: probe.verb,
            resource: probe.resource,
            namespace: probe.namespace,
            outcome,
        });
    }
    checks
}

/// Run one SelfSubjectAccessReview for `probe`; anything short of an
/// answered review reads as [`PermissionOutcome::Unknown`].
async fn review_once(
    api: &Api<SelfSubjectAccessReview>,
    probe: &PermissionProbe,
) -> PermissionOutcome {
    let review = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            resource_attributes: Some(ResourceAttributes {
                namespace: probe.namespace.clone(),
                verb: Some(probe.verb.clone()),
                resource: Some(probe.resource.clone()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    match api.create(&PostParams::default(), &review).await {
        Ok(created) => verdict(created.status),
        Err(_) => PermissionOutcome::Unknown,
    }
}

/// Interpret one review status honestly: an evaluation error or a missing
/// status says nothing, and must never read as allowed or denied.
fn verdict(status: Option<SubjectAccessReviewStatus>) -> PermissionOutcome {
    match status {
        Some(status) if status.evaluation_error.is_some() => PermissionOutcome::Unknown,
        Some(status) if status.allowed => PermissionOutcome::Allowed,
        Some(_) => PermissionOutcome::Denied,
        None => PermissionOutcome::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_interpret_without_fabricating_verdicts() {
        let allowed = SubjectAccessReviewStatus {
            allowed: true,
            denied: None,
            evaluation_error: None,
            reason: Some("RBAC: allowed".into()),
        };
        assert_eq!(verdict(Some(allowed.clone())), PermissionOutcome::Allowed);

        let denied = SubjectAccessReviewStatus {
            allowed: false,
            denied: Some(true),
            evaluation_error: None,
            reason: None,
        };
        assert_eq!(verdict(Some(denied)), PermissionOutcome::Denied);

        let errored = SubjectAccessReviewStatus {
            allowed: false,
            denied: None,
            evaluation_error: Some("authorizer misconfigured".into()),
            reason: None,
        };
        assert_eq!(verdict(Some(errored)), PermissionOutcome::Unknown);
        assert_eq!(verdict(None), PermissionOutcome::Unknown);
    }

    #[test]
    fn oversized_probe_sets_are_typed_conflicts() {
        let probes: Vec<PermissionProbe> = (0..MAX_PROBES + 1)
            .map(|index| PermissionProbe {
                verb: "list".into(),
                resource: format!("kind{index}"),
                namespace: None,
            })
            .collect();
        assert!(validate_probe_count(&probes).is_err());
        assert!(validate_probe_count(&probes[..MAX_PROBES]).is_ok());
    }
}
