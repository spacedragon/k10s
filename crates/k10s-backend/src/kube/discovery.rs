//! Discovery boundary for real clusters.
//!
//! Runs kube-rs API discovery against one context client and normalizes the
//! result into backend-owned catalog types. The kube-rs discovery machinery is
//! confined to this module: only normalized [`ResourceTypesData`] or typed,
//! sanitized [`BackendError`]s cross the seam — never raw API server text.

use std::time::Duration;

use kube::discovery::{Discovery, Scope, verbs};

use crate::port::{ApiResourceDescriptor, BackendError, Gvk, ResourceTypesData};

/// Documented freshness of a cached discovery catalog: entries older than this
/// age are re-discovered through the same query path that first filled them.
pub const DISCOVERY_TTL: Duration = Duration::from_secs(300);

/// Bounded size of the per-context discovery cache: at most this many context
/// catalogs stay in memory; overflow evicts the oldest entry (see `KubeAdapter`).
pub const MAX_CACHED_CONTEXTS: usize = 8;

/// Run full API discovery for one context client and normalize it into a
/// sorted resource catalog.
///
/// Fails closed: every cluster or transport failure maps to a typed
/// [`BackendError`] instead of serving an empty or fabricated catalog.
pub(crate) async fn discover_resource_types(
    client: &kube::Client,
    context: &str,
) -> Result<ResourceTypesData, BackendError> {
    let discovery = Discovery::new(client.clone())
        .run()
        .await
        .map_err(|error| sanitize_discovery_error(context, error))?;

    let mut types = Vec::new();
    for group in discovery.groups().collect::<Vec<_>>() {
        // Cover every version the cluster advertises: built-ins and CRDs alike.
        for version in group.versions() {
            for (resource, capabilities) in group.versioned_resources(version) {
                // Only resources the list/watch path can actually open belong
                // in the picker: Kubernetes discovery also advertises
                // create-only submission types (Binding, TokenReview,
                // SubjectAccessReview), which must stay out of the catalog.
                if !capabilities.supports_operation(verbs::LIST) {
                    continue;
                }
                types.push(ApiResourceDescriptor {
                    gvk: Gvk::new(
                        resource.group.clone(),
                        resource.version.clone(),
                        resource.kind.clone(),
                    ),
                    plural: resource.plural.clone(),
                    namespaced: matches!(capabilities.scope, Scope::Namespaced),
                    // A recorded /scale subresource marks the type as scalable.
                    supports_scale: capabilities
                        .subresources
                        .iter()
                        .any(|(sub, _)| sub.plural == "scale"),
                    supports_watch: capabilities.supports_operation(verbs::WATCH),
                    supports_patch: capabilities.supports_operation(verbs::PATCH),
                    supports_delete: capabilities.supports_operation(verbs::DELETE),
                });
            }
        }
    }

    types.sort_by(|left, right| left.gvk.cmp(&right.gvk));
    Ok(ResourceTypesData {
        context: context.to_owned(),
        types,
    })
}

/// Map kube-rs discovery failures to bounded operator-facing details.
///
/// The API server's raw Status text is never echoed back — it may expose
/// internal cluster detail; only a sanitized category and HTTP code survive,
/// keeping the detail under 200 characters even for long context names.
fn sanitize_discovery_error(context: &str, error: kube::Error) -> BackendError {
    let detail = match error {
        // The API server answered but rejected discovery (401/403 and friends).
        kube::Error::Api(status) => format!(
            "resource discovery failed for context '{context}': api-server rejected the request with HTTP {}",
            status.code
        ),
        // Transport, TLS, parse, or unexpected client errors: keep no raw text.
        _ => {
            format!("resource discovery failed for context '{context}': kubernetes api unreachable")
        }
    };
    let detail = detail.chars().take(190).collect::<String>();
    BackendError::Internal(detail)
}
