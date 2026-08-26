//! Discovery boundary for real clusters.
//!
//! Runs kube-rs API discovery against one context client and normalizes the
//! result into backend-owned catalog types. The kube-rs discovery machinery is
//! confined to this module: only normalized [`ResourceTypesData`] or typed,
//! sanitized [`BackendError`]s cross the seam — never raw API server text.

use std::time::{Duration, Instant};

use kube::discovery::{Discovery, Scope, verbs};

use crate::port::{ApiResourceDescriptor, BackendError, Gvk, ResourceTypesData};

/// Documented freshness of a cached discovery catalog: entries older than this
/// age are re-discovered through the same query path that first filled them.
pub const DISCOVERY_TTL: Duration = Duration::from_secs(300);

/// Bounded size of the per-context discovery cache: at most this many context
/// catalogs stay in memory; overflow evicts the oldest entry (see `KubeAdapter`).
pub const MAX_CACHED_CONTEXTS: usize = 8;

/// Prefer two-request aggregated API discovery for one context client, fall
/// back to legacy discovery only for known compatibility failures, and
/// normalize the result into a sorted resource catalog.
///
/// Fails closed: every cluster or transport failure maps to a typed
/// [`BackendError`] instead of serving an empty or fabricated catalog.
pub(crate) async fn discover_resource_types(
    client: &kube::Client,
    context: &str,
) -> Result<ResourceTypesData, BackendError> {
    let (discovery, mode) = execute_discovery(client, context).await?;
    let data = normalize_discovery(&discovery, context);
    tracing::debug!(
        context,
        mode,
        outcome = "normalized",
        "kubernetes discovery completed"
    );
    Ok(data)
}

async fn execute_discovery(
    client: &kube::Client,
    context: &str,
) -> Result<(Discovery, &'static str), BackendError> {
    let started = Instant::now();
    match Discovery::new(client.clone()).run_aggregated().await {
        Ok(discovery) if has_usable_core(&discovery) => {
            trace_attempt(context, "aggregated", started, "usable");
            Ok((discovery, "aggregated"))
        }
        Ok(_) => {
            trace_attempt(context, "aggregated", started, "compatibility_empty_core");
            execute_legacy_discovery(client, context).await
        }
        Err(error) if aggregated_error_allows_fallback(&error) => {
            trace_attempt(context, "aggregated", started, "compatibility_error");
            execute_legacy_discovery(client, context).await
        }
        Err(error) => {
            trace_attempt(context, "aggregated", started, "failed_closed");
            Err(sanitize_discovery_error(context, error))
        }
    }
}

async fn execute_legacy_discovery(
    client: &kube::Client,
    context: &str,
) -> Result<(Discovery, &'static str), BackendError> {
    let started = Instant::now();
    match Discovery::new(client.clone()).run().await {
        Ok(discovery) => {
            trace_attempt(context, "legacy", started, "usable");
            Ok((discovery, "legacy"))
        }
        Err(error) => {
            trace_attempt(context, "legacy", started, "failed_closed");
            Err(sanitize_discovery_error(context, error))
        }
    }
}

fn trace_attempt(context: &str, mode: &str, started: Instant, outcome: &str) {
    tracing::debug!(
        context,
        mode,
        elapsed_ms = started.elapsed().as_millis() as u64,
        outcome,
        "kubernetes discovery attempt completed"
    );
}

fn has_usable_core(discovery: &Discovery) -> bool {
    discovery.get("").is_some_and(|core| {
        core.versions().any(|version| {
            !version.is_empty()
                && core
                    .versioned_resources(version)
                    .into_iter()
                    .any(|(resource, capabilities)| {
                        !resource.kind.is_empty()
                            && !resource.plural.is_empty()
                            && capabilities.supports_operation(verbs::LIST)
                    })
        })
    })
}

fn aggregated_error_allows_fallback(error: &kube::Error) -> bool {
    match error {
        kube::Error::Api(status) => matches!(status.code, 404 | 406 | 415),
        kube::Error::SerdeError(_) | kube::Error::Discovery(_) => true,
        _ => false,
    }
}

fn normalize_discovery(discovery: &Discovery, context: &str) -> ResourceTypesData {
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
                    supports_create: capabilities.supports_operation(verbs::CREATE),
                    supports_delete: capabilities.supports_operation(verbs::DELETE),
                });
            }
        }
    }

    types.sort_by(|left, right| left.gvk.cmp(&right.gvk));
    ResourceTypesData {
        context: context.to_owned(),
        types,
    }
}

/// Map kube-rs discovery failures to bounded operator-facing details.
///
/// The API server's raw Status text is never echoed back — it may expose
/// internal cluster detail; only a sanitized category and HTTP code survive,
/// keeping the detail under 200 characters even for long context names.
fn sanitize_discovery_error(context: &str, error: kube::Error) -> BackendError {
    if let Some(unavailable) = super::auth::context_unavailable(&error) {
        return unavailable;
    }
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
