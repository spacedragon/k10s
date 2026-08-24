//! On-demand list/detail reads for the real adapter.
//!
//! Lists and details are read fresh from the cluster and normalized into
//! view-model rows; only the rows are stored (in the watch caches), never
//! raw objects. YAML, details, relations, and events are fetched on demand.
//! Kube-rs types stay confined to this module tree: normalized data or
//! sanitized errors cross the seam.

use kube::api::ListParams;

use crate::port::{BackendError, Gvk, ResourceRef};
use crate::runtime::supervisor::WatchRow;

use super::normalize::normalize_row;
use super::watch::{dynamic_api, sanitize_list_error};

/// One normalized LIST result for a selection.
pub(crate) struct ListRead {
    /// Rows in cluster order; callers sort before publishing.
    pub rows: Vec<WatchRow>,
}

/// Run one one-off LIST against the cluster and normalize its items.
///
/// The opaque Kubernetes resourceVersion is deliberately dropped here: a
/// query snapshot carries no resumable cut, only the current view models.
pub(crate) async fn list_resource(
    client: &kube::Client,
    context: &str,
    gvk: &Gvk,
    plural: &str,
    namespaced: bool,
    namespace: Option<&str>,
) -> Result<ListRead, BackendError> {
    let api = dynamic_api(
        client.clone(),
        gvk.clone(),
        plural.to_owned(),
        namespaced,
        namespace.map(str::to_owned),
    );
    let listed = api
        .list(&ListParams::default())
        .await
        .map_err(sanitize_read_list_error)?;
    let rows = listed
        .items
        .iter()
        .map(|object| normalize_row(context, gvk, namespaced, namespace, object))
        .collect();
    Ok(ListRead { rows })
}

/// One normalized GET result for an exact object identity.
pub(crate) struct DetailRead {
    /// The object's normalized view-model row.
    pub row: WatchRow,
    /// YAML rendered from the fetched object, bound to its UID and opaque
    /// resourceVersion so guarded edits can detect drift.
    pub manifest: String,
}

/// Run one one-off GET against the cluster and normalize the exact object.
///
/// Identity is enforced, never assumed: a 404 and an existing name carrying
/// a different UID (delete/recreate reuse) are both typed not-founds, so a
/// stale reference can never resolve to a recreated object.
pub(crate) async fn get_resource(
    client: &kube::Client,
    gvk: &Gvk,
    plural: &str,
    namespaced: bool,
    namespace: Option<&str>,
    reference: &ResourceRef,
) -> Result<DetailRead, BackendError> {
    let api = dynamic_api(
        client.clone(),
        gvk.clone(),
        plural.to_owned(),
        namespaced,
        namespace.map(str::to_owned),
    );
    let object = api.get(&reference.name).await.map_err(sanitize_get_error)?;
    let uid = kube::ResourceExt::uid(&object).unwrap_or_default();
    if uid != reference.uid {
        // A reused name with another UID is not this object.
        return Err(BackendError::NotFound);
    }
    let row = normalize_row(&reference.context, gvk, namespaced, namespace, &object);
    let manifest = render_manifest(&object);
    Ok(DetailRead { row, manifest })
}

/// Render the fetched object's authoritative read-only YAML manifest.
///
/// Serialization keeps `metadata.uid` and `metadata.resourceVersion` in
/// place, binding the text to exactly the object that was read; any shape
/// mismatch degrades to an empty manifest rather than a wrong one.
fn render_manifest(object: &kube::core::DynamicObject) -> String {
    serde_yaml::to_string(object).unwrap_or_default()
}

/// Sanitized GET failure detail: 404s become typed not-founds and raw
/// Kubernetes Status text never crosses the seam.
pub(crate) fn sanitize_get_error(error: kube::Error) -> BackendError {
    match error {
        kube::Error::Api(status) if status.code == 404 => BackendError::NotFound,
        kube::Error::Api(status) if status.code == 403 => BackendError::Forbidden,
        kube::Error::Api(status) => BackendError::Internal(format!(
            "resource get rejected by the api server with HTTP {}",
            status.code
        )),
        _ => BackendError::Internal("kubernetes api unreachable for resource get".to_owned()),
    }
}

/// Preserve authorization denials as the typed protocol error while keeping
/// all Kubernetes Status messages out of the normalized boundary.
fn sanitize_read_list_error(error: kube::Error) -> BackendError {
    match error {
        kube::Error::Api(status) if status.code == 403 => BackendError::Forbidden,
        other => BackendError::Internal(sanitize_list_error(other)),
    }
}
