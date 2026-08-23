//! On-demand list reads for the real adapter.
//!
//! Lists are read fresh from the cluster and normalized into view-model
//! rows; only the rows are stored (in the watch caches), never raw objects,
//! YAML, or details — those are fetched on demand by later tasks. Kube-rs
//! types stay confined to this module tree: normalized data or sanitized
//! errors cross the seam.

use kube::api::ListParams;

use crate::port::{BackendError, Gvk};
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
        .map_err(|error| BackendError::Internal(sanitize_list_error(error)))?;
    let rows = listed
        .items
        .iter()
        .map(|object| normalize_row(context, gvk, namespaced, namespace, object))
        .collect();
    Ok(ListRead { rows })
}
