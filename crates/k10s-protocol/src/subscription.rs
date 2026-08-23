//! Subscription selectors and resource stream payloads.
//!
//! Selectors are the client-visible contract for opening a subscription; the
//! payloads below are the normalized frames streamed back on it. Snapshot
//! pages ride inside envelope `snapshotChunk.data`; deltas arrive as `event`
//! frames whose kind starts with `resource.`.

use serde::{Deserialize, Serialize};

use crate::metrics::InfrastructureWatchSpec;
use crate::resource::{BackendRevision, GroupVersionKind, ResourceIdentity, ResourceListRow};

/// Envelope event kind carrying a [`ResourceChanged`] delta.
pub const RESOURCE_EVENT_CHANGED: &str = "resource.changed";
/// Envelope event kind carrying a [`ResourceGone`] delta.
pub const RESOURCE_EVENT_GONE: &str = "resource.gone";

/// A typed subscription selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum SubscriptionSelector {
    /// Plan 1 server-lifecycle subscription.
    BootstrapStatus,
    /// Watch one resource type on one context.
    Resource(ResourceWatchSpec),
    /// Watch Overview, Nodes, Storage, and metrics for one context.
    Infrastructure(InfrastructureWatchSpec),
}

/// The resource type a watch follows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceWatchSpec {
    /// Context to watch.
    pub context: String,
    /// Type of resources to watch.
    pub gvk: GroupVersionKind,
    /// Optional namespace restriction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl ResourceWatchSpec {
    /// Return the selector that opens this watch.
    #[must_use]
    pub fn selector(&self) -> SubscriptionSelector {
        SubscriptionSelector::Resource(self.clone())
    }

    /// Whether a resource identity belongs to this watch, honoring the
    /// optional namespace restriction and cluster scope. Used by clients to
    /// keep deltas from other selectors out of a retained list view.
    #[must_use]
    pub fn matches(&self, identity: &ResourceIdentity) -> bool {
        self.context == identity.context
            && self.gvk == identity.gvk
            && self
                .namespace
                .as_ref()
                .is_none_or(|wanted| Some(wanted.as_str()) == identity.namespace.as_deref())
    }
}

/// One page of a chunked resource snapshot.
///
/// Pages are placed into `snapshotChunk.data` frames; a snapshot always
/// carries at least one page even when the list is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSnapshotPage {
    /// Backend revision covering the whole snapshot.
    pub revision: BackendRevision,
    /// Rows belonging to this page.
    pub rows: Vec<ResourceListRow>,
}

/// A resource upsert delta for one identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceChanged {
    /// Stable identity of the changed object.
    pub identity: ResourceIdentity,
    /// The full normalized row as of `revision`.
    pub row: ResourceListRow,
}

/// A resource-gone delta; the object no longer exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceGone {
    /// Stable identity of the removed object.
    pub identity: ResourceIdentity,
    /// Backend revision at which the removal happened.
    pub revision: BackendRevision,
}

/// Request payload listing the resource types available on one context.
///
/// Powers the searchable GVK picker of the custom-resources window; the
/// entries cover built-in workload kinds and every CRD-backed type the
/// adapter knows about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTypesRequest {
    /// Context whose types are listed.
    pub context: String,
}

/// One type selectable in the searchable GVK picker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTypeEntry {
    /// Group/version/kind of the selectable type.
    pub gvk: GroupVersionKind,
    /// Whether objects of this type live inside namespaces.
    pub namespaced: bool,
}

/// Response payload for the searchable GVK picker query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTypesResponse {
    /// Context the types were read from.
    pub context: String,
    /// Selectable types, sorted by group/version/kind.
    pub types: Vec<ResourceTypeEntry>,
}
