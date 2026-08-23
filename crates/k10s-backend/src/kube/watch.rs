//! Real-cluster list/watch source for the supervised runtime.
//!
//! Bridges one `(context, GVK, scope)` selection onto kube-rs dynamic API
//! calls: a LIST seeds the cache and its opaque resourceVersion resumes a
//! WATCH stream. Kube types stay confined to this module — only normalized
//! [`WatchRow`]s, [`WatchUpdate`]s, and sanitized error strings ever reach
//! the runtime, so no credential or raw API shape can leak downstream.
//!
//! Rows are normalized through the shared [`super::normalize`] normalizers,
//! so live deltas carry the same per-kind summaries as list reads.

use futures_util::StreamExt;
use kube::api::{Api, ListParams, WatchParams};
use kube::core::{DynamicObject, WatchEvent};
use kube::discovery::ApiResource;

use crate::port::Gvk;
use crate::runtime::supervisor::{ListedState, WatchRow, WatchSource, WatchUpdate};

use super::normalize::normalize_row;

/// A list/watch source bound to one selection on one cluster client.
#[derive(Clone)]
pub struct KubeWatchSource {
    client: kube::Client,
    context: String,
    gvk: Gvk,
    plural: String,
    namespaced: bool,
    namespace: Option<String>,
}

impl std::fmt::Debug for KubeWatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The client owns transport state; only the selection is reported.
        f.debug_struct("KubeWatchSource")
            .field("context", &self.context)
            .field("gvk", &self.gvk)
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl KubeWatchSource {
    /// Bind one selection to `client`. `plural` comes from the context's
    /// discovery catalog; `namespace` scopes the selection when set.
    ///
    /// A namespace on a cluster-scoped type is canonicalized away so the
    /// source's stream can never disagree with its own scope, whatever the
    /// caller passed in.
    #[must_use]
    pub fn new(
        client: kube::Client,
        context: impl Into<String>,
        gvk: Gvk,
        plural: String,
        namespaced: bool,
        namespace: Option<String>,
    ) -> Self {
        let namespace = if namespaced { namespace } else { None };
        Self {
            client,
            context: context.into(),
            gvk,
            plural,
            namespaced,
            namespace,
        }
    }

    fn api(&self) -> Api<DynamicObject> {
        dynamic_api(
            self.client.clone(),
            self.gvk.clone(),
            self.plural.clone(),
            self.namespaced,
            self.namespace.clone(),
        )
    }

    /// Normalize one dynamic object into a runtime row.
    fn normalize(&self, object: &DynamicObject) -> WatchRow {
        normalize_row(
            &self.context,
            &self.gvk,
            self.namespaced,
            self.namespace.as_deref(),
            object,
        )
    }
}

/// Bind one selection onto a kube-rs dynamic API handle, shared by the live
/// watch source and the on-demand read path. A namespace on a cluster-scoped
/// type is canonicalized away so the request can never disagree with its own
/// scope.
pub(crate) fn dynamic_api(
    client: kube::Client,
    gvk: Gvk,
    plural: String,
    namespaced: bool,
    namespace: Option<String>,
) -> Api<DynamicObject> {
    let api_version = if gvk.group.is_empty() {
        gvk.version.clone()
    } else {
        format!("{}/{}", gvk.group, gvk.version)
    };
    let api_resource = ApiResource {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
        plural,
        api_version,
    };
    let namespace = if namespaced { namespace } else { None };
    match (namespaced, namespace.as_deref()) {
        (true, Some(namespace)) => Api::namespaced_with(client, namespace, &api_resource),
        _ => Api::all_with(client, &api_resource),
    }
}

impl WatchSource for KubeWatchSource {
    fn list<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ListedState, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let listed = self
                .api()
                .list(&ListParams::default())
                .await
                .map_err(sanitize_list_error)?;
            let rows = listed
                .items
                .iter()
                .map(|item| self.normalize(item))
                .collect();
            Ok(ListedState {
                resource_version: listed.metadata.resource_version.unwrap_or_default(),
                rows,
            })
        })
    }

    fn attach_watch<'a>(
        &'a self,
        resource_version: String,
        out: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let params = WatchParams::default();
            let stream = match self.api().watch(&params, &resource_version).await {
                Ok(stream) => stream,
                Err(_) => return, // any attach failure restarts with a relist
            };
            tokio::pin!(stream);
            while let Some(event) = stream.next().await {
                match event {
                    Ok(WatchEvent::Added(object)) | Ok(WatchEvent::Modified(object)) => {
                        let _ = out.send(WatchUpdate::Upsert(self.normalize(&object)));
                    }
                    Ok(WatchEvent::Deleted(object)) => {
                        let row = self.normalize(&object);
                        let _ = out.send(WatchUpdate::Delete(row.reference));
                    }
                    Ok(WatchEvent::Bookmark(_)) => {}
                    // Error events end this stream attempt; the supervisor
                    // relists from scratch instead of resuming a suspect cut.
                    Ok(WatchEvent::Error(_)) => return,
                    Err(_) => return,
                }
            }
        })
    }
}

/// Sanitized relist failure detail; raw Kubernetes Status text never crosses.
pub(crate) fn sanitize_list_error(error: kube::Error) -> String {
    match error {
        kube::Error::Api(status) => format!(
            "resource list rejected by the api server with HTTP {}",
            status.code
        ),
        _ => "kubernetes api unreachable for resource list".to_owned(),
    }
}
