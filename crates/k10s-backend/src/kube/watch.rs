//! Real-cluster list/watch source for the supervised runtime.
//!
//! Bridges one `(context, GVK, scope)` selection onto kube-rs dynamic API
//! calls: a LIST seeds the cache and its opaque resourceVersion resumes a
//! WATCH stream. Kube types stay confined to this module — only normalized
//! [`WatchRow`]s, [`WatchUpdate`]s, and sanitized error strings ever reach
//! the runtime, so no credential or raw API shape can leak downstream.
//!
//! Rows carry standard metadata only (identity, labels, owner references,
//! creation timestamp); per-kind status summaries arrive with the Plan 3
//! normalization task and stay empty here rather than being guessed.

use futures_util::StreamExt;
use kube::ResourceExt;
use kube::api::{Api, ListParams, WatchParams};
use kube::core::{DynamicObject, WatchEvent};
use kube::discovery::ApiResource;

use crate::port::{Gvk, OwnerRef, ResourceRef};
use crate::runtime::supervisor::{ListedState, WatchRow, WatchSource, WatchUpdate};

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
        let api_version = if self.gvk.group.is_empty() {
            self.gvk.version.clone()
        } else {
            format!("{}/{}", self.gvk.group, self.gvk.version)
        };
        let api_resource = ApiResource {
            group: self.gvk.group.clone(),
            version: self.gvk.version.clone(),
            kind: self.gvk.kind.clone(),
            plural: self.plural.clone(),
            api_version,
        };
        match (self.namespaced, self.namespace.as_deref()) {
            (true, Some(namespace)) => {
                Api::namespaced_with(self.client.clone(), namespace, &api_resource)
            }
            _ => Api::all_with(self.client.clone(), &api_resource),
        }
    }

    /// Normalize one dynamic object into a runtime row.
    fn normalize(&self, object: &DynamicObject) -> WatchRow {
        let name = object.name_any();
        let uid = object.uid().unwrap_or_else(|| {
            // Server-assigned UIDs are always present on real clusters; the
            // deterministic fallback only covers degenerate recorded data.
            format!("uid-{}-{}", self.gvk.kind.to_lowercase(), name)
        });
        let namespace = object
            .namespace()
            .or_else(|| self.namespaced.then(|| self.namespace.clone()).flatten());
        let owner_references: Vec<OwnerRef> = object
            .owner_references()
            .iter()
            .map(|owner| {
                let (group, version) = split_api_version(&owner.api_version);
                OwnerRef {
                    gvk: Gvk::new(group, version, owner.kind.clone()),
                    name: owner.name.clone(),
                    uid: owner.uid.clone(),
                    controller: owner.controller.unwrap_or(false),
                }
            })
            .collect();
        WatchRow {
            reference: ResourceRef {
                context: self.context.clone(),
                gvk: self.gvk.clone(),
                namespace,
                name,
                uid,
            },
            labels: object.labels().clone(),
            // Per-kind summaries are the normalization task's job; empty is
            // honest where guessing would fabricate status.
            summary: String::new(),
            created_at: object
                .creation_timestamp()
                .map(|time| time.0.to_string())
                .unwrap_or_default(),
            owner_references,
        }
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

fn split_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((group, version)) => (group.to_owned(), version.to_owned()),
        None => (String::new(), api_version.to_owned()),
    }
}

/// Sanitized relist failure detail; raw Kubernetes Status text never crosses.
fn sanitize_list_error(error: kube::Error) -> String {
    match error {
        kube::Error::Api(status) => format!(
            "resource list rejected by the api server with HTTP {}",
            status.code
        ),
        _ => "kubernetes api unreachable for resource list".to_owned(),
    }
}
