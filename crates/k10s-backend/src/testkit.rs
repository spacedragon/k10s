//! Test support for recording Kubernetes API traffic at the tower Service layer.
//!
//! Integration tests in this crate and `k10s-server` construct a real
//! [`kube::Client`] on top of [`RecordedApiServer`] instead of dialing a live
//! API server: canned discovery responses are served from a map keyed by
//! request path, so the full kube-rs client and discovery stack runs against
//! deterministic, credential-free fixtures. Never enable this feature in
//! production builds; it is wired exclusively through dev-dependencies.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use tower::{BoxError, Service};

type Recorded = (u16, String);

/// Shared recorded state behind clones of one [`RecordedApiServer`].
#[derive(Debug)]
struct RecordedState {
    /// Canned responses keyed by request path.
    responses: BTreeMap<String, Recorded>,
    /// Per-path request hit counts for refresh assertions.
    hits: BTreeMap<String, usize>,
}

/// A recorded Kubernetes API server implemented as a tower Service.
///
/// Clones share state with the original, so tests can mutate responses and
/// read hit counts while the client under test still holds its own clone.
#[derive(Debug, Clone)]
pub struct RecordedApiServer {
    state: Arc<std::sync::Mutex<RecordedState>>,
}

impl Service<Request<kube::client::Body>> for RecordedApiServer {
    type Response = Response<Full<Bytes>>;
    type Error = BoxError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // Recorded responses are always available immediately.
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<kube::client::Body>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let state = self.state.clone();
        Box::pin(async move {
            let mut shared = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *shared.hits.entry(path.clone()).or_insert(0) += 1;
            let (status, body) = match shared.responses.get(&path) {
                Some(recorded) => recorded.clone(),
                None => (404u16, status_json("no recorded response for /{path}")),
            };
            Ok(Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))?)
        })
    }
}

impl Default for RecordedApiServer {
    fn default() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(RecordedState {
                responses: BTreeMap::new(),
                hits: BTreeMap::new(),
            })),
        }
    }
}

impl RecordedApiServer {
    /// Build a client for this recorded server.
    #[must_use]
    pub fn into_client(self, default_namespace: impl Into<String>) -> kube::Client {
        kube::client::ClientBuilder::new(self, default_namespace).build()
    }

    /// Record one canned response for an exact request path.
    pub fn set_response(&self, path: &str, status: u16, body: &str) {
        let mut shared = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shared
            .responses
            .insert(path.to_owned(), (status, body.to_owned()));
    }

    /// How many times one path has been requested so far.
    #[must_use]
    pub fn hit_count(&self, path: &str) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .hits
            .get(path)
            .copied()
            .unwrap_or(0)
    }

    /// A recorded discovery surface matching the standard fake Kubernetes
    /// world: core built-ins, apps workloads (with scale subresources where a
    /// real cluster exposes them), apiextensions, and one CRD group.
    #[must_use]
    pub fn standard() -> Self {
        let server = Self::default();
        server.set_response("/apis", 200, APIS_GROUP_LIST);
        server.set_response("/api", 200, API_VERSIONS_V1);
        server.set_response(
            "/api/v1",
            200,
            r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[
              {"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list","watch","create","update","patch","delete"]},
              {"name":"nodes","singularName":"node","namespaced":false,"kind":"Node","verbs":["get","list","watch","update","patch"]},
              {"name":"services","singularName":"service","namespaced":true,"kind":"Service","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["svc"]},
              {"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["cm"]},
              {"name":"namespaces","singularName":"namespace","namespaced":false,"kind":"Namespace","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["ns"]},
              {"name":"tokenreviews","singularName":"tokenreview","namespaced":false,"kind":"TokenReview","verbs":["create"]}
            ]}"#,
        );
        server.set_response(
            "/apis/apps/v1",
            200,
            r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"apps/v1","resources":[
              {"name":"deployments","singularName":"deployment","namespaced":true,"kind":"Deployment","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["deploy"]},
              {"name":"deployments/scale","singularName":"","namespaced":true,"kind":"Scale","verbs":["get","update","patch"]},
              {"name":"replicasets","singularName":"replicaset","namespaced":true,"kind":"ReplicaSet","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["rs"]},
              {"name":"replicasets/scale","singularName":"","namespaced":true,"kind":"Scale","verbs":["get","update","patch"]},
              {"name":"statefulsets","singularName":"statefulset","namespaced":true,"kind":"StatefulSet","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["sts"]},
              {"name":"statefulsets/scale","singularName":"","namespaced":true,"kind":"Scale","verbs":["get","update","patch"]},
              {"name":"daemonsets","singularName":"daemonset","namespaced":true,"kind":"DaemonSet","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["ds"]}
            ]}"#,
        );
        server.set_response(
            "/apis/batch/v1",
            200,
            r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"batch/v1","resources":[
              {"name":"jobs","singularName":"job","namespaced":true,"kind":"Job","verbs":["get","list","watch","create","update","patch","delete"]},
              {"name":"cronjobs","singularName":"cronjob","namespaced":true,"kind":"CronJob","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["cj"]}
            ]}"#,
        );
        server.set_response(
            "/apis/apiextensions.k8s.io/v1",
            200,
            r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"apiextensions.k8s.io/v1","resources":[
              {"name":"customresourcedefinitions","singularName":"customresourcedefinition","namespaced":false,"kind":"CustomResourceDefinition","verbs":["get","list","watch","create","update","patch","delete"],"shortNames":["crd","crds"]}
            ]}"#,
        );
        server.set_response(
            "/apis/k10s.example.com/v1alpha1",
            200,
            r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"k10s.example.com/v1alpha1","resources":[
              {"name":"gadgets","singularName":"gadget","namespaced":true,"kind":"Gadget","verbs":["get","list","watch","create","update","patch","delete"]},
              {"name":"gadgets/status","singularName":"","namespaced":true,"kind":"","verbs":["get","update","patch"]}
            ]}"#,
        );
        server
    }
}

/// Kubernetes Status error body for unrecorded paths (mirrors real clusters).
fn status_json(message: &str) -> String {
    format!(
        r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","message":{message:?},"reason":"NotFound","code":404}}"#
    )
}

/// The recorded /apis group list: the standard fake world's API surface.
const APIS_GROUP_LIST: &str = r#"{"kind":"APIGroupList","apiVersion":"v1","groups":[
  {"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}},
  {"name":"batch","versions":[{"groupVersion":"batch/v1","version":"v1"}],"preferredVersion":{"groupVersion":"batch/v1","version":"v1"}},
  {"name":"apiextensions.k8s.io","versions":[{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}},
  {"name":"k10s.example.com","versions":[{"groupVersion":"k10s.example.com/v1alpha1","version":"v1alpha1"}]}
]}"#;

/// The recorded /api core-group version list.
const API_VERSIONS_V1: &str = r#"{"kind":"APIVersions","apiVersion":"v1","versions":["v1"],"resources":["namespacedNames","nonNamespacedNames"]}"#;
