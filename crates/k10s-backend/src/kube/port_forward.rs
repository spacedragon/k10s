//! Real Kubernetes Service-to-Pod resolution and Pod port-forward streams
//! through kube-rs.
//!
//! Resolution follows the designed policy exactly: fetch the Service and
//! verify its UID, reject non-forwardable ports, scope EndpointSlices by
//! the `kubernetes.io/service-name` label AND a matching owner-reference
//! UID (so stale slices from a recreated Service are skipped), keep only
//! ready same-namespace Pod endpoints, sort candidates deterministically,
//! and re-verify the chosen Pod's UID before any stream opens. No selector,
//! legacy Endpoints, or raw-IP fallbacks exist in this version.

use std::future::Future;
use std::pin::Pin;

use kube::ResourceExt;
use kube::api::{Api, ListParams};

use crate::port::BackendError;
use crate::port_forward::{
    PortForwardRequest, PortForwardSeam, PortForwardStream, RejectionCategory, ResolvedPortForward,
};

/// Rejection helper carrying a stable category.
fn rejected(category: RejectionCategory, message: impl Into<String>) -> BackendError {
    BackendError::PortForward {
        category,
        message: message.into(),
    }
}

/// Map a Kubernetes API error onto sanitized backend failures.
pub(crate) fn sanitize_api_error(error: &kube::Error) -> BackendError {
    if let kube::Error::Api(response) = error {
        if response.code == 403 {
            return BackendError::Forbidden;
        }
        if response.code == 404 {
            return BackendError::NotFound;
        }
    }
    BackendError::Internal("kubernetes api call failed".into())
}

/// One client-backed seam over the real cluster APIs.
#[derive(Clone)]
pub struct KubePortForwardSeam {
    clients: std::collections::BTreeMap<String, kube::Client>,
}

impl std::fmt::Debug for KubePortForwardSeam {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Clients own transport state; only their count is reported.
        formatter
            .debug_struct("KubePortForwardSeam")
            .field("contexts", &self.clients.len())
            .finish()
    }
}

impl KubePortForwardSeam {
    /// Build the seam from per-context clients.
    pub(crate) fn new(clients: std::collections::BTreeMap<String, kube::Client>) -> Self {
        Self { clients }
    }

    fn client(&self, context: &str) -> Result<kube::Client, BackendError> {
        self.clients
            .get(context)
            .cloned()
            .ok_or(BackendError::NotFound)
    }
}

/// Typed core/v1 Service API for one namespace.
fn service_api(client: kube::Client, namespace: &str) -> Api<k8s_openapi::api::core::v1::Service> {
    Api::namespaced(client, namespace)
}

/// Dynamic discovery.k8s.io/v1 EndpointSlice API for one namespace.
fn endpoint_slice_api(client: kube::Client, namespace: &str) -> Api<kube::core::DynamicObject> {
    let resource = kube::core::ApiResource {
        group: "discovery.k8s.io".into(),
        version: "v1".into(),
        kind: "EndpointSlice".into(),
        api_version: "discovery.k8s.io/v1".into(),
        plural: "endpointslices".into(),
    };
    Api::namespaced_with(client, namespace, &resource)
}

/// Typed core/v1 Pod API for one namespace.
fn pod_api(client: kube::Client, namespace: &str) -> Api<k8s_openapi::api::core::v1::Pod> {
    Api::namespaced(client, namespace)
}

impl PortForwardSeam for KubePortForwardSeam {
    fn resolve<'a>(
        &'a self,
        request: PortForwardRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedPortForward, BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let client = self.client(&request.context)?;

            // 1+2. Fetch the exact Service and verify its live UID.
            let service = service_api(client.clone(), &request.namespace)
                .get(&request.service_name)
                .await
                .map_err(|error| match sanitize_api_error(&error) {
                    BackendError::NotFound => rejected(
                        RejectionCategory::VanishedResource,
                        "the service does not exist",
                    ),
                    BackendError::Forbidden => rejected(
                        RejectionCategory::Forbidden,
                        "reading the service was denied",
                    ),
                    other => other,
                })?;
            let Some(service_uid) = service.uid() else {
                return Err(rejected(
                    RejectionCategory::VanishedResource,
                    "the service has no uid",
                ));
            };
            if service_uid != request.service_uid {
                return Err(rejected(
                    RejectionCategory::VanishedResource,
                    "the service was recreated; retry with the fresh identity",
                ));
            }

            // 3. Match exactly one declared port and require TCP.
            let spec = service.spec.clone().unwrap_or_default();
            if spec.type_.as_deref() == Some("ExternalName") {
                return Err(rejected(
                    RejectionCategory::UnsupportedService,
                    "ExternalName services cannot be forwarded",
                ));
            }
            let matched: Vec<&k8s_openapi::api::core::v1::ServicePort> = spec
                .ports
                .iter()
                .flatten()
                .filter(|declared| match &request.port {
                    crate::port_forward::PortForwardPortSelection::Name(name) => {
                        declared.name.as_deref() == Some(name)
                    }
                    crate::port_forward::PortForwardPortSelection::Number(number) => {
                        u16::try_from(declared.port).ok() == Some(*number)
                    }
                })
                .collect();
            let [declared] = matched.as_slice() else {
                let message = if matched.is_empty() {
                    "no declared service port matches the selection"
                } else {
                    "the selection matches multiple declared ports"
                };
                return Err(rejected(RejectionCategory::UnsupportedService, message));
            };
            if declared.protocol.as_deref().unwrap_or("TCP") != "TCP" {
                return Err(rejected(
                    RejectionCategory::UnsupportedService,
                    "only TCP service ports can be forwarded",
                ));
            }

            // 4+5. Scope slices by label AND owning Service UID.
            let slices = endpoint_slice_api(client.clone(), &request.namespace)
                .list(&ListParams::default().labels(&format!(
                    "kubernetes.io/service-name={}",
                    request.service_name
                )))
                .await
                .map_err(|error| match sanitize_api_error(&error) {
                    BackendError::NotFound => rejected(
                        RejectionCategory::UnavailableEndpoint,
                        "endpoint discovery failed",
                    ),
                    BackendError::Forbidden => rejected(
                        RejectionCategory::Forbidden,
                        "listing endpoint slices was denied",
                    ),
                    other => other,
                })?;
            let owned: Vec<&kube::core::DynamicObject> = slices
                .items
                .iter()
                .filter(|slice| {
                    slice
                        .owner_references()
                        .iter()
                        .any(|owner| owner.uid == request.service_uid)
                })
                .collect();

            // 6+7. Ready same-namespace Pod endpoints whose slice carries a
            // matching port with a numeric target port.
            let service_port_number = u16::try_from(declared.port).map_err(|_| {
                rejected(
                    RejectionCategory::UnsupportedService,
                    "service port number out of range",
                )
            })?;
            let service_port_name = declared.name.clone();
            let mut candidates: Vec<(String, String, u16)> = Vec::new();
            for slice in &owned {
                let data = &slice.data;
                let Some(ports) = data.get("ports").and_then(serde_json::Value::as_array) else {
                    continue;
                };
                let target_port = ports
                    .iter()
                    .filter(|entry| {
                        let name_matches = service_port_name.as_deref().is_some_and(|name| {
                            entry.get("name").and_then(|n| n.as_str()) == Some(name)
                        });
                        let number_matches = entry
                            .get("port")
                            .and_then(serde_json::Value::as_i64)
                            .map(|p| u16::try_from(p).ok())
                            == Some(Some(service_port_number));
                        service_port_name.is_some() && name_matches
                            || service_port_name.is_none() && number_matches
                    })
                    .filter_map(|entry| entry.get("targetPort"))
                    .find_map(|target| match target {
                        serde_json::Value::Number(raw) => {
                            raw.as_u64().and_then(|p| u16::try_from(p).ok())
                        }
                        serde_json::Value::String(name) => name.parse::<u16>().ok(),
                        _ => None,
                    });
                let Some(target_port) = target_port else {
                    continue;
                };
                let Some(endpoints) = data.get("endpoints").and_then(serde_json::Value::as_array)
                else {
                    continue;
                };
                for endpoint in endpoints {
                    let ready_not_false = endpoint
                        .get("conditions")
                        .and_then(|conditions| conditions.get("ready"))
                        .and_then(serde_json::Value::as_bool)
                        != Some(false);
                    if !ready_not_false {
                        continue;
                    }
                    let Some(target_ref) = endpoint.get("targetRef") else {
                        continue;
                    };
                    let is_pod =
                        target_ref.get("kind").and_then(|kind| kind.as_str()) == Some("Pod");
                    let same_namespace = target_ref
                        .get("namespace")
                        .and_then(|ns| ns.as_str())
                        .is_none_or(|ns| ns == request.namespace);
                    if !(is_pod && same_namespace) {
                        continue;
                    }
                    let Some(pod_name) = target_ref.get("name").and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    let Some(pod_uid) = target_ref.get("uid").and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    candidates.push((pod_name.to_owned(), pod_uid.to_owned(), target_port));
                }
            }

            // 8. Deterministic order by Pod name then UID.
            candidates.sort();
            let Some((pod_name, pod_uid, pod_port)) = candidates.into_iter().next() else {
                return Err(rejected(
                    RejectionCategory::UnavailableEndpoint,
                    "no ready endpoint backs this service port",
                ));
            };

            // 9. Verify the pinned Pod still exists with that UID.
            let pod = pod_api(client, &request.namespace)
                .get(&pod_name)
                .await
                .map_err(|error| match sanitize_api_error(&error) {
                    BackendError::NotFound => rejected(
                        RejectionCategory::UnavailableEndpoint,
                        "the selected endpoint no longer exists",
                    ),
                    BackendError::Forbidden => rejected(
                        RejectionCategory::Forbidden,
                        "reading the selected pod was denied",
                    ),
                    other => other,
                })?;
            if pod.uid().as_deref() != Some(pod_uid.as_str()) {
                return Err(rejected(
                    RejectionCategory::UnavailableEndpoint,
                    "the selected endpoint was replaced",
                ));
            }

            Ok(ResolvedPortForward {
                context: request.context,
                namespace: request.namespace,
                service_uid: request.service_uid,
                pod_name,
                pod_uid,
                pod_port,
            })
        })
    }

    fn connect<'a>(
        &'a self,
        resolved: &'a ResolvedPortForward,
    ) -> Pin<Box<dyn Future<Output = Result<PortForwardStream, BackendError>> + Send + 'a>> {
        Box::pin(async move {
            let client = self.client(&resolved.context)?;
            let pods: Api<k8s_openapi::api::core::v1::Pod> = pod_api(client, &resolved.namespace);
            let mut forwarder = pods
                .portforward(&resolved.pod_name, &[resolved.pod_port])
                .await
                .map_err(|error| {
                    let category = if matches!(sanitize_api_error(&error), BackendError::Forbidden)
                    {
                        RejectionCategory::Forbidden
                    } else {
                        RejectionCategory::TransportClosed
                    };
                    rejected(category, "the pod stream could not be opened")
                })?;
            let stream = forwarder.take_stream(resolved.pod_port).ok_or_else(|| {
                rejected(
                    RejectionCategory::TransportClosed,
                    "the pod stream closed before it was taken",
                )
            })?;
            Ok(PortForwardStream::new(Box::new(stream)))
        })
    }
}
