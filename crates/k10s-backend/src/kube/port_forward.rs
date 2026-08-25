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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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

/// One client-backed seam sharing the adapter's live per-context clients.
///
/// Holding the adapter's own client map keeps construction synchronous and
/// lets late-created contexts participate without rebuilding the seam.
#[derive(Clone)]
pub struct KubePortForwardSeam {
    clients: Arc<tokio::sync::Mutex<HashMap<String, kube::Client>>>,
}

impl std::fmt::Debug for KubePortForwardSeam {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KubePortForwardSeam")
    }
}

impl KubePortForwardSeam {
    /// Build the seam over the adapter's shared client map.
    pub(crate) fn shared(clients: Arc<tokio::sync::Mutex<HashMap<String, kube::Client>>>) -> Self {
        Self { clients }
    }

    async fn client(&self, context: &str) -> Result<kube::Client, BackendError> {
        self.clients
            .lock()
            .await
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
            let client = self.client(&request.context).await?;

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

            // 6. Ready same-namespace Pod endpoints behind a slice port
            // matching the selected Service port by name (discovery/v1
            // EndpointPort mirrors the Service port name and carries no
            // targetPort; the container port derives from the Service).
            let service_port_number = u16::try_from(declared.port).map_err(|_| {
                rejected(
                    RejectionCategory::UnsupportedService,
                    "service port number out of range",
                )
            })?;
            let mut candidates: Vec<(String, String)> = Vec::new();
            for slice in &owned {
                let data = &slice.data;
                let Some(ports) = data.get("ports").and_then(serde_json::Value::as_array) else {
                    continue;
                };
                let matches_declared = ports.iter().any(|entry| {
                    let name_matches = entry.get("name").and_then(serde_json::Value::as_str)
                        == declared.name.as_deref();
                    // discovery/v1 slices carry the Service port number in
                    // `port`; tolerate its absence on hand-crafted data.
                    let number_ok = entry
                        .get("port")
                        .and_then(serde_json::Value::as_i64)
                        .map(|p| u16::try_from(p).ok())
                        != Some(None)
                        && entry
                            .get("port")
                            .and_then(serde_json::Value::as_i64)
                            .map(|p| u16::try_from(p).ok() == Some(service_port_number))
                            .unwrap_or(true);
                    name_matches && number_ok
                });
                if !matches_declared {
                    continue;
                }
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
                    candidates.push((pod_name.to_owned(), pod_uid.to_owned()));
                }
            }

            // 7+8. Deterministic order by Pod name then UID.
            candidates.sort();
            candidates.dedup();
            let Some((pod_name, pod_uid)) = candidates.into_iter().next() else {
                return Err(rejected(
                    RejectionCategory::UnavailableEndpoint,
                    "no ready endpoint backs this service port",
                ));
            };

            // 9. Verify the pinned Pod still exists with that UID, then
            // resolve the numeric container port from the declared
            // Service targetPort against that Pod's declared container
            // ports.
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
            use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
            let pod_port = match declared.target_port.as_ref() {
                Some(IntOrString::Int(port)) => u16::try_from(*port).map_err(|_| {
                    rejected(
                        RejectionCategory::UnsupportedService,
                        "the declared target port is out of range",
                    )
                })?,
                Some(IntOrString::String(name)) => resolve_named_container_port(&pod, name)
                    .ok_or_else(|| {
                        rejected(
                            RejectionCategory::UnavailableEndpoint,
                            "no declared TCP container port carries the target port name",
                        )
                    })?,
                None => service_port_number,
            };

            Ok(ResolvedPortForward {
                context: request.context,
                namespace: request.namespace,
                service_uid: request.service_uid,
                service_port: service_port_number,
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
            let client = self.client(&resolved.context).await?;
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

/// Resolve a named target port against one Pod's declared TCP container
/// ports across regular and init containers.
fn resolve_named_container_port(pod: &k8s_openapi::api::core::v1::Pod, name: &str) -> Option<u16> {
    let status_phase_running_or_none = pod
        .status
        .as_ref()
        .map(|status| status.phase.as_deref() != Some("Failed"));
    if status_phase_running_or_none == Some(false) {
        return None;
    }
    let containers = pod.spec.iter().flat_map(|spec| {
        spec.containers
            .iter()
            .chain(spec.init_containers.iter().flatten())
    });
    for container in containers {
        for port in container.ports.iter().flatten() {
            if port.name.as_deref() == Some(name)
                && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
            {
                return u16::try_from(port.container_port).ok();
            }
        }
    }
    None
}
