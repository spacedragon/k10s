//! EndpointSlice discovery and endpoint normalization for core/v1 Service details.

use kube::Api;
use kube::api::ListParams;
use std::time::SystemTime;

use crate::port::{ServiceEndpointProjection, ServiceProjection, ServiceSliceProjection};

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

/// Query and populate EndpointSlices and resolved endpoints for a Service.
pub(super) async fn resolve_service_endpoints(
    client: &kube::Client,
    namespace: Option<&str>,
    service_name: &str,
    service_uid: &str,
    projection: &mut ServiceProjection,
) {
    let Some(namespace) = namespace else {
        return;
    };
    let api = endpoint_slice_api(client.clone(), namespace);
    let Ok(slices) = api
        .list(
            &ListParams::default().labels(&format!("kubernetes.io/service-name={}", service_name)),
        )
        .await
    else {
        return;
    };

    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    let mut resolved_endpoints = Vec::new();
    let mut resolved_slices = Vec::new();

    for slice in slices.items {
        // Only keep slices owned by this Service or labeled for it.
        let is_owned = slice
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|owners| owners.iter().any(|o| o.uid == service_uid));
        let label_matches = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
            .is_some_and(|s| s == service_name);

        if !is_owned && !label_matches {
            continue;
        }

        let slice_name = slice.metadata.name.clone().unwrap_or_default();
        let managed_by = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
            .cloned();
        let address_type = slice
            .data
            .get("addressType")
            .and_then(|v| v.as_str())
            .unwrap_or("IPv4")
            .to_owned();

        let mut slice_ports = Vec::new();
        let mut default_port = None;
        if let Some(ports) = slice.data.get("ports").and_then(|v| v.as_array()) {
            for port in ports {
                let p_num = port.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
                let p_name = port.get("name").and_then(|v| v.as_str());
                if default_port.is_none() {
                    default_port = p_num;
                }
                match (p_name, p_num) {
                    (Some(name), Some(num)) => slice_ports.push(format!("{name} {num}")),
                    (None, Some(num)) => slice_ports.push(num.to_string()),
                    _ => {}
                }
            }
        }

        let age =
            slice.metadata.creation_timestamp.as_ref().map(|t| {
                format_age(now.saturating_sub(u64::try_from(t.0.as_second()).unwrap_or(now)))
            });

        let mut slice_endpoint_count = 0;
        if let Some(endpoints) = slice.data.get("endpoints").and_then(|v| v.as_array()) {
            slice_endpoint_count = endpoints.len();
            for ep in endpoints {
                let address = ep
                    .get("addresses")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);

                let target_pod = ep
                    .get("targetRef")
                    .and_then(|v| v.get("name"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);

                let node = ep
                    .get("nodeName")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);

                let zone = ep.get("zone").and_then(|v| v.as_str()).map(str::to_owned);

                let conditions = ep.get("conditions");
                let ready = conditions
                    .and_then(|c| c.get("ready"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let serving = conditions
                    .and_then(|c| c.get("serving"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(ready);
                let terminating = conditions
                    .and_then(|c| c.get("terminating"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                let port =
                    default_port.or_else(|| projection.ports.first().map(|p| p.service_port));

                resolved_endpoints.push(ServiceEndpointProjection {
                    address,
                    port,
                    target_pod,
                    node,
                    zone,
                    ready,
                    serving,
                    terminating,
                    slice_name: Some(slice_name.clone()),
                });
            }
        }

        resolved_slices.push(ServiceSliceProjection {
            name: slice_name,
            managed_by,
            address_type,
            ports: slice_ports,
            endpoint_count: slice_endpoint_count,
            max_endpoints: 100,
            age,
        });
    }

    if !resolved_endpoints.is_empty() {
        projection.endpoints = resolved_endpoints;
    }
    if !resolved_slices.is_empty() {
        projection.slices = resolved_slices;
    }
}

fn format_age(seconds: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    const YEAR: u64 = 365 * DAY;
    match seconds {
        value if value >= YEAR => format!("{}y", value / YEAR),
        value if value >= DAY => format!("{}d", value / DAY),
        value if value >= HOUR => format!("{}h", value / HOUR),
        value if value >= MINUTE => format!("{}m", value / MINUTE),
        value => format!("{value}s"),
    }
}
