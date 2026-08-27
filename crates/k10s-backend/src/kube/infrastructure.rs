//! Live Kubernetes storage inventory for the infrastructure projection.

use k8s_openapi::api::{
    core::v1::{PersistentVolume, PersistentVolumeClaim},
    storage::v1::StorageClass,
};
use kube::{Api, ResourceExt, api::ListParams};

use crate::{
    catalog::{CatalogPv, CatalogPvc, CatalogStorageClass},
    port::BackendError,
};

pub(crate) async fn storage_inventory(
    client: kube::Client,
) -> Result<(Vec<CatalogPvc>, Vec<CatalogPv>, Vec<CatalogStorageClass>), BackendError> {
    let claim_api = Api::<PersistentVolumeClaim>::all(client.clone());
    let volume_api = Api::<PersistentVolume>::all(client.clone());
    let class_api = Api::<StorageClass>::all(client);
    let params = ListParams::default();
    let (claims, volumes, classes) = tokio::join!(
        async { claim_api.list(&params).await.map(|list| list.items) },
        async { volume_api.list(&params).await.map(|list| list.items) },
        async { class_api.list(&params).await.map(|list| list.items) },
    );
    let claims = optional_items(claims)?;
    let volumes = optional_items(volumes)?;
    let classes = optional_items(classes)?;

    let mut claims: Vec<_> = claims.into_iter().map(normalize_claim).collect();
    let mut volumes: Vec<_> = volumes.into_iter().map(normalize_volume).collect();
    let mut classes: Vec<_> = classes.into_iter().map(normalize_class).collect();
    claims.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    volumes.sort_by(|a, b| a.name.cmp(&b.name));
    classes.sort_by(|a, b| a.name.cmp(&b.name));

    Ok((claims, volumes, classes))
}

fn optional_items<T>(result: Result<Vec<T>, kube::Error>) -> Result<Vec<T>, BackendError> {
    match result {
        Ok(items) => Ok(items),
        Err(error) => match super::read::sanitize_infrastructure_list_error(error) {
            BackendError::NotFound => Ok(Vec::new()),
            error => Err(error),
        },
    }
}

fn normalize_claim(claim: PersistentVolumeClaim) -> CatalogPvc {
    let capacity = claim
        .status
        .as_ref()
        .and_then(|status| status.capacity.as_ref())
        .and_then(|capacity| capacity.get("storage"))
        .or_else(|| {
            claim
                .spec
                .as_ref()
                .and_then(|spec| spec.resources.as_ref())
                .and_then(|resources| resources.requests.as_ref())
                .and_then(|requests| requests.get("storage"))
        })
        .map(|quantity| quantity.0.clone())
        .unwrap_or_default();
    let spec = claim.spec.as_ref();
    CatalogPvc {
        namespace: claim.namespace().unwrap_or_default(),
        name: claim.name_any(),
        status: claim
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_default(),
        capacity,
        access_modes: spec
            .and_then(|spec| spec.access_modes.clone())
            .unwrap_or_default(),
        storage_class: spec
            .and_then(|spec| spec.storage_class_name.clone())
            .unwrap_or_default(),
        bound_volume: spec
            .and_then(|spec| spec.volume_name.clone())
            .unwrap_or_else(|| "—".into()),
        age: age(&claim.metadata),
    }
}

fn normalize_volume(volume: PersistentVolume) -> CatalogPv {
    let spec = volume.spec.as_ref();
    let bound_claim = spec
        .and_then(|spec| spec.claim_ref.as_ref())
        .map(
            |claim| match (claim.namespace.as_deref(), claim.name.as_deref()) {
                (Some(namespace), Some(name)) => format!("{namespace}/{name}"),
                _ => "—".into(),
            },
        )
        .unwrap_or_else(|| "—".into());
    CatalogPv {
        name: volume.name_any(),
        status: volume
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_default(),
        capacity: spec
            .and_then(|spec| spec.capacity.as_ref())
            .and_then(|capacity| capacity.get("storage"))
            .map(|quantity| quantity.0.clone())
            .unwrap_or_default(),
        access_modes: spec
            .and_then(|spec| spec.access_modes.clone())
            .unwrap_or_default(),
        storage_class: spec
            .and_then(|spec| spec.storage_class_name.clone())
            .unwrap_or_default(),
        bound_claim,
        reclaim_policy: spec
            .and_then(|spec| spec.persistent_volume_reclaim_policy.clone())
            .unwrap_or_default(),
        age: age(&volume.metadata),
    }
}

fn normalize_class(class: StorageClass) -> CatalogStorageClass {
    CatalogStorageClass {
        name: class.name_any(),
        provisioner: class.provisioner,
        reclaim_policy: class.reclaim_policy.unwrap_or_default(),
        volume_binding_mode: class.volume_binding_mode.unwrap_or_default(),
        age: age(&class.metadata),
    }
}

fn age(metadata: &kube::core::ObjectMeta) -> String {
    let Some(created) = metadata.creation_timestamp.as_ref() else {
        return "—".into();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    let created = u64::try_from(created.0.as_second()).unwrap_or(now);
    format_age(now.saturating_sub(created))
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

// Real-cluster node inventory normalization.

use k10s_protocol::{CapacityUsage, NodeRow};

use crate::port::Gvk;
use crate::runtime::supervisor::WatchRow;

use super::normalize::normalize_row;
use super::watch::{dynamic_api, sanitize_list_error};

pub(crate) struct NodeInventory {
    pub(crate) rows: Vec<WatchRow>,
    pub(crate) nodes: Vec<NodeRow>,
}

pub(crate) async fn nodes(
    client: kube::Client,
    context: &str,
) -> Result<NodeInventory, BackendError> {
    let gvk = Gvk::core("v1", "Node");
    let api = dynamic_api(client, gvk.clone(), "nodes".into(), false, None);
    let listed = api
        .list(&ListParams::default())
        .await
        .map_err(|error| BackendError::Internal(sanitize_list_error(error)))?;
    let mut nodes: Vec<_> = listed.items.iter().filter_map(normalize_node).collect();
    nodes.sort_by(|left, right| left.name.cmp(&right.name));
    let rows = listed
        .items
        .iter()
        .map(|node| normalize_row(context, &gvk, false, None, node))
        .collect();
    Ok(NodeInventory { rows, nodes })
}

fn normalize_node(node: &kube::core::DynamicObject) -> Option<NodeRow> {
    let name = node.metadata.name.clone()?;
    let value = serde_json::to_value(node).ok()?;
    let status = value.get("status");
    let allocatable = status.and_then(|status| status.get("allocatable"));
    let labels = node.metadata.labels.as_ref();
    let mut roles: Vec<String> = labels
        .into_iter()
        .flat_map(|labels| labels.keys())
        .filter_map(|key| key.strip_prefix("node-role.kubernetes.io/"))
        .filter(|role| !role.is_empty())
        .map(str::to_owned)
        .collect();
    roles.sort();
    roles.dedup();

    Some(NodeRow {
        name,
        status: ready_status(status),
        roles,
        kubernetes_version: status
            .and_then(|status| status.get("nodeInfo"))
            .and_then(|info| info.get("kubeletVersion"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        cpu: CapacityUsage::new(None, allocatable.and_then(|a| quantity_cpu(a.get("cpu")?))),
        memory: CapacityUsage::new(
            None,
            allocatable.and_then(|a| quantity_memory(a.get("memory")?)),
        ),
        pods: CapacityUsage::new(None, allocatable.and_then(|a| quantity_u64(a.get("pods")?))),
        age: "—".into(),
    })
}

fn ready_status(status: Option<&serde_json::Value>) -> String {
    let ready = status
        .and_then(|status| status.get("conditions"))
        .and_then(serde_json::Value::as_array)
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(serde_json::Value::as_str) == Some("Ready")
            })
        })
        .and_then(|condition| condition.get("status"))
        .and_then(serde_json::Value::as_str);
    if ready == Some("True") {
        "Ready"
    } else {
        "Not Ready"
    }
    .into()
}

fn quantity_text(value: &serde_json::Value) -> Option<&str> {
    value.as_str()
}

fn quantity_u64(value: &serde_json::Value) -> Option<u64> {
    quantity_text(value)?.parse().ok()
}

fn quantity_cpu(value: &serde_json::Value) -> Option<u64> {
    let text = quantity_text(value)?;
    text.strip_suffix('m')
        .and_then(|number| number.parse().ok())
        .or_else(|| text.parse::<u64>().ok()?.checked_mul(1_000))
}

fn quantity_memory(value: &serde_json::Value) -> Option<u64> {
    let text = quantity_text(value)?;
    for (suffix, multiplier) in [("Ki", 1_024), ("Mi", 1_048_576), ("Gi", 1_073_741_824)] {
        if let Some(number) = text.strip_suffix(suffix) {
            return number.parse::<u64>().ok()?.checked_mul(multiplier);
        }
    }
    text.parse().ok()
}
