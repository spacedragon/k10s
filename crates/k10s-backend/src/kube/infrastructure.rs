//! Real-cluster infrastructure inventory normalization.

use k10s_protocol::{CapacityUsage, NodeRow};
use kube::api::ListParams;

use crate::port::{BackendError, Gvk};
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
