//! Resource Metrics API polling for the real adapter.
//!
//! Collects `metrics.k8s.io/v1beta1` NodeMetrics and PodMetrics cuts plus the
//! core Node list (coverage denominators and allocatable pod capacity) in one
//! poll cycle per consumer demand. The Metrics API's own availability is
//! probed by reading it: a 404 means the API is absent from the cluster, a
//! 403 means RBAC denied the read, and anything else is unreachable — each
//! state is reported explicitly instead of collapsing into empty data.
//!
//! Honesty rules enforced here: usage numbers come only from the metrics cut,
//! never from requests or capacity; missing samples stay absent rather than
//! zeroed; and samples older than the freshness window withhold their values
//! while keeping the last-known collection time visible so staleness can be
//! shown instead of disguised.

use std::collections::BTreeMap;
use std::time::Duration;

use kube::api::ListParams;
use kube::core::DynamicObject;

use crate::port::{BackendError, Gvk, MetricsSample, ResourceRef};
use crate::runtime::{
    MetricsApiState, MetricsPollSource, MetricsSnapshot, ResourceUsageSample, now_rfc3339,
};

use super::read::sanitize_get_error;
use super::watch::dynamic_api;

/// Group/version of the Resource Metrics API.
const METRICS_API: (&str, &str) = ("metrics.k8s.io", "v1beta1");

/// How long after its source timestamp a collected sample still counts as
/// fresh; older cuts withhold their values instead of masquerading as live.
pub(crate) const METRICS_FRESHNESS: Duration = Duration::from_secs(180);

/// One supervised metrics poll source bound to one context client.
///
/// The registry spawns exactly one task per context over this source; each
/// `poll` performs one complete collection cycle and always yields a cut.
pub(crate) struct MetricsSource {
    client: kube::Client,
    context: String,
}

impl std::fmt::Debug for MetricsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The client owns transport state; only the binding is reported.
        f.debug_struct("MetricsSource")
            .field("context", &self.context)
            .finish()
    }
}

impl MetricsSource {
    /// Bind one poll source to `client`.
    pub(crate) fn new(client: kube::Client, context: impl Into<String>) -> Self {
        Self {
            client,
            context: context.into(),
        }
    }
}

impl MetricsPollSource for MetricsSource {
    fn poll(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = MetricsSnapshot> + Send + '_>> {
        Box::pin(async move { collect_once(&self.client, &self.context).await })
    }
}

/// Run one full collection cycle: core Nodes (membership + allocatable pod
/// capacity), NodeMetrics, then PodMetrics. Every failure mode lands in an
/// explicit snapshot state — collection never fails a consumer with zeros.
async fn collect_once(client: &kube::Client, context: &str) -> MetricsSnapshot {
    // Core membership first: honest coverage denominators and pod capacity.
    let core = core_nodes(client).await;

    let node_cut = list_metrics(
        client,
        Gvk::new(METRICS_API.0, METRICS_API.1, "NodeMetrics"),
        "nodes",
    )
    .await;
    let state = match &node_cut {
        Err(MetricsProbe::Absent) => MetricsApiState::Absent,
        Err(MetricsProbe::Forbidden) => MetricsApiState::Forbidden,
        Err(MetricsProbe::Unreachable) => MetricsApiState::Unreachable,
        Ok(_) => MetricsApiState::Ready,
    };

    let mut node_usage = BTreeMap::new();
    let mut pod_usage = BTreeMap::new();
    let mut newest: Option<String> = None;
    let mut window: Option<u64> = None;

    // Pod cuts only make sense once the API itself answered.
    if let Ok(cut) = node_cut {
        for item in cut {
            if let Some((name, sample)) = normalize_node_metrics(&item) {
                node_usage.insert(name, sample);
            }
            absorb_time_meta(&item, &mut newest, &mut window);
        }
        if let Ok(pod_cut) = list_metrics(
            client,
            Gvk::new(METRICS_API.0, METRICS_API.1, "PodMetrics"),
            "pods",
        )
        .await
        {
            for item in pod_cut {
                if let Some(((namespace, name), sample)) = normalize_pod_metrics(&item) {
                    pod_usage.insert(format!("{namespace}/{name}"), sample);
                }
                absorb_time_meta(&item, &mut newest, &mut window);
            }
        }
    }

    MetricsSnapshot {
        context: context.to_owned(),
        collected_at: now_rfc3339(),
        source_updated_at: newest,
        window_seconds: window,
        state,
        node_usage,
        pod_usage,
        node_names: core.node_names,
        pod_capacity_total: core.pod_capacity_total,
    }
}

/// Outcome of probing one Metrics endpoint.
enum MetricsProbe {
    /// The cluster does not serve this API at all.
    Absent,
    /// RBAC denied the read.
    Forbidden,
    /// Transport or other unexpected failure.
    Unreachable,
}

type RawCut = Result<Vec<DynamicObject>, MetricsProbe>;

/// List one metrics resource cluster-wide, mapping failures to probe states.
async fn list_metrics(client: &kube::Client, gvk: Gvk, plural: &str) -> RawCut {
    let api = dynamic_api(client.clone(), gvk, plural.to_owned(), false, None);
    match api.list(&ListParams::default()).await {
        Ok(objects) => Ok(objects.items),
        Err(kube::Error::Api(status)) if status.code == 404 => Err(MetricsProbe::Absent),
        Err(kube::Error::Api(status)) if status.code == 403 => Err(MetricsProbe::Forbidden),
        Err(_) => Err(MetricsProbe::Unreachable),
    }
}

/// Core Node membership and summed allocatable pod capacity.
struct CoreNodes {
    node_names: Vec<String>,
    pod_capacity_total: Option<u64>,
}

/// Read the core Node list. Capacity derives exclusively from
/// `status.allocatable.pods`; unreadable lists leave capacity unknown.
async fn core_nodes(client: &kube::Client) -> CoreNodes {
    let api = dynamic_api(
        client.clone(),
        Gvk::core("v1", "Node"),
        "nodes".to_owned(),
        false,
        None,
    );
    let mut node_names = Vec::new();
    let mut pod_capacity_total = Some(0u64);
    let listed = api.list(&ListParams::default()).await;
    match listed {
        Ok(listed) => {
            for object in &listed.items {
                let Some(name) = object.metadata.name.clone() else {
                    continue;
                };
                node_names.push(name);
                let capacity = serde_json::to_value(object)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("status")?
                            .get("allocatable")?
                            .get("pods")?
                            .as_str()
                            .map(str::to_owned)
                    })
                    .and_then(|text| quantity_pods(&text));
                match capacity {
                    Some(pods) => {
                        if let Some(total) = pod_capacity_total {
                            pod_capacity_total = total.checked_add(pods);
                        }
                    }
                    // An unparsable denominator keeps the total honestly unknown.
                    None => pod_capacity_total = None,
                }
            }
        }
        Err(_) => pod_capacity_total = None,
    }
    CoreNodes {
        node_names,
        pod_capacity_total,
    }
}

/// Source-reported `timestamp`/`window` pair of one raw metrics item.
fn item_time_meta(value: &serde_json::Value) -> (Option<String>, Option<u64>) {
    (
        value.get("timestamp").and_then(as_text),
        value
            .get("window")
            .and_then(as_text)
            .and_then(parse_window_seconds),
    )
}

/// Absorb `timestamp`/`window` metadata from one raw metrics item.
fn absorb_time_meta(item: &DynamicObject, newest: &mut Option<String>, window: &mut Option<u64>) {
    let Ok(value) = serde_json::to_value(item) else {
        return;
    };
    if let Some(timestamp) = value.get("timestamp").and_then(as_text) {
        let newer = newest
            .as_deref()
            .and_then(parse_rfc3339_unix)
            .map(|known| parse_rfc3339_unix(&timestamp).is_some_and(|candidate| candidate > known))
            .unwrap_or(true);
        if newer {
            *newest = Some(timestamp);
        }
    }
    if window.is_none() {
        *window = value
            .get("window")
            .and_then(as_text)
            .and_then(parse_window_seconds);
    }
}

/// Normalize one raw NodeMetrics item into `(node name, usage)` carrying the
/// item's own timestamp/window so freshness gates per sample.
fn normalize_node_metrics(item: &DynamicObject) -> Option<(String, ResourceUsageSample)> {
    let name = item.metadata.name.clone()?;
    let value = serde_json::to_value(item).ok()?;
    let usage = value.get("usage")?;
    let (timestamp, window_seconds) = item_time_meta(&value);
    Some((
        name,
        ResourceUsageSample {
            cpu_millicores: quantity_millicores(usage.get("cpu").and_then(as_text)),
            memory_bytes: quantity_bytes(usage.get("memory").and_then(as_text)),
            timestamp,
            window_seconds,
        },
    ))
}

/// Normalize one raw PodMetrics item into `((namespace, name), usage)` where
/// each field sums every container and fails closed to `None` unless all of
/// them reported that field, alongside the item's own timestamp/window so
/// freshness gates per sample.
fn normalize_pod_metrics(item: &DynamicObject) -> Option<((String, String), ResourceUsageSample)> {
    let namespace = item.metadata.namespace.clone()?;
    let name = item.metadata.name.clone()?;
    let value = serde_json::to_value(item).ok()?;
    let containers = value.get("containers")?.as_array()?;
    let (timestamp, window_seconds) = item_time_meta(&value);
    Some((
        (namespace, name),
        ResourceUsageSample {
            cpu_millicores: sum_complete(containers.iter().map(|container| {
                quantity_millicores(
                    container
                        .get("usage")
                        .and_then(|usage| usage.get("cpu"))
                        .and_then(as_text),
                )
            })),
            memory_bytes: sum_complete(containers.iter().map(|container| {
                quantity_bytes(
                    container
                        .get("usage")
                        .and_then(|usage| usage.get("memory"))
                        .and_then(as_text),
                )
            })),
            timestamp,
            window_seconds,
        },
    ))
}

/// Sum one usage field across every container, failing closed to `None`
/// unless at least one value arrived and no container left the field out —
/// a skipped contribution is a fabricated zero, never an honest sum.
fn sum_complete(contributions: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    let mut total = Some(0u64);
    let mut reported = false;
    for contribution in contributions {
        let value = contribution?;
        total = total.and_then(|running| running.checked_add(value));
        reported = true;
    }
    if reported { total } else { None }
}

fn as_text(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

/// Map the latest cached cut onto one exact pod reference's port-type sample.
///
/// Missing pods, non-ready APIs, unmetered pods, and stale samples all
/// produce explicitly absent fields — never inferred numbers. Freshness is
/// judged by the requested pod's own source timestamp, so a fresh sibling
/// item can never vouch for this pod's older or unparseable cut. Stale
/// samples keep their own source timestamp so consumers can display age
/// without serving dead values.
pub(crate) fn sample_for_reference(
    snapshot: Option<&MetricsSnapshot>,
    reference: &ResourceRef,
) -> MetricsSample {
    let default = || MetricsSample {
        cpu_millicores: None,
        memory_bytes: None,
        collected_at: None,
    };
    let Some(snapshot) = snapshot else {
        return default();
    };
    if snapshot.state != MetricsApiState::Ready {
        return default();
    }
    let namespace = reference.namespace.as_deref().unwrap_or_default();
    let Some(usage) = snapshot
        .pod_usage
        .get(&format!("{namespace}/{}", reference.name))
    else {
        return default();
    };
    // An unparseable per-sample timestamp has nothing honest to display.
    let Some(sampled) = usage.timestamp.as_deref().and_then(parse_rfc3339_unix) else {
        return default();
    };
    if now_unix_secs() >= sampled.saturating_add(METRICS_FRESHNESS.as_secs()) {
        return MetricsSample {
            cpu_millicores: None,
            memory_bytes: None,
            collected_at: usage.timestamp.clone(),
        };
    }
    MetricsSample {
        cpu_millicores: usage.cpu_millicores,
        memory_bytes: usage.memory_bytes,
        collected_at: usage.timestamp.clone(),
    }
}

/// Verify one reference resolves to an existing pod of exactly this identity.
///
/// A reused name carrying another UID (delete/recreate reuse) is the same
/// typed not-found as a vanished object, mirroring detail reads.
pub(crate) async fn verify_pod_identity(
    client: &kube::Client,
    reference: &ResourceRef,
) -> Result<(), BackendError> {
    let Some(namespace) = reference.namespace.as_deref() else {
        return Err(BackendError::NotFound);
    };
    let api = dynamic_api(
        client.clone(),
        reference.gvk.clone(),
        "pods".to_owned(),
        true,
        Some(namespace.to_owned()),
    );
    let object = api.get(&reference.name).await.map_err(sanitize_get_error)?;
    let uid = kube::ResourceExt::uid(&object).unwrap_or_default();
    if uid != reference.uid {
        return Err(BackendError::NotFound);
    }
    Ok(())
}

// --- Quantity and time parsing ------------------------------------------------

/// Parse a Kubernetes quantity string into its base-unit value.
///
/// Covers decimal SI suffixes (`n`, `u`, `m`, plain, `k`, `M`, `G`, `T`, `P`,
/// `E`) and binary suffixes (`Ki` through `Ei`), including decimal mantissas
/// and scientific notation per the Kubernetes quantity grammar.
fn parse_quantity(text: &str) -> Option<f64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    // Binary suffixes are exactly two letters ending in 'i'.
    if let Some(rest) = text.strip_suffix('i') {
        let exponent = match rest.as_bytes().last() {
            Some(b'K') | Some(b'k') => 1,
            Some(b'M') => 2,
            Some(b'G') => 3,
            Some(b'T') => 4,
            Some(b'P') => 5,
            Some(b'E') => 6,
            _ => return None,
        };
        return decimal_before(text, 2).map(|value| value * 1024f64.powi(exponent));
    }
    // Decimal SI suffixes are one letter; a bare mantissa means base units.
    match text.as_bytes()[text.len() - 1] {
        b'n' => decimal_before(text, 1).map(|value| value / 1e9),
        b'u' => decimal_before(text, 1).map(|value| value / 1e6),
        b'm' => decimal_before(text, 1).map(|value| value / 1e3),
        b'k' | b'K' => decimal_before(text, 1).map(|value| value * 1e3),
        b'M' => decimal_before(text, 1).map(|value| value * 1e6),
        b'G' => decimal_before(text, 1).map(|value| value * 1e9),
        b'T' => decimal_before(text, 1).map(|value| value * 1e12),
        b'P' => decimal_before(text, 1).map(|value| value * 1e15),
        b'E' => decimal_before(text, 1).map(|value| value * 1e18),
        _ => parse_decimal(text),
    }
}

/// Parse the numeric portion of `text` excluding its last `suffix_len` bytes.
fn decimal_before(text: &str, suffix_len: usize) -> Option<f64> {
    parse_decimal(&text[..text.len() - suffix_len])
}

/// Parse the numeric portion of a quantity (digits, decimals, exponents).
fn parse_decimal(text: &str) -> Option<f64> {
    if text.is_empty() {
        return None;
    }
    // Kubernetes quantities may use exponent notation ("1e3"); anything the
    // numeric grammar rejects fails closed here.
    text.parse::<f64>().ok().filter(|value| value.is_finite())
}

/// Parse a CPU quantity into millicores, rounding to the nearest millicore.
fn quantity_millicores(text: Option<String>) -> Option<u64> {
    let cores = parse_quantity(&text?)?;
    non_negative_rounded(cores * 1000.0)
}

/// Parse a memory quantity into bytes, rounding to the nearest byte.
fn quantity_bytes(text: Option<String>) -> Option<u64> {
    non_negative_rounded(parse_quantity(&text?)?)
}

/// Parse an allocatable pod count into whole pods.
fn quantity_pods(text: &str) -> Option<u64> {
    non_negative_rounded(parse_quantity(text)?)
}

fn non_negative_rounded(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    u64::try_from(value.round().max(0.0) as u128).ok()
}

/// Parse a metrics-server `window` value (`"30s"`) into whole seconds.
fn parse_window_seconds(text: String) -> Option<u64> {
    let text = text.trim();
    let digits = text.strip_suffix('s').unwrap_or(text);
    digits.parse::<u64>().ok()
}

/// Parse an RFC 3339 UTC timestamp into unix seconds.
///
/// Accepts the `Z` form with optional fractional seconds, which is what both
/// metrics-server and core Kubernetes emit. Anything else fails closed so
/// unparseable timestamps read as stale rather than fresh.
fn parse_rfc3339_unix(text: &str) -> Option<u64> {
    let rest = text.strip_suffix('Z').or_else(|| text.strip_suffix('z'))?;
    let bytes = rest.as_bytes();
    let shaped = bytes.len() >= 19
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':';
    if !shaped {
        return None;
    }
    let year: i64 = rest.get(0..4)?.parse().ok()?;
    let month: u32 = rest.get(5..7)?.parse().ok()?;
    let day: u32 = rest.get(8..10)?.parse().ok()?;
    let hour: u64 = rest.get(11..13)?.parse().ok()?;
    let minute: u64 = rest.get(14..16)?.parse().ok()?;
    let second: u64 = rest.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days as u64 * 86_400 + hour * 3_600 + minute * 60 + second)
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

/// Howard Hinnant's civil-date-to-days conversion for UTC dates.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_quantities_parse_into_millicores() {
        assert_eq!(quantity_millicores(Some("1250m".into())), Some(1250));
        assert_eq!(quantity_millicores(Some("2".into())), Some(2000));
        assert_eq!(quantity_millicores(Some("123456789n".into())), Some(123));
        assert_eq!(quantity_millicores(Some("250u".into())), Some(0));
        assert_eq!(quantity_millicores(None), None);
        assert_eq!(quantity_millicores(Some("bogus".into())), None);
    }

    #[test]
    fn memory_quantities_parse_into_bytes() {
        assert_eq!(quantity_bytes(Some("1Mi".into())), Some(1_048_576));
        assert_eq!(quantity_bytes(Some("512Ki".into())), Some(524_288));
        assert_eq!(quantity_bytes(Some("1000".into())), Some(1000));
        assert_eq!(quantity_bytes(Some("1.5Gi".into())), Some(1_610_612_736));
        assert_eq!(quantity_bytes(Some("-4".into())), None);
    }

    #[test]
    fn windows_and_timestamps_fail_closed() {
        assert_eq!(parse_window_seconds("30s".into()), Some(30));
        assert_eq!(parse_window_seconds("45".into()), Some(45));
        assert_eq!(parse_window_seconds("1m0s".into()), None);
        // 2026-08-21T00:04:00Z in unix seconds.
        assert_eq!(
            parse_rfc3339_unix("2026-08-21T00:04:00Z"),
            Some(1_787_270_640)
        );
        assert_eq!(
            parse_rfc3339_unix("2020-01-01T00:00:00Z"),
            Some(1_577_836_800)
        );
        assert_eq!(
            parse_rfc3339_unix("2026-08-21T00:07:30.000000Z"),
            Some(1_787_270_850),
            "fractional seconds are tolerated and truncated"
        );
        assert_eq!(parse_rfc3339_unix("not-a-time"), None);
    }

    #[test]
    fn incomplete_fields_fail_closed_when_summed() {
        assert_eq!(sum_complete([]), None, "nothing reported stays absent");
        assert_eq!(sum_complete([Some(2)]), Some(2));
        assert_eq!(sum_complete([Some(2), Some(3)]), Some(5));
        assert_eq!(sum_complete([Some(2), None]), None);
        assert_eq!(sum_complete([None, Some(3)]), None);
    }
}
