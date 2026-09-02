//! Context-scoped Kubernetes API transport telemetry.

use serde::{Deserialize, Serialize};

/// Envelope event kind carrying a [`TrafficSample`].
pub const TRAFFIC_EVENT_UPDATED: &str = "traffic.updated";

/// Selects transport telemetry for one kubeconfig context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficWatchSpec {
    pub context: String,
}

impl TrafficWatchSpec {
    #[must_use]
    pub fn new(context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
        }
    }
}

/// One cumulative and interval sample from the server-side Kubernetes client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficSample {
    pub context: String,
    pub captured_at_ms: u64,
    pub upload_bytes_per_second: u64,
    pub download_bytes_per_second: u64,
    pub uploaded_bytes_total: u64,
    pub downloaded_bytes_total: u64,
    pub requests_total: u64,
    pub active_requests: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_uses_stable_camel_case_wire_fields() {
        let sample = TrafficSample {
            context: "dev".into(),
            captured_at_ms: 42,
            upload_bytes_per_second: 7,
            download_bytes_per_second: 11,
            uploaded_bytes_total: 70,
            downloaded_bytes_total: 110,
            requests_total: 3,
            active_requests: 1,
        };
        let value = serde_json::to_value(&sample).unwrap();
        assert_eq!(value["downloadBytesPerSecond"], 11);
        assert_eq!(value["activeRequests"], 1);
        assert_eq!(
            serde_json::from_value::<TrafficSample>(value).unwrap(),
            sample
        );
    }
}
