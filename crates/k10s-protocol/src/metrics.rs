//! Normalized metrics payloads for the k10s control protocol.
//!
//! Metrics are availability-gated: a value is present only when the backend
//! actually collected it, and UI code must never render a missing metric as
//! zero.

use serde::{Deserialize, Serialize};

use crate::resource::ResourceIdentity;

/// Availability of a metrics sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricsAvailability {
    /// All designed values were collected.
    Available,
    /// Some values were collected; missing values stay absent.
    Partial,
    /// No fresh values exist for the object.
    Unavailable,
}

/// A normalized metrics sample for one pod.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodMetrics {
    /// Whether and how completely this sample was collected.
    pub availability: MetricsAvailability,
    /// CPU usage in millicores, absent when not collected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millicores: Option<u64>,
    /// Working-set memory in bytes, absent when not collected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// Deterministic collection timestamp formatted as RFC 3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collected_at: Option<String>,
}

impl PodMetrics {
    /// A sample for which nothing could be collected.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            availability: MetricsAvailability::Unavailable,
            cpu_millicores: None,
            memory_bytes: None,
            collected_at: None,
        }
    }
}

/// Response payload for a single-pod metrics query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetricsResponse {
    /// Identity of the sampled pod.
    pub identity: ResourceIdentity,
    /// Availability-gated metrics sample.
    pub metrics: PodMetrics,
}
