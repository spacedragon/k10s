//! Window bookkeeping: stable identity, kind, geometry, z-order, and the
//! window-local content (a resource list or a pinned detail).

use serde::{Deserialize, Serialize};

use super::detail::DetailState;
use super::resource::ResourceWindowState;
use super::service::ServiceWindowState;

/// Stable identity of a workspace window. Ids are never reused after a
/// window closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowId(pub u64);

/// Workload list kinds shown under the launcher's collapsible group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    Deployments,
    Pods,
    StatefulSets,
    DaemonSets,
    Jobs,
    CronJobs,
    CustomResources,
}

impl WorkloadKind {
    pub const ALL: [WorkloadKind; 7] = [
        WorkloadKind::Deployments,
        WorkloadKind::Pods,
        WorkloadKind::StatefulSets,
        WorkloadKind::DaemonSets,
        WorkloadKind::Jobs,
        WorkloadKind::CronJobs,
        WorkloadKind::CustomResources,
    ];

    /// Default window title for this kind.
    pub fn title(self) -> &'static str {
        match self {
            WorkloadKind::Deployments => "Deployments",
            WorkloadKind::Pods => "Pods",
            WorkloadKind::StatefulSets => "StatefulSets",
            WorkloadKind::DaemonSets => "DaemonSets",
            WorkloadKind::Jobs => "Jobs",
            WorkloadKind::CronJobs => "CronJobs",
            WorkloadKind::CustomResources => "Custom Resources",
        }
    }
}

/// What a window displays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowKind {
    Overview,
    Nodes,
    Storage,
    /// The singleton Services panel.
    Services,
    Workload(WorkloadKind),
    /// A dedicated detail window pinned to one resource identity.
    Detail,
}

impl WindowKind {
    pub fn title(self) -> &'static str {
        match self {
            WindowKind::Overview => "Overview",
            WindowKind::Nodes => "Nodes",
            WindowKind::Storage => "Storage",
            WindowKind::Services => "Services",
            WindowKind::Workload(kind) => kind.title(),
            WindowKind::Detail => "Detail",
        }
    }
}

/// Position and size in egui points, plus the collapse flag. Positions are
/// relative to the workspace canvas origin so a restored layout stays
/// correct across different outer window sizes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WindowGeom {
    /// Top-left corner, `[x, y]`.
    pub position: [f32; 2],
    /// `[width, height]`.
    pub size: [f32; 2],
    pub collapsed: bool,
}

impl WindowGeom {
    /// Default geometry for the `index`-th window, staggered so freshly
    /// opened windows do not fully overlap.
    pub fn staggered(index: usize, size: [f32; 2]) -> Self {
        let step = (index % 8) as f32 * 28.0;
        Self {
            position: [64.0 + step, 48.0 + step],
            size,
            collapsed: false,
        }
    }
}

/// Window-local content: a resource list, the singleton Services list, or
/// a pinned detail.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowContent<I> {
    Resource(ResourceWindowState<I>),
    Services(ServiceWindowState<I>),
    Detail(DetailState<I>),
}

/// One open workspace window.
#[derive(Debug, Clone, PartialEq)]
pub struct Window<I> {
    pub id: WindowId,
    pub kind: WindowKind,
    pub title: String,
    pub geometry: WindowGeom,
    /// Z-order; higher means raised. Focus and opening bump this counter.
    pub z: u64,
    pub content: WindowContent<I>,
}
