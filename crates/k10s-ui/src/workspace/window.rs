//! Window bookkeeping: stable identity, kind, geometry, z-order, and the
//! window-local content (a resource list or a pinned detail).

use serde::{Deserialize, Serialize};

use super::detail::DetailState;
use super::port_forward::PortForwardWindowState;
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
    Events,
    Namespaces,
    Deployments,
    Pods,
    StatefulSets,
    DaemonSets,
    Jobs,
    CronJobs,
    CustomResources,
    Ingresses,
    Endpoints,
    NetworkPolicies,
    ConfigMaps,
    Secrets,
    PersistentVolumeClaims,
    PersistentVolumes,
    StorageClasses,
    ServiceAccounts,
    Roles,
    RoleBindings,
}

impl WorkloadKind {
    /// Every resource-backed launcher entry, including cluster, workload,
    /// network, config, storage, and access resources.
    pub const TAXONOMY: [WorkloadKind; 20] = [
        WorkloadKind::Events,
        WorkloadKind::Namespaces,
        WorkloadKind::Deployments,
        WorkloadKind::Pods,
        WorkloadKind::StatefulSets,
        WorkloadKind::DaemonSets,
        WorkloadKind::Jobs,
        WorkloadKind::CronJobs,
        WorkloadKind::CustomResources,
        WorkloadKind::Ingresses,
        WorkloadKind::Endpoints,
        WorkloadKind::NetworkPolicies,
        WorkloadKind::ConfigMaps,
        WorkloadKind::Secrets,
        WorkloadKind::PersistentVolumeClaims,
        WorkloadKind::PersistentVolumes,
        WorkloadKind::StorageClasses,
        WorkloadKind::ServiceAccounts,
        WorkloadKind::Roles,
        WorkloadKind::RoleBindings,
    ];

    pub const ALL: [WorkloadKind; 7] = [
        WorkloadKind::Deployments,
        WorkloadKind::Pods,
        WorkloadKind::StatefulSets,
        WorkloadKind::DaemonSets,
        WorkloadKind::Jobs,
        WorkloadKind::CronJobs,
        WorkloadKind::CustomResources,
    ];

    pub const NETWORK: [WorkloadKind; 3] = [
        WorkloadKind::Ingresses,
        WorkloadKind::Endpoints,
        WorkloadKind::NetworkPolicies,
    ];
    pub const CONFIG: [WorkloadKind; 2] = [WorkloadKind::ConfigMaps, WorkloadKind::Secrets];
    pub const STORAGE: [WorkloadKind; 3] = [
        WorkloadKind::PersistentVolumeClaims,
        WorkloadKind::PersistentVolumes,
        WorkloadKind::StorageClasses,
    ];
    pub const ACCESS: [WorkloadKind; 3] = [
        WorkloadKind::ServiceAccounts,
        WorkloadKind::Roles,
        WorkloadKind::RoleBindings,
    ];

    /// Default window title for this kind.
    pub fn title(self) -> &'static str {
        match self {
            WorkloadKind::Events => "Events",
            WorkloadKind::Namespaces => "Namespaces",
            WorkloadKind::Deployments => "Deployments",
            WorkloadKind::Pods => "Pods",
            WorkloadKind::StatefulSets => "StatefulSets",
            WorkloadKind::DaemonSets => "DaemonSets",
            WorkloadKind::Jobs => "Jobs",
            WorkloadKind::CronJobs => "CronJobs",
            WorkloadKind::CustomResources => "Custom Resources",
            WorkloadKind::Ingresses => "Ingresses",
            WorkloadKind::Endpoints => "Endpoints",
            WorkloadKind::NetworkPolicies => "NetworkPolicies",
            WorkloadKind::ConfigMaps => "ConfigMaps",
            WorkloadKind::Secrets => "Secrets",
            WorkloadKind::PersistentVolumeClaims => "PersistentVolumeClaims",
            WorkloadKind::PersistentVolumes => "PersistentVolumes",
            WorkloadKind::StorageClasses => "StorageClasses",
            WorkloadKind::ServiceAccounts => "ServiceAccounts",
            WorkloadKind::Roles => "Roles",
            WorkloadKind::RoleBindings => "RoleBindings",
        }
    }

    pub fn namespaced(self) -> bool {
        !matches!(
            self,
            WorkloadKind::Namespaces
                | WorkloadKind::PersistentVolumes
                | WorkloadKind::StorageClasses
        )
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
    /// The singleton global port-forward session manager.
    PortForwards,
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
            WindowKind::PortForwards => "Port Forwards",
            WindowKind::Workload(kind) => kind.title(),
            WindowKind::Detail => "Detail",
        }
    }

    /// Smallest usable outer size, shared by layout commands and rendering.
    pub const fn min_size(self) -> [f32; 2] {
        match self {
            Self::Workload(_) | Self::Detail => [672.0, 424.0],
            Self::Overview | Self::Nodes | Self::Storage | Self::Services | Self::PortForwards => {
                [480.0, 320.0]
            }
        }
    }
}

/// Position and size in egui points, plus the collapse flag. Positions are
/// relative to the workspace canvas origin so a restored layout stays
/// correct across different outer window sizes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Deterministic row-major grid. If the canvas cannot fit practical
    /// minima, cells retain those minima and form an intentional overflow
    /// surface instead of shrinking windows into unusable slivers.
    pub fn tiled(index: usize, count: usize, canvas: [f32; 2], minimum: [f32; 2]) -> Self {
        let columns = (count as f32).sqrt().ceil() as usize;
        let rows = count.div_ceil(columns);
        let width = (canvas[0] / columns as f32).max(minimum[0]);
        let height = (canvas[1] / rows as f32).max(minimum[1]);
        Self {
            position: [
                (index % columns) as f32 * width,
                (index / columns) as f32 * height,
            ],
            size: [width, height],
            collapsed: false,
        }
    }

    pub fn cascade(index: usize, canvas: [f32; 2], minimum: [f32; 2]) -> Self {
        let offset = index as f32 * 28.0;
        Self {
            position: [offset, offset],
            size: [
                (canvas[0] - offset).max(minimum[0]),
                (canvas[1] - offset).max(minimum[1]),
            ],
            collapsed: false,
        }
    }

    pub fn focused(canvas: [f32; 2], minimum: [f32; 2]) -> Self {
        Self {
            position: [0.0, 0.0],
            size: [canvas[0].max(minimum[0]), canvas[1].max(minimum[1])],
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
    PortForwards(PortForwardWindowState),
    Detail(DetailState<I>),
}

/// One open workspace window.
#[derive(Debug, Clone, PartialEq)]
pub struct Window<I> {
    pub id: WindowId,
    pub kind: WindowKind,
    pub title: String,
    pub geometry: WindowGeom,
    /// True only until the freshly-created geometry is explicitly replaced.
    /// This is runtime-only: restored snapshots always retain their geometry.
    pub initial_geometry: bool,
    /// Incremented when a layout command must override egui's remembered size.
    pub layout_revision: u64,
    /// Z-order; higher means raised. Focus and opening bump this counter.
    pub z: u64,
    pub content: WindowContent<I>,
}
