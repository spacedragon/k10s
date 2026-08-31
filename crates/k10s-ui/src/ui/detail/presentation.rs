//! The frozen, feed-independent input to the shared detail frame.
//!
//! Callers resolve protocol feeds exactly once before invoking a kind body.
//! That keeps integrated and dedicated views observationally identical.

use k10s_protocol::{OwnerReference, ResourceDetailResponse, ResourceIdentity, ResourceProjection};

use crate::ui::resource_window::RowIdentity;
use crate::ui::{PrimaryDetailState, RelationState, ResourceFeed, WindowFreshness};

/// Primary-detail lifecycle projected for a single pinned identity.
#[derive(Clone, Copy)]
pub(crate) enum DetailPrimary<'a> {
    Loading,
    Loaded(&'a ResourceDetailResponse),
    Failed(&'a crate::ui::SafeUiError),
}

/// Small, exact metrics used by the generic shared chrome.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DetailMetrics<'a> {
    pub status: Option<&'a str>,
    pub age: Option<&'a str>,
}

/// Shared, per-window transient expansion state consumed by kind renderers.
#[allow(dead_code)] // Labels/metadata are frozen Task-5 extension state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DetailExpansionState {
    pub more_vitals: bool,
    pub labels: bool,
    pub metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DetailVital {
    pub label: &'static str,
    pub value: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DetailActionProjection<'a> {
    pub can_scale: bool,
    pub can_restart: bool,
    pub can_delete: bool,
    pub verified_owner: Option<&'a OwnerReference>,
}

/// Frozen projection shared by the frame and Pod/Deployment renderers.
/// Every field is resolved once from the exact pinned identity.
pub(crate) struct DetailFrameProjection<'a> {
    pub identity: &'a ResourceIdentity,
    pub freshness: Option<&'a WindowFreshness>,
    #[allow(dead_code)] // Frozen for the Task-5 Pod renderer.
    pub resource_metrics: Option<&'a k10s_protocol::ResourceMetricsResponse>,
    #[allow(dead_code)] // Frozen for the Task-5 Pod/Deployment renderers.
    pub relations: Option<&'a RelationState>,
    pub actions: DetailActionProjection<'a>,
    pub shortcut_labels: &'static [&'static str],
    pub visible_vitals: Vec<DetailVital>,
    pub overflow_vitals: Vec<DetailVital>,
    pub vital_expansion_label: Option<&'static str>,
    pub expansion: DetailExpansionState,
}

/// Everything a detail frame and a kind body may observe in one render.
///
/// In particular, kind bodies receive no [`ResourceFeed`], preventing a
/// second, divergent detail lookup after the pinned identity was resolved.
pub(crate) struct DetailPresentationInput<'a> {
    pub identity: &'a ResourceIdentity,
    pub primary: DetailPrimary<'a>,
    pub metrics: DetailMetrics<'a>,
    pub resource_metrics: Option<&'a k10s_protocol::ResourceMetricsResponse>,
    pub relations: Option<&'a RelationState>,
    pub freshness: Option<&'a WindowFreshness>,
    pub gone: bool,
    pub mutations_allowed: bool,
    pub port_forward_available: bool,
    pub port_forward_sessions: &'a [k10s_protocol::PortForwardSession],
    pub port_forward_error: Option<&'a str>,
}

impl<'a> DetailPresentationInput<'a> {
    pub(crate) fn from_feed<I: RowIdentity>(
        detail: &'a crate::workspace::DetailState<I>,
        feed: &'a ResourceFeed,
        gone: bool,
        freshness: Option<&'a WindowFreshness>,
        mutations_allowed: bool,
    ) -> Option<Self> {
        let identity = detail.identity.as_row_identity()?;
        let primary = match feed.primary_details.get(identity) {
            Some(PrimaryDetailState::Loaded(view)) => DetailPrimary::Loaded(view),
            Some(PrimaryDetailState::Failed(error)) => DetailPrimary::Failed(error),
            Some(PrimaryDetailState::Loading) => DetailPrimary::Loading,
            None => match feed.details.get(identity) {
                Some(view) => DetailPrimary::Loaded(view),
                None => DetailPrimary::Loading,
            },
        };
        let view = match primary {
            DetailPrimary::Loaded(view) => Some(view),
            DetailPrimary::Loading | DetailPrimary::Failed(_) => None,
        };
        let metrics = DetailMetrics {
            status: view.and_then(status_summary),
            age: view.map(|view| view.created_at.as_str()),
        };
        Some(Self {
            identity,
            primary,
            metrics,
            resource_metrics: feed
                .metrics
                .get(identity)
                .filter(|metrics| metrics.identity == *identity),
            relations: feed.relations.get(identity),
            freshness,
            gone,
            mutations_allowed,
            port_forward_available: feed.port_forward_available,
            port_forward_sessions: &feed.port_forward_sessions,
            port_forward_error: feed.port_forward_error.as_deref(),
        })
    }

    pub(crate) fn frame_projection(
        &'a self,
        expansion: DetailExpansionState,
    ) -> DetailFrameProjection<'a> {
        let view = match self.primary {
            DetailPrimary::Loaded(view) => Some(view),
            DetailPrimary::Loading | DetailPrimary::Failed(_) => None,
        };
        let capabilities = view.map(|view| &view.capabilities);
        let verified_owner = self.verified_owner();
        let (visible_vitals, overflow_vitals, vital_expansion_label) =
            typed_vitals(self.identity, view, self.metrics);
        DetailFrameProjection {
            identity: self.identity,
            freshness: self.freshness,
            resource_metrics: self.resource_metrics,
            relations: self.relations,
            actions: DetailActionProjection {
                can_scale: capabilities.is_some_and(|caps| caps.can_scale),
                can_restart: capabilities.is_some_and(|caps| caps.can_restart),
                can_delete: capabilities.is_some_and(|caps| caps.can_delete),
                verified_owner,
            },
            shortcut_labels: super::shortcut_labels_for(
                &self.identity.gvk,
                verified_owner.is_some(),
            ),
            visible_vitals,
            overflow_vitals,
            vital_expansion_label,
            expansion,
        }
    }

    pub(crate) fn verified_owner(&self) -> Option<&OwnerReference> {
        let DetailPrimary::Loaded(view) = self.primary else {
            return None;
        };
        view.owner_references.iter().find(|owner| {
            owner.controller
                && !owner.uid.is_empty()
                && !owner.name.is_empty()
                && !owner.gvk.kind.is_empty()
        })
    }
}

pub(crate) fn owner_identity(
    identity: &ResourceIdentity,
    owner: &OwnerReference,
) -> ResourceIdentity {
    ResourceIdentity {
        context: identity.context.clone(),
        gvk: owner.gvk.clone(),
        namespace: identity.namespace.clone(),
        name: owner.name.clone(),
        uid: owner.uid.clone(),
    }
}

fn typed_vitals(
    identity: &ResourceIdentity,
    view: Option<&ResourceDetailResponse>,
    generic: DetailMetrics<'_>,
) -> (Vec<DetailVital>, Vec<DetailVital>, Option<&'static str>) {
    match view.and_then(|view| view.projection.as_ref()) {
        Some(ResourceProjection::Pod(pod)) => (
            vec![
                vital("Status", pod.phase.as_deref()),
                DetailVital {
                    label: "Ready",
                    value: pair(pod.ready_containers, pod.total_containers),
                },
                vital_number("Restarts", pod.restart_count),
                vital("Age", pod.created_at.as_deref()),
            ],
            vec![
                vital("Node", pod.node_name.as_deref()),
                vital("Pod IP", pod.pod_ip.as_deref()),
            ],
            Some("Pod"),
        ),
        Some(ResourceProjection::Deployment(deployment)) => (
            vec![
                DetailVital {
                    label: "Rollout",
                    value: deployment
                        .conditions
                        .iter()
                        .find(|condition| condition.condition_type == "Progressing")
                        .and_then(|condition| condition.reason.as_deref())
                        .unwrap_or("—")
                        .to_owned(),
                },
                DetailVital {
                    label: "Ready",
                    value: pair(deployment.ready_replicas, deployment.desired_replicas),
                },
                vital_number("Up-to-date", deployment.updated_replicas),
                vital_number("Available", deployment.available_replicas),
            ],
            vec![
                vital("Strategy", deployment.strategy.as_deref()),
                vital("Age", deployment.created_at.as_deref()),
            ],
            Some("Deployment"),
        ),
        _ if identity.gvk.group.is_empty()
            && identity.gvk.version == "v1"
            && identity.gvk.kind == "Pod" =>
        {
            (
                vec![
                    vital("Status", None),
                    DetailVital {
                        label: "Ready",
                        value: "—".into(),
                    },
                    vital_number("Restarts", None),
                    vital("Age", None),
                ],
                vec![vital("Node", None), vital("Pod IP", None)],
                Some("Pod"),
            )
        }
        _ if identity.gvk.group == "apps"
            && identity.gvk.version == "v1"
            && identity.gvk.kind == "Deployment" =>
        {
            (
                vec![
                    vital("Rollout", None),
                    DetailVital {
                        label: "Ready",
                        value: "—".into(),
                    },
                    vital_number("Up-to-date", None),
                    vital_number("Available", None),
                ],
                vec![vital("Strategy", None), vital("Age", None)],
                Some("Deployment"),
            )
        }
        _ => (
            vec![vital("Status", generic.status), vital("Age", generic.age)],
            Vec::new(),
            None,
        ),
    }
}

fn vital(label: &'static str, value: Option<&str>) -> DetailVital {
    DetailVital {
        label,
        value: value.unwrap_or("—").to_owned(),
    }
}

fn vital_number(label: &'static str, value: Option<u32>) -> DetailVital {
    DetailVital {
        label,
        value: value.map_or_else(|| "—".to_owned(), |value| value.to_string()),
    }
}

fn pair(left: Option<u32>, right: Option<u32>) -> String {
    match (left, right) {
        (Some(left), Some(right)) => format!("{left}/{right}"),
        _ => "—".to_owned(),
    }
}

fn status_summary(view: &ResourceDetailResponse) -> Option<&str> {
    view.sections
        .iter()
        .find(|section| section.title == "Overview")
        .and_then(|section| {
            section
                .rows
                .iter()
                .find(|row| row.label == "Status")
                .map(|row| row.value.as_str())
        })
}
