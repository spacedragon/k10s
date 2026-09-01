//! The frozen, feed-independent input to the shared detail frame.
//!
//! Callers resolve protocol feeds exactly once before invoking a kind body.
//! That keeps integrated and dedicated views observationally identical.

use k10s_protocol::{OwnerReference, ResourceDetailResponse, ResourceIdentity, ResourceProjection};
use web_time::{SystemTime, UNIX_EPOCH};

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
    pub tone: DetailVitalTone,
    pub shape: Option<DetailVitalShape>,
}

impl DetailVital {
    pub(crate) fn new(label: &'static str, value: impl Into<String>) -> Self {
        Self {
            label,
            value: value.into(),
            tone: DetailVitalTone::default(),
            shape: None,
        }
    }
}

/// Semantic color applied to a detail vital.
#[allow(dead_code)] // Frozen extension surface for the final kind renderers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailVitalTone {
    #[default]
    Neutral,
    Healthy,
    Warning,
    Danger,
}

/// Visible marker paired with a detail vital's semantic color.
#[allow(dead_code)] // Frozen extension surface for the final kind renderers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailVitalShape {
    Dot,
    Triangle,
    Cross,
}

impl DetailVitalShape {
    pub(crate) const fn glyph(self) -> &'static str {
        match self {
            Self::Dot => "●",
            Self::Triangle => "▲",
            Self::Cross => "✕",
        }
    }
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
    pub freshness: DetailFreshness<'a>,
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

/// Effective detail freshness after primary loading and exact source
/// authority have been combined for the pinned identity.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DetailFreshness<'a> {
    Loading,
    Unavailable,
    Gone,
    Source(&'a WindowFreshness),
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
    pub now: SystemTime,
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
        let mutations_allowed =
            mutations_allowed && !gone && matches!(primary, DetailPrimary::Loaded(_));
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
            now: SystemTime::now(),
            gone,
            mutations_allowed,
            // Starting a forward creates new backend state and therefore
            // shares exact mutation authority. Existing sessions remain
            // visible so Stop can still perform safe cleanup when authority
            // to create new sessions has been revoked.
            port_forward_available: feed.port_forward_available && mutations_allowed,
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
            typed_vitals(self.identity, view, self.metrics, self.now);
        DetailFrameProjection {
            identity: self.identity,
            freshness: if self.gone {
                DetailFreshness::Gone
            } else {
                match self.primary {
                    DetailPrimary::Loading => DetailFreshness::Loading,
                    DetailPrimary::Failed(_) => DetailFreshness::Unavailable,
                    DetailPrimary::Loaded(_) => self
                        .freshness
                        .map_or(DetailFreshness::Unavailable, DetailFreshness::Source),
                }
            },
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
    now: SystemTime,
) -> (Vec<DetailVital>, Vec<DetailVital>, Option<&'static str>) {
    match view.and_then(|view| view.projection.as_ref()) {
        Some(ResourceProjection::Pod(pod)) => (
            vec![
                vital("Status", pod.phase.as_deref()),
                DetailVital::new("Ready", pair(pod.ready_containers, pod.total_containers)),
                vital_number("Restarts", pod.restart_count),
                age_vital(pod.created_at.as_deref(), now),
            ],
            vec![
                vital("Node", pod.node_name.as_deref()),
                vital("Pod IP", pod.pod_ip.as_deref()),
            ],
            Some("Pod"),
        ),
        Some(ResourceProjection::Deployment(deployment)) => (
            vec![
                DetailVital::new(
                    "Rollout",
                    deployment
                        .conditions
                        .iter()
                        .find(|condition| condition.condition_type == "Progressing")
                        .and_then(|condition| condition.reason.as_deref())
                        .unwrap_or("—")
                        .to_owned(),
                ),
                DetailVital::new(
                    "Ready",
                    pair(deployment.ready_replicas, deployment.desired_replicas),
                ),
                vital_number("Up-to-date", deployment.updated_replicas),
                vital_number("Available", deployment.available_replicas),
            ],
            vec![
                vital("Strategy", deployment.strategy.as_deref()),
                age_vital(deployment.created_at.as_deref(), now),
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
                    DetailVital::new("Ready", "—"),
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
                    DetailVital::new("Ready", "—"),
                    vital_number("Up-to-date", None),
                    vital_number("Available", None),
                ],
                vec![vital("Strategy", None), vital("Age", None)],
                Some("Deployment"),
            )
        }
        _ => (
            vec![vital("Status", generic.status), age_vital(generic.age, now)],
            Vec::new(),
            None,
        ),
    }
}

fn vital(label: &'static str, value: Option<&str>) -> DetailVital {
    DetailVital::new(label, value.unwrap_or("—"))
}

fn age_vital(created_at: Option<&str>, now: SystemTime) -> DetailVital {
    DetailVital::new("Age", format_age(created_at, now))
}

pub(crate) fn format_age(created_at: Option<&str>, now: SystemTime) -> String {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;
    const WEEK: i64 = 7 * DAY;

    let Some(created_at) = created_at else {
        return "—".to_owned();
    };
    let Ok(created_at) = created_at.parse::<jiff::Timestamp>() else {
        return "—".to_owned();
    };
    let Ok(now_since_epoch) = now.duration_since(UNIX_EPOCH) else {
        return "—".to_owned();
    };
    let Ok(now_seconds) = i64::try_from(now_since_epoch.as_secs()) else {
        return "—".to_owned();
    };
    let Ok(now) = jiff::Timestamp::new(now_seconds, now_since_epoch.subsec_nanos() as i32) else {
        return "—".to_owned();
    };
    let age = now.duration_since(created_at);
    if age.is_negative() {
        return "—".to_owned();
    }
    let age = age.as_secs();
    if age >= WEEK {
        return format!("{}d", age / DAY);
    }
    if age >= DAY {
        let days = age / DAY;
        let hours = age % DAY / HOUR;
        return if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d {hours}h")
        };
    }
    if age >= HOUR {
        let hours = age / HOUR;
        let minutes = age % HOUR / MINUTE;
        return if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        };
    }
    if age >= MINUTE {
        return format!("{}m", age / MINUTE);
    }
    "<1m".to_owned()
}

pub(crate) fn system_time_from_rfc3339(value: &str) -> Option<SystemTime> {
    let timestamp = value.parse::<jiff::Timestamp>().ok()?;
    let seconds = u64::try_from(timestamp.as_second()).ok()?;
    let nanos = u32::try_from(timestamp.subsec_nanosecond()).ok()?;
    UNIX_EPOCH.checked_add(std::time::Duration::new(seconds, nanos))
}

fn vital_number(label: &'static str, value: Option<u32>) -> DetailVital {
    DetailVital::new(
        label,
        value.map_or_else(|| "—".to_owned(), |value| value.to_string()),
    )
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k10s_protocol::{
        BackendRevision, DeploymentProjection, EventsCondition, GroupVersionKind, PodProjection,
        ResourceCapabilities, ResourceDetailResponse, ResourceIdentity, ResourceProjection,
    };
    use web_time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{DetailExpansionState, DetailMetrics, DetailPresentationInput, DetailPrimary};

    const NOW_SECONDS: u64 = 32 * 24 * 60 * 60;

    fn fixed_now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(NOW_SECONDS)
    }

    #[test]
    fn backend_timestamp_provides_a_deterministic_render_clock() {
        let parsed = super::system_time_from_rfc3339("1970-01-02T00:00:00Z").unwrap();
        assert_eq!(parsed.duration_since(UNIX_EPOCH).unwrap().as_secs(), 86_400);
        assert!(super::system_time_from_rfc3339("not-rfc3339").is_none());
    }

    #[test]
    fn age_formatter_uses_compact_boundaries() {
        for (created_at, expected) in [
            ("1970-01-02T00:00:00Z", "31d"),
            ("1970-01-26T00:00:00Z", "7d"),
            ("1970-01-26T01:00:00Z", "6d 23h"),
            ("1970-01-28T22:00:00Z", "4d 2h"),
            ("1970-02-01T00:00:00Z", "1d"),
            ("1970-02-01T22:59:00Z", "1h 1m"),
            ("1970-02-01T23:42:00Z", "18m"),
            ("1970-02-01T23:59:00Z", "1m"),
            ("1970-02-01T23:59:01Z", "<1m"),
        ] {
            assert_eq!(super::format_age(Some(created_at), fixed_now()), expected);
        }
    }

    #[test]
    fn age_formatter_rejects_missing_invalid_and_future_timestamps() {
        assert_eq!(super::format_age(None, fixed_now()), "—");
        assert_eq!(super::format_age(Some("not-rfc3339"), fixed_now()), "—");
        assert_eq!(
            super::format_age(Some("1970-02-02T00:00:01Z"), fixed_now()),
            "—"
        );
    }

    #[test]
    fn age_formatter_distinguishes_subsecond_future_now_and_past() {
        assert_eq!(
            super::format_age(Some("1970-02-02T00:00:00.000000001Z"), fixed_now()),
            "—"
        );
        assert_eq!(
            super::format_age(Some("1970-02-02T00:00:00Z"), fixed_now()),
            "<1m"
        );
        assert_eq!(
            super::format_age(Some("1970-02-01T23:59:59.999999999Z"), fixed_now()),
            "<1m"
        );
    }

    #[test]
    fn typed_pod_and_deployment_vitals_format_age_from_the_injected_clock() {
        let pod = detail(ResourceProjection::Pod(PodProjection {
            phase: Some("Running".into()),
            ready_containers: Some(1),
            total_containers: Some(1),
            restart_count: Some(0),
            containers: Vec::new(),
            conditions: Vec::new(),
            node_name: None,
            pod_ip: None,
            host_ip: None,
            qos_class: None,
            priority: None,
            service_account: None,
            restart_policy: None,
            ports: Vec::new(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            created_at: Some("1970-02-01T23:42:00Z".into()),
        }));
        let pod_input = input(&pod);
        let pod_frame = pod_input.frame_projection(DetailExpansionState::default());
        assert_eq!(
            labels(&pod_frame.visible_vitals),
            ["Status", "Ready", "Restarts", "Age"]
        );
        assert_eq!(labels(&pod_frame.overflow_vitals), ["Node", "Pod IP"]);
        assert_eq!(vital(&pod_frame.visible_vitals, "Age"), "18m");

        let deployment = detail(ResourceProjection::Deployment(DeploymentProjection {
            desired_replicas: Some(2),
            ready_replicas: Some(2),
            updated_replicas: Some(2),
            available_replicas: Some(2),
            strategy: Some("RollingUpdate".into()),
            selector: BTreeMap::new(),
            max_surge: None,
            max_unavailable: None,
            conditions: Vec::new(),
            template_containers: Vec::new(),
            template_labels: BTreeMap::new(),
            template_annotations: BTreeMap::new(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            created_at: Some("1970-01-28T22:00:00Z".into()),
        }));
        let deployment_input = input(&deployment);
        let deployment_frame = deployment_input.frame_projection(DetailExpansionState {
            more_vitals: true,
            ..DetailExpansionState::default()
        });
        assert_eq!(
            labels(&deployment_frame.visible_vitals),
            ["Rollout", "Ready", "Up-to-date", "Available"]
        );
        assert_eq!(
            labels(&deployment_frame.overflow_vitals),
            ["Strategy", "Age"]
        );
        assert_eq!(vital(&deployment_frame.overflow_vitals, "Age"), "4d 2h");
    }

    #[test]
    fn generic_vitals_format_resource_created_at() {
        let mut generic = detail(ResourceProjection::Pod(PodProjection {
            phase: None,
            ready_containers: None,
            total_containers: None,
            restart_count: None,
            containers: Vec::new(),
            conditions: Vec::new(),
            node_name: None,
            pod_ip: None,
            host_ip: None,
            qos_class: None,
            priority: None,
            service_account: None,
            restart_policy: None,
            ports: Vec::new(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            created_at: None,
        }));
        generic.identity.gvk = GroupVersionKind {
            group: "example.io".into(),
            version: "v1".into(),
            kind: "Widget".into(),
        };
        generic.projection = None;
        generic.created_at = "1970-01-02T00:00:00Z".into();

        let input = input(&generic);
        let frame = input.frame_projection(DetailExpansionState::default());
        assert_eq!(labels(&frame.visible_vitals), ["Status", "Age"]);
        assert!(frame.overflow_vitals.is_empty());
        assert_eq!(vital(&frame.visible_vitals, "Age"), "31d");
    }

    fn detail(projection: ResourceProjection) -> ResourceDetailResponse {
        ResourceDetailResponse {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: match projection {
                    ResourceProjection::Pod(_) => GroupVersionKind::core("v1", "Pod"),
                    ResourceProjection::Deployment(_) => GroupVersionKind {
                        group: "apps".into(),
                        version: "v1".into(),
                        kind: "Deployment".into(),
                    },
                    ResourceProjection::ReplicaSet(_) | ResourceProjection::Service(_) => {
                        unreachable!("test only builds Pod and Deployment projections")
                    }
                },
                namespace: Some("default".into()),
                name: "sample".into(),
                uid: "uid-sample".into(),
            },
            revision: BackendRevision::new(1),
            created_at: "1970-02-01T23:42:00Z".into(),
            owner_references: Vec::new(),
            sections: Vec::new(),
            events_condition: EventsCondition::Available,
            events: Vec::new(),
            related: Vec::new(),
            capabilities: ResourceCapabilities::default(),
            manifest: String::new(),
            projection: Some(projection),
        }
    }

    fn input(detail: &ResourceDetailResponse) -> DetailPresentationInput<'_> {
        DetailPresentationInput {
            identity: &detail.identity,
            primary: DetailPrimary::Loaded(detail),
            metrics: DetailMetrics {
                status: None,
                age: Some(detail.created_at.as_str()),
            },
            resource_metrics: None,
            relations: None,
            freshness: None,
            now: fixed_now(),
            gone: false,
            mutations_allowed: false,
            port_forward_available: false,
            port_forward_sessions: &[],
            port_forward_error: None,
        }
    }

    fn vital<'a>(vitals: &'a [super::DetailVital], label: &str) -> &'a str {
        vitals
            .iter()
            .find(|vital| vital.label == label)
            .map(|vital| vital.value.as_str())
            .expect("vital is projected")
    }

    fn labels(vitals: &[super::DetailVital]) -> Vec<&'static str> {
        vitals.iter().map(|vital| vital.label).collect()
    }
}
