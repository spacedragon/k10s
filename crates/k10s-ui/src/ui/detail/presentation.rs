//! The frozen, feed-independent input to the shared detail frame.
//!
//! Callers resolve protocol feeds exactly once before invoking a kind body.
//! That keeps integrated and dedicated views observationally identical.

use k10s_protocol::{ResourceDetailResponse, ResourceIdentity};

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

/// Everything a detail frame and a kind body may observe in one render.
///
/// In particular, kind bodies receive no [`ResourceFeed`], preventing a
/// second, divergent detail lookup after the pinned identity was resolved.
pub(crate) struct DetailPresentationInput<'a> {
    pub identity: &'a ResourceIdentity,
    pub primary: DetailPrimary<'a>,
    pub metrics: DetailMetrics<'a>,
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
            relations: feed.relations.get(identity),
            freshness,
            gone,
            mutations_allowed,
            port_forward_available: feed.port_forward_available,
            port_forward_sessions: &feed.port_forward_sessions,
            port_forward_error: feed.port_forward_error.as_deref(),
        })
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
