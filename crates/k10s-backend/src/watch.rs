//! Bounded resource-watch registry shared by Kubernetes adapters.
//!
//! The first subscriber for a selector starts a watch: registration hands
//! back a bounded broadcast receiver plus the sender used to publish the
//! initial snapshot, which callers must send while still holding their own
//! state lock so later mutations can only enqueue deltas after that initial
//! snapshot event. Broadcasts are matched against the full resource
//! identity of the change; watches whose receivers were dropped are pruned
//! on every mutation so repeated subscribe cycles never retain dead state.

use tokio::sync::broadcast;

use crate::port::{BackendEvent, Gvk, ResourceRef};

/// Broadcast capacity per watch hub; bounded like every other queue.
pub const WATCH_CAPACITY: usize = 128;

/// The selector of one registered resource watch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchSelector {
    /// Context being watched.
    pub context: String,
    /// Type being watched.
    pub gvk: Gvk,
    /// Namespace restriction; `None` watches every namespace.
    pub namespace: Option<String>,
}

impl WatchSelector {
    /// Whether a changed resource belongs to this watch.
    #[must_use]
    pub fn matches(&self, reference: &ResourceRef) -> bool {
        reference.context == self.context
            && reference.gvk == self.gvk
            && self
                .namespace
                .as_ref()
                .is_none_or(|watched| Some(watched.as_str()) == reference.namespace.as_deref())
    }
}

#[derive(Debug)]
struct RegisteredWatch {
    selector: WatchSelector,
    sender: broadcast::Sender<BackendEvent>,
}

/// A bounded registry of active resource watches.
///
/// Clones are not supported; adapters own the hub inside their interior-
/// mutable state so registration and broadcast serialize on one lock.
#[derive(Debug)]
pub struct WatchHub {
    capacity: usize,
    watches: Vec<RegisteredWatch>,
}

impl WatchHub {
    /// Create an empty hub with the given bounded channel capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            watches: Vec::new(),
        }
    }

    /// Start a watch for `selector`: registers a bounded broadcast channel
    /// and returns `(snapshot_sender, delta_receiver)`. The caller publishes
    /// the initial snapshot through the returned sender under its own state
    /// lock before yielding.
    pub fn register(
        &mut self,
        selector: WatchSelector,
    ) -> (
        broadcast::Sender<BackendEvent>,
        broadcast::Receiver<BackendEvent>,
    ) {
        let (sender, receiver) = broadcast::channel(self.capacity);
        self.watches.push(RegisteredWatch {
            selector,
            sender: sender.clone(),
        });
        (sender, receiver)
    }

    /// Deliver an event to every matching live watch, pruning watches whose
    /// receivers were all dropped.
    pub fn broadcast(&mut self, event: BackendEvent, matches: impl Fn(&WatchSelector) -> bool) {
        self.watches.retain(|watch| {
            if watch.sender.receiver_count() == 0 {
                return false;
            }
            if matches(&watch.selector) {
                let _ = watch.sender.send(event.clone());
            }
            true
        });
    }

    /// Number of registered watches; observability for pruning behavior.
    #[must_use]
    pub fn live_count(&self) -> usize {
        self.watches.len()
    }
}

impl Default for WatchHub {
    fn default() -> Self {
        Self::new(WATCH_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{Gvk, ResourceRef};

    fn pod_ref(name: &str) -> ResourceRef {
        ResourceRef {
            context: "dev".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: name.into(),
            uid: format!("uid-{name}"),
        }
    }

    #[test]
    fn selectors_match_identity_including_namespace_restriction() {
        let all = WatchSelector {
            context: "dev".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: None,
        };
        let scoped = WatchSelector {
            namespace: Some("default".into()),
            ..all.clone()
        };
        let other_namespace = ResourceRef {
            namespace: Some("kube-system".into()),
            ..pod_ref("agent")
        };
        assert!(all.matches(&kube_ref_default()));
        assert!(scoped.matches(&kube_ref_default()));
        assert!(
            !scoped.matches(&other_namespace),
            "other namespaces stay out"
        );
        assert!(!all.matches(&ResourceRef {
            context: "prod".into(),
            ..pod_ref("agent")
        }));
    }

    fn kube_ref_default() -> ResourceRef {
        pod_ref("web")
    }

    #[test]
    fn broadcasts_reach_matching_watches_and_prune_dead_ones() {
        let mut hub = WatchHub::default();
        let (_kept_sender, mut kept) = hub.register(WatchSelector {
            context: "dev".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: None,
        });
        drop(hub.register(WatchSelector {
            context: "dev".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: Some("kube-system".into()),
        }));

        let reference = kube_ref_default();
        hub.broadcast(
            BackendEvent::Changed(crate::port::ResourceRecord {
                reference: reference.clone(),
                revision: 2,
                labels: Default::default(),
                summary: "Running".into(),
                created_at: "2026-08-21T00:00:00Z".into(),
                owner_references: Vec::new(),
                events: Vec::new(),
            }),
            |selector| selector.matches(&reference),
        );

        assert_eq!(
            hub.live_count(),
            1,
            "watches without receivers must be pruned"
        );
        match kept.try_recv() {
            Ok(BackendEvent::Changed(record)) => assert_eq!(record.reference.name, "web"),
            other => panic!("kept watch missed the delta: {other:?}"),
        }
    }
}
