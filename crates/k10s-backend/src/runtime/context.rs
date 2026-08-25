//! The prepare-then-commit context registry served by adapters.
//!
//! Adapters never hand the kernel raw Kubernetes data: they build a candidate
//! [`ContextRegistry`] from validated, credential-free summaries (prepare),
//! and only on success commit it as their immutable bootstrap state. A failed
//! prepare leaves nothing to observe — no partial or ambiguous registries.

use std::collections::HashSet;

use crate::port::{AdapterError, BackendError, ContextAvailability, ContextInfo};

/// Committed registry of safe context summaries exposed at bootstrap.
///
/// Built once through [`ContextRegistry::prepare`]; readers always see a
/// complete, consistent snapshot. Summaries carry names and cluster references
/// only — never tokens or certificate material.
#[derive(Debug, Clone)]
pub struct ContextRegistry {
    contexts: Vec<ContextInfo>,
    generation: u64,
}

impl ContextRegistry {
    /// Prepare a candidate registry from validated summaries.
    ///
    /// Rejects corrupt input (duplicate context names, more than one current
    /// context) before anything is committed so adapters fail at startup with
    /// a typed error instead of serving an ambiguous list.
    pub fn prepare(summaries: Vec<ContextInfo>) -> Result<Self, AdapterError> {
        if summaries.is_empty() {
            return Err(AdapterError::InvalidContextSummaries {
                detail: "no context summaries to commit".into(),
            });
        }

        let mut seen = HashSet::with_capacity(summaries.len());
        for summary in &summaries {
            if !seen.insert(summary.name.as_str()) {
                return Err(AdapterError::InvalidContextSummaries {
                    detail: format!("duplicate context name '{}'", summary.name),
                });
            }
        }

        let current_count = summaries.iter().filter(|s| s.is_current).count();
        if current_count > 1 {
            return Err(AdapterError::InvalidContextSummaries {
                detail: format!("{current_count} contexts marked as current"),
            });
        }

        Ok(Self {
            contexts: summaries,
            generation: 0,
        })
    }

    /// All committed context summaries in stable order.
    #[must_use]
    pub fn contexts(&self) -> &[ContextInfo] {
        &self.contexts
    }

    /// Names of all committed contexts in stable order.
    #[must_use]
    pub fn context_names(&self) -> Vec<&str> {
        self.contexts.iter().map(|c| c.name.as_str()).collect()
    }

    /// Look up one committed summary by name.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&ContextInfo> {
        self.contexts.iter().find(|context| context.name == name)
    }

    /// Clone a generation-tagged snapshot for work performed without holding
    /// the registry lock.
    #[must_use]
    pub fn snapshot(&self) -> (u64, Vec<ContextInfo>) {
        (self.generation, self.contexts.clone())
    }

    /// Commit a successful credential probe when its snapshot is still current.
    pub fn mark_available(&mut self, generation: u64, name: &str) -> bool {
        if generation != self.generation {
            return false;
        }
        let Some(context) = self
            .contexts
            .iter_mut()
            .find(|context| context.name == name)
        else {
            return false;
        };
        context.availability = ContextAvailability::Available;
        context.unavailable_reason = None;
        self.generation = self.generation.wrapping_add(1);
        true
    }

    /// Commit a failed exec credential probe when its snapshot is still current.
    pub fn mark_unavailable(&mut self, generation: u64, name: &str, reason: String) -> bool {
        if generation != self.generation {
            return false;
        }
        let Some(context) = self
            .contexts
            .iter_mut()
            .find(|context| context.name == name)
        else {
            return false;
        };
        context.availability = ContextAvailability::Unavailable;
        context.unavailable_reason = Some(reason);
        self.generation = self.generation.wrapping_add(1);
        true
    }

    /// Keep an available current context or atomically select the first one in
    /// stable kubeconfig order. If none is available, clear the current marker.
    pub fn choose_available_fallback(&mut self) -> Option<String> {
        let selected = self
            .contexts
            .iter()
            .find(|context| {
                context.is_current && context.availability == ContextAvailability::Available
            })
            .or_else(|| {
                self.contexts
                    .iter()
                    .find(|context| context.availability == ContextAvailability::Available)
            })
            .map(|context| context.name.clone());
        let changed = self
            .contexts
            .iter()
            .any(|context| context.is_current != (selected.as_deref() == Some(&context.name)));
        for context in &mut self.contexts {
            context.is_current = selected.as_deref() == Some(context.name.as_str());
        }
        if changed {
            self.generation = self.generation.wrapping_add(1);
        }
        selected
    }

    /// Prepare a candidate switch of the current context toward `to`.
    ///
    /// Validation-only phase of the same prepare-then-commit protocol the
    /// registry itself follows: an unknown destination is rejected here
    /// before anything observable moves, and callers still owe the commit a
    /// successful destination read-path validation. The returned token is
    /// inert until [`Self::commit_switch`] installs it.
    pub fn prepare_switch(&self, to: &str) -> Result<PreparedSwitch, BackendError> {
        let destination = self.find(to).ok_or(BackendError::NotFound)?;
        if destination.availability == ContextAvailability::Unavailable {
            return Err(BackendError::ContextUnavailable {
                context: destination.name.clone(),
                reason: destination
                    .unavailable_reason
                    .clone()
                    .unwrap_or_else(|| "credential plugin is unavailable".into()),
            });
        }
        Ok(PreparedSwitch {
            to: to.to_owned(),
            previous: self
                .contexts
                .iter()
                .find(|context| context.is_current)
                .map(|context| context.name.clone()),
            generation: self.generation,
        })
    }

    /// Commit a prepared switch as one atomic step: exactly the destination
    /// carries the current marker afterwards. Returns the previously current
    /// context name, when one existed.
    pub(crate) fn commit_switch(
        &mut self,
        prepared: PreparedSwitch,
    ) -> Result<Option<String>, BackendError> {
        let PreparedSwitch {
            to,
            previous,
            generation,
        } = prepared;
        if generation != self.generation {
            let destination = self.find(&to).ok_or(BackendError::NotFound)?;
            if destination.availability == ContextAvailability::Unavailable {
                return Err(BackendError::ContextUnavailable {
                    context: destination.name.clone(),
                    reason: destination
                        .unavailable_reason
                        .clone()
                        .unwrap_or_else(|| "credential plugin is unavailable".into()),
                });
            }
            return Err(BackendError::Conflict(
                "context availability changed while the destination was being validated".into(),
            ));
        }
        let destination = self
            .contexts
            .iter_mut()
            .find(|context| context.name == to)
            .ok_or(BackendError::NotFound)?;
        if destination.availability == ContextAvailability::Unavailable {
            return Err(BackendError::ContextUnavailable {
                context: destination.name.clone(),
                reason: destination
                    .unavailable_reason
                    .clone()
                    .unwrap_or_else(|| "credential plugin is unavailable".into()),
            });
        }
        destination.availability = ContextAvailability::Available;
        destination.unavailable_reason = None;
        for context in &mut self.contexts {
            context.is_current = context.name == to;
        }
        self.generation = self.generation.wrapping_add(1);
        Ok(previous)
    }
}

/// A validated but not-yet-committed switch of the current context.
#[derive(Debug, Clone)]
pub struct PreparedSwitch {
    /// Destination context name.
    to: String,
    /// Context holding the current marker before the commit, when any.
    previous: Option<String>,
    /// Registry generation captured before the external destination probe.
    generation: u64,
}

#[cfg(test)]
mod tests {
    use crate::port::{ContextAvailability, ContextInfo};

    use super::ContextRegistry;

    #[test]
    fn stale_prepared_switch_cannot_override_a_newer_unavailable_transition() {
        let mut registry = ContextRegistry::prepare(vec![
            ContextInfo::available("active", "cluster-a", None, true),
            ContextInfo::available("destination", "cluster-b", None, false),
        ])
        .expect("registry prepares");
        let prepared = registry
            .prepare_switch("destination")
            .expect("destination initially prepares");
        let (generation, _) = registry.snapshot();
        assert!(registry.mark_unavailable(
            generation,
            "destination",
            "runtime plugin denied".into()
        ));

        let _ = registry.commit_switch(prepared);

        assert!(registry.find("active").expect("active exists").is_current);
        let destination = registry.find("destination").expect("destination exists");
        assert!(!destination.is_current);
        assert_eq!(destination.availability, ContextAvailability::Unavailable);
    }
}
