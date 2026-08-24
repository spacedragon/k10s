//! The prepare-then-commit context registry served by adapters.
//!
//! Adapters never hand the kernel raw Kubernetes data: they build a candidate
//! [`ContextRegistry`] from validated, credential-free summaries (prepare),
//! and only on success commit it as their immutable bootstrap state. A failed
//! prepare leaves nothing to observe — no partial or ambiguous registries.

use std::collections::HashSet;

use crate::port::{AdapterError, ContextInfo};

/// Committed registry of safe context summaries exposed at bootstrap.
///
/// Built once through [`ContextRegistry::prepare`]; readers always see a
/// complete, consistent snapshot. Summaries carry names and cluster references
/// only — never tokens or certificate material.
#[derive(Debug, Clone)]
pub struct ContextRegistry {
    contexts: Vec<ContextInfo>,
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

    /// Prepare a candidate switch of the current context toward `to`.
    ///
    /// Validation-only phase of the same prepare-then-commit protocol the
    /// registry itself follows: an unknown destination is rejected here
    /// before anything observable moves, and callers still owe the commit a
    /// successful destination read-path validation. The returned token is
    /// inert until [`Self::commit_switch`] installs it.
    pub fn prepare_switch(&self, to: &str) -> Result<PreparedSwitch, AdapterError> {
        if self.find(to).is_none() {
            return Err(AdapterError::InvalidContextSummaries {
                detail: format!("unknown destination context '{to}'"),
            });
        }
        Ok(PreparedSwitch {
            to: to.to_owned(),
            previous: self
                .contexts
                .iter()
                .find(|context| context.is_current)
                .map(|context| context.name.clone()),
        })
    }

    /// Commit a prepared switch as one atomic step: exactly the destination
    /// carries the current marker afterwards. Returns the previously current
    /// context name, when one existed.
    pub(crate) fn commit_switch(&mut self, prepared: PreparedSwitch) -> Option<String> {
        let PreparedSwitch { to, previous } = prepared;
        for context in &mut self.contexts {
            context.is_current = context.name == to;
        }
        previous
    }
}

/// A validated but not-yet-committed switch of the current context.
#[derive(Debug, Clone)]
pub struct PreparedSwitch {
    /// Destination context name.
    to: String,
    /// Context holding the current marker before the commit, when any.
    previous: Option<String>,
}
