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
}
