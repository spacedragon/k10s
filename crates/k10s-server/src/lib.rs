//! Axum-based control server and embeddable runtime for k10s.

mod auth;
mod config;
mod control;
mod lifecycle;
mod outbound;
mod probes;

pub use config::{ServerConfig, StandaloneConfig, StandaloneConfigError};
pub use lifecycle::{ServerHandle, router, run, run_with_assets, spawn_loopback};
pub use outbound::{EnqueueError, Priority, RevisionGap, ScheduledItem, Scheduler};
pub use probes::{Readiness, ReadinessState};
